"""Strategy slots and the router: dispatch step 4 (ARCHITECTURE *Dispatch*, *Strategies*).

Each `[[strategy]]` (a `StrategySpec`) fans out into slots — one per selected market, or one per
group with `slot_by` — each an isolated strategy instance with its own context, state, CPU budget,
and failure isolation (invariant 7). Slots appear as matching contracts arrive and stop when their
markets close. The router delivers every event only to the slots it concerns, coalesces book/trade/
input hooks for slots with a cadence, and buffers their intents for the order manager, which runs
right after it on the same event and drains them with `take_intents`."""

from __future__ import annotations

import heapq
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Literal

import msgspec

from pmx.core.position import AvgCost
from pmx.core.types import BookDelta, BookSnapshot, Contract, ContractStatus, StreamReset, Trade
from pmx.core.units import cents, fmt_qty
from pmx.engine.events import (
    Advice,
    Event,
    Exposure,
    Forecast,
    Halt,
    LogEvent,
    Mode,
    OrderUpdate,
    RiskReject,
    Schedule,
    ScopeStats,
    SlotFill,
    SlotStatus,
    Timer,
)
from pmx.engine.state import MarketId, State, market_id
from pmx.plugins import LINKERS, STRATEGIES, Linker, PluginError, build_params
from pmx.strategies.base import (
    CancelAll,
    GroupContext,
    Intent,
    MarketContext,
    OrderView,
    PortfolioView,
    Quote,
    Strategy,
    Take,
    Target,
    WorkingOrder,
)

log = logging.getLogger(__name__)

_CLOSED = frozenset({"closed", "determined", "settled"})  # a paused market keeps its slots


class MarketSelector(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Which markets a strategy trades: every contract in `series`, `events`, or `tickers` (nothing
    listed selects nothing), narrowed to those `linker` links to an entity. New slots start only on
    contracts with `status` (None = any)."""

    series: list[str] = []
    tickers: list[str] = []
    events: list[str] = []
    linker: str | None = None
    status: ContractStatus | None = "open"

    def lists(self, c: Contract, mid: MarketId) -> bool:
        series = c.series_ticker or c.event_ticker.partition("-")[0]
        return series in self.series or c.event_ticker in self.events or mid in self.tickers


class ByLinker(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """`slot_by = { linker = "<name>" }`: one slot per key a strategy-specific grouping linker returns."""

    linker: str


class StrategySpec(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One configured strategy (a `[[strategy]]` table in the config)."""

    id: str
    kind: str
    params: dict[str, Any] = {}
    mode: Mode = "paper"
    model: str | None = None  # forecasts from this model instance reach `on_forecast`; `m.forecast()`'s default
    markets: MarketSelector = MarketSelector()
    slot_by: Literal["market", "event", "entity", "all"] | ByLinker = "market"
    inputs: list[str] = []  # added to the class's `inputs`
    advisors: list[str] = []
    cadence_ms: int | None = None  # None = the class's
    warmup_s: float | None = None
    cpu_budget_ms: float = 50  # per second of engine time
    risk: Any = None  # the slot's RiskLimits or its raw table; read here only for `stale_input_ms`


def _risk_value(risk: Any, name: str) -> Any:
    if risk is None:
        return None
    return risk.get(name) if isinstance(risk, dict) else getattr(risk, name, None)


class _Strategy:
    """A validated spec with its class and decoded params."""

    def __init__(self, spec: StrategySpec, linkers: Mapping[str, Linker]) -> None:
        if not spec.id or "/" in spec.id:
            raise PluginError(f"strategy id {spec.id!r} must be non-empty and contain no '/'")
        cls = STRATEGIES.get(spec.kind)
        if cls is None:
            known = ", ".join(sorted(STRATEGIES)) or "none"
            raise PluginError(f"strategy {spec.id!r}: unknown kind {spec.kind!r} (known: {known})")
        if not (isinstance(cls, type) and issubclass(cls, Strategy)):
            raise PluginError(f"strategy {spec.id!r}: kind {spec.kind!r} isn't a Strategy subclass")
        for name in (spec.markets.linker, spec.slot_by.linker if isinstance(spec.slot_by, ByLinker) else None):
            if name is not None and name not in linkers:
                known = ", ".join(linkers) or "none"
                raise PluginError(f"strategy {spec.id!r}: unknown linker {name!r} (known: {known})")
        self.spec, self.id, self.cls = spec, spec.id, cls
        self.params = build_params(cls, spec.params)
        self.inputs = tuple(dict.fromkeys((*cls.inputs, *spec.inputs)))
        self.cadence_ms = cls.cadence_ms if spec.cadence_ms is None else spec.cadence_ms
        self.warmup_ms = round(1000 * (cls.warmup_s if spec.warmup_s is None else spec.warmup_s))
        # Timers go to every slot each second; skip the call where the strategy ignores them.
        self.wants_timer = cls.on_timer is not Strategy.on_timer or cls.on_event is not Strategy.on_event
        self.slots: list[Slot] = []  # live (not stopped)


class Slot:
    """One strategy instance on one market or group. Read by risk (`warming`, `risk_limits`) and
    the order manager (`mode`)."""

    def __init__(self, strategy: _Strategy, key: str) -> None:
        spec = strategy.spec
        self.id = f"{spec.id}/{key}"
        self.strategy, self.key = strategy, key
        self.mode, self.model = spec.mode, spec.model
        self.group = spec.slot_by != "market"
        self.inputs, self.advisors = strategy.inputs, tuple(spec.advisors)
        self.risk_limits = spec.risk
        self.stale_input_ms: int | None = _risk_value(spec.risk, "stale_input_ms")
        self.cadence_ms, self.warmup_ms = strategy.cadence_ms, strategy.warmup_ms
        self.cpu_budget_ns = spec.cpu_budget_ms * 1e6
        self.tickers: dict[MarketId, None] = {}  # current members, in join order
        self.traded: dict[MarketId, None] = {}  # every market it has had (positions outlive membership)
        self.instance: Strategy | None = None
        self.ctx: MarketContext | GroupContext | None = None
        self.outcomes: Callable[[], Mapping[str, Mapping[MarketId, str | int]]] = dict
        self.started_ms = 0
        self.ready = False
        self.error = ""  # set once its strategy raised; the slot is dead until restart
        self.halted = ""  # halt reason while halted
        self.stopped = False
        self.last_action = ""
        self.intents: list[Intent] = []  # buffered by the context during a hook
        self.targets: dict[MarketId, Any] = {}  # market -> its TargetWorker
        self.windows: list | None = None  # the schedule windows in its risk limits (targets pause in them)
        self.worked = -1  # the event its targets last stepped on: once per event (the order manager acts after)
        self.logs: list[LogEvent] = []
        self.pending: dict[tuple[str, str], Event] = {}  # cadence: latest book/trade per market, event per input
        self.due: int | None = None
        self.cpu_sec = -1  # engine second being measured
        self.cpu_ns = 0
        self.cpu_last_over = -2
        self.cpu_over_s = 0  # consecutive engine seconds over budget
        self.cpu_warned = False
        self.status: SlotStatus | None = None  # last published
        self.status_ms = 0

    @property
    def warming(self) -> bool:
        return not self.ready

    @property
    def live(self) -> bool:
        """Its hooks run."""
        return not (self.error or self.halted or self.stopped)

    def state(self, position: int, working: int) -> Literal["warming", "idle", "active", "halted", "error"]:
        if self.error:
            return "error"
        if self.halted:
            return "halted"
        if not self.ready:
            return "warming"
        return "active" if position or working else "idle"


class Router:
    """Dispatch step 4: market events → slots on that market; entity events → slots on its linked
    markets; a `Forecast` → slots on its market trading that model; `Advice` → slots on its target
    listing that advisor; events from a declared input source → every slot of that strategy
    (`on_input`); own fills/orders/rejects → their slot; `Timer` → every slot."""

    cpu_warn_s = 10  # consecutive engine seconds over `cpu_budget_ms` before a warning
    cpu_halt_s = 60  # ... before the slot is halted (reason "slow")

    def __init__(
        self,
        specs: Iterable[StrategySpec],
        *,
        orders: OrderView | None = None,
        portfolio: PortfolioView | None = None,
        linkers: Mapping[str, Linker] | None = None,
        outcomes: Callable[[str], Mapping[str, Mapping[MarketId, str | int]]] | None = None,
        status_ms: int = 1_000,
        cpu_clock: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        self.linkers = LINKERS if linkers is None else linkers
        self.strategies: dict[str, _Strategy] = {}
        for spec in specs:
            if spec.id in self.strategies:
                raise PluginError(f"duplicate strategy id {spec.id!r}")
            self.strategies[spec.id] = _Strategy(spec, self.linkers)
        self.tracker = OrderTracker()  # positions/working from events, unless an order manager is given
        self.orders: OrderView = self.tracker if orders is None else orders
        self.portfolio: PortfolioView = self.tracker if portfolio is None else portfolio
        self.outcomes = outcomes  # entity -> {outcome: {ticker: leg}} (portfolio/scenarios.py:OutcomeMap)
        self.status_ms = status_ms
        self.cpu_clock = cpu_clock  # measures hooks; budgets are per engine second
        self.slots: dict[str, Slot] = {}
        self._of: dict[tuple[str, MarketId], Slot] = {}  # (strategy id, market) -> its slot
        self._on: dict[MarketId, list[Slot]] = {}  # market -> slots trading it
        self._by_input: dict[str, list[_Strategy]] = {}
        for s in self.strategies.values():
            for name in s.inputs:
                self._by_input.setdefault(name, []).append(s)
        self._intents: list[Intent] = []
        self._out: list[Event] = []
        self._due: list[tuple[int, str]] = []  # (due ms, slot id): cadence windows
        self._restore: dict[str, bytes] = {}
        self._state: State | None = None
        # Timers touch only these, so their cost follows activity, not the number of slots.
        self._dirty: dict[str, Slot] = {}  # status may have changed
        self._exposed: dict[str, Slot] = {}  # holds a position: unrealized PnL moves with the market
        self._warming: dict[str, Slot] = {}
        self._stopping: dict[str, Slot] = {}  # stopped, waiting for their last orders to go
        self._targeting: dict[str, Slot] = {}  # with target positions to work
        self._events = 0
        self._routes: dict[type, Callable[[Any, State, int], None]] = {
            BookSnapshot: self._market_event, BookDelta: self._market_event, Trade: self._market_event,
            Forecast: self._forecast, Advice: self._advice, Schedule: self._schedule, Contract: self._contract,
            SlotFill: self._own, OrderUpdate: self._own, RiskReject: self._own, Timer: self._timer, Halt: self._halt,
            StreamReset: self._reset,
        }

    # ---- the engine's view ---------------------------------------------------------------------------

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        self._events += 1
        self._state = self.tracker.state = state
        out = self._out = []
        if self._due and self._due[0][0] <= now_ms:
            self._flush(now_ms)
        route = self._routes.get(type(ev))
        if route is not None:
            route(ev, state, now_ms)
        elif state.is_entity(ev):
            self._entity(ev, state, now_ms)
        if state.source:
            self._input(ev, state.source, now_ms)
        return out

    def take_intents(self) -> list[Intent]:
        """Intents buffered since the last call, in the order slots made them (the order manager's input)."""
        out, self._intents = self._intents, []
        return out

    def slot(self, slot_id: str) -> Slot | None:
        return self.slots.get(slot_id)

    def stop_all(self, now_ms: int) -> list[Event]:
        """Shutdown: every live slot's `on_stop`, and a cancel-all for each."""
        out = self._out = []
        for slot in list(self.slots.values()):
            if not slot.stopped:
                self._stop(slot, now_ms, "shutdown")
        return out

    def checkpoint(self) -> dict[str, bytes]:
        """Each slot's `on_save` blob (`pmx run` persists them with the run and strategy version)."""
        out = {}
        for sid, slot in self.slots.items():
            if slot.instance is None or slot.error:
                continue
            try:
                blob = slot.instance.on_save()
            except Exception:
                log.exception("slot %s: on_save failed", sid)
                continue
            if blob is not None:
                out[sid] = blob
        return out

    def restore(self, blobs: Mapping[str, bytes]) -> None:
        """Checkpoints to hand each slot's `on_load` when it starts."""
        self._restore.update(blobs)

    # ---- routing ---------------------------------------------------------------------------------------

    def _market_event(self, ev: BookSnapshot | BookDelta | Trade, state: State, now_ms: int) -> None:
        mid = market_id(ev.venue, ev.ticker)
        slots = self._on.get(mid)
        if slots:
            key = ("trade" if type(ev) is Trade else "book", mid)
            for slot in slots:
                self._deliver(slot, ev, now_ms, key)

    def _forecast(self, ev: Forecast, state: State, now_ms: int) -> None:
        for slot in self._on.get(ev.ticker, ()):
            if slot.model == ev.model:
                self._deliver(slot, ev, now_ms)

    def _advice(self, ev: Advice, state: State, now_ms: int) -> None:
        if ev.expires_ms < now_ms:
            return
        for slot in self._targeted(ev.target, state):
            if ev.advisor in slot.advisors:
                self._deliver(slot, ev, now_ms)

    def _schedule(self, ev: Schedule, state: State, now_ms: int) -> None:
        for slot in self._targeted(ev.target, state):
            self._deliver(slot, ev, now_ms)

    def _entity(self, ev: Any, state: State, now_ms: int) -> None:
        for slot in self._targeted(ev.entity, state):
            self._deliver(slot, ev, now_ms)

    def _targeted(self, target: str, state: State) -> list[Slot]:
        """Slots on a market, or on any market linked to an entity (each once)."""
        if target in state.contracts:
            return list(self._on.get(target, ()))
        on = self._on
        return list(dict.fromkeys(s for t in sorted(state.tickers_of.get(target, ())) for s in on.get(t, ())))

    def _own(self, ev: SlotFill | OrderUpdate | RiskReject, state: State, now_ms: int) -> None:
        if type(ev) is not RiskReject:
            self.tracker.on_event(ev)
        slot = self.slots.get(ev.slot)
        if slot is None:
            return
        self._dirty[slot.id] = slot
        if type(ev) is RiskReject:
            slot.last_action = f"rejected ({ev.rule}): {ev.detail}"
        elif type(ev) is SlotFill:
            position = self.orders.position
            if any(position(slot.id, t) for t in slot.traded):
                self._exposed[slot.id] = slot
            else:
                self._exposed.pop(slot.id, None)
        self._deliver(slot, ev, now_ms)

    def _input(self, ev: Event, source: str, now_ms: int) -> None:
        for strategy in self._by_input.get(source, ()):
            for slot in strategy.slots:
                if not slot.live or (not slot.ready and not self._warm(slot, now_ms)):
                    continue
                if slot.cadence_ms:
                    self._defer(slot, ("input", source), ev, now_ms)
                else:
                    self._call(slot, slot.instance.on_input, source, ev, slot.ctx)

    def _timer(self, ev: Timer, state: State, now_ms: int) -> None:
        for s in self.strategies.values():
            if s.wants_timer:
                for slot in list(s.slots):
                    self._deliver(slot, ev, now_ms)
        for slot in list(self._warming.values()):
            if slot.live and not slot.strategy.wants_timer:
                self._warm(slot, now_ms)
        for slot in list(self._targeting.values()):
            if not slot.strategy.wants_timer and slot.live and slot.ready:
                self._work(slot, ev, now_ms)
        working = self.orders.working
        for sid, slot in list(self._stopping.items()):
            if not any(working(sid, t) for t in slot.traded):  # its last orders are gone
                del self.slots[sid], self._stopping[sid]
                self._dirty.pop(sid, None)
                self._exposed.pop(sid, None)
        for slot in list({**self._exposed, **self._dirty}.values()):
            if now_ms - slot.status_ms >= self.status_ms:
                self._publish(slot, now_ms)

    def _reset(self, ev: StreamReset, state: State, now_ms: int) -> None:
        """Books dropped: targets on them pull their resting orders now, not on the next Timer."""
        for slot in list(self._targeting.values()):
            if slot.live and slot.ready:
                self._work(slot, ev, now_ms)

    def _halt(self, ev: Halt, state: State, now_ms: int) -> None:
        if not ev.scope:
            slots: Iterable[Slot] = self.slots.values()
        elif ev.scope in self.strategies:
            slots = [s for s in self.slots.values() if s.strategy.id == ev.scope]
        else:
            slots = [self.slots[ev.scope]] if ev.scope in self.slots else []
        for slot in list(slots):
            reason = ev.reason or "halted"
            if ev.active == bool(slot.halted) and (not ev.active or slot.halted == reason):
                continue
            slot.halted = reason if ev.active else ""
            slot.cpu_over_s, slot.cpu_warned = 0, False
            slot.last_action = f"halted: {reason}" if ev.active else "resumed"
            self._publish(slot, now_ms, force=True)

    # ---- slot lifecycle --------------------------------------------------------------------------------

    def _contract(self, c: Contract, state: State, now_ms: int) -> None:
        mid = market_id(c.venue, c.ticker)
        closing = c.status in _CLOSED
        for s in self.strategies.values():
            sel = s.spec.markets
            key = None
            if not closing and sel.lists(c, mid) and (sel.linker is None or self._link(sel.linker, c) is not None):
                key = self._key(s, c, mid, state)
            cur = self._of.get((s.id, mid))
            if cur is not None:
                if cur.key == key:
                    continue
                self._leave(cur, mid, now_ms, c.status if closing else "left the selection")
            if key is not None and (sel.status is None or c.status == sel.status):
                self._join(s, key, mid, now_ms, state)
        for slot in self._on.get(mid, ()):
            self._deliver(slot, c, now_ms)

    def _key(self, s: _Strategy, c: Contract, mid: MarketId, state: State) -> str | None:
        by = s.spec.slot_by
        if by == "market":
            return mid
        if by == "event":
            return market_id(c.venue, c.event_ticker)
        if by == "entity":
            return state.entity_of.get(mid)
        if by == "all":
            return "all"
        return self._link(by.linker, c)

    def _link(self, name: str, c: Contract) -> str | None:
        try:
            return self.linkers[name](c)
        except Exception as e:  # pure functions, but a broken one must not take the router down
            log.exception("linker %s failed on %s", name, c.ticker)
            now = self._state.now_ms if self._state is not None else 0
            self._out.append(LogEvent("error", f"linker:{name}", f"{c.ticker}: {type(e).__name__}: {e}", now))
            return None

    def _join(self, s: _Strategy, key: str, mid: MarketId, now_ms: int, state: State) -> None:
        sid = f"{s.id}/{key}"
        slot = self.slots.get(sid)
        start = slot is None or slot.stopped
        if slot is None:
            slot = self.slots[sid] = Slot(s, key)
            if self.outcomes is not None and slot.group:
                slot.outcomes = lambda slot=slot: self._outcomes(slot)
        slot.tickers[mid] = slot.traded[mid] = None
        self._of[(s.id, mid)] = slot
        self._on.setdefault(mid, []).append(slot)
        self._dirty[sid] = slot
        if start:
            self._start(slot, now_ms, state)

    def _start(self, slot: Slot, now_ms: int, state: State) -> None:
        s = slot.strategy
        slot.stopped, slot.started_ms, slot.ready = False, now_ms, False
        s.slots.append(slot)
        self._stopping.pop(slot.id, None)
        self._warming[slot.id] = slot
        if slot.ctx is None:
            if slot.group:
                slot.ctx = GroupContext(slot, state, self.orders, self.portfolio)
            else:
                slot.ctx = MarketContext(slot, slot.key, state, self.orders, self.portfolio)
        try:
            if slot.instance is None:
                slot.instance = s.cls(s.params)
                blob = self._restore.pop(slot.id, None)
                if blob is not None:
                    slot.instance.on_load(blob)
        except Exception as e:
            self._fail(slot, e, now_ms)
            return
        if slot.error:
            return
        self._call(slot, slot.instance.on_start, slot.ctx)
        if slot.live and self._warm(slot, now_ms):
            self._publish(slot, now_ms, force=True)

    def _leave(self, slot: Slot, mid: MarketId, now_ms: int, why: str) -> None:
        del self._of[(slot.strategy.id, mid)]
        on = self._on[mid]
        on.remove(slot)
        if not on:
            del self._on[mid]
        del slot.tickers[mid]
        slot.targets.pop(mid, None)
        slot.pending = {k: v for k, v in slot.pending.items() if k[1] != mid}
        self._intents.append(CancelAll(slot.id, mid))
        slot.last_action = f"{mid} {why}"
        self._dirty[slot.id] = slot
        if not slot.tickers:
            self._stop(slot, now_ms, f"{mid} {why}")

    def _stop(self, slot: Slot, now_ms: int, why: str) -> None:
        """No more hooks; the slot is dropped once its working orders are gone."""
        if slot.live:
            self._call(slot, slot.instance.on_stop, slot.ctx)
        for mid in list(slot.tickers):
            del self._of[(slot.strategy.id, mid)]
            on = self._on[mid]
            on.remove(slot)
            if not on:
                del self._on[mid]
        slot.tickers.clear()
        slot.intents.clear()
        slot.targets.clear()
        self._targeting.pop(slot.id, None)
        self._intents.append(CancelAll(slot.id))
        slot.stopped, slot.pending, slot.due = True, {}, None
        if slot in slot.strategy.slots:
            slot.strategy.slots.remove(slot)
        self._warming.pop(slot.id, None)
        self._stopping[slot.id] = slot
        slot.last_action = f"stopped: {why}"
        self._publish(slot, now_ms, force=True)

    def _outcomes(self, slot: Slot) -> Mapping[str, Mapping[MarketId, str | int]]:
        """The domain's outcomes for the entity behind a group, limited to its markets."""
        assert self.outcomes is not None and self._state is not None
        if slot.strategy.spec.slot_by == "entity":
            entity = slot.key
        else:
            entities = {self._state.entity_of.get(t) for t in slot.tickers}
            if len(entities) != 1 or None in entities:
                return {}
            entity = entities.pop()
        members = slot.tickers
        return {name: {t: side for t, side in legs.items() if t in members}
                for name, legs in self.outcomes(entity).items()}

    # ---- calling strategy code ---------------------------------------------------------------------

    def _deliver(self, slot: Slot, ev: Event, now_ms: int, key: tuple[str, str] | None = None) -> None:
        if not slot.live or (not slot.ready and not self._warm(slot, now_ms)):
            return
        if key is not None and slot.cadence_ms:
            self._defer(slot, key, ev, now_ms)
        else:
            self._call(slot, slot.instance.on_event, ev, slot.ctx)
        if slot.targets and slot.live:
            self._work(slot, ev, now_ms)

    def _defer(self, slot: Slot, key: tuple[str, str], ev: Event, now_ms: int) -> None:
        """Cadence: the first change opens a window; when it ends, the hook runs once per
        market/input with the latest state."""
        slot.pending[key] = ev
        if slot.due is None:
            slot.due = now_ms + slot.cadence_ms
            heapq.heappush(self._due, (slot.due, slot.id))

    def _flush(self, now_ms: int) -> None:
        due = self._due
        while due and due[0][0] <= now_ms:
            at, sid = heapq.heappop(due)
            slot = self.slots.get(sid)
            if slot is None or slot.due != at:
                continue
            pending, slot.pending, slot.due = slot.pending, {}, None
            if not slot.ready and not self._warm(slot, now_ms):
                continue
            for (kind, name), ev in pending.items():
                if not slot.live:
                    break
                if kind == "input":
                    self._call(slot, slot.instance.on_input, name, ev, slot.ctx)
                else:
                    self._call(slot, slot.instance.on_event, ev, slot.ctx)

    def _call(self, slot: Slot, fn: Callable[..., Any], *args: Any) -> None:
        """Run one hook: CPU accounting, isolation, then hand over its logs and intents."""
        now = self._state.now_ms if self._state is not None else 0
        if now // 1000 != slot.cpu_sec:
            self._cpu_second(slot, now)
            if not slot.live:
                return
        clock = self.cpu_clock
        t0 = clock()
        try:
            fn(*args)
        except Exception as e:
            slot.cpu_ns += clock() - t0
            self._fail(slot, e, now)
            return
        slot.cpu_ns += clock() - t0
        if slot.logs:
            self._out += slot.logs
            slot.logs = []
        targeted = fn == self._step_targets or any(type(i) is Target for i in slot.intents)
        if targeted and fn != self._step_targets and not self._set_targets(slot, now):
            return
        if slot.intents:
            self._intents += slot.intents
            if not targeted:  # a worker's own progress line says more
                slot.last_action = describe(slot.intents[-1], slot.group)
            slot.intents = []
            self._dirty[slot.id] = slot

    # ---- target positions ------------------------------------------------------------------------

    def _set_targets(self, slot: Slot, now_ms: int) -> bool:
        """A hook's `Target` intents replace its workers on those markets, which act on them at once (their quotes
        and takes follow the hook's intents; the `Target`s stay in the list for whoever watches — the order manager
        ignores them). False if a target was malformed (the slot fails)."""
        from pmx.execution.algos import TargetWorker

        targets = [i for i in slot.intents if type(i) is Target]
        try:
            for t in targets:
                slot.targets[t.ticker] = TargetWorker(t, now_ms)
            self._step_targets(slot, dict.fromkeys(t.ticker for t in targets), now_ms)
        except Exception as e:
            self._fail(slot, e, now_ms)
            return False
        self._targeting[slot.id] = slot
        return True

    def _work(self, slot: Slot, ev: Event, now_ms: int) -> None:
        """Step the workers an event concerns: their market's book or prints, their own fills, orders, and rejects;
        all of them on a Timer."""
        t = type(ev)
        if t is BookSnapshot or t is BookDelta or t is Trade:
            tickers = [market_id(ev.venue, ev.ticker)]
        elif t is SlotFill:
            tickers = [market_id(ev.fill.venue, ev.fill.ticker)]
        elif t is OrderUpdate:
            tickers = [ev.ticker]
        elif t is RiskReject:
            w = slot.targets.get(ev.ticker)
            if w is not None:
                w.rejected(ev.rule, now_ms)
                self._progress(slot, w)
            return
        elif t is Timer or t is StreamReset:
            tickers = list(slot.targets)
        else:
            return
        tickers = [k for k in tickers if k in slot.targets]
        if tickers and slot.worked != self._events:
            self._call(slot, self._step_targets, slot, tickers, now_ms)

    def _step_targets(self, slot: Slot, tickers: Iterable[MarketId], now_ms: int) -> None:
        from pmx.execution.risk import RiskLimits, window_hit

        slot.worked = self._events
        if slot.windows is None:
            slot.windows = RiskLimits.of(slot.risk_limits).windows if slot.risk_limits is not None else []
        state = self._state
        for ticker in tickers:
            w = slot.targets.get(ticker)
            if w is None:
                continue
            hit = window_hit(slot.windows, ticker, state, now_ms) if slot.windows and state is not None else None
            w.step(slot.ctx.market(ticker), None if hit is None else hit[0])
            if w.done:
                del slot.targets[ticker]
            self._progress(slot, w)
        if not slot.targets:
            self._targeting.pop(slot.id, None)

    def _progress(self, slot: Slot, w: Any) -> None:
        text = f"{w.ticker}: {w.progress}" if slot.group else w.progress
        if text != slot.last_action:
            slot.last_action = text
            self._dirty[slot.id] = slot

    def _warm(self, slot: Slot, now_ms: int) -> bool:
        """Leave `warming` once `warmup_s` has passed and `Strategy.ready` agrees. False if it raised."""
        if now_ms - slot.started_ms < slot.warmup_ms:
            return True
        try:
            slot.ready = bool(slot.instance.ready(slot.ctx))
        except Exception as e:
            self._fail(slot, e, now_ms)
            return False
        if slot.ready:
            self._warming.pop(slot.id, None)
            self._publish(slot, now_ms, force=True)
        return True

    def _fail(self, slot: Slot, e: Exception, now_ms: int) -> None:
        """A strategy raised: the slot stops, its orders are canceled, siblings carry on."""
        slot.intents = []
        slot.targets.clear()
        self._targeting.pop(slot.id, None)
        if slot.logs:
            self._out += slot.logs
            slot.logs = []
        slot.error = f"{type(e).__name__}: {e}"
        slot.last_action = f"error: {slot.error}"
        slot.pending, slot.due = {}, None
        self._warming.pop(slot.id, None)
        self._intents.append(CancelAll(slot.id))
        log.error("slot %s raised", slot.id, exc_info=e)
        text = f"strategy raised {slot.error}; slot stopped, orders canceled"
        self._out.append(LogEvent("error", slot.id, text, now_ms))
        self._publish(slot, now_ms, force=True)

    def _cpu_second(self, slot: Slot, now_ms: int) -> None:
        """Close the engine second just measured; warn or halt a slot over budget for long."""
        sec, prev = now_ms // 1000, slot.cpu_sec
        if prev >= 0:
            if slot.cpu_ns > slot.cpu_budget_ns:
                slot.cpu_over_s = slot.cpu_over_s + 1 if prev == slot.cpu_last_over + 1 else 1
                slot.cpu_last_over = prev
            else:
                slot.cpu_over_s = 0
            if sec > prev + 1:  # idle seconds in between were under budget
                slot.cpu_over_s = 0
        slot.cpu_sec, slot.cpu_ns = sec, 0
        over = slot.cpu_over_s
        if over >= self.cpu_halt_s:
            text = f"over its CPU budget ({slot.cpu_budget_ns / 1e6:g} ms/s) for {over} s; halted"
            slot.halted = "slow"
            slot.last_action = "halted: slow"
            slot.pending, slot.due = {}, None
            self._intents.append(CancelAll(slot.id))
            self._out += [LogEvent("error", slot.id, text, now_ms), Halt(slot.id, "slow", True, now_ms)]
            self._publish(slot, now_ms, force=True)
        elif over >= self.cpu_warn_s and not slot.cpu_warned:
            slot.cpu_warned = True
            text = f"over its CPU budget ({slot.cpu_budget_ns / 1e6:g} ms/s) for {over} s; halts at {self.cpu_halt_s} s"
            self._out.append(LogEvent("warn", slot.id, text, now_ms))
        elif not over:
            slot.cpu_warned = False

    # ---- status ------------------------------------------------------------------------------------------

    def _publish(self, slot: Slot, now_ms: int, *, force: bool = False) -> None:
        """Queue a `SlotStatus` if anything changed: at once on a state change (or `force`), else at
        most every `status_ms`."""
        orders = self.orders
        position = working = 0
        for t in slot.traded:
            position += orders.position(slot.id, t)
            working += len(orders.working(slot.id, t))
        stats = self.portfolio.stats(f"slot:{slot.id}", slot.mode)
        st = SlotStatus(slot.id, slot.strategy.id, slot.key, slot.mode, slot.state(position, working), position,
                        stats.realized, stats.unrealized, stats.fees, working, slot.last_action, now_ms)
        last = slot.status
        if last is not None and msgspec.structs.replace(last, ts_ms=now_ms) == st:
            self._dirty.pop(slot.id, None)
            return
        if not force and last is not None and last.state == st.state and now_ms - slot.status_ms < self.status_ms:
            return
        self._out.append(st)
        slot.status, slot.status_ms = st, now_ms
        self._dirty.pop(slot.id, None)


def describe(i: Intent, group: bool = False) -> str:
    """One line for `SlotStatus.last_action`."""
    match i:
        case Quote(bid=bid, ask=ask):
            text = f"quote {_level(bid)} / {_level(ask)}"
        case Take(side=side, limit=limit, qty=qty):
            text = f"take {'buy' if side == 'bid' else 'sell'} {_qty(qty)} @ {cents(limit)}"
        case CancelAll():
            text = "cancel all"
        case Target(qty=qty, style=style):
            text = f"target {_qty(qty)} ({style})"
    ticker = getattr(i, "ticker", None)
    return f"{ticker}: {text}" if group and ticker else text


def _qty(q: int) -> str:
    return fmt_qty(q).rstrip("0").rstrip(".")


def _level(level: tuple[int, int] | None) -> str:
    return "–" if level is None else f"{_qty(level[1])} @ {cents(level[0])}"


# ---- positions and working orders from events ----------------------------------------------------


class OrderTracker:
    """Positions, working orders, and slot PnL from `SlotFill`/`OrderUpdate` events: the router's
    `OrderView` when there's no order manager (tests, the scenario kit), and a minimal `PortfolioView`
    (realized, unrealized at mid, fees, equity, gross exposure, open positions) when there's no portfolio tracker."""

    def __init__(self) -> None:
        self.state: State | None = None  # for marks; the router binds it
        self._pos: dict[str, dict[MarketId, AvgCost]] = {}
        self._working: dict[str, dict[MarketId, dict[str, WorkingOrder]]] = {}

    def on_event(self, ev: SlotFill | OrderUpdate) -> None:
        if type(ev) is SlotFill:
            f = ev.fill
            by_ticker = self._pos.setdefault(ev.slot, {})
            mid = market_id(f.venue, f.ticker)
            p = by_ticker.get(mid)
            if p is None:
                p = by_ticker[mid] = AvgCost(ev.mode)
            p.fill(f.qty if f.side == "bid" else -f.qty, f.price, f.fee)
        else:
            orders = self._working.setdefault(ev.slot, {}).setdefault(ev.ticker, {})
            if ev.status in ("queued", "pending", "open"):
                prev = orders.get(ev.client_order_id)
                orders[ev.client_order_id] = WorkingOrder(
                    ev.client_order_id, ev.side, ev.price, ev.qty, ev.filled, ev.status,
                    None if prev is None else prev.queue_ahead, ev.ts_ms if prev is None else prev.placed_ms)
            else:
                orders.pop(ev.client_order_id, None)

    def position(self, slot: str, ticker: MarketId) -> int:
        p = self._pos.get(slot, {}).get(ticker)
        return 0 if p is None else p.qty

    def working(self, slot: str, ticker: MarketId) -> list[WorkingOrder]:
        return list(self._working.get(slot, {}).get(ticker, {}).values())

    def throttled(self, slot: str) -> bool:
        return any(o.status == "queued" for orders in self._working.get(slot, {}).values() for o in orders.values())

    def stats(self, scope: str, mode: str) -> ScopeStats:
        kind, _, name = scope.partition(":")
        realized = unrealized = fees = gross = n = 0
        stale = False
        if kind == "slot":
            scoped = [self._pos.get(name, {})]
        else:
            scoped = [ps for slot, ps in self._pos.items() if kind != "strategy" or slot.startswith(name + "/")]
        for by_ticker in scoped:
            for ticker, p in by_ticker.items():
                if p.mode != mode:
                    continue
                realized += p.realized
                fees += p.fees
                if not p.qty:
                    continue
                n += 1
                book = self.state.book(ticker) if self.state is not None else None
                mid = None if book is None else book.mid()
                if mid is None:
                    stale = True
                    continue
                value = round(p.qty * mid)
                unrealized += value - p.cost
                gross += abs(value)
        equity = realized + unrealized - fees
        return ScopeStats(scope, mode, realized, unrealized, fees, equity, 0, 0, 0, 0, 0, None, None, gross, 0, n,  # type: ignore[arg-type]
                          stale)

    def exposures(self, mode: str, strategy: str | None = None) -> list[Exposure]:
        return []
