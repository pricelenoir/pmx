"""Risk (ARCHITECTURE *Execution*, *Failure handling*). Every order a slot wants
passes `RiskManager.check` before the order scheduler or a broker sees it.

Limits come at three scopes — the slot, its strategy, everything — and each applies to its own scope's
numbers: a strategy's `max_open_orders` counts all its slots' working orders, the global `max_loss` all
of today's PnL. Per-order limits (`max_order_qty`, `price_band`, the staleness limits, `windows`) take the
tightest value across the three. Halts (a slot, a strategy, or everything) block every new order. The
staleness, warm-up, and window rules — and every limit that bounds risk — block only orders that *add*
risk: cancels never come here, and an order that only shrinks a position always passes them, so a slot
can flatten after its inputs die.

The order manager supplies positions and working orders (`Positions`), the paper or live balance
(`Balance`), and cancels what `on_cancel` asks for; the portfolio tracker supplies `PortfolioRisk`."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

import msgspec

from pmx.core.types import OrderRequest
from pmx.core.units import ONE_DOLLAR, Micros, Price, Qty, cents, fmt_qty, parse_micros, parse_price, parse_qty
from pmx.engine.events import Event, Exposure, Halt, LogEvent, Mode, RiskReject, ScopeStats, Timer
from pmx.engine.state import MarketId, State, market_id
from pmx.strategies.base import WorkingOrder

log = logging.getLogger(__name__)


class Window(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Around each `Schedule` of `kind` for a slot's market or entity: no new risk from `before_s` before
    it to `after_s` after it; `pull` also cancels the slot's resting quotes when the window opens."""

    kind: str
    before_s: int
    after_s: int
    action: Literal["pull", "block_new"] = "pull"


class RiskLimits(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    max_position: Qty | None = None  # |signed YES| on a market: position + same-side working + the order
    max_order_qty: Qty | None = None
    max_open_orders: int | None = None
    max_loss: Micros | None = None  # today's realized + marked PnL, since local midnight
    price_band: Price | None = None  # max distance from the slot's model fair (else the mid)
    stale_book_ms: int | None = None
    stale_forecast_ms: int | None = None
    stale_input_ms: int | None = None
    windows: list[Window] = []
    max_capital_at_risk: Micros | None = None  # portfolio-level, via PortfolioRisk
    max_worst_case: Micros | None = None
    max_entity_risk: Micros | None = None
    max_drawdown: Micros | None = None

    @classmethod
    def of(cls, table: RiskLimits | Mapping[str, Any] | None) -> RiskLimits:
        """From a config table: quantities and money as strings ("5", "25.00"; whole ints accepted),
        milliseconds and counts as ints."""
        if table is None:
            return cls()
        if isinstance(table, RiskLimits):
            return table
        out: dict[str, Any] = {}
        for key, value in table.items():
            parse = _FIELDS.get(key)
            if parse is None:
                raise ValueError(f"risk: unknown limit {key!r} (known: {', '.join(_FIELDS)})")
            out[key] = parse(key, value)
        return cls(**out)

    @property
    def portfolio_level(self) -> bool:
        return any(v is not None for v in (self.max_capital_at_risk, self.max_worst_case, self.max_entity_risk,
                                           self.max_drawdown))


def _units(parse: Callable[[str], int]) -> Callable[[str, Any], int]:
    def convert(key: str, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, str | int):
            raise ValueError(f"risk: {key} must be a string like \"25.00\", got {value!r}")
        return parse(str(value))
    return convert


def _int(key: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"risk: {key} must be an integer, got {value!r}")
    return value


def _windows(key: str, value: Any) -> list[Window]:
    return msgspec.convert(value, list[Window])


_FIELDS: dict[str, Callable[[str, Any], Any]] = {
    "max_position": _units(parse_qty), "max_order_qty": _units(parse_qty), "max_open_orders": _int,
    "max_loss": _units(parse_micros), "price_band": _units(parse_price), "stale_book_ms": _int,
    "stale_forecast_ms": _int, "stale_input_ms": _int, "windows": _windows,
    "max_capital_at_risk": _units(parse_micros), "max_worst_case": _units(parse_micros),
    "max_entity_risk": _units(parse_micros), "max_drawdown": _units(parse_micros),
}


class Positions(Protocol):
    """Positions, working orders, and PnL by scope — a slot id, a strategy id, or "" for everything —
    in one mode (paper and live never net). The order manager implements it."""

    def position(self, scope: str, ticker: MarketId, mode: Mode | None = None) -> Qty: ...

    def working(self, scope: str, ticker: MarketId | None = None, mode: Mode | None = None) -> list[WorkingOrder]: ...

    def pnl(self, scope: str, mode: Mode, state: State) -> Micros: ...  # realized + marked to mid − fees


class Balance(Protocol):
    def available(self, mode: Mode, shard: int | None = None) -> Micros: ...  # cash free of collateral


class PortfolioRisk(Protocol):
    """The portfolio tracker. Scopes are "slot:<id>", "strategy:<id>", "portfolio"."""

    def what_if(self, slot: str, req: OrderRequest, state: State) -> ScopeStats: ...

    def stats(self, scope: str, mode: str) -> ScopeStats: ...

    def exposures(self, mode: str, strategy: str | None = None) -> list[Exposure]: ...


class SlotInfo(Protocol):
    """What risk reads of a slot (`engine/slots.py:Slot`)."""

    id: str
    mode: Mode
    model: str | None
    inputs: tuple[str, ...]
    risk_limits: Any
    tickers: Mapping[MarketId, Any]

    @property
    def warming(self) -> bool: ...


def strategy_of(slot: str) -> str:
    return slot.split("/", 1)[0]


def _in_scope(scope: str, slot: str) -> bool:
    return not scope or slot == scope or slot.startswith(scope + "/")


def _remaining(orders: Iterable[WorkingOrder], side: str) -> Qty:
    return sum(o.qty - o.filled for o in orders if o.side == side)


class RiskManager:
    """`check` each order; `halt` / `resume` scopes; feed it every event through `on_event` (Timers run
    the kill switch, the daily reset, and window pulls; `Halt` events from elsewhere are honoured)."""

    def __init__(
        self,
        *,
        positions: Positions,
        slots: Mapping[str, SlotInfo] | None = None,
        global_limits: RiskLimits | Mapping[str, Any] | None = None,
        strategy_limits: Mapping[str, RiskLimits | Mapping[str, Any]] | None = None,
        balance: Balance | None = None,
        portfolio: PortfolioRisk | None = None,
        venue_limits: Mapping[MarketId, Qty] | None = None,
        halt_file: Path | None = None,
        tz: str = "America/New_York",
    ) -> None:
        self.positions, self.balance, self.portfolio = positions, balance, portfolio
        self.slots: Mapping[str, SlotInfo] = slots if slots is not None else {}
        self.global_limits = RiskLimits.of(global_limits)
        self.strategy_limits = {k: RiskLimits.of(v) for k, v in (strategy_limits or {}).items()}
        self.venue_limits = dict(venue_limits or {})
        self.halt_file = halt_file
        self.tz = ZoneInfo(tz)
        self.halts: dict[str, str] = {}  # scope ("" = everything) -> reason
        self.on_cancel: list[Callable[[str, MarketId | None, str], None]] = []  # (scope, ticker or None, why)
        self._out: list[Event] = []
        self._slot_limits: dict[str, RiskLimits] = {}
        self._file_halts: dict[str, str] = {}  # scope -> reason, as the kill-switch file last said
        self._day: str | None = None
        self._day_start: dict[tuple[str, str], Micros] = {}  # (scope, mode) -> PnL at the day's first look
        self._pulled: set[tuple[str, MarketId, str, int]] = set()  # (slot, ticker, kind, at_ms) windows pulled
        self._warned = False

    # ---- halts ------------------------------------------------------------------------------------------

    def halt(self, reason: str, scope: str | None = None, now_ms: int = 0, *, level: str = "error") -> None:
        """Halt a slot (`"strategy/key"`), a strategy (`"strategy"`), or everything (None): no new orders
        in scope, and the order manager is asked to cancel its working orders. `level` is the log's (a
        planned stop is no error)."""
        key = scope or ""
        if self.halts.get(key) == reason:
            return
        self.halts[key] = reason
        what = key or "everything"
        self._out += [Halt(key, reason, True, now_ms), LogEvent(level, "risk", f"halted {what}: {reason}", now_ms)]  # type: ignore[arg-type]
        for cancel in self.on_cancel:
            cancel(key, None, f"halted: {reason}")

    def resume(self, scope: str | None = None, now_ms: int = 0) -> None:
        key = scope or ""
        if self.halts.pop(key, None) is not None:
            text = f"resumed {key or 'everything'}"
            self._out += [Halt(key, "", False, now_ms), LogEvent("info", "risk", text, now_ms)]

    def halted(self, slot: str) -> str | None:
        """The reason `slot` is halted (its own, its strategy's, or everything's), if it is."""
        for scope in (slot, strategy_of(slot), ""):
            if scope in self.halts:
                return f"{scope or 'everything'} halted: {self.halts[scope]}"
        return None

    # ---- the engine's view ------------------------------------------------------------------------------

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        if not self._warned:
            self._warned = True
            if self.portfolio is None and any(lim.portfolio_level for lim in self._all_limits()):
                self._out.append(LogEvent("warn", "risk", "portfolio-level limits (capital at risk, worst case, "
                                          "entity risk, drawdown) are configured but not enforced until the "
                                          "portfolio tracker is running", now_ms))
        t = type(ev)
        if t is Timer:
            self._timer(state, now_ms)
        elif t is Halt:
            if ev.active:
                self.halts.setdefault(ev.scope, ev.reason)
            else:
                self.halts.pop(ev.scope, None)
        out, self._out = self._out, []
        return out

    def _all_limits(self) -> list[RiskLimits]:
        slots = [lim for s in self.slots.values() if (lim := self._limits_of(s)) is not None]
        return [self.global_limits, *self.strategy_limits.values(), *slots]

    def _timer(self, state: State, now_ms: int) -> None:
        if self.halt_file is not None:  # the kill switch: `pmx halt` writes the file, `pmx resume` clears it
            want = read_halts(self.halt_file)
            for scope, reason in want.items():
                if self._file_halts.get(scope) != reason:
                    self.halt(f"kill switch: {reason}", scope or None, now_ms)
            for scope in [k for k in self._file_halts if k not in want]:
                self.resume(scope or None, now_ms)
            self._file_halts = want
        day = self._local_day(now_ms)
        if day != self._day:
            self._day, self._day_start = day, {}
        self._watch_losses(state, now_ms)
        for slot in list(self.slots.values()):  # pull quotes as `pull` windows open
            lims = self._limits(slot.id)
            for w in lims.windows:
                if w.action != "pull":
                    continue
                for mid in list(slot.tickers):
                    s = self._window_hit([w], mid, state, now_ms)
                    if s is not None and (slot.id, mid, w.kind, s[1]) not in self._pulled:
                        self._pulled.add((slot.id, mid, w.kind, s[1]))
                        for cancel in self.on_cancel:
                            cancel(slot.id, mid, f"window: {w.kind}")

    def _watch_losses(self, state: State, now_ms: int) -> None:
        """A scope past its `max_loss`, or (with the portfolio tracker) its `max_drawdown` from today's high, is halted
        (and its orders canceled) without waiting for its next order."""
        scopes: dict[tuple[str, Mode], RiskLimits] = {}
        for s in self.slots.values():
            strategy = strategy_of(s.id)
            for scope, lim in ((s.id, self._limits_of(s)), (strategy, self.strategy_limits.get(strategy)),
                               ("", self.global_limits)):
                if lim is not None and (lim.max_loss is not None or lim.max_drawdown is not None):
                    scopes[(scope, s.mode)] = lim
        for (scope, mode), lim in scopes.items():
            if scope in self.halts:
                continue
            if lim.max_loss is not None:
                loss = -self._today(scope, mode, state)
                if loss >= lim.max_loss:
                    why = f"max_loss: lost {_dollars(loss)} today (limit {_dollars(lim.max_loss)})"
                    self.halt(why, scope or None, now_ms)
                    continue
            if lim.max_drawdown is not None and self.portfolio is not None:
                dd = self.portfolio.stats(_scope_name(scope), mode).drawdown
                if dd >= lim.max_drawdown:
                    why = f"max_drawdown: {_dollars(dd)} below today's high (limit {_dollars(lim.max_drawdown)})"
                    self.halt(why, scope or None, now_ms)

    def _local_day(self, now_ms: int) -> str:
        return datetime.fromtimestamp(now_ms / 1000, UTC).astimezone(self.tz).date().isoformat()

    # ---- the check ----------------------------------------------------------------------------------------

    def check(self, slot: str, req: OrderRequest, state: State, now_ms: int, *, mode: Mode | None = None
              ) -> RiskReject | None:
        """None if `req` may go to the scheduler, else the first rule it breaks."""
        s = self.slots.get(slot)
        mode = mode or (s.mode if s is not None else "paper")
        mid = market_id(req.venue, req.ticker)

        def reject(rule: str, detail: str) -> RiskReject:
            return RiskReject(slot, mid, req.side, req.price, req.qty, rule, detail, now_ms)

        if (why := self.halted(slot)) is not None:
            return reject("halted", why)
        c = state.contracts.get(mid)
        if c is None:
            return reject("market_not_open", f"{mid} isn't a known market")
        if c.status != "open":
            return reject("market_not_open", f"{mid} is {c.status}")
        lims = self._limits(slot)
        pos = self.positions.position(slot, mid, mode)
        working = self.positions.working(slot, mid, mode)
        room = (-pos if req.side == "bid" else pos) - _remaining(working, req.side)
        reducing = 0 < req.qty <= room
        book = state.book(mid)
        if not reducing:
            if s is not None and s.warming:
                return reject("warming", f"{slot} is still warming up")
            hit = self._window_hit(lims.windows, mid, state, now_ms)
            if hit is not None:
                return reject("window", hit[0])
            if book is None:
                return reject("stale_book", f"no live book for {mid} (not snapshotted yet, or reset)")
            if lims.stale_book_ms is not None and now_ms - book.ts_ms > lims.stale_book_ms:
                return reject("stale_book", f"book last updated {(now_ms - book.ts_ms) / 1000:.1f}s ago "
                                            f"(limit {lims.stale_book_ms / 1000:g}s)")
            if lims.stale_forecast_ms is not None and s is not None and s.model:
                f = state.forecast(mid, s.model)
                if f is None or now_ms - f.ts_ms > lims.stale_forecast_ms:
                    age = "none yet" if f is None else f"{(now_ms - f.ts_ms) / 1000:.1f}s old"
                    limit = lims.stale_forecast_ms / 1000
                    return reject("stale_forecast", f"model {s.model}: {age} (limit {limit:g}s)")
            if lims.stale_input_ms is not None and s is not None:
                for name in s.inputs:
                    seen = state.inputs.get(name)
                    if seen is None or now_ms - seen[0] > lims.stale_input_ms:
                        age = "nothing yet" if seen is None else f"{(now_ms - seen[0]) / 1000:.1f}s old"
                        return reject("stale_input", f"input {name}: {age} (limit {lims.stale_input_ms / 1000:g}s)")
        if lims.price_band is not None:
            fair, what = self._fair(s, mid, state)
            if fair is not None and abs(req.price - fair) > lims.price_band:
                return reject("price_band", f"{cents(req.price)} is {cents(round(abs(req.price - fair)))} from {what} "
                                            f"{cents(round(fair))} (band {cents(lims.price_band)})")
        if lims.max_order_qty is not None and req.qty > lims.max_order_qty:
            return reject("max_order_qty", f"{fmt_qty(req.qty)} > {fmt_qty(lims.max_order_qty)} contracts")
        if reducing:
            return None
        return (self._scoped(slot, req, mid, mode, state, now_ms, reject)
                or self._portfolio(slot, req, mid, mode, state, reject)
                or self._cash(req, mode, reject, c.shard))

    def _scoped(self, slot: str, req: OrderRequest, mid: MarketId, mode: Mode, state: State, now_ms: int,
                reject: Callable[[str, str], RiskReject]) -> RiskReject | None:
        """Limits that count a scope's numbers: open orders, position, loss — slot, strategy, everything."""
        strategy = strategy_of(slot)
        signed = req.qty if req.side == "bid" else -req.qty
        layers = ((slot, self._limits_of(self.slots.get(slot))), (strategy, self.strategy_limits.get(strategy)),
                  ("", self.global_limits))
        for scope, lim in layers:
            if lim is None:
                continue
            what = scope or "everything"
            if lim.max_open_orders is not None:
                count = getattr(self.positions, "count_working", None)
                n = count(scope, mode) if count is not None else len(self.positions.working(scope, None, mode))
                if n >= lim.max_open_orders:
                    return reject("max_open_orders", f"{what} has {n} open orders (limit {lim.max_open_orders})")
            if lim.max_position is not None:
                pos = self.positions.position(scope, mid, mode)
                after = pos + _remaining(self.positions.working(scope, mid, mode), req.side) * (1 if signed > 0 else -1)
                after += signed
                if abs(after) > lim.max_position:
                    return reject("max_position", f"{what} would hold {fmt_qty(after)} on {mid} counting working "
                                                  f"orders (limit {fmt_qty(lim.max_position)})")
            if lim.max_loss is not None:
                loss = -self._today(scope, mode, state)
                if loss >= lim.max_loss:
                    self.halt(f"max_loss: lost {_dollars(loss)} today (limit {_dollars(lim.max_loss)})",
                              scope or None, now_ms)
                    return reject("max_loss", f"{what} lost {_dollars(loss)} today (limit {_dollars(lim.max_loss)})")
        cap = self.venue_limits.get(mid)
        if cap is not None:
            after = self.positions.position("", mid, mode) + signed
            if abs(after) > cap:
                return reject("venue_limit", f"the exchange caps {mid} at {fmt_qty(cap)} contracts")
        return None

    def _portfolio(self, slot: str, req: OrderRequest, mid: MarketId, mode: Mode, state: State,
                   reject: Callable[[str, str], RiskReject]) -> RiskReject | None:
        """Capital at risk, worst case, entity risk, drawdown — needs the portfolio tracker."""
        if self.portfolio is None:
            return None
        strategy = strategy_of(slot)
        scopes = [(f"strategy:{strategy}", strategy, self.strategy_limits.get(strategy)),
                  ("portfolio", "", self.global_limits)]
        after = None
        for name, scope, lim in scopes:
            if lim is None or not lim.portfolio_level:
                continue
            now_stats = self.portfolio.stats(name, mode)
            if scope == "":
                after = after or self.portfolio.what_if(slot, req, state)
                stats = after
            else:  # no what-if at strategy scope: today's numbers plus the order's own capital at risk
                add = (req.price if req.side == "bid" else ONE_DOLLAR - req.price) * req.qty
                stats = msgspec.structs.replace(now_stats, capital_at_risk=now_stats.capital_at_risk + add)
            what = scope or "the portfolio"
            at_risk = stats.capital_at_risk + stats.working_exposure  # working orders count as if they filled
            if lim.max_capital_at_risk is not None and at_risk > lim.max_capital_at_risk:
                return reject("max_capital_at_risk", f"{what} would have {_dollars(at_risk)} at risk, working orders "
                                                     f"included (limit {_dollars(lim.max_capital_at_risk)})")
            if lim.max_worst_case is not None and stats.worst_case < -lim.max_worst_case:
                return reject("max_worst_case", f"{what}'s worst case would be {_dollars(stats.worst_case)} "
                                                f"(limit -{_dollars(lim.max_worst_case)})")
            if lim.max_drawdown is not None and now_stats.drawdown >= lim.max_drawdown:
                return reject("max_drawdown", f"{what} is {_dollars(now_stats.drawdown)} below its high-water mark "
                                              f"(limit {_dollars(lim.max_drawdown)})")
            if lim.max_entity_risk is not None:
                entity = state.entity_of.get(mid, mid)
                add = (req.price if req.side == "bid" else ONE_DOLLAR - req.price) * req.qty
                for e in self.portfolio.exposures(mode, strategy if scope else None):
                    if e.entity == entity and e.capital_at_risk + add > lim.max_entity_risk:
                        return reject("max_entity_risk", f"{entity} would have {_dollars(e.capital_at_risk + add)} "
                                                         f"at risk (limit {_dollars(lim.max_entity_risk)})")
        return None

    def _cash(self, req: OrderRequest, mode: Mode, reject: Callable[[str, str], RiskReject],
              shard: int = 0) -> RiskReject | None:
        """Buying YES ties up price × qty, selling YES (buying NO) (1 − price) × qty."""
        if self.balance is None:
            return None
        need = (req.price if req.side == "bid" else ONE_DOLLAR - req.price) * req.qty
        have = self.balance.available(mode, shard)
        if need > have:
            where = f", shard {shard}" if mode == "live" and shard else ""
            return reject("insufficient_balance", f"needs {_dollars(need)}, {_dollars(have)} available ({mode}{where})")
        return None

    # ---- helpers ------------------------------------------------------------------------------------------

    def _limits_of(self, s: SlotInfo | None) -> RiskLimits | None:
        if s is None or s.risk_limits is None:
            return None
        lim = self._slot_limits.get(s.id)
        if lim is None:
            lim = self._slot_limits[s.id] = RiskLimits.of(s.risk_limits)
        return lim

    def _limits(self, slot: str) -> RiskLimits:
        """Per-order limits merged across slot, strategy, and global: the tightest of each; windows from all."""
        layers = [x for x in (self._limits_of(self.slots.get(slot)), self.strategy_limits.get(strategy_of(slot)),
                              self.global_limits) if x is not None]

        def tightest(field: str) -> Any:
            values = [getattr(x, field) for x in layers if getattr(x, field) is not None]
            return min(values) if values else None

        return RiskLimits(
            max_order_qty=tightest("max_order_qty"), price_band=tightest("price_band"),
            stale_book_ms=tightest("stale_book_ms"), stale_forecast_ms=tightest("stale_forecast_ms"),
            stale_input_ms=tightest("stale_input_ms"), windows=[w for x in layers for w in x.windows],
        )

    def _window_hit(self, windows: list[Window], mid: MarketId, state: State, now_ms: int) -> tuple[str, int] | None:
        return window_hit(windows, mid, state, now_ms)

    def _fair(self, s: SlotInfo | None, mid: MarketId, state: State) -> tuple[float | None, str]:
        if s is not None and s.model:
            f = state.forecast(mid, s.model)
            if f is not None:
                return f.p * ONE_DOLLAR, f"{s.model}'s fair"
        book = state.book(mid)
        m = None if book is None else book.mid()
        return m, "the mid"

    def _today(self, scope: str, mode: Mode, state: State) -> Micros:
        """Today's PnL in scope: the tracker's `today` when there is one; otherwise the change since this
        manager first looked today (so a restart mid-day counts from the restart)."""
        if self.portfolio is not None:
            return self.portfolio.stats(_scope_name(scope), mode).today
        pnl = self.positions.pnl(scope, mode, state)
        return pnl - self._day_start.setdefault((scope, mode), pnl)


def window_hit(windows: list[Window], mid: MarketId, state: State, now_ms: int) -> tuple[str, int] | None:
    """(why, schedule time) for the first window around a schedule for `mid` or its entity that's open now."""
    if not windows:
        return None
    targets = [mid, *([e] if (e := state.entity_of.get(mid)) is not None else [])]
    for w in windows:
        for target in targets:
            for sch in state.schedules.get(target, ()):
                if sch.kind == w.kind and sch.at_ms - w.before_s * 1000 <= now_ms <= sch.at_ms + w.after_s * 1000:
                    when = datetime.fromtimestamp(sch.at_ms / 1000, UTC).strftime("%H:%M:%SZ")
                    return f"{w.kind} at {when} ({sch.detail}): no new risk {w.before_s}s before to " \
                           f"{w.after_s}s after", sch.at_ms
    return None


def _scope_name(scope: str) -> str:
    """A risk scope (slot id, strategy id, "") as the portfolio tracker names it."""
    return "portfolio" if not scope else f"slot:{scope}" if "/" in scope else f"strategy:{scope}"


# ---- the kill-switch file (`data/HALT`) -------------------------------------------------------------------------
# One halt per line, `{"scope": "<slot id> | <strategy id> | \"\" (everything)", "reason": "…"}`. Any other
# non-empty text halts everything with that text as the reason, so `echo why > data/HALT` works by hand.


def read_halts(path: Path) -> dict[str, str]:
    """scope ("" = everything) -> reason; {} when there's no file."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        return {}
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            h = msgspec.json.decode(line)
            scope, reason = h["scope"], h["reason"]
            if not (isinstance(scope, str) and isinstance(reason, str)):
                raise TypeError
        except (msgspec.DecodeError, KeyError, TypeError):
            scope, reason = "", line
        out[scope] = reason or "kill switch"
    return out or {"": "kill switch"}  # an empty file (`touch data/HALT`) halts everything


def write_halt(path: Path, scope: str, reason: str) -> None:
    halts = read_halts(path) if path.exists() else {}
    halts[scope] = reason
    _write_halts(path, halts)


def clear_halt(path: Path, scope: str | None = None) -> bool:
    """Remove one scope's halt (None: the whole file). True if there was one."""
    if scope is None:
        had = path.exists()
        path.unlink(missing_ok=True)
        return had
    halts = read_halts(path)
    if scope not in halts:
        return False
    del halts[scope]
    _write_halts(path, halts)
    return True


def _write_halts(path: Path, halts: Mapping[str, str]) -> None:
    if not halts:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(msgspec.json.encode({"scope": k, "reason": v}).decode() + "\n" for k, v in halts.items()))
    tmp.replace(path)  # atomic: the engine never reads half a file


def _dollars(m: Micros) -> str:
    return f"{'-' if m < 0 else ''}${abs(m) / 1_000_000:,.2f}"
