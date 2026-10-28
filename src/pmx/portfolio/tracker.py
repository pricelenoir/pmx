"""The portfolio tracker (dispatch step 2b; ARCHITECTURE *Portfolio*). Sees every fill, order update, and
settlement; marks positions to book mids lazily (nothing runs per market-data event); and answers, per mode
and scope — `slot:<id>`, `strategy:<id>`, `portfolio` — realized and unrealized PnL, fees, equity, today's
change and drawdown, capital at risk, scenario worst case, EV and stdev under the model and the market, gross
and working exposure. Risk asks it before orders (`what_if`); the router hands it to slots (`m.portfolio`);
viewers get a `PortfolioSnapshot` each second. It writes equity snapshots and per-fill annotations (fair,
mid, spread, phase, markouts at +10 s / +1 m / +5 m of engine time) to the ledger.

One engine runs one env, so every scope here is that env's; paper and live never net. A settled market's
positions can't change again: they're folded into per-scope realized and fee totals and leave the live set, so the
per-Timer work follows the open book, not the run's whole history."""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from pmx.core.position import AvgCost
from pmx.core.types import Contract, OrderRequest
from pmx.core.units import ONE_DOLLAR, Micros, Price, Qty
from pmx.engine.events import (
    Event,
    Exposure,
    LimitUsage,
    Mode,
    OrderUpdate,
    PortfolioSnapshot,
    ScopeStats,
    SlotFill,
    Timer,
)
from pmx.engine.state import MarketId, State, market_id
from pmx.ledger.db import Ledger
from pmx.portfolio import scenarios as sc

PORTFOLIO = "portfolio"
MARKOUTS = (("10s", 10_000), ("1m", 60_000), ("5m", 300_000))
_WORKING = frozenset({"queued", "pending", "open"})

type _Held = tuple[str, MarketId, AvgCost]  # (slot, market, position)


def slot_scope(slot: str) -> str:
    return f"slot:{slot}"


def strategy_scope(strategy: str) -> str:
    return f"strategy:{strategy}"


def _capital(qty: Qty, mark: float) -> Micros:
    """The most a position can still lose at settlement: long YES its value, long NO (1 − mid) a contract."""
    return round(qty * mark) if qty > 0 else round(-qty * (ONE_DOLLAR - mark))


class PortfolioTracker:
    equity_every_ms = 10_000
    snapshot_every_ms = 1_000

    def __init__(self, *, slots: Mapping[str, Any] | None = None, outcomes: sc.OutcomeMap | None = None,
                 ledger: Ledger | None = None, risk: Any = None, tz: str = "America/New_York") -> None:
        """`slots` is the router's slot table (each slot's mode, model, and limits); `outcomes` the domains'
        outcome map (built on the engine's state on first use when not given); `risk` the risk manager, whose
        configured limits the snapshot reports usage of."""
        self.slots: Mapping[str, Any] = slots if slots is not None else {}
        self.outcomes, self.ledger, self.risk = outcomes, ledger, risk
        self.tz = ZoneInfo(tz)
        self.state: State | None = None
        self.now_ms = 0
        self._pos: dict[tuple[Mode, str, MarketId], AvgCost] = {}
        self._closed: dict[tuple[Mode, str], list[Micros]] = {}  # (mode, scope) -> [realized, fees] of settled ones
        self._working: dict[str, OrderUpdate] = {}
        self._working_in: dict[tuple[str, str], dict[str, OrderUpdate]] = {}  # (mode, scope) -> its working orders
        self._last_mid: dict[MarketId, float] = {}
        self._fill_ids: set[tuple[str, str]] = set()
        self._settled: set[tuple[Mode, MarketId]] = set()
        self._markouts: list[tuple[int, int, str, Mode, str, str, MarketId, int, Price]] = []
        self._seq = 0
        self._day: str | None = None
        self._open: dict[tuple[str, Mode], Micros] = {}  # equity when the local day began
        self._high: dict[tuple[str, Mode], Micros] = {}  # today's high-water mark
        self._written: dict[tuple[str, Mode], tuple[Micros, ...]] = {}
        self._next_equity: int | None = None
        self._next_snapshot: int | None = None
        self._cache: dict[tuple[str, Mode], ScopeStats] = {}

    # ---- the engine's view ---------------------------------------------------------------------------------

    def start(self, now_ms: int) -> list[tuple[str, Mode, list[tuple[int, Micros]]]]:
        """After a restart: positions from the ledger's fills and settlements, today's opening equity and
        high-water mark from its equity snapshots. Returns the last day of equity per portfolio and strategy
        scope, for viewers' curves."""
        self.now_ms = now_ms
        if self.ledger is None:
            self._roll(now_ms)
            return []
        for mode in ("paper", "live"):
            for slot, f in self.ledger.fills(mode):
                self._fill_ids.add((f.venue, f.fill_id))
                self._book(mode, slot, market_id(f.venue, f.ticker), f.side, f.qty, f.price, f.fee)
            for (venue, ticker), (price, _) in self.ledger.settlements(mode).items():
                self._settle(mode, market_id(venue, ticker), price)
        self._roll(now_ms)
        out = []
        for mode in self._modes():
            for scope in [PORTFOLIO, *(strategy_scope(s) for s in self._strategies(mode))]:
                points = self.ledger.equity_series(mode, scope, now_ms - 86_400_000)
                if points:
                    out.append((scope, mode, points))
        return out

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        self.state, self.now_ms = state, now_ms
        if self.outcomes is None:
            self.outcomes = sc.OutcomeMap(state)
        self._cache.clear()  # numbers hold for the rest of this event's dispatch (risk asks during it)
        out: list[Event] = []
        t = type(ev)
        if t is SlotFill:
            self._fill(ev, state, now_ms)
        elif t is OrderUpdate:
            scopes = (PORTFOLIO, strategy_scope(ev.slot.split("/", 1)[0]), slot_scope(ev.slot))
            if ev.status in _WORKING:
                self._working[ev.client_order_id] = ev
                for scope in scopes:
                    self._working_in.setdefault((ev.mode, scope), {})[ev.client_order_id] = ev
            else:
                self._working.pop(ev.client_order_id, None)
                for scope in scopes:
                    self._working_in.get((ev.mode, scope), {}).pop(ev.client_order_id, None)
        elif t is Contract:
            self._contract(ev)
        if self._markouts and self._markouts[0][0] <= now_ms:
            self._due_markouts(state, now_ms)
        if t is Timer:
            self._timer(now_ms, out)
        return out

    # ---- views: risk (PortfolioRisk), slots (PortfolioView) ---------------------------------------------------

    def stats(self, scope: str, mode: str) -> ScopeStats:
        key = (scope, mode)
        hit = self._cache.get(key)
        if hit is None:
            hit = self._cache[key] = self._stats(scope, mode)  # type: ignore[arg-type]
        return hit

    def exposures(self, mode: str, strategy: str | None = None) -> list[Exposure]:
        """One row per entity with an open position, most at risk first."""
        scope = PORTFOLIO if strategy is None else strategy_scope(strategy)
        rows = [e.row(mode) for e in self._entities(list(self._held(scope, mode)), scope)]  # type: ignore[arg-type]
        return sorted(rows, key=lambda r: (r.worst_case, r.entity))

    def position(self, slot: str) -> Qty:
        return sum(p.qty for (_, s, _), p in self._pos.items() if s == slot)

    def what_if(self, slot: str, req: OrderRequest, state: State) -> ScopeStats:
        """The portfolio if `req` filled in full at its limit (fees aside): risk's pre-check."""
        self.state = state
        s = self.slots.get(slot)
        mode: Mode = s.mode if s is not None else "paper"
        key = (mode, slot, market_id(req.venue, req.ticker))
        pos = dict(self._pos)
        p = AvgCost(mode)
        if (old := pos.get(key)) is not None:
            p.qty, p.cost, p.realized, p.fees = old.qty, old.cost, old.realized, old.fees
        p.fill(req.qty if req.side == "bid" else -req.qty, req.price, 0)
        pos[key] = p
        return self._stats(PORTFOLIO, mode, pos, keep=False)

    # ---- what happens ---------------------------------------------------------------------------------------

    def _fill(self, ev: SlotFill, state: State, now_ms: int) -> None:
        f = ev.fill
        if (f.venue, f.fill_id) in self._fill_ids:
            return
        self._fill_ids.add((f.venue, f.fill_id))
        mid = market_id(f.venue, f.ticker)
        self._book(ev.mode, ev.slot, mid, f.side, f.qty, f.price, f.fee)
        self._cache.clear()
        if self.ledger is None:
            return
        s = self.slots.get(ev.slot)
        model = getattr(s, "model", None)
        fc = state.forecast(mid, model) if model else None
        book = state.book(mid)
        bid, ask = (book.best_bid(), book.best_ask()) if book is not None else (None, None)
        data = state.data(mid)
        self.ledger.record_fill_meta(
            f.venue, ev.mode, f.fill_id, now_ms, ev.slot, entity=state.entity_of.get(mid), model=model,
            model_version=fc.version if fc is not None else None, fair=fc.p if fc is not None else None,
            mid=book.mid() if book is not None else None,
            spread=ask[0] - bid[0] if bid is not None and ask is not None else None,
            phase=getattr(data, "phase", None))
        signed = f.qty if f.side == "bid" else -f.qty
        for horizon, after in MARKOUTS:
            self._seq += 1
            heapq.heappush(self._markouts, (now_ms + after, self._seq, f.venue, ev.mode, f.fill_id, horizon, mid,
                                            signed, f.price))
        self._write_equity(now_ms, [PORTFOLIO, strategy_scope(ev.slot.split("/", 1)[0]), slot_scope(ev.slot)],
                           ev.mode)

    def _due_markouts(self, state: State, now_ms: int) -> None:
        """Each fill marked to the book's mid when +10 s / +1 m / +5 m of engine time has passed (signed: what
        the fill made in YES terms if closed at that mid)."""
        assert self.ledger is not None
        due = self._markouts
        while due and due[0][0] <= now_ms:
            _, _, venue, mode, fill_id, horizon, mid, signed, price = heapq.heappop(due)
            book = state.book(mid)
            m = book.mid() if book is not None else None
            self.ledger.set_markout(venue, mode, fill_id, horizon,  # type: ignore[arg-type]
                                    None if m is None else round(signed * (m - price)))

    def _book(self, mode: Mode, slot: str, mid: MarketId, side: str, qty: Qty, price: Price, fee: Micros) -> None:
        p = self._pos.get((mode, slot, mid))
        if p is None:
            p = self._pos[(mode, slot, mid)] = AvgCost(mode)
        p.fill(qty if side == "bid" else -qty, price, fee)

    def _contract(self, c: Contract) -> None:
        price = c.settlement if c.settlement is not None else (
            ONE_DOLLAR if c.result == "yes" else 0 if c.result == "no" else None)
        if c.status == "settled" and price is not None:
            mid = market_id(c.venue, c.ticker)
            for mode in ("paper", "live"):
                if (mode, mid) not in self._settled:
                    self._settle(mode, mid, price)
            self._cache.clear()

    def _settle(self, mode: Mode, mid: MarketId, price: Price) -> None:
        self._settled.add((mode, mid))
        done = [(key, p) for key, p in self._pos.items() if key[0] == mode and key[2] == mid]
        for _, p in done:
            if p.qty:
                p.settle(price)
        if self.ledger is not None and done:  # the settled values, before they leave the live set
            self._cache.clear()
            self._write_equity(self.now_ms, [slot_scope(slot) for (_, slot, _), _ in done], mode)
        for key, p in done:
            del self._pos[key]
            slot = key[1]
            for scope in (PORTFOLIO, strategy_scope(slot.split("/", 1)[0]), slot_scope(slot)):
                tot = self._closed.setdefault((mode, scope), [0, 0])
                tot[0] += p.realized
                tot[1] += p.fees

    def _timer(self, now_ms: int, out: list[Event]) -> None:
        self._roll(now_ms)
        if self._next_equity is None or now_ms >= self._next_equity:
            self._next_equity = now_ms + self.equity_every_ms
            for mode in self._modes():
                self._write_equity(now_ms, self._scopes(mode), mode)
        if self._next_snapshot is None or now_ms >= self._next_snapshot:
            self._next_snapshot = now_ms + self.snapshot_every_ms
            out.append(self.snapshot(now_ms))

    def snapshot(self, now_ms: int) -> PortfolioSnapshot:
        from pmx.portfolio.metrics import limit_usage

        scopes: list[ScopeStats] = []
        exposures: list[Exposure] = []
        limits: list[LimitUsage] = []
        for mode in self._modes():
            scopes += [self.stats(s, mode) for s in self._scopes(mode, slots="open")]
            exposures += self.exposures(mode)
            limits += limit_usage(self, mode)
        return PortfolioSnapshot(now_ms, scopes, exposures, limits)

    # ---- the day: today's change and high-water mark ------------------------------------------------------------

    def _local_day(self, now_ms: int) -> tuple[str, int]:
        t = datetime.fromtimestamp(now_ms / 1000, self.tz)
        midnight = t.replace(hour=0, minute=0, second=0, microsecond=0)
        return t.date().isoformat(), round(midnight.timestamp() * 1000)

    def _roll(self, now_ms: int) -> None:
        day, midnight = self._local_day(now_ms)
        if day == self._day:
            return
        first = self._day is None
        self._day = day
        self._open, self._high = {}, {}
        self._cache.clear()
        for mode in self._modes():
            opening = self.ledger.equity_before(mode, midnight) if first and self.ledger is not None else {}
            highs = self.ledger.equity_high_since(mode, midnight) if first and self.ledger is not None else {}
            for scope in self._scopes(mode):
                eq = self._stats(scope, mode, keep=False).equity
                self._open[(scope, mode)] = opening.get(scope, eq)
                self._high[(scope, mode)] = max(highs.get(scope, eq), eq, self._open[(scope, mode)])

    # ---- the numbers ----------------------------------------------------------------------------------------

    def _held(self, scope: str, mode: str, pos: Mapping[tuple[Mode, str, MarketId], AvgCost] | None = None
              ) -> Iterator[_Held]:
        kind, _, name = scope.partition(":")
        prefix = name + "/"
        for (m, slot, mid), p in (self._pos if pos is None else pos).items():
            if m != mode or (kind == "slot" and slot != name) or (kind == "strategy" and not slot.startswith(prefix)):
                continue
            yield slot, mid, p

    def _mark(self, mid: MarketId, p: AvgCost | None = None) -> tuple[float, bool]:
        """(mid, stale): the book's mid, else the last one seen, else the position's average price."""
        book = self.state.book(mid) if self.state is not None else None
        m = book.mid() if book is not None else None
        if m is not None:
            self._last_mid[mid] = m
            return m, False
        last = self._last_mid.get(mid)
        if last is not None:
            return last, True
        return (p.cost / p.qty if p is not None and p.qty else ONE_DOLLAR / 2), True

    def _stats(self, scope: str, mode: Mode, pos: Mapping[tuple[Mode, str, MarketId], AvgCost] | None = None, *,
               keep: bool = True) -> ScopeStats:
        held = list(self._held(scope, mode, pos))
        realized, fees = self._closed.get((mode, scope), (0, 0))
        unrealized = cap = gross = n = 0
        stale = False
        for _, mid, p in held:
            realized += p.realized
            fees += p.fees
            if not p.qty:
                continue
            n += 1
            mark, old = self._mark(mid, p)
            stale |= old
            value = round(p.qty * mark)
            unrealized += value - p.cost
            gross += abs(value)
            cap += _capital(p.qty, mark)
        ents = self._entities(held, scope)
        worst = sum(e.worst for e in ents)
        modeled = [e for e in ents if e.ev_model is not None]
        ev_model = stdev = None
        if modeled:  # entities without a model forecast count at the market's odds
            ev_model = sum((e.ev_model if e.ev_model is not None else e.ev_market or (0, 0.0))[0] for e in ents)
            stdev = sc.stdev((e.ev_model or e.ev_market or (0, 0.0))[1] for e in ents)
        equity = realized + unrealized - fees
        key = (scope, mode)
        opening = self._open.get(key, 0)
        high = max(self._high.get(key, opening), equity)
        if keep:
            self._high[key] = high
        return ScopeStats(scope, mode, realized, unrealized, fees, equity, equity - opening, high, high - equity, cap,
                          worst, ev_model, stdev, gross, self._working_exposure(scope, mode, pos), n, stale)

    def _working_exposure(self, scope: str, mode: str, pos: Mapping | None) -> Micros:
        """Extra capital at risk if the working orders in scope filled: per market, whichever of all its bids or
        all its asks filling would add more."""
        sides: dict[MarketId, list[Qty]] = {}
        for o in self._working_in.get((mode, scope), {}).values():
            q = sides.setdefault(o.ticker, [0, 0])
            q[0 if o.side == "bid" else 1] += o.qty - o.filled
        total = 0
        for mid, (bids, asks) in sides.items():
            q = sum(p.qty for _, t, p in self._held(scope, mode, pos) if t == mid)
            mark, _ = self._mark(mid)
            base = _capital(q, mark)
            total += max(0, max(_capital(q + bids, mark), _capital(q - asks, mark)) - base)
        return total

    def _entities(self, held: list[_Held], scope: str) -> list[_Entity]:
        assert self.outcomes is not None or self.state is None
        groups: dict[str, _Entity] = {}
        for slot, mid, p in held:
            if self.outcomes is not None:
                entity, outcomes = self.outcomes.of(mid)
            else:
                entity, outcomes = mid, sc.binary(mid)
            e = groups.get(entity)
            if e is None:
                e = groups[entity] = _Entity(entity, outcomes)
            e.held.append((slot, mid, p))
        kind, _, name = scope.partition(":")
        out = []
        for e in groups.values():
            if not any(p.qty for _, _, p in e.held):
                continue
            model = self._model_of(e, None if kind == PORTFOLIO else name, kind)
            e.measure(self, model)
            out.append(e)
        return out

    def _model_of(self, e: _Entity, name: str | None, kind: str) -> str | None:
        """The model whose odds price this entity's EV: the scope's strategy's, or across strategies the one
        holding the most of it."""
        held = [h for h in e.held if h[2].qty]
        if kind == "slot":
            held = [h for h in held if h[0] == name]
        best = max(held, key=lambda h: (abs(h[2].qty), h[0]))
        s = self.slots.get(best[0])
        return getattr(s, "model", None)

    def _p(self, model: str | None) -> Callable[[MarketId], float | None]:
        state = self.state

        def p(t: MarketId) -> float | None:
            if model is None:
                mark, stale = self._mark(t)
                return None if stale and t not in self._last_mid else mark / ONE_DOLLAR
            f = state.forecast(t, model) if state is not None else None
            return None if f is None else f.p
        return p

    def _modes(self) -> list[Mode]:
        modes = {m for m, _, _ in self._pos} | {m for m, _ in self._closed} | {
            getattr(s, "mode", "paper") for s in self.slots.values()}
        return sorted(modes) or ["paper"]  # type: ignore[return-value]

    def _strategies(self, mode: str) -> list[str]:
        out = {slot.split("/", 1)[0] for m, slot, _ in self._pos if m == mode}
        out |= {scope.split(":", 1)[1] for m, scope in self._closed if m == mode and scope.startswith("strategy:")}
        out |= {sid.split("/", 1)[0] for sid, s in self.slots.items() if getattr(s, "mode", "paper") == mode}
        return sorted(out)

    def _scopes(self, mode: str, *, slots: str = "all") -> list[str]:
        """The portfolio, every strategy, and the slots with a position history (`open`: holding one or working
        orders now)."""
        if slots == "open":
            live = {slot for m, slot, _ in self._pos if m == mode and self._pos[(m, slot, _)].qty}
            live |= {o.slot for o in self._working.values() if o.mode == mode}
        else:
            live = {slot for m, slot, _ in self._pos if m == mode}
        return [PORTFOLIO, *(strategy_scope(s) for s in self._strategies(mode)), *(slot_scope(s) for s in sorted(live))]

    def _write_equity(self, now_ms: int, scopes: list[str], mode: Mode) -> None:
        if self.ledger is None:
            return
        rows = []
        for scope in scopes:
            s = self.stats(scope, mode)
            values = (s.equity, s.realized, s.unrealized, s.fees, s.capital_at_risk)
            if self._written.get((scope, mode)) != values:
                self._written[(scope, mode)] = values
                rows.append((now_ms, mode, scope, *values))
        self.ledger.record_equity(rows)


class _Entity:
    """One scenario entity's positions and numbers."""

    __slots__ = ("entity", "outcomes", "held", "scenarios", "worst", "capital", "ev_model", "ev_market")

    def __init__(self, entity: str, outcomes: sc.Outcomes) -> None:
        self.entity, self.outcomes = entity, outcomes
        self.held: list[_Held] = []
        self.scenarios: dict[str, Micros] = {}
        self.worst = self.capital = 0
        self.ev_model: tuple[Micros, float] | None = None
        self.ev_market: tuple[Micros, float] | None = None

    def measure(self, t: PortfolioTracker, model: str | None) -> None:
        self.scenarios = sc.scenarios(((mid, p) for _, mid, p in self.held), self.outcomes)
        self.worst = min(self.scenarios.values())
        self.capital = sum(_capital(p.qty, t._mark(mid, p)[0]) for _, mid, p in self.held if p.qty)
        self.ev_market = sc.ev(self.scenarios, sc.probabilities(self.outcomes, t._p(None)))
        self.ev_model = sc.ev(self.scenarios, sc.probabilities(self.outcomes, t._p(model))) if model else None

    def row(self, mode: str) -> Exposure:
        return Exposure(self.entity, mode, sorted({mid for _, mid, _ in self.held}),  # type: ignore[arg-type]
                        sorted({slot.split("/", 1)[0] for slot, _, p in self.held if p.qty}), self.capital,
                        dict(self.scenarios), self.worst, None if self.ev_model is None else self.ev_model[0],
                        None if self.ev_market is None else self.ev_market[0])
