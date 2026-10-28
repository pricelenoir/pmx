"""Scenario tests for strategies (ARCHITECTURE *Strategy test kit*, GUIDE): script what a market does,
assert what the strategy wants to do. One slot with the real router and `MarketContext`, a minimal
`State`, engine time you move by hand, and a recording order desk — no engine, no network, no fills
you didn't ask for. Prices and quantities are human strings (dollars, contracts); probabilities floats.
Cadence windows close on the next event at or after they end, as in the engine; `advance` sends a
`Timer` every whole second.

    sc = Scenario("edge_taker", params={"min_edge": 0.04, "size": "5"})
    sc.book(bids=[("0.55", "100")], asks=[("0.57", "100")])
    sc.forecast(0.63)
    assert sc.intents == [Take("bid", "0.57", "5")]
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import msgspec

from pmx import plugins
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, OrderRequest, StreamReset, Trade
from pmx.core.units import Price, Qty, fmt_price, fmt_qty, parse_micros, parse_price, parse_qty
from pmx.engine.events import Advice, Event, Forecast, LogEvent, RiskReject, Schedule, SlotFill, Timer
from pmx.engine.slots import MarketSelector, Router, Slot, StrategySpec
from pmx.engine.state import MarketId, State, venue_ticker
from pmx.execution.risk import RiskManager
from pmx.strategies import base
from pmx.strategies.base import Strategy, WorkingOrder

_kit_kinds = itertools.count()


def _price(v: str | int) -> Price:
    return parse_price(v) if isinstance(v, str) else v


def _qty(v: str | int) -> Qty:
    return parse_qty(v) if isinstance(v, str) else v


def _level(v: tuple[str | int, str | int] | None) -> tuple[Price, Qty] | None:
    return None if v is None else (_price(v[0]), _qty(v[1]))


def _show(p: Price) -> str:
    """0.57, 0.505 — at least cents."""
    s = fmt_price(p)
    return s[:-2] + s[-2:].rstrip("0")


def _show_qty(q: Qty) -> str:
    """5, 1.5"""
    return fmt_qty(q).rstrip("0").rstrip(".")


# ---- what a slot asked for, comparable in human units ------------------------------------------------------


@dataclass(frozen=True, init=False)
class Take:
    side: str
    limit: Price
    qty: Qty
    ticker: str | None = None  # set by GroupScenario

    def __init__(self, side: str, limit: str | int, qty: str | int, ticker: str | None = None) -> None:
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "limit", _price(limit))
        object.__setattr__(self, "qty", _qty(qty))
        object.__setattr__(self, "ticker", ticker)

    def __repr__(self) -> str:
        at = f", ticker={self.ticker!r}" if self.ticker else ""
        return f"Take({self.side!r}, {_show(self.limit)!r}, {_show_qty(self.qty)!r}{at})"


@dataclass(frozen=True, init=False)
class Quote:
    bid: tuple[Price, Qty] | None
    ask: tuple[Price, Qty] | None
    ticker: str | None = None

    def __init__(self, bid: tuple[str | int, str | int] | None = None, ask: tuple[str | int, str | int] | None = None,
                 ticker: str | None = None) -> None:
        object.__setattr__(self, "bid", _level(bid))
        object.__setattr__(self, "ask", _level(ask))
        object.__setattr__(self, "ticker", ticker)

    def __repr__(self) -> str:
        def lv(x: tuple[Price, Qty] | None) -> str:
            return "None" if x is None else f"({_show(x[0])!r}, {_show_qty(x[1])!r})"
        at = f", ticker={self.ticker!r}" if self.ticker else ""
        return f"Quote(bid={lv(self.bid)}, ask={lv(self.ask)}{at})"


@dataclass(frozen=True)
class CancelAll:
    ticker: str | None = None


@dataclass(frozen=True, init=False)
class Target:
    qty: Qty
    style: str = "passive"
    ticker: str | None = None

    def __init__(self, qty: str | int, style: str = "passive", ticker: str | None = None) -> None:
        object.__setattr__(self, "qty", _qty(qty))
        object.__setattr__(self, "style", style)
        object.__setattr__(self, "ticker", ticker)

    def __repr__(self) -> str:
        at = f", ticker={self.ticker!r}" if self.ticker else ""
        return f"Target({_show_qty(self.qty)!r}, {self.style!r}{at})"


# ---- the recording order desk -------------------------------------------------------------------------------


class _Desk:
    """`OrderView` for the slot: quotes rest at once, takes vanish, fills happen only via `Scenario.fill`."""

    def __init__(self) -> None:
        self.positions: dict[MarketId, Qty] = {}
        self.quotes: dict[MarketId, dict[str, WorkingOrder]] = {}
        self.throttle = False
        self._ids = itertools.count(1)

    def apply(self, intent: base.Intent, now_ms: int) -> None:
        if type(intent) is base.Quote:
            book = self.quotes.setdefault(intent.ticker, {})
            for side, level in (("bid", intent.bid), ("ask", intent.ask)):
                if level is None:
                    book.pop(side, None)
                elif (o := book.get(side)) is None or (o.price, o.qty) != level:
                    coid = f"kit-{next(self._ids)}"
                    book[side] = WorkingOrder(coid, side, level[0], level[1], 0, "open", None, now_ms)
        elif type(intent) is base.CancelAll:
            for t in [intent.ticker] if intent.ticker is not None else list(self.quotes):
                self.quotes.pop(t, None)

    def fill(self, ticker: MarketId, side: str, price: Price, qty: Qty) -> str:
        """Book a fill; a resting quote on that side at that price is (partly) filled. Returns its id."""
        self.positions[ticker] = self.positions.get(ticker, 0) + (qty if side == "bid" else -qty)
        book = self.quotes.get(ticker, {})
        o = book.get(side)
        if o is None or o.price != price:
            return "kit"
        if o.filled + qty >= o.qty:
            del book[side]
        else:
            book[side] = msgspec.structs.replace(o, filled=o.filled + qty)
        return o.client_order_id

    def position(self, scope: str, ticker: MarketId, mode: Any = None) -> Qty:
        return self.positions.get(ticker, 0)

    def working(self, scope: str, ticker: MarketId | None = None, mode: Any = None) -> list[WorkingOrder]:
        books = [self.quotes.get(ticker, {})] if ticker is not None else list(self.quotes.values())
        return [o for b in books for o in b.values()]

    def throttled(self, slot: str) -> bool:
        return self.throttle

    def pnl(self, scope: str, mode: Any, state: State) -> int:
        return 0


# ---- scenarios --------------------------------------------------------------------------------------------------


class Scenario:
    """One slot of `strategy` (a registered kind or a `Strategy` class) on `ticker`."""

    def __init__(self, strategy: str | type, *, params: Mapping[str, Any] | None = None, ticker: str = "TEST-1",
                 model: str = "m", mode: str = "paper", contract: Contract | None = None, start_ms: int = 0,
                 inputs: Iterable[str] = (), advisors: Iterable[str] = ("advisor",), cadence_ms: int | None = None,
                 warmup_s: float | None = None, risk: Mapping[str, Any] | None = None,
                 _tickers: Iterable[str] | None = None, _slot_by: str = "market") -> None:
        kind = self._kind(strategy)
        tickers = list(_tickers) if _tickers is not None else [ticker]
        self.ticker = tickers[0]
        self.model = model
        self.now_ms = start_ms
        self.state = State()
        self.desk = _Desk()
        self.spec = StrategySpec(id="test", kind=kind, params=dict(params or {}), mode=mode, model=model,  # type: ignore[arg-type]
                                 markets=MarketSelector(tickers=tickers, status=None), slot_by=_slot_by,  # type: ignore[arg-type]
                                 inputs=list(inputs), advisors=list(advisors), cadence_ms=cadence_ms,
                                 warmup_s=warmup_s, risk=dict(risk) if risk is not None else None)
        self.router = Router([self.spec], orders=self.desk, portfolio=None, linkers={})
        self.risk: RiskManager | None = None
        self._group = _slot_by != "market"
        self._rejects: list[RiskReject] = []
        self._logs: list[LogEvent] = []
        self._intents: list[Any] = []
        self._fills = itertools.count(1)
        self._linked: set[str] = set()
        for t in tickers:
            venue, bare = venue_ticker(t)
            c = contract if contract is not None and len(tickers) == 1 else \
                Contract(venue, bare, t.rsplit("-", 1)[0] if "-" in t else t, "TEST", "", "open", None)
            self._send(c)
        if risk is not None:
            self.with_risk()

    @staticmethod
    def _kind(strategy: str | type) -> str:
        if isinstance(strategy, str):
            if strategy not in plugins.STRATEGIES:
                plugins.load_builtin_plugins()
            return strategy
        if not (isinstance(strategy, type) and issubclass(strategy, Strategy)):
            raise TypeError(f"expected a strategy kind or a Strategy subclass, got {strategy!r}")
        for kind, cls in plugins.STRATEGIES.items():
            if cls is strategy:
                return kind
        kind = f"_scenario_{next(_kit_kinds)}"
        plugins.STRATEGIES[kind] = strategy
        return kind

    @property
    def slot(self) -> Slot:
        return next(iter(self.router.slots.values()))

    def with_risk(self, limits: Mapping[str, Any] | None = None) -> Scenario:
        """Check every order the slot wants against risk — the slot's own `risk` limits plus `limits` as global
        ones. Rejected orders still show in `intents` (what the strategy wanted) but don't rest; their
        `RiskReject`s are in `rejects`."""
        self.risk = RiskManager(positions=self.desk, slots=self.router.slots,
                                global_limits=limits if limits is not None else None)
        return self

    # ---- drive the market ------------------------------------------------------------------------------------

    def book(self, bids: Iterable[tuple[str, str]] = (), asks: Iterable[tuple[str, str]] = (), *,
             ticker: str | None = None) -> Scenario:
        t = ticker or self.ticker
        venue, bare = venue_ticker(t)
        side = lambda levels: tuple((_price(p), _qty(q)) for p, q in levels)  # noqa: E731
        return self._send(BookSnapshot(venue, bare, side(bids), tuple(sorted(side(asks))), self.now_ms))

    def delta(self, side: str, price: str, qty_change: str, *, ticker: str | None = None) -> Scenario:
        venue, bare = venue_ticker(ticker or self.ticker)
        return self._send(BookDelta(venue, bare, side, _price(price), _qty(qty_change), self.now_ms))

    def trade(self, price: str, qty: str, taker_side: str = "bid", *, ticker: str | None = None) -> Scenario:
        venue, bare = venue_ticker(ticker or self.ticker)
        return self._send(Trade(venue, f"kit-t{next(self._fills)}", bare, _price(price), _qty(qty), taker_side,
                                self.now_ms))

    def forecast(self, p: float, *, model: str | None = None, age_s: float = 0, ticker: str | None = None) -> Scenario:
        ts = self.now_ms - round(age_s * 1000)
        return self._send(Forecast(model or self.model, ticker or self.ticker, p, ts, ts))

    def data(self, obj: Any, *, ticker: str | None = None) -> Scenario:
        """An entity's state (e.g. a domain `GameState`), linked to the scenario's market(s)."""
        if obj.entity not in self._linked:
            self._linked.add(obj.entity)
            for t in [ticker] if ticker else list(self.state.contracts):
                self.state.entity_of[t] = obj.entity
                self.state.tickers_of.setdefault(obj.entity, set()).add(t)
        return self._send(obj)

    def input(self, name: str, ev: Event) -> Scenario:
        """An event from the declared input source `name`."""
        return self._send(ev, source=name)

    def advice(self, *, p: float | None = None, direction: str | None = None, confidence: float = 1.0,
               expires_s: float = 60, advisor: str | None = None, rationale: str = "",
               target: str | None = None) -> Scenario:
        name = advisor or (self.spec.advisors[0] if self.spec.advisors else "advisor")
        return self._send(Advice(name, target or self.ticker, p, direction, confidence,  # type: ignore[arg-type]
                                 self.now_ms + round(expires_s * 1000), rationale, self.now_ms))

    def fill(self, side: str, price: str, qty: str, *, fee: str = "0", ticker: str | None = None) -> Scenario:
        """One of the slot's orders filled (only when the test says so); a resting quote at that price
        fills first. `fee` in dollars."""
        t = ticker or self.ticker
        venue, bare = venue_ticker(t)
        p, q = _price(price), _qty(qty)
        order_id = self.desk.fill(t, side, p, q)
        f = Fill(venue, f"kit-f{next(self._fills)}", order_id, bare, side, p, q, parse_micros(fee), True,
                 self.now_ms)
        return self._send(SlotFill(self.slot.id, self.slot.mode, f))

    def contract(self, *, ticker: str | None = None, **changes: Any) -> Scenario:
        """The venue updated the contract: `contract(status="closed")`, `contract(settlement="1")`, … — a
        closing market cancels the slot's orders and stops it."""
        t = ticker or self.ticker
        if "settlement" in changes and isinstance(changes["settlement"], str):
            changes["settlement"] = parse_price(changes["settlement"])
        return self._send(msgspec.structs.replace(self.state.contracts[t], **changes))

    def schedule(self, kind: str, *, in_s: float, detail: str = "", target: str | None = None) -> Scenario:
        """A known future moment (a release, a kickoff) `in_s` seconds from now."""
        at = self.now_ms + round(in_s * 1000)
        return self._send(Schedule(target or self.ticker, kind, at, detail or kind, self.now_ms))

    def reset(self, venue: str = "kalshi") -> Scenario:
        return self._send(StreamReset(venue, "test", self.now_ms))

    def advance(self, seconds: float) -> Scenario:
        """Move engine time on, with a `Timer` each whole second (cadence windows close on the way)."""
        end = self.now_ms + round(seconds * 1000)
        nxt = (self.now_ms // 1000 + 1) * 1000
        while nxt <= end:
            self.now_ms = nxt
            self._send(Timer(nxt))
            nxt += 1000
        self.now_ms = end
        return self

    def throttle(self, on: bool = True) -> Scenario:
        """The order scheduler is holding this slot's writes (`m.throttled`)."""
        self.desk.throttle = on
        return self

    # ---- assert ---------------------------------------------------------------------------------------------------

    @property
    def intents(self) -> list[Any]:
        """What the slot asked for since the last read (reading clears it)."""
        out, self._intents = self._intents, []
        return out

    @property
    def rejects(self) -> list[RiskReject]:
        """Risk rejections since the last read (reading clears it); needs `with_risk` or `risk=`."""
        out, self._rejects = self._rejects, []
        return out

    @property
    def position(self) -> Qty:
        return self.desk.positions.get(self.ticker, 0)

    @property
    def working(self) -> list[WorkingOrder]:
        return self.desk.working("", self.ticker)

    @property
    def logs(self) -> list[str]:
        return [e.text for e in self._logs]

    @property
    def ready(self) -> bool:
        return self.slot.ready

    @property
    def m(self) -> Any:
        """The slot's context, as the strategy sees it."""
        return self.slot.ctx

    # ---- plumbing ---------------------------------------------------------------------------------------------------

    def _send(self, ev: Any, source: str = "") -> Scenario:
        pending = [(ev, source)]
        while pending:
            e, src = pending.pop(0)
            derived = self.state.apply(e, self.now_ms, src)
            derived += self.router.on_event(e, self.state, self.now_ms)
            for d in derived:
                if type(d) is LogEvent:
                    self._logs.append(d)
            pending += [(d, "") for d in derived if type(d) is not LogEvent]
            pending += [(r, "") for r in self._collect()]  # rejects reach the slot, as in the engine
        return self

    def _collect(self) -> list[RiskReject]:
        group = self._group
        out: list[RiskReject] = []
        for i in self.router.take_intents():
            if type(i) in (base.Quote, base.Take) and self.risk is not None:
                rejected = self._check(i)
                if rejected:
                    out += rejected
                    self._intents.append(self._human(i, group))
                    continue
            self.desk.apply(i, self.now_ms)
            self._intents.append(self._human(i, group))
        return out

    def _check(self, i: base.Quote | base.Take) -> list[RiskReject]:
        assert self.risk is not None
        venue, bare = venue_ticker(i.ticker)
        reqs = [(i.side, i.limit, i.qty)] if type(i) is base.Take else \
            [(s, lv[0], lv[1]) for s, lv in (("bid", i.bid), ("ask", i.ask)) if lv is not None]
        bad = []
        for side, price, qty in reqs:
            r = self.risk.check(i.slot, OrderRequest(venue, bare, side, price, qty, strategy=i.slot), self.state,
                                self.now_ms)
            if r is not None:
                self._rejects.append(r)
                bad.append(r)
        return bad

    @staticmethod
    def _human(i: base.Intent, group: bool) -> Any:
        t = i.ticker if group else None
        match i:
            case base.Take():
                return Take(i.side, i.limit, i.qty, t)
            case base.Quote():
                return Quote(i.bid, i.ask, t)
            case base.CancelAll():
                return CancelAll(i.ticker if group else None)
            case base.Target():
                return Target(i.qty, i.style, t)
        raise TypeError(i)


class GroupScenario(Scenario):
    """One group slot (`slot_by = "event"` by default) over `tickers`; intents carry their ticker."""

    def __init__(self, strategy: str | type, *, tickers: Iterable[str], slot_by: str = "event", **kw: Any) -> None:
        tickers = list(tickers)
        super().__init__(strategy, ticker=tickers[0], _tickers=tickers, _slot_by=slot_by, **kw)

