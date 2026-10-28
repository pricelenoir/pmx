"""Engine events -> viewer messages (dispatch step 6), in the feed process or in-process
(replay, tests): maps events to feed messages, keeps the hub's snapshot state current, and
conflates by engine time. Market data (books, prints, fairs, slot positions) goes out as each
market's `top` + `book` at most every `market_ms` (`traded_ms` for markets with slots or working
orders); slot statuses and the portfolio at most every second. Throttling is latest-wins: the
hub always holds the newest value, and `flush` sends what was held back. Prints, orders, fills,
rejects, halts, and logs are never held back."""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable

from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, StreamReset, Trade
from pmx.engine.events import (
    AccountSnapshot,
    Advice,
    EntityState,
    Forecast,
    Halt,
    LogEvent,
    OrderUpdate,
    PortfolioSnapshot,
    RiskReject,
    SlotFill,
    SlotStatus,
    SourceStatus,
    Timer,
)
from pmx.feed import proto
from pmx.feed.hub import FeedHub


class _Throttle:
    """At most one send per `every_ms` per key (or the `every_ms` given to `ready`). Keys refused
    while too soon wait in `pending` with a deadline; `due` pops only expired deadlines, so it
    costs nothing per event when nothing is due, however many markets are held back."""

    def __init__(self, every_ms: int) -> None:
        self.every_ms = every_ms
        self.sent_at: dict[object, int] = {}
        self.pending: set[object] = set()
        self._deadlines: list[tuple[int, int, object]] = []  # (due ms, seq, key) min-heap
        self._seq = 0

    def ready(self, key: object, now_ms: int, every_ms: int | None = None) -> bool:
        every = self.every_ms if every_ms is None else every_ms
        last = self.sent_at.get(key)
        if last is not None and now_ms - last < every:
            if key not in self.pending:
                self.pending.add(key)
                self._seq += 1
                heapq.heappush(self._deadlines, (last + every, self._seq, key))
            return False
        self.sent_at[key] = now_ms
        self.pending.discard(key)
        return True

    def due(self, now_ms: int) -> list[object]:
        out = []
        deadlines = self._deadlines
        while deadlines and deadlines[0][0] <= now_ms:
            key = heapq.heappop(deadlines)[2]
            if key in self.pending:  # else it was sent meanwhile; the entry is stale
                self.pending.discard(key)
                self.sent_at[key] = now_ms
                out.append(key)
        return out


class Publisher:
    def __init__(
        self,
        hub: FeedHub,
        *,
        tickers_of: Callable[[str], Iterable[str]] = lambda entity: (),
        market_ms: int = 250,
        traded_ms: int = 100,
        slot_ms: int = 1_000,
        portfolio_ms: int = 1_000,
    ) -> None:
        self.hub = hub
        self.tickers_of = tickers_of
        self.market_ms, self.traded_ms = market_ms, traded_ms
        self._markets = _Throttle(market_ms)
        self._slots = _Throttle(slot_ms)
        self._portfolio = _Throttle(portfolio_ms)

    def set_models(self, names: list[str]) -> None:
        self.hub.set_models(names)

    def publish_perf(self, msg: proto.PerfMsg) -> None:
        self.hub.set_perf(msg)

    def set_status(self, connected: bool, detail: str, source: str = "", ts_ms: int | None = None) -> None:
        self.hub.set_status(connected, detail, source, ts_ms)

    def seed_equity(self, scope: str, mode: str, points: list[tuple[int, int, int]]) -> None:
        self.hub.seed_equity(scope, mode, points)

    def apply(self, msg: proto.Msg, now_ms: int) -> None:
        """A feed message the engine side sent as is (what isn't an engine event: models, perf,
        equity history, statuses, entities already rendered for viewers)."""
        hub = self.hub
        match msg:
            case proto.EntityMsg():
                hub.set_entity(msg)
            case proto.Models(names=names):
                hub.set_models(names)
            case proto.PerfMsg():
                hub.set_perf(msg)
            case proto.EquityMsg(scope=scope, mode=mode, points=points, replace=True):
                hub.seed_equity(scope, mode, points)
            case proto.Status(connected=connected, detail=detail, source=source, ts_ms=ts):
                hub.set_status(connected, detail, source, ts)

    def on_event(self, ev: object, now_ms: int) -> None:
        self.flush(now_ms)
        hub = self.hub
        match ev:
            case BookSnapshot() | BookDelta() | Trade():
                if (ticker := hub.on_event(ev)) is not None:
                    self._market(ticker, now_ms)
            case StreamReset() | Contract():
                hub.on_event(ev)
            case Fill() | Timer():
                pass  # venue fills reach viewers attributed, as SlotFill
            case SlotFill(slot=slot, mode=mode, fill=f):
                hub.add_fill(proto.FillMsg(f.ticker, f.side, f.price, f.qty, f.fee, f.is_taker, f.ts_ms, slot, mode))
            case Forecast():
                if ev.model not in hub.models:
                    hub.set_models([*hub.models, ev.model])
                self._market(hub.set_forecast(proto.ForecastMsg.of(ev)), now_ms)
            case SlotStatus():
                if (ticker := hub.set_slot(proto.SlotMsg.of(ev), send=self._slots.ready(ev.slot, now_ms))) is not None:
                    self._market(ticker, now_ms)
            case Advice():
                hub.set_advice(proto.AdviceMsg.of(ev))
            case OrderUpdate():
                hub.set_order(proto.OrderMsg.of(ev))
            case RiskReject():
                hub.add_risk(proto.RiskMsg.of(ev))
            case LogEvent():
                hub.add_log(proto.LogMsg.of(ev))
            case Halt():
                hub.set_halt(proto.HaltMsg.of(ev))
            case SourceStatus():
                hub.set_status(ev.connected, ev.detail, ev.source, ev.ts_ms)
            case AccountSnapshot():
                hub.set_account(ev.balance, ev.positions)
            case PortfolioSnapshot():
                send = self._portfolio.ready("portfolio", now_ms)
                hub.set_portfolio(proto.PortfolioMsg.of(ev), send=send)
                if send:
                    self._equity_points()
            case proto.EntityMsg() | proto.Models() | proto.PerfMsg() | proto.EquityMsg() | proto.Status():
                self.apply(ev, now_ms)
            case EntityState():
                hub.set_entity(proto.EntityMsg.of(ev, sorted(self.tickers_of(ev.entity))))

    def _market(self, ticker: str, now_ms: int) -> None:
        if ticker not in self.hub.contracts:  # e.g. a group slot's key
            return
        every = self.traded_ms if self.hub.traded(ticker) else self.market_ms
        if self._markets.ready(ticker, now_ms, every):
            self.hub.send_market(ticker, now_ms)

    def flush(self, now_ms: int) -> None:
        hub = self.hub
        for ticker in self._markets.due(now_ms):
            hub.send_market(ticker, now_ms)
        for key in self._slots.due(now_ms):
            hub.set_slot(hub.slots[key])
        if self._portfolio.due(now_ms) and hub.portfolio is not None:
            hub.set_portfolio(hub.portfolio)
            self._equity_points()

    def _equity_points(self) -> None:
        p = self.hub.portfolio
        assert p is not None
        for s in p.scopes:
            if not s.scope.startswith("slot:"):  # slot curves live in the ledger, not the feed
                self.hub.add_equity(s.scope, s.mode, (p.ts_ms, s.equity, s.drawdown))
