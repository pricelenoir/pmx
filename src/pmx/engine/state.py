"""What the engine knows right now (dispatch step 1). Read-only to models and strategies; only the
engine calls `apply`.

Markets are keyed by `MarketId`: the ticker on Kalshi, `<venue>:<ticker>` on any other venue, so
ids stay unique once a second venue exists. Nothing else may assume tickers are unique."""

from __future__ import annotations

import bisect
import logging
from collections.abc import Mapping

from pmx.core.book import BookTracker, OrderBook
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, StreamReset, Trade
from pmx.engine.events import (
    Advice,
    EntityState,
    Event,
    Forecast,
    LogEvent,
    OrderUpdate,
    Refused,
    RiskReject,
    Schedule,
    SlotFill,
    SlotStatus,
    Timer,
    advice_problem,
)
from pmx.plugins import Linker

log = logging.getLogger(__name__)

type MarketId = str


def market_id(venue: str, ticker: str) -> MarketId:
    return ticker if venue == "kalshi" else f"{venue}:{ticker}"


def venue_ticker(mid: MarketId) -> tuple[str, str]:
    venue, sep, ticker = mid.partition(":")
    return (venue, ticker) if sep else ("kalshi", mid)


class State:
    def __init__(self, linkers: Mapping[str, Linker] | None = None, *, keep_past_schedules_ms: int = 3_600_000) -> None:
        self.linkers = dict(linkers or {})  # tried in order; the first entity wins
        self.keep_past_schedules_ms = keep_past_schedules_ms  # risk windows look a little past a release
        self.contracts: dict[MarketId, Contract] = {}
        self.books = BookTracker()
        self.entity_of: dict[MarketId, str] = {}
        self.tickers_of: dict[str, set[MarketId]] = {}
        self.entities: dict[str, EntityState] = {}
        self.forecasts: dict[tuple[str, MarketId], Forecast] = {}  # (model, ticker)
        self.advice: dict[tuple[str, str], Advice] = {}  # (advisor, target), unexpired
        self.schedules: dict[str, list[Schedule]] = {}  # target -> upcoming (and just passed), soonest first
        self.last_trade: dict[MarketId, Trade] = {}
        self.inputs: dict[str, tuple[int, Event]] = {}  # source name -> (receive ms, latest event)
        self.source = ""  # source of the event being dispatched; "" = made inside the engine
        self.now_ms = 0
        self._advice_due: int | None = None  # earliest expiry, so Timers prune only when something lapsed
        self._schedules_due: int | None = None
        self._entity_types: dict[type, bool] = {}
        self._resolves: dict[MarketId, int] = {}  # resolution time last announced per market
        self._apply = {
            BookSnapshot: self._book, BookDelta: self._book, StreamReset: self._book, Contract: self._contract,
            Trade: self._trade, Forecast: self._forecast, Advice: self._advice, Schedule: self._schedule,
            Timer: self._timer,
        }

    # ---- read -------------------------------------------------------------

    def book(self, ticker: MarketId) -> OrderBook | None:
        """None until snapshotted, and again after a reset or a bad delta (invariant 8)."""
        return self.books.get(*venue_ticker(ticker))

    def forecast(self, ticker: MarketId, model: str) -> Forecast | None:
        return self.forecasts.get((model, ticker))

    def data(self, ticker: MarketId) -> EntityState | None:
        """The linked entity's latest state."""
        entity = self.entity_of.get(ticker)
        return None if entity is None else self.entities.get(entity)

    def is_entity(self, ev: object) -> bool:
        t = type(ev)
        known = self._entity_types.get(t)
        if known is None:
            known = self._entity_types[t] = t not in self._apply and isinstance(ev, EntityState)
        return known

    def keys_of(self, ev: Event) -> tuple[MarketId | None, str | None]:
        """(market, entity) an event is about; either may be None. An `Advice`/`Schedule` target
        is a market if it's a known contract, else an entity."""
        match ev:
            case BookSnapshot() | BookDelta() | Trade() | Fill() | Contract():
                mid = market_id(ev.venue, ev.ticker)
            case Forecast() | OrderUpdate() | RiskReject() | SlotStatus():
                mid = ev.ticker
            case SlotFill():
                mid = market_id(ev.fill.venue, ev.fill.ticker)
            case Advice() | Schedule():
                if ev.target not in self.contracts:
                    return None, ev.target
                mid = ev.target
            case _:
                return (None, ev.entity) if self.is_entity(ev) else (None, None)
        return mid, self.entity_of.get(mid)

    def venues(self) -> set[str]:
        return {v for v, _ in self.books.books} | {c.venue for c in self.contracts.values()}

    # ---- write (the engine only) ---------------------------------------------

    def apply(self, ev: Event, now_ms: int, source: str = "") -> list[Event]:
        """Reflect one dispatched event; returns events to dispatch next (logs from linkers)."""
        self.now_ms = now_ms
        self.source = source
        if source:
            self.inputs[source] = (now_ms, ev)
        fn = self._apply.get(type(ev))
        if fn is not None:
            return fn(ev)
        if self.is_entity(ev):
            prev = self.entities.get(ev.entity)
            if prev is None or ev.ts_ms >= prev.ts_ms:  # a late, older state never replaces a newer one
                self.entities[ev.entity] = ev
        return []

    def _book(self, ev: BookSnapshot | BookDelta | StreamReset) -> list[Event]:
        self.books.on_event(ev)
        return []

    def _trade(self, ev: Trade) -> list[Event]:
        self.last_trade[market_id(ev.venue, ev.ticker)] = ev
        return []

    def _forecast(self, ev: Forecast) -> list[Event]:
        self.forecasts[(ev.model, ev.ticker)] = ev
        return []

    def _contract(self, c: Contract) -> list[Event]:
        """Store it, link it (re-dispatching the entity's latest state if it arrived first, so models and
        viewers see the link), and announce its resolution time as a `Schedule` when that's new."""
        mid = market_id(c.venue, c.ticker)
        self.contracts[mid] = c
        out: list[Event] = []
        at = c.event_ts_ms or c.close_ts_ms
        if at is not None and self._resolves.get(mid) != at:
            self._resolves[mid] = at
            out.append(Schedule(mid, "resolution", at, f"{c.title or c.ticker}: expected resolution", self.now_ms))
        entity = None
        for name, fn in self.linkers.items():
            try:
                entity = fn(c)
            except Exception as e:  # a broken linker leaves the contract unlinked, never the engine down
                log.exception("linker %s failed on %s", name, mid)
                out.append(LogEvent("error", f"linker:{name}", f"{mid}: {type(e).__name__}: {e}", self.now_ms))
                continue
            if entity is not None:
                break
        prev = self.entity_of.get(mid)
        if entity == prev:
            return out
        if prev is not None:
            tickers = self.tickers_of[prev]
            tickers.discard(mid)
            if not tickers:
                del self.tickers_of[prev]
        if entity is None:
            del self.entity_of[mid]
        else:
            self.entity_of[mid] = entity
            self.tickers_of.setdefault(entity, set()).add(mid)
            known = self.entities.get(entity)
            if known is not None:  # data that came before its market: dispatch it again, now linked
                out.append(known)
        return out

    def _advice(self, a: Advice) -> list[Event]:
        known = a.target in self.contracts or a.target in self.tickers_of or a.target in self.entities
        why = advice_problem(a, known=known)
        if why is not None:
            raise Refused(f"advisor:{a.advisor}", f"advice for {a.target} dropped: {why}")
        key = (a.advisor, a.target)
        if a.expires_ms < self.now_ms:  # already lapsed: the advisor's latest word is "nothing"
            self.advice.pop(key, None)
            return []
        self.advice[key] = a
        if self._advice_due is None or a.expires_ms < self._advice_due:
            self._advice_due = a.expires_ms
        return []

    def _schedule(self, s: Schedule) -> list[Event]:
        upcoming = self.schedules.setdefault(s.target, [])
        for i, old in enumerate(upcoming):  # the same moment announced again replaces the old one
            if old.kind == s.kind and old.at_ms == s.at_ms:
                upcoming[i] = s
                return []
        bisect.insort(upcoming, s, key=lambda x: x.at_ms)
        drop_at = s.at_ms + self.keep_past_schedules_ms
        if self._schedules_due is None or drop_at < self._schedules_due:
            self._schedules_due = drop_at
        return []

    def _timer(self, ev: Timer) -> list[Event]:
        now = self.now_ms
        if self._advice_due is not None and self._advice_due < now:  # actionable through `expires_ms`
            self.advice = {k: a for k, a in self.advice.items() if a.expires_ms >= now}
            self._advice_due = min((a.expires_ms for a in self.advice.values()), default=None)
        if self._schedules_due is not None and self._schedules_due < now:
            cutoff = now - self.keep_past_schedules_ms
            for target in list(self.schedules):
                kept = [s for s in self.schedules[target] if s.at_ms >= cutoff]
                if kept:
                    self.schedules[target] = kept
                else:
                    del self.schedules[target]
            self._schedules_due = min((ss[0].at_ms + self.keep_past_schedules_ms for ss in self.schedules.values()),
                                      default=None)
        return []
