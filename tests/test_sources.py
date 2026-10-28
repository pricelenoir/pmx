from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import msgspec
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from conftest import delta, frame, snapshot
from pmx.connectors.base import Control, Frame
from pmx.connectors.kalshi.auth import KalshiSigner
from pmx.connectors.kalshi.config import KalshiConfig
from pmx.connectors.kalshi.rest import KalshiRest
from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.core.fees import FeeSchedule
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, StreamReset, Trade, VenueOrder
from pmx.engine.clock import SimClock
from pmx.engine.engine import Engine
from pmx.engine.events import AccountSnapshot, SourceStatus, Timer
from pmx.engine.state import State
from pmx.recorder.sink import looks_secret
from pmx.sources.espn_nfl import EspnNfl
from pmx.sources.kalshi import RAW, RAW_REST, KalshiAccount, KalshiMarketData, offset_bounds
from pmx.sources.timer import TimerSource

FIX = Path(__file__).parent / "fixtures"
NFL = "KXNFLGAME-26OCT18HOUJAC-HOU"
NFL2 = "KXNFLGAME-26OCT18HOUJAC-JAC"


class FakeSink:
    """Every raw record is checked for credentials: the shared redaction test."""

    def __init__(self) -> None:
        self.events: list = []
        self.records: list[tuple[str, str, str]] = []
        self.on_overflow: list = []

    def emit(self, ev) -> None:
        self.events.append(ev)

    def record(self, source: str, kind: str, payload: str) -> None:
        assert looks_secret(payload) is None, f"{source} recorded something that looks like a credential"
        self.records.append((source, kind, payload))

    def of(self, t: type) -> list:
        return [e for e in self.events if isinstance(e, t)]


def market(ticker: str, status: str = "active") -> dict:
    return {"ticker": ticker, "event_ticker": "KXNFLGAME-26OCT18HOUJAC", "status": status, "title": "HOU at JAX",
            "rules_primary": "If Houston wins, Yes.", "rules_secondary": "A tie resolves to $0.50."}


def book(bid: str = "0.4000") -> dict:
    return {"orderbook_fp": {"yes_dollars": [[bid, "10.00"]], "no_dollars": [["0.5500", "5.00"]]}}


def rest_venue(handler, signer=None) -> KalshiVenue:
    fast = (1e9, 1e9)
    rest = KalshiRest("https://x/trade-api/v2", signer, transport=httpx.MockTransport(handler), read=fast)
    return KalshiVenue(KalshiConfig("demo"), rest)


# ---- timer ------------------------------------------------------------------------------------------


def test_timer_ticks_at_its_interval_and_stops():
    sink, stop = FakeSink(), asyncio.Event()

    async def run() -> None:
        task = asyncio.create_task(TimerSource("timer", TimerSource.Params(interval_ms=10)).run(sink, stop))
        await asyncio.sleep(0.065)
        stop.set()
        await asyncio.wait_for(task, 1)

    asyncio.run(run())
    ticks = sink.of(Timer)
    assert [(st.source, st.connected) for st in sink.of(SourceStatus)] == [("timer", True)], "up at once"
    assert 5 <= len(ticks) <= 8 and all(b.ts_ms >= a.ts_ms for a, b in zip(ticks, ticks[1:], strict=False))
    with pytest.raises(ValueError):
        TimerSource("t", TimerSource.Params(interval_ms=0))


# ---- the engine's per-source queues ------------------------------------------------------------------


class Seen:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.events.append(ev)


def queued(policy: str, events: list, *, max: int = 3, venue: str = "") -> tuple[list, object]:
    seen = Seen()
    engine = Engine(SimClock(0), State(), publisher=seen)
    sink = engine.source("feed", max=max, policy=policy, venue=venue)
    hooks: list[str] = []
    sink.on_overflow.append(lambda: hooks.append("resync"))
    for ev in events:
        sink.emit(ev)
    assert sink.pending <= max
    asyncio.run(engine.dispatch_all())
    assert sink.pending == 0
    return seen.events, (sink, hooks)


def trade(ticker: str, n: int) -> Trade:
    return Trade("kalshi", f"t{n}", ticker, 4_000 + n, 100, "bid", n)


def test_conflate_keeps_the_latest_per_key_and_drops_the_oldest_key_past_max():
    events, (sink, _) = queued("conflate", [trade("A", 1), trade("B", 2), trade("A", 3), trade("C", 4)])
    assert [e.trade_id for e in events if isinstance(e, Trade)] == ["t2", "t3", "t4"] and sink.overflows == 0
    events, (sink, _) = queued("conflate", [trade(t, i) for i, t in enumerate("ABCDE")])
    assert [e.ticker for e in events if isinstance(e, Trade)] == ["C", "D", "E"] and sink.overflows == 2
    status = [e for e in events if isinstance(e, SourceStatus)]
    assert len(status) == 1 and "queue overflow: 1 dropped (conflate, max 3)" in status[0].detail  # once a second


def test_drop_oldest_keeps_the_newest_max():
    events, (sink, _) = queued("drop_oldest", [trade("A", i) for i in range(6)])
    assert [e.trade_id for e in events if isinstance(e, Trade)] == ["t3", "t4", "t5"] and sink.overflows == 3


def test_reset_drops_everything_queued_and_resets_the_venue_books():
    deltas = [BookDelta("kalshi", "A", "bid", 4_000, 100, i) for i in range(5)]
    events, (sink, hooks) = queued("reset", deltas, venue="kalshi")
    kinds = [type(e).__name__ for e in events]
    assert kinds == ["StreamReset", "SourceStatus", "BookDelta", "BookDelta"]  # the 4th overflowed: 1-3 dropped
    assert events[0] == StreamReset("kalshi", "queue overflow", 0) and hooks == ["resync"]


def test_source_sinks_record_through_the_engine_recorder_and_names_are_unique():
    recorded: list = []

    class Rec:
        def record(self, source, kind, payload):
            recorded.append((source, kind, payload))

    engine = Engine(SimClock(0), State(), recorder=Rec())
    engine.source("feed").record("feed", "f", "{}")
    assert recorded == [("feed", "f", "{}")]
    with pytest.raises(ValueError, match="already has a sink"):
        engine.source("feed")


# ---- Kalshi over REST (no key) -----------------------------------------------------------------------


def test_kalshi_poll_mode_emits_contracts_books_and_tape_and_records_raw():
    stop = asyncio.Event()
    calls: list[str] = []
    state = {"books": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path.removeprefix("/trade-api/v2")
        calls.append(path)
        if path == "/markets":
            return httpx.Response(200, json={"markets": [market(NFL), market(NFL2)], "cursor": ""})
        if path.startswith("/series/"):
            return httpx.Response(200, json={"series": {"ticker": "KXNFLGAME", "fee_type": "quadratic_with_maker_fees",
                                                        "fee_multiplier": 1, "settlement_sources": [
                                                            {"name": "the Governing League", "url": "https://x"}]}})
        if path == "/markets/trades":
            return httpx.Response(200, json={"trades": [
                {"trade_id": "t2", "ticker": NFL, "yes_price_dollars": "0.4100", "count_fp": "3.00",
                 "taker_side": "yes", "created_time": "2026-10-18T13:00:02Z"},
                {"trade_id": "t1", "ticker": "OTHER-1", "yes_price_dollars": "0.5000", "count_fp": "1.00",
                 "taker_side": "no", "created_time": "2026-10-18T13:00:01Z"}], "cursor": ""})
        state["books"] += 1
        if state["books"] >= 6:
            stop.set()
        return httpx.Response(200, json=book("0.4100" if state["books"] >= 5 else "0.4000"))

    src = KalshiMarketData("kalshi", KalshiMarketData.Params(series=["KXNFLGAME"], rps=1e6, clock_check_s=0),
                           venue=rest_venue(handler))
    sink = FakeSink()
    asyncio.run(asyncio.wait_for(src.run(sink, stop), 10))
    contracts = sink.of(Contract)
    assert [c.ticker for c in contracts] == [NFL, NFL2] and not src.live
    assert contracts[0].fees == FeeSchedule(700, 175) and contracts[0].rules.startswith("If Houston wins")
    assert contracts[0].settlement_source == "the Governing League (https://x)"
    books = sink.of(BookSnapshot)
    assert [b.ticker for b in books] == [NFL, NFL2, NFL, NFL2]  # polls 3-4 unchanged: not re-emitted
    assert books[0].bids == ((4_000, 1_000),) and books[0].asks == ((4_500, 500),)
    assert [t.trade_id for t in sink.of(Trade)] == ["t2"]  # outside the universe: skipped; repeats: deduped
    assert any(s.connected and s.detail.startswith("REST poll (no API key): 2 books") for s in sink.of(SourceStatus))
    kinds = {(src_, k) for src_, k, _ in sink.records}
    assert kinds == {(RAW_REST, "c"), (RAW_REST, "m"), (RAW_REST, "f")}
    assert sink.records[0] == (RAW_REST, "c", '{"event":"universe","contracts":2}')
    first = json.loads(next(p for s_, k, p in sink.records if k == "f"))
    assert first["url"] == f"/markets/{NFL}/orderbook?depth=0" and first["body"] == book()


# ---- Kalshi over the websocket -----------------------------------------------------------------------


class FakeStream:
    def __init__(self, items: list) -> None:
        self.items, self.calls = items, []
        self.closed = asyncio.Event()

    async def __aiter__(self):
        for item in self.items:
            yield item
            await asyncio.sleep(0)
        await self.closed.wait()

    async def add(self, tickers) -> None:
        self.calls.append(("add", sorted(tickers)))

    async def remove(self, tickers) -> None:
        self.calls.append(("remove", sorted(tickers)))

    async def reconnect(self) -> None:
        self.calls.append(("reconnect",))

    async def close(self) -> None:
        self.closed.set()


def test_kalshi_live_mode_records_frames_decodes_resyncs_and_refreshes_the_universe():
    universe = {"markets": [market(NFL)]}
    signer = KalshiSigner("kid", ed25519.Ed25519PrivateKey.generate())

    def handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path.removeprefix("/trade-api/v2")
        assert "KALSHI-ACCESS-SIGNATURE" in req.headers  # signed requests, never recorded
        if path == "/markets":
            return httpx.Response(200, json={**universe, "cursor": ""})
        return httpx.Response(200, json={"series": {"ticker": "KXNFLGAME", "fee_type": "quadratic"}})

    venue = rest_venue(handler, signer)
    venue.signer = signer
    stop = asyncio.Event()
    stream = FakeStream([
        Control(1_000_000_000, "connected", {"tickers": 1}),
        Frame(2_000_000_000, snapshot(NFL, [["0.4000", "10.00"]], [["0.5500", "5.00"]], seq=1)),
        Frame(3_000_000_000, delta(NFL, "0.4100", "2.00", "yes", seq=2)),
        Frame(4_000_000_000, frame("not_a_known_type", {"x": 1}, seq=3)),
    ])
    venue.stream = lambda tickers, **kw: stream  # type: ignore[method-assign]
    src = KalshiMarketData("kalshi", KalshiMarketData.Params(series=["KXNFLGAME"], refresh_s=0.05, clock_check_s=0),
                           venue=venue)
    sink = FakeSink()

    async def run() -> None:
        task = asyncio.create_task(src.run(sink, stop))
        await asyncio.sleep(0.02)
        sink.on_overflow[0]()  # the engine's queue overflowed: re-snapshot
        stream.items.append(Frame(5_000_000_000, delta(NFL, "0.4100", "1.00", "yes", seq=4)))
        universe["markets"] = [market(NFL2)]  # NFL closed, NFL2 listed
        await asyncio.sleep(0.15)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert src.live
    assert [(s, k) for s, k, _ in sink.records][:6] == [(RAW, "c"), (RAW, "m"), (RAW, "c"), (RAW, "f"), (RAW, "f"),
                                                        (RAW, "f")]
    assert [type(e).__name__ for e in sink.events if not isinstance(e, (SourceStatus, Contract))][:3] == [
        "StreamReset", "BookSnapshot", "BookDelta"]
    assert sink.of(SourceStatus)[1].detail.startswith("websocket connected")
    assert ("add", [NFL2]) in stream.calls and ("remove", [NFL]) in stream.calls
    assert NFL2 in {c.ticker for c in sink.of(Contract)} and src.universe.keys() == {NFL2}
    assert src.skew_ms is not None


def test_kalshi_lists_the_whole_universe_again_on_a_new_utc_day(monkeypatch):
    import datetime as dt

    import pmx.sources.kalshi as kalshi

    universe = {"markets": [market(NFL), market(NFL2)]}

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path.removeprefix("/trade-api/v2") == "/markets":
            return httpx.Response(200, json={**universe, "cursor": ""})
        return httpx.Response(200, json={"series": {"ticker": "KXNFLGAME", "fee_type": "quadratic"}})

    signer = KalshiSigner("kid", ed25519.Ed25519PrivateKey.generate())
    venue = rest_venue(handler, signer)
    venue.signer = signer
    stream = FakeStream([])
    venue.stream = lambda tickers, **kw: stream  # type: ignore[method-assign]
    day = [dt.date(2026, 10, 18)]
    monkeypatch.setattr(kalshi, "_utc_day", lambda: day[0])
    src = KalshiMarketData("kalshi", KalshiMarketData.Params(series=["KXNFLGAME"], refresh_s=0.02, clock_check_s=0),
                           venue=venue)
    sink = FakeSink()
    stop = asyncio.Event()

    async def run() -> None:
        task = asyncio.create_task(src.run(sink, stop))
        await asyncio.sleep(0.1)  # refreshes on the same day: nothing changed, nothing recorded
        day[0] = dt.date(2026, 10, 19)
        universe["markets"] = [market(NFL), market(NFL2, "closed")]
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    listing = '{"event":"universe","contracts":2}'
    recs = [(k, json.loads(p).get("ticker", p)) for _, k, p in sink.records]
    assert recs == [("c", listing), ("m", NFL), ("m", NFL2), ("c", listing), ("m", NFL), ("m", NFL2)]
    assert [(c.ticker, c.status) for c in sink.of(Contract)] == [(NFL, "open"), (NFL2, "open"), (NFL2, "closed")], \
        "the listing records unchanged contracts without emitting them again"


def test_kalshi_live_mode_without_a_key_says_so_and_waits():
    venue = rest_venue(lambda req: httpx.Response(500))
    stop = asyncio.Event()
    sink = FakeSink()

    async def run() -> None:
        task = asyncio.create_task(KalshiMarketData("kalshi", KalshiMarketData.Params(mode="live"),
                                                    venue=venue).run(sink, stop))
        await asyncio.sleep(0.01)
        stop.set()
        await task

    asyncio.run(run())
    assert [s.connected for s in sink.of(SourceStatus)] == [False] and "API key" in sink.of(SourceStatus)[0].detail


def test_kalshi_account_streams_fills_and_polls_the_balance():
    def handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path.removeprefix("/trade-api/v2")
        if path == "/portfolio/balance":
            return httpx.Response(200, json={"balance_dollars": "123.45"})
        return httpx.Response(200, json={"market_positions": [], "cursor": ""})

    signer = KalshiSigner("kid", ed25519.Ed25519PrivateKey.generate())
    venue = rest_venue(handler, signer)
    venue.signer = signer
    fill = frame("fill", {"trade_id": "f1", "order_id": "o1", "client_order_id": "c1", "market_ticker": NFL,
                          "is_taker": True, "side": "yes", "yes_price_dollars": "0.4000", "count_fp": "2.00",
                          "action": "buy", "ts": 1_792_330_000, "fee_cost": "0.0336"}, sid=9, seq=1)
    order = frame("user_order", {"order_id": "o1", "client_order_id": "c1", "ticker": NFL, "status": "canceled",
                                 "book_side": "bid", "yes_price_dollars": "0.4000", "fill_count_fp": "2.00",
                                 "remaining_count_fp": "0.00", "initial_count_fp": "5.00",
                                 "last_updated_ts_ms": 1_792_330_000_500, "last_update_reason": "SelfTradeCancel"},
                  sid=10, seq=1)
    stream = FakeStream([Control(1, "connected"), Frame(2, fill), Frame(3, order)])
    asked = {}

    def streaming(tickers, **kw):
        asked.update(kw)
        return stream

    venue.stream = streaming  # type: ignore[method-assign]
    stop = asyncio.Event()
    sink = FakeSink()
    account = KalshiAccount("account", venue=venue)
    connects: list[int] = []
    account.on_connect.append(lambda: connects.append(1))

    async def run() -> None:
        task = asyncio.create_task(account.run(sink, stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    fills, snaps = sink.of(Fill), sink.of(AccountSnapshot)
    assert len(fills) == 1 and fills[0].ticker == NFL and fills[0].side == "bid" and fills[0].qty == 200
    assert snaps and snaps[0].balance == 123_450_000
    assert {(s, k) for s, k, _ in sink.records} == {("account", "c"), ("account", "f")}
    assert asked == {"trades": False, "fills": True, "orders": True} and connects == [1]
    assert sink.of(StreamReset) == [], "the account stream has no books: its connects mustn't reset the venue's"
    [o] = sink.of(VenueOrder)
    assert (o.client_order_id, o.status, o.qty, o.filled, o.remaining, o.price, o.reason) == (
        "c1", "canceled", 500, 200, 0, 4_000, "SelfTradeCancel")
    no_key = FakeSink()

    async def nokey() -> None:
        stop2 = asyncio.Event()
        task = asyncio.create_task(KalshiAccount("account", venue=rest_venue(handler)).run(no_key, stop2))
        await asyncio.sleep(0.01)
        stop2.set()
        await task

    asyncio.run(nokey())
    assert [s.connected for s in no_key.of(SourceStatus)] == [False]


def test_clock_offset_bounds_from_one_second_date_headers():
    assert offset_bounds([(1_000.0, 1_050.0, 1)]) == (-50.0, 1_000.0)  # one sample: within a second
    straddle = [(1_900.0, 1_950.0, 1), (1_960.0, 2_010.0, 2)]  # the server's second ticked between them
    lo, hi = offset_bounds(straddle)
    assert (lo, hi) == (-10.0, 100.0)  # from ±1 s down to 110 ms
    assert offset_bounds([(0.0, 10.0, 5), (20.0, 30.0, 1)]) is None  # inconsistent


def test_espn_source_records_no_credentials():
    board = json.loads((FIX / "espn" / "scoreboard_pre_post.json").read_text())
    stop = asyncio.Event()

    def handler(req: httpx.Request) -> httpx.Response:
        stop.set()
        return httpx.Response(200, json=board)

    sink = FakeSink()
    src = EspnNfl("espn", EspnNfl.Params(idle_s=0.001), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    asyncio.run(asyncio.wait_for(src.run(sink, stop), 5))
    assert sink.records and all(s == "espn" for s, _, _ in sink.records)


def test_state_announces_each_contracts_resolution_time_once():
    from pmx.engine.events import Schedule

    s = State()
    c = Contract("kalshi", NFL, "KXNFLGAME-26OCT18HOUJAC", "HOU at JAX", "Houston", "open", 9_000, event_ts_ms=8_000)
    assert s.apply(c, 100) == [Schedule(NFL, "resolution", 8_000, "HOU at JAX: expected resolution", 100)]
    assert s.apply(c, 200) == []  # a refresh with nothing new
    assert [e.at_ms for e in s.apply(msgspec.structs.replace(c, event_ts_ms=8_500), 300)] == [8_500]
    no_time = Contract("kalshi", "X-1", "X", "", "", "open", None)
    assert s.apply(no_time, 400) == []
