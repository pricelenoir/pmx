from __future__ import annotations

import asyncio
from pathlib import Path

import msgspec

from pmx.cli import build_parser
from pmx.cli.feed import serve_argv
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, StreamReset, Trade
from pmx.engine.events import (
    Forecast,
    Halt,
    LimitUsage,
    LogEvent,
    OrderUpdate,
    PortfolioSnapshot,
    RiskReject,
    ScopeStats,
    SlotFill,
    SlotStatus,
    SourceStatus,
    Timer,
)
from pmx.feed import proto
from pmx.feed.hub import FeedHub
from pmx.feed.publisher import Publisher

C = Contract("kalshi", "T-1", "E-1", "Title", "Sub", "open", 2_000, series_ticker="E", event_ts_ms=1_000)


def hub(**kw) -> FeedHub:
    return FeedHub(proto.Hello("kalshi", "prod", "live"), **kw)


def drain(client) -> list[proto.Msg]:
    out = []
    while not client.queue.empty():
        line = client.queue.get_nowait()
        out.append(None if line is None else proto.decoder.decode(line))
    return out


def test_new_viewer_gets_full_state_then_updates():
    h = hub()
    h.on_event(C)
    h.on_event(BookSnapshot("kalshi", "T-1", ((5500, 100),), ((5700, 50),), 1))
    h.on_event(BookDelta("kalshi", "T-1", "bid", 5600, 30, 2))
    h.on_event(Trade("kalshi", "x1", "T-1", 5600, 300, "bid", 3))
    h.set_status(True, "websocket connected")

    c = h.attach()
    state = drain(c)
    assert [type(m).__name__ for m in state] == ["Hello", "Status", "ContractMsg", "Book", "TradeMsg", "Top", "Synced"]
    book, top = state[3], state[5]
    assert book.bids == [(5600, 30), (5500, 100)] and book.asks == [(5700, 50)]
    assert (top.bid, top.ask, top.last) == ((5600, 30), (5700, 50), (5600, 300, "bid"))
    assert state[2].event_ts_ms == 1_000

    # Market data reaches viewers only when the publisher sends the market (v3): a book + top, once per change.
    assert h.on_event(BookDelta("kalshi", "T-1", "ask", 5700, -50, 4)) == "T-1"
    assert h.on_event(BookDelta("kalshi", "OTHER", "ask", 5700, 10, 4)) is None  # no book yet
    assert drain(c) == []
    assert h.send_market("T-1", 5) == 2 and h.send_market("T-1", 6) == 0
    sent = drain(c)
    assert sent[0] == proto.Book("T-1", [(5600, 30), (5500, 100)], [], 4) and sent[1].ask is None


def test_out_of_sync_delta_marks_book_stale_and_reset_clears():
    h = hub()
    h.on_event(BookSnapshot("kalshi", "T-1", ((5500, 100),), (), 1))
    c = h.attach()
    drain(c)
    h.on_event(BookDelta("kalshi", "T-1", "bid", 5500, -101, 2))
    assert drain(c) == [proto.Stale("T-1")]
    assert h.books.get("kalshi", "T-1") is None

    h.on_event(BookSnapshot("kalshi", "T-1", ((5500, 100),), (), 3))
    h.on_event(StreamReset("kalshi", "gap", 4))
    assert isinstance(drain(c)[-1], proto.Reset)
    assert not h.books.books


def test_slow_viewer_is_dropped():
    h = hub(queue_max=4)
    c = h.attach()  # hello + status
    for i in range(10):
        h.on_event(Trade("kalshi", f"x{i}", "T-1", 5600, 100, "bid", i))
    assert c.dropped
    assert drain(c) == [None]


def test_socket_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # relative path keeps the socket under the AF_UNIX length limit

    async def run() -> list[proto.Msg]:
        h = hub()
        h.on_event(C)
        server = await h.serve(Path("f.sock"))
        try:
            reader, writer = await asyncio.open_unix_connection("f.sock")
            got = [proto.decoder.decode(await reader.readline()) for _ in range(4)]
            h.on_event(Trade("kalshi", "x1", "T-1", 5600, 300, "ask", 3))
            got.append(proto.decoder.decode(await asyncio.wait_for(reader.readline(), 2)))
            writer.close()
            return got
        finally:
            server.close()

    got = asyncio.run(run())
    assert [type(m).__name__ for m in got] == ["Hello", "Status", "ContractMsg", "Synced", "TradeMsg"]
    assert got[4].taker_side == "ask"


def test_messages_are_tagged_json():
    raw = msgspec.json.decode(proto.encode(proto.Stale("T-1")))
    assert raw == {"t": "stale", "ticker": "T-1"}


def test_tui_passes_feed_args_to_serve():
    args = build_parser().parse_args(["tui", "--env", "prod", "--series", "KXNFLGAME", "KXFED", "--poll", "3"])
    assert serve_argv(args) == ["--env", "prod", "--poll", "3.0", "--series", "KXNFLGAME", "KXFED"]
    assert build_parser().parse_args(["serve", *serve_argv(args)]).series == ["KXNFLGAME", "KXFED"]


# ---- v2 / v3 ------------------------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures" / "feed"


def test_v2_fixture_still_parses():
    """v2 messages stay valid input (fields added since then default)."""
    lines = (FIXTURES / "v2.ndjson").read_bytes().splitlines(keepends=True)
    for line in lines:
        raw = msgspec.json.decode(line)
        again = msgspec.json.decode(proto.encode(proto.decoder.decode(line)))
        assert {k: again[k] for k in raw} == raw
    tags = {msgspec.json.decode(line)["t"] for line in lines}
    assert tags == {"hello", "status", "account", "models", "contract", "book", "delta", "stale", "reset", "trade",
                    "fill", "entity", "forecast", "slot", "order", "risk", "log", "halt", "portfolio", "equity", "perf"}


def test_shared_v3_fixture_is_what_the_code_sends():
    """`v3.ndjson` (parsed by cargo test too) is the real publisher's output; regenerate it with
    `tests/fixtures/feed/make_v3.py` when the protocol changes."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("make_v3", FIXTURES / "make_v3.py")
    make_v3 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(make_v3)
    lines = (FIXTURES / "v3.ndjson").read_bytes().splitlines(keepends=True)
    assert make_v3.session() == lines
    assert [proto.encode(proto.decoder.decode(line)) for line in lines] == lines
    tags = {msgspec.json.decode(line)["t"] for line in lines}
    assert tags == {"hello", "synced", "status", "account", "models", "contract", "book", "top", "stale", "reset",
                    "trade", "fill", "entity", "advice", "slot", "order", "risk", "log", "halt", "portfolio", "equity",
                    "perf"}


def _publisher(**kw):
    h = hub()
    links = {"x:one": {"T-1", "T-2"}}
    pub = Publisher(h, tickers_of=lambda e: links.get(e, ()), **kw)
    return h, pub, h.attach()


class _Thing(msgspec.Struct, frozen=True):
    """A domain-free entity, the shape any domain's state has."""

    key: str
    temp: int
    ts_ms: int

    @property
    def entity(self) -> str:
        return self.key

    def summary(self) -> str:
        return f"{self.temp}°F"

    def display_fields(self) -> list[tuple[str, str]]:
        return [("now", f"{self.temp}°F")]


def _fill(ts: int = 5) -> Fill:
    return Fill("kalshi", "f1", "o1", "T-1", "bid", 5500, 500, 4_331, False, ts)


def test_publisher_maps_engine_events():
    h, pub, c = _publisher()
    drain(c)
    pub.on_event(_Thing("x:one", 64, 1), 1)
    pub.on_event(_fill(), 2)  # raw venue fill: viewers see it attributed instead
    pub.on_event(SlotFill("mm/T-1", "paper", _fill()), 3)
    pub.on_event(OrderUpdate("mm/T-1", "paper", "c1", None, "T-1", "bid", 5500, 500, 0, "open", 4), 4)
    pub.on_event(RiskReject("mm/T-1", "T-1", "bid", 5600, 100, "max_position", "50 >= 50", 5), 5)
    pub.on_event(LogEvent("error", "mm/T-1", "boom", 6), 6)
    pub.on_event(Halt("", "kill switch", True, 7), 7)
    pub.on_event(SourceStatus("weather", False, "timeout", 8), 8)
    got = drain(c)
    assert [type(m).__name__ for m in got] == ["EntityMsg", "FillMsg", "OrderMsg", "RiskMsg", "LogMsg", "HaltMsg",
                                               "Status"]
    assert got[0] == proto.EntityMsg("x:one", "_Thing", ["T-1", "T-2"], "64°F", [("now", "64°F")], 1)
    assert (got[1].slot, got[1].mode, got[1].fee) == ("mm/T-1", "paper", 4_331)
    assert got[6].source == "weather" and h.status.source == ""

    # Queued (rate-limited) orders count as working until they finish.
    pub.on_event(OrderUpdate("mm/T-1", "paper", "c2", None, "T-1", "ask", 5800, 500, 0, "queued", 8), 8)
    assert "c2" in h.working
    pub.on_event(OrderUpdate("mm/T-1", "paper", "c2", None, "T-1", "ask", 5800, 500, 0, "canceled", 8), 8)
    drain(c)

    # Fills finish the working order; a late viewer sees only what's still working.
    pub.on_event(OrderUpdate("mm/T-1", "paper", "c1", "p1", "T-1", "bid", 5500, 500, 500, "filled", 9), 9)
    pub.on_event(Halt("", "kill switch", False, 10), 10)
    assert not h.working and not h.halts
    late = [type(m).__name__ for m in h.snapshot()]
    assert late == ["Hello", "Status", "Status", "EntityMsg", "OrderMsg", "OrderMsg", "FillMsg", "RiskMsg", "LogMsg",
                    "Synced"]
    assert [m.status for m in h.orders] == ["canceled", "filled"], "finished orders, oldest first"


def _market(pub, ticker: str = "T-1") -> None:
    pub.on_event(msgspec.structs.replace(C, ticker=ticker), 0)
    pub.on_event(BookSnapshot("kalshi", ticker, ((5500, 100),), ((5700, 100),), 0), 0)


def test_forecasts_reach_viewers_in_their_markets_top_latest_wins():
    h, pub, c = _publisher()
    _market(pub)
    drain(c)
    for i in range(1, 1_001):  # 1,000 forecasts over 1 s of engine time
        pub.on_event(Forecast("fair", "T-1", i / 1_000, i, i), i)
    sent = drain(c)
    assert sent[0] == proto.Models(["fair"]) and not any(isinstance(m, proto.ForecastMsg) for m in sent)
    tops = [m for m in sent if isinstance(m, proto.Top)]
    assert len(tops) <= 4 and all(m.ticker == "T-1" for m in tops)  # at most every 250 ms, from t=0
    assert h.forecasts[("fair", "T-1")].p == 1.0  # a late viewer gets the latest
    pub.on_event(Timer(1_250), 1_250)
    assert [m.fairs for m in drain(c)] == [{"fair": 1.0}]
    pub.on_event(Timer(2_000), 2_000)
    assert drain(c) == []


def test_market_data_conflates_to_4_per_second_10_when_traded_and_never_holds_prints():
    h, pub, c = _publisher()
    for t in ("T-1", "T-2"):
        _market(pub, t)
    pub.on_event(SlotStatus("mm/T-2", "mm", "T-2", "paper", "active", 0, 0, 0, 0, 1, "quote", 0), 0)
    drain(c)
    for ms in range(1, 1_001):  # a delta on each market every ms, a print every 10 ms
        for t in ("T-1", "T-2"):
            pub.on_event(BookDelta("kalshi", t, "bid", 5500, 1, ms), ms)
            if ms % 10 == 0:
                pub.on_event(Trade("kalshi", f"{t}{ms}", t, 5700, 1, "bid", ms), ms)
    sent = drain(c)
    books = {t: sum(isinstance(m, proto.Book) and m.ticker == t for m in sent) for t in ("T-1", "T-2")}
    assert 3 <= books["T-1"] <= 4 and 9 <= books["T-2"] <= 10, books
    assert sum(isinstance(m, proto.TradeMsg) for m in sent) == 200, "prints are never conflated"
    pub.flush(2_000)
    last = {m.ticker: m for m in drain(c) if isinstance(m, proto.Book)}
    assert last["T-1"].bids == [(5500, 1_100)], "latest wins: the held-back state goes out at the deadline"
    assert h.send_market("T-1", 2_001) == 0, "nothing changed: nothing resent"


def test_slot_positions_show_in_tops_per_mode():
    h, pub, c = _publisher()
    _market(pub)
    for slot, mode, q in (("a/T-1", "paper", 300), ("b/T-1", "paper", -100), ("c/T-1", "live", 200)):
        pub.on_event(SlotStatus(slot, slot[0], "T-1", mode, "active", q, 0, 0, 0, 0, "", 0), 0)
    pub.flush(1_000)
    assert h.top("T-1", 0).position == {"paper": 200, "live": 200} and h.traded("T-1")
    pub.on_event(SlotStatus("c/T-1", "c", "T-1", "live", "idle", 0, 0, 0, 0, 0, "", 1_100), 1_100)
    assert h.top("T-1", 0).position == {"paper": 200}
    pub.on_event(SlotStatus("g/EV", "g", "EV", "paper", "active", 500, 0, 0, 0, 0, "", 1_200), 1_200)
    assert not any(isinstance(m, proto.Top) and m.ticker == "EV" for m in drain(c)), "a group key isn't a market"


def test_throttle_cost_does_not_grow_with_held_back_markets():
    import time

    from pmx.feed.publisher import _Throttle

    th = _Throttle(250)
    for i in range(5_000):
        assert th.ready(i, 0)
        assert not th.ready(i, 1)  # now pending, due at 250
    t = time.perf_counter()
    for now in range(2, 20_002):
        assert th.due(now % 249) == []  # nothing due yet
    assert time.perf_counter() - t < 0.2, "flush must not scan pending keys"
    assert len(th.due(250)) == 5_000
    assert th.due(10_000) == [] and not th.pending


def test_slot_status_and_portfolio_throttle_to_one_per_second():
    h, pub, c = _publisher()
    drain(c)

    def slot(ts: int, pos: int) -> SlotStatus:
        return SlotStatus("mm/T-1", "mm", "T-1", "paper", "active", pos, 0, 0, 0, 1, "quote", ts)

    def snap(ts: int, equity: int) -> PortfolioSnapshot:
        scopes = [ScopeStats(scope, "paper", equity, 0, 0, equity, 0, equity, 0, 0, 0, None, None, 0, 0, 0)
                  for scope in ("portfolio", "strategy:mm", "slot:mm/T-1")]
        return PortfolioSnapshot(ts, scopes, [], [LimitUsage("global", "max_loss", 0, 100, False)])

    for ts in range(0, 1_000, 100):
        pub.on_event(slot(ts, ts), ts)
        pub.on_event(snap(ts, ts), ts)
    sent = drain(c)
    assert [type(m).__name__ for m in sent] == ["SlotMsg", "PortfolioMsg", "EquityMsg", "EquityMsg"]
    assert {(m.scope, m.points[0][1]) for m in sent[2:]} == {("portfolio", 0), ("strategy:mm", 0)}
    pub.flush(1_000)
    sent = drain(c)
    assert sent[0].position == 900 and sent[1].ts_ms == 900
    assert h.equity[("portfolio", "paper")] == [(0, 0, 0), (900, 900, 0)]


def test_equity_history_thins_by_age_and_is_resent_whole():
    h = hub()
    day, now = 86_400_000, 40 * 86_400_000
    # 40 days at one point a minute, then the last hour at one a second.
    pts = [(t, t, 0) for t in range(0, now - 3_600_000, 60_000)]
    pts += [(t, t, 0) for t in range(now - 3_600_000, now + 1, 1_000)]
    h.seed_equity("portfolio", "paper", pts)
    series = h.equity[("portfolio", "paper")]
    assert len(series) <= 2_000 and series[-1] == (now, now, 0)
    assert series[-600:] == [(t, t, 0) for t in range(now - 599_000, now + 1, 1_000)], "last 10 min in full"
    month = [t for t, _, _ in series if now - 30 * day <= t < now - 2 * day]
    assert 600 < len(month) < 700, "about hourly 2-30 days back"
    assert series[0][0] < now - 30 * day, "older history survives, at daily resolution"
    eq = [m for m in h.snapshot() if isinstance(m, proto.EquityMsg)]
    assert len(eq) == 1 and eq[0].replace and eq[0].points == series
    # Live points keep arriving at a second each without eroding the old history.
    for t in range(now + 1_000, now + 3 * 3_600_000, 1_000):
        h.add_equity("portfolio", "paper", (t, t, 0))
    series = h.equity[("portfolio", "paper")]
    assert len(series) <= 2_000 and series[0][0] < now - 30 * day


def test_demo_session_is_complete_consistent_and_labeled():
    from pmx.feed.demo import DemoEngine

    h = FeedHub(proto.Hello("kalshi", "demo", "demo"), queue_max=1_000_000)
    c = h.attach()
    t0 = 1_792_000_000_000
    demo = DemoEngine(h, now_ms=t0)
    for i in range(1, 1_501):  # 2.5 minutes at 10 Hz (the scripted reject fires at 150 s)
        demo.step(t0 + i * 100)
        for m in demo.markets.values():
            assert not m.book.crossed()
    msgs = drain(c)
    tags = {msgspec.json.decode(proto.encode(m))["t"] for m in msgs}
    assert {"entity", "top", "models", "slot", "order", "fill", "risk", "log", "portfolio", "equity", "perf",
            "status", "book", "trade", "advice"} <= tags
    assert all(m.ticker.startswith("DEMO-") for m in msgs if isinstance(m, proto.ContractMsg))
    for s in demo.slots:  # positions reconcile with the fills the viewer saw
        fills = [m for m in msgs if isinstance(m, proto.FillMsg) and m.slot == s.id]
        assert s.q == sum(f.qty if f.side == "bid" else -f.qty for f in fills)
        assert s.fees == sum(f.fee for f in fills)
    assert {m.mode for m in msgs if isinstance(m, proto.SlotMsg)} == {"paper", "live"}


def test_tui_rebuilds_when_sources_are_newer(tmp_path):
    import os

    from pmx.cli.feed import _tui_stale

    (tmp_path / "src").mkdir()
    rs, binary = tmp_path / "src" / "main.rs", tmp_path / "pmx-tui"
    rs.write_text("fn main() {}")
    assert _tui_stale(binary, tmp_path), "missing binary"
    binary.write_text("")
    os.utime(rs, (1, 1))
    assert not _tui_stale(binary, tmp_path)
    os.utime(rs, None)
    os.utime(binary, (1, 1))
    assert _tui_stale(binary, tmp_path), "a source changed after the build"


def test_demo_has_its_own_socket():
    from pmx.cli.common import socket_for

    args = build_parser().parse_args(["tui", "--demo"])
    assert serve_argv(args) == ["--demo"]
    assert socket_for(args).name == "pmx-demo.sock"


def test_up_starts_the_engine_and_tui_only_the_feed():
    from pmx.cli.feed import _command

    p = build_parser()
    assert _command(p.parse_args(["up", "--env", "prod"])) == "run"
    assert _command(p.parse_args(["up", "--feed-only"])) == "serve"
    assert _command(p.parse_args(["up", "--demo"])) == "serve"
    up = p.parse_args(["up", "--live", "--config", "c.toml"])
    assert build_parser().parse_args(["run", *serve_argv(up)]).live
    assert "--live" not in serve_argv(p.parse_args(["tui"])), "the viewer never asks for a trading engine"


# ---- feed process ---------------------------------------------------------------------------------------


def test_link_codec_round_trips_every_event_and_renders_entities():
    from pmx.feed.server import EVENT_TYPES, Decoder, Encoder

    fill = Fill("kalshi", "f", "o", "T-1", "bid", 4000, 100, 7, True, 10)
    events = [C, BookSnapshot("kalshi", "T-1", ((5500, 100),), ((5700, 50),), 1),
              BookDelta("kalshi", "T-1", "bid", 5600, 30, 2), Trade("kalshi", "x", "T-1", 5600, 300, "bid", 3),
              StreamReset("kalshi", "gap", 4), Forecast("m", "T-1", 0.5, 5, 4), Timer(5), fill,
              SourceStatus("espn", True, "ok", 7), SlotFill("s/T-1", "paper", fill),
              OrderUpdate("s/T-1", "paper", "c1", None, "T-1", "bid", 4000, 100, 0, "open", 9),
              RiskReject("s/T-1", "T-1", "bid", 4000, 100, "max_position", "x", 11),
              SlotStatus("s/T-1", "s", "T-1", "paper", "active", 100, 0, 0, 0, 1, "", 12), Halt("", "x", True, 13),
              LogEvent("info", "x", "hello", 15), proto.Models(["m"]), _Thing("x:one", 64, 16)]
    frame = Encoder(lambda e: ["T-1"]).frame(list(enumerate(events)), 3)
    kind, dropped, items = Decoder().decode(frame[4:])
    sent = [(i, e) for i, e in enumerate(events) if type(e) not in (Timer, Fill)]  # these stay in the engine
    assert (kind, dropped) == (0, 3) and [i for i, _ in items] == [i for i, _ in sent]
    assert [e for _, e in items[:-1]] == [e for _, e in sent[:-1]]
    assert items[-1][1] == proto.EntityMsg("x:one", "_Thing", ["T-1"], "64°F", [("now", "64°F")], 16)
    assert {t.__name__ for _, t in EVENT_TYPES.items()} >= {type(e).__name__ for _, e in sent[:-2]}


class _Ring:
    """Stands in for the Tap: what the link discards on a resync."""

    def __init__(self) -> None:
        self.held: list = []
        self.dropped = 0

    def discard(self) -> list:
        out, self.held = self.held, []
        return out


class _Writer:
    def __init__(self) -> None:
        self.frames: list[bytes] = []
        self.buffered = 0
        self.transport = self

    def get_write_buffer_size(self) -> int:
        return self.buffered

    def is_closing(self) -> bool:
        return False

    def write(self, b: bytes) -> None:
        self.frames.append(b)


def test_link_snapshot_is_whole_and_resyncs_after_drops():
    from pmx.feed.server import SNAPSHOT, Decoder, EngineLink

    async def run():
        ring = _Ring()
        link = EngineLink(Path("x.sock"), proto.Hello("kalshi", "prod", "engine"), tap=ring, clock=lambda: 99,
                          snapshot=lambda: [BookSnapshot("kalshi", "T-1", ((5500, 100),), (), 50)], high_water=100)
        await link.deliver([(1, C), (2, SlotStatus("s/T-1", "s", "T-1", "paper", "active", 0, 0, 0, 0, 2, "", 2))])
        assert link.dropped == 0, "nothing can be missed before the first feed process exists"
        w = _Writer()
        link._writer = w
        ring.held = [(3, OrderUpdate("s/T-1", "paper", "c1", None, "T-1", "bid", 4000, 100, 0, "open", 3)),
                     (3, OrderUpdate("s/T-1", "paper", "c0", None, "T-1", "ask", 6000, 100, 0, "open", 3)),
                     (4, OrderUpdate("s/T-1", "paper", "c0", None, "T-1", "ask", 6000, 100, 0, "canceled", 4)),
                     (4, Halt("s/T-1", "max_loss", True, 4)), (5, Halt("s/T-1", "", False, 5)),
                     (5, _Thing("x:one", 64, 5))]
        link._resync(w)  # what a new feed process gets first
        kind, _, items = Decoder().decode(w.frames[-1][4:])
        assert kind == SNAPSHOT
        assert [type(e).__name__ for _, e in items] == ["Hello", "Contract", "BookSnapshot", "EntityMsg",
                                                         "SlotStatus", "OrderUpdate"]
        assert items[-1][1].client_order_id == "c1", "finished orders and lifted halts aren't resent"
        await link.deliver([(6, Trade("kalshi", "x", "T-1", 5600, 1, "bid", 6))])
        assert len(w.frames) == 2 and link.sent == 1
        w.buffered = 101  # the feed process fell behind
        await link.deliver([(7, Trade("kalshi", "y", "T-1", 5600, 1, "bid", 7))])
        await link.deliver([(8, Trade("kalshi", "z", "T-1", 5600, 1, "bid", 8))])
        assert len(w.frames) == 2 and link.dropped == 2 and link._stale
        w.buffered = 0
        await link.deliver([(9, Trade("kalshi", "w", "T-1", 5600, 1, "bid", 9))])
        assert link.dropped == 3, "stale: dropped until the resync"
        link._resync(w)
        assert Decoder().decode(w.frames[-1][4:])[:2] == (SNAPSHOT, 3)

    asyncio.run(run())


def test_link_a_snapshot_larger_than_high_water_does_not_count_as_behind():
    from pmx.feed.server import EngineLink

    async def run():
        books = [BookSnapshot("kalshi", f"T-{i}", ((5500, 100),), (), 50) for i in range(20)]
        link = EngineLink(Path("x.sock"), proto.Hello("kalshi", "prod", "engine"), snapshot=lambda: books,
                          high_water=100)
        w = _Writer()
        link._writer = w
        link._resync(w)
        snap = len(w.frames[-1])
        assert snap > 100
        trade = (6, Trade("kalshi", "x", "T-1", 5600, 1, "bid", 6))
        w.buffered = snap + 50  # the snapshot plus a few batches
        await link.deliver([trade])
        assert link.dropped == 0 and len(w.frames) == 2
        w.buffered = 120  # mostly drained: at most 120 bytes of the snapshot can be left
        await link.deliver([trade])
        assert link.dropped == 0
        w.buffered = 221
        await link.deliver([trade])
        assert link.dropped == 1 and link._stale

    asyncio.run(run())


async def _read_until(reader, pred, timeout=10.0) -> list[dict]:
    out: list[dict] = []
    async with asyncio.timeout(timeout):
        while True:
            line = await reader.readline()
            if not line:
                raise ConnectionError("feed closed")
            m = msgspec.json.decode(line)
            out.append(m)
            if pred(m):
                return out


def test_feed_process_serves_viewers_and_a_killed_one_is_restarted(tmp_path, monkeypatch):
    import os
    import signal
    import time

    from pmx.engine.engine import Tap
    from pmx.feed.demo import LoadGen
    from pmx.feed.server import EngineLink, TapPublisher

    monkeypatch.chdir(tmp_path)  # a short relative socket path; the feed process inherits the cwd
    sock = Path("f.sock")

    async def run():
        stop = asyncio.Event()
        link = EngineLink(sock, proto.Hello("kalshi", "demo", "demo"))
        tap = link.tap = Tap(link.deliver)
        gen = LoadGen(TapPublisher(tap), markets=40, rate=2_000)
        link.snapshot = gen.market_snapshot
        steps = [0]

        async def engine():  # the stand-in engine must keep stepping whatever happens to the feed
            while not stop.is_set():
                gen.step(int(time.time() * 1000))
                steps[0] += 1
                await asyncio.sleep(0.01)

        async def until(cond, timeout=10.0):
            async with asyncio.timeout(timeout):
                while not cond():
                    await asyncio.sleep(0.02)

        tasks = [asyncio.ensure_future(x) for x in (engine(), tap.run(stop), link.run(stop))]
        try:
            await until(lambda: link.up and sock.exists())
            r, w = await asyncio.open_unix_connection(str(sock))
            first = await _read_until(r, lambda m: m["t"] == "top")
            assert first[0] == {"t": "hello", "venue": "kalshi", "env": "demo", "source": "demo", "version": 3,
                                "run": ""}
            assert sum(m["t"] == "contract" for m in first) == 40 and any(m["t"] == "book" for m in first)
            pid = link.proc.pid
            os.kill(pid, signal.SIGKILL)
            before = steps[0]
            async with asyncio.timeout(10):  # the viewer sees the feed go down
                while await r.read(65_536):
                    pass
            w.close()
            await until(lambda: link.proc.pid != pid and link.up)
            assert steps[0] > before, "the engine kept going"
            async with asyncio.timeout(10):
                while True:  # a killed feed leaves its socket file; the new one claims it once it has state
                    try:
                        r, w = await asyncio.open_unix_connection(str(sock))
                        break
                    except (ConnectionRefusedError, FileNotFoundError):
                        await asyncio.sleep(0.02)
            again = await _read_until(r, lambda m: m["t"] == "status" and m["source"] == "engine")
            assert again[0]["t"] == "hello" and again[-1]["dropped"] > 0, "the gap is counted and shown"
            assert link.restarts == 1
            w.close()
        finally:
            stop.set()
            await asyncio.wait_for(asyncio.gather(*tasks), 10)
        assert link.proc.returncode == 0 and not sock.exists(), "a clean stop leaves nothing behind"

    asyncio.run(run())


def test_load_generator_keeps_its_rate_and_books_honest():
    from pmx.feed.demo import LoadGen

    h = hub(queue_max=10_000_000)
    gen = LoadGen(h, markets=500, rate=10_000, slots=0.1, now_ms=0)
    assert len(gen.contracts) == 500 and len(gen.slot_idx) == 50
    assert {c.series_ticker.split("-")[1].rstrip("0123456789") for c in gen.contracts} >= {"LOADSPORTS", "LOADWEATHER"}
    n = sum(gen.step(ms) for ms in range(20, 2_001, 20))
    assert n == 20_000
    assert all(not b.crossed() for b in gen.books)
    assert h.books.get("kalshi", gen.contracts[0].ticker).levels("bid") == gen.books[0].levels("bid")
