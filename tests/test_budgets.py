"""Performance budgets (ARCHITECTURE *Budgets*). Slow, so not in the default run:

    uv run pytest -m slow

Each fails when its budget, or for per-event costs what was measured, is exceeded by more than 2×
on the reference machine (an Apple M1 Pro, 8 cores, 16 GB, Python 3.13).
The TUI's budgets are in `tui/src/ui.rs` (`cargo test --release scale -- --ignored`)."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from pmx.bench.feed import run_case, tap_append_us
from pmx.core.types import BookDelta
from pmx.feed import proto
from pmx.feed.demo import LoadGen
from pmx.feed.hub import FeedHub
from pmx.feed.publisher import Publisher
from pmx.feed.server import EngineLink, TapPublisher

pytestmark = pytest.mark.slow


class _Ring:
    def __init__(self) -> None:
        self.items: list = []
        self.dropped = 0

    def append(self, ev, now_ms) -> None:
        self.items.append((now_ms, ev))

    def discard(self) -> list:
        out, self.items = self.items, []
        return out


class _Writer:
    def __init__(self) -> None:
        self.transport = self

    def get_write_buffer_size(self) -> int:
        return 0

    def is_closing(self) -> bool:
        return False

    def write(self, b: bytes) -> None: ...


def _load(markets: int, seconds: float = 10.0) -> tuple[LoadGen, list[list]]:
    """The generator's setup plus `seconds` of 20k events/s, in 20 ms batches."""
    ring = _Ring()
    gen = LoadGen(TapPublisher(ring), markets=markets, rate=20_000, now_ms=0)
    batches = [ring.items]
    for ms in range(20, round(seconds * 1000) + 1, 20):
        ring.items = []
        gen.step(ms)
        batches.append(ring.items)
    return gen, batches


def test_tap_append_on_the_engine_thread():
    assert min(tap_append_us() for _ in range(5)) < 0.30, "measured 0.13 µs/event"


def test_link_encodes_and_writes_a_batch_cheaply():
    async def run() -> float:
        gen, batches = _load(5_000)
        link = EngineLink(Path("x.sock"), proto.Hello("kalshi", "bench", "demo"), tap=_Ring(),
                          snapshot=gen.market_snapshot)
        w = _Writer()
        link._writer = w
        link._resync(w)
        n = sum(len(b) for b in batches[1:])
        t = time.perf_counter()
        for b in batches[1:]:
            await link.deliver(b)
        return (time.perf_counter() - t) / n * 1e6

    assert min(asyncio.run(run()) for _ in range(3)) < 0.62, "measured 0.31 µs/event (keep 0.05 + frame 0.26)"


def test_feed_process_side_cost_per_event_at_5000_markets():
    _, batches = _load(5_000)
    hub = FeedHub(proto.Hello("kalshi", "bench", "demo"), queue_max=10_000_000)
    pub = Publisher(hub)
    for now, ev in batches[0]:
        pub.on_event(ev, now)
    hub.attach()
    events = [e for b in batches[1:] for e in b]
    t = time.perf_counter()
    for now, ev in events:
        pub.on_event(ev, now)
    assert (time.perf_counter() - t) / len(events) * 1e6 < 8.2, "measured 4.1 µs/event (publisher + hub + 1 viewer)"


def test_publisher_cost_is_flat_however_many_markets_are_held_back():
    """Conflation keeps a deadline heap: a flush pops what's due, never scans what's pending.
    Reintroducing a scan of pending keys makes this fail by orders of magnitude."""

    def per_event(held_back: int) -> float:
        hub = FeedHub(proto.Hello("kalshi", "bench", "demo"))
        pub = Publisher(hub)
        gen = LoadGen(pub, markets=held_back + 1, rate=0, slots=0, models=(), now_ms=0)
        for i, c in enumerate(gen.contracts[1:], start=1):  # every other market: sent at 0, changed at 1
            pub.on_event(BookDelta("kalshi", c.ticker, "bid", gen.books[i].best_bid()[0], 100, 1), 1)
        hot = gen.contracts[0].ticker
        price = gen.books[0].best_bid()[0]
        t = time.perf_counter()
        for i in range(20_000):  # all within the window: nothing is due, everything held back
            pub.on_event(BookDelta("kalshi", hot, "bid", price, 100, 2 + i // 100), 2 + i // 100)
        return (time.perf_counter() - t) / 20_000 * 1e6

    few, many = min(per_event(10) for _ in range(3)), min(per_event(5_000) for _ in range(3))
    assert many < 2 * few + 0.5, (few, many)


def test_engine_core_dispatch_at_20k_inputs_per_second(tmp_path, monkeypatch):
    from pmx.bench.core import run_core

    monkeypatch.chdir(tmp_path)
    r = asyncio.run(run_core(markets=5_000, events=60_000, models=2, slots=True, feed=Path("c.sock")))
    assert r["dropped"] == 0
    assert r["p99_us"] < 2_000, "budget: p99 < 1 ms at 20k events/s"
    assert r["p99_us"] < 27, "measured p99 13.3 µs with the tap (state + 2 models + a noop slot per market)"


def test_feed_under_load_conflates_drops_nothing_and_costs_the_engine_the_same_for_3_viewers(tmp_path,
                                                                                             monkeypatch):
    monkeypatch.chdir(tmp_path)
    kw = dict(sock=Path("f.sock"), markets=5_000, rate=20_000, slots=0.02, seconds=4.0, warmup=2.0)
    rows = [asyncio.run(run_case("process", n, **kw)) for n in (0, 3)]
    for r in rows:
        assert r["dropped"] == 0 and r["events_per_s"] > 18_000
        for v in r["per_viewer"]:
            span = v["seconds"]
            assert v["max_books_per_s"] <= (span // 0.25 + 1) / span, "≤ 4 books/s per changed market"
            assert v["max_books_per_s_traded"] <= (span // 0.1 + 1) / span, "≤ 10/s with slots or working orders"
            assert v["msgs_per_s"] < 40_000, "measured 15.6k msg/s per viewer (book 9.1k, top 4.1k, trade 2k)"
    none, three = (r["deliver_us_per_event"] for r in rows)
    assert none < 1.5 and three < 1.5, "measured 0.5-0.7 µs/event inside the tap's delivery"
    assert three < 2 * none + 0.2, "the engine's cost doesn't grow with viewers"


def test_a_slow_advisor_leaves_engine_dispatch_latency_unchanged(tmp_path, monkeypatch):
    """Advisors think in their own process; one taking 5 s per call costs the engine nothing."""
    from pmx.advisors.base import AdvisorSpec
    from pmx.advisors.worker import AdvisorLink
    from pmx.bench.core import run_core

    monkeypatch.chdir(tmp_path)
    links: list = []

    def slow_advisor(engine, stop):
        spec = AdvisorSpec("slow", "scripted_advice", {"steps": [{"at": "+0s", "series": "DEMO-LOADSPORTS-00000",
                                                                  "p": 0.5}], "think_s": 5.0, "loop": True})
        link = AdvisorLink([spec], engine.source(AdvisorLink.SOURCE), Path("c.sock"), clock=engine.clock.now_ms)
        links.append(link)
        return [link.run(stop)]

    kw = dict(markets=5_000, events=60_000, models=2, slots=True, feed=Path("c.sock"))
    alone = asyncio.run(run_core(**kw))
    advised = asyncio.run(run_core(**kw, attach=slow_advisor))
    assert links[0].proc is not None and links[0].restarts == 0, "the worker ran the whole time"
    assert advised["p99_us"] < 1.5 * alone["p99_us"] + 5, (alone["p99_us"], advised["p99_us"])
    assert advised["mean_us"] < 1.3 * alone["mean_us"] + 1, (alone["mean_us"], advised["mean_us"])
