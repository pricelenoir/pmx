from __future__ import annotations

import asyncio
import sys
import time

import msgspec
import pytest

from pmx import plugins
from pmx.core.types import BookDelta, BookSnapshot, Contract, StreamReset, Trade
from pmx.engine.clock import LiveClock, SimClock
from pmx.engine.engine import Engine, Tap, to_publisher
from pmx.engine.events import Advice, Forecast, LogEvent, Schedule, Timer
from pmx.engine.state import State, market_id, venue_ticker
from pmx.feed import proto
from pmx.feed.hub import FeedHub
from pmx.feed.publisher import Publisher


def contract(ticker: str, event: str = "E-1", venue: str = "kalshi") -> Contract:
    return Contract(venue, ticker, event, "Title", "Sub", "open", 2_000)


def snap(ticker: str, ts: int = 0, bid: int = 4_000, ask: int = 4_200) -> BookSnapshot:
    return BookSnapshot("kalshi", ticker, ((bid, 1_000),), ((ask, 1_000),), ts)


class Thing(msgspec.Struct, frozen=True):
    """A domain's state about one entity (satisfies EntityState)."""

    key: str
    ts_ms: int
    value: int = 0

    @property
    def entity(self) -> str:
        return self.key

    def summary(self) -> str:
        return f"{self.key}={self.value}"

    def display_fields(self) -> list[tuple[str, str]]:
        return [("value", str(self.value))]


def label(ev) -> str:
    key = getattr(ev, "ticker", None) or getattr(ev, "entity", None) or getattr(ev, "text", None)
    return f"{type(ev).__name__}:{key}" if key else type(ev).__name__


class Step:
    """A fake broker/portfolio/router/order manager: records calls, optionally reacts."""

    def __init__(self, name: str, calls: list, react=None) -> None:
        self.name, self.calls, self.react = name, calls, react

    def on_event(self, ev, state, now_ms):
        self.calls.append((self.name, label(ev), now_ms))
        return self.react(ev, state, now_ms) if self.react else []


class FakeModel:
    """Emits P(YES) = book mid for every book change."""

    def __init__(self, name: str, calls: list, *, fail_on=None) -> None:
        self.name, self.calls, self.fail_on = name, calls, fail_on

    def wants(self, ev) -> bool:
        return isinstance(ev, (BookSnapshot, BookDelta, Thing, Timer))

    def on_event(self, ev, state, now_ms):
        self.calls.append((f"model:{self.name}", label(ev), now_ms))
        if self.fail_on is not None and self.fail_on(ev):
            raise RuntimeError("boom")
        if isinstance(ev, (BookSnapshot, BookDelta)):
            book = state.book(ev.ticker)
            if book is not None and book.mid() is not None:
                return [Forecast(self.name, ev.ticker, book.mid() / 10_000, now_ms, ev.ts_ms)]
        return []


class FakePublisher:
    def __init__(self, calls: list | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.calls.append(("publisher", label(ev), now_ms))
        self.events.append(ev)


class RecState(State):
    def __init__(self, calls: list, **kw) -> None:
        super().__init__(**kw)
        self.calls = calls

    def apply(self, ev, now_ms, source=""):
        self.calls.append(("state", label(ev), now_ms))
        return super().apply(ev, now_ms, source)


def drain(engine: Engine) -> int:
    return asyncio.run(engine.dispatch_all())


# ---- dispatch --------------------------------------------------------------------


def test_dispatch_order_and_derived_events_after_their_input():
    calls: list = []
    clock = SimClock(1_000)
    engine = Engine(
        clock, RecState(calls), models=[FakeModel("m", calls)],
        broker=Step("broker", calls), portfolio=Step("portfolio", calls), router=Step("router", calls),
        order_manager=Step("order_manager", calls), publisher=FakePublisher(calls),
    )
    engine.emit(snap("T-1"))
    clock.advance(2_000)
    engine.emit(Trade("kalshi", "x", "T-1", 4_100, 100, "bid", 2_000))
    assert drain(engine) == 2
    each = ["state", "broker", "portfolio", "router", "order_manager", "publisher"]
    expected = [(s, "BookSnapshot:T-1", 1_000) for s in each]
    expected.insert(3, ("model:m", "BookSnapshot:T-1", 1_000))
    expected += [(s, "Forecast:T-1", 1_000) for s in each]  # derived, before the next input
    expected += [(s, "Trade:T-1", 2_000) for s in each]
    assert calls == expected
    assert engine.state.forecast("T-1", "m").p == pytest.approx(0.41)


def test_derived_events_are_fifo_including_their_own_descendants():
    calls: list = []
    log_a, log_b, log_c = (LogEvent("info", "x", t, 0) for t in "ABC")
    router = Step("router", calls, lambda ev, s, now: [log_a, log_b] if ev == Timer(0) else [])
    broker = Step("broker", calls, lambda ev, s, now: [log_c] if ev is log_a else [])
    pub = FakePublisher()
    engine = Engine(SimClock(), State(), router=router, broker=broker, publisher=pub)
    engine.emit(Timer(0))
    engine.emit(Timer(1))
    drain(engine)
    assert [label(e) for e in pub.events] == ["Timer", "LogEvent:A", "LogEvent:B", "LogEvent:C", "Timer"]


def test_receive_time_is_stamped_on_emit_and_is_the_time_components_see():
    calls: list = []
    clock = SimClock(1_000)
    engine = Engine(clock, State(), router=Step("router", calls))
    engine.emit(Timer(0))
    clock.advance(1_500)
    engine.emit(Timer(0))
    clock.advance(9_999)  # dispatching later doesn't change when they were received
    assert engine.queue_depth == 2
    drain(engine)
    assert [c[2] for c in calls] == [1_000, 1_500]
    assert engine.queue_depth == 0 and engine.max_queue_depth == 2 and engine.dispatched == 2


def test_source_name_travels_with_the_event_into_state():
    clock = SimClock(5)
    engine = Engine(clock, State())
    engine.emit(Thing("a:1", 5), source="espn")
    engine.emit(Timer(5))
    drain(engine)
    st = engine.state
    assert st.inputs == {"espn": (5, Thing("a:1", 5))}
    assert st.source == ""


def test_same_script_twice_gives_identical_call_sequences():
    linker = {"pair": lambda c: "g:1" if c.event_ticker == "E-1" else None}
    script = [
        (0, contract("T-A")), (0, contract("T-B")), (0, contract("T-C", event="E-2")),
        (10, snap("T-A", 10)), (10, snap("T-B", 10, 5_000, 5_300)),
        (20, BookDelta("kalshi", "T-A", "bid", 4_100, 300, 20)), (25, Thing("g:1", 25, 3)),
        (30, Trade("kalshi", "t1", "T-B", 5_300, 100, "bid", 30)), (1_000, Timer(1_000)),
        (1_000, BookDelta("kalshi", "T-A", "ask", 4_200, -1_000, 1_000)), (2_000, Timer(2_000)),
    ]

    def run_once() -> list:
        calls: list = []
        clock = SimClock()
        engine = Engine(clock, State(linkers=linker), models=[FakeModel("mid", calls), FakeModel("other", calls)],
                        router=Step("router", calls), publisher=FakePublisher(calls))
        for t, ev in script:
            clock.advance(t)
            engine.emit(ev)
            drain(engine)
        return calls + sorted(engine.state.forecasts.items())

    first = run_once()
    assert first == run_once()
    # 2 models x 3 book changes with a mid (the last delta empties the ask side: no mid, no forecast)
    assert sum(c[0] == "router" and c[1].startswith("Forecast") for c in first) == 6


# ---- linking ----------------------------------------------------------------------


def test_linker_maps_contracts_to_one_entity_and_entity_events_reach_state():
    pairs = {"pair": lambda c: f"game:{c.event_ticker}" if c.event_ticker.startswith("E-") else None}
    engine = Engine(SimClock(), State(linkers=pairs))
    for c in (contract("T-A"), contract("T-B"), contract("T-C", event="X-9")):
        engine.emit(c)
    engine.emit(Thing("game:E-1", 7, 21))
    drain(engine)
    st = engine.state
    assert st.tickers_of == {"game:E-1": {"T-A", "T-B"}}
    assert st.entity_of == {"T-A": "game:E-1", "T-B": "game:E-1"}
    assert st.entities["game:E-1"] == Thing("game:E-1", 7, 21)
    assert st.data("T-B").summary() == "game:E-1=21" and st.data("T-C") is None
    assert st.keys_of(Thing("game:E-1", 8)) == (None, "game:E-1")
    assert st.keys_of(snap("T-A")) == ("T-A", "game:E-1")

    engine.emit(contract("T-A", event="X-1"))  # relisted under another event: unlinked
    engine.emit(Thing("game:E-1", 5, 99))  # older than what we have: ignored
    drain(engine)
    assert st.tickers_of == {"game:E-1": {"T-B"}} and "T-A" not in st.entity_of
    assert st.entities["game:E-1"].value == 21


def test_a_raising_linker_is_logged_and_leaves_the_contract_unlinked():
    def bad(c):
        raise KeyError(c.ticker)

    pub = FakePublisher()
    engine = Engine(SimClock(), State(linkers={"bad": bad, "ok": lambda c: "e:1"}), publisher=pub)
    engine.emit(contract("T-A"))
    drain(engine)
    logs = [e for e in pub.events if isinstance(e, LogEvent)]
    assert len(logs) == 1 and logs[0].source == "linker:bad" and "T-A" in logs[0].text
    assert engine.state.entity_of == {"T-A": "e:1"}, "the next linker still runs"


# ---- isolation --------------------------------------------------------------------


def test_a_raising_model_is_disabled_for_that_entity_only_and_the_engine_keeps_going():
    calls: list = []
    bad = FakeModel("bad", calls, fail_on=lambda ev: isinstance(ev, Thing) and ev.key == "g:1")
    good = FakeModel("good", calls)
    pub = FakePublisher()
    engine = Engine(SimClock(), State(), models=[bad, good], publisher=pub)
    for ev in (Thing("g:1", 1), Thing("g:2", 2), Thing("g:1", 3), Thing("g:2", 4), snap("T-1")):
        engine.emit(ev)
    drain(engine)
    seen = [c[1] for c in calls if c[0] == "model:bad"]
    assert seen == ["Thing:g:1", "Thing:g:2", "Thing:g:2", "BookSnapshot:T-1"]
    assert [c[1] for c in calls if c[0] == "model:good"].count("Thing:g:1") == 2
    errors = [e for e in pub.events if isinstance(e, LogEvent)]
    assert len(errors) == 1 and errors[0].level == "error" and errors[0].source == "model:bad"
    assert "disabled for g:1" in errors[0].text
    assert engine.disabled("bad") == {"g:1"} and engine.errors == {"model:bad": 1}
    assert engine.state.forecast("T-1", "bad") is not None, "still forecasts what it wasn't disabled for"


def test_a_model_raising_on_a_market_event_is_disabled_for_that_market():
    calls: list = []
    bad = FakeModel("bad", calls, fail_on=lambda ev: getattr(ev, "ticker", "") == "T-1")
    engine = Engine(SimClock(), State(), models=[bad])
    for ev in (snap("T-1"), snap("T-2"), BookDelta("kalshi", "T-1", "bid", 4_000, 5, 0), snap("T-2")):
        engine.emit(ev)
    drain(engine)
    assert [c[1] for c in calls] == ["BookSnapshot:T-1", "BookSnapshot:T-2", "BookSnapshot:T-2"]
    assert engine.disabled("bad") == {"T-1"}


def test_a_model_raising_on_an_event_with_no_market_or_entity_is_disabled_everywhere():
    calls: list = []
    engine = Engine(SimClock(), State(), models=[FakeModel("bad", calls, fail_on=lambda ev: isinstance(ev, Timer))])
    for ev in (Timer(0), snap("T-1"), Thing("g:1", 1)):
        engine.emit(ev)
    drain(engine)
    assert [c[1] for c in calls] == ["Timer"] and engine.disabled("bad") == {"*"}


def test_failing_components_are_logged_and_never_stop_dispatch():
    calls: list = []

    def explode(ev, state, now):
        raise ValueError("nope")

    pub = FakePublisher()
    engine = Engine(SimClock(), State(), router=Step("router", calls, explode),
                    order_manager=Step("order_manager", calls), publisher=pub)
    engine.emit(Timer(0))
    engine.emit(Timer(1))
    drain(engine)
    assert [label(e) for e in pub.events] == [
        "Timer", "LogEvent:router failed on Timer: ValueError: nope",
        "Timer", "LogEvent:router failed on Timer: ValueError: nope",
    ], "a router failing on its own failure log doesn't feed itself more logs"
    assert engine.errors == {"router": 4}
    assert [c[0] for c in calls].count("order_manager") == 4, "later steps still run"


def test_an_event_state_cannot_apply_goes_no_further():
    calls: list = []

    class Brittle(State):
        def apply(self, ev, now_ms, source=""):
            if isinstance(ev, Trade):
                raise RuntimeError("bad trade")
            return super().apply(ev, now_ms, source)

    pub = FakePublisher()
    engine = Engine(SimClock(), Brittle(), router=Step("router", calls), publisher=pub)
    engine.emit(Trade("kalshi", "x", "T-1", 4_100, 100, "bid", 0))
    engine.emit(Timer(0))
    drain(engine)
    assert [c[1] for c in calls] == ["LogEvent:state failed on Trade: RuntimeError: bad trade", "Timer"]
    assert not any(isinstance(e, Trade) for e in pub.events)


def test_a_runaway_chain_of_derived_events_is_cut():
    class Echo:
        name = "echo"

        def wants(self, ev):
            return isinstance(ev, Forecast)

        def on_event(self, ev, state, now_ms):
            return [Forecast("echo", ev.ticker, ev.p, now_ms, now_ms)]

    pub = FakePublisher()
    engine = Engine(SimClock(), State(), models=[Echo()], publisher=pub, max_derived=50)
    engine.emit(Forecast("seed", "T-1", 0.5, 0, 0))
    engine.emit(Timer(0))
    drain(engine)
    assert sum(isinstance(e, Forecast) for e in pub.events) == 51
    assert "more than 50 events derived" in pub.events[-2].text
    assert isinstance(pub.events[-1], Timer)


# ---- live loop, clock, sleep -------------------------------------------------------


def test_run_dispatches_as_events_arrive_and_returns_on_stop():
    calls: list = []

    async def go():
        engine = Engine(LiveClock(), State(), router=Step("router", calls))
        stop = asyncio.Event()
        task = asyncio.create_task(engine.run(stop))
        for i in range(3):
            engine.emit(Timer(i))
            await asyncio.sleep(0.01)
        assert engine.dispatched == 3
        stop.set()
        await asyncio.wait_for(task, 1)

    asyncio.run(go())
    assert len(calls) == 3


def test_sim_clock_never_goes_backwards():
    c = SimClock(100)
    c.advance(100)
    c.advance(250)
    assert c.now_ms() == 250
    with pytest.raises(ValueError, match="backwards"):
        c.advance(249)


def test_live_clock_holds_when_the_system_clock_steps_back():
    times = iter([10.0, 9.0, 10.5])
    c = LiveClock(lambda: next(times))
    assert [c.now_ms(), c.now_ms(), c.now_ms()] == [10_000, 10_000, 10_500]


class FakeRecorder:
    def __init__(self) -> None:
        self.records: list = []

    def record(self, source: str, kind: str, payload: str) -> None:
        self.records.append((source, kind, payload))


def test_a_wall_clock_jump_between_timers_drops_books_before_the_timer_is_dispatched():
    now = [1_000.0]
    calls: list = []
    rec, gaps = FakeRecorder(), []
    engine = Engine(LiveClock(lambda: now[0]), State(), router=Step("router", calls), recorder=rec)
    engine.on_sleep_gap.append(gaps.append)

    def at(t: float, ev) -> None:
        now[0] = t
        engine.emit(ev)
        drain(engine)

    at(1_000.0, Timer(0))
    at(1_000.5, snap("T-1"))
    at(1_001.0, Timer(0))
    assert engine.state.book("T-1") is not None and rec.records == []
    at(1_031.0, Timer(0))  # the laptop slept for half a minute
    assert [c[1] for c in calls[-3:]] == [
        "LogEvent:wall clock jumped 30.0 s between timers (likely a sleep); books re-snapshot", "StreamReset", "Timer"]
    assert engine.state.book("T-1") is None
    assert rec.records == [("engine", "c", '{"event":"sleep_gap","gap_ms":30000}')]
    assert gaps == [30_000]


def test_simulated_time_never_counts_as_sleep():
    clock = SimClock()
    engine = Engine(clock, State(), recorder=(rec := FakeRecorder()))
    for t in (0, 60_000, 600_000):
        clock.advance(t)
        engine.emit(Timer(t))
    drain(engine)
    assert engine.sleep_gap_ms is None and rec.records == []


# ---- observability tap -------------------------------------------------------------


def test_a_stalled_consumer_never_delays_dispatch_and_drops_are_counted():
    stalled = asyncio.Event()  # never set: the consumer is stuck (a viewer's socket that never drains)
    delivered: list = []

    async def deliver(batch):
        delivered.extend(batch)
        await stalled.wait()

    async def go():
        tap = Tap(deliver, capacity=100, batch=10)
        engine = Engine(SimClock(), State(), publisher=tap)
        engine.yield_every = 50  # let the writer take its first batch early
        stop = asyncio.Event()
        writer = asyncio.create_task(tap.run(stop))
        for i in range(1_000):
            engine.emit(Timer(i))
        t0 = time.perf_counter()
        await asyncio.wait_for(engine.dispatch_all(), 2)
        elapsed = time.perf_counter() - t0
        writer.cancel()
        return engine, tap, elapsed

    engine, tap, elapsed = asyncio.run(go())
    assert engine.dispatched == 1_000 and elapsed < 1.0
    assert len(delivered) == 10 and len(tap) == 100
    assert tap.dropped == 1_000 - 100 - 10


def test_tap_delivers_in_order_and_reports_drops():
    batches: list = []

    async def deliver(batch):
        batches.append(batch)

    tap = Tap(deliver, capacity=5, batch=4)
    for i in range(8):
        tap.append(Timer(i), i)
    assert tap.dropped == 3

    async def go():
        stop = asyncio.Event()
        stop.set()  # deliver what's there, then return
        await asyncio.wait_for(tap.run(stop), 1)

    asyncio.run(go())
    flat = [ev for b in batches for _, ev in b]
    assert flat[:5] == [Timer(i) for i in range(5)], "the gap comes after everything that made it in"
    assert isinstance(flat[5], LogEvent) and "3 events dropped" in flat[5].text and len(flat) == 6
    assert [len(b) for b in batches] == [4, 2] and len(tap) == 0


def test_the_drop_warning_marks_where_the_gap_is_even_after_the_ring_refills():
    tap = Tap(None, capacity=3, batch=2)
    for i in range(5):  # 0-2 kept, 3-4 dropped
        tap.append(Timer(i), i)
    first = tap.take()
    tap.append(Timer(5), 5)  # after the gap
    tap.append(Timer(6), 6)
    rest = tap.take() + tap.take()
    labels = [label(ev) if isinstance(ev, LogEvent) else ev.ts_ms for _, ev in first + rest]
    assert labels == [0, 1, 2, "LogEvent:viewer feed fell behind: 2 events dropped", 5, 6]
    assert rest[1][0] == 3, "stamped with the first dropped event's time"


def test_engine_to_hub_through_the_tap():
    hub = FeedHub(proto.Hello("kalshi", "prod", "engine"))
    state = State(linkers={"one": lambda c: "g:1"})
    tap = Tap(to_publisher(Publisher(hub, tickers_of=lambda e: state.tickers_of.get(e, ()))))
    engine = Engine(SimClock(), state, models=[FakeModel("mid", [])], publisher=tap)
    for ev in (contract("T-1"), snap("T-1"), Thing("g:1", 3, 7)):
        engine.emit(ev)

    async def go():
        stop = asyncio.Event()
        writer = asyncio.create_task(tap.run(stop))
        await engine.dispatch_all()
        stop.set()
        await asyncio.wait_for(writer, 1)

    asyncio.run(go())
    assert hub.books.get("kalshi", "T-1").best_bid() == (4_000, 1_000)
    assert hub.forecasts[("mid", "T-1")].p == pytest.approx(0.41)
    assert hub.entities["g:1"].tickers == ["T-1"] and hub.entities["g:1"].summary == "g:1=7"


# ---- state ------------------------------------------------------------------------


def test_market_ids_are_tickers_on_kalshi_and_venue_qualified_elsewhere():
    assert market_id("kalshi", "KXNFLGAME-26OCT18HOUJAC-HOU") == "KXNFLGAME-26OCT18HOUJAC-HOU"
    assert market_id("polymarket", "0xabc") == "polymarket:0xabc"
    for mid in ("KXHIGHNY-26OCT18-B65.5", "polymarket:0xabc"):
        assert market_id(*venue_ticker(mid)) == mid
    st = State()
    st.apply(BookSnapshot("polymarket", "0xabc", ((100, 1),), (), 0), 0)
    st.apply(contract("0xabc", venue="polymarket"), 0)
    assert st.book("polymarket:0xabc") is not None and st.book("0xabc") is None
    assert "polymarket:0xabc" in st.contracts and st.venues() == {"polymarket"}


def test_stream_reset_drops_that_venues_books_until_the_next_snapshot():
    st = State()
    st.apply(snap("T-1"), 0)
    st.apply(BookSnapshot("other", "X", ((100, 1),), (), 0), 0)
    st.apply(StreamReset("kalshi", "gap", 1), 1)
    assert st.book("T-1") is None and st.book("other:X") is not None
    st.apply(BookDelta("kalshi", "T-1", "bid", 4_000, 5, 2), 2)
    assert st.book("T-1") is None, "a delta can't resurrect a dropped book"
    st.apply(snap("T-1", 3), 3)
    assert st.book("T-1").best_ask() == (4_200, 1_000)


def test_advice_is_kept_only_while_unexpired():
    st = State()
    for t in ("T-1", "T-2"):
        st.apply(Contract("kalshi", t, "E", "Title", "", "open", None), 0)
    a = Advice("news", "T-1", 0.6, None, 0.8, 5_000, "why", 1_000)
    st.apply(a, 1_000)
    st.apply(Advice("news", "T-2", 0.4, None, 0.5, 9_000, "why", 1_000), 1_000)
    assert st.advice[("news", "T-1")] == a
    st.apply(Timer(5_000), 5_000)
    assert ("news", "T-1") in st.advice, "actionable through its expiry"
    st.apply(Timer(5_001), 5_001)
    assert list(st.advice) == [("news", "T-2")]
    st.apply(Advice("news", "T-2", 0.4, None, 0.5, 5_500, "withdrawn", 6_000), 6_000)
    assert st.advice == {}, "advice that has already lapsed replaces nothing with nothing"


def test_schedules_are_sorted_deduplicated_and_pruned_after_a_grace_period():
    st = State(keep_past_schedules_ms=60_000)
    for s in (Schedule("cpi", "release", 300_000, "CPI", 0), Schedule("cpi", "report", 100_000, "jobs", 0),
              Schedule("cpi", "release", 300_000, "CPI 08:30 ET", 1)):
        st.apply(s, 1)
    assert [(s.kind, s.detail) for s in st.schedules["cpi"]] == [("report", "jobs"), ("release", "CPI 08:30 ET")]
    st.apply(Timer(160_000), 160_000)
    assert [s.kind for s in st.schedules["cpi"]] == ["report", "release"], "just passed: risk windows still see it"
    st.apply(Timer(160_001), 160_001)
    assert [s.kind for s in st.schedules["cpi"]] == ["release"]
    st.apply(Timer(360_001), 360_001)
    assert st.schedules == {}


def test_trades_forecasts_and_targets_are_addressable():
    st = State(linkers={"one": lambda c: "g:1"})
    st.apply(contract("T-1"), 0)
    t = Trade("kalshi", "x", "T-1", 4_100, 100, "bid", 3)
    st.apply(t, 3)
    st.apply(Forecast("m", "T-1", 0.4, 3, 2), 3)
    assert st.last_trade["T-1"] == t and st.forecast("T-1", "m").p == 0.4 and st.forecast("T-1", "x") is None
    assert st.keys_of(Advice("a", "T-1", 0.5, None, 1, 9, "", 0)) == ("T-1", "g:1")
    assert st.keys_of(Advice("a", "g:1", None, "buy", 1, 9, "", 0)) == (None, "g:1")
    assert st.keys_of(Timer(0)) == (None, None)


# ---- plugins ----------------------------------------------------------------------


@pytest.fixture
def registries():
    plugins.load_builtin_plugins()  # first: a built-in imported during the test would never register again
    saved = {k: dict(v) for k, v in plugins._REGISTRIES.items()}
    yield
    for k, v in plugins._REGISTRIES.items():
        v.clear()
        v.update(saved[k])


def test_duplicate_kinds_are_errors_but_the_same_definition_may_reload(registries):
    @plugins.model("e1_dupe")
    class A:
        pass

    with pytest.raises(plugins.PluginError, match="duplicate model kind 'e1_dupe'"):
        @plugins.model("e1_dupe")
        class B:
            pass

    def make():
        @plugins.strategy("e1_reload")
        class S:
            pass

        return S

    first, second = make(), make()
    assert first is not second and plugins.STRATEGIES["e1_reload"] is second
    assert plugins.MODELS["e1_dupe"] is A


def test_kinds_are_per_registry(registries):
    @plugins.source("e1_same")
    class Src:
        pass

    @plugins.linker("e1_same")
    def link(c):
        return None

    assert plugins.SOURCES["e1_same"] is Src and plugins.LINKERS["e1_same"] is link


class Leg(msgspec.Struct, frozen=True):
    ticker: str
    weight: float = 1.0


def test_build_params_converts_and_rejects_unknown_keys_at_any_depth(registries):
    @plugins.strategy("e1_params")
    class Strat:
        class Params(msgspec.Struct, frozen=True):
            min_edge: float = 0.04
            size: str = "5"
            first: Leg | None = None
            legs: list[Leg] = []
            by_name: dict[str, Leg] = {}

    p = plugins.build_params(Strat, {"min_edge": 0, "legs": [{"ticker": "T-1"}], "by_name": {"a": {"ticker": "B"}}})
    assert p == Strat.Params(min_edge=0.0, legs=[Leg("T-1")], by_name={"a": Leg("B")})
    assert plugins.build_params(Strat, None) == Strat.Params()
    with pytest.raises(plugins.PluginError, match=r"strategy 'e1_params': unknown params key 'min_egde' \(known: "):
        plugins.build_params(Strat, {"min_egde": 0.05})
    with pytest.raises(plugins.PluginError, match=r"unknown params.first key 'wieght'"):
        plugins.build_params(Strat, {"first": {"ticker": "T", "wieght": 2}})
    with pytest.raises(plugins.PluginError, match=r"unknown params.legs\[1\] key 'x'"):
        plugins.build_params(Strat, {"legs": [{"ticker": "T"}, {"ticker": "U", "x": 1}]})
    with pytest.raises(plugins.PluginError, match=r"unknown params.by_name.a key 'x'"):
        plugins.build_params(Strat, {"by_name": {"a": {"ticker": "T", "x": 1}}})
    with pytest.raises(plugins.PluginError, match=r"strategy 'e1_params': bad params: Expected `str`, got `int`"):
        plugins.build_params(Strat, {"size": 5})


def test_a_plugin_without_params_accepts_only_an_empty_table(registries):
    @plugins.strategy("e1_bare")
    class Bare:
        pass

    assert plugins.build_params(Bare, {}) is None
    with pytest.raises(plugins.PluginError, match="strategy 'e1_bare' takes no params, got size"):
        plugins.build_params(Bare, {"size": "1"})


def _package(root, name: str, files: dict[str, str]) -> None:
    for rel, body in files.items():
        path = root / name / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)


def test_load_builtin_plugins_imports_plugin_and_linker_modules(tmp_path, monkeypatch, registries):
    _package(tmp_path, "e1plug", {
        "__init__.py": "",
        "mine.py": "from pmx.plugins import strategy\n@strategy('e1_loaded')\nclass S: pass\n",
        "sub/__init__.py": "",
        "sub/deep.py": "from pmx.plugins import model\n@model('e1_deep')\nclass M: pass\n",
    })
    _package(tmp_path, "e1domain", {
        "__init__.py": "", "wx/__init__.py": "",
        "wx/linker.py": "from pmx.plugins import linker\n@linker('e1_wx')\ndef station(c): return None\n",
        "wx/research.py": "raise RuntimeError('research code is never imported by the engine')\n",
    })
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(plugins, "PLUGIN_PACKAGES", ("e1plug", "pmx.not_built_yet", "e1_missing_root"))
    monkeypatch.setattr(plugins, "DOMAIN_PACKAGES", ("e1domain",))
    plugins.load_builtin_plugins()
    assert "e1_loaded" in plugins.STRATEGIES and "e1_deep" in plugins.MODELS and "e1_wx" in plugins.LINKERS
    for mod in [m for m in sys.modules if m.startswith(("e1plug", "e1domain"))]:
        monkeypatch.delitem(sys.modules, mod)


def test_a_broken_plugin_module_stops_loading(tmp_path, monkeypatch, registries):
    _package(tmp_path, "e1broken", {"__init__.py": "", "bad.py": "import e1_no_such_dependency\n"})
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(plugins, "PLUGIN_PACKAGES", ("e1broken",))
    monkeypatch.setattr(plugins, "DOMAIN_PACKAGES", ())
    with pytest.raises(ModuleNotFoundError, match="e1_no_such_dependency"):
        plugins.load_builtin_plugins()
    for mod in [m for m in sys.modules if m.startswith("e1broken")]:
        monkeypatch.delitem(sys.modules, mod)


def test_load_builtin_plugins_on_the_real_tree(registries):
    plugins.load_builtin_plugins()  # missing domain packages are skipped; nothing may fail
    assert {"noop", "watch", "scripted"} <= set(plugins.STRATEGIES)


def test_a_broken_publisher_or_recorder_never_stops_the_engine():
    class Broken:
        def append(self, ev, now_ms):
            raise OSError("feed gone")

        def record(self, source, kind, payload):
            raise OSError("disk full")

    now = [100.0]
    calls: list = []
    engine = Engine(LiveClock(lambda: now[0]), State(), router=Step("router", calls), publisher=Broken(),
                    recorder=Broken())
    for t in (100.0, 160.0, 161.0):  # the second Timer is a sleep gap: the recorder raises
        now[0] = t
        engine.emit(Timer(0))
        drain(engine)
    assert [c[1] for c in calls].count("Timer") == 3
    assert engine.errors["publisher"] == engine.dispatched and "router" not in engine.errors
