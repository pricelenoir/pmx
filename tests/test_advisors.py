"""Advisors: validation, isolation and budgets in the worker, the view, the engine's side, replay."""

from __future__ import annotations

import ast
import asyncio
import pathlib
import time
from datetime import date
from pathlib import Path

import msgspec
import pytest

from pmx import plugins
from pmx.advisors.base import AdvisorEnv, AdvisorParams, AdvisorSpec, Paused, backtest_refusal
from pmx.advisors.view import EntityView, FeedView
from pmx.advisors.worker import AdviceLine, AdvisorLink, AdvisorRunner, LogLine
from pmx.core.types import BookSnapshot, Contract, Trade
from pmx.engine.clock import SimClock
from pmx.engine.engine import Engine
from pmx.engine.events import Advice, LogEvent, Timer
from pmx.engine.slots import MarketSelector, Router, StrategySpec
from pmx.engine.state import State
from pmx.feed import proto
from pmx.plugins import advisor, strategy
from pmx.strategies.base import Strategy

T, T2 = "SER-EV1-A", "SER-EV1-B"


def contract(ticker: str = T, status: str = "open") -> Contract:
    return Contract("kalshi", ticker, "SER-EV1", "Title", "Sub", status, None, series_ticker="SER")


class Collect:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.events.append(ev)


@strategy("a1_listen")
class Listen(Strategy):
    def __init__(self, params) -> None:
        super().__init__(params)
        self.heard: list = []

    def on_advice(self, a, m) -> None:
        self.heard.append((a.advisor, a.target, a.p, m.advice() and m.advice().p))


def rig():
    router = Router([StrategySpec(id="l", kind="a1_listen", advisors=["news"], markets=MarketSelector(tickers=[T]))])
    pub = Collect()
    engine = Engine(SimClock(0), State(), router=router, publisher=pub)
    engine.dispatch(contract(), 0)
    return engine, router, pub


def advice(target: str = T, p: float | None = 0.6, *, direction=None, confidence=0.8, expires_ms=10_000,
           rationale="why", ts_ms=1_000, name="news") -> Advice:
    return Advice(name, target, p, direction, confidence, expires_ms, rationale, ts_ms)


# ---- the engine validates advice as it dispatches it ----------------------------------------------------------


def test_malformed_advice_is_dropped_with_a_reason_and_goes_nowhere():
    engine, router, pub = rig()
    slot = router.slots["l/" + T]
    bad = [advice("NOPE"), advice(p=1.5), advice(p=float("nan")), advice(p=None),
           advice(confidence=1.2), advice(rationale="x" * 201), advice(p=None, direction="short")]
    for a in bad:
        engine.dispatch(a, 1_000)
    logs = [e.text for e in pub.events if isinstance(e, LogEvent)]
    assert logs == [
        "advice for NOPE dropped: unknown target 'NOPE'",
        "advice for SER-EV1-A dropped: p 1.5 isn't in [0, 1]",
        "advice for SER-EV1-A dropped: p nan isn't in [0, 1]",
        "advice for SER-EV1-A dropped: neither p nor a direction",
        "advice for SER-EV1-A dropped: confidence 1.2 isn't in [0, 1]",
        "advice for SER-EV1-A dropped: rationale is 201 characters (at most 200)",
        "advice for SER-EV1-A dropped: direction 'short' isn't buy, sell, or flat",
    ]
    assert {e.source for e in pub.events if isinstance(e, LogEvent)} == {"advisor:news"}
    assert not any(isinstance(e, Advice) for e in pub.events), "refused advice is never published"
    assert slot.instance.heard == [] and engine.state.advice == {} and engine.errors == {}
    engine.dispatch(advice(rationale="x" * 200), 1_000)
    assert slot.instance.heard == [("news", T, 0.6, 0.6)], "200 characters is fine"


def test_lapsed_advice_never_reaches_on_advice_and_m_advice_goes_none():
    engine, router, pub = rig()
    slot = router.slots["l/" + T]
    engine.dispatch(advice(expires_ms=999), 1_000)  # already lapsed on arrival
    assert slot.instance.heard == []
    engine.dispatch(advice(expires_ms=5_000), 1_000)
    assert slot.instance.heard == [("news", T, 0.6, 0.6)]
    engine.dispatch(Timer(5_000), 5_000)
    assert slot.ctx.advice() is not None, "actionable through its expiry"
    engine.dispatch(Timer(5_001), 5_001)
    assert slot.ctx.advice() is None and len(slot.instance.heard) == 1
    engine.dispatch(advice(name="other", expires_ms=9_000), 6_000)
    assert len(slot.instance.heard) == 1, "advice from an advisor the strategy doesn't list"


# ---- the worker's runner ---------------------------------------------------------------------------------------


@advisor("a1_broken")
class Broken:
    starts: list[float] = []

    class Params(AdvisorParams, frozen=True, kw_only=True):
        pass

    def __init__(self, name, params, env) -> None:
        self.name = name

    async def run(self, view, sink, stop) -> None:
        Broken.starts.append(time.monotonic())
        raise RuntimeError("model API down")


@advisor("a1_chatty")
class Chatty:
    class Params(AdvisorParams, frozen=True, kw_only=True):
        every_s: float = 0.02
        sleep_s: float = 0.0  # per "call": a slow agent

    def __init__(self, name, params, env) -> None:
        self.name, self.p, self.env = name, params, env

    async def run(self, view, sink, stop) -> None:
        while not stop.is_set():
            if self.p.sleep_s:
                await self.env.call(asyncio.sleep(self.p.sleep_s), what="think")
            now = self.env.now_ms()
            sink.advise(Advice("someone-else", T, 0.55, None, 0.9, now + 1_000, "chatty", now))
            await asyncio.sleep(self.p.every_s)


def test_an_advisor_that_raises_is_restarted_with_back_off_and_the_others_carry_on():
    Broken.starts.clear()
    out: list = []

    async def run():
        runner = AdvisorRunner([AdvisorSpec("bad", "a1_broken"), AdvisorSpec("good", "a1_chatty")], FeedView(),
                               out.append, backoff=0.05, max_backoff=0.2)
        stop = asyncio.Event()
        task = asyncio.ensure_future(runner.run(stop))
        await asyncio.sleep(0.8)
        stop.set()
        await asyncio.wait_for(task, 2)
        return runner

    runner = asyncio.run(run())
    gaps = [b - a for a, b in zip(Broken.starts, Broken.starts[1:], strict=False)]
    assert runner.restarts["bad"] >= 4 and runner.restarts["good"] == 0
    assert 0.04 < gaps[0] < 0.09 and gaps[1] > 1.6 * gaps[0] and max(gaps) < 0.3, gaps  # 0.05, 0.1, 0.2, 0.2…
    errors = [x for x in out if isinstance(x, LogLine) and x.advisor == "bad"]
    assert errors[0].level == "error" and "raised RuntimeError: model API down; restarting in 0.05s" in errors[0].text
    good = [x.advice for x in out if isinstance(x, AdviceLine)]
    assert len(good) > 20 and {a.advisor for a in good} == {"good"}, "an advisor speaks only for itself"


def test_budgets_time_out_calls_and_pause_until_the_window_resets():
    notes: list = []

    async def run():
        params = plugins.build_params(Chatty, {"timeout_s": 0.05, "max_calls_per_min": 2, "max_cost_per_day": "1.00"})
        env = AdvisorEnv("x", params, note=lambda level, text: notes.append((level, text)))
        env.window_s = 0.3
        with pytest.raises(TimeoutError):
            await env.call(asyncio.sleep(1), what="slow call")
        await env.call(asyncio.sleep(0))
        t = time.monotonic()
        await env.call(asyncio.sleep(0))  # third in the window: waits it out
        waited = time.monotonic() - t
        env.charge("0.60")
        env.charge(400_000)
        assert env.cost == 1_000_000 and env.paused_until is not None
        env.stop.set()
        with pytest.raises(Paused):
            await env.call(asyncio.sleep(0))
        return waited

    waited = asyncio.run(run())
    assert 0.2 < waited < 0.5
    texts = [t for _, t in notes]
    assert texts[0] == "slow call timed out after 0.05s"
    assert texts[1].startswith("over budget: 2 calls in the last minute (max_calls_per_min 2); paused for 0.")
    assert texts[2] == "budget window reset: running again"
    assert texts[3].startswith("over budget: spent $1.00 today (max_cost_per_day $1.00); paused for")


# ---- the view -----------------------------------------------------------------------------------------------------


def test_feed_view_is_a_read_only_state_view_built_from_feed_messages():
    v = FeedView()
    for line in (Path(__file__).parent / "fixtures" / "feed" / "v3.ndjson").read_bytes().splitlines():
        if b'"t":"reset"' not in line:
            v.apply(proto.decoder.decode(line))
    hou, b68 = "DEMO-NFL-26OCT18HOUJAC-HOU", "DEMO-HIGHNY-26OCT18-B68"
    assert v.tickers() == sorted([hou, "DEMO-NFL-26OCT18HOUJAC-JAC", b68])
    assert v.tickers(series="DEMO-HIGHNY") == [b68]
    assert v.book(hou).best_ask() == (5_700, 300) and v.book(b68) is None, "conflated books; stale is gone"
    assert v.forecast(hou, "fair").p == 0.582 and v.forecast(hou, "nope") is None
    e = v.data("demo:nfl:HOU-JAC")
    assert isinstance(e, EntityView) and e.entity == "demo:nfl:HOU-JAC"
    assert e.summary() == "Q3 4:12 HOU 17-14 JAC" and ("score", "HOU 17 - 14 JAC") in e.display_fields()
    assert v.position("mm/" + hou) == 500 and v.position("nope") == 0
    v.apply(proto.Hello("kalshi", "prod", "engine"))
    assert v.tickers() == [] and v.book(hou) is None


# ---- the reference advisor ----------------------------------------------------------------------------------


def test_scripted_advice_follows_its_schedule_and_fans_out_by_series():
    from pmx.advisors.reference.scripted_advice import ScriptedAdvice

    out: list = []
    view = FeedView()
    for t in (T, T2):
        view.apply(proto.ContractMsg(t, "SER-EV1", "SER", "Title", "Sub", "open", None, None, None))
    steps = [{"at": "+0s", "target": T, "p": 0.6, "expires": "2s", "rationale": "first"},
             {"at": "+100ms", "series": "SER", "direction": "sell", "confidence": 0.5}]

    async def run():
        runner = AdvisorRunner([AdvisorSpec("s", "scripted_advice", {"steps": steps})], view, out.append)
        await asyncio.wait_for(runner.run(asyncio.Event()), 2)  # a script without `loop` finishes

    t0 = time.time() * 1000
    asyncio.run(run())
    sent = [x.advice for x in out if isinstance(x, AdviceLine)]
    assert [(a.target, a.p, a.direction, a.rationale) for a in sent] == [
        (T, 0.6, None, "first"), (T, None, "sell", "scripted"), (T2, None, "sell", "scripted")]
    assert sent[0].expires_ms - sent[0].ts_ms == 2_000 and sent[1].ts_ms - t0 >= 90
    assert any(isinstance(x, LogLine) and x.text == "finished" for x in out)
    for bad, why in (({"at": "soon", "target": T, "p": 0.5}, "expected a span"),
                     ({"at": "+1s", "p": 0.5}, "give `target` or `series`"),
                     ({"at": "+1s", "target": T}, "p and/or direction")):
        with pytest.raises(plugins.PluginError, match=why):
            plugins.build_params(ScriptedAdvice, {"steps": [bad]})


def test_only_deterministic_advisors_trained_before_the_window_run_in_backtests():
    from pmx.advisors.reference.scripted_advice import ScriptedAdvice

    class Hosted:
        pass

    class Local:
        deterministic = True
        trained_until = "2026-01-31"

    class Unsure:
        deterministic = True

    assert backtest_refusal(ScriptedAdvice, date(2025, 9, 1)) is None
    assert "isn't deterministic: backtests replay its recorded advice only" in \
        backtest_refusal(Hosted, date(2026, 3, 1))
    assert "starts 2026-01-15, not after advisor Local's trained_until 2026-01-31" in \
        backtest_refusal(Local, date(2026, 1, 15))
    assert backtest_refusal(Local, date(2026, 2, 1)) is None
    assert "declares no trained_until" in backtest_refusal(Unsure, date(2026, 3, 1))


# ---- advisors can't place orders --------------------------------------------------------------------------------

_ORDERISH = ("order", "place", "take", "quote", "cancel", "broker", "intent")


def test_no_code_path_lets_an_advisor_place_an_order():
    root = pathlib.Path(plugins.__file__).parent / "advisors"
    problems = []
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                mods = [node.module or ""]
            for m in mods:
                if m.startswith(("pmx.execution", "pmx.strategies", "pmx.connectors", "pmx.ledger")):
                    problems.append(f"{path.name} imports {m}")
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
                task_cancel = name == "cancel" and not node.args  # asyncio's; an order cancel names its order
                if name in ("place", "place_now", "cancel", "cancel_now", "take", "quote", "cancel_all", "target") \
                        and not task_cancel:
                    problems.append(f"{path.name}:{node.lineno} calls {name}()")
    assert problems == []
    # What an advisor is handed has nothing to trade with.
    from pmx.advisors.worker import _Sink

    for obj in (AdvisorEnv("x"), FeedView(), _Sink("x", lambda line: None)):
        public = [a for a in dir(obj) if not a.startswith("_")]
        assert not [a for a in public if any(w in a.lower() for w in _ORDERISH)], (type(obj).__name__, public)


def test_the_engine_side_accepts_only_advice_and_logs_from_the_worker():
    class Sink:
        def __init__(self) -> None:
            self.emitted, self.recorded = [], []

        def emit(self, ev) -> None:
            self.emitted.append(ev)

        def record(self, source, kind, payload) -> None:
            self.recorded.append((source, kind, payload))

    sink = Sink()
    link = AdvisorLink([AdvisorSpec("news", "scripted_advice", {})], sink, Path("f.sock"), clock=lambda: 7)
    a = advice()
    link.handle(msgspec.json.encode(AdviceLine(a)))
    link.handle(b'{"t":"order","ticker":"SER-EV1-A","side":"bid","price":9900,"qty":10000}')
    link.handle(b'{"t":"log","advisor":"news","level":"warn","text":"rate limited"}')
    link.handle(b"not json")
    assert sink.recorded == [("advisors", "e", '{"type":"Advice","event":' + msgspec.json.encode(a).decode() + "}")]
    assert sink.emitted[0] == a and link.received == 1 and link.refused == 2
    assert [(e.level, e.source) for e in sink.emitted[1:]] == [("warn", "advisors"), ("warn", "advisor:news"),
                                                              ("warn", "advisors")]
    assert "dropped a line from the advisor worker" in sink.emitted[1].text


# ---- end to end: the worker process, and replay ------------------------------------------------------------


def test_the_worker_process_feeds_advice_into_the_engine_and_is_restarted_if_it_dies(tmp_path, monkeypatch):
    import os
    import signal

    from pmx.engine.clock import LiveClock
    from pmx.feed.hub import FeedHub
    from pmx.recorder.rawlog import decode_event, iter_records, log_files
    from pmx.recorder.sink import RecordingSink

    monkeypatch.chdir(tmp_path)  # short relative socket paths; the worker inherits the cwd

    async def run():
        hub = FeedHub(proto.Hello("kalshi", "test", "engine"))
        hub.on_event(contract())
        server = await hub.serve(Path("f.sock"))
        rec = RecordingSink(tmp_path / "raw")
        engine = Engine(LiveClock(), State(), recorder=rec, publisher=(pub := Collect()))
        engine.dispatch(contract(), engine.clock.now_ms())
        specs = [AdvisorSpec("script", "scripted_advice", {"steps": [
                     {"at": "+0s", "series": "SER", "p": 0.62, "expires": "1m", "rationale": "from the view"}],
                     "loop": False}),
                 AdvisorSpec("slow", "scripted_advice", {"steps": [{"at": "+0s", "target": T, "p": 0.5}],
                                                         "think_s": 5.0, "loop": True}),  # 5 s per call
                 AdvisorSpec("bad", "scripted_advice", {"steps": [{"at": "+0s", "target": T, "p": 0.4}],
                                                        "fail": "model API down"})]
        link = AdvisorLink(specs, engine.source(AdvisorLink.SOURCE), Path("f.sock"), clock=engine.clock.now_ms)
        stop = asyncio.Event()
        tasks = [asyncio.ensure_future(engine.run(stop)), asyncio.ensure_future(link.run(stop))]
        try:
            async with asyncio.timeout(20):
                while ("script", T) not in engine.state.advice:
                    await asyncio.sleep(0.02)
            t = time.perf_counter()
            for i in range(2_000):  # the engine is never held up by advisors thinking or failing
                engine.dispatch(Trade("kalshi", f"x{i}", T, 5_000, 100, "bid", 0), engine.clock.now_ms())
            busy = (time.perf_counter() - t) / 2_000
            pid = link.proc.pid
            os.kill(pid, signal.SIGKILL)
            async with asyncio.timeout(10):
                while link.restarts == 0 or link.proc.pid == pid:
                    await asyncio.sleep(0.02)
        finally:
            stop.set()
            await asyncio.wait_for(asyncio.gather(*tasks), 15)
            server.close()
            rec.close()
        return engine, link, pub, busy

    engine, link, pub, busy = asyncio.run(run())
    a = engine.state.advice[("script", T)]
    assert (a.p, a.rationale, a.expires_ms - a.ts_ms) == (0.62, "from the view", 60_000)
    assert busy < 200e-6
    recorded = [decode_event(r[2]) for r in iter_records(log_files(tmp_path / "raw", "advisors")) if r[1] == "e"]
    assert recorded and recorded[0] == a, "advice is recorded raw before it's queued"
    logs = [(e.source, e.text) for e in pub.events if isinstance(e, LogEvent)]
    assert any(s == "advisor:bad" and "raised RuntimeError: model API down" in t for s, t in logs)
    assert any(s == "advisors" and "advisor worker exited (-9); restarting" in t for s, t in logs)
    assert link.proc.returncode == 0, "a clean stop: the worker sees EOF and exits"


@strategy("a1_follow")
class Follow(Strategy):
    """Takes 1 contract toward fresh advice: buys when its p is above the ask, sells when below the bid."""

    def on_advice(self, a, m) -> None:
        book = m.book
        if a.p is None or book is None:
            return
        fair, ask, bid = round(a.p * 10_000), book.best_ask(), book.best_bid()
        if ask is not None and fair > ask[0] and m.position < 300:
            m.take("bid", ask[0], 100)
        elif bid is not None and fair < bid[0] and m.position > -300:
            m.take("ask", bid[0], 100)


def _paper_session(recorder=None):
    from pmx.execution.orders import OrderManager
    from pmx.execution.paper import PaperBroker
    from pmx.execution.risk import RiskManager
    from pmx.ledger.db import Ledger

    router = Router([StrategySpec(id="f", kind="a1_follow", advisors=["news"], markets=MarketSelector(tickers=[T]))])
    ledger, paper = Ledger(":memory:", env="demo"), PaperBroker()
    om = OrderManager(intents=router.take_intents, slots=router.slots, brokers={"paper": paper}, ledger=ledger,
                      instance="t")
    router.orders = om
    om.risk = risk = RiskManager(positions=om, balance=om, slots=router.slots)
    risk.on_cancel.append(om.cancel_scope)
    clock = SimClock(0)
    engine = Engine(clock, State(), broker=paper, router=router, order_manager=om, recorder=recorder)
    return clock, engine, ledger


def _rows(ledger) -> dict[str, list]:
    tables = ("orders", "fills", "settled")
    return {t: ledger.db.execute(f"SELECT * FROM {t} ORDER BY rowid").fetchall() for t in tables}


def test_a_recorded_paper_session_with_advice_replays_to_an_identical_ledger(tmp_path):
    from pmx.recorder.rawlog import encode_event
    from pmx.recorder.replay import replay_merged
    from pmx.recorder.sink import RecordingSink

    holder: list = []
    rec = RecordingSink(tmp_path, wall_ns=lambda: holder[0].now_ms() * 1_000_000)  # one clock for both
    clock, engine, ledger = _paper_session(rec)
    holder.append(clock)
    market = engine.source("market")
    link = AdvisorLink([], engine.source(AdvisorLink.SOURCE), Path("unused.sock"), clock=clock.now_ms)

    def venue(ev) -> None:  # a source: raw first, then into the engine
        market.record("market", "e", encode_event(ev))
        market.emit(ev)

    def says(**kw) -> None:  # what the worker would send over its socket
        link.handle(msgspec.json.encode(AdviceLine(advice(**kw))))

    book = lambda bid, ask: BookSnapshot("kalshi", T, ((bid, 1_000),), ((ask, 1_000),), clock.now_ms())  # noqa: E731
    script = [
        (0, lambda: venue(contract())), (0, lambda: venue(book(4_000, 4_200))),
        (1_000, lambda: says(p=0.70, expires_ms=30_000, ts_ms=1_000)),  # above the 42¢ ask: buy 1
        (2_000, lambda: venue(book(4_500, 4_700))),
        (3_000, lambda: says(p=0.30, expires_ms=30_000, ts_ms=3_000)),  # below the 45¢ bid: sell 1
        (4_000, lambda: says(p=0.90, expires_ms=3_999, ts_ms=4_000)),  # lapsed on arrival: nothing
        (5_000, lambda: says(p=2.0, ts_ms=5_000)),  # malformed: dropped
        (6_000, lambda: says(p=0.80, ts_ms=6_000, expires_ms=30_000)),  # buy again
        (7_000, lambda: venue(msgspec.structs.replace(contract(status="settled"), result="yes", settlement=10_000))),
    ]

    async def live():
        for at, act in script:
            clock.advance(at)
            act()
            await engine.dispatch_all()

    asyncio.run(live())
    rec.close()
    live_rows = _rows(ledger)
    assert len(live_rows["fills"]) == 3 and len(live_rows["settled"]) == 1, "the advice traded, and it settled"

    clock2, engine2, ledger2 = _paper_session()
    for ns, source, ev in replay_merged(tmp_path, {"market": None, "advisors": None}):
        clock2.advance(ns // 1_000_000)
        engine2.dispatch(ev, ns // 1_000_000, source)
    assert _rows(ledger2) == live_rows
