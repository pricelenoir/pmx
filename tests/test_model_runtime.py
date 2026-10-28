from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import msgspec
import pytest

from pmx.core.types import BookDelta, BookSnapshot, Contract, Trade
from pmx.engine.clock import LiveClock, SimClock
from pmx.engine.engine import Engine
from pmx.engine.events import Forecast, LogEvent
from pmx.engine.state import State
from pmx.models.base import forecast
from pmx.models.espn_wp import EspnWp
from pmx.models.market_mid import MarketMid
from pmx.models.runtime import Delayed, ModelSpec, ProcessModel, build_model
from pmx.plugins import LINKERS, PluginError, model
from pmx.recorder.rawlog import decode_event
from pmx.sports.nfl import espn
from pmx.sports.nfl.linker import nfl_game
from pmx.sports.nfl.state import GameState

DEN, SEA = "KXNFLGAME-26OCT15SEADEN-DEN", "KXNFLGAME-26OCT15SEADEN-SEA"
FINAL = Path(__file__).parent / "fixtures" / "espn" / "summary_final_401872995.json"


def contract(ticker: str, venue: str = "kalshi") -> Contract:
    return Contract(venue, ticker, ticker.rsplit("-", 1)[0], "SEA at DEN", ticker[-3:], "open", None)


def snap(ticker: str, bid: int, ask: int, ts: int = 0) -> BookSnapshot:
    return BookSnapshot("kalshi", ticker, ((bid, 1_000),), ((ask, 1_000),), ts)


class Seen:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.events.append(ev)

    def forecasts(self, name: str | None = None) -> list[Forecast]:
        return [e for e in self.events if type(e) is Forecast and (name is None or e.model == name)]


def engine_with(*models, clock=None) -> tuple[Engine, Seen]:
    seen = Seen()
    return Engine(clock or SimClock(0), State({"nfl_game": nfl_game}), models=models, publisher=seen), seen


# ---- linking -------------------------------------------------------------------------------------------


def test_linker_gives_the_game_states_own_entity_key():
    state = espn.parse_summary(json.loads(FINAL.read_text()), now_ms=0)  # kicked off 2026-10-16T00:15Z
    assert state.date == "2026-10-15" and state.entity == "nfl:2026-10-15:DEN-SEA"
    assert nfl_game(contract(DEN)) == nfl_game(contract(SEA)) == state.entity  # the ticker's local date
    assert nfl_game(contract("KXNFLGAME-26OCT18HOUJAC-JAC")) == "nfl:2026-10-18:HOU-JAX"
    assert nfl_game(contract("KXHIGHNY-26OCT18-B60")) is None
    assert nfl_game(contract(DEN, venue="polymarket")) is None
    assert LINKERS["nfl_game"] is nfl_game


# ---- the pass-through models -------------------------------------------------------------------------


def test_two_models_run_together_with_forecasts_keyed_by_instance():
    engine, seen = engine_with(MarketMid("mid"), EspnWp("espn"))
    for t in (DEN, SEA):
        engine.dispatch(contract(t), 0)
    engine.dispatch(snap(DEN, 4_000, 4_200), 1)
    game = GameState("401872995", 2, "2026-10-15", "DEN", "SEA", "in", 2, 300, 7, 3, espn_home_wp=0.7)
    engine.dispatch(game, 2)
    assert [(f.model, f.ticker, round(f.p, 4)) for f in seen.forecasts()] == [
        ("mid", DEN, 0.41), ("espn", DEN, 0.7), ("espn", SEA, 0.3)]
    assert engine.state.forecast(DEN, "mid").p == pytest.approx(0.41)
    assert engine.state.forecast(DEN, "espn").version == "espn_wp/1"


def test_two_instances_of_one_kind_with_their_own_params_and_throttling():
    fine, coarse = MarketMid("fine", MarketMid.Params(min_change=0.001)), MarketMid("coarse", MarketMid.Params(0.05))
    engine, seen = engine_with(fine, coarse)
    engine.dispatch(contract(DEN), 0)
    engine.dispatch(snap(DEN, 4_000, 4_200), 1)  # 0.41
    for i, bid in enumerate((4_002, 4_020, 4_100)):  # mids 0.4101 (too small a move for either), 0.411, 0.415
        engine.dispatch(BookDelta("kalshi", DEN, "bid", bid, 100, i), 2 + i)
    engine.dispatch(snap(DEN, 4_600, 4_800), 9)  # 0.47
    assert [round(f.p, 4) for f in seen.forecasts("fine")] == [0.41, 0.411, 0.415, 0.47]
    assert [round(f.p, 4) for f in seen.forecasts("coarse")] == [0.41, 0.47]


def test_espn_wp_final_states_settle_and_a_tie_pays_half():
    engine, seen = engine_with(EspnWp("espn"))
    for t in (DEN, SEA):
        engine.dispatch(contract(t), 0)
    base = GameState("g", 1, "2026-10-15", "DEN", "SEA", "post", 5, 0, 17, 14, winner="home")
    engine.dispatch(base, 1)
    engine.dispatch(msgspec.structs.replace(base, home_score=14, winner=None, ts_ms=2), 2)
    engine.dispatch(msgspec.structs.replace(base, status="in", espn_home_wp=None, ts_ms=3), 3)  # nothing to say
    assert [(f.ticker[-3:], f.p) for f in seen.forecasts()] == [("DEN", 1.0), ("SEA", 0.0), ("DEN", 0.5),
                                                                ("SEA", 0.5)]


def test_a_raising_model_is_isolated_from_the_other():
    class Broken:
        name = "broken"

        def wants(self, ev):
            return isinstance(ev, BookSnapshot)

        def on_event(self, ev, state, now_ms):
            raise RuntimeError("bad model")

    engine, seen = engine_with(Broken(), MarketMid("mid"))
    engine.dispatch(contract(DEN), 0)
    engine.dispatch(snap(DEN, 4_000, 4_200), 1)
    engine.dispatch(snap(DEN, 4_400, 4_600), 2)
    assert [round(f.p, 2) for f in seen.forecasts("mid")] == [0.41, 0.45]
    assert engine.disabled("broken") == {DEN}
    assert any(type(e) is LogEvent and "bad model" in e.text for e in seen.events)


def test_forecast_helper_clamps():
    assert forecast("m", "T", 1.2, 5, 4).p == 1.0 and forecast("m", "T", -0.1, 5, 4).p == 0.0
    assert forecast("m", "T", 0.25, 5, 4, version="v1") == Forecast("m", "T", 0.25, 5, 4, {}, "v1")


# ---- replay check (acceptance) ----------------------------------------------------------------------------


def test_replayed_game_drives_espn_wp_to_the_result_while_market_mid_follows_the_books():
    final = json.loads(FINAL.read_text())
    states = espn.summary_states(final) + [espn.parse_summary(final, 0)]
    engine, seen = engine_with(MarketMid("mid"), EspnWp("espn"))
    for t in (DEN, SEA):
        engine.dispatch(contract(t), 0)
    for i, s in enumerate(states):
        engine.dispatch(s, s.ts_ms)
        if i % 20 == 0:  # synthetic books around ESPN's number
            wp = s.espn_home_wp if s.espn_home_wp is not None else 0.5
            bid = max(100, min(9_800, round(wp * 100) * 100 - 100))
            engine.dispatch(snap(DEN, bid, bid + 200, s.ts_ms), s.ts_ms)
    den, sea = seen.forecasts("espn")[-2:]
    assert (den.ticker, den.p, sea.ticker, sea.p) == (DEN, 1.0, SEA, 0.0)  # DEN won 17-14 in OT
    books = [e for e in seen.events if type(e) is BookSnapshot]
    mids = [round(f.p, 4) for f in seen.forecasts("mid")]
    assert mids == [round((b.bids[0][0] + b.asks[0][0]) / 20_000, 4) for b in books][: len(mids)]
    assert len(mids) >= len(books) - 2  # repeats of the same mid are throttled away


# ---- runtimes -------------------------------------------------------------------------------------------------


@model("e3_slow")
class Slow:
    """20 ms per event: too slow for the dispatch loop."""

    event_types = (Trade,)

    def __init__(self, name, params=None, env=None) -> None:
        self.name = name

    def wants(self, ev) -> bool:
        return type(ev) is Trade

    def on_event(self, ev, state, now_ms):
        time.sleep(0.02)
        linked = state.contracts.get(ev.ticker) is not None  # the worker's own state saw the contract
        return [Forecast(self.name, ev.ticker, 0.6 if linked else 0.4, now_ms, ev.ts_ms)]


class Recorded:
    def __init__(self) -> None:
        self.records: list = []

    def record(self, source, kind, payload) -> None:
        self.records.append((source, kind, payload))


def test_a_slow_model_in_a_worker_process_doesnt_slow_dispatch():
    recorder = Recorded()
    pm = build_model(ModelSpec(name="slow", kind="e3_slow", runtime="process"))
    assert isinstance(pm, ProcessModel)
    seen = Seen()
    engine = Engine(LiveClock(), State(), models=[pm], publisher=seen, recorder=recorder)

    async def main() -> float:
        pm.attach(engine.source("slow", policy="drop_oldest"))
        stop = asyncio.Event()
        runner = asyncio.create_task(engine.run(stop))
        engine.dispatch(contract(DEN), 0)
        t0 = time.perf_counter()
        for i in range(20):
            engine.dispatch(Trade("kalshi", f"t{i}", DEN, 4_000, 100, "bid", i), i)
        took = time.perf_counter() - t0
        for _ in range(200):  # the worker takes ≥ 0.4 s, plus its start-up
            if len(seen.forecasts("slow")) == 20:
                break
            await asyncio.sleep(0.05)
        stop.set()
        await runner
        pm.close()
        return took

    took = asyncio.run(main())
    assert took < 0.1  # inline it would be ≥ 0.4 s
    got = seen.forecasts("slow")
    assert len(got) == 20 and {f.p for f in got} == {0.6}
    assert [decode_event(p) for s, k, p in recorder.records if k == "e"] == got  # recorded raw, for replays
    with pytest.raises(PluginError, match="needs `event_types`"):
        ProcessModel("x", Recorded, None)  # any class that doesn't declare what to forward


def test_backtests_run_models_inline_with_their_latency():
    m = build_model(ModelSpec(name="mid", kind="market_mid", runtime="process", latency_ms=500), backtest=True)
    assert isinstance(m, Delayed)
    engine, seen = engine_with(m)
    engine.dispatch(contract(DEN), 0)
    engine.dispatch(snap(DEN, 4_000, 4_200), 1_000)
    engine.dispatch(snap(DEN, 4_400, 4_600), 1_200)
    assert seen.forecasts() == []
    engine.dispatch(snap(DEN, 4_400, 4_600, 9), 1_500)  # releases the first
    engine.dispatch(snap(DEN, 4_600, 4_800), 1_800)  # and the second
    assert [(round(f.p, 2), f.ts_ms) for f in seen.forecasts()] == [(0.41, 1_500), (0.45, 1_700)]
    assert isinstance(build_model(ModelSpec(name="mid", kind="market_mid")), MarketMid)
    with pytest.raises(PluginError, match="unknown kind"):
        build_model(ModelSpec(name="x", kind="nope"))
    with pytest.raises(PluginError, match="unknown params key"):
        build_model(ModelSpec(name="x", kind="market_mid", params={"min_chnge": 1}))


def test_data_that_arrives_before_its_market_is_dispatched_again_once_linked():
    engine, seen = engine_with(EspnWp("espn"))
    game = GameState("g", 1, "2026-10-15", "DEN", "SEA", "in", 2, 300, 7, 3, espn_home_wp=0.7)
    engine.dispatch(game, 1)  # ESPN's first board can beat Kalshi's market discovery
    assert seen.forecasts() == []
    engine.dispatch(contract(DEN), 2)
    engine.dispatch(contract(SEA), 3)
    assert [(f.ticker, round(f.p, 2)) for f in seen.forecasts()] == [(DEN, 0.7), (DEN, 0.7), (SEA, 0.3)]
    assert [type(e).__name__ for e in seen.events].count("GameState") == 3  # viewers get it with its markets
