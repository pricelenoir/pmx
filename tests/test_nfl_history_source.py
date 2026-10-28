"""NFL games rebuilt from nflverse plays and Kalshi candles — ordering, latency, linking, settlement, runs."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pmx.backtest.nfl_history import PRE_MS, NflHistory
from pmx.backtest.runner import run
from pmx.core.types import BookSnapshot, Contract, Trade
from pmx.engine.config import load
from pmx.harness import kalshi_history as kh
from pmx.sports.nfl import data as nfl_data
from pmx.sports.nfl.linker import nfl_game
from pmx.sports.nfl.state import GameState

FIX = Path(__file__).parent / "fixtures"
TICKERS = {"KXNFLGAME-25SEP07NYGDAL-DAL": "yes", "KXNFLGAME-25SEP07NYGDAL-NYG": "no",
           "KXNFLGAME-25SEP15KCBUF-BUF": "yes", "KXNFLGAME-25SEP15KCBUF-KC": "no"}  # the second dated a day late
CONFIG = """
[engine]
record = false
[risk]
max_loss = "100.00"
[[model]]
name = "wp"
kind = "espn_wp"
[[strategy]]
id = "smoke"
kind = "scripted"
markets = { series = ["KXNFLGAME"], linker = "nfl_game" }
params = { steps = [{ at = "+0s", action = "quote", side = "bid", price = "bid", qty = "1" },
                    { at = "+60s", action = "take", side = "bid", price = "ask", qty = "1" }] }
[[strategy]]
id = "idle"
kind = "noop"
markets = { series = ["KXNFLGAME"], linker = "nfl_game" }
[[strategy]]
id = "watch"
kind = "watch"
model = "wp"
markets = { series = ["KXNFLGAME"], linker = "nfl_game" }
params = { threshold = 0.02 }
"""


@pytest.fixture(scope="module")
def root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("hist")
    (root / "nflverse").mkdir()
    shutil.copy(FIX / "nfl" / "pbp_mini.parquet", nfl_data.pbp_path(root, 2025))
    games = kh.nfl_games(root, [2025])
    rows = []
    for t, result in TICKERS.items():
        gid = "2025_01_NYG_DAL" if "NYGDAL" in t else "2025_02_KC_BUF"
        ko = games.filter(pl.col("game_id") == gid)["kickoff_ms"][0]
        rows.append({"ticker": t, "event_ticker": t.rsplit("-", 1)[0], "team": t.rsplit("-", 1)[1], "result": result,
                     "close_ms": ko + 4 * 3_600_000, "expected_expiration_ms": ko + 4 * 3_600_000, "volume": 100_000,
                     "settlement": 10_000 if result == "yes" else 0, "source": "historical"})
        n = 4 * 60
        win = result == "yes"
        mids = [5_000 + (k * 4_000 // n if win else -k * 4_000 // n) for k in range(n)]  # drifts toward the result
        mids = [m - m % 100 for m in mids]
        asks = [m + 100 for m in mids]
        candles = pl.DataFrame({
            "end_ms": [ko - PRE_MS + 60_000 * (k + 1) for k in range(n)],
            "bid_open": mids, "bid_low": mids, "bid_high": mids, "bid_close": mids,
            "ask_open": asks, "ask_low": asks, "ask_high": asks, "ask_close": asks,
            "price_open": mids, "price_high": asks, "price_low": mids, "price_close": mids,
            "volume": [1_000] * n, "oi": [50_000] * n}, schema=kh._CANDLE_SCHEMA)
        kh.candle_path(root, t).parent.mkdir(parents=True, exist_ok=True)
        candles.write_parquet(kh.candle_path(root, t))
    markets = pl.DataFrame(rows)
    markets.write_parquet(kh.history_dir(root) / "markets.parquet")
    links = kh.link_games(root, markets, [2025])
    assert links.games.height == 2 and not links.unmatched
    return root


def test_events_come_in_time_order_known_when_they_happened_and_shifted_by_latency(root):
    h = NflHistory(root, seasons=[2025], latency_s=15)
    market, game = (list(s.events()) for s in h.streams())
    for evs in (market, game):
        assert [ms for ms, _ in evs] == sorted(ms for ms, _ in evs)
    for ms, ev in market + game:
        if isinstance(ev, (GameState, BookSnapshot, Trade)):
            assert ev.ts_ms == ms, "dispatched exactly when it became known"
    st = nfl_data.states(root, [2025]).filter(pl.col("game_id") == "2025_01_NYG_DAL")
    first_in = next(ev for _, ev in game if isinstance(ev, GameState) and ev.status == "in")
    assert first_in.ts_ms == st.filter(pl.col("ts_ms").is_not_null())["ts_ms"][0] + 15_000
    trades = [ev for _, ev in market if isinstance(ev, Trade)]
    assert {t.taker_side for t in trades} == {"bid", "ask"} and trades[0].qty == 500, "half the minute's volume each"


def test_games_link_like_live_and_settle_at_the_end(root):
    h = NflHistory(root, seasons=[2025])
    market, game = (list(s.events()) for s in h.streams())
    entities = {ev.entity for _, ev in game if isinstance(ev, GameState)}
    contracts = [ev for _, ev in market if isinstance(ev, Contract)]
    assert {nfl_game(c) for c in contracts} == entities == {"nfl:2025-09-07:DAL-NYG", "nfl:2025-09-15:BUF-KC"}
    for t, result in TICKERS.items():
        mine = [(ms, ev) for ms, ev in market if getattr(ev, "ticker", None) == t]
        assert mine[0][1].status == "open" and mine[-1][1].status == "settled"
        assert mine[-1][1].result == result and mine[-1][0] == max(ms for ms, _ in mine)
    finals = [ev for _, ev in game if isinstance(ev, GameState) and ev.status == "post"]
    assert sorted((f.home, f.winner, f.home_score, f.away_score) for f in finals) == [("BUF", "home", 10, 7),
                                                                                    ("DAL", "home", 7, 3)]


def test_the_default_config_shapes_run_and_are_deterministic(root, tmp_path):
    cfg = load(text=CONFIG)

    def once(name: str):
        h = NflHistory(root, seasons=[2025])
        return run(cfg, start=date(2025, 9, 7), end=date(2025, 9, 15), root=root, run_dir=tmp_path / name,
                   history=h.streams, fidelity="candle", timer_ms=5_000, idle_after_s=120)

    a, b = once("a"), once("b")
    assert a.fidelity == "candle" and a.strategies["idle"].orders == 0
    assert a.strategies["smoke"].orders == 2 * len(TICKERS), "a quote and a take per slot, as scripted"
    assert a.logs.get("watch", 0) > 0, "espn_wp from the history's WP gives watch signals"
    assert (tmp_path / "a" / "ledger.sqlite").read_bytes() == (tmp_path / "b" / "ledger.sqlite").read_bytes()
    assert (a.inputs, a.fills, a.timers) == (b.inputs, b.fills, b.timers)
