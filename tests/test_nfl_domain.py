from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pmx.sports.nfl import data, features
from pmx.sports.nfl.catalog import parse_ticker
from pmx.sports.nfl.state import GameState
from pmx.sports.nfl.teams import KALSHI_CODES, TEAMS, canon

FIXTURE = Path(__file__).parent / "fixtures" / "nfl" / "pbp_mini.parquet"


@pytest.fixture(scope="module")
def states(tmp_path_factory) -> pl.DataFrame:
    root = tmp_path_factory.mktemp("nfl")
    (root / "nflverse").mkdir()
    shutil.copy(FIXTURE, data.pbp_path(root, 2025))
    return data.states(root, [2025])


def play(df: pl.DataFrame, game: str, play_id: int) -> dict:
    return df.filter((pl.col("game_id") == game) & (pl.col("play_id") == play_id)).row(0, named=True)


G1, G2 = "2025_01_NYG_DAL", "2025_02_KC_BUF"


# ---- teams and tickers -----------------------------------------------------------------------------


def test_canon_maps_every_alias_and_rejects_unknown_codes():
    assert {canon(t) for t in TEAMS} == TEAMS and all(canon(t) == t for t in TEAMS)
    assert canon("JAC") == "JAX" and canon("LAR") == "LA" and canon("WSH") == "WAS" and canon("jax") == "JAX"
    assert {canon(c) for c in KALSHI_CODES} == TEAMS
    for bad in ("XXX", "", "JA", "STL"):
        with pytest.raises(ValueError, match="unknown NFL team"):
            canon(bad)


def test_parse_ticker_reads_every_matchup_in_kalshi_codes():
    n = 0
    for away in KALSHI_CODES:
        for home in KALSHI_CODES:
            if canon(away) == canon(home):
                continue
            for yes in (away, home):
                c = parse_ticker(f"KXNFLGAME-26OCT18{away}{home}-{yes}")
                assert c is not None, (away, home)
                assert c.teams == (canon(away), canon(home)) and c.team == canon(yes)
                assert c.date == date(2026, 10, 18) and c.event_ticker == f"KXNFLGAME-26OCT18{away}{home}"
                n += 1
    assert n == 2 * (31 * 32 + 2 * 31)  # 33 codes; the Rams are both LA and LAR, and never play themselves


def test_parse_ticker_rejects_other_series_bad_dates_and_unknown_codes():
    for bad in ("KXNBAGAME-26OCT18HOUJAC-HOU", "KXNFLGAME-26OCX18HOUJAC-HOU", "KXNFLGAME-26OCT32HOUJAC-HOU",
                "KXNFLGAME-26OCT18HOUXXX-HOU", "KXNFLGAME-26OCT18HOUJAC-XXX", "KXNFLGAME-26OCT18HOUJAC",
                "KXNFLGAME-26OCT18HOUJAC-HOU-1", "KXNFLGAME-26OCT18-HOU", ""):
        assert parse_ticker(bad) is None, bad


def test_matches_allows_a_day_either_side_and_either_team_order():
    c = parse_ticker("KXNFLGAME-26OCT15SEADEN-DEN")
    assert c is not None
    assert c.matches(date(2026, 10, 15), "DEN", "SEA")
    assert c.matches(date(2026, 10, 16), "SEA", "DEN")  # a late kickoff is the next day in UTC
    assert c.matches(date(2026, 10, 14), "DEN", "SEA")
    assert not c.matches(date(2026, 10, 17), "DEN", "SEA")
    assert not c.matches(date(2026, 10, 15), "DEN", "KC")
    jac = parse_ticker("KXNFLGAME-26OCT18HOUJAC-JAC")
    assert jac is not None and jac.matches(date(2026, 10, 18), "JAX", "HOU") and jac.team == "JAX"


# ---- game state ------------------------------------------------------------------------------------


def gs(status="in", period=1, clock_s=900, **kw) -> GameState:
    return GameState("g", 0, "2026-10-18", "JAX", "HOU", status, period, clock_s, **kw)


@pytest.mark.parametrize(("state", "secs", "half", "short"), [
    (gs("pre", 0), 3600, 1800, "pre HOU 0-0 JAX"),
    (gs(period=1, clock_s=600), 3300, 1500, "Q1 10:00 HOU 0-0 JAX"),
    (gs(period=2, clock_s=125), 1925, 125, "Q2 2:05 HOU 0-0 JAX"),
    (gs("half", 2, 0, home_score=10, away_score=7), 1800, 0, "half HOU 7-10 JAX"),
    (gs(period=3, clock_s=900), 1800, 1800, "Q3 15:00 HOU 0-0 JAX"),
    (gs(period=4, clock_s=59), 59, 59, "Q4 0:59 HOU 0-0 JAX"),
    (gs(period=5, clock_s=600), 600, 600, "OT 10:00 HOU 0-0 JAX"),
    (gs(period=6, clock_s=300), 300, 300, "2OT 5:00 HOU 0-0 JAX"),
    (gs("post", 4, 0, home_score=24, away_score=27, winner="away"), 0, 0, "final HOU 27-24 JAX"),
])
def test_game_state_clock_and_short_form(state, secs, half, short):
    assert (state.secs_left, state.half_secs_left, state.short()) == (secs, half, short)
    assert state.summary() == short


def test_game_state_is_an_entity_with_viewer_fields():
    s = gs(period=3, clock_s=252, home_score=14, away_score=17, possession="home", down=3, distance=4,
           yards_to_endzone=38, espn_home_wp=0.41, last_play="pass incomplete")
    assert s.entity == "nfl:2026-10-18:HOU-JAX" and s.score_diff == -3
    assert dict(s.display_fields()) == {
        "status": "in", "score": "HOU 17 - 14 JAX", "possession": "JAX ball, 3rd & 4, 38 yd to goal",
        "espn wp": "JAX 41%", "last play": "pass incomplete",
    }


# ---- nflverse states --------------------------------------------------------------------------------


def test_states_are_pre_play_and_stamped_when_known(states):
    assert states.height == 29 and states["game_id"].unique().sort().to_list() == [G1, G2]
    td, xp = play(states, G1, 160), play(states, G1, 185)
    assert td["score_diff"] == 0 and (xp["home_score"], xp["away_score"], xp["score_diff"]) == (6, 0, 6)
    assert play(states, G1, 200)["score_diff"] == 7
    assert play(states, G1, 280)["away_score"] == 3  # the field goal shows from the next play on
    ms = lambda t: int(pl.Series([f"2025-09-07T{t}Z"]).str.to_datetime(time_zone="UTC").dt.epoch("ms")[0])  # noqa: E731
    assert play(states, G1, 60)["ts_ms"] == ms("17:00:07.500")  # end of the kickoff
    assert play(states, G1, 360)["ts_ms"] == ms("19:00:06.000")  # previous play had no end: snap + 6 s
    assert play(states, G1, 210)["ts_ms"] == ms("17:05:36.000")  # after the timeout row (no end either)
    assert play(states, G1, 40)["ts_ms"] == ms("17:00:00.000")  # first timed play: its own snap
    assert play(states, G1, 1)["ts_ms"] is None


def test_states_possession_timeouts_and_second_half_receiver(states):
    assert [play(states, G1, p)["poss"] for p in (60, 110, 135, 200, 300)] == [-1, -1, 1, 0, 1]
    assert (play(states, G1, 200)["away_to"], play(states, G1, 210)["away_to"]) == (3, 2)  # pre-play count
    assert (play(states, G1, 300)["home_to"], play(states, G1, 300)["away_to"]) == (3, 3)  # reset at the half
    assert (play(states, G2, 460)["home_to"], play(states, G2, 460)["away_to"]) == (2, 2)  # overtime
    assert play(states, G1, 230)["home_rec_2h"] == 1.0 and play(states, G2, 55)["home_rec_2h"] == 0.0
    assert play(states, G1, 300)["home_rec_2h"] is None and play(states, G2, 480)["home_rec_2h"] is None
    assert play(states, G2, 480)["is_ot"] == 1.0 and play(states, G2, 440)["is_ot"] == 0.0
    assert play(states, G1, 1)["home_win"] == 1 and play(states, G2, 1)["final_home"] == 10


def state_of(row: dict, *, home_receives_2h: bool) -> GameState:
    """The `GameState` a live feed would hold for this nflverse row."""
    period = int(row["qtr"])
    secs = int(row["secs_left"])
    clock = secs if period > 4 else secs - (4 - period) * 900

    def opt(v):
        return None if v is None else int(v)

    return GameState(
        row["game_id"], row["ts_ms"] or 0, row["game_date"], row["home"], row["away"], "in", period, clock,
        int(row["home_score"]), int(row["away_score"]), {1: "home", -1: "away", 0: None}[row["poss"]],
        opt(row["down"]), opt(row["ydstogo"]), opt(row["yte"]), int(row["home_to"]), int(row["away_to"]),
        home_receives_2h,
    )


def test_features_from_a_live_state_equal_the_training_features_exactly(states):
    """The key test: the live model sees exactly what training saw for the same facts."""
    receives = {G1: True, G2: False}
    rows = [(G1, p) for p in (1, 40, 60, 110, 135, 185, 200, 230, 280, 300, 340, 380)]
    rows += [(G2, p) for p in (35, 80, 400, 440, 460, 480, 520)]
    priors = np.linspace(-1.2, 1.5, len(rows))
    for (game, pid), prior in zip(rows, priors, strict=True):
        frame = states.filter((pl.col("game_id") == game) & (pl.col("play_id") == pid))
        row = frame.row(0, named=True)
        s = state_of(row, home_receives_2h=receives[game])
        assert s.secs_left == row["secs_left"] and s.half_secs_left == row["half_secs_left"], (game, pid)
        train = features.build(**data.feature_inputs(frame, np.array([prior])))
        live = features.from_state(s, prior)
        np.testing.assert_array_equal(live, train, err_msg=f"{game} play {pid}")
        assert live.shape == (1, len(features.FEATURES))


def test_features_hide_down_distance_and_field_without_a_possession():
    s = gs(period=2, clock_s=30, possession=None, down=2, distance=7, yards_to_endzone=40, home_receives_2h=True)
    row = dict(zip(features.FEATURES, features.from_state(s, 0.3)[0], strict=True))
    assert np.isnan(row["down"]) and np.isnan(row["ydstogo"]) and np.isnan(row["yte"])
    assert row["poss"] == 0 and row["poss_field"] == 0 and row["home_rec_2h"] == 1.0
    third = dict(zip(features.FEATURES, features.from_state(gs(period=3, home_receives_2h=True), 0.3)[0], strict=True))
    assert np.isnan(third["home_rec_2h"]) and third["prior_time"] == pytest.approx(0.3 * 0.5)
