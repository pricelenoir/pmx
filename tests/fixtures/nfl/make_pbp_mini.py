"""Writes `pbp_mini.parquet`: two synthetic games in nflverse's play-by-play schema (only the columns
`sports/nfl/data.py` keeps), for `tests/test_nfl_domain.py`.

Game 1, NYG @ DAL (home kicked off first): an interception, a touchdown + extra point, a timeout row
with no `posteam`, halftime (timeouts reset), a play with no `end_clock_time`. DAL wins 7-3.
Game 2, KC @ BUF (home received first): tied 7-7 after regulation, BUF field goal in overtime.

    uv run python tests/fixtures/nfl/make_pbp_mini.py
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

SCHEMA = {
    "game_id": pl.String, "season": pl.Int32, "season_type": pl.String, "week": pl.Int32, "game_date": pl.String,
    "home_team": pl.String, "away_team": pl.String, "posteam": pl.String, "play_id": pl.Float64, "qtr": pl.Float64,
    "game_seconds_remaining": pl.Float64, "half_seconds_remaining": pl.Float64, "down": pl.Float64,
    "ydstogo": pl.Float64, "yardline_100": pl.Float64, "total_home_score": pl.Float64,
    "total_away_score": pl.Float64, "home_timeouts_remaining": pl.Float64, "away_timeouts_remaining": pl.Float64,
    "home_opening_kickoff": pl.Float64, "spread_line": pl.Float64, "result": pl.Int32, "vegas_home_wp": pl.Float64,
    "time_of_day": pl.String, "end_clock_time": pl.String, "desc": pl.String,
}

# (play_id, qtr, game secs left, posteam, down, ydstogo, yardline_100, home, away (scores after the play),
#  home TOs, away TOs (after the play, as nflverse has them), snap "HH:MM:SS", end "HH:MM:SS" | None, desc)
GAME1 = [
    (1, 1, 3600, None, None, 0, None, 0, 0, 3, 3, None, None, "GAME"),
    (40, 1, 3600, "NYG", None, 0, 35, 0, 0, 3, 3, "17:00:00", "17:00:07", "DAL kicks off"),
    (60, 1, 3595, "NYG", 1, 10, 75, 0, 0, 3, 3, "17:00:40", "17:00:46", "NYG run for 4"),
    (85, 1, 3560, "NYG", 2, 6, 71, 0, 0, 3, 3, "17:01:20", "17:01:27", "NYG pass incomplete"),
    (110, 1, 3555, "NYG", 3, 6, 71, 0, 0, 3, 3, "17:02:00", "17:02:09", "NYG pass INTERCEPTED by DAL at NYG 40"),
    (135, 1, 3500, "DAL", 1, 10, 40, 0, 0, 3, 3, "17:03:00", "17:03:06", "DAL run for 2"),
    (160, 1, 3460, "DAL", 2, 8, 38, 6, 0, 3, 3, "17:03:45", "17:03:55", "DAL pass for 38, TOUCHDOWN"),
    (185, 1, 3460, "DAL", None, 0, 15, 7, 0, 3, 3, "17:05:00", "17:05:04", "extra point is GOOD"),
    (200, 1, 3460, None, None, 0, None, 7, 0, 3, 2, "17:05:30", None, "Timeout #1 by NYG"),
    (210, 1, 3460, "NYG", None, 0, 35, 7, 0, 3, 2, "17:06:30", "17:06:37", "DAL kicks off"),
    (230, 2, 1700, "NYG", 1, 10, 75, 7, 0, 3, 2, "17:40:00", "17:40:06", "NYG pass for 20"),
    (260, 2, 1650, "NYG", 4, 3, 30, 7, 3, 3, 2, "17:44:00", "17:44:05", "field goal is GOOD"),
    (280, 2, 1800, None, None, 0, None, 7, 3, 3, 2, "17:58:00", "17:58:00", "END QUARTER 2"),
    (300, 3, 1800, "DAL", None, 0, 35, 7, 3, 3, 3, "18:15:00", "18:15:07", "NYG kicks off"),
    (320, 3, 1790, "DAL", 1, 10, 70, 7, 3, 3, 3, "18:15:40", "18:15:46", "DAL run for 3"),
    (340, 4, 600, "DAL", 2, 5, 30, 7, 3, 3, 3, "19:00:00", None, "DAL run for 3"),
    (360, 4, 560, "DAL", 3, 2, 27, 7, 3, 3, 3, "19:00:50", "19:00:56", "DAL pass incomplete"),
    (380, 4, 0, None, None, 0, None, 7, 3, 3, 3, "19:30:00", "19:30:00", "END GAME"),
]
GAME2 = [
    (1, 1, 3600, None, None, 0, None, 0, 0, 3, 3, None, None, "GAME"),
    (35, 1, 3600, "BUF", None, 0, 35, 0, 0, 3, 3, "20:20:00", "20:20:07", "KC kicks off"),
    (55, 1, 3590, "BUF", 1, 10, 75, 0, 0, 3, 3, "20:20:40", "20:20:47", "BUF pass for 75, TOUCHDOWN"),
    (80, 1, 3580, "BUF", None, 0, 15, 7, 0, 3, 3, "20:22:00", "20:22:04", "extra point is GOOD"),
    (400, 4, 30, "KC", 1, 5, 5, 7, 6, 3, 3, "23:00:00", "23:00:08", "KC run for 5, TOUCHDOWN"),
    (420, 4, 30, "KC", None, 0, 15, 7, 7, 3, 3, "23:01:00", "23:01:04", "extra point is GOOD"),
    (440, 4, 0, None, None, 0, None, 7, 7, 3, 3, "23:03:00", "23:03:00", "END QUARTER 4"),
    (460, 5, 600, "BUF", None, 0, 35, 7, 7, 2, 2, "23:10:00", "23:10:07", "KC kicks off"),
    (480, 5, 594, "BUF", 1, 10, 75, 7, 7, 2, 2, "23:10:40", "23:10:46", "BUF pass for 45"),
    (500, 5, 400, "BUF", 4, 2, 20, 10, 7, 2, 2, "23:14:00", "23:14:05", "field goal is GOOD"),
    (520, 5, 400, None, None, 0, None, 10, 7, 2, 2, "23:14:30", "23:14:30", "END GAME"),
]


def rows(game_id, date, home, away, kickoff, spread, plays, day):
    result = plays[-1][7] - plays[-1][8]
    for pid, qtr, gsr, pos, down, togo, yl, hs, as_, hto, ato, snap, end, desc in plays:
        hsr = gsr - 1800 if qtr <= 2 else gsr
        yield {
            "game_id": game_id, "season": 2025, "season_type": "REG", "week": int(game_id[5:7]), "game_date": date,
            "home_team": home, "away_team": away, "posteam": pos, "play_id": pid, "qtr": qtr,
            "game_seconds_remaining": gsr, "half_seconds_remaining": hsr, "down": down, "ydstogo": togo,
            "yardline_100": yl, "total_home_score": hs, "total_away_score": as_, "home_timeouts_remaining": hto,
            "away_timeouts_remaining": ato, "home_opening_kickoff": kickoff, "spread_line": spread, "result": result,
            "vegas_home_wp": 0.6, "desc": desc,
            "time_of_day": None if snap is None else f"{day}T{snap}.000Z",
            "end_clock_time": None if end is None else f"{day}T{end}.500Z",
        }


def main() -> None:
    data = [*rows("2025_01_NYG_DAL", "2025-09-07", "DAL", "NYG", 1, 3.5, GAME1, "2025-09-07"),
            *rows("2025_02_KC_BUF", "2025-09-14", "BUF", "KC", 0, -1.5, GAME2, "2025-09-14")]
    out = Path(__file__).with_name("pbp_mini.parquet")
    pl.DataFrame(data, schema=SCHEMA).write_parquet(out)
    print(f"{len(data)} plays -> {out}")


if __name__ == "__main__":
    main()
