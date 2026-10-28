"""NFL `GameState`s for scenario tests: `sc.data(nfl_state(q=3, clock="4:12", home=17, away=14))`."""

from __future__ import annotations

from typing import Any

from pmx.sports.nfl.state import GameState, GameStatus


def nfl_state(q: int = 1, clock: str = "15:00", home: int = 0, away: int = 0, *,
              teams: tuple[str, str] = ("HOU", "JAX"), status: GameStatus | None = None, date: str = "2026-10-18",
              ts_ms: int = 0, game_id: str = "test", **fields: Any) -> GameState:
    """`teams` is (away, home), so the default links to `KXNFLGAME-26OCT18HOUJAC-*`. `q=0` is pregame; the
    status is "in" otherwise unless given. Any other `GameState` field passes through (`possession="home"`,
    `down=3`, `espn_home_wp=0.61`, `winner="home"`)."""
    mins, _, secs = clock.partition(":")
    away_team, home_team = teams
    if status is None:
        status = "pre" if q == 0 else "in"
    return GameState(game_id, ts_ms, date, home_team, away_team, status, q, int(mins) * 60 + int(secs or 0),
                     home, away, **fields)
