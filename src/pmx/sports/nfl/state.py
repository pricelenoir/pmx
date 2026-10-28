"""Venue-neutral NFL game state: what is known about a game at one wall-clock instant.
Field position is from the possessing team's view (`yards_to_endzone`)."""

from __future__ import annotations

from typing import ClassVar, Literal

import msgspec

type GameStatus = Literal["pre", "in", "half", "post"]
type Side = Literal["home", "away"]

REG_PERIOD_S = 900


class GameState(msgspec.Struct, frozen=True):
    kind: ClassVar[str] = "nfl_game"

    game_id: str
    ts_ms: int  # when this state became known
    date: str  # local game date, YYYY-MM-DD
    home: str
    away: str
    status: GameStatus
    period: int = 0  # 5+ = overtime
    clock_s: int = REG_PERIOD_S  # left in the period
    home_score: int = 0
    away_score: int = 0
    possession: Side | None = None
    down: int | None = None
    distance: int | None = None
    yards_to_endzone: int | None = None
    home_timeouts: int = 3
    away_timeouts: int = 3
    home_receives_2h: bool | None = None
    kickoff_ms: int | None = None
    last_play: str = ""
    espn_home_wp: float | None = None
    winner: Side | None = None  # set when final (None on a tie)

    @property
    def entity(self) -> str:
        """nfl:2026-10-18:HOU-JAX — the local game date and both teams in alphabetical order, so the key
        doesn't depend on home/away (`sports/nfl/linker.py` builds the same from a ticker)."""
        a, b = sorted((self.away, self.home))
        return f"nfl:{self.date}:{a}-{b}"

    @property
    def phase(self) -> str:
        """pre | Q1–Q4 | half | OT | final — what fill annotations and breakdowns split by."""
        if self.status in ("pre", "half"):
            return self.status
        if self.status == "post":
            return "final"
        return "OT" if self.period > 4 else f"Q{max(self.period, 1)}"

    @property
    def score_diff(self) -> int:
        return self.home_score - self.away_score

    @property
    def secs_left(self) -> int:
        """Regulation seconds remaining; in overtime, seconds left in the period."""
        if self.status == "pre" or self.period == 0:
            return 4 * REG_PERIOD_S
        if self.period > 4:
            return self.clock_s
        return (4 - self.period) * REG_PERIOD_S + self.clock_s

    @property
    def half_secs_left(self) -> int:
        if self.period > 4:
            return self.clock_s
        if self.status == "pre" or self.period == 0:
            return 2 * REG_PERIOD_S
        return (1 - (self.period - 1) % 2) * REG_PERIOD_S + self.clock_s

    def summary(self) -> str:
        return self.short()

    def display_fields(self) -> list[tuple[str, str]]:
        out = [("status", self.status), ("score", f"{self.away} {self.away_score} - {self.home_score} {self.home}")]
        if self.status == "in" and self.possession is not None:
            ball = f"{self.home if self.possession == 'home' else self.away} ball"
            if self.down and self.distance is not None:
                ball += f", {self.down}{('st', 'nd', 'rd', 'th')[min(self.down, 4) - 1]} & {self.distance}"
            if self.yards_to_endzone is not None:
                ball += f", {self.yards_to_endzone} yd to goal"
            out.append(("possession", ball))
        if self.espn_home_wp is not None:
            out.append(("espn wp", f"{self.home} {self.espn_home_wp:.0%}"))
        if self.last_play:
            out.append(("last play", self.last_play))
        return out

    def short(self) -> str:
        """Q3 4:12 HOU 17-14 JAX"""
        match self.status:
            case "pre":
                head = "pre"
            case "post":
                head = "final"
            case "half":
                head = "half"
            case _:
                q = f"Q{self.period}" if self.period <= 4 else ("OT" if self.period == 5 else f"{self.period - 4}OT")
                head = f"{q} {self.clock_s // 60}:{self.clock_s % 60:02d}"
        return f"{head} {self.away} {self.away_score}-{self.home_score} {self.home}"
