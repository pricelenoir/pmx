"""Kalshi NFL game contracts <-> games. `KXNFLGAME-26OCT18HOUJAC-HOU` is "Houston beats
Jacksonville in the game on Oct 18 2026 (local date)"; the event names away then home,
but home/away is taken from the game schedule, not the ticker."""

from __future__ import annotations

from datetime import date, datetime

import msgspec

from pmx.sports.nfl.teams import KALSHI_CODES, canon

SERIES = "KXNFLGAME"


class NflContract(msgspec.Struct, frozen=True):
    ticker: str
    event_ticker: str
    date: date
    teams: tuple[str, str]  # canonical, in ticker order (away, home)
    team: str  # the YES side

    def matches(self, day: date, home: str, away: str) -> bool:
        return abs((self.date - day).days) <= 1 and set(self.teams) == {home, away}


def _split_codes(s: str) -> tuple[str, str] | None:
    for i in (2, 3):
        a, b = s[:i], s[i:]
        if a in KALSHI_CODES and b in KALSHI_CODES:
            return a, b
    return None


def parse_ticker(ticker: str) -> NflContract | None:
    parts = ticker.split("-")
    if len(parts) != 3 or parts[0] != SERIES:
        return None
    _, ev, team = parts
    try:
        day = datetime.strptime(ev[:7].title(), "%y%b%d").date()
    except ValueError:
        return None
    codes = _split_codes(ev[7:])
    if codes is None or team not in KALSHI_CODES:
        return None
    return NflContract(ticker, f"{SERIES}-{ev}", day, (canon(codes[0]), canon(codes[1])), canon(team))
