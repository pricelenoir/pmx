"""`nfl_game`: Kalshi NFL game contracts → the game's entity key, the same string `GameState.entity` gives
(`nfl:2026-10-18:HOU-JAX`: the local game date and both teams in alphabetical order, so neither home/away
nor the ticker's team order matters). Pure; the engine calls it on every `Contract`."""

from __future__ import annotations

from pmx.core.types import Contract
from pmx.plugins import linker
from pmx.sports.nfl.catalog import parse_ticker


@linker("nfl_game")
def nfl_game(c: Contract) -> str | None:
    if c.venue != "kalshi":
        return None
    n = parse_ticker(c.ticker)
    return None if n is None else f"nfl:{n.date.isoformat()}:{'-'.join(sorted(n.teams))}"
