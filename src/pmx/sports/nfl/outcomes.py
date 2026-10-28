"""NFL game outcomes for scenario risk: either team wins, or a tie, which Kalshi settles at $0.50 on both
team contracts. Outcomes are named by team code (`HOU`, `JAX`, `tie`)."""

from __future__ import annotations

from pmx.core.types import Contract
from pmx.core.units import ONE_DOLLAR
from pmx.plugins import outcomes
from pmx.sports.nfl.catalog import parse_ticker

TIE_PRICE = ONE_DOLLAR // 2


@outcomes("nfl_game")
def nfl_game(entity: str, contracts: list[Contract]) -> dict[str, dict[str, str | int]]:
    by_team = {}
    for c in contracts:
        n = parse_ticker(c.ticker)
        if n is not None:
            by_team[n.team] = c.ticker
    if len(by_team) < 2:  # one team's market alone: its own yes / no
        return {}
    out: dict[str, dict[str, str | int]] = {team: {t: "yes" if other == team else "no" for other, t in by_team.items()}
                                            for team in sorted(by_team)}
    out["tie"] = {t: TIE_PRICE for t in by_team.values()}
    return out
