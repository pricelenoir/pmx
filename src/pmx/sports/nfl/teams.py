"""NFL team codes. Canonical = nflverse (the training data); Kalshi and ESPN differ for a few."""

from __future__ import annotations

TEAMS = frozenset({
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE", "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LA", "LAC", "LV", "MIA", "MIN", "NE", "NO", "NYG", "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WAS",
})

_ALIASES = {"JAC": "JAX", "LAR": "LA", "WSH": "WAS"}

KALSHI_CODES = (TEAMS - {"JAX"}) | {"JAC", "LAR"}


def canon(code: str) -> str:
    c = code.upper()
    c = _ALIASES.get(c, c)
    if c not in TEAMS:
        raise ValueError(f"unknown NFL team code {code!r}")
    return c
