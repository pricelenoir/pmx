"""Win-probability features, home perspective. One builder for training (nflverse
columns) and live (`GameState`), so the model sees identical inputs in both."""

from __future__ import annotations

import math

import numpy as np

from pmx.sports.nfl.state import GameState

FEATURES = (
    "prior_logit", "score_diff", "secs_left", "half_secs_left", "elapsed", "diff_time", "prior_time",
    "poss", "down", "ydstogo", "yte", "poss_field", "home_to", "away_to", "home_rec_2h", "is_ot",
)
# +1: higher means better for home. Keeps the model sane where data is thin.
MONOTONE = {"prior_logit": 1, "score_diff": 1, "diff_time": 1, "prior_time": 1, "home_to": 1, "away_to": -1}


def logit(p: float) -> float:
    p = min(max(p, 1e-4), 1 - 1e-4)
    return math.log(p / (1 - p))


def build(
    *, prior_logit, score_diff, secs_left, half_secs_left, poss, down, ydstogo, yte, home_to, away_to,
    home_rec_2h, is_ot,
) -> np.ndarray:
    """Arrays (or scalars) in, (n, len(FEATURES)) float matrix out. NaN = unknown."""
    a = {k: np.atleast_1d(np.asarray(v, dtype=float)) for k, v in locals().items()}
    reg_left = np.where(a["is_ot"] > 0, 0.0, np.clip(a["secs_left"] / 3600.0, 0.0, 1.0))
    yte_known = np.nan_to_num(a["yte"], nan=50.0)
    cols = {
        **a,
        "elapsed": 1.0 - reg_left,
        "diff_time": a["score_diff"] / np.sqrt(a["secs_left"] / 60.0 + 1.0),
        "prior_time": a["prior_logit"] * reg_left,
        "poss_field": np.where(a["poss"] != 0, a["poss"] * (100.0 - yte_known) / 100.0, 0.0),
    }
    n = max(len(v) for v in cols.values())
    return np.column_stack([np.broadcast_to(cols[f], n) for f in FEATURES])


def from_state(s: GameState, prior_logit: float) -> np.ndarray:
    """Same rules as `data.feature_inputs`: down, distance, and field position only with a possession."""
    poss = {"home": 1, "away": -1, None: 0}[s.possession]
    first_half = s.period <= 2
    down = s.down if poss and s.down else None
    return build(
        prior_logit=prior_logit,
        score_diff=s.score_diff,
        secs_left=s.secs_left,
        half_secs_left=s.half_secs_left,
        poss=poss,
        down=down if down is not None else math.nan,
        ydstogo=s.distance if down is not None and s.distance is not None else math.nan,
        yte=s.yards_to_endzone if poss and s.yards_to_endzone is not None else math.nan,
        home_to=s.home_timeouts,
        away_to=s.away_timeouts,
        home_rec_2h=float(s.home_receives_2h) if first_half and s.home_receives_2h is not None else math.nan,
        is_ot=float(s.period > 4),
    )
