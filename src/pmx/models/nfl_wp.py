"""NFL in-game win probability: P(home wins | game state, pregame market prior).

The prior is the market's pregame belief as a logit. Training derives it from the
closing spread (nflverse); live, it is the Kalshi mid at kickoff. Either way the model
learns how the game state should move the market's opening view."""

from __future__ import annotations

import logging
import pickle
from pathlib import Path

import msgspec
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression

from pmx.harness import metrics
from pmx.sports.nfl import data, features
from pmx.sports.nfl.state import GameState

log = logging.getLogger(__name__)

NAME = "nfl_wp"


class SpreadPrior(msgspec.Struct, frozen=True):
    """logit P(home wins) = a + b * spread_line (nflverse: positive = home favored)."""

    a: float
    b: float

    def logit(self, spread: np.ndarray) -> np.ndarray:
        return self.a + self.b * spread


class Artifact(msgspec.Struct):
    model: object  # fitted HistGradientBoostingClassifier
    prior: SpreadPrior
    features: tuple[str, ...]
    train_seasons: list[int]
    params: dict
    metrics: dict


def artifact_path(root: Path) -> Path:
    return root / "models" / f"{NAME}.pkl"


def fit_prior(df: pl.DataFrame) -> SpreadPrior:
    games = df.group_by("game_id").agg(pl.col("spread_line").first(), pl.col("home_win").first()).drop_nulls()
    lr = LogisticRegression(C=1e6).fit(games["spread_line"].to_numpy()[:, None], games["home_win"].to_numpy())
    return SpreadPrior(float(lr.intercept_[0]), float(lr.coef_[0, 0]))


def design(df: pl.DataFrame, prior: SpreadPrior) -> tuple[np.ndarray, np.ndarray]:
    prior_logit = prior.logit(df["spread_line"].cast(pl.Float64).fill_null(0.0).to_numpy())
    X = features.build(**data.feature_inputs(df, prior_logit))
    return X, df["home_win"].to_numpy()


def _model(max_iter: int, seed: int = 0) -> HistGradientBoostingClassifier:
    cst = [features.MONOTONE.get(f, 0) for f in features.FEATURES]
    return HistGradientBoostingClassifier(
        max_iter=max_iter, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=400, l2_regularization=1.0,
        monotonic_cst=cst, early_stopping=False, random_state=seed,
    )


def train(root: Path, train_seasons: list[int], valid_seasons: list[int], grid=(150, 300, 600)) -> Artifact:
    """Pick the iteration count on `valid_seasons`, then refit on train + valid."""
    tr = data.states(root, train_seasons).filter(pl.col("spread_line").is_not_null())
    va = data.states(root, valid_seasons).filter(pl.col("spread_line").is_not_null())
    prior = fit_prior(tr)
    Xtr, ytr = design(tr, prior)
    Xva, yva = design(va, prior)
    results = {}
    for n in grid:
        p = _model(n).fit(Xtr, ytr).predict_proba(Xva)[:, 1]
        results[n] = metrics.log_loss(p, yva)
        log.info("max_iter=%d valid log_loss=%.4f", n, results[n])
    best = min(results, key=results.__getitem__)
    full = pl.concat([tr, va])
    prior = fit_prior(full)
    X, y = design(full, prior)
    model = _model(best).fit(X, y)
    return Artifact(model, prior, features.FEATURES, train_seasons + valid_seasons,
                    {"max_iter": best}, {"valid_log_loss": results})


def save(art: Artifact, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pickle.dumps(art))


def load(path: Path) -> Artifact:
    art = pickle.loads(path.read_bytes())
    if tuple(art.features) != features.FEATURES:
        raise ValueError(f"{path} was trained on different features; retrain it with `nfl_wp.train`")
    return art


class WinProb:
    """Runtime predictor."""

    def __init__(self, art: Artifact) -> None:
        self.art = art

    @classmethod
    def from_file(cls, path: Path) -> WinProb:
        return cls(load(path))

    def home(self, state: GameState, prior_logit: float) -> float:
        if state.status == "post":
            return 1.0 if state.winner == "home" else 0.0
        return float(self.art.model.predict_proba(features.from_state(state, prior_logit))[0, 1])

    def frame(self, df: pl.DataFrame, prior_logit: np.ndarray) -> np.ndarray:
        X = features.build(**data.feature_inputs(df, prior_logit))
        return self.art.model.predict_proba(X)[:, 1]
