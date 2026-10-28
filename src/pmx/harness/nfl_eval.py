"""Win-probability model scorecards: `holdout` scores out-of-sample plays, model vs nflfastR's spread-adjusted WP."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from pmx.harness import metrics
from pmx.models.nfl_wp import Artifact, WinProb
from pmx.sports.nfl import data


def holdout(root: Path, art: Artifact, seasons: list[int]) -> dict:
    overlap = sorted(set(seasons) & set(art.train_seasons))
    if overlap:
        raise ValueError(f"seasons {overlap} are in the training set")
    df = data.states(root, seasons).filter(pl.col("spread_line").is_not_null() & pl.col("nflfastr_wp").is_not_null())
    p = WinProb(art).frame(df, art.prior.logit(df["spread_line"].cast(pl.Float64).to_numpy()))
    y = df["home_win"].to_numpy().astype(float)
    ref = df["nflfastr_wp"].to_numpy()
    qtr = df["qtr"].to_numpy()
    rows = []
    for label, m in [("all", np.ones(len(df), bool)), ("Q1", qtr == 1), ("Q2", qtr == 2), ("Q3", qtr == 3),
                     ("Q4", qtr == 4), ("OT", qtr > 4)]:
        if m.sum() == 0:
            continue
        a, b = metrics.score(p[m], y[m]), metrics.score(ref[m], y[m])
        rows.append({"slice": label, "plays": int(m.sum()), "model_brier": a["brier"], "nflfastr_brier": b["brier"],
                     "model_logloss": a["log_loss"], "nflfastr_logloss": b["log_loss"], "model_ece": a["ece"],
                     "nflfastr_ece": b["ece"]})
    return {
        "seasons": seasons,
        "games": df["game_id"].n_unique(),
        "slices": pl.DataFrame(rows),
        "calibration": pl.DataFrame(metrics.calibration(p, y), schema=["predicted", "observed", "n"], orient="row"),
    }
