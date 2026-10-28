"""Probability scoring. Everything takes arrays of P(yes) and 0/1 outcomes."""

from __future__ import annotations

import numpy as np

EPS = 1e-6


def brier(p: np.ndarray, y: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def log_loss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.clip(p, EPS, 1 - EPS)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def calibration(p: np.ndarray, y: np.ndarray, bins: int = 10) -> list[tuple[float, float, int]]:
    """(mean predicted, observed rate, n) per equal-width bin, empty bins skipped."""
    idx = np.minimum((p * bins).astype(int), bins - 1)
    out = []
    for b in range(bins):
        m = idx == b
        if m.any():
            out.append((float(p[m].mean()), float(y[m].mean()), int(m.sum())))
    return out


def ece(p: np.ndarray, y: np.ndarray, bins: int = 10) -> float:
    """Expected calibration error: n-weighted |predicted - observed| over bins."""
    rows = calibration(p, y, bins)
    n = sum(r[2] for r in rows)
    return sum(abs(r[0] - r[1]) * r[2] for r in rows) / n if n else float("nan")


def score(p: np.ndarray, y: np.ndarray) -> dict[str, float]:
    return {"n": float(len(p)), "brier": brier(p, y), "log_loss": log_loss(p, y), "ece": ece(p, y)}
