"""Scoring a rule's trades the same way every time.

A study returns one row per trade: `event` (the cluster: bets on one event are correlated), `close_ms` (when the
outcome became known), `pnl` (dollars per contract, net of costs: `costs.buy` / `costs.sell`), and optionally `qty`
(contracts, default 1), `t_ms`, `p_model` / `p_market` with `y` (calibration), and `capacity` (contracts that could
really have traded, e.g. a share of `costs.next_volume`). The confidence interval resamples whole events
(`analytics.stats.resample_clusters`, the engine's verdict uses the same)."""

from __future__ import annotations

from typing import Any

import msgspec
import numpy as np
import polars as pl

from pmx.analytics.stats import resample_clusters
from pmx.harness import metrics

REQUIRED = ("event", "close_ms", "pnl")
CONCENTRATION = 0.5  # one event carrying more than this share of |PnL| is a warning sign


class Quarter(msgspec.Struct):
    quarter: str
    events: int
    edge_c: float | None


class Calibration(msgspec.Struct):
    n: int
    brier_model: float
    brier_market: float | None
    log_loss_model: float
    log_loss_market: float | None
    bins: list[tuple[float, float, int]]


class Check(msgspec.Struct):
    name: str
    want: float
    got: float | None
    ok: bool


class Result(msgspec.Struct):
    trades: int
    events: int
    contracts: float
    days: int  # calendar days from the first close to the last
    edge_c: float | None  # net ¢ per contract
    ci_c: tuple[float, float] | None  # 95%, resampling events
    p_le0: float | None  # bootstrap share with edge <= 0
    pnl: float  # dollars at the given qty
    win_rate: float | None
    quarters: list[Quarter]
    top_event_share: float | None
    capacity_per_day: float | None  # contracts/day that could have traded, over `days`
    usd_per_day: float | None  # capacity × edge
    calibration: Calibration | None = None
    checks: list[Check] = []
    passed: bool | None = None
    warnings: list[str] = []


def evaluate(trades: pl.DataFrame, *, pass_: dict[str, Any] | None = None) -> Result:
    missing = [c for c in REQUIRED if c not in trades.columns]
    if missing:
        raise ValueError(f"trades need columns {', '.join(REQUIRED)}; missing {', '.join(missing)}")
    df = trades.with_columns(pl.col("pnl").cast(pl.Float64))
    df = df.filter(pl.col("pnl").is_not_null() & pl.col("pnl").is_not_nan())
    if "qty" not in df.columns:
        df = df.with_columns(qty=pl.lit(1.0))
    df = df.with_columns(w=pl.col("pnl") * pl.col("qty"))
    n, contracts = df.height, float(df["qty"].sum() or 0)
    days = (int(df["close_ms"].max() - df["close_ms"].min()) // 86_400_000 + 1) if n else 0
    by_ev = df.group_by("event").agg(pl.col("w").sum(), pl.col("qty").sum(), pl.col("close_ms").max())
    edge = 100 * float(df["w"].sum()) / contracts if contracts else None
    ci = p_le0 = None
    if by_ev.height >= 2 and contracts:
        tot = resample_clusters(by_ev.select("w", "qty").to_numpy().astype(np.float64))
        per = 100 * tot[:, 0] / np.maximum(tot[:, 1], 1e-12)
        lo, hi = np.percentile(per, [2.5, 97.5])
        ci, p_le0 = (round(float(lo), 3), round(float(hi), 3)), round(float((per <= 0).mean()), 4)
    when = pl.from_epoch("close_ms", time_unit="ms")
    quarter = pl.format("{}Q{}", when.dt.year(), when.dt.quarter())
    quarters = [Quarter(q, int(e), round(100 * w / c, 3) if c else None) for q, e, w, c in
                by_ev.with_columns(q=quarter).group_by("q").agg(pl.len(), pl.col("w").sum(), pl.col("qty").sum())
                .sort("q").iter_rows()]
    total_abs = float(by_ev["w"].abs().sum())
    top = float(by_ev["w"].abs().max()) / total_abs if by_ev.height and total_abs else None
    cap = usd = None
    if "capacity" in df.columns and days:
        cap = float(df["capacity"].fill_null(0).sum()) / days
        usd = cap * edge / 100 if edge is not None else None
    r = Result(n, by_ev.height, contracts, days, None if edge is None else round(edge, 3), ci, p_le0,
               round(float(df["w"].sum()), 2), float((df["pnl"] > 0).mean()) if n else None, quarters,
               None if top is None else round(top, 3), cap, usd, _calibration(df))
    if top is not None and top > CONCENTRATION and by_ev.height > 1:
        r.warnings.append(f"one event carries {top:.0%} of |PnL|")
    if pass_:
        r.checks = checks(r, pass_)
        r.passed = all(c.ok for c in r.checks)
    return r


def _calibration(df: pl.DataFrame) -> Calibration | None:
    if "p_model" not in df.columns or "y" not in df.columns:
        return None
    cols = ["p_model", "y"] + (["p_market"] if "p_market" in df.columns else [])
    d = df.select(cols).drop_nulls()
    if d.is_empty():
        return None
    p, y = d["p_model"].to_numpy(), d["y"].to_numpy().astype(float)
    q = d["p_market"].to_numpy() if "p_market" in d.columns else None
    return Calibration(d.height, metrics.brier(p, y), None if q is None else metrics.brier(q, y),
                       metrics.log_loss(p, y), None if q is None else metrics.log_loss(q, y),
                       metrics.calibration(p, y))


# Pass criteria a spec can set under `[pass]`.
CRITERIA = {
    "min_events": "at least this many events",
    "min_edge_c": "mean net edge (¢/contract) at least this",
    "ci_low_c": "lower end of the 95% CI (¢/contract) at least this",
    "max_p_le0": "bootstrap P(edge <= 0) at most this",
    "min_usd_per_day": "capacity × edge ($/day) at least this",
    "recent_quarters_positive": "each of the last N quarters has positive edge",
}


def checks(r: Result, want: dict[str, Any]) -> list[Check]:
    unknown = set(want) - set(CRITERIA)
    if unknown:
        raise ValueError(f"unknown pass criteria {', '.join(sorted(unknown))}; known: {', '.join(CRITERIA)}")
    out = []
    for k, v in want.items():
        v = float(v)
        if k == "min_events":
            got: float | None = r.events
            ok = r.events >= v
        elif k == "min_edge_c":
            got = r.edge_c
            ok = got is not None and got >= v
        elif k == "ci_low_c":
            got = r.ci_c[0] if r.ci_c else None
            ok = got is not None and got >= v
        elif k == "max_p_le0":
            got = r.p_le0
            ok = got is not None and got <= v
        elif k == "min_usd_per_day":
            got = r.usd_per_day
            ok = got is not None and got >= v
        else:
            recent = r.quarters[-int(v):] if v else []
            got = sum((q.edge_c or 0) > 0 for q in recent)
            ok = len(recent) == int(v) and got == int(v)
        out.append(Check(k, v, got, bool(ok)))
    return out
