"""Running one stage of an experiment: check the lock, give `run.py` a catalog cut to the stage, score every
variant the same way, and record it (results/, trials.csv, state.json).

`run.py` defines `run(ctx, params) -> polars.DataFrame` with one row per trade (`evaluate.REQUIRED`); `ctx` holds the
stage's catalog, the spec, and helpers. Rows are judged on the stage's markets (by `close_ms`); earlier rows — e.g. a
model's training era in validation — are dropped from the score. A row decided at or after its market's close
(`t_ms >= close_ms`) is lookahead and fails the run."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import msgspec
import polars as pl

from pmx.research import report
from pmx.research.catalog import Catalog
from pmx.research.evaluate import REQUIRED, Result, evaluate
from pmx.research.panel import panel
from pmx.research.spec import Spec
from pmx.research.windows import Stage, Windows
from pmx.research.workspace import Experiment, ResearchError, Workspace, now_iso


@dataclass
class Context:
    catalog: Catalog
    stage: Stage
    spec: Spec
    windows: Windows

    @property
    def config(self) -> dict[str, Any]:
        """The spec's front-matter keys besides series/params/grid/variants_max/pass."""
        return self.spec.config

    def panel(self, series: str | list[str] | None = None, **kw: Any) -> pl.DataFrame:
        return panel(self.catalog, series or self.spec.series, **kw)


@dataclass
class VariantRun:
    params: dict[str, Any]
    result: Result
    outside: int  # rows outside the judged window (not scored)


def _load(e: Experiment):
    mod_spec = importlib.util.spec_from_file_location(f"pmx_experiment_{e.name.replace('-', '_')}", e.run_path)
    assert mod_spec and mod_spec.loader
    mod = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(mod)
    if not callable(getattr(mod, "run", None)):
        raise ResearchError(f"{e.run_path}: define run(ctx, params) -> polars.DataFrame")
    return mod


def _check(df: Any, e: Experiment, params: dict) -> pl.DataFrame:
    if not isinstance(df, pl.DataFrame):
        raise ResearchError(f"{e.name}: run() returned {type(df).__name__}, not a polars DataFrame")
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ResearchError(f"{e.name} {params}: run() rows need {', '.join(REQUIRED)}; missing {', '.join(missing)}")
    if "t_ms" in df.columns:
        late = df.filter(pl.col("t_ms") >= pl.col("close_ms")).height
        if late:
            raise ResearchError(f"{e.name} {params}: {late} rows decided at or after their market's close "
                                f"(t_ms >= close_ms): lookahead")
    return df


def summary(stage: str, runs: list[VariantRun]) -> str:
    def one(v: VariantRun) -> str:
        r = v.result
        ci = f" [{r.ci_c[0]:+.2f}, {r.ci_c[1]:+.2f}]" if r.ci_c else ""
        edge = "n/a" if r.edge_c is None else f"{r.edge_c:+.2f}¢"
        verdict = "" if r.passed is None else (" pass" if r.passed else " fail")
        return f"{edge}{ci}, {r.events} events{verdict}"

    if len(runs) == 1:
        return one(runs[0])
    scored = [v for v in runs if v.result.edge_c is not None]
    if not scored:
        return f"{len(runs)} variants, none scored"
    best = max(scored, key=lambda v: v.result.edge_c or 0)
    return f"{len(runs)} variants; best {one(best)} ({report.fmt_params(best.params)})"


def run_stage(ws: Workspace, e: Experiment, data_root: Path, *, stage: Stage | None = None,
              force: bool = False) -> tuple[list[VariantRun], str]:
    st = e.state
    status = st["status"]
    if status == "proposed":
        raise ResearchError(f"{e.name} needs approval first: `pmx research approve {e.name}` "
                            f"(run it yourself, in a terminal)")
    if status in ("promoted", "dead"):
        raise ResearchError(f"{e.name} is closed ({status})")
    current: Stage = "discovery" if status == "approved" else status
    stage = stage or current
    if stage != current:
        raise ResearchError(f"{e.name} is at {current}, so it runs {current}"
                            + (" (`pmx research advance` moves it on)" if stage != "discovery" else ""))
    fp = e.check_lock(stage)
    holdouts = sum(r["stage"] == "holdout" for r in st["runs"])
    if stage == "holdout" and holdouts and not force:
        raise ResearchError(f"{e.name}: the holdout already ran; it runs once (--force reruns it, and every "
                            f"report says so)")
    spec = e.spec()
    windows = ws.windows()
    cat = Catalog(data_root, windows, stage)
    mod = _load(e)
    lo, hi = windows.judged_ms(stage)
    runs: list[VariantRun] = []
    for params in spec.variants(stage):
        df = _check(mod.run(Context(cat, stage, spec, windows), dict(params)), e, params)
        judged = df
        if lo is not None:
            judged = judged.filter(pl.col("close_ms") >= lo)
        if hi is not None:
            judged = judged.filter(pl.col("close_ms") < hi)
        runs.append(VariantRun(params, evaluate(judged, pass_=spec.pass_), df.height - judged.height))

    at = now_iso()
    ws.log_trials([{
        "at": at, "experiment": e.name, "stage": stage, "variant": report.fmt_params(v.params), "hash": fp[:12],
        "events": v.result.events, "trades": v.result.trades, "edge_c": v.result.edge_c,
        "ci_low_c": v.result.ci_c[0] if v.result.ci_c else "", "ci_high_c": v.result.ci_c[1] if v.result.ci_c else "",
        "passed": "" if v.result.passed is None else v.result.passed} for v in runs])
    st["runs"].append({"stage": stage, "at": at, "hash": fp, "variants": len(runs), "forced": bool(force and holdouts),
                       "summary": summary(stage, runs)})
    st["status"] = stage
    e.save(st)

    text = report.format_run(e, spec, stage, windows, runs, state=st, trials=ws.trial_count(e.name),
                             trials_all=ws.trial_count(), holdout_runs=holdouts + (stage == "holdout"))
    e.results.mkdir(exist_ok=True)
    payload = {"experiment": e.name, "stage": stage, "at": at, "hash": fp,
               "variants": [{"params": v.params, "outside": v.outside, "result": msgspec.to_builtins(v.result)}
                            for v in runs]}
    stamp = at.replace(":", "").replace("-", "")
    for name in (f"{stage}.json", f"{stage}-{stamp}.json"):
        (e.results / name).write_text(json.dumps(payload, indent=2, default=str) + "\n")
    (e.results / f"{stage}.txt").write_text(text)
    return runs, text
