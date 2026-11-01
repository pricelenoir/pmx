"""Text reports for experiment runs and the workspace status — short enough to read in a terminal."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pmx.research.windows import Stage, Windows

if TYPE_CHECKING:
    from pmx.research.runner import VariantRun
    from pmx.research.spec import Spec
    from pmx.research.workspace import Experiment, Workspace


def fmt_params(p: dict[str, Any]) -> str:
    return " ".join(f"{k}={json.dumps(v, separators=(',', ':'))}" for k, v in p.items()) or "(defaults)"


def _num(x: float | None, fmt: str = "+.2f") -> str:
    return "-" if x is None else format(x, fmt)


def format_run(e: Experiment, spec: Spec, stage: Stage, windows: Windows, runs: list[VariantRun], *,
               state: dict, trials: int, trials_all: int, holdout_runs: int) -> str:
    amend = len(state.get("amendments", []))
    out = [f"{e.name} — {stage}", spec.title,
           f"judged on {windows.describe(stage)} · lock {state['locks'][stage]['hash'][:12]} · amendments {amend}"
           f" · trials: {trials} here, {trials_all} research-wide", ""]
    if stage == "holdout" and holdout_runs > 1:
        out += [f"!! the holdout has now run {holdout_runs} times: it is no longer out of sample", ""]
    w = max(24, *(len(fmt_params(v.params)) for v in runs))
    out.append(f"{'variant':<{w}} {'events':>7} {'trades':>7} {'edge ¢':>8} {'95% CI ¢':>17} {'P≤0':>6} {'win':>5} "
               f"{'$/day':>8} {'qtrs +':>7}  pass")
    for v in runs:
        r = v.result
        ci = f"[{r.ci_c[0]:+.2f}, {r.ci_c[1]:+.2f}]" if r.ci_c else "-"
        pos = f"{sum((q.edge_c or 0) > 0 for q in r.quarters)}/{len(r.quarters)}" if r.quarters else "-"
        ok = "" if r.passed is None else ("yes" if r.passed else "no")
        out.append(f"{fmt_params(v.params):<{w}} {r.events:>7} {r.trades:>7} {_num(r.edge_c):>8} {ci:>17} "
                   f"{_num(r.p_le0, '.3f'):>6} {_num(r.win_rate, '.2f'):>5} {_num(r.usd_per_day, ',.2f'):>8} "
                   f"{pos:>7}  {ok}")
    if len(runs) > 1:
        out += ["", f"Best of {len(runs)} is biased upward: discovery picks a variant, validation judges it. "
                    "Prefer a variant whose neighbours also work."]
    if len(runs) == 1:
        r = runs[0].result
        if r.quarters:
            out += ["", "by quarter (events, edge ¢): " + "  ".join(f"{q.quarter} {q.events} {_num(q.edge_c)}"
                                                                   for q in r.quarters)]
        if r.calibration:
            c = r.calibration
            mk = f", market {c.brier_market:.4f}" if c.brier_market is not None else ""
            out.append(f"calibration (n={c.n}): Brier model {c.brier_model:.4f}{mk}")
        for c in r.checks:
            got = "-" if c.got is None else f"{c.got:g}"
            out.append(f"  {'✓' if c.ok else '✗'} {c.name}: want {c.want:g}, got {got}")
    for v in runs:
        for msg in v.result.warnings:
            out.append(f"warning ({fmt_params(v.params)}): {msg}")
        if v.outside:
            out.append(f"note ({fmt_params(v.params)}): {v.outside} rows outside the judged window, not scored")
    return "\n".join(out) + "\n"


def format_status(ws: Workspace) -> str:
    exps = ws.all()
    w = ws.windows()
    out = [f"windows: discovery < {w.discovery_end} ≤ validation < {w.validation_end} ≤ holdout",
           f"trials research-wide: {ws.trial_count()}", ""]
    if not exps:
        return "\n".join(out + ["no experiments yet: `pmx research new SLUG`"]) + "\n"
    n = max(len(e.name) for e in exps)
    for e in exps:
        st = e.state
        last = st["runs"][-1]["summary"] if st["runs"] else ""
        amend = f" · {len(st['amendments'])} amendments" if st["amendments"] else ""
        out.append(f"{e.name:<{n}}  {st['status']:<10} {last}{amend}")
    return "\n".join(out) + "\n"
