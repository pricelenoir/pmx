"""Backtests: `pmx backtest` runs one; `pmx backtest report | compare | view` reads them."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

from pmx import config
from pmx.cli.common import die


def _usd(m: int) -> str:
    return f"{'-' if m < 0 else '+' if m > 0 else ''}${abs(m) / 1e6:,.2f}"


def _run_dir(ref: str) -> Path:
    p = Path(ref)
    return p if p.is_dir() else config.data_root() / "backtests" / ref


def _report(ref: str, args: argparse.Namespace):  # noqa: ANN202
    """A run id / path, or `paper:FROM[:TO]` — the live paper ledger over local days."""
    from pmx.backtest.report import from_ledger, local_day_ms, report

    env = args.env or "prod"
    if ref.startswith("paper:") or ref == "paper":
        days = ref.split(":")[1:] or [args.start or "", args.end or ""]
        start, end = days[0] or None, (days[1] if len(days) > 1 and days[1] else days[0]) or None
        path = Path(args.ledger) if args.ledger else config.data_root() / "ledger.sqlite"
        return from_ledger(path, env=env, mode="paper", start_ms=local_day_ms(start) if start else None,
                           end_ms=local_day_ms(end, end=True) if end else None,
                           title=f"paper {start or 'start'}..{end or 'now'}", meta={"ledger": str(path)})
    run_dir = _run_dir(ref)
    if not (run_dir / "ledger.sqlite").exists():
        die(f"no backtest at {run_dir}")
    return report(run_dir, env=env)


def cmd_backtest(args: argparse.Namespace) -> None:
    if args.action == "report":
        from pmx.backtest.report import markdown, write

        ref = "paper" if args.paper else (args.runs[0] if args.runs else None)
        if ref is None:
            die("usage: pmx backtest report RUN_ID, or pmx backtest report --paper --from DAY [--to DAY]")
        r = _report(ref, args)
        if ref != "paper":
            write(r, _run_dir(ref))
        print(markdown(r))
        return
    if args.action == "compare":
        from pmx.backtest.report import compare

        if len(args.runs) != 2:
            die("usage: pmx backtest compare RUN_A RUN_B (each a run ID, or paper:YYYY-MM-DD[:YYYY-MM-DD])")
        print(compare(_report(args.runs[0], args), _report(args.runs[1], args)))
        return
    if args.action == "view":
        if len(args.runs) != 1:
            die("usage: pmx backtest view RUN_ID")
        _view(_run_dir(args.runs[0]), args)
        return
    from pmx.backtest.runner import Runner, day, default_run_id, instant
    from pmx.engine.config import ConfigError, load

    try:
        cfg = load(args.config)
    except ConfigError as e:
        die(str(e))
    history = None
    start_ms = end_ms = None  # instants inside the first / last day
    if args.nfl_history:
        from pmx.backtest.nfl_history import NflHistory

        seasons = [int(s) for s in args.seasons.split(",")]
        try:
            history = NflHistory(config.data_root(), seasons=seasons, latency_s=args.latency, depth=args.depth,
                                 start=day(args.start) if args.start else None,
                                 end=day(args.end) if args.end else None)
        except FileNotFoundError as e:
            die(str(e))
        if history.games.is_empty():
            die("no linked games in that range; sync them with `pmx data kalshi-history`")
        start, end = day(history.games["day"].min()), day(history.games["day"].max())
    elif not args.start:
        die("--from is required (or --nfl-history)")
    else:
        try:
            (start, start_ms), (end, end_ms) = instant(args.start), instant(args.end or args.start.partition("T")[0])
        except ValueError as e:
            die(str(e))
    run_id = args.run_id or (default_run_id(cfg, start, end) + ("-nflhist" if history else "")
                             + (f"-{(start_ms or 0) // 1000}-{(end_ms or 0) // 1000}" if start_ms or end_ms else ""))
    run_dir = config.data_root() / "backtests" / run_id
    sources = [s.strip() for s in args.sources.split(",") if s.strip()] if args.sources else None
    try:
        runner = Runner(cfg, start=start, end=end, run_dir=run_dir, sources=sources, run_id=run_id,
                        history=history.streams if history else None, fidelity="candle" if history else "book",
                        start_ms=start_ms, end_ms=end_ms,
                        **({"timer_ms": 5_000, "idle_after_s": 120, "idle_step_s": 60} if history else {}))
    except ValueError as e:
        die(str(e))
    if history:
        print(f"candle fidelity: {history.games.height} NFL games from Kalshi 1-minute history + nflverse plays "
              f"({args.latency:g} s latency) — resting-order fills are approximate")
    if not runner.sources:
        die(f"nothing recorded for {start}..{end} under {config.data_root() / 'raw'}"
            + (f" (sources {', '.join(sources)})" if sources else ""))
    print(f"backtest {run_id}: {start}..{end}, replaying " + ", ".join(f"{s.raw} as {s.name}" for s in runner.sources))
    s = runner.run()
    print(f"{s.inputs:,} inputs + {s.timers:,} timers in {s.wall_s:.1f} s ({s.inputs_per_s:,.0f}/s); "
          f"{s.orders:,} orders, {s.fills:,} fills" + (f"; {s.clamped:,} out-of-order inputs" if s.clamped else ""))
    for sid, r in s.strategies.items():
        print(f"  {sid:<20} equity {_usd(r.equity):>10}  realized {_usd(r.realized):>10}  fees {_usd(r.fees):>9}  "
              f"{r.orders:,} orders  {r.fills:,} fills  {s.logs.get(sid, 0):,} log lines")
    print(f"results: {run_dir} (ledger.sqlite, config.toml, summary.json, log.txt)")


def _view(run_dir: Path, args: argparse.Namespace) -> None:
    """The run in the TUI's Portfolio and Strategies views, from a temporary socket."""
    import asyncio
    import subprocess

    from pmx.backtest.report import serve_view, view_messages
    from pmx.cli.feed import TUI_DIR, _tui_stale

    if not (run_dir / "ledger.sqlite").exists():
        die(f"no backtest at {run_dir}")
    msgs = view_messages(run_dir, env=args.env or "prod")
    binary = TUI_DIR / "target" / "release" / "pmx-tui"
    if _tui_stale(binary):
        subprocess.run(["cargo", "build", "--release", "--manifest-path", str(TUI_DIR / "Cargo.toml")], check=True)
    sock = config.data_root() / "pmx-view.sock"

    async def go() -> int:
        stop = asyncio.Event()
        server = asyncio.create_task(serve_view(msgs, sock, stop))
        await asyncio.sleep(0.1)
        proc = await asyncio.create_subprocess_exec(str(binary), "--socket", str(sock))
        code = await proc.wait()
        stop.set()
        await server
        return code

    sys.exit(asyncio.run(go()))


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    b = sub.add_parser("backtest", parents=[common], help="run a backtest (→ data/backtests/<run id>/), or report, "
                                                         "compare, view runs")
    b.add_argument("action", nargs="?", choices=["run", "report", "compare", "view"], default="run")
    b.add_argument("runs", nargs="*", help="report/view: a run id (or path); compare: two (or paper:FROM[:TO])")
    b.add_argument("--paper", action="store_true", help="report: the live paper ledger over --from..--to")
    b.add_argument("--ledger", help="report --paper: default data/ledger.sqlite")
    b.add_argument("--config", help="default config/strategies.toml")
    b.add_argument("--from", dest="start", metavar="YYYY-MM-DD",
                   help="first UTC day, or an instant (2026-10-19T08:31:10Z) — required unless --nfl-history")
    b.add_argument("--to", dest="end", metavar="YYYY-MM-DD", help="last UTC day or an instant (default: the first day)")
    b.add_argument("--sources", help="comma-separated engine sources to replay (default: all recorded)")
    b.add_argument("--run-id", help="default bt-<from>-<to>-<config hash>")
    b.add_argument("--nfl-history", action="store_true", help="NFL games rebuilt from Kalshi candles + nflverse")
    b.add_argument("--seasons", default="2025,2026", help="--nfl-history: NFL seasons")
    b.add_argument("--latency", type=float, default=15.0, help="--nfl-history: seconds from a play to its state")
    b.add_argument("--depth", type=int, default=500, help="--nfl-history: contracts per synthetic book level")
    return {"backtest": cmd_backtest}
