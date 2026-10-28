"""Performance measurements: `pmx bench feed`, `pmx bench core`, `pmx bench engine` (the whole `pmx run`
engine: risk, orders, fills, portfolio), `pmx bench soak`."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from collections.abc import Callable
from pathlib import Path

from pmx import config


async def cmd_bench_feed(args: argparse.Namespace) -> None:
    from pmx.bench.feed import bench_feed, report

    sock = Path(args.socket) if args.socket else config.data_root() / "pmx-bench.sock"
    rows = await bench_feed(sock=sock, markets=args.markets, rate=args.rate, slots=args.slots, seconds=args.seconds,
                            warmup=args.warmup, viewers=tuple(int(v) for v in args.viewers.split(",")),
                            inprocess=args.inprocess)
    print(json.dumps(rows, indent=1) if args.json else report(rows))


async def cmd_bench_core(args: argparse.Namespace) -> None:
    from pmx.bench.core import bench_core, report

    sock = Path(args.socket) if args.socket else config.data_root() / "pmx-bench.sock"
    rows = await bench_core(sock=sock, markets=args.markets, events=args.events, models=args.models,
                            slots=not args.no_slots)
    print(json.dumps(rows, indent=1) if args.json else report(rows))


async def cmd_bench_engine(args: argparse.Namespace) -> None:
    from pmx.bench.engine import report, run_engine

    r = run_engine(markets=args.markets, events=args.events, rate=args.rate)
    print(json.dumps(r, indent=1) if args.json else report(r))


async def cmd_bench_soak(args: argparse.Namespace) -> None:
    from pmx.bench.soak import soak
    from pmx.cli.common import on_signal

    sock = Path(args.socket) if args.socket else config.data_root() / "pmx-soak.sock"
    out = Path(args.out) if args.out else config.data_root() / "soak.csv"
    task = asyncio.ensure_future(soak(sock=sock, out=out, hours=args.hours, markets=args.markets, rate=args.rate,
                                      slots=args.slots, every_s=args.every, viewer_pids=args.viewer_pid or []))
    on_signal(task.cancel)
    with contextlib.suppress(asyncio.CancelledError):
        await task
    print(f"samples in {out}")


async def cmd_bench(args: argparse.Namespace) -> None:
    await {"feed": cmd_bench_feed, "core": cmd_bench_core, "engine": cmd_bench_engine,
           "soak": cmd_bench_soak}[args.bench](args)


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    bench = sub.add_parser("bench", help="performance budgets under load").add_subparsers(dest="bench",
                                                                                                required=True)
    feed = bench.add_parser("feed", parents=[common], help="the viewer feed at a fixed event rate with 0/1/3 viewers")
    feed.add_argument("--markets", type=int, default=5_000)
    feed.add_argument("--rate", type=float, default=20_000, help="market-data events/s in total")
    feed.add_argument("--slots", type=float, default=0.02, help="share of markets with a slot")
    feed.add_argument("--seconds", type=float, default=10.0, help="measured window per case")
    feed.add_argument("--warmup", type=float, default=3.0)
    feed.add_argument("--viewers", default="0,1,3", help="viewer counts to measure, comma-separated")
    feed.add_argument("--inprocess", action="store_true", help="publish in the engine process instead (for comparison)")
    feed.add_argument("--socket", help="default data/pmx-bench.sock")
    feed.add_argument("--json", action="store_true")
    core = bench.add_parser("core", parents=[common], help="engine core dispatch latency, bare and with the tap")
    core.add_argument("--markets", type=int, default=5_000)
    core.add_argument("--events", type=int, default=200_000, help="market-data inputs to dispatch")
    core.add_argument("--models", type=int, default=2, help="market_mid instances")
    core.add_argument("--no-slots", action="store_true", help="no noop slot per market (the dispatch core alone)")
    core.add_argument("--socket", help="default data/pmx-bench.sock")
    core.add_argument("--json", action="store_true")
    eng = bench.add_parser("engine", parents=[common], help="the whole engine: risk, orders, fills, portfolio")
    eng.add_argument("--markets", type=int, default=1_000)
    eng.add_argument("--events", type=int, default=100_000, help="market-data inputs to dispatch")
    eng.add_argument("--rate", type=float, default=5_000, help="generated events per simulated second")
    eng.add_argument("--json", action="store_true")
    soak = bench.add_parser("soak", parents=[common], help="hours of load; memory, files, and queues must plateau")
    soak.add_argument("--hours", type=float, default=4.0)
    soak.add_argument("--markets", type=int, default=5_000)
    soak.add_argument("--rate", type=float, default=20_000)
    soak.add_argument("--slots", type=float, default=0.02)
    soak.add_argument("--every", type=float, default=60.0, help="seconds between samples")
    soak.add_argument("--viewer-pid", type=int, action="append", help="also sample this viewer (e.g. a pmx-tui)")
    soak.add_argument("--socket", help="default data/pmx-soak.sock")
    soak.add_argument("--out", help="CSV of samples (default data/soak.csv)")
    return {"bench": cmd_bench}
