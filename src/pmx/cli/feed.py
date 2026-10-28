"""The viewer feed and its lifecycle: serve, up, down, tui."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pmx import config, lifecycle
from pmx.cli.common import die, engine_flags, engine_pidfile, on_signal, socket_for


@contextlib.asynccontextmanager
async def _serving(hub, sock: Path):
    """Listen on `sock` in this process and hold its pidfile until done; leave neither behind."""
    server = await hub.serve(sock)
    try:
        with lifecycle.pidfile(lifecycle.pidfile_for(sock)):
            yield
    finally:
        server.close()
        sock.unlink(missing_ok=True)


async def _serve_demo(args: argparse.Namespace, sock: Path, stop: asyncio.Event) -> None:
    from pmx.engine.engine import Tap
    from pmx.feed.demo import DemoEngine, LoadGen
    from pmx.feed.proto import Hello
    from pmx.feed.server import EngineLink, TapPublisher

    link = EngineLink(sock, Hello("kalshi", "demo", "demo"))
    tap = link.tap = Tap(link.deliver)
    if args.markets or args.rate:
        demo = LoadGen(TapPublisher(tap), markets=args.markets or 5_000, rate=args.rate or 20_000,
                       slots=0.02 if args.slots is None else args.slots)
        what = f"{len(demo.contracts):,} markets at {demo.rate:,.0f} events/s"
    else:
        demo = DemoEngine(TapPublisher(tap))
        what = "an invented session"
    link.snapshot = demo.market_snapshot
    with engine_pidfile(sock):
        logging.getLogger(__name__).info("demo: %s, viewers on %s", what, sock)
        await asyncio.gather(demo.run(stop), tap.run(stop), link.run(stop))


async def cmd_serve(args: argparse.Namespace) -> None:
    """The engine with no strategies (`pmx run` without trading), the demo, or a recorded day."""
    from pmx.cli.run import run_engine
    from pmx.connectors.kalshi.decode import KalshiDecoder
    from pmx.feed.hub import FeedHub
    from pmx.feed.proto import Hello
    from pmx.feed.sources import run_replay

    if not (args.demo or args.replay):
        await run_engine(args, strategies=False)
        return
    stop = asyncio.Event()
    on_signal(stop.set)
    sock = socket_for(args)
    if args.demo:
        await _serve_demo(args, sock, stop)
        return
    hub = FeedHub(Hello("kalshi", "replay", "replay"))
    async with _serving(hub, sock):
        await run_replay(config.data_root(), "kalshi", KalshiDecoder(), date.fromisoformat(args.replay), hub,
                         stop, speed=1.0 if args.speed is None else args.speed)


TUI_DIR = config.PROJECT / "tui"


def _tui_stale(binary: Path, src: Path = TUI_DIR) -> bool:
    """Missing, or older than any Rust source or manifest."""
    if not binary.exists():
        return True
    built = binary.stat().st_mtime
    return any(p.stat().st_mtime > built for p in [*src.glob("src/**/*.rs"), src / "Cargo.toml", src / "Cargo.lock"]
               if p.exists())


def serve_argv(args: argparse.Namespace) -> list[str]:
    """`pmx serve` / `pmx run` flags that reproduce this command's engine (for `up` and `tui`)."""
    out = ["--env", args.env] if args.env else []
    for flag in ("source", "config", "poll", "replay", "speed", "socket", "models", "markets", "rate", "slots",
                 "confirm"):
        if getattr(args, flag, None) is not None:
            out += [f"--{flag}", str(getattr(args, flag))]
    for flag in ("series", "tickers"):
        if getattr(args, flag, None):
            out += [f"--{flag}", *getattr(args, flag)]
    for flag in ("demo", "live"):
        if getattr(args, flag, False):
            out.append(f"--{flag}")
    return out


def _command(args: argparse.Namespace) -> str:
    """What `pmx up` starts: the engine with its strategies, unless it's the demo, a replay, or --feed-only."""
    return "serve" if args.demo or args.replay or getattr(args, "feed_only", False) else "run"


def _running(sock: Path) -> bool:
    from pmx.feed.hub import socket_alive

    return asyncio.run(socket_alive(sock))


def _start(args: argparse.Namespace, sock: Path, cmd: str = "serve") -> int:
    """Start `pmx <cmd>` headless in the background; returns its pid once the socket accepts."""
    log = _log(cmd)
    if cmd == "serve":  # no strategies: nothing live to allow or confirm
        args = argparse.Namespace(**{**vars(args), "live": False, "confirm": None})
    argv = serve_argv(args)
    proc = lifecycle.start_detached([sys.executable, "-m", "pmx", cmd, *argv], log)
    if not lifecycle.wait_for(lambda: proc.poll() is not None or _running(sock), 60.0) or proc.poll() is not None:
        tail = "".join(log.read_text().splitlines(keepends=True)[-15:]) if log.exists() else ""
        die(f"pmx failed to start (log: {log}):\n{tail}")
    return proc.pid


def _log(cmd: str) -> Path:
    """Where a background `pmx <cmd>`'s output goes (`pmx run` also logs to data/logs/run-<date>.log)."""
    return config.data_root() / "logs" / f"{cmd}.log"


def _flags(args: argparse.Namespace) -> str:
    """The flags that name this instance, for the hints we print."""
    return (" --demo" if args.demo else "") + (f" --socket {args.socket}" if args.socket else "")


def _where(sock: Path) -> str:
    pid = lifecycle.read_pid(lifecycle.pidfile_for(sock))
    return f"pid {pid}, {sock}" if pid else str(sock)


def cmd_up(args: argparse.Namespace) -> None:
    sock = socket_for(args)
    if _running(sock):
        print(f"pmx is already running ({_where(sock)})")
        return
    cmd = _command(args)
    pid = _start(args, sock, cmd)
    log = config.data_root() / "logs" / f"run-{date.today()}.log" if cmd == "run" else _log(cmd)
    what = "the engine" if cmd == "run" else "the market-data feed" if not args.demo else "the demo"
    print(f"pmx is running in the background: {what} (pid {pid}, {sock})")
    print(f"  watch: pmx tui{_flags(args)}   stop: pmx down{_flags(args)}   log: {log}")


def cmd_down(args: argparse.Namespace) -> None:
    sock = socket_for(args)
    pid = lifecycle.read_pid(lifecycle.pidfile_for(sock))
    if pid is None:
        if _running(sock):
            die(f"another process is serving {sock} without a pidfile; stop it where it was started")
        print("pmx isn't running")
        return
    print(f"stopping pmx (pid {pid})…")
    if not lifecycle.stop(pid):
        die(f"process {pid} did not exit")
    print("stopped")
    from pmx.ops.guard import guard_pidfile, heartbeat_path

    guard = lifecycle.read_pid(guard_pidfile(sock))
    if guard is not None:  # last: it exits by itself after a clean stop, or cancels what a dirty one left
        if lifecycle.wait_for(lambda: not lifecycle.alive(guard), 15.0):
            print("pmx guard exited")
        elif not heartbeat_path(sock).exists():
            lifecycle.stop(guard)
            print("pmx guard stopped")
        else:
            print(f"pmx guard (pid {guard}) is still running: live orders may remain; see data/logs/guard.log")


def cmd_tui(args: argparse.Namespace) -> None:
    """Attach the viewer. Quitting it never stops pmx; if nothing is running, the feed is
    started headless first (never a trading engine — that is only ever started explicitly)."""
    import subprocess

    sock = socket_for(args)
    binary = TUI_DIR / "target" / "release" / "pmx-tui"
    if args.build or _tui_stale(binary):
        subprocess.run(["cargo", "build", "--release", "--manifest-path", str(TUI_DIR / "Cargo.toml")], check=True)
    if not _running(sock):
        print("pmx isn't running — starting it in the background…")
        _start(args, sock)
    code = subprocess.call([str(binary), "--socket", str(sock)])
    if _running(sock):
        print(f"pmx is still running ({_where(sock)}) — reattach: pmx tui{_flags(args)} · stop: pmx down{_flags(args)}")
    sys.exit(code)


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    feed = argparse.ArgumentParser(add_help=False, parents=[engine_flags()])
    feed.add_argument("--replay", metavar="YYYY-MM-DD", help="replay a recorded UTC day instead of connecting")
    feed.add_argument("--speed", type=float, default=None, help="replay speed multiplier, 0 = max (default 1)")
    target = argparse.ArgumentParser(add_help=False)
    target.add_argument("--socket", help="default $PMX_SOCK or data/pmx.sock (demo: data/pmx-demo.sock)")
    target.add_argument("--demo", action="store_true", help="an invented session: fake markets, slots, PnL")
    target.add_argument("--markets", type=int, help="demo load: this many invented markets (default 5000)")
    target.add_argument("--rate", type=float, help="demo load: market-data events/s in total (default 20000)")
    target.add_argument("--slots", type=float, help="demo load: share of markets with a slot (default 0.02)")
    sub.add_parser("serve", parents=[common, feed, target],
                   help="market data + models → the viewer feed, no strategies (foreground)")
    up = sub.add_parser("up", parents=[common, feed, target], help="pmx run headless in the background")
    up.add_argument("--feed-only", action="store_true", help="the market-data feed (pmx serve), no strategies")
    up.add_argument("--live", action="store_true", help="pmx run --live")
    up.add_argument("--confirm", metavar="ENV", help="pmx run --confirm: live on prod, headless")
    sub.add_parser("down", parents=[common, target], help="stop what pmx up started")
    t = sub.add_parser("tui", parents=[common, feed, target],
                       help="attach the Rust dashboard (starts pmx serve if nothing runs; q detaches)")
    t.add_argument("--build", action="store_true", help="rebuild the Rust binary (automatic when sources changed)")
    return {"serve": cmd_serve, "up": cmd_up, "down": cmd_down, "tui": cmd_tui}
