"""Operations: `pmx guard`, `pmx doctor`, `pmx ops install|uninstall|backup`."""

from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
from collections.abc import Callable
from pathlib import Path

from pmx import config, lifecycle
from pmx.cli.common import die, on_signal, socket_for, venue


async def cmd_guard(args: argparse.Namespace) -> None:
    """The dead-man watchdog for this socket's live engine (`pmx run --live` starts it)."""
    from pmx.control.server import control_socket
    from pmx.ops.guard import Guard, guard_pidfile, heartbeat_path

    sock = socket_for(args)
    instance = args.instance or (config.data_root() / "instance").read_text().strip()
    v = venue(args)
    if v.signer is None:
        die(f"pmx guard needs the {v.cfg.env} API key to cancel orders")
    stop = asyncio.Event()
    on_signal(stop.set)
    try:
        with lifecycle.pidfile(guard_pidfile(sock)):
            await Guard(v, instance=instance, heartbeat=heartbeat_path(sock), ctl=control_socket(sock),
                        guard_s=args.guard_s).run(stop)
    finally:
        await v.aclose()


def cmd_doctor(args: argparse.Namespace) -> None:
    from pmx.ops.doctor import checks

    bad = 0
    for what, ok, detail in checks():
        mark = "✓" if ok else "?" if ok is None else "✗"
        bad += ok is False
        print(f"  {mark} {what:<16} {detail}")
    print("all checks passed" if not bad else f"{bad} problem{'s' * (bad > 1)} found")
    if bad:
        sys.exit(1)


def _ops_section(path: str | None):
    from pmx.engine.config import ConfigError, OpsSection, load

    try:
        return load(path).ops
    except (ConfigError, ValueError, FileNotFoundError):
        return OpsSection()


def cmd_ops(args: argparse.Namespace) -> None:
    from pmx.ops import launchd

    if args.action == "backup":
        from pmx.ops.backup import backup

        ops = _ops_section(args.config)
        dest = args.to or ops.backup_dir
        if not dest:
            die("no backup destination; pass --to DIR or set [ops] backup_dir in config/strategies.toml")
        r = backup(config.data_root(), Path(dest).expanduser(), keep_days=args.keep_days or ops.backup_keep_days)
        print(f"raw: {r.raw_copied} files copied ({r.raw_bytes / 1e6:,.1f} MB), {r.raw_current} already current; "
              f"ledgers: {', '.join(p.name for p in r.ledgers) or 'none'}; {r.pruned} old ledger copies pruned")
        for p in r.problems:
            print(f"  ! {p}")
        if r.problems:
            sys.exit(1)
    elif shutil.which("launchctl") is None:
        die(f"pmx ops {args.action} manages launchd agents (macOS); elsewhere, run pmx under your own supervisor "
            "(e.g. a systemd user unit running `pmx run`, `pmx record`, `pmx ops backup`)")
    elif args.action == "install":
        if not (args.record or args.run or args.backup):
            die("choose what to install: --record, --run, and/or --backup")
        env = args.env or "prod"
        plists = launchd.agents(record=args.record, run=args.run, backup=args.backup, env=env, live=args.live)
        for p in launchd.install(plists):
            print(f"installed and loaded {p}")
        print("logs: data/logs/launchd-*.log · remove: pmx ops uninstall")
    elif args.action == "uninstall":
        names = args.names or launchd.installed()
        removed = launchd.uninstall(names)
        print("\n".join(f"unloaded and removed {p}" for p in removed) or "no pmx agents installed")


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    g = sub.add_parser("guard", parents=[common], help="live engine dead-man watchdog (pmx run --live starts it)")
    g.add_argument("--socket", help="the engine's feed socket (default data/pmx.sock)")
    g.add_argument("--instance", help="the engine instance (default data/instance)")
    g.add_argument("--guard-s", type=float, default=10.0, help="seconds without a heartbeat before canceling")
    sub.add_parser("doctor", parents=[common], help="check the setup: venv vs uv.lock, TUI, keys, clock, disk")
    o = sub.add_parser("ops", parents=[common], help="launchd agents (install/uninstall) and backups")
    o.add_argument("action", choices=["install", "uninstall", "backup"])
    o.add_argument("names", nargs="*", help="uninstall: which agents (default all)")
    o.add_argument("--record", action="store_true", help="install: pmx record, restarted on crash")
    o.add_argument("--run", action="store_true", help="install: pmx run, restarted on crash")
    o.add_argument("--backup", action="store_true", help="install: pmx ops backup, daily at 04:30")
    o.add_argument("--live", action="store_true", help="install: the run agent trades live (prod: --confirm prod)")
    o.add_argument("--to", help="backup: destination directory (default [ops] backup_dir)")
    o.add_argument("--keep-days", type=int, help="backup: ledger copies to keep (default [ops] backup_keep_days)")
    o.add_argument("--config", help="default config/strategies.toml")
    return {"guard": cmd_guard, "doctor": cmd_doctor, "ops": cmd_ops}
