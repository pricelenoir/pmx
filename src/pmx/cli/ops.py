"""Operations: `pmx guard`, `pmx doctor`, `pmx ops install|uninstall|backup`."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
from collections.abc import Callable
from pathlib import Path

from pmx import config, lifecycle
from pmx.cli import style
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
    print(style.heading("pmx doctor"))
    for what, ok, detail in checks():
        bad += ok is False
        print(f"  {style.mark(ok)} {style.pad(what, 16)} {style.red(detail) if ok is False else detail}")
    print(style.green("all checks passed") if not bad else style.red(f"{bad} problem{'s' * (bad > 1)} found"))
    if bad:
        sys.exit(1)


def _ops_section(path: str | None):
    from pmx.engine.config import ConfigError, OpsSection, load

    try:
        return load(path).ops
    except (ConfigError, ValueError, FileNotFoundError):
        return OpsSection()


def _supervisor():
    """The service manager here: launchd (macOS) or systemd user services (Linux)."""
    from pmx.ops import launchd, systemd

    if shutil.which("launchctl") is not None:
        return launchd
    if shutil.which("systemctl") is not None:
        return systemd
    die("pmx ops install manages launchd agents (macOS) or systemd user services (Linux); neither is here, so run "
        "pmx under your own supervisor (`pmx record`, `pmx run`, and `pmx ops backup` daily)")


def _backup(args: argparse.Namespace) -> None:
    import subprocess

    from pmx.ops.backup import backup

    ops = _ops_section(args.config)
    dest = args.to or ops.backup_dir
    if not dest:
        die("no backup destination; pass --to DIR or set [ops] backup_dir in config/strategies.toml")
    dest_path = Path(dest).expanduser()
    r = backup(config.data_root(), dest_path, keep_days=args.keep_days or ops.backup_keep_days,
               folders=[config.research_root(), config.PROJECT / "planning"])
    print(f"raw: {r.raw_copied} files copied ({r.raw_bytes / 1e6:,.1f} MB), {r.raw_current} already current; "
          f"ledgers: {', '.join(p.name for p in r.ledgers) or 'none'}; {r.pruned} old ledger copies pruned; "
          f"research + planning: {r.folders_copied} files copied")
    for p in r.problems:
        print(f"  ! {p}")
    if r.problems:
        sys.exit(1)
    if ops.backup_sync:
        argv = [a.replace("{dir}", str(dest_path)) for a in ops.backup_sync]
        print(f"sync: {argv[0]} …")
        try:
            code = subprocess.run(argv).returncode
        except OSError as e:
            die(f"backup_sync couldn't start: {e}")
        if code:
            die(f"backup_sync exited {code}; the local copy at {dest_path} is complete")


def cmd_ops(args: argparse.Namespace) -> None:
    if args.action == "backup":
        _backup(args)
        return
    if args.action == "alert-test":
        from pmx.ops.notify import channels, notify

        notify("pmx", f"test alert from {os.uname().nodename}")
        print(f"sent a test alert to: {', '.join(channels())} (a webhook is set with PMX_ALERT_WEBHOOK in .env)")
        return
    sup = _supervisor()
    kind = "launchd agent" if sup.__name__.endswith("launchd") else "systemd user service"
    if args.action == "install":
        if not (args.record or args.run or args.backup):
            die("choose what to install: --record, --run, and/or --backup")
        env = args.env or "prod"
        installed = sup.installed()
        # One recorder: with the record service there, the engine doesn't record too (replays would double frames).
        run_args = ["--no-record"] if args.record or "record" in installed else []
        if sup.__name__.endswith("launchd"):
            written = sup.install(sup.agents(record=args.record, run=args.run, backup=args.backup, env=env,
                                             live=args.live, run_args=run_args))
            logs = "data/logs/launchd-*.log"
        else:
            written = sup.install(sup.units(record=args.record, run=args.run, backup=args.backup, env=env,
                                            live=args.live, run_args=run_args))
            logs = "journalctl --user -u 'pmx-*' -f (and data/logs/)"
            if sup.lingering() is False:
                print("! these start at boot only with lingering on: sudo loginctl enable-linger $USER")
        for p in written:
            print(f"installed and started {p}")
        if args.record and not args.run and "run" in installed:
            print("! the run service records too: reinstall it (pmx ops install --run) so only the recorder records")
        print(f"logs: {logs} · state: pmx ops status · remove: pmx ops uninstall")
    elif args.action == "uninstall":
        names = args.names or sup.installed()
        removed = sup.uninstall(names)
        print("\n".join(f"stopped and removed {p}" for p in removed) or f"no pmx {kind}s installed")
    elif args.action == "status":
        names = sup.installed()
        if not names:
            print(f"no pmx {kind}s installed (pmx ops install --record --run --backup)")
            return
        for name, state in sup.states(names).items():
            print(f"  {style.dot(state)} {name:<8} {style.word(state)}")
        if sup.__name__.endswith("systemd") and sup.lingering() is False:
            print("! lingering is off: services stop when you log out (sudo loginctl enable-linger $USER)")


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    g = sub.add_parser("guard", parents=[common], help="live engine dead-man watchdog (pmx run --live starts it)")
    g.add_argument("--socket", help="the engine's feed socket (default data/pmx.sock)")
    g.add_argument("--instance", help="the engine instance (default data/instance)")
    g.add_argument("--guard-s", type=float, default=10.0, help="seconds without a heartbeat before canceling")
    sub.add_parser("doctor", parents=[common], help="check the setup: venv vs uv.lock, TUI, keys, clock, disk")
    o = sub.add_parser("ops", parents=[common],
                       help="services (launchd or systemd: install/uninstall/status), backups, alert-test")
    o.add_argument("action", choices=["install", "uninstall", "status", "backup", "alert-test"])
    o.add_argument("names", nargs="*", help="uninstall: which services (default all)")
    o.add_argument("--record", action="store_true", help="install: pmx record, restarted on crash")
    o.add_argument("--run", action="store_true", help="install: pmx run, restarted on crash")
    o.add_argument("--backup", action="store_true", help="install: pmx ops backup, daily at 04:30")
    o.add_argument("--live", action="store_true", help="install: the run agent trades live (prod: --confirm prod)")
    o.add_argument("--to", help="backup: destination directory (default [ops] backup_dir)")
    o.add_argument("--keep-days", type=int, help="backup: ledger copies to keep (default [ops] backup_keep_days)")
    o.add_argument("--config", help="default config/strategies.toml")
    return {"guard": cmd_guard, "doctor": cmd_doctor, "ops": cmd_ops}
