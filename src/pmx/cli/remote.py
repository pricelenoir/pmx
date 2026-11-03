"""`pmx remote HOST …`: watch and control pmx on another machine over SSH (a Linux VM, say).

  pmx remote HOST tui            the dashboard here, on the remote engine's feed (its socket forwarded over SSH)
  pmx remote HOST logs [NAME]    follow the remote services' logs (journalctl; NAME: record, run, backup)
  pmx remote HOST <command …>    run any pmx command there: status, positions, risk, halt, resume, perf, …

HOST is anything `ssh` accepts (an alias from ~/.ssh/config is easiest); pmx lives in `--dir` there (default
$PMX_REMOTE_DIR or ~/pmx). Nothing listens on the network: the feed is a Unix socket, forwarded by SSH."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from pmx.cli.common import die

SOCKET_WAIT_S = 15.0


def remote_cd(rdir: str) -> str:
    """`cd` to the remote pmx directory, keeping a leading ~ for the remote shell to expand."""
    if rdir == "~" or rdir.startswith("~/"):
        return "cd ~" + (f"/{shlex.quote(rdir[2:])}" if rdir[2:] else "")
    return f"cd {shlex.quote(rdir)}"


def ssh_argv(host: str, command: str, *, tty: bool = False) -> list[str]:
    return ["ssh", *(["-t"] if tty else []), host, command]


def pmx_command(rdir: str, args: list[str]) -> str:
    return f"{remote_cd(rdir)} && exec .venv/bin/pmx {shlex.join(args)}"


def logs_command(name: str | None) -> str:
    unit = f"pmx-{name}" if name else "pmx-*"
    return f"journalctl --user -u {shlex.quote(unit)} -n 200 -f"


def _ssh_output(host: str, command: str) -> str:
    r = subprocess.run(["ssh", host, command], capture_output=True, text=True)
    if r.returncode:
        die(f"ssh {host} failed: {(r.stderr.strip().splitlines() or ['no output'])[-1]}")
    return r.stdout.strip()


def _tui(host: str, rdir: str, remote_socket: str | None) -> int:
    from pmx.cli.feed import TUI_DIR, _tui_stale

    sock = remote_socket or _ssh_output(host, f"{remote_cd(rdir)} && pwd") + "/data/pmx.sock"
    if _ssh_output(host, f"test -S {shlex.quote(sock)} && echo yes || echo no") != "yes":
        die(f"pmx isn't running on {host} (no feed socket at {sock}); start it there: pmx remote {host} up --env …, "
            f"or the services (pmx ops install)")
    binary = TUI_DIR / "target" / "release" / "pmx-tui"
    if _tui_stale(binary):
        subprocess.run(["cargo", "build", "--release", "--manifest-path", str(TUI_DIR / "Cargo.toml")], check=True)
    tmp = Path(tempfile.mkdtemp(prefix="pmx-remote-"))
    local = tmp / "pmx.sock"
    tunnel = subprocess.Popen(["ssh", "-N", "-o", "ExitOnForwardFailure=yes", "-o", "StreamLocalBindUnlink=yes",
                               "-L", f"{local}:{sock}", host], stdin=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + SOCKET_WAIT_S
        while not local.exists():
            if tunnel.poll() is not None:
                die(f"the SSH tunnel to {host} closed (exit {tunnel.returncode})")
            if time.monotonic() > deadline:
                die(f"the SSH tunnel to {host} didn't come up in {SOCKET_WAIT_S:.0f} s")
            time.sleep(0.1)
        return subprocess.call([str(binary), "--socket", str(local)])
    finally:
        tunnel.terminate()
        try:
            tunnel.wait(timeout=5)
        except subprocess.TimeoutExpired:
            tunnel.kill()
        shutil.rmtree(tmp, ignore_errors=True)


def cmd_remote(args: argparse.Namespace) -> None:
    if shutil.which("ssh") is None:
        die("ssh isn't installed")
    rest = list(args.command)
    if not rest:
        die(f"what to run on {args.host}? e.g. pmx remote {args.host} tui | status | logs")
    rdir = args.dir or os.environ.get("PMX_REMOTE_DIR", "~/pmx")
    if rest[0] == "tui":
        code = _tui(args.host, rdir, args.remote_socket)
    elif rest[0] == "logs":
        code = subprocess.call(ssh_argv(args.host, logs_command(rest[1] if len(rest) > 1 else None)))
    else:
        code = subprocess.call(ssh_argv(args.host, pmx_command(rdir, rest), tty=sys.stdin.isatty()))
    sys.exit(code)


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    r = sub.add_parser("remote", parents=[common],
                       help="watch and control pmx on another machine over SSH: tui, logs, any command",
                       description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    r.add_argument("--dir", help="pmx's directory on the host (default $PMX_REMOTE_DIR or ~/pmx)")
    r.add_argument("--remote-socket", help="tui: the remote feed socket (default <dir>/data/pmx.sock)")
    r.add_argument("host", help="an ssh destination, e.g. an alias from ~/.ssh/config")
    r.add_argument("command", nargs=argparse.REMAINDER, help="tui | logs [NAME] | any pmx command")
    return {"remote": cmd_remote}
