"""`pmx doctor`: is this machine set up to run pmx reproducibly? The venv matches `uv.lock`, the TUI
builds, keys are where `.env` says and private, the clock is synced, the data disk has room."""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

from pmx import config

type Check = tuple[str, bool | None, str]  # (what, ok — None when it can't be checked, detail)


def _run(argv: list[str], timeout: float = 60.0) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout, cwd=config.PROJECT)
    except (OSError, subprocess.SubprocessError):
        return None


def venv_matches_lock() -> Check:
    if not (config.PROJECT / "uv.lock").exists():
        return "venv = uv.lock", False, "no uv.lock (run `uv lock`)"
    if shutil.which("uv") is None:
        return "venv = uv.lock", None, "uv isn't installed"
    r = _run(["uv", "sync", "--locked", "--dry-run", "--python", str(config.PROJECT / ".venv")])
    if r is None or r.returncode != 0:
        return "venv = uv.lock", False, (r.stderr.strip().splitlines() or ["uv sync failed"])[-1] if r else "uv failed"
    changes = [line.strip() for line in r.stderr.splitlines() + r.stdout.splitlines()
               if line.strip()[:2] in ("- ", "+ ", "~ ") and " pmx" not in f" {line.strip()[2:]}"]
    return ("venv = uv.lock", not changes,
            "matches" if not changes else f"{len(changes)} differences: {', '.join(changes[:4])} (uv sync)")


def tui_builds() -> Check:
    if shutil.which("cargo") is None:
        return "TUI toolchain", False, "cargo isn't installed (rustup)"
    binary = config.PROJECT / "tui" / "target" / "release" / "pmx-tui"
    if not binary.exists():
        return "TUI toolchain", None, "cargo found; the TUI isn't built yet (pmx tui builds it)"
    return "TUI toolchain", True, f"cargo found; {binary.relative_to(config.PROJECT)} built"


def keys() -> list[Check]:
    out: list[Check] = []
    for env in ("demo", "prod"):
        key_id = os.environ.get(f"KALSHI_{env.upper()}_API_KEY_ID")
        path = os.environ.get(f"KALSHI_{env.upper()}_PRIVATE_KEY_PATH")
        if not key_id:
            out.append((f"{env} key", None, "no key ID in .env (pmx setkey --env " + env + ")"))
            continue
        p = Path(path).expanduser() if path else None
        if p is None or not p.exists():
            out.append((f"{env} key", False, f"key ID set but the private key file is missing ({path})"))
            continue
        mode = stat.S_IMODE(p.stat().st_mode)
        out.append((f"{env} key", mode & 0o077 == 0,
                    f"{p} ({oct(mode)})" + ("" if mode & 0o077 == 0 else " — chmod 600 it")))
    return out


def clock(max_ms: float = 250.0) -> Check:
    if shutil.which("sntp") is None:
        return "clock", None, "sntp isn't available"
    r = _run(["sntp", "-t", "3", "time.apple.com"], timeout=15)
    m = re.search(r"([+-]\d+\.\d+)\s+\+/-", (r.stdout + r.stderr) if r else "")
    if m is None:
        return "clock", None, "couldn't reach time.apple.com"
    off = float(m[1]) * 1000
    return "clock", abs(off) <= max_ms, f"{off:+.0f} ms vs time.apple.com" + ("" if abs(off) <= max_ms else
                                                                                  " — sync the clock")


def disk(min_gb: float = 5.0) -> Check:
    data = config.data_root()
    data.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(data).free / 1e9
    writable = os.access(data, os.W_OK)
    return "data disk", free >= min_gb and writable, f"{data}: {free:.0f} GB free" + ("" if writable else
                                                                                         ", NOT writable")


def checks() -> list[Check]:
    return [venv_matches_lock(), tui_builds(), *keys(), clock(), disk()]
