"""launchd user agents: `pmx record` and `pmx run` restart when they crash and start at login; the backup runs
daily. `pmx ops install` writes them to `~/Library/LaunchAgents` and loads them — only when you run it;
`pmx ops uninstall` unloads and removes them. A clean stop (`pmx down`, exit 0) isn't restarted."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

from pmx import config

AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
PREFIX = "com.pmx."


def agent(name: str, args: list[str], *, keep_alive: bool = True, daily_at: tuple[int, int] | None = None,
          data: Path | None = None) -> dict[str, Any]:
    data = data or config.data_root()
    log = str(data / "logs" / f"launchd-{name}.log")
    plist: dict[str, Any] = {
        "Label": PREFIX + name,
        "ProgramArguments": [sys.executable, "-m", "pmx", *args],
        "WorkingDirectory": str(config.PROJECT),
        "EnvironmentVariables": {"PMX_DATA": str(data), "PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        "StandardOutPath": log,
        "StandardErrorPath": log,
        "ProcessType": "Interactive",  # not throttled like a background job
    }
    if keep_alive:
        plist |= {"RunAtLoad": True, "KeepAlive": {"SuccessfulExit": False}, "ThrottleInterval": 10}
    if daily_at is not None:
        plist["StartCalendarInterval"] = {"Hour": daily_at[0], "Minute": daily_at[1]}
    return plist


def agents(*, record: bool, run: bool, backup: bool, env: str, live: bool = False, record_args: list[str] = (),
           run_args: list[str] = ()) -> dict[str, dict[str, Any]]:
    out = {}
    if record:
        out["record"] = agent("record", ["record", "--env", env, *record_args])
    if run:
        flags = ["--live", *(["--confirm", "prod"] if env == "prod" else [])] if live else []
        out["run"] = agent("run", ["run", "--env", env, *flags, *run_args])
    if backup:
        out["backup"] = agent("backup", ["ops", "backup"], keep_alive=False, daily_at=(4, 30))
    return out


def install(plists: dict[str, dict[str, Any]], *, agents_dir: Path = AGENTS_DIR, load: bool = True) -> list[Path]:
    agents_dir.mkdir(parents=True, exist_ok=True)
    (config.data_root() / "logs").mkdir(parents=True, exist_ok=True)
    written = []
    for name, plist in plists.items():
        path = agents_dir / f"{PREFIX}{name}.plist"
        if load and path.exists():
            subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}/{PREFIX}{name}"], capture_output=True)
        with open(path, "wb") as f:
            plistlib.dump(plist, f)
        if load:
            subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(path)], check=True)
        written.append(path)
    return written


def uninstall(names: list[str], *, agents_dir: Path = AGENTS_DIR, unload: bool = True) -> list[Path]:
    removed = []
    for name in names:
        path = agents_dir / f"{PREFIX}{name}.plist"
        if unload:
            subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}/{PREFIX}{name}"], capture_output=True)
        if path.exists():
            path.unlink()
            removed.append(path)
    return removed


def installed(agents_dir: Path = AGENTS_DIR) -> list[str]:
    return sorted(p.name.removeprefix(PREFIX).removesuffix(".plist") for p in agents_dir.glob(PREFIX + "*.plist"))
