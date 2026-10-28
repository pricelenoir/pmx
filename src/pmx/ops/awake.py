"""Staying awake: a Mac that sleeps mid-game loses data (`sleep_gap`) and leaves live orders unmanaged.
While `pmx run` / `pmx record` runs, `caffeinate -i -s -w <pid>` holds a no-sleep assertion (`-s` only holds on AC
power: `on_battery` is worth a warning) and exits with the process."""

from __future__ import annotations

import os
import shutil
import subprocess


def stay_awake(pid: int | None = None) -> subprocess.Popen | None:
    if shutil.which("caffeinate") is None:
        return None
    return subprocess.Popen(["caffeinate", "-i", "-s", "-w", str(pid or os.getpid())], stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def on_battery() -> bool | None:
    """True on battery, False on AC, None where `pmset` can't say."""
    if shutil.which("pmset") is None:
        return None
    try:
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return "Battery Power" in out if "Power" in out else None
