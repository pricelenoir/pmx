"""systemd user services (Linux): the launchd agents' counterpart. `pmx record` and `pmx run` restart when they crash
and start at boot; the backup runs daily from a timer. `pmx ops install` writes units to `~/.config/systemd/user`
and enables them — only when you run it; `pmx ops uninstall` disables and removes them. A clean stop
(`pmx down`, exit 0) isn't restarted. Starting at boot without a login needs lingering (`loginctl enable-linger`).
Output goes to the journal: `journalctl --user -u pmx-record -f`."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

from pmx import config

UNITS_DIR = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "systemd" / "user"
PREFIX = "pmx-"
STOP_S = 90  # `pmx run` stops in order (halt, cancel working orders, save) on SIGTERM; give it time


def _systemctl(*args: str, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, check=check)


def _esc(s: str) -> str:
    """systemd expands %-specifiers in unit values: a literal % is %%."""
    return s.replace("%", "%%")


def service(name: str, args: list[str], *, restart: bool = True, data: Path | None = None) -> str:
    data = data or config.data_root()
    argv = _esc(shlex.join([sys.executable, "-m", "pmx", *args]))
    lines = [
        "[Unit]", f"Description=pmx {' '.join(args)}", "After=network-online.target", "Wants=network-online.target",
        *(["StartLimitIntervalSec=300", "StartLimitBurst=5"] if restart else []),  # a crash loop stops: `failed`
        "", "[Service]", f"ExecStart={argv}", f"WorkingDirectory={_esc(str(config.PROJECT))}",
        f"Environment={_esc(shlex.quote(f'PMX_DATA={data}'))}",
        f"Environment={_esc(shlex.quote('PATH=' + os.environ.get('PATH', '/usr/local/bin:/usr/bin:/bin')))}",
        "Environment=PYTHONUNBUFFERED=1", f"TimeoutStopSec={STOP_S}",
    ]
    if restart:
        lines += ["Type=simple", "Restart=on-failure", "RestartSec=10", "", "[Install]", "WantedBy=default.target"]
    else:
        lines += ["Type=oneshot"]
    return "\n".join(lines) + "\n"


def timer(name: str, at: tuple[int, int]) -> str:
    return "\n".join([
        "[Unit]", f"Description=pmx {name}, daily", "", "[Timer]", f"OnCalendar=*-*-* {at[0]:02d}:{at[1]:02d}:00",
        "Persistent=true", "", "[Install]", "WantedBy=timers.target"]) + "\n"


def units(*, record: bool, run: bool, backup: bool, env: str, live: bool = False, record_args: list[str] = (),
          run_args: list[str] = ()) -> dict[str, str]:
    """{file name: contents}; a name's `.timer`, when present, is what gets enabled."""
    out: dict[str, str] = {}
    if record:
        out[f"{PREFIX}record.service"] = service("record", ["record", "--env", env, *record_args])
    if run:
        flags = ["--live", *(["--confirm", "prod"] if env == "prod" else [])] if live else []
        out[f"{PREFIX}run.service"] = service("run", ["run", "--env", env, *flags, *run_args])
    if backup:
        out[f"{PREFIX}backup.service"] = service("backup", ["ops", "backup"], restart=False)
        out[f"{PREFIX}backup.timer"] = timer("backup", (4, 30))
    return out


def _enabled_unit(files: list[str], name: str) -> str:
    """What to enable for a service name: its timer if it has one."""
    return f"{PREFIX}{name}.timer" if f"{PREFIX}{name}.timer" in files else f"{PREFIX}{name}.service"


def install(files: dict[str, str], *, units_dir: Path = UNITS_DIR, load: bool = True) -> list[Path]:
    units_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for fname, text in files.items():
        path = units_dir / fname
        path.write_text(text)
        written.append(path)
    if load:
        _systemctl("daemon-reload", check=True)
        names = sorted({f.removeprefix(PREFIX).rsplit(".", 1)[0] for f in files})
        for name in names:
            unit = _enabled_unit(list(files), name)
            _systemctl("enable", unit, check=True)
            _systemctl("restart", unit, check=True)  # picks up a changed unit when reinstalling
    return written


def uninstall(names: list[str], *, units_dir: Path = UNITS_DIR, unload: bool = True) -> list[Path]:
    removed = []
    for name in names:
        for suffix in ("timer", "service"):
            path = units_dir / f"{PREFIX}{name}.{suffix}"
            if unload:
                _systemctl("disable", "--now", path.name)
            if path.exists():
                path.unlink()
                removed.append(path)
    if unload and removed:
        _systemctl("daemon-reload")
    return removed


def installed(units_dir: Path = UNITS_DIR) -> list[str]:
    return sorted({p.name.removeprefix(PREFIX).rsplit(".", 1)[0] for p in units_dir.glob(PREFIX + "*.service")})


def states(names: list[str], units_dir: Path = UNITS_DIR) -> dict[str, str]:
    """name -> active state of what's enabled for it (`active`, `failed`, `inactive`, …)."""
    files = [p.name for p in units_dir.glob(PREFIX + "*")]
    return {n: _systemctl("is-active", _enabled_unit(files, n)).stdout.strip() or "unknown" for n in names}


def lingering(user: str | None = None) -> bool | None:
    """Whether user services start at boot without a login (None: can't tell)."""
    try:
        r = subprocess.run(["loginctl", "show-user", user or os.environ.get("USER", ""), "-p", "Linger", "--value"],
                           capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return {"yes": True, "no": False}.get(r.stdout.strip())
