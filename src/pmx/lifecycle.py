"""pmx runs headless; viewers attach and detach. The serving process holds
a pidfile next to its socket for as long as it serves, so `pmx down` can stop it however it
was started — `pmx up`, `pmx tui`, or by hand in tmux."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from collections.abc import Callable, Iterator
from pathlib import Path


def pidfile_for(sock: Path) -> Path:
    """data/pmx.sock -> data/pmx.pid, data/pmx-demo.sock -> data/pmx-demo.pid."""
    return sock.with_suffix(".pid")


def alive(pid: int) -> bool:
    # Reap it first if it's our own exited child, so a zombie doesn't count as running.
    with contextlib.suppress(ChildProcessError):
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_pid(path: Path) -> int | None:
    """The pid in `path` if that process is still running."""
    try:
        pid = int(path.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None
    return pid if alive(pid) else None


@contextlib.contextmanager
def pidfile(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{os.getpid()}\n")
    try:
        yield
    finally:
        with contextlib.suppress(FileNotFoundError, ValueError):
            if int(path.read_text().strip()) == os.getpid():
                path.unlink()


def start_detached(argv: list[str], log: Path) -> subprocess.Popen:
    """Run `argv` in its own session: closing this terminal (SIGHUP) doesn't reach it."""
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "a") as out:
        return subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                                start_new_session=True)


def wait_for(check: Callable[[], bool], timeout_s: float, every_s: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout_s
    while not check():
        if time.monotonic() >= deadline:
            return False
        time.sleep(every_s)
    return True


def stop(pid: int, timeout_s: float = 10.0) -> bool:
    """SIGTERM (an orderly shutdown), then SIGKILL after `timeout_s`. True once it's gone."""
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
    if wait_for(lambda: not alive(pid), timeout_s):
        return True
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)
    return wait_for(lambda: not alive(pid), 2.0)
