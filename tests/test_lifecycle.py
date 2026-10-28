from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from pmx import lifecycle


def test_pidfile_is_held_while_serving_and_left_alone_if_someone_else_owns_it(tmp_path):
    path = tmp_path / "pmx.pid"
    with lifecycle.pidfile(path):
        assert lifecycle.read_pid(path) == os.getpid()
    assert not path.exists()
    with lifecycle.pidfile(path):
        path.write_text("1\n")  # another server took over the file
    assert path.read_text() == "1\n"


def test_read_pid_ignores_garbage_and_dead_processes(tmp_path):
    path = tmp_path / "pmx.pid"
    assert lifecycle.read_pid(path) is None
    path.write_text("not a pid")
    assert lifecycle.read_pid(path) is None
    done = subprocess.Popen([sys.executable, "-c", "pass"])
    done.wait()
    path.write_text(f"{done.pid}\n")
    assert lifecycle.read_pid(path) is None
    assert lifecycle.pidfile_for(Path("data/pmx-demo.sock")) == Path("data/pmx-demo.pid")


def test_stop_terminates_a_detached_process(tmp_path):
    proc = lifecycle.start_detached([sys.executable, "-c", "import time; time.sleep(60)"], tmp_path / "log")
    assert os.getsid(proc.pid) != os.getsid(0), "own session: closing the terminal doesn't reach it"
    assert lifecycle.alive(proc.pid)
    assert lifecycle.stop(proc.pid, timeout_s=5)
    assert not lifecycle.alive(proc.pid)


def _pmx(*args: str, cwd: Path, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "pmx", *args], cwd=cwd, env=env, capture_output=True, text=True,
                          timeout=90)


def test_up_runs_headless_and_down_stops_it(tmp_path):
    env = {**os.environ, "PMX_DATA": str(tmp_path / "data")}
    sock = ["--demo", "--socket", "x.sock"]  # relative: AF_UNIX paths are short
    up = _pmx("up", *sock, cwd=tmp_path, env=env)
    assert up.returncode == 0, up.stderr
    assert "running in the background" in up.stdout
    assert "pmx down --demo --socket x.sock" in up.stdout, "hints name this instance"
    pid = lifecycle.read_pid(tmp_path / "x.pid")
    assert pid is not None and (tmp_path / "x.sock").exists()
    try:
        again = _pmx("up", *sock, cwd=tmp_path, env=env)
        assert f"already running (pid {pid}" in again.stdout
    finally:
        down = _pmx("down", *sock, cwd=tmp_path, env=env)
    assert down.returncode == 0 and "stopped" in down.stdout, down.stderr
    assert not lifecycle.alive(pid)
    assert not (tmp_path / "x.sock").exists() and not (tmp_path / "x.pid").exists(), "nothing left behind"
    assert "isn't running" in _pmx("down", *sock, cwd=tmp_path, env=env).stdout
