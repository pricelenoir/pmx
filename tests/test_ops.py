"""The live watchdog, alerts, health files, backups, launchd agents, doctor."""

from __future__ import annotations

import asyncio
import json
import os
import plistlib
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from pmx.core.types import Fill
from pmx.engine.config import OpsSection
from pmx.engine.events import Halt, LogEvent, SlotFill, SlotStatus, SourceStatus
from pmx.ops import doctor, launchd
from pmx.ops.alerts import Alerts
from pmx.ops.backup import backup
from pmx.ops.guard import Guard, Heartbeat, guard_pidfile, heartbeat_path
from pmx.ops.health import health_path, read
from test_live_broker import FakeVenue

# ---- pmx guard --------------------------------------------------------------------------------------------------


def _venue() -> FakeVenue:
    v = FakeVenue()
    v.rest_order("pmx-t1-aaaaaa-1")
    v.rest_order("pmx-t1-bbbbbb-2")
    v.rest_order("pmx-zz-aaaaaa-1")  # another instance's
    v.rest_order("manual-1")  # placed by hand on Kalshi's site
    return v


def _dead_pid() -> int:
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid


def _guard(tmp_path: Path, v: FakeVenue, alerts: list) -> Guard:
    return Guard(v, instance="t1", heartbeat=tmp_path / "pmx.heartbeat", ctl=tmp_path / "nothing-ctl.sock",
                 guard_s=0.3, alert=lambda title, text: alerts.append(text), every_s=0.02)


def test_a_dead_engine_has_this_instances_orders_canceled_and_nothing_else(tmp_path):
    v, alerts = _venue(), []
    hb = tmp_path / "pmx.heartbeat"
    hb.write_text(json.dumps({"pid": _dead_pid(), "instance": "t1"}))
    g = _guard(tmp_path, v, alerts)
    asyncio.run(asyncio.wait_for(g.run(asyncio.Event()), 5))
    assert {c: o.status for c, o in v.known.items()} == {"pmx-t1-aaaaaa-1": "canceled", "pmx-t1-bbbbbb-2": "canceled",
                                                         "pmx-zz-aaaaaa-1": "resting", "manual-1": "resting"}
    assert alerts == ["the live engine died: canceled 2 resting orders"] and not hb.exists()


def test_a_hung_engine_trips_after_guard_s_and_rearms_when_it_answers_again(tmp_path):
    v, alerts = _venue(), []
    hb = Heartbeat(tmp_path / "pmx.heartbeat", instance="t1", env="demo", run_id="r")  # this process: alive
    g = _guard(tmp_path, v, alerts)

    async def go() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(g.run(stop))
        hb.beat()
        await asyncio.sleep(0.1)
        assert g.trips == 0, "fresh heartbeat: alive"
        await asyncio.sleep(0.6)  # no beats: stale, and its control socket doesn't answer
        assert g.trips == 1 and v.known["pmx-t1-aaaaaa-1"].status == "canceled"
        v.rest_order("pmx-t1-cccccc-3")
        hb.beat()  # it recovered
        await asyncio.sleep(0.1)
        await asyncio.sleep(0.6)
        assert g.trips == 2 and v.known["pmx-t1-cccccc-3"].status == "canceled", "re-armed, tripped again"
        hb.beat()
        await asyncio.sleep(0.1)
        hb.remove()  # a clean stop
        await asyncio.wait_for(task, 2)

    asyncio.run(go())
    assert len(alerts) == 2 and all("unresponsive" in a for a in alerts)


def test_a_clean_stop_or_no_engine_at_all_cancels_nothing(tmp_path):
    v, alerts = _venue(), []
    g = _guard(tmp_path, v, alerts)
    asyncio.run(asyncio.wait_for(g.run(asyncio.Event(), start_s=0.1), 5))  # no heartbeat ever: gives up
    assert g.trips == 0 and all(o.status == "resting" for o in v.known.values()) and alerts == []


def test_a_failed_cancel_is_retried_until_it_goes(tmp_path, monkeypatch):
    from pmx.connectors.kalshi.rest import KalshiError

    v, alerts = _venue(), []
    v.cancel_errors = [KalshiError(503, "busy")] * 3
    hb = tmp_path / "pmx.heartbeat"
    hb.write_text(json.dumps({"pid": _dead_pid(), "instance": "t1"}))
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda s: real_sleep(0))
    asyncio.run(asyncio.wait_for(_guard(tmp_path, v, alerts).run(asyncio.Event()), 5))
    assert v.known["pmx-t1-aaaaaa-1"].status == "canceled" and v.known["pmx-t1-bbbbbb-2"].status == "canceled"


def test_guard_files_sit_beside_the_socket():
    sock = Path("data/pmx.sock")
    assert heartbeat_path(sock) == Path("data/pmx.heartbeat") and guard_pidfile(sock) == Path("data/pmx-guard.pid")
    assert health_path(sock) == Path("data/health.json")
    assert health_path(Path("data/pmx-demo.sock")) == Path("data/health-demo.json")


# ---- alerts ---------------------------------------------------------------------------------------------------------


def test_alerts_fire_on_what_matters_once_per_window(tmp_path):
    a = Alerts(OpsSection(alert_every_s=60, source_down_s=30, throttled_s=10, disk_min_gb=0), send=lambda *_: None)
    fill = Fill("kalshi", "F1", "V1", "T", "bid", 4_000, 100, 0, False, 1, "c")
    a.observe(SlotFill("s/T", "live", fill), 1_000)
    a.observe(SlotFill("s/T", "paper", fill), 1_000)
    a.observe(Halt("s", "max_loss", True, 1_000), 1_000)
    a.observe(Halt("", "pmx stopping", True, 1_000), 1_000)
    a.observe(Halt("s", "max_loss", True, 2_000), 2_000)  # again inside the window
    a.observe(SlotStatus("s/T", "s", "T", "paper", "error", 0, 0, 0, 0, 0, "", 1), 1_000)
    a.observe(SourceStatus("kalshi", True, "queue overflow: 3 dropped (reset, max 10)", 1_000), 1_000)
    a.observe(LogEvent("warn", "s/T", "over its CPU budget (50 ms/s) for 5 s; halts at 30 s", 1_000), 1_000)
    a.observe(SourceStatus("espn", False, "timeout", 1_000), 1_000)
    a.check(20_000, data=tmp_path, queued_writes=4)
    a.check(40_000, data=tmp_path, queued_writes=4, dropped=7)
    a.sleep_gap(120_000, 41_000)
    keys = [k for k, _ in a.sent]
    assert keys == ["fill:F1", "halt:s", "slot:s/T", "overflow:kalshi", "cpu:s/T", "down:espn", "throttled",
                    "observability", "sleep"]
    a.observe(SourceStatus("espn", True, "ok", 50_000), 50_000)
    a.check(100_000, data=tmp_path)
    assert len(a.sent) == len(keys), "back up, not throttled: nothing new"


def test_alerts_off_still_log(tmp_path):
    logged = []
    a = Alerts(OpsSection(alerts=False), send=lambda *_: pytest.fail("no notifications"),
               log_only=lambda title, text: logged.append(text))
    a.observe(Halt("", "x", True, 1), 1)
    assert logged == ["halted everything: x"]


# ---- health, backups, launchd, doctor -------------------------------------------------------------------------------


def test_a_running_engine_writes_its_health_file(tmp_path, monkeypatch):
    from test_control import CONFIG, _running

    monkeypatch.chdir(tmp_path)

    async def go() -> dict:
        s, stop, task = await _running(tmp_path, text=CONFIG + "[ops]\nhealth_s = 0.1\nalerts = false\n")
        path = health_path(Path("r.sock"))
        for _ in range(100):
            if read(path) is not None:
                break
            await asyncio.sleep(0.05)
        stop.set()
        await task
        return read(path)

    h = asyncio.run(go())
    assert h["kind"] == "run" and h["env"] == "demo" and h["pid"] == os.getpid()
    assert h["sources"]["kalshi"]["last_ms"] and "pending" in h["sources"]["timer"]
    assert h["feed"]["dropped"] == 0 and h["disk_free_gb"] > 0


def test_backups_copy_raw_incrementally_and_the_ledger_consistently(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "backup"
    raw = data / "raw" / "kalshi" / "2026-10-19"
    raw.mkdir(parents=True)
    (raw / "00-1.log.zst").write_bytes(b"a" * 1000)
    (raw / "01-2.log.zst").write_bytes(b"b" * 10)
    db = sqlite3.connect(data / "ledger.sqlite")
    db.execute("CREATE TABLE t (x)")
    db.execute("INSERT INTO t VALUES (1)")
    db.commit()
    first = backup(data, dest, keep_days=3, now=1_000_000)
    assert (first.raw_copied, first.raw_current, first.problems) == (2, 0, [])
    (raw / "01-2.log.zst").write_bytes(b"b" * 20)  # the current hour grew
    again = backup(data, dest, keep_days=3, now=1_000_000 + 86_400)
    assert (again.raw_copied, again.raw_current) == (1, 1)
    assert (dest / "raw" / "kalshi" / "2026-10-19" / "01-2.log.zst").read_bytes() == b"b" * 20
    copy = sqlite3.connect(again.ledgers[0])
    assert copy.execute("SELECT x FROM t").fetchall() == [(1,)]
    old = first.ledgers[0]
    os.utime(old, (time.time() - 10 * 86_400,) * 2)
    third = backup(data, dest, keep_days=3)
    assert third.pruned == 1 and not old.exists()


def test_launchd_agents_restart_on_crash_and_install_where_told(tmp_path):
    plists = launchd.agents(record=True, run=True, backup=True, env="prod", live=True)
    run = plists["run"]
    assert run["ProgramArguments"][3:] == ["run", "--env", "prod", "--live", "--confirm", "prod"]
    assert run["KeepAlive"] == {"SuccessfulExit": False} and run["RunAtLoad"]
    daily = plists["backup"]
    assert daily["StartCalendarInterval"] == {"Hour": 4, "Minute": 30} and "KeepAlive" not in daily
    written = launchd.install(plists, agents_dir=tmp_path, load=False)
    assert [p.name for p in written] == ["com.pmx.record.plist", "com.pmx.run.plist", "com.pmx.backup.plist"]
    assert plistlib.loads(written[0].read_bytes())["Label"] == "com.pmx.record"
    assert launchd.installed(tmp_path) == ["backup", "record", "run"]
    assert len(launchd.uninstall(["record", "run", "backup"], agents_dir=tmp_path, unload=False)) == 3
    assert launchd.installed(tmp_path) == []


def test_doctor_checks_key_files_are_private(tmp_path, monkeypatch):
    good, loose = tmp_path / "good.key", tmp_path / "loose.key"
    good.write_text("k")
    loose.write_text("k")
    good.chmod(0o600)
    loose.chmod(0o644)
    monkeypatch.setenv("KALSHI_DEMO_API_KEY_ID", "id")
    monkeypatch.setenv("KALSHI_DEMO_PRIVATE_KEY_PATH", str(good))
    monkeypatch.setenv("KALSHI_PROD_API_KEY_ID", "id")
    monkeypatch.setenv("KALSHI_PROD_PRIVATE_KEY_PATH", str(loose))
    (demo, ok_demo, _), (prod, ok_prod, detail) = doctor.keys()
    assert (demo, ok_demo, prod, ok_prod) == ("demo key", True, "prod key", False) and "chmod 600" in detail


def test_ops_install_refuses_clearly_without_launchd(monkeypatch):
    from pmx import cli
    from pmx.cli import ops as ops_cli

    monkeypatch.setattr(ops_cli.shutil, "which", lambda name: None)
    with pytest.raises(SystemExit, match="launchd agents"):
        cli.main(["ops", "install", "--record"])
