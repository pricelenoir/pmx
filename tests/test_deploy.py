"""Running unattended on Linux and watching from elsewhere: systemd user services, alert webhooks, `pmx remote`,
the backup sync hook, and the Linux doctor checks."""

from __future__ import annotations

import http.server
import json
import shutil
import sys
import threading
from pathlib import Path

import pytest

from pmx.cli import remote
from pmx.ops import doctor, notify, systemd

# ---- systemd -------------------------------------------------------------------------------------------------------


def test_units_restart_on_crash_not_on_a_clean_stop_and_back_up_daily():
    units = systemd.units(record=True, run=True, backup=True, env="prod", live=True)
    assert sorted(units) == ["pmx-backup.service", "pmx-backup.timer", "pmx-record.service", "pmx-run.service"]
    run = units["pmx-run.service"]
    assert "-m pmx run --env prod --live --confirm prod" in run
    assert "Restart=on-failure" in run and "WantedBy=default.target" in run  # exit 0 (pmx down) isn't restarted
    assert f"TimeoutStopSec={systemd.STOP_S}" in run and "After=network-online.target" in run
    assert "Type=oneshot" in units["pmx-backup.service"] and "Restart=" not in units["pmx-backup.service"]
    assert "OnCalendar=*-*-* 04:30:00" in units["pmx-backup.timer"] and "Persistent=true" in units["pmx-backup.timer"]
    paper = systemd.units(record=False, run=True, backup=False, env="prod")["pmx-run.service"]
    assert "--live" not in paper


def test_unit_paths_with_spaces_are_quoted(monkeypatch, tmp_path):
    monkeypatch.setattr(systemd.sys, "executable", "/opt/my venv/bin/python")
    unit = systemd.service("record", ["record"], data=tmp_path / "my data")
    assert "ExecStart='/opt/my venv/bin/python' -m pmx record" in unit
    assert f"Environment='PMX_DATA={tmp_path}/my data'" in unit


def test_install_enables_timers_not_their_services(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(systemd, "_systemctl", lambda *a, check=False: calls.append(a))
    units = systemd.units(record=True, run=False, backup=True, env="demo")
    written = systemd.install(units, units_dir=tmp_path)
    assert {p.name for p in written} == set(units)
    assert ("daemon-reload",) in calls
    assert ("enable", "pmx-backup.timer") in calls and ("enable", "pmx-record.service") in calls
    assert ("enable", "pmx-backup.service") not in calls
    assert systemd.installed(tmp_path) == ["backup", "record"]
    calls.clear()
    removed = systemd.uninstall(["backup", "record"], units_dir=tmp_path)
    assert len(removed) == 3 and systemd.installed(tmp_path) == []
    assert ("disable", "--now", "pmx-backup.timer") in calls


# ---- alerts --------------------------------------------------------------------------------------------------------


def test_webhook_formats_follow_the_host():
    h, body = notify.webhook_request("https://hooks.slack.com/services/T/B/x", "pmx", "halted")
    assert json.loads(body) == {"text": "*pmx*: halted"} and "application/json" in h[0]
    _, body = notify.webhook_request("https://discord.com/api/webhooks/1/abc", "pmx", "halted")
    assert json.loads(body) == {"content": "**pmx**: halted"}
    h, body = notify.webhook_request("https://ntfy.sh/my-secret-topic", "pmx", "halted")
    assert body == "halted" and "Title: pmx" in h


def test_curl_config_escapes_and_never_reads_files():
    cfg = notify.curl_config("https://ntfy.sh/t", "pmx", '@/etc/passwd "quoted" \\ line\nnext')
    assert 'data-raw = "@/etc/passwd \\"quoted\\" \\\\ line\\nnext"' in cfg  # data-raw: @ is text
    assert 'url = "https://ntfy.sh/t"' in cfg and "data-binary" not in cfg


@pytest.mark.skipif(shutil.which("curl") is None, reason="curl isn't installed")
def test_a_webhook_alert_reaches_the_server(monkeypatch):
    got: list[tuple[dict, bytes]] = []

    class Hook(http.server.BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            got.append((dict(self.headers), self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.handle_request, daemon=True).start()
    p = notify.send_webhook(f"http://127.0.0.1:{server.server_port}/topic", "pmx", "slot x failed and stopped")
    assert p is not None and p.wait(timeout=10) == 0
    server.server_close()
    headers, body = got[0]
    assert body == b"slot x failed and stopped" and headers["Title"] == "pmx"


def test_notify_uses_the_webhook_only_when_set(monkeypatch):
    sent = []
    monkeypatch.setattr(notify, "send_webhook", lambda url, t, x: sent.append(url))
    monkeypatch.setattr(notify.shutil, "which", lambda name: None)  # no desktop here
    monkeypatch.delenv(notify.WEBHOOK_ENV, raising=False)
    notify.notify("pmx", "a")
    assert sent == [] and notify.channels() == ["log"]
    monkeypatch.setenv(notify.WEBHOOK_ENV, "https://ntfy.sh/t")
    notify.notify("pmx", "b")
    assert sent == ["https://ntfy.sh/t"] and notify.channels() == ["log", "webhook (ntfy.sh)"]


# ---- pmx remote ----------------------------------------------------------------------------------------------------


def test_remote_commands_quote_arguments_and_keep_the_home_tilde():
    assert remote.remote_cd("~/pmx") == "cd ~/pmx" and remote.remote_cd("~") == "cd ~"
    assert remote.remote_cd("/srv/my pmx") == "cd '/srv/my pmx'"
    cmd = remote.pmx_command("~/pmx", ["halt", "--reason", "lunch; rm -rf /"])
    assert cmd == "cd ~/pmx && exec .venv/bin/pmx halt --reason 'lunch; rm -rf /'"
    assert remote.ssh_argv("vm", "x", tty=True) == ["ssh", "-t", "vm", "x"]
    assert remote.logs_command("run") == "journalctl --user -u pmx-run -n 200 -f"
    assert remote.logs_command(None) == "journalctl --user -u 'pmx-*' -n 200 -f"


def test_remote_runs_the_command_over_ssh(monkeypatch):
    from pmx.cli import main

    calls = []
    monkeypatch.setattr(remote.shutil, "which", lambda name: "/usr/bin/ssh")
    monkeypatch.setattr(remote.subprocess, "call", lambda argv: calls.append(argv) or 0)
    monkeypatch.setattr(remote.sys.stdin, "isatty", lambda: False)
    with pytest.raises(SystemExit) as e:
        main(["remote", "--dir", "/srv/pmx", "vm", "positions", "--mode", "paper"])
    assert e.value.code == 0
    assert calls == [["ssh", "vm", "cd /srv/pmx && exec .venv/bin/pmx positions --mode paper"]]
    with pytest.raises(SystemExit, match="what to run"):
        main(["remote", "vm"])


# ---- backups · doctor ----------------------------------------------------------------------------------------------


def test_backup_sync_runs_after_a_good_backup(tmp_path, monkeypatch):
    from pmx.cli import main

    data, dest, marker = tmp_path / "data", tmp_path / "backup", tmp_path / "synced"
    data.mkdir()
    cfg = tmp_path / "s.toml"
    write = "import pathlib, sys; pathlib.Path(sys.argv[1]).write_text(sys.argv[2])"
    cfg.write_text(f'[ops]\nbackup_dir = "{dest}"\n'
                   f'backup_sync = ["{sys.executable}", "-c", "{write}", "{marker}", "{{dir}}"]\n')
    monkeypatch.setenv("PMX_DATA", str(data))
    monkeypatch.setenv("PMX_RESEARCH", str(tmp_path / "no-research"))
    main(["ops", "backup", "--config", str(cfg)])
    assert marker.read_text() == str(dest)
    fail = f'["{sys.executable}", "-c", "raise SystemExit(3)"]'
    cfg.write_text(f'[ops]\nbackup_dir = "{dest}"\nbackup_sync = {fail}\n')
    with pytest.raises(SystemExit, match="backup_sync exited 3"):
        main(["ops", "backup", "--config", str(cfg)])


def test_doctor_on_a_linux_server(monkeypatch):
    monkeypatch.setattr(doctor.sys, "platform", "linux")
    monkeypatch.setattr(doctor.shutil, "which", lambda name: {"timedatectl": "/bin/timedatectl",
                                                              "systemctl": "/bin/systemctl"}.get(name))

    class R:
        stdout = "yes\n"

    monkeypatch.setattr(doctor, "_run", lambda argv, timeout=60.0: R())
    assert doctor.clock() == ("clock", True, "NTP synchronized")
    assert doctor.tui_builds()[1] is None  # a server needs no dashboard toolchain
    monkeypatch.setattr(systemd, "lingering", lambda user=None: False)
    monkeypatch.setattr(systemd, "installed", lambda units_dir=None: ["record"])
    monkeypatch.setattr(systemd, "states", lambda names, units_dir=None: {"record": "failed"})
    svc, linger = doctor.services()
    assert svc == ("services", False, "record failed") and linger[1] is False and "enable-linger" in linger[2]


def test_runbook_exists_and_names_every_step():
    text = (Path(__file__).parents[1] / "docs" / "runbooks" / "linux-vm.md").read_text()
    for step in ("scripts/setup-linux.sh", "enable-linger", "pmx ops install", "pmx remote", "PMX_ALERT_WEBHOOK",
                 "backup_sync"):
        assert step in text, step


def test_with_the_recorder_service_the_engine_service_does_not_record_too(monkeypatch):
    from pmx.cli import main
    from pmx.cli import ops as ops_cli

    got = {}
    monkeypatch.setattr(ops_cli.shutil, "which", lambda name: "/bin/systemctl" if name == "systemctl" else None)
    monkeypatch.setattr(systemd, "installed", lambda units_dir=None: [])
    monkeypatch.setattr(systemd, "install", lambda files: got.update(files) or [])
    monkeypatch.setattr(systemd, "lingering", lambda user=None: True)
    main(["ops", "install", "--env", "demo", "--record", "--run"])
    assert "-m pmx run --env demo --no-record" in got["pmx-run.service"]
    got.clear()
    monkeypatch.setattr(systemd, "installed", lambda units_dir=None: ["record"])  # installed earlier
    main(["ops", "install", "--env", "demo", "--run"])
    assert "--no-record" in got["pmx-run.service"]
    got.clear()
    monkeypatch.setattr(systemd, "installed", lambda units_dir=None: [])
    main(["ops", "install", "--env", "demo", "--run"])
    assert "--no-record" not in got["pmx-run.service"]  # alone, the engine records (config [engine] record)


def test_uptime_pings_at_most_once_a_minute_and_only_with_a_url(monkeypatch):
    sent, now = [], [0.0]
    monkeypatch.setattr(notify, "_curl", lambda config, what: sent.append(config) or object())
    hb = notify.UptimePing("https://hc-ping.com/abc", clock=lambda: now[0])
    assert hb.beat() is not None and 'url = "https://hc-ping.com/abc"' in sent[0]
    now[0] = 30.0
    assert hb.beat() is None
    now[0] = 61.0
    assert hb.beat() is not None and len(sent) == 2
    monkeypatch.delenv(notify.UPTIME_ENV, raising=False)
    assert notify.UptimePing().beat() is None  # no URL: nothing to do


@pytest.mark.skipif(shutil.which("curl") is None, reason="curl isn't installed")
def test_an_uptime_ping_reaches_the_server():
    got = []

    class Ping(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            got.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Ping)
    threading.Thread(target=server.handle_request, daemon=True).start()
    p = notify.UptimePing(f"http://127.0.0.1:{server.server_port}/ping/abc").beat()
    assert p is not None and p.wait(timeout=10) == 0
    server.server_close()
    assert got == ["/ping/abc"]



def test_an_engine_socket_path_too_long_to_bind_is_a_clear_error(tmp_path, monkeypatch):
    from pmx.cli import main

    monkeypatch.setenv("PMX_SOCK", str(tmp_path / ("d" * 120) / "pmx.sock"))
    with pytest.raises(SystemExit, match="too long for a Unix socket.*PMX_SOCK"):
        main(["run", "--env", "demo"])
