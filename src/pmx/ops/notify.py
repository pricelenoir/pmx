"""Alert delivery, in child processes so nothing waits on it: a macOS notification (`osascript`) where there is a
desktop, and a webhook when `PMX_ALERT_WEBHOOK` is set in `.env` (the URL is a secret: never in config, never
logged). The webhook's format follows its host: Slack and Discord get their JSON; anything else (e.g. an ntfy.sh
topic, which pushes to a phone) gets the text as the body with a `Title` header.

`UptimePing` covers what no alert can: the whole machine gone. pmx pings `PMX_UPTIME_URL` every minute while it
runs; a dead-man service (healthchecks.io, Better Stack, Uptime Kuma, …) alerts you when the pings stop."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from collections.abc import Callable
from urllib.parse import urlparse

log = logging.getLogger(__name__)
WEBHOOK_ENV = "PMX_ALERT_WEBHOOK"
UPTIME_ENV = "PMX_UPTIME_URL"


def _spawn(argv: list[str]) -> None:
    try:
        subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
    except OSError as e:
        log.warning("couldn't deliver an alert with %s: %s", argv[0], e)


def webhook_request(url: str, title: str, text: str) -> tuple[list[str], str]:
    """(headers, body) for the webhook's kind."""
    host = (urlparse(url).hostname or "").lower()
    if host == "hooks.slack.com":
        return ["Content-Type: application/json"], json.dumps({"text": f"*{title}*: {text}"})
    if host in ("discord.com", "discordapp.com") and "/api/webhooks/" in url:
        return ["Content-Type: application/json"], json.dumps({"content": f"**{title}**: {text}"})
    return [f"Title: {title}", "Content-Type: text/plain; charset=utf-8"], text


def _curl_string(s: str) -> str:
    """A double-quoted string in curl's config syntax."""
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r") + '"'


def curl_config(url: str, title: str, text: str) -> str:
    """The whole request as a curl config, given on stdin so the URL (a secret) isn't in any process listing."""
    headers, body = webhook_request(url, title[:60], text[:1000])
    lines = [f"url = {_curl_string(url)}", "request = POST", "fail", "silent", "show-error", "max-time = 15",
             *(f"header = {_curl_string(h)}" for h in headers),
             f"data-raw = {_curl_string(body)}"]  # data-raw: a leading @ is text, not a file to read
    return "\n".join(lines) + "\n"


def _curl(config: str, what: str) -> subprocess.Popen | None:
    """Run curl on a config given over stdin; nothing waits for it."""
    if shutil.which("curl") is None:
        log.warning("%s set but curl isn't installed", what)
        return None
    try:
        p = subprocess.Popen(["curl", "--config", "-"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
        assert p.stdin is not None
        p.stdin.write(config.encode())
        p.stdin.close()
        return p
    except OSError as e:
        log.warning("couldn't reach the %s: %s", what, e)
        return None


def send_webhook(url: str, title: str, text: str) -> subprocess.Popen | None:
    return _curl(curl_config(url, title, text), "alert webhook")


class UptimePing:
    """Pings `url` (default `PMX_UPTIME_URL`) at most every `every_s`; does nothing when there's no URL."""

    def __init__(self, url: str | None = None, *, every_s: float = 60.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.url = (url if url is not None else os.environ.get(UPTIME_ENV, "")).strip()
        self.every_s, self.clock = every_s, clock
        self._last: float | None = None

    def beat(self) -> subprocess.Popen | None:
        now = self.clock()
        if not self.url or (self._last is not None and now - self._last < self.every_s):
            return None
        self._last = now
        return _curl("\n".join([f"url = {_curl_string(self.url)}", "fail", "silent", "max-time = 10",
                                "retry = 2"]) + "\n", "uptime URL")


def notify(title: str, text: str) -> None:
    """Deliver an alert everywhere configured; logged either way."""
    log.warning("ALERT %s: %s", title, text)
    if shutil.which("osascript") is not None:
        script = f"display notification {json.dumps(text[:240])} with title {json.dumps(title[:60])}"
        _spawn(["osascript", "-e", script])
    url = os.environ.get(WEBHOOK_ENV, "").strip()
    if url:
        send_webhook(url, title, text)


def channels() -> list[str]:
    """Where alerts go on this machine, for `pmx doctor` / `pmx ops alert-test`."""
    out = ["log"]
    if shutil.which("osascript") is not None:
        out.append("macOS notifications")
    url = os.environ.get(WEBHOOK_ENV, "").strip()
    if url:
        out.append(f"webhook ({urlparse(url).hostname or 'invalid URL'})")
    return out


def uptime_target() -> str | None:
    """The heartbeat URL's host, for `pmx doctor` (None: no heartbeat)."""
    url = os.environ.get(UPTIME_ENV, "").strip()
    return (urlparse(url).hostname or "invalid URL") if url else None
