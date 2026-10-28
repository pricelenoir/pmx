"""Alerts: what you should hear about while pmx runs unattended, as macOS notifications.

`observe` sees every event the run publishes (cheap type checks): a live fill, a halt, a slot error, a source
queue overflow, a slot over its CPU budget, a failing advisor. `check` runs with the health file: sources
down longer than `source_down_s`, the order scheduler holding writes longer than `throttled_s`, low disk,
observability drops. The same alert (by key) goes out at most every `alert_every_s`; all are logged."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pmx.core.units import cents, fmt_qty
from pmx.engine.config import OpsSection
from pmx.engine.events import Halt, LogEvent, SlotFill, SlotStatus, SourceStatus
from pmx.ops.notify import notify


def disk_free_gb(path: Path) -> float:
    return shutil.disk_usage(path).free / 1e9


class Alerts:
    def __init__(self, ops: OpsSection, *, send: Callable[[str, str], None] = notify,
                 log_only: Callable[[str, str], None] | None = None) -> None:
        self.ops = ops
        self.send = send if ops.alerts else (log_only or _log_only)
        self.sent: list[tuple[str, str]] = []
        self._last: dict[str, int] = {}  # key -> ms of its last alert
        self._down: dict[str, int] = {}  # source -> ms it went down
        self._throttled_since: int | None = None
        self._dropped = 0

    def alert(self, key: str, text: str, now_ms: int) -> None:
        last = self._last.get(key)
        if last is not None and now_ms - last < self.ops.alert_every_s * 1000:
            return
        self._last[key] = now_ms
        self.sent.append((key, text))
        self.send("pmx", text)

    # ---- events (inside dispatch) ------------------------------------------------------------------------------

    def observe(self, ev: Any, now_ms: int) -> None:
        t = type(ev)
        if t is SlotFill:
            if ev.mode == "live":
                f = ev.fill
                self.alert(f"fill:{f.fill_id}", f"live fill {ev.slot}: {'bought' if f.side == 'bid' else 'sold'} "
                                                f"{fmt_qty(f.qty)} @ {cents(f.price)}", now_ms)
        elif t is Halt:
            if ev.active and ev.reason != "pmx stopping":
                self.alert(f"halt:{ev.scope}", f"halted {ev.scope or 'everything'}: {ev.reason}", now_ms)
        elif t is SlotStatus:
            if ev.state == "error":
                self.alert(f"slot:{ev.slot}", f"slot {ev.slot} failed and stopped", now_ms)
        elif t is SourceStatus:
            if ev.connected:
                self._down.pop(ev.source, None)
            else:
                self._down.setdefault(ev.source, now_ms)
            if ev.detail.startswith("queue overflow"):
                self.alert(f"overflow:{ev.source}", f"source {ev.source}: {ev.detail}", now_ms)
        elif t is LogEvent:
            if "over its CPU budget" in ev.text:
                self.alert(f"cpu:{ev.source}", f"{ev.source} {ev.text}", now_ms)
            elif ev.source == "advisors" and ev.level == "error":
                self.alert("advisors", f"advisors: {ev.text}", now_ms)

    def sleep_gap(self, gap_ms: int, now_ms: int) -> None:
        self.alert("sleep", f"the computer slept for {gap_ms / 1000:.0f} s while pmx was running", now_ms)

    # ---- periodic --------------------------------------------------------------------------------------------------

    def check(self, now_ms: int, *, data: Path, queued_writes: int = 0, dropped: int = 0) -> None:
        for source, since in self._down.items():
            if now_ms - since >= self.ops.source_down_s * 1000:
                self.alert(f"down:{source}", f"source {source} has been down for {(now_ms - since) / 1000:.0f} s",
                           now_ms)
        if queued_writes:
            self._throttled_since = self._throttled_since or now_ms
            if now_ms - self._throttled_since >= self.ops.throttled_s * 1000:
                self.alert("throttled", f"the order scheduler has held {queued_writes} writes for "
                                        f"{(now_ms - self._throttled_since) / 1000:.0f} s (venue write limit)", now_ms)
        else:
            self._throttled_since = None
        free = disk_free_gb(data)
        if free < self.ops.disk_min_gb:
            self.alert("disk", f"only {free:.1f} GB free on the data disk", now_ms)
        if dropped > self._dropped:
            self.alert("observability", f"the feed dropped {dropped - self._dropped} events (viewers resynced)", now_ms)
        self._dropped = max(self._dropped, dropped)


def _log_only(title: str, text: str) -> None:
    import logging

    logging.getLogger(__name__).warning("ALERT (notifications off) %s: %s", title, text)
