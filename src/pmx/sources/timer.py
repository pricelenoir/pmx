"""`timer`: a `Timer` every `interval_ms` of wall time. The engine's heartbeat: strategies' `on_timer`,
pruning, status throttles, and sleep detection (a laptop sleep shows as one long gap, not a burst)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import msgspec

from pmx.engine.events import SourceStatus, Timer
from pmx.plugins import source
from pmx.sources.base import Sink, sleep_or_stop


@source("timer")
class TimerSource:
    class Params(msgspec.Struct, frozen=True):
        interval_ms: int = 1_000

    def __init__(self, name: str, params: Params | None = None, env: Any = None) -> None:
        self.name, self.p = name, params or self.Params()
        if self.p.interval_ms <= 0:
            raise ValueError("interval_ms must be positive")

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        step = self.p.interval_ms / 1000
        due = time.monotonic()
        sink.emit(SourceStatus(self.name, True, "ok", int(time.time() * 1000)))
        while not stop.is_set():
            sink.emit(Timer(int(time.time() * 1000)))
            due += step
            wait = due - time.monotonic()
            if wait < 0:  # fell behind (or slept): carry on from now rather than catching up
                due, wait = time.monotonic(), 0.0
            if await sleep_or_stop(stop, wait):
                return
