"""Engine time. Components read time only from the engine (invariant 4): `LiveClock` is the wall
clock, `SimClock` is moved by the backtest runner."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class LiveClock:
    """Wall-clock ms that never goes backwards: if the system clock steps back, time holds until
    it catches up, so receive-time order stays dispatch order."""

    def __init__(self, time_fn: Callable[[], float] = time.time) -> None:
        self._time = time_fn
        self._last = 0

    def now_ms(self) -> int:
        t = int(self._time() * 1000)
        if t > self._last:
            self._last = t
        return self._last


class SimClock:
    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, ts_ms: int) -> None:
        if ts_ms < self._now:
            raise ValueError(f"SimClock can't go backwards: {ts_ms} < {self._now}")
        self._now = ts_ms
