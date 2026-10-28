"""Live performance for viewers: every `every_s`, and soon after a settlement, each strategy's stats, breakdowns,
and verdict for each mode it has traded and each range (today, 7 days, 30 days, all) go out as `perf` messages.
The work runs in a thread on its own read-only connection to the ledger (WAL lets it read beside the engine's
writes), never on the dispatch path; the messages are handed to the tap from the event loop."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pmx.analytics.breakdowns import breakdowns
from pmx.analytics.stats import TZ, ReadOnly, compute, load
from pmx.analytics.verdict import verdict
from pmx.feed import proto

log = logging.getLogger(__name__)

RANGES: tuple[proto.Range, ...] = ("today", "7d", "30d", "all")


def range_start(name: str, now_ms: int) -> int | None:
    now = datetime.fromtimestamp(now_ms / 1000, TZ)
    if name == "today":
        return int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    if name in ("7d", "30d"):
        return int((now - timedelta(days=int(name[:-1]))).timestamp() * 1000)
    return None


def report(ledger: Any, strategy: str, mode: str, range_name: proto.Range, now_ms: int) -> proto.PerfMsg:
    d = load(ledger, mode=mode, strategy=strategy, start_ms=range_start(range_name, now_ms))
    stats = compute(d)
    rows = breakdowns(d)
    return proto.PerfMsg(strategy, mode, range_name, now_ms, stats, rows, verdict(stats, rows))


def reports(path: Path, env: str, strategies: list[str], now_ms: int) -> list[proto.PerfMsg]:
    """Every strategy × traded mode × range, from a fresh read-only connection (call it off the event loop)."""
    led = ReadOnly(path, env)
    try:
        traded = {(slot.split("/", 1)[0], mode) for slot, mode in led.db.execute(
            "SELECT DISTINCT o.strategy, f.mode FROM fills f JOIN orders o ON o.client_order_id = f.client_order_id "
            "WHERE f.env=?", (env,))}
        return [report(led, sid, mode, r, now_ms) for sid in strategies for mode in ("paper", "live")
                if (sid, mode) in traded for r in RANGES]
    finally:
        led.close()


class PerfPublisher:
    def __init__(self, path: Path, env: str, strategies: list[str], send: Callable[[proto.PerfMsg], None], *,
                 every_s: float = 30.0, clock: Callable[[], int] = lambda: int(time.time() * 1000)) -> None:
        self.path, self.env, self.strategies, self.send = path, env, strategies, send
        self.every_s, self.clock = every_s, clock
        self.runs = 0
        self._soon = asyncio.Event()

    def soon(self) -> None:
        """Publish again shortly (a market settled)."""
        self._soon.set()

    def observe(self, ev: Any, now_ms: int) -> None:
        if type(ev).__name__ == "LogEvent" and " settled at " in ev.text:
            self.soon()

    async def run(self, stop: asyncio.Event) -> None:
        from pmx.sources.base import sleep_or_stop

        loop = asyncio.get_running_loop()
        while not stop.is_set():
            soon, halt = asyncio.ensure_future(self._soon.wait()), asyncio.ensure_future(stop.wait())
            await asyncio.wait({soon, halt}, timeout=self.every_s, return_when=asyncio.FIRST_COMPLETED)
            soon.cancel()
            halt.cancel()
            if stop.is_set() or (self._soon.is_set() and await sleep_or_stop(stop, 1.0)):  # let a settlement land
                return
            self._soon.clear()
            if not self.path.exists():
                continue
            try:
                msgs = await loop.run_in_executor(None, reports, self.path, self.env, self.strategies, self.clock())
            except Exception:
                log.exception("perf: computing performance failed")
                continue
            self.runs += 1
            for m in msgs:
                self.send(m)
