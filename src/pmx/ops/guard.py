"""`pmx guard`: the dead-man watchdog for a live engine.

Live resting orders stay at Kalshi when the engine that manages them dies or hangs, and Kalshi offers no
cancel-on-disconnect. A live `pmx run` writes a heartbeat file every second from its event loop (`Heartbeat`)
and starts a guard in its own session. The guard watches it: an engine whose process is gone while its
heartbeat file remains (a crash, `kill -9`) trips at once; one whose heartbeat is older than `guard_s` and
whose control socket doesn't answer (a hung loop) trips then. Tripping cancels every resting order of this
engine instance (`pmx-<instance>-…`) over REST — the guard's own Kalshi client, nothing of the engine's —
and alerts. A hung engine that recovers re-arms the guard. A clean shutdown cancels its orders, then removes
the heartbeat, and the guard exits."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pmx.lifecycle import alive
from pmx.ops.notify import notify
from pmx.sources.base import sleep_or_stop

log = logging.getLogger(__name__)


def heartbeat_path(sock: Path) -> Path:
    """data/pmx.sock -> data/pmx.heartbeat."""
    return sock.with_suffix(".heartbeat")


def guard_pidfile(sock: Path) -> Path:
    """data/pmx.sock -> data/pmx-guard.pid."""
    return sock.with_name(sock.stem + "-guard.pid")


class Heartbeat:
    """The engine side: `{pid, instance, env, run_id, ts_ms}`, replaced atomically every `every_s`."""

    every_s = 1.0

    def __init__(self, path: Path, *, instance: str, env: str, run_id: str) -> None:
        self.path = path
        self.body = {"pid": os.getpid(), "instance": instance, "env": env, "run_id": run_id}

    def beat(self) -> None:
        tmp = self.path.with_suffix(".heartbeat.tmp")
        tmp.write_text(json.dumps({**self.body, "ts_ms": int(time.time() * 1000)}))
        os.replace(tmp, self.path)

    async def run(self, stop: asyncio.Event) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            self.beat()
            if await sleep_or_stop(stop, self.every_s):
                return

    def remove(self) -> None:
        """A clean stop: nothing left for the guard to do."""
        self.path.unlink(missing_ok=True)


class Guard:
    def __init__(self, venue: Any, *, instance: str, heartbeat: Path, ctl: Path | None = None, guard_s: float = 10.0,
                 alert: Callable[[str, str], None] = notify, every_s: float = 1.0) -> None:
        self.venue, self.instance, self.heartbeat, self.ctl = venue, instance, heartbeat, ctl
        self.prefix = f"pmx-{instance}-"
        self.guard_s, self.alert, self.every_s = guard_s, alert, every_s
        self.trips = 0
        self.canceled = 0

    async def engine(self) -> str:
        """`waiting` (no heartbeat yet), `alive`, `stale` (hung), `dead` (gone, heartbeat left), `stopped`."""
        try:
            hb = json.loads(self.heartbeat.read_text())
            age_s = time.time() - self.heartbeat.stat().st_mtime
        except FileNotFoundError:
            return "stopped"
        except (ValueError, OSError):
            return "alive"  # mid-replace: look again next time
        if not alive(int(hb["pid"])):
            return "dead"
        if age_s <= self.guard_s:
            return "alive"
        return "alive" if await self._answers() else "stale"

    async def _answers(self) -> bool:
        if self.ctl is None:
            return False
        from pmx.control import protocol
        from pmx.control.client import NotRunning, request

        try:
            return (await request(self.ctl, protocol.Status(), timeout=2.0)).ok
        except (NotRunning, TimeoutError, OSError):
            return False

    async def cancel_all(self) -> int:
        """Cancel every resting order of this instance; returns how many went."""
        resting = [o for o in await self.venue.orders(status="resting") if o.client_order_id.startswith(self.prefix)]
        results = await asyncio.gather(*(self._cancel(o) for o in resting))
        n = sum(results)
        self.canceled += n
        if n < len(resting):
            log.error("guard: %d of %d resting orders couldn't be canceled", len(resting) - n, len(resting))
        return n

    async def _cancel(self, o: Any) -> bool:
        for attempt in range(4):
            try:
                await self.venue.cancel(o.order_id, o.ticker)
                return True
            except Exception as e:  # the venue says it's gone (404) or anything else: try again, then give up
                if getattr(e, "status", None) == 404:
                    return True
                log.warning("guard: cancel %s failed (%s), attempt %d", o.client_order_id, e, attempt + 1)
                await asyncio.sleep(0.5 * 2**attempt)
        return False

    async def run(self, stop: asyncio.Event, *, start_s: float = 120.0) -> None:
        """Watch until the engine stops cleanly, or dies and its orders are canceled. Gives up after `start_s` if
        the engine never sends a first heartbeat."""
        seen = tripped = failed = False
        started = time.monotonic()
        log.info("guard: watching %s (instance %s, %.0f s)", self.heartbeat, self.instance, self.guard_s)
        while True:
            state = await self.engine()
            if state == "alive":
                if tripped:
                    log.warning("guard: the engine is answering again; re-armed")
                seen, tripped = True, False
            elif state == "stopped" and (seen or time.monotonic() - started > start_s):
                log.info("guard: %s; exiting", "the engine stopped cleanly" if seen else "no engine ever started")
                return
            elif state in ("dead", "stale") and not tripped:
                what = "died" if state == "dead" else f"has been unresponsive for over {self.guard_s:.0f} s"
                try:
                    n = await self.cancel_all()
                except Exception as e:  # keep trying: these orders are nobody's now
                    log.exception("guard: couldn't cancel")
                    if not failed:
                        self.alert("pmx guard", f"the live engine {what} and canceling its orders failed ({e}); "
                                                "retrying. Check open orders on Kalshi")
                    failed = True
                else:
                    tripped, failed = True, False
                    self.trips += 1
                    self.alert("pmx guard", f"the live engine {what}: canceled {n} resting orders")
                    if state == "dead":
                        self.heartbeat.unlink(missing_ok=True)
                        return
            if await sleep_or_stop(stop, self.every_s if not failed else 5.0):
                return
