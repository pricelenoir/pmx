"""`pmx bench soak`: the load generator for hours with the engine side here, a real feed
process, and whatever viewers attach (a TUI in tmux), sampling every `every_s`: resident memory and
open files of this process, the feed process, and named viewer pids; the tap's ring and the link's
socket buffer; drops. Memory, handles, and queues must plateau. Writes CSV, one row per sample."""

from __future__ import annotations

import asyncio
import csv
import os
import subprocess
import time
from pathlib import Path

from pmx.engine.engine import Tap
from pmx.feed import proto
from pmx.feed.demo import LoadGen
from pmx.feed.server import EngineLink, TapPublisher

FIELDS = ("t_s", "events", "engine_rss_mb", "engine_fds", "feed_rss_mb", "feed_fds", "viewer_rss_mb", "viewer_fds",
          "tap_ring", "link_buffer_kb", "dropped", "resyncs", "restarts")


def rss_mb(pid: int) -> float | None:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return round(int(out) / 1024, 1) if out else None


def open_files(pid: int) -> int | None:
    if pid == os.getpid():
        return len(os.listdir("/dev/fd"))
    out = subprocess.run(["lsof", "-p", str(pid)], capture_output=True, text=True).stdout
    return max(0, len(out.splitlines()) - 1) if out else None


async def soak(*, sock: Path, out: Path, hours: float, markets: int, rate: float, slots: float, every_s: float,
               viewer_pids: list[int]) -> None:
    stop = asyncio.Event()
    link = EngineLink(sock, proto.Hello("kalshi", "soak", "demo"))
    tap = link.tap = Tap(link.deliver)
    gen = LoadGen(TapPublisher(tap), markets=markets, rate=rate, slots=slots)
    link.snapshot = gen.market_snapshot

    async def engine() -> None:
        while not stop.is_set():
            gen.step(int(time.time() * 1000))
            await asyncio.sleep(0.02)

    async def sample() -> None:
        start = time.monotonic()
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            while not stop.is_set():
                feed = link.proc.pid if link.proc is not None and link.proc.returncode is None else None
                viewers = [(rss_mb(p), open_files(p)) for p in viewer_pids]
                buf = link._writer.transport.get_write_buffer_size() if link.up else 0
                w.writerow([round(time.monotonic() - start), gen.n_events, rss_mb(os.getpid()), open_files(os.getpid()),
                            feed and rss_mb(feed), feed and open_files(feed),
                            "/".join(str(v[0]) for v in viewers), "/".join(str(v[1]) for v in viewers),
                            len(tap), round(buf / 1024, 1), link.total_dropped(), link.resyncs, link.restarts])
                f.flush()
                if time.monotonic() - start >= hours * 3600:
                    stop.set()
                    return
                try:
                    await asyncio.wait_for(stop.wait(), every_s)
                except TimeoutError:
                    pass

    await asyncio.gather(engine(), tap.run(stop), link.run(stop), sample())
