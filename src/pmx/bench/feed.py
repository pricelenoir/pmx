"""`pmx bench feed`: the viewer feed under load. For each viewer count, the load generator
(`feed/demo.py:LoadGen`) plays the engine and publishes through the real tap — to the feed process,
or with `inprocess` to a publisher + hub in this process — while counting viewers
run as their own processes (`bench/viewer.py`). Reported per case, over the same measured window:

- engine-side observability cost per event: the tap's append (calibrated here) plus the time inside
  its delivery, measured in place. (CPU-time differences against a generator-only baseline aren't
  used: on Apple silicon they swing with the core type a run lands on.) This process's CPU share
  is shown for context, next to the generator alone;
- feed process CPU, link drops and resyncs;
- per viewer: messages and bytes a second, and the busiest market's `book` rate (traded or not).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from pmx.core.types import BookDelta
from pmx.engine.engine import Tap, to_publisher
from pmx.feed import proto
from pmx.feed.demo import LoadGen
from pmx.feed.hub import FeedHub
from pmx.feed.publisher import Publisher
from pmx.feed.server import EngineLink, TapPublisher


class _Null:
    """The baseline: the generator alone."""

    def on_event(self, ev: object, now_ms: int) -> None: ...

    def set_models(self, names: list[str]) -> None: ...

    def set_status(self, *args: Any, **kw: Any) -> None: ...


def cpu_seconds(pid: int) -> float | None:
    """A process's CPU time so far, from `ps` (`[[dd-]hh:]mm:ss.ss`)."""
    out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    if not out:
        return None
    days, _, rest = out.rpartition("-")
    secs = 0.0
    for part in rest.split(":"):
        secs = secs * 60 + float(part)
    return secs + (int(days) * 86_400 if days else 0)


def tap_append_us(n: int = 200_000) -> float:
    """The tap's per-event cost on the engine thread, µs."""
    async def nothing(batch) -> None: ...

    tap = Tap(nothing, capacity=n + 1)
    ev = BookDelta("kalshi", "T-1", "bid", 5_000, 100, 0)
    t = time.perf_counter()
    for i in range(n):
        tap.append(ev, i)
    return (time.perf_counter() - t) / n * 1e6


def _timed(deliver, acc: list[int]):
    async def wrapped(batch):
        t = time.perf_counter_ns()
        await deliver(batch)
        acc[0] += time.perf_counter_ns() - t
        acc[1] += len(batch)
    return wrapped


async def _accepting(sock: Path, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            _, w = await asyncio.open_unix_connection(str(sock))
            w.close()
            return
        except OSError:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.05)


async def run_case(mode: str, viewers: int, *, sock: Path, markets: int, rate: float, slots: float,
                   seconds: float, warmup: float) -> dict[str, Any]:
    """One measurement. `mode`: "baseline" (generator only) · "process" · "inprocess"."""
    stop = asyncio.Event()
    acc = [0, 0]  # ns inside the tap's delivery, events delivered
    tasks: list[asyncio.Future] = []
    link: EngineLink | None = None
    server = None
    hello = proto.Hello("kalshi", "bench", "demo")
    if mode == "baseline":
        gen = LoadGen(_Null(), markets=markets, rate=rate, slots=slots)
    elif mode == "process":
        link = EngineLink(sock, hello)
        tap = link.tap = Tap(_timed(link.deliver, acc))
        gen = LoadGen(TapPublisher(tap), markets=markets, rate=rate, slots=slots)
        link.snapshot = gen.market_snapshot
        tasks += [asyncio.ensure_future(tap.run(stop)), asyncio.ensure_future(link.run(stop))]
    elif mode == "inprocess":
        hub = FeedHub(hello, queue_max=1_000_000)
        tap = Tap(_timed(to_publisher(Publisher(hub)), acc))
        gen = LoadGen(TapPublisher(tap), markets=markets, rate=rate, slots=slots)
        server = await hub.serve(sock)
        tasks.append(asyncio.ensure_future(tap.run(stop)))
    else:
        raise ValueError(f"mode {mode!r}")

    async def engine() -> None:
        while not stop.is_set():
            gen.step(int(time.time() * 1000))
            await asyncio.sleep(0.02)

    tasks.append(asyncio.ensure_future(engine()))
    procs = []
    try:
        if mode != "baseline":
            await _accepting(sock)
        procs = [await asyncio.create_subprocess_exec(
            sys.executable, "-m", "pmx.bench.viewer", str(sock), "--seconds", str(seconds), "--warmup", str(warmup),
            stdout=asyncio.subprocess.PIPE) for _ in range(viewers)]
        await asyncio.sleep(warmup)
        feed_pid = link.proc.pid if link is not None and link.proc is not None else None
        cpu0, ev0, wall0 = time.process_time(), gen.n_events, time.monotonic()
        feed0 = cpu_seconds(feed_pid) if feed_pid else None
        acc[0] = acc[1] = 0
        dropped0 = link.total_dropped() if link is not None else 0
        await asyncio.sleep(seconds)
        cpu, events, wall = time.process_time() - cpu0, gen.n_events - ev0, time.monotonic() - wall0
        feed1 = cpu_seconds(feed_pid) if feed_pid else None
        seen = [json.loads((await p.communicate())[0]) for p in procs]
    finally:
        stop.set()
        for p in procs:
            with contextlib.suppress(ProcessLookupError):
                p.kill()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 15)
        if server is not None:
            server.close()
            sock.unlink(missing_ok=True)
    return {
        "mode": mode, "viewers": viewers, "markets": len(gen.contracts), "events_per_s": round(events / wall),
        "engine_cpu_pct": round(100 * cpu / wall, 1), "engine_us_per_event": cpu / max(events, 1) * 1e6,
        "deliver_us_per_event": acc[0] / max(acc[1], 1) / 1e3 if acc[1] else None,
        "feed_cpu_pct": round(100 * (feed1 - feed0) / wall, 1) if feed0 is not None and feed1 is not None else None,
        "dropped": (link.total_dropped() - dropped0) if link is not None else 0,
        "resyncs": link.resyncs if link is not None else 0, "per_viewer": seen,
    }


async def bench_feed(*, sock: Path, markets: int = 5_000, rate: float = 20_000, slots: float = 0.02,
                     seconds: float = 10.0, warmup: float = 3.0, viewers: tuple[int, ...] = (0, 1, 3),
                     inprocess: bool = False) -> list[dict[str, Any]]:
    kw = dict(sock=sock, markets=markets, rate=rate, slots=slots, seconds=seconds, warmup=warmup)
    rows = [await run_case("baseline", 0, **kw)]
    for n in viewers:
        row = await run_case("inprocess" if inprocess else "process", n, **kw)
        row["append_us_per_event"] = append = tap_append_us()
        row["obs_us_per_event"] = append + (row["deliver_us_per_event"] or 0.0)
        row["obs_core_pct"] = round(row["obs_us_per_event"] * row["events_per_s"] / 1e4, 2)
        rows.append(row)
    return rows


def _mean(viewers: list[dict[str, Any]], key: str) -> float:
    return sum(v[key] for v in viewers) / len(viewers)


def report(rows: list[dict[str, Any]]) -> str:
    base = rows[0]
    out = [f"generator alone: {base['events_per_s']:,} events/s over {base['markets']:,} markets, "
           f"{base['engine_us_per_event']:.2f} µs/event, {base['engine_cpu_pct']}% CPU", ""]
    head = (f"{'mode':<10}{'viewers':>8}{'events/s':>10}{'obs µs/ev':>10}{'(core %)':>9}{'engine %':>10}"
            f"{'feed %':>8}{'dropped':>9}{'msg/s/viewer':>14}{'KB/s/viewer':>13}{'max book/s':>12}{'traded':>8}")
    out += [head, "-" * len(head)]
    for r in rows[1:]:
        v = r["per_viewer"]
        msgs = "-" if not v else f"{_mean(v, 'msgs_per_s'):,.0f}"
        kbs = "-" if not v else f"{_mean(v, 'bytes_per_s') / 1e3:,.0f}"
        book = "-" if not v else f"{max(x['max_books_per_s'] for x in v):.2f}"
        traded = "-" if not v else f"{max(x['max_books_per_s_traded'] for x in v):.2f}"
        feed = "-" if r["feed_cpu_pct"] is None else f"{r['feed_cpu_pct']}"
        out.append(f"{r['mode']:<10}{r['viewers']:>8}{r['events_per_s']:>10,}{r['obs_us_per_event']:>10.2f}"
                   f"{r['obs_core_pct']:>9}{r['engine_cpu_pct']:>10}{feed:>8}{r['dropped']:>9,}{msgs:>14}{kbs:>13}"
                   f"{book:>12}{traded:>8}")
    out += ["", "obs = the tap's append + its delivery, per event (core % = of one core at that rate); engine % = "
                "this process's CPU incl. the generator; feed % = the feed process"]
    if any(r["per_viewer"] for r in rows[1:]):
        v = next(r["per_viewer"][0] for r in rows[1:] if r["per_viewer"])
        out += ["", "one viewer, by message: " + ", ".join(f"{k} {n:,.0f}/s" for k, n in v["by_tag_per_s"].items())]
    return "\n".join(out)
