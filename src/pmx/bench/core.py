"""`pmx bench core`: dispatch latency of the engine core — state, models, and the router with a
slot per market — on the load generator's market data, bare and with the tap feeding a real feed
process. It's the engine budget's row until `pmx bench engine` adds risk and the order path.

Inputs are paced in real time at the generated rate (default 20k/s), so the feed process sees a live
load. Each input is timed from `dispatch` in to everything it caused done (its derived forecasts
included); the tap's delivery runs between chunks of inputs, off the dispatch path, and is timed
separately. The pacing spins rather than sleeps: on Apple silicon an idle-then-busy process lands on
whichever core type and clock is free, and latencies stop being comparable between runs. The
observability share is computed within the fed run: (tap appends × their calibrated cost + delivery)
over the rest of dispatch.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from pmx.bench.feed import tap_append_us
from pmx.core.types import Contract
from pmx.engine.clock import SimClock
from pmx.engine.engine import Engine, Tap
from pmx.engine.slots import MarketSelector, Router, StrategySpec
from pmx.engine.state import State, market_id
from pmx.feed import proto
from pmx.feed.demo import LoadGen
from pmx.feed.server import EngineLink, market_snapshot
from pmx.models.market_mid import MarketMid
from pmx.plugins import load_builtin_plugins


class _Capture:
    def __init__(self) -> None:
        self.events: list[tuple[int, object]] = []

    def on_event(self, ev: object, now_ms: int) -> None:
        self.events.append((now_ms, ev))

    def set_models(self, names: list[str]) -> None: ...

    def set_status(self, *args: Any, **kw: Any) -> None: ...


class _Drain:
    """The order manager's seat: takes the router's intents so they don't pile up (noop makes none)."""

    def __init__(self, router: Router) -> None:
        self.router = router

    def on_event(self, ev: object, state: State, now_ms: int) -> list:
        self.router.take_intents()
        return []


def _pct(xs: list[int], q: float) -> float:
    return xs[min(len(xs) - 1, int(q * len(xs)))] / 1e3


async def run_core(*, markets: int, events: int, models: int, slots: bool, feed: Path | None,
                   rate: float = 20_000, chunk: int = 200,
                   attach: Callable[[Engine, asyncio.Event], list[Awaitable[None]]] | None = None) -> dict[str, Any]:
    """`attach(engine, stop)` adds tasks to the engine's loop for the run (e.g. an advisor worker's link)."""
    load_builtin_plugins()
    cap = _Capture()
    gen = LoadGen(cap, markets=markets, rate=rate, slots=0, models=(), now_ms=0)
    setup = [e for e in cap.events if type(e[1]) is Contract or type(e[1]).__name__ == "BookSnapshot"]
    cap.events = []
    for ms in range(10, round(events / rate * 1000) + 1, 10):
        gen.step(ms)
    stream = [(t, e) for t, e in cap.events if type(e).__name__ in ("BookDelta", "Trade")]
    clock = SimClock(0)
    state = State()
    series = sorted({c.series_ticker for c in gen.contracts})
    router = Router([StrategySpec(id="noop", kind="noop", markets=MarketSelector(series=series))]) if slots else None
    stop = asyncio.Event()
    link: EngineLink | None = None
    tap = None
    tasks = []
    acc = [0, 0]
    if feed is not None:
        link = EngineLink(feed, proto.Hello("kalshi", "bench", "demo"), snapshot=lambda: market_snapshot(state),
                          clock=clock.now_ms)

        async def deliver(batch):
            t = time.perf_counter_ns()
            await link.deliver(batch)
            acc[0] += time.perf_counter_ns() - t
            acc[1] += len(batch)

        tap = link.tap = Tap(deliver)
        tasks = [asyncio.ensure_future(tap.run(stop)), asyncio.ensure_future(link.run(stop))]
    engine = Engine(clock, state, models=[MarketMid(f"mid{i}") for i in range(models)], router=router,
                    order_manager=_Drain(router) if router else None, publisher=tap)
    if attach is not None:
        tasks += [asyncio.ensure_future(t) for t in attach(engine, stop)]
    try:
        for _, ev in setup:
            engine.dispatch(ev, 0)
        if link is not None:
            await asyncio.wait_for(link.ready.wait(), 30)
            await asyncio.sleep(0.5)
        acc[0] = acc[1] = 0
        lat: list[int] = []
        ns = time.perf_counter_ns
        dispatch = engine.dispatch
        wall0, t0 = time.monotonic(), stream[0][0]
        for i in range(0, len(stream), chunk):
            for now_ms, ev in stream[i:i + chunk]:
                clock.advance(now_ms)
                t = ns()
                dispatch(ev, now_ms)
                lat.append(ns() - t)
            # Real time: the tap's writer delivers while we wait, as between live inputs.
            due = wall0 + (stream[min(i + chunk, len(stream) - 1)][0] - t0) / 1000
            while True:
                await asyncio.sleep(0)
                if time.monotonic() >= due:
                    break
    finally:
        stop.set()
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 15)
    lat.sort()
    return {
        "markets": markets, "slots": len(router.slots) if router else 0, "models": models, "inputs": len(lat),
        "feed": feed is not None, "p50_us": _pct(lat, 0.5), "p99_us": _pct(lat, 0.99), "max_us": lat[-1] / 1e3,
        "mean_us": statistics.fmean(lat) / 1e3,
        "deliver_us_per_event": acc[0] / acc[1] / 1e3 if acc[1] else None,
        "events_tapped": acc[1], "dropped": link.total_dropped() if link else 0,
        "append_us": tap_append_us() if tap is not None else None,
        "markets_linked": len({market_id(c.venue, c.ticker) for c in gen.contracts}),
    }


async def bench_core(*, sock: Path, markets: int = 5_000, events: int = 200_000, models: int = 2,
                     slots: bool = True) -> list[dict[str, Any]]:
    bare = await run_core(markets=markets, events=events, models=models, slots=slots, feed=None)
    fed = await run_core(markets=markets, events=events, models=models, slots=slots, feed=sock)
    return [bare, fed]


def report(rows: list[dict[str, Any]]) -> str:
    bare, fed = rows
    out = [f"{bare['inputs']:,} market-data inputs over {bare['markets']:,} markets, {bare['models']} market_mid "
           f"models, {bare['slots']:,} noop slots", ""]
    for r in rows:
        what = "with tap → feed process" if r["feed"] else "bare (no observability)"
        out.append(f"{what:<26} p50 {r['p50_us']:6.1f} µs   p99 {r['p99_us']:6.1f} µs   max {r['max_us']:8.1f} µs"
                   f"   mean {r['mean_us']:6.2f} µs")
    per_input = fed["events_tapped"] / fed["inputs"]
    append = fed["append_us"] * per_input
    obs = append + fed["deliver_us_per_event"] * per_input
    core = fed["mean_us"] - append
    out += ["", f"tap: append {fed['append_us']:.2f} µs + delivery {fed['deliver_us_per_event']:.2f} µs per event, "
                f"{per_input:.1f} events tapped per input, dropped {fed['dropped']}",
            f"observability ≈ {obs:.2f} µs per input = {100 * obs / core:.1f}% of the rest of dispatch "
            f"({core:.2f} µs)"]
    return "\n".join(out)
