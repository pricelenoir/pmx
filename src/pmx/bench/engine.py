"""`pmx bench engine`: dispatch latency of the whole engine `pmx run` builds — state, models, the router
with a slot per market, risk, the order manager and scheduler, the paper broker, the portfolio tracker, the
ledger — on the load generator's market data, with a quoting strategy on every market so the order path, fills,
and portfolio work are in it. Inputs and a `Timer` every simulated second are dispatched as fast as they go (a
backtest's pace); each is timed from `dispatch` in to everything it caused done. Budget: p99 < 1 ms (ARCHITECTURE
*Budgets*)."""

from __future__ import annotations

import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from pmx.core.types import Contract
from pmx.engine.clock import SimClock
from pmx.engine.config import load
from pmx.engine.events import Timer
from pmx.engine.run import Core
from pmx.feed.demo import LoadGen
from pmx.ledger.db import Ledger
from pmx.plugins import load_builtin_plugins

CONFIG = """
[engine]
record = false
[risk]
max_loss = "100000.00"
max_open_orders = 100000
[[model]]
name = "mid"
kind = "market_mid"
[[strategy]]
id = "quote"
kind = "scripted"
markets = {{ series = {series} }}
params = {{ loop = true, jitter_s = 10, steps = [
  {{ at = "+0s", action = "quote", side = "bid", price = "mid-0.03", qty = "1" }},
  {{ at = "+0s", action = "quote", side = "ask", price = "mid+0.03", qty = "1" }},
  {{ at = "+20s", action = "cancel_all" }},
  {{ at = "+30s", action = "cancel_all" }} ] }}
"""


class _Capture:
    def __init__(self) -> None:
        self.events: list[tuple[int, object]] = []

    def on_event(self, ev: object, now_ms: int) -> None:
        self.events.append((now_ms, ev))

    def set_models(self, names: list[str]) -> None: ...

    def set_status(self, *args: Any, **kw: Any) -> None: ...


def _pct(xs: list[int], q: float) -> float:
    return xs[min(len(xs) - 1, int(q * len(xs)))] / 1e3


def run_engine(*, markets: int = 1_000, events: int = 100_000, rate: float = 5_000) -> dict[str, Any]:
    load_builtin_plugins()
    cap = _Capture()
    gen = LoadGen(cap, markets=markets, rate=rate, slots=0, models=(), now_ms=0)
    setup = [e for e in cap.events if type(e[1]) is Contract or type(e[1]).__name__ == "BookSnapshot"]
    cap.events = []
    for ms in range(10, round(events / rate * 1000) + 1, 10):
        gen.step(ms)
    stream = [(t, e) for t, e in cap.events if type(e).__name__ in ("BookDelta", "Trade")]
    series = sorted({c.series_ticker for c in gen.contracts})
    cfg = load(text=CONFIG.format(series="[" + ", ".join(f'"{s}"' for s in series) + "]"))
    with tempfile.TemporaryDirectory() as tmp:
        ledger = Ledger(Path(tmp) / "ledger.sqlite", env="bench")
        clock = SimClock(0)
        core = Core(cfg, env="bench", clock=clock, ledger=ledger, instance="bench", run_id="bench", sha="bench",
                    dirty=False, data=Path(tmp), backtest=True)
        core.portfolio.start(0)
        dispatch = core.engine.dispatch
        for _, ev in setup:
            dispatch(ev, 0)
        lat: list[int] = []
        slow: list[tuple[int, str, int]] = []  # (ns, event type, engine ms)
        timers = 0
        ns = time.perf_counter_ns
        next_timer = 1_000
        t0 = time.perf_counter()
        for now_ms, ev in stream:
            while next_timer <= now_ms:
                clock.advance(next_timer)
                t = ns()
                dispatch(Timer(next_timer), next_timer, "timer")
                lat.append(d := ns() - t)
                slow.append((d, "Timer", next_timer))
                timers += 1
                next_timer += 1_000
            clock.advance(now_ms)
            t = ns()
            dispatch(ev, now_ms, "kalshi")
            lat.append(d := ns() - t)
            if d > 5_000_000:
                slow.append((d, type(ev).__name__, now_ms))
        wall = time.perf_counter() - t0
        orders = ledger.db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        fills = ledger.db.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
        ledger.close()
    lat.sort()
    return {"markets": markets, "slots": len(core.router.slots), "inputs": len(stream), "timers": timers,
            "orders": orders, "fills": fills, "p50_us": _pct(lat, 0.5), "p99_us": _pct(lat, 0.99),
            "max_us": lat[-1] / 1e3, "mean_us": statistics.fmean(lat) / 1e3,
            "dispatches_per_s": round(len(lat) / wall),
            "slowest": [(round(d / 1e6, 1), what, at) for d, what, at in sorted(slow, reverse=True)[:5]]}


def report(r: dict[str, Any]) -> str:
    ok = "within" if r["p99_us"] < 1_000 else "OVER"
    return (f"engine (whole Core) at {r['markets']:,} markets, {r['slots']:,} quoting slots: {r['inputs']:,} inputs + "
            f"{r['timers']:,} timers, {r['orders']:,} orders, {r['fills']:,} fills\n"
            f"  dispatch p50 {r['p50_us']:.1f} µs · p99 {r['p99_us']:.1f} µs ({ok} the 1 ms budget) · max "
            f"{r['max_us']:.0f} µs · {r['dispatches_per_s']:,} dispatches/s\n"
            f"  slowest: " + ", ".join(f"{ms} ms {what} at {at / 1000:.0f} s" for ms, what, at in r["slowest"]))
