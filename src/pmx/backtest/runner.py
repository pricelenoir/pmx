"""The backtest runner (ARCHITECTURE *Dispatch*, invariant 6). The exact live configuration — the same `Core`
`pmx run` builds: models, strategy slots, risk, the order manager, the paper broker, the portfolio tracker — over
recorded days, as fast as the engine dispatches, deterministically: the same inputs and config give a
byte-identical ledger.

Recorded sources merge by `(receive time, source order, record order)`; a `SimClock` follows them, a `Timer` is
dispatched every `timer_ms` of simulated time in between (every second by default; every `idle_step_s` once nothing
has arrived for `idle_after_s`, when set), and each input goes straight to `Engine.dispatch`. Strategies' log lines
(signals) go to `run_dir/log.txt` and are counted per strategy in the summary.
`start_ms` / `end_ms` cut the days to instants (replaying a live session that started mid-day): records before
`start_ms` only build state (books, trades, entities) and their contracts are held back, then announced at
`start_ms` — as a live engine started then discovers its markets and finds books already trading.
Every slot trades paper (a live strategy is run as paper, with a warning), into its own ledger
`run_dir/ledger.sqlite` next to a copy of the config and the summary. Models run inline with their `latency_ms`;
recorded advice replays as it arrived; advisors aren't run again. A sweep (`walk_forward`) declares a training
window and a later evaluation window, and reports both."""

from __future__ import annotations

import heapq
import logging
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import msgspec

from pmx import config, plugins
from pmx.backtest.raw_source import RawLogSource, sources_for
from pmx.core.types import BookDelta, BookSnapshot, Contract, StreamReset, Trade
from pmx.engine.clock import SimClock
from pmx.engine.config import RunConfig
from pmx.engine.events import LogEvent, Timer
from pmx.engine.run import Core, code_version
from pmx.ledger.db import Ledger

log = logging.getLogger(__name__)

INSTANCE = "bt"  # client order ids `pmx-bt-…`, the same every run
_MARKET_DATA = (BookSnapshot, BookDelta, StreamReset, Trade)  # what `start_ms` warms state with


class StrategyResult(msgspec.Struct):
    equity: int  # micros: realized + unrealized − fees
    realized: int
    unrealized: int
    fees: int
    orders: int
    fills: int


class Summary(msgspec.Struct):
    run_id: str
    start: str
    end: str
    inputs: int  # recorded events dispatched
    timers: int
    sources: dict[str, int]  # events per raw source
    first_ms: int | None
    last_ms: int | None
    clamped: int  # inputs recorded out of order, dispatched at the engine's current time
    orders: int
    fills: int
    strategies: dict[str, StrategyResult]
    wall_s: float  # not part of the result: varies run to run
    inputs_per_s: float
    fidelity: str = "book"  # "candle": rebuilt from 1-minute history, resting-order fills approximate
    logs: dict[str, int] = {}  # log lines (e.g. watch signals) per strategy


def default_run_id(cfg: RunConfig, start: date, end: date) -> str:
    return f"bt-{start:%Y%m%d}-{end:%Y%m%d}-{cfg.hash[:8]}"


def as_paper(cfg: RunConfig) -> RunConfig:
    """Every strategy as paper: a backtest never trades live."""
    if not cfg.live:
        return cfg
    log.warning("backtest: live strategies run as paper (%s)", ", ".join(cfg.live))
    return msgspec.structs.replace(cfg, strategies=[msgspec.structs.replace(s, mode="paper") for s in cfg.strategies])


class _Logs:
    """The backtest's publisher: keeps the slots' log lines, nothing else."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.count: dict[str, int] = {}

    def append(self, ev: Any, now_ms: int) -> None:
        if type(ev) is LogEvent and "/" in ev.source:
            sid = ev.source.split("/", 1)[0]
            self.count[sid] = self.count.get(sid, 0) + 1
            self.lines.append(f"{datetime.fromtimestamp(now_ms / 1000, UTC):%Y-%m-%d %H:%M:%S}Z {ev.level:<5} "
                              f"{ev.source}: {ev.text}")


class Runner:
    """One backtest. `run()` does it all; `step(n)` / `reset()` let a sweep or a learning strategy drive it."""

    def __init__(self, cfg: RunConfig, *, start: date, end: date, run_dir: Path, root: Path | None = None,
                 sources: list[str] | None = None, run_id: str | None = None, env: str = "prod",
                 history: Callable[[], list[Any]] | None = None, fidelity: str = "book", timer_ms: int = 1_000,
                 idle_after_s: float | None = None, idle_step_s: float = 60.0, start_ms: int | None = None,
                 end_ms: int | None = None) -> None:
        """`history` builds the inputs instead of the raw logs (e.g. `NflHistory(...).streams`), fresh on each
        reset; `fidelity` labels the result; `timer_ms`, `idle_after_s`, `idle_step_s` pace the Timers; `start_ms` /
        `end_ms` start and stop inside the first and last days."""
        self.timer_ms = timer_ms
        self.idle_after_ms = None if idle_after_s is None else round(idle_after_s * 1000)
        self.idle_step_ms = round(idle_step_s * 1000)
        if end < start:
            raise ValueError(f"backtest ends ({end}) before it starts ({start})")
        if start_ms is not None and end_ms is not None and end_ms <= start_ms:
            raise ValueError("backtest ends before it starts")
        self.start_ms, self.end_ms = start_ms, end_ms
        plugins.load_builtin_plugins()
        self.cfg, self.start, self.end, self.run_dir, self.env = as_paper(cfg), start, end, run_dir, env
        self.root = root or config.data_root()
        self.only = sources
        self.history, self.fidelity = history, fidelity
        if fidelity != "book":
            log.warning("backtest: %s fidelity — rebuilt from history; resting-order fills are approximate", fidelity)
        self.run_id = run_id or default_run_id(cfg, start, end)
        self.sha, self.dirty = code_version()
        if cfg.advisors:
            log.warning("backtest: advisors aren't run again; their recorded advice replays (%s)",
                        ", ".join(a.name for a in cfg.advisors))
        self.reset()

    def reset(self) -> None:
        """Start over: a fresh engine, a fresh ledger, the sources from the beginning."""
        self.run_dir.mkdir(parents=True, exist_ok=True)
        path = self.run_dir / "ledger.sqlite"
        for p in (path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")):
            p.unlink(missing_ok=True)
        self.ledger = Ledger(path, env=self.env)
        self.clock = SimClock(0)
        self.logs = _Logs()
        self.core = Core(self.cfg, env=self.env, clock=self.clock, ledger=self.ledger, instance=INSTANCE,
                         run_id=self.run_id, sha=self.sha, dirty=self.dirty, data=self.root, backtest=True,
                         publisher=self.logs)
        self.sources: list[Any] = self.history() if self.history is not None else \
            sources_for(self.cfg, self.root, self.start, self.end, only=self.only)
        self._inputs = self._merged()
        self.inputs = self.timers = self.clamped = 0
        self.first_ms: int | None = None
        self._held: dict[tuple[str, str], tuple[str, Contract]] = {}  # contracts before start_ms, announced at it
        self._next_timer: int | None = None
        self._last_input_ms = 0
        self._finished = False
        self._t0 = time.perf_counter()

    def _merged(self) -> Iterator[tuple[int, int, int, str, Any]]:
        def tagged(order: int, src: RawLogSource) -> Iterator[tuple[int, int, int, str, Any]]:
            for seq, (ms, ev) in enumerate(src.events()):
                yield ms, order, seq, src.name, ev

        return heapq.merge(*(tagged(i, s) for i, s in enumerate(self.sources)), key=lambda r: r[:3])

    @property
    def now_ms(self) -> int:
        return self.clock.now_ms()

    def step(self, n: int = 1) -> int:
        """Dispatch up to `n` recorded inputs (and the Timers before each); returns how many — 0 at the end."""
        engine, clock = self.core.engine, self.clock
        done = 0
        for ms, _, _, name, ev in self._inputs:
            if self.end_ms is not None and ms > self.end_ms:
                if self.first_ms is not None:
                    self._tick(self.end_ms)
                break
            if self.first_ms is None:
                if self.start_ms is not None and ms < self.start_ms:
                    self._warm(ms, name, ev)
                    continue
                t0 = ms if self.start_ms is None else self.start_ms
                self.first_ms = t0
                clock.advance(t0)
                self._start(t0)
                self._next_timer = (t0 // self.timer_ms + 1) * self.timer_ms
                for src, c in sorted(self._held.values(), key=lambda h: h[1].ticker):
                    engine.dispatch(c, t0, src)
                    self.inputs += 1
                self._held = {}
            self._tick(ms)
            if ms < clock.now_ms():
                self.clamped += 1
                ms = clock.now_ms()
            clock.advance(ms)
            engine.dispatch(ev, ms, name)
            self._last_input_ms = ms
            self.inputs += 1
            done += 1
            if done >= n:
                break
        else:
            if self.end_ms is not None and self.first_ms is not None:
                self._tick(self.end_ms)  # the inputs ran out first: the session still ran until end_ms
        return done

    def _tick(self, ms: int) -> None:
        """The Timers due up to `ms`."""
        assert self._next_timer is not None
        while self._next_timer <= ms:
            t = self._next_timer
            self.clock.advance(t)
            self.core.engine.dispatch(Timer(t), t, "timer")
            self.timers += 1
            idle = self.idle_after_ms is not None and t - self._last_input_ms >= self.idle_after_ms
            self._next_timer = t + (self.idle_step_ms if idle else self.timer_ms)
            if idle and self._next_timer > ms:  # inputs resume: back to the usual pace, on the grid after this one
                self._next_timer = (ms // self.timer_ms + 1) * self.timer_ms

    def _warm(self, ms: int, name: str, ev: Any) -> None:
        """Before `start_ms`: books, trades and entities into state — no strategies, models, or orders. Advice and
        schedules are dropped: an engine started at `start_ms` wouldn't have them either."""
        if ms > self.clock.now_ms():
            self.clock.advance(ms)
        state = self.core.state
        if type(ev) is Contract:
            self._held[(ev.venue, ev.ticker)] = (name, ev)  # the newest metadata wins
        elif type(ev) in _MARKET_DATA or state.is_entity(ev):
            state.apply(ev, self.clock.now_ms(), name)

    def _start(self, now_ms: int) -> None:
        self.ledger.start_run(self.run_id, instance=INSTANCE, started_ms=now_ms, git_sha=self.sha, dirty=self.dirty,
                              config_hash=self.cfg.hash, config_toml=self.cfg.text)
        self.core.portfolio.start(now_ms)

    def run(self) -> Summary:
        while self.step(10_000):
            pass
        return self.finish()

    def finish(self) -> Summary:
        """Stop every slot, cancel what's still working, close the ledger, write the summary."""
        if self._finished:
            raise RuntimeError("this backtest already finished; reset() to run it again")
        self._finished = True
        now = self.clock.now_ms()
        engine = self.core.engine
        if self.first_ms is not None:
            for ev in self.core.router.stop_all(now):
                engine.dispatch(ev, now, "")
            engine.dispatch(Timer(now), now, "timer")
            self.ledger.cancel_open("paper", "backtest ended", now)
            self.ledger.end_run(self.run_id, now)
        summary = self._summary(now)
        self.ledger.close()
        (self.run_dir / "config.toml").write_text(self.cfg.text)
        (self.run_dir / "log.txt").write_text("".join(line + "\n" for line in self.logs.lines))
        (self.run_dir / "summary.json").write_bytes(msgspec.json.format(msgspec.json.encode(summary)))
        return summary

    def _summary(self, now: int) -> Summary:
        db = self.ledger.db
        by: dict[str, StrategyResult] = {}
        for s in self.cfg.strategies:
            st = self.core.portfolio.stats(f"strategy:{s.id}", "paper")
            orders = db.execute("SELECT COUNT(*) FROM orders WHERE strategy LIKE ?", (s.id + "/%",)).fetchone()[0]
            fills = db.execute("SELECT COUNT(*) FROM fills f JOIN orders o ON o.client_order_id = f.client_order_id "
                               "WHERE o.strategy LIKE ?", (s.id + "/%",)).fetchone()[0]
            by[s.id] = StrategyResult(st.equity, st.realized, st.unrealized, st.fees, orders, fills)
        wall = time.perf_counter() - self._t0
        start = self.start.isoformat() if self.start_ms is None else _iso(self.start_ms)
        end = self.end.isoformat() if self.end_ms is None else _iso(self.end_ms)
        return Summary(self.run_id, start, end, self.inputs, self.timers,
                       {s.raw: s.count for s in self.sources}, self.first_ms,
                       None if self.first_ms is None else now, self.clamped,
                       db.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
                       db.execute("SELECT COUNT(*) FROM fills").fetchone()[0], by, round(wall, 3),
                       round(self.inputs / wall, 1) if wall > 0 else 0.0, self.fidelity, dict(self.logs.count))


def run(cfg: RunConfig, *, start: date, end: date, run_dir: Path, **kw: Any) -> Summary:
    """One backtest, start to finish."""
    return Runner(cfg, start=start, end=end, run_dir=run_dir, **kw).run()


def walk_forward(configs: Mapping[str, RunConfig], *, train: tuple[date, date], evaluate: tuple[date, date],
                 out_dir: Path, **kw: Any) -> list[dict[str, Any]]:
    """A sweep: every variant on the training window and on a later evaluation window — both are reported,
    so a variant is never judged only on the window it was picked on."""
    if evaluate[0] <= train[1]:
        raise ValueError(f"the evaluation window must start after the training window ends ({train[1]})")
    rows = []
    for label, cfg in configs.items():
        tr = run(cfg, start=train[0], end=train[1], run_dir=out_dir / label / "train", **kw)
        ev = run(cfg, start=evaluate[0], end=evaluate[1], run_dir=out_dir / label / "evaluate", **kw)
        rows.append({"variant": label, "train": tr, "evaluate": ev})
    return rows


def day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC).date()


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{ms % 1000:03d}Z"


def instant(s: str) -> tuple[date, int | None]:
    """`YYYY-MM-DD` (the whole UTC day: ms None) or an ISO time `YYYY-MM-DDTHH:MM[:SS]Z` (UTC unless an offset is
    given) → (its UTC day, epoch ms)."""
    if "T" not in s:
        return day(s), None
    t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    t = t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)
    return t.date(), round(t.timestamp() * 1000)
