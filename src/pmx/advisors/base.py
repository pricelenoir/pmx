"""What an advisor is and gets (ARCHITECTURE *Advisors*). An advisor is a class registered with
`@advisor("kind")`, built as `cls(name, params, env)`, whose `run(view, sink, stop)` the worker keeps
running. It reads `view` (a `StateView`), sends `Advice` to `sink`, and makes its own outside calls
through `env.call` so the worker can enforce its budgets. There is nothing here to place an order with.

Budgets live in the advisor's params (`AdvisorParams`): `timeout_s` per call, `max_calls_per_min`,
`max_cost_per_day` (the advisor reports each call's cost with `env.charge`). Over a budget, the advisor
is paused until the window resets — its next `env.call` waits — and that is logged."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, TypeVar

import msgspec

from pmx.core.book import OrderBook
from pmx.core.units import Micros, Qty, parse_micros
from pmx.engine.events import Advice, EntityState, Forecast
from pmx.plugins import ADVISORS, PluginError, build_params

T = TypeVar("T")


class StateView(Protocol):
    """Read-only, eventually consistent: built from the viewer feed, so it may lag the engine."""

    def book(self, ticker: str) -> OrderBook | None: ...

    def data(self, entity: str) -> EntityState | None: ...  # as the domain renders it (summary, fields)

    def forecast(self, ticker: str, model: str) -> Forecast | None: ...

    def position(self, slot: str) -> Qty: ...

    def tickers(self, *, series: str | None = None) -> list[str]: ...


class AdviceSink(Protocol):
    def advise(self, a: Advice) -> None: ...  # recorded raw by the engine first, then queued


class Advisor(Protocol):
    name: str  # instance name from config

    async def run(self, view: StateView, sink: AdviceSink, stop: asyncio.Event) -> None: ...


class AdvisorParams(msgspec.Struct, frozen=True, kw_only=True):
    """Budgets every advisor's `Params` can inherit; the worker enforces them through `env`."""

    timeout_s: float = 30.0  # per `env.call`
    max_calls_per_min: int | None = None
    max_cost_per_day: str | None = None  # dollars, e.g. "5.00"; the day is UTC


class AdvisorSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """One `[[advisor]]` from config."""

    name: str
    kind: str
    params: dict[str, Any] = {}


class Paused(Exception):
    """Raised into an advisor's `env.call` only if the worker is stopping while it waits out a budget."""


class AdvisorEnv:
    """What the worker hands an advisor: its clock, its log, and budgeted outside calls."""

    window_s = 60.0  # `max_calls_per_min`'s window

    def __init__(self, name: str, params: Any = None, *, clock: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
                 note: Callable[[str, str], None] = lambda level, text: None,
                 stop: asyncio.Event | None = None) -> None:
        self.name = name
        self.log = logging.getLogger(f"pmx.advisor.{name}")
        self.clock = clock  # engine time: the wall clock live, the engine's clock in a backtest
        self.note = note  # a line for the engine's log (and the TUI), e.g. a budget pause
        self.stop = stop or asyncio.Event()
        self.timeout_s = getattr(params, "timeout_s", 30.0)
        self.max_calls_per_min = getattr(params, "max_calls_per_min", None)
        cap = getattr(params, "max_cost_per_day", None)
        self.max_cost_per_day: Micros | None = None if cap is None else parse_micros(cap)
        self.calls = 0
        self.cost: Micros = 0  # today's
        self._day = self._today()
        self._recent: deque[float] = deque()  # monotonic times of calls in the last minute
        self.paused_until: float | None = None  # monotonic

    def now_ms(self) -> int:
        return self.clock()

    @staticmethod
    def _today() -> date:
        return datetime.now(UTC).date()

    async def call(self, aw: Awaitable[T], *, what: str = "call") -> T:
        """Await an outside call under the advisor's budgets: wait out a pause, count it against
        `max_calls_per_min`, and time it out after `timeout_s` (`TimeoutError`)."""
        try:
            await self._wait_budget()
        except Paused:
            if inspect.iscoroutine(aw):
                aw.close()  # never started: don't leave it dangling
            raise
        self._recent.append(time.monotonic())
        self.calls += 1
        try:
            return await asyncio.wait_for(aw, self.timeout_s)
        except TimeoutError:
            self.note("warn", f"{what} timed out after {self.timeout_s:g}s")
            raise

    def charge(self, cost: str | Micros) -> None:
        """Report what a call cost (dollars as a string, or micros); past `max_cost_per_day` the
        advisor pauses until the next UTC day."""
        self._roll_day()
        self.cost += parse_micros(cost) if isinstance(cost, str) else cost
        if self.max_cost_per_day is not None and self.cost >= self.max_cost_per_day and self.paused_until is None:
            tomorrow = datetime.combine(self._day + timedelta(days=1), datetime.min.time(), UTC)
            self._pause((tomorrow - datetime.now(UTC)).total_seconds(),
                        f"spent ${self.cost / 1e6:.2f} today (max_cost_per_day ${self.max_cost_per_day / 1e6:.2f})")

    def _roll_day(self) -> None:
        today = self._today()
        if today != self._day:
            self._day, self.cost = today, 0

    def _pause(self, seconds: float, why: str) -> None:
        self.paused_until = time.monotonic() + seconds
        self.note("warn", f"over budget: {why}; paused for {seconds:.1f}s")

    async def _wait_budget(self) -> None:
        cap = self.max_calls_per_min
        if cap is not None:
            now = time.monotonic()
            while self._recent and now - self._recent[0] >= self.window_s:
                self._recent.popleft()
            if len(self._recent) >= cap and self.paused_until is None:
                self._pause(self.window_s - (now - self._recent[0]),
                            f"{len(self._recent)} calls in the last minute (max_calls_per_min {cap})")
        while self.paused_until is not None:
            left = self.paused_until - time.monotonic()
            if left <= 0:
                self.paused_until = None
                self._roll_day()
                self.note("info", "budget window reset: running again")
                break
            try:
                await asyncio.wait_for(self.stop.wait(), left)
            except TimeoutError:
                continue
            raise Paused(f"{self.name}: stopping while paused")


def build_advisor(spec: AdvisorSpec, env_for: Callable[[str, Any], AdvisorEnv]) -> tuple[Any, AdvisorEnv]:
    """The configured instance and its env. Bad config raises `PluginError` at start-up."""
    cls = ADVISORS.get(spec.kind)
    if cls is None:
        known = ", ".join(sorted(ADVISORS)) or "none"
        raise PluginError(f"advisor {spec.name!r}: unknown kind {spec.kind!r} (known: {known})")
    params = build_params(cls, spec.params)
    env = env_for(spec.name, params)
    return cls(spec.name, params, env), env


def backtest_refusal(cls: type, start: date) -> str | None:
    """Why `cls` may not run inside a backtest starting `start` (backtests replay recorded advice
    instead), or None. Only a deterministic advisor — pinned local weights, temperature 0, no network —
    whose `trained_until` is before the window may: anything else may have seen the outcome. A
    class declares them as `deterministic = True` and `trained_until = "YYYY-MM-DD"` (the newest data it
    could have learned from)."""
    name = getattr(cls, "__name__", repr(cls))
    if not getattr(cls, "deterministic", False):
        return f"advisor {name} isn't deterministic: backtests replay its recorded advice only"
    until = getattr(cls, "trained_until", None)
    if until is None:
        return f"advisor {name} declares no trained_until"
    if start <= date.fromisoformat(until):
        return f"the backtest starts {start}, not after advisor {name}'s trained_until {until}"
    return None
