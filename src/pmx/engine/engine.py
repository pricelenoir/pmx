"""The engine: one single-threaded, deterministic dispatch loop (ARCHITECTURE *Dispatch*).

Every input enters through `emit`, stamped with its receive time, and is dispatched in that order
through state → broker → portfolio → models → router → order manager → observability tap. Events
those steps return (forecasts, fills, order updates, logs) are dispatched FIFO right after their
cause and before the next input, so the sequence depends only on the inputs: live, paper, and
backtest see the same thing. Every collaborator is optional; one that raises is logged and the
engine carries on."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Hashable, Iterable
from typing import Literal, Protocol

import msgspec

from pmx.core.types import StreamReset
from pmx.engine.clock import Clock, LiveClock
from pmx.engine.events import Event, LogEvent, Refused, SourceStatus, Timer
from pmx.engine.state import State

log = logging.getLogger(__name__)


class Component(Protocol):
    """A dispatch step (broker, portfolio tracker, router, order manager): sees every event after
    state reflects it and returns events to dispatch next."""

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]: ...


class Model(Protocol):
    name: str  # instance name from config

    def wants(self, ev: Event) -> bool: ...

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]: ...


class Publisher(Protocol):
    """The observability tap: must take an event in O(1) and never block."""

    def append(self, ev: Event, now_ms: int) -> None: ...


class Recorder(Protocol):
    def record(self, source: str, kind: str, payload: str) -> None: ...


_ALL = "*"  # a model disabled for every key
_json = msgspec.json.Encoder()

type QueuePolicy = Literal["conflate", "drop_oldest", "reset"]
type _Entry = list  # [receive ms, source name, event (None once dropped), SourceSink | None, conflation key]


def _default_key(ev: Event) -> Hashable:
    return type(ev).__name__, getattr(ev, "ticker", None) or getattr(ev, "entity", None)


class SourceSink:
    """A source's way into the engine (`Sink`, ARCHITECTURE *Sources*): `emit` queues an event stamped
    with its receive time; `record` is raw capture through the engine's recorder.

    At most `max` of a source's events wait at once. Past that, `policy` decides: `conflate` keeps
    only the latest event per key (a newer one replaces a queued one at any depth) and drops the oldest
    beyond `max` keys; `drop_oldest` drops the oldest; `reset` (venue books, where a lost delta corrupts
    a book) drops all of them and queues a `StreamReset` for `venue`, then calls `on_overflow` so the
    source can re-snapshot. Overflows are counted and reported as a `SourceStatus`, at most once a second."""

    def __init__(self, engine: Engine, name: str, *, max: int, policy: QueuePolicy,
                 key: Callable[[Event], Hashable] | None, venue: str) -> None:
        self.engine, self.name, self.max, self.policy, self.venue = engine, name, max, policy, venue
        self.key = key or _default_key
        self.overflows = 0
        self.last_ms: int | None = None  # receive time of the newest event (health)
        self.on_overflow: list[Callable[[], None]] = []
        self._entries: deque[_Entry] = deque()  # this source's queued entries, oldest first (some dropped)
        self._latest: dict[Hashable, _Entry] = {}  # conflate: key -> its queued entry
        self._pending = 0
        self._reported_ms = -1_000

    @property
    def pending(self) -> int:
        return self._pending

    def emit(self, ev: Event) -> None:
        key = None
        if self.policy == "conflate":
            key = self.key(ev)
            old = self._latest.get(key)
            if old is not None and old[2] is not None:
                old[2] = None  # replaced by the newer one: not an overflow
                self._pending -= 1
        if self._pending >= self.max:
            self._overflow()
        entry = self.engine._enqueue(ev, self.name, self, key)
        self.last_ms = entry[0]
        self._entries.append(entry)
        self._pending += 1
        if key is not None:
            self._latest[key] = entry

    def record(self, source: str, kind: str, payload: str) -> None:
        if self.engine.recorder is not None:
            self.engine.recorder.record(source, kind, payload)

    def _dispatched(self, entry: _Entry) -> None:
        entries = self._entries
        while entries and entries[0] is not entry:
            entries.popleft()  # dropped ones ahead of it
        if entries:
            entries.popleft()
        self._pending -= 1
        if entry[4] is not None and self._latest.get(entry[4]) is entry:
            del self._latest[entry[4]]

    def _overflow(self) -> None:
        self.overflows += 1
        now = self.engine.clock.now_ms()
        if self.policy == "reset":
            for e in self._entries:
                e[2] = None
            self._entries.clear()
            self._latest.clear()
            self._pending = 0
            self.engine._enqueue(StreamReset(self.venue or self.name, "queue overflow", now), self.name, None, None)
            for hook in self.on_overflow:
                hook()
        else:
            entries = self._entries
            while entries:
                e = entries.popleft()
                if e[2] is not None:
                    e[2] = None
                    self._pending -= 1
                    break
        if now - self._reported_ms >= 1_000:
            self._reported_ms = now
            text = f"queue overflow: {self.overflows} dropped ({self.policy}, max {self.max})"
            self.engine._enqueue(SourceStatus(self.name, True, text, now), self.name, None, None)


class Engine:
    yield_every = 1_000  # dispatch lets sources and the tap writer run at least this often

    def __init__(
        self,
        clock: Clock,
        state: State,
        *,
        models: Iterable[Model] = (),
        router: Component | None = None,
        order_manager: Component | None = None,
        broker: Component | None = None,
        portfolio: Component | None = None,
        publisher: Publisher | None = None,
        recorder: Recorder | None = None,
        sleep_gap_ms: int = 5_000,
        max_derived: int = 100_000,
    ) -> None:
        self.clock, self.state = clock, state
        self.models = list(models)
        self.publisher, self.recorder = publisher, recorder
        steps = {"broker": broker, "portfolio": portfolio, "router": router, "order_manager": order_manager}
        self._before_models = [(n, steps[n]) for n in ("broker", "portfolio") if steps[n] is not None]
        self._after_models = [(n, steps[n]) for n in ("router", "order_manager") if steps[n] is not None]
        # A Timer more than this after the previous one means the machine slept. Live only.
        self.sleep_gap_ms = sleep_gap_ms if isinstance(clock, LiveClock) else None
        self.on_sleep_gap: list[Callable[[int], None]] = []  # e.g. a venue source re-snapshotting; must not block
        self.max_derived = max_derived  # per input event; past it the chain is cut (a component feeding itself)
        self.now_ms = clock.now_ms()  # receive time of the event being dispatched
        self.dispatched = 0
        self.max_queue_depth = 0
        self.errors: dict[str, int] = {}  # component -> failures
        self.sources: dict[str, SourceSink] = {}
        self._queue: deque[_Entry] = deque()
        self._wake = asyncio.Event()
        self._disabled: dict[str, set[str]] = {}  # model -> markets/entities it's off for
        self._last_timer_ms: int | None = None

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    def emit(self, ev: Event, source: str = "") -> None:
        """Queue an input, stamped with its receive time on the engine clock. `source` is the
        emitting source's name, which strategies list as `inputs`. Unbounded: sources use `source()`."""
        self._enqueue(ev, source, None, None)

    def source(self, name: str, *, max: int = 10_000, policy: QueuePolicy = "conflate",
               key: Callable[[Event], Hashable] | None = None, venue: str = "") -> SourceSink:
        """The bounded `Sink` a source named `name` emits into (config `queue = { max, policy }`)."""
        if name in self.sources:
            raise ValueError(f"source {name!r} already has a sink")
        sink = self.sources[name] = SourceSink(self, name, max=max, policy=policy, key=key, venue=venue)
        return sink

    def _enqueue(self, ev: Event, source: str, sink: SourceSink | None, key: Hashable) -> _Entry:
        entry = [self.clock.now_ms(), source, ev, sink, key]
        q = self._queue
        q.append(entry)
        if len(q) > self.max_queue_depth:
            self.max_queue_depth = len(q)
        self._wake.set()
        return entry

    async def dispatch_all(self) -> int:
        """Dispatch until the queue is empty (backtests: the runner advances the clock, emits, and
        calls this). Returns the number of inputs dispatched."""
        n = 0
        while self._queue:
            n += self._dispatch_some(self.yield_every)
            await asyncio.sleep(0)
        return n

    async def run(self, stop: asyncio.Event) -> None:
        """Live: dispatch inputs as they arrive until `stop`. Inputs still queued then are left for
        the shutdown sequence."""
        stopping = asyncio.ensure_future(stop.wait())
        stopping.add_done_callback(lambda _: self._wake.set())
        try:
            while not stop.is_set():
                if not self._queue:
                    self._wake.clear()
                    await self._wake.wait()
                    continue
                self._dispatch_some(self.yield_every)
                await asyncio.sleep(0)
        finally:
            stopping.cancel()

    def _dispatch_some(self, limit: int) -> int:
        q = self._queue
        n = 0
        while q and n < limit:
            entry = q.popleft()
            recv_ms, source, ev, sink = entry[0], entry[1], entry[2], entry[3]
            n += 1
            if ev is None:  # dropped by its source's queue policy
                continue
            if sink is not None:
                sink._dispatched(entry)
            self.dispatch(ev, recv_ms, source)
        return n

    def dispatch(self, ev: Event, now_ms: int, source: str = "") -> None:
        """One input and everything it causes. `now_ms` is its receive time."""
        self.now_ms = now_ms
        if self.sleep_gap_ms is not None and type(ev) is Timer:
            self._chain(self._sleep_check(now_ms), now_ms)  # books drop before anything acts on this Timer
        self._chain(self._steps(ev, now_ms, source), now_ms)

    def _chain(self, derived: list[Event], now_ms: int) -> None:
        pending = deque(derived)
        n = 0
        while pending:
            n += 1
            if n > self.max_derived:
                text = f"more than {self.max_derived} events derived from one input; dropped {len(pending)}"
                log.error(text)
                if self.publisher is not None:
                    self.publisher.append(LogEvent("error", "engine", text, now_ms), now_ms)
                return
            pending.extend(self._steps(pending.popleft(), now_ms, ""))

    def _steps(self, ev: Event, now_ms: int, source: str) -> list[Event]:
        state = self.state
        out: list[Event] = []
        try:
            out += state.apply(ev, now_ms, source)
        except Refused as e:  # e.g. malformed advice: dropped, and said so
            out.append(LogEvent("warn", e.source, str(e), now_ms))
            return out
        except Exception as e:  # not reflected in state, so nothing downstream may see it
            self._failed("state", ev, e, out, now_ms)
            return out
        for name, c in self._before_models:
            try:
                out += c.on_event(ev, state, now_ms) or ()
            except Exception as e:
                self._failed(name, ev, e, out, now_ms)
        disabled = self._disabled
        for m in self.models:
            if disabled and m.name in disabled and self._is_disabled(m.name, ev):
                continue
            try:
                if m.wants(ev):
                    out += m.on_event(ev, state, now_ms) or ()
            except Exception as e:
                self._disable(m.name, ev, e, out, now_ms)
        for name, c in self._after_models:
            try:
                out += c.on_event(ev, state, now_ms) or ()
            except Exception as e:
                self._failed(name, ev, e, out, now_ms)
        if self.publisher is not None:
            try:
                self.publisher.append(ev, now_ms)
            except Exception as e:
                self._failed("publisher", ev, e, None, now_ms)  # not published: the publisher is what's broken
        self.dispatched += 1
        return out

    # ---- failures ------------------------------------------------------------

    def _failed(self, component: str, ev: Event, e: Exception, out: list[Event] | None, now_ms: int) -> None:
        """Count it, log the first traceback, and queue a `LogEvent` into `out` — unless the failure
        was on a log (a component failing on logs must not feed itself more of them)."""
        count = self.errors[component] = self.errors.get(component, 0) + 1
        text = f"{component} failed on {type(ev).__name__}: {type(e).__name__}: {e}"
        if count == 1:
            log.error(text, exc_info=e)
        if out is not None and type(ev) is not LogEvent:
            out.append(LogEvent("error", component, text, now_ms))

    def _is_disabled(self, model: str, ev: Event) -> bool:
        off = self._disabled[model]
        if _ALL in off:
            return True
        ticker, entity = self.state.keys_of(ev)
        return (ticker is not None and ticker in off) or (entity is not None and entity in off)

    def _disable(self, model: str, ev: Event, e: Exception, out: list[Event], now_ms: int) -> None:
        ticker, entity = self.state.keys_of(ev)
        key = entity if ticker is None and entity is not None else ticker or _ALL
        self._disabled.setdefault(model, set()).add(key)
        component = f"model:{model}"
        self.errors[component] = self.errors.get(component, 0) + 1
        scope = "everything" if key == _ALL else key
        text = f"model {model} raised on {type(ev).__name__}: {type(e).__name__}: {e}; disabled for {scope}"
        log.error(text, exc_info=e)
        out.append(LogEvent("error", component, text, now_ms))

    def disabled(self, model: str) -> set[str]:
        """Markets/entities a model has been switched off for after raising ("*" = all)."""
        return set(self._disabled.get(model, ()))

    # ---- sleep -----------------------------------------------------------------

    def _sleep_check(self, now_ms: int) -> list[Event]:
        last, self._last_timer_ms = self._last_timer_ms, now_ms
        assert self.sleep_gap_ms is not None
        if last is None or now_ms - last <= self.sleep_gap_ms:
            return []
        gap = now_ms - last
        try:
            if self.recorder is not None:
                self.recorder.record("engine", "c", _json.encode({"event": "sleep_gap", "gap_ms": gap}).decode())
            for hook in self.on_sleep_gap:
                hook(gap)
        except Exception:
            log.exception("sleep-gap record/hook failed")
        text = f"wall clock jumped {gap / 1000:.1f} s between timers (likely a sleep); books re-snapshot"
        log.warning(text)
        return [LogEvent("warn", "engine", text, now_ms),
                *(StreamReset(v, "sleep_gap", now_ms) for v in sorted(self.state.venues()))]


# ---- observability tap ---------------------------------------------------

type Batch = list[tuple[int, Event]]  # (engine ms, event), in dispatch order


class Tap:
    """A bounded ring the engine appends every dispatched event to in O(1). `run` drains it in
    batches to `deliver` (the in-process hub, or the feed process) off the dispatch path.
    When the ring is full the event is dropped and counted — the engine never waits on viewers — and
    a warning `LogEvent` is delivered where the gap is, so a consumer knows what to resync."""

    def __init__(self, deliver: Callable[[Batch], Awaitable[None]], *, capacity: int = 65_536,
                 batch: int = 1_024) -> None:
        self.deliver = deliver
        self.capacity, self.batch = capacity, batch
        self.dropped = 0
        self._reported = 0
        self._taken = 0  # events handed to `deliver` so far
        self._gap: tuple[int, int] | None = None  # (events ahead of the first unreported drop, its ms)
        self._ring: deque[tuple[int, Event]] = deque()
        self._wake = asyncio.Event()

    def __len__(self) -> int:
        return len(self._ring)

    def append(self, ev: Event, now_ms: int) -> None:
        ring = self._ring
        n = len(ring)
        if n >= self.capacity:
            if self._gap is None:
                self._gap = (self._taken + n, now_ms)
            self.dropped += 1
            return
        ring.append((now_ms, ev))
        if not n:  # the writer only sleeps on an empty ring
            self._wake.set()

    def discard(self) -> Batch:
        """Take everything queued without delivering it — the consumer is about to get a snapshot that
        already reflects it (a viewer's resync) — and any pending drop warning with it."""
        out = list(self._ring)
        self._taken += len(out)
        self._ring.clear()
        self._gap, self._reported = None, self.dropped
        return out

    def take(self) -> Batch:
        """The next batch, oldest first, with the drop warning in its place."""
        ring = self._ring
        n = min(len(ring), self.batch)
        self._taken += n
        gap = self._gap
        if gap is None or gap[0] > self._taken:
            return [ring.popleft() for _ in range(n)]
        ahead = n - (self._taken - gap[0])
        out = [ring.popleft() for _ in range(ahead)]
        dropped, self._reported, self._gap = self.dropped - self._reported, self.dropped, None
        out.append((gap[1], LogEvent("warn", "engine", f"viewer feed fell behind: {dropped} events dropped", gap[1])))
        out += [ring.popleft() for _ in range(n - ahead)]
        return out

    async def run(self, stop: asyncio.Event) -> None:
        """Deliver batches until `stop`, then what's left. A `deliver` that never returns keeps this
        task waiting (cancel it); dispatch is unaffected either way."""
        stopping = asyncio.ensure_future(stop.wait())
        stopping.add_done_callback(lambda _: self._wake.set())
        try:
            while True:
                if not self._ring and self._gap is None:
                    if stop.is_set():
                        return
                    self._wake.clear()
                    await self._wake.wait()
                    continue
                await self.deliver(self.take())
                await asyncio.sleep(0)
        finally:
            stopping.cancel()


def to_publisher(publisher) -> Callable[[Batch], Awaitable[None]]:
    """`Tap` delivery into `feed/publisher.py:Publisher` in this process."""

    async def deliver(batch: Batch) -> None:
        for now_ms, ev in batch:
            publisher.on_event(ev, now_ms)

    return deliver
