"""Where a model's `on_event` runs (ARCHITECTURE *Models*).

- `loop` (default): inline in the engine's dispatch; keep it to microseconds.
- `process`: in a worker process at lower OS priority. The engine side (`ProcessModel`) forwards the
  events the model's class lists in `event_types` — plus every `Contract` and `StreamReset`, so the
  worker's own `State` can link and drop books — and returns at once. Forecasts come back through a
  source sink: stamped with their receive time, queued, and recorded raw, so a replay is exact.
- In backtests a model always runs inline; with `latency_ms` its outputs are held back that long in
  engine time (`Delayed`), so a slow model doesn't look instant and results stay deterministic."""

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import os
import threading
from collections import deque
from collections.abc import Mapping
from typing import Any, Literal

import msgspec

from pmx.core.types import Contract, StreamReset
from pmx.engine.events import Event, Forecast, LogEvent
from pmx.engine.state import State
from pmx.models.base import Model, ModelEnv
from pmx.plugins import MODELS, Linker, PluginError, build_params

log = logging.getLogger(__name__)


class ModelSpec(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One `[[model]]` from config."""

    name: str
    kind: str
    params: dict[str, Any] = {}
    runtime: Literal["loop", "process"] = "loop"
    latency_ms: int = 0  # backtests: outputs are held back this long (engine time)


def build_model(spec: ModelSpec, env: ModelEnv | None = None, *, backtest: bool = False,
                linkers: Mapping[str, Linker] | None = None) -> Model:
    """The configured instance, in its runtime. Bad config raises `PluginError` at startup."""
    cls = MODELS.get(spec.kind)
    if cls is None:
        raise PluginError(f"model {spec.name!r}: unknown kind {spec.kind!r} (known: {', '.join(sorted(MODELS))})")
    params = build_params(cls, spec.params)
    if spec.runtime == "process" and not backtest:
        return ProcessModel(spec.name, cls, params, env, linkers=linkers)
    m = cls(spec.name, params, env)
    return Delayed(m, spec.latency_ms) if backtest and spec.latency_ms else m


class Delayed:
    """Runs a model inline but releases each of its outputs `latency_ms` of engine time after the event
    that caused it, on the next event dispatched at or past that time. Forecasts are re-stamped then."""

    def __init__(self, model: Model, latency_ms: int) -> None:
        self.model, self.latency_ms, self.name = model, latency_ms, model.name
        self._held: deque[tuple[int, Event]] = deque()

    def wants(self, ev: Event) -> bool:
        return True  # any event may be the one that releases held outputs

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        held, out = self._held, []
        while held and held[0][0] <= now_ms:
            out.append(held.popleft()[1])
        if self.model.wants(ev):
            due = now_ms + self.latency_ms
            for e in self.model.on_event(ev, state, now_ms) or ():
                held.append((due, msgspec.structs.replace(e, ts_ms=due) if type(e) is Forecast else e))
        return out


# ---- worker processes -----------------------------------------------------------------------------


def _worker(name: str, cls: type, params: Any, env: Any, linkers: dict[str, Linker], inbox: Any, outbox: Any,
            nice: int) -> None:
    """The worker's loop: its own `State` from forwarded events, the model on the ones it wants."""
    try:
        os.nice(nice)
    except OSError:
        pass
    model = cls(name, params, env)
    state = State(linkers)
    while (item := inbox.get()) is not None:
        recv_ms, ev = item
        try:
            state.apply(ev, recv_ms)
            if model.wants(ev):
                for out in model.on_event(ev, state, recv_ms) or ():
                    outbox.put(out)
        except Exception as e:  # reported, not fatal: the next event may be fine
            outbox.put(LogEvent("error", f"model:{name}", f"{type(e).__name__}: {e}", recv_ms))
    outbox.put(None)


class ProcessModel:
    """The engine's side of a model in a worker process. Call `attach(sink)` (a `SourceSink` named
    after the model) before dispatching and `close()` at shutdown. `on_event` never blocks: if the
    worker falls `max_queue` events behind, further events are dropped and counted."""

    def __init__(self, name: str, cls: type, params: Any, env: Any = None, *,
                 linkers: Mapping[str, Linker] | None = None, max_queue: int = 10_000, nice: int = 5) -> None:
        types = getattr(cls, "event_types", None)
        if not types:
            raise PluginError(f"model {name!r}: runtime 'process' needs `event_types` on {cls.__qualname__}")
        self.name = name
        self.types = frozenset((*types, Contract, StreamReset))
        self.dropped = 0
        ctx = mp.get_context("spawn")
        self._inbox, self._outbox = ctx.Queue(max_queue), ctx.Queue()
        self._proc = ctx.Process(target=_worker, name=f"pmx-model-{name}", daemon=True,
                                 args=(name, cls, params, env, dict(linkers or {}), self._inbox, self._outbox, nice))
        self._reader: threading.Thread | None = None

    def attach(self, sink: Any, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Start the worker; its outputs are emitted into `sink` (on the engine's loop) and recorded raw."""
        from pmx.recorder.rawlog import encode_event

        loop = loop or asyncio.get_running_loop()
        self._proc.start()

        def pump() -> None:
            while (ev := self._outbox.get()) is not None:
                sink.record(self.name, "e", encode_event(ev))
                loop.call_soon_threadsafe(sink.emit, ev)

        self._reader = threading.Thread(target=pump, name=f"pmx-model-{self.name}-out", daemon=True)
        self._reader.start()

    def wants(self, ev: Event) -> bool:
        return type(ev) in self.types

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        try:
            self._inbox.put_nowait((now_ms, ev))
        except Exception:  # queue.Full: the worker is behind
            self.dropped += 1
            if self.dropped == 1:
                return [LogEvent("warn", f"model:{self.name}", "worker fell behind; dropping its events", now_ms)]
        return []

    def close(self, timeout: float = 5.0) -> None:
        if self._proc.is_alive():
            self._inbox.put(None)
            self._proc.join(timeout)
            if self._proc.is_alive():
                self._proc.terminate()
        if self._reader is not None:
            self._reader.join(timeout)
