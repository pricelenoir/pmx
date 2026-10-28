"""The source contract (ARCHITECTURE *Sources*). A live source runs as its own task, emits normalized
events into the engine through a `Sink`, and records every raw payload first (invariant 3)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, Protocol

import msgspec

from pmx.engine.events import Event
from pmx.plugins import SOURCES, PluginError, build_params


class Sink(Protocol):
    def emit(self, ev: Event) -> None: ...

    def record(self, source: str, kind: str, payload: str) -> None: ...  # raw capture, never credentials


class Source(Protocol):
    name: str  # instance name from config: what strategies list in `inputs`

    async def run(self, sink: Sink, stop: asyncio.Event) -> None: ...  # never raises; reports SourceStatus


class HistoricalSource(Protocol):
    name: str

    def events(self) -> Iterator[tuple[int, Event]]: ...  # (known_at_ms, event), non-decreasing


class SourceEnv:
    """What `pmx run` hands every source: the venue environment and clients sources share."""

    def __init__(self, env: str = "demo", venues: dict[str, Any] | None = None) -> None:
        self.env = env  # "demo" | "prod"
        self.venues = dict(venues or {})  # venue name -> client (e.g. "kalshi" -> KalshiVenue)


class SourceSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """One `[[source]]` from config."""

    name: str
    kind: str
    params: dict[str, Any] = {}


def build_source(spec: SourceSpec, env: Any = None) -> Source:
    """The configured instance of a registered source kind; bad config raises `PluginError` at startup."""
    cls = SOURCES.get(spec.kind)
    if cls is None:
        raise PluginError(f"source {spec.name!r}: unknown kind {spec.kind!r} (known: {', '.join(sorted(SOURCES))})")
    return cls(spec.name, build_params(cls, spec.params), env)


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Wait `seconds` or until `stop`; True if stopping."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True
