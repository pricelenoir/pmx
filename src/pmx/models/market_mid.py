"""`market_mid`: P(YES) = the book's mid. The baseline every other model is measured against."""

from __future__ import annotations

from typing import Any

import msgspec

from pmx.core.types import BookDelta, BookSnapshot
from pmx.core.units import ONE_DOLLAR
from pmx.engine.events import Event
from pmx.engine.state import State, market_id
from pmx.models.base import forecast
from pmx.plugins import model

VERSION = "market_mid/1"


@model("market_mid")
class MarketMid:
    event_types = (BookSnapshot, BookDelta)  # for `runtime = "process"`

    class Params(msgspec.Struct, frozen=True):
        min_change: float = 0.001  # probability points: smaller moves of the mid don't make a forecast

    def __init__(self, name: str, params: Params | None = None, env: Any = None) -> None:
        self.name, self.p = name, params or self.Params()
        self.last: dict[str, float] = {}

    def wants(self, ev: Event) -> bool:
        t = type(ev)
        return t is BookDelta or t is BookSnapshot

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        mid_id = market_id(ev.venue, ev.ticker)
        book = state.book(mid_id)
        mid = None if book is None else book.mid()
        if mid is None:
            return []
        p = mid / ONE_DOLLAR
        last = self.last.get(mid_id)
        if last is not None and abs(p - last) < self.p.min_change:
            return []
        self.last[mid_id] = p
        return [forecast(self.name, mid_id, p, now_ms, ev.ts_ms or now_ms, version=VERSION)]
