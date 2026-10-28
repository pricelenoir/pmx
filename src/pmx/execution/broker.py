"""Brokers (ARCHITECTURE *Execution*). The order manager sends each write the order scheduler releases to
the broker for the order's mode.

`PaperBroker` executes on engine time — `place_now` / `cancel_now` return the resulting fills and
order updates as events — so paper sessions and backtests are deterministic. `LiveBroker` has the same
interface: it starts the venue call and returns nothing, and what the venue answers comes back later as
engine events. Two broker conventions the order manager relies on: a `pending` update means the write's
outcome is unknown (its slot gets no new orders until that's resolved), and an `open` live update with reason
`ADOPTED` is an order an earlier run placed, which the order manager takes on."""

from __future__ import annotations

from typing import Protocol

from pmx.core.types import OrderAck, OrderRequest
from pmx.core.units import Qty
from pmx.engine.events import Event, Mode
from pmx.engine.state import State

ADOPTED = "adopted from an earlier run"


class Brokers:
    """The engine's broker step over several brokers (paper and live)."""

    def __init__(self, *brokers: SyncBroker) -> None:
        self.brokers = brokers

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        out: list[Event] = []
        for b in self.brokers:
            out += b.on_event(ev, state, now_ms)
        return out


class Broker(Protocol):
    """A live venue."""

    mode: Mode

    async def place(self, req: OrderRequest) -> OrderAck: ...

    async def cancel(self, order_id: str, ticker: str) -> Qty: ...

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]: ...


class SyncBroker(Protocol):
    """A broker that executes on engine time (paper)."""

    mode: Mode

    def place_now(self, req: OrderRequest, slot: str, state: State, now_ms: int) -> list[Event]: ...

    def cancel_now(self, client_order_id: str, now_ms: int) -> list[Event]: ...

    def queue_ahead(self, client_order_id: str) -> Qty | None: ...

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]: ...
