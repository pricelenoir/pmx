"""The `Venue` seam. A connector does two jobs that stay separate on purpose:
`stream()` yields raw frames (what the recorder stores verbatim) and `decode()`
turns a frame into normalized events (what strategies consume). Replay runs the
same `decode()` over recorded frames, so live and backtest see identical events.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from typing import Protocol

import msgspec

from pmx.core.types import (
    BookSnapshot,
    Contract,
    Fill,
    MarketEvent,
    OrderAck,
    OrderRequest,
    VenueOrder,
    VenuePosition,
)
from pmx.core.units import Micros, Qty


class Frame(msgspec.Struct, frozen=True):
    recv_ns: int
    text: str


class Control(msgspec.Struct, frozen=True):
    """A local stream event (connect, gap, disconnect) — recorded alongside frames."""

    recv_ns: int
    event: str
    detail: dict = {}


type StreamItem = Frame | Control


class MarketStream(Protocol):
    def __aiter__(self) -> AsyncIterator[StreamItem]: ...
    async def add(self, tickers: Iterable[str]) -> None: ...
    async def remove(self, tickers: Iterable[str]) -> None: ...
    async def close(self) -> None: ...


class Decoder(Protocol):
    def __call__(self, item: StreamItem) -> list[MarketEvent]: ...


class Venue(Protocol):
    name: str

    async def list_contracts(
        self, *, series: str | None = None, event: str | None = None, status: str | None = "open"
    ) -> list[Contract]: ...
    async def fetch_book(self, ticker: str) -> BookSnapshot: ...
    async def place(self, req: OrderRequest) -> OrderAck: ...
    async def cancel(self, order_id: str, ticker: str) -> Qty: ...
    async def orders(self, status: str | None = None, since_ms: int | None = None) -> list[VenueOrder]: ...
    async def order(self, order_id: str) -> VenueOrder: ...
    async def queue_positions(self, tickers: list[str]) -> dict[str, Qty]: ...
    async def positions(self) -> list[VenuePosition]: ...
    async def fills(self, since_ms: int | None = None) -> list[Fill]: ...
    async def balance(self) -> Micros: ...
    async def balances(self) -> tuple[Micros, dict[int, Micros]]: ...  # total, per exchange shard
    def stream(self, tickers: Iterable[str], *, trades: bool = True, fills: bool = False,
               orders: bool = False) -> MarketStream: ...
    def decoder(self) -> Decoder: ...
