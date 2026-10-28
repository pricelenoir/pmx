"""Venue-neutral records. Everything is quoted from the YES side: a `bid` buys YES,
an `ask` sells YES (equivalently buys NO at 1 - price). Positions are signed YES
quantity: negative means long NO.
"""

from __future__ import annotations

import uuid
from typing import Literal

import msgspec

from pmx.core.fees import KALSHI_DEFAULT_FEES, FeeSchedule
from pmx.core.units import ONE_DOLLAR, Micros, Price, Qty

type BookSide = Literal["bid", "ask"]
type TimeInForce = Literal["gtc", "ioc", "fok"]
type ContractStatus = Literal["unopened", "open", "paused", "closed", "determined", "settled"]


class PriceBand(msgspec.Struct, frozen=True):
    start: Price
    end: Price
    step: Price


class PriceGrid(msgspec.Struct, frozen=True):
    """A market's valid prices. Orders off the grid are rejected by the venue."""

    bands: tuple[PriceBand, ...]

    def _band(self, p: Price) -> PriceBand | None:
        for b in self.bands:
            if b.start <= p <= b.end:
                return b
        return None

    def is_valid(self, p: Price) -> bool:
        b = self._band(p)
        return 0 < p < ONE_DOLLAR and b is not None and (p - b.start) % b.step == 0

    def snap(self, p: Price, *, up: bool = False) -> Price:
        """Nearest valid price at or below `p` (at or above with `up`), clamped to (0, 1)."""
        p = min(max(p, 1), ONE_DOLLAR - 1)
        b = self._band(p)
        if b is None:
            raise ValueError(f"price {p} outside grid")
        off = (p - b.start) % b.step
        if off:
            p = p - off + (b.step if up else 0)
        if p <= 0:
            p = b.start + b.step
        if p >= ONE_DOLLAR:
            p = ONE_DOLLAR - b.step
        return p

    def tick_below(self, p: Price) -> Price:
        return self.snap(p - 1)

    def tick_above(self, p: Price) -> Price:
        return self.snap(p + 1, up=True)


CENT_GRID = PriceGrid((PriceBand(0, ONE_DOLLAR, 100),))


class Contract(msgspec.Struct, frozen=True):
    venue: str
    ticker: str
    event_ticker: str
    title: str
    subtitle: str
    status: ContractStatus
    close_ts_ms: int | None
    grid: PriceGrid = CENT_GRID
    result: Literal["yes", "no"] | None = None
    series_ticker: str = ""
    event_ts_ms: int | None = None  # when the underlying event is expected to resolve
    rules: str = ""  # the venue's resolution criteria, verbatim
    settlement_source: str = ""
    fees: FeeSchedule = KALSHI_DEFAULT_FEES  # the series' schedule
    settlement: Price | None = None  # YES settlement value once settled; not always 0 or 1 (a tie can pay 0.50)
    shard: int = 0  # the venue's exchange shard (Kalshi `exchange_index`): balances are per shard


class OrderRequest(msgspec.Struct, frozen=True):
    venue: str
    ticker: str
    side: BookSide
    price: Price
    qty: Qty
    tif: TimeInForce = "gtc"
    post_only: bool = False
    client_order_id: str = msgspec.field(default_factory=lambda: uuid.uuid4().hex)
    strategy: str = ""


class OrderAck(msgspec.Struct, frozen=True):
    venue: str
    order_id: str
    client_order_id: str
    filled: Qty
    remaining: Qty
    ts_ms: int


class Fill(msgspec.Struct, frozen=True):
    venue: str
    fill_id: str
    order_id: str
    ticker: str
    side: BookSide
    price: Price  # YES price
    qty: Qty
    fee: Micros
    is_taker: bool
    ts_ms: int
    client_order_id: str = ""


class VenueOrder(msgspec.Struct, frozen=True):
    """The venue's view of one of the account's orders (Kalshi `user_orders`, `GET /portfolio/orders`)."""

    venue: str
    order_id: str
    client_order_id: str
    ticker: str
    side: BookSide
    price: Price  # YES price
    qty: Qty  # initial
    filled: Qty
    remaining: Qty
    status: Literal["resting", "canceled", "executed", "unknown"]
    ts_ms: int
    reason: str = ""  # why it last changed, when the venue says


class Trade(msgspec.Struct, frozen=True):
    """A public print."""

    venue: str
    trade_id: str
    ticker: str
    price: Price  # YES price
    qty: Qty
    taker_side: BookSide
    ts_ms: int


class VenuePosition(msgspec.Struct, frozen=True):
    venue: str
    ticker: str
    qty: Qty  # signed YES
    realized_pnl: Micros
    fees: Micros


class BookSnapshot(msgspec.Struct, frozen=True):
    venue: str
    ticker: str
    bids: tuple[tuple[Price, Qty], ...]
    asks: tuple[tuple[Price, Qty], ...]
    ts_ms: int


class BookDelta(msgspec.Struct, frozen=True):
    venue: str
    ticker: str
    side: BookSide
    price: Price
    delta: Qty
    ts_ms: int


class StreamReset(msgspec.Struct, frozen=True):
    """Books for this venue are no longer trustworthy (reconnect, sequence gap)."""

    venue: str
    reason: str
    ts_ms: int


type MarketEvent = BookSnapshot | BookDelta | Trade | Fill | StreamReset | VenueOrder
