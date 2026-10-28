"""Kalshi wire shapes (fixed-point `_dollars` / `_fp` fields only) and their mapping
to core types. Unknown fields are ignored, so new API fields don't break decoding."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import msgspec

from pmx.core.fees import KALSHI_DEFAULT_FEES, FeeSchedule
from pmx.core.types import (
    CENT_GRID,
    BookDelta,
    BookSide,
    BookSnapshot,
    Contract,
    ContractStatus,
    Fill,
    PriceBand,
    PriceGrid,
    Trade,
    VenueOrder,
    VenuePosition,
)
from pmx.core.units import complement, parse_micros, parse_price, parse_qty

VENUE = "kalshi"

type Level = tuple[str, str]  # [price_dollars, count_fp]

_STATUS: dict[str, ContractStatus] = {
    "initialized": "unopened",
    "active": "open",
    "inactive": "paused",
    "closed": "closed",
    "determined": "determined",
    "disputed": "determined",
    "amended": "determined",
    "finalized": "settled",
}


def _ts_ms(rfc3339: str | None) -> int | None:
    if not rfc3339:
        return None
    return int(datetime.fromisoformat(rfc3339).timestamp() * 1000)


# --------------------------------------------------------------------------- REST

class WirePriceRange(msgspec.Struct):
    start: str
    end: str
    step: str


class WireMarket(msgspec.Struct):
    ticker: str
    event_ticker: str
    status: str
    title: str = ""
    yes_sub_title: str = ""
    close_time: str | None = None
    occurrence_datetime: str | None = None
    expected_expiration_time: str | None = None
    result: str = ""
    price_ranges: list[WirePriceRange] = []
    yes_bid_dollars: str | None = None
    yes_ask_dollars: str | None = None
    rules_primary: str = ""
    rules_secondary: str = ""
    settlement_value_dollars: str | None = None
    exchange_index: int = 0


class WireSettlementSource(msgspec.Struct):
    name: str = ""
    url: str = ""


class WireSeries(msgspec.Struct):
    ticker: str
    fee_type: str = ""
    fee_multiplier: float = 1.0
    settlement_sources: list[WireSettlementSource] | None = None


class WireSeriesResponse(msgspec.Struct):
    series: WireSeries


class WireMarketsPage(msgspec.Struct):
    markets: list[WireMarket]
    cursor: str = ""


class WireOrderbook(msgspec.Struct):
    yes_dollars: list[Level] = []
    no_dollars: list[Level] = []


class WireOrderbookResponse(msgspec.Struct):
    orderbook_fp: WireOrderbook


class WireTrade(msgspec.Struct):
    trade_id: str
    ticker: str
    yes_price_dollars: str
    count_fp: str
    taker_side: Literal["yes", "no"]
    taker_book_side: BookSide | None = None
    created_time: str | None = None


class WireTradesPage(msgspec.Struct):
    trades: list[WireTrade]
    cursor: str = ""


class WireFill(msgspec.Struct):
    fill_id: str
    order_id: str
    ticker: str
    count_fp: str
    yes_price_dollars: str
    is_taker: bool
    fee_cost: str
    book_side: BookSide | None = None
    side: str = ""
    action: str = ""
    created_time: str | None = None
    ts: int | None = None
    client_order_id: str = ""


class WireFillsPage(msgspec.Struct):
    fills: list[WireFill]
    cursor: str = ""


class WireMarketPosition(msgspec.Struct):
    ticker: str
    position_fp: str
    realized_pnl_dollars: str = "0"
    fees_paid_dollars: str = "0"


class WirePositionsPage(msgspec.Struct):
    market_positions: list[WireMarketPosition] = []
    cursor: str = ""


class WireShardBalance(msgspec.Struct):
    exchange_index: int
    balance: str


class WireBalance(msgspec.Struct):
    balance_dollars: str
    balance_breakdown: list[WireShardBalance] | None = None  # per exchange shard


class WireOrderCreated(msgspec.Struct):
    order_id: str
    fill_count: str
    remaining_count: str
    ts_ms: int
    client_order_id: str = ""


class WireOrderCanceled(msgspec.Struct):
    order_id: str
    reduced_by: str
    ts_ms: int


class WireOrder(msgspec.Struct):
    """`GET /portfolio/orders` items; the websocket's `user_order` messages carry the same fields."""

    order_id: str
    ticker: str
    status: Literal["resting", "canceled", "executed", "unknown"]
    yes_price_dollars: str
    fill_count_fp: str
    remaining_count_fp: str
    initial_count_fp: str
    client_order_id: str = ""
    book_side: BookSide | None = None
    side: str = ""
    action: str = ""
    last_update_time: str | None = None
    created_time: str | None = None
    last_updated_ts_ms: int | None = None
    last_update_reason: str = ""  # e.g. SelfTradeCancel, HaltCancel, CloseCancel (websocket only)


class WireQueuePosition(msgspec.Struct):
    order_id: str
    market_ticker: str
    queue_position_fp: str


class WireQueuePositions(msgspec.Struct):
    queue_positions: list[WireQueuePosition] | None = None  # null when nothing rests


class WireOrdersPage(msgspec.Struct):
    orders: list[WireOrder]
    cursor: str = ""


class WireOrderResponse(msgspec.Struct):
    order: WireOrder


def to_grid(ranges: list[WirePriceRange]) -> PriceGrid:
    if not ranges:
        return CENT_GRID
    return PriceGrid(tuple(PriceBand(parse_price(r.start), parse_price(r.end), parse_price(r.step)) for r in ranges))


def series_of(event_ticker: str) -> str:
    return event_ticker.split("-", 1)[0]


# Kalshi's fee formula is rate × C × P × (1 − P); `fee_type` says whether makers pay too (verified on
# /series 2026-10-18: "quadratic", "quadratic_with_maker_fees"; `fee_multiplier` scales both).
_FEE_RATES = {"quadratic": (700, 0), "quadratic_with_maker_fees": (700, 175)}


def to_fees(s: WireSeries) -> FeeSchedule | None:
    """The series' schedule; None when its `fee_type` is one we don't know (use the conservative default)."""
    rates = _FEE_RATES.get(s.fee_type)
    if rates is None:
        return None
    return FeeSchedule(round(rates[0] * s.fee_multiplier), round(rates[1] * s.fee_multiplier))


def to_contract(m: WireMarket, series: WireSeries | None = None) -> Contract:
    fees = to_fees(series) if series is not None else None
    sources = (series.settlement_sources or []) if series is not None else []
    return Contract(
        venue=VENUE,
        ticker=m.ticker,
        event_ticker=m.event_ticker,
        title=m.title,
        subtitle=m.yes_sub_title,
        status=_STATUS.get(m.status, "closed"),
        close_ts_ms=_ts_ms(m.close_time),
        grid=to_grid(m.price_ranges),
        result=m.result if m.result in ("yes", "no") else None,  # type: ignore[arg-type]
        series_ticker=series_of(m.event_ticker),
        event_ts_ms=_ts_ms(m.occurrence_datetime or m.expected_expiration_time),
        rules="\n\n".join(r for r in (m.rules_primary, m.rules_secondary) if r),
        settlement_source="; ".join(f"{s.name} ({s.url})" if s.url else s.name for s in sources if s.name),
        fees=fees or KALSHI_DEFAULT_FEES,
        settlement=parse_price(m.settlement_value_dollars) if m.settlement_value_dollars else None,
        shard=m.exchange_index,
    )


def to_snapshot(ticker: str, yes: list[Level], no: list[Level], ts_ms: int) -> BookSnapshot:
    """Kalshi publishes bids only. A NO bid at p is a YES ask at 1 - p."""
    bids = tuple((parse_price(p), parse_qty(q)) for p, q in yes)
    asks = tuple(sorted((complement(parse_price(p)), parse_qty(q)) for p, q in no))
    return BookSnapshot(VENUE, ticker, bids, asks, ts_ms)


def _side(book_side: BookSide | None, side: str, action: str) -> BookSide:
    if book_side:
        return book_side
    buys_yes = (side == "yes") == (action == "buy")
    return "bid" if buys_yes else "ask"


def to_fill(f: WireFill) -> Fill:
    ts = f.ts * 1000 if f.ts is not None else (_ts_ms(f.created_time) or 0)
    return Fill(
        venue=VENUE,
        fill_id=f.fill_id,
        order_id=f.order_id,
        ticker=f.ticker,
        side=_side(f.book_side, f.side, f.action),
        price=parse_price(f.yes_price_dollars),
        qty=parse_qty(f.count_fp),
        fee=parse_micros(f.fee_cost),
        is_taker=f.is_taker,
        ts_ms=ts,
        client_order_id=f.client_order_id,
    )


def to_trade(t: WireTrade) -> Trade:
    side: BookSide = t.taker_book_side or ("bid" if t.taker_side == "yes" else "ask")
    return Trade(VENUE, t.trade_id, t.ticker, parse_price(t.yes_price_dollars), parse_qty(t.count_fp), side,
                 _ts_ms(t.created_time) or 0)


def to_order(o: WireOrder, fallback_ms: int = 0) -> VenueOrder:
    ts = o.last_updated_ts_ms or _ts_ms(o.last_update_time) or _ts_ms(o.created_time) or fallback_ms
    return VenueOrder(VENUE, o.order_id, o.client_order_id, o.ticker,
                      _side(o.book_side, o.side or "yes", o.action or "buy"), parse_price(o.yes_price_dollars),
                      parse_qty(o.initial_count_fp), parse_qty(o.fill_count_fp), parse_qty(o.remaining_count_fp),
                      o.status, ts, o.last_update_reason)


def to_position(p: WireMarketPosition) -> VenuePosition:
    pnl, fees = parse_micros(p.realized_pnl_dollars), parse_micros(p.fees_paid_dollars)
    return VenuePosition(VENUE, p.ticker, parse_qty(p.position_fp), pnl, fees)


# --------------------------------------------------------------------------- WebSocket

class WsEnvelope(msgspec.Struct):
    type: str
    sid: int | None = None
    seq: int | None = None
    id: int | None = None
    msg: msgspec.Raw = msgspec.Raw(b"null")


class WsSnapshot(msgspec.Struct):
    market_ticker: str
    yes_dollars_fp: list[Level] = []
    no_dollars_fp: list[Level] = []


class WsDelta(msgspec.Struct):
    market_ticker: str
    price_dollars: str
    delta_fp: str
    side: Literal["yes", "no"]
    ts_ms: int | None = None


class WsTrade(msgspec.Struct):
    trade_id: str
    market_ticker: str
    yes_price_dollars: str
    count_fp: str
    taker_side: Literal["yes", "no"]
    taker_book_side: BookSide | None = None
    ts_ms: int | None = None
    ts: int | None = None


class WsFill(msgspec.Struct):
    trade_id: str
    order_id: str
    market_ticker: str
    is_taker: bool
    yes_price_dollars: str
    count_fp: str
    fee_cost: str
    book_side: BookSide | None = None
    side: str = ""
    action: str = ""
    client_order_id: str = ""
    ts_ms: int | None = None
    ts: int | None = None


class WsSubscribed(msgspec.Struct):
    channel: str
    sid: int


class WsError(msgspec.Struct):
    code: int
    msg: str = ""


def ws_delta(d: WsDelta, fallback_ms: int) -> BookDelta:
    p, q = parse_price(d.price_dollars), parse_qty(d.delta_fp)
    if d.side == "yes":
        return BookDelta(VENUE, d.market_ticker, "bid", p, q, d.ts_ms or fallback_ms)
    return BookDelta(VENUE, d.market_ticker, "ask", complement(p), q, d.ts_ms or fallback_ms)


def ws_trade(t: WsTrade, fallback_ms: int) -> Trade:
    side: BookSide = t.taker_book_side or ("bid" if t.taker_side == "yes" else "ask")
    ts = t.ts_ms or (t.ts * 1000 if t.ts else fallback_ms)
    return Trade(VENUE, t.trade_id, t.market_ticker, parse_price(t.yes_price_dollars), parse_qty(t.count_fp), side, ts)


def ws_fill(f: WsFill, fallback_ms: int) -> Fill:
    ts = f.ts_ms or (f.ts * 1000 if f.ts else fallback_ms)
    return Fill(
        venue=VENUE,
        fill_id=f.trade_id,
        order_id=f.order_id,
        ticker=f.market_ticker,
        side=_side(f.book_side, f.side, f.action),
        price=parse_price(f.yes_price_dollars),
        qty=parse_qty(f.count_fp),
        fee=parse_micros(f.fee_cost),
        is_taker=f.is_taker,
        ts_ms=ts,
        client_order_id=f.client_order_id,
    )
