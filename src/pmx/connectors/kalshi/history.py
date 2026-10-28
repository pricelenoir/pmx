"""Kalshi history: settled markets and 1-minute candles, from the archive or the live API.

Markets settled before the archive cutoff (`GET /historical/cutoff`) are served only by `/historical/*`; later ones
by the live endpoints. The two candle endpoints return the same candles in two schemas — the live one with
`*_dollars` / `*_fp` fields, the archive without the suffixes — normalized here to integer units. A request may
cover at most 5,000 candles (the API says so when asked for more: `max candlesticks: 5000`), so longer windows
are chunked."""

from __future__ import annotations

from datetime import datetime

import msgspec

from pmx.connectors.kalshi.rest import KalshiRest
from pmx.core.units import Price, Qty, parse_price, parse_qty

MAX_CANDLES = 5_000


class HistMarket(msgspec.Struct):
    ticker: str
    event_ticker: str
    status: str = ""
    result: str = ""
    close_time: str | None = None
    expected_expiration_time: str | None = None
    occurrence_datetime: str | None = None
    expiration_value: str = ""
    yes_sub_title: str = ""
    volume_fp: str | None = None
    settlement_value_dollars: str | None = None


class _MarketsPage(msgspec.Struct):
    markets: list[HistMarket]
    cursor: str = ""


class _Cutoff(msgspec.Struct):
    market_settled_ts: str


class _BidAsk(msgspec.Struct, frozen=True):
    open_dollars: str | None = None
    low_dollars: str | None = None
    high_dollars: str | None = None
    close_dollars: str | None = None


class _Price(msgspec.Struct, frozen=True):
    open_dollars: str | None = None
    low_dollars: str | None = None
    high_dollars: str | None = None
    close_dollars: str | None = None


class _LiveCandle(msgspec.Struct):
    end_period_ts: int
    yes_bid: _BidAsk = _BidAsk()
    yes_ask: _BidAsk = _BidAsk()
    price: _Price = _Price()
    volume_fp: str | None = None
    open_interest_fp: str | None = None


class _BidAskH(msgspec.Struct, frozen=True):
    open: str | None = None
    low: str | None = None
    high: str | None = None
    close: str | None = None


class _ArchiveCandle(msgspec.Struct):
    end_period_ts: int
    yes_bid: _BidAskH = _BidAskH()
    yes_ask: _BidAskH = _BidAskH()
    price: _BidAskH = _BidAskH()
    volume: str | None = None
    open_interest: str | None = None


class _LivePage(msgspec.Struct):
    candlesticks: list[_LiveCandle] = []


class _ArchivePage(msgspec.Struct):
    candlesticks: list[_ArchiveCandle] = []


class Candle(msgspec.Struct, frozen=True):
    """One minute of one market, YES terms, integer units; None where there was no quote or no trade."""

    end_ms: int
    bid_open: Price | None
    bid_low: Price | None
    bid_high: Price | None
    bid_close: Price | None
    ask_open: Price | None
    ask_low: Price | None
    ask_high: Price | None
    ask_close: Price | None
    price_open: Price | None
    price_high: Price | None
    price_low: Price | None
    price_close: Price | None
    volume: Qty
    oi: Qty | None


def _p(s: str | None) -> Price | None:
    return parse_price(s) if s not in (None, "") else None


def _q(s: str | None) -> Qty | None:
    return parse_qty(s) if s not in (None, "") else None


def _live(c: _LiveCandle) -> Candle:
    b, a, p = c.yes_bid, c.yes_ask, c.price
    return Candle(c.end_period_ts * 1000, _p(b.open_dollars), _p(b.low_dollars), _p(b.high_dollars),
                  _p(b.close_dollars), _p(a.open_dollars), _p(a.low_dollars), _p(a.high_dollars), _p(a.close_dollars),
                  _p(p.open_dollars), _p(p.high_dollars), _p(p.low_dollars), _p(p.close_dollars),
                  _q(c.volume_fp) or 0, _q(c.open_interest_fp))


def _archive(c: _ArchiveCandle) -> Candle:
    b, a, p = c.yes_bid, c.yes_ask, c.price
    return Candle(c.end_period_ts * 1000, _p(b.open), _p(b.low), _p(b.high), _p(b.close), _p(a.open), _p(a.low),
                  _p(a.high), _p(a.close), _p(p.open), _p(p.high), _p(p.low), _p(p.close), _q(c.volume) or 0,
                  _q(c.open_interest))


def ts_ms(rfc3339: str | None) -> int | None:
    if not rfc3339:
        return None
    return int(datetime.fromisoformat(rfc3339.replace("Z", "+00:00")).timestamp() * 1000)


async def cutoff_ms(rest: KalshiRest) -> int:
    """Markets settled before this are only in the archive."""
    c = msgspec.json.decode(await rest.get("/historical/cutoff"), type=_Cutoff)
    return ts_ms(c.market_settled_ts) or 0


async def _pages(rest: KalshiRest, path: str, params: dict) -> list[HistMarket]:
    out, cursor = [], None
    while True:
        page = msgspec.json.decode(await rest.get(path, params | ({"cursor": cursor} if cursor else {})),
                                   type=_MarketsPage)
        out += page.markets
        cursor = page.cursor
        if not cursor:
            return out


async def settled_markets(rest: KalshiRest, series: str) -> list[tuple[str, HistMarket]]:
    """(`historical` | `live`, market) for every settled market of `series`, the archive's first."""
    archived = await _pages(rest, "/historical/markets", {"series_ticker": series, "limit": 1000})
    live = await _pages(rest, "/markets", {"series_ticker": series, "status": "settled", "limit": 1000})
    seen = {m.ticker for m in archived}
    return [("historical", m) for m in archived] + [("live", m) for m in live if m.ticker not in seen]


async def candles(rest: KalshiRest, series: str, ticker: str, start_ms: int, end_ms: int, *, archived: bool,
                  period_min: int = 1) -> list[Candle]:
    """`period_min` candles ending in [start, end], chunked under the per-request limit."""
    out: list[Candle] = []
    step_s = MAX_CANDLES * period_min * 60
    start_s, end_s = start_ms // 1000, -(-end_ms // 1000)
    while start_s <= end_s:
        chunk_end = min(end_s, start_s + step_s - 60)
        params = {"start_ts": start_s, "end_ts": chunk_end, "period_interval": period_min}
        if archived:
            page = msgspec.json.decode(await rest.get(f"/historical/markets/{ticker}/candlesticks", params),
                                       type=_ArchivePage)
            out += [_archive(c) for c in page.candlesticks]
        else:
            page_l = msgspec.json.decode(await rest.get(f"/series/{series}/markets/{ticker}/candlesticks", params),
                                         type=_LivePage)
            out += [_live(c) for c in page_l.candlesticks]
        start_s = chunk_end + 60
    return out
