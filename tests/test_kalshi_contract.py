"""Kalshi's live API still matches what pmx decodes: read-only calls to public prod endpoints through pmx's own clients
(no key, no orders). Marked `network`, so the offline suite skips them; CI runs them weekly (`pytest -m network`).
A failure here means Kalshi changed something pmx parses — fix the wire structs before it reaches a live engine."""

from __future__ import annotations

import asyncio
import time

import msgspec
import pytest

from pmx.connectors.kalshi import history, wire
from pmx.connectors.kalshi.config import KalshiConfig
from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.research import kalshi_data

pytestmark = pytest.mark.network

SERIES = "KXHIGHNY"  # a daily series: always open markets and years of settled ones


def _run(coro_fn):
    async def go():
        v = KalshiVenue(KalshiConfig.from_env("prod"))
        try:
            return await coro_fn(v)
        finally:
            await v.aclose()

    return asyncio.run(go())


def test_open_markets_series_and_order_books_decode():
    async def go(v: KalshiVenue):
        series = await v.rest.series(SERIES)  # strict: no fallback to default fees here
        contracts = await v.list_contracts(series=SERIES)
        book = await v.fetch_book(contracts[0].ticker)
        return series, contracts, book

    series, contracts, book = _run(go)
    assert wire.to_fees(series) is not None, f"unknown fee_type {series.fee_type!r}"
    assert contracts and all(c.ticker.startswith(SERIES) and c.status for c in contracts)
    assert book.ticker == contracts[0].ticker


def test_public_trades_decode():
    trades = _run(lambda v: v.trades(int(time.time() * 1000) - 6 * 3_600_000, max_pages=1))
    assert all(t.qty > 0 and 0 < t.price < 10_000 for t in trades)


def test_history_markets_candles_and_trades_decode():
    async def go(v: KalshiVenue):
        cutoff = await history.cutoff_ms(v.rest)
        page = msgspec.json.decode(await v.rest.get("/historical/markets", {"series_ticker": SERIES, "limit": 5}),
                                   type=history._MarketsPage)
        m = page.markets[0]
        close = history.ts_ms(m.close_time)
        candles = await history.candles(v.rest, SERIES, m.ticker, close - 2 * 86_400_000, close, archived=True,
                                        period_min=60)
        trades = await history.trades(v.rest, m.ticker, archived=True)
        return cutoff, m, candles, trades

    cutoff, m, candles, trades = _run(go)
    assert cutoff > 0 and m.result in ("yes", "no", "") and m.strike_type
    assert candles and all(c.end_ms > 0 for c in candles)
    assert all(t.ts_ms > 0 and t.qty > 0 for t in trades)


def test_series_listing_by_category_decodes():
    found = _run(lambda v: kalshi_data.series_in_category(v.rest, "Climate and Weather"))
    assert SERIES in found

