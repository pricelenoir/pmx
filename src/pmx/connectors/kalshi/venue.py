from __future__ import annotations

import logging
import time
from collections.abc import Iterable

import msgspec

from pmx.connectors.kalshi import wire
from pmx.connectors.kalshi.config import KalshiConfig
from pmx.connectors.kalshi.decode import KalshiDecoder
from pmx.connectors.kalshi.rest import KalshiError, KalshiRest
from pmx.connectors.kalshi.stream import KalshiStream
from pmx.core.types import BookSnapshot, Contract, Fill, OrderAck, OrderRequest, Trade, VenueOrder, VenuePosition
from pmx.core.units import Micros, Qty, parse_micros, parse_qty

log = logging.getLogger(__name__)


class KalshiVenue:
    name = wire.VENUE

    def __init__(self, cfg: KalshiConfig, rest: KalshiRest | None = None) -> None:
        self.cfg = cfg
        self.signer = cfg.signer()
        self.rest = rest or KalshiRest(cfg.rest_base, self.signer)
        self._series: dict[str, wire.WireSeries | None] = {}

    async def aclose(self) -> None:
        await self.rest.aclose()

    async def list_contracts(
        self, *, series: str | None = None, event: str | None = None, status: str | None = "open"
    ) -> list[Contract]:
        markets = await self.rest.markets(series_ticker=series, event_ticker=event, status=status)
        return await self._contracts(markets)

    async def contracts_by_ticker(self, tickers: list[str]) -> list[Contract]:
        markets: list[wire.WireMarket] = []
        for i in range(0, len(tickers), 100):
            markets += await self.rest.markets(tickers=tickers[i : i + 100])
        return await self._contracts(markets)

    async def series(self, ticker: str) -> wire.WireSeries | None:
        """Fee schedule and settlement sources, fetched once per series; None if Kalshi won't say."""
        if ticker not in self._series:
            try:
                self._series[ticker] = await self.rest.series(ticker)
            except (KalshiError, msgspec.DecodeError) as e:
                log.warning("series %s: %s; using the default fee schedule", ticker, e)
                self._series[ticker] = None
            s = self._series[ticker]
            if s is not None and wire.to_fees(s) is None:
                log.warning("series %s: unknown fee_type %r; using the default fee schedule", ticker, s.fee_type)
        return self._series[ticker]

    async def _contracts(self, markets: list[wire.WireMarket]) -> list[Contract]:
        out = []
        for m in markets:
            out.append(wire.to_contract(m, await self.series(wire.series_of(m.event_ticker))))
        return out

    async def fetch_book(self, ticker: str) -> BookSnapshot:
        ob = await self.rest.orderbook(ticker)
        return wire.to_snapshot(ticker, ob.yes_dollars, ob.no_dollars, int(time.time() * 1000))

    async def trades(self, since_ms: int, max_pages: int | None = 5) -> list[Trade]:
        """Public prints across all markets since `since_ms`, oldest first."""
        raw = await self.rest.trades(min_ts=since_ms // 1000, max_pages=max_pages)
        return [wire.to_trade(t) for t in reversed(raw)]

    async def place(self, req: OrderRequest) -> OrderAck:
        r = await self.rest.create_order(req)
        return OrderAck(
            self.name, r.order_id, r.client_order_id or req.client_order_id,
            parse_qty(r.fill_count), parse_qty(r.remaining_count), r.ts_ms,
        )

    async def cancel(self, order_id: str, ticker: str) -> Qty:
        return parse_qty((await self.rest.cancel_order(order_id, ticker)).reduced_by)

    async def orders(self, status: str | None = None, since_ms: int | None = None) -> list[VenueOrder]:
        min_ts = since_ms // 1000 if since_ms is not None else None
        return [wire.to_order(o) for o in await self.rest.orders(status=status, min_ts=min_ts)]

    async def queue_positions(self, tickers: list[str]) -> dict[str, Qty]:
        """order id -> contracts ahead of it, for our resting orders on `tickers`."""
        out: dict[str, Qty] = {}
        for i in range(0, len(tickers), 50):
            for q in await self.rest.queue_positions(tickers[i : i + 50]):
                out[q.order_id] = parse_qty(q.queue_position_fp)
        return out

    async def order(self, order_id: str) -> VenueOrder:
        return wire.to_order(await self.rest.order(order_id))

    async def positions(self) -> list[VenuePosition]:
        return [wire.to_position(p) for p in await self.rest.positions()]

    async def fills(self, since_ms: int | None = None) -> list[Fill]:
        min_ts = since_ms // 1000 if since_ms is not None else None
        return [wire.to_fill(f) for f in await self.rest.fills(min_ts=min_ts)]

    async def balance(self) -> Micros:
        return parse_micros((await self.rest.balance()).balance_dollars)

    async def balances(self) -> tuple[Micros, dict[int, Micros]]:
        """The available balance in all, and per exchange shard (a market trades only its shard's money)."""
        b = await self.rest.balance()
        return parse_micros(b.balance_dollars), {s.exchange_index: parse_micros(s.balance)
                                                 for s in b.balance_breakdown or []}

    def stream(self, tickers: Iterable[str], *, trades: bool = True, fills: bool = False,
               orders: bool = False) -> KalshiStream:
        if self.signer is None:
            raise KalshiError(401, f"Kalshi websocket needs an API key (KALSHI_{self.cfg.env.upper()}_API_KEY_ID)")
        return KalshiStream(self.cfg.ws_url, self.signer, tickers, trades=trades, fills=fills, orders=orders)

    def decoder(self) -> KalshiDecoder:
        return KalshiDecoder()
