"""Kalshi REST client (async). Client-side token buckets mirror the venue's limits so
we throttle ourselves instead of eating 429s. Order writes retry only on 429 — a 5xx
may have been processed, so reconciliation (not a blind retry) decides. Every 429 on a
write is reported to `on_429` hooks (the order scheduler backs off); reads have their
own bucket."""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections.abc import Callable
from typing import Any

import httpx
import msgspec

from pmx.connectors.kalshi import wire
from pmx.connectors.kalshi.auth import KalshiSigner
from pmx.connectors.kalshi.config import REST_ROOT
from pmx.core.types import OrderRequest
from pmx.core.units import fmt_price, fmt_qty

log = logging.getLogger(__name__)

DEFAULT_COST = 10
CANCEL_COST = 2  # an order cancel (spec 3.34.0); creates cost DEFAULT_COST
# Basic tier: read 200 tokens/s (3 s burst), write 100 tokens/s (1 s burst).
BASIC_READ = (200.0, 600.0)
BASIC_WRITE = (100.0, 100.0)

_TIF = {"gtc": "good_till_canceled", "ioc": "immediate_or_cancel", "fok": "fill_or_kill"}


class KalshiError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"kalshi {status}: {body[:300]}")
        self.status = status
        self.body = body


class TokenBucket:
    def __init__(self, rate: float, capacity: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.rate, self.capacity, self._clock = rate, capacity, clock
        self.tokens = capacity
        self._t = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self.tokens = min(self.capacity, self.tokens + (now - self._t) * self.rate)
        self._t = now

    async def take(self, cost: float = DEFAULT_COST) -> None:
        async with self._lock:
            self._refill()
            while self.tokens < cost:
                await asyncio.sleep((cost - self.tokens) / self.rate)
                self._refill()
            self.tokens -= cost


class KalshiRest:
    def __init__(
        self,
        base_url: str,
        signer: KalshiSigner | None = None,
        *,
        read: tuple[float, float] = BASIC_READ,
        write: tuple[float, float] = BASIC_WRITE,
        transport: httpx.AsyncBaseTransport | None = None,
        max_retries: int = 5,
    ) -> None:
        self.signer = signer
        self._http = httpx.AsyncClient(base_url=base_url, timeout=20.0, transport=transport)
        self._read = TokenBucket(*read)
        self._write = TokenBucket(*write)
        self._max_retries = max_retries
        self.on_429: list[Callable[[], None]] = []

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(
        self, method: str, path: str, *, params: dict | None = None, body: Any = None,
        auth: bool = False, write: bool = False, cost: float = DEFAULT_COST,
    ) -> bytes:
        if auth and self.signer is None:
            raise KalshiError(401, "no Kalshi API key configured for this environment")
        params = {k: v for k, v in (params or {}).items() if v is not None}
        content = msgspec.json.encode(body) if body is not None else None
        for attempt in range(self._max_retries + 1):
            await (self._write if write else self._read).take(cost)
            headers = {"Content-Type": "application/json"} if content else {}
            if self.signer is not None:
                headers |= self.signer.headers(method, REST_ROOT + path)
            r = await self._http.request(method, path, params=params, content=content, headers=headers)
            retryable = r.status_code == 429 or (not write and r.status_code >= 500)
            if r.status_code < 400:
                return r.content
            if r.status_code == 429 and write:
                for hook in self.on_429:
                    hook()
            if not retryable or attempt == self._max_retries:
                raise KalshiError(r.status_code, r.text)
            wait = min(0.25 * 2**attempt, 8.0)
            log.info("kalshi %s %s -> %s, retry in %.2fs", method, path, r.status_code, wait)
            await asyncio.sleep(wait)
        raise AssertionError("unreachable")

    async def _pages[T](
        self, path: str, page_type: type[T], items: str, params: dict, *, auth: bool, max_pages: int | None = None
    ) -> list:
        out: list = []
        cursor = None
        for _ in itertools.count() if max_pages is None else range(max_pages):
            raw = await self._request("GET", path, params=params | {"cursor": cursor}, auth=auth)
            page = msgspec.json.decode(raw, type=page_type)
            out.extend(getattr(page, items))
            cursor = page.cursor  # type: ignore[attr-defined]
            if not cursor:
                break
        return out

    async def get(self, path: str, params: dict | None = None) -> bytes:
        """A public GET's body, verbatim (raw capture); throttled and retried like every read."""
        return await self._request("GET", path, params=params)

    # ---- market data ----------------------------------------------------

    async def markets(
        self, *, series_ticker: str | None = None, event_ticker: str | None = None,
        status: str | None = None, tickers: list[str] | None = None, mve_filter: str | None = None,
        max_pages: int | None = None,
    ) -> list[wire.WireMarket]:
        params = {
            "limit": 1000, "series_ticker": series_ticker, "event_ticker": event_ticker, "status": status,
            "tickers": ",".join(tickers) if tickers else None, "mve_filter": mve_filter,
        }
        return await self._pages("/markets", wire.WireMarketsPage, "markets", params, auth=False, max_pages=max_pages)

    async def series(self, ticker: str) -> wire.WireSeries:
        raw = await self._request("GET", f"/series/{ticker}")
        return msgspec.json.decode(raw, type=wire.WireSeriesResponse).series

    async def orderbook(self, ticker: str, depth: int = 0) -> wire.WireOrderbook:
        raw = await self._request("GET", f"/markets/{ticker}/orderbook", params={"depth": depth})
        return msgspec.json.decode(raw, type=wire.WireOrderbookResponse).orderbook_fp

    async def trades(
        self, *, min_ts: int | None = None, ticker: str | None = None, max_pages: int | None = None
    ) -> list[wire.WireTrade]:
        """Public prints, newest first. `min_ts` is unix seconds."""
        params = {"limit": 1000, "min_ts": min_ts, "ticker": ticker}
        return await self._pages("/markets/trades", wire.WireTradesPage, "trades", params, auth=False,
                                 max_pages=max_pages)

    # ---- portfolio ------------------------------------------------------

    async def balance(self) -> wire.WireBalance:
        return msgspec.json.decode(await self._request("GET", "/portfolio/balance", auth=True), type=wire.WireBalance)

    async def positions(self) -> list[wire.WireMarketPosition]:
        params = {"limit": 1000, "count_filter": "position"}
        return await self._pages("/portfolio/positions", wire.WirePositionsPage, "market_positions", params, auth=True)

    async def fills(self, *, min_ts: int | None = None, ticker: str | None = None) -> list[wire.WireFill]:
        params = {"limit": 1000, "min_ts": min_ts, "ticker": ticker}
        return await self._pages("/portfolio/fills", wire.WireFillsPage, "fills", params, auth=True)

    async def orders(self, *, status: str | None = None, ticker: str | None = None,
                     min_ts: int | None = None) -> list[wire.WireOrder]:
        """Our orders (`status`: resting | canceled | executed), newest first; `min_ts` is unix seconds."""
        params = {"limit": 1000, "status": status, "ticker": ticker, "min_ts": min_ts}
        return await self._pages("/portfolio/orders", wire.WireOrdersPage, "orders", params, auth=True)

    async def queue_positions(self, tickers: list[str]) -> list[wire.WireQueuePosition]:
        """Contracts ahead of each of our resting orders on these markets."""
        raw = await self._request("GET", "/portfolio/orders/queue_positions",
                                  params={"market_tickers": ",".join(tickers)}, auth=True)
        return msgspec.json.decode(raw, type=wire.WireQueuePositions).queue_positions or []

    async def order(self, order_id: str) -> wire.WireOrder:
        raw = await self._request("GET", f"/portfolio/orders/{order_id}", auth=True, cost=2)
        return msgspec.json.decode(raw, type=wire.WireOrderResponse).order

    async def create_order(self, req: OrderRequest) -> wire.WireOrderCreated:
        body = {
            "ticker": req.ticker,
            "client_order_id": req.client_order_id,
            "side": req.side,
            "count": fmt_qty(req.qty),
            "price": fmt_price(req.price),
            "time_in_force": _TIF[req.tif],
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": req.post_only,
            "cancel_order_on_pause": True,  # the venue pulls it if trading pauses, even with pmx down
        }
        raw = await self._request("POST", "/portfolio/events/orders", body=body, auth=True, write=True)
        return msgspec.json.decode(raw, type=wire.WireOrderCreated)

    async def cancel_order(self, order_id: str, ticker: str) -> wire.WireOrderCanceled:
        raw = await self._request(
            "DELETE", f"/portfolio/events/orders/{order_id}", params={"market_ticker": ticker}, auth=True, write=True,
            cost=CANCEL_COST,
        )
        return msgspec.json.decode(raw, type=wire.WireOrderCanceled)
