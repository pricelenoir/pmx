"""Kalshi WebSocket stream: yields raw frames plus `Control` records for connects,
disconnects and sequence gaps. A gap closes the socket; reconnecting re-snapshots
every book, which is simpler and safer than patching one subscription."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable
from typing import Any

import msgspec
import websockets
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from pmx.connectors.base import Control, Frame, StreamItem
from pmx.connectors.kalshi import wire
from pmx.connectors.kalshi.auth import KalshiSigner
from pmx.connectors.kalshi.config import WS_PATH
from pmx.connectors.kalshi.decode import SeqTracker

log = logging.getLogger(__name__)

BOOK = "orderbook_delta"
TRADE = "trade"
FILL = "fill"
ORDERS = "user_orders"
CHUNK = 200

_env = msgspec.json.Decoder(wire.WsEnvelope)
_subscribed = msgspec.json.Decoder(wire.WsSubscribed)


class KalshiStream:
    def __init__(
        self,
        url: str,
        signer: KalshiSigner,
        tickers: Iterable[str],
        *,
        trades: bool = True,
        fills: bool = False,
        orders: bool = False,
        connect: Callable[..., Any] = websockets.connect,
        clock: Callable[[], int] = time.time_ns,
        backoff: tuple[float, float] = (1.0, 30.0),
    ) -> None:
        self.url = url
        self.signer = signer
        self.tickers: set[str] = set(tickers)
        self.market_channels = [BOOK, TRADE] if trades else [BOOK]
        self.fills = fills
        self.orders = orders
        self._connect = connect
        self._clock = clock
        self._backoff = backoff
        self._ws: Any = None
        self._ids = itertools.count(1)
        self._pending: dict[int, set[str]] = {}  # cmd id -> tickers it subscribed
        self._sid_tickers: dict[int, set[str]] = {}
        self._closed = False

    # ---- control --------------------------------------------------------

    async def _send(self, cmd: str, params: dict) -> int:
        cid = next(self._ids)
        await self._ws.send(json.dumps({"id": cid, "cmd": cmd, "params": params}))
        return cid

    async def _subscribe(self, tickers: list[str]) -> None:
        for i in range(0, len(tickers), CHUNK):
            chunk = tickers[i : i + CHUNK]
            cid = await self._send("subscribe", {"channels": self.market_channels, "market_tickers": chunk})
            self._pending[cid] = set(chunk)

    async def add(self, tickers: Iterable[str]) -> None:
        new = sorted(set(tickers) - self.tickers)
        self.tickers.update(new)
        if new and self._ws is not None:
            await self._subscribe(new)

    async def remove(self, tickers: Iterable[str]) -> None:
        gone = set(tickers) & self.tickers
        self.tickers -= gone
        if not gone or self._ws is None:
            return
        for sid, held in list(self._sid_tickers.items()):
            drop = held & gone
            if not drop:
                continue
            if drop == held:
                await self._send("unsubscribe", {"sids": [sid]})
                del self._sid_tickers[sid]
            else:
                params = {"sids": [sid], "market_tickers": sorted(drop), "action": "delete_markets"}
                await self._send("update_subscription", params)
                held -= drop

    async def reconnect(self) -> None:
        """Drop the connection; the stream reconnects and resubscribes, so every book re-snapshots."""
        if self._ws is not None:
            await self._ws.close()

    async def close(self) -> None:
        self._closed = True
        if self._ws is not None:
            await self._ws.close()

    # ---- iteration ------------------------------------------------------

    def _ctl(self, event: str, **detail: Any) -> Control:
        return Control(self._clock(), event, detail)

    async def __aiter__(self) -> AsyncIterator[StreamItem]:
        delay = self._backoff[0]
        while not self._closed:
            seq = SeqTracker()
            healthy = False
            try:
                headers = self.signer.headers("GET", WS_PATH)
                async with self._connect(self.url, additional_headers=headers, max_size=2**24) as ws:
                    self._ws, self._pending, self._sid_tickers = ws, {}, {}
                    yield self._ctl("connected", tickers=len(self.tickers))
                    if self.tickers:
                        await self._subscribe(sorted(self.tickers))
                    account = [FILL] * self.fills + [ORDERS] * self.orders
                    if account:
                        await self._send("subscribe", {"channels": account})
                    async for text in ws:
                        recv = self._clock()
                        if isinstance(text, bytes):
                            text = text.decode()
                        healthy = True
                        try:
                            env = _env.decode(text)
                        except msgspec.DecodeError:
                            log.warning("undecodable kalshi frame: %.200s", text)
                            yield Frame(recv, text)
                            continue
                        if env.type == "subscribed" and env.id in self._pending:
                            sub = _subscribed.decode(env.msg)
                            if sub.channel in self.market_channels:
                                self._sid_tickers[sub.sid] = set(self._pending[env.id])
                        yield Frame(recv, text)
                        gap = seq.check(env.sid, env.seq)
                        if gap is not None:
                            yield self._ctl("gap", sid=env.sid, expected=gap[0], got=gap[1])
                            break
                    else:
                        if not self._closed:
                            yield self._ctl("disconnected", error="closed by server")
            except (TimeoutError, ConnectionClosed, OSError, InvalidHandshake) as e:
                # InvalidHandshake: a rejected upgrade (a 401 right after a wake, a 429/503) — retried like a drop
                log.warning("kalshi ws dropped: %s", e)
                yield self._ctl("disconnected", error=str(e))
            finally:
                self._ws = None
            if self._closed:
                return
            delay = self._backoff[0] if healthy else min(delay * 2, self._backoff[1])
            await asyncio.sleep(delay)
