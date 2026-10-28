"""Stateful frame decoder. Same object runs live and in replay, so sequence-gap
handling is identical in both."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable

import msgspec

from pmx.connectors.base import Control, Frame, StreamItem
from pmx.connectors.kalshi import wire
from pmx.core.types import MarketEvent, StreamReset

log = logging.getLogger(__name__)

_env = msgspec.json.Decoder(wire.WsEnvelope)
_snap = msgspec.json.Decoder(wire.WsSnapshot)
_delta = msgspec.json.Decoder(wire.WsDelta)
_trade = msgspec.json.Decoder(wire.WsTrade)
_fill = msgspec.json.Decoder(wire.WsFill)
_order = msgspec.json.Decoder(wire.WireOrder)
_err = msgspec.json.Decoder(wire.WsError)


class SeqTracker:
    """`seq` is per subscription (sid) and increments by one, including on `ok` acks."""

    def __init__(self) -> None:
        self._last: dict[int, int] = {}

    def reset(self) -> None:
        self._last.clear()

    def check(self, sid: int | None, seq: int | None) -> tuple[int, int] | None:
        """Returns (expected, got) on a gap."""
        if sid is None or seq is None:
            return None
        prev = self._last.get(sid)
        self._last[sid] = seq
        if prev is not None and seq != prev + 1:
            return prev + 1, seq
        return None


class KalshiDecoder:
    def __init__(self) -> None:
        self.seq = SeqTracker()

    def __call__(self, item: StreamItem) -> list[MarketEvent]:
        if isinstance(item, Control):
            if item.event in ("connected", "disconnected", "gap", "sleep_gap"):
                self.seq.reset()
                return [StreamReset(wire.VENUE, item.event, item.recv_ns // 1_000_000)]
            return []
        return self.decode_frame(item)

    def decode_frame(self, f: Frame) -> list[MarketEvent]:
        try:
            return self._decode(f)
        except (msgspec.DecodeError, ValueError) as e:
            log.warning("bad kalshi frame (%s): %.200s", e, f.text)
            return []

    def _decode(self, f: Frame) -> list[MarketEvent]:
        recv_ms = f.recv_ns // 1_000_000
        env = _env.decode(f.text)
        gap = self.seq.check(env.sid, env.seq)
        if gap is not None:
            self.seq.reset()
            return [StreamReset(wire.VENUE, f"seq gap sid={env.sid} expected={gap[0]} got={gap[1]}", recv_ms)]
        match env.type:
            case "orderbook_snapshot":
                s = _snap.decode(env.msg)
                return [wire.to_snapshot(s.market_ticker, s.yes_dollars_fp, s.no_dollars_fp, recv_ms)]
            case "orderbook_delta":
                return [wire.ws_delta(_delta.decode(env.msg), recv_ms)]
            case "trade":
                return [wire.ws_trade(_trade.decode(env.msg), recv_ms)]
            case "fill":
                return [wire.ws_fill(_fill.decode(env.msg), recv_ms)]
            case "user_order":
                return [wire.to_order(_order.decode(env.msg), recv_ms)]
            case "error":
                e = _err.decode(env.msg)
                log.warning("kalshi ws error %s: %s (cmd id=%s)", e.code, e.msg, env.id)
        return []


class _Envelope(msgspec.Struct):
    url: str
    status: int
    ms: int
    body: msgspec.Raw = msgspec.Raw(b"null")


_envelope = msgspec.json.Decoder(_Envelope)
_book = msgspec.json.Decoder(wire.WireOrderbookResponse)
_tape = msgspec.json.Decoder(wire.WireTradesPage)


class KalshiRestDecoder:
    """REST poll responses (raw source `kalshi_rest`, recorded `{url, status, ms, body}` envelopes) → the events the
    poller emits: a book snapshot per order-book response, and each public trade once — tape pages overlap, so
    trades are deduplicated by id — for markets in `universe`. The live poller (`sources/kalshi.py`) and replays
    run this same code."""

    def __init__(self, universe: Callable[[str], bool] | None = None, *, keep: int = 20_000) -> None:
        self.tickers: set[str] = set()  # markets seen in contract metadata (the replay's universe)
        self.universe = universe or self.tickers.__contains__
        self._ids: deque[str] = deque(maxlen=keep)
        self._seen: set[str] = set()

    def __call__(self, item: StreamItem) -> list[MarketEvent]:
        if not isinstance(item, Frame):
            return []
        try:
            env = _envelope.decode(item.text)
        except msgspec.DecodeError as e:
            log.warning("bad kalshi poll envelope (%s): %.200s", e, item.text)
            return []
        return self.on_response(env.url, env.status, env.ms, bytes(env.body))

    def on_response(self, url: str, status: int, ms: int, body: bytes) -> list[MarketEvent]:
        if status != 200:
            return []
        path = url.split("?", 1)[0]
        try:
            if path.startswith("/markets/") and path.endswith("/orderbook"):
                ticker = path[len("/markets/"):-len("/orderbook")]
                ob = _book.decode(body).orderbook_fp
                return [wire.to_snapshot(ticker, ob.yes_dollars, ob.no_dollars, ms)]
            if path == "/markets/trades":
                return self._trades(body)
        except (msgspec.DecodeError, ValueError) as e:
            log.warning("bad kalshi poll body for %s (%s)", path, e)
        return []

    def _trades(self, body: bytes) -> list[MarketEvent]:
        out: list[MarketEvent] = []
        for t in reversed(_tape.decode(body).trades):  # oldest first
            trade = wire.to_trade(t)
            if trade.trade_id in self._seen:
                continue
            if len(self._ids) == self._ids.maxlen:
                self._seen.discard(self._ids[0])
            self._ids.append(trade.trade_id)
            self._seen.add(trade.trade_id)
            if self.universe(trade.ticker):
                out.append(trade)
        return out
