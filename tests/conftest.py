from __future__ import annotations

import json

import pytest


def frame(type_: str, msg: dict, *, sid: int | None = 1, seq: int | None = None, **extra) -> str:
    d: dict = {"type": type_, "msg": msg, **extra}
    if sid is not None:
        d["sid"] = sid
    if seq is not None:
        d["seq"] = seq
    return json.dumps(d)


def snapshot(ticker: str, yes: list, no: list, *, sid: int = 1, seq: int = 1) -> str:
    return frame("orderbook_snapshot", {"market_ticker": ticker, "market_id": "x", "yes_dollars_fp": yes,
                                        "no_dollars_fp": no}, sid=sid, seq=seq)


def delta(ticker: str, price: str, d: str, side: str, *, sid: int = 1, seq: int = 2) -> str:
    return frame("orderbook_delta", {"market_ticker": ticker, "market_id": "x", "price_dollars": price,
                                     "delta_fp": d, "side": side, "ts_ms": 1_700_000_000_000}, sid=sid, seq=seq)


@pytest.fixture
def frames():
    """Kalshi doc examples (orderbook / trade / fill channels)."""
    return {
        "snapshot": snapshot("FED-23DEC-T3.00", [["0.0800", "300.00"], ["0.2200", "333.00"]],
                             [["0.5400", "20.00"], ["0.5600", "146.00"]], sid=2, seq=2),
        "delta": delta("FED-23DEC-T3.00", "0.9600", "-54.00", "yes", sid=2, seq=3),
        "trade": frame("trade", {
            "trade_id": "d91bc706", "market_ticker": "HIGHNY-22DEC23-B53.5", "yes_price_dollars": "0.3600",
            "no_price_dollars": "0.6400", "count_fp": "136.00", "taker_side": "no", "taker_outcome_side": "no",
            "taker_book_side": "ask", "is_block_trade": False, "ts": 1669149841, "ts_ms": 1669149841000,
        }, sid=11, seq=2),
        "fill": frame("fill", {
            "trade_id": "d91bc706", "order_id": "ee587a1c", "client_order_id": "my-order-1",
            "market_ticker": "HIGHNY-22DEC23-B53.5", "exchange_index": 2, "is_taker": True, "side": "yes",
            "yes_price_dollars": "0.7500", "count_fp": "278.00", "fee_cost": "0.010000", "action": "buy",
            "ts": 1671899397, "ts_ms": 1671899397000, "post_position_fp": "500.00", "purchased_side": "yes",
            "outcome_side": "yes", "book_side": "bid",
        }, sid=13),
    }
