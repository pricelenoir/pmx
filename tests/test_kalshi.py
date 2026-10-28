from __future__ import annotations

import asyncio
import base64
import json

import httpx
import msgspec
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

from conftest import delta, snapshot
from pmx.connectors.base import Control, Frame
from pmx.connectors.kalshi import wire
from pmx.connectors.kalshi.auth import KalshiSigner
from pmx.connectors.kalshi.decode import KalshiDecoder
from pmx.connectors.kalshi.rest import KalshiError, KalshiRest
from pmx.connectors.kalshi.stream import KalshiStream
from pmx.core.types import BookDelta, BookSnapshot, Fill, OrderRequest, StreamReset, Trade

# --------------------------------------------------------------------------- decode


def test_decode_snapshot_converts_no_bids_to_yes_asks(frames):
    [ev] = KalshiDecoder()(Frame(1_000_000_000, frames["snapshot"]))
    assert isinstance(ev, BookSnapshot)
    assert ev.bids == ((800, 30_000), (2200, 33_300))
    assert ev.asks == ((4400, 14_600), (4600, 2_000))  # NO 0.56 -> YES ask 0.44


def test_decode_delta_trade_fill(frames):
    d = KalshiDecoder()
    d(Frame(0, frames["snapshot"]))
    [dl] = d(Frame(0, frames["delta"]))
    assert dl == BookDelta("kalshi", "FED-23DEC-T3.00", "bid", 9600, -5400, 1_700_000_000_000)
    [tr] = d(Frame(0, frames["trade"]))
    assert tr == Trade("kalshi", "d91bc706", "HIGHNY-22DEC23-B53.5", 3600, 13_600, "ask", 1669149841000)
    [fl] = d(Frame(0, frames["fill"]))
    assert isinstance(fl, Fill) and fl.side == "bid" and fl.fee == 10_000 and fl.qty == 27_800


def test_no_side_delta_maps_to_ask():
    d = KalshiDecoder()
    [ev] = d(Frame(0, delta("T", "0.7300", "5.00", "no", seq=1)))
    assert (ev.side, ev.price, ev.delta) == ("ask", 2700, 500)


def test_seq_gap_and_controls_emit_reset():
    d = KalshiDecoder()
    d(Frame(0, snapshot("T", [], [], seq=1)))
    assert d(Frame(0, delta("T", "0.5", "1", "yes", seq=2)))[0].delta == 100
    [reset] = d(Frame(0, delta("T", "0.5", "1", "yes", seq=4)))
    assert isinstance(reset, StreamReset) and "expected=3" in reset.reason
    assert isinstance(d(Control(0, "connected"))[0], StreamReset)
    assert d(Frame(0, "{not json")) == []
    assert d(Frame(0, json.dumps({"type": "subscribed", "id": 1, "msg": {"channel": "trade", "sid": 9}}))) == []


def test_market_to_contract():
    m = msgspec.json.decode(json.dumps({
        "ticker": "KXNFLGAME-26OCT19WASSF-WAS", "event_ticker": "KXNFLGAME-26OCT19WASSF", "status": "active",
        "title": "Washington at San Francisco", "yes_sub_title": "Washington", "close_time": "2026-10-22T00:15:00Z",
        "occurrence_datetime": "2026-10-20T03:15:00Z", "result": "",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}], "new_field": 1,
    }), type=wire.WireMarket)
    c = wire.to_contract(m)
    assert c.status == "open" and c.series_ticker == "KXNFLGAME" and c.result is None
    assert c.event_ts_ms == 1792628100000 - 45 * 3_600_000
    assert c.close_ts_ms == 1792628100000 and c.grid.is_valid(2300) and not c.grid.is_valid(2350)


# --------------------------------------------------------------------------- auth


def _verify_rsa(key, headers, method, path):
    msg = (headers["KALSHI-ACCESS-TIMESTAMP"] + method + path).encode()
    key.public_key().verify(
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]), msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256(),
    )


def test_signer_rsa_and_ed25519_strip_query():
    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    h = KalshiSigner("kid", rsa_key).headers("get", "/trade-api/v2/portfolio/fills?limit=5", now_ms=123)
    assert h["KALSHI-ACCESS-KEY"] == "kid" and h["KALSHI-ACCESS-TIMESTAMP"] == "123"
    _verify_rsa(rsa_key, h, "GET", "/trade-api/v2/portfolio/fills")

    ed = ed25519.Ed25519PrivateKey.generate()
    h = KalshiSigner("kid", ed).headers("GET", "/trade-api/ws/v2", now_ms=5)
    ed.public_key().verify(base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]), b"5GET/trade-api/ws/v2")


# --------------------------------------------------------------------------- REST


def _rest(handler, signer=None, **kw) -> KalshiRest:
    fast = (1e9, 1e9)
    return KalshiRest("https://x/trade-api/v2", signer, transport=httpx.MockTransport(handler), read=fast, write=fast,
                      **kw)


def test_markets_paginates():
    pages = {None: {"markets": [{"ticker": "A", "event_ticker": "E", "status": "active"}], "cursor": "c1"},
             "c1": {"markets": [{"ticker": "B", "event_ticker": "E", "status": "active"}], "cursor": ""}}
    seen = []

    def handler(req: httpx.Request):
        seen.append(dict(req.url.params))
        return httpx.Response(200, json=pages[req.url.params.get("cursor")])

    ms = asyncio.run(_rest(handler).markets(series_ticker="S", status="open"))
    assert [m.ticker for m in ms] == ["A", "B"]
    assert seen[0] == {"limit": "1000", "series_ticker": "S", "status": "open"}


def test_reads_retry_429_and_5xx_but_writes_only_429(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    calls = {"n": 0}

    def flaky(req):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503 if calls["n"] == 1 else 429, json={})
        return httpx.Response(200, json={"orderbook_fp": {"yes_dollars": [["0.5", "1"]], "no_dollars": []}})

    ob = asyncio.run(_rest(flaky).orderbook("T"))
    assert ob.yes_dollars == [("0.5", "1")] and calls["n"] == 3

    signer = KalshiSigner("k", ed25519.Ed25519PrivateKey.generate())
    posts = []

    def broken(req):
        posts.append(req)
        return httpx.Response(500, json={"code": "internal"})

    req = OrderRequest("kalshi", "T", "bid", 5600, 1000)
    with pytest.raises(KalshiError):
        asyncio.run(_rest(broken, signer).create_order(req))
    assert len(posts) == 1
    body = json.loads(posts[0].content)
    assert body["side"] == "bid" and body["price"] == "0.5600" and body["count"] == "10.00"
    assert body["time_in_force"] == "good_till_canceled" and body["client_order_id"] == req.client_order_id
    assert posts[0].url.path == "/trade-api/v2/portfolio/events/orders"


def test_portfolio_requires_key():
    with pytest.raises(KalshiError, match="no Kalshi API key"):
        asyncio.run(_rest(lambda r: httpx.Response(200)).balance())


async def _no_sleep(*_a, **_k):
    return None


# --------------------------------------------------------------------------- stream


class FakeWS:
    def __init__(self, frames: list[str]):
        self.frames = frames
        self.sent: list[dict] = []

    async def send(self, s: str) -> None:
        self.sent.append(json.loads(s))

    async def close(self) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for f in self.frames:
            await asyncio.sleep(0)
            yield f


def _subscribed(cid, channel, sid):
    return json.dumps({"id": cid, "type": "subscribed", "msg": {"channel": channel, "sid": sid}})


def test_stream_resubscribes_after_gap():
    s1 = FakeWS([_subscribed(1, "orderbook_delta", 1), snapshot("A", [], [], seq=1),
                 delta("A", "0.5", "1", "yes", seq=2), delta("A", "0.5", "1", "yes", seq=5),
                 delta("A", "0.5", "1", "yes", seq=6)])
    s2 = FakeWS([snapshot("A", [], [], seq=1)])
    sessions = iter([s1, s2])
    signer = KalshiSigner("k", ed25519.Ed25519PrivateKey.generate())
    stream = KalshiStream("wss://x", signer, ["A", "B"], trades=False,
                          connect=lambda url, **kw: next(sessions), backoff=(0, 0))

    async def collect():
        out = []
        async for item in stream:
            out.append(item)
            if len(out) == 8:
                await stream.close()
        return out

    items = asyncio.run(collect())
    kinds = [i.event if isinstance(i, Control) else json.loads(i.text)["type"] for i in items]
    assert kinds == ["connected", "subscribed", "orderbook_snapshot", "orderbook_delta", "orderbook_delta", "gap",
                     "connected", "orderbook_snapshot"]
    assert s1.sent[0] == {"id": 1, "cmd": "subscribe",
                          "params": {"channels": ["orderbook_delta"], "market_tickers": ["A", "B"]}}
    assert s2.sent[0]["params"]["market_tickers"] == ["A", "B"]


def test_stream_retries_a_rejected_handshake():
    from websockets.datastructures import Headers
    from websockets.exceptions import InvalidStatus
    from websockets.http11 import Response

    class Rejected:
        async def __aenter__(self):
            raise InvalidStatus(Response(401, "Unauthorized", Headers()))

        async def __aexit__(self, *a):
            return False

    sessions = iter([Rejected(), FakeWS([snapshot("A", [], [], seq=1)])])
    signer = KalshiSigner("k", ed25519.Ed25519PrivateKey.generate())
    stream = KalshiStream("wss://x", signer, ["A"], trades=False,
                          connect=lambda url, **kw: next(sessions), backoff=(0, 0))

    async def collect():
        out = []
        async for item in stream:
            out.append(item)
            if len(out) == 3:
                await stream.close()
        return out

    items = asyncio.run(collect())
    assert [i.event for i in items if isinstance(i, Control)] == ["disconnected", "connected"]
    assert "401" in items[0].detail["error"]


def test_stream_remove_uses_tracked_sids():
    ws = FakeWS([])
    stream = KalshiStream("wss://x", KalshiSigner("k", ed25519.Ed25519PrivateKey.generate()), [])
    stream._ws = ws
    stream.tickers = {"A", "B", "C"}
    stream._sid_tickers = {1: {"A", "B"}, 2: {"C"}}
    asyncio.run(stream.remove(["A", "C"]))
    assert ws.sent[0]["cmd"] == "update_subscription"
    assert ws.sent[0]["params"] == {"sids": [1], "market_tickers": ["A"], "action": "delete_markets"}
    assert ws.sent[1] == {"id": 2, "cmd": "unsubscribe", "params": {"sids": [2]}}
    assert stream.tickers == {"B"} and stream._sid_tickers == {1: {"B"}}
