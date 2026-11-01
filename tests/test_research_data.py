"""`pmx data kalshi`: any series' settled markets (both listings), fees, per-event candles and trades, the cache."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import polars as pl

from pmx.connectors.kalshi.rest import KalshiRest
from pmx.research import kalshi_data as kd


def _m(ticker: str, result: str, close: str, **kw) -> dict:
    return {"ticker": ticker, "event_ticker": ticker.rsplit("-", 1)[0], "status": "finalized", "result": result,
            "close_time": close, "open_time": "2025-01-14T15:00:00Z", "volume_fp": "40.00",
            "settlement_value_dollars": "1.0000" if result == "yes" else "0.0000", "strike_type": "between",
            "floor_strike": 33, "cap_strike": 34, "title": "High temp in NYC", **kw}


ARCHIVED = [_m("KXT-25JAN15-B33.5", "yes", "2025-01-16T04:59:00Z"),
            _m("KXT-25JAN15-B35.5", "no", "2025-01-16T04:59:00Z")]
LIVE = [_m("KXT-26OCT01-B60.5", "no", "2026-10-02T03:59:00Z", open_time=None)]


class Api:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path.removeprefix("/trade-api/v2")
        p = dict(req.url.params)
        self.calls.append(path)
        if path == "/series/KXT":
            return httpx.Response(200, json={"series": {"ticker": "KXT", "title": "NYC high", "category": "Climate",
                                                        "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 2}})
        if path == "/series":
            return httpx.Response(200, json={"series": [{"ticker": "KXT"}, {"ticker": "KXU"}], "cursor": ""})
        if path == "/historical/markets":
            return httpx.Response(200, json={"markets": ARCHIVED, "cursor": ""})
        if path == "/markets":
            return httpx.Response(200, json={"markets": LIVE, "cursor": ""})
        if path.endswith("/candlesticks"):
            archived = path.startswith("/historical/")
            start, end = int(p["start_ts"]), int(p["end_ts"])
            ends = range(start - start % 3600 + 3600, end + 1, 3600)
            key = (lambda k: k) if archived else (lambda k: k + "_dollars")
            return httpx.Response(200, json={"candlesticks": [
                {"end_period_ts": t, "yes_bid": {key("close"): "0.4000"}, "yes_ask": {key("close"): "0.4200"},
                 "price": {key("close"): "0.4100"}, ("volume" if archived else "volume_fp"): "3.00"} for t in ends]})
        if path in ("/historical/trades", "/markets/trades"):
            assert path == "/historical/trades" or p["ticker"] == "KXT-26OCT01-B60.5"
            first = "cursor" not in p
            return httpx.Response(200, json={"cursor": "c2" if first else "", "trades": [
                {"trade_id": f"{p['ticker']}-{'a' if first else 'b'}", "ticker": p["ticker"], "count_fp": "2.00",
                 "yes_price_dollars": "0.4100" if first else "0.3900", "taker_side": "yes" if first else "no",
                 "created_time": "2025-01-15T12:00:00Z" if first else "2025-01-15T11:00:00Z"}]})
        raise AssertionError(f"unexpected {path}")


def _rest(api: Api) -> KalshiRest:
    return KalshiRest("https://x/trade-api/v2", transport=httpx.MockTransport(api), read=(1e9, 1e9))


def test_sync_series_writes_markets_fees_candles_trades_and_caches(tmp_path: Path):
    api = Api()

    async def go():
        rest = _rest(api)
        first = await kd.sync_series(tmp_path, rest, "KXT", trades=True)
        again = await kd.sync_series(tmp_path, rest, "KXT", trades=True)
        cats = await kd.series_in_category(rest, "Climate")
        return first, again, cats

    first, again, cats = asyncio.run(go())
    assert cats == ["KXT", "KXU"]
    assert (first.markets, first.events, first.fetched, first.cached, first.capped) == (3, 2, 4, 0, False)
    assert (again.fetched, again.cached) == (0, 4)

    d = kd.series_dir(tmp_path, "KXT")
    meta = json.loads((d / "series.json").read_text())
    assert (meta["taker_e4"], meta["maker_e4"]) == (1400, 350)  # multiplier 2 on both
    m = pl.read_parquet(d / "markets.parquet")
    assert m["ticker"].to_list() == ["KXT-25JAN15-B33.5", "KXT-25JAN15-B35.5", "KXT-26OCT01-B60.5"]
    assert m["settlement"].to_list() == [10_000, 0, 0] and m["source"].to_list() == ["historical"] * 2 + ["live"]
    assert m["floor_strike"][0] == 33.0 and m["strike_type"][0] == "between" and m["volume"][0] == 4_000

    c = pl.read_parquet(d / "candles_1h" / "KXT-25JAN15.parquet")
    assert set(c["ticker"]) == {"KXT-25JAN15-B33.5", "KXT-25JAN15-B35.5"}
    assert c["bid_close"].unique().to_list() == [4_000] and c["volume"].unique().to_list() == [300]
    live = pl.read_parquet(d / "candles_1h" / "KXT-26OCT01.parquet")  # no open time: a week before close
    assert live.height == 7 * 24 and live["ask_close"].unique().to_list() == [4_200]

    t = pl.read_parquet(d / "trades" / "KXT-25JAN15.parquet").filter(pl.col("ticker") == "KXT-25JAN15-B33.5")
    assert t["taker_yes"].to_list() == [False, True] and t["price"].to_list() == [3_900, 4_100]  # oldest first


def test_since_lists_everything_but_fetches_only_recent_events(tmp_path: Path):
    api = Api()
    r = asyncio.run(kd.sync_series(tmp_path, _rest(api), "KXT", since_ms=1_767_225_600_000))  # 2026-01-01
    assert (r.markets, r.events, r.fetched) == (3, 1, 1)
    assert not any("/historical/markets/" in c for c in api.calls)


def test_a_full_archive_listing_is_flagged(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(kd, "LISTING_CAP", 2)
    r = asyncio.run(kd.sync_series(tmp_path, _rest(Api()), "KXT"))
    assert r.capped


def test_an_event_that_gains_settled_markets_is_fetched_again(tmp_path: Path):
    api = Api()
    rest = _rest(api)
    asyncio.run(kd.sync_series(tmp_path, rest, "KXT"))
    ARCHIVED.append(_m("KXT-25JAN15-B37.5", "no", "2025-01-16T04:59:00Z", volume_fp="0.00"))
    try:
        r = asyncio.run(kd.sync_series(tmp_path, rest, "KXT"))
    finally:
        ARCHIVED.pop()
    assert (r.fetched, r.cached) == (1, 1)
    c = pl.read_parquet(kd.candles_dir(tmp_path, "KXT", "1h") / "KXT-25JAN15.parquet")
    assert c["ticker"].n_unique() == 3


def test_one_failing_event_is_reported_not_fatal(tmp_path: Path):
    class Flaky(Api):
        def __call__(self, req):
            if "KXT-26OCT01" in req.url.path:
                raise httpx.ConnectError("boom")
            return super().__call__(req)

    r = asyncio.run(kd.sync_series(tmp_path, _rest(Flaky()), "KXT"))
    assert r.fetched == 1 and len(r.failed) == 1 and "KXT-26OCT01" in r.failed[0]
