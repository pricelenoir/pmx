"""Kalshi NFL history — both candle schemas, pagination, chunking, linking to nflverse games, the cache."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path

import httpx
import polars as pl

from pmx.connectors.kalshi import history
from pmx.connectors.kalshi.rest import KalshiRest
from pmx.harness import kalshi_history as kh

FIX = Path(__file__).parent / "fixtures"
KO = 1_757_264_400_000  # 2025-09-07 17:00Z, the first snap of NYG @ DAL in the mini pbp fixture


def _market(ticker: str, result: str, volume: str = "1000.00", close: str = "2025-09-08T00:00:00Z") -> dict:
    return {"ticker": ticker, "event_ticker": ticker.rsplit("-", 1)[0], "status": "finalized", "result": result,
            "close_time": close, "volume_fp": volume, "settlement_value_dollars": "1.0000" if result == "yes" else
            "0.0000", "expiration_value": "", "yes_sub_title": ""}


ARCHIVED = [
    _market("KXNFLGAME-25SEP07NYGDAL-DAL", "yes"), _market("KXNFLGAME-25SEP07NYGDAL-NYG", "no"),
    _market("KXNFLGAME-25SEP06NYGDAL-DAL", "yes", "0.00"),  # listed twice; this one never traded
    _market("KXNFLGAME-25SEP06NYGDAL-NYG", "no", "0.00"),
    _market("KXNFLGAME-25AUG30NYGDAL-DAL", "no"), _market("KXNFLGAME-25AUG30NYGDAL-NYG", "yes"),  # preseason
]
LIVE = [_market("KXNFLGAME-25SEP15BUFKC-BUF", "no", close="2025-09-15T04:00:00Z"),  # +1 day, teams swapped
        _market("KXNFLGAME-25SEP15BUFKC-KC", "yes", close="2025-09-15T04:00:00Z")]


def _candle_live(end_s: int, bid: str, ask: str) -> dict:
    return {"end_period_ts": end_s, "yes_bid": {"close_dollars": bid, "low_dollars": bid, "high_dollars": bid},
            "yes_ask": {"close_dollars": ask}, "price": {"close_dollars": ask, "low_dollars": bid,
                                                           "high_dollars": ask},
            "volume_fp": "12.50", "open_interest_fp": "100.00"}


def _candle_archive(end_s: int, bid: str, ask: str) -> dict:
    return {"end_period_ts": end_s, "yes_bid": {"close": bid, "low": bid, "high": bid}, "yes_ask": {"close": ask},
            "price": {"close": ask, "low": bid, "high": ask}, "volume": "12.50", "open_interest": "100.00"}


class Api:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path.removeprefix("/trade-api/v2")
        p = dict(req.url.params)
        self.calls.append(path)
        if path == "/historical/cutoff":
            return httpx.Response(200, json={"market_settled_ts": "2025-09-10T00:00:00Z"})
        if path == "/historical/markets":  # two pages
            first = p.get("cursor") is None
            return httpx.Response(200, json={"markets": ARCHIVED[:3] if first else ARCHIVED[3:],
                                             "cursor": "c2" if first else ""})
        if path == "/markets":
            assert p["status"] == "settled"
            return httpx.Response(200, json={"markets": LIVE, "cursor": ""})
        start, end = int(p["start_ts"]), int(p["end_ts"])
        assert (end - start) // 60 + 1 <= history.MAX_CANDLES, "chunked under the limit"
        win = "-DAL" in path or "-KC" in path
        ends = range(start - start % 60 + 60, end + 1, 3_600)
        if path.startswith("/historical/markets/"):
            return httpx.Response(200, json={"candlesticks": [
                _candle_archive(t, "0.9800" if win else "0.0100", "0.9900" if win else "0.0200") for t in ends]})
        assert path.startswith("/series/KXNFLGAME/markets/")
        return httpx.Response(200, json={"candlesticks": [
            _candle_live(t, "0.9700" if win else "0.0200", "0.9800" if win else "0.0300") for t in ends]})


def _rest(api: Api) -> KalshiRest:
    return KalshiRest("https://x/trade-api/v2", transport=httpx.MockTransport(api), read=(1e9, 1e9))


def test_both_candle_schemas_normalize_to_integer_units_and_long_windows_chunk():
    api = Api()

    async def go():
        rest = _rest(api)
        live = await history.candles(rest, "KXNFLGAME", "KXNFLGAME-25SEP15BUFKC-KC", 0, 6 * 86_400_000, archived=False)
        old = await history.candles(rest, "KXNFLGAME", "KXNFLGAME-25SEP07NYGDAL-NYG", 0, 3_600_000, archived=True)
        return live, old

    live, old = asyncio.run(go())
    assert api.calls.count("/series/KXNFLGAME/markets/KXNFLGAME-25SEP15BUFKC-KC/candlesticks") == 2, "8,640 > 5,000"
    c = live[0]
    assert (c.bid_close, c.ask_close, c.price_low, c.volume, c.oi, c.ask_low) == (9_700, 9_800, 9_700, 1_250, 10_000,
                                                                                 None)
    assert (old[0].bid_close, old[0].price_close, old[0].volume) == (100, 200, 1_250)


def test_sync_links_games_fetches_candles_once_and_checks_results(tmp_path):
    shutil.copy(FIX / "nfl" / "pbp_mini.parquet", tmp_path / "pbp.parquet")
    (tmp_path / "nflverse").mkdir()
    shutil.move(tmp_path / "pbp.parquet", tmp_path / "nflverse" / "pbp_2025.parquet")
    api = Api()

    async def go():
        rest = _rest(api)
        cutoff = await history.cutoff_ms(rest)
        markets = await kh.sync_markets(tmp_path, rest)
        links = kh.link_games(tmp_path, markets, [2025])
        first = await kh.sync_candles(tmp_path, rest, links.games, cutoff_ms=cutoff)
        again = await kh.sync_candles(tmp_path, rest, links.games, cutoff_ms=cutoff)
        return markets, links, first, again

    markets, links, first, again = asyncio.run(go())
    assert markets.height == 8 and set(markets["source"]) == {"historical", "live"}
    g = links.games
    assert g.select("game_id", "home_ticker", "away_ticker").rows() == [
        ("2025_01_NYG_DAL", "KXNFLGAME-25SEP07NYGDAL-DAL", "KXNFLGAME-25SEP07NYGDAL-NYG"),
        ("2025_02_KC_BUF", "KXNFLGAME-25SEP15BUFKC-BUF", "KXNFLGAME-25SEP15BUFKC-KC")]
    assert g["kickoff_ms"][0] == KO
    assert (links.unmatched, links.outside, links.duplicates) == ([], 1, 1), "preseason outside; the untraded twin"
    assert first == (4, 0) and again == (0, 4), "cached: nothing fetched twice"
    calls = [c for c in api.calls if "candlesticks" in c]
    assert sum(c.startswith("/historical/") for c in calls) == 2 and len(calls) == 4, "archive vs live by cutoff"
    cs = pl.read_parquet(kh.candle_path(tmp_path, "KXNFLGAME-25SEP07NYGDAL-DAL"))
    assert cs["end_ms"].min() >= KO - kh.PRE_KICKOFF_MS and cs["bid_close"][0] == 9_800
    c = kh.check(tmp_path, g)
    assert (c.games, c.with_candles, c.consistent, c.inconsistent) == (2, 2, 2, []), "DAL won ~99¢, BUF lost ~3¢"
