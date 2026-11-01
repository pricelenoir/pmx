"""Kalshi history for any series → `data/kalshi/series/<SERIES>/`, the market side of research:

  markets.parquet                    every settled market: times, result, settlement, strikes, volume
  series.json                        title, category, fee schedule
  candles_<period>/<EVENT>.parquet   candles of every market in one event (`ticker` column)
  trades/<EVENT>.parquet             every public print (optional: large for busy series)

Incremental: each event file records the tickers it covers and is fetched again only when the event has gained
settled markets since (or with `force`). Units as everywhere in pmx: prices in Price (1e-4 $), quantities in Qty
(1e-2 contracts), times in ms since the epoch."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import msgspec
import polars as pl
import pyarrow.parquet as pq

from pmx.connectors.kalshi import history
from pmx.connectors.kalshi.rest import KalshiError, KalshiRest
from pmx.connectors.kalshi.wire import WireSeries, to_fees
from pmx.core.fees import KALSHI_DEFAULT_FEES
from pmx.core.units import ONE_DOLLAR, parse_qty

PERIODS = {"1m": 1, "1h": 60, "1d": 1440}
LISTING_CAP = 10_000  # the archive's market listing stops here for a series (seen on KXBTCD)
DEFAULT_LIFE_MS = 7 * 86_400_000  # candle window before close when a market has no open time

MARKET_SCHEMA = {
    "ticker": pl.Utf8, "event_ticker": pl.Utf8, "series": pl.Utf8, "title": pl.Utf8, "yes_sub_title": pl.Utf8,
    "open_ms": pl.Int64, "close_ms": pl.Int64, "expiration_ms": pl.Int64, "result": pl.Utf8,
    "settlement": pl.Int64, "strike_type": pl.Utf8, "floor_strike": pl.Float64, "cap_strike": pl.Float64,
    "volume": pl.Int64, "source": pl.Utf8,
}
CANDLE_SCHEMA = {"ticker": pl.Utf8} | {f: pl.Int64 for f in history.Candle.__struct_fields__}
TRADE_SCHEMA = {"ticker": pl.Utf8, "trade_id": pl.Utf8, "ts_ms": pl.Int64, "price": pl.Int64, "qty": pl.Int64,
                "taker_yes": pl.Boolean}


def series_dir(root: Path, series: str) -> Path:
    return root / "kalshi" / "series" / series


def candles_dir(root: Path, series: str, period: str) -> Path:
    return series_dir(root, series) / f"candles_{period}"


def trades_dir(root: Path, series: str) -> Path:
    return series_dir(root, series) / "trades"


class _SeriesMeta(msgspec.Struct):
    ticker: str
    title: str = ""
    category: str = ""
    fee_type: str = ""
    fee_multiplier: float = 1.0


class _SeriesPage(msgspec.Struct):
    series: list[_SeriesMeta] | None = None
    cursor: str = ""


@dataclass
class SyncReport:
    series: str
    markets: int = 0
    events: int = 0
    fetched: int = 0
    cached: int = 0
    capped: bool = False
    failed: list[str] = field(default_factory=list)


async def series_in_category(rest: KalshiRest, category: str) -> list[str]:
    """Every series ticker Kalshi lists under `category` (e.g. "Climate and Weather")."""
    out, cursor = [], None
    while True:
        page = msgspec.json.decode(await rest.get("/series", {"category": category} | (
            {"cursor": cursor} if cursor else {})), type=_SeriesPage)
        out += [s.ticker for s in page.series or []]
        cursor = page.cursor
        if not cursor:
            return sorted(set(out))


async def sync_meta(root: Path, rest: KalshiRest, series: str) -> dict:
    body = json.loads(await rest.get(f"/series/{series}"))["series"]
    s = msgspec.convert(body, _SeriesMeta)
    fees = to_fees(WireSeries(s.ticker, s.fee_type, s.fee_multiplier)) or KALSHI_DEFAULT_FEES
    meta = {"series": series, "title": s.title, "category": s.category, "fee_type": s.fee_type,
            "taker_e4": fees.taker_e4, "maker_e4": fees.maker_e4}
    path = series_dir(root, series) / "series.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(meta, indent=2) + "\n")
    return meta


async def sync_markets(root: Path, rest: KalshiRest, series: str) -> tuple[pl.DataFrame, bool]:
    """All settled markets of `series` → markets.parquet; also whether the archive listing hit its cap."""
    listed = await history.settled_markets(rest, series)
    rows = []
    for source, m in listed:
        rows.append({
            "ticker": m.ticker, "event_ticker": m.event_ticker, "series": series, "title": m.title,
            "yes_sub_title": m.yes_sub_title, "open_ms": history.ts_ms(m.open_time),
            "close_ms": history.ts_ms(m.close_time), "expiration_ms": history.ts_ms(m.expected_expiration_time),
            "result": m.result, "settlement": round(float(m.settlement_value_dollars) * ONE_DOLLAR)
            if m.settlement_value_dollars else None, "strike_type": m.strike_type, "floor_strike": m.floor_strike,
            "cap_strike": m.cap_strike, "volume": parse_qty(m.volume_fp) if m.volume_fp else None, "source": source,
        })
    df = pl.DataFrame(rows, schema=MARKET_SCHEMA).sort("close_ms", "ticker")
    out = series_dir(root, series) / "markets.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    return df, sum(1 for s, _ in listed if s == "historical") >= LISTING_CAP


_TICKERS = b"pmx.tickers"  # parquet metadata: the markets a file covers (a market can have no candles or trades)


def _write(frame: pl.DataFrame, path: Path, tickers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    table = frame.to_arrow()
    pq.write_table(table.replace_schema_metadata((table.schema.metadata or {}) | {
        _TICKERS: json.dumps(sorted(tickers)).encode()}), tmp)
    tmp.replace(path)


def _covers(path: Path, tickers: list[str]) -> bool:
    if not path.exists():
        return False
    meta = pq.read_schema(path).metadata or {}
    if _TICKERS in meta:
        covered = set(json.loads(meta[_TICKERS]))
    else:  # written without the list: the markets it has rows for
        covered = set(pl.read_parquet(path, columns=["ticker"])["ticker"].to_list())
    return set(tickers) <= covered


async def _event_candles(rest: KalshiRest, series: str, markets: list[dict], period: str) -> pl.DataFrame:
    rows = []
    for m in markets:
        end = m["close_ms"]
        start = m["open_ms"] or end - DEFAULT_LIFE_MS
        cs = await history.candles(rest, series, m["ticker"], start, end, archived=m["source"] == "historical",
                                   period_min=PERIODS[period])
        rows += [{"ticker": m["ticker"]} | msgspec.structs.asdict(c) for c in cs]
    return pl.DataFrame(rows, schema=CANDLE_SCHEMA)


async def _event_trades(rest: KalshiRest, markets: list[dict]) -> pl.DataFrame:
    rows = []
    for m in markets:
        ts = await history.trades(rest, m["ticker"], archived=m["source"] == "historical")
        rows += [{"ticker": m["ticker"]} | msgspec.structs.asdict(t) for t in ts]
    return pl.DataFrame(rows, schema=TRADE_SCHEMA)


async def sync_series(root: Path, rest: KalshiRest, series: str, *, period: str = "1h", trades: bool = False,
                      since_ms: int | None = None, force: bool = False, concurrency: int = 8,
                      progress: Callable[[str], None] | None = None) -> SyncReport:
    """Markets, then candles (and trades) per event; events closing before `since_ms` are listed but not fetched."""
    if period not in PERIODS:
        raise ValueError(f"period {period!r}: one of {', '.join(PERIODS)}")
    r = SyncReport(series)
    await sync_meta(root, rest, series)
    markets, r.capped = await sync_markets(root, rest, series)
    r.markets = markets.height
    todo = markets.filter(pl.col("close_ms").is_not_null())
    if since_ms is not None:
        todo = todo.filter(pl.col("close_ms") >= since_ms)
    events = todo.partition_by("event_ticker", as_dict=False, maintain_order=True)
    r.events = len(events)
    sem = asyncio.Semaphore(concurrency)
    done = 0

    async def one(ev: pl.DataFrame) -> None:
        nonlocal done
        name = ev["event_ticker"][0]
        jobs = [(candles_dir(root, series, period) / f"{name}.parquet", "candles")]
        if trades:
            jobs.append((trades_dir(root, series) / f"{name}.parquet", "trades"))
        rows = ev.to_dicts()
        tickers = [m["ticker"] for m in rows]
        async with sem:
            for path, kind in jobs:
                if not force and _covers(path, tickers):
                    r.cached += 1
                    continue
                try:
                    frame = (await _event_candles(rest, series, rows, period) if kind == "candles"
                             else await _event_trades(rest, rows))
                except (KalshiError, httpx.HTTPError) as e:
                    r.failed.append(f"{name} {kind}: {e}")
                    continue
                _write(frame, path, tickers)
                r.fetched += 1
        done += 1
        if progress and (done % 100 == 0 or done == r.events):
            progress(f"{series}: {done}/{r.events} events")

    await asyncio.gather(*(one(ev) for ev in events))
    return r
