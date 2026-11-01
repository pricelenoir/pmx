"""The research table: one row per (market, decision time), holding only what was known at that time.

`panel(cat, "KXHIGHNY", at="T-6h")` gives each visible market's last candle at or before 6 hours before its close;
`at="every"` gives a row at the end of every candle. Prices are dollars as floats (= implied probabilities),
from the integer candles. Outside data joins on with `asof`, which only matches rows with `known_ms <= t_ms`.
An empty side is null, not a price: Kalshi reports no YES bid as 0 and no YES ask as $1.00, neither tradable.
The outcome (`y`, `settlement`) is on every row for scoring; a rule that reads it to decide is lookahead."""

from __future__ import annotations

import re

import polars as pl

from pmx.core.units import ONE_DOLLAR
from pmx.research.catalog import Catalog

_UNITS = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def duration_ms(s: str) -> int:
    """`90s`, `30m`, `6h`, `2d` → milliseconds."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", s.strip())
    if not m:
        raise ValueError(f"duration {s!r}: a number and one of s, m, h, d (e.g. 6h)")
    return round(float(m[1]) * _UNITS[m[2]])


def quote_history(cat: Catalog, series: str | list[str], period: str) -> pl.DataFrame:
    """Per candle end: closing bid/ask/last (Price) and cumulative volume (Qty), sorted by end_ms."""
    return (cat.candles(series, period)
            .select("ticker", "end_ms", "bid_close", "ask_close", "price_close", "volume")
            .with_columns(volume_cum=pl.col("volume").cum_sum().over("ticker"))
            .drop("volume").sort("end_ms"))


def panel(cat: Catalog, series: str | list[str], *, at: str = "T-1h", period: str = "1h") -> pl.DataFrame:
    """Columns: series, event_ticker, ticker, yes_sub_title, strike_type, floor_strike, cap_strike, t_ms, close_ms,
    hours_to_close, bid, ask, mid, spread (from the last candle at or before t; null sides had no quote), last (that
    candle's last trade, null if none), volume_cum (contracts traded up to t), quote_age_s (t − that candle's end),
    y, settlement."""
    markets = cat.markets(series).select(
        "series", "event_ticker", "ticker", "yes_sub_title", "strike_type", "floor_strike", "cap_strike", "open_ms",
        "close_ms", "y", "settlement")
    candles = quote_history(cat, series, period)
    if at == "every":
        rows = candles.select("ticker", pl.col("end_ms").alias("t_ms")).join(markets, on="ticker")
        rows = rows.filter(pl.col("t_ms") < pl.col("close_ms"))
    elif at.startswith("T-"):
        off = duration_ms(at[2:])
        rows = markets.with_columns(t_ms=pl.col("close_ms") - off)
        rows = rows.filter(pl.col("open_ms").is_null() | (pl.col("t_ms") >= pl.col("open_ms")))
    else:
        raise ValueError(f"at {at!r}: 'T-<duration>' (before each market's close) or 'every'")
    rows = rows.sort("t_ms").join_asof(candles, left_on="t_ms", right_on="end_ms", by="ticker", strategy="backward",
                                       check_sortedness=False)  # both sides sorted just above
    d = float(ONE_DOLLAR)
    out = rows.with_columns(
        bid=pl.when(pl.col("bid_close") > 0).then(pl.col("bid_close") / d),
        ask=pl.when(pl.col("ask_close") < ONE_DOLLAR).then(pl.col("ask_close") / d), last=pl.col("price_close") / d,
        volume_cum=pl.col("volume_cum").fill_null(0) / 100, quote_age_s=(pl.col("t_ms") - pl.col("end_ms")) / 1000,
        hours_to_close=(pl.col("close_ms") - pl.col("t_ms")) / 3_600_000, settlement=pl.col("settlement") / d,
    ).with_columns(mid=(pl.col("bid") + pl.col("ask")) / 2, spread=pl.col("ask") - pl.col("bid"))
    return out.select(
        "series", "event_ticker", "ticker", "yes_sub_title", "strike_type", "floor_strike", "cap_strike", "t_ms",
        "close_ms", "hours_to_close", "bid", "ask", "mid", "spread", "last", "volume_cum", "quote_age_s", "y",
        "settlement").sort("close_ms", "ticker", "t_ms")


def asof(left: pl.DataFrame, right: pl.DataFrame, *, by: str | list[str] | None = None,
         left_on: str = "t_ms") -> pl.DataFrame:
    """Each left row gets the latest right row with `known_ms <= left_on` (per `by` key): no lookahead."""
    if "known_ms" not in right.columns:
        raise ValueError("asof: the right table needs a known_ms column (when each row became public)")
    return left.sort(left_on).join_asof(right.sort("known_ms"), left_on=left_on, right_on="known_ms", by=by,
                                        strategy="backward", check_sortedness=False)
