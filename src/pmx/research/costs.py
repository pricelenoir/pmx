"""Costs, per contract in dollars: every PnL a study reports is net of these.

`with_fees` puts each series' fee rates on the panel; `buy` / `sell` are the PnL of buying or selling one YES contract
at a price column and holding it to settlement. `next_volume` is what traded after each decision — future data, for
capping fills (capacity) only, never for deciding."""

from __future__ import annotations

import polars as pl

from pmx.research.catalog import Catalog
from pmx.research.panel import duration_ms, quote_history


def with_fees(df: pl.DataFrame, cat: Catalog) -> pl.DataFrame:
    """Adds taker_rate / maker_rate (fraction of P × (1 − P) per contract) for each row's series."""
    rates = pl.DataFrame([{"series": s, "taker_rate": f.taker_e4 / 10_000, "maker_rate": f.maker_e4 / 10_000}
                          for s in df["series"].unique().to_list() for f in [cat.fees(s)]],
                         schema={"series": pl.Utf8, "taker_rate": pl.Float64, "maker_rate": pl.Float64})
    return df.join(rates, on="series", how="left")


def fee(price: str | pl.Expr, *, taker: bool = True) -> pl.Expr:
    p = pl.col(price) if isinstance(price, str) else price
    return pl.col("taker_rate" if taker else "maker_rate") * p * (1 - p)


def buy(price: str | pl.Expr = "ask", *, taker: bool = True) -> pl.Expr:
    """PnL of buying one YES at `price` and holding to settlement (needs `with_fees`)."""
    p = pl.col(price) if isinstance(price, str) else price
    return pl.col("settlement") - p - fee(p, taker=taker)


def sell(price: str | pl.Expr = "bid", *, taker: bool = True) -> pl.Expr:
    """PnL of selling one YES (= buying NO at 1 − price) and holding to settlement."""
    p = pl.col(price) if isinstance(price, str) else price
    return p - pl.col("settlement") - fee(p, taker=taker)


def next_volume(cat: Catalog, df: pl.DataFrame, *, horizon: str = "1h", period: str = "1h") -> pl.DataFrame:
    """Adds next_volume: contracts traded in (t_ms, t_ms + horizon], from candles (FUTURE data: capacity only)."""
    c = quote_history(cat, df["series"].unique().to_list(), period).select("ticker", "end_ms", "volume_cum")
    h = duration_ms(horizon)
    out = df.with_columns(_t_end=pl.col("t_ms") + h).sort("t_ms")
    out = out.join_asof(c.rename({"volume_cum": "_v0"}), left_on="t_ms", right_on="end_ms", by="ticker",
                        strategy="backward", check_sortedness=False).drop("end_ms").sort("_t_end")
    out = out.join_asof(c.rename({"volume_cum": "_v1"}), left_on="_t_end", right_on="end_ms", by="ticker",
                        strategy="backward", check_sortedness=False).drop("end_ms")
    return out.with_columns(next_volume=((pl.col("_v1").fill_null(0) - pl.col("_v0").fill_null(0)) / 100)
                            .clip(lower_bound=0)).drop("_t_end", "_v0", "_v1")
