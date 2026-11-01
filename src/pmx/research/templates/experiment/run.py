"""{{name}} — the rule under test.

`run(ctx, params)` returns one row per trade: event, close_ms, pnl (dollars per contract, net of costs), and
optionally t_ms, qty, capacity, p_model / p_market with y. This example sells YES (buys NO) on every market whose
bid is inside a price band at the decision time, and holds to settlement."""

import polars as pl

from pmx.research import costs


def run(ctx, params):
    p = ctx.panel(at=params["at"])                       # one row per market, as known at t_ms
    p = costs.with_fees(p, ctx.catalog)
    lo, hi = params["band"]
    p = p.filter(pl.col("bid").is_between(lo, hi) & pl.col("settlement").is_not_null())
    p = costs.next_volume(ctx.catalog, p, horizon="1h")  # future volume: capacity only, never a decision input
    return p.select(
        event=pl.col("event_ticker"),
        close_ms=pl.col("close_ms"),
        t_ms=pl.col("t_ms"),
        pnl=costs.sell("bid"),                           # taker at the bid, held to settlement
        capacity=pl.col("next_volume") * 0.1,            # at most 10% of the next hour's volume
        p_market=pl.col("mid"),
        y=pl.col("y"),
    )
