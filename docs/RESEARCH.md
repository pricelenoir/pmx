# Research — finding edge with pre-registered experiments

An edge is real only if it survives a test written down before the results were seen. pmx makes that the path of
least resistance: exploration is free, and confirmation goes through experiments whose spec is locked by hash,
judged on data the exploration never touched, with every variant counted.

```
 idea ──► AGENDA.md ──► spec.md ──approve──► discovery ──► validation ──► holdout (once) ──► promote?
          (mechanism)   (locked)             (≤ N variants) (1 frozen rule) (1 shot)         you decide
                                                  └────────────── dead ──────────────┴──► FINDINGS.md
```

A promoted experiment becomes a model and/or strategy and continues through the trading path: engine backtest →
paper → live at minimum size (GUIDE). Its expected edge is the bar paper and live results are held to.

## Data

```sh
pmx data kalshi --series KXHIGHNY KXBTCD          # settled markets + hourly candles (--period 1m|1h|1d)
pmx data kalshi --series KXHIGHNY --trades         # + every public trade, with the taker's side
pmx data kalshi --category "Climate and Weather" --since 2025-01-01
```

Everything lands in `data/kalshi/series/<SERIES>/`: `markets.parquet` (times, result, settlement, strikes,
volume), `series.json` (fees), `candles_<period>/` and `trades/` (one file per event, fetched again only when the
event gains settled markets; re-running the command resumes an interrupted download). Kalshi serves
markets settled before its archive cutoff from `/historical/*`; both are read. For a few very busy series the archive
listing stops at 10,000 markets, and the download says so.

What history can't give you, record: order-book depth (candles carry only the top of book per period), sub-period
quotes, and outside data nobody archives as it was known. `pmx record` keeps raw days (`config/recorder.toml`).

Outside data for a study is any parquet under the data root with a `known_ms` column: when each row became
public. That column is what makes it joinable without lookahead.

## Windows

`research/windows.toml` splits time by market close (when the outcome became known):

| Stage | Sees | Judged on |
|---|---|---|
| discovery (and exploration) | markets closing before `discovery_end` | the same |
| validation | everything before `validation_end` | `[discovery_end, validation_end)` |
| holdout | everything | on or after `validation_end`, once per experiment |

Validation can fit on discovery-era data and is scored only on its own window. Roll the dates forward every few
months so the newest data becomes the locked holdout again.

## The library (`pmx.research`)

```python
from pmx.research import costs
from pmx.research.catalog import Catalog
from pmx.research.panel import panel, asof

cat = Catalog(data_root, windows, "discovery")
p = panel(cat, "KXHIGHNY", at="T-6h")          # one row per market: quotes as known 6 h before close
p = asof(p, obs, by="station")                  # outside data with known_ms <= t_ms only
p = costs.with_fees(p, cat)
p = p.with_columns(pnl=costs.sell("bid"))       # sell YES at the bid, hold to settlement, net of fees
```

- **`panel`**: `at="T-<duration>"` (before each close) or `"every"` (each candle end). Prices are dollars (=
  probabilities) from the last candle at or before the decision; an empty side (Kalshi's $0 bid / $1.00 ask) is
  null, and `quote_age_s` says how old the quote is. `y` and `settlement` are there for scoring only.
- **`costs`**: the series' taker/maker fee (`rate × P × (1 − P)` per contract); `buy` / `sell` PnL to settlement;
  `next_volume` (what traded after the decision; future data, for capping fills only).
- **`evaluate`**: one row per trade (`event`, `close_ms`, `pnl`, optional `qty`, `t_ms`, `capacity`, `p_model`,
  `p_market`, `y`) → net ¢/contract with a 95% CI that resamples whole events (the engine's verdict uses the same
  bootstrap), P(edge ≤ 0), edge by quarter, capacity × edge in $/day over the calendar span, calibration (Brier
  of `p_model` vs `p_market`), and the spec's pass criteria
  (`min_events`, `min_edge_c`, `ci_low_c`, `max_p_le0`, `min_usd_per_day`, `recent_quarters_positive`).

## Experiments

```sh
pmx research init                     # creates research/ (private, git-ignored; `pmx ops backup` copies it)
pmx research new weather-longshot     # experiments/<date>-weather-longshot/: spec.md, run.py, notes.md
pmx research approve NAME             # you, in a terminal: locks spec.md + run.py
pmx research run NAME                 # runs the current stage, prints the report, logs trials
pmx research advance NAME             # discovery → validation (set params first) → holdout
pmx research amend NAME --reason …    # re-lock after a change; counted in every report
pmx research close NAME --as dead|promoted --note …   # promoting: you, after a holdout run
pmx research status
```

`spec.md` is TOML front matter plus the reasoning:

```markdown
+++
series = ["KXHIGH*"]
variants_max = 6
params = { side = "sell" }                       # validation/holdout run exactly this
grid = { band = [[0.03, 0.10], [0.05, 0.15], [0.10, 0.20]], at = ["T-6h", "T-2h"] }
pass = { min_events = 100, ci_low_c = 0.0, recent_quarters_positive = 2 }
+++
# Longshot temperature buckets are overpriced
## Mechanism   who is on the other side, why they misprice, why it persists
## Test        decision time, rule, prices, exit, costs, fill cap
## What would kill it
```

`run.py` defines `run(ctx, params) -> polars.DataFrame` (one row per trade); `ctx.panel`, `ctx.catalog`,
`ctx.config` (other front-matter keys) are what it reads. The runner:

- refuses a proposed (unapproved) experiment, and anything whose `spec.md` or `run.py` changed since its lock;
- runs every grid variant in discovery (at most `variants_max`), only `params` afterwards;
- fails a run whose rows were decided at or after their market's close (lookahead);
- scores only the stage's judged window, and says how many rows it dropped;
- writes `results/<stage>.json|txt`, appends each variant to `trials.csv`, and records the run in `state.json`;
- runs the holdout once (`--force` reruns it, and every later report says it's no longer out of sample).

Trials count distinct (experiment, stage, locked version, variant); re-running the same thing isn't a new trial.
Reports show the count for the experiment and research-wide: the best of many tries is biased upward, and the
count is how much.

## Rules of thumb

- Start from a mechanism: who loses money to you, why, and why nobody has taken it. Scans find hypotheses, not
  evidence.
- Every number is net of fees; capacity matters as much as edge (5¢ on $200/day is worth little).
- Look at edge by quarter: Kalshi changes fast, and an edge that is shrinking is ending.
- A variant whose neighbours in the grid also work is more believable than a lone spike.
- Dead is a result. Write down what it rules out.
