"""Research: find edge with pre-registered experiments (docs/RESEARCH.md).

  kalshi_data   any series' settled markets, candles, trades → data/kalshi/series/
  windows       discovery / validation / holdout by close time
  catalog       the tables a stage may see
  panel         one row per (market, decision time); `asof` joins outside data without lookahead
  costs         fees and fill capacity; PnL per contract net of costs
  evaluate      net edge with an event-clustered CI, stability, calibration, capacity, pass criteria
  spec · workspace · runner · report   experiments: spec.md locked by hash, lifecycle, trials, reports"""
