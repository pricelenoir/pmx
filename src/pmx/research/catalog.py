"""The research catalog: every table a study reads, cut to what its stage may see.

Kalshi tables come from `pmx data kalshi` (`data/kalshi/series/<SERIES>/`). Outside data is any parquet under the
data root with a `known_ms` column — when the value became public — so `panel.asof` can join it without lookahead.
A market is visible when it closed before the stage's end (`windows.py`); its candles and trades come with it."""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path

import polars as pl

from pmx.core.fees import KALSHI_DEFAULT_FEES, FeeSchedule
from pmx.research import kalshi_data as kd
from pmx.research.windows import Stage, Windows


class Catalog:
    def __init__(self, data_root: Path, windows: Windows, stage: Stage = "discovery") -> None:
        self.root, self.windows, self.stage = data_root, windows, stage
        self.visible_end_ms = windows.visible_end_ms(stage)
        self._read: dict[tuple, pl.DataFrame | None] = {}  # files don't change during a run: read each set once

    def series(self, patterns: str | list[str]) -> list[str]:
        """Downloaded series matching names or globs (`KXHIGH*`)."""
        pats = [patterns] if isinstance(patterns, str) else patterns
        if not pats:
            raise ValueError("no series given: list them in the spec's `series` or pass them to panel()")
        have = sorted(p.name for p in (self.root / "kalshi" / "series").glob("*") if (p / "markets.parquet").exists())
        out = [s for s in have if any(fnmatch.fnmatchcase(s, p) for p in pats)]
        if not out:
            raise FileNotFoundError(f"no downloaded series match {', '.join(pats)}; fetch with `pmx data kalshi "
                                    f"--series …` (have: {', '.join(have) or 'none'})")
        return out

    def _visible(self, frame: pl.LazyFrame | pl.DataFrame, col: str):
        if self.visible_end_ms is None:
            return frame
        return frame.filter(pl.col(col) < self.visible_end_ms)

    def markets(self, series: str | list[str]) -> pl.DataFrame:
        """Settled markets the stage may see, with `y` = 1.0 (YES) / 0.0 (NO) / null (anything else)."""
        frames = [pl.read_parquet(kd.series_dir(self.root, s) / "markets.parquet") for s in self.series(series)]
        df = pl.concat(frames).filter(pl.col("close_ms").is_not_null())
        df = self._visible(df, "close_ms")
        return df.with_columns(y=pl.when(pl.col("result") == "yes").then(1.0)
                               .when(pl.col("result") == "no").then(0.0).otherwise(None))

    def _per_event(self, series: str | list[str], sub: str) -> pl.DataFrame | None:
        names = self.series(series)
        key = (tuple(names), sub)
        if key not in self._read:
            files = [f for s in names for f in sorted((kd.series_dir(self.root, s) / sub).glob("*.parquet"))]
            tickers = self.markets(names).select("ticker", "series", "event_ticker", "close_ms")
            self._read[key] = (pl.scan_parquet(files).join(tickers.lazy(), on="ticker", how="inner").collect()
                               if files else None)
        return self._read[key]

    def candles(self, series: str | list[str], period: str = "1h") -> pl.DataFrame:
        """Candles of visible markets; each row is known at its `end_ms`."""
        df = self._per_event(series, f"candles_{period}")
        if df is None:
            raise FileNotFoundError(f"no {period} candles for {series}; fetch with `pmx data kalshi --series … "
                                    f"--period {period}`")
        return self._visible(df, "end_ms").sort("ticker", "end_ms")

    def trades(self, series: str | list[str]) -> pl.DataFrame:
        df = self._per_event(series, "trades")
        if df is None:
            raise FileNotFoundError(f"no trades for {series}; fetch with `pmx data kalshi --series … --trades`")
        return self._visible(df, "ts_ms").sort("ticker", "ts_ms")

    def fees(self, series: str) -> FeeSchedule:
        path = kd.series_dir(self.root, series) / "series.json"
        if not path.exists():
            return KALSHI_DEFAULT_FEES
        meta = json.loads(path.read_text())
        return FeeSchedule(meta["taker_e4"], meta["maker_e4"])

    def table(self, path: str) -> pl.DataFrame:
        """Outside data (`<data root>/<path>`, parquet with `known_ms`), cut at the stage's end."""
        df = pl.read_parquet(self.root / path)
        if "known_ms" not in df.columns:
            raise ValueError(f"{path}: outside data needs a known_ms column (when each row became public)")
        return self._visible(df, "known_ms")
