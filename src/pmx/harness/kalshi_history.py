"""Kalshi NFL history: every settled game market, linked to nflverse games, with 1-minute candles — the market
side of model-vs-market comparisons and of candle-fidelity backtests.

`data/kalshi/history/markets.parquet` (every settled market), `games.parquet` (one row per linked game), and
`candles/<ticker>.parquet` (the game's window: 90 min before the first snap to the market's close). Candles are
fetched once per ticker; re-running fetches only what's missing."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import msgspec
import polars as pl

from pmx.connectors.kalshi import history
from pmx.connectors.kalshi.rest import KalshiRest
from pmx.core.units import ONE_DOLLAR, parse_qty
from pmx.sports.nfl import data as nfl_data
from pmx.sports.nfl.catalog import SERIES, parse_ticker

log = logging.getLogger(__name__)

PRE_KICKOFF_MS = 90 * 60_000


def history_dir(root: Path) -> Path:
    return root / "kalshi" / "history"


def candle_path(root: Path, ticker: str) -> Path:
    return history_dir(root) / "candles" / f"{ticker}.parquet"


async def sync_markets(root: Path, rest: KalshiRest, series: str = SERIES) -> pl.DataFrame:
    rows = []
    for source, m in await history.settled_markets(rest, series):
        c = parse_ticker(m.ticker)
        rows.append({"ticker": m.ticker, "event_ticker": m.event_ticker, "team": c.team if c else None,
                     "result": m.result, "close_ms": history.ts_ms(m.close_time),
                     "expected_expiration_ms": history.ts_ms(m.expected_expiration_time),
                     "volume": parse_qty(m.volume_fp) if m.volume_fp else None,
                     "settlement": round(float(m.settlement_value_dollars) * ONE_DOLLAR)
                     if m.settlement_value_dollars else None, "source": source})
    df = pl.DataFrame(rows, schema={"ticker": pl.Utf8, "event_ticker": pl.Utf8, "team": pl.Utf8, "result": pl.Utf8,
                                    "close_ms": pl.Int64, "expected_expiration_ms": pl.Int64, "volume": pl.Int64,
                                    "settlement": pl.Int64, "source": pl.Utf8})
    out = history_dir(root) / "markets.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    return df


def nfl_games(root: Path, seasons: list[int]) -> pl.DataFrame:
    """One row per REG/POST game in the local nflverse data: id, season, week, date, teams, first snap, result."""
    have = [s for s in seasons if nfl_data.pbp_path(root, s).exists()]
    if not have:
        return pl.DataFrame()
    st = nfl_data.states(root, have)
    return st.group_by("game_id").agg(
        pl.col("season").first(), pl.col("season_type").first(), pl.col("week").first(),
        pl.col("game_date").first(), pl.col("home").first(), pl.col("away").first(),
        pl.col("snap_ms").min().alias("kickoff_ms"), pl.col("home_win").first(), pl.col("result").first(),
    ).sort(["season", "week", "game_id"])


@dataclass
class Links:
    games: pl.DataFrame
    unmatched: list[str] = field(default_factory=list)  # event tickers with no nflverse game
    outside: int = 0  # events outside the nflverse data's seasons or dates (preseason, not yet downloaded)
    duplicates: int = 0  # a game's other, less traded events


def link_games(root: Path, markets: pl.DataFrame, seasons: list[int]) -> Links:
    games = nfl_games(root, seasons)
    if games.is_empty():
        return Links(pl.DataFrame())
    by_event: dict[str, dict[str, dict]] = {}
    for m in markets.iter_rows(named=True):
        c = parse_ticker(m["ticker"])
        if c is not None:
            by_event.setdefault(c.event_ticker, {})[c.team] = {**m, "contract": c}
    days = {date.fromisoformat(d).toordinal() for d in games["game_date"]}
    newest = max(days)
    chosen: dict[str, tuple[int, dict]] = {}  # game id -> (volume, row)
    unmatched, outside, duplicates = [], 0, 0
    index: dict[frozenset[str], list[dict]] = {}
    for g in games.iter_rows(named=True):
        index.setdefault(frozenset((g["home"], g["away"])), []).append(g)
    for event, legs in sorted(by_event.items()):
        c = next(iter(legs.values()))["contract"]
        day = c.date.toordinal()
        if day > newest + 1 or not any(day + k in days for k in range(-3, 4)):  # newer data, or preseason
            outside += 1
            continue
        game = next((g for g in index.get(frozenset(c.teams), [])
                     if c.matches(date.fromisoformat(g["game_date"]), g["home"], g["away"])), None)
        if game is None:
            unmatched.append(event)
            continue
        volume = sum(x["volume"] or 0 for x in legs.values())
        prev = chosen.get(game["game_id"])
        if prev is not None and prev[0] >= volume:
            duplicates += 1  # Kalshi listed the game twice (a date not yet set): the one that traded wins
            continue
        duplicates += prev is not None
        home, away = legs.get(game["home"]), legs.get(game["away"])
        chosen[game["game_id"]] = (volume, {"game_id": game["game_id"], "season": game["season"], "week": game["week"],
                     "season_type": game["season_type"], "home": game["home"], "away": game["away"],
                     "event_ticker": event, "home_ticker": home["ticker"] if home else None,
                     "away_ticker": away["ticker"] if away else None, "kickoff_ms": game["kickoff_ms"],
                     "home_win": game["home_win"], "home_result": home["result"] if home else None,
                     "away_result": away["result"] if away else None,
                     "home_settlement": home["settlement"] if home else None,
                     "close_ms": max((x["close_ms"] or 0) for x in legs.values()), "volume": volume})
    rows = [r for _, r in sorted(chosen.values(), key=lambda v: (v[1]["season"], v[1]["week"], v[1]["game_id"]))]
    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    if rows:
        df.write_parquet(history_dir(root) / "games.parquet")
    return Links(df, unmatched, outside, duplicates)


async def sync_candles(root: Path, rest: KalshiRest, games: pl.DataFrame, *, cutoff_ms: int, force: bool = False,
                       series: str = SERIES) -> tuple[int, int]:
    """Candles for both tickers of every linked game; returns (fetched, already cached)."""
    fetched = cached = 0
    for g in games.iter_rows(named=True):
        for ticker in (g["home_ticker"], g["away_ticker"]):
            if ticker is None:
                continue
            path = candle_path(root, ticker)
            if path.exists() and not force:
                cached += 1
                continue
            start = g["kickoff_ms"] - PRE_KICKOFF_MS
            end = max(g["close_ms"] or 0, g["kickoff_ms"] + 5 * 3_600_000)  # a close can come before kickoff
            cs = await history.candles(rest, series, ticker, start, end, archived=end < cutoff_ms)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            frame = pl.DataFrame([msgspec.structs.asdict(c) for c in cs], schema=_CANDLE_SCHEMA)
            frame.write_parquet(tmp)
            tmp.replace(path)
            fetched += 1
    return fetched, cached


_CANDLE_SCHEMA = {f: pl.Int64 for f in history.Candle.__struct_fields__}


@dataclass
class Check:
    games: int
    with_candles: int
    consistent: int  # the last candle agrees with the result (winner ≥ 95¢ or loser ≤ 5¢)
    inconsistent: list[str]


def check(root: Path, games: pl.DataFrame) -> Check:
    consistent, bad, with_c = 0, [], 0
    for g in games.iter_rows(named=True):
        path = candle_path(root, g["home_ticker"]) if g["home_ticker"] else None
        if path is None or not path.exists():
            continue
        cs = pl.read_parquet(path)
        if cs.is_empty():
            continue
        with_c += 1
        last = cs.filter(pl.col("price_close").is_not_null() | pl.col("bid_close").is_not_null()).tail(1)
        if last.is_empty():
            bad.append(g["game_id"])
            continue
        row = last.row(0, named=True)
        price = row["price_close"] if row["price_close"] is not None else row["bid_close"]
        settle = g.get("home_settlement")
        if settle is None:
            settle = ONE_DOLLAR if g["home_result"] == "yes" else 0
        if abs(price - settle) <= 500:  # within 5¢ of what it paid (a tie pays 50¢)
            consistent += 1
        else:
            bad.append(f"{g['game_id']} ({g['home_ticker']} last {price / 100:.0f}¢, result {g['home_result']})")
    return Check(len(games), with_c, consistent, bad)


def spot_check(root: Path, ticker: str, around_ms: int, minutes: int = 5) -> pl.DataFrame:
    path = candle_path(root, ticker)
    if not path.exists():
        return pl.DataFrame()
    cs = pl.read_parquet(path)
    near = (pl.col("end_ms") >= around_ms - minutes * 60_000) & (pl.col("end_ms") <= around_ms + minutes * 60_000)
    return cs.filter(near).with_columns(pl.from_epoch("end_ms", time_unit="ms").alias("end_utc"))


def kickoff_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%MZ")
