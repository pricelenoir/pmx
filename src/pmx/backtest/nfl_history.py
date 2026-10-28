"""Candle-fidelity NFL history: backtest NFL strategies over 2025–26 now, before there are recordings, from
nflverse plays and Kalshi's 1-minute candles (`data/kalshi/history/`). Lower fidelity than recorded books,
and labeled so (`fidelity = "candle"`): resting-order fills are approximate.

For each linked game, from 90 minutes before kickoff:
- market data (engine source `kalshi`): both contracts at the window's start; per candle, two synthetic prints at
  the minute's low (taker sold) and high (taker bought), half its volume each, 1 ms before the candle's end, then
  a one-level book at the closing bid and ask, `depth` contracts a side; the settled contracts at the end.
- game state (engine source `espn`): a `pre` state at the window's start, the state before each play `latency_s`
  after it became known, and the final. `espn_home_wp` carries nflfastR's WP (`vegas_home_wp`) unless `wp="none"`
  — the history's stand-in for the live feed's number, so `espn_wp` models have something to pass through. The
  game's date is the Kalshi event's, so the `nfl_game` linker joins them as it does live.
Days are built one at a time into a heap that releases an event only once no later day can come before it, so
the stream is in exact time order (a market that closed days later stretches past the next day's start) while
memory holds a few days' games, not a season's."""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any, Literal

import polars as pl

from pmx.core.types import BookSnapshot, Contract, Trade
from pmx.core.units import ONE_DOLLAR
from pmx.harness import kalshi_history as kh
from pmx.sports.nfl import data as nfl_data
from pmx.sports.nfl.catalog import SERIES, parse_ticker
from pmx.sports.nfl.state import REG_PERIOD_S, GameState

PRE_MS = kh.PRE_KICKOFF_MS


class Stream:
    """One engine source's view of the history (what the backtest runner merges)."""

    def __init__(self, history: NflHistory, name: str, kind: Literal["market", "game"]) -> None:
        self.history, self.name, self.kind = history, name, kind
        self.raw = f"nfl_history:{name}"
        self.count = 0

    def events(self) -> Iterator[tuple[int, Any]]:
        for ms, ev in self.history.events(self.kind):
            self.count += 1
            yield ms, ev


class NflHistory:
    fidelity = "candle"

    def __init__(self, root: Path, *, seasons: list[int] | None = None, games: list[str] | None = None,
                 start: date | None = None, end: date | None = None, latency_s: float = 15.0, depth: int = 500,
                 wp: Literal["nflfastr", "none"] = "nflfastr") -> None:
        self.root, self.latency_ms, self.depth, self.wp = root, round(latency_s * 1000), depth * 100, wp
        path = kh.history_dir(root) / "games.parquet"
        if not path.exists():
            raise FileNotFoundError(f"no Kalshi history at {path}: run `pmx data kalshi-history` first")
        g = pl.read_parquet(path).filter(pl.col("home_ticker").is_not_null() & pl.col("away_ticker").is_not_null())
        if seasons:
            g = g.filter(pl.col("season").is_in(seasons))
        if games:
            g = g.filter(pl.col("game_id").is_in(games))
        g = g.with_columns(day=pl.col("home_ticker").map_elements(lambda t: parse_ticker(t).date.isoformat(),
                                                                   return_dtype=pl.Utf8))
        if start is not None:
            g = g.filter(pl.col("day") >= start.isoformat())
        if end is not None:
            g = g.filter(pl.col("day") <= end.isoformat())
        self.games = g.sort(["kickoff_ms", "game_id"])
        markets = pl.read_parquet(kh.history_dir(root) / "markets.parquet")
        self.markets = {m["ticker"]: m for m in markets.iter_rows(named=True)}
        self._states: pl.DataFrame | None = None

    def streams(self) -> list[Stream]:
        return [Stream(self, "kalshi", "market"), Stream(self, "espn", "game")]

    # ---- the stream -------------------------------------------------------------------------------------------

    def events(self, kind: str) -> Iterator[tuple[int, Any]]:
        days = [day for _, day in self.games.group_by("day", maintain_order=True)]
        starts = [int(d["kickoff_ms"].min()) - PRE_MS for d in days]
        heap: list[tuple[int, int, Any]] = []
        n = 0
        for i, day in enumerate(days):
            for g in day.iter_rows(named=True):
                for ms, _, ev in sorted(self._market(g) if kind == "market" else self._game(g),
                                        key=lambda e: (e[0], e[1])):
                    n += 1
                    heapq.heappush(heap, (ms, n, ev))
            later = starts[i + 1] if i + 1 < len(days) else None
            while heap and (later is None or heap[0][0] < later):
                ms, _, ev = heapq.heappop(heap)
                yield ms, ev

    def _market(self, g: dict) -> list[tuple[int, int, Any]]:
        out: list[tuple[int, int, Any]] = []
        start, seq = g["kickoff_ms"] - PRE_MS, 0
        last = start
        for ticker in (g["home_ticker"], g["away_ticker"]):
            m = self.markets.get(ticker, {})
            c = parse_ticker(ticker)
            assert c is not None
            contract = Contract("kalshi", ticker, c.event_ticker, f"{c.team} to win", c.team, "open", m.get("close_ms"),
                                series_ticker=SERIES, event_ts_ms=m.get("expected_expiration_ms"))
            out.append((start, seq, contract))
            seq += 1
            path = kh.candle_path(self.root, ticker)
            cs = pl.read_parquet(path) if path.exists() else pl.DataFrame()
            for r in cs.iter_rows(named=True):
                end = r["end_ms"]
                if end < start:
                    continue
                vol = r["volume"] or 0
                if vol > 0 and r["price_low"] is not None and r["price_high"] is not None:
                    half = max(vol // 2, 1)
                    out.append((end - 1, seq, Trade("kalshi", f"h-{ticker}-{end}-l", ticker, r["price_low"], half,
                                                    "ask", end - 1)))
                    out.append((end - 1, seq + 1, Trade("kalshi", f"h-{ticker}-{end}-h", ticker, r["price_high"],
                                                        half, "bid", end - 1)))
                bids = ((r["bid_close"], self.depth),) if r["bid_close"] else ()
                asks = ((r["ask_close"], self.depth),) if r["ask_close"] else ()
                out.append((end, seq + 2, BookSnapshot("kalshi", ticker, bids, asks, end)))
                seq += 3
                last = max(last, end)
            out.append((-1, seq, (ticker, contract, m)))  # settled below, once the window's end is known
            seq += 1
        end = last + 60_000
        settled = []
        for ms, s, ev in out:
            if ms == -1:
                ticker, contract, m = ev
                result = m.get("result")
                price = m.get("settlement")
                settled.append((end, s, Contract(
                    "kalshi", ticker, contract.event_ticker, contract.title, contract.subtitle, "settled",
                    contract.close_ts_ms, series_ticker=SERIES, event_ts_ms=contract.event_ts_ms,
                    result=result if result in ("yes", "no") else None,
                    settlement=price if price is not None else (ONE_DOLLAR if result == "yes" else 0))))
        return [e for e in out if e[0] != -1] + settled

    def _states_of(self, game_id: str) -> pl.DataFrame:
        if self._states is None:
            seasons = sorted({int(s) for s in self.games["season"].unique()})
            self._states = nfl_data.states(self.root, seasons)
        return self._states.filter(pl.col("game_id") == game_id)

    def _game(self, g: dict) -> list[tuple[int, int, Any]]:
        day = parse_ticker(g["home_ticker"]).date.isoformat()  # the event's date: the linker's key
        home, away, kickoff = g["home"], g["away"], g["kickoff_ms"]
        st = self._states_of(g["game_id"])
        out: list[tuple[int, int, Any]] = [(kickoff - PRE_MS, 0, GameState(g["game_id"], kickoff - PRE_MS, day, home,
                                                                          away, "pre", kickoff_ms=kickoff))]
        prev_desc, last_ms, seq = "", kickoff, 1
        for r in st.iter_rows(named=True):
            if r["ts_ms"] is None:  # nflverse doesn't say when it was known: not replayable
                continue
            qtr = int(r["qtr"])
            left = int(r["secs_left"] or 0)
            clock = left - (4 - qtr) * REG_PERIOD_S if qtr <= 4 else left
            known = int(r["ts_ms"]) + self.latency_ms
            poss = {1: "home", -1: "away"}.get(r["poss"])
            hr = r["home_rec_2h"]
            out.append((known, seq, GameState(
                g["game_id"], known, day, home, away, "in", qtr, max(0, clock), int(r["home_score"]),
                int(r["away_score"]), poss, _int(r["down"]), _int(r["ydstogo"]), _int(r["yte"]),
                _int(r["home_to"], 3), _int(r["away_to"], 3), None if hr is None else bool(hr), kickoff, prev_desc,
                r["nflfastr_wp"] if self.wp == "nflfastr" else None)))
            prev_desc, last_ms, seq = r["desc"] or "", known, seq + 1
        if not st.is_empty():
            f = st.row(-1, named=True)
            fh, fa = int(f["final_home"]), int(f["final_away"])
            end = last_ms + 60_000
            out.append((end, seq, GameState(g["game_id"], end, day, home, away, "post", int(f["qtr"]), 0, fh, fa,
                                            kickoff_ms=kickoff, last_play=prev_desc,
                                            winner="home" if fh > fa else "away" if fa > fh else None)))
        return out


def _int(v: Any, default: int | None = None) -> int | None:
    return default if v is None else int(v)
