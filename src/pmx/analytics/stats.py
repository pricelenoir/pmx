"""Performance statistics: is a strategy (or a slot, or everything) profitable, why, and how sure are we?

Everything comes from the ledger — fills, settlements, `fill_meta` (model fair, mid, phase, markouts at entry),
the `equity` series — filtered by env and mode, so live, paper, and backtest ledgers read the same way.

An **episode** is one slot's position in one market from flat back to flat (a fill that flips the position closes
one episode and opens the next), or to settlement. Episodes count in the range they close in; still-open ones
are left out of the episode stats and valued, in total, at the latest `equity` row's unrealized PnL. Sports results
are few and correlated within a game, so confidence comes from a bootstrap that resamples **entities** (a game, a
station-day…; a market's event when nothing links it) with their episodes, 2,000 times, seeded: a strategy that
won three games reads as inconclusive, however large each win. Money is in micros; `*_c` in cents per contract."""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

from pmx.core.units import ONE_DOLLAR
from pmx.feed.proto import PerfStats

TZ = ZoneInfo("America/New_York")
RESAMPLES = 2_000
SEED = 7


class ReadOnly:
    """A ledger opened read-only (the live engine's analytics run beside its writer)."""

    def __init__(self, path: Path | str, env: str) -> None:
        self.db = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True, check_same_thread=False)
        self.env = env

    def close(self) -> None:
        self.db.close()


@dataclass
class Episode:
    slot: str
    ticker: str
    entity: str
    opened_ms: int
    closed_ms: int | None = None
    pnl: int = 0  # net of fees, micros
    fees: int = 0
    contracts: int = 0  # traded, Qty
    phase: str | None = None
    edge: float | None = None  # predicted edge at entry, ¢ per contract (qty-weighted over entry fills)
    fair: float | None = None  # at the first entry, for Brier
    mid: float | None = None
    settled: int | None = None  # YES settlement price, if it settled


@dataclass
class Data:
    """What one query of the ledger gives the stats: closed episodes in range, totals, the equity series."""

    episodes: list[Episode] = field(default_factory=list)
    open_episodes: int = 0
    open_fees: int = 0  # fees already paid on the positions still open (the open mark excludes them)
    fills: int = 0
    orders: int = 0
    resting: int = 0
    resting_filled: int = 0
    contracts: int = 0
    maker_qty: int = 0
    fees: int = 0
    markets: set[str] = field(default_factory=set)
    entities: set[str] = field(default_factory=set)
    edge_entry: list[tuple[float, int]] = field(default_factory=list)  # (¢, qty)
    edge_realized: list[tuple[float, float, int]] = field(default_factory=list)  # (entry ¢, realized ¢, qty)
    markouts: dict[str, list[tuple[int, int]]] = field(default_factory=lambda: {"10s": [], "1m": [], "5m": []})
    equity: list[tuple[int, int, int, int]] = field(default_factory=list)  # (ts, equity, unrealized, capital at risk)
    equity_before: int = 0


def entity_of(ticker: str, linked: str | None) -> str:
    return linked or ticker.rsplit("-", 1)[0]


def _in(ts: int, start: int | None, end: int | None) -> bool:
    return (start is None or ts >= start) and (end is None or ts < end)


def load(ledger: Any, *, mode: str, strategy: str | None = None, slot: str | None = None,
         start_ms: int | None = None, end_ms: int | None = None) -> Data:
    db, env = ledger.db, ledger.env
    where, args = "f.env=? AND f.mode=?", [env, mode]
    if slot is not None:
        where += " AND o.strategy=?"
        args.append(slot)
    elif strategy is not None:
        where += " AND o.strategy LIKE ? ESCAPE '\\'"
        args.append(strategy.replace("%", "\\%").replace("_", "\\_") + "/%")
    rows = db.execute(
        "SELECT COALESCE(o.strategy, ''), f.ticker, f.side, f.price, f.qty, f.fee, f.is_taker, f.ts_ms, m.entity, "
        "m.fair, m.mid, m.phase, m.markout_10s, m.markout_1m, m.markout_5m FROM fills f "
        "LEFT JOIN orders o ON o.client_order_id = f.client_order_id "
        "LEFT JOIN fill_meta m ON m.venue = f.venue AND m.mode = f.mode AND m.env = f.env AND m.fill_id = f.fill_id "
        f"WHERE {where} ORDER BY f.ts_ms, f.rowid", args).fetchall()
    settled: dict[str, tuple[int, int]] = {  # ticker -> (YES settlement price, ts)
        t: (ONE_DOLLAR if r == "yes" else 0, ts) for t, r, ts in db.execute(
            "SELECT ticker, result, ts_ms FROM settlements WHERE env=? AND mode=?", (env, mode))}
    settled |= {t: (p, ts) for t, p, ts in db.execute(
        "SELECT ticker, price, ts_ms FROM settled WHERE env=? AND mode=?", (env, mode))}
    d = Data()
    pos: dict[tuple[str, str], int] = defaultdict(int)
    open_: dict[tuple[str, str], Episode] = {}
    cash: dict[tuple[str, str], int] = defaultdict(int)  # open episode: YES-terms trade cash
    edge_w: dict[tuple[str, str], tuple[float, int]] = {}
    for (slot_, ticker, side, price, qty, fee, taker, ts, linked, fair, mid, phase, m10, m1, m5) in rows:
        key = (slot_, ticker)
        entity = entity_of(ticker, linked)
        signed = qty if side == "bid" else -qty
        before = pos[key]
        in_range = _in(ts, start_ms, end_ms)
        if in_range:
            d.fills += 1
            d.contracts += qty
            d.maker_qty += 0 if taker else qty
            d.fees += fee
            d.markets.add(ticker)
            d.entities.add(entity)
            for h, m in (("10s", m10), ("1m", m1), ("5m", m5)):
                if m is not None:
                    d.markouts[h].append((m, qty))
        # split a flip into the closing part and the opening part
        closing = min(qty, abs(before)) if before and (before > 0) != (signed > 0) else 0
        parts = [(closing, False)] if closing else []
        if qty - closing:
            parts.append((qty - closing, True))
        for i, (part, opening) in enumerate(parts):
            share_fee = fee * part // qty if i < len(parts) - 1 else fee - fee * parts[0][0] // qty * i
            sgn = part if side == "bid" else -part
            if opening and pos[key] == 0:
                open_[key] = Episode(slot_, ticker, entity, ts, phase=phase, fair=fair, mid=mid)
                cash[key] = 0
                edge_w.pop(key, None)
            ep = open_[key]
            ep.contracts += part
            ep.fees += share_fee
            cash[key] -= sgn * price
            if opening and fair is not None:
                e = (fair * ONE_DOLLAR - price if side == "bid" else price - fair * ONE_DOLLAR) / 100
                w_e, w = edge_w.get(key, (0.0, 0))
                edge_w[key] = (w_e + e * part, w + part)
                if in_range:
                    d.edge_entry.append((e, part))
                    if ticker in settled:
                        s = settled[ticker][0]
                        d.edge_realized.append((e, (s - price if side == "bid" else price - s) / 100, part))
            pos[key] += sgn
            if pos[key] == 0:
                _close(ep, cash[key], edge_w.pop(key, None), ts)
                del open_[key]
                if _in(ts, start_ms, end_ms):
                    d.episodes.append(ep)
    for key, ep in list(open_.items()):
        t = settled.get(key[1])
        if t is not None:
            ep.settled, at = t
            _close(ep, cash[key] + pos[key] * t[0], edge_w.get(key), at)
            if _in(at, start_ms, end_ms):
                d.episodes.append(ep)
        else:
            d.open_episodes += 1
            d.open_fees += ep.fees
    for ep in d.episodes:
        if ep.settled is None and ep.ticker in settled:
            ep.settled = settled[ep.ticker][0]
    _orders(d, db, env, mode, strategy, slot, start_ms, end_ms)
    scope = f"slot:{slot}" if slot else f"strategy:{strategy}" if strategy else "portfolio"
    eq_where, eq_args = "env=? AND mode=? AND scope=?", [env, mode, scope]
    if start_ms is not None:
        before = db.execute(f"SELECT equity FROM equity WHERE {eq_where} AND ts_ms < ? ORDER BY ts_ms DESC LIMIT 1",
                            [*eq_args, start_ms]).fetchone()
        d.equity_before = before[0] if before else 0
    rng, rng_args = "", []
    if start_ms is not None:
        rng, rng_args = rng + " AND ts_ms >= ?", [*rng_args, start_ms]
    if end_ms is not None:
        rng, rng_args = rng + " AND ts_ms < ?", [*rng_args, end_ms]
    d.equity = db.execute(f"SELECT ts_ms, equity, unrealized, capital_at_risk FROM equity WHERE {eq_where}{rng} "
                          "ORDER BY ts_ms", [*eq_args, *rng_args]).fetchall()
    return d


def _close(ep: Episode, cash: int, edge: tuple[float, int] | None, ts: int) -> None:
    ep.pnl = cash - ep.fees
    ep.closed_ms = ts
    if edge is not None and edge[1]:
        ep.edge = edge[0] / edge[1]


def _orders(d: Data, db: sqlite3.Connection, env: str, mode: str, strategy: str | None, slot: str | None,
            start_ms: int | None, end_ms: int | None) -> None:
    where, args = "env=? AND mode=?", [env, mode]
    if slot is not None:
        where += " AND strategy=?"
        args.append(slot)
    elif strategy is not None:
        where += " AND strategy LIKE ? ESCAPE '\\'"
        args.append(strategy.replace("%", "\\%").replace("_", "\\_") + "/%")
    if start_ms is not None:
        where += " AND created_ms >= ?"
        args.append(start_ms)
    if end_ms is not None:
        where += " AND created_ms < ?"
        args.append(end_ms)
    d.orders, d.resting, d.resting_filled = db.execute(
        f"SELECT COUNT(*), COALESCE(SUM(tif = 'gtc'), 0), COALESCE(SUM(tif = 'gtc' AND filled > 0), 0) FROM orders "
        f"WHERE {where}", args).fetchone()


# ---- the numbers --------------------------------------------------------------------------------------------------


def cpc(pnl: float, qty: float) -> float | None:
    """Cents per contract: micros over Qty (1e-2 contracts)."""
    return None if not qty else pnl / qty * 100 / 10_000


def bootstrap(episodes: list[Episode], *, resamples: int = RESAMPLES, seed: int = SEED
              ) -> tuple[tuple[int, int], tuple[float, float], float] | None:
    """95% CIs of total PnL and ¢/contract, and P(¢/contract ≤ 0), resampling entities with their episodes."""
    by: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for ep in episodes:
        e = by[ep.entity]
        e[0] += ep.pnl
        e[1] += ep.contracts
    if len(by) < 2:
        return None
    arr = np.array(list(by.values()), dtype=np.float64)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(arr), size=(resamples, len(arr)))
    pnl = arr[idx, 0].sum(axis=1)
    qty = arr[idx, 1].sum(axis=1)
    per = np.where(qty > 0, pnl / np.maximum(qty, 1) * 100 / 10_000, 0.0)
    lo, hi = np.percentile(pnl, [2.5, 97.5])
    clo, chi = np.percentile(per, [2.5, 97.5])
    return (round(lo), round(hi)), (round(float(clo), 3), round(float(chi), 3)), round(float((per <= 0).mean()), 4)


def _daily(d: Data) -> list[tuple[int, int]]:
    """(local midnight ms, PnL that day) from the equity series."""
    last: dict[int, int] = {}
    for ts, eq, _, _ in d.equity:
        day = datetime.fromtimestamp(ts / 1000, TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        last[int(day.timestamp() * 1000)] = eq
    out, prev = [], d.equity_before
    for day in sorted(last):
        out.append((day, last[day] - prev))
        prev = last[day]
    return out


def _under_water(d: Data) -> tuple[int, int]:
    """(max drawdown, longest time under water ms) over the equity series."""
    hwm, worst, longest, since = None, 0, 0, None
    for ts, eq, _, _ in d.equity:
        if hwm is None or eq >= hwm:
            if since is not None:
                longest = max(longest, ts - since)
            hwm, since = eq, None
        else:
            worst = max(worst, hwm - eq)
            since = since if since is not None else ts
    if since is not None and d.equity:
        longest = max(longest, d.equity[-1][0] - since)
    return worst, longest


def _brier(episodes: list[Episode]) -> tuple[float | None, float | None]:
    pts = [(ep.fair, ep.mid, ep.settled / ONE_DOLLAR) for ep in episodes if ep.settled is not None]
    model = [(f - y) ** 2 for f, _, y in pts if f is not None]
    market = [(m / ONE_DOLLAR - y) ** 2 for _, m, y in pts if m is not None]
    return (round(sum(model) / len(model), 5) if model else None,
            round(sum(market) / len(market), 5) if market else None)


def compute(d: Data) -> PerfStats:
    eps = d.episodes
    wins = [e.pnl for e in eps if e.pnl > 0]
    losses = [e.pnl for e in eps if e.pnl <= 0]
    closed_qty = sum(e.contracts for e in eps)
    realized_net = sum(e.pnl for e in eps)
    fees_closed = sum(e.fees for e in eps)
    open_mark = d.equity[-1][2] if d.equity else 0
    boot = bootstrap(eps)
    entry_q = sum(q for _, q in d.edge_entry)
    real_q = sum(q for _, _, q in d.edge_realized)
    edge_entry = sum(e * q for e, q in d.edge_entry) / entry_q if entry_q else None
    edge_real = sum(r * q for _, r, q in d.edge_realized) / real_q if real_q else None
    edge_at = sum(e * q for e, _, q in d.edge_realized) / real_q if real_q else None
    daily = _daily(d)
    pnls = [p for _, p in daily]
    sharpe = None
    if len(pnls) >= 2:
        sd = float(np.std(pnls, ddof=1))
        sharpe = round(float(np.mean(pnls)) / sd, 3) if sd > 0 else None
    worst, longest = _under_water(d)
    car = [c for _, _, _, c in d.equity if c]
    brier_model, brier_market = _brier(eps)
    net = realized_net + open_mark - d.open_fees
    return PerfStats(
        net=net, gross=net + d.fees, fees=d.fees, realized=realized_net + fees_closed, open=open_mark,
        orders=d.orders, fills=d.fills, contracts=d.contracts,
        maker_share=round(d.maker_qty / d.contracts, 4) if d.contracts else None,
        fill_rate=round(d.resting_filled / d.resting, 4) if d.resting else None,
        episodes=len(eps), markets=len(d.markets), entities=len(d.entities),
        win_rate=round(len(wins) / len(eps), 4) if eps else None,
        avg_win=round(sum(wins) / len(wins)) if wins else None,
        avg_loss=round(sum(losses) / len(losses)) if losses else None,
        profit_factor=round(sum(wins) / -sum(losses), 3) if losses and sum(losses) < 0 else None,
        expectancy_c=round(c, 3) if (c := cpc(realized_net, closed_qty)) is not None else None,
        expectancy_ci=boot[1] if boot else None, pnl_ci=boot[0] if boot else None,
        p_nonpositive=boot[2] if boot else None,
        best_episode=max((e.pnl for e in eps), default=None), worst_episode=min((e.pnl for e in eps), default=None),
        edge_entry_c=round(edge_entry, 3) if edge_entry is not None else None,
        edge_realized_c=round(edge_real, 3) if edge_real is not None else None,
        capture=round(edge_real / edge_at, 3) if edge_real is not None and edge_at else None,
        markouts_c={h: round(sum(m for m, _ in v) / sum(q for _, q in v) * 100 / 10_000, 3)
                    for h, v in d.markouts.items() if v and sum(q for _, q in v)},
        max_drawdown=worst, time_under_water_ms=longest, max_capital_at_risk=max(car, default=0),
        return_on_risk=round(net / (sum(car) / len(car)), 4) if car else None,
        daily=daily, sharpe_day=sharpe, days=len(daily),
        pct_days_positive=round(sum(p > 0 for p in pnls) / len(pnls), 4) if pnls else None,
        brier_model=brier_model, brier_market=brier_market)


def strategy_stats(ledger: Any, *, mode: str, strategy: str | None = None, slot: str | None = None,
                   start_ms: int | None = None, end_ms: int | None = None) -> PerfStats:
    return compute(load(ledger, mode=mode, strategy=strategy, slot=slot, start_ms=start_ms, end_ms=end_ms))


def spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    rx = np.argsort(np.argsort(xs)).astype(float)
    ry = np.argsort(np.argsort(ys)).astype(float)
    if rx.std() == 0 or ry.std() == 0:
        return None
    r = float(np.corrcoef(rx, ry)[0, 1])
    return None if math.isnan(r) else round(r, 3)
