"""SQLite ledger: orders, fills, settlements. Positions and PnL are derived from fills,
never stored, so they can't drift from the record. Paper and live share the schema,
separated by `mode`; demo and prod by `env` (invariant 17).

Accounting is in YES terms: a bid pays price*qty, an ask receives it, and a settlement pays
its YES price per signed YES contract — $1 for YES, $0 for NO, and whatever else the venue
settles at (a tied NFL game pays $0.50). PnL matches the venue's; cash differs from the
venue balance by NO collateral.

Schema changes are additive only (new tables, new columns with defaults), so an old file
opens with a new build.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import msgspec

from pmx.core.position import AvgCost
from pmx.core.types import Fill, OrderAck, OrderRequest
from pmx.core.units import ONE_DOLLAR, Micros, Price, Qty

type Mode = Literal["paper", "live"]
type OrderStatus = Literal["pending", "open", "filled", "canceled", "rejected"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
    price INTEGER NOT NULL,
    qty INTEGER NOT NULL,
    tif TEXT NOT NULL,
    post_only INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('paper', 'live')),
    order_id TEXT,
    status TEXT NOT NULL,
    filled INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_open ON orders (mode, status);
CREATE TABLE IF NOT EXISTS fills (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
    price INTEGER NOT NULL,
    qty INTEGER NOT NULL,
    fee INTEGER NOT NULL,
    is_taker INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    PRIMARY KEY (venue, mode, fill_id)
);
CREATE TABLE IF NOT EXISTS settlements (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL,
    ticker TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result IN ('yes', 'no')),
    ts_ms INTEGER NOT NULL,
    PRIMARY KEY (venue, mode, ticker)
);
CREATE TABLE IF NOT EXISTS settled (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL,
    env TEXT NOT NULL DEFAULT '',
    ticker TEXT NOT NULL,
    price INTEGER NOT NULL,
    ts_ms INTEGER NOT NULL,
    PRIMARY KEY (venue, mode, env, ticker)
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    instance TEXT NOT NULL,
    started_ms INTEGER NOT NULL,
    ended_ms INTEGER,
    git_sha TEXT NOT NULL,
    dirty INTEGER NOT NULL,
    config_hash TEXT NOT NULL,
    config_toml TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS equity (
    ts_ms INTEGER NOT NULL,
    mode TEXT NOT NULL,
    env TEXT NOT NULL,
    scope TEXT NOT NULL,
    equity INTEGER NOT NULL,
    realized INTEGER NOT NULL,
    unrealized INTEGER NOT NULL,
    fees INTEGER NOT NULL,
    capital_at_risk INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS equity_scope ON equity (env, mode, scope, ts_ms);
CREATE TABLE IF NOT EXISTS fill_meta (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL,
    env TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    slot TEXT NOT NULL,
    entity TEXT,
    model TEXT,
    model_version TEXT,
    fair REAL,
    mid REAL,
    spread INTEGER,
    phase TEXT,
    markout_10s INTEGER,
    markout_1m INTEGER,
    markout_5m INTEGER,
    PRIMARY KEY (venue, mode, fill_id)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    env TEXT NOT NULL,
    slot TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    blob BLOB NOT NULL,
    PRIMARY KEY (env, slot)
);
"""
# Added after the first schema: attribution and environment on every order and fill.
_COLUMNS = {
    "orders": ["env TEXT NOT NULL DEFAULT ''", "run_id TEXT NOT NULL DEFAULT ''",
               "strategy_version TEXT NOT NULL DEFAULT ''", "params_hash TEXT NOT NULL DEFAULT ''"],
    "fills": ["env TEXT NOT NULL DEFAULT ''", "run_id TEXT NOT NULL DEFAULT ''",
              "strategy_version TEXT NOT NULL DEFAULT ''", "params_hash TEXT NOT NULL DEFAULT ''"],
    "settlements": ["env TEXT NOT NULL DEFAULT ''"],
}


class Attribution(msgspec.Struct, frozen=True):
    """Who placed an order: the run, the strategy's code version, its params' hash."""

    run_id: str = ""
    strategy_version: str = ""
    params_hash: str = ""


NO_ATTRIBUTION = Attribution()


class Position(msgspec.Struct):
    venue: str
    ticker: str
    qty: Qty = 0  # signed YES; zero after settlement
    cash: Micros = 0  # trade + settlement cash flows, excluding fees
    fees: Micros = 0
    settled: Literal["yes", "no"] | None = None
    settled_price: Price | None = None
    slot: str = ""  # by="slot" only

    def pnl(self, mark: Price | None = None) -> Micros:
        """Net PnL with open quantity valued at `mark` (required when qty != 0)."""
        if self.qty and mark is None:
            raise ValueError(f"{self.ticker}: open position needs a mark")
        return self.cash - self.fees + (self.qty * mark if self.qty else 0)


class Ledger:
    def __init__(self, path: Path | str, *, env: str = "", durable: bool = False) -> None:
        """`env` tags every row written and filters every read (demo and prod never mix). `durable`
        (any live slot) syncs every commit to disk."""
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.env = env
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(f"PRAGMA synchronous={'FULL' if durable else 'NORMAL'}")
        check = self.db.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"ledger {path} failed its integrity check: {check}")
        self.db.executescript(SCHEMA)
        for table, columns in _COLUMNS.items():
            have = {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}
            for col in columns:
                if col.split()[0] not in have:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN {col}")

    def close(self) -> None:
        self.db.close()

    # ---- orders ---------------------------------------------------------

    def record_order(self, req: OrderRequest, mode: Mode, now_ms: int | None = None, *,
                     attribution: Attribution = NO_ATTRIBUTION, status: str = "pending") -> None:
        now = now_ms or int(time.time() * 1000)
        a = attribution
        self.db.execute(
            "INSERT INTO orders (client_order_id, venue, ticker, side, price, qty, tif, post_only, strategy, mode, "
            "order_id, status, filled, error, created_ms, updated_ms, env, run_id, strategy_version, params_hash) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?,0,NULL,?,?,?,?,?,?)",
            (req.client_order_id, req.venue, req.ticker, req.side, req.price, req.qty, req.tif,
             int(req.post_only), req.strategy, mode, status, now, now, self.env, a.run_id, a.strategy_version,
             a.params_hash),
        )

    def ack_order(self, ack: OrderAck) -> None:
        status = "filled" if ack.remaining == 0 and ack.filled else ("open" if ack.remaining else "canceled")
        self.db.execute(
            "UPDATE orders SET order_id=?, status=?, filled=?, updated_ms=? WHERE client_order_id=?",
            (ack.order_id, status, ack.filled, ack.ts_ms, ack.client_order_id),
        )

    def set_order_status(self, client_order_id: str, status: OrderStatus, error: str | None = None, *,
                         filled: Qty | None = None, order_id: str | None = None, now_ms: int | None = None) -> None:
        self.db.execute(
            "UPDATE orders SET status=?, error=COALESCE(?, error), filled=COALESCE(?, filled), "
            "order_id=COALESCE(?, order_id), updated_ms=? WHERE client_order_id=?",
            (status, error, filled, order_id, now_ms or int(time.time() * 1000), client_order_id),
        )

    def open_orders(self, mode: Mode) -> list[dict]:
        cur = self.db.execute("SELECT * FROM orders WHERE mode=? AND env=? AND status IN ('pending','open')",
                              (mode, self.env))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur]

    def find_order(self, *, client_order_id: str | None = None, order_id: str | None = None) -> dict | None:
        """One order row of this env by our id or the venue's (live reconciliation)."""
        col, val = ("client_order_id", client_order_id) if client_order_id else ("order_id", order_id)
        cur = self.db.execute(f"SELECT * FROM orders WHERE {col}=? AND env=?", (val, self.env))
        row = cur.fetchone()
        return None if row is None else dict(zip([c[0] for c in cur.description], row, strict=True))

    def has_fill(self, venue: str, mode: Mode, fill_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM fills WHERE venue=? AND mode=? AND fill_id=?",
                               (venue, mode, fill_id)).fetchone() is not None

    def last_fill_ms(self, mode: Mode) -> int | None:
        return self.db.execute("SELECT MAX(ts_ms) FROM fills WHERE mode=? AND env=?", (mode, self.env)).fetchone()[0]

    def last_seq(self, prefix: str) -> int:
        """The highest sequence among client order ids `<prefix><hex seq>` (any env, mode, or run); 0 if none."""
        n = 0
        for (cid,) in self.db.execute("SELECT client_order_id FROM orders WHERE client_order_id LIKE ? ESCAPE '\\'",
                                      (prefix.replace("%", "\\%").replace("_", "\\_") + "%",)):
            try:
                n = max(n, int(cid[len(prefix):].rsplit("-", 1)[-1], 16))
            except ValueError:
                continue
        return n

    def cancel_open(self, mode: Mode, reason: str, now_ms: int | None = None) -> int:
        """Mark open orders canceled (paper orders don't survive a restart); returns how many."""
        cur = self.db.execute(
            "UPDATE orders SET status='canceled', error=?, updated_ms=? "
            "WHERE mode=? AND env=? AND status IN ('pending','open')",
            (reason, now_ms or int(time.time() * 1000), mode, self.env),
        )
        return cur.rowcount

    # ---- fills / settlements -------------------------------------------

    def record_fill(self, f: Fill, mode: Mode, *, attribution: Attribution = NO_ATTRIBUTION) -> bool:
        """Idempotent; returns False for a fill already recorded."""
        a = attribution
        cur = self.db.execute(
            "INSERT OR IGNORE INTO fills (venue, mode, fill_id, order_id, client_order_id, ticker, side, price, qty, "
            "fee, is_taker, ts_ms, env, run_id, strategy_version, params_hash) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f.venue, mode, f.fill_id, f.order_id, f.client_order_id, f.ticker, f.side, f.price, f.qty, f.fee,
             int(f.is_taker), f.ts_ms, self.env, a.run_id, a.strategy_version, a.params_hash),
        )
        return cur.rowcount == 1

    def record_settlement(self, venue: str, ticker: str, result: Literal["yes", "no"] | Price, mode: Mode,
                          ts_ms: int | None = None) -> None:
        """`result` is "yes" / "no", or the YES settlement price when it's neither (a tie, a void)."""
        price = ONE_DOLLAR if result == "yes" else 0 if result == "no" else int(result)
        self.db.execute("INSERT OR REPLACE INTO settled VALUES (?,?,?,?,?,?)",
                        (venue, mode, self.env, ticker, price, ts_ms or int(time.time() * 1000)))

    def settlements(self, mode: Mode) -> dict[tuple[str, str], tuple[Price, int]]:
        """(venue, ticker) -> (YES settlement price, ts_ms)."""
        out = {(v, t): (ONE_DOLLAR if r == "yes" else 0, ts) for v, t, r, ts in self.db.execute(
            "SELECT venue, ticker, result, ts_ms FROM settlements WHERE mode=? AND env=?", (mode, self.env))}
        for v, t, price, ts in self.db.execute(
                "SELECT venue, ticker, price, ts_ms FROM settled WHERE mode=? AND env=?", (mode, self.env)):
            out[(v, t)] = (price, ts)
        return out

    # ---- portfolio ------------------------------------------------

    def record_equity(self, rows: list[tuple[int, Mode, str, Micros, Micros, Micros, Micros, Micros]]) -> None:
        """(ts_ms, mode, scope, equity, realized, unrealized, fees, capital_at_risk) rows, in one transaction."""
        if not rows:
            return
        with self.db:
            self.db.execute("BEGIN")
            self.db.executemany("INSERT INTO equity VALUES (?,?,?,?,?,?,?,?,?)",
                                [(ts, mode, self.env, *rest) for ts, mode, *rest in rows])

    def equity_before(self, mode: Mode, ts_ms: int) -> dict[str, Micros]:
        """scope -> its last equity before `ts_ms` (the day's opening equity after a restart)."""
        return {scope: eq for scope, eq in self.db.execute(
            "SELECT scope, equity FROM equity e WHERE env=? AND mode=? AND ts_ms = (SELECT MAX(ts_ms) FROM equity "
            "WHERE env=e.env AND mode=e.mode AND scope=e.scope AND ts_ms < ?)", (self.env, mode, ts_ms))}

    def equity_high_since(self, mode: Mode, ts_ms: int) -> dict[str, Micros]:
        """scope -> its highest equity at or after `ts_ms`."""
        return {scope: eq for scope, eq in self.db.execute(
            "SELECT scope, MAX(equity) FROM equity WHERE env=? AND mode=? AND ts_ms >= ? GROUP BY scope",
            (self.env, mode, ts_ms))}

    def equity_series(self, mode: Mode, scope: str, since_ms: int = 0) -> list[tuple[int, Micros]]:
        return list(self.db.execute("SELECT ts_ms, equity FROM equity WHERE env=? AND mode=? AND scope=? AND "
                                    "ts_ms >= ? ORDER BY ts_ms", (self.env, mode, scope, since_ms)))

    def record_fill_meta(self, venue: str, mode: Mode, fill_id: str, ts_ms: int, slot: str, *, entity: str | None,
                         model: str | None, model_version: str | None, fair: float | None, mid: float | None,
                         spread: Price | None, phase: str | None) -> None:
        self.db.execute("INSERT OR REPLACE INTO fill_meta VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)",
                        (venue, mode, self.env, fill_id, ts_ms, slot, entity, model, model_version, fair, mid,
                         spread, phase))

    def set_markout(self, venue: str, mode: Mode, fill_id: str, horizon: Literal["10s", "1m", "5m"],
                    value: Micros | None) -> None:
        self.db.execute(f"UPDATE fill_meta SET markout_{horizon}=? WHERE venue=? AND mode=? AND fill_id=?",
                        (value, venue, mode, fill_id))

    def fill_meta(self, mode: Mode) -> list[dict]:
        cur = self.db.execute("SELECT * FROM fill_meta WHERE env=? AND mode=? ORDER BY ts_ms", (self.env, mode))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur]

    # ---- runs and checkpoints --------------------------------------

    def start_run(self, run_id: str, *, instance: str, started_ms: int, git_sha: str, dirty: bool,
                  config_hash: str, config_toml: str) -> None:
        self.db.execute("INSERT INTO runs VALUES (?,?,?,?,NULL,?,?,?,?)",
                        (run_id, self.env, instance, started_ms, git_sha, int(dirty), config_hash, config_toml))

    def end_run(self, run_id: str, ended_ms: int) -> None:
        self.db.execute("UPDATE runs SET ended_ms=? WHERE run_id=?", (ended_ms, run_id))

    def runs(self) -> list[dict]:
        cur = self.db.execute("SELECT * FROM runs WHERE env=? ORDER BY started_ms", (self.env,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur]

    def save_checkpoints(self, blobs: dict[str, tuple[str, bytes]], run_id: str, ts_ms: int) -> None:
        """slot -> (strategy version, blob); the latest per slot is kept."""
        with self.db:
            self.db.execute("BEGIN")
            self.db.executemany("INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?,?,?)",
                                [(self.env, slot, v, run_id, ts_ms, blob) for slot, (v, blob) in blobs.items()])

    def checkpoints(self) -> dict[str, tuple[str, bytes]]:
        """slot -> (strategy version, blob) as last saved."""
        return {slot: (v, bytes(blob)) for slot, v, blob in self.db.execute(
            "SELECT slot, strategy_version, blob FROM checkpoints WHERE env=?", (self.env,))}

    # ---- derived --------------------------------------------------------

    def fills(self, mode: Mode, slot: str | None = None) -> Iterator[tuple[str, Fill]]:
        """(slot, fill) in time order; the slot comes from the order's `strategy` ("" if unknown)."""
        q = ("SELECT COALESCE(o.strategy, ''), f.venue, f.fill_id, f.order_id, f.ticker, f.side, f.price, f.qty, "
             "f.fee, f.is_taker, f.ts_ms, f.client_order_id FROM fills f "
             "LEFT JOIN orders o ON o.client_order_id = f.client_order_id WHERE f.mode=? AND f.env=?")
        args: list = [mode, self.env]
        if slot is not None:
            q += " AND o.strategy=?"
            args.append(slot)
        for row in self.db.execute(q + " ORDER BY f.ts_ms, f.rowid", args):
            yield row[0], Fill(*row[1:9], bool(row[9]), row[10], row[11])

    def positions(self, mode: Mode, *, by: Literal["ticker", "slot"] = "ticker",
                  include_flat: bool = False) -> dict[tuple, Position]:
        """Keyed (venue, ticker), or (slot, venue, ticker) with `by="slot"` (attribution via the order)."""
        out: dict[tuple, Position] = {}
        for slot, f in self.fills(mode):
            key = (f.venue, f.ticker) if by == "ticker" else (slot, f.venue, f.ticker)
            p = out.get(key)
            if p is None:
                p = out[key] = Position(f.venue, f.ticker, slot=slot if by == "slot" else "")
            signed = f.qty if f.side == "bid" else -f.qty
            p.qty += signed
            p.cash -= signed * f.price
            p.fees += f.fee
        for (venue, ticker), (price, _) in self.settlements(mode).items():
            for p in out.values():
                if (p.venue, p.ticker) == (venue, ticker):
                    p.cash += p.qty * price
                    p.qty, p.settled_price = 0, price
                    p.settled = "yes" if price == ONE_DOLLAR else "no" if price == 0 else None
        if not include_flat:
            out = {k: p for k, p in out.items() if p.qty}
        return out

    def realized_pnl(self, mode: Mode) -> Micros:
        """PnL of flat and settled positions only."""
        return sum(p.pnl() for p in self.positions(mode, include_flat=True).values() if p.qty == 0)

    def pnl_series(self, mode: Mode, slot: str | None = None) -> list[tuple[int, Micros]]:
        """(ts_ms, cumulative realized PnL net of fees) after every fill and settlement — average cost per
        slot and market, the same arithmetic the order manager uses."""
        books: dict[tuple[str, str, str], AvgCost] = {}
        events: list[tuple[int, int, tuple, object]] = []
        for i, (s, f) in enumerate(self.fills(mode, slot)):
            events.append((f.ts_ms, i, (s, f.venue, f.ticker), f))
        settlements = self.settlements(mode)
        out: list[tuple[int, Micros]] = []
        total = 0
        done: set[tuple[str, str]] = set()
        for ts, _, key, f in sorted(events, key=lambda e: (e[0], e[1])):
            for (venue, ticker), (price, sts) in settlements.items():  # settlements strictly before this fill
                if (venue, ticker) not in done and sts < ts:
                    done.add((venue, ticker))
                    total += sum(b.settle(price) for k, b in books.items() if k[1:] == (venue, ticker))
                    out.append((sts, total))
            b = books.setdefault(key, AvgCost(mode))
            before = b.realized - b.fees
            b.fill(f.qty if f.side == "bid" else -f.qty, f.price, f.fee)
            total += b.realized - b.fees - before
            out.append((ts, total))
        for (venue, ticker), (price, sts) in sorted(settlements.items(), key=lambda kv: kv[1][1]):
            if (venue, ticker) not in done and any(k[1:] == (venue, ticker) for k in books):
                total += sum(b.settle(price) for k, b in books.items() if k[1:] == (venue, ticker))
                out.append((sts, total))
        return out
