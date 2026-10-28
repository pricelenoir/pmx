"""Raw log -> research tables, one parquet per UTC day:

  bbo/<day>.parquet        a row whenever a contract's top of book changes (null = empty side / reset)
  trades/<day>.parquet     public prints
  contracts.parquet        latest metadata per contract (merged across days)

Full depth is always recoverable from the raw log via `replay`.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pmx.connectors.base import Decoder
from pmx.core.book import BookTracker
from pmx.core.types import BookDelta, BookSnapshot, Contract, StreamReset, Trade
from pmx.recorder.replay import replay

BBO_SCHEMA = pa.schema([
    ("recv_ms", pa.int64()), ("ticker", pa.string()),
    ("bid", pa.int32()), ("bid_qty", pa.int64()), ("ask", pa.int32()), ("ask_qty", pa.int64()),
])
TRADE_SCHEMA = pa.schema([
    ("recv_ms", pa.int64()), ("ts_ms", pa.int64()), ("ticker", pa.string()), ("trade_id", pa.string()),
    ("price", pa.int32()), ("qty", pa.int64()), ("taker_side", pa.string()),
])
CONTRACT_SCHEMA = pa.schema([
    ("venue", pa.string()), ("ticker", pa.string()), ("event_ticker", pa.string()), ("series_ticker", pa.string()),
    ("title", pa.string()), ("subtitle", pa.string()), ("status", pa.string()), ("close_ts_ms", pa.int64()),
    ("event_ts_ms", pa.int64()), ("result", pa.string()), ("seen_ns", pa.int64()),
])


def compact_day(root: Path, source: str, day: date, decoder: Decoder) -> dict[str, int]:
    tracker = BookTracker()
    top: dict[str, tuple] = {}
    bbo: list[tuple] = []
    trades: list[tuple] = []
    contracts: dict[str, tuple] = {}

    sleep_gaps = 0
    for ns, ev in replay(root, source, decoder, day, day):
        ms = ns // 1_000_000
        if isinstance(ev, Contract):
            contracts[ev.ticker] = (ev.venue, ev.ticker, ev.event_ticker, ev.series_ticker, ev.title, ev.subtitle,
                                    ev.status, ev.close_ts_ms, ev.event_ts_ms, ev.result, ns)
        elif isinstance(ev, Trade):
            trades.append((ms, ev.ts_ms, ev.ticker, ev.trade_id, ev.price, ev.qty, ev.taker_side))
        elif isinstance(ev, StreamReset):
            sleep_gaps += ev.reason == "sleep_gap"
            tracker.on_event(ev)
            for t in [t for t, v in top.items() if v != (None,) * 4]:
                top[t] = (None,) * 4
                bbo.append((ms, t, None, None, None, None))
        elif isinstance(ev, BookSnapshot | BookDelta):
            book = tracker.on_event(ev)
            if book is None:
                continue
            b, a = book.best_bid() or (None, None), book.best_ask() or (None, None)
            cur = (b[0], b[1], a[0], a[1])
            if top.get(book.ticker) != cur:
                top[book.ticker] = cur
                bbo.append((ms, book.ticker, *cur))

    out = Path(root) / "parquet" / source
    counts = {"bbo": len(bbo), "trades": len(trades), "contracts": len(contracts), "sleep_gaps": sleep_gaps}
    _write(out / "bbo" / f"{day}.parquet", BBO_SCHEMA, bbo)
    _write(out / "trades" / f"{day}.parquet", TRADE_SCHEMA, trades)
    _merge_contracts(out / "contracts.parquet", contracts)
    return counts


def _write(path: Path, schema: pa.Schema, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*rows, strict=True)) if rows else [[] for _ in schema]
    pq.write_table(pa.table([pa.array(c, type=f.type) for c, f in zip(cols, schema, strict=True)], schema=schema), path)


def _merge_contracts(path: Path, new: dict[str, tuple]) -> None:
    rows: dict[str, tuple] = {}
    if path.exists():
        for r in pq.read_table(path).to_pylist():
            rows[r["ticker"]] = tuple(r[f.name] for f in CONTRACT_SCHEMA)
    for t, r in new.items():
        if t not in rows or r[-1] >= rows[t][-1]:
            rows[t] = r
    _write(path, CONTRACT_SCHEMA, list(rows.values()))
