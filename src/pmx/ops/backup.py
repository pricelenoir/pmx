"""Backups: `pmx ops backup` copies what can't be rebuilt — raw logs and the ledger — to a second place.

Raw logs are append-only hourly files, so a copy is incremental: a file whose size differs at the destination is
copied again (to a temporary name, then renamed) and verified by hash. Each ledger (`*.sqlite` under the data
directory) is copied with SQLite's online backup API, so it's consistent while the engine writes, verified with
`PRAGMA integrity_check`, and kept `keep_days` days."""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class Report:
    raw_copied: int = 0
    raw_bytes: int = 0
    raw_current: int = 0
    ledgers: list[Path] = field(default_factory=list)
    pruned: int = 0
    problems: list[str] = field(default_factory=list)


def _digest(path: Path, limit: int | None = None) -> str:
    """Of the first `limit` bytes (all by default)."""
    h = hashlib.blake2b(digest_size=16)
    left = limit if limit is not None else -1
    with open(path, "rb") as f:
        while left and (chunk := f.read(1 << 20 if left < 0 else min(1 << 20, left))):
            h.update(chunk)
            left -= len(chunk) if left > 0 else 0
    return h.hexdigest()


def backup(data: Path, dest: Path, *, keep_days: int = 14, now: float | None = None) -> Report:
    now = time.time() if now is None else now
    r = Report()
    raw = data / "raw"
    for src in sorted(raw.rglob("*.log.zst")) if raw.is_dir() else []:
        target = dest / "raw" / src.relative_to(raw)
        size = src.stat().st_size
        if target.exists() and target.stat().st_size == size:
            r.raw_current += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".part")
        shutil.copyfile(src, tmp)
        copied = tmp.stat().st_size  # the hour being written grows meanwhile: what's copied must match its start
        if _digest(tmp) != _digest(src, copied):
            tmp.unlink(missing_ok=True)
            r.problems.append(f"{src}: the copy doesn't match; tried again next time")
            continue
        os.replace(tmp, target)
        r.raw_copied += 1
        r.raw_bytes += copied
    stamp = datetime.fromtimestamp(now).strftime("%Y%m%d-%H%M%S")
    (dest / "ledger").mkdir(parents=True, exist_ok=True)
    for db in sorted(data.glob("*.sqlite")):
        target = dest / "ledger" / f"{db.stem}-{stamp}.sqlite"
        src_conn, dst_conn = sqlite3.connect(db), sqlite3.connect(target)
        try:
            src_conn.backup(dst_conn)
            ok = dst_conn.execute("PRAGMA integrity_check").fetchone()[0]
        finally:
            src_conn.close()
            dst_conn.close()
        if ok != "ok":
            r.problems.append(f"{target}: integrity check said {ok!r}")
        r.ledgers.append(target)
    cutoff = now - keep_days * 86_400
    for old in (dest / "ledger").glob("*.sqlite"):
        if old.stat().st_mtime < cutoff and old not in r.ledgers:
            old.unlink()
            r.pruned += 1
    return r
