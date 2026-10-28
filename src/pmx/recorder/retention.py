"""Raw-log retention: a source's day of raw logs is deleted once it is older than `keep_days`
*and* compacted to parquet (`parquet/<source>/*/<day>.parquet`). A day that was never compacted is
kept however old it is: raw is the only copy until then."""

from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path

import msgspec


class Expired(msgspec.Struct, frozen=True):
    source: str
    day: date
    path: Path
    compacted: bool  # False: kept, because nothing else holds it yet


def plan(root: Path, keep_days: int, today: date) -> list[Expired]:
    """Raw day directories older than `keep_days`, oldest first per source."""
    cutoff = today - timedelta(days=keep_days)
    out = []
    raw = Path(root) / "raw"
    for source_dir in sorted(p for p in raw.glob("*") if p.is_dir()):
        for day_dir in sorted(p for p in source_dir.glob("*") if p.is_dir()):
            try:
                day = date.fromisoformat(day_dir.name)
            except ValueError:
                continue
            if day < cutoff:
                compacted = any((Path(root) / "parquet" / source_dir.name).glob(f"*/{day}.parquet"))
                out.append(Expired(source_dir.name, day, day_dir, compacted))
    return out


def apply(expired: list[Expired]) -> int:
    """Delete the compacted ones; returns how many days went."""
    n = 0
    for e in expired:
        if e.compacted:
            shutil.rmtree(e.path)
            n += 1
    return n
