"""Recording: record, compact."""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from datetime import UTC, date, datetime
from pathlib import Path

from pmx import config
from pmx.cli.common import die, on_signal, venue

log = logging.getLogger(__name__)


async def cmd_record(args: argparse.Namespace) -> None:
    from pmx import plugins
    from pmx.recorder import retention
    from pmx.recorder.sink import RecordingSink
    from pmx.sources.base import build_source
    from pmx.sources.kalshi import KalshiMarketData

    root = config.data_root()
    if args.counts:
        _counts(root, date.fromisoformat(args.counts) if args.counts != "today" else datetime.now(UTC).date())
        return
    keep_days = config.recorder_settings(Path(args.config) if args.config else None).keep_days
    if args.dry_run_retention:
        expired = retention.plan(root, keep_days, datetime.now(UTC).date())
        for e in expired:
            print(f"{e.source:12s} {e.day}  {'delete' if e.compacted else 'keep (not compacted yet)'}  {e.path}")
        print(f"{sum(e.compacted for e in expired)} of {len(expired)} raw days older than {keep_days} days would go")
        return
    cfg_path = Path(args.config) if args.config else None
    specs = {s.name: s for s in config.recorder_sources(cfg_path)}
    known = ["kalshi", *specs]
    sources = [s.strip() for s in args.sources.split(",") if s.strip()] if args.sources else known
    if unknown := sorted(set(sources) - set(known)):
        die(f"unknown source(s) {', '.join(unknown)} (configured: {', '.join(known)})")
    plugins.load_builtin_plugins()
    built = {name: build_source(specs[name]) for name in sources if name in specs}  # bad config fails here
    sink = RecordingSink(root)
    stop = asyncio.Event()
    on_signal(stop.set)
    jobs: dict[str, Coroutine] = {}
    v = None
    if "kalshi" in sources:
        v = venue(args)
        cfg = config.recorder_config(cfg_path)["kalshi"]
        if args.series:
            cfg = type(cfg)(series=args.series, tickers=cfg.tickers, refresh_s=cfg.refresh_s)
        if v.signer is None:
            log.warning("kalshi: no API key, so no websocket; polling books over REST instead (source kalshi_rest)")
        params = KalshiMarketData.Params(series=cfg.series, tickers=cfg.tickers, refresh_s=cfg.refresh_s)
        jobs["kalshi"] = KalshiMarketData("kalshi", params, venue=v).run(sink, stop)
    for name, src in built.items():
        jobs[name] = src.run(sink, stop)  # the sink keeps raw records and drops parsed events
    if not jobs:
        die("nothing to record")

    async def housekeeping() -> None:
        """The health file every 10 s, progress every minute, retention at start and daily."""
        from pmx.ops.health import recorder_health, write

        next_retention = next_progress = 0.0
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_retention:
                gone = retention.apply(retention.plan(root, keep_days, datetime.now(UTC).date()))
                if gone:
                    log.info("retention: deleted %d compacted raw days older than %d days", gone, keep_days)
                next_retention = now + 86_400
            if now >= next_progress:
                log.info("recorded so far: %s", ", ".join(f"{k} {n}" for k, n in sink.records.items()) or "nothing")
                next_progress = now + 60
            write(root / "health-record.json", recorder_health(sink, root))
            try:
                await asyncio.wait_for(stop.wait(), timeout=10)
            except TimeoutError:
                pass

    from pmx.ops.awake import on_battery, stay_awake

    awake = stay_awake()  # a sleeping Mac records nothing
    if on_battery():
        log.warning("on battery: the Mac may still sleep (closing the lid, low power) and leave gaps")
    log.info("recording %s into %s", ", ".join(jobs), root / "raw")
    try:
        results = await asyncio.gather(*jobs.values(), housekeeping(), return_exceptions=True)
        for name, r in zip(jobs, results, strict=False):
            if isinstance(r, BaseException):
                log.error("%s recorder stopped: %r", name, r)
    finally:
        sink.close()
        if awake is not None:
            awake.terminate()
        if v is not None:
            await v.aclose()
        log.info("stopped; recorded %s", ", ".join(f"{k} {n}" for k, n in sink.records.items()) or "nothing")


def _counts(root: Path, day: date) -> None:
    """Records per source and kind for one UTC day: the quick check that a recording worked."""
    from collections import Counter

    from pmx.recorder.rawlog import iter_records, log_files, sources

    for source in sources(root):
        files = log_files(root, source, day, day)
        if not files:
            continue
        kinds: Counter[str] = Counter()
        first = last = 0
        for ns, kind, _ in iter_records(files):
            kinds[kind] += 1
            first, last = first or ns, ns
        size = sum(f.stat().st_size for f in files) / 1e6
        span = f"{datetime.fromtimestamp(first / 1e9, UTC):%H:%M}–{datetime.fromtimestamp(last / 1e9, UTC):%H:%M}Z"
        by_kind = ", ".join(f"{k} {n:,}" for k, n in sorted(kinds.items()))
        total = sum(kinds.values())
        print(f"{source:12s} {total:>9,} records ({by_kind})  {span}  {size:.1f} MB in {len(files)} files")


def cmd_compact(args: argparse.Namespace) -> None:
    from pmx.connectors.kalshi.decode import KalshiDecoder
    from pmx.recorder.compact import compact_day

    root = config.data_root()
    if args.day:
        days = [date.fromisoformat(args.day)]
    else:
        raw = root / "raw" / "kalshi"
        today = datetime.now(UTC).date()
        days = [date.fromisoformat(p.name) for p in sorted(raw.glob("*")) if p.is_dir()]
        days = [d for d in days if d < today]
        if not days:
            print("no complete UTC days recorded yet (pass --day to compact a partial one)")
    for d in days:
        counts = compact_day(root, "kalshi", d, KalshiDecoder())
        print(f"{d}: {counts}")


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    r = sub.add_parser("record", parents=[common], help="record raw data: the venue plus configured sources")
    r.add_argument("--sources", help="comma-separated names: kalshi and the [[source]]s in the config (default all)")
    r.add_argument("--config")
    r.add_argument("--dry-run-retention", action="store_true", help="list the raw days retention would delete")
    r.add_argument("--counts", metavar="DAY", nargs="?", const="today",
                   help="print records per source for a UTC day (default today) and exit")
    r.add_argument("--series", nargs="+", help="override the configured series")

    c = sub.add_parser("compact", parents=[common], help="raw logs → parquet research tables (data/parquet/)")
    c.add_argument("--day", help="UTC day (default: every complete day on disk)")
    return {"record": cmd_record, "compact": cmd_compact}
