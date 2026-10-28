"""Health files: `pmx run` rewrites `data/health.json` and `pmx record` `data/health-record.json` every
`health_s` — what `pmx status` shows when the engine doesn't answer, and what a glance or a script reads."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from pmx.ops.alerts import disk_free_gb


def health_path(sock: Path) -> Path:
    """data/pmx.sock -> data/health.json; another socket gets its own (data/pmx-demo.sock -> data/health-demo.json)."""
    stem = sock.stem.removeprefix("pmx")
    return sock.with_name(f"health{stem}.json" if not stem or stem.startswith("-") else f"health-{sock.stem}.json")


def write(path: Path, body: dict[str, Any]) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(body, indent=1, sort_keys=True))
    os.replace(tmp, path)


def read(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return None


def engine_health(system: Any) -> dict[str, Any]:
    """`pmx status`'s answer plus what only the file says: per-source queues and last event, disk, the feed link."""
    body = system.control.status()
    for name, sink in system.engine.sources.items():
        src = body["sources"].setdefault(name, {"connected": None, "detail": ""})
        src |= {"pending": sink.pending, "overflows": sink.overflows, "last_ms": sink.last_ms}
    link = system.link
    body |= {"kind": "run", "pid": os.getpid(), "ts_ms": int(time.time() * 1000),
             "disk_free_gb": round(disk_free_gb(system.data), 1),
             "feed": {"up": link.up, "dropped": link.total_dropped(), "resyncs": link.resyncs,
                      "restarts": link.restarts}}
    return body


def recorder_health(sink: Any, data: Path) -> dict[str, Any]:
    return {"kind": "record", "pid": os.getpid(), "ts_ms": int(time.time() * 1000),
            "sources": {name: {"records": w.records, "last_ms": w.last_ns // 1_000_000 if w.last_ns else None}
                        for name, w in sink.writers.items()},
            "sleep_gaps": sink.sleep_gaps, "redacted": sink.redacted,
            "disk_free_gb": round(disk_free_gb(data), 1)}
