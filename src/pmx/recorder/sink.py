"""`RecordingSink`: raw capture for any number of sources, one `RawLogWriter` per source name
(`raw/<source>/…`). Implements `Sink.record(source, kind, payload)` (ARCHITECTURE *Sources*), so the
recorder and `pmx run` write the same files; as the recorder's whole `Sink` it drops emitted events.

Two guards on the way in: a payload that looks like a credential is replaced by a placeholder
(sources must never hand one over; this is defense in depth), and a wall-clock jump the monotonic
clock didn't see (the machine slept) writes a `sleep_gap` control record to every open log."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from pathlib import Path

import msgspec

from pmx.connectors.base import StreamItem
from pmx.core.types import Contract
from pmx.engine.events import SourceStatus
from pmx.recorder.rawlog import RawLogWriter, encode_event

log = logging.getLogger(__name__)

_SECRET = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----"
    r"|KALSHI-ACCESS-(?:KEY|SIGNATURE|TIMESTAMP)"
    r"|\bBearer\s+[A-Za-z0-9._~+/=-]{16,}"
    r"|[\"']?\bauthorization[\"']?\s*[:=]"
    r"|[?&](?:api_?key|access_token|token|signature|secret)=[^&\s\"']{8,}",
    re.IGNORECASE,
)


def looks_secret(payload: str) -> str | None:
    """The suspicious fragment if `payload` looks like it carries a credential, else None."""
    m = _SECRET.search(payload)
    return None if m is None else m.group(0)[:24]


_enc = msgspec.json.Encoder()


class RecordingSink:
    sleep_gap_ms = 5_000

    def __init__(self, root: Path, *, flush_s: float = 5.0, wall_ns: Callable[[], int] = time.time_ns,
                 mono_ns: Callable[[], int] = time.monotonic_ns) -> None:
        self.root, self.flush_s = Path(root), flush_s
        self.writers: dict[str, RawLogWriter] = {}
        self.redacted = 0
        self.emitted = 0
        self.sleep_gaps = 0
        self._wall, self._mono = wall_ns, mono_ns
        self._last: tuple[int, int] | None = None

    def writer(self, source: str) -> RawLogWriter:
        w = self.writers.get(source)
        if w is None:
            w = self.writers[source] = RawLogWriter(self.root, source, flush_s=self.flush_s)
        return w

    def now_ns(self) -> int:
        """Wall time for the next record, after checking for a sleep since the last one."""
        wall, mono = self._wall(), self._mono()
        last, self._last = self._last, (wall, mono)
        if last is not None:
            slept_ms = ((wall - last[0]) - (mono - last[1])) // 1_000_000
            if slept_ms > self.sleep_gap_ms:
                self.sleep_gaps += 1
                log.warning("wall clock jumped %.1f s past the monotonic clock (likely a sleep)", slept_ms / 1000)
                payload = _enc.encode({"event": "sleep_gap", "gap_ms": slept_ms}).decode()
                for w in self.writers.values():
                    w.write_line(wall, "c", payload)
        return wall

    def record(self, source: str, kind: str, payload: str, recv_ns: int | None = None) -> None:
        ns = self.now_ns() if recv_ns is None else recv_ns
        hit = looks_secret(payload)
        if hit is not None:
            self.redacted += 1
            log.error("refused to record a %s payload that looks like a credential (%r…)", source, hit)
            payload = _enc.encode({"event": "redacted", "kind": kind, "bytes": len(payload)}).decode()
            kind = "c"
        self.writer(source).write_line(ns, kind, payload)

    def emit(self, ev: object) -> None:
        """`Sink.emit`: a recorder keeps only what sources record raw, so parsed events are dropped —
        except that a source's health goes to the log."""
        self.emitted += 1
        if type(ev) is SourceStatus:
            log.log(logging.INFO if ev.connected else logging.WARNING, "%s: %s", ev.source, ev.detail)

    def record_event(self, source: str, ev: msgspec.Struct) -> None:
        """An engine event replays must reproduce (advice, data fetched at runtime): kind `e`."""
        self.record(source, "e", encode_event(ev))

    def write(self, source: str, item: StreamItem) -> None:
        """A venue stream item (frame or control) as received; `recv_ns` is the stream's own stamp."""
        self.now_ns()
        self.writer(source).write(item)

    def write_contracts(self, source: str, contracts: list[Contract]) -> None:
        self.writer(source).write_contracts(contracts, self.now_ns())

    @property
    def records(self) -> dict[str, int]:
        return {name: w.records for name, w in self.writers.items()}

    def flush(self) -> None:
        for w in self.writers.values():
            w.flush()

    def close(self) -> None:
        for w in self.writers.values():
            w.close()
