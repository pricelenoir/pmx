"""Append-only raw capture: zstd-compressed lines `recv_ns \\t kind \\t payload`.

kind  f = source frame (verbatim: a venue's websocket JSON, or an HTTP poll's envelope)
      c = local control record (connect, gap, sleep_gap, redacted; universe: every contract's `m` follows)
      m = contract metadata
      e = an engine event replays must reproduce (advice, data fetched at runtime), type-tagged JSON
Layout  <root>/raw/<source>/<YYYY-MM-DD>/<HH>-<start_s>.log.zst  (UTC; one file per
hour per process, so a crash never leaves a half-written frame in front of new data).
Frames are flushed as complete zstd frames every `flush_s`, so a killed process
loses at most that window.
"""

from __future__ import annotations

import io
import json
import logging
import os
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import msgspec
import zstandard

from pmx.connectors.base import Control, Frame, StreamItem
from pmx.core import types as core_types
from pmx.core.types import Contract
from pmx.engine import events as engine_events

log = logging.getLogger(__name__)

type Record = tuple[int, str, str]  # (recv_ns, kind, payload)

_enc = msgspec.json.Encoder()


class RawLogWriter:
    def __init__(self, root: Path, source: str, *, flush_s: float = 5.0, level: int = 3) -> None:
        self.dir = Path(root) / "raw" / source
        self.source = source
        self.flush_s = flush_s
        self._cctx = zstandard.ZstdCompressor(level=level)
        self._hour: str | None = None
        self._fh: io.BufferedWriter | None = None
        self._zw: zstandard.ZstdCompressionWriter | None = None
        self._last_flush = time.monotonic()
        self.records = 0
        self.last_ns = 0  # receive time of the newest record

    def _path_for(self, recv_ns: int) -> tuple[str, Path]:
        dt = datetime.fromtimestamp(recv_ns / 1e9, UTC)
        hour = dt.strftime("%Y-%m-%d/%H")
        return hour, self.dir / dt.strftime("%Y-%m-%d") / f"{dt:%H}-{int(time.time())}.log.zst"

    def _roll(self, recv_ns: int) -> None:
        hour, path = self._path_for(recv_ns)
        if hour == self._hour:
            return
        self.close()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "ab")  # noqa: SIM115 - owned and closed by close()
        self._zw = self._cctx.stream_writer(self._fh, closefd=False)
        self._hour = hour

    def write_line(self, recv_ns: int, kind: str, payload: str) -> None:
        self._roll(recv_ns)
        assert self._zw is not None
        payload = payload.replace("\n", " ").replace("\r", " ")  # JSON strings can't hold raw newlines
        self._zw.write(f"{recv_ns}\t{kind}\t{payload}\n".encode())
        self.records += 1
        self.last_ns = recv_ns
        if time.monotonic() - self._last_flush >= self.flush_s:
            self.flush()

    def write(self, item: StreamItem) -> None:
        if isinstance(item, Frame):
            self.write_line(item.recv_ns, "f", item.text)
        else:
            self.write_line(item.recv_ns, "c", _enc.encode({"event": item.event, **item.detail}).decode())

    def write_contracts(self, contracts: list[Contract], recv_ns: int | None = None) -> None:
        ns = recv_ns or time.time_ns()
        for c in contracts:
            self.write_line(ns, "m", _enc.encode(c).decode())

    def flush(self) -> None:
        if self._zw is not None and self._fh is not None:
            self._zw.flush(zstandard.FLUSH_FRAME)
            self._fh.flush()
            os.fsync(self._fh.fileno())
        self._last_flush = time.monotonic()

    def close(self) -> None:
        if self._zw is not None and self._fh is not None:
            self.flush()
            self._zw.close()
            self._fh.close()
        self._zw = self._fh = None
        self._hour = None


def sources(root: Path) -> list[str]:
    """Every source with a raw log."""
    base = Path(root) / "raw"
    return sorted(p.name for p in base.iterdir() if p.is_dir()) if base.is_dir() else []


def log_files(root: Path, source: str, start: date | None = None, end: date | None = None) -> list[Path]:
    """Raw files for `source` with start <= day <= end, in time order."""
    base = Path(root) / "raw" / source
    out = []
    for day_dir in sorted(p for p in base.glob("*") if p.is_dir()):
        d = date.fromisoformat(day_dir.name)
        if (start and d < start) or (end and d > end):
            continue
        out += sorted(day_dir.glob("*.log.zst"), key=lambda p: (p.name.split("-")[0], int(p.name.split("-")[1][:-8])))
    return out


def read_records(path: Path) -> Iterator[Record]:
    """Lines of one file. A truncated final frame (killed writer) ends the file quietly."""
    dctx = zstandard.ZstdDecompressor()
    with open(path, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as zr:
        text = io.TextIOWrapper(zr, encoding="utf-8")
        try:
            for line in text:
                if not line.endswith("\n"):
                    break
                ns, kind, payload = line[:-1].split("\t", 2)
                yield int(ns), kind, payload
        except zstandard.ZstdError as e:
            log.warning("%s: truncated (%s)", path, e)


def iter_records(paths: list[Path]) -> Iterator[Record]:
    for p in paths:
        yield from read_records(p)


def envelope(url: str, status: int, ms: int, body: str) -> str:
    """An HTTP poll's response as one `f` line, `{"url", "status", "ms", "body"}`: a JSON body is
    spliced in verbatim, anything else (an error page) goes in as a string."""
    text = body.strip()
    try:
        json.loads(text)
    except ValueError:
        text = json.dumps(text)
    return f'{{"url":{json.dumps(url)},"status":{status},"ms":{ms},"body":{text}}}'


_ctl = msgspec.json.Decoder(dict)


def to_stream_item(rec: Record) -> StreamItem | None:
    ns, kind, payload = rec
    if kind == "f":
        return Frame(ns, payload)
    if kind == "c":
        d = _ctl.decode(payload)
        return Control(ns, d.pop("event"), d)
    return None


_contract = msgspec.json.Decoder(Contract)


def decode_contract(payload: str) -> Contract:
    return _contract.decode(payload)


# ---- `e` records: engine events, tagged with their type's name ----------------------------------

EVENT_TYPES: dict[str, type] = {
    t.__name__: t
    for mod in (core_types, engine_events)
    for t in vars(mod).values()
    if isinstance(t, type) and issubclass(t, msgspec.Struct) and t.__module__ == mod.__name__
}


def _qualname(t: type) -> str:
    return f"{t.__module__}.{t.__qualname__}"


def register_event_type(cls: type) -> type:
    """Domain events (e.g. NFL `GameState`) that may be recorded as `e`; usable as a decorator."""
    prev = EVENT_TYPES.get(cls.__name__)
    if prev is not None and _qualname(prev) != _qualname(cls):
        raise ValueError(f"event type name {cls.__name__!r} is taken by {_qualname(prev)}")
    EVENT_TYPES[cls.__name__] = cls
    return cls


def encode_event(ev: msgspec.Struct) -> str:
    name = type(ev).__name__
    known = EVENT_TYPES.get(name)
    if known is None or _qualname(known) != _qualname(type(ev)):
        raise ValueError(f"{name} isn't a registered event type (rawlog.register_event_type)")
    return _enc.encode({"type": name, "event": ev}).decode()


_tagged = msgspec.json.Decoder(dict)


def decode_event(payload: str) -> msgspec.Struct:
    d = _tagged.decode(payload)
    cls = EVENT_TYPES.get(d.get("type", ""))
    if cls is None:
        raise ValueError(f"unknown recorded event type {d.get('type')!r}")
    return msgspec.convert(d["event"], cls)
