"""Replay recorded sources through their live decoders: one source in recorded order, or several merged
by receive time, so a session replays in the order it was seen."""

from __future__ import annotations

import heapq
from collections.abc import Iterator, Mapping
from datetime import date
from pathlib import Path

from pmx.connectors.base import Decoder
from pmx.recorder.rawlog import Record, decode_contract, decode_event, iter_records, log_files, to_stream_item


def _decode(rec: Record, decoder: Decoder | None) -> list:
    ns, kind, payload = rec
    if kind == "m":
        return [decode_contract(payload)]
    if kind == "e":
        return [decode_event(payload)]
    item = to_stream_item(rec)
    return [] if item is None or decoder is None else decoder(item)


def replay(root: Path, source: str, decoder: Decoder, start: date | None = None,
           end: date | None = None) -> Iterator[tuple[int, object]]:
    """(recv_ns, event) for one source in recorded order. Contract metadata records come through as
    `Contract`s, `e` records as the events they hold."""
    for rec in iter_records(log_files(root, source, start, end)):
        for ev in _decode(rec, decoder):
            yield rec[0], ev


def replay_merged(root: Path, decoders: Mapping[str, Decoder | None], start: date | None = None,
                  end: date | None = None) -> Iterator[tuple[int, str, object]]:
    """(recv_ns, source, event) across sources, merged by receive time; ties keep `decoders` order.
    A source mapped to None contributes only its `m` and `e` records."""
    def tagged(source: str) -> Iterator[tuple[int, str, Record]]:
        for rec in iter_records(log_files(root, source, start, end)):
            yield rec[0], source, rec

    for ns, source, rec in heapq.merge(*(tagged(s) for s in decoders), key=lambda r: r[0]):
        for ev in _decode(rec, decoders[source]):
            yield ns, source, ev
