"""Recorded raw logs as a backtest's sources. Each source plugin says what it records and with which decoder a
replay reads it (`raw_logs(name) -> {raw source: decoder class}`; without one, a source's raw log holds only
contract metadata and engine events). Replays run the live decoders, so a backtest sees what the live engine saw
(invariant 3). Venue market data starts with the contracts open before the first day — metadata is written when the
recorder discovers or lists markets, not every day — as a live engine started that day would discover them."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pmx import plugins
from pmx.core.types import Contract
from pmx.engine.config import RunConfig
from pmx.recorder.rawlog import decode_contract, iter_records, log_files
from pmx.recorder.replay import replay
from pmx.sources.kalshi import KalshiMarketData

ADVICE = "advisors"  # recorded advice replays as it arrived
PRIME_DAYS = 14  # how far before the first day to look for contract metadata


class RawLogSource:
    """One raw source's records under `root/raw/<raw>/`, for UTC days `start`..`end`, decoded by `decoder`: `events()`
    yields (receive ms, event). `name` is the engine source the events come from (what strategies list in
    `inputs`). With `prime`, the contracts still open before `start` (`primed()`) come first, at the first record's
    time."""

    def __init__(self, raw: str, root: Path, start: date, end: date, decoder: Any = None, *,
                 name: str | None = None, prime: bool = False) -> None:
        self.raw, self.root, self.start, self.end, self.decoder = raw, root, start, end, decoder
        self.name = name or raw
        self.prime = prime
        self.count = 0

    def __repr__(self) -> str:
        return f"RawLogSource({self.raw!r} as {self.name!r}, {self.start}..{self.end})"

    def events(self) -> Iterator[tuple[int, Any]]:
        known = getattr(self.decoder, "tickers", None)  # a decoder that filters to the markets it has metadata for
        primed = self.primed() if self.prime else []
        for recv_ns, ev in replay(self.root, self.raw, self.decoder, self.start, self.end):
            for c in primed:
                if known is not None:
                    known.add(c.ticker)
                self.count += 1
                yield recv_ns // 1_000_000, c
            primed = []
            if known is not None and type(ev) is Contract:
                known.add(ev.ticker)
            self.count += 1
            yield recv_ns // 1_000_000, ev

    def primed(self) -> list[Contract]:
        """Open contracts by ticker, from each one's newest metadata before `start`: the days before, newest first,
        back to one with a whole-universe listing (or `PRIME_DAYS`)."""
        latest: dict[str, Contract] = {}
        for back in range(1, PRIME_DAYS + 1):
            day = self.start - timedelta(days=back)
            found: dict[str, Contract] = {}
            listed = False
            for _, kind, payload in iter_records(log_files(self.root, self.raw, day, day)):
                if kind == "m":
                    c = decode_contract(payload)
                    found[c.ticker] = c
                elif kind == "c" and '"universe"' in payload:
                    listed = True
            for t, c in found.items():
                latest.setdefault(t, c)  # a later day's metadata wins
            if listed:
                break
        return [latest[t] for t in sorted(latest) if latest[t].status == "open"]


def raw_logs(kind: str, name: str) -> dict[str, Any]:
    """{raw source: decoder class | None} for a source plugin of `kind` named `name`."""
    cls = plugins.SOURCES.get(kind)
    fn = getattr(cls, "raw_logs", None)
    return fn(name) if fn is not None else {name: None}


def sources_for(cfg: RunConfig, root: Path, start: date, end: date, *,
                only: Iterable[str] | None = None) -> list[RawLogSource]:
    """What the backtest of `cfg` replays: the Kalshi market data (websocket or REST poll recordings), every
    `[[source]]`, and recorded advice — those with logs in the range, limited to engine sources named in `only`.
    Models aren't replayed: they run again, inline."""
    wanted = None if only is None else set(only)
    kalshi = KalshiMarketData.raw_logs("kalshi")
    plan: list[tuple[str, str, Any]] = [(raw, "kalshi", dec) for raw, dec in kalshi.items()]
    for s in cfg.sources:
        plan += [(raw, s.name, dec) for raw, dec in raw_logs(s.kind, s.name).items()]
    plan.append((ADVICE, ADVICE, None))
    out = []
    for raw, name, dec in plan:
        if wanted is not None and name not in wanted:
            continue
        if log_files(root, raw, start, end):
            out.append(RawLogSource(raw, root, start, end, dec() if dec is not None else None, name=name,
                                    prime=raw in kalshi))
    return out
