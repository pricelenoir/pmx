"""The venue universe: every open market in the configured series, plus explicit tickers. `pmx record`
and the `kalshi_market` source re-discover it every `refresh_s` seconds."""

from __future__ import annotations

import logging

import msgspec

from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.core.types import Contract

log = logging.getLogger(__name__)


class RecorderConfig(msgspec.Struct, frozen=True):
    series: list[str]
    tickers: list[str] = []
    refresh_s: float = 300.0


async def discover(venue: KalshiVenue, cfg: RecorderConfig) -> dict[str, Contract]:
    found: dict[str, Contract] = {}
    for s in cfg.series:
        for c in await venue.list_contracts(series=s, status="open"):
            found[c.ticker] = c
    if cfg.tickers:
        for c in await venue.contracts_by_ticker(cfg.tickers):
            if c.status == "open":
                found[c.ticker] = c
    return found
