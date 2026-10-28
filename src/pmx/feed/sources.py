"""Where `pmx serve` gets its data: Kalshi through the engine (the websocket with an API key, else REST
polling) and on to the feed process, or a recorded day replayed into a hub in this process."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any, Literal

from pmx.connectors.base import Decoder
from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.feed import proto
from pmx.feed.hub import FeedHub
from pmx.recorder.daemon import RecorderConfig
from pmx.recorder.replay import replay

log = logging.getLogger(__name__)


async def run_market_feed(venue: KalshiVenue, cfg: RecorderConfig, sock: Path, hello: proto.Hello,
                          stop: asyncio.Event, *, mode: Literal["live", "poll"], trades_s: float = 2.0,
                          recorder: Any = None, sources: Sequence[Any] = (), models: Sequence[Any] = (),
                          linkers: Mapping[str, Any] | None = None) -> None:
    """The market-data service behind `pmx serve`: the engine with no strategies, fed by the Kalshi market
    source (plus the account source with a key), a timer, and any other `sources` (outside data, linked
    to markets by `linkers`), running `models`, publishing through the tap to the feed process, which
    serves viewers on `sock`."""
    from pmx.engine.clock import LiveClock
    from pmx.engine.engine import Engine, Tap
    from pmx.engine.state import State
    from pmx.feed.server import EngineLink, TapPublisher, market_snapshot
    from pmx.sources.kalshi import KalshiAccount, KalshiMarketData
    from pmx.sources.timer import TimerSource

    state = State(linkers)
    clock = LiveClock()
    link = EngineLink(sock, hello, snapshot=lambda: market_snapshot(state), clock=clock.now_ms,
                      tickers_of=lambda entity: state.tickers_of.get(entity, ()))
    tap = link.tap = Tap(link.deliver)
    if models:
        TapPublisher(tap, clock.now_ms).set_models([m.name for m in models])
    engine = Engine(clock, state, models=models, publisher=tap, recorder=recorder)
    params = KalshiMarketData.Params(series=cfg.series, tickers=cfg.tickers, refresh_s=cfg.refresh_s, mode=mode,
                                     trades_s=trades_s)
    market = KalshiMarketData(venue.name, params, venue=venue)
    engine.on_sleep_gap.append(lambda gap_ms: market.resync())
    work = [engine.run(stop), tap.run(stop), link.run(stop),
            market.run(engine.source(venue.name, max=100_000, policy="reset", venue=venue.name), stop),
            TimerSource("timer").run(engine.source("timer", policy="drop_oldest"), stop)]
    if venue.signer is not None:
        work.append(KalshiAccount("account", venue=venue).run(engine.source("account", policy="drop_oldest"), stop))
    work += [src.run(engine.source(src.name), stop) for src in sources]
    log.info("market feed: %s, %d series, %d tickers", mode, len(cfg.series), len(cfg.tickers))
    await asyncio.gather(*work)


async def run_replay(
    root: Path, venue: str, decoder: Decoder, day: date, hub: FeedHub, stop: asyncio.Event, *, speed: float = 1.0
) -> None:
    """Recorded `day` through the live decoder, paced at `speed`x (0 = as fast as possible), conflated
    by recorded receive time like a live feed."""
    from pmx.feed.publisher import Publisher

    pub = Publisher(hub)
    hub.set_status(True, f"replaying {day} at {speed:g}x")
    prev: int | None = None
    for n, (recv_ns, ev) in enumerate(replay(root, venue, decoder, day, day)):
        if stop.is_set():
            return
        gap = (recv_ns - prev) / 1e9 / speed if prev is not None and speed > 0 else 0.0
        prev = recv_ns
        if gap > 0.001:
            await asyncio.sleep(min(gap, 5.0))
        elif n % 500 == 0:
            await asyncio.sleep(0)
        pub.on_event(ev, recv_ns // 1_000_000)
    if prev is not None:
        pub.flush(prev // 1_000_000 + 60_000)
    hub.set_status(False, f"replay of {day} finished")
    await stop.wait()
