"""Kalshi sources (ARCHITECTURE *Sources*).

`kalshi_market` — contracts, books, and prints for a universe of series/tickers. With an API key it
streams the websocket (every delta; raw source `kalshi`, frames and control records exactly as
`pmx record` has always written them). Without one it polls over REST (raw source `kalshi_rest`): every
book round-robin as a snapshot, the public tape every few seconds. Contracts carry their series' fee
schedule and rules, and the universe is refreshed periodically (new contracts join, closed ones leave
as `Contract` updates). The whole universe's metadata is recorded at start and again on each new UTC day,
after a `universe` control record, so every day's log lists its contracts. Never raises: problems become
`SourceStatus` events.

`kalshi_account` — our fills and order updates (websocket `fill` and `user_orders` channels) and balance +
positions (`AccountSnapshot`); needs an API key. Each (re)connect of its websocket runs `on_connect` hooks (the
live broker reconciles)."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import statistics
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal

import httpx
import msgspec

from pmx.connectors.base import Frame
from pmx.connectors.kalshi import wire
from pmx.connectors.kalshi.config import KalshiConfig
from pmx.connectors.kalshi.decode import KalshiDecoder, KalshiRestDecoder
from pmx.connectors.kalshi.rest import KalshiError
from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.core.types import Contract, Fill, VenueOrder
from pmx.engine.events import AccountSnapshot, Event, SourceStatus
from pmx.plugins import source
from pmx.recorder.daemon import RecorderConfig, discover
from pmx.recorder.rawlog import envelope
from pmx.sources.base import Sink, SourceEnv, sleep_or_stop

log = logging.getLogger(__name__)

RAW = "kalshi"  # websocket frames
RAW_REST = "kalshi_rest"  # REST poll envelopes
_enc = msgspec.json.Encoder()
_book = msgspec.json.Decoder(wire.WireOrderbookResponse)
_tape = msgspec.json.Decoder(wire.WireTradesPage)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_day() -> date:
    return datetime.now(UTC).date()


def _venue(env: SourceEnv | None) -> KalshiVenue:
    env = env or SourceEnv()
    v = env.venues.get("kalshi")
    return v if v is not None else KalshiVenue(KalshiConfig.from_env(env.env))  # type: ignore[arg-type]


class _Health:
    """`SourceStatus` when connectivity changes, and for a changed detail at most every `every_s`, so
    figures like the poll cycle or the skew don't flood the engine."""

    def __init__(self, name: str, sink: Sink, every_s: float = 60.0) -> None:
        self.name, self.sink, self.every_s = name, sink, every_s
        self.state: tuple[bool, str] | None = None
        self._at = 0.0

    def __call__(self, connected: bool, detail: str) -> None:
        state = self.state
        if (connected, detail) == state:
            return
        now = time.monotonic()
        if state is not None and state[0] == connected and now - self._at < self.every_s:
            return
        self.state, self._at = (connected, detail), now
        self.sink.emit(SourceStatus(self.name, connected, detail, _now_ms()))


def offset_bounds(samples: list[tuple[float, float, int]]) -> tuple[float, float] | None:
    """Server-minus-local clock offset bounds (ms) from HTTP `Date` headers, which have 1 s resolution:
    each (local send ms, local receive ms, server second) says the offset is in
    [S·1000 − receive, S·1000 + 1000 − send]; samples straddling a second boundary narrow it to about
    the round trip. None if the samples disagree."""
    lo, hi = float("-inf"), float("inf")
    for sent, recv, server_s in samples:
        lo = max(lo, server_s * 1000 - recv)
        hi = min(hi, server_s * 1000 + 1000 - sent)
    return (lo, hi) if lo <= hi else None


@source("kalshi_market")
class KalshiMarketData:
    @staticmethod
    def raw_logs(name: str) -> dict[str, Any]:
        """What this source records, by raw source, and the decoder a replay reads it with: the websocket's
        frames, or REST poll envelopes without a key."""
        return {RAW: KalshiDecoder, RAW_REST: KalshiRestDecoder}

    class Params(msgspec.Struct, frozen=True):
        series: list[str] = []
        tickers: list[str] = []
        mode: Literal["auto", "live", "poll"] = "auto"  # auto: the websocket if an API key is configured
        refresh_s: float = 60.0  # universe re-discovery (contract status changes, new markets)
        trades_s: float = 2.0  # poll mode: the public tape every this often
        rps: float = 15.0  # poll mode: REST requests per second (unauthenticated reads 429 near 20)
        clock_check_s: float = 3_600.0  # 0 = never
        clock_warn_ms: float = 250.0

    def __init__(self, name: str, params: Params | None = None, env: SourceEnv | None = None, *,
                 venue: KalshiVenue | None = None, http: httpx.AsyncClient | None = None) -> None:
        self.name, self.p = name, params or self.Params()
        self.venue = venue or _venue(env)
        self._http = http  # clock checks; tests inject a fake
        self.universe: dict[str, Contract] = {}
        self.skews: deque[int] = deque(maxlen=1_000)  # receive ms − exchange ms
        self._resync = False
        self._stream: Any = None
        self._listed: date | None = None  # UTC day of the last whole-universe listing

    @property
    def live(self) -> bool:
        return self.p.mode == "live" or (self.p.mode == "auto" and self.venue.signer is not None)

    @property
    def skew_ms(self) -> int | None:
        """Median receive − exchange time over recent events (latency plus clock difference)."""
        return round(statistics.median(self.skews)) if self.skews else None

    def resync(self) -> None:
        """Re-snapshot every book: after a queue overflow, a sequence gap the venue missed, a sleep."""
        self._resync = True

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        health = _Health(self.name, sink)
        hooks = getattr(sink, "on_overflow", None)
        if isinstance(hooks, list):
            hooks.append(self.resync)
        if self.p.mode == "live" and self.venue.signer is None:
            health(False, "the websocket needs an API key (pmx keygen, then pmx setkey)")
            await stop.wait()
            return
        cfg = RecorderConfig(series=self.p.series, tickers=self.p.tickers, refresh_s=self.p.refresh_s)
        raw = RAW if self.live else RAW_REST
        health(False, "discovering markets")
        while not stop.is_set():
            try:
                self.universe = await discover(self.venue, cfg)
                break
            except (httpx.HTTPError, KalshiError) as e:
                health(False, f"market discovery failed: {e}")
                if await sleep_or_stop(stop, 10):
                    return
        self._listing(sink, raw, len(self.universe))
        self._contracts(sink, raw, list(self.universe.values()))
        work = [self._live(sink, health, stop) if self.live else self._poll(sink, health, stop),
                self._refresh(sink, raw, cfg, stop)]
        if self.p.clock_check_s > 0:
            work.append(self._clock_check(health, stop))
        await asyncio.gather(*(self._guard(w, health, stop) for w in work))

    async def _guard(self, work: Any, health: _Health, stop: asyncio.Event) -> None:
        """Fail soft: a bug in one loop is reported and the loop isn't retried, but `run` never raises."""
        try:
            await work
        except Exception as e:
            log.exception("%s failed", self.name)
            health(False, f"stopped: {type(e).__name__}: {e}")
            await stop.wait()

    def _contracts(self, sink: Sink, raw: str, contracts: list[Contract]) -> None:
        for c in contracts:
            sink.record(raw, "m", _enc.encode(c).decode())
            sink.emit(c)

    def _listing(self, sink: Sink, raw: str, n: int) -> None:
        """Marks that all `n` contracts' metadata follows: a backtest of the day needs no earlier day."""
        sink.record(raw, "c", _enc.encode({"event": "universe", "contracts": n}).decode())
        self._listed = _utc_day()

    def _emit(self, sink: Sink, ev: Event, recv_ms: int) -> None:
        ts = getattr(ev, "ts_ms", 0)
        if ts and not isinstance(ev, Contract):
            self.skews.append(recv_ms - ts)
        sink.emit(ev)

    # ---- websocket -------------------------------------------------------------------------------

    async def _live(self, sink: Sink, health: _Health, stop: asyncio.Event) -> None:
        self._stream = stream = self.venue.stream(self.universe, trades=True)
        decode = self.venue.decoder()
        stopping = asyncio.ensure_future(stop.wait())
        stopping.add_done_callback(lambda _: asyncio.ensure_future(stream.close()))
        try:
            async for item in stream:
                if isinstance(item, Frame):
                    sink.record(RAW, "f", item.text)
                else:
                    sink.record(RAW, "c", _enc.encode({"event": item.event, **item.detail}).decode())
                    detail = f"websocket {item.event}" + (f": {item.detail}" if item.detail else "")
                    health(item.event == "connected", detail)
                try:
                    events = decode(item)
                except Exception as e:  # schema drift: keep recording raw, say so
                    log.exception("kalshi frame didn't decode")
                    health(False, f"parse error: {type(e).__name__}: {e}")
                    continue
                recv_ms = item.recv_ns // 1_000_000
                for ev in events:
                    self._emit(sink, ev, recv_ms)
                if self._resync:
                    self._resync = False
                    await stream.reconnect()
        finally:
            stopping.cancel()

    # ---- REST poll -------------------------------------------------------------------------------

    async def _poll(self, sink: Sink, health: _Health, stop: asyncio.Event) -> None:
        rest = self.venue.rest
        seen: dict[str, bytes] = {}  # path -> digest of its last body
        decode = KalshiRestDecoder(lambda t: t in self.universe)  # what replays of `kalshi_rest` run too
        since_s = int(time.time()) - 60
        next_at = time.monotonic()
        last_tape = 0.0

        async def get(path: str, params: dict) -> bytes | None:
            nonlocal next_at
            wait = next_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            next_at = max(next_at, time.monotonic() - 1) + 1 / self.p.rps
            url = path + "?" + "&".join(f"{k}={v}" for k, v in params.items())
            try:
                body = await rest.get(path, params)
            except KalshiError as e:
                sink.record(RAW_REST, "f", envelope(url, e.status, _now_ms(), e.body))
                return None
            digest = hashlib.blake2b(body, digest_size=16).digest()
            if seen.get(path) == digest:
                return None  # unchanged since the last poll
            seen[path] = digest
            sink.record(RAW_REST, "f", envelope(url, 200, _now_ms(), body.decode()))
            return body

        while not stop.is_set():
            t0 = time.monotonic()
            try:
                for ticker in list(self.universe):
                    if stop.is_set():
                        return
                    path = f"/markets/{ticker}/orderbook"
                    body = await get(path, {"depth": 0})
                    if body is not None:
                        now = _now_ms()
                        for ev in decode.on_response(path, 200, now, body):
                            self._emit(sink, ev, now)
                    if time.monotonic() - last_tape >= self.p.trades_s:
                        last_tape = time.monotonic()
                        tape = await get("/markets/trades", {"limit": 1000, "min_ts": since_s})
                        if tape is not None:
                            now = _now_ms()
                            trades = decode.on_response("/markets/trades", 200, now, tape)
                            for t in trades:
                                self._emit(sink, t, now)
                            newest = max((t.ts_ms for t in trades), default=since_s * 1000) // 1000
                            since_s = max(since_s, newest - 60)  # a minute of overlap; ids dedupe it
                cycle = time.monotonic() - t0
                skew = self.skew_ms
                health(True, f"REST poll (no API key): {len(self.universe)} books every {round(cycle):d}s"
                       + (f", tape skew {skew} ms" if skew is not None else ""))
            except (httpx.HTTPError, KalshiError, msgspec.DecodeError) as e:
                health(False, f"REST poll failed: {type(e).__name__}: {e}")
                if await sleep_or_stop(stop, 5):
                    return
            if not self.universe and await sleep_or_stop(stop, 30):
                return

    # ---- universe and clock ----------------------------------------------------------------------

    async def _refresh(self, sink: Sink, raw: str, cfg: RecorderConfig, stop: asyncio.Event) -> None:
        while not await sleep_or_stop(stop, self.p.refresh_s):
            try:
                fresh = await discover(self.venue, cfg)
                gone = sorted(self.universe.keys() - fresh.keys())
                if gone:  # closed or determined: their final metadata
                    self._contracts(sink, raw, await self.venue.contracts_by_ticker(gone))
                listing = _utc_day() != self._listed
                if listing:
                    self._listing(sink, raw, len(fresh))
                for t, c in fresh.items():
                    if self.universe.get(t) != c:
                        self._contracts(sink, raw, [c])
                    elif listing:  # unchanged: recorded for the day's listing, not emitted again
                        sink.record(raw, "m", _enc.encode(c).decode())
                if self._stream is not None:
                    await self._stream.remove(gone)
                    await self._stream.add(fresh.keys() - self.universe.keys())
                self.universe = fresh
            except (httpx.HTTPError, KalshiError) as e:
                log.warning("kalshi universe refresh failed, keeping the current set: %s", e)

    async def _clock_check(self, health: _Health, stop: asyncio.Event) -> None:
        """Compare our clock with Kalshi's (HTTP `Date`) at start and every `clock_check_s`."""
        http = self._http or httpx.AsyncClient(timeout=10)
        url = self.venue.cfg.rest_base + "/exchange/status"
        try:
            while not stop.is_set():
                samples = []
                for _ in range(8):  # spread over ~1 s so some straddle a second boundary
                    sent = time.time() * 1000
                    try:
                        r = await http.get(url)
                    except httpx.HTTPError:
                        break
                    recv = time.time() * 1000
                    date = r.headers.get("date")
                    if date:
                        samples.append((sent, recv, int(parsedate_to_datetime(date).timestamp())))
                    await asyncio.sleep(0.137)
                bounds = offset_bounds(samples) if samples else None
                if bounds is not None and (bounds[0] > self.p.clock_warn_ms or bounds[1] < -self.p.clock_warn_ms):
                    off = bounds[0] if bounds[0] > 0 else bounds[1]
                    health(True, f"local clock is {abs(off):.0f}+ ms {'behind' if off > 0 else 'ahead of'} Kalshi's")
                elif bounds is not None:
                    log.info("clock vs Kalshi: offset within [%.0f, %.0f] ms", *bounds)
                if await sleep_or_stop(stop, self.p.clock_check_s):
                    return
        finally:
            if self._http is None:
                await http.aclose()


@source("kalshi_account")
class KalshiAccount:
    class Params(msgspec.Struct, frozen=True):
        poll_s: float = 15.0  # balance and positions

    def __init__(self, name: str, params: Params | None = None, env: SourceEnv | None = None, *,
                 venue: KalshiVenue | None = None) -> None:
        self.name, self.p = name, params or self.Params()
        self.venue = venue or _venue(env)
        self.on_connect: list[Callable[[], None]] = []

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        health = _Health(self.name, sink)
        if self.venue.signer is None:
            health(False, "no API key: no fills or balance")
            await stop.wait()
            return
        await asyncio.gather(self._orders(sink, health, stop), self._balance(sink, health, stop))

    async def _orders(self, sink: Sink, health: _Health, stop: asyncio.Event) -> None:
        stream = self.venue.stream([], trades=False, fills=True, orders=True)
        decode = self.venue.decoder()
        stopping = asyncio.ensure_future(stop.wait())
        stopping.add_done_callback(lambda _: asyncio.ensure_future(stream.close()))
        try:
            async for item in stream:
                if isinstance(item, Frame):
                    sink.record(self.name, "f", item.text)
                else:
                    sink.record(self.name, "c", _enc.encode({"event": item.event, **item.detail}).decode())
                    if item.event == "connected":
                        for hook in self.on_connect:
                            hook()
                try:
                    for ev in decode(item):
                        if isinstance(ev, (Fill, VenueOrder)):  # no books here: its reconnects don't reset the venue's
                            sink.emit(ev)
                except Exception as e:
                    health(False, f"parse error: {type(e).__name__}: {e}")
        finally:
            stopping.cancel()

    async def _balance(self, sink: Sink, health: _Health, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                (balance, shards), positions = await self.venue.balances(), await self.venue.positions()
                sink.emit(AccountSnapshot(balance, positions, _now_ms(), shards))
                health(True, "ok")
            except (httpx.HTTPError, KalshiError) as e:
                health(False, f"account poll failed: {e}")
            if await sleep_or_stop(stop, self.p.poll_s):
                return
