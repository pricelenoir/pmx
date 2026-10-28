"""The feed process (ARCHITECTURE *Process model and the hot path*).

Viewer state, conflation, and fan-out run in their own process, so the engine's cost of being
watched stays a ring append plus one batched write however many viewers attach. The engine side,
`EngineLink`, is the `Tap`'s delivery: it starts the feed process as a child over a socketpair and
writes each batch to it without ever waiting. When the child is slow (its socket buffer is past
`high_water`) or down, batches are dropped and counted; once it catches up, or has been restarted,
the link discards what the tap still holds and sends a snapshot of current state instead, and
viewers resync. The child (`python -m pmx.feed.server`) decodes batches into the `Publisher` +
`FeedHub` and serves viewers on the feed socket. It exits when the engine goes away, so a dead
engine still reads as "feed down" to viewers.

Frames: a 4-byte big-endian length, then msgpack `(kind, dropped, {tag: [(seq, now_ms, obj), …]})`.
`kind` SNAPSHOT replaces the feed's state; `dropped` is the engine's running count of events that
never reached the feed; `tag` is an engine event type name, or "msg" for a feed message sent as
is — models, perf, equity history, statuses, and entity states already rendered for viewers, so
the feed process needs no domain code."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import socket
import struct
import sys
import time
from collections.abc import Callable, Iterable
from operator import itemgetter
from pathlib import Path
from typing import Any

import msgspec

from pmx.core.types import BookDelta, BookSnapshot, Contract, StreamReset, Trade
from pmx.engine.events import (
    AccountSnapshot,
    Advice,
    EntityState,
    Forecast,
    Halt,
    LogEvent,
    OrderUpdate,
    PortfolioSnapshot,
    RiskReject,
    SlotFill,
    SlotStatus,
    SourceStatus,
)
from pmx.feed import proto

log = logging.getLogger(__name__)

BATCH, SNAPSHOT = 0, 1
_LEN = struct.Struct(">I")
_MSG = "msg"
_ENTITY = "entity"  # classify(): render to an EntityMsg first

# Engine events the feed uses, by wire tag; anything else (timers, raw venue fills, schedules…) stays home.
EVENT_TYPES: dict[str, type] = {t.__name__: t for t in (
    BookSnapshot, BookDelta, Trade, StreamReset, Contract, Forecast, Advice, SourceStatus, AccountSnapshot,
    OrderUpdate, SlotFill, RiskReject, SlotStatus, Halt, PortfolioSnapshot, LogEvent)}
# Feed messages the engine side may send as is.
PASS_THROUGH = (proto.Hello, proto.Status, proto.Models, proto.PerfMsg, proto.EquityMsg, proto.EntityMsg)
_WORKING = frozenset({"queued", "pending", "open"})


class Encoder:
    """Engine side: batches -> frames."""

    def __init__(self, tickers_of: Callable[[str], Iterable[str]] = lambda entity: ()) -> None:
        self.tickers_of = tickers_of
        self._tags: dict[type, str | None] = {t: name for name, t in EVENT_TYPES.items()}
        self._tags.update(dict.fromkeys(PASS_THROUGH, _MSG))
        self._enc = msgspec.msgpack.Encoder()

    def tag(self, ev: object) -> str | None:
        """Wire tag for `ev`'s type (cached per type): an event name, "msg", "entity", or None = not sent."""
        t = type(ev)
        tag = self._tags.get(t, "")
        if tag == "":
            tag = self._tags[t] = _ENTITY if isinstance(ev, EntityState) else None
        return tag

    def render(self, ev: Any) -> proto.EntityMsg:
        return proto.EntityMsg.of(ev, sorted(self.tickers_of(ev.entity)))

    def frame(self, items: Iterable[tuple[int, object]], dropped: int, kind: int = BATCH) -> bytes:
        groups: dict[str, list[tuple[int, int, object]]] = {}
        tags, tag_of = self._tags, self.tag
        for seq, (now_ms, ev) in enumerate(items):
            tag = tags.get(type(ev), "")
            if tag == "":
                tag = tag_of(ev)
            if tag is None:
                continue
            if tag == _ENTITY:
                ev, tag = self.render(ev), _MSG
            g = groups.get(tag)
            if g is None:
                g = groups[tag] = []
            g.append((seq, now_ms, ev))
        body = self._enc.encode((kind, dropped, groups))
        return _LEN.pack(len(body)) + body


class Decoder:
    """Feed side: frame bodies -> `(kind, dropped, [(now_ms, event or message), …])` in engine order."""

    def __init__(self) -> None:
        self._frame = msgspec.msgpack.Decoder(tuple[int, int, dict[str, msgspec.Raw]])
        self._groups: dict[str, msgspec.msgpack.Decoder] = {
            name: msgspec.msgpack.Decoder(list[tuple[int, int, t]]) for name, t in EVENT_TYPES.items()}
        self._groups[_MSG] = msgspec.msgpack.Decoder(list[tuple[int, int, proto.Msg]])

    def decode(self, body: bytes) -> tuple[int, int, list[tuple[int, object]]]:
        kind, dropped, groups = self._frame.decode(body)
        items: list[tuple[int, int, object]] = []
        for tag, raw in groups.items():
            dec = self._groups.get(tag)
            if dec is not None:  # else a newer engine's type: skip it
                items += dec.decode(raw)
        items.sort(key=itemgetter(0))
        return kind, dropped, [(now_ms, ev) for _, now_ms, ev in items]


def market_snapshot(state: Any) -> Iterable[object]:
    """What a resync needs from engine `State` that the link doesn't keep itself: full books (the feed
    applies deltas to them), every model's latest forecast, and each market's last print."""
    for (venue, ticker), b in state.books.books.items():
        yield BookSnapshot(venue, ticker, tuple(b.levels("bid")), tuple(b.levels("ask")), b.ts_ms)
    yield from state.last_trade.values()
    yield from state.forecasts.values()


# Low-rate state the link keeps (latest per key) so a snapshot can resend it, in snapshot order.
_KINDS = ("models", "fstatus", "status", "contract", "entity", "advice", "slot", "order", "halt", "account",
          "portfolio", "perf", "equity")


class EngineLink:
    """The engine's side of the feed process: the `Tap`'s delivery and the child's supervisor.

    `snapshot()` yields the high-rate market state the link doesn't keep itself (`market_snapshot`
    for the engine); everything low-rate — contracts, entities, unexpired advice, slots, working orders,
    halts, source status, account, portfolio, and the feed messages sent as is — it keeps from the events
    it sees.
    `clock` stamps snapshot items (engine time)."""

    def __init__(self, sock: Path, hello: proto.Hello, *, snapshot: Callable[[], Iterable[object]] = tuple,
                 tickers_of: Callable[[str], Iterable[str]] = lambda entity: (), tap: Any = None,
                 clock: Callable[[], int] = lambda: int(time.time() * 1000), high_water: int = 4 << 20,
                 resync_s: float = 5.0, argv: list[str] | None = None) -> None:
        self.sock, self.hello, self.snapshot, self.tap, self.clock = sock, hello, snapshot, tap, clock
        self.enc = Encoder(tickers_of)
        self.high_water, self.resync_s = high_water, resync_s
        self.argv = argv or [sys.executable, "-m", "pmx.feed.server"]
        self.dropped = 0  # events that never reached the feed process from here (plus the tap's own)
        self.sent = 0  # events written
        self.frames = 0
        self.resyncs = 0
        self.restarts = 0
        self.proc: asyncio.subprocess.Process | None = None
        self.ready = asyncio.Event()  # the child has its first snapshot
        self._writer: asyncio.StreamWriter | None = None
        self._stale = True  # the child needs a snapshot before anything else
        self._started = False  # before the first feed process, nobody can have missed anything
        self._resynced_at = float("-inf")
        self._grace = 0  # queued bytes that can still be the last snapshot's: not a sign the child is behind
        self._cache: dict[str, dict[object, tuple[int, object]]] = {k: {} for k in _KINDS}

    @property
    def up(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    def total_dropped(self) -> int:
        return self.dropped + (self.tap.dropped if self.tap is not None else 0)

    # ---- the Tap's delivery (engine thread, never waits) -------------------------------------------

    async def deliver(self, batch: list[tuple[int, object]]) -> None:
        self._keep(batch)
        w = self._writer
        if w is None or w.is_closing() or self._stale:
            if self._started:
                self.dropped += len(batch)
            self._stale = True
            return
        queued = w.transport.get_write_buffer_size()
        self._grace = min(self._grace, queued)  # the buffer drains in order, so at most `queued` is snapshot
        if queued > self.high_water + self._grace:  # a snapshot can outgrow high_water (~5 MB at 5,000 markets)
            self.dropped += len(batch)
            self._stale = True
            log.warning("feed process is behind (%d bytes queued): dropping until it catches up", queued)
            return
        w.write(self.enc.frame(batch, self.total_dropped()))
        self.sent += len(batch)
        self.frames += 1

    def _keep(self, batch: list[tuple[int, object]]) -> None:
        """Latest low-rate state per key, from every batch — delivered or not — so snapshots are whole.
        Entity states are rendered here once and replaced in the batch."""
        cache, tag_of = self._cache, self.enc.tag
        for i, (now_ms, ev) in enumerate(batch):
            t = type(ev)
            if t is BookDelta or t is BookSnapshot or t is Trade or t is Forecast:
                continue
            match ev:
                case Contract():
                    cache["contract"][(ev.venue, ev.ticker)] = (now_ms, ev)
                case SlotStatus():
                    cache["slot"][ev.slot] = (now_ms, ev)
                case OrderUpdate():
                    if ev.status in _WORKING:
                        cache["order"][ev.client_order_id] = (now_ms, ev)
                    else:
                        cache["order"].pop(ev.client_order_id, None)
                case Halt():
                    if ev.active:
                        cache["halt"][ev.scope] = (now_ms, ev)
                    else:
                        cache["halt"].pop(ev.scope, None)
                case Advice():
                    if ev.expires_ms < now_ms:
                        cache["advice"].pop((ev.advisor, ev.target), None)
                    else:
                        cache["advice"][(ev.advisor, ev.target)] = (now_ms, ev)
                case SourceStatus():
                    cache["status"][ev.source] = (now_ms, ev)
                case AccountSnapshot():
                    cache["account"][None] = (now_ms, ev)
                case PortfolioSnapshot():
                    cache["portfolio"][None] = (now_ms, ev)
                case proto.Models():
                    cache["models"][None] = (now_ms, ev)
                case proto.Status():
                    cache["fstatus"][ev.source] = (now_ms, ev)
                case proto.PerfMsg():
                    cache["perf"][(ev.strategy, ev.mode, ev.range)] = (now_ms, ev)
                case proto.EquityMsg(replace=True):
                    cache["equity"][(ev.scope, ev.mode)] = (now_ms, ev)
                case proto.EntityMsg():
                    cache["entity"][ev.key] = (now_ms, ev)
                case _ if tag_of(ev) == _ENTITY:
                    msg = self.enc.render(ev)
                    batch[i] = (now_ms, msg)
                    cache["entity"][msg.key] = (now_ms, msg)

    # ---- resync -----------------------------------------------------------------------------------------

    def _resync(self, w: asyncio.StreamWriter) -> None:
        """Everything the tap still holds is already reflected in current state: drop it, send the state."""
        if self.tap is not None:
            held = self.tap.discard()
            self._keep(held)
            if self._started:
                self.dropped += len(held)
        self._started = True
        now = self.clock()
        cache = self._cache
        cache["advice"] = {k: v for k, v in cache["advice"].items() if v[1].expires_ms >= now}
        items: list[tuple[int, object]] = [(now, self.hello)]
        for kind in _KINDS[:4]:  # models, statuses, contracts
            items += cache[kind].values()
        items += [(now, ev) for ev in self.snapshot()]
        for kind in _KINDS[4:]:
            items += cache[kind].values()
        frame = self.enc.frame(items, self.total_dropped(), SNAPSHOT)
        w.write(frame)
        self._grace = len(frame)
        self._stale = False
        self._resynced_at = time.monotonic()
        self.resyncs += 1
        self.frames += 1

    # ---- supervision ----------------------------------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Keep a feed process running until `stop`; restart it (with back-off) whenever it exits."""
        from pmx.sources.base import sleep_or_stop

        backoff = 0.25
        while not stop.is_set():
            started = time.monotonic()
            try:
                code = await self._session(stop)
            except Exception:
                log.exception("feed process failed to start")
                code = None
            if stop.is_set():
                return
            self.restarts += 1
            if time.monotonic() - started > 60:
                backoff = 0.25
            log.warning("feed process exited (code %s); the engine carries on, restarting it in %.2fs", code, backoff)
            await sleep_or_stop(stop, backoff)
            backoff = min(backoff * 2, 10.0)

    async def _session(self, stop: asyncio.Event) -> int | None:
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            proc = self.proc = await asyncio.create_subprocess_exec(
                *self.argv, "--fd", str(child.fileno()), "--socket", str(self.sock),
                pass_fds=(child.fileno(),), start_new_session=True)  # Ctrl-C reaches the engine, not the feed
        finally:
            child.close()
        _, w = await asyncio.open_unix_connection(sock=parent)
        self._writer = w
        self._resync(w)
        self.ready.set()
        exited = asyncio.ensure_future(proc.wait())
        stopping = asyncio.ensure_future(stop.wait())
        try:
            while not (exited.done() or stopping.done()):
                await asyncio.wait({exited, stopping}, timeout=0.25)
                if self._stale and self.up and time.monotonic() - self._resynced_at >= self.resync_s \
                        and w.transport.get_write_buffer_size() < self.high_water // 2:
                    log.info("feed process caught up: resyncing it (%d events dropped so far)", self.total_dropped())
                    self._resync(w)
        finally:
            stopping.cancel()
            self._writer = None
            self._stale = True
            self.ready.clear()
            w.close()  # the child sees EOF and exits
            if not exited.done():
                try:
                    await asyncio.wait_for(asyncio.shield(exited), 5.0)
                except TimeoutError:
                    proc.kill()
                    await exited
        return proc.returncode


class TapPublisher:
    """`Publisher`'s methods for code that produces viewer data in the engine process (the demo):
    everything goes into the tap, and so to the feed process."""

    def __init__(self, tap: Any, clock: Callable[[], int] = lambda: int(time.time() * 1000)) -> None:
        self.tap, self.clock = tap, clock

    def on_event(self, ev: object, now_ms: int) -> None:
        self.tap.append(ev, now_ms)

    def set_models(self, names: list[str]) -> None:
        self.tap.append(proto.Models(list(names)), self.clock())

    def publish_perf(self, msg: proto.PerfMsg) -> None:
        self.tap.append(msg, self.clock())

    def set_status(self, connected: bool, detail: str, source: str = "", ts_ms: int | None = None) -> None:
        now = self.clock()
        self.tap.append(proto.Status(connected, detail, ts_ms or now, source), now)

    def seed_equity(self, scope: str, mode: str, points: list[tuple[int, int, int]]) -> None:
        self.tap.append(proto.EquityMsg(scope, mode, sorted(points), replace=True), self.clock())

    def flush(self, now_ms: int) -> None:
        pass


# ---- the feed process -------------------------------------------------------------------------------------


async def serve_feed(conn: socket.socket, sock: Path, *, flush_ms: int = 50) -> None:
    """Read frames from the engine until it goes away; serve viewers on `sock` from the first snapshot on."""
    from pmx.feed.hub import FeedHub
    from pmx.feed.publisher import Publisher

    reader, _ = await asyncio.open_unix_connection(sock=conn)
    dec = Decoder()
    hub: FeedHub | None = None
    pub: Publisher | None = None
    server: asyncio.Server | None = None
    clock = [0, time.monotonic()]  # engine ms of the newest item, and when it arrived

    def engine_now() -> int:
        return clock[0] + int((time.monotonic() - clock[1]) * 1000)

    async def flusher() -> None:  # held-back conflated values go out on time when the engine is quiet
        while True:
            await asyncio.sleep(flush_ms / 1000)
            if pub is not None:
                pub.flush(engine_now())

    flushing = asyncio.ensure_future(flusher())
    seen_dropped, status_at = -1, 0.0
    try:
        while True:
            try:
                (n,) = _LEN.unpack(await reader.readexactly(4))
                body = await reader.readexactly(n)
            except asyncio.IncompleteReadError:
                log.info("engine closed the feed link")
                return
            kind, dropped, items = dec.decode(body)
            if kind == SNAPSHOT:
                hello = next((ev for _, ev in items if type(ev) is proto.Hello), None)
                if hub is None:
                    if hello is None:
                        raise RuntimeError("the engine's first frame must be a snapshot with a hello")
                    hub = FeedHub(hello)
                    pub = Publisher(hub)
                else:
                    hub.restate()
            if pub is None:
                continue
            for now_ms, ev in items:
                if type(ev) is not proto.Hello:
                    pub.on_event(ev, now_ms)
            if items:
                clock[0], clock[1] = max(clock[0], items[-1][0]), time.monotonic()
            assert hub is not None
            if dropped != seen_dropped and (kind == SNAPSHOT or time.monotonic() - status_at >= 1.0):
                seen_dropped, status_at = dropped, time.monotonic()
                hub.set_status(True, "engine connected", "engine", dropped=dropped)
            if kind == SNAPSHOT:
                if server is None:
                    server = await hub.serve(sock)
                    log.info("feed process serving viewers on %s", sock)
                else:
                    hub.resend_all()
                    log.info("resynced from the engine (%d events dropped so far)", dropped)
    finally:
        flushing.cancel()
        if server is not None:
            server.close()
            sock.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="pmx.feed.server", description="the feed process (started by the engine)")
    p.add_argument("--fd", type=int, required=True, help="the engine's end of the link (an inherited socket)")
    p.add_argument("--socket", required=True, help="where viewers connect")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s[feed]: %(message)s")
    conn = socket.socket(fileno=args.fd)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve_feed(conn, Path(args.socket)))


if __name__ == "__main__":
    main()
