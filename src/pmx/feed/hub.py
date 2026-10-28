"""Fan-out of state to local viewers over a Unix socket. The hub keeps current state so a
viewer that connects mid-session starts from a full picture: markets and books, plus (fed by
`feed/publisher.py`) entities, fairs, slots, orders, fills, rejects, logs, halts, the
portfolio, equity history, and performance. It sends what it's told: when a market's
conflated `top` and `book` go out is the publisher's call (v3)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path

from pmx.core.book import BookTracker
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, MarketEvent, StreamReset, Trade, VenuePosition
from pmx.feed import proto

log = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


class _Client:
    __slots__ = ("queue", "dropped")

    def __init__(self, maxsize: int) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize)
        self.dropped = False


EQUITY_POINTS = 2_000
# (max age, bucket): full resolution for 10 min, then coarser with age (~1,900 points per 30 days).
EQUITY_TIERS: tuple[tuple[int | None, int], ...] = (
    (600_000, 0), (6 * 3_600_000, 60_000), (2 * 86_400_000, 600_000), (30 * 86_400_000, 3_600_000), (None, 86_400_000)
)


def thin(points: list[tuple[int, int, int]], now_ms: int) -> list[tuple[int, int, int]]:
    """Downsample an equity series by age, keeping the last point of each bucket."""
    out: list[tuple[int, int, int]] = []
    prev: tuple[int, int] | None = None
    for p in points:
        age = now_ms - p[0]
        step = next(step for limit, step in EQUITY_TIERS if limit is None or age <= limit)
        key = (step, p[0] // step) if step else None
        if key is not None and key == prev:
            out[-1] = p
        else:
            out.append(p)
        prev = key
    return out


class FeedHub:
    def __init__(self, hello: proto.Hello, *, trades_kept: int = 500, recent: int = 500, queue_max: int = 50_000,
                 clock: Callable[[], int] = _now_ms) -> None:
        self.hello = hello
        self.venue = hello.venue
        self.clock = clock  # stamps what isn't stamped by an event
        self.status = proto.Status(False, "starting", clock())
        self.statuses: dict[str, proto.Status] = {}  # outside sources; the venue's is `status`
        self.account: proto.Account | None = None
        self.contracts: dict[str, proto.ContractMsg] = {}
        self.books = BookTracker()
        self.trades: deque[proto.TradeMsg] = deque(maxlen=trades_kept)
        self.last_trade: dict[str, proto.TradeMsg] = {}
        self.models: list[str] = []
        self.entities: dict[str, proto.EntityMsg] = {}
        self.forecasts: dict[tuple[str, str], proto.ForecastMsg] = {}
        self.fairs: dict[str, dict[str, float]] = {}  # ticker -> model -> p
        self.advice: dict[tuple[str, str], proto.AdviceMsg] = {}  # (advisor, target) -> latest
        self.slots: dict[str, proto.SlotMsg] = {}
        self.positions: dict[str, dict[str, int]] = {}  # ticker -> mode -> qty, summed over its slots (nonzero)
        self._slots_on: dict[str, int] = {}  # ticker -> slots trading it
        self._working_on: dict[str, int] = {}  # ticker -> working orders there
        self._sent_top: dict[str, proto.Top] = {}
        self._sent_book: dict[str, tuple[list, list]] = {}
        self.working: dict[str, proto.OrderMsg] = {}  # client_order_id -> open/pending order
        self.orders: deque[proto.OrderMsg] = deque(maxlen=recent)  # finished orders
        self.fills: deque[proto.FillMsg] = deque(maxlen=recent)
        self.risks: deque[proto.RiskMsg] = deque(maxlen=recent)
        self.logs: deque[proto.LogMsg] = deque(maxlen=2 * recent)
        self.halts: dict[str, proto.HaltMsg] = {}
        self.portfolio: proto.PortfolioMsg | None = None
        self.equity: dict[tuple[str, str], list[tuple[int, int, int]]] = {}
        self.perf: dict[tuple[str, str, str], proto.PerfMsg] = {}
        self._clients: set[_Client] = set()
        self._queue_max = queue_max
        self.muted = False  # applying a resync snapshot: viewers get the result whole, after

    @property
    def clients(self) -> int:
        return len(self._clients)

    # ---- state updates ---------------------------------------------------

    def set_status(self, connected: bool, detail: str, source: str = "", ts_ms: int | None = None,
                   dropped: int = 0) -> None:
        if not source or source == self.venue:
            if (connected, detail) != (self.status.connected, self.status.detail):
                self.status = proto.Status(connected, detail, ts_ms or self.clock())
                self._send(self.status)
            return
        prev = self.statuses.get(source)
        if prev is None or (connected, detail, dropped) != (prev.connected, prev.detail, prev.dropped):
            self.statuses[source] = msg = proto.Status(connected, detail, ts_ms or self.clock(), source, dropped)
            self._send(msg)

    def set_contract(self, c: Contract) -> None:
        msg = proto.ContractMsg.of(c)
        if self.contracts.get(c.ticker) != msg:
            self.contracts[c.ticker] = msg
            self._send(msg)

    def set_account(self, balance: int, positions: list[VenuePosition]) -> None:
        rows = [proto.PositionRow(p.ticker, p.qty, p.realized_pnl, p.fees) for p in positions]
        self.account = proto.Account(balance, rows, self.clock())
        self._send(self.account)

    def on_event(self, ev: MarketEvent | Contract) -> str | None:
        """Apply market data; returns the ticker whose `top`/`book` may now differ from what viewers
        have (sending them is the publisher's call). A book that breaks goes `stale` at once."""
        if isinstance(ev, BookSnapshot | BookDelta):
            known = self.books.get(ev.venue, ev.ticker) is not None
            if self.books.on_event(ev) is not None:
                return ev.ticker
            if known:
                self._sent_book.pop(ev.ticker, None)
                self._send(proto.Stale(ev.ticker))
            return None
        if isinstance(ev, Trade):
            msg = proto.TradeMsg(ev.ticker, ev.price, ev.qty, ev.taker_side, ev.ts_ms)
            self.trades.append(msg)
            self.last_trade[ev.ticker] = msg
            self._send(msg)
            return ev.ticker
        if isinstance(ev, Contract):
            self.set_contract(ev)
        elif isinstance(ev, Fill):
            self._send(proto.FillMsg(ev.ticker, ev.side, ev.price, ev.qty, ev.fee, ev.is_taker, ev.ts_ms))
        elif isinstance(ev, StreamReset):
            self.books.on_event(ev)
            self._sent_book.clear()
            self._send(proto.Reset(ev.reason, ev.ts_ms))
        return None

    # ---- conflated market view (v3) ------------------------------------------

    def top(self, ticker: str, ts_ms: int) -> proto.Top:
        book = self.books.get(self.venue, ticker)
        last = self.last_trade.get(ticker)
        return proto.Top(ticker, None if book is None else book.best_bid(), None if book is None else book.best_ask(),
                         None if last is None else (last.price, last.qty, last.taker_side),
                         dict(self.fairs.get(ticker, {})), dict(self.positions.get(ticker, {})), ts_ms)

    def book_msg(self, ticker: str) -> proto.Book | None:
        book = self.books.get(self.venue, ticker)
        if book is None:
            return None
        return proto.Book(ticker, book.levels("bid", proto.BOOK_LEVELS), book.levels("ask", proto.BOOK_LEVELS),
                          book.ts_ms)

    def send_market(self, ticker: str, ts_ms: int) -> int:
        """Send this market's `book` and `top` if either changed since last sent; returns how many went."""
        n = 0
        book = self.book_msg(ticker)
        if book is not None:
            levels = (book.bids, book.asks)
            if self._sent_book.get(ticker) != levels:
                self._sent_book[ticker] = levels
                self._send(book)
                n += 1
        top = self.top(ticker, ts_ms)
        prev = self._sent_top.get(ticker)
        if prev is None or (prev.bid, prev.ask, prev.last, prev.fairs, prev.position) != \
                (top.bid, top.ask, top.last, top.fairs, top.position):
            self._sent_top[ticker] = top
            self._send(top)
            n += 1
        return n

    def traded(self, ticker: str) -> bool:
        """A slot trades it or we have orders working there: viewers get it at the faster rate."""
        return ticker in self._slots_on or ticker in self._working_on

    # ---- v2 state (the engine's publisher calls these) ----------------------

    def set_models(self, names: list[str]) -> None:
        if names != self.models:
            self.models = list(names)
            self._send(proto.Models(self.models))

    def set_entity(self, msg: proto.EntityMsg) -> None:
        self.entities[msg.key] = msg
        self._send(msg)

    def set_forecast(self, msg: proto.ForecastMsg) -> str:
        """Kept as the latest; it reaches viewers in its market's `top`. Returns the ticker."""
        self.forecasts[(msg.model, msg.ticker)] = msg
        self.fairs.setdefault(msg.ticker, {})[msg.model] = msg.p
        return msg.ticker

    def set_advice(self, msg: proto.AdviceMsg) -> None:
        if msg.expires_ms < msg.ts_ms:  # withdrawn
            self.advice.pop((msg.advisor, msg.target), None)
        else:
            self.advice[(msg.advisor, msg.target)] = msg
        self._send(msg)

    def set_slot(self, msg: proto.SlotMsg, *, send: bool = True) -> str | None:
        """Returns the ticker whose position changed, if any."""
        prev = self.slots.get(msg.slot)
        self.slots[msg.slot] = msg
        if send:
            self._send(msg)
        if prev is not None:
            if (prev.ticker, prev.mode, prev.position) == (msg.ticker, msg.mode, msg.position):
                return None
            _count(self._slots_on, prev.ticker, -1)
            self._add_position(prev.ticker, prev.mode, -prev.position)
        _count(self._slots_on, msg.ticker, 1)
        self._add_position(msg.ticker, msg.mode, msg.position)
        return msg.ticker

    def _add_position(self, ticker: str, mode: str, qty: int) -> None:
        if not qty:
            return
        by_mode = self.positions.setdefault(ticker, {})
        q = by_mode.get(mode, 0) + qty
        if q:
            by_mode[mode] = q
        else:
            del by_mode[mode]
            if not by_mode:
                del self.positions[ticker]

    def set_order(self, msg: proto.OrderMsg) -> None:
        was = self.working.get(msg.client_order_id)
        if msg.working:
            self.working[msg.client_order_id] = msg
            if was is None:
                _count(self._working_on, msg.ticker, 1)
        else:
            if self.working.pop(msg.client_order_id, None) is not None:
                _count(self._working_on, msg.ticker, -1)
            self.orders.append(msg)
        self._send(msg)

    def add_fill(self, msg: proto.FillMsg) -> None:
        self.fills.append(msg)
        self._send(msg)

    def add_risk(self, msg: proto.RiskMsg) -> None:
        self.risks.append(msg)
        self._send(msg)

    def add_log(self, msg: proto.LogMsg) -> None:
        self.logs.append(msg)
        self._send(msg)

    def set_halt(self, msg: proto.HaltMsg) -> None:
        if msg.active:
            self.halts[msg.scope] = msg
        else:
            self.halts.pop(msg.scope, None)
        self._send(msg)

    def set_portfolio(self, msg: proto.PortfolioMsg, *, send: bool = True) -> None:
        self.portfolio = msg
        if send:
            self._send(msg)

    def seed_equity(self, scope: str, mode: str, points: list[tuple[int, int, int]]) -> None:
        """Replace a scope's history (e.g. from the ledger) and resend it whole."""
        points = sorted(points)
        self.equity[(scope, mode)] = series = thin(points, points[-1][0] if points else self.clock())
        self._send(proto.EquityMsg(scope, mode, series, replace=True))

    def add_equity(self, scope: str, mode: str, point: tuple[int, int, int]) -> None:
        series = self.equity.setdefault((scope, mode), [])
        series.append(point)
        if len(series) > EQUITY_POINTS:
            self.equity[(scope, mode)] = thin(series, point[0])
        self._send(proto.EquityMsg(scope, mode, [point]))

    def set_perf(self, msg: proto.PerfMsg) -> None:
        self.perf[(msg.strategy, msg.mode, msg.range)] = msg
        self._send(msg)

    def restate(self) -> None:
        """Forget current state a resync snapshot resends in full (books, fairs, entities, slots,
        orders, halts, statuses, account, portfolio); history (prints, finished orders, fills,
        rejects, logs, equity, perf) stays. Muted until `resend_all`."""
        self.muted = True
        self.books = BookTracker()
        self.last_trade.clear()
        self.forecasts.clear()
        self.fairs.clear()
        self.entities.clear()
        self.advice.clear()
        self.slots.clear()
        self.positions.clear()
        self._slots_on.clear()
        self.working.clear()
        self._working_on.clear()
        self.halts.clear()
        self.statuses.clear()
        self.account = None
        self.portfolio = None
        self._sent_top.clear()
        self._sent_book.clear()

    def resend_all(self) -> None:
        """Every viewer starts over from the current state (a `hello` resets a viewer)."""
        self.muted = False
        lines = [proto.encode(m) for m in self.snapshot()]
        for c in list(self._clients):
            for line in lines:
                if not self._put(c, line):
                    break

    def snapshot(self) -> list[proto.Msg]:
        out: list[proto.Msg] = [self.hello, self.status, *self.statuses.values()]
        if self.account is not None:
            out.append(self.account)
        if self.models:
            out.append(proto.Models(self.models))
        out += self.contracts.values()
        for (_, ticker), b in self.books.books.items():
            out.append(proto.Book(ticker, b.levels("bid", proto.BOOK_LEVELS), b.levels("ask", proto.BOOK_LEVELS),
                                  b.ts_ms))
        out += self.trades
        now = self.clock()
        out += [self.top(t, now) for t in self.contracts if t in self.fairs or t in self.last_trade
                or t in self.positions or self.books.get(self.venue, t) is not None]
        out += self.entities.values()
        self.advice = {k: a for k, a in self.advice.items() if a.expires_ms >= now}
        out += self.advice.values()
        out += self.slots.values()
        out += self.orders
        out += self.working.values()
        out += self.fills
        out += self.risks
        out += self.logs
        out += self.halts.values()
        if self.portfolio is not None:
            out.append(self.portfolio)
        out += [proto.EquityMsg(scope, mode, pts, replace=True) for (scope, mode), pts in self.equity.items()]
        out += self.perf.values()
        out.append(proto.Synced())
        return out

    # ---- fan-out ---------------------------------------------------------

    def _send(self, msg: proto.Msg) -> None:
        if not self._clients or self.muted:
            return
        line = proto.encode(msg)
        for c in self._clients:
            self._put(c, line)

    @staticmethod
    def _put(c: _Client, line: bytes) -> bool:
        if c.dropped:
            return False
        try:
            c.queue.put_nowait(line)
        except asyncio.QueueFull:
            # A viewer that can't keep up gets disconnected; it resyncs on reconnect.
            c.dropped = True
            while not c.queue.empty():
                c.queue.get_nowait()
            c.queue.put_nowait(None)
            return False
        return True

    def attach(self) -> _Client:
        """Register a client with the current state queued. No await between the two, so
        nothing published in between is lost or duplicated."""
        c = _Client(self._queue_max)
        for msg in self.snapshot():
            c.queue.put_nowait(proto.encode(msg))
        self._clients.add(c)
        return c

    def detach(self, c: _Client) -> None:
        self._clients.discard(c)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        c = self.attach()
        log.info("viewer connected (%d)", self.clients)
        eof = asyncio.create_task(reader.read())  # viewers don't send; this just notices hang-ups
        try:
            while True:
                get = asyncio.create_task(c.queue.get())
                done, _ = await asyncio.wait({get, eof}, return_when=asyncio.FIRST_COMPLETED)
                if eof in done:
                    get.cancel()
                    break
                line, batch = get.result(), []
                while line is not None:
                    batch.append(line)
                    if c.queue.empty():
                        break
                    line = c.queue.get_nowait()
                writer.writelines(batch)
                await writer.drain()
                if line is None:
                    log.warning("viewer too slow; disconnected")
                    break
        except (ConnectionError, BrokenPipeError):
            pass
        finally:
            self.detach(c)
            eof.cancel()
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            log.info("viewer disconnected (%d)", self.clients)

    async def serve(self, path: Path) -> asyncio.Server:
        await _claim_socket(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        server = await asyncio.start_unix_server(self._handle, path=str(path))
        os.chmod(path, 0o600)
        return server


def _count(counts: dict[str, int], key: str, by: int) -> None:
    n = counts.get(key, 0) + by
    if n > 0:
        counts[key] = n
    else:
        counts.pop(key, None)


async def socket_alive(path: Path) -> bool:
    try:
        _, w = await asyncio.open_unix_connection(str(path))
    except (FileNotFoundError, ConnectionRefusedError, OSError):
        return False
    w.close()
    return True


async def _claim_socket(path: Path) -> None:
    if not path.exists():
        return
    if await socket_alive(path):
        raise RuntimeError(f"another `pmx serve` is already listening on {path}")
    path.unlink()
