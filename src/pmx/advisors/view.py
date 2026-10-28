"""`FeedView`: an advisor's `StateView`, built from the viewer feed (`data/pmx.sock`) like the TUI's —
read-only, and as current as the feed (conflated books, at most 4–10 a second per market). Linked
entities come as their domain renders them (`EntityView`: summary and fields), since the feed carries
no domain types."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path

import msgspec

from pmx.core.book import OrderBook
from pmx.core.units import Qty
from pmx.engine.events import Forecast
from pmx.feed import proto

log = logging.getLogger(__name__)


class EntityView(msgspec.Struct, frozen=True):
    """An entity as the feed shows it; satisfies `EntityState`."""

    key: str
    kind: str
    tickers: list[str]
    text: str
    fields: list[tuple[str, str]]
    ts_ms: int

    @property
    def entity(self) -> str:
        return self.key

    def summary(self) -> str:
        return self.text

    def display_fields(self) -> list[tuple[str, str]]:
        return self.fields


class FeedView:
    def __init__(self) -> None:
        self.connected = False
        self.contracts: dict[str, proto.ContractMsg] = {}
        self.books: dict[str, OrderBook] = {}
        self.forecasts: dict[tuple[str, str], Forecast] = {}
        self.entities: dict[str, EntityView] = {}
        self.slots: dict[str, proto.SlotMsg] = {}
        self.updated = asyncio.Event()  # set on every message (advisors may wait on it)
        self.synced = asyncio.Event()  # the feed's full state has arrived

    # ---- StateView ---------------------------------------------------------------------------------------

    def book(self, ticker: str) -> OrderBook | None:
        return self.books.get(ticker)

    def data(self, entity: str) -> EntityView | None:
        return self.entities.get(entity)

    def forecast(self, ticker: str, model: str) -> Forecast | None:
        return self.forecasts.get((model, ticker))

    def position(self, slot: str) -> Qty:
        s = self.slots.get(slot)
        return 0 if s is None else s.position

    def tickers(self, *, series: str | None = None) -> list[str]:
        """Open markets, optionally of one series."""
        return sorted(t for t, c in self.contracts.items()
                      if c.status == "open" and (series is None or c.series == series))

    # ---- the feed ---------------------------------------------------------------------------------------

    def apply(self, msg: proto.Msg) -> None:
        match msg:
            case proto.Synced():
                self.synced.set()
            case proto.Hello():  # a (re)connect or a resync: the whole state follows
                self.contracts.clear()
                self.books.clear()
                self.forecasts.clear()
                self.entities.clear()
                self.slots.clear()
            case proto.ContractMsg():
                self.contracts[msg.ticker] = msg
            case proto.Book(ticker=t):
                b = self.books.get(t) or OrderBook(t)
                b.replace(msg.bids, msg.asks, msg.ts_ms)
                self.books[t] = b
            case proto.Delta(ticker=t):  # v2 feeds
                b = self.books.get(t)
                if b is not None:
                    try:
                        b.add(msg.side, msg.price, msg.delta, msg.ts_ms)
                    except Exception:
                        del self.books[t]
            case proto.Stale(ticker=t):
                self.books.pop(t, None)
            case proto.Reset():
                self.books.clear()
            case proto.Top(ticker=t, ts_ms=ts):
                for model, p in msg.fairs.items():
                    self.forecasts[(model, t)] = Forecast(model, t, p, ts, ts)
            case proto.ForecastMsg():  # v2 feeds
                self.forecasts[(msg.model, msg.ticker)] = Forecast(msg.model, msg.ticker, msg.p, msg.ts_ms,
                                                                    msg.data_ts_ms)
            case proto.EntityMsg():
                self.entities[msg.key] = EntityView(msg.key, msg.kind, msg.tickers, msg.summary, msg.fields,
                                                    msg.ts_ms)
            case proto.SlotMsg():
                self.slots[msg.slot] = msg
        self.updated.set()
        self.updated.clear()


async def follow(view: FeedView, sock: Path, stop: asyncio.Event, *, retry_s: float = 1.0) -> None:
    """Keep `view` current from the feed at `sock` until `stop`, reconnecting when it goes away."""
    while not stop.is_set():
        try:
            reader, writer = await asyncio.open_unix_connection(str(sock), limit=1 << 24)
        except OSError:
            view.connected = False
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), retry_s)
            continue
        view.connected = True
        stopping = asyncio.ensure_future(stop.wait())
        try:
            while True:
                line = asyncio.ensure_future(reader.readline())
                await asyncio.wait({line, stopping}, return_when=asyncio.FIRST_COMPLETED)
                if stopping.done():
                    line.cancel()
                    return
                raw = line.result()
                if not raw:
                    break
                try:
                    view.apply(proto.decoder.decode(raw))
                except msgspec.DecodeError:
                    log.warning("advisor view: unreadable feed line skipped")
        except ConnectionError:
            pass
        finally:
            stopping.cancel()
            writer.close()
            view.connected = False
