"""Aggregated L2 book for one binary contract, YES-side. Levels are sparse dicts —
subpenny grids make fixed arrays wasteful, and real books hold tens of levels.
"""

from __future__ import annotations

from collections.abc import Iterable

from pmx.core.types import BookDelta, BookSide, BookSnapshot, MarketEvent, StreamReset
from pmx.core.units import Price, Qty


class BookError(RuntimeError):
    """The book can't be what the venue says it is — treat as a gap and resync."""


class OrderBook:
    __slots__ = ("ticker", "bids", "asks", "ts_ms")

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.bids: dict[Price, Qty] = {}
        self.asks: dict[Price, Qty] = {}
        self.ts_ms = 0

    def replace(self, bids: Iterable[tuple[Price, Qty]], asks: Iterable[tuple[Price, Qty]], ts_ms: int = 0) -> None:
        self.bids = {p: q for p, q in bids if q > 0}
        self.asks = {p: q for p, q in asks if q > 0}
        self.ts_ms = ts_ms

    def add(self, side: BookSide, price: Price, delta: Qty, ts_ms: int = 0) -> None:
        levels = self.bids if side == "bid" else self.asks
        q = levels.get(price, 0) + delta
        if q < 0:
            raise BookError(f"{self.ticker}: {side} {price} would go to {q}")
        if q:
            levels[price] = q
        else:
            levels.pop(price, None)
        self.ts_ms = ts_ms or self.ts_ms

    def apply(self, ev: BookSnapshot | BookDelta) -> None:
        if isinstance(ev, BookSnapshot):
            self.replace(ev.bids, ev.asks, ev.ts_ms)
        else:
            self.add(ev.side, ev.price, ev.delta, ev.ts_ms)

    def best_bid(self) -> tuple[Price, Qty] | None:
        if not self.bids:
            return None
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self) -> tuple[Price, Qty] | None:
        if not self.asks:
            return None
        p = min(self.asks)
        return p, self.asks[p]

    def mid(self) -> float | None:
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else (b[0] + a[0]) / 2

    def spread(self) -> Price | None:
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else a[0] - b[0]

    def crossed(self) -> bool:
        s = self.spread()
        return s is not None and s <= 0

    def levels(self, side: BookSide, n: int | None = None) -> list[tuple[Price, Qty]]:
        """Best-first."""
        src = self.bids if side == "bid" else self.asks
        out = sorted(src.items(), reverse=(side == "bid"))
        return out if n is None else out[:n]

    def depth_to(self, side: BookSide, limit: Price) -> Qty:
        """Quantity a taker could hit/lift on `side` at prices no worse than `limit`."""
        if side == "bid":
            return sum(q for p, q in self.bids.items() if p >= limit)
        return sum(q for p, q in self.asks.items() if p <= limit)

    def __repr__(self) -> str:
        return f"OrderBook({self.ticker} bid={self.best_bid()} ask={self.best_ask()})"


class BookTracker:
    """Books for many contracts, kept honest across resets: after a `StreamReset` a
    book is dropped until its next snapshot, and deltas for unknown books are ignored."""

    def __init__(self) -> None:
        self.books: dict[tuple[str, str], OrderBook] = {}

    def get(self, venue: str, ticker: str) -> OrderBook | None:
        return self.books.get((venue, ticker))

    def on_event(self, ev: MarketEvent) -> OrderBook | None:
        """Apply `ev`; returns the book it changed, if any."""
        if isinstance(ev, StreamReset):
            for key in [k for k in self.books if k[0] == ev.venue]:
                del self.books[key]
            return None
        if isinstance(ev, BookSnapshot):
            book = self.books.setdefault((ev.venue, ev.ticker), OrderBook(ev.ticker))
            book.apply(ev)
            return book
        if isinstance(ev, BookDelta):
            book = self.books.get((ev.venue, ev.ticker))
            if book is None:
                return None
            try:
                book.apply(ev)
            except BookError:
                del self.books[(ev.venue, ev.ticker)]
                return None
            return book
        return None
