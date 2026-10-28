"""The paper broker: simulated fills against the real (or replayed) book, conservative on purpose
— optimistic fills manufacture fake edge.

- A take (IOC) walks the visible opposite side up to its limit. Depth it consumed stays consumed until
  that price level updates again, so two takes can't both eat the same size.
- A resting order joins the back of the queue: `queue_ahead` is the size displayed at its price when it
  was placed. A print at its price eats the queue first and fills it with what's left; a print *through*
  its price fills it up to the print's size; the opposite side's best price reaching it fills it in full.
- A post-only order that would cross is rejected, as the venue would; a plain limit order that crosses
  takes what's visible and rests the rest.
- Taker and maker fees come from each contract's schedule.

It executes on engine time when the order scheduler releases a write (`place_now` / `cancel_now`), and
as the engine's `broker` step it fills resting orders on every book change and print (`on_event`). Fills
and order updates come back as engine events, so the order manager, the ledger, and the slots see a paper
session exactly as they'd see a live one."""

from __future__ import annotations

from pmx.core.book import OrderBook
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, OrderRequest, Trade
from pmx.core.units import Price, Qty
from pmx.engine.events import Event, Mode, OrderUpdate, SlotFill
from pmx.engine.state import MarketId, State, market_id


class PaperOrder:
    __slots__ = ("req", "slot", "mid", "order_id", "filled", "queue_ahead", "placed_ms")

    def __init__(self, req: OrderRequest, slot: str, mid: MarketId, order_id: str, queue_ahead: Qty,
                 placed_ms: int) -> None:
        self.req, self.slot, self.mid, self.order_id = req, slot, mid, order_id
        self.filled, self.queue_ahead, self.placed_ms = 0, queue_ahead, placed_ms

    @property
    def remaining(self) -> Qty:
        return self.req.qty - self.filled


class PaperBroker:
    mode: Mode = "paper"

    def __init__(self) -> None:
        self.resting: dict[str, PaperOrder] = {}  # client order id -> order
        self._on: dict[MarketId, list[PaperOrder]] = {}  # market -> its resting orders
        self._consumed: dict[tuple[MarketId, str, Price], Qty] = {}  # (market, book side, price) -> taken
        self._seq = 0

    def queue_ahead(self, client_order_id: str) -> Qty | None:
        o = self.resting.get(client_order_id)
        return None if o is None else max(0, o.queue_ahead)

    # ---- writes ------------------------------------------------------------------------------------------

    def place_now(self, req: OrderRequest, slot: str, state: State, now_ms: int) -> list[Event]:
        self._seq += 1
        order_id = f"P{self._seq}"
        mid = market_id(req.venue, req.ticker)

        def update(status: str, filled: Qty, reason: str = "") -> OrderUpdate:
            return OrderUpdate(slot, "paper", req.client_order_id, order_id, mid, req.side, req.price, req.qty,
                               filled, status, now_ms, reason)

        book = state.book(mid)
        c = state.contracts.get(mid)
        if c is None or c.status != "open":
            return [update("rejected", 0, f"market {'unknown' if c is None else c.status}")]
        crosses = book is not None and self._crosses(req.side, req.price, book)
        if req.post_only and crosses:
            return [update("rejected", 0, "post-only order would cross")]
        out: list[Event] = []
        filled = 0
        if crosses:
            filled = self._take(req, slot, order_id, mid, book, state, now_ms, out)
        if req.tif in ("ioc", "fok") or filled == req.qty:
            out.append(update("filled" if filled == req.qty else "canceled", filled,
                              "" if filled == req.qty else "IOC remainder"))
            return out
        ahead = 0 if book is None else (book.bids if req.side == "bid" else book.asks).get(req.price, 0)
        o = PaperOrder(req, slot, mid, order_id, ahead, now_ms)
        o.filled = filled
        self.resting[req.client_order_id] = o
        self._on.setdefault(mid, []).append(o)
        out.append(update("open", filled))
        return out

    def cancel_now(self, client_order_id: str, now_ms: int) -> list[Event]:
        o = self.resting.pop(client_order_id, None)
        if o is None:
            return []  # already filled or gone: nothing to cancel
        self._drop(o)
        return [OrderUpdate(o.slot, "paper", client_order_id, o.order_id, o.mid, o.req.side, o.req.price, o.req.qty,
                            o.filled, "canceled", now_ms)]

    @staticmethod
    def _crosses(side: str, price: Price, book: OrderBook) -> bool:
        best = book.best_ask() if side == "bid" else book.best_bid()
        return best is not None and (price >= best[0] if side == "bid" else price <= best[0])

    def _take(self, req: OrderRequest, slot: str, order_id: str, mid: MarketId, book: OrderBook, state: State,
              now_ms: int, out: list[Event]) -> Qty:
        """Walk the visible opposite side within the limit; returns the quantity filled."""
        book_side = "ask" if req.side == "bid" else "bid"
        fees = state.contracts[mid].fees
        want, got = req.qty, 0
        for price, size in book.levels(book_side):
            if (req.side == "bid" and price > req.price) or (req.side == "ask" and price < req.price) or got == want:
                break
            key = (mid, book_side, price)
            available = size - self._consumed.get(key, 0)
            if available <= 0:
                continue
            q = min(available, want - got)
            self._consumed[key] = self._consumed.get(key, 0) + q
            got += q
            out.append(self._fill(slot, req, order_id, mid, price, q, fees.fee(price, q, taker=True), True, now_ms))
        return got

    def _fill(self, slot: str, req: OrderRequest, order_id: str, mid: MarketId, price: Price, qty: Qty, fee: int,
              taker: bool, now_ms: int) -> SlotFill:
        self._seq += 1
        # Unique across runs because client order ids are (the ledger keys fills by id).
        return SlotFill(slot, "paper", Fill(req.venue, f"{req.client_order_id}-f{self._seq}", order_id, req.ticker,
                                            req.side, price, qty, fee, taker, now_ms, req.client_order_id))

    def _drop(self, o: PaperOrder) -> None:
        on = self._on.get(o.mid)
        if on is not None:
            on.remove(o)
            if not on:
                del self._on[o.mid]

    # ---- the market moves -----------------------------------------------------------------------------------

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        t = type(ev)
        if t is BookDelta:
            mid = market_id(ev.venue, ev.ticker)
            self._consumed.pop((mid, ev.side, ev.price), None)  # that level updated: its size is real again
            return self._through(mid, state, now_ms) if mid in self._on else []
        if t is BookSnapshot:
            mid = market_id(ev.venue, ev.ticker)
            for key in [k for k in self._consumed if k[0] == mid]:
                del self._consumed[key]
            return self._through(mid, state, now_ms) if mid in self._on else []
        if t is Trade:
            mid = market_id(ev.venue, ev.ticker)
            return self._print(ev, mid, state, now_ms) if mid in self._on else []
        if t is Contract:
            mid = market_id(ev.venue, ev.ticker)
            if ev.status != "open" and mid in self._on:  # the venue cancels resting orders on a pause or close
                return [e for o in list(self._on[mid]) for e in self.cancel_now(o.req.client_order_id, now_ms)]
        return []

    def _through(self, mid: MarketId, state: State, now_ms: int) -> list[Event]:
        """The opposite best reached a resting order's price: it fills in full, at its own price."""
        book = state.book(mid)
        if book is None:
            return []
        out: list[Event] = []
        for o in list(self._on.get(mid, ())):
            best = self._best_available(mid, "ask" if o.req.side == "bid" else "bid", book)
            if best is not None and (best <= o.req.price if o.req.side == "bid" else best >= o.req.price):
                self._maker(o, o.remaining, state, now_ms, out)
        return out

    def _best_available(self, mid: MarketId, book_side: str, book: OrderBook) -> Price | None:
        """The best price on `book_side` with size our own paper takes haven't already consumed."""
        for price, size in book.levels(book_side):
            if size > self._consumed.get((mid, book_side, price), 0):
                return price
        return None

    def _print(self, t: Trade, mid: MarketId, state: State, now_ms: int) -> list[Event]:
        """A print hits resting orders on the side its taker sold into (`taker_side="ask"` hits bids)."""
        side = "ask" if t.taker_side == "bid" else "bid"
        left = t.qty
        mine = [o for o in self._on.get(mid, ()) if o.req.side == side
                and (o.req.price >= t.price if side == "bid" else o.req.price <= t.price)]
        mine.sort(key=lambda o: (-o.req.price if side == "bid" else o.req.price, o.placed_ms))  # price, then time
        out: list[Event] = []
        for o in mine:
            if left <= 0:
                break
            if o.req.price == t.price:  # at our price: the queue ahead trades first
                eaten = min(left, max(0, o.queue_ahead))
                o.queue_ahead -= eaten
                left -= eaten
            q = min(left, o.remaining)
            if q > 0:
                left -= q
                self._maker(o, q, state, now_ms, out)
        return out

    def _maker(self, o: PaperOrder, qty: Qty, state: State, now_ms: int, out: list[Event]) -> None:
        fee = state.contracts[o.mid].fees.fee(o.req.price, qty, taker=False)
        out.append(self._fill(o.slot, o.req, o.order_id, o.mid, o.req.price, qty, fee, False, now_ms))
        o.filled += qty
        done = o.remaining == 0
        if done:
            del self.resting[o.req.client_order_id]
            self._drop(o)
        out.append(OrderUpdate(o.slot, "paper", o.req.client_order_id, o.order_id, o.mid, o.req.side, o.req.price,
                               o.req.qty, o.filled, "filled" if done else "open", now_ms))
