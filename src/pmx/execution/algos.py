"""Execution helpers (ARCHITECTURE *Strategies* → *Target positions*). A strategy says what position it
wants on a market — `m.target(qty, style=…)` — and a `TargetWorker` works it with the same `quote` and `take`
intents a strategy would send, so risk, the order scheduler, and the ledger see nothing new. The router runs a
slot's workers when their market's book or prints change, on the slot's own fills, order updates, and rejects,
and on timers; never while the slot is warming, halted, or failed.

Styles:
- `passive`: rest what's left at the best price on our side — others' best: in live mode the venue's book holds
  our own orders, which don't count.
- `improve`: `improve_ticks` (default 1) inside that, never crossing the other side.
- either with `cross_after_s`: once that long has passed since the target was set, take what's left at the best
  opposite price, or with `max_cross_c` sweep up to that many cents worse than the mid, the taker fee included.
- `expires_s`: after that, pull and stop.

A worker waits — and says why in the slot's `last_action` — while the slot is throttled, its market has no book,
one of the slot's schedule windows is open, or for `REJECT_BACKOFF_MS` after a risk reject. A new target replaces
the old (its clock restarts). Don't also quote or take by hand on a market you target."""

from __future__ import annotations

import math
from typing import Any

from pmx.core.book import OrderBook
from pmx.core.types import BookSide, PriceGrid
from pmx.core.units import ONE_DOLLAR, Price, Qty, cents
from pmx.strategies.base import MarketContext, Target

OPTS = ("improve_ticks", "cross_after_s", "max_cross_c", "expires_s")
STYLES = ("passive", "improve")
REJECT_BACKOFF_MS = 5_000
TAKE_EVERY_MS = 1_000  # between IOC takes: one that didn't fill isn't resent on every book change
_IN_FLIGHT = frozenset({"queued", "pending"})


def _q(q: Qty) -> str:
    return f"{q / 100:+g}"


class TargetWorker:
    __slots__ = ("ticker", "qty", "style", "improve", "cross_after_ms", "max_cross", "expires_ms", "set_ms",
                 "blocked_until", "took_ms", "progress", "done")

    def __init__(self, t: Target, now_ms: int) -> None:
        unknown = sorted(set(t.opts) - set(OPTS))
        if unknown:
            raise ValueError(f"{t.slot}: unknown target option(s) {', '.join(unknown)} (known: {', '.join(OPTS)})")
        if t.style not in STYLES:
            raise ValueError(f"{t.slot}: target style {t.style!r} isn't one of {', '.join(STYLES)}")
        o: dict[str, Any] = t.opts
        self.ticker, self.qty, self.style = t.ticker, t.qty, t.style
        self.improve = int(o.get("improve_ticks", 1 if t.style == "improve" else 0))
        if self.improve < 0 or (t.style == "passive" and self.improve):
            raise ValueError(f"{t.slot}: improve_ticks needs style='improve' and a count ≥ 0")
        after, cross, expires = o.get("cross_after_s"), o.get("max_cross_c"), o.get("expires_s")
        if cross is not None and after is None:
            raise ValueError(f"{t.slot}: max_cross_c bounds crossing, which needs cross_after_s")
        self.cross_after_ms = None if after is None else round(float(after) * 1000)
        self.max_cross: Price | None = None if cross is None else round(float(cross) * 100)  # cents → Price
        self.expires_ms = None if expires is None else round(float(expires) * 1000)
        self.set_ms = now_ms
        self.blocked_until = 0
        self.took_ms: int | None = None
        self.progress = f"target {_q(t.qty)} ({t.style})"
        self.done = False

    def rejected(self, rule: str, now_ms: int) -> None:
        self.blocked_until = now_ms + REJECT_BACKOFF_MS
        self.progress = f"target {_q(self.qty)}: rejected ({rule}), retrying in {REJECT_BACKOFF_MS // 1000} s"

    def step(self, m: MarketContext, paused: str | None = None) -> None:
        """Move toward the target; `paused` is why not now (an open schedule window), if anything."""
        if self.done:
            return
        now = m.now_ms
        have = m.position
        head = f"target {_q(self.qty)}, have {_q(have)}"
        if self.expires_ms is not None and now >= self.set_ms + self.expires_ms:
            self._pull(m)
            self.done, self.progress = True, f"{head}: expired"
            return
        need = self.qty - have
        if not need:
            self._pull(m)
            self.progress = f"{head}: reached"
            return
        if now < self.blocked_until:
            return
        book = m.book
        why = paused or ("no book" if book is None else "throttled" if m.throttled else None)
        if why is not None:
            if book is None or paused:
                self._pull(m)
            self.progress = f"{head}: waiting ({why})"
            return
        assert book is not None
        side: BookSide = "bid" if need > 0 else "ask"
        size = abs(need)
        grid = m.contract.grid
        if self.cross_after_ms is not None and now >= self.set_ms + self.cross_after_ms:
            limit = self._cross_limit(m, book, side, grid)
            if limit is not None:
                if any(o.status in _IN_FLIGHT for o in m.working) or (
                        self.took_ms is not None and now - self.took_ms < TAKE_EVERY_MS):
                    return  # one take at a time: what's in flight may still fill
                self._pull(m)
                m.take(side, limit, size)
                self.took_ms = now
                self.progress = f"{head}: taking {size / 100:g} ≤ {cents(limit)}" if side == "bid" else \
                    f"{head}: taking {size / 100:g} ≥ {cents(limit)}"
                return
        price = self._resting_price(m, book, side, grid)
        if price is None:
            self.progress = f"{head}: waiting (no price on either side)"
            return
        resting = [o for o in m.working if o.side == side]
        if not (len(resting) == 1 and resting[0].price == price and resting[0].qty - resting[0].filled == size):
            m.quote(bid=(price, size) if side == "bid" else None, ask=(price, size) if side == "ask" else None)
        self.progress = f"{head}, resting {size / 100:g} @ {cents(price)}"

    # ---- prices -------------------------------------------------------------------------------------------

    def _resting_price(self, m: MarketContext, book: OrderBook, side: BookSide, grid: PriceGrid) -> Price | None:
        """Others' best on our side (`improve` ticks inside it), never touching the other side."""
        mine: dict[Price, Qty] = {}
        if m.mode == "live":  # paper orders never enter the venue's book; live ones do
            for o in m.working:
                if o.side == side:
                    mine[o.price] = mine.get(o.price, 0) + o.qty - o.filled
        best = next((p for p, q in book.levels(side) if q - mine.get(p, 0) > 0), None)
        other = book.best_ask() if side == "bid" else book.best_bid()
        opp = None if other is None else other[0]
        if best is None:
            if opp is None:
                return None
            best = grid.tick_below(opp) if side == "bid" else grid.tick_above(opp)
        price = best
        for _ in range(self.improve):
            nxt = grid.tick_above(price) if side == "bid" else grid.tick_below(price)
            if nxt == price or (opp is not None and (nxt >= opp if side == "bid" else nxt <= opp)):
                break
            price = nxt
        if opp is not None and (price >= opp if side == "bid" else price <= opp):
            return None
        return price

    def _cross_limit(self, m: MarketContext, book: OrderBook, side: BookSide, grid: PriceGrid) -> Price | None:
        """The IOC limit: the best opposite price, or with `max_cross_c` the worst price whose all-in cost (price ±
        the taker fee) is within that of the mid. None when nothing is takeable within it."""
        other = book.best_ask() if side == "bid" else book.best_bid()
        if other is None:
            return None
        if self.max_cross is None:
            return other[0]
        mid = book.mid()
        if mid is None:
            return None
        if side == "bid":
            bound = mid + self.max_cross
            limit = grid.snap(min(math.floor(bound), ONE_DOLLAR - 1))
            while limit > other[0] and limit + m.taker_fee(limit) > bound:
                limit = grid.tick_below(limit)
            return limit if limit >= other[0] and limit + m.taker_fee(limit) <= bound else None
        bound = mid - self.max_cross
        limit = grid.snap(max(math.ceil(bound), 1), up=True)
        while limit < other[0] and limit - m.taker_fee(limit) < bound:
            limit = grid.tick_above(limit)
        return limit if limit <= other[0] and limit - m.taker_fee(limit) >= bound else None

    @staticmethod
    def _pull(m: MarketContext) -> None:
        if m.working:  # resting quotes go; an IOC take in flight finishes on its own
            m.quote(bid=None, ask=None)
