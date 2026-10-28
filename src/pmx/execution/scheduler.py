"""The order scheduler (ARCHITECTURE *Order scheduler*): every order write — paper or live — waits
here between risk and the broker, under the venue's write limit that all slots share (Kalshi basic: 100
tokens/s; a create costs 10, a cancel 2 — `GET /account/endpoint_costs` — so ≈ 10 creates or 50 cancels a second).

Cancels go first, then orders that reduce a position, then new risk; within each, slots take turns, so
one busy slot can't starve the rest. A take still queued after `take_ttl_ms` expires instead of trading
on an old decision. A 429 halves the rate, which then recovers by 10% a second. Time is engine time, so
a backtest is throttled exactly like a live session and stays deterministic."""

from __future__ import annotations

from collections import OrderedDict, deque
from typing import Literal

CANCEL, REDUCE, NEW = 0, 1, 2  # priorities, highest first


class Write:
    __slots__ = ("kind", "slot", "client_order_id", "priority", "queued_ms", "expires_ms")

    def __init__(self, kind: Literal["place", "cancel"], slot: str, client_order_id: str, priority: int,
                 queued_ms: int, expires_ms: int | None = None) -> None:
        self.kind, self.slot, self.client_order_id = kind, slot, client_order_id
        self.priority, self.queued_ms, self.expires_ms = priority, queued_ms, expires_ms

    def __repr__(self) -> str:
        return f"Write({self.kind} {self.client_order_id} p{self.priority})"


class OrderScheduler:
    def __init__(self, *, rate: float = 100.0, burst: float = 100.0, cost: float = 10.0, cancel_cost: float = 2.0,
                 take_ttl_ms: int = 1_000) -> None:
        self.base_rate, self.rate, self.burst, self.cost, self.cancel_cost = rate, rate, burst, cost, cancel_cost
        self.take_ttl_ms = take_ttl_ms
        self.tokens = burst
        self.sent = 0
        self.expired = 0
        self._last_ms: int | None = None
        self._queues: list[OrderedDict[str, deque[Write]]] = [OrderedDict(), OrderedDict(), OrderedDict()]
        self._where: dict[str, Write] = {}  # client order id -> its queued write (one per order)
        self._expiring: dict[str, Write] = {}  # the queued writes with a deadline (takes)

    def __len__(self) -> int:
        return len(self._where)

    def submit(self, w: Write) -> None:
        """Queue a write. A cancel replaces the order's queued place (the order was never sent)."""
        prev = self._where.get(w.client_order_id)
        if prev is not None:
            self._remove(prev)
        self._where[w.client_order_id] = w
        if w.expires_ms is not None:
            self._expiring[w.client_order_id] = w
        self._queues[w.priority].setdefault(w.slot, deque()).append(w)

    def drop(self, client_order_id: str) -> bool:
        """Remove an order's queued write; True if there was one (it was never sent)."""
        w = self._where.pop(client_order_id, None)
        self._expiring.pop(client_order_id, None)
        if w is None:
            return False
        self._remove(w, indexed=False)
        return True

    def _remove(self, w: Write, *, indexed: bool = True) -> None:
        """Take a write out of line. Its slot keeps its turn, so a quote replaced before it was sent
        doesn't send the slot to the back (an empty turn is skipped when it comes up)."""
        if indexed:
            self._where.pop(w.client_order_id, None)
        self._expiring.pop(w.client_order_id, None)
        queue = self._queues[w.priority].get(w.slot)
        if queue is not None:
            queue.remove(w)

    def queued(self, client_order_id: str) -> bool:
        return client_order_id in self._where

    def throttled(self, slot: str) -> bool:
        """The slot has writes waiting for capacity (`m.throttled`)."""
        return any(q.get(slot) for q in self._queues)

    def due(self, now_ms: int) -> tuple[list[Write], list[Write]]:
        """(writes to send now, takes that expired while queued)."""
        self._refill(now_ms)
        expired = [w for w in self._expiring.values() if w.expires_ms <= now_ms]  # type: ignore[operator]
        for w in expired:
            self._remove(w)
        self.expired += len(expired)
        out: list[Write] = []
        while self._where:
            cost = self.cancel_cost if self._peek().kind == "cancel" else self.cost
            if self.tokens < cost:
                break
            w = self._next()
            del self._where[w.client_order_id]
            self._expiring.pop(w.client_order_id, None)
            self.tokens -= cost
            out.append(w)
        self.sent += len(out)
        return out, expired

    def _peek(self) -> Write:
        """The write `_next` would take, left in line."""
        for q in self._queues:
            for queue in q.values():
                if queue:
                    return queue[0]
        raise AssertionError("no queued write")

    def _next(self) -> Write:
        """The next write: highest priority first, slots in turn within it."""
        for q in self._queues:
            while q:
                slot, queue = next(iter(q.items()))
                if not queue:
                    del q[slot]  # an empty turn
                    continue
                w = queue.popleft()
                if queue:
                    q.move_to_end(slot)  # its next write waits for the other slots' turns
                else:
                    del q[slot]
                return w
        raise AssertionError("no queued write")

    def rate_limited(self, now_ms: int) -> None:
        """The venue answered 429: halve the rate and drop what's left of the bucket."""
        self._refill(now_ms)
        self.rate = max(self.cost, self.rate / 2)
        self.tokens = 0.0

    def _refill(self, now_ms: int) -> None:
        last, self._last_ms = self._last_ms, now_ms
        if last is None or now_ms <= last:
            return
        dt = (now_ms - last) / 1000
        self.tokens = min(self.burst, self.tokens + dt * self.rate)
        if self.rate < self.base_rate:
            self.rate = min(self.base_rate, self.rate * (1 + 0.1 * dt))
