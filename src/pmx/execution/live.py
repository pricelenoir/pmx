"""The live broker (ARCHITECTURE *Execution*): real orders through the paper broker's interface.

When the order scheduler releases a write, `place_now` / `cancel_now` start the venue call and return nothing;
the order stays `pending` in the order manager. What the venue answers comes back later as engine events
through `emit`: an `OrderUpdate` for each ack, cancel, and change the account stream reports (`VenueOrder`),
and a `SlotFill` for each fill of ours, attributed by client order id, else by the venue's order id (this run's,
or an earlier run's from the ledger). A fill nobody here placed is logged loudly and not booked.

A create that fails with a 5xx or a timeout may have executed: the order is reported `pending` (outcome
unknown), its slot gets no new orders, and reconciliation decides. Cancels are idempotent, so a failed cancel
is retried; one for an order the venue no longer has asks the venue what became of it.

Reconciliation runs at start and on every reconnect of the account stream: the venue's resting orders — this
instance's are synced, adopted (an earlier run's, when its strategy is still live here) or canceled (no live
strategy owns them); other pmx instances' are left alone with a warning; anyone else's are never touched —
then orders whose outcome is unknown or that finished unobserved, then the fills since the last one in the
ledger, then the balance and positions (so the live balance is known before the first order at start). Positions
are cross-checked against each `AccountSnapshot`; a mismatch seen twice is a loud warning.

`queue_ahead` estimates each resting order's place in line: the size displayed at its price when its ack
arrived, minus prints at that price since, floored at 0; a venue book reset forgets it until the next snapshot.
While orders rest, `run` corrects it every `queue_s` from the venue's own count (`queue_positions`).
Reported fees are compared with the series schedule; the venue's rounding to its balance precision (a
rounding fee, rebated across an order's fills) is within `FEE_SLACK` and not reported."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable, Container, Coroutine
from typing import Any

import httpx
import msgspec

from pmx.connectors.base import Venue
from pmx.connectors.kalshi.rest import KalshiError
from pmx.core.types import BookSnapshot, Fill, OrderRequest, StreamReset, Trade, VenueOrder
from pmx.core.units import Micros, Qty, cents, fmt_qty
from pmx.engine.events import AccountSnapshot, Event, LogEvent, Mode, OrderUpdate, SlotFill
from pmx.engine.state import MarketId, State, market_id
from pmx.execution.broker import ADOPTED
from pmx.ledger.db import Ledger

log = logging.getLogger(__name__)

FEE_SLACK: Micros = 100  # Kalshi rounds each fill's balance change to $0.0001 (a direct member's precision)
_STATUS = {"resting": "open", "executed": "filled", "canceled": "canceled"}
_FINAL = frozenset({"filled", "canceled", "rejected"})


class LiveOrder:
    __slots__ = ("req", "slot", "mid", "order_id", "filled", "booked", "status", "cancel", "unknown", "ahead",
                 "placed_ms")

    def __init__(self, req: OrderRequest, slot: str, placed_ms: int) -> None:
        self.req, self.slot, self.mid, self.placed_ms = req, slot, market_id(req.venue, req.ticker), placed_ms
        self.order_id: str | None = None
        self.filled: Qty = 0  # the most the venue or our fills say
        self.booked: Qty = 0  # fills received
        self.status = "pending"
        self.cancel = False  # wanted canceled before the venue gave us its id
        self.unknown = False  # the create's outcome is unknown until reconciliation
        self.ahead: Qty | None = None

    @property
    def final(self) -> bool:
        return self.status in _FINAL


class LiveBroker:
    mode: Mode = "live"
    keep = 10_000  # finished orders remembered for late fills
    cancel_tries = 4
    unknown_grace_ms = 5_000  # an unknown order missing from the venue's lists this soon may just not show yet

    def __init__(self, venue: Venue, *, instance: str, emit: Callable[[Event], None], clock: Callable[[], int],
                 ledger: Ledger | None = None, live: Container[str] = (),
                 positions: Callable[[MarketId], Qty] = lambda mid: 0) -> None:
        """`emit` queues an event into the engine; `live` holds the ids of strategies that trade live in this
        run (an earlier run's resting order is adopted only for one of them); `positions` is the order
        manager's live position in a market across slots."""
        self.venue, self.emit, self.clock, self.ledger = venue, emit, clock, ledger
        self.instance, self.prefix = instance, f"pmx-{instance}-"
        self.live, self.positions = live, positions
        self.orders: dict[str, LiveOrder] = {}  # client order id -> working or recently finished order
        self.by_id: dict[str, str] = {}  # venue order id -> client order id
        self.unattributed = 0
        self.reconciles = 0
        self._finished: deque[str] = deque()
        self._fill_ids: set[str] = set()
        self._tasks: set[asyncio.Task] = set()
        self._reconciling: asyncio.Task | None = None
        self._again = False
        self._recheck = False
        self._mismatch: dict[str, tuple[Qty, Qty]] = {}  # ticker -> (venue, ours) at the last snapshot
        self._warned: set[tuple[str, Qty, Qty]] = set()
        self._fee_warned: set[str] = set()

    # ---- writes (engine thread; never wait) -----------------------------------------------------------------

    def place_now(self, req: OrderRequest, slot: str, state: State, now_ms: int) -> list[Event]:
        o = self.orders[req.client_order_id] = LiveOrder(req, slot, now_ms)
        self._spawn(self._place(o))
        return []

    def cancel_now(self, client_order_id: str, now_ms: int) -> list[Event]:
        o = self.orders.get(client_order_id)
        if o is None or o.final:
            return []
        if o.order_id is None:
            o.cancel = True  # the venue hasn't said its id yet
        else:
            self._spawn(self._cancel(o))
        return []

    def queue_ahead(self, client_order_id: str) -> Qty | None:
        o = self.orders.get(client_order_id)
        return None if o is None or o.ahead is None else max(0, o.ahead)

    async def _place(self, o: LiveOrder) -> None:
        try:
            ack = await self.venue.place(o.req)
        except KalshiError as e:
            if e.status >= 500:
                self._unknown(o, f"kalshi {e.status}")
            else:  # refused (a 429 is one too: the client already retried it)
                self._set(o, "rejected", reason=f"kalshi {e.status}: {e.body[:200]}")
            return
        except httpx.HTTPError as e:
            self._unknown(o, type(e).__name__)
            return
        self._known(o, ack.order_id)
        if o.final:  # its fills (or the account stream) got here first
            return
        o.filled = max(o.filled, ack.filled)
        status = "open" if ack.remaining else "filled" if o.filled >= o.req.qty else "canceled"
        self._set(o, status, reason="IOC remainder" if status == "canceled" else "", ts_ms=ack.ts_ms)

    async def _cancel(self, o: LiveOrder) -> None:
        assert o.order_id is not None
        for attempt in range(self.cancel_tries):
            try:
                reduced = await self.venue.cancel(o.order_id, o.req.ticker)
            except KalshiError as e:
                if e.status == 404:  # gone already (filled, or canceled by the venue): ask what happened
                    await self._refresh(o)
                    return
                why = f"kalshi {e.status}"
            except httpx.HTTPError as e:
                why = type(e).__name__
            else:
                if not o.final:
                    o.filled = max(o.filled, o.req.qty - reduced)
                    self._set(o, "canceled")
                return
            log.warning("cancel of %s failed (%s), attempt %d", o.req.client_order_id, why, attempt + 1)
            await asyncio.sleep(0.5 * 2**attempt)
        self._log("error", f"couldn't cancel {o.slot} {o.req.client_order_id} after {self.cancel_tries} tries: "
                           "reconciling")
        self.reconcile_soon()

    async def _refresh(self, o: LiveOrder) -> None:
        assert o.order_id is not None
        try:
            self.emit(await self.venue.order(o.order_id))
        except (KalshiError, httpx.HTTPError) as e:
            self._log("warn", f"couldn't look up order {o.order_id}: {e}; reconciling")
            self.reconcile_soon()

    def _unknown(self, o: LiveOrder, why: str) -> None:
        o.unknown = True
        self._set(o, "pending", reason=f"outcome unknown ({why}): reconciling")
        self.reconcile_soon()

    # ---- the engine's broker step -----------------------------------------------------------------------------

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        t = type(ev)
        if t is Fill:
            return self._fill(ev, state, now_ms)
        if t is VenueOrder:
            self._venue_order(ev)
        elif t is Trade:
            mid = market_id(ev.venue, ev.ticker)
            side = "ask" if ev.taker_side == "bid" else "bid"  # the resting side it traded with
            for o in self.orders.values():
                if o.mid == mid and o.ahead is not None and o.req.side == side and o.req.price == ev.price:
                    o.ahead -= ev.qty
        elif t is OrderUpdate:
            o = self.orders.get(ev.client_order_id)
            if ev.mode == "live" and ev.status == "open" and o is not None and o.ahead is None:
                o.ahead = self._displayed(o, state)
        elif t is StreamReset:
            for o in self.orders.values():
                if o.req.venue == ev.venue:
                    o.ahead = None
        elif t is BookSnapshot:
            mid = market_id(ev.venue, ev.ticker)
            for o in self.orders.values():
                if o.mid == mid and o.ahead is None and o.status == "open":
                    o.ahead = self._displayed(o, state)
        elif t is AccountSnapshot:
            return self._cross_check(ev, now_ms)
        return []

    @staticmethod
    def _displayed(o: LiveOrder, state: State) -> Qty | None:
        book = state.book(o.mid)
        return None if book is None else (book.bids if o.req.side == "bid" else book.asks).get(o.req.price, 0)

    def _fill(self, f: Fill, state: State, now_ms: int) -> list[Event]:
        if f.fill_id in self._fill_ids:
            return []
        if self.ledger is not None and self.ledger.has_fill(f.venue, "live", f.fill_id):
            self._fill_ids.add(f.fill_id)  # an earlier run's, sent again by reconciliation: slots saw it then
            return []
        coid = f.client_order_id if f.client_order_id in self.orders else self.by_id.get(f.order_id)
        slot = None
        if coid is not None:
            slot = self.orders[coid].slot
        elif self.ledger is not None:
            row = (self.ledger.find_order(client_order_id=f.client_order_id) if f.client_order_id else None) \
                or self.ledger.find_order(order_id=f.order_id)
            if row is not None and row["mode"] == "live":
                coid, slot = row["client_order_id"], row["strategy"]
        if slot is None:
            self.unattributed += 1
            whose = "an earlier pmx order" if f.client_order_id.startswith(self.prefix) else "an order pmx didn't place"
            return [LogEvent("error", "live", f"fill {f.fill_id} on {f.ticker} ({f.side} {fmt_qty(f.qty)} @ "
                                              f"{cents(f.price)}) is for {whose}: not booked", now_ms)]
        self._fill_ids.add(f.fill_id)
        if f.client_order_id != coid:
            f = msgspec.structs.replace(f, client_order_id=coid)  # REST fills only carry the venue's order id
        out: list[Event] = [SlotFill(slot, "live", f)]
        c = state.contracts.get(market_id(f.venue, f.ticker))
        if c is not None and abs(f.fee - (want := c.fees.fee(f.price, f.qty, taker=f.is_taker))) > FEE_SLACK \
                and f.ticker not in self._fee_warned:
            self._fee_warned.add(f.ticker)
            out.append(LogEvent("warn", "live", f"{f.ticker}: fee ${f.fee / 1e6:.6f} for {fmt_qty(f.qty)} @ "
                                                f"{cents(f.price)} vs ${want / 1e6:.6f} from the series schedule "
                                                "(first difference on this market)", now_ms))
        o = self.orders.get(coid)
        if o is not None:
            self._known(o, f.order_id)  # a fill can beat the create's ack
            o.booked += f.qty
            if not o.final and o.booked > o.filled:  # ahead of the venue's last word on the order
                o.filled = min(o.req.qty, o.booked)
                if o.filled >= o.req.qty:
                    self._finish(o, "filled")
                out.append(self._update(o, o.status, now_ms))
        return out

    def _venue_order(self, v: VenueOrder) -> None:
        coid = v.client_order_id if v.client_order_id in self.orders else self.by_id.get(v.order_id)
        o = self.orders.get(coid) if coid is not None else None
        status = _STATUS.get(v.status)
        if o is None or status is None or o.final:
            return
        self._known(o, v.order_id)
        filled = max(o.filled, v.filled)
        if (status, filled) != (o.status, o.filled) or o.unknown:
            o.filled = filled
            self._set(o, status, reason=v.reason, ts_ms=v.ts_ms)

    def _cross_check(self, snap: AccountSnapshot, now_ms: int) -> list[Event]:
        """Venue positions vs ours, per market; a difference seen in two snapshots in a row is reported once."""
        venue = {p.ticker: p.qty for p in snap.positions if p.venue == self.venue.name}
        tickers = set(venue) | {t for t in self._mismatch}
        seen: dict[str, tuple[Qty, Qty]] = {}
        out: list[Event] = []
        for t in sorted(tickers | {o.req.ticker for o in self.orders.values()}):
            pair = (venue.get(t, 0), self.positions(market_id(self.venue.name, t)))
            if pair[0] == pair[1]:
                continue
            seen[t] = pair
            if self._mismatch.get(t) == pair and (t, *pair) not in self._warned:
                self._warned.add((t, *pair))
                out.append(LogEvent("warn", "live", f"{t}: the venue holds {fmt_qty(pair[0])}, pmx's live slots "
                                                    f"{fmt_qty(pair[1])} (orders placed outside pmx?)", now_ms))
        self._mismatch = seen
        return out

    # ---- reconciliation -------------------------------------------------------------------------------------

    def reconcile_soon(self) -> None:
        """Reconcile now, or again right after the one running."""
        if self._reconciling is not None and not self._reconciling.done():
            self._again = True
            return
        self._reconciling = self._spawn(self._reconcile_loop())

    async def _reconcile_loop(self) -> None:
        while True:
            self._again = False
            try:
                await self.reconcile()
            except (KalshiError, httpx.HTTPError) as e:
                self._log("error", f"reconciliation failed: {e}; trying again in 5 s")
                await asyncio.sleep(5)
                self._again = True
            if self._recheck:  # an unknown order too new for the venue's lists to show yet
                self._recheck = False
                await asyncio.sleep(self.unknown_grace_ms / 1000)
                self._again = True
            if not self._again:
                return

    async def reconcile(self, why: str = "") -> None:
        """What the venue says against what we think; results arrive as engine events. Raises on venue errors."""
        self.reconciles += 1
        resting = await self.venue.orders(status="resting")
        adopted, canceled, others, foreign = 0, 0, 0, 0
        on_venue = set()
        for v in resting:
            on_venue.add(v.client_order_id)
            if v.client_order_id in self.orders:
                self.emit(v)
            elif v.client_order_id.startswith(self.prefix):
                slot = self._owner(v)
                if slot is not None:
                    self._adopt(v, slot)
                    adopted += 1
                else:
                    self._spawn(self._cancel_stray(v))
                    canceled += 1
            elif v.client_order_id.startswith("pmx-"):
                others += 1
            else:
                foreign += 1
        lost = [o for o in self.orders.values()
                if not o.final and (o.unknown or o.order_id is not None) and o.req.client_order_id not in on_venue]
        if lost:
            recent = {v.client_order_id: v for v in
                      await self.venue.orders(since_ms=min(o.placed_ms for o in lost) - 60_000)}
            for o in lost:
                v = recent.get(o.req.client_order_id)
                if v is not None:
                    self.emit(v)
                elif o.unknown and self.clock() - o.placed_ms < self.unknown_grace_ms:
                    self._recheck = True  # Kalshi's order lists lag its writes by a second or two
                elif o.unknown:
                    o.unknown = False
                    self._set(o, "rejected", reason="never reached the venue")
                else:
                    self.emit(await self.venue.order(o.order_id))  # type: ignore[arg-type]
        if why == "start" and self.ledger is not None:
            await self._close_orphans(on_venue)
        since = self.ledger.last_fill_ms("live") if self.ledger is not None else None
        for f in await self.venue.fills(since_ms=None if since is None else since - 60_000):
            if f.fill_id not in self._fill_ids:
                self.emit(f)
        balance, shards = await self.venue.balances()
        self.emit(AccountSnapshot(balance, await self.venue.positions(), self.clock(), shards))
        parts = [f"{len(resting)} resting at the venue"]
        parts += [f"{n} {what}" for n, what in ((adopted, "adopted"), (canceled, "canceled (no live strategy owns "
                                                "them)"), (foreign, "not pmx's, left alone")) if n]
        self._log("info", f"reconciled{f' ({why})' if why else ''}: " + ", ".join(parts))
        if others:
            self._log("warn", f"{others} resting orders belong to another pmx instance on this account: left alone "
                              "(one engine per account)")

    async def _close_orphans(self, on_venue: set[str]) -> None:
        """An earlier run's live orders the ledger still has working, finished at the venue while no engine
        watched (it died): their final status from the venue's order history."""
        assert self.ledger is not None
        rows = [r for r in self.ledger.open_orders("live")
                if r["client_order_id"] not in on_venue and r["client_order_id"] not in self.orders]
        if not rows:
            return
        history = {v.client_order_id: v for v in await self.venue.orders(since_ms=min(r["created_ms"] for r in rows)
                                                                          - 60_000)}
        now = self.clock()
        for r in rows:
            v = history.get(r["client_order_id"])
            status = _STATUS.get(v.status, "canceled") if v is not None else "rejected"
            self.ledger.set_order_status(r["client_order_id"], status,  # type: ignore[arg-type]
                                         None if v is not None else "never reached the venue",
                                         filled=v.filled if v is not None else 0,
                                         order_id=v.order_id if v is not None else None, now_ms=now)
        self._log("info", f"closed {len(rows)} orders the last run left open in the ledger (finished at the venue)")

    def _owner(self, v: VenueOrder) -> str | None:
        """The slot an earlier run's order belongs to, if its strategy trades live in this run."""
        row = self.ledger.find_order(client_order_id=v.client_order_id) if self.ledger is not None else None
        slot = row["strategy"] if row is not None else None
        return slot if slot and slot.split("/", 1)[0] in self.live else None

    def _adopt(self, v: VenueOrder, slot: str) -> None:
        req = OrderRequest(v.venue, v.ticker, v.side, v.price, v.qty, "gtc", True, v.client_order_id, slot)
        o = self.orders[v.client_order_id] = LiveOrder(req, slot, v.ts_ms)
        self._known(o, v.order_id)
        o.filled = v.filled
        self._set(o, "open", reason=ADOPTED, ts_ms=v.ts_ms)

    async def _cancel_stray(self, v: VenueOrder) -> None:
        try:
            await self.venue.cancel(v.order_id, v.ticker)
        except (KalshiError, httpx.HTTPError) as e:
            self._log("error", f"couldn't cancel {v.client_order_id} (no live strategy owns it): {e}")

    # ---- the venue's queue positions -----------------------------------------------------------------------

    queue_s = 10.0

    async def run(self, stop: asyncio.Event) -> None:
        from pmx.sources.base import sleep_or_stop

        while not await sleep_or_stop(stop, self.queue_s):
            try:
                await self.refresh_queue()
            except (KalshiError, httpx.HTTPError) as e:
                log.info("queue positions unavailable: %s", e)

    async def refresh_queue(self) -> int:
        """Set each resting order's place in line from the venue; returns how many were known."""
        resting = {o.order_id: o for o in self.orders.values() if o.status == "open" and o.order_id}
        if not resting:
            return 0
        got = await self.venue.queue_positions(sorted({o.req.ticker for o in resting.values()}))
        for order_id, ahead in got.items():
            if (o := resting.get(order_id)) is not None:
                o.ahead = ahead
        return len(got)

    # ---- shutdown -------------------------------------------------------------------------------------------

    def working(self) -> list[LiveOrder]:
        return [o for o in self.orders.values() if not o.final]

    async def drain(self, timeout: float) -> None:
        """Wait (bounded) for venue calls in flight."""
        if self._tasks:
            await asyncio.wait(set(self._tasks), timeout=timeout)

    # ---- helpers ------------------------------------------------------------------------------------------------

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task:
        t = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._ended)
        return t

    def _ended(self, t: asyncio.Task) -> None:
        self._tasks.discard(t)
        if not t.cancelled() and (e := t.exception()) is not None:
            log.error("live broker task failed", exc_info=e)
            self._log("error", f"live broker: {type(e).__name__}: {e}")

    def _known(self, o: LiveOrder, order_id: str) -> None:
        if o.order_id is None:
            o.order_id = order_id
            self.by_id[order_id] = o.req.client_order_id
            if o.cancel:
                self._spawn(self._cancel(o))

    def _set(self, o: LiveOrder, status: str, *, reason: str = "", ts_ms: int | None = None) -> None:
        if status in _FINAL:
            self._finish(o, status)
        else:
            o.status = status
        if status != "pending":
            o.unknown = False
        self.emit(self._update(o, status, ts_ms or self.clock(), reason))

    def _finish(self, o: LiveOrder, status: str) -> None:
        o.status = status
        self._finished.append(o.req.client_order_id)
        while len(self._finished) > self.keep:
            old = self.orders.pop(self._finished.popleft(), None)
            if old is not None and old.order_id is not None:
                self.by_id.pop(old.order_id, None)

    def _update(self, o: LiveOrder, status: str, ts_ms: int, reason: str = "") -> OrderUpdate:
        return OrderUpdate(o.slot, "live", o.req.client_order_id, o.order_id, o.mid, o.req.side, o.req.price,
                           o.req.qty, o.filled, status, ts_ms, reason)  # type: ignore[arg-type]

    def _log(self, level: str, text: str) -> None:
        getattr(log, {"warn": "warning"}.get(level, level))(text)
        self.emit(LogEvent(level, "live", text, self.clock()))  # type: ignore[arg-type]
