"""The order manager (dispatch step 5): slot intents become orders.

On every event it drains the router's intents and turns them into writes:
- `Quote` declares the resting orders a slot wants on a market. Each side is diffed against the slot's
  working quote: unchanged price and size → nothing; anything else → cancel and place (post-only GTC).
  An order that's still queued is simply dropped, so a newer target replaces an unsent one.
- `Take` is one IOC order. `CancelAll` cancels the slot's working orders (on one market, or all).
- A control-channel `Command` places a manual order as slot `manual/<ticker>` (strategy `manual`, so
  `[manual]` risk limits apply), cancels, halts, or resumes, and answers with a `CommandResult`.
Each new order is checked for crossing one of our own working orders on that market in any slot
(`self_cross`, never sent), then by risk, then queued in the order scheduler, which releases writes
to the mode's broker as write capacity allows (cancels first). Brokers' fills and order updates come
back as engine events; the order manager records them in the ledger (orders, fills, settlements, with
attribution) and keeps each slot's positions and working orders, which it serves to the router
(`OrderView`), to risk (`Positions`), and as the paper balance (`Balance`).

Market pauses and closes cancel resting orders; a settled market settles every position in it at the
recorded price. Paper orders don't survive a restart: `start` cancels them in the ledger and
rebuilds positions from its fills. Live orders do: the live broker adopts an earlier run's resting ones
(`ADOPTED`), and a slot whose order has an unknown outcome gets no new orders (`unresolved`) until the broker
resolves it."""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable, Mapping
from typing import Any

from pmx.control import protocol
from pmx.core.position import AvgCost
from pmx.core.types import Contract, OrderRequest
from pmx.core.units import ONE_DOLLAR, Micros, Price, Qty, cents
from pmx.engine.events import (
    AccountSnapshot,
    Command,
    CommandResult,
    Event,
    LogEvent,
    Mode,
    OrderUpdate,
    RiskReject,
    SlotFill,
)
from pmx.engine.state import MarketId, State, market_id, venue_ticker
from pmx.execution.broker import ADOPTED, SyncBroker
from pmx.execution.risk import RiskManager
from pmx.execution.scheduler import CANCEL, NEW, REDUCE, OrderScheduler, Write
from pmx.ledger.db import NO_ATTRIBUTION, Attribution, Ledger
from pmx.strategies.base import CancelAll, Intent, Quote, Take, WorkingOrder

log = logging.getLogger(__name__)

MANUAL = "manual"  # the strategy id of orders placed by hand (`pmx order`)
FINAL = frozenset({"filled", "canceled", "rejected"})


class Order:
    __slots__ = ("req", "slot", "mode", "mid", "kind", "status", "order_id", "filled", "placed_ms", "canceling",
                 "unknown")

    def __init__(self, req: OrderRequest, slot: str, mode: Mode, mid: MarketId, kind: str, placed_ms: int) -> None:
        self.req, self.slot, self.mode, self.mid, self.kind = req, slot, mode, mid, kind
        self.status, self.order_id, self.filled, self.placed_ms = "queued", None, 0, placed_ms
        self.canceling = False
        self.unknown = False  # the broker can't say whether the venue has it (a 5xx, a timeout)

    @property
    def remaining(self) -> Qty:
        return self.req.qty - self.filled


def _in_scope(scope: str, slot: str) -> bool:
    return not scope or slot == scope or slot.startswith(scope + "/")


class OrderManager:
    def __init__(
        self,
        *,
        intents: Callable[[], list[Intent]] | None = None,
        slots: Mapping[str, Any] | None = None,
        risk: RiskManager | None = None,
        brokers: Mapping[str, SyncBroker] | None = None,
        scheduler: OrderScheduler | None = None,
        ledger: Ledger | None = None,
        instance: str | None = None,
        paper_cash: Micros = 1_000_000_000,
        attribution: Mapping[str, Attribution] | None = None,
    ) -> None:
        """`intents` is the router's `take_intents`; `slots` its slot table (for each slot's mode);
        `brokers` maps a mode to its broker; `instance` tags our client order ids (one per data dir);
        `paper_cash` is the paper account's starting balance; `attribution` is per strategy id."""
        self._intents, self.slots = intents, slots if slots is not None else {}
        self.risk, self.ledger = risk, ledger
        self.brokers = dict(brokers or {})
        self.scheduler = scheduler if scheduler is not None else OrderScheduler()
        self.instance = instance or os.urandom(3).hex()
        self.paper_cash = paper_cash
        self.attribution = dict(attribution or {})
        self.orders: dict[str, Order] = {}  # client order id -> live order (queued, pending, open)
        self._on: dict[MarketId, dict[str, Order]] = {}
        self._pos: dict[tuple[Mode, str, MarketId], AvgCost] = {}
        self._pos_on: dict[MarketId, dict[tuple[Mode, str], AvgCost]] = {}  # the same, by market
        self._open: dict[tuple[Mode, str], int] = {}  # working orders per (mode, scope): "", strategy, slot
        self._reserved: dict[tuple[Mode, MarketId], Micros] = {}  # paper collateral per market (`available`)
        self._reserved_total: dict[Mode, Micros] = {"paper": 0, "live": 0}
        self._stale_reserve: set[MarketId] = set()  # markets whose collateral changed since it was summed
        self._cash: dict[Mode, Micros] = {"paper": 0, "live": 0}  # YES-terms trade + settlement cash
        self._fees: dict[Mode, Micros] = {"paper": 0, "live": 0}
        self._fill_ids: set[tuple[str, str]] = set()
        self._settled: set[tuple[Mode, MarketId]] = set()
        self._live_balance: Micros | None = None
        self._live_balance_ms = 0
        self._live_shards: dict[int, Micros] = {}
        self._shard_of: dict[MarketId, int] = {}
        self._cancels: list[tuple[str, MarketId | None, str]] = []  # asked for by risk, done on the next pass
        self._new: list[Order] = []
        self._seq = 0
        if risk is not None:
            risk.on_cancel.append(self.cancel_scope)

    def cancel_scope(self, scope: str, ticker: MarketId | None, why: str) -> None:
        """Cancel every working order in scope (a slot, a strategy, "" = everything), on one market or all, on
        the next pass — risk's `on_cancel` hook (halts, window pulls)."""
        self._cancels.append((scope, ticker, why))

    def start(self, now_ms: int = 0) -> list[Event]:
        """After a restart: paper orders are gone, positions come back from the ledger's fills, and client order
        ids carry on from the last run's (they're unique per instance for good)."""
        out: list[Event] = []
        if self.ledger is None:
            return out
        self._seq = max(self._seq, self.ledger.last_seq(f"pmx-{self.instance}-"))
        n = self.ledger.cancel_open("paper", "paper orders don't survive a restart", now_ms)
        if n:
            out.append(LogEvent("info", "orders", f"canceled {n} paper orders left open by the last run", now_ms))
        for mode in ("paper", "live"):
            for slot, f in self.ledger.fills(mode):
                self._fill_ids.add((f.venue, f.fill_id))
                self._book(mode, slot, market_id(f.venue, f.ticker), f.side, f.qty, f.price, f.fee)
            for (venue, ticker), (price, _) in self.ledger.settlements(mode).items():
                self._settle(mode, market_id(venue, ticker), price)
        return out

    # ---- the engine's view ---------------------------------------------------------------------------

    def on_event(self, ev: Event, state: State, now_ms: int) -> list[Event]:
        out: list[Event] = []
        t = type(ev)
        if t is OrderUpdate:
            self._update(ev)
        elif t is SlotFill:
            self._fill(ev)
        elif t is Contract:
            self._contract(ev, now_ms, out)
        elif t is AccountSnapshot:
            self._live_balance, self._live_balance_ms, self._live_shards = ev.balance, ev.ts_ms, ev.shards
        elif t is Command:
            self._command(ev, state, now_ms, out)
        if self.risk is not None:
            out += self.risk.on_event(ev, state, now_ms)
        if self._intents is not None:
            for intent in self._intents():
                self._intent(intent, state, now_ms, out)
        while self._cancels:
            scope, ticker, why = self._cancels.pop(0)
            for o in [o for o in self.orders.values() if _in_scope(scope, o.slot) and ticker in (None, o.mid)]:
                self._cancel(o, why, now_ms, out)
        self._send(state, now_ms, out)
        for o in self._new:  # still waiting for write capacity: say so (`m.throttled`)
            if o.status == "queued" and o.req.client_order_id in self.orders:
                out.append(self._update_of(o, "queued", now_ms))
        self._new.clear()
        return out

    # ---- intents ------------------------------------------------------------------------------------------

    def _intent(self, i: Intent, state: State, now_ms: int, out: list[Event]) -> None:
        t = type(i)
        if t is Quote:
            self._quote(i.slot, i.ticker, "bid", i.bid, state, now_ms, out)
            self._quote(i.slot, i.ticker, "ask", i.ask, state, now_ms, out)
        elif t is Take:
            self._place(i.slot, i.ticker, i.side, i.limit, i.qty, "ioc", False, "take", state, now_ms, out)
        elif t is CancelAll:
            for o in [o for o in self.orders.values() if o.slot == i.slot and i.ticker in (None, o.mid)]:
                self._cancel(o, "cancel_all", now_ms, out)
        # A Target is worked by the router's execution helpers: what reaches here are its quotes and takes.

    def _quote(self, slot: str, mid: MarketId, side: str, level: tuple[Price, Qty] | None, state: State,
               now_ms: int, out: list[Event]) -> None:
        """Diff one side of a slot's quote against its working quote orders."""
        live = [o for o in self._on.get(mid, {}).values()
                if o.slot == slot and o.kind == "quote" and o.req.side == side and not o.canceling]
        keep = None
        if level is not None:
            keep = next((o for o in live if o.req.price == level[0] and o.remaining == level[1]), None)
        for o in live:
            if o is not keep:
                self._cancel(o, "requote", now_ms, out)
        if level is not None and keep is None:
            self._place(slot, mid, side, level[0], level[1], "gtc", True, "quote", state, now_ms, out)

    def _command(self, ev: Command, state: State, now_ms: int, out: list[Event]) -> None:
        r, coids, error = ev.request, [], ""
        if type(r) is protocol.Order:
            mid = market_id(r.venue, r.ticker)
            if r.mode == "live" and "live" not in self.brokers:
                error = "live orders need the engine started with `pmx run --live`"
            elif r.mode == "live" and not r.confirmed:
                error = "a live manual order must be confirmed"
            else:
                n = len(out)
                coid = self._place(f"{MANUAL}/{mid}", mid, r.side, r.price, r.qty, r.tif, r.post_only, MANUAL, state,
                                   now_ms, out, mode=r.mode)
                if coid is not None:
                    coids.append(coid)
                else:
                    why = next((e for e in reversed(out[n:]) if type(e) is RiskReject), None)
                    error = f"rejected: {why.rule} — {why.detail}" if why is not None else "rejected"
        elif type(r) is protocol.Cancel:
            if r.client_order_id:
                found = [o for o in self.orders.values() if o.req.client_order_id == r.client_order_id]
                if not found:
                    error = f"no working order {r.client_order_id}"
            elif r.ticker or r.all:
                mid = market_id("kalshi", r.ticker) if r.ticker else None
                found = [o for o in self.orders.values() if o.slot.startswith(MANUAL + "/") and mid in (None, o.mid)]
            else:
                found, error = [], "say which: an order id, a ticker, or all"
            for o in found:
                if r.mode is None or o.mode == r.mode:
                    coids.append(o.req.client_order_id)
                    self._cancel(o, "pmx cancel", now_ms, out)
        elif type(r) in (protocol.Halt, protocol.Resume):
            if self.risk is None:
                error = "no risk manager to halt"
            elif type(r) is protocol.Halt:
                self.risk.halt(r.reason, r.scope or None, now_ms, level="warn")
            else:
                self.risk.resume(r.scope or None, now_ms)
        else:
            error = f"{type(r).__name__} isn't for the order manager"
        out.append(CommandResult(ev.ref, not error, error, coids, now_ms))

    def _place(self, slot: str, mid: MarketId, side: str, price: Price, qty: Qty, tif: str, post_only: bool, kind: str,
               state: State, now_ms: int, out: list[Event], *, mode: Mode | None = None) -> str | None:
        """The new order's client order id, or None if it was rejected (a `RiskReject` in `out`)."""
        mode = mode or self._mode(slot)
        venue, ticker = venue_ticker(mid)
        self._seq += 1
        tag = hashlib.blake2b(slot.encode(), digest_size=3).hexdigest()
        req = OrderRequest(venue, ticker, side, price, qty, tif, post_only, f"pmx-{self.instance}-{tag}-{self._seq:x}",
                           slot)

        def reject(rule: str, detail: str) -> None:
            out.append(RiskReject(slot, mid, side, price, qty, rule, detail, now_ms))

        if mode not in self.brokers:
            return reject("no_broker", f"no {mode} broker running")
        if any(o.unknown for o in self.orders.values() if o.slot == slot):
            return reject("unresolved", "an earlier order's outcome is unknown until the broker reconciles")
        crossed = self._crossing(mid, side, price)
        if crossed is not None:
            return reject("self_cross", f"would cross our {crossed.req.side} at {cents(crossed.req.price)} "
                                        f"({crossed.slot})")
        if self.risk is not None and (r := self.risk.check(slot, req, state, now_ms, mode=mode)) is not None:
            out.append(r)
            return None
        o = Order(req, slot, mode, mid, kind, now_ms)
        self.orders[req.client_order_id] = o
        self._on.setdefault(mid, {})[req.client_order_id] = o
        self._counted(o, 1)
        self._stale_reserve.add(mid)
        self._new.append(o)
        priority = REDUCE if self._reduces(slot, mid, side, qty, mode) else NEW
        ttl = now_ms + self.scheduler.take_ttl_ms if tif == "ioc" else None
        self.scheduler.submit(Write("place", slot, req.client_order_id, priority, now_ms, ttl))
        return req.client_order_id

    def _cancel(self, o: Order, why: str, now_ms: int, out: list[Event]) -> None:
        if o.status == "queued" and self.scheduler.drop(o.req.client_order_id):  # never sent: just forget it
            self._done(o)
            out.append(self._update_of(o, "canceled", now_ms, why))
        elif not o.canceling and o.status not in FINAL:
            o.canceling = True
            self.scheduler.submit(Write("cancel", o.slot, o.req.client_order_id, CANCEL, now_ms))

    def _send(self, state: State, now_ms: int, out: list[Event]) -> None:
        writes, expired = self.scheduler.due(now_ms)
        for w in expired:
            o = self.orders.get(w.client_order_id)
            if o is not None:
                self._done(o)
                why = f"still queued after {self.scheduler.take_ttl_ms} ms"
                out.append(self._update_of(o, "canceled", now_ms, why))
        for w in writes:
            o = self.orders.get(w.client_order_id)
            if o is None:
                continue  # filled or canceled meanwhile: nothing to send
            broker = self.brokers[o.mode]
            if w.kind == "place":
                o.status = "pending"
                if self.ledger is not None:
                    self.ledger.record_order(o.req, o.mode, now_ms, attribution=self._attribution(o.slot))
                out += broker.place_now(o.req, o.slot, state, now_ms)
            else:
                out += broker.cancel_now(o.req.client_order_id, now_ms)

    # ---- what comes back ----------------------------------------------------------------------------------

    def _update(self, ev: OrderUpdate) -> None:
        o = self.orders.get(ev.client_order_id)
        if o is None and ev.status == "open" and ev.reason == ADOPTED:
            o = self._adopt(ev)
        if o is None or ev.status == "queued":
            return
        o.unknown = ev.status == "pending"
        o.status, o.filled = ev.status, ev.filled
        self._stale_reserve.add(o.mid)
        o.order_id = ev.order_id or o.order_id
        if self.ledger is not None:
            self.ledger.set_order_status(ev.client_order_id, ev.status, ev.reason or None, filled=ev.filled,
                                         order_id=ev.order_id, now_ms=ev.ts_ms)
        if ev.status in FINAL:
            self._done(o)

    def _adopt(self, ev: OrderUpdate) -> Order:
        """An earlier run's resting order, taken on by its slot as a working quote."""
        venue, ticker = venue_ticker(ev.ticker)
        req = OrderRequest(venue, ticker, ev.side, ev.price, ev.qty, "gtc", True, ev.client_order_id, ev.slot)
        o = Order(req, ev.slot, ev.mode, ev.ticker, "quote", ev.ts_ms)
        self.orders[req.client_order_id] = o
        self._on.setdefault(o.mid, {})[req.client_order_id] = o
        self._counted(o, 1)
        if self.ledger is not None and self.ledger.find_order(client_order_id=req.client_order_id) is None:
            self.ledger.record_order(req, ev.mode, ev.ts_ms, attribution=self._attribution(ev.slot), status="open")
        return o

    def _fill(self, ev: SlotFill) -> None:
        f = ev.fill
        key = (f.venue, f.fill_id)
        if key in self._fill_ids:
            return  # a duplicate (e.g. replayed after a reconnect)
        self._fill_ids.add(key)
        if self.ledger is not None and not self.ledger.record_fill(f, ev.mode, attribution=self._attribution(ev.slot)):
            return
        self._book(ev.mode, ev.slot, market_id(f.venue, f.ticker), f.side, f.qty, f.price, f.fee)

    def _book(self, mode: Mode, slot: str, mid: MarketId, side: str, qty: Qty, price: Price, fee: Micros) -> None:
        signed = qty if side == "bid" else -qty
        p = self._pos.get((mode, slot, mid))
        if p is None:
            p = self._pos[(mode, slot, mid)] = self._pos_on.setdefault(mid, {})[(mode, slot)] = AvgCost(mode)
        p.fill(signed, price, fee)
        self._stale_reserve.add(mid)
        self._cash[mode] -= signed * price
        self._fees[mode] += fee

    def _contract(self, c: Contract, now_ms: int, out: list[Event]) -> None:
        mid = market_id(c.venue, c.ticker)
        self._shard_of[mid] = c.shard
        if c.status != "open":
            for o in list(self._on.get(mid, {}).values()):
                self._cancel(o, f"market {c.status}", now_ms, out)
        price = c.settlement if c.settlement is not None else (
            ONE_DOLLAR if c.result == "yes" else 0 if c.result == "no" else None)
        if c.status != "settled" or price is None:
            return  # cash comes back when the market settles, not when it's determined
        for mode in ("paper", "live"):
            if (mode, mid) in self._settled:
                continue
            made = self._settle(mode, mid, price)
            if made is None:
                continue
            if self.ledger is not None:
                self.ledger.record_settlement(c.venue, c.ticker, price, mode, now_ms)
            out.append(LogEvent("info", "orders", f"{mid} settled at {cents(price)} ({mode}): {made / 1e6:+,.2f}",
                                now_ms))

    def _settle(self, mode: Mode, mid: MarketId, price: Price) -> Micros | None:
        self._settled.add((mode, mid))
        self._stale_reserve.add(mid)
        held = [(k, p) for k, p in self._pos_on.get(mid, {}).items() if k[0] == mode and p.qty]
        if not held:
            return None
        made = 0
        for _, p in held:
            self._cash[mode] += p.qty * price
            made += p.settle(price)
        return made

    def _counted(self, o: Order, n: int) -> None:
        for scope in ("", o.slot.split("/", 1)[0], o.slot):
            self._open[(o.mode, scope)] = self._open.get((o.mode, scope), 0) + n

    def _done(self, o: Order) -> None:
        if self.orders.pop(o.req.client_order_id, None) is not None:
            self._counted(o, -1)
            self._stale_reserve.add(o.mid)
        on = self._on.get(o.mid)
        if on is not None:
            on.pop(o.req.client_order_id, None)
            if not on:
                del self._on[o.mid]

    # ---- views: the router (OrderView), risk (Positions), the paper balance (Balance) -----------------------

    def position(self, scope: str, ticker: MarketId, mode: Mode | None = None) -> Qty:
        return sum(p.qty for (m, s), p in self._pos_on.get(ticker, {}).items()
                   if (mode is None or m == mode) and _in_scope(scope, s))

    def count_working(self, scope: str, mode: Mode | None = None) -> int:
        """How many orders are working in scope ("" = all, a strategy, or a slot) — what `max_open_orders` counts."""
        modes = (mode,) if mode is not None else ("paper", "live")
        return sum(self._open.get((m, scope), 0) for m in modes)

    def working(self, scope: str, ticker: MarketId | None = None, mode: Mode | None = None) -> list[WorkingOrder]:
        orders = self._on.get(ticker, {}).values() if ticker is not None else self.orders.values()
        out = []
        for o in orders:
            if _in_scope(scope, o.slot) and (mode is None or o.mode == mode):
                broker = self.brokers.get(o.mode)
                ahead = broker.queue_ahead(o.req.client_order_id) if broker is not None else None
                out.append(WorkingOrder(o.req.client_order_id, o.req.side, o.req.price, o.req.qty, o.filled, o.status,
                                        ahead, o.placed_ms))
        return out

    def throttled(self, slot: str) -> bool:
        return self.scheduler.throttled(slot)

    def pnl(self, scope: str, mode: Mode, state: State) -> Micros:
        """Realized + unrealized at mid − fees, since inception (a market with no book is valued at cost)."""
        total = 0
        for (m, s, t), p in self._pos.items():
            if m == mode and _in_scope(scope, s):
                book = state.book(t)
                mid = None if book is None else book.mid()
                total += p.realized - p.fees + (p.unrealized(mid) if mid is not None else 0)
        return total

    def available(self, mode: Mode, shard: int | None = None) -> Micros:
        """Cash not tied up as collateral: paper — starting cash + trade and settlement cash − fees, minus
        $1 per short (long NO) contract held, minus what resting orders tie up (bids price × qty, asks
        (1 − price) × the qty not covered by a long); live — the venue's last reported balance (`shard`'s, when
        the venue reports per shard: a market trades only its shard's money), minus what orders sent since then tie
        up."""
        if mode == "live":
            by_shard = shard is not None and bool(self._live_shards)
            since = sum(o.req.price * o.remaining if o.req.side == "bid" else (ONE_DOLLAR - o.req.price) * o.remaining
                        for o in self.orders.values() if o.mode == "live" and o.placed_ms >= self._live_balance_ms
                        and (not by_shard or self._shard_of.get(o.mid, 0) == shard))
            base = self._live_shards.get(shard, 0) if by_shard else (self._live_balance or 0)  # type: ignore[arg-type]
            return base - since
        for mid in self._stale_reserve:
            for m in ("paper", "live"):
                new = self._reserve(m, mid)
                self._reserved_total[m] += new - self._reserved.get((m, mid), 0)
                self._reserved[(m, mid)] = new
        self._stale_reserve.clear()
        return self.paper_cash + self._cash[mode] - self._fees[mode] - self._reserved_total[mode]

    def _reserve(self, mode: Mode, mid: MarketId) -> Micros:
        """One market's collateral: $1 per short contract held, plus what its resting orders tie up."""
        held = sum(p.qty for (m, _), p in self._pos_on.get(mid, {}).items() if m == mode)
        out = -held * ONE_DOLLAR if held < 0 else 0
        for o in self._on.get(mid, {}).values():
            if o.mode != mode:
                continue
            if o.req.side == "bid":
                out += o.req.price * o.remaining
            else:
                out += (ONE_DOLLAR - o.req.price) * max(0, o.remaining - max(0, held))
        return out

    # ---- helpers ------------------------------------------------------------------------------------------

    def _mode(self, slot: str) -> Mode:
        s = self.slots.get(slot)
        return s.mode if s is not None else "paper"

    def _attribution(self, slot: str) -> Attribution:
        return self.attribution.get(slot.split("/", 1)[0], NO_ATTRIBUTION)

    def _crossing(self, mid: MarketId, side: str, price: Price) -> Order | None:
        """One of our working orders on this market, in any slot, that an order at `price` would trade with."""
        for o in self._on.get(mid, {}).values():
            if o.canceling or o.req.side == side or o.req.tif == "ioc":  # a take never rests: nothing to trade with
                continue
            if (side == "bid" and o.req.price <= price) or (side == "ask" and o.req.price >= price):
                return o
        return None

    def _reduces(self, slot: str, mid: MarketId, side: str, qty: Qty, mode: Mode) -> bool:
        pos = self.position(slot, mid, mode)
        room = (-pos if side == "bid" else pos) - sum(o.remaining for o in self._on.get(mid, {}).values()
                                                     if o.slot == slot and o.req.side == side)
        return 0 < qty <= room

    def _update_of(self, o: Order, status: str, now_ms: int, reason: str = "") -> OrderUpdate:
        return OrderUpdate(o.slot, o.mode, o.req.client_order_id, o.order_id, o.mid, o.req.side, o.req.price,
                           o.req.qty, o.filled, status, now_ms, reason)
