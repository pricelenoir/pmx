"""The live broker against an in-memory venue — attribution, reconciliation, unknown outcomes, lifecycle races,
queue-ahead, cross-checks, and a live `pmx run` on demo end to end."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import msgspec
import pytest

from pmx.connectors.base import Control
from pmx.connectors.kalshi.decode import KalshiDecoder
from pmx.connectors.kalshi.rest import KalshiError
from pmx.core.fees import KALSHI_DEFAULT_FEES
from pmx.core.types import (
    BookSnapshot,
    Contract,
    Fill,
    OrderAck,
    OrderRequest,
    StreamReset,
    Trade,
    VenueOrder,
    VenuePosition,
)
from pmx.engine.clock import SimClock
from pmx.engine.engine import Engine
from pmx.engine.events import AccountSnapshot, LogEvent, OrderUpdate, RiskReject, SlotFill, Timer
from pmx.engine.state import State
from pmx.execution.broker import Brokers
from pmx.execution.live import LiveBroker
from pmx.execution.orders import OrderManager
from pmx.execution.paper import PaperBroker
from pmx.ledger.db import Ledger
from pmx.strategies.base import CancelAll, Quote, Take

T = "SER-EV1-A"
A, B = "a/" + T, "b/" + T


class FakeVenue:
    """Kalshi as far as the live broker can tell: orders rest until canceled or filled by `fill`; `place_errors`
    and `cancel_errors` script failures (`("exec", e)`: the order is made, then `e` raised anyway)."""

    name = "kalshi"

    def __init__(self) -> None:
        self.known: dict[str, VenueOrder] = {}  # client order id -> the venue's latest
        self.calls: list[tuple[str, str]] = []
        self.place_errors: list[tuple[str, Exception]] = []
        self.cancel_errors: list[Exception] = []
        self.hold: asyncio.Event | None = None
        self.fills_made: list[Fill] = []
        self.held: list[VenuePosition] = []
        self.signer = object()
        self.rest = SimpleNamespace(on_429=[])
        self._n = 0

    def rest_order(self, coid: str, *, ticker: str = T, side: str = "bid", price: int = 3_800, qty: int = 100) -> None:
        self._n += 1
        self.known[coid] = VenueOrder("kalshi", f"V{self._n}", coid, ticker, side, price, qty, 0, qty, "resting", 1)

    def fill(self, coid: str, qty: int, *, with_coid: bool = True, fee: int | None = None) -> Fill:
        v = self.known[coid]
        rem = v.remaining - qty
        self.known[coid] = msgspec.structs.replace(v, filled=v.filled + qty, remaining=rem,
                                                   status="executed" if rem == 0 else v.status)
        self._n += 1
        f = Fill("kalshi", f"F{self._n}", v.order_id, v.ticker, v.side, v.price, qty,
                 KALSHI_DEFAULT_FEES.fee(v.price, qty, taker=False) if fee is None else fee, False, 7,
                 coid if with_coid else "")
        self.fills_made.append(f)
        return f

    async def place(self, req: OrderRequest) -> OrderAck:
        self.calls.append(("place", req.client_order_id))
        if self.hold is not None:
            await self.hold.wait()
        how, err = self.place_errors.pop(0) if self.place_errors else ("ok", None)
        if how in ("ok", "exec"):
            self.rest_order(req.client_order_id, ticker=req.ticker, side=req.side, price=req.price, qty=req.qty)
            if req.tif == "ioc":  # nothing to take here: the rest is canceled
                v = self.known[req.client_order_id]
                self.known[req.client_order_id] = msgspec.structs.replace(v, remaining=0, status="canceled")
        if err is not None:
            raise err
        v = self.known[req.client_order_id]
        return OrderAck("kalshi", v.order_id, req.client_order_id, 0, v.remaining, 5)

    async def cancel(self, order_id: str, ticker: str) -> int:
        self.calls.append(("cancel", order_id))
        if self.cancel_errors:
            raise self.cancel_errors.pop(0)
        v = next((v for v in self.known.values() if v.order_id == order_id), None)
        if v is None or v.status != "resting":
            raise KalshiError(404, '{"code":"not_found"}')
        self.known[v.client_order_id] = msgspec.structs.replace(v, remaining=0, status="canceled")
        return v.remaining

    async def orders(self, status: str | None = None, since_ms: int | None = None) -> list[VenueOrder]:
        self.calls.append(("orders", status or "any"))
        return [v for v in self.known.values() if status is None or v.status == status]

    async def order(self, order_id: str) -> VenueOrder:
        self.calls.append(("order", order_id))
        return next(v for v in self.known.values() if v.order_id == order_id)

    async def fills(self, since_ms: int | None = None) -> list[Fill]:
        return list(self.fills_made)

    async def queue_positions(self, tickers: list[str]) -> dict[str, int]:
        return {v.order_id: 12_300 for v in self.known.values() if v.status == "resting" and v.ticker in tickers}

    async def positions(self) -> list[VenuePosition]:
        return list(self.held)

    async def balance(self) -> int:
        return 50_000_000

    async def balances(self) -> tuple[int, dict[int, int]]:
        return 50_000_000, {0: 50_000_000, 2: 0}

    def stream(self, tickers, **kw):
        return _Stream()

    def decoder(self) -> KalshiDecoder:
        return KalshiDecoder()


class _Stream:
    def __init__(self) -> None:
        self.closed = asyncio.Event()

    async def __aiter__(self):
        yield Control(1, "connected", {})
        await self.closed.wait()

    async def close(self) -> None:
        self.closed.set()


class Seen:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.events.append(ev)

    def of(self, t: type) -> list:
        return [e for e in self.events if type(e) is t]


class Slot:
    def __init__(self, id: str) -> None:
        self.id, self.mode, self.model, self.inputs, self.risk_limits = id, "live", None, (), None
        self.warming, self.tickers = False, {T: None}


class Rig:
    """The engine with the paper and live brokers and an order manager fed intents by hand."""

    def __init__(self, *, ledger: Ledger | None = None, live: set[str] | None = None) -> None:
        self.clock = SimClock(1_000_000)
        self.venue = FakeVenue()
        self.ledger = ledger or Ledger(":memory:", env="demo")
        self.pending: list = []
        self.live = LiveBroker(self.venue, instance="t1", emit=lambda ev: self.engine.emit(ev, "live"),
                               clock=self.clock.now_ms, ledger=self.ledger, live={"a", "b"} if live is None else live)
        self.om = OrderManager(intents=self._take, slots={A: Slot(A), B: Slot(B)}, ledger=self.ledger, instance="t1",
                               brokers={"paper": PaperBroker(), "live": self.live})
        self.live.positions = lambda mid: self.om.position("", mid, "live")
        self.seen = Seen()
        self.engine = Engine(self.clock, State(), broker=Brokers(self.om.brokers["paper"], self.live),
                             order_manager=self.om, publisher=self.seen)
        self.send(Contract("kalshi", T, "SER-EV1", "Title", "Sub", "open", None))
        self.send(BookSnapshot("kalshi", T, ((4_000, 1_000), (3_800, 700)), ((4_200, 1_000),), 0))

    def _take(self) -> list:
        out, self.pending = self.pending, []
        return out

    def send(self, ev) -> None:
        self.engine.dispatch(ev, self.clock.now_ms())

    async def settle(self) -> None:
        """Let venue calls finish and what they report dispatch."""
        for _ in range(20):
            await asyncio.sleep(0)
            await self.engine.dispatch_all()

    async def act(self, *intents) -> None:
        self.pending += intents
        self.send(Timer(self.clock.now_ms()))
        await self.settle()

    async def account(self, ev) -> None:
        """What the account stream (or a REST page) delivers."""
        self.engine.emit(ev, "account")
        await self.settle()

    def updates(self, coid: str | None = None) -> list[str]:
        return [e.status for e in self.seen.of(OrderUpdate) if coid is None or e.client_order_id == coid]


def bid(slot=A, price=3_800, qty=100) -> Quote:
    return Quote(slot, T, (price, qty), None)


def run(coro):
    return asyncio.run(coro)


# ---- writes, fills, attribution ----------------------------------------------------------------------------------


def test_orders_rest_fill_and_cancel_through_the_venue_with_fills_attributed_to_slots():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        assert w.status == "open" and r.venue.calls == [("place", w.client_order_id)]
        f = r.venue.fill(w.client_order_id, 40)
        await r.account(f)
        await r.account(f)  # replayed after a reconnect: once
        assert r.om.position(A, T, "live") == 40 and r.om.working(A, T)[0].filled == 40
        await r.act(CancelAll(A))
        assert r.updates(w.client_order_id)[-1] == "canceled" and r.om.working(A) == []
        late = r.venue.fill(w.client_order_id, 10)  # a fill the venue made before it saw the cancel
        await r.account(late)
        assert r.om.position(A, T, "live") == 50, "a fill after the cancel still counts, for the right slot"
        return r

    r = run(go())
    assert [(slot, f.fill_id) for slot, f in r.ledger.fills("live")] == [(A, f.fill_id) for f in r.venue.fills_made]
    row = r.ledger.find_order(client_order_id=r.venue.fills_made[0].client_order_id)
    assert (row["status"], row["order_id"], row["mode"]) == ("canceled", "V1", "live")


def test_fills_without_our_id_attribute_by_the_venues_order_id_and_strangers_are_not_booked():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        await r.account(r.venue.fill(w.client_order_id, 100, with_coid=False))  # a REST page's fill
        assert r.om.position(A, T, "live") == 100 and r.updates(w.client_order_id)[-1] == "filled"
        r.venue.rest_order("someone-else-1")
        await r.account(r.venue.fill("someone-else-1", 5))
        assert r.om.position("", T, "live") == 100 and r.live.unattributed == 1
        return r

    r = run(go())
    [(slot, f)] = list(r.ledger.fills("live"))
    assert slot == A and f.client_order_id.startswith("pmx-t1-"), "the ledger joins it to its order"
    errors = [e.text for e in r.seen.of(LogEvent) if e.level == "error"]
    assert any("an order pmx didn't place: not booked" in t for t in errors)


def test_a_cancel_before_the_ack_goes_out_once_the_venue_names_the_order():
    async def go() -> Rig:
        r = Rig()
        r.venue.hold = asyncio.Event()
        await r.act(bid())
        [w] = r.om.working(A, T)
        assert w.status == "pending"
        await r.act(CancelAll(A))
        assert [c for c, _ in r.venue.calls] == ["place"], "nothing to cancel yet"
        r.venue.hold.set()
        await r.settle()
        assert [c for c, _ in r.venue.calls] == ["place", "cancel"]
        assert r.updates(w.client_order_id)[-2:] == ["open", "canceled"] and r.om.working(A) == []
        return r

    run(go())


def test_a_cancel_for_an_order_the_venue_already_filled_asks_what_happened():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        r.venue.fill(w.client_order_id, 100)  # filled at the venue; the fill message hasn't arrived
        await r.act(CancelAll(A))
        assert ("order", "V1") in r.venue.calls and r.updates(w.client_order_id)[-1] == "filled"
        await r.account(r.venue.fills_made[0])
        assert r.om.position(A, T, "live") == 100
        return r

    run(go())


def test_failed_cancels_are_retried(monkeypatch):
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        r.venue.cancel_errors = [KalshiError(503, "busy"), httpx.ReadTimeout("slow")]
        monkeypatch.setattr(r.live, "cancel_tries", 3)
        real_sleep = asyncio.sleep
        monkeypatch.setattr(asyncio, "sleep", lambda s: real_sleep(0))
        await r.act(CancelAll(A))
        await r.settle()
        assert [c for c, _ in r.venue.calls].count("cancel") == 3 and r.updates(w.client_order_id)[-1] == "canceled"
        return r

    run(go())


# ---- unknown outcomes --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("how", "resolved"), [("exec", "open"), ("lost", "rejected")])
def test_a_5xx_on_create_blocks_the_slot_until_reconciliation_says_what_happened(how, resolved):
    async def go() -> Rig:
        r = Rig()
        hold = asyncio.Event()
        real = r.live.reconcile

        async def later(why: str = "") -> None:
            await hold.wait()
            await real(why)

        r.live.reconcile = later  # type: ignore[method-assign]
        r.live.unknown_grace_ms = 30  # Kalshi's lists can lag its writes: an order this new isn't called lost
        r.venue.place_errors = [(how, KalshiError(503, "unavailable"))]
        await r.act(bid())
        [w] = r.om.working(A, T)
        assert w.status == "pending" and r.updates(w.client_order_id) == ["pending"]
        await r.act(Take(A, T, "bid", 4_200, 100))
        assert r.seen.of(RiskReject)[-1].rule == "unresolved", "no new orders while one's outcome is unknown"
        await r.act(bid(slot=B))
        assert r.om.working(B, T), "other slots trade on"
        hold.set()
        await r.settle()
        if how == "lost":
            assert r.updates(w.client_order_id)[-1] == "pending", "too new to call lost: looked at again"
            r.clock.advance(r.clock.now_ms() + 50)
            await asyncio.sleep(0.05)
            await r.settle()
        assert r.updates(w.client_order_id)[-1] == resolved
        assert ("orders", "resting") in r.venue.calls
        await r.act(Take(A, T, "bid", 4_200, 100))
        assert r.seen.of(RiskReject)[-1].rule == "unresolved" and len(r.seen.of(RiskReject)) == 1, "unblocked"
        return r

    run(go())


def test_a_timeout_on_create_is_an_unknown_outcome_and_a_4xx_is_a_rejection():
    async def go() -> Rig:
        r = Rig()
        r.live.unknown_grace_ms = 0
        r.venue.place_errors = [("lost", httpx.ReadTimeout("slow")), ("lost", KalshiError(400, "bad price"))]
        await r.act(bid(), bid(slot=B, price=3_700))
        statuses = {e.slot: e.status for e in r.seen.of(OrderUpdate)}
        assert statuses == {A: "rejected", B: "rejected"}
        reasons = {e.slot: e.reason for e in r.seen.of(OrderUpdate)}
        assert reasons[A] == "never reached the venue" and reasons[B].startswith("kalshi 400")
        return r

    run(go())


# ---- reconciliation ----------------------------------------------------------------------------------------------


def test_reconcile_adopts_ours_cancels_strays_and_leaves_everyone_elses_alone(tmp_path):
    path = tmp_path / "ledger.sqlite"
    first = Ledger(path, env="demo")
    for coid, slot in (("pmx-t1-aaaaaa-1", A), ("pmx-t1-cccccc-2", "c/" + T)):
        first.record_order(OrderRequest("kalshi", T, "bid", 3_800, 100, "gtc", True, coid, slot), "live", 1)
    first.close()

    async def go() -> Rig:
        r = Rig(ledger=Ledger(path, env="demo"))
        v = r.venue
        v.rest_order("pmx-t1-aaaaaa-1")  # ours, its strategy trades live here: adopted
        v.rest_order("pmx-t1-cccccc-2")  # ours, its strategy doesn't: canceled
        v.rest_order("pmx-t1-dddddd-3")  # ours, not in the ledger: no owner, canceled
        v.rest_order("pmx-zz-aaaaaa-1")  # another pmx instance's
        v.rest_order("manual-1")  # placed by hand on Kalshi's site
        await r.live.reconcile("start")
        await r.settle()
        assert [(w.client_order_id, w.status) for w in r.om.working(A, T)] == [("pmx-t1-aaaaaa-1", "open")]
        assert {v.known[c].status for c in ("pmx-t1-cccccc-2", "pmx-t1-dddddd-3")} == {"canceled"}
        assert {v.known[c].status for c in ("pmx-zz-aaaaaa-1", "manual-1")} == {"resting"}
        texts = [e.text for e in r.seen.of(LogEvent)]
        assert any("1 adopted, 2 canceled" in t and "1 not pmx's" in t for t in texts)
        assert any("another pmx instance" in t for t in texts)
        await r.act(bid())  # the slot wants what it already has: kept, nothing sent
        assert [c for c, _ in v.calls if c in ("place", "cancel")] == ["cancel", "cancel"]
        await r.act(bid(price=3_900))  # and replaces it when it wants something else
        assert v.known["pmx-t1-aaaaaa-1"].status == "canceled" and r.om.working(A, T)[0].price == 3_900
        return r

    run(go())


def test_reconcile_books_fills_missed_while_away_and_settles_orders_that_finished_unseen():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid(), bid(slot=B, price=3_700))
        a, b = r.om.working(A, T)[0].client_order_id, r.om.working(B, T)[0].client_order_id
        r.venue.fill(a, 100, with_coid=False)  # while the account stream was down
        r.venue.known[b] = msgspec.structs.replace(r.venue.known[b], status="canceled", remaining=0)
        await r.live.reconcile("reconnect")
        await r.settle()
        assert r.om.position(A, T, "live") == 100 and r.updates(a)[-1] == "filled"
        assert r.updates(b)[-1] == "canceled" and r.om.working("") == []
        return r

    run(go())


def test_after_a_restart_fills_already_in_the_ledger_are_not_sent_to_slots_again(tmp_path):
    path = tmp_path / "ledger.sqlite"

    async def first() -> FakeVenue:
        r = Rig(ledger=Ledger(path, env="demo"))
        await r.act(Take(A, T, "bid", 4_200, 100))
        coid = r.venue.calls[0][1]
        r.venue.rest_order(coid)
        await r.account(r.venue.fill(coid, 100))
        r.ledger.close()
        return r.venue

    venue = run(first())

    async def second() -> Rig:
        r = Rig(ledger=Ledger(path, env="demo"))
        r.venue = r.live.venue = venue
        r.om.start(r.clock.now_ms())
        await r.live.reconcile("start")  # the venue's fill history includes the last run's fill
        await r.settle()
        assert r.seen.of(SlotFill) == [] and r.om.position(A, T, "live") == 100
        return r

    run(second())


def test_a_fill_that_beats_the_ack_still_records_the_venues_order_id():
    async def go() -> Rig:
        r = Rig()
        r.venue.hold = asyncio.Event()
        await r.act(Take(A, T, "bid", 4_200, 100))
        coid = r.venue.calls[0][1]
        r.venue.rest_order(coid, price=4_200)  # the venue made it and filled it; its REST reply is late
        await r.account(r.venue.fill(coid, 100))
        assert r.updates(coid)[-1] == "filled" and r.om.position(A, T, "live") == 100
        r.venue.hold.set()
        await r.settle()
        assert r.updates(coid).count("filled") == 1, "the late ack adds nothing"
        return r

    r = run(go())
    row = r.ledger.find_order(client_order_id=r.venue.calls[0][1])
    assert (row["status"], row["order_id"]) == ("filled", r.venue.fills_made[0].order_id)


def test_a_restart_closes_orders_the_dead_run_left_open_in_the_ledger(tmp_path):
    path = tmp_path / "ledger.sqlite"
    seed = Ledger(path, env="demo")
    for coid in ("pmx-t1-aaaaaa-1", "pmx-t1-aaaaaa-2", "pmx-t1-aaaaaa-3"):
        seed.record_order(OrderRequest("kalshi", T, "bid", 3_800, 100, "gtc", True, coid, A), "live", 1)
        seed.set_order_status(coid, "open", now_ms=1)
    seed.close()

    async def go() -> Rig:
        r = Rig(ledger=Ledger(path, env="demo"))
        v = r.venue
        v.rest_order("pmx-t1-aaaaaa-1")
        v.known["pmx-t1-aaaaaa-1"] = msgspec.structs.replace(v.known["pmx-t1-aaaaaa-1"], status="canceled",
                                                             remaining=0)  # pmx guard canceled it
        v.rest_order("pmx-t1-aaaaaa-2")
        v.fill("pmx-t1-aaaaaa-2", 100)  # filled while nothing watched
        await r.live.reconcile("start")  # -3 never reached the venue
        await r.settle()
        return r

    r = run(go())
    rows = {c: (st, f) for c, st, f in r.ledger.db.execute("SELECT client_order_id, status, filled FROM orders")}
    assert rows == {"pmx-t1-aaaaaa-1": ("canceled", 0), "pmx-t1-aaaaaa-2": ("filled", 100),
                    "pmx-t1-aaaaaa-3": ("rejected", 0)}
    assert r.ledger.open_orders("live") == [] and r.om.position(A, T, "live") == 100, "and its fill is booked"


def test_the_venue_canceling_an_order_reaches_its_slot_with_the_reason():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        v = msgspec.structs.replace(r.venue.known[w.client_order_id], status="canceled", remaining=0,
                                    reason="SelfTradeCancel")
        await r.account(v)
        await r.account(v)  # repeated: nothing new
        last = r.seen.of(OrderUpdate)[-1]
        assert (last.status, last.reason) == ("canceled", "SelfTradeCancel") and r.updates(w.client_order_id) == [
            "open", "canceled"]
        return r

    run(go())


# ---- estimates and checks ----------------------------------------------------------------------------------------


def test_queue_ahead_is_the_size_displayed_at_the_ack_less_prints_at_our_price():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        assert w.queue_ahead == 700, "700 displayed at 38¢ when the ack came"
        r.send(Trade("kalshi", "t1", T, 3_800, 300, "ask", 0))  # sold into the bids at our price
        r.send(Trade("kalshi", "t2", T, 3_800, 300, "bid", 0))  # bought from the asks: not our queue
        r.send(Trade("kalshi", "t3", T, 3_900, 300, "ask", 0))  # another price
        assert r.om.working(A, T)[0].queue_ahead == 400
        r.send(Trade("kalshi", "t4", T, 3_800, 900, "ask", 0))
        assert r.om.working(A, T)[0].queue_ahead == 0, "floored"
        r.send(StreamReset("kalshi", "gap", 0))
        assert r.om.working(A, T)[0].queue_ahead is None, "unknown after a book reset"
        r.send(BookSnapshot("kalshi", T, ((3_800, 250),), ((4_200, 1_000),), 0))
        assert r.om.working(A, T)[0].queue_ahead == 250
        assert await r.live.refresh_queue() == 1 and r.om.working(A, T)[0].queue_ahead == 12_300, "the venue's count"
        return r

    run(go())


def test_positions_are_cross_checked_and_fees_compared_with_the_schedule():
    async def go() -> Rig:
        r = Rig()
        await r.act(bid())
        [w] = r.om.working(A, T)
        await r.account(r.venue.fill(w.client_order_id, 50, fee=KALSHI_DEFAULT_FEES.fee(3_800, 50, taker=False) + 27))
        await r.account(r.venue.fill(w.client_order_id, 50, fee=123))  # the venue's rounding is within $0.0001
        held = [VenuePosition("kalshi", T, 300, 0, 0)]  # 200 more than pmx's slots hold
        for _ in range(3):
            await r.account(AccountSnapshot(0, held, r.clock.now_ms()))
        await r.account(AccountSnapshot(0, [VenuePosition("kalshi", T, 100, 0, 0)], r.clock.now_ms()))
        return r

    r = run(go())
    warns = [e.text for e in r.seen.of(LogEvent) if e.level == "warn"]
    assert sum("the venue holds 3.00, pmx's live slots 1.00" in t for t in warns) == 1, "seen twice, said once"
    assert sum("from the series schedule" in t for t in warns) == 1


def test_live_balance_is_the_venues_less_what_orders_sent_since_tie_up():
    async def go() -> Rig:
        r = Rig()
        r.send(AccountSnapshot(10_000_000, [], r.clock.now_ms()))
        await r.act(bid(price=4_000, qty=500))
        assert r.om.available("live") == 10_000_000 - 4_000 * 500
        r.clock.advance(r.clock.now_ms() + 1)
        r.send(AccountSnapshot(8_000_000, [], r.clock.now_ms()))
        assert r.om.available("live") == 8_000_000, "the venue's balance counts it now"
        return r

    run(go())


def test_live_balance_is_per_exchange_shard_when_the_venue_reports_it():
    async def go() -> Rig:
        r = Rig()
        r.send(Contract("kalshi", "SER-EV2-A", "SER-EV2", "T", "S", "open", None, shard=2))
        r.send(AccountSnapshot(10_000_000, [], r.clock.now_ms(), {0: 10_000_000, 2: 0}))
        assert r.om.available("live", 0) == 10_000_000 and r.om.available("live", 2) == 0
        assert r.om.available("live") == 10_000_000, "no shard asked: the total"
        return r

    run(go())


def test_429s_reach_the_hooks_that_slow_the_scheduler(monkeypatch):
    from pmx.connectors.kalshi.auth import KalshiSigner
    from pmx.connectors.kalshi.rest import KalshiRest

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    from cryptography.hazmat.primitives.asymmetric import ed25519

    n = {"calls": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        n["calls"] += 1
        return httpx.Response(429 if n["calls"] == 1 else 200, json={"order_id": "o", "reduced_by": "1.00", "ts_ms": 1})

    rest = KalshiRest("https://x/trade-api/v2", KalshiSigner("k", ed25519.Ed25519PrivateKey.generate()),
                      transport=httpx.MockTransport(handler), read=(1e9, 1e9), write=(1e9, 1e9))
    hits: list[int] = []
    rest.on_429.append(lambda: hits.append(1))
    asyncio.run(rest.cancel_order("o", T))
    assert hits == [1] and n["calls"] == 2
    n["calls"] = 0
    with pytest.raises(msgspec.ValidationError):
        asyncio.run(rest.balance())  # a read's 429 (this body isn't a balance): its own bucket, no hook
    assert hits == [1] and n["calls"] == 2


# ---- pmx run --live on demo --------------------------------------------------------------------------------------


def test_a_live_run_on_demo_reconciles_quotes_through_the_venue_and_cancels_everything_at_shutdown(tmp_path,
                                                                                                   monkeypatch):
    from pmx.engine.config import load
    from pmx.engine.run import System
    from test_run_config import RUN, FakeMarket, _until

    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    (data / "instance").write_text("t1\n")
    seed = Ledger(data / "ledger.sqlite", env="demo")
    seed.record_order(OrderRequest("kalshi", "KXFED-B", "bid", 3_800, 100, "gtc", True, "pmx-t1-bbbbbb-1",
                                   "smoke/KXFED-B"), "live", 1)
    seed.close()
    venue = FakeVenue()
    venue.rest_order("pmx-t1-bbbbbb-1", ticker="KXFED-B")  # the last run's bid, still resting
    venue.rest_order("manual-1", ticker="KXFED-A", price=1_000)
    text = RUN.replace('id = "smoke"\nkind = "scripted"\n', 'id = "smoke"\nkind = "scripted"\nmode = "live"\n')

    async def go() -> System:
        s = System(load(text=text), env="demo", sock=Path("r.sock"), data=data, market=FakeMarket(), venue=venue,
                   live=True, guard=False)  # a real guard would use the real demo key
        stop = asyncio.Event()
        task = asyncio.create_task(s.run(stop))
        await _until(lambda: len([o for o in s.orders.orders.values() if o.mode == "live" and o.status == "open"])
                     == 2, 10, "a bid on each market: one placed, one adopted")
        assert [c for c, _ in venue.calls if c == "place"] == ["place"], "the adopted one is kept"
        assert venue.rest.on_429, "429s slow the order scheduler"
        venue.rest.on_429[0]()
        assert s.orders.scheduler.rate < s.orders.scheduler.base_rate
        stop.set()
        await task
        return s

    s = asyncio.run(go())
    statuses = {c: v.status for c, v in venue.known.items()}
    placed = next(c for c in statuses if c.startswith("pmx-t1-") and c != "pmx-t1-bbbbbb-1")
    assert statuses == {"pmx-t1-bbbbbb-1": "canceled", placed: "canceled", "manual-1": "resting"}
    assert s.live is not None and s.live.working() == []
    left = Ledger(data / "ledger.sqlite", env="demo").open_orders("live")
    assert left == [], "every live order closed in the ledger"


def test_live_on_prod_shows_the_plan_and_needs_prod_typed(monkeypatch, capsys):
    import io

    from pmx.cli.run import confirmed
    from pmx.engine.config import load
    from test_run_config import RUN

    smoke = 'id = "smoke"\nkind = "scripted"\n'
    cfg = load(text=RUN.replace(smoke, smoke + 'mode = "live"\n'))
    assert confirmed(cfg, "prod") and not confirmed(cfg, "demo")
    tty = io.StringIO()
    tty.isatty = lambda: True  # type: ignore[method-assign]
    monkeypatch.setattr("sys.stdin", tty)
    assert confirmed(cfg, None, ask=lambda prompt: "prod")
    assert not confirmed(cfg, None, ask=lambda prompt: "yes")
    assert "LIVE: smoke" in capsys.readouterr().out
    monkeypatch.setattr("sys.stdin", io.StringIO())
    assert not confirmed(cfg, None, ask=lambda prompt: "prod"), "headless needs --confirm"


def test_manual_live_orders_need_live_and_confirmation_and_go_to_the_venue(tmp_path, monkeypatch):
    from pmx.control import protocol
    from pmx.control.client import request
    from pmx.engine.config import load
    from pmx.engine.run import System
    from test_control import CONFIG
    from test_run_config import FakeMarket, _until

    monkeypatch.chdir(tmp_path)
    venue = FakeVenue()

    async def go() -> System:
        s = System(load(text=CONFIG), env="demo", sock=Path("r.sock"), data=tmp_path / "data", market=FakeMarket(),
                   venue=venue, live=True, guard=False)
        stop = asyncio.Event()
        task = asyncio.create_task(s.run(stop))
        ctl = Path("r-ctl.sock")
        await _until(lambda: ctl.exists() and s.state.book("KXFED-A") is not None, 10, "engine up")
        unconfirmed = await request(ctl, protocol.Order("KXFED-A", "bid", 3_900, 100, "live"))
        assert not unconfirmed.ok and "confirmed" in unconfirmed.error and venue.calls[-1][0] != "place"
        ok = await request(ctl, protocol.Order("KXFED-A", "bid", 3_900, 100, "live", confirmed=True))
        assert ok.ok and ok.orders[-1]["status"] == "open" and ok.orders[-1]["mode"] == "live", "waits for the ack"
        assert [c for c, _ in venue.calls if c == "place"] == ["place"]
        assert (await request(ctl, protocol.Status())).data["live"]
        stop.set()
        await task
        return s

    asyncio.run(go())
    assert [v.status for v in venue.known.values()] == ["canceled"], "shutdown canceled it at the venue"
