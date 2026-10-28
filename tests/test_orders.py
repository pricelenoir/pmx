from __future__ import annotations

import pytest

from pmx.core.types import BookSnapshot, Contract, Fill, Trade
from pmx.engine.clock import SimClock
from pmx.engine.engine import Engine
from pmx.engine.events import LogEvent, OrderUpdate, RiskReject, SlotFill, Timer
from pmx.engine.state import State
from pmx.execution.orders import OrderManager
from pmx.execution.paper import PaperBroker
from pmx.execution.risk import RiskManager
from pmx.execution.scheduler import CANCEL, NEW, REDUCE, OrderScheduler, Write
from pmx.ledger.db import Attribution, Ledger
from pmx.strategies.base import CancelAll, Quote, Take, Target

T = "SER-EV1-A"
A, B = "a/" + T, "b/" + T


def contract(status="open", **kw) -> Contract:
    return Contract("kalshi", T, "SER-EV1", "Title", "Sub", status, None, **kw)


class Seen:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.events.append(ev)

    def of(self, t: type) -> list:
        return [e for e in self.events if type(e) is t]


class Slot:
    def __init__(self, id: str, mode="paper") -> None:
        self.id, self.mode, self.model, self.inputs, self.risk_limits = id, mode, None, (), None
        self.warming, self.tickers = False, {T: None}


class Rig:
    """The engine with the paper broker and an order manager fed intents by hand (no router)."""

    def __init__(self, *, risk: dict | None = None, scheduler: OrderScheduler | None = None, cash: int = 10**9,
                 slots=(A, B), ledger: Ledger | None = None) -> None:
        self.clock = SimClock(1_000_000)
        self.state = State()
        self.paper = PaperBroker()
        self.pending: list = []
        self.ledger = ledger or Ledger(":memory:", env="demo")
        self.slots = {s: Slot(s) for s in slots}
        self.om = OrderManager(intents=self._take, slots=self.slots, brokers={"paper": self.paper},
                               scheduler=scheduler if scheduler is not None else OrderScheduler(), ledger=self.ledger,
                               instance="t1", paper_cash=cash, attribution={"a": Attribution("run1", "abc123", "p9")})
        if risk is not None:
            self.risk = RiskManager(positions=self.om, balance=self.om, slots=self.slots, **risk)
            self.om.risk = self.risk
            self.risk.on_cancel.append(self.om.cancel_scope)
        self.seen = Seen()
        self.engine = Engine(self.clock, self.state, broker=self.paper, order_manager=self.om, publisher=self.seen)
        self.send(contract())
        self.send(BookSnapshot("kalshi", T, ((4_000, 1_000),), ((4_200, 1_000),), 0))

    def _take(self) -> list:
        out, self.pending = self.pending, []
        return out

    def send(self, ev, at: int | None = None) -> None:
        if at is not None:
            self.clock.advance(at)
        self.engine.dispatch(ev, self.clock.now_ms())

    def act(self, *intents, at: int | None = None) -> None:
        self.pending += intents
        self.send(Timer(0), at)

    def updates(self, coid: str | None = None) -> list[str]:
        return [e.status for e in self.seen.of(OrderUpdate) if coid is None or e.client_order_id == coid]


def q(slot=A, bid=(4_000, 100), ask=None) -> Quote:
    return Quote(slot, T, bid, ask)


# ---- quotes, takes, cancels ---------------------------------------------------------------------------------


def test_quotes_are_diffed_against_working_orders_without_churn():
    r = Rig()
    r.act(q(bid=(3_900, 100), ask=(4_300, 200)))
    assert [(w.side, w.price, w.qty, w.status) for w in r.om.working(A, T)] == [("bid", 3_900, 100, "open"),
                                                                                ("ask", 4_300, 200, "open")]
    placed = r.paper._seq
    r.act(q(bid=(3_900, 100), ask=(4_300, 200)))  # same target: nothing sent
    assert r.paper._seq == placed and len(r.om.working(A, T)) == 2
    r.act(q(bid=(3_800, 100), ask=(4_300, 200)))  # the bid moves: cancel + place; the ask stays
    assert [(w.side, w.price) for w in r.om.working(A, T)] == [("ask", 4_300), ("bid", 3_800)]
    r.act(Quote(A, T, None, None))
    assert r.om.working(A, T) == [] and r.updates().count("canceled") == 3
    coids = [c for (c,) in r.ledger.db.execute("SELECT client_order_id FROM orders")]
    assert len(coids) == 3 and all(c.startswith("pmx-t1-") for c in coids)  # our instance, for reconciliation


def test_takes_fill_and_positions_are_kept_per_slot_on_one_ticker():
    r = Rig()
    r.act(Take(A, T, "bid", 4_200, 300), Take(B, T, "ask", 4_000, 200))
    assert r.om.position(A, T) == 300 and r.om.position(B, T) == -200 and r.om.position("", T) == 100
    fills = r.seen.of(SlotFill)
    assert [(f.slot, f.fill.side, f.fill.price, f.fill.qty) for f in fills] == [(A, "bid", 4_200, 300),
                                                                                (B, "ask", 4_000, 200)]
    rows = r.ledger.positions("paper", by="slot")
    assert {k[0]: p.qty for k, p in rows.items()} == {A: 300, B: -200}
    run = r.ledger.db.execute("SELECT run_id, strategy_version, params_hash, env FROM fills ORDER BY rowid").fetchall()
    assert run == [("run1", "abc123", "p9", "demo"), ("", "", "", "demo")]


def test_cancel_all_and_target_intents():
    r = Rig()
    r.act(q(bid=(3_900, 100), ask=(4_300, 100)), q(slot=B, bid=(3_800, 100)))
    r.act(CancelAll(A))
    assert r.om.working(A, T) == [] and len(r.om.working(B, T)) == 1
    logs = len(r.seen.of(LogEvent))
    r.act(Target(A, T, 500), Target(A, T, 600))  # the router's execution helpers work these
    assert r.om.working(A, T) == [] and len(r.seen.of(LogEvent)) == logs


# ---- the order scheduler -------------------------------------------------------------------------------------


def test_thirty_slots_requoting_every_tick_stay_within_ten_writes_a_second():
    slots = [f"s{i}/{T}" for i in range(30)]
    r = Rig(slots=slots)
    sends: list[int] = []
    placed: set[str] = set()
    place = r.paper.place_now

    def counting(req, slot, state, now_ms):
        sends.append(now_ms)
        placed.add(slot)
        return place(req, slot, state, now_ms)

    r.paper.place_now = counting  # type: ignore[method-assign]
    for tick in range(100):  # 10 s of 100 ms ticks
        r.act(*(q(slot=s, bid=(3_000 + 100 * (tick % 5), 100)) for s in slots), at=1_000_000 + tick * 100)
    per_second: dict[int, int] = {}
    for t in sends:
        per_second[t // 1000] = per_second.get(t // 1000, 0) + 1
    assert max(per_second.values()) <= 20  # a full bucket (10) plus a second's refill (10)
    assert len(sends) <= 10 + 10 * 10
    assert placed == set(slots)  # nobody starved
    assert any(r.om.throttled(s) for s in slots)


def test_cancels_jump_the_queue_and_slots_take_turns():
    s = OrderScheduler(rate=10, burst=10, cost=10)  # one write per second
    for i in range(5):
        s.submit(Write("place", "busy", f"b{i}", NEW, 0))
    s.submit(Write("place", "quiet", "q0", NEW, 0))
    s.submit(Write("place", "reducer", "r0", REDUCE, 0))
    s.submit(Write("cancel", "busy", "x0", CANCEL, 0))
    order = [s.due(t)[0][0].client_order_id for t in range(0, 8_000, 1_000)]
    assert order == ["x0", "r0", "b0", "q0", "b1", "b2", "b3", "b4"]


def test_cancels_cost_what_the_venue_charges_for_them():
    s = OrderScheduler()  # Kalshi basic: 100 tokens; a create costs 10, a cancel 2
    for i in range(20):
        s.submit(Write("cancel", "a", f"c{i}", CANCEL, 0))
        s.submit(Write("place", "a", f"p{i}", NEW, 0))
    sent, _ = s.due(0)
    assert [w.kind for w in sent].count("cancel") == 20 and [w.kind for w in sent].count("place") == 6


def test_a_newer_quote_replaces_one_still_queued():
    r = Rig(scheduler=OrderScheduler(rate=10, burst=10, cost=10))
    r.act(Take(A, T, "bid", 4_200, 100))  # uses the only token
    r.act(q(bid=(3_800, 100)), at=1_000_100)
    r.act(q(bid=(3_700, 100)), at=1_000_200)  # the 38¢ bid was never sent: dropped, not canceled at the venue
    r.send(Timer(0), at=1_001_100)
    assert [(w.price, w.status) for w in r.om.working(A, T)] == [(3_700, "open")]
    assert r.paper._seq <= 4  # one take (+ its fill) and one place — no cancel write for 38¢
    assert "queued" in r.updates()


def test_queued_takes_expire():
    r = Rig(scheduler=OrderScheduler(rate=10, burst=10, cost=10, take_ttl_ms=500))
    r.act(Take(A, T, "bid", 4_200, 100), Take(B, T, "bid", 4_200, 100))  # B waits
    r.send(Timer(0), at=1_000_600)
    expired = [e for e in r.seen.of(OrderUpdate) if e.slot == B]
    assert [(e.status, e.reason) for e in expired] == [("queued", ""), ("canceled", "still queued after 500 ms")]
    assert r.om.position(B, T) == 0


def test_paper_fills_see_throttled_quotes_late():
    r = Rig(scheduler=OrderScheduler(rate=10, burst=10, cost=10))
    r.act(Take(A, T, "bid", 4_200, 100))
    r.act(q(slot=B, bid=(4_000, 100)), at=1_000_100)  # queued behind the take
    r.send(Trade("kalshi", "t1", T, 3_900, 500, "ask", 0), at=1_000_200)  # trades through 40¢ — B isn't resting yet
    assert r.om.position(B, T) == 0
    r.send(Timer(0), at=1_001_100)
    r.send(Trade("kalshi", "t2", T, 3_900, 500, "ask", 0), at=1_001_200)
    assert r.om.position(B, T) == 100


def test_rate_limited_halves_the_rate_and_recovers():
    s = OrderScheduler(rate=100, burst=100, cost=10)
    s.due(0)
    s.rate_limited(1_000)
    assert s.rate == 50 and s.tokens == 0
    s.due(11_000)
    assert s.rate == 100


# ---- safety and money ----------------------------------------------------------------------------------------


def test_self_cross_is_rejected_and_never_sent():
    r = Rig()
    r.act(q(slot=A, bid=(4_100, 100)))
    sent = r.paper._seq
    r.act(Take(B, T, "ask", 4_000, 100))
    [rej] = r.seen.of(RiskReject)
    assert (rej.slot, rej.rule, rej.detail) == (B, "self_cross", f"would cross our bid at 41¢ ({A})")
    assert r.paper._seq == sent


def test_paper_balance_runs_out():
    r = Rig(risk={}, cash=1_000_000)  # $1
    r.act(Take(A, T, "bid", 4_200, 200))  # $0.84 + fee
    assert r.om.position(A, T) == 200 and r.om.available("paper") == 1_000_000 - 840_000 - 34_104
    r.act(q(slot=B, bid=(4_000, 100)))
    [rej] = r.seen.of(RiskReject)
    assert rej.rule == "insufficient_balance" and rej.detail.startswith("needs $0.40")
    r.act(Take(A, T, "ask", 4_000, 200))  # selling what it holds needs no cash
    assert r.om.position(A, T) == 0


def test_the_paper_balance_kept_per_market_matches_a_full_recount():
    from pmx.core.units import ONE_DOLLAR

    def recount(om) -> int:
        held: dict = {}
        for (m, _, t), p in om._pos.items():
            if m == "paper":
                held[t] = held.get(t, 0) + p.qty
        reserved = sum(-q * ONE_DOLLAR for q in held.values() if q < 0)
        for o in om.orders.values():
            if o.mode == "paper":
                reserved += (o.req.price * o.remaining if o.req.side == "bid"
                             else (ONE_DOLLAR - o.req.price) * max(0, o.remaining - max(0, held.get(o.mid, 0))))
        return om.paper_cash + om._cash["paper"] - om._fees["paper"] - reserved

    r = Rig()
    steps = [lambda: r.act(q(bid=(3_900, 300), ask=(4_300, 200))), lambda: r.act(Take(B, T, "ask", 4_000, 500)),
             lambda: r.send(Trade("kalshi", "t1", T, 3_800, 400, "ask", 0)), lambda: r.act(q(bid=(3_800, 100))),
             lambda: r.act(CancelAll(A)), lambda: r.act(Take(A, T, "bid", 4_200, 200)),
             lambda: r.send(contract("settled", result="no"))]
    for step in steps:
        step()
        assert r.om.available("paper") == recount(r.om)


def test_a_halt_cancels_the_scopes_orders():
    r = Rig(risk={})
    r.act(q(slot=A, bid=(3_900, 100)), q(slot=B, bid=(3_800, 100)))
    r.risk.halt("testing", "a")
    r.send(Timer(0))
    assert r.om.working(A, T) == [] and len(r.om.working(B, T)) == 1
    r.act(q(slot=A, bid=(3_900, 100)))
    assert r.seen.of(RiskReject)[-1].rule == "halted"


def test_a_paused_market_cancels_resting_orders():
    r = Rig()
    r.act(q(bid=(3_900, 100)))
    r.send(contract("paused"))
    assert r.om.working(A, T) == [] and r.updates()[-1] == "canceled"


def test_lifecycle_races():
    r = Rig()
    r.act(q(bid=(4_000, 300)))
    [w] = r.om.working(A, T)
    partial = Trade("kalshi", "t1", T, 3_900, 100, "ask", 0)  # through our price: 1 of 3
    r.send(partial)
    assert r.om.position(A, T) == 100
    fill = next(e for e in r.seen.of(SlotFill))
    r.send(fill)  # the same fill again (a reconnect replaying it)
    assert r.om.position(A, T) == 100
    r.act(CancelAll(A))  # partial fill, then cancel
    assert r.updates(w.client_order_id)[-1] == "canceled" and r.om.position(A, T) == 100
    row = r.ledger.db.execute("SELECT status, filled FROM orders WHERE client_order_id=?", (w.client_order_id,))
    assert row.fetchone() == ("canceled", 100)
    r.act(Take(A, T, "bid", 4_200, 100))
    done = r.updates()[-1]
    r.act(CancelAll(A))  # cancel of an order already filled: nothing happens
    assert done == "filled" and r.updates()[-1] == "filled"
    stray = SlotFill(A, "paper", Fill("kalshi", "late-1", "O9", T, "bid", 4_000, 100, 0, False, 0, "unknown"))
    r.send(stray)  # a fill for an order we no longer track (e.g. after the cancel was sent) still counts
    assert r.om.position(A, T) == 300


@pytest.mark.parametrize(("settle", "pnl"), [
    (dict(result="yes"), 580_000 - 17_052),  # bought 1 @ 42¢ (taker fee $0.017052), paid $1
    (dict(result="no"), -420_000 - 17_052),
    (dict(settlement=5_000), 80_000 - 17_052),  # a tie: $0.50
])
def test_settlement_realizes_yes_no_and_ties(settle, pnl):
    r = Rig()
    r.act(Take(A, T, "bid", 4_200, 100))
    r.send(contract("determined", **settle))  # determined isn't settled: nothing yet
    assert r.om.position(A, T) == 100
    r.send(contract("settled", **settle))
    p = r.ledger.positions("paper", by="slot", include_flat=True)[(A, "kalshi", T)]
    assert p.qty == 0 and p.pnl() == pnl
    assert r.ledger.pnl_series("paper")[-1][1] == pnl
    assert r.om.position(A, T) == 0 and r.om.pnl(A, "paper", r.state) == pnl
    assert any("settled at" in e.text for e in r.seen.of(LogEvent))


def test_restart_cancels_paper_orders_and_restores_positions(tmp_path):
    path = tmp_path / "ledger.sqlite"
    r = Rig(ledger=Ledger(path, env="demo"))
    r.act(Take(A, T, "bid", 4_200, 300), q(slot=B, bid=(3_900, 100)))
    r.ledger.close()
    again = Ledger(path, env="demo")
    om = OrderManager(ledger=again, brokers={"paper": PaperBroker()}, instance="t2")
    [log] = om.start(5)
    assert log.text == "canceled 1 paper orders left open by the last run"
    assert om.position(A, T) == 300 and om.working("") == []
    assert again.open_orders("paper") == [] and Ledger(path, env="prod").positions("paper") == {}  # envs never mix


def test_a_second_run_on_the_same_ledger_keeps_ids_unique_and_its_fills(tmp_path):
    """Same data dir, same instance: client order ids carry on, paper fill ids follow them, so the ledger
    (keyed by both) records the second run's orders and fills instead of ignoring them as duplicates."""
    path = tmp_path / "ledger.sqlite"
    first = Rig(ledger=Ledger(path, env="demo"))
    first.act(Take(A, T, "bid", 4_200, 100))
    first.ledger.close()
    second = Rig(ledger=Ledger(path, env="demo"))
    second.om.start(5)
    second.act(Take(A, T, "bid", 4_200, 100))
    fills = list(second.ledger.fills("paper"))
    assert len(fills) == 2 and len({f.fill_id for _, f in fills}) == 2
    assert second.om.position(A, T) == 200
    ids = [r[0] for r in second.ledger.db.execute("SELECT client_order_id FROM orders ORDER BY rowid")]
    assert ids[0] != ids[1] and ids[1].endswith("-2")


# ---- acceptance -----------------------------------------------------------------------------------------------


def test_a_scripted_session_matches_the_hand_computed_ledger():
    """Two slots on one ticker: A makes (quotes), B takes; prints fill A's resting bid; the market settles YES.

    | step                                   | A qty | A cash      | A fees  | B qty | B cash      | B fees  |
    |----------------------------------------|-------|-------------|---------|-------|-------------|---------|
    | A quotes 1 @ 39¢ bid (behind 10 @ 39¢) |   0   |      0      |    0    |   0   |      0      |    0    |
    | print 12 @ 39¢: 10 queue, 2 → A fills 1|  +1   |  −$0.39     | 0.4164  |   0   |      0      |    0    |
    | B takes 3 @ 42¢ ask side (buys YES)    |  +1   |  −$0.39     | 0.4164  |  +3   |  −$1.26     | 5.1156  |
    | B sells 1 @ 40¢ (hits the 40¢ bid)     |  +1   |  −$0.39     | 0.4164  |  +2   |  −$0.86     | 6.7956  |
    | settles YES: $1 per YES contract       |   0   |  +$0.61     | 0.4164  |   0   |  +$1.14     | 6.7956  |
    (fees in cents: maker 1.75%·p(1−p), taker 7%·p(1−p) per contract, rounded up to the micro-dollar.)
    PnL: A = 0.61 − 0.004164 = $0.605836; B = 1.14 − 0.067956 = $1.072044.
    """
    r = Rig()
    r.send(BookSnapshot("kalshi", T, ((4_000, 1_000), (3_900, 1_000)), ((4_200, 1_000),), 0))
    r.act(q(slot=A, bid=(3_900, 100)))
    r.send(Trade("kalshi", "t1", T, 3_900, 1_200, "ask", 0))
    r.act(Take(B, T, "bid", 4_200, 300))
    r.act(Take(B, T, "ask", 4_000, 100))
    r.send(contract("settled", result="yes"))
    pos = r.ledger.positions("paper", by="slot", include_flat=True)
    a, b = pos[(A, "kalshi", T)], pos[(B, "kalshi", T)]
    assert (a.qty, a.cash, a.fees) == (0, 610_000, 4_164)
    assert (b.qty, b.cash, b.fees) == (0, 1_140_000, 67_956)
    assert (a.pnl(), b.pnl()) == (605_836, 1_072_044)
    assert r.ledger.pnl_series("paper", A)[-1][1] == 605_836
    assert r.ledger.pnl_series("paper")[-1][1] == 605_836 + 1_072_044
    assert r.om.pnl(A, "paper", r.state) == 605_836 and r.om.pnl("", "paper", r.state) == 1_677_880



def test_the_router_scripted_smoke_strategy_and_paper_trading_end_to_end():
    from pmx.engine.slots import MarketSelector, Router, StrategySpec
    from pmx.strategies.reference import scripted  # noqa: F401  (registers the kind)

    router = Router([StrategySpec(id="smoke", kind="scripted", params={"script": "config/scripts/smoke.toml"},
                                  markets=MarketSelector(tickers=[T]))])
    ledger, paper = Ledger(":memory:"), PaperBroker()
    om = OrderManager(intents=router.take_intents, slots=router.slots, brokers={"paper": paper}, ledger=ledger,
                      instance="t")
    router.orders = om  # slots read their positions and working orders from the order manager
    risk = RiskManager(positions=om, balance=om, slots=router.slots)
    om.risk = risk
    risk.on_cancel.append(om.cancel_scope)
    clock, seen = SimClock(0), Seen()
    engine = Engine(clock, State(), broker=paper, router=router, order_manager=om, publisher=seen)
    engine.dispatch(contract(), 0)
    engine.dispatch(BookSnapshot("kalshi", T, ((4_200, 1_000),), ((4_700, 1_000),), 0), 0)
    for s in range(1, 200):
        clock.advance(s * 1_000)
        engine.dispatch(Timer(0), s * 1_000)
    sid = "smoke/" + T
    statuses = [(e.status, e.price) for e in seen.of(OrderUpdate)]
    assert statuses[:2] == [("open", 3_900), ("canceled", 3_900)]  # rested 5¢ under mid, pulled at +60 s
    assert [(f.fill.side, f.fill.price) for f in seen.of(SlotFill)] == [("bid", 4_700), ("ask", 4_200)]
    p = ledger.positions("paper", by="slot", include_flat=True)[(sid, "kalshi", T)]
    assert (p.qty, p.cash, p.fees) == (0, -50_000, 17_437 + 17_052)  # bought at 47¢, sold at 42¢, two taker fees
    assert router.slots[sid].ctx.position == 0 and seen.of(RiskReject) == []
