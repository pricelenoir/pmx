from __future__ import annotations

import ast
import pathlib

import msgspec
import pytest

from pmx import plugins
from pmx import testing as kit
from pmx.core.fees import FeeSchedule
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, Trade
from pmx.core.units import parse_price, parse_qty
from pmx.engine.clock import SimClock
from pmx.engine.engine import Engine
from pmx.engine.events import Advice, Forecast, Halt, LogEvent, OrderUpdate, Schedule, SlotFill, SlotStatus, Timer
from pmx.engine.slots import ByLinker, MarketSelector, OrderTracker, Router, StrategySpec, describe
from pmx.engine.state import State, venue_ticker
from pmx.plugins import PluginError, strategy
from pmx.strategies.base import CancelAll, GroupContext, MarketContext, Quote, Strategy, Take, Target
from pmx.strategies.reference.scripted import Scripted
from pmx.testing import Scenario

T1, T2, T3 = "SER-EV1-A", "SER-EV1-B", "SER-EV2-A"


def contract(ticker: str, event: str = "SER-EV1", status: str = "open", series: str = "SER") -> Contract:
    return Contract("kalshi", ticker, event, "Title", "Sub", status, None, series_ticker=series)


def snap(ticker: str, bid: str = "0.40", ask: str = "0.44", size: str = "10") -> BookSnapshot:
    return BookSnapshot("kalshi", ticker, ((parse_price(bid), parse_qty(size)),),
                        ((parse_price(ask), parse_qty(size)),), 0)


class Thing(msgspec.Struct, frozen=True):
    key: str
    ts_ms: int

    @property
    def entity(self) -> str:
        return self.key

    def summary(self) -> str:
        return self.key

    def display_fields(self) -> list[tuple[str, str]]:
        return []


class Tick(msgspec.Struct, frozen=True):  # an outside feed's event (a strategy input)
    value: float


class Collect:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, ev, now_ms) -> None:
        self.events.append(ev)


class FakeOrders:
    """The order manager's seat in dispatch: drains the router's intents; can fill takes at their limit."""

    def __init__(self, router: Router, fill_takes: bool = False) -> None:
        self.router, self.fill_takes = router, fill_takes
        self.intents: list = []
        self.n = 0

    def on_event(self, ev, state, now_ms):
        new = self.router.take_intents()
        self.intents += new
        out = []
        for i in new:
            if self.fill_takes and isinstance(i, Take):
                self.n += 1
                venue, ticker = venue_ticker(i.ticker)
                f = Fill(venue, f"f{self.n}", f"o{self.n}", ticker, i.side, i.limit, i.qty, 0, True, now_ms)
                out.append(SlotFill(i.slot, "paper", f))
        return out


class Rig:
    """Engine (SimClock) + State + Router + a fake order manager."""

    def __init__(self, *specs: StrategySpec, linkers=None, fill_takes=False, **kw) -> None:
        self.clock = SimClock(0)
        self.state = State(linkers=linkers)
        self.router = Router(specs, linkers=linkers or {}, **kw)
        self.om = FakeOrders(self.router, fill_takes)
        self.pub = Collect()
        self.engine = Engine(self.clock, self.state, router=self.router, order_manager=self.om, publisher=self.pub)

    def send(self, ev, at: int | None = None, source: str = "") -> None:
        if at is not None:
            self.clock.advance(at)
        self.engine.dispatch(ev, self.clock.now_ms(), source)

    def intents(self) -> list:
        out, self.om.intents = self.om.intents, []
        return out

    def events(self, t: type) -> list:
        return [e for e in self.pub.events if isinstance(e, t)]

    def seen(self, slot: str) -> list:
        return self.router.slots[slot].instance.seen

    def clear(self) -> None:
        for slot in self.router.slots.values():
            if slot.instance is not None:
                slot.instance.seen.clear()
        self.pub.events.clear()
        self.intents()


@strategy("test_rec")
class Rec(Strategy):
    """Records every hook; optionally takes on each book, or raises on one market."""

    class Params(msgspec.Struct, frozen=True):
        take: str = ""
        fail_on: str = ""
        need_book: bool = False

    def __init__(self, params) -> None:
        super().__init__(params)
        self.seen: list = []

    def ready(self, m) -> bool:
        return not self.p.need_book or m.book is not None

    def on_start(self, m) -> None:
        self.seen.append(("start",))

    def on_book(self, book, m) -> None:
        self.seen.append(("book", book.ticker, book.best_bid()))
        if book.ticker == self.p.fail_on:
            raise RuntimeError("bad market")
        if self.p.take:
            m.take("bid", book.best_ask()[0], parse_qty(self.p.take))

    def on_trade(self, t, m) -> None:
        self.seen.append(("trade", t.ticker))

    def on_forecast(self, f, m) -> None:
        self.seen.append(("forecast", f.model, f.ticker))

    def on_data(self, data, m) -> None:
        self.seen.append(("data", data.entity))

    def on_input(self, name, ev, m) -> None:
        self.seen.append(("input", name, ev))

    def on_advice(self, a, m) -> None:
        self.seen.append(("advice", a.advisor, a.target))

    def on_fill(self, f, m) -> None:
        self.seen.append(("fill", f.fill.ticker, m.market(f.fill.ticker).position))

    def on_timer(self, m) -> None:
        self.seen.append(("timer", m.now_ms))

    def on_stop(self, m) -> None:
        self.seen.append(("stop",))

    def on_save(self) -> bytes | None:
        return repr(len(self.seen)).encode()

    def on_load(self, blob: bytes) -> None:
        self.seen.append(("load", blob))


def rec(id: str, **kw) -> StrategySpec:
    kw.setdefault("markets", MarketSelector(series=["SER"]))
    return StrategySpec(id=id, kind="test_rec", **kw)


def kinds(seen: list) -> list:
    return [s[0] for s in seen]


# ---- selection and slot lifecycle ------------------------------------------------------------------


def test_selectors_create_one_slot_per_market_including_contracts_that_appear_mid_run():
    r = Rig(rec("a"), rec("b", markets=MarketSelector(tickers=[T2], events=["OTHER-EV9"])))
    r.send(contract(T1))
    r.send(contract(T2))
    r.send(contract("NOPE-1", event="NOPE", series="NOPE"))
    r.send(contract(T3, event="SER-EV2", status="unopened"))  # selection starts slots only on open markets
    assert sorted(r.router.slots) == ["a/" + T1, "a/" + T2, "b/" + T2]
    r.send(contract(T3, event="SER-EV2"), at=5_000)  # it opens mid-run
    r.send(contract("OTHER-EV9-X", event="OTHER-EV9", series="OTHER"))
    assert "a/" + T3 in r.router.slots and "b/OTHER-EV9-X" in r.router.slots
    assert r.seen("a/" + T3) == [("start",)] and r.router.slots["a/" + T3].started_ms == 5_000
    statuses = {s.slot: s for s in r.events(SlotStatus)}
    assert statuses["a/" + T1].state == "idle" and statuses["a/" + T1].ticker == T1


def test_a_closing_market_stops_its_slot_once_orders_are_cleared():
    r = Rig(rec("a"))
    r.send(contract(T1))
    sid = "a/" + T1
    r.send(OrderUpdate(sid, "paper", "c1", "o1", T1, "bid", 4_000, 100, 0, "open", 0))
    r.send(contract(T1, status="paused"))  # paused isn't closed: the slot stays
    assert not r.router.slots[sid].stopped
    r.clear()
    r.send(contract(T1, status="closed"), at=1_000)
    slot = r.router.slots[sid]
    assert slot.stopped and r.seen(sid) == [("stop",)]
    assert CancelAll(sid, T1) in r.intents()
    assert r.events(SlotStatus)[-1].last_action == f"stopped: {T1} closed"
    r.send(snap(T1))
    r.send(Timer(2_000), at=2_000)
    assert r.seen(sid) == [("stop",)] and sid in r.router.slots  # still has a working order
    r.send(OrderUpdate(sid, "paper", "c1", "o1", T1, "bid", 4_000, 100, 0, "canceled", 2_500), at=2_500)
    r.send(Timer(3_000), at=3_000)
    assert sid not in r.router.slots


# ---- routing -------------------------------------------------------------------------------------------


def test_each_event_reaches_exactly_the_slots_it_concerns():
    linkers = {"ev": lambda c: f"ent:{c.event_ticker}"}
    a = rec("a", model="m1", advisors=["adv"], inputs=["feed"])
    b = rec("b", model="m2", markets=MarketSelector(tickers=[T2]))
    r = Rig(a, b, linkers=linkers)
    for t in (T1, T2):
        r.send(contract(t))
    r.clear()
    a1, a2, b2 = "a/" + T1, "a/" + T2, "b/" + T2

    def deliveries(ev, source=""):
        r.clear()
        r.send(ev, source=source)
        return {sid: kinds(r.seen(sid)) for sid in (a1, a2, b2) if r.seen(sid)}

    assert deliveries(snap(T1)) == {a1: ["book"]}
    assert deliveries(Trade("kalshi", "x", T2, 4_100, 100, "bid", 0)) == {a2: ["trade"], b2: ["trade"]}
    assert deliveries(Forecast("m1", T2, 0.5, 0, 0)) == {a2: ["forecast"]}
    assert deliveries(Forecast("m2", T2, 0.5, 0, 0)) == {b2: ["forecast"]}
    assert deliveries(Forecast("m3", T2, 0.5, 0, 0)) == {}
    assert deliveries(Thing("ent:SER-EV1", 0)) == {a1: ["data"], a2: ["data"], b2: ["data"]}
    assert deliveries(Thing("ent:elsewhere", 0)) == {}
    assert deliveries(Advice("adv", T1, 0.6, None, 0.9, 10_000, "why", 0)) == {a1: ["advice"]}
    entity_wide = Advice("adv", "ent:SER-EV1", 0.6, None, 0.9, 10_000, "why", 0)
    assert deliveries(entity_wide) == {a1: ["advice"], a2: ["advice"]}
    assert deliveries(Advice("other", T1, 0.6, None, 0.9, 10_000, "why", 0)) == {}
    assert deliveries(Advice("adv", T1, 0.6, None, 0.9, -1, "lapsed", 0)) == {}
    assert deliveries(Tick(1.0), source="feed") == {a1: ["input"], a2: ["input"]}
    assert deliveries(Tick(1.0), source="elsewhere") == {}
    fill = Fill("kalshi", "f1", "o1", T2, "bid", 4_000, 100, 0, True, 0)
    assert deliveries(SlotFill(b2, "paper", fill)) == {b2: ["fill"]}
    assert deliveries(Timer(0)) == {a1: ["timer"], a2: ["timer"], b2: ["timer"]}
    assert r.seen(b2) == [("timer", 0)]


def test_a_raising_slot_halts_alone_and_cancels_its_orders():
    r = Rig(rec("a", params={"fail_on": T1}))
    for t in (T1, T2):
        r.send(contract(t))
    r.clear()
    r.send(snap(T1), at=1_000)
    r.send(snap(T2))
    a1, a2 = r.router.slots["a/" + T1], r.router.slots["a/" + T2]
    assert a1.error == "RuntimeError: bad market" and not a2.error
    assert CancelAll("a/" + T1) in r.intents()
    logs = [e for e in r.events(LogEvent) if e.source == "a/" + T1]
    assert logs and logs[0].level == "error" and "bad market" in logs[0].text
    assert [s.state for s in r.events(SlotStatus) if s.slot == "a/" + T1] == ["error"]
    r.send(snap(T1, bid="0.41"))
    r.send(Timer(2_000), at=2_000)
    assert kinds(a1.instance.seen) == ["book"]  # nothing after the failure
    assert kinds(a2.instance.seen) == ["book", "timer"]


def test_two_strategies_on_overlapping_markets_keep_their_own_slots_and_positions():
    r = Rig(rec("a", params={"take": "1"}), rec("b", params={"take": "3"}, markets=MarketSelector(tickers=[T2])),
            fill_takes=True)
    for t in (T1, T2):
        r.send(contract(t))
    for _ in range(2):
        r.send(snap(T1))
        r.send(snap(T2))
    orders = r.router.orders
    assert orders.position("a/" + T1, T1) == 200
    assert orders.position("a/" + T2, T2) == 200
    assert orders.position("b/" + T2, T2) == 600
    assert orders.position("b/" + T2, T1) == 0
    assert ("fill", T2, 600) in r.seen("b/" + T2)  # the slot sees its position already updated
    takes = [i for i in r.intents() if isinstance(i, Take)]
    assert {i.slot for i in takes} == {"a/" + T1, "a/" + T2, "b/" + T2}
    r.send(Timer(1_000), at=1_000)
    status = {s.slot: s for s in r.events(SlotStatus)}
    assert status["b/" + T2].position == 600 and status["b/" + T2].state == "active"
    assert status["b/" + T2].last_action == "take buy 3 @ 44¢"


# ---- cadence, CPU budget, warm-up --------------------------------------------------------------------


def test_cadence_coalesces_book_hooks_but_never_delays_fills():
    r = Rig(rec("a", cadence_ms=100, markets=MarketSelector(tickers=[T1])))
    r.send(contract(T1))
    r.send(snap(T1, bid="0.30"), at=1_000)
    r.clear()
    for i in range(100):  # 100 deltas within 50 ms
        r.clock.advance(1_000 + i // 2)
        r.send(BookDelta("kalshi", T1, "bid", parse_price("0.31") + 100 * i, 100, 0))
        if i == 40:
            r.send(SlotFill("a/" + T1, "paper", Fill("kalshi", "f", "o", T1, "bid", 3_000, 100, 0, False, 0)))
    seen = r.seen("a/" + T1)
    assert kinds(seen) == ["fill"]  # delivered at once, mid-burst
    r.send(Timer(1_200), at=1_200)
    assert kinds(seen) == ["fill", "book", "timer"]
    assert seen[1][2] == (parse_price("0.31") + 100 * 99, 100)  # the final book


def test_cpu_budget_warns_then_halts_a_slow_slot_only():
    cpu = [0]

    @strategy("test_slow")
    class Slow(Strategy):
        def on_timer(self, m):
            cpu[0] += 60_000_000  # 60 ms of "CPU" per engine second; the budget is 50

    r = Rig(StrategySpec(id="slow", kind="test_slow", markets=MarketSelector(tickers=[T1])),
            rec("fast", markets=MarketSelector(tickers=[T1])), cpu_clock=lambda: cpu[0])
    r.send(contract(T1))
    for s in range(62):
        r.send(Timer(s * 1_000), at=s * 1_000)
    logs = [e for e in r.events(LogEvent) if e.source == "slow/" + T1]
    assert [e.level for e in logs] == ["warn", "error"]
    assert logs[0].ts_ms == 10_000 and logs[1].ts_ms == 60_000
    assert Halt("slow/" + T1, "slow", True, 60_000) in r.events(Halt)
    assert r.router.slots["slow/" + T1].halted == "slow" and CancelAll("slow/" + T1) in r.intents()
    assert kinds(r.seen("fast/" + T1)).count("timer") == 62


def test_warm_up_lasts_warmup_s_of_engine_time_and_until_ready():
    r = Rig(rec("a", warmup_s=5, params={"need_book": True}, markets=MarketSelector(tickers=[T1])))
    r.send(contract(T1), at=1_000)
    slot = r.router.slots["a/" + T1]
    assert slot.warming and not slot.ctx.ready
    r.send(Timer(5_000), at=5_000)
    assert slot.warming
    r.send(Timer(6_000), at=6_000)  # time's up but `ready` wants a book
    assert slot.warming
    r.send(snap(T1), at=6_500)
    assert slot.ctx.ready
    assert [s.state for s in r.events(SlotStatus)] == ["warming", "idle"]


# ---- group slots ---------------------------------------------------------------------------------------


@strategy("test_pair")
class Pair(Strategy):
    """Quotes every member of its group 2¢ around mid whenever any book changes."""

    def on_book(self, book, m: GroupContext) -> None:
        for t in m.tickers:
            b = m.market(t).book
            if b is not None:
                mid = round(b.mid())
                m.quote(t, bid=(mid - 200, 100), ask=(mid + 200, 100))


def test_a_group_slot_quotes_both_markets_of_one_event_and_tracks_members():
    outcomes = {"ent:SER-EV1": {"A": {T1: "yes", T2: "no"}, "B": {T1: "no", T2: "yes"}}}
    spec = StrategySpec(id="pair", kind="test_pair", markets=MarketSelector(series=["SER"]), slot_by="event")
    r = Rig(spec, linkers={"ev": lambda c: f"ent:{c.event_ticker}"}, outcomes=outcomes.get)
    for t in (T1, T2):
        r.send(contract(t))
    assert list(r.router.slots) == ["pair/SER-EV1"]
    slot = r.router.slots["pair/SER-EV1"]
    assert isinstance(slot.ctx, GroupContext) and slot.ctx.tickers == [T1, T2]
    assert slot.ctx.outcomes == outcomes["ent:SER-EV1"]
    r.send(snap(T1, "0.40", "0.44"))
    r.send(snap(T2, "0.56", "0.60"))
    assert r.intents()[-2:] == [Quote("pair/SER-EV1", T1, (4_000, 100), (4_400, 100)),
                                Quote("pair/SER-EV1", T2, (5_600, 100), (6_000, 100))]
    r.send(contract("SER-EV1-C"))  # joins at runtime
    assert slot.ctx.tickers == [T1, T2, "SER-EV1-C"]
    r.send(contract(T1, status="determined"))  # leaves; the slot carries on with the rest
    assert slot.ctx.tickers == [T2, "SER-EV1-C"] and not slot.stopped
    assert CancelAll("pair/SER-EV1", T1) in r.intents()
    assert slot.ctx.outcomes == {"A": {T2: "no"}, "B": {T2: "yes"}}


def test_group_keys_by_entity_custom_linker_and_all():
    linkers = {"ev": lambda c: f"ent:{c.event_ticker}", "half": lambda c: c.ticker[-1]}
    sel = MarketSelector(series=["SER"])
    r = Rig(StrategySpec(id="e", kind="test_rec", markets=sel, slot_by="entity"),
            StrategySpec(id="h", kind="test_rec", markets=sel, slot_by=ByLinker("half")),
            StrategySpec(id="x", kind="test_rec", markets=sel, slot_by="all"),
            StrategySpec(id="l", kind="test_rec", markets=MarketSelector(series=["SER"], linker="half")),
            linkers=linkers)
    for t, ev in ((T1, "SER-EV1"), (T2, "SER-EV1"), (T3, "SER-EV2")):
        r.send(contract(t, event=ev))
    assert sorted(r.router.slots) == sorted([
        "e/ent:SER-EV1", "e/ent:SER-EV2", "h/A", "h/B", "x/all", "l/" + T1, "l/" + T2, "l/" + T3])
    assert r.router.slots["h/A"].ctx.tickers == [T1, T3]
    assert r.router.slots["x/all"].ctx.tickers == [T1, T2, T3]


# ---- context -------------------------------------------------------------------------------------------


def test_context_reads_inputs_advice_schedules_and_forecasts():
    linkers = {"ev": lambda c: f"ent:{c.event_ticker}"}
    r = Rig(rec("a", model="m", inputs=["feed"], advisors=["adv", "manual"], risk={"stale_input_ms": 2_000},
                markets=MarketSelector(tickers=[T1])), linkers=linkers)
    r.send(contract(T1))
    m: MarketContext = r.router.slots["a/" + T1].ctx
    assert m.input("feed") is None and m.advice() is None and m.forecast() is None
    r.send(Tick(1.5), at=1_000, source="feed")
    r.send(Forecast("m", T1, 0.55, 1_000, 900))
    r.send(Forecast("other", T1, 0.45, 1_000, 900))
    r.send(Advice("adv", "ent:SER-EV1", 0.6, None, 0.5, 5_000, "entity-wide", 1_000))
    r.send(Advice("manual", T1, 0.7, "buy", 1.0, 3_000, "mine", 1_100))
    r.send(Schedule("ent:SER-EV1", "kickoff", 10_000, "kickoff", 1_000))
    r.send(Schedule(T1, "report", 4_000, "report", 1_000))
    assert m.input("feed") == Tick(1.5) and m.forecast().p == 0.55 and m.forecast("other").p == 0.45
    assert m.advice().rationale == "mine" and m.advice("adv").rationale == "entity-wide"
    assert m.next_schedule().kind == "report" and m.next_schedule("kickoff").at_ms == 10_000
    r.send(Timer(3_500), at=3_500)
    assert m.input("feed") is None  # older than stale_input_ms
    assert m.advice().rationale == "entity-wide"  # "manual" expired at 3_000
    r.send(Timer(4_500), at=4_500)
    assert m.next_schedule().kind == "kickoff"
    with pytest.raises(ValueError, match="isn't declared"):
        m.input("nope")
    with pytest.raises(ValueError, match="isn't listed"):
        m.advice("nope")


def test_context_rejects_impossible_orders_and_the_slot_errors():
    @strategy("test_bad")
    class Bad(Strategy):
        class Params(msgspec.Struct, frozen=True):
            how: str = ""

        def on_book(self, book, m):
            match self.p.how:
                case "float":
                    m.take("bid", 0.45, 100)
                case "offgrid":
                    m.quote(bid=(4_050, 100), ask=None)
                case "crossed":
                    m.quote(bid=(4_500, 100), ask=(4_400, 100))
                case "zero":
                    m.take("ask", 4_000, 0)

    hows = ("float", "offgrid", "crossed", "zero")
    r = Rig(*(StrategySpec(id=h, kind="test_bad", params={"how": h}, markets=MarketSelector(tickers=[T1]))
              for h in hows))
    r.send(contract(T1))
    r.send(snap(T1))
    errors = {sid.split("/")[0]: s.error for sid, s in r.router.slots.items()}
    assert "integer" in errors["float"] and "price grid" in errors["offgrid"]
    assert "crosses itself" in errors["crossed"] and "positive" in errors["zero"]
    assert [i for i in r.intents() if not isinstance(i, CancelAll)] == []


def test_intents_fees_and_last_action():
    r = Rig(rec("a", markets=MarketSelector(tickers=[T1])))
    r.send(contract(T1))
    m: MarketContext = r.router.slots["a/" + T1].ctx
    m.quote(bid=(4_000, 500), ask=None)
    m.take("ask", 3_900, 150)
    m.target(1_000, style="improve", max_cross_c=2)
    m.cancel_all()
    slot = r.router.slots["a/" + T1]
    assert slot.intents == [Quote(m.slot, T1, (4_000, 500), None), Take(m.slot, T1, "ask", 3_900, 150),
                            Target(m.slot, T1, 1_000, "improve", {"max_cross_c": 2}), CancelAll(m.slot, T1)]
    assert [describe(i) for i in slot.intents] == ["quote 5 @ 40¢ / –", "take sell 1.5 @ 39¢",
                                                   "target 10 (improve)", "cancel all"]
    assert describe(slot.intents[0], group=True) == f"{T1}: quote 5 @ 40¢ / –"
    assert m.taker_fee(5_000) == 175 and m.maker_fee(5_000) == 44  # 7% / 1.75% × p(1-p), rounded up
    assert m.taker_fee(100) == 7
    r.send(msgspec.structs.replace(contract(T1), fees=FeeSchedule(700, 0)))  # a series without maker fees
    assert m.maker_fee(5_000) == 0 and m.taker_fee(5_000) == 175


def test_halt_events_stop_hooks_by_scope_and_resume():
    r = Rig(rec("a"), rec("b"))
    for t in (T1, T2):
        r.send(contract(t))
    r.send(Halt("a", "testing", True, 0))
    r.clear()
    r.send(Timer(1_000), at=1_000)
    assert not r.seen("a/" + T1) and not r.seen("a/" + T2) and r.seen("b/" + T1)
    assert {s.slot: s.state for s in r.events(SlotStatus)}.get("a/" + T1) is None  # unchanged since the halt
    assert r.router.slots["a/" + T1].status.state == "halted"
    r.send(Halt("", "everything", True, 0))
    r.send(Halt("", "", False, 0))
    r.send(Timer(2_000), at=2_000)
    assert kinds(r.seen("a/" + T1)) == ["timer"] and r.router.slots["a/" + T1].status.state == "idle"


def test_slot_status_is_throttled_to_once_a_second_plus_state_changes():
    r = Rig(rec("a", params={"take": "1"}, markets=MarketSelector(tickers=[T1])), fill_takes=True)
    r.send(contract(T1))
    for i in range(50):
        r.send(snap(T1), at=100 + i * 10)
    r.send(Timer(1_000), at=1_000)
    r.send(Timer(2_000), at=2_000)
    statuses = r.events(SlotStatus)
    assert [(s.state, s.ts_ms) for s in statuses] == [("idle", 0), ("active", 1_000)]
    assert statuses[-1].position == 5_000


def test_checkpoints_round_trip_through_on_save_and_on_load():
    r = Rig(rec("a", markets=MarketSelector(tickers=[T1])))
    r.send(contract(T1))
    r.send(Timer(1_000), at=1_000)
    blobs = r.router.checkpoint()
    assert blobs == {"a/" + T1: b"2"}
    r2 = Rig(rec("a", markets=MarketSelector(tickers=[T1])))
    r2.router.restore(blobs)
    r2.send(contract(T1))
    assert r2.seen("a/" + T1) == [("load", b"2"), ("start",)]


def test_stop_all_stops_every_slot_and_cancels():
    r = Rig(rec("a"))
    for t in (T1, T2):
        r.send(contract(t))
    out = r.router.stop_all(5_000)
    assert all(s.stopped for s in r.router.slots.values())
    assert {i.slot for i in r.router.take_intents() if isinstance(i, CancelAll)} == {"a/" + T1, "a/" + T2}
    assert [s.state for s in out if isinstance(s, SlotStatus)] == ["idle", "idle"]
    assert r.seen("a/" + T1)[-1] == ("stop",)


def test_strategy_specs_are_checked_when_the_router_is_built():
    with pytest.raises(PluginError, match="unknown kind 'nope'"):
        Router([StrategySpec(id="x", kind="nope")])
    with pytest.raises(PluginError, match="unknown params key 'bogus'"):
        Router([StrategySpec(id="x", kind="test_rec", params={"bogus": 1})])
    with pytest.raises(PluginError, match="duplicate strategy id"):
        Router([rec("x"), rec("x")])
    with pytest.raises(PluginError, match="unknown linker 'nfl'"):
        Router([rec("x", markets=MarketSelector(series=["S"], linker="nfl"))], linkers={})
    with pytest.raises(PluginError, match="no '/'"):
        Router([rec("x/y")])
    spec = msgspec.convert({"id": "s", "kind": "test_rec", "markets": {"series": ["KXNFLGAME"], "linker": "ev"},
                            "slot_by": {"linker": "ev"}, "risk": {"max_position": "5"}}, StrategySpec)
    assert spec.slot_by == ByLinker("ev") and spec.risk == {"max_position": "5"}
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"id": "s", "kind": "test_rec", "slot_by": "team"}, StrategySpec)


def test_order_tracker_average_cost_pnl_and_working_orders():
    t = OrderTracker()
    t.state = State()
    t.state.apply(snap(T1, "0.50", "0.54"), 0)

    def fill(side, price, qty, fee=0):
        t.on_event(SlotFill("s/x", "paper", Fill("kalshi", "f", "o", T1, side, parse_price(price), parse_qty(qty),
                                                 fee, True, 0)))

    fill("bid", "0.40", "10", fee=10)
    fill("bid", "0.46", "10", fee=10)  # avg 0.43
    fill("ask", "0.50", "5")  # realize (0.50 - 0.43) × 5
    assert t.position("s/x", T1) == 1_500
    st = t.stats("slot:s/x", "paper")
    assert st.realized == 350_000 and st.fees == 20
    assert st.unrealized == round(1_500 * 5_200) - 6_450_000  # 15 @ mid 0.52 vs 15 @ 0.43
    fill("ask", "0.30", "20")  # close 15 (loss), then short 5
    assert t.position("s/x", T1) == -500 and t.stats("slot:s/x", "paper").realized == 350_000 - 1_950_000
    assert t.stats("strategy:s", "paper").realized == t.stats("portfolio", "paper").realized
    assert t.stats("slot:s/x", "live").realized == 0
    t.on_event(OrderUpdate("s/x", "paper", "c1", None, T1, "bid", 4_000, 100, 0, "queued", 7))
    assert t.throttled("s/x") and t.working("s/x", T1)[0].placed_ms == 7
    t.on_event(OrderUpdate("s/x", "paper", "c1", "o1", T1, "bid", 4_000, 100, 0, "open", 9))
    assert not t.throttled("s/x") and t.working("s/x", T1)[0].placed_ms == 7
    t.on_event(OrderUpdate("s/x", "paper", "c1", "o1", T1, "bid", 4_000, 100, 100, "filled", 9))
    assert t.working("s/x", T1) == []


# ---- reference strategies ----------------------------------------------------------------------------


def test_noop_lives_and_stops_quietly():
    sc = Scenario("noop")
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    sc.advance(1)
    sc.contract(status="settled")
    assert sc.intents == [kit.CancelAll(), kit.CancelAll()]  # the market closing, then the slot stopping
    assert sc.slot.stopped and sc.slot.status.state == "idle" and sc.logs == []


def test_watch_logs_when_model_and_mid_disagree():
    sc = Scenario("watch", params={"threshold": 0.05})
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])  # mid 0.42
    for p in (0.45, 0.48, 0.49, 0.30, 0.43):
        sc.forecast(p)
    sc.book(bids=[("0.20", "10")], asks=[("0.24", "10")])  # mid 0.22: 0.43 is now far above
    assert sc.logs == [
        "signal buy: m 0.480 vs mid 0.420 (+0.060)",
        "signal sell: m 0.300 vs mid 0.420 (-0.120)",
        "signal off: m 0.430 vs mid 0.420 (+0.010)",
        "signal buy: m 0.430 vs mid 0.220 (+0.210)",
    ]
    assert sc.intents == []


def test_scripted_smoke_script_rests_cancels_takes_and_sells_back():
    sc = Scenario("scripted", params={"script": "config/scripts/smoke.toml"}, start_ms=500)
    sc.advance(0.5)  # the Timer at 1 s: no book yet, so the first step waits
    assert sc.intents == [] and "waiting for a mid price" in sc.logs[0]
    sc.advance(0.5)
    sc.book(bids=[("0.42", "10")], asks=[("0.47", "10")])  # mid 0.445 → bid 0.395 → 0.39 on the cent grid
    by_time = {}
    for s in range(2, 200):
        sc.advance(1)  # the Timer at s seconds
        if got := sc.intents:
            by_time[s] = got
    assert by_time == {
        2: [kit.Quote(bid=("0.39", "1"))],
        61: [kit.CancelAll(), kit.Take("bid", "0.47", "1")],
        181: [kit.Take("ask", "0.42", "1")],
    }


def test_scripted_jitter_spreads_slots_deterministically():
    steps = [{"at": "+0s", "action": "quote", "side": "bid", "price": "0.40"}]

    def first_quote_s(ticker: str) -> int:
        sc = Scenario("scripted", params={"steps": steps, "jitter_s": 30}, ticker=ticker)
        sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
        for s in range(1, 40):
            sc.advance(1)
            if sc.intents:
                return s
        raise AssertionError("never quoted")

    starts = [first_quote_s(f"KX-{i}") for i in range(8)]
    assert all(1 <= s <= 31 for s in starts) and len(set(starts)) >= 5, starts
    assert starts == [first_quote_s(f"KX-{i}") for i in range(8)], "the same slot always starts at the same offset"


def test_scripted_snaps_to_the_grid_loops_and_takes_absolute_times():
    steps = [{"at": "+0s", "action": "quote", "side": "ask", "price": "mid+0.011"},
             {"at": "+1s", "action": "quote", "side": "bid", "price": "bid-0.01", "qty": "2"},
             {"at": "+2s", "action": "cancel_all"}]
    sc = Scenario("scripted", params={"steps": steps, "loop": True})
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    sc.advance(6)
    ask, both = kit.Quote(ask=("0.44", "1")), kit.Quote(bid=("0.39", "2"), ask=("0.44", "1"))
    assert sc.intents == [ask, both, kit.CancelAll()] * 3  # each pass restarts when the last step ran
    p = plugins.build_params(Scripted, {"steps": [{"at": "2026-10-18T17:05:00Z", "action": "take", "side": "bid",
                                                   "price": "0.40"}]})
    assert Scripted(p).plan[0].at_ms == 1_792_343_100_000
    for bad, why in (({"at": "soon", "action": "cancel_all"}, "expected '\\+10s'"),
                     ({"at": "+1s", "action": "take", "price": "0.4"}, "needs a side"),
                     ({"at": "+1s", "action": "take", "side": "bid", "price": "mod-1"}, "must be 'mid'"),
                     ({"at": "2026-10-18T17:05:00", "action": "cancel_all"}, "time zone")):
        with pytest.raises(PluginError, match=why):
            plugins.build_params(Scripted, {"steps": [bad]})
    with pytest.raises(PluginError, match="no steps"):
        plugins.build_params(Scripted, {})


# ---- rules ---------------------------------------------------------------------------------------------

_FORBIDDEN_MODULES = {"time", "socket", "asyncio", "subprocess", "requests", "httpx", "urllib", "aiohttp",
                      "websockets", "threading", "os", "random"}
_FORBIDDEN_CALLS = {"now", "utcnow", "today", "time", "monotonic", "perf_counter", "sleep", "place", "cancel",
                    "read_text", "read_bytes", "write_text", "write_bytes"}


def test_no_strategy_code_calls_the_broker_or_the_clock_or_does_io():
    """Strategies are deterministic: no broker, no clock, no network, no files. The one exception
    is `scripted` reading its script file once, when the config loads (`load_script`)."""
    root = pathlib.Path(plugins.__file__).parent / "strategies"
    problems = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [(node.module or "").split(".")[0]]
            else:
                names = []
            problems += [f"{path.name}: imports {n}" for n in names if n in _FORBIDDEN_MODULES]
            if isinstance(node, ast.ImportFrom) and node.module and "execution" in node.module:
                problems.append(f"{path.name}: imports {node.module}")
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
                if name in _FORBIDDEN_CALLS:
                    problems.append(f"{path.name}:{node.lineno}: calls {name}()")
                if name == "open" and not _inside(tree, node, "load_script"):
                    problems.append(f"{path.name}:{node.lineno}: opens a file")
    assert problems == []


def _inside(tree: ast.AST, target: ast.AST, fn_name: str) -> bool:
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name == fn_name and any(n is target for n in ast.walk(fn)):
            return True
    return False
