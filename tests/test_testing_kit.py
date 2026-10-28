"""The strategy test kit itself: drivers, assertions, and the GUIDE example verbatim."""

from __future__ import annotations

import msgspec
import pytest

from pmx.core.units import ONE_DOLLAR, parse_price, parse_qty
from pmx.engine.events import Advice
from pmx.plugins import strategy
from pmx.sports.nfl.testing import nfl_state
from pmx.strategies.base import MarketContext, Strategy
from pmx.testing import CancelAll, GroupScenario, Quote, Scenario, Take, Target

# ---- the GUIDE example, verbatim ----------------------------------------------------------------------------


@strategy("edge_taker")
class EdgeTaker(Strategy):
    """Cross the spread when the model's fair value beats the price by min_edge after fees."""

    class Params(msgspec.Struct, frozen=True):
        min_edge: float = 0.04          # probability points
        size: str = "5"                 # contracts per take
        max_spread: str = "0.03"        # dollars; skip wide books

    def on_forecast(self, f, m: MarketContext) -> None:
        book = m.book
        if book is None or (book.spread() or ONE_DOLLAR) > parse_price(self.p.max_spread):
            return
        fair = round(f.p * ONE_DOLLAR)
        ask, bid = book.best_ask(), book.best_bid()
        if ask and fair - ask[0] - m.taker_fee(ask[0]) >= self.p.min_edge * ONE_DOLLAR:
            m.take("bid", limit=ask[0], qty=parse_qty(self.p.size))
        elif bid and bid[0] - fair - m.taker_fee(bid[0]) >= self.p.min_edge * ONE_DOLLAR:
            m.take("ask", limit=bid[0], qty=parse_qty(self.p.size))


def test_takes_when_edge_beats_fees():
    sc = Scenario("edge_taker", params={"min_edge": 0.04, "size": "5"})
    sc.book(bids=[("0.55", "100")], asks=[("0.57", "100")])
    sc.forecast(0.63)
    assert sc.intents == [Take("bid", "0.57", "5")]


def test_guide_strategy_sells_and_skips_wide_books():
    sc = Scenario("edge_taker", params={"min_edge": 0.04, "size": "5"})
    sc.book(bids=[("0.55", "100")], asks=[("0.57", "100")])
    sc.forecast(0.49)
    assert sc.intents == [Take("ask", "0.55", "5")]
    sc.forecast(0.58)  # inside the edge
    assert sc.intents == []
    sc.book(bids=[("0.50", "100")], asks=[("0.57", "100")])  # 7¢ wide
    sc.forecast(0.80)
    assert sc.intents == []


# ---- a strategy that records what it sees -----------------------------------------------------------------


class Probe(Strategy):
    """Unregistered on purpose: `Scenario` takes a class too."""

    inputs = ("feed",)

    class Params(msgspec.Struct, frozen=True):
        quote: bool = False
        boom: str = ""  # hook name that raises

    def __init__(self, params):
        super().__init__(params)
        self.seen: list = []

    def _maybe_boom(self, hook):
        if self.p.boom == hook:
            raise RuntimeError(f"boom in {hook}")

    def on_book(self, book, m):
        self._maybe_boom("on_book")
        self.seen.append(("book", m.now_ms, book.best_bid(), book.best_ask()))
        if self.p.quote and book.best_bid() and book.best_ask():
            m.quote(bid=(book.best_bid()[0], 200), ask=(book.best_ask()[0], 200))

    def on_trade(self, t, m):
        self.seen.append(("trade", t.price, t.qty, t.taker_side))

    def on_forecast(self, f, m):
        self.seen.append(("forecast", f.model, f.p, m.now_ms - f.data_ts_ms))

    def on_data(self, data, m):
        self.seen.append(("data", data.summary()))

    def on_input(self, name, ev, m):
        self.seen.append(("input", name, m.input(name)))

    def on_advice(self, a, m):
        self.seen.append(("advice", a.p, a.direction))

    def on_fill(self, f, m):
        self.seen.append(("fill", f.fill.side, f.fill.qty, m.position))

    def on_timer(self, m):
        self.seen.append(("timer", m.now_ms))

    def on_stop(self, m):
        self.seen.append(("stop",))


class Tick(msgspec.Struct, frozen=True):
    value: float


def probe(**kw) -> Scenario:
    params = kw.pop("params", {})
    return Scenario(Probe, params=params, **kw)


def seen(sc: Scenario, kind: str | None = None) -> list:
    out = sc.slot.instance.seen
    return [s for s in out if kind is None or s[0] == kind]


# ---- intents --------------------------------------------------------------------------------------------------


def test_intents_compare_in_human_units_and_clear_on_read():
    assert Take("bid", "0.57", "5") == Take("bid", 5_700, 500)
    assert Quote(bid=("0.40", "2"), ask=None) == Quote((4_000, 200), None)
    assert Target("10", "improve") == Target(1_000, "improve")
    assert repr(Take("ask", "0.505", "1.5")) == "Take('ask', '0.505', '1.5')"
    assert repr(Quote(bid=("0.4", "2"))) == "Quote(bid=('0.40', '2'), ask=None)"
    sc = probe(params={"quote": True})
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert sc.intents == [Quote(bid=("0.40", "2"), ask=("0.44", "2"))]
    assert sc.intents == []
    sc.m.take("bid", 4_400, 100)
    sc.m.target(500, style="improve")
    sc.m.cancel_all()
    sc.advance(0)  # no event yet: intents a test makes directly show up on the next one
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert sc.intents == [Take("bid", "0.44", "1"), Target("5", "improve"), CancelAll(),
                          Quote(bid=("0.40", "2"), ask=("0.44", "2")),
                          Quote(bid=("0.41", "5"), ask=None)]  # the target helper: one tick inside the best bid


def test_quotes_rest_until_replaced_pulled_or_filled():
    sc = probe(params={"quote": True})
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert {(o.side, o.price, o.qty) for o in sc.working} == {("bid", 4_000, 200), ("ask", 4_400, 200)}
    sc.fill("bid", "0.40", "1")
    assert sc.position == 100 and seen(sc, "fill") == [("fill", "bid", 100, 100)]
    assert {(o.side, o.filled) for o in sc.working} == {("bid", 100), ("ask", 0)}
    sc.fill("bid", "0.40", "1")
    assert [o.side for o in sc.working] == ["ask"]  # fully filled: gone
    sc.book(bids=[("0.41", "10")], asks=[("0.44", "10")])  # requote the bid; the ask is unchanged
    assert {(o.side, o.price) for o in sc.working} == {("bid", 4_100), ("ask", 4_400)}
    sc.m.cancel_all()
    sc.advance(1)
    assert sc.working == [] and sc.position == 200


def test_fills_take_fees_in_dollars_and_fill_without_a_resting_order():
    sc = probe()
    sc.fill("ask", "0.60", "3", fee="0.05")
    assert sc.position == -300 and sc.working == []


# ---- drivers ------------------------------------------------------------------------------------------------------


def test_book_delta_trade_and_reset_drive_the_market():
    sc = probe()
    assert sc.m.book is None
    sc.book(bids=[("0.40", "10"), ("0.39", "5")], asks=[("0.45", "10")])
    sc.delta("ask", "0.44", "3")
    sc.delta("bid", "0.40", "-10")
    assert seen(sc, "book")[-1] == ("book", 0, (3_900, 500), (4_400, 300))
    sc.trade("0.44", "2", "bid")
    assert seen(sc, "trade") == [("trade", 4_400, 200, "bid")]
    sc.reset()
    assert sc.m.book is None  # stale until the next snapshot (invariant 8)
    sc.delta("bid", "0.41", "1")
    assert sc.m.book is None
    sc.book(bids=[("0.41", "1")], asks=[("0.44", "1")])
    assert sc.m.book is not None


def test_forecasts_default_to_the_strategy_model_and_carry_their_age():
    sc = probe(model="wp", start_ms=10_000)
    sc.forecast(0.61, age_s=2.5)
    sc.forecast(0.30, model="other")  # not the strategy's model: no hook, but readable
    assert seen(sc, "forecast") == [("forecast", "wp", 0.61, 2_500)]
    assert sc.m.forecast().p == 0.61 and sc.m.forecast("other").p == 0.30


def test_entity_data_reaches_on_data_with_the_nfl_helper():
    sc = probe(ticker="KXNFLGAME-26OCT18HOUJAC-JAC")
    sc.data(nfl_state(q=3, clock="4:12", home=17, away=14, possession="home", espn_home_wp=0.71))
    sc.data(nfl_state(q=4, clock="0:00", home=20, away=14, status="post", winner="home"))
    assert seen(sc, "data") == [("data", "Q3 4:12 HOU 14-17 JAX"), ("data", "final HOU 14-20 JAX")]
    assert sc.m.data().winner == "home" and sc.m.data().entity == "nfl:2026-10-18:HOU-JAX"
    pre = nfl_state(q=0)
    assert (pre.status, pre.period, pre.clock_s, pre.away, pre.home) == ("pre", 0, 900, "HOU", "JAX")


def test_inputs_reach_on_input_and_go_stale():
    sc = probe(risk={"stale_input_ms": 1_500})
    sc.input("feed", Tick(1.0))
    assert seen(sc, "input") == [("input", "feed", Tick(1.0))]
    sc.advance(1)
    assert sc.m.input("feed") == Tick(1.0)
    sc.advance(1)
    assert sc.m.input("feed") is None  # older than stale_input_ms
    with pytest.raises(ValueError, match="isn't declared"):
        sc.m.input("other")


def test_advice_expires_and_lapsed_advice_never_reaches_on_advice():
    sc = probe(advisors=("news",))
    sc.advice(p=0.62, expires_s=1.5, rationale="injury report")
    assert seen(sc, "advice") == [("advice", 0.62, None)]
    assert sc.m.advice().rationale == "injury report" and sc.m.advice("news").p == 0.62
    sc.advance(1)
    assert sc.m.advice() is not None
    sc.advance(1)
    assert sc.m.advice() is None
    sc.advice(direction="sell", expires_s=-1)  # already lapsed when it arrives
    assert seen(sc, "advice") == [("advice", 0.62, None)] and sc.m.advice() is None
    with pytest.raises(ValueError, match="isn't listed"):
        sc.m.advice("someone")
    assert isinstance(sc.state.advice, dict) and all(isinstance(a, Advice) for a in sc.state.advice.values())


def test_advance_fires_timers_each_second_and_throttle_shows_in_the_context():
    sc = probe(start_ms=500)
    sc.advance(2.6)
    assert seen(sc, "timer") == [("timer", 1_000), ("timer", 2_000), ("timer", 3_000)] and sc.now_ms == 3_100
    assert not sc.m.throttled
    sc.throttle()
    assert sc.m.throttled
    sc.throttle(False)
    assert not sc.m.throttled


def test_cadence_coalesces_book_hooks_to_the_latest_state():
    sc = probe(cadence_ms=1_000)
    for bid in ("0.40", "0.41", "0.42", "0.43"):
        sc.book(bids=[(bid, "1")], asks=[("0.50", "1")])
    assert seen(sc, "book") == []  # the first change opens the window
    sc.advance(1)
    assert seen(sc, "book") == [("book", 1_000, (4_300, 100), (5_000, 100))]  # once, latest state
    sc.fill("bid", "0.50", "1")  # fills are never delayed
    assert seen(sc, "fill") == [("fill", "bid", 100, 100)]


def test_warm_up_and_risk_rejects():
    sc = Scenario("edge_taker", params={"min_edge": 0.04, "size": "5"}, warmup_s=2,
                  risk={"max_position": "8"})
    sc.book(bids=[("0.55", "100")], asks=[("0.57", "100")])
    sc.forecast(0.70)
    assert not sc.ready and sc.intents == [Take("bid", "0.57", "5")]  # what it wanted…
    assert [r.rule for r in sc.rejects] == ["warming"]  # …and why it didn't happen
    assert sc.rejects == []
    sc.advance(2)
    assert sc.ready
    sc.forecast(0.70)
    assert sc.intents == [Take("bid", "0.57", "5")] and sc.rejects == []
    sc.fill("bid", "0.57", "5")
    sc.forecast(0.70)
    assert [r.rule for r in sc.rejects] == ["max_position"]


def test_with_risk_adds_global_limits():
    sc = Scenario("edge_taker", params={"size": "5"}).with_risk({"max_order_qty": "2"})
    sc.book(bids=[("0.55", "100")], asks=[("0.57", "100")])
    sc.forecast(0.70)
    assert sc.intents == [Take("bid", "0.57", "5")] and [r.rule for r in sc.rejects] == ["max_order_qty"]


def test_contract_updates_schedules_and_closing():
    sc = probe(params={"quote": True}, start_ms=1_000)
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    sc.intents  # noqa: B018  (clear)
    sc.schedule("release", in_s=60, detail="CPI m/m")
    assert sc.m.next_schedule("release").at_ms == 61_000 and sc.m.next_schedule("kickoff") is None
    sc.contract(status="closed")
    assert sc.intents == [CancelAll(), CancelAll()] and seen(sc)[-1] == ("stop",)
    assert sc.working == []


def test_a_raising_strategy_errors_its_slot():
    sc = probe(params={"boom": "on_book"})
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert "boom in on_book" in sc.slot.error and sc.intents == [CancelAll()]
    assert any("boom in on_book" in line for line in sc.logs)
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert seen(sc, "book") == []  # no more hooks


def test_strategy_kinds_and_classes_and_bad_input():
    assert Scenario("noop").slot.id == "test/TEST-1"
    assert Scenario(Probe).slot.instance.__class__ is Probe
    with pytest.raises(TypeError, match="Strategy subclass"):
        Scenario(object)


# ---- group slots ---------------------------------------------------------------------------------------------------


@strategy("kit_pair")
class Pair(Strategy):
    """Quotes the two markets of an event so their bids sum to under $1."""

    def on_book(self, book, m):
        a, b = m.tickers
        books = [m.market(t).book for t in (a, b)]
        if all(bk is not None and bk.best_bid() for bk in books):
            m.quote(a, bid=(books[0].best_bid()[0], 100), ask=None)
            m.quote(b, bid=(books[1].best_bid()[0], 100), ask=None)


def test_group_scenario_trades_every_member_and_intents_name_their_ticker():
    sc = GroupScenario("kit_pair", tickers=["EV-26OCT18-A", "EV-26OCT18-B"])
    assert sc.slot.id == "test/EV-26OCT18" and sc.m.tickers == ["EV-26OCT18-A", "EV-26OCT18-B"]
    sc.book(bids=[("0.40", "1")], asks=[("0.45", "1")], ticker="EV-26OCT18-A")
    assert sc.intents == []
    sc.book(bids=[("0.52", "1")], asks=[("0.58", "1")], ticker="EV-26OCT18-B")
    assert sc.intents == [Quote(bid=("0.40", "1"), ticker="EV-26OCT18-A"),
                          Quote(bid=("0.52", "1"), ticker="EV-26OCT18-B")]
    sc.fill("bid", "0.52", "1", ticker="EV-26OCT18-B")
    assert sc.desk.positions == {"EV-26OCT18-B": 100}
    assert sc.m.market("EV-26OCT18-B").position == 100 and sc.m.market("EV-26OCT18-A").working[0].price == 4_000
