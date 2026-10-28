from __future__ import annotations

from datetime import UTC, datetime

import msgspec
import pytest

from pmx.core.types import BookSnapshot, Contract, OrderRequest
from pmx.engine.events import Exposure, Forecast, Halt, LogEvent, RiskReject, Schedule, ScopeStats, Timer
from pmx.engine.state import State
from pmx.execution.risk import RiskLimits, RiskManager, Window
from pmx.strategies.base import WorkingOrder

T, T2 = "SER-EV1-A", "SER-EV1-B"
NOW = 1_792_330_000_000  # 2026-10-18T13:26:40Z


class Slot:
    def __init__(self, id: str, *, mode="paper", model=None, inputs=(), risk=None, warming=False, tickers=(T,)):
        self.id, self.mode, self.model, self.inputs, self.risk_limits = id, mode, model, tuple(inputs), risk
        self.warming = warming
        self.tickers = dict.fromkeys(tickers)


def _in(scope: str, slot: str) -> bool:
    return not scope or slot == scope or slot.startswith(scope + "/")


class Book:
    """A fake order manager: positions, working orders, PnL per slot."""

    def __init__(self) -> None:
        self.pos: dict[tuple[str, str], int] = {}
        self.orders: list[tuple[str, str, WorkingOrder]] = []
        self.pnls: dict[str, int] = {}

    def position(self, scope, ticker, mode=None):
        return sum(q for (s, t), q in self.pos.items() if _in(scope, s) and t == ticker)

    def working(self, scope, ticker=None, mode=None):
        return [o for s, t, o in self.orders if _in(scope, s) and (ticker is None or t == ticker)]

    def pnl(self, scope, mode, state):
        return sum(v for s, v in self.pnls.items() if _in(scope, s))

    def rest(self, slot: str, side: str, qty: int, ticker: str = T) -> None:
        n = len(self.orders)
        self.orders.append((slot, ticker, WorkingOrder(f"c{n}", side, 4_000, qty, 0, "open", None, 0)))


def state(*, book_ts: int = NOW, status: str = "open") -> State:
    s = State()
    for t in (T, T2):
        s.apply(Contract("kalshi", t, "SER-EV1", "Title", "Sub", status, None), NOW)
        s.apply(BookSnapshot("kalshi", t, ((4_000, 1_000),), ((4_200, 1_000),), book_ts), NOW)
    return s


def req(side: str = "bid", price: int = 4_200, qty: int = 100, ticker: str = T) -> OrderRequest:
    return OrderRequest("kalshi", ticker, side, price, qty, strategy="s/" + ticker)


def manager(*slots: Slot, book: Book | None = None, **kw) -> tuple[RiskManager, Book]:
    book = book or Book()
    return RiskManager(positions=book, slots={s.id: s for s in slots}, **kw), book


def rule(r: RiskReject | None) -> str | None:
    return None if r is None else r.rule


# ---- config ------------------------------------------------------------------------------------------------


def test_limits_parse_money_and_quantities_from_strings():
    lim = RiskLimits.of({"max_position": "50", "max_order_qty": 10, "max_loss": "25.00", "price_band": "0.05",
                         "stale_book_ms": 5000, "windows": [{"kind": "release", "before_s": 60, "after_s": 10}]})
    assert (lim.max_position, lim.max_order_qty, lim.max_loss, lim.price_band) == (5_000, 1_000, 25_000_000, 500)
    assert lim.windows == [Window("release", 60, 10, "pull")] and not lim.portfolio_level
    assert RiskLimits.of({"max_drawdown": "300.00"}).portfolio_level
    assert RiskLimits.of(None) == RiskLimits() and RiskLimits.of(lim) is lim
    with pytest.raises(ValueError, match="unknown limit 'max_postion'"):
        RiskLimits.of({"max_postion": "5"})
    with pytest.raises(ValueError, match="must be a string"):
        RiskLimits.of({"max_loss": 2.5})
    with pytest.raises(ValueError, match="must be an integer"):
        RiskLimits.of({"stale_book_ms": "5000"})


# ---- the rules, in order ------------------------------------------------------------------------------------


def test_a_plain_order_passes():
    risk, _ = manager(Slot("s/" + T, risk={"max_position": "5"}))
    assert risk.check("s/" + T, req(), state(), NOW) is None


def test_halts_by_slot_strategy_and_everything_and_resume():
    risk, _ = manager(Slot("s/" + T), Slot("t/" + T))
    canceled: list = []
    risk.on_cancel.append(lambda scope, ticker, why: canceled.append((scope, why)))
    st = state()
    risk.halt("testing", "s/" + T, NOW)
    assert rule(risk.check("s/" + T, req(), st, NOW)) == "halted" and risk.check("t/" + T, req(), st, NOW) is None
    risk.halt("strategy-wide", "t", NOW)
    r = risk.check("t/" + T, req(), st, NOW)
    assert r.rule == "halted" and r.detail == "t halted: strategy-wide"
    risk.resume("t", NOW)
    risk.halt("lunch", None, NOW)
    assert risk.check("t/" + T, req(), st, NOW).detail == "everything halted: lunch"
    assert canceled == [("s/" + T, "halted: testing"), ("t", "halted: strategy-wide"), ("", "halted: lunch")]
    events = risk.on_event(Timer(NOW), st, NOW)
    assert [e for e in events if type(e) is Halt] == [
        Halt("s/" + T, "testing", True, NOW), Halt("t", "strategy-wide", True, NOW), Halt("t", "", False, NOW),
        Halt("", "lunch", True, NOW)]
    risk.on_event(Halt("", "", False, NOW), st, NOW)  # resumed elsewhere (`pmx resume`)
    risk.on_event(Halt("t/" + T, "slow", True, NOW), st, NOW)  # the router halted a slow slot
    assert risk.check("t/" + T, req(), st, NOW).detail == f"t/{T} halted: slow"


def test_market_not_open():
    risk, _ = manager(Slot("s/" + T))
    assert risk.check("s/" + T, req(ticker="NOPE-1"), state(), NOW).detail == "NOPE-1 isn't a known market"
    assert risk.check("s/" + T, req(), state(status="paused"), NOW).detail == f"{T} is paused"


def test_warming_blocks_new_risk_but_not_flattening():
    risk, book = manager(Slot("s/" + T, warming=True))
    assert risk.check("s/" + T, req(), state(), NOW).rule == "warming"
    book.pos[("s/" + T, T)] = 300
    assert risk.check("s/" + T, req("ask", 4_000, 200), state(), NOW) is None  # reduces a long


def test_schedule_windows_block_new_risk_and_pull_quotes_once():
    slot = Slot("s/" + T, risk={"windows": [{"kind": "release", "before_s": 60, "after_s": 10}]})
    risk, _ = manager(slot)
    pulls: list = []
    risk.on_cancel.append(lambda scope, ticker, why: pulls.append((scope, ticker, why)))
    st = state()
    st.apply(Schedule(T, "release", NOW + 30_000, "CPI m/m", NOW), NOW)
    r = risk.check("s/" + T, req(), st, NOW)
    assert r.rule == "window" and "release at 13:27:10Z (CPI m/m)" in r.detail
    assert risk.check("s/" + T, req(), st, NOW - 31_000) is None  # before the window opens
    assert risk.check("s/" + T, req(), st, NOW + 41_000) is None  # after it closes
    risk.on_event(Timer(NOW), st, NOW)
    risk.on_event(Timer(NOW + 1_000), st, NOW + 1_000)
    assert pulls == [("s/" + T, T, "window: release")]
    window = {"kind": "release", "before_s": 60, "after_s": 10, "action": "block_new"}
    block = Slot("b/" + T, risk={"windows": [window]})
    risk2, _ = manager(block)
    risk2.on_cancel.append(lambda *a: pulls.append(a))
    risk2.on_event(Timer(NOW), st, NOW)
    assert len(pulls) == 1 and risk2.check("b/" + T, req(), st, NOW).rule == "window"


def test_entity_schedules_apply_to_linked_markets():
    risk, _ = manager(Slot("s/" + T, risk={"windows": [{"kind": "kickoff", "before_s": 5, "after_s": 5}]}))
    st = State({"ev": lambda c: "game:1"})
    st.apply(Contract("kalshi", T, "SER-EV1", "Title", "Sub", "open", None), NOW)
    st.apply(BookSnapshot("kalshi", T, ((4_000, 1_000),), ((4_200, 1_000),), NOW), NOW)
    st.apply(Schedule("game:1", "kickoff", NOW, "kickoff", NOW), NOW)
    assert risk.check("s/" + T, req(), st, NOW).rule == "window"


def test_stale_book():
    risk, _ = manager(Slot("s/" + T, risk={"stale_book_ms": 5_000}))
    st = state(book_ts=NOW - 6_000)
    assert risk.check("s/" + T, req(), st, NOW).detail == "book last updated 6.0s ago (limit 5s)"
    st.apply(BookSnapshot("kalshi", T, ((4_000, 1_000),), ((4_200, 1_000),), NOW - 1_000), NOW)
    assert risk.check("s/" + T, req(), st, NOW) is None
    st.apply(Contract("kalshi", T, "SER-EV1", "Title", "Sub", "open", None), NOW)
    from pmx.core.types import StreamReset

    st.apply(StreamReset("kalshi", "gap", NOW), NOW)
    assert risk.check("s/" + T, req(), st, NOW).detail.startswith("no live book")


def test_stale_forecast_and_stale_input():
    slot = Slot("s/" + T, model="m", inputs=["feed"], risk={"stale_forecast_ms": 10_000, "stale_input_ms": 2_000})
    risk, _ = manager(slot)
    st = state()
    assert risk.check("s/" + T, req(), st, NOW).detail == "model m: none yet (limit 10s)"
    st.apply(Forecast("m", T, 0.42, NOW - 15_000, NOW - 15_000), NOW - 15_000)
    assert risk.check("s/" + T, req(), st, NOW).rule == "stale_forecast"
    st.apply(Forecast("m", T, 0.42, NOW - 1_000, NOW - 1_000), NOW - 1_000)
    assert risk.check("s/" + T, req(), st, NOW).detail == "input feed: nothing yet (limit 2s)"
    st.apply(Timer(0), NOW - 3_000, source="feed")
    assert risk.check("s/" + T, req(), st, NOW).detail == "input feed: 3.0s old (limit 2s)"
    st.apply(Timer(0), NOW - 500, source="feed")
    assert risk.check("s/" + T, req(), st, NOW) is None


def test_price_band_against_the_models_fair_else_the_mid():
    risk, _ = manager(Slot("s/" + T, model="m", risk={"price_band": "0.03"}))
    st = state()  # mid 0.41
    assert risk.check("s/" + T, req(price=4_400), st, NOW) is None  # 3¢ from mid
    r = risk.check("s/" + T, req(price=4_500), st, NOW)
    assert r.rule == "price_band" and r.detail == "45¢ is 4¢ from the mid 41¢ (band 3¢)"
    st.apply(Forecast("m", T, 0.48, NOW, NOW), NOW)
    assert risk.check("s/" + T, req(price=4_500), st, NOW) is None
    assert risk.check("s/" + T, req(price=4_000), st, NOW).detail == "40¢ is 8¢ from m's fair 48¢ (band 3¢)"


def test_max_order_qty_takes_the_tightest_scope():
    slot = Slot("s/" + T, risk={"max_order_qty": "10"})
    risk, _ = manager(slot, strategy_limits={"s": {"max_order_qty": "5"}}, global_limits={"max_order_qty": "20"})
    assert risk.check("s/" + T, req(qty=500), state(), NOW) is None
    assert risk.check("s/" + T, req(qty=600), state(), NOW).detail == "6.00 > 5.00 contracts"


def test_max_open_orders_counts_the_scope():
    risk, book = manager(Slot("s/" + T), Slot("s/" + T2, tickers=(T2,)), strategy_limits={"s": {"max_open_orders": 2}})
    book.rest("s/" + T, "bid", 100)
    assert risk.check("s/" + T2, req(ticker=T2), state(), NOW) is None
    book.rest("s/" + T2, "ask", 100, T2)
    assert risk.check("s/" + T2, req(ticker=T2), state(), NOW).detail == "s has 2 open orders (limit 2)"


def test_max_position_counts_same_side_working_orders_per_scope():
    risk, book = manager(Slot("s/" + T, risk={"max_position": "5"}), Slot("x/" + T),
                         global_limits={"max_position": "8"})
    book.pos[("s/" + T, T)] = 200
    book.rest("s/" + T, "bid", 200)
    book.rest("s/" + T, "ask", 300)  # the other side doesn't add
    assert risk.check("s/" + T, req(qty=100), state(), NOW) is None
    r = risk.check("s/" + T, req(qty=200), state(), NOW)
    assert r.rule == "max_position" and r.detail == f"s/{T} would hold 6.00 on {T} counting working orders (limit 5.00)"
    book.pos[("x/" + T, T)] = 400  # another strategy on the same market
    assert risk.check("s/" + T, req(qty=100), state(), NOW).detail.startswith("everything would hold 9.00")


def test_venue_limit():
    risk, book = manager(Slot("s/" + T), venue_limits={T: 1_000})
    book.pos[("s/" + T, T)] = 900
    assert risk.check("s/" + T, req(qty=200), state(), NOW).rule == "venue_limit"


def test_max_loss_halts_its_scope_and_resets_at_local_midnight():
    risk, book = manager(Slot("s/" + T, risk={"max_loss": "5.00"}), Slot("t/" + T))
    canceled: list = []
    risk.on_cancel.append(lambda scope, ticker, why: canceled.append(scope))
    st = state()
    book.pnls["s/" + T] = 1_000_000  # +$1 before today's first look
    risk.on_event(Timer(NOW), st, NOW)
    book.pnls["s/" + T] = -3_000_000  # down $4 today
    assert risk.check("s/" + T, req(), st, NOW) is None
    book.pnls["s/" + T] = -4_500_000  # down $5.50
    r = risk.check("s/" + T, req(), st, NOW)
    assert r.rule == "max_loss" and r.detail == f"s/{T} lost $5.50 today (limit $5.00)"
    assert canceled == ["s/" + T] and risk.check("s/" + T, req(), st, NOW).rule == "halted"
    assert risk.check("t/" + T, req(), st, NOW) is None  # another strategy carries on
    risk.resume("s/" + T, NOW)
    tomorrow = int(datetime(2026, 10, 19, 4, 1, tzinfo=UTC).timestamp() * 1000)  # 00:01 in New York
    risk.on_event(Timer(tomorrow), st, tomorrow)
    assert risk.check("s/" + T, req(), st, tomorrow) is None  # a new day's baseline


def test_loss_limits_are_watched_on_timers_too():
    risk, book = manager(Slot("s/" + T, risk={"max_loss": "5.00"}))
    st = state()
    risk.on_event(Timer(NOW), st, NOW)
    book.pnls["s/" + T] = -6_000_000
    events = risk.on_event(Timer(NOW + 1_000), st, NOW + 1_000)
    assert Halt("s/" + T, "max_loss: lost $6.00 today (limit $5.00)", True, NOW + 1_000) in events


class Portfolio:
    def __init__(self, **kw) -> None:
        self.now = ScopeStats("portfolio", "paper", 0, 0, 0, 0, kw.get("today", 0), 0, kw.get("drawdown", 0),
                              kw.get("car", 0), kw.get("worst", 0), None, None, 0, 0, 0)
        self.after = msgspec.structs.replace(self.now, capital_at_risk=kw.get("car_after", 0),
                                             worst_case=kw.get("worst_after", 0))
        self.entity = kw.get("entity_car", 0)

    def what_if(self, slot, req, state):
        return self.after

    def stats(self, scope, mode):
        return msgspec.structs.replace(self.now, scope=scope)

    def exposures(self, mode, strategy=None):
        return [Exposure(T, mode, [T], ["s"], self.entity, {}, 0, None, None)]


@pytest.mark.parametrize(("portfolio", "limits", "expected"), [
    (Portfolio(car_after=900_000_000), {"max_capital_at_risk": "1000.00"}, None),
    (Portfolio(car_after=1_100_000_000), {"max_capital_at_risk": "1000.00"}, "max_capital_at_risk"),
    (Portfolio(worst_after=-450_000_000), {"max_worst_case": "400.00"}, "max_worst_case"),
    (Portfolio(drawdown=300_000_000), {"max_drawdown": "300.00"}, "max_drawdown"),
    (Portfolio(entity_car=149_900_000), {"max_entity_risk": "150.00"}, "max_entity_risk"),  # + $0.42 for this order
])
def test_portfolio_limits_through_the_portfolio_tracker(portfolio, limits, expected):
    risk, _ = manager(Slot("s/" + T), global_limits=limits, portfolio=portfolio)
    assert rule(risk.check("s/" + T, req(), state(), NOW)) == expected


def test_portfolio_limits_without_a_tracker_warn_once():
    risk, _ = manager(Slot("s/" + T), global_limits={"max_drawdown": "300.00"})
    first = risk.on_event(Timer(NOW), state(), NOW)
    assert [e.level for e in first if type(e) is LogEvent] == ["warn"] and "not enforced" in first[0].text
    assert risk.on_event(Timer(NOW), state(), NOW) == []
    assert risk.check("s/" + T, req(), state(), NOW) is None


class Cash:
    def __init__(self, micros: int) -> None:
        self.micros = micros

    def available(self, mode, shard=None):
        return self.micros


def test_insufficient_balance_counts_collateral_for_each_side():
    risk, book = manager(Slot("s/" + T), balance=Cash(1_000_000))  # $1
    assert risk.check("s/" + T, req("bid", 4_000, 200), state(), NOW) is None  # 2 × $0.40
    r = risk.check("s/" + T, req("bid", 4_000, 300), state(), NOW)
    assert r.rule == "insufficient_balance" and r.detail == "needs $1.20, $1.00 available (paper)"
    assert risk.check("s/" + T, req("ask", 4_000, 200), state(), NOW).rule == "insufficient_balance"  # 2 × $0.60
    book.pos[("s/" + T, T)] = 500
    assert risk.check("s/" + T, req("ask", 4_000, 500), state(), NOW) is None  # selling what it holds


def test_risk_reducing_orders_pass_every_limit_but_halts_market_status_size_and_band():
    limits = {"stale_forecast_ms": 1, "max_position": "1", "max_loss": "1.00", "max_order_qty": "3",
              "price_band": "0.05"}
    slot = Slot("s/" + T, warming=True, model="m", risk=limits)
    risk, book = manager(slot, balance=Cash(0), global_limits={"max_open_orders": 0})
    book.pos[("s/" + T, T)] = -500  # short 5 (long NO)
    book.pnls["s/" + T] = -9_000_000
    st = state(book_ts=0)
    assert risk.check("s/" + T, req("bid", 4_200, 300), st, NOW) is None  # buys back 3
    assert risk.check("s/" + T, req("bid", 4_200, 400), st, NOW).rule == "max_order_qty"
    assert risk.check("s/" + T, req("bid", 5_000, 300), st, NOW).rule == "price_band"
    book.rest("s/" + T, "bid", 300)  # already buying back 3 of the 5
    assert risk.check("s/" + T, req("bid", 4_200, 300), st, NOW).rule == "warming"  # 3 more would flip it
    risk.halt("x", "s/" + T, NOW)
    assert risk.check("s/" + T, req("bid", 4_200, 100), st, NOW).rule == "halted"


def test_kill_switch_file_halts_everything_until_removed(tmp_path):
    flag = tmp_path / "HALT"
    risk, _ = manager(Slot("s/" + T), halt_file=flag)
    st = state()
    assert risk.on_event(Timer(NOW), st, NOW) == []
    flag.write_text("market looks broken\n")
    events = risk.on_event(Timer(NOW + 1_000), st, NOW + 1_000)
    assert Halt("", "kill switch: market looks broken", True, NOW + 1_000) in events
    assert risk.check("s/" + T, req(), st, NOW).detail == "everything halted: kill switch: market looks broken"
    flag.unlink()
    assert Halt("", "", False, NOW + 2_000) in risk.on_event(Timer(NOW + 2_000), st, NOW + 2_000)
    assert risk.check("s/" + T, req(), st, NOW) is None


def test_every_reject_names_its_rule_and_explains_itself():
    risk, _ = manager(Slot("s/" + T, risk={"max_order_qty": "1"}))
    r = risk.check("s/" + T, req(qty=200), state(), NOW)
    assert r == RiskReject("s/" + T, T, "bid", 4_200, 200, "max_order_qty", "2.00 > 1.00 contracts", NOW)
