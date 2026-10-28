"""The portfolio tracker against a hand-computed book, scenario netting, markouts on engine time, risk."""

from __future__ import annotations

import math

import pytest

from pmx import plugins
from pmx.core.types import BookSnapshot, Contract, Fill, OrderRequest, StreamReset
from pmx.engine.events import Forecast, Halt, OrderUpdate, RiskReject, SlotFill, Timer
from pmx.engine.state import State
from pmx.execution.risk import RiskManager
from pmx.ledger.db import Ledger
from pmx.portfolio.scenarios import OutcomeMap, probabilities
from pmx.portfolio.tracker import PortfolioTracker

plugins.load_builtin_plugins()

G1H, G1J = "KXNFLGAME-26OCT18HOUJAC-HOU", "KXNFLGAME-26OCT18HOUJAC-JAC"  # game 1: Houston at Jacksonville
G2B, G2M = "KXNFLGAME-26OCT18BUFMIA-BUF", "KXNFLGAME-26OCT18BUFMIA-MIA"  # game 2: Buffalo at Miami
G1, G2 = "nfl:2026-10-18:HOU-JAX", "nfl:2026-10-18:BUF-MIA"
T0 = 1_792_350_000_000  # 2026-10-18 ~ 11:40 New York


class Slot:
    def __init__(self, id: str, model: str | None = None, mode: str = "paper", risk_limits=None) -> None:
        self.id, self.model, self.mode, self.risk_limits = id, model, mode, risk_limits
        self.inputs, self.tickers, self.warming = (), {}, False


class Book:
    """State + tracker, fed by hand."""

    def __init__(self, ledger: Ledger | None = None) -> None:
        self.state = State(plugins.LINKERS)
        self.slots = {"a/" + G1H: Slot("a/" + G1H, "m"), "a/" + G1J: Slot("a/" + G1J, "m"),
                      "b/" + G2B: Slot("b/" + G2B)}
        self.t = PortfolioTracker(slots=self.slots, outcomes=OutcomeMap(self.state), ledger=ledger)
        if ledger is None:
            self.t.start(T0)  # the day begins flat
        self.now = T0
        self.n = 0
        for ticker in (G1H, G1J, G2B, G2M):
            self.send(Contract("kalshi", ticker, ticker.rsplit("-", 1)[0], "", "", "open", None))

    def send(self, ev, at: int | None = None) -> list:
        self.now = self.now if at is None else at
        self.state.apply(ev, self.now)
        return self.t.on_event(ev, self.state, self.now)

    def book(self, ticker: str, bid: int, ask: int) -> None:
        self.send(BookSnapshot("kalshi", ticker, ((bid, 1_000),), ((ask, 1_000),), self.now))

    def fill(self, slot: str, side: str, qty: int, price: int, fee: int) -> None:
        self.n += 1
        f = Fill("kalshi", f"f{self.n}", f"o{self.n}", slot.split("/", 1)[1], side, price, qty, fee, True, self.now,
                 f"c{self.n}")
        self.send(SlotFill(slot, "paper", f))

    def settle(self, ticker: str, price: int) -> None:
        self.send(Contract("kalshi", ticker, ticker.rsplit("-", 1)[0], "", "", "settled", None, settlement=price))


def _scenario_book(ledger: Ledger | None = None) -> Book:
    """| fill                       | qty | price | fee   |
    |----------------------------|-----|-------|-------|
    | a/HOU buys                 | 10  | 0.40  | 0.17  |
    | a/JAC buys (the other team)|  5  | 0.55  | 0.09  |
    | a/HOU sells (partial close)|  4  | 0.50  | 0.07  |  realized 4 × 0.10 = 0.40
    | b/BUF buys                 |  8  | 0.30  | 0.15  |
    Books: HOU 0.47/0.49 (mid 0.48), JAC 0.50/0.52 (0.51), BUF 0.34/0.36 (0.35), MIA 0.63/0.65 (0.64)."""
    b = Book(ledger)
    for t, bid, ask in ((G1H, 4_700, 4_900), (G1J, 5_000, 5_200), (G2B, 3_400, 3_600), (G2M, 6_300, 6_500)):
        b.book(t, bid, ask)
    b.fill("a/" + G1H, "bid", 1_000, 4_000, 170_000)
    b.fill("a/" + G1J, "bid", 500, 5_500, 90_000)
    b.fill("a/" + G1H, "ask", 400, 5_000, 70_000)
    b.fill("b/" + G2B, "bid", 800, 3_000, 150_000)
    return b


def _stats(s) -> tuple:
    return (s.realized, s.unrealized, s.fees, s.equity, s.capital_at_risk, s.worst_case, s.gross_exposure,
            s.working_exposure, s.open_positions, s.stale)


def test_every_scope_matches_the_hand_computed_book():
    b = _scenario_book()
    st = b.t.stats
    # a/HOU: 6 @ 0.40 left; unrealized 6 × 0.48 − 2.40 = 0.48; scenarios: HOU 0.16 + 6 − 2.40 = 3.76,
    # JAX 0.16 − 2.40 = −2.24, tie 0.16 + 3 − 2.40 = 0.76 (realized 0.40 − fees 0.24 = 0.16).
    assert _stats(st("slot:a/" + G1H, "paper")) == (400_000, 480_000, 240_000, 640_000, 2_880_000, -2_240_000,
                                                   2_880_000, 0, 1, False)
    # a: both teams of game 1 net. Base 0.40 − 0.33 = 0.07; HOU 0.07 + 3.60 − 2.75 = 0.92; JAX 0.07 − 2.40 + 2.25
    # = −0.08; tie 0.07 + 0.60 − 0.25 = 0.42 → worst −0.08 (alone they'd be −2.24 and −2.84).
    a = st("strategy:a", "paper")
    assert _stats(a) == (400_000, 280_000, 330_000, 350_000, 5_430_000, -80_000, 5_430_000, 0, 2, False)
    # b: 8 @ 0.30; unrealized 2.80 − 2.40 = 0.40; BUF −0.15 + 8 − 2.40 = 5.45, MIA −2.55, tie 1.45.
    assert _stats(st("strategy:b", "paper")) == (0, 400_000, 150_000, 250_000, 2_800_000, -2_550_000, 2_800_000,
                                                 0, 1, False)
    p = st("portfolio", "paper")
    assert _stats(p) == (400_000, 680_000, 480_000, 600_000, 8_230_000, -2_630_000, 8_230_000, 0, 3, False)
    # EV under a's model (P HOU 0.60, JAX 0.38, tie the rest, 0.02) = 0.552 − 0.0304 + 0.0084 = 0.53; game 2 has
    # no model, so it counts at the market's odds (0.35 / 0.64 / 0.01): 1.9075 − 1.632 + 0.0145 = 0.29.
    assert a.ev_model is None, "no forecasts yet"
    b.send(Forecast("m", G1H, 0.60, b.now, b.now))
    b.send(Forecast("m", G1J, 0.38, b.now, b.now))
    a, p = st("strategy:a", "paper"), st("portfolio", "paper")
    assert a.ev_model == pytest.approx(530_000, abs=1) and p.ev_model == pytest.approx(820_000, abs=1)
    var_g1 = 0.6 * 390_000 ** 2 + 0.38 * 610_000 ** 2 + 0.02 * 110_000 ** 2
    var_g2 = 0.35 * 5_160_000 ** 2 + 0.64 * 2_840_000 ** 2 + 0.01 * 1_160_000 ** 2  # around the market EV 0.29
    assert a.stdev_model == pytest.approx(math.sqrt(var_g1), abs=2)
    assert p.stdev_model == pytest.approx(math.sqrt(var_g1 + var_g2), abs=2)
    assert st("strategy:b", "paper").ev_model is None

    (e1, e2) = sorted(b.t.exposures("paper"), key=lambda e: e.entity)
    assert e2.entity == G1 and e2.scenarios == {"HOU": 920_000, "JAX": -80_000, "tie": 420_000}
    assert e2.tickers == [G1H, G1J] and e2.strategies == ["a"] and e2.capital_at_risk == 5_430_000
    assert e2.ev_market == pytest.approx(405_000, abs=1)  # 0.48 × 0.92 − 0.51 × 0.08 + 0.01 × 0.42
    assert e1.entity == G2 and e1.scenarios == {"BUF": 5_450_000, "MIA": -2_550_000, "tie": 1_450_000}
    assert e1.ev_model is None and e1.worst_case == -2_550_000
    assert [e.entity for e in b.t.exposures("paper")] == [G2, G1], "most at risk first"


def test_settlement_tie_drawdown_today_and_working_orders():
    b = _scenario_book()
    st = b.t.stats
    b.send(Timer(b.now))  # the day starts (equity 0 before any fill)
    assert st("portfolio", "paper").today == 600_000 and st("portfolio", "paper").drawdown == 0
    b.book(G1H, 3_900, 4_100)  # HOU's mid 0.48 → 0.40: −0.48 marked
    p = st("portfolio", "paper")
    assert (p.equity, p.hwm, p.drawdown, p.today) == (120_000, 600_000, 480_000, 120_000)
    b.send(OrderUpdate("b/" + G2B, "paper", "w1", "x", G2B, "bid", 3_300, 200, 0, "open", b.now))
    assert st("strategy:b", "paper").working_exposure == 700_000  # 2 more @ mid 0.35
    assert st("strategy:a", "paper").working_exposure == 0
    b.send(OrderUpdate("b/" + G2B, "paper", "w1", "x", G2B, "bid", 3_300, 200, 0, "canceled", b.now))
    assert st("portfolio", "paper").working_exposure == 0
    b.settle(G2B, 10_000)  # Buffalo wins: 8 − 2.40 realized
    b.settle(G2M, 0)
    s = st("strategy:b", "paper")
    assert (s.realized, s.unrealized, s.equity, s.open_positions, s.capital_at_risk) == (5_600_000, 0, 5_450_000, 0, 0)
    assert s.equity == 5_450_000, "the BUF scenario"
    b.settle(G1H, 5_000)  # a tie settles both teams at 0.50
    b.settle(G1J, 5_000)
    a = st("strategy:a", "paper")
    assert (a.realized, a.fees, a.equity, a.open_positions) == (750_000, 330_000, 420_000, 0), "the tie scenario"
    assert b.t.exposures("paper") == []
    b.send(Timer(T0 + 86_400_000), at=T0 + 86_400_000)  # the next local day: today restarts
    assert st("portfolio", "paper").today == 0 and st("portfolio", "paper").drawdown == 0


def test_settled_positions_leave_the_live_set_with_every_number_kept():
    b = _scenario_book()
    st = b.t.stats
    before = {sc: _stats(st(sc, "paper")) for sc in ("portfolio", "strategy:b", "slot:b/" + G2B)}
    b.settle(G2B, 10_000)
    assert not any(k[2] == G2B for k in b.t._pos), "settled: folded into totals"
    s = st("slot:b/" + G2B, "paper")
    assert (s.realized, s.fees, s.equity, s.open_positions) == (5_600_000, 150_000, 5_450_000, 0)
    assert st("strategy:b", "paper").equity == 5_450_000 and "strategy:b" in b.t._scopes("paper")
    assert before["slot:b/" + G2B] != _stats(s)
    p = st("portfolio", "paper")
    assert p.realized == 400_000 + 5_600_000 and p.fees == 170_000 + 90_000 + 70_000 + 150_000


def test_both_teams_long_nets_to_the_worst_outcome_not_the_sum():
    b = Book()
    for t, bid, ask in ((G1H, 4_400, 4_600), (G1J, 5_500, 5_700)):
        b.book(t, bid, ask)
    b.fill("a/" + G1H, "bid", 200, 4_500, 0)  # 2 HOU @ 0.45
    b.fill("a/" + G1J, "bid", 100, 5_600, 0)  # 1 JAC @ 0.56
    (e,) = b.t.exposures("paper")
    # HOU wins 2 × 0.55 − 0.56 = 0.54; JAX wins −0.90 + 0.44 = −0.46; tie 2 × 0.05 − 0.06 = 0.04.
    assert e.scenarios == {"HOU": 540_000, "JAX": -460_000, "tie": 40_000} and e.worst_case == -460_000
    alone = [b.t.stats("slot:a/" + t, "paper").worst_case for t in (G1H, G1J)]
    assert alone == [-900_000, -560_000] and b.t.stats("strategy:a", "paper").worst_case == -460_000 > sum(alone)


def test_a_market_without_domain_outcomes_is_its_own_yes_no_entity():
    b = Book()
    b.send(Contract("kalshi", "KXFED-26DEC-T4.00", "KXFED-26DEC", "", "", "open", None))
    b.book("KXFED-26DEC-T4.00", 2_000, 2_200)
    b.slots["c/KXFED-26DEC-T4.00"] = Slot("c/KXFED-26DEC-T4.00")
    b.fill("c/KXFED-26DEC-T4.00", "ask", 300, 2_000, 0)  # short 3 YES = long 3 NO @ 0.80
    (e,) = b.t.exposures("paper")
    assert e.entity == "KXFED-26DEC-T4.00" and e.scenarios == {"yes": -2_400_000, "no": 600_000}
    assert b.t.stats("portfolio", "paper").capital_at_risk == 2_370_000  # 3 × (1 − mid 0.21)
    assert probabilities({"yes": {"T": "yes"}, "no": {"T": "no"}}, lambda t: 0.3) == pytest.approx(
        {"yes": 0.3, "no": 0.7})


def test_stale_marks_use_the_last_mid_and_say_so():
    b = _scenario_book()
    b.t.stats("portfolio", "paper")
    b.send(StreamReset("kalshi", "test", b.now))  # every book dropped until re-snapshotted
    p = b.t.stats("portfolio", "paper")
    assert p.stale and p.unrealized == 680_000, "marked at the last mids seen"


def test_markouts_land_exactly_when_engine_time_passes():
    led = Ledger(":memory:", env="demo")
    b = _scenario_book(led)
    t_fill = b.now
    rows = {r["fill_id"]: r for r in led.fill_meta("paper")}
    assert set(rows) == {"f1", "f2", "f3", "f4"}
    r = rows["f1"]
    assert (r["slot"], r["entity"], r["model"], r["mid"], r["spread"]) == ("a/" + G1H, G1, "m", 4_800, 200)
    b.book(G1H, 5_100, 5_300)  # mid 0.52
    b.send(Timer(t_fill + 9_999), at=t_fill + 9_999)
    assert led.fill_meta("paper")[0]["markout_10s"] is None
    b.send(Timer(t_fill + 10_000), at=t_fill + 10_000)
    m = {r["fill_id"]: r for r in led.fill_meta("paper")}
    assert m["f1"]["markout_10s"] == 1_200_000, "bought 10 @ 0.40, mid 0.52 → +1.20"
    assert m["f3"]["markout_10s"] == -80_000, "sold 4 @ 0.50, mid 0.52 → −0.08"
    assert m["f1"]["markout_1m"] is None
    b.send(Timer(t_fill + 60_000), at=t_fill + 60_000)
    b.send(Timer(t_fill + 300_000), at=t_fill + 300_000)
    m = {r["fill_id"]: r for r in led.fill_meta("paper")}
    assert m["f1"]["markout_1m"] == m["f1"]["markout_5m"] == 1_200_000
    assert led.equity_series("paper", "portfolio"), "equity snapshots on fills and every 10 s"


def test_restart_restores_positions_and_todays_opening_equity(tmp_path):
    path = tmp_path / "ledger.sqlite"
    led = Ledger(path, env="demo")
    from pmx.core.types import OrderRequest as Req

    for i, (slot, side, qty, price, fee) in enumerate((("a/" + G1H, "bid", 1_000, 4_000, 170_000),
                                                        ("b/" + G2B, "bid", 800, 3_000, 150_000)), start=1):
        led.record_order(Req("kalshi", slot.split("/", 1)[1], side, price, qty, client_order_id=f"c{i}",
                             strategy=slot), "paper", T0)
        led.record_fill(Fill("kalshi", f"f{i}", f"o{i}", slot.split("/", 1)[1], side, price, qty, fee, True, T0,
                             f"c{i}"), "paper")
    led.record_equity([(T0 - 86_400_000, "paper", "portfolio", -1_000_000, 0, 0, 0, 0),  # yesterday's close
                       (T0 - 60_000, "paper", "portfolio", 900_000, 0, 0, 0, 0)])        # earlier today
    b = Book(led)
    curves = b.t.start(T0)
    assert ("portfolio", "paper") in {(s, m) for s, m, _ in curves}
    assert b.t.position("a/" + G1H) == 1_000 and b.t.position("b/" + G2B) == 800
    b.book(G1H, 4_000, 4_000)
    b.book(G2B, 3_000, 3_000)
    p = b.t.stats("portfolio", "paper")
    assert p.equity == -320_000 and p.today == 680_000 and p.hwm == 900_000 and p.drawdown == 1_220_000


def test_snapshot_reports_scopes_exposures_and_limit_usage():
    b = _scenario_book()
    risk = RiskManager(positions=_NoPositions(), slots=b.slots,
                       global_limits={"max_capital_at_risk": "10.00", "max_worst_case": "2.00", "max_loss": "5.00"},
                       strategy_limits={"a": {"max_position": "5"}}, portfolio=b.t)
    b.t.risk = risk
    (snap,) = [e for e in b.send(Timer(b.now)) if type(e).__name__ == "PortfolioSnapshot"]
    assert [s.scope for s in snap.scopes][:3] == ["portfolio", "strategy:a", "strategy:b"]
    assert {s.scope for s in snap.scopes} >= {"slot:a/" + G1H, "slot:b/" + G2B}
    usage = {(u.scope, u.limit): (u.used, u.cap, u.breached) for u in snap.limits}
    assert usage[("portfolio", "max_capital_at_risk")] == (8_230_000, 10_000_000, False)
    assert usage[("portfolio", "max_worst_case")] == (2_630_000, 2_000_000, True)
    assert usage[("strategy:a", "max_position")] == (600, 500, True)
    assert [type(e).__name__ for e in b.send(Timer(b.now + 500), at=b.now + 500)] == [], "at most one a second"


class _NoPositions:
    def position(self, scope, ticker, mode=None):
        return 0

    def working(self, scope, ticker=None, mode=None):
        return []

    def pnl(self, scope, mode, state):
        return 0


def test_risk_rejects_an_order_over_entity_risk_and_a_drawdown_halts_only_its_scope():
    b = _scenario_book()
    risk = RiskManager(positions=_NoPositions(), slots=b.slots, global_limits={"max_entity_risk": "6.00"},
                       strategy_limits={"b": {"max_drawdown": "0.30"}}, portfolio=b.t)
    b.t.risk = risk
    req = OrderRequest("kalshi", G1H, "bid", 4_900, 200, strategy="a/" + G1H)
    r = risk.check("a/" + G1H, req, b.state, b.now)
    assert isinstance(r, RiskReject) and r.rule == "max_entity_risk"
    assert r.detail == f"{G1} would have $6.41 at risk (limit $6.00)"  # 5.43 + 2 × 0.49
    assert risk.check("a/" + G1H, OrderRequest("kalshi", G1H, "bid", 4_900, 100, strategy="a/" + G1H),
                      b.state, b.now) is None
    b.send(Timer(b.now))
    risk.on_event(Timer(b.now), b.state, b.now)
    b.book(G2B, 3_000, 3_000)  # BUF mid 0.35 → 0.30: b loses 0.40 from its high
    b.send(Timer(b.now + 1_000), at=b.now + 1_000)
    out = risk.on_event(Timer(b.now), b.state, b.now)
    assert Halt("b", "max_drawdown: $0.40 below today's high (limit $0.30)", True, b.now) in out
    assert set(risk.halts) == {"b"}, "only its scope"


def test_pmx_risk_prints_scopes_exposures_and_breached_limits():
    from pmx.cli.risk import report
    from pmx.feed import proto

    b = _scenario_book()
    b.t.risk = RiskManager(positions=_NoPositions(), slots=b.slots, global_limits={"max_worst_case": "2.00"},
                           portfolio=b.t)
    (snap,) = b.send(Timer(b.now))
    text = report(proto.Hello("kalshi", "prod", "live"), proto.PortfolioMsg.of(snap), "paper")
    assert "pmx risk — prod paper" in text
    assert "portfolio" in text and "+$0.60" in text and "$8.23" in text and "-$2.63" in text
    assert f"{G2:<40}" in text and "BUF +$5.45 · MIA -$2.55 · tie +$1.45" in text
    assert "max_worst_case" in text and "$2.63 / $2.00" in text and "BREACHED" in text
