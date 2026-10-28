"""Target positions worked by the execution helpers, driven through the scenario kit."""

from __future__ import annotations

from pmx.testing import CancelAll, GroupScenario, Quote, Scenario, Take, Target

PULL = Quote(bid=None, ask=None)


def _sc(**kw) -> Scenario:
    sc = Scenario("noop", **kw)
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    return sc


def _target(sc: Scenario, qty: str, style: str = "passive", **opts) -> list:
    """Set a target and let the next event carry it (what a strategy does inside a hook)."""
    sc.m.target(round(float(qty) * 100), style=style, **opts)
    sc.delta("bid", "0.30", "1")  # an event far from the top: nothing else changes
    return sc.intents


def test_passive_joins_the_best_bid_and_reaches_its_target_through_partial_fills():
    sc = _sc()
    assert _target(sc, "5") == [Target("5", "passive"), Quote(bid=("0.40", "5"), ask=None)]
    sc.fill("bid", "0.40", "2")
    assert sc.intents == [], "the remaining 3 still rest: no requote"
    assert sc.slot.last_action == "target +5, have +2, resting 3 @ 40¢"
    sc.delta("ask", "0.47", "50").trade("0.44", "1", "bid").advance(3)
    assert sc.intents == [], "no churn while the best bid doesn't move"
    sc.delta("bid", "0.41", "7")  # someone bids higher: follow
    assert sc.intents == [Quote(bid=("0.41", "3"), ask=None)]
    sc.fill("bid", "0.41", "3")
    assert sc.intents == [] and sc.position == 500 and sc.slot.last_action == "target +5, have +5: reached"


def test_improve_steps_inside_the_spread_but_never_crosses():
    sc = _sc()
    assert _target(sc, "5", "improve")[1:] == [Quote(bid=("0.41", "5"), ask=None)]
    sc.delta("bid", "0.42", "3")
    assert sc.intents == [Quote(bid=("0.43", "5"), ask=None)]
    sc.delta("bid", "0.43", "3")  # one-tick spread: nothing inside it, so join the best
    assert sc.intents == [] and sc.working[0].price == 4_300
    sc2 = _sc()
    assert _target(sc2, "-2", "improve", improve_ticks=3)[1:] == [Quote(bid=None, ask=("0.41", "2"))]


def test_live_mode_does_not_chase_its_own_order_in_the_book():
    sc = Scenario("noop", mode="live")
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert _target(sc, "5", "improve")[1:] == [Quote(bid=("0.41", "5"), ask=None)]
    sc.book(bids=[("0.41", "5"), ("0.40", "10")], asks=[("0.44", "10")])  # the venue now shows our bid
    assert sc.intents == [], "others' best is still 0.40"


def test_cross_after_takes_the_remainder_within_max_cross():
    sc = _sc()
    assert _target(sc, "5", cross_after_s=10)[1:] == [Quote(bid=("0.40", "5"), ask=None)]
    sc.fill("bid", "0.40", "1")
    sc.advance(9)
    assert sc.intents == []
    sc.advance(1)
    assert sc.intents == [PULL, Take("bid", "0.44", "4")]
    assert sc.slot.last_action == "target +5, have +1: taking 4 ≤ 44¢"
    sc.advance(0.5)
    assert sc.intents == [], "at most one take a second"

    bounded = _sc()  # mid 0.42: 1¢ worse is 0.43 all in, but the best ask is 0.44
    _target(bounded, "5", cross_after_s=0, max_cross_c=1)
    bounded.advance(5)
    assert bounded.intents == [] and bounded.working[0].price == 4_000, "nothing within 1¢: keep resting"
    wide = _sc()  # 4¢: 0.45 + its 1.74¢ fee is past 0.46; 0.44 + 1.73¢ isn't
    assert _target(wide, "5", cross_after_s=0, max_cross_c=4)[1:] == [Take("bid", "0.44", "5")]
    sell = _sc()  # selling: 0.40 − 1.68¢ fee ≥ 0.42 − 4¢
    assert _target(sell, "-3", cross_after_s=0, max_cross_c=4)[1:] == [Take("ask", "0.40", "3")]


def test_a_new_target_replaces_the_old_and_zero_flattens():
    sc = _sc()
    _target(sc, "5")
    sc.fill("bid", "0.40", "2")
    assert _target(sc, "-1") == [Target("-1", "passive"), Quote(bid=None, ask=("0.44", "3"))]
    sc.fill("ask", "0.44", "3")
    assert sc.position == -100 and sc.intents == [] and sc.working == []
    sc.fill("bid", "0.40", "1")  # filled elsewhere back to flat: the target works its way to -1 again
    assert sc.intents == [Quote(bid=None, ask=("0.44", "1"))]
    assert _target(sc, "0") == [Target("0", "passive"), PULL]
    assert sc.slot.last_action == "target +0, have +0: reached"


def test_it_waits_when_throttled_without_a_book_or_in_a_window_and_expires():
    sc = _sc()
    sc.throttle()
    assert _target(sc, "5") == [Target("5", "passive")]
    assert sc.slot.last_action == "target +5, have +0: waiting (throttled)"
    sc.throttle(False).delta("bid", "0.30", "1")
    assert sc.intents == [Quote(bid=("0.40", "5"), ask=None)]
    sc.reset()  # the book goes stale: pull
    assert sc.intents == [PULL] and sc.slot.last_action.endswith("waiting (no book)")
    sc.book(bids=[("0.40", "10")], asks=[("0.44", "10")])
    assert sc.intents == [Quote(bid=("0.40", "5"), ask=None)]

    w = _sc(risk={"windows": [{"kind": "release", "before_s": 60, "after_s": 10}]})
    w.schedule("release", in_s=30)
    assert _target(w, "5") == [Target("5", "passive")]
    assert "waiting (release at" in w.slot.last_action
    w.advance(41)
    assert w.intents == [Quote(bid=("0.40", "5"), ask=None)], "the window closed"

    e = _sc()
    _target(e, "5", expires_s=5)
    e.advance(5)
    assert e.intents == [PULL] and e.slot.last_action == "target +5, have +0: expired"
    e.delta("bid", "0.41", "1")
    assert e.intents == [], "an expired target is gone"


def test_a_risk_reject_backs_off_instead_of_resending_every_event():
    sc = _sc().with_risk({"max_order_qty": "1"})
    _target(sc, "5")
    assert [r.rule for r in sc.rejects] == ["max_order_qty"]
    assert sc.slot.last_action == "target +5: rejected (max_order_qty), retrying in 5 s"
    sc.delta("bid", "0.30", "2").delta("bid", "0.30", "3").advance(4)
    assert sc.intents == [] and sc.rejects == []
    sc.advance(1)
    assert sc.intents == [Quote(bid=("0.40", "5"), ask=None)] and len(sc.rejects) == 1


def test_bad_options_fail_the_slot():
    sc = _sc()
    sc.m.target(500, style="aggressive")
    sc.delta("bid", "0.30", "1")
    assert "isn't one of passive, improve" in sc.slot.error and sc.intents[-1] == CancelAll()
    sc2 = _sc()
    sc2.m.target(500, max_cross_c=2)
    sc2.delta("bid", "0.30", "1")
    assert "needs cross_after_s" in sc2.slot.error


def test_group_slots_target_each_market():
    g = GroupScenario("noop", tickers=["EV-A", "EV-B"])
    g.book(bids=[("0.40", "10")], asks=[("0.44", "10")], ticker="EV-A")
    g.book(bids=[("0.55", "10")], asks=[("0.58", "10")], ticker="EV-B")
    g.m.target("EV-A", 200)
    g.m.target("EV-B", -100, style="improve")
    g.delta("bid", "0.30", "1", ticker="EV-A")
    assert g.intents == [Target("2", "passive", ticker="EV-A"), Target("-1", "improve", ticker="EV-B"),
                         Quote(bid=("0.40", "2"), ask=None, ticker="EV-A"),
                         Quote(bid=None, ask=("0.57", "1"), ticker="EV-B")]
