from __future__ import annotations

from pmx.core.types import BookDelta, BookSnapshot, Contract, OrderRequest, Trade
from pmx.engine.events import OrderUpdate, SlotFill
from pmx.engine.state import State
from pmx.execution.paper import PaperBroker

T = "SER-EV1-A"


def market(bids=((4_000, 1_000),), asks=((4_200, 1_000),), status="open") -> State:
    s = State()
    s.apply(Contract("kalshi", T, "SER-EV1", "Title", "Sub", status, None), 0)
    s.apply(BookSnapshot("kalshi", T, tuple(bids), tuple(asks), 0), 0)
    return s


def order(side="bid", price=4_200, qty=100, tif="gtc", post_only=False, coid="c1") -> OrderRequest:
    return OrderRequest("kalshi", T, side, price, qty, tif, post_only, coid, "s/" + T)


def fills(events) -> list[tuple]:
    return [(e.fill.price, e.fill.qty, e.fill.fee, e.fill.is_taker) for e in events if type(e) is SlotFill]


def updates(events) -> list[tuple]:
    return [(e.status, e.filled, e.reason) for e in events if type(e) is OrderUpdate]


def test_a_take_walks_visible_depth_and_pays_the_taker_fee():
    st = market(asks=((4_000, 500), (4_100, 500), (4_300, 1_000)))
    paper = PaperBroker()
    out = paper.place_now(order("bid", 4_200, 1_200, "ioc"), "s/" + T, st, 7)
    # 7% × p × (1 − p) × contracts, rounded up to the micro-dollar: 5 @ 40¢ → $0.084, 5 @ 41¢ → $0.084665 → 0.084665
    assert fills(out) == [(4_000, 500, 84_000, True), (4_100, 500, 84_665, True)]
    assert updates(out) == [("canceled", 1_000, "IOC remainder")]
    assert out[0].fill.client_order_id == "c1" and out[0].slot == "s/" + T and out[0].fill.ts_ms == 7


def test_depth_a_take_consumed_stays_consumed_until_that_level_updates():
    st = market(asks=((4_000, 500),))
    paper = PaperBroker()
    paper.place_now(order("bid", 4_000, 300, "ioc"), "s/" + T, st, 0)
    assert fills(paper.place_now(order("bid", 4_000, 500, "ioc", coid="c2"), "s/" + T, st, 0)) == [
        (4_000, 200, 33_600, True)]  # only the 2 left
    assert fills(paper.place_now(order("bid", 4_000, 500, "ioc", coid="c3"), "s/" + T, st, 0)) == []
    delta = BookDelta("kalshi", T, "ask", 4_000, -100, 1)  # the venue's own update for that level
    st.apply(delta, 1)
    paper.on_event(delta, st, 1)
    assert fills(paper.place_now(order("bid", 4_000, 500, "ioc", coid="c4"), "s/" + T, st, 1)) == [
        (4_000, 400, 67_200, True)]


def test_a_resting_order_waits_out_the_queue_ahead_of_it():
    st = market(bids=((4_000, 1_000),))
    paper = PaperBroker()
    assert updates(paper.place_now(order("bid", 4_000, 300, post_only=True), "s/" + T, st, 0)) == [("open", 0, "")]
    assert paper.queue_ahead("c1") == 1_000
    first = paper.on_event(Trade("kalshi", "t1", T, 4_000, 600, "ask", 1), st, 1)
    assert first == [] and paper.queue_ahead("c1") == 400
    out = paper.on_event(Trade("kalshi", "t2", T, 4_000, 600, "ask", 2), st, 2)
    # 4 more ahead of us, then 2 ours; maker fee 1.75% × 0.4 × 0.6 × 2 = $0.0084
    assert fills(out) == [(4_000, 200, 8_400, False)] and updates(out) == [("open", 200, "")]
    assert paper.on_event(Trade("kalshi", "t3", T, 4_000, 100, "bid", 3), st, 3) == []  # a buyer lifting asks


def test_a_print_through_our_price_fills_us_up_to_its_size():
    st = market(bids=((4_000, 1_000),))
    paper = PaperBroker()
    paper.place_now(order("bid", 4_000, 500, post_only=True), "s/" + T, st, 0)
    out = paper.on_event(Trade("kalshi", "t1", T, 3_900, 300, "ask", 1), st, 1)
    assert fills(out) == [(4_000, 300, 12_600, False)]  # at our price, maker fee


def test_the_opposite_side_reaching_our_price_fills_us_in_full():
    st = market()
    paper = PaperBroker()
    paper.place_now(order("ask", 4_100, 300, post_only=True), "s/" + T, st, 0)
    up = BookDelta("kalshi", T, "bid", 4_100, 500, 1)
    st.apply(up, 1)
    out = paper.on_event(up, st, 1)
    assert fills(out) == [(4_100, 300, 12_700, False)] and updates(out) == [("filled", 300, "")]
    assert paper.resting == {}


def test_post_only_crossing_is_rejected_a_plain_limit_takes_then_rests():
    st = market(asks=((4_200, 200),))
    paper = PaperBroker()
    assert updates(paper.place_now(order("bid", 4_200, 100, post_only=True), "s/" + T, st, 0)) == [
        ("rejected", 0, "post-only order would cross")]
    out = paper.place_now(order("bid", 4_200, 500, coid="c2"), "s/" + T, st, 0)
    assert fills(out) == [(4_200, 200, 34_104, True)] and updates(out) == [("open", 200, "")]
    assert paper.resting["c2"].remaining == 300
    assert paper.on_event(BookDelta("kalshi", T, "bid", 4_000, 100, 1), st, 1) == []  # its own take doesn't refill it


def test_cancels_and_market_status():
    st = market()
    paper = PaperBroker()
    paper.place_now(order("bid", 3_900, 100, post_only=True), "s/" + T, st, 0)
    assert updates(paper.cancel_now("c1", 1)) == [("canceled", 0, "")]
    assert paper.cancel_now("c1", 2) == []  # already gone
    paper.place_now(order("bid", 3_900, 100, post_only=True, coid="c2"), "s/" + T, st, 0)
    paused = Contract("kalshi", T, "SER-EV1", "Title", "Sub", "paused", None)
    st.apply(paused, 3)
    assert updates(paper.on_event(paused, st, 3)) == [("canceled", 0, "")]
    assert updates(paper.place_now(order(coid="c3"), "s/" + T, st, 4)) == [("rejected", 0, "market paused")]
