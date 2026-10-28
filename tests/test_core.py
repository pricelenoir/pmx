from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pmx.core.book import BookError, BookTracker, OrderBook
from pmx.core.fees import KALSHI_DEFAULT_FEES, FeeSchedule
from pmx.core.types import CENT_GRID, BookDelta, BookSnapshot, PriceBand, PriceGrid, StreamReset
from pmx.core.units import (
    cents,
    complement,
    fmt_micros,
    fmt_price,
    fmt_qty,
    parse_micros,
    parse_price,
    parse_qty,
)


def test_parse_fixed_point():
    assert parse_price("0.5600") == 5600
    assert parse_price("0.56") == 5600
    assert parse_price("1") == 10_000
    assert parse_price("0.560000") == 5600  # responses may emit 6 dp
    assert parse_qty("10211.65") == 1_021_165
    assert parse_qty("-54.00") == -5400
    assert parse_micros("0.003639") == 3639
    with pytest.raises(ValueError):
        parse_price("0.56001")
    with pytest.raises(ValueError):
        parse_qty("abc")


@given(st.integers(-10**12, 10**12))
def test_fixed_point_round_trips(v):
    assert parse_price(fmt_price(v)) == v
    assert parse_qty(fmt_qty(v)) == v
    assert parse_micros(fmt_micros(v)) == v


def test_display_and_complement():
    assert cents(5600) == "56¢"
    assert cents(5650) == "56.5¢"
    assert cents(25) == "0.25¢"
    assert complement(7500) == 2500


def test_kalshi_fee_matches_formula():
    # 0.07 * 1 contract * 0.55 * 0.45 = $0.017325
    assert KALSHI_DEFAULT_FEES.fee(5500, 100, taker=True) == 17_325
    # rounds up to the micro-dollar
    assert FeeSchedule(700, 0).fee(5501, 1, taker=True) == 174
    assert FeeSchedule(700, 0).fee(5500, 100, taker=False) == 0


def test_price_grid_tapered():
    grid = PriceGrid((PriceBand(0, 1000, 10), PriceBand(1000, 9000, 100), PriceBand(9000, 10_000, 10)))
    assert grid.is_valid(50) and grid.is_valid(5600) and grid.is_valid(9990)
    assert not grid.is_valid(5650) and not grid.is_valid(0) and not grid.is_valid(10_000)
    assert grid.snap(5650) == 5600 and grid.snap(5650, up=True) == 5700
    assert grid.tick_above(1000) == 1100 and grid.tick_below(1000) == 990
    assert CENT_GRID.snap(0) == 100 and CENT_GRID.snap(10_000) == 9900


def test_order_book_levels_and_errors():
    b = OrderBook("T")
    b.replace([(2200, 333), (800, 300)], [(4400, 146), (4600, 20)])
    assert b.best_bid() == (2200, 333) and b.best_ask() == (4400, 146)
    assert b.spread() == 2200 and b.mid() == 3300
    b.add("bid", 2200, -333)
    assert b.best_bid() == (800, 300)
    assert b.levels("ask") == [(4400, 146), (4600, 20)]
    assert b.depth_to("ask", 4500) == 146
    with pytest.raises(BookError):
        b.add("ask", 4400, -1000)
    b.add("bid", 5000, 10)
    assert b.crossed()


def test_tracker_drops_books_on_reset_until_next_snapshot():
    t = BookTracker()
    snap = BookSnapshot("kalshi", "T", ((5000, 100),), ((5200, 100),), 1)
    assert t.on_event(snap) is not None
    assert t.on_event(BookDelta("kalshi", "T", "bid", 5100, 50, 2)).best_bid() == (5100, 50)
    t.on_event(StreamReset("kalshi", "gap", 3))
    assert t.get("kalshi", "T") is None
    assert t.on_event(BookDelta("kalshi", "T", "bid", 5100, 50, 4)) is None
    assert t.on_event(snap) is not None
    # an impossible delta invalidates just that book
    assert t.on_event(BookDelta("kalshi", "T", "ask", 5200, -500, 5)) is None
    assert t.get("kalshi", "T") is None
