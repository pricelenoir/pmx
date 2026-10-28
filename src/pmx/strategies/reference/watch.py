"""`watch`: never trades. Logs a signal line when the strategy's model and the book's mid disagree by
at least `threshold`, again if the side flips, and when the gap closes."""

from __future__ import annotations

import msgspec

from pmx.core.book import OrderBook
from pmx.core.units import ONE_DOLLAR
from pmx.engine.events import Forecast
from pmx.plugins import strategy
from pmx.strategies.base import MarketContext, Strategy


@strategy("watch")
class Watch(Strategy):
    class Params(msgspec.Struct, frozen=True):
        threshold: float = 0.05  # probability points

    def __init__(self, params: Params) -> None:
        super().__init__(params)
        self.side = 0  # +1 model above mid, -1 below, 0 within threshold

    def on_forecast(self, f: Forecast, m: MarketContext) -> None:
        self._check(m)

    def on_book(self, book: OrderBook, m: MarketContext) -> None:
        self._check(m)

    def _check(self, m: MarketContext) -> None:
        f, book = m.forecast(), m.book
        mid = None if book is None else book.mid()
        if f is None or mid is None:
            return
        edge = f.p - mid / ONE_DOLLAR
        side = 0 if abs(edge) < self.p.threshold else 1 if edge > 0 else -1
        if side == self.side:
            return
        self.side = side
        what = "signal off" if not side else f"signal {'buy' if side > 0 else 'sell'}"
        m.log(f"{what}: {f.model} {f.p:.3f} vs mid {mid / ONE_DOLLAR:.3f} ({edge:+.3f})")
