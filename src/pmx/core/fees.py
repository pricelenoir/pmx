"""Fee models. Kalshi charges `rate * C * P * (1 - P)` per fill, rounded up to $0.000001;
balance-precision rounding on top of that is settled by the venue and shows up in
each fill's reported fee.
"""

from __future__ import annotations

import msgspec

from pmx.core.units import ONE_DOLLAR, Micros, Price, Qty


class FeeSchedule(msgspec.Struct, frozen=True):
    taker_e4: int  # rate in 1e-4 (700 == 7%)
    maker_e4: int

    def fee(self, price: Price, qty: Qty, *, taker: bool) -> Micros:
        rate = self.taker_e4 if taker else self.maker_e4
        num = rate * qty * price * (ONE_DOLLAR - price)
        return -(-num // 100_000_000)


# Per-series schedules vary (GET /series fee fields); maker is set conservatively.
KALSHI_DEFAULT_FEES = FeeSchedule(taker_e4=700, maker_e4=175)
