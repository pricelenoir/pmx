"""Builds the analytics fixture ledgers from hand-written trades with known answers (`python build.py OUT.sqlite`
writes one; the tests call `build(path)`).

Strategies (all paper, env "prod", no fees unless said, entity = the game):
- `win3`: three games, one episode each: buy 10 @ 40¢ (fair 0.50, mid 0.42), settles YES → +$6.00 each.
- `luck`: twelve games, buy 10 @ 50¢ (fair 0.53), +1 m markout −2¢/contract; 11 settle YES (+$5), one NO (−$5).
- `loser`: twelve games, buy 1 @ 60¢ then sell 1 @ 55¢ (taker fee 1¢ each) → −7¢ each.
- `flip`: one market: buy 10 @ 40¢, sell 15 @ 50¢ (flat, then short 5), buy 5 @ 45¢ (flat): +$1.00 and +$0.25.
- `edges`: four games: predicted edge 3¢ then 8¢ — the 8¢ ones lose (not monotone); one game carries the PnL.
- equity rows for `strategy:win3`: 0 → +$6 → +$2 (drawdown $4, 2 h under water) → +$18 the next day."""

from __future__ import annotations

import sys
from pathlib import Path

from pmx.core.types import Fill, OrderRequest
from pmx.ledger.db import Ledger

DAY = 1_792_300_000_000  # 2026-10-18 ~01:06 New York
H = 3_600_000
ENV = "prod"


class _Book:
    def __init__(self, led: Ledger) -> None:
        self.led, self.n = led, 0

    def trade(self, slot: str, ticker: str, side: str, price: int, qty: int, ts: int, *, fee: int = 0,
              taker: bool = True, entity: str | None = None, fair: float | None = None, mid: float | None = None,
              phase: str | None = None, markout_1m: int | None = None, tif: str = "ioc") -> str:
        self.n += 1
        coid = f"pmx-fx-{self.n:x}"
        self.led.record_order(OrderRequest("kalshi", ticker, side, price, qty, tif, False, coid, slot), "paper", ts)
        self.led.set_order_status(coid, "filled", filled=qty, order_id=f"V{self.n}", now_ms=ts)
        fid = f"F{self.n}"
        self.led.record_fill(Fill("kalshi", fid, f"V{self.n}", ticker, side, price, qty, fee, taker, ts, coid),
                             "paper")
        self.led.record_fill_meta("kalshi", "paper", fid, ts, slot, entity=entity, model="m" if fair else None,
                                  model_version=None, fair=fair, mid=mid, spread=None, phase=phase)
        if markout_1m is not None:
            self.led.set_markout("kalshi", "paper", fid, "1m", markout_1m)
        return coid


def build(path: Path) -> Path:
    path.unlink(missing_ok=True)
    led = Ledger(path, env=ENV)
    b = _Book(led)
    for g in range(3):
        t = f"SER-WIN{g}-A"
        b.trade(f"win3/{t}", t, "bid", 4_000, 1_000, DAY + g * H, entity=f"game:w{g}", fair=0.50, mid=4_200,
                phase="Q1")
        led.record_settlement("kalshi", t, "yes", "paper", DAY + g * H + H // 2)
    for g in range(12):
        t = f"SER-LUCK{g}-A"
        b.trade(f"luck/{t}", t, "bid", 5_000, 1_000, DAY + g * 60_000, entity=f"game:l{g}", fair=0.53,
                markout_1m=-1_000 * 200)  # −2¢ × 10 contracts, in micros
        led.record_settlement("kalshi", t, "yes" if g else "no", "paper", DAY + H)
    for g in range(12):
        t = f"SER-LOSE{g}-A"
        b.trade(f"loser/{t}", t, "bid", 6_000, 100, DAY + g * 60_000, fee=10_000, entity=f"game:x{g}")
        b.trade(f"loser/{t}", t, "ask", 5_500, 100, DAY + g * 60_000 + 30_000, fee=10_000, entity=f"game:x{g}")
    t = "SER-FLIP-A"
    b.trade(f"flip/{t}", t, "bid", 4_000, 1_000, DAY, entity="game:f")
    b.trade(f"flip/{t}", t, "ask", 5_000, 1_500, DAY + 60_000, entity="game:f")
    b.trade(f"flip/{t}", t, "bid", 4_500, 500, DAY + 120_000, entity="game:f")
    for g, (edge_fair, settle) in enumerate([(0.43, "yes"), (0.43, "yes"), (0.48, "no"), (0.48, "no")]):
        t = f"SER-EDGE{g}-A"
        qty = 4_000 if g == 0 else 100
        b.trade(f"edges/{t}", t, "bid", 4_000, qty, DAY + g * 60_000, entity=f"game:e{g}", fair=edge_fair)
        led.record_settlement("kalshi", t, settle, "paper", DAY + H)
    led.record_equity([
        (DAY, "paper", "strategy:win3", 0, 0, 0, 0, 0),
        (DAY + H, "paper", "strategy:win3", 6_000_000, 0, 6_000_000, 0, 4_000_000),
        (DAY + 2 * H, "paper", "strategy:win3", 2_000_000, 0, 2_000_000, 0, 4_000_000),
        (DAY + 4 * H, "paper", "strategy:win3", 6_000_000, 6_000_000, 0, 0, 0),
        (DAY + 24 * H, "paper", "strategy:win3", 18_000_000, 18_000_000, 0, 0, 0),
    ])
    led.close()
    return path


if __name__ == "__main__":
    print(build(Path(sys.argv[1] if len(sys.argv) > 1 else "perf.sqlite")))
