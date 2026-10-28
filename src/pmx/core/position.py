"""Average-cost accounting for one position (a slot's, a strategy's, …) in one market: YES terms, integer
units. Shared by the order manager, the router's tracker, and the ledger's per-slot replay, so every
number about a position comes from the same arithmetic (ARCHITECTURE *Portfolio*)."""

from __future__ import annotations

from pmx.core.units import Micros, Price, Qty


class AvgCost:
    __slots__ = ("mode", "qty", "cost", "realized", "fees")

    def __init__(self, mode: str = "paper") -> None:
        self.mode = mode
        self.qty: Qty = 0  # signed YES
        self.cost: Micros = 0  # signed micros paid for the open qty
        self.realized: Micros = 0
        self.fees: Micros = 0

    def fill(self, qty: Qty, price: Price, fee: Micros) -> None:
        """`qty` signed: + bought YES, − sold YES. Reducing realizes against the average cost."""
        self.fees += fee
        q = self.qty
        if not q or (q > 0) == (qty > 0):
            self.qty, self.cost = q + qty, self.cost + qty * price
            return
        closed = min(abs(qty), abs(q))
        sign = 1 if q > 0 else -1
        removed = self.cost if closed == abs(q) else self.cost * closed // abs(q)
        self.realized += sign * closed * price - removed
        self.cost -= removed
        self.qty = q - sign * closed
        rest = qty + sign * closed
        if rest:  # flipped through flat
            self.qty, self.cost = rest, rest * price

    def settle(self, price: Price) -> Micros:
        """The market resolved at `price` per YES contract (1.00, 0.00, or e.g. 0.50 for a tie); returns
        what that realized."""
        made = self.qty * price - self.cost
        self.realized += made
        self.qty = self.cost = 0
        return made

    def unrealized(self, mark: float) -> Micros:
        return round(self.qty * mark) - self.cost if self.qty else 0
