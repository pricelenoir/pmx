"""`scripted`: runs a script of timed actions, so system tests and milestone exits are deterministic.

    loop = false               # optional: start over after the last step
    jitter_s = 30              # optional (params): each slot starts up to this late, so many slots spread their writes
    [[steps]]
    at = "+10s"                # after the slot starts (or the loop restarts), or "2026-10-18T17:05:00Z"
    action = "quote"           # quote | take | cancel_all
    side = "bid"               # bid buys YES, ask sells YES
    price = "mid-0.05"         # "0.40" | "bid" | "ask" | "mid", ± dollars; snapped to the market's grid
    qty = "1"                  # contracts

Steps run on `Timer`s, in order. A step whose price needs a book side that isn't there waits for it.
Quote steps set one side of the slot's quote and keep the other; `cancel_all` clears both. The script
is read once, when the config is loaded."""

from __future__ import annotations

import hashlib
import math
import re
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Literal

import msgspec

from pmx.config import PROJECT
from pmx.core.types import BookSide
from pmx.core.units import ONE_DOLLAR, Price, Qty, parse_price, parse_qty
from pmx.plugins import strategy
from pmx.strategies.base import MarketContext, Strategy

_AFTER = re.compile(r"\+(\d+(?:\.\d+)?)(ms|s|m|h)")
_UNIT_MS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}
_PRICE = re.compile(r"(mid|bid|ask)(?:\s*([+-])\s*(\d*\.?\d+))?")


class Step(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    at: str
    action: Literal["quote", "take", "cancel_all"]
    side: BookSide | None = None
    price: str = ""
    qty: str = "1"

    def __post_init__(self) -> None:
        _Plan.of(self)  # reject a bad step when the config loads, not when it's due


class Script(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    steps: list[Step]
    loop: bool = False


def load_script(path: str) -> Script:
    """A relative path is from the working directory, else from the project root (`config/scripts/…`)."""
    p = Path(path)
    if not p.is_absolute() and not p.exists():
        p = PROJECT / p
    with open(p, "rb") as f:
        return msgspec.convert(tomllib.load(f), Script)


class _Plan:
    """A parsed step."""

    __slots__ = ("after_ms", "at_ms", "action", "side", "base", "offset", "qty")

    def __init__(self, after_ms: int | None, at_ms: int | None, action: str, side: BookSide | None,
                 base: str | None, offset: Price, qty: Qty) -> None:
        self.after_ms, self.at_ms, self.action, self.side = after_ms, at_ms, action, side
        self.base, self.offset, self.qty = base, offset, qty

    @classmethod
    def of(cls, s: Step) -> _Plan:
        after_ms = at_ms = None
        if m := _AFTER.fullmatch(s.at.strip()):
            after_ms = round(float(m[1]) * _UNIT_MS[m[2]])
        else:
            try:
                t = datetime.fromisoformat(s.at.strip())
            except ValueError:
                raise ValueError(f"step at {s.at!r}: expected '+10s' / '+2m' or an ISO-8601 time") from None
            if t.tzinfo is None:
                raise ValueError(f"step at {s.at!r}: give a time zone (e.g. 'Z')")
            at_ms = round(t.timestamp() * 1000)
        if s.action == "cancel_all":
            return cls(after_ms, at_ms, s.action, None, None, 0, 0)
        if s.side is None:
            raise ValueError(f"{s.action} step at {s.at!r} needs a side ('bid' buys YES, 'ask' sells YES)")
        base, offset = None, 0
        if m := _PRICE.fullmatch(s.price.strip()):
            base = m[1]
            offset = parse_price(m[3]) * (-1 if m[2] == "-" else 1) if m[3] else 0
        else:
            try:
                offset = parse_price(s.price)
            except ValueError:
                offset = 0
            if not 0 < offset < ONE_DOLLAR:
                raise ValueError(f"step at {s.at!r}: price {s.price!r} must be 'mid' / 'bid' / 'ask' ± dollars, "
                                 "or a price between 0 and 1")
        qty = parse_qty(s.qty)
        if qty <= 0:
            raise ValueError(f"step at {s.at!r}: qty must be positive, got {s.qty!r}")
        return cls(after_ms, at_ms, s.action, s.side, base, offset, qty)


@strategy("scripted")
class Scripted(Strategy):
    class Params(msgspec.Struct, frozen=True):
        script: str = ""  # a TOML file of steps
        steps: list[Step] = []  # or the steps inline
        loop: bool = False
        jitter_s: float = 0  # each slot starts up to this late (spread by slot id), so slots don't all act at once

        def __post_init__(self) -> None:
            if self.script:
                if self.steps:
                    raise ValueError("give `script` or `steps`, not both")
                s = load_script(self.script)
                msgspec.structs.force_setattr(self, "steps", s.steps)
                msgspec.structs.force_setattr(self, "loop", self.loop or s.loop)
            if not self.steps:
                raise ValueError("no steps: give `script` (a TOML file) or `steps`")

    def __init__(self, params: Params) -> None:
        super().__init__(params)
        self.plan = [_Plan.of(s) for s in params.steps]
        self.next = 0
        self.t0 = 0  # start of the current pass
        self.bid: tuple[Price, Qty] | None = None
        self.ask: tuple[Price, Qty] | None = None
        self.waiting = False

    def on_start(self, m: MarketContext) -> None:
        spread = round(self.p.jitter_s * 1000)
        delay = int.from_bytes(hashlib.blake2b(m.slot.encode(), digest_size=4).digest()) % spread if spread else 0
        self.t0 = m.now_ms + delay

    def on_timer(self, m: MarketContext) -> None:
        now = m.now_ms
        while self.next < len(self.plan):
            step = self.plan[self.next]
            due = step.at_ms if step.at_ms is not None else self.t0 + step.after_ms
            if now < due or not self._do(step, m):
                return
            self.next += 1
        if self.p.loop:
            self.next, self.t0 = 0, now

    def _do(self, step: _Plan, m: MarketContext) -> bool:
        if step.action == "cancel_all":
            self.bid = self.ask = None
            m.cancel_all()
            return True
        price = self._price(step, m)
        if price is None:
            if not self.waiting:
                self.waiting = True
                m.log(f"step {self.next + 1}: waiting for a {step.base} price", "warn")
            return False
        self.waiting = False
        if step.action == "take":
            m.take(step.side, price, step.qty)
            return True
        if step.side == "bid":
            self.bid = (price, step.qty)
        else:
            self.ask = (price, step.qty)
        m.quote(bid=self.bid, ask=self.ask)
        return True

    def _price(self, step: _Plan, m: MarketContext) -> Price | None:
        """The step's price on the market's grid: buys snap down, sells up."""
        sell = step.side == "ask"
        if step.base is None:
            raw = step.offset
        else:
            book = m.book
            if book is None:
                return None
            if step.base == "mid":
                mid = book.mid()
                if mid is None:
                    return None
                exact = mid + step.offset
                raw = math.ceil(exact) if sell else math.floor(exact)
            else:
                level = book.best_bid() if step.base == "bid" else book.best_ask()
                if level is None:
                    return None
                raw = level[0] + step.offset
        return m.contract.grid.snap(raw, up=sell)
