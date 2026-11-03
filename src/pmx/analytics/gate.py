"""The promotion gate: `pmx run --live` trades a strategy live only after it has paper-traded with the same params.

A live strategy passes when the paper ledger (same env) has its fills under the current params hash spanning at
least `min_days`, and its paper verdict isn't `unprofitable` (an `inconclusive` one passes, and says so). Changing a
strategy's params starts its paper record over; commits that don't change them don't. `--skip-paper-check REASON`
goes live anyway, and the reason is logged."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pmx.analytics.stats import compute, load
from pmx.analytics.verdict import verdict

DAY_MS = 86_400_000


@dataclass(frozen=True)
class Check:
    strategy: str
    ok: bool
    days: float  # from the first paper fill to the last, under these params
    fills: int
    call: str  # the paper verdict: profitable / unprofitable / inconclusive / none
    why: str


def paper_record(ledger: Any, strategy: str, params_hash: str, *, min_days: float) -> Check:
    row = ledger.db.execute(
        "SELECT COUNT(*), MIN(f.ts_ms), MAX(f.ts_ms) FROM fills f "
        "JOIN orders o ON o.client_order_id = f.client_order_id "
        "WHERE f.env=? AND f.mode='paper' AND f.params_hash=? AND o.strategy LIKE ? ESCAPE '\\'",
        (ledger.env, params_hash, strategy.replace("%", "\\%").replace("_", "\\_") + "/%")).fetchone()
    fills, first, last = row[0], row[1], row[2]
    if not fills:
        return Check(strategy, False, 0.0, 0, "none", f"no paper trading yet with these params ({params_hash})")
    days = (last - first) / DAY_MS
    v = verdict(compute(load(ledger, mode="paper", strategy=strategy, params_hash=params_hash)))
    call = v.verdict
    if days < min_days:
        return Check(strategy, False, days, fills, call, f"paper-traded {days:.1f} of the {min_days:g} days needed")
    if call == "unprofitable":
        return Check(strategy, False, days, fills, call, f"paper verdict: unprofitable — {v.reason}")
    return Check(strategy, True, days, fills, call, f"paper verdict: {call} — {v.reason}")


def check(ledger: Any | None, live: dict[str, str], *, min_days: float) -> list[Check]:
    """One result per live strategy id (id -> params hash); no ledger means no paper record."""
    if ledger is None:
        return [Check(s, False, 0.0, 0, "none", "no ledger yet: paper-trade it first") for s in live]
    return [paper_record(ledger, s, h, min_days=min_days) for s, h in sorted(live.items())]
