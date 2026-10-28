"""The verdict: `profitable` when the 95% CI of ¢/contract (bootstrap over entities) is above zero,
`unprofitable` when below, else `inconclusive` — and always inconclusive under `MIN_ENTITIES`, where a bootstrap
over a handful of games says more about the games than the strategy. With a one-line reason (over closed positions,
what the expectancy is measured on), a rough sample needed to decide, and warning signs: positive PnL with negative
markouts (luck), edge buckets that don't rise with the edge, one entity carrying most of the PnL."""

from __future__ import annotations

import math

from pmx.feed.proto import BucketRow, PerfStats, Verdict

LUCK_HORIZON = "1m"
ENTITY_SHARE = 0.5
MIN_ENTITIES = 10


def verdict(stats: PerfStats, rows: dict[str, list[BucketRow]] | None = None, *, entity: str = "entities") -> Verdict:
    rows = rows or {}
    if stats.episodes == 0 or stats.expectancy_c is None:
        return Verdict("inconclusive", "no closed positions yet", needed="closed positions in at least 2 " + entity)
    ci = stats.expectancy_ci
    n_ent = len(rows.get("entity", [])) or stats.entities
    closed = sum(r.contracts for r in rows.get("entity", [])) or stats.contracts
    head = (f"{stats.expectancy_c:+.2f}¢/contract" + (f", 95% CI [{ci[0]:+.2f}, {ci[1]:+.2f}]" if ci else "")
            + f", {closed / 100:,.0f} contracts in closed positions over {n_ent} {entity}")
    if ci is None or n_ent < MIN_ENTITIES:
        call, needed = "inconclusive", f"at least {MIN_ENTITIES} {entity} before calling it ({n_ent} so far)"
    elif ci[0] > 0:
        call, needed = "profitable", ""
    elif ci[1] < 0:
        call, needed = "unprofitable", ""
    else:
        call, needed = "inconclusive", _needed(stats.expectancy_c, ci, n_ent, entity)
    return Verdict(call, head, _flags(stats, rows), needed)


def _needed(mean: float, ci: tuple[float, float], n: int, entity: str) -> str:
    """The CI's half-width shrinks like 1/√n: how many entities until it excludes zero at this mean."""
    half = (ci[1] - ci[0]) / 2
    if mean == 0 or half <= 0 or n <= 0:
        return "more data (no edge visible yet)"
    total = math.ceil(n * (half / abs(mean)) ** 2)
    return f"about {max(total - n, 1):,} more {entity} at this mean and spread ({total:,} in all)"


def _flags(stats: PerfStats, rows: dict[str, list[BucketRow]]) -> list[str]:
    out = []
    mk = stats.markouts_c.get(LUCK_HORIZON)
    if stats.net > 0 and mk is not None and mk < 0:
        out.append(f"positive PnL but negative markouts ({mk:+.2f}¢ at {LUCK_HORIZON}): fills are adverse — luck?")
    edge = [r for r in rows.get("edge", []) if r.cpc is not None and r.key != "<2¢"]
    if len(edge) >= 2 and any(b.cpc < a.cpc for a, b in zip(edge, edge[1:], strict=False)):  # type: ignore[operator]
        out.append("bigger predicted edges don't earn more (edge buckets not monotone)")
    ents = rows.get("entity", [])
    total = sum(r.pnl for r in ents)
    if total > 0 and ents and max(r.pnl for r in ents) > ENTITY_SHARE * total and len(ents) > 1:
        top = max(ents, key=lambda r: r.pnl)
        out.append(f"{top.key} carries {top.pnl / total:.0%} of the PnL")
    return out
