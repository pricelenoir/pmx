"""Breakdowns: where the PnL came from — by market, series, entity, phase, predicted-edge bucket, and day — each
a list of `BucketRow`s over the closed episodes in range. Phases are whatever the domain records (`fill_meta.phase`,
e.g. NFL quarters); the edge buckets ask whether a bigger predicted edge earns more."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import datetime

from pmx.analytics.stats import TZ, Data, Episode, cpc, spearman
from pmx.feed.proto import BucketRow

EDGE_BUCKETS = ((None, 2.0, "<2¢"), (2.0, 4.0, "2–4¢"), (4.0, 6.0, "4–6¢"), (6.0, 10.0, "6–10¢"), (10.0, None, ">10¢"))


def edge_bucket(edge: float | None) -> str | None:
    if edge is None:
        return None
    for lo, hi, name in EDGE_BUCKETS:
        if (lo is None or edge >= lo) and (hi is None or edge < hi):
            return name
    return None


def _rows(episodes: Iterable[Episode], key: Callable[[Episode], str | None]) -> list[BucketRow]:
    acc: dict[str, list] = defaultdict(lambda: [0, 0, 0, 0.0, 0])  # episodes, contracts, pnl, edge·qty, edge qty
    for ep in episodes:
        k = key(ep)
        if k is None:
            continue
        a = acc[k]
        a[0] += 1
        a[1] += ep.contracts
        a[2] += ep.pnl
        if ep.edge is not None:
            a[3] += ep.edge * ep.contracts
            a[4] += ep.contracts
    return [BucketRow(k, a[0], a[1], a[2], round(c, 3) if (c := cpc(a[2], a[1])) is not None else None,
                      round(a[3] / a[4], 3) if a[4] else None) for k, a in acc.items()]


def _day(ep: Episode) -> str:
    return datetime.fromtimestamp((ep.closed_ms or ep.opened_ms) / 1000, TZ).strftime("%Y-%m-%d")


def breakdowns(d: Data) -> dict[str, list[BucketRow]]:
    """`fills` in each row counts episodes."""
    eps = d.episodes
    by_pnl = lambda rows: sorted(rows, key=lambda r: -abs(r.pnl))  # noqa: E731
    order = [name for _, _, name in EDGE_BUCKETS]
    return {
        "market": by_pnl(_rows(eps, lambda e: e.ticker)),
        "series": by_pnl(_rows(eps, lambda e: e.ticker.split("-", 1)[0])),
        "entity": by_pnl(_rows(eps, lambda e: e.entity)),
        "phase": sorted(_rows(eps, lambda e: e.phase), key=lambda r: r.key),
        "edge": sorted(_rows(eps, lambda e: edge_bucket(e.edge)), key=lambda r: order.index(r.key)),
        "day": sorted(_rows(eps, _day), key=lambda r: r.key),
    }


def edge_spearman(d: Data) -> float | None:
    """Rank correlation of each episode's predicted edge with its result per contract."""
    pts = [(ep.edge, cpc(ep.pnl, ep.contracts)) for ep in d.episodes if ep.edge is not None and ep.contracts]
    return spearman([p[0] for p in pts], [p[1] for p in pts]) if pts else None
