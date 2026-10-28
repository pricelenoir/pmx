"""Limit usage: how much of each configured risk limit every scope uses — the global limits at `portfolio`,
each strategy's at `strategy:<id>`, and each slot's (holding a position or working orders) at `slot:<id>` — for
the snapshot viewers and `pmx risk` show. Risk enforces the limits; this only measures them."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pmx.engine.events import LimitUsage
from pmx.execution.risk import RiskLimits

if TYPE_CHECKING:
    from pmx.portfolio.tracker import PortfolioTracker

_AT_LIMIT = frozenset({"max_loss", "max_drawdown"})  # reaching these halts; the rest bind past the cap


def limit_usage(t: PortfolioTracker, mode: str) -> list[LimitUsage]:
    from pmx.portfolio.tracker import PORTFOLIO, slot_scope, strategy_scope

    risk: Any = t.risk
    if risk is None:
        return []
    rows: list[LimitUsage] = []
    rows += _usage(t, PORTFOLIO, "", mode, risk.global_limits)
    for sid in t._strategies(mode):
        lim = risk.strategy_limits.get(sid)
        if lim is not None:
            rows += _usage(t, strategy_scope(sid), sid, mode, lim)
    for scope in t._scopes(mode, slots="open"):
        if not scope.startswith("slot:"):
            continue
        sid = scope[5:]
        s = t.slots.get(sid)
        lim = RiskLimits.of(getattr(s, "risk_limits", None)) if s is not None else None
        if lim is not None:
            rows += _usage(t, slot_scope(sid), sid, mode, lim)
    return rows


def _usage(t: PortfolioTracker, scope: str, prefix: str, mode: str, lim: RiskLimits) -> list[LimitUsage]:
    s = t.stats(scope, mode)
    in_scope = (lambda slot: True) if not prefix else (
        (lambda slot: slot == prefix) if "/" in prefix else (lambda slot: slot.startswith(prefix + "/")))
    used: dict[str, int] = {}
    if lim.max_position is not None:
        by_market: dict[str, int] = {}
        for (m, slot, mid), p in t._pos.items():
            if m == mode and in_scope(slot):
                by_market[mid] = by_market.get(mid, 0) + p.qty
        used["max_position"] = max((abs(q) for q in by_market.values()), default=0)
    if lim.max_open_orders is not None:
        used["max_open_orders"] = sum(1 for o in t._working.values() if o.mode == mode and in_scope(o.slot))
    if lim.max_loss is not None:
        used["max_loss"] = max(0, -s.today)
    if lim.max_capital_at_risk is not None:
        used["max_capital_at_risk"] = s.capital_at_risk + s.working_exposure
    if lim.max_worst_case is not None:
        used["max_worst_case"] = max(0, -s.worst_case)
    if lim.max_entity_risk is not None:
        strategy = None if not prefix else prefix.split("/", 1)[0]
        used["max_entity_risk"] = max((e.capital_at_risk for e in t.exposures(mode, strategy)), default=0)
    if lim.max_drawdown is not None:
        used["max_drawdown"] = s.drawdown
    out = []
    for name, value in used.items():
        cap = getattr(lim, name)
        out.append(LimitUsage(scope, name, value, cap, value >= cap if name in _AT_LIMIT else value > cap))
    return out
