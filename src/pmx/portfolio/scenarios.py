"""Outcome scenarios (ARCHITECTURE *Portfolio*). An entity's mutually exclusive outcomes come from its
domain (`@outcomes` registered for the linker that made the entity); a market no domain describes is its own
entity with outcomes `yes` and `no`. Positions net within an entity: holding both teams of one game is a
hedge, and its worst case is the outcome where both lose (a tie), not the sum of each position's worst."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterable, Mapping

from pmx import plugins
from pmx.core.position import AvgCost
from pmx.core.units import ONE_DOLLAR, Micros, Price
from pmx.engine.state import MarketId, State

log = logging.getLogger(__name__)

type Leg = str | int  # "yes" ($1), "no" ($0), or the YES settlement price
type Outcomes = Mapping[str, Mapping[MarketId, Leg]]


def leg_price(leg: Leg) -> Price:
    return ONE_DOLLAR if leg == "yes" else 0 if leg == "no" else int(leg)


def binary(ticker: MarketId) -> dict[str, dict[MarketId, Leg]]:
    return {"yes": {ticker: "yes"}, "no": {ticker: "no"}}


class OutcomeMap:
    """entity → its outcomes over the markets linked to it now, from the domain's hook; cached until the
    entity's markets change. Also the router's `outcomes` (group slots)."""

    def __init__(self, state: State, *, linkers: Mapping[str, plugins.Linker] | None = None,
                 registry: Mapping[str, plugins.Outcomes] | None = None) -> None:
        self.state = state
        self.linkers = plugins.LINKERS if linkers is None else linkers
        self.registry = plugins.OUTCOMES if registry is None else registry
        self._cache: dict[str, tuple[frozenset[MarketId], dict[str, dict[MarketId, Leg]]]] = {}

    def __call__(self, entity: str) -> dict[str, dict[MarketId, Leg]]:
        tickers = frozenset(self.state.tickers_of.get(entity, ()))
        hit = self._cache.get(entity)
        if hit is not None and hit[0] == tickers:
            return hit[1]
        out: dict[str, dict[MarketId, Leg]] = {}
        contracts = [self.state.contracts[t] for t in sorted(tickers) if t in self.state.contracts]
        for kind, fn in self.registry.items():
            link = self.linkers.get(kind)
            if not contracts or link is None or link(contracts[0]) != entity:
                continue
            try:
                out = {name: dict(legs) for name, legs in fn(entity, contracts).items()}
            except Exception:
                log.exception("outcomes %r failed for %s", kind, entity)
            break
        self._cache[entity] = (tickers, out)
        return out

    def of(self, ticker: MarketId) -> tuple[str, Mapping[str, Mapping[MarketId, Leg]]]:
        """The scenario entity a market nets in, and its outcomes: its linked entity when the domain's outcomes
        cover it, else the market alone."""
        entity = self.state.entity_of.get(ticker)
        if entity is not None:
            out = self(entity)
            if out and all(ticker in legs for legs in out.values()):
                return entity, out
        return ticker, binary(ticker)


def scenarios(positions: Iterable[tuple[MarketId, AvgCost]], outcomes: Outcomes) -> dict[str, Micros]:
    """{outcome: PnL of these positions if it happens} — realized − fees + Σ (qty × settlement − cost)."""
    held = list(positions)
    base = sum(p.realized - p.fees for _, p in held)
    return {name: base + sum(p.qty * leg_price(legs[t]) - p.cost for t, p in held if p.qty)
            for name, legs in outcomes.items()}


def probabilities(outcomes: Outcomes, p_of: Callable[[MarketId], float | None]) -> dict[str, float] | None:
    """P(outcome) from per-market P(YES) (a model's forecasts or book mids), normalized over the outcomes: an
    outcome with markets that settle at $1 gets the smallest of their P(YES); the others share what's left.
    None when a needed market has no probability."""
    raw: dict[str, float] = {}
    rest: list[str] = []
    for name, legs in outcomes.items():
        ps = [p_of(t) for t, leg in legs.items() if leg_price(leg) == ONE_DOLLAR]
        if not ps:
            rest.append(name)
            continue
        if any(p is None for p in ps):
            return None
        raw[name] = min(ps)  # type: ignore[type-var]
    left = max(0.0, 1.0 - sum(raw.values()))
    for name in rest:
        raw[name] = left / len(rest)
    total = sum(raw.values())
    return {k: v / total for k, v in raw.items()} if total > 0 else None


def ev(scen: Mapping[str, Micros], p: Mapping[str, float] | None) -> tuple[Micros, float] | None:
    """(expected PnL, its variance) under `p`."""
    if p is None:
        return None
    mean = sum(p[k] * v for k, v in scen.items())
    var = sum(p[k] * (v - mean) ** 2 for k, v in scen.items())
    return round(mean), var


def stdev(variances: Iterable[float]) -> Micros:
    """Entities are independent: variances add."""
    return round(math.sqrt(sum(variances)))
