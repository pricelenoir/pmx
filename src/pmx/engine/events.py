"""Engine events beyond the market records in `core/types.py`: everything that flows
through the engine queue and out to the feed. Integer units and YES terms throughout.

Nothing here knows about a domain. Outside data (a sports game, a weather station-day, an
economic release) arrives as domain structs that satisfy `EntityState`."""

from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

import msgspec

from pmx.core.types import (
    BookDelta,
    BookSide,
    BookSnapshot,
    Contract,
    Fill,
    StreamReset,
    Trade,
    VenueOrder,
    VenuePosition,
)
from pmx.core.units import Micros, Price, Qty

type Mode = Literal["paper", "live"]


@runtime_checkable
class EntityState(Protocol):
    """Latest known state of one outside entity, keyed like `nfl:2026-10-18:HOU-JAX` or
    `wx:KNYC:2026-10-18`. `summary` and `display_fields` are what viewers show."""

    ts_ms: int

    @property
    def entity(self) -> str: ...

    def summary(self) -> str: ...

    def display_fields(self) -> list[tuple[str, str]]: ...


class Timer(msgspec.Struct, frozen=True):
    ts_ms: int


class SourceStatus(msgspec.Struct, frozen=True):
    source: str
    connected: bool
    detail: str
    ts_ms: int


class AccountSnapshot(msgspec.Struct, frozen=True):
    balance: Micros
    positions: list[VenuePosition]
    ts_ms: int
    shards: dict[int, Micros] = {}  # balance per exchange shard, when the venue says


class Forecast(msgspec.Struct, frozen=True):
    model: str  # model instance name from config
    ticker: str
    p: float  # P(YES)
    ts_ms: int  # engine time it was made
    data_ts_ms: int  # newest input it used
    meta: dict[str, float] = {}
    version: str = ""  # model artifact hash or code version


class Advice(msgspec.Struct, frozen=True):
    """From an advisor (an AI agent, a slow service) running outside the engine loop.
    Strategies that list the advisor treat it like a forecast; advisors never place orders."""

    advisor: str  # advisor instance name from config
    target: str  # ticker, or entity key (reaches every linked ticker)
    p: float | None  # P(YES) for a ticker target
    direction: Literal["buy", "sell", "flat"] | None
    confidence: float  # 0..1, the advisor's own
    expires_ms: int  # engine time after which nothing may act on it
    rationale: str  # one line
    ts_ms: int


RATIONALE_MAX = 200
_DIRECTIONS = (None, "buy", "sell", "flat")


def advice_problem(a: Advice, *, known: bool) -> str | None:
    """Why `a` can't be acted on, or None. `known`: its target is a contract or an entity the
    engine knows. Advice is bounded data — never instructions."""
    if not known:
        return f"unknown target {a.target!r}"
    if a.p is None and a.direction is None:
        return "neither p nor a direction"
    if a.p is not None and not 0 <= a.p <= 1:  # NaN fails too
        return f"p {a.p} isn't in [0, 1]"
    if a.direction not in _DIRECTIONS:
        return f"direction {a.direction!r} isn't buy, sell, or flat"
    if not 0 <= a.confidence <= 1:
        return f"confidence {a.confidence} isn't in [0, 1]"
    if len(a.rationale) > RATIONALE_MAX:
        return f"rationale is {len(a.rationale)} characters (at most {RATIONALE_MAX})"
    return None


class Refused(ValueError):
    """`State` won't take this event (malformed advice): it goes no further — not routed, not
    published — and becomes a warning `LogEvent` from `source`."""

    def __init__(self, source: str, reason: str) -> None:
        super().__init__(reason)
        self.source = source


class Schedule(msgspec.Struct, frozen=True):
    """A known future moment strategies and risk windows key off (a release, a kickoff, a
    report, a contract's resolution)."""

    target: str  # entity key or ticker
    kind: str  # "release" | "kickoff" | "report" | "resolution" | ...
    at_ms: int
    detail: str
    ts_ms: int  # when this became known


class OrderUpdate(msgspec.Struct, frozen=True):
    slot: str  # "<strategy_id>/<ticker>"
    mode: Mode
    client_order_id: str
    order_id: str | None
    ticker: str
    side: BookSide
    price: Price
    qty: Qty
    filled: Qty
    status: Literal["queued", "pending", "open", "filled", "canceled", "rejected"]  # queued: awaiting write capacity
    ts_ms: int
    reason: str = ""


class SlotFill(msgspec.Struct, frozen=True):
    slot: str
    mode: Mode
    fill: Fill


class RiskReject(msgspec.Struct, frozen=True):
    slot: str
    ticker: str
    side: BookSide
    price: Price
    qty: Qty
    rule: str  # "max_position" | "stale_book" | "halted" | ...
    detail: str
    ts_ms: int


class SlotStatus(msgspec.Struct, frozen=True):
    slot: str
    strategy: str
    ticker: str
    mode: Mode
    state: Literal["warming", "idle", "active", "halted", "error"]
    position: Qty
    realized: Micros
    unrealized: Micros
    fees: Micros
    working: int
    last_action: str
    ts_ms: int


class Halt(msgspec.Struct, frozen=True):
    """A halt was set or cleared. `scope` is a slot id, a strategy id, or "" for everything."""

    scope: str
    reason: str
    active: bool
    ts_ms: int


class Exposure(msgspec.Struct, frozen=True):
    entity: str  # or a lone ticker
    mode: Mode  # paper and live exposure are never netted
    tickers: list[str]
    strategies: list[str]
    capital_at_risk: Micros
    scenarios: dict[str, Micros]  # outcome -> total PnL if it happens
    worst_case: Micros
    ev_model: Micros | None
    ev_market: Micros | None


class ScopeStats(msgspec.Struct, frozen=True):
    scope: str  # "portfolio" | "strategy:<id>" | "slot:<id>"
    mode: Mode
    realized: Micros
    unrealized: Micros  # at mid
    fees: Micros
    equity: Micros  # realized + unrealized - fees, since inception
    today: Micros  # equity change since local midnight
    hwm: Micros  # today's high-water mark (local day)
    drawdown: Micros  # hwm - equity
    capital_at_risk: Micros
    worst_case: Micros
    ev_model: Micros | None
    stdev_model: Micros | None
    gross_exposure: Micros  # sum |q| * mid
    working_exposure: Micros  # extra capital at risk if every working order fills
    open_positions: int
    stale: bool = False  # some mark is a last-known mid, not a live book


class LimitUsage(msgspec.Struct, frozen=True):
    scope: str
    limit: str  # "max_loss" | "max_capital_at_risk" | ...
    used: int
    cap: int
    breached: bool


class PortfolioSnapshot(msgspec.Struct, frozen=True):
    ts_ms: int
    scopes: list[ScopeStats]
    exposures: list[Exposure]
    limits: list[LimitUsage]


class LogEvent(msgspec.Struct, frozen=True):
    level: Literal["info", "warn", "error"]
    source: str  # component or slot id
    text: str
    ts_ms: int


class Command(msgspec.Struct, frozen=True):
    """A control-channel request as an engine input, so manual orders, cancels, and halts take the queue and
    the order manager like everything else. `request` is a `control.protocol` struct; `ref` matches the result."""

    ref: int
    request: msgspec.Struct
    ts_ms: int


class CommandResult(msgspec.Struct, frozen=True):
    ref: int
    ok: bool
    error: str
    client_order_ids: list[str]
    ts_ms: int


type Event = (
    BookSnapshot | BookDelta | Trade | Fill | VenueOrder | StreamReset | Contract | Forecast | Advice | Schedule
    | Timer
    | SourceStatus | AccountSnapshot | OrderUpdate | SlotFill | RiskReject | SlotStatus | Halt | PortfolioSnapshot
    | LogEvent | EntityState | Command | CommandResult
)
