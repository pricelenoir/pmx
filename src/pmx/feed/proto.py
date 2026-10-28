"""Feed protocol: newline-delimited JSON from the feed process to viewers (the Rust TUI in
`tui/`, `tui/src/proto.rs` — change both together). Integer units and YES terms, as in core.
A new connection gets the full state first, then live updates. Viewers ignore tags they
don't know, and fields default, so additions are compatible.

v3 conflates market data: per changed market a `top` and its top-`BOOK_LEVELS` `book`,
at most 4/s (10/s with slots or working orders), latest wins. Deltas and per-forecast
messages aren't sent; a v2 viewer still gets books (and ignores `top`).

Nothing here is domain-specific: outside data is an `entity` with a domain-written
summary and display fields."""

from __future__ import annotations

from typing import Literal

import msgspec

from pmx.core.types import BookSide, Contract
from pmx.engine.events import (
    Advice,
    EntityState,
    Exposure,
    Forecast,
    Halt,
    LimitUsage,
    LogEvent,
    OrderUpdate,
    PortfolioSnapshot,
    RiskReject,
    ScopeStats,
    SlotStatus,
)

VERSION = 3
BOOK_LEVELS = 10  # per side in a conflated `book`

type Range = Literal["today", "7d", "30d", "all"]


class Hello(msgspec.Struct, tag="hello", tag_field="t"):
    venue: str
    env: str
    source: str  # live | poll | replay | engine | backtest | demo
    version: int = VERSION
    run: str = ""  # backtest run id


class Synced(msgspec.Struct, tag="synced", tag_field="t"):
    """The full state a viewer gets on connect (or a resync) has all been sent; live updates follow."""


class Status(msgspec.Struct, tag="status", tag_field="t"):
    """Health of one data source (the venue connection, an outside feed, or `source="engine"`:
    the engine's link to the feed process)."""

    connected: bool
    detail: str
    ts_ms: int
    source: str = ""  # "" = the venue
    dropped: int = 0  # engine: events that never reached the feed (it was slow or down)


class ContractMsg(msgspec.Struct, tag="contract", tag_field="t"):
    ticker: str
    event_ticker: str
    series: str
    title: str
    subtitle: str
    status: str
    close_ts_ms: int | None
    event_ts_ms: int | None
    result: str | None

    @classmethod
    def of(cls, c: Contract) -> ContractMsg:
        return cls(c.ticker, c.event_ticker, c.series_ticker, c.title, c.subtitle, c.status, c.close_ts_ms,
                   c.event_ts_ms, c.result)


class Book(msgspec.Struct, tag="book", tag_field="t"):
    ticker: str
    bids: list[tuple[int, int]]
    asks: list[tuple[int, int]]
    ts_ms: int


class Delta(msgspec.Struct, tag="delta", tag_field="t"):
    ticker: str
    side: BookSide
    price: int
    delta: int
    ts_ms: int


class Top(msgspec.Struct, tag="top", tag_field="t"):
    """One market at a glance (v3, conflated with its `book`): best levels, last print, every
    model's fair, and the slots' position per mode."""

    ticker: str
    bid: tuple[int, int] | None  # (price, qty)
    ask: tuple[int, int] | None
    last: tuple[int, int, BookSide] | None  # (price, qty, taker side)
    fairs: dict[str, float]  # model -> P(YES)
    position: dict[str, int]  # mode -> signed YES qty, nonzero only
    ts_ms: int


class Stale(msgspec.Struct, tag="stale", tag_field="t"):
    """One book is no longer trustworthy until its next `book`."""

    ticker: str


class Reset(msgspec.Struct, tag="reset", tag_field="t"):
    """Every book is stale until its next `book`."""

    reason: str
    ts_ms: int


class TradeMsg(msgspec.Struct, tag="trade", tag_field="t"):
    ticker: str
    price: int
    qty: int
    taker_side: BookSide
    ts_ms: int


class FillMsg(msgspec.Struct, tag="fill", tag_field="t"):
    ticker: str
    side: BookSide
    price: int
    qty: int
    fee: int
    is_taker: bool
    ts_ms: int
    slot: str = ""  # "" = not attributed to a slot (v1 `pmx serve`)
    mode: str = ""


class PositionRow(msgspec.Struct):
    ticker: str
    qty: int
    realized_pnl: int
    fees: int


class Account(msgspec.Struct, tag="account", tag_field="t"):
    balance: int  # micros
    positions: list[PositionRow]
    ts_ms: int


class EntityMsg(msgspec.Struct, tag="entity", tag_field="t"):
    """Latest state of an outside entity linked to `tickers`, as its domain displays it."""

    key: str
    kind: str
    tickers: list[str]
    summary: str
    fields: list[tuple[str, str]]
    ts_ms: int

    @classmethod
    def of(cls, ev: EntityState, tickers: list[str]) -> EntityMsg:
        kind = getattr(ev, "kind", type(ev).__name__)
        return cls(ev.entity, kind, tickers, ev.summary(), ev.display_fields(), ev.ts_ms)


class ForecastMsg(msgspec.Struct, tag="forecast", tag_field="t"):
    model: str
    ticker: str
    p: float
    ts_ms: int
    data_ts_ms: int

    @classmethod
    def of(cls, f: Forecast) -> ForecastMsg:
        return cls(f.model, f.ticker, f.p, f.ts_ms, f.data_ts_ms)


class AdviceMsg(msgspec.Struct, tag="advice", tag_field="t"):
    """An advisor's latest word on a ticker or entity. `expires_ms` before `ts_ms` withdraws it."""

    advisor: str
    target: str
    p: float | None
    direction: str | None
    confidence: float
    expires_ms: int
    rationale: str
    ts_ms: int

    @classmethod
    def of(cls, a: Advice) -> AdviceMsg:
        return cls(**msgspec.structs.asdict(a))


class Models(msgspec.Struct, tag="models", tag_field="t"):
    names: list[str]


class SlotMsg(msgspec.Struct, tag="slot", tag_field="t"):
    slot: str
    strategy: str
    ticker: str
    mode: str
    state: str
    position: int
    realized: int
    unrealized: int
    fees: int
    working: int
    last_action: str
    ts_ms: int

    @classmethod
    def of(cls, s: SlotStatus) -> SlotMsg:
        return cls(**msgspec.structs.asdict(s))


class OrderMsg(msgspec.Struct, tag="order", tag_field="t"):
    slot: str
    mode: str
    client_order_id: str
    order_id: str | None
    ticker: str
    side: BookSide
    price: int
    qty: int
    filled: int
    status: str  # queued | pending | open | filled | canceled | rejected
    ts_ms: int
    reason: str = ""

    @classmethod
    def of(cls, o: OrderUpdate) -> OrderMsg:
        return cls(**msgspec.structs.asdict(o))

    @property
    def working(self) -> bool:
        return self.status in ("queued", "pending", "open")


class RiskMsg(msgspec.Struct, tag="risk", tag_field="t"):
    slot: str
    ticker: str
    side: BookSide
    price: int
    qty: int
    rule: str
    detail: str
    ts_ms: int

    @classmethod
    def of(cls, r: RiskReject) -> RiskMsg:
        return cls(**msgspec.structs.asdict(r))


class LogMsg(msgspec.Struct, tag="log", tag_field="t"):
    level: str  # info | warn | error
    source: str
    text: str
    ts_ms: int

    @classmethod
    def of(cls, e: LogEvent) -> LogMsg:
        return cls(e.level, e.source, e.text, e.ts_ms)


class HaltMsg(msgspec.Struct, tag="halt", tag_field="t"):
    scope: str  # slot id, strategy id, or "" = everything
    reason: str
    active: bool
    ts_ms: int

    @classmethod
    def of(cls, h: Halt) -> HaltMsg:
        return cls(h.scope, h.reason, h.active, h.ts_ms)


class PortfolioMsg(msgspec.Struct, tag="portfolio", tag_field="t"):
    ts_ms: int
    scopes: list[ScopeStats]
    exposures: list[Exposure]
    limits: list[LimitUsage]

    @classmethod
    def of(cls, p: PortfolioSnapshot) -> PortfolioMsg:
        return cls(p.ts_ms, p.scopes, p.exposures, p.limits)


class EquityMsg(msgspec.Struct, tag="equity", tag_field="t"):
    """Equity series for one scope: `(ts_ms, equity, drawdown)` micros. `replace` = this is
    the whole history (sent on connect); otherwise append."""

    scope: str
    mode: str
    points: list[tuple[int, int, int]]
    replace: bool = False


class PerfStats(msgspec.Struct):
    """One strategy's performance over a range (computed by analytics). Money in micros;
    `*_c` fields in cents per contract; None = not enough data to say."""

    net: int = 0
    gross: int = 0
    fees: int = 0
    realized: int = 0
    open: int = 0
    orders: int = 0
    fills: int = 0
    contracts: int = 0  # Qty
    maker_share: float | None = None
    fill_rate: float | None = None
    episodes: int = 0
    markets: int = 0
    entities: int = 0
    win_rate: float | None = None
    avg_win: int | None = None
    avg_loss: int | None = None
    profit_factor: float | None = None
    expectancy_c: float | None = None
    expectancy_ci: tuple[float, float] | None = None  # 95%, bootstrap clustered by entity
    pnl_ci: tuple[int, int] | None = None
    p_nonpositive: float | None = None  # P(true expectancy <= 0)
    best_episode: int | None = None
    worst_episode: int | None = None
    edge_entry_c: float | None = None
    edge_realized_c: float | None = None
    capture: float | None = None
    markouts_c: dict[str, float] = {}  # "10s" | "1m" | "5m"
    max_drawdown: int = 0
    time_under_water_ms: int = 0
    max_capital_at_risk: int = 0
    return_on_risk: float | None = None
    daily: list[tuple[int, int]] = []  # (local midnight ts_ms, pnl)
    sharpe_day: float | None = None
    days: int = 0
    pct_days_positive: float | None = None
    brier_model: float | None = None
    brier_market: float | None = None


class BucketRow(msgspec.Struct):
    key: str
    fills: int
    contracts: int
    pnl: int
    cpc: float | None = None  # realized cents per contract
    edge_c: float | None = None  # average predicted edge at entry


class Verdict(msgspec.Struct):
    verdict: Literal["profitable", "unprofitable", "inconclusive"]
    reason: str
    flags: list[str] = []
    needed: str = ""  # sample needed to decide, at the current mean and variance


class PerfMsg(msgspec.Struct, tag="perf", tag_field="t"):
    strategy: str
    mode: str
    range: Range
    ts_ms: int
    stats: PerfStats
    breakdowns: dict[str, list[BucketRow]]  # "market" | "entity" | "phase" | "edge" | "day"
    verdict: Verdict


type Msg = (
    Hello | Synced | Status | ContractMsg | Book | Delta | Top | Stale | Reset | TradeMsg | FillMsg | Account
    | EntityMsg | ForecastMsg | AdviceMsg | Models | SlotMsg | OrderMsg | RiskMsg | LogMsg | HaltMsg | PortfolioMsg
    | EquityMsg | PerfMsg
)

encoder = msgspec.json.Encoder()
decoder = msgspec.json.Decoder(Msg)


def encode(msg: Msg) -> bytes:
    return encoder.encode(msg) + b"\n"
