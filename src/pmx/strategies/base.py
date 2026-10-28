"""Strategies (ARCHITECTURE *Strategies*). A strategy is written for one market, or
one group of related markets; the router (`engine/slots.py`) runs an isolated instance per slot and
calls its hooks with a context. Strategies act only through that context: `quote` / `take` /
`cancel_all` / `target` buffer intents for the order manager. Nothing here calls a broker, reads the
clock, or does I/O."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol

import msgspec

from pmx.core.book import OrderBook
from pmx.core.types import BookDelta, BookSide, BookSnapshot, Contract, Trade
from pmx.core.units import ONE_CONTRACT, Price, Qty
from pmx.engine.events import (
    Advice,
    EntityState,
    Event,
    Exposure,
    Forecast,
    LogEvent,
    Schedule,
    ScopeStats,
    SlotFill,
    Timer,
)
from pmx.engine.state import MarketId, State, market_id

if TYPE_CHECKING:
    from pmx.engine.slots import Slot

# ---- intents: what a slot asks the order manager for ---------------------------------------------


class Quote(msgspec.Struct, frozen=True):
    """The resting orders a slot wants on one market now (post-only GTC). Replaces its previous
    quote there; both sides None pulls it."""

    slot: str
    ticker: MarketId
    bid: tuple[Price, Qty] | None
    ask: tuple[Price, Qty] | None


class Take(msgspec.Struct, frozen=True):
    """Immediate-or-cancel at `limit` or better; `side="bid"` buys YES."""

    slot: str
    ticker: MarketId
    side: BookSide
    limit: Price
    qty: Qty


class CancelAll(msgspec.Struct, frozen=True):
    slot: str
    ticker: MarketId | None = None  # None = every market of the slot


class Target(msgspec.Struct, frozen=True):
    """A position to work toward; the execution helpers (`execution/algos.py`) turn it into quotes and takes."""

    slot: str
    ticker: MarketId
    qty: Qty
    style: str = "passive"
    opts: dict[str, Any] = {}


type Intent = Quote | Take | CancelAll | Target


class WorkingOrder(msgspec.Struct, frozen=True):
    client_order_id: str
    side: BookSide
    price: Price
    qty: Qty
    filled: Qty
    status: Literal["queued", "pending", "open"]
    queue_ahead: Qty | None  # size ahead of us at our price: paper exact, live estimated
    placed_ms: int


class OrderView(Protocol):
    """A slot's own orders and position: the order manager, or `engine/slots.py:OrderTracker` without one."""

    def position(self, slot: str, ticker: MarketId) -> Qty: ...

    def working(self, slot: str, ticker: MarketId) -> list[WorkingOrder]: ...

    def throttled(self, slot: str) -> bool: ...


class PortfolioView(Protocol):
    """Read-only portfolio numbers (`PortfolioTracker`, or a minimal one from `OrderTracker`)."""

    def stats(self, scope: str, mode: str) -> ScopeStats: ...

    def exposures(self, mode: str, strategy: str | None = None) -> list[Exposure]: ...


# ---- the strategy base class -----------------------------------------------------------------------


class Strategy:
    """Subclass and override the hooks you need. One instance runs per slot, so whatever a slot
    remembers goes on `self`. No I/O, no clock (use `m.now_ms`), no globals. Hooks get a `MarketContext`,
    or a `GroupContext` in a group slot."""

    Params: ClassVar[type | None] = None
    inputs: ClassVar[tuple[str, ...]] = ()  # source names it reads (config `inputs` adds to these)
    cadence_ms: ClassVar[int] = 0  # 0 = every event; N = book/trade/input hooks at most every N ms
    warmup_s: ClassVar[float] = 0  # no orders until this much engine time has passed

    def __init__(self, params: Any = None) -> None:
        self.p = params

    def ready(self, m: MarketContext) -> bool:
        """Extra readiness check once `warmup_s` has passed; the slot stays `warming` until it's true."""
        return True

    def on_start(self, m: MarketContext) -> None: ...

    def on_event(self, ev: Event, m: MarketContext) -> None:
        """Every event routed to the slot except declared inputs, which go to `on_input` (the same
        event can reach a slot both ways). Dispatches to the hooks below."""
        t = type(ev)
        if t is BookDelta or t is BookSnapshot:
            book = m.market(market_id(ev.venue, ev.ticker)).book
            if book is not None:
                self.on_book(book, m)
        elif t is Forecast:
            self.on_forecast(ev, m)
        elif t is Trade:
            self.on_trade(ev, m)
        elif t is SlotFill:
            self.on_fill(ev, m)
        elif t is Timer:
            self.on_timer(m)
        elif t is Advice:
            self.on_advice(ev, m)
        elif m._state.is_entity(ev):
            self.on_data(ev, m)

    def on_book(self, book: OrderBook, m: MarketContext) -> None: ...

    def on_trade(self, t: Trade, m: MarketContext) -> None: ...

    def on_forecast(self, f: Forecast, m: MarketContext) -> None: ...

    def on_data(self, data: EntityState, m: MarketContext) -> None:
        """The linked entity changed."""

    def on_input(self, name: str, ev: Event, m: MarketContext) -> None:
        """An event from a declared input source."""

    def on_advice(self, a: Advice, m: MarketContext) -> None: ...

    def on_fill(self, f: SlotFill, m: MarketContext) -> None: ...

    def on_timer(self, m: MarketContext) -> None: ...

    def on_stop(self, m: MarketContext) -> None: ...

    def on_save(self) -> bytes | None:
        """A checkpoint of learned state, persisted by `pmx run`; None = nothing to keep."""
        return None

    def on_load(self, blob: bytes) -> None: ...


# ---- contexts ----------------------------------------------------------------------------------------


class _SlotContext:
    """What one slot can see and do (invariant 7); shared by `MarketContext` and `GroupContext`."""

    __slots__ = ("_slot", "_state", "_orders", "portfolio", "slot", "mode")

    def __init__(self, slot: Slot, state: State, orders: OrderView, portfolio: PortfolioView) -> None:
        self._slot, self._state, self._orders, self.portfolio = slot, state, orders, portfolio
        self.slot, self.mode = slot.id, slot.mode

    @property
    def now_ms(self) -> int:
        return self._state.now_ms

    @property
    def ready(self) -> bool:
        """Warm-up done and `Strategy.ready` true; until then risk rejects new orders (`warming`)."""
        return self._slot.ready

    @property
    def throttled(self) -> bool:
        """The order scheduler is holding writes for this slot."""
        return self._orders.throttled(self.slot)

    def input(self, name: str) -> Event | None:
        """Latest event from a declared input; None if none yet or older than `stale_input_ms`."""
        slot = self._slot
        if name not in slot.inputs:
            declared = ", ".join(slot.inputs) or "none"
            raise ValueError(f"{self.slot}: input {name!r} isn't declared (declared: {declared})")
        seen = self._state.inputs.get(name)
        if seen is None:
            return None
        recv_ms, ev = seen
        stale = slot.stale_input_ms
        return None if stale is not None and self._state.now_ms - recv_ms > stale else ev

    def log(self, text: str, level: Literal["info", "warn", "error"] = "info") -> None:
        self._slot.logs.append(LogEvent(level, self.slot, text, self._state.now_ms))

    def _advice(self, targets: Iterable[str], advisor: str | None) -> Advice | None:
        slot = self._slot
        if advisor is not None and advisor not in slot.advisors:
            listed = ", ".join(slot.advisors) or "none"
            raise ValueError(f"{self.slot}: advisor {advisor!r} isn't listed (listed: {listed})")
        now, advice = self._state.now_ms, self._state.advice
        best = None
        for name in slot.advisors if advisor is None else (advisor,):
            for target in targets:
                a = advice.get((name, target))
                if a is not None and a.expires_ms >= now and (best is None or a.ts_ms > best.ts_ms):
                    best = a
        return best

    def _next_schedule(self, targets: Iterable[str], kind: str | None) -> Schedule | None:
        now, schedules = self._state.now_ms, self._state.schedules
        best = None
        for target in targets:
            for s in schedules.get(target, ()):  # soonest first
                if s.at_ms >= now and (kind is None or s.kind == kind):
                    if best is None or s.at_ms < best.at_ms:
                        best = s
                    break
        return best


class MarketContext(_SlotContext):
    """A slot's view of one market. In a group slot, `GroupContext.market(t)` returns one per member,
    acting for the group's slot."""

    __slots__ = ("ticker",)

    def __init__(self, slot: Slot, ticker: MarketId, state: State, orders: OrderView, portfolio: PortfolioView) -> None:
        super().__init__(slot, state, orders, portfolio)
        self.ticker = ticker

    @property
    def contract(self) -> Contract:
        return self._state.contracts[self.ticker]

    @property
    def book(self) -> OrderBook | None:
        """None while stale or not yet snapshotted."""
        return self._state.book(self.ticker)

    @property
    def position(self) -> Qty:
        """This slot's signed YES position on this market."""
        return self._orders.position(self.slot, self.ticker)

    @property
    def working(self) -> list[WorkingOrder]:
        return self._orders.working(self.slot, self.ticker)

    def market(self, ticker: MarketId) -> MarketContext:
        if ticker != self.ticker:
            raise ValueError(f"{self.slot} doesn't trade {ticker}")
        return self

    def forecast(self, model: str | None = None) -> Forecast | None:
        """Latest forecast for this market; by default from the strategy's configured model."""
        model = model or self._slot.model
        return None if model is None else self._state.forecasts.get((model, self.ticker))

    def data(self) -> EntityState | None:
        return self._state.data(self.ticker)

    def advice(self, advisor: str | None = None) -> Advice | None:
        """Newest unexpired advice for this market or its entity, from one listed advisor or any."""
        return self._advice(self._targets(), advisor)

    def next_schedule(self, kind: str | None = None) -> Schedule | None:
        """Soonest upcoming `Schedule` for this market or its entity."""
        return self._next_schedule(self._targets(), kind)

    def _targets(self) -> tuple[str, ...]:
        entity = self._state.entity_of.get(self.ticker)
        return (self.ticker,) if entity is None else (self.ticker, entity)

    def quote(self, *, bid: tuple[Price, Qty] | None, ask: tuple[Price, Qty] | None) -> None:
        """Declare the resting orders wanted now; the order manager diffs against working orders."""
        self._check(bid, "bid")
        self._check(ask, "ask")
        if bid is not None and ask is not None and bid[0] >= ask[0]:
            raise ValueError(f"{self.slot}: quote crosses itself on {self.ticker}: bid {bid[0]} >= ask {ask[0]}")
        self._slot.intents.append(Quote(self.slot, self.ticker, bid, ask))

    def take(self, side: BookSide, limit: Price, qty: Qty) -> None:
        """Immediate-or-cancel: `side="bid"` buys YES at `limit` or lower."""
        if side not in ("bid", "ask"):
            raise ValueError(f"{self.slot}: side must be 'bid' or 'ask', got {side!r}")
        self._check((limit, qty), side)
        self._slot.intents.append(Take(self.slot, self.ticker, side, limit, qty))

    def cancel_all(self) -> None:
        self._slot.intents.append(CancelAll(self.slot, self.ticker))

    def target(self, qty: Qty, *, style: str = "passive", **opts: Any) -> None:
        """Declare the position wanted; an execution helper works it with quotes and takes."""
        if type(qty) is not int:
            raise ValueError(f"{self.slot}: qty is integer Qty (1e-2 contracts), got {qty!r}")
        self._slot.intents.append(Target(self.slot, self.ticker, qty, style, opts))

    def taker_fee(self, price: Price) -> Price:
        """Fee per contract for taking at `price` under the contract's schedule, rounded up."""
        return -(-self.contract.fees.fee(price, ONE_CONTRACT, taker=True) // 100)

    def maker_fee(self, price: Price) -> Price:
        return -(-self.contract.fees.fee(price, ONE_CONTRACT, taker=False) // 100)

    def _check(self, level: tuple[Price, Qty] | None, side: str) -> None:
        if level is None:
            return
        price, qty = level
        if type(price) is not int or type(qty) is not int:
            raise ValueError(f"{self.slot}: {side} must be integer (Price, Qty) units, got {level!r}")
        if qty <= 0:
            raise ValueError(f"{self.slot}: {side} qty must be positive, got {qty}")
        if not self.contract.grid.is_valid(price):
            raise ValueError(f"{self.slot}: {side} price {price} isn't on {self.ticker}'s price grid")


class GroupContext(_SlotContext):
    """A group slot's view (`slot_by = "event" | "entity" | {linker} | "all"`): every member market at
    once, for math across related contracts. Members join and leave as contracts come and go."""

    __slots__ = ("key", "_markets")

    def __init__(self, slot: Slot, state: State, orders: OrderView, portfolio: PortfolioView) -> None:
        super().__init__(slot, state, orders, portfolio)
        self.key = slot.key
        self._markets: dict[MarketId, MarketContext] = {}

    @property
    def tickers(self) -> list[MarketId]:
        return list(self._slot.tickers)

    @property
    def outcomes(self) -> Mapping[str, Mapping[MarketId, str | int]]:
        """The domain's mutually exclusive outcomes over these markets ({} if it declares none): {outcome: {ticker:
        "yes" | "no" | the YES settlement price when it's neither (an NFL tie: 5000 = $0.50)}}."""
        return self._slot.outcomes()

    def market(self, ticker: MarketId) -> MarketContext:
        """A member market's view (contract, book, position, working, forecast, data, quote, take…).
        Former members stay readable so late fills on them can be handled."""
        m = self._markets.get(ticker)
        if m is None:
            if ticker not in self._slot.tickers:
                raise ValueError(f"{self.slot} doesn't trade {ticker}")
            m = self._markets[ticker] = MarketContext(self._slot, ticker, self._state, self._orders, self.portfolio)
        return m

    def quote(self, ticker: MarketId, *, bid: tuple[Price, Qty] | None, ask: tuple[Price, Qty] | None) -> None:
        self.market(ticker).quote(bid=bid, ask=ask)

    def take(self, ticker: MarketId, side: BookSide, limit: Price, qty: Qty) -> None:
        self.market(ticker).take(side, limit, qty)

    def cancel_all(self, ticker: MarketId | None = None) -> None:
        if ticker is None:
            self._slot.intents.append(CancelAll(self.slot))
        else:
            self.market(ticker).cancel_all()

    def target(self, ticker: MarketId, qty: Qty, *, style: str = "passive", **opts: Any) -> None:
        self.market(ticker).target(qty, style=style, **opts)

    def advice(self, advisor: str | None = None) -> Advice | None:
        """Newest unexpired advice for any member market or its entity."""
        return self._advice(self._targets(), advisor)

    def next_schedule(self, kind: str | None = None) -> Schedule | None:
        return self._next_schedule(self._targets(), kind)

    def _targets(self) -> list[str]:
        entity_of = self._state.entity_of
        out = list(self._slot.tickers)
        out += dict.fromkeys(e for t in self._slot.tickers if (e := entity_of.get(t)) is not None)
        return out
