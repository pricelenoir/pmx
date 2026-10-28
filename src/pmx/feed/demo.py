"""`pmx serve --demo`: an invented engine session, so the dashboard can be seen and
developed before the real engine runs. Every message is real protocol through the real
`Publisher`; every number is made up. Tickers start with `DEMO-` and the hello says
`source="demo"`, which the TUI shows as a badge on every view.

Four unrelated underlyings show that nothing downstream is domain-specific: a sports
match, a temperature bracket set, an economic print, and a crypto price threshold."""

from __future__ import annotations

import asyncio
import math
import random
import time
from datetime import datetime
from typing import Any

import msgspec

from pmx.core.book import OrderBook
from pmx.core.fees import KALSHI_DEFAULT_FEES
from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, Trade
from pmx.core.units import ONE_DOLLAR
from pmx.engine.events import (
    Advice,
    Exposure,
    Forecast,
    Halt,
    LimitUsage,
    LogEvent,
    OrderUpdate,
    PortfolioSnapshot,
    RiskReject,
    ScopeStats,
    SlotFill,
    SlotStatus,
    SourceStatus,
    Timer,
)
from pmx.feed import proto
from pmx.feed.hub import FeedHub
from pmx.feed.publisher import Publisher

TICK = 100
LEVELS = 12
DAY_MS = 86_400_000
VENUE = "kalshi"


def _phi(z: float) -> float:
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def _sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


class DemoEntity(msgspec.Struct, frozen=True):
    key: str
    kind: str
    text: str
    fields: list[tuple[str, str]]
    ts_ms: int

    @property
    def entity(self) -> str:
        return self.key

    def summary(self) -> str:
        return self.text

    def display_fields(self) -> list[tuple[str, str]]:
        return self.fields


class _Underlying:
    """One outside thing: its contracts, mutually exclusive outcomes (outcome -> tickers that
    settle YES), and made-up dynamics."""

    key: str
    kind: str
    contracts: list[Contract]
    outcomes: dict[str, set[str]]

    def step(self, rng: random.Random, dt: float) -> None: ...

    def probs(self) -> dict[str, float]: ...

    def describe(self, now_ms: int) -> DemoEntity: ...

    def outcome_probs(self, p: dict[str, float]) -> dict[str, float]:
        """Outcome probabilities implied by per-contract P(YES) — exclusive contracts by default."""
        out = {o: sum(p.get(t, 0.0) for t in yes) for o, yes in self.outcomes.items() if yes}
        rest = [o for o, yes in self.outcomes.items() if not yes]
        left = max(0.0, 1 - sum(out.values()))
        out |= {o: left / len(rest) for o in rest}
        total = sum(out.values()) or 1.0
        return {o: v / total for o, v in out.items()}


def _contract(ticker: str, event: str, title: str, sub: str, now_ms: int, ends_in_ms: int) -> Contract:
    return Contract(VENUE, ticker, event, title, sub, "open", now_ms + ends_in_ms + DAY_MS,
                    series_ticker=event.rsplit("-", 1)[0], event_ts_ms=now_ms + ends_in_ms)


class _Match(_Underlying):
    kind = "demo_sports"
    TIE = 0.004
    PLAYS = ("pass complete for 12", "run for 3", "incomplete pass", "sack, loss of 7", "punt, fair catch",
             "pass complete for 31", "run for no gain", "penalty, 5 yards")

    def __init__(self, rng: random.Random, now_ms: int) -> None:
        self.away, self.home = "HOU", "JAC"
        event = "DEMO-NFL-26OCT18HOUJAC"
        self.key = "demo:nfl:HOU-JAC"
        self.contracts = [
            _contract(f"{event}-HOU", event, "Houston at Jacksonville: Houston wins", "Houston", now_ms, 5_400_000),
            _contract(f"{event}-JAC", event, "Houston at Jacksonville: Jacksonville wins", "Jacksonville", now_ms,
                      5_400_000),
        ]
        self.outcomes = {self.away: {self.contracts[0].ticker}, self.home: {self.contracts[1].ticker}, "tie": set()}
        self.rng = rng
        self._kickoff()
        self.period, self.clock = 2, 431.0
        self.score = {self.away: 10, self.home: 7}

    def _kickoff(self) -> None:
        self.x = self.rng.gauss(0, 0.5)
        self.period, self.clock = 1, 900.0
        self.score = {self.away: 0, self.home: 0}
        self.ball, self.down, self.dist, self.play = self.home, 1, 10, "kickoff, touchback"

    def step(self, rng: random.Random, dt: float) -> None:
        self.clock -= dt
        if self.clock <= 0:
            self.period += 1
            self.clock = 900.0
            if self.period > 4:
                self._kickoff()
        self.x += rng.gauss(0, 0.012 * math.sqrt(dt))
        if rng.random() < dt / 9:
            self.play = rng.choice(self.PLAYS)
            self.down = 1 if self.down >= 4 or rng.random() < 0.3 else self.down + 1
            self.dist = 10 if self.down == 1 else rng.randint(1, 12)
            if rng.random() < 0.15:
                self.ball = self.away if self.ball == self.home else self.home
        if rng.random() < dt / 80:
            team = self.home if rng.random() < _sigmoid(self.x) else self.away
            pts = rng.choice((3, 7, 7))
            self.score[team] += pts
            self.play = f"{team} {'field goal' if pts == 3 else 'touchdown'}"

    def probs(self) -> dict[str, float]:
        left = ((4 - self.period) * 900 + self.clock) / 3600
        z = self.x + 0.16 * (self.score[self.home] - self.score[self.away]) / max(left, 0.05) ** 0.5
        ph = min(max(_sigmoid(z), 0.01), 0.99) * (1 - self.TIE)
        return {self.contracts[0].ticker: 1 - self.TIE - ph, self.contracts[1].ticker: ph}

    def describe(self, now_ms: int) -> DemoEntity:
        m, s = divmod(int(self.clock), 60)
        q = f"Q{self.period} {m}:{s:02d}"
        a, h = self.score[self.away], self.score[self.home]
        nth = ("st", "nd", "rd", "th")[self.down - 1]
        return DemoEntity(self.key, self.kind, f"{q} {self.away} {a}-{h} {self.home}", [
            ("clock", q), ("score", f"{self.away} {a} - {h} {self.home}"),
            ("possession", f"{self.ball}, {self.down}{nth} & {self.dist}"), ("last play", self.play),
        ], now_ms)


class _Temperature(_Underlying):
    kind = "demo_weather"
    BRACKETS = ((None, 65, "B65", "65° or below"), (66, 67, "B66", "66° to 67°"), (68, 69, "B68", "68° to 69°"),
                (70, None, "T70", "70° or above"))

    def __init__(self, rng: random.Random, now_ms: int) -> None:
        event = "DEMO-HIGHNY-26OCT18"
        self.key = "wx:KNYC:2026-10-18"
        self.contracts = [_contract(f"{event}-{code}", event, f"NYC high temperature Oct 18: {sub}", sub, now_ms,
                                    14_400_000) for _, _, code, sub in self.BRACKETS]
        self.outcomes = {c.subtitle: {c.ticker} for c in self.contracts}
        self.mu, self.sigma, self.now, self.high = 68.2, 1.6, 63.0, 64.0

    def step(self, rng: random.Random, dt: float) -> None:
        self.mu += rng.gauss(0, 0.01 * math.sqrt(dt))
        self.now += (self.mu - 1.5 - self.now) * 0.0008 * dt + rng.gauss(0, 0.02 * math.sqrt(dt))
        self.high = max(self.high, self.now)

    def _cdf(self, x: float) -> float:  # CDF of the day's high: at least what's been seen
        return 0.0 if x < self.high else _phi((x - self.mu) / self.sigma)

    def probs(self) -> dict[str, float]:
        out = {}
        for c, (lo, hi, _, _) in zip(self.contracts, self.BRACKETS, strict=True):
            top = 1.0 if hi is None else self._cdf(hi + 0.5)
            bottom = 0.0 if lo is None else self._cdf(lo - 0.5)
            out[c.ticker] = min(max(top - bottom, 0.01), 0.99)
        return out

    def describe(self, now_ms: int) -> DemoEntity:
        return DemoEntity(self.key, self.kind, f"KNYC {self.now:.0f}°F · high so far {self.high:.0f}°F", [
            ("now", f"{self.now:.1f}°F"), ("high so far", f"{self.high:.1f}°F"),
            ("forecast high", f"{self.mu:.1f}°F ± {self.sigma:.1f}"), ("settles", "NWS climate report"),
        ], now_ms)


class _Print(_Underlying):
    kind = "demo_econ"

    def __init__(self, rng: random.Random, now_ms: int) -> None:
        event = "DEMO-CPI-26NOV"
        self.key = "econ:CPI:2026-10"
        self.contracts = [
            _contract(f"{event}-T0.2", event, "CPI m/m for October above 0.2%", "above 0.2%", now_ms, 23 * DAY_MS),
            _contract(f"{event}-T0.3", event, "CPI m/m for October above 0.3%", "above 0.3%", now_ms, 23 * DAY_MS),
        ]
        a2, a3 = (c.ticker for c in self.contracts)
        self.outcomes = {"≤0.2%": set(), "0.3%": {a2}, "≥0.4%": {a2, a3}}
        self.mu, self.sigma = 0.27, 0.07

    def step(self, rng: random.Random, dt: float) -> None:
        self.mu += rng.gauss(0, 0.0004 * math.sqrt(dt))

    def probs(self) -> dict[str, float]:
        a2, a3 = (c.ticker for c in self.contracts)
        return {a2: 1 - _phi((0.25 - self.mu) / self.sigma), a3: 1 - _phi((0.35 - self.mu) / self.sigma)}

    def outcome_probs(self, p: dict[str, float]) -> dict[str, float]:
        a2, a3 = (min(max(p.get(c.ticker, 0.5), 0.0), 1.0) for c in self.contracts)
        a3 = min(a3, a2)
        return {"≤0.2%": 1 - a2, "0.3%": a2 - a3, "≥0.4%": a3}

    def describe(self, now_ms: int) -> DemoEntity:
        return DemoEntity(self.key, self.kind, f"CPI Oct m/m · nowcast +{self.mu:.2f}% · release Nov 13", [
            ("nowcast", f"+{self.mu:.2f}% ± {self.sigma:.2f}"), ("consensus", "+0.3%"),
            ("release", "Nov 13 08:30 ET"),
        ], now_ms)


class _Threshold(_Underlying):
    kind = "demo_crypto"
    STRIKE, VOL, HORIZON_S = 70_000.0, 0.55, 3 * 3600.0

    def __init__(self, rng: random.Random, now_ms: int) -> None:
        event = "DEMO-BTC-26OCT1817"
        self.key = "crypto:BTC:2026-10-18T17"
        self.contracts = [_contract(f"{event}-T70000", event, "Bitcoin above $70,000 at 5pm ET", "above $70,000",
                                    now_ms, int(self.HORIZON_S * 1000))]
        self.outcomes = {"yes": {self.contracts[0].ticker}, "no": set()}
        self.spot, self.left = 70_350.0, self.HORIZON_S

    def step(self, rng: random.Random, dt: float) -> None:
        self.spot *= math.exp(rng.gauss(0, self.VOL * math.sqrt(dt / 31_536_000)))
        self.left -= dt
        if self.left < 600:
            self.left = self.HORIZON_S

    def probs(self) -> dict[str, float]:
        sd = self.VOL * math.sqrt(self.left / 31_536_000)
        return {self.contracts[0].ticker: min(max(_phi(math.log(self.spot / self.STRIKE) / sd), 0.01), 0.99)}

    def describe(self, now_ms: int) -> DemoEntity:
        text = f"BTC ${self.spot:,.0f} · strike $70,000 · {self.left / 3600:.1f}h left"
        return DemoEntity(self.key, self.kind, text, [
            ("spot", f"${self.spot:,.0f}"), ("strike", "$70,000"), ("time left", f"{self.left / 60:.0f} min"),
        ], now_ms)


class _Market:
    def __init__(self, contract: Contract, under: _Underlying) -> None:
        self.c, self.under = contract, under
        self.book = OrderBook(contract.ticker)
        self.fair = 0.5
        self.spread = 2  # ticks
        self.ema: float | None = None
        self.model = 0.5  # the "fair" model's noisy, lagged view
        self.last: Trade | None = None


# strategy -> (mode, order size in Qty, per-slot day loss limit in micros)
STRATEGIES = {"mm": ("paper", 1_000, 50_000_000), "edge": ("paper", 500, 40_000_000), "smoke": ("live", 100, 5_000_000)}


class _Slot:
    def __init__(self, strategy: str, m: _Market) -> None:
        self.id, self.strategy, self.m = f"{strategy}/{m.c.ticker}", strategy, m
        self.mode, self.size, self.max_loss = STRATEGIES[strategy]
        self.q = 0
        self.avg = 0.0
        self.realized = self.fees = 0
        self.orders: dict[str, OrderUpdate] = {}
        self.state = "active"
        self.last_action = "started"
        self.cooldown_ms = 0

    def mid(self) -> float:
        return self.m.book.mid() or self.m.fair * ONE_DOLLAR

    def unrealized(self) -> int:
        return round(self.q * (self.mid() - self.avg))

    def pnl(self) -> int:
        return self.realized + self.unrealized() - self.fees

    def fill(self, side: str, price: int, qty: int, fee: int) -> None:
        dq = qty if side == "bid" else -qty
        if self.q == 0 or (self.q > 0) == (dq > 0):
            self.avg = (self.avg * abs(self.q) + price * qty) / (abs(self.q) + qty)
        else:
            closed = min(abs(self.q), qty)
            self.realized += round((price - self.avg) * closed * (1 if self.q > 0 else -1))
            if qty > closed:
                self.avg = price
        self.q += dq
        if self.q == 0:
            self.avg = 0.0
        self.fees += fee

    def capital_at_risk(self) -> int:
        mid = self.mid()
        return round(self.q * mid if self.q > 0 else -self.q * (ONE_DOLLAR - mid))

    def working_exposure(self) -> int:
        return sum((o.qty - o.filled) * (o.price if o.side == "bid" else ONE_DOLLAR - o.price)
                   for o in self.orders.values())


class DemoEngine:
    """Steps the invented world at `step(now_ms)` and publishes what an engine would, to a `Publisher`
    (or a `FeedHub`, wrapped in one) in this process, or a `TapPublisher` for the feed process."""

    def __init__(self, out: FeedHub | Any, *, seed: int = 7, now_ms: int | None = None) -> None:
        now_ms = now_ms or int(time.time() * 1000)
        self.rng = random.Random(seed)
        self.unders: list[_Underlying] = [cls(self.rng, now_ms) for cls in (_Match, _Temperature, _Print, _Threshold)]
        self.markets = {c.ticker: _Market(c, u) for u in self.unders for c in u.contracts}
        self.pub = Publisher(out) if isinstance(out, FeedHub) else out
        self.forecasts: dict[tuple[str, str], Forecast] = {}  # latest published, for `market_snapshot`
        t = {m.c.subtitle: m for m in self.markets.values()}
        self.slots = [
            *(_Slot("mm", t[k]) for k in ("Houston", "Jacksonville", "68° to 69°", "above $70,000")),
            *(_Slot("edge", t[k]) for k in ("above 0.2%", "above 0.3%", "70° or above", "66° to 67°")),
            _Slot("smoke", t["above $70,000"]),
        ]
        self.n = self.n_trades = self.n_orders = 0
        self.last_ms = now_ms
        self.start_ms = now_ms
        self.base: dict[tuple[str, str], int] = {}
        self.hwm: dict[tuple[str, str], int] = {}
        self.day_open: dict[tuple[str, str], int] = {}
        self.daily: dict[str, list[tuple[int, int]]] = {}
        self._seed_history(now_ms)
        self.pub.set_models(["fair", "mid", "ema"])
        self.pub.set_status(True, "demo market data (invented)", ts_ms=now_ms)
        for src in ("scores", "weather", "prices"):
            self.pub.set_status(True, "demo source (invented)", src, now_ms)
        for m in self.markets.values():
            self.pub.on_event(m.c, now_ms)
            m.fair = m.under.probs()[m.c.ticker]
            m.model = m.fair
            self._requote_book(m, now_ms, snapshot=True)
        for u in self.unders:
            self._entity(u, now_ms)
        self._log("info", "demo", "demo engine started: every number on screen is invented", now_ms)

    def _entity(self, u: _Underlying, now_ms: int) -> None:
        self.pub.on_event(proto.EntityMsg.of(u.describe(now_ms), [c.ticker for c in u.contracts]), now_ms)

    def market_snapshot(self) -> list[object]:
        """Books, last prints, and forecasts now — what a feed process resync needs (`EngineLink`)."""
        out: list[object] = [BookSnapshot(VENUE, t, tuple(m.book.levels("bid")), tuple(m.book.levels("ask")),
                                          m.book.ts_ms) for t, m in self.markets.items()]
        out += [m.last for m in self.markets.values() if m.last is not None]
        return out + list(self.forecasts.values())

    # ---- history (what the ledger would hold) ---------------------------------

    def _seed_history(self, now_ms: int) -> None:
        drift = {"mm": (2_500_000, 7_000_000), "edge": (-600_000, 9_000_000), "smoke": (-20_000, 60_000)}
        midnight = int(datetime.fromtimestamp(now_ms / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
                       .timestamp() * 1000)
        start = midnight - 30 * DAY_MS
        hours = (now_ms - start) // 3_600_000
        curves: dict[tuple[str, str], list[tuple[int, int]]] = {}
        for sid, (mean, sd) in drift.items():
            mode = STRATEGIES[sid][0]
            eq, pts, by_day = 0, [], {}
            for h in range(hours + 1):
                ts = start + h * 3_600_000
                step = round(self.rng.gauss(mean / 24, sd / math.sqrt(24)))
                eq += step
                pts.append((ts, eq))
                day = start + (ts - start) // DAY_MS * DAY_MS
                by_day[day] = by_day.get(day, 0) + step
            curves[(f"strategy:{sid}", mode)] = pts
            self.daily[sid] = sorted(by_day.items())
            self.day_open[(f"strategy:{sid}", mode)] = next(e for t, e in reversed(pts) if t <= midnight)
        for mode in ("paper", "live"):
            series = [c for (scope, m), c in curves.items() if m == mode]
            curves[("portfolio", mode)] = [(ts, sum(c[i][1] for c in series)) for i, (ts, _) in enumerate(series[0])]
            self.day_open[("portfolio", mode)] = next(e for t, e in reversed(curves[("portfolio", mode)])
                                                      if t <= midnight)
        for (scope, mode), pts in curves.items():
            hwm, out = 0, []
            for ts, e in pts:
                hwm = max(hwm, e)
                out.append((ts, e, hwm - e))
            self.base[(scope, mode)] = pts[-1][1]
            self.hwm[(scope, mode)] = hwm
            self.pub.seed_equity(scope, mode, out)

    # ---- the loop ---------------------------------------------------------------

    def step(self, now_ms: int) -> None:
        dt = max(0.0, (now_ms - self.last_ms) / 1000)
        self.last_ms = now_ms
        self.n += 1
        rng, pub = self.rng, self.pub
        for u in self.unders:
            u.step(rng, dt)
            for t, p in u.probs().items():
                self.markets[t].fair = p
        for m in self.markets.values():
            self._requote_book(m, now_ms, snapshot=self.n % 600 == 0)
            if rng.random() < 0.08:
                self._trade(m, now_ms)
            mid = m.book.mid()
            if mid is not None:
                m.ema = mid if m.ema is None else m.ema + (mid - m.ema) * min(1.0, dt / 60)
            m.model += (m.fair - m.model) * min(1.0, dt / 20) + rng.gauss(0, 0.003)
            m.model = min(max(m.model, 0.01), 0.99)
            data_ts = now_ms - 2_000
            self._forecast(Forecast("fair", m.c.ticker, round(m.model, 4), now_ms, data_ts), now_ms)
            if mid is not None:
                self._forecast(Forecast("mid", m.c.ticker, mid / ONE_DOLLAR, now_ms, now_ms), now_ms)
                self._forecast(Forecast("ema", m.c.ticker, round(m.ema / ONE_DOLLAR, 4), now_ms, now_ms), now_ms)
        for s in self.slots:
            self._run_slot(s, now_ms)
        if self.n % 10 == 0:
            for u in self.unders:
                self._entity(u, now_ms)
            for s in self.slots:
                pub.on_event(self._slot_status(s, now_ms), now_ms)
            pub.on_event(self._portfolio(now_ms), now_ms)
        if self.n % 50 == 0:
            for sid in STRATEGIES:
                for rng_name in ("today", "7d", "30d", "all"):
                    pub.publish_perf(self._perf(sid, rng_name, now_ms))
        if self.n % 200 == 20:  # an invented advisor's word now and then
            m = rng.choice(list(self.markets.values()))
            p = min(max(m.fair + rng.gauss(0, 0.04), 0.02), 0.98)
            mid = (m.book.mid() or 5_000) / ONE_DOLLAR
            pub.on_event(Advice("news", m.c.ticker, round(p, 3), "buy" if p > mid else "sell",
                                round(rng.uniform(0.5, 0.9), 2), now_ms + 60_000, "invented headline (demo)", now_ms),
                         now_ms)
        if self.n % 900 == 450:
            pub.on_event(SourceStatus("weather", False, "demo outage: no observation for 30 s", now_ms), now_ms)
            self._log("warn", "weather", "source disconnected (demo)", now_ms)
        if self.n % 900 == 750:
            pub.on_event(SourceStatus("weather", True, "demo source (invented)", now_ms), now_ms)
        pub.on_event(Timer(now_ms), now_ms)

    async def run(self, stop: asyncio.Event, *, hz: float = 10.0) -> None:
        while not stop.is_set():
            self.step(int(time.time() * 1000))
            try:
                await asyncio.wait_for(stop.wait(), 1 / hz)
            except TimeoutError:
                pass

    # ---- market data ------------------------------------------------------------

    def _forecast(self, f: Forecast, now_ms: int) -> None:
        self.forecasts[(f.model, f.ticker)] = f
        self.pub.on_event(f, now_ms)

    def _size(self, depth: int) -> int:
        return max(1, int(self.rng.lognormvariate(math.log(120 * (1 + 0.45 * depth)), 0.6))) * 100

    def _requote_book(self, m: _Market, now_ms: int, *, snapshot: bool = False) -> None:
        rng = self.rng
        if rng.random() < 0.01:
            m.spread = rng.choice((1, 2, 2, 3))
        fair = m.fair * ONE_DOLLAR
        bb = int((fair - m.spread * TICK / 2) // TICK) * TICK
        bb = min(max(bb, TICK), ONE_DOLLAR - (m.spread + 1) * TICK)
        ba = bb + m.spread * TICK
        want = {
            "bid": [bb - i * TICK for i in range(LEVELS) if bb - i * TICK >= TICK],
            "ask": [ba + i * TICK for i in range(LEVELS) if ba + i * TICK <= ONE_DOLLAR - TICK],
        }
        target: dict[str, dict[int, int]] = {}
        for side, prices in want.items():
            cur = m.book.bids if side == "bid" else m.book.asks
            lv = {}
            for i, p in enumerate(prices):
                q = cur.get(p)
                if q is None or rng.random() < 0.03:
                    q = self._size(i)
                lv[p] = q
            target[side] = lv
        if snapshot or not (m.book.bids or m.book.asks):
            ev = BookSnapshot(VENUE, m.c.ticker, tuple(sorted(target["bid"].items(), reverse=True)),
                              tuple(sorted(target["ask"].items())), now_ms)
            m.book.apply(ev)
            self.pub.on_event(ev, now_ms)
            return
        for side, lv in target.items():
            cur = dict(m.book.bids if side == "bid" else m.book.asks)
            for p in sorted(cur.keys() | lv.keys()):
                d = lv.get(p, 0) - cur.get(p, 0)
                if d:
                    ev = BookDelta(VENUE, m.c.ticker, side, p, d, now_ms)
                    m.book.apply(ev)
                    self.pub.on_event(ev, now_ms)

    def _trade(self, m: _Market, now_ms: int) -> None:
        mid = m.book.mid()
        if mid is None:
            return
        buy = self.rng.random() < min(max(0.5 + (m.fair * ONE_DOLLAR - mid) / 400, 0.1), 0.9)
        side, touch = ("bid", m.book.best_ask()) if buy else ("ask", m.book.best_bid())
        if touch is None:
            return
        price, avail = touch
        qty = min(avail, max(1, int(self.rng.lognormvariate(math.log(40), 0.9))) * 100)
        self.n_trades += 1
        trade = m.last = Trade(VENUE, f"demo-{self.n_trades}", m.c.ticker, price, qty, side, now_ms)
        self.pub.on_event(trade, now_ms)
        delta = BookDelta(VENUE, m.c.ticker, "ask" if buy else "bid", price, -qty, now_ms)
        m.book.apply(delta)
        self.pub.on_event(delta, now_ms)
        for s in self.slots:
            if s.m is m:
                self._paper_fills(s, trade, now_ms)

    # ---- strategies ---------------------------------------------------------------

    def _order(self, s: _Slot, side: str, price: int, qty: int, now_ms: int, status: str = "open") -> OrderUpdate:
        self.n_orders += 1
        o = OrderUpdate(s.id, s.mode, f"demo-c{self.n_orders}", f"demo-o{self.n_orders}", s.m.c.ticker, side, price,
                        qty, 0, status, now_ms)
        if status == "open":
            s.orders[o.client_order_id] = o
        self.pub.on_event(o, now_ms)
        return o

    def _cancel_all(self, s: _Slot, now_ms: int) -> None:
        for o in list(s.orders.values()):
            self.pub.on_event(msgspec.structs.replace(o, status="canceled", ts_ms=now_ms), now_ms)
        s.orders.clear()

    def _book_fill(self, s: _Slot, o: OrderUpdate, qty: int, now_ms: int, *, taker: bool) -> None:
        fee = KALSHI_DEFAULT_FEES.fee(o.price, qty, taker=taker)
        s.fill(o.side, o.price, qty, fee)
        f = Fill(VENUE, f"demo-f{now_ms}-{o.client_order_id}", o.order_id or "", o.ticker, o.side, o.price, qty, fee,
                 taker, now_ms, o.client_order_id)
        self.pub.on_event(SlotFill(s.id, s.mode, f), now_ms)
        done = o.filled + qty >= o.qty
        upd = msgspec.structs.replace(o, filled=o.filled + qty, status="filled" if done else "open", ts_ms=now_ms)
        if done:
            s.orders.pop(o.client_order_id, None)
        else:
            s.orders[o.client_order_id] = upd
        self.pub.on_event(upd, now_ms)
        verb = "bought" if o.side == "bid" else "sold"
        s.last_action = f"{verb} {qty // 100} @ {o.price // 100}¢"

    def _paper_fills(self, s: _Slot, t: Trade, now_ms: int) -> None:
        """A resting order fills only when a print trades at or through its price."""
        for o in list(s.orders.values()):
            through = (o.side == "bid" and t.taker_side == "ask" and t.price <= o.price) or (
                o.side == "ask" and t.taker_side == "bid" and t.price >= o.price)
            if through and self.rng.random() < 0.5:
                self._book_fill(s, o, min(o.qty - o.filled, t.qty), now_ms, taker=False)

    def _run_slot(self, s: _Slot, now_ms: int) -> None:
        if s.state != "active":
            return
        if s.pnl() <= -s.max_loss:
            s.state, s.last_action = "halted", "halted: max_loss"
            self._cancel_all(s, now_ms)
            self.pub.on_event(Halt(s.id, "max_loss", True, now_ms), now_ms)
            self._log("error", s.id, f"slot halted: day loss reached ${s.max_loss / 1e6:.2f}", now_ms)
            return
        book = s.m.book
        bb, ba = book.best_bid(), book.best_ask()
        if bb is None or ba is None:
            return
        if s.strategy == "mm":
            want = {}
            if s.q < 3 * s.size:
                want["bid"] = bb[0]
            if s.q > -3 * s.size:
                want["ask"] = ba[0]
            for o in list(s.orders.values()):
                if want.get(o.side) != o.price:
                    s.orders.pop(o.client_order_id)
                    self.pub.on_event(msgspec.structs.replace(o, status="canceled", ts_ms=now_ms), now_ms)
            for side, price in want.items():
                if not any(o.side == side for o in s.orders.values()):
                    self._order(s, side, price, s.size, now_ms)
                    s.last_action = f"quote {bb[0] // 100}¢ / {ba[0] // 100}¢"
        elif s.strategy == "edge":
            if now_ms < s.cooldown_ms:
                return
            fair = s.m.model * ONE_DOLLAR
            side = "bid" if fair - ba[0] > 300 and s.q < 10 * s.size else (
                "ask" if bb[0] - fair > 300 and s.q > -10 * s.size else None)
            if side is not None:
                px, avail = ba if side == "bid" else bb
                o = self._order(s, side, px, min(s.size, avail), now_ms, status="pending")
                self._book_fill(s, o, min(s.size, avail), now_ms, taker=True)
                s.cooldown_ms = now_ms + 15_000
        elif s.strategy == "smoke" and self.n % 300 == 0:
            if self.n % 1500 == 0:
                self.pub.on_event(RiskReject(s.id, s.m.c.ticker, "bid", bb[0], 5_000, "max_order_qty",
                                             "50 contracts > max_order_qty 1", now_ms), now_ms)
                self._log("warn", s.id, "risk rejected scripted order: max_order_qty", now_ms)
            elif s.orders:
                self._cancel_all(s, now_ms)
                s.last_action = "canceled scripted bid"
            else:
                self._order(s, "bid", max(TICK, bb[0] - 3 * TICK), s.size, now_ms)
                s.last_action = "placed scripted bid"

    def _slot_status(self, s: _Slot, now_ms: int) -> SlotStatus:
        return SlotStatus(s.id, s.strategy, s.m.c.ticker, s.mode, s.state, s.q, s.realized, s.unrealized(), s.fees,
                          len(s.orders), s.last_action, now_ms)

    # ---- portfolio ------------------------------------------------------------------

    def _scope(self, scope: str, mode: str, slots: list[_Slot], now_ms: int) -> ScopeStats:
        key = (scope, mode)
        realized = sum(s.realized for s in slots)
        unreal = sum(s.unrealized() for s in slots)
        fees = sum(s.fees for s in slots)
        equity = self.base.get(key, 0) + realized + unreal - fees
        self.hwm[key] = hwm = max(self.hwm.get(key, 0), equity)
        exps = self._exposures(slots, mode)
        return ScopeStats(
            scope, mode, realized, unreal, fees, equity, equity - self.day_open.get(key, 0), hwm, hwm - equity,
            sum(s.capital_at_risk() for s in slots), sum(e.worst_case for e in exps),
            sum(e.ev_model or 0 for e in exps), None,
            sum(round(abs(s.q) * s.mid()) for s in slots), sum(s.working_exposure() for s in slots),
            sum(1 for s in slots if s.q))

    def _exposures(self, slots: list[_Slot], mode: str) -> list[Exposure]:
        out = []
        for u in self.unders:
            mine = [s for s in slots if s.m.under is u and s.mode == mode and (s.q or s.realized or s.fees)]
            if not mine:
                continue
            scen = {o: sum(s.realized - s.fees + round(s.q * ((ONE_DOLLAR if s.m.c.ticker in yes else 0) - s.avg))
                           for s in mine) for o, yes in u.outcomes.items()}
            p_model = u.outcome_probs({t: self.markets[t].model for t in self.markets if self.markets[t].under is u})
            p_mkt = u.outcome_probs({t: (m.book.mid() or m.fair * ONE_DOLLAR) / ONE_DOLLAR
                                     for t, m in self.markets.items() if m.under is u})
            out.append(Exposure(
                u.key, mode, sorted({s.m.c.ticker for s in mine}), sorted({s.strategy for s in mine}),
                sum(s.capital_at_risk() for s in mine), scen, min(scen.values()),
                round(sum(p_model[o] * v for o, v in scen.items())), round(sum(p_mkt[o] * v for o, v in scen.items()))))
        return sorted(out, key=lambda e: e.worst_case)

    def _portfolio(self, now_ms: int) -> PortfolioSnapshot:
        scopes, limits = [], []
        for mode in ("paper", "live"):
            mine = [s for s in self.slots if s.mode == mode]
            port = self._scope("portfolio", mode, mine, now_ms)
            scopes.append(port)
            for sid, (smode, _, max_loss) in STRATEGIES.items():
                if smode != mode:
                    continue
                st = self._scope(f"strategy:{sid}", mode, [s for s in mine if s.strategy == sid], now_ms)
                scopes.append(st)
                limits.append(LimitUsage(f"strategy:{sid}", "max_loss", max(0, -st.today), max_loss,
                                         -st.today >= max_loss))
            if mode == "paper":
                ent = max((e.capital_at_risk for e in self._exposures(mine, mode)), default=0)
                limits += [
                    LimitUsage("global", "max_capital_at_risk", port.capital_at_risk + port.working_exposure,
                               500_000_000, port.capital_at_risk + port.working_exposure >= 500_000_000),
                    LimitUsage("global", "max_worst_case", max(0, -port.worst_case), 300_000_000,
                               -port.worst_case >= 300_000_000),
                    LimitUsage("global", "max_entity_risk", ent, 150_000_000, ent >= 150_000_000),
                    LimitUsage("global", "max_drawdown", port.drawdown, 200_000_000, port.drawdown >= 200_000_000),
                ]
        exposures = [e for mode in ("paper", "live") for e in self._exposures(self.slots, mode)]
        return PortfolioSnapshot(now_ms, scopes, exposures, limits)

    # ---- performance (what analytics would compute from the ledger) ----------------

    def _perf(self, sid: str, range_name: str, now_ms: int) -> proto.PerfMsg:
        mode = STRATEGIES[sid][0]
        slots = [s for s in self.slots if s.strategy == sid]
        today = sum(s.pnl() for s in slots) + self.daily[sid][-1][1]
        days = [*self.daily[sid][:-1], (self.daily[sid][-1][0], today)]
        days = {"today": days[-1:], "7d": days[-7:], "30d": days[-30:], "all": days}[range_name]
        pnl = [v for _, v in days]
        n = len(pnl)
        net = sum(pnl)
        per_day = {"mm": 40_000, "edge": 6_000, "smoke": 100}[sid]  # contracts (Qty) per day
        contracts = per_day * n
        fees = round(contracts * 0.44 * 100)  # ~0.44¢ per contract
        mean = net / n
        sd = (sum((v - mean) ** 2 for v in pnl) / (n - 1)) ** 0.5 if n > 1 else None
        cpc = net / 10_000 / (contracts / 100) if contracts else None
        stats = proto.PerfStats(
            net=net, gross=net + fees, fees=fees, realized=net - sum(s.unrealized() for s in slots),
            open=sum(s.unrealized() for s in slots), orders=n * per_day // 40, fills=n * per_day // 200,
            contracts=contracts, maker_share=1.0 if sid != "edge" else 0.0, fill_rate=0.31 if sid == "mm" else None,
            episodes=n * 9, markets=len(slots), entities=len({s.m.under.key for s in slots}),
            win_rate=sum(v > 0 for v in pnl) / n, days=n, pct_days_positive=sum(v > 0 for v in pnl) / n,
            daily=days, expectancy_c=cpc, max_capital_at_risk=60_000_000,
            markouts_c={"10s": 0.21, "1m": 0.12, "5m": -0.05} if sid == "mm" else {"10s": -0.4, "1m": -0.9, "5m": -1.3},
            edge_entry_c=1.0 if sid == "mm" else 3.6, edge_realized_c=cpc,
            brier_model=0.2138 if sid == "edge" else None, brier_market=0.2121 if sid == "edge" else None)
        wins, losses = [v for v in pnl if v > 0], [v for v in pnl if v < 0]
        stats.avg_win = round(sum(wins) / len(wins)) if wins else None
        stats.avg_loss = round(sum(losses) / len(losses)) if losses else None
        stats.profit_factor = sum(wins) / -sum(losses) if losses else None
        if stats.edge_entry_c and cpc is not None:
            stats.capture = cpc / stats.edge_entry_c
        eq, hwm, dd, under, under_since = 0, 0, 0, 0, None
        for ts, v in days:
            eq += v
            hwm = max(hwm, eq)
            dd = max(dd, hwm - eq)
            if eq < hwm and under_since is None:
                under_since = ts
            elif eq >= hwm and under_since is not None:
                under, under_since = max(under, ts - under_since), None
        stats.max_drawdown, stats.time_under_water_ms = dd, max(under, now_ms - under_since if under_since else 0)
        stats.return_on_risk = net / stats.max_capital_at_risk
        if sd:
            half = 1.96 * sd * math.sqrt(n)
            stats.pnl_ci = (round(net - half), round(net + half))
            stats.expectancy_ci = (stats.pnl_ci[0] / 100 / contracts, stats.pnl_ci[1] / 100 / contracts)
            stats.sharpe_day = mean / sd
            stats.p_nonpositive = _phi(-net / (sd * math.sqrt(n)))
        verdict = self._verdict(stats, n, mean, sd)
        edge = [proto.BucketRow(k, f, f * 500, round(c * f * 50_000), c, e)  # 5 contracts per fill
                for k, f, c, e in (("2-4¢", 140, 0.4, 2.9), ("4-6¢", 60, 0.9, 4.8), ("6-10¢", 22, 0.3, 7.6),
                                   (">10¢", 6, -1.8, 13.1))]
        by_market = [proto.BucketRow(s.m.c.ticker, 0, abs(s.q), s.pnl(), None, None) for s in slots]
        by_day = [proto.BucketRow(datetime.fromtimestamp(ts / 1000).strftime("%a %m-%d"), 0, per_day, v,
                                  v / 10_000 / (per_day / 100), None) for ts, v in days[-10:]]
        third = max(1, contracts // 3)
        phase = [proto.BucketRow(k, 0, third, round(net * w), net * w / 100 / third, None)
                 for k, w in (("early", 0.5), ("middle", 0.3), ("late", 0.2))]
        return proto.PerfMsg(sid, mode, range_name, now_ms, stats,
                             {"edge": edge, "market": by_market, "phase": phase, "day": by_day}, verdict)

    @staticmethod
    def _verdict(st: proto.PerfStats, n: int, mean: float, sd: float | None) -> proto.Verdict:
        if st.expectancy_ci is None or sd is None:
            return proto.Verdict("inconclusive", f"{n} day of data: too little to say", [], "at least 2 days")
        lo, hi = st.expectancy_ci
        why = (f"{st.expectancy_c:+.2f}¢/contract, 95% CI [{lo:+.2f}, {hi:+.2f}], "
               f"{st.contracts // 100:,} contracts over {n} days")
        flags = []
        if st.net > 0 and st.markouts_c.get("5m", 0) < 0:
            flags.append("positive PnL with negative 5m markouts: may be luck")
        needed = ""
        if mean and lo <= 0 <= hi:
            more = max(0, math.ceil((1.96 * sd / abs(mean)) ** 2) - n)
            needed = f"~{more} more days at this mean and variance"
        verdict = "profitable" if lo > 0 else "unprofitable" if hi < 0 else "inconclusive"
        return proto.Verdict(verdict, why, flags, needed)

    def _log(self, level: str, source: str, text: str, now_ms: int) -> None:
        self.pub.on_event(LogEvent(level, source, text, now_ms), now_ms)


# ---- load ------------------------------------------------------------------------------------------

_LOAD_DOMAINS = (  # kind, contracts per event, outcome labels
    ("demo_sports", ("A", "B")),
    ("demo_weather", ("B65", "B66", "B68", "T70")),
    ("demo_econ", ("T0.2", "T0.3")),
    ("demo_crypto", ("T70000",)),
)


class LoadGen:
    """`pmx serve --demo --markets N --rate R`: `markets` invented markets across the four demo
    domains and `rate` market-data events a second in total (90% book deltas, 10% prints), plus one
    forecast per model for every ten of them, an entity update per event every 10 s, and slots on a
    `slots` share of the markets (a resting quote each, requoted now and then, status every second).
    Activity is skewed like a real venue: a few markets get most of the events. Cheap per event, so a
    load test measures the feed, not the generator. Publishes like `DemoEngine`."""

    def __init__(self, out: FeedHub | Any, *, markets: int = 5_000, rate: float = 20_000, slots: float = 0.02,
                 models: tuple[str, ...] = ("fair", "mid"), seed: int = 7, now_ms: int | None = None) -> None:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        self.pub = Publisher(out) if isinstance(out, FeedHub) else out
        self.rng = rng = random.Random(seed)
        self.rate, self.models = rate, models
        self.contracts: list[Contract] = []
        self.events: dict[str, list[str]] = {}  # entity key -> tickers
        k = 0
        while len(self.contracts) < markets:
            kind, legs = _LOAD_DOMAINS[k % len(_LOAD_DOMAINS)]
            event = f"DEMO-LOAD{kind[5:].upper()}-{k:05d}"
            key = f"demo:{kind[5:]}:{k:05d}"
            for leg in legs[: markets - len(self.contracts)]:
                c = _contract(f"{event}-{leg}", event, f"{kind[5:]} #{k} {leg}", leg, now_ms,
                              rng.randint(1, 72) * 3_600_000)
                self.contracts.append(c)
                self.events.setdefault(key, []).append(c.ticker)
            k += 1
        self.index = {c.ticker: i for i, c in enumerate(self.contracts)}
        self.books = [OrderBook(c.ticker) for c in self.contracts]
        self.fair = [rng.uniform(0.05, 0.95) for _ in self.contracts]
        n = len(self.contracts)
        self.slot_idx = sorted(rng.sample(range(n), min(n, round(n * slots))))
        self.quotes: dict[int, tuple[OrderUpdate, OrderUpdate]] = {}
        self.n_events = self.n_orders = self.n_trades = 0
        self._carry = 0.0
        self.last_ms = now_ms
        self._entity_at = now_ms
        self._slot_at = now_ms
        self.last: dict[int, Trade] = {}
        self.forecasts: dict[tuple[str, str], Forecast] = {}
        pub = self.pub
        pub.set_models(list(models))
        pub.set_status(True, f"demo load: {n:,} markets, {rate:,.0f} events/s (invented)", ts_ms=now_ms)
        for c in self.contracts:
            pub.on_event(c, now_ms)
        for i in range(n):
            self._snapshot(i, now_ms)
        for key in self.events:
            self._entity(key, now_ms)
        for i in self.slot_idx:
            self._quote(i, now_ms)
            pub.on_event(self._slot_status(i, now_ms), now_ms)

    def _pick(self) -> int:
        """Skewed: about 10% of markets get half the events."""
        return int(len(self.contracts) * self.rng.random() ** 3)

    def _snapshot(self, i: int, now_ms: int) -> None:
        f = round(self.fair[i] * 100) * TICK
        bb = min(max(f - TICK, TICK), ONE_DOLLAR - 2 * TICK)
        bids = tuple((bb - j * TICK, self._size()) for j in range(LEVELS) if bb - j * TICK >= TICK)
        asks = tuple((bb + (j + 2) * TICK, self._size()) for j in range(LEVELS) if bb + (j + 2) * TICK < ONE_DOLLAR)
        ev = BookSnapshot(VENUE, self.contracts[i].ticker, bids, asks, now_ms)
        self.books[i].apply(ev)
        self.pub.on_event(ev, now_ms)

    def _size(self) -> int:
        return self.rng.randint(1, 400) * 100

    def _entity(self, key: str, now_ms: int) -> None:
        tickers = self.events[key]
        mids = [self.books[self.index[t]].mid() for t in tickers]
        text = " · ".join(f"{t.rsplit('-', 1)[1]} {m / 100:.0f}¢" for t, m in zip(tickers, mids, strict=True) if m)
        self.pub.on_event(proto.EntityMsg(key, "demo_load", tickers, text or "-", [("markets", str(len(tickers)))],
                                          now_ms), now_ms)

    def _quote(self, i: int, now_ms: int) -> None:
        book, slot = self.books[i], f"load/{self.contracts[i].ticker}"
        for o in self.quotes.pop(i, ()):
            self.pub.on_event(msgspec.structs.replace(o, status="canceled", ts_ms=now_ms), now_ms)
        bb, ba = book.best_bid(), book.best_ask()
        if bb is None or ba is None:
            return
        pair = []
        for side, (price, _) in (("bid", bb), ("ask", ba)):
            self.n_orders += 1
            o = OrderUpdate(slot, "paper", f"load-c{self.n_orders}", f"load-o{self.n_orders}",
                            self.contracts[i].ticker, side, price, 500, 0, "open", now_ms)
            pair.append(o)
            self.pub.on_event(o, now_ms)
        self.quotes[i] = (pair[0], pair[1])

    def _slot_status(self, i: int, now_ms: int) -> SlotStatus:
        t = self.contracts[i].ticker
        return SlotStatus(f"load/{t}", "load", t, "paper", "active", 0, 0, 0, 0, len(self.quotes.get(i, ())),
                          "quoting", now_ms)

    def step(self, now_ms: int) -> int:
        """Emit the events due since the last step; returns how many market-data events went out."""
        rng, pub, books, contracts = self.rng, self.pub, self.books, self.contracts
        dt_ms = min(max(0, now_ms - self.last_ms), 1_000)  # after a stall (the machine slept), resume: no burst
        want = self.rate * dt_ms / 1000 + self._carry
        n = int(want)
        self._carry = want - n
        self.last_ms = now_ms
        for j in range(n):
            i = self._pick()
            book, ticker = books[i], contracts[i].ticker
            buy = rng.random() < 0.5
            if rng.random() < 0.1 and (lvl := (book.best_ask() if buy else book.best_bid())):
                taker = "bid" if buy else "ask"
                qty = min(lvl[1], rng.randint(1, 50) * 100)
                self.n_trades += 1
                t = self.last[i] = Trade(VENUE, f"load-{self.n_trades}", ticker, lvl[0], qty, taker, now_ms)
                pub.on_event(t, now_ms)
                ev = BookDelta(VENUE, ticker, "ask" if taker == "bid" else "bid", lvl[0], -qty, now_ms)
            else:
                side = "bid" if buy else "ask"
                levels = book.bids if buy else book.asks
                if not levels:
                    self._snapshot(i, now_ms)
                    continue
                price = rng.choice(list(levels)) if rng.random() < 0.9 else None
                if price is None:  # a new level just outside the best
                    best = max(levels) if side == "bid" else min(levels)
                    price = best + TICK if side == "bid" else best - TICK
                    other = book.asks if side == "bid" else book.bids
                    if not TICK <= price < ONE_DOLLAR or (other and (price >= min(other) if side == "bid"
                                                                     else price <= max(other))):
                        continue
                cur = levels.get(price, 0)
                delta = rng.randint(1, 100) * 100 * (1 if rng.random() < 0.55 or cur <= 100 else -1)
                ev = BookDelta(VENUE, ticker, side, price, max(delta, -cur), now_ms)
            if ev.delta:
                book.apply(ev)
                pub.on_event(ev, now_ms)
            if j % 10 == 0 and self.models:
                model = self.models[(j // 10) % len(self.models)]
                self.fair[i] = min(max(self.fair[i] + rng.gauss(0, 0.004), 0.01), 0.99)
                p = (book.mid() or 5_000) / ONE_DOLLAR if model == "mid" else round(self.fair[i], 4)
                f = self.forecasts[(model, ticker)] = Forecast(model, ticker, p, now_ms, now_ms)
                pub.on_event(f, now_ms)
        self.n_events += n
        if now_ms - self._entity_at >= 1_000:  # a tenth of the entities each second
            self._entity_at = now_ms
            keys = list(self.events)
            for key in keys[(now_ms // 1_000) % 10::10]:
                self._entity(key, now_ms)
        if now_ms - self._slot_at >= 1_000:
            self._slot_at = now_ms
            for i in self.slot_idx:
                if rng.random() < 0.2:
                    self._quote(i, now_ms)
                pub.on_event(self._slot_status(i, now_ms), now_ms)
            pub.on_event(Timer(now_ms), now_ms)
        return n

    def market_snapshot(self) -> list[object]:
        out: list[object] = [BookSnapshot(VENUE, b.ticker, tuple(b.levels("bid")), tuple(b.levels("ask")), b.ts_ms)
                             for b in self.books]
        return out + list(self.last.values()) + list(self.forecasts.values())

    async def run(self, stop: asyncio.Event, *, hz: float = 50.0) -> None:
        while not stop.is_set():
            self.step(int(time.time() * 1000))
            try:
                await asyncio.wait_for(stop.wait(), 1 / hz)
            except TimeoutError:
                pass
