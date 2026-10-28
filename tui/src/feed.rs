//! Everything the feed has told us. No UI state here: views read it, `App` owns it.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::book::Book;
use crate::fmt;
use crate::proto::{
    Account, Advice, Contract, Entity, Equity, EquityPoint, Fill, Forecast, Halt, Msg, Order, Perf, Portfolio, Risk,
    Side, Slot, Top, Trade,
};

/// (price, qty)
pub type PriceQty = (i64, i64);

/// Per-market detail kept for tracked markets (recently viewed or traded, like the heatmap)…
const TRADES_PER_MARKET: usize = 200;
const HISTORY_PER_MARKET: usize = 4_000;
/// …and a tail for the rest, so thousands of markets stay ~10s of MB (at the full caps,
/// 5,000 markets came to ~1 GB).
const TRADES_UNTRACKED: usize = 20;
const HISTORY_UNTRACKED: usize = 200;
const TAPE: usize = 500;
const RECENT: usize = 1_000;
const EVENTS: usize = 5_000;
const EQUITY_POINTS: usize = 2_500;
/// (max age ms, bucket ms) — same tiers as the server (`feed/hub.py`): full resolution for
/// 10 min, then coarser with age, so a long session never erodes the 30-day history.
const EQUITY_TIERS: [(i64, i64); 5] = [
    (600_000, 0),
    (6 * 3_600_000, 60_000),
    (2 * 86_400_000, 600_000),
    (30 * 86_400_000, 3_600_000),
    (i64::MAX, 86_400_000),
];

/// Downsample an equity series by age, keeping the last point of each bucket.
pub fn thin_by_age(points: &[EquityPoint], now_ms: i64) -> Vec<EquityPoint> {
    let mut out: Vec<EquityPoint> = Vec::with_capacity(points.len());
    let mut prev = None;
    for &p in points {
        let step = EQUITY_TIERS.iter().find(|&&(limit, _)| now_ms - p.0 <= limit).map_or(86_400_000, |t| t.1);
        let key = (step > 0).then(|| (step, p.0.div_euclid(step)));
        match out.last_mut() {
            Some(last) if key.is_some() && key == prev => *last = p,
            _ => out.push(p),
        }
        prev = key;
    }
    out
}
/// Heatmap columns kept per tracked market (one per second).
pub const HEAT_COLS: usize = 600;
const HEAT_LEVELS: usize = 15;
/// A level that changed this recently is drawn highlighted.
pub const FLASH_MS: i64 = 1_500;

/// One heatmap column: the visible book at a moment.
#[derive(Debug, Clone, PartialEq)]
pub struct HeatCol {
    pub ts_ms: i64,
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub struct Market {
    pub contract: Contract,
    /// None until the first snapshot, and again after a reset/stale until the next one.
    pub book: Option<Book>,
    /// The latest conflated summary (v3); None from a v2 server, and after a reset/stale.
    pub top: Option<Top>,
    pub trades: VecDeque<Trade>,
    /// Prints as (wall ms received, price, taker side), for charts on the local clock.
    pub prints: VecDeque<(i64, i64, Side)>,
    /// (wall ms, YES mid) on every change.
    pub history: VecDeque<(i64, f64)>,
    pub volume: i64,
    /// Direction of the last print vs the one before: >0 up, <0 down.
    pub tick: i8,
    /// Latest forecast per model.
    pub fairs: BTreeMap<String, Forecast>,
    /// (wall ms, P(YES)) per model, on every change.
    pub fair_history: BTreeMap<String, VecDeque<(i64, f64)>>,
    /// Level -> (wall ms it changed, +1 grew / -1 shrank).
    pub flashes: HashMap<(Side, i64), (i64, i8)>,
    /// Wall ms of the last book change.
    pub book_rx_ms: i64,
    pub heat: VecDeque<HeatCol>,
    /// Recently viewed or traded: keeps full history (`App::tick` sets it).
    pub tracked: bool,
}

impl Market {
    fn new(contract: Contract) -> Self {
        Market {
            contract,
            book: None,
            top: None,
            trades: VecDeque::new(),
            prints: VecDeque::new(),
            history: VecDeque::new(),
            volume: 0,
            tick: 0,
            fairs: BTreeMap::new(),
            fair_history: BTreeMap::new(),
            flashes: HashMap::new(),
            book_rx_ms: 0,
            heat: VecDeque::new(),
            tracked: false,
        }
    }

    pub fn last(&self) -> Option<&Trade> {
        self.trades.back()
    }

    /// Best bid and ask, (price, qty): from the summary when the server sends one, else from the book.
    pub fn bbo(&self) -> (Option<PriceQty>, Option<PriceQty>) {
        match (&self.top, &self.book) {
            (Some(t), _) => (t.bid, t.ask),
            (None, Some(b)) => (b.best_bid(), b.best_ask()),
            (None, None) => (None, None),
        }
    }

    pub fn mid(&self) -> Option<f64> {
        self.book.as_ref().and_then(Book::mid)
    }

    /// Model fair minus mid, in price units.
    pub fn edge(&self, model: &str) -> Option<f64> {
        Some(self.fairs.get(model)?.p * 10_000.0 - self.mid()?)
    }

    fn caps(&self) -> (usize, usize) {
        if self.tracked { (HISTORY_PER_MARKET, TRADES_PER_MARKET) } else { (HISTORY_UNTRACKED, TRADES_UNTRACKED) }
    }

    fn record_mid(&mut self, now_ms: i64) {
        let Some(mid) = self.mid() else { return };
        if self.history.back().is_some_and(|&(_, m)| m == mid) {
            return;
        }
        let cap = self.caps().0;
        push_capped(&mut self.history, (now_ms, mid), cap);
    }

    fn on_trade(&mut self, t: Trade, now_ms: i64) {
        if let Some(prev) = self.trades.back() {
            self.tick = (t.price - prev.price).signum() as i8;
        }
        self.volume += t.qty;
        let cap = self.caps().1;
        push_capped(&mut self.prints, (now_ms, t.price, t.taker_side), cap);
        push_capped(&mut self.trades, t, cap);
    }

    /// (bought, sold) contracts printed since `since_ms` (wall clock).
    pub fn flow(&self, since_ms: i64) -> (i64, i64) {
        let qty = |side| -> i64 {
            self.prints
                .iter()
                .zip(&self.trades)
                .filter(|((at, _, s), _)| *at >= since_ms && *s == side)
                .map(|(_, t)| t.qty)
                .sum()
        };
        (qty(Side::Bid), qty(Side::Ask))
    }

    fn on_forecast(&mut self, f: Forecast, now_ms: i64) {
        let cap = self.caps().0;
        let hist = self.fair_history.entry(f.model.clone()).or_default();
        if hist.back().is_none_or(|&(_, p)| p != f.p) {
            push_capped(hist, (now_ms, f.p), cap);
        }
        self.fairs.insert(f.model.clone(), f);
    }

    fn flash(&mut self, side: Side, price: i64, dir: i64, now_ms: i64) {
        self.flashes.insert((side, price), (now_ms, dir.signum() as i8));
        self.book_rx_ms = now_ms;
    }

    /// Flash every level a fresh snapshot changed relative to the book we had.
    fn diff_flash(&mut self, new: &Book, now_ms: i64) {
        let old = self.book.take().unwrap_or_default();
        for (side, a, b) in [(Side::Bid, &old.bids, &new.bids), (Side::Ask, &old.asks, &new.asks)] {
            for (&p, &q) in b {
                let was = a.get(&p).copied().unwrap_or(0);
                if q != was {
                    self.flash(side, p, q - was, now_ms);
                }
            }
            for &p in a.keys().filter(|p| !b.contains_key(p)) {
                self.flash(side, p, -1, now_ms);
            }
        }
        self.book_rx_ms = now_ms;
    }

    pub fn flash_at(&self, side: Side, price: i64, now_ms: i64) -> Option<i8> {
        self.flashes.get(&(side, price)).filter(|&&(at, _)| now_ms - at < FLASH_MS).map(|&(_, d)| d)
    }

    pub fn sample_heat(&mut self, now_ms: i64) {
        let Some(b) = &self.book else { return };
        if self.heat.len() == HEAT_COLS {
            self.heat.pop_front();
        }
        self.heat.push_back(HeatCol {
            ts_ms: now_ms,
            bids: b.levels(Side::Bid).take(HEAT_LEVELS).collect(),
            asks: b.levels(Side::Ask).take(HEAT_LEVELS).collect(),
        });
        let cutoff = now_ms - FLASH_MS;
        self.flashes.retain(|_, &mut (at, _)| at >= cutoff);
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Session {
    pub venue: String,
    pub env: String,
    pub source: String,
    pub run: String,
}

impl Session {
    pub fn is_demo(&self) -> bool {
        self.source == "demo"
    }

    pub fn is_backtest(&self) -> bool {
        self.source == "backtest"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn parse(s: &str) -> Self {
        match s {
            "error" => Level::Error,
            "warn" | "warning" => Level::Warn,
            _ => Level::Info,
        }
    }
}

/// One line of the unified event log: logs, fills, rejects, halts, source health.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub ts_ms: i64,
    pub level: Level,
    pub kind: &'static str,
    pub source: String,
    pub text: String,
}

#[derive(Default)]
pub struct Feed {
    pub session: Option<Session>,
    /// The venue connection.
    pub upstream: Option<(bool, String)>,
    /// Engine events that never reached the feed process (it was slow or down).
    pub dropped: i64,
    /// Outside data sources by name.
    pub sources: BTreeMap<String, (bool, String)>,
    pub markets: HashMap<String, Market>,
    pub account: Option<Account>,
    pub tape: VecDeque<Trade>,
    pub models: Vec<String>,
    pub entities: HashMap<String, Entity>,
    /// ticker -> entity key
    pub entity_of: HashMap<String, String>,
    /// (advisor, target) -> latest advice
    pub advice: BTreeMap<(String, String), Advice>,
    pub slots: BTreeMap<String, Slot>,
    /// client_order_id -> pending/open order
    pub working: BTreeMap<String, Order>,
    /// Finished orders, oldest first.
    pub orders: VecDeque<Order>,
    pub fills: VecDeque<Fill>,
    pub risks: VecDeque<crate::proto::Risk>,
    pub events: VecDeque<Event>,
    /// Events ever logged (the log view pauses against this).
    pub events_total: u64,
    pub halts: BTreeMap<String, Halt>,
    pub portfolio: Option<Portfolio>,
    /// (scope, mode) -> equity series
    pub equity: HashMap<(String, String), Vec<EquityPoint>>,
    /// (strategy, mode, range) -> perf
    pub perf: HashMap<(String, String, String), Perf>,
    /// The market set changed: rows must be rebuilt now.
    pub rows_changed: bool,
    /// Something rows may sort or filter by changed (books, prints, fairs, linked data,
    /// slots); the view re-sorts on its own schedule.
    pub values_changed: bool,
}

/// Append, dropping the oldest down to `cap` (which can shrink: a market no longer tracked).
fn push_capped<T>(q: &mut VecDeque<T>, item: T, cap: usize) {
    while q.len() >= cap {
        q.pop_front();
    }
    q.push_back(item);
}

impl Feed {
    pub fn books_live(&self) -> usize {
        self.markets.values().filter(|m| m.book.is_some()).count()
    }

    pub fn position(&self, ticker: &str) -> Option<i64> {
        self.account.as_ref()?.positions.iter().find(|p| p.ticker == ticker).map(|p| p.qty)
    }

    pub fn entity_for(&self, ticker: &str) -> Option<&Entity> {
        self.entities.get(self.entity_of.get(ticker)?)
    }

    /// Unexpired advice for a market or its linked entity, newest first.
    pub fn advice_on(&self, ticker: &str, now_ms: i64) -> Vec<&Advice> {
        let entity = self.entity_of.get(ticker).map(String::as_str);
        let mut out: Vec<&Advice> = self
            .advice
            .values()
            .filter(|a| a.expires_ms >= now_ms && (a.target == ticker || Some(a.target.as_str()) == entity))
            .collect();
        out.sort_by_key(|a| std::cmp::Reverse(a.ts_ms));
        out
    }

    pub fn slots_on<'a>(&'a self, ticker: &'a str) -> impl Iterator<Item = &'a Slot> + 'a {
        self.slots.values().filter(move |s| s.ticker == ticker)
    }

    pub fn working_on<'a>(&'a self, ticker: &'a str) -> impl Iterator<Item = &'a Order> + 'a {
        self.working.values().filter(move |o| o.ticker == ticker)
    }

    /// Markets something is trading: a slot, a working order, or a venue position.
    pub fn traded(&self, ticker: &str) -> bool {
        self.slots_on(ticker).next().is_some()
            || self.working_on(ticker).next().is_some()
            || self.position(ticker).is_some_and(|p| p != 0)
    }

    pub fn global_halt(&self) -> Option<&Halt> {
        self.halts.get("")
    }

    fn event(&mut self, ts_ms: i64, level: Level, kind: &'static str, source: &str, text: String) {
        push_capped(&mut self.events, Event { ts_ms, level, kind, source: source.to_string(), text }, EVENTS);
        self.events_total += 1;
    }

    /// Everything the server sends lands here. `now_ms` is local wall time (for charts).
    pub fn on_msg(&mut self, msg: Msg, now_ms: i64) {
        match msg {
            Msg::Hello { venue, env, source, run, .. } => {
                // A (re)connect: the server is about to resend its whole state.
                let (events, events_total) = (std::mem::take(&mut self.events), self.events_total);
                *self = Feed { events, events_total, ..Feed::default() };
                self.session = Some(Session { venue, env, source, run });
                self.rows_changed = true;
            }
            Msg::Status { connected, detail, ts_ms, source, dropped } => {
                if source == "engine" {
                    self.dropped = dropped;
                }
                let name = if source.is_empty() {
                    self.session.as_ref().map_or("venue", |s| s.venue.as_str())
                } else {
                    &source
                };
                let name = name.to_string();
                let prev = if source.is_empty() { self.upstream.clone() } else { self.sources.get(&source).cloned() };
                if prev.as_ref().map(|p| p.0) != Some(connected) {
                    let (level, verb) = if connected { (Level::Info, "up") } else { (Level::Warn, "down") };
                    self.event(ts_ms, level, "SRC", &name, format!("{verb}: {detail}"));
                }
                if source.is_empty() {
                    self.upstream = Some((connected, detail));
                } else {
                    self.sources.insert(source, (connected, detail));
                }
            }
            Msg::Contract(c) => {
                match self.markets.get_mut(&c.ticker) {
                    Some(m) => m.contract = c,
                    None => {
                        self.markets.insert(c.ticker.clone(), Market::new(c));
                    }
                }
                self.rows_changed = true;
            }
            Msg::Book { ticker, bids, asks, ts_ms } => {
                if let Some(m) = self.markets.get_mut(&ticker) {
                    let book = Book::from_levels(&bids, &asks, ts_ms);
                    if m.book.is_some() {
                        m.diff_flash(&book, now_ms);
                    }
                    m.book = Some(book);
                    m.book_rx_ms = now_ms;
                    m.record_mid(now_ms);
                    self.values_changed = true;
                }
            }
            Msg::Delta { ticker, side, price, delta, ts_ms } => {
                if let Some(m) = self.markets.get_mut(&ticker) {
                    if let Some(b) = m.book.as_mut() {
                        if b.apply(side, price, delta, ts_ms).is_err() {
                            m.book = None;
                        } else {
                            m.flash(side, price, delta, now_ms);
                        }
                    }
                    m.record_mid(now_ms);
                    self.values_changed = true;
                }
            }
            Msg::Top(t) => {
                for (model, _) in &t.fairs {
                    if !self.models.contains(model) {
                        self.models.push(model.clone());
                    }
                }
                if let Some(m) = self.markets.get_mut(&t.ticker) {
                    for (model, p) in &t.fairs {
                        if m.fairs.get(model).is_none_or(|f| f.p != *p) {
                            let f = Forecast {
                                model: model.clone(),
                                ticker: t.ticker.clone(),
                                p: *p,
                                ts_ms: t.ts_ms,
                                data_ts_ms: t.ts_ms,
                            };
                            m.on_forecast(f, now_ms);
                        }
                    }
                    m.top = Some(t);
                    self.values_changed = true;
                }
            }
            Msg::Stale { ticker } => {
                if let Some(m) = self.markets.get_mut(&ticker) {
                    m.book = None;
                    m.top = None;
                }
            }
            Msg::Reset { reason, ts_ms } => {
                for m in self.markets.values_mut() {
                    m.book = None;
                    m.top = None;
                }
                self.event(ts_ms, Level::Warn, "SRC", "venue", format!("books reset: {reason}"));
            }
            Msg::Trade(t) => {
                push_capped(&mut self.tape, t.clone(), TAPE);
                if let Some(m) = self.markets.get_mut(&t.ticker) {
                    m.on_trade(t, now_ms);
                    self.values_changed = true;
                }
            }
            Msg::Fill(f) => {
                let verb = if f.side == Side::Bid { "bought" } else { "sold" };
                let how = if f.is_taker { "taker" } else { "maker" };
                let who = if f.slot.is_empty() { f.ticker.clone() } else { f.slot.clone() };
                let mode = if f.mode.is_empty() { String::new() } else { format!("{} ", f.mode.to_uppercase()) };
                let text = format!(
                    "{mode}{verb} {} @ {} ({how}, fee {})",
                    fmt::qty(f.qty),
                    fmt::cents(f.price),
                    fmt::money(f.fee)
                );
                self.event(f.ts_ms, Level::Info, "FILL", &who, text);
                push_capped(&mut self.fills, f, RECENT);
            }
            Msg::Account(a) => self.account = Some(a),
            Msg::Entity(e) => {
                for t in &e.tickers {
                    self.entity_of.insert(t.clone(), e.key.clone());
                }
                self.entities.insert(e.key.clone(), e);
                self.values_changed = true;
            }
            Msg::Forecast(f) => {
                if !self.models.contains(&f.model) {
                    self.models.push(f.model.clone());
                }
                if let Some(m) = self.markets.get_mut(&f.ticker) {
                    m.on_forecast(f, now_ms);
                    self.values_changed = true;
                }
            }
            Msg::Advice(a) => {
                let key = (a.advisor.clone(), a.target.clone());
                if a.expires_ms < a.ts_ms {
                    self.advice.remove(&key);
                    self.event(a.ts_ms, Level::Info, "ADV", &a.advisor, format!("{}: withdrawn", a.target));
                } else {
                    let mut what = vec![];
                    if let Some(p) = a.p {
                        what.push(format!("p {}", fmt::prob(p)));
                    }
                    if let Some(d) = &a.direction {
                        what.push(d.clone());
                    }
                    let secs = (a.expires_ms - a.ts_ms) / 1_000;
                    let text =
                        format!("{}: {} (conf {:.2}, {secs}s) {}", a.target, what.join(" "), a.confidence, a.rationale);
                    self.event(a.ts_ms, Level::Info, "ADV", &a.advisor, text);
                    self.advice.insert(key, a);
                }
                self.values_changed = true;
            }
            Msg::Models { names } => {
                for n in names {
                    if !self.models.contains(&n) {
                        self.models.push(n);
                    }
                }
            }
            Msg::Slot(s) => {
                let prev = self.slots.get(&s.slot).map(|p| p.state.clone());
                if prev.as_deref() != Some(s.state.as_str()) && (s.state == "error" || s.state == "halted") {
                    let text = format!("slot {}: {}", s.state, s.last_action);
                    self.event(s.ts_ms, Level::Error, "SLOT", &s.slot, text);
                }
                self.slots.insert(s.slot.clone(), s);
                self.values_changed = true;
            }
            Msg::Order(o) => {
                self.values_changed = true;
                if o.working() {
                    self.working.insert(o.client_order_id.clone(), o);
                } else {
                    self.working.remove(&o.client_order_id);
                    if o.status == "rejected" {
                        let text = format!("order rejected: {}", o.reason);
                        self.event(o.ts_ms, Level::Warn, "ORDER", &o.slot, text);
                    }
                    push_capped(&mut self.orders, o, RECENT);
                }
            }
            Msg::Risk(r) => {
                let side = if r.side == Side::Bid { "buy" } else { "sell" };
                let text =
                    format!("{} rejected {side} {} @ {}: {}", r.rule, fmt::qty(r.qty), fmt::cents(r.price), r.detail);
                self.event(r.ts_ms, Level::Warn, "RISK", &r.slot, text);
                push_capped(&mut self.risks, r, RECENT);
            }
            Msg::Log(l) => {
                let level = Level::parse(&l.level);
                self.event(l.ts_ms, level, "LOG", &l.source, l.text);
            }
            Msg::Halt(h) => {
                let scope = if h.scope.is_empty() { "everything".to_string() } else { h.scope.clone() };
                let (level, text) = if h.active {
                    (Level::Error, format!("HALTED: {}", h.reason))
                } else {
                    (Level::Info, format!("resumed ({})", h.reason))
                };
                self.event(h.ts_ms, level, "HALT", &scope, text);
                if h.active {
                    self.halts.insert(h.scope.clone(), h);
                } else {
                    self.halts.remove(&h.scope);
                }
            }
            Msg::Portfolio(p) => self.portfolio = Some(*p),
            Msg::Equity(Equity { scope, mode, points, replace }) => {
                let series = self.equity.entry((scope, mode)).or_default();
                if replace {
                    *series = points;
                } else {
                    series.extend(points);
                }
                if series.len() > EQUITY_POINTS {
                    let now = series.last().map_or(0, |p| p.0);
                    *series = thin_by_age(series, now);
                }
            }
            Msg::Perf(p) => {
                self.perf.insert((p.strategy.clone(), p.mode.clone(), p.range.clone()), *p);
            }
            Msg::Synced | Msg::Unknown => {}
        }
    }

    /// Strategies seen in slots or perf, with their modes, sorted.
    pub fn strategies(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .slots
            .values()
            .map(|s| (s.strategy.clone(), s.mode.clone()))
            .chain(self.perf.keys().map(|(s, m, _)| (s.clone(), m.clone())))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    pub fn perf_for(&self, strategy: &str, mode: &str, range: &str) -> Option<&Perf> {
        self.perf.get(&(strategy.to_string(), mode.to_string(), range.to_string()))
    }

    pub fn equity_for(&self, scope: &str, mode: &str) -> &[EquityPoint] {
        self.equity.get(&(scope.to_string(), mode.to_string())).map_or(&[], Vec::as_slice)
    }

    /// Recent fills, risk rejects and log lines for one slot, newest first.
    pub fn slot_fills<'a>(&'a self, slot: &'a str) -> impl Iterator<Item = &'a Fill> + 'a {
        self.fills.iter().rev().filter(move |f| f.slot == slot)
    }

    pub fn slot_risks<'a>(&'a self, slot: &'a str) -> impl Iterator<Item = &'a Risk> + 'a {
        self.risks.iter().rev().filter(move |r| r.slot == slot)
    }

    pub fn slot_events<'a>(&'a self, slot: &'a str) -> impl Iterator<Item = &'a Event> + 'a {
        self.events.iter().rev().filter(move |e| e.source == slot && e.kind == "LOG")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::proto::parse;
    use crate::proto::tests::{FIXTURE, FIXTURE_V3};

    /// A feed holding the shared v2 fixture, received at `now`.
    pub fn fixture(now: i64) -> Feed {
        let mut f = Feed::default();
        for line in FIXTURE.lines() {
            f.on_msg(parse(line).unwrap(), now);
        }
        f
    }

    #[test]
    fn fixture_builds_the_whole_picture() {
        let f = fixture(0);
        assert_eq!(f.session.as_ref().unwrap().source, "engine");
        assert_eq!(f.models, ["fair", "mid"]);
        assert_eq!(f.sources["weather"], (false, "poll timeout".to_string()));
        let hou = "DEMO-NFL-26OCT18HOUJAC-HOU";
        assert_eq!(f.entity_for(hou).unwrap().summary, "Q3 4:12 HOU 17-14 JAC");
        assert_eq!(f.markets[hou].book.as_ref().unwrap().best_bid(), Some((5600, 100)));
        assert!(f.markets[hou].edge("fair").unwrap() > 0.0);
        assert!(f.traded(hou) && !f.traded("DEMO-NFL-26OCT18HOUJAC-JAC"));
        assert_eq!(f.working.len(), 1, "the filled order is finished, the open one works");
        assert!(f.halts.contains_key("smoke/DEMO-HIGHNY-26OCT18-B68") && f.global_halt().is_none());
        assert_eq!(f.equity_for("portfolio", "paper").len(), 4, "history replaced, then one point appended");
        assert_eq!(f.strategies(), [("mm".into(), "paper".into()), ("smoke".into(), "live".into())]);
        let kinds: Vec<&str> = f.events.iter().map(|e| e.kind).collect();
        for k in ["SRC", "FILL", "SLOT", "RISK", "LOG", "HALT"] {
            assert!(kinds.contains(&k), "{k} in {kinds:?}");
        }
    }

    #[test]
    fn v3_fixture_builds_markets_from_tops_and_conflated_books() {
        let mut f = Feed::default();
        for line in FIXTURE_V3.lines() {
            f.on_msg(parse(line).unwrap(), 5_000);
        }
        assert_eq!(f.dropped, 1234);
        assert_eq!(f.models, ["fair", "mid"]);
        let hou_advice = f.advice_on("DEMO-NFL-26OCT18HOUJAC-HOU", 1_792_000_000_000);
        assert_eq!(hou_advice.len(), 1, "unexpired advice for the market");
        assert!(f.advice_on("DEMO-NFL-26OCT18HOUJAC-HOU", 1_792_000_060_001).is_empty(), "lapsed");
        assert!(f.events.iter().any(|e| e.kind == "ADV" && e.text.contains("p 61.0¢ buy")));
        let hou = &f.markets["DEMO-NFL-26OCT18HOUJAC-HOU"];
        assert!(hou.book.is_none() && hou.top.is_none(), "the trailing reset leaves every book stale");
        assert_eq!(hou.fairs["fair"].p, 0.582, "fairs come from tops");
        assert_eq!(hou.fair_history["mid"].len(), 1);
        let mut g = Feed::default();
        for line in FIXTURE_V3.lines().filter(|l| !l.contains(r#""t":"reset""#)) {
            g.on_msg(parse(line).unwrap(), 5_000);
        }
        let hou = &g.markets["DEMO-NFL-26OCT18HOUJAC-HOU"];
        assert_eq!(hou.bbo(), (Some((5500, 10000)), Some((5700, 300))));
        assert_eq!(hou.book.as_ref().unwrap().levels(Side::Bid).count(), 10);
        let b68 = &g.markets["DEMO-HIGHNY-26OCT18-B68"];
        assert!(b68.book.is_none() && b68.bbo() == (None, None), "stale clears the summary too");
    }

    #[test]
    fn untracked_markets_keep_a_short_history_tracked_ones_the_full() {
        let mut f = fixture(0);
        let hou = "DEMO-NFL-26OCT18HOUJAC-HOU";
        let feed = |f: &mut Feed, from: i64, n: i64| {
            for i in from..from + n {
                let p = 5_000 + i % 2_000;
                let t =
                    format!(r#"{{"t":"trade","ticker":"{hou}","price":{p},"qty":100,"taker_side":"bid","ts_ms":{i}}}"#);
                f.on_msg(parse(&t).unwrap(), i);
                let fc = format!(
                    r#"{{"t":"forecast","model":"fair","ticker":"{hou}","p":0.{p},"ts_ms":{i},"data_ts_ms":{i}}}"#
                );
                f.on_msg(parse(&fc).unwrap(), i);
            }
        };
        feed(&mut f, 0, 5_000);
        let m = &f.markets[hou];
        assert_eq!((m.trades.len(), m.prints.len(), m.fair_history["fair"].len()), (20, 20, 200));
        f.markets.get_mut(hou).unwrap().tracked = true;
        feed(&mut f, 5_000, 5_000);
        let m = &f.markets[hou];
        assert_eq!((m.trades.len(), m.fair_history["fair"].len()), (200, 4_000));
        f.markets.get_mut(hou).unwrap().tracked = false;
        feed(&mut f, 10_000, 1);
        assert_eq!(f.markets[hou].fair_history["fair"].len(), 200, "shrinks back once no longer tracked");
    }

    #[test]
    fn hello_resets_state_but_keeps_the_log() {
        let mut f = fixture(0);
        let n = f.events.len();
        f.on_msg(
            parse(r#"{"t":"hello","venue":"kalshi","env":"prod","source":"backtest","version":2,"run":"r1"}"#).unwrap(),
            0,
        );
        assert!(f.markets.is_empty() && f.slots.is_empty() && f.portfolio.is_none() && f.equity.is_empty());
        assert_eq!(f.events.len(), n);
        assert!(f.session.as_ref().unwrap().is_backtest());
    }

    #[test]
    fn equity_history_thins_by_age_not_by_dropping_the_oldest() {
        let (day, now) = (86_400_000i64, 40 * 86_400_000i64);
        let mut f = Feed::default();
        let hist: Vec<String> = (0..40 * 24i64).map(|h| format!("[{},{},0]", h * 3_600_000, h)).collect();
        f.on_msg(
            parse(&format!(
                r#"{{"t":"equity","scope":"portfolio","mode":"paper","points":[{}],"replace":true}}"#,
                hist.join(",")
            ))
            .unwrap(),
            0,
        );
        for t in (now..now + 3 * 3_600_000).step_by(1_000) {
            f.on_msg(
                parse(&format!(r#"{{"t":"equity","scope":"portfolio","mode":"paper","points":[[{t},1,0]]}}"#)).unwrap(),
                0,
            );
        }
        let s = f.equity_for("portfolio", "paper");
        assert!(s.len() <= EQUITY_POINTS);
        assert!(s[0].0 < day, "the first day is still represented");
        assert!(s.iter().filter(|p| p.0 < now - 2 * day && p.0 >= now - 30 * day).count() > 600, "hourly a month back");
        assert_eq!(s.last().unwrap().0, now + 3 * 3_600_000 - 1_000);
        assert!(thin_by_age(&[], 0).is_empty());
    }

    #[test]
    fn deltas_and_snapshots_flash_changed_levels() {
        let mut f = fixture(0);
        let hou = "DEMO-NFL-26OCT18HOUJAC-HOU";
        f.on_msg(
            parse(&format!(r#"{{"t":"delta","ticker":"{hou}","side":"ask","price":5700,"delta":-100,"ts_ms":9}}"#))
                .unwrap(),
            10_000,
        );
        let m = &f.markets[hou];
        assert_eq!(m.flash_at(Side::Ask, 5700, 10_500), Some(-1));
        assert_eq!(m.flash_at(Side::Ask, 5700, 10_000 + FLASH_MS), None);
        f.on_msg(
            parse(&format!(
                r#"{{"t":"book","ticker":"{hou}","bids":[[5600,100],[5500,10000]],"asks":[[5800,12000]],"ts_ms":10}}"#
            ))
            .unwrap(),
            20_000,
        );
        let m = &f.markets[hou];
        assert_eq!(m.flash_at(Side::Bid, 5400, 20_000), Some(-1), "level removed by the snapshot");
        assert_eq!(m.flash_at(Side::Bid, 5600, 20_000), None, "unchanged level");
    }
}
