//! UI state over the feed: which view, selection, filters, sort, and keys. Watch-only:
//! nothing here sends anything anywhere.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::book::Book;
use crate::feed::{Feed, Level, Market};
use crate::proto::{Contract, Msg};

const TOAST: Duration = Duration::from_secs(6);
const HEAT_EVERY_MS: i64 = 1_000;
/// Recently selected markets keep recording their heatmap.
const HEAT_RECENT: usize = 16;
const DAY_MS: i64 = 86_400_000;
/// Value-driven re-sorts (volume, spread, edge, filters) happen at most this often, so a
/// busy feed costs a sort per interval rather than per frame.
const RESORT_MS: i64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    Markets,
    Portfolio,
    Strategies,
    Slots,
    Log,
}

impl View {
    pub const ALL: [View; 5] = [View::Markets, View::Portfolio, View::Strategies, View::Slots, View::Log];

    pub fn name(self) -> &'static str {
        match self {
            View::Markets => "Markets",
            View::Portfolio => "Portfolio",
            View::Strategies => "Strategies",
            View::Slots => "Slots",
            View::Log => "Log",
        }
    }

    pub fn short(self) -> &'static str {
        match self {
            View::Markets => "Mkts",
            View::Portfolio => "Port",
            View::Strategies => "Strat",
            View::Slots => "Slots",
            View::Log => "Log",
        }
    }

    fn index(self) -> usize {
        View::ALL.iter().position(|&v| v == self).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    Time,
    Volume,
    Spread,
    Edge,
    Ticker,
}

impl Sort {
    fn next(self) -> Self {
        match self {
            Sort::Time => Sort::Volume,
            Sort::Volume => Sort::Spread,
            Sort::Spread => Sort::Edge,
            Sort::Edge => Sort::Ticker,
            Sort::Ticker => Sort::Time,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Sort::Time => "time",
            Sort::Volume => "volume",
            Sort::Spread => "spread",
            Sort::Edge => "edge",
            Sort::Ticker => "ticker",
        }
    }
}

/// What the market detail's chart panel shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartMode {
    /// Mid, every model's fair, prints, and our fills over time.
    Price,
    /// Cumulative depth by price, now.
    Depth,
    /// Resting size by price over time.
    Heat,
}

impl ChartMode {
    fn next(self) -> Self {
        match self {
            ChartMode::Price => ChartMode::Depth,
            ChartMode::Depth => ChartMode::Heat,
            ChartMode::Heat => ChartMode::Price,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ChartMode::Price => "price",
            ChartMode::Depth => "depth",
            ChartMode::Heat => "heatmap",
        }
    }
}

/// Paper and live are never summed: BOTH shows them side by side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeSel {
    Paper,
    Live,
    Both,
}

impl ModeSel {
    fn next(self) -> Self {
        match self {
            ModeSel::Paper => ModeSel::Live,
            ModeSel::Live => ModeSel::Both,
            ModeSel::Both => ModeSel::Paper,
        }
    }

    pub fn modes(self) -> &'static [&'static str] {
        match self {
            ModeSel::Paper => &["paper"],
            ModeSel::Live => &["live"],
            ModeSel::Both => &["paper", "live"],
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ModeSel::Paper => "PAPER",
            ModeSel::Live => "LIVE",
            ModeSel::Both => "BOTH",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Range {
    Today,
    Week,
    Month,
    All,
}

impl Range {
    const ALL: [Range; 4] = [Range::Today, Range::Week, Range::Month, Range::All];

    pub fn name(self) -> &'static str {
        match self {
            Range::Today => "today",
            Range::Week => "7d",
            Range::Month => "30d",
            Range::All => "all",
        }
    }

    /// First wall ms in range; `midnight_ms` is local midnight today.
    pub fn start_ms(self, midnight_ms: i64) -> i64 {
        match self {
            Range::Today => midnight_ms,
            Range::Week => midnight_ms - 6 * DAY_MS,
            Range::Month => midnight_ms - 29 * DAY_MS,
            Range::All => i64::MIN,
        }
    }

    fn step(self, by: isize) -> Self {
        let i = Range::ALL.iter().position(|&r| r == self).unwrap_or(0) as isize + by;
        Range::ALL[i.clamp(0, 3) as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotSort {
    Slot,
    Pnl,
    Position,
}

impl SlotSort {
    fn next(self) -> Self {
        match self {
            SlotSort::Slot => SlotSort::Pnl,
            SlotSort::Pnl => SlotSort::Position,
            SlotSort::Position => SlotSort::Slot,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SlotSort::Slot => "slot",
            SlotSort::Pnl => "pnl",
            SlotSort::Position => "position",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Move {
    By(isize),
    Top,
    Bottom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Filter,
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    Connecting,
    Up,
    Down(String),
}

pub struct MarketsUi {
    /// Tickers in display order (filtered + sorted).
    pub rows: Vec<String>,
    pub selected: usize,
    selected_ticker: Option<String>,
    pub filter: String,
    pub sort: Sort,
    pub show_closed: bool,
    pub traded_only: bool,
    model: usize,
    pub chart: ChartMode,
    recent: VecDeque<String>,
    /// First visible row; drawing only builds rows in the window.
    pub offset: std::cell::Cell<usize>,
}

pub struct SlotsUi {
    pub rows: Vec<String>,
    pub selected: usize,
    pub filter: String,
    /// Set by Enter on a strategy; Esc clears it.
    pub strategy: Option<String>,
    pub sort: SlotSort,
}

pub struct LogUi {
    pub min_level: Level,
    pub filter: String,
    /// `Feed::events_total` when paused; the view stays put while new lines arrive.
    pub paused_at: Option<u64>,
    /// Lines scrolled up from the bottom.
    pub offset: usize,
}

/// Draw and apply costs over the last second (`PMX_DEBUG=1`).
#[derive(Debug, Default, Clone, Copy)]
pub struct Perf {
    pub frames: u32,
    pub frame_max: Duration,
    pub frame_total: Duration,
    pub applied: u32,
    pub apply_total: Duration,
}

impl Perf {
    pub fn frame_avg(&self) -> Duration {
        self.frame_total.checked_div(self.frames).unwrap_or_default()
    }

    /// Mean time to apply one message, µs.
    pub fn apply_us(&self) -> f64 {
        if self.applied == 0 { 0.0 } else { self.apply_total.as_secs_f64() * 1e6 / f64::from(self.applied) }
    }
}

pub struct App {
    pub socket: String,
    pub link: Link,
    pub feed: Feed,
    pub view: View,
    pub mode: Mode,
    pub markets: MarketsUi,
    pub strategy_selected: usize,
    /// 0 = charts; otherwise the n-th breakdown table.
    pub breakdown: usize,
    pub slots: SlotsUi,
    pub log: LogUi,
    pub mode_sel: ModeSel,
    pub range: Range,
    pub exposure_scroll: usize,
    pub toast: Option<(String, Instant)>,
    pub msg_rate: u32,
    msg_count: u32,
    rate_at: Instant,
    /// `PMX_DEBUG=1`: the header shows `perf` (frame and apply times against their budgets).
    pub debug: bool,
    pub perf: Perf,
    window: Perf,
    heat_at_ms: i64,
    redraw_at_ms: i64,
    sorted_at_ms: i64,
    pub quit: bool,
    pub dirty: bool,
}

impl App {
    pub fn new(socket: String) -> Self {
        App {
            socket,
            link: Link::Connecting,
            feed: Feed::default(),
            view: View::Markets,
            mode: Mode::Normal,
            markets: MarketsUi {
                rows: Vec::new(),
                selected: 0,
                selected_ticker: None,
                filter: String::new(),
                sort: Sort::Time,
                show_closed: false,
                traded_only: false,
                model: 0,
                chart: ChartMode::Price,
                recent: VecDeque::new(),
                offset: std::cell::Cell::new(0),
            },
            strategy_selected: 0,
            breakdown: 0,
            slots: SlotsUi {
                rows: Vec::new(),
                selected: 0,
                filter: String::new(),
                strategy: None,
                sort: SlotSort::Slot,
            },
            log: LogUi { min_level: Level::Info, filter: String::new(), paused_at: None, offset: 0 },
            mode_sel: ModeSel::Paper,
            range: Range::Today,
            exposure_scroll: 0,
            toast: None,
            msg_rate: 0,
            msg_count: 0,
            rate_at: Instant::now(),
            debug: std::env::var("PMX_DEBUG").is_ok_and(|v| v == "1"),
            perf: Perf::default(),
            window: Perf::default(),
            heat_at_ms: 0,
            redraw_at_ms: 0,
            sorted_at_ms: 0,
            quit: false,
            dirty: true,
        }
    }

    pub fn selected_market(&self) -> Option<&Market> {
        self.markets.rows.get(self.markets.selected).and_then(|t| self.feed.markets.get(t))
    }

    /// The model whose edge the Markets view shows (`m` cycles).
    pub fn model(&self) -> Option<&str> {
        let n = self.feed.models.len();
        (n > 0).then(|| self.feed.models[self.markets.model % n].as_str())
    }

    pub fn selected_strategy(&self) -> Option<(String, String)> {
        self.visible_strategies().get(self.strategy_selected).cloned()
    }

    /// Strategies in the selected mode(s).
    pub fn visible_strategies(&self) -> Vec<(String, String)> {
        let modes = self.mode_sel.modes();
        self.feed.strategies().into_iter().filter(|(_, m)| modes.contains(&m.as_str())).collect()
    }

    pub fn selected_slot(&self) -> Option<&crate::proto::Slot> {
        self.slots.rows.get(self.slots.selected).and_then(|s| self.feed.slots.get(s))
    }

    pub fn set_link(&mut self, link: Link) {
        if let Link::Down(_) = link {
            // Nothing is being kept in sync any more.
            for m in self.feed.markets.values_mut() {
                m.book = None;
            }
            self.feed.upstream = None;
        }
        self.link = link;
        self.dirty = true;
    }

    pub fn on_msg(&mut self, msg: Msg, now_ms: i64) {
        self.msg_count += 1;
        self.dirty = true;
        if let Msg::Fill(f) = &msg {
            let side = if f.side == crate::proto::Side::Bid { "bought" } else { "sold" };
            let mode = if f.mode.is_empty() { String::new() } else { format!("{} ", f.mode.to_uppercase()) };
            self.toast(format!(
                "{mode}FILL {side} {} YES @ {} {}",
                crate::fmt::qty(f.qty),
                crate::fmt::cents(f.price),
                f.ticker
            ));
        }
        self.feed.on_msg(msg, now_ms);
    }

    /// One frame drawn in `took`.
    pub fn note_frame(&mut self, took: Duration) {
        self.window.frames += 1;
        self.window.frame_max = self.window.frame_max.max(took);
        self.window.frame_total += took;
    }

    /// `n` messages applied in `took`.
    pub fn note_apply(&mut self, took: Duration, n: u32) {
        self.window.applied += n;
        self.window.apply_total += took;
    }

    pub fn toast(&mut self, text: String) {
        self.toast = Some((text, Instant::now()));
        self.dirty = true;
    }

    /// Once per frame: rebuild rows if needed, roll the msg/s counter, sample heatmaps,
    /// expire toasts.
    pub fn tick(&mut self, now_ms: i64) {
        if self.rate_at.elapsed() >= Duration::from_secs(1) {
            self.msg_rate = self.msg_count;
            self.msg_count = 0;
            self.perf = std::mem::take(&mut self.window);
            self.rate_at = Instant::now();
            self.dirty = true;
        }
        if self.toast.as_ref().is_some_and(|(_, at)| at.elapsed() > TOAST) {
            self.toast = None;
            self.dirty = true;
        }
        if now_ms - self.heat_at_ms >= HEAT_EVERY_MS {
            self.heat_at_ms = now_ms;
            let feed = &mut self.feed;
            let tracked: HashSet<String> =
                feed.markets.keys().filter(|t| self.markets.recent.contains(t) || feed.traded(t)).cloned().collect();
            for (t, m) in feed.markets.iter_mut() {
                m.tracked = tracked.contains(t);
                if m.tracked {
                    m.sample_heat(now_ms);
                }
            }
        }
        // Level flashes and book age move without new messages.
        if self.view == View::Markets && now_ms - self.redraw_at_ms >= 250 {
            self.redraw_at_ms = now_ms;
            self.dirty = true;
        }
        let ui = &self.markets;
        let by_values = !matches!(ui.sort, Sort::Time | Sort::Ticker) || !ui.filter.is_empty() || ui.traded_only;
        let due = self.feed.values_changed && by_values && now_ms - self.sorted_at_ms >= RESORT_MS;
        if self.feed.rows_changed || due {
            self.sorted_at_ms = now_ms;
            self.rebuild_rows();
        }
    }

    fn matches(&self, m: &Market) -> bool {
        let c: &Contract = &m.contract;
        if !self.markets.show_closed && !c.is_open() {
            return false;
        }
        if self.markets.traded_only && !self.feed.traded(&c.ticker) {
            return false;
        }
        let entity = self.feed.entity_for(&c.ticker).map_or("", |e| e.summary.as_str());
        let hay = format!("{} {} {} {entity}", c.ticker, c.title, c.subtitle).to_lowercase();
        self.markets.filter.to_lowercase().split_whitespace().all(|term| hay.contains(term))
    }

    pub fn rebuild_rows(&mut self) {
        self.feed.rows_changed = false;
        self.feed.values_changed = false;
        let model = self.model().map(str::to_string);
        let mut rows: Vec<&Market> = self.feed.markets.values().filter(|m| self.matches(m)).collect();
        let by_ticker = |a: &Market, b: &Market| a.contract.ticker.cmp(&b.contract.ticker);
        match self.markets.sort {
            Sort::Time => rows.sort_by(|a, b| {
                let key = |m: &Market| (m.contract.ends_ts().unwrap_or(i64::MAX), m.contract.event_ticker.clone());
                key(a).cmp(&key(b)).then_with(|| by_ticker(a, b))
            }),
            Sort::Volume => rows.sort_by(|a, b| b.volume.cmp(&a.volume).then_with(|| by_ticker(a, b))),
            Sort::Spread => rows.sort_by(|a, b| {
                let s = |m: &Market| m.book.as_ref().and_then(Book::spread).unwrap_or(i64::MAX);
                s(a).cmp(&s(b)).then_with(|| by_ticker(a, b))
            }),
            Sort::Edge => rows.sort_by(|a, b| {
                let e = |m: &Market| model.as_deref().and_then(|md| m.edge(md)).map_or(-1.0, f64::abs);
                e(b).total_cmp(&e(a)).then_with(|| by_ticker(a, b))
            }),
            Sort::Ticker => rows.sort_by(|a, b| by_ticker(a, b)),
        }
        let ui = &mut self.markets;
        ui.rows = rows.into_iter().map(|m| m.contract.ticker.clone()).collect();
        ui.selected = ui
            .selected_ticker
            .as_ref()
            .and_then(|t| ui.rows.iter().position(|r| r == t))
            .unwrap_or(ui.selected.min(ui.rows.len().saturating_sub(1)));
        self.select_market(self.markets.selected);
        self.rebuild_slots();
        self.dirty = true;
    }

    fn slot_matches(&self, s: &crate::proto::Slot) -> bool {
        if self.slots.strategy.as_ref().is_some_and(|st| *st != s.strategy) {
            return false;
        }
        let label = self.feed.markets.get(&s.ticker).map_or("", |m| m.contract.label());
        let hay = format!("{} {} {} {} {}", s.slot, s.mode, s.state, label, s.last_action).to_lowercase();
        self.slots.filter.to_lowercase().split_whitespace().all(|term| hay.contains(term))
    }

    pub fn rebuild_slots(&mut self) {
        let mut rows: Vec<&crate::proto::Slot> = self.feed.slots.values().filter(|s| self.slot_matches(s)).collect();
        match self.slots.sort {
            SlotSort::Slot => {}
            SlotSort::Pnl => rows.sort_by_key(|s| s.net()),
            SlotSort::Position => rows.sort_by_key(|s| -s.position.abs()),
        }
        let keep = self.slots.rows.get(self.slots.selected).cloned();
        self.slots.rows = rows.into_iter().map(|s| s.slot.clone()).collect();
        self.slots.selected = keep
            .and_then(|k| self.slots.rows.iter().position(|r| *r == k))
            .unwrap_or(self.slots.selected.min(self.slots.rows.len().saturating_sub(1)));
    }

    fn select_market(&mut self, i: usize) {
        let ui = &mut self.markets;
        ui.selected = i.min(ui.rows.len().saturating_sub(1));
        ui.selected_ticker = ui.rows.get(ui.selected).cloned();
        if let Some(t) = &ui.selected_ticker
            && !ui.recent.contains(t)
        {
            if ui.recent.len() == HEAT_RECENT {
                ui.recent.pop_front();
            }
            ui.recent.push_back(t.clone());
        }
        self.dirty = true;
    }

    /// Jump to the Markets view with `ticker` selected.
    fn show_market(&mut self, ticker: &str) {
        self.view = View::Markets;
        if !self.markets.rows.iter().any(|r| r == ticker) {
            self.markets.filter.clear();
            self.markets.traded_only = false;
            self.markets.show_closed = true;
            self.rebuild_rows();
        }
        if let Some(i) = self.markets.rows.iter().position(|r| r == ticker) {
            self.select_market(i);
        }
    }

    fn filter_text(&mut self) -> Option<&mut String> {
        match self.view {
            View::Markets => Some(&mut self.markets.filter),
            View::Slots => Some(&mut self.slots.filter),
            View::Log => Some(&mut self.log.filter),
            View::Portfolio | View::Strategies => None,
        }
    }

    fn refilter(&mut self) {
        match self.view {
            View::Markets => self.rebuild_rows(),
            View::Slots => self.rebuild_slots(),
            _ => {}
        }
        self.log.offset = 0;
    }

    /// Move the active view's selection (in the Log view: scroll, which pauses it).
    fn move_sel(&mut self, mv: Move) {
        let to = |i: usize, n: usize| match mv {
            Move::By(d) => i.saturating_add_signed(d).min(n.saturating_sub(1)),
            Move::Top => 0,
            Move::Bottom => n.saturating_sub(1),
        };
        match self.view {
            View::Markets => self.select_market(to(self.markets.selected, self.markets.rows.len())),
            View::Slots => self.slots.selected = to(self.slots.selected, self.slots.rows.len()),
            View::Strategies => self.strategy_selected = to(self.strategy_selected, self.visible_strategies().len()),
            View::Portfolio => {
                let n = self.feed.portfolio.as_ref().map_or(0, |p| p.exposures.len());
                self.exposure_scroll = to(self.exposure_scroll, n);
            }
            View::Log => match mv {
                Move::Bottom => {
                    self.log.paused_at = None;
                    self.log.offset = 0;
                }
                Move::Top => {
                    self.log.paused_at.get_or_insert(self.feed.events_total);
                    self.log.offset = self.feed.events.len();
                }
                Move::By(d) => {
                    self.log.paused_at.get_or_insert(self.feed.events_total);
                    self.log.offset = self.log.offset.saturating_add_signed(-d).min(self.feed.events.len());
                }
            },
        }
    }

    pub fn on_key(&mut self, key: KeyEvent, page: usize) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && key.code == KeyCode::Char('c') {
            self.quit = true;
            return;
        }
        self.dirty = true;
        match self.mode {
            Mode::Help => self.mode = Mode::Normal,
            Mode::Filter => match key.code {
                KeyCode::Esc => {
                    if let Some(t) = self.filter_text() {
                        t.clear();
                    }
                    self.mode = Mode::Normal;
                    self.refilter();
                }
                KeyCode::Enter => self.mode = Mode::Normal,
                KeyCode::Backspace => {
                    if let Some(t) = self.filter_text() {
                        t.pop();
                    }
                    self.refilter();
                }
                KeyCode::Down => self.move_sel(Move::By(1)),
                KeyCode::Up => self.move_sel(Move::By(-1)),
                KeyCode::Char(ch) if !ctrl => {
                    if let Some(t) = self.filter_text() {
                        t.push(ch);
                    }
                    self.refilter();
                }
                _ => {}
            },
            Mode::Normal => self.normal_key(key, ctrl, page),
        }
    }

    fn normal_key(&mut self, key: KeyEvent, ctrl: bool, page: usize) {
        match key.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char('?') => self.mode = Mode::Help,
            KeyCode::Char(c @ '1'..='5') => self.view = View::ALL[(c as u8 - b'1') as usize],
            KeyCode::Tab => self.view = View::ALL[(self.view.index() + 1) % 5],
            KeyCode::BackTab => self.view = View::ALL[(self.view.index() + 4) % 5],
            KeyCode::Char('/') if self.filter_text().is_some() => self.mode = Mode::Filter,
            KeyCode::Esc => {
                if self.view == View::Slots && self.slots.strategy.take().is_some() {
                    self.rebuild_slots();
                } else if let Some(t) = self.filter_text().filter(|t| !t.is_empty()) {
                    t.clear();
                    self.refilter();
                } else if self.view == View::Log {
                    self.log.paused_at = None;
                    self.log.offset = 0;
                }
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_sel(Move::By(1)),
            KeyCode::Char('k') | KeyCode::Up => self.move_sel(Move::By(-1)),
            KeyCode::Char('d') if ctrl => self.move_sel(Move::By(page as isize / 2)),
            KeyCode::Char('u') if ctrl => self.move_sel(Move::By(-(page as isize) / 2)),
            KeyCode::PageDown => self.move_sel(Move::By(page as isize)),
            KeyCode::PageUp => self.move_sel(Move::By(-(page as isize))),
            KeyCode::Char('g') | KeyCode::Home => self.move_sel(Move::Top),
            KeyCode::Char('G') | KeyCode::End => self.move_sel(Move::Bottom),
            KeyCode::Char('p') if matches!(self.view, View::Portfolio | View::Strategies) => {
                self.mode_sel = self.mode_sel.next();
                self.strategy_selected = 0;
                self.toast(format!("mode: {}", self.mode_sel.name()));
            }
            KeyCode::Char('[') => self.range = self.range.step(-1),
            KeyCode::Char(']') => self.range = self.range.step(1),
            _ => self.view_key(key),
        }
    }

    fn view_key(&mut self, key: KeyEvent) {
        match (self.view, key.code) {
            (View::Markets, KeyCode::Char('s')) => {
                self.markets.sort = self.markets.sort.next();
                self.rebuild_rows();
                self.toast(format!("sort: {}", self.markets.sort.name()));
            }
            (View::Markets, KeyCode::Char('c')) => {
                self.markets.show_closed = !self.markets.show_closed;
                self.rebuild_rows();
                self.toast(format!("closed contracts {}", if self.markets.show_closed { "shown" } else { "hidden" }));
            }
            (View::Markets, KeyCode::Char('t')) => {
                self.markets.traded_only = !self.markets.traded_only;
                self.rebuild_rows();
                self.toast(
                    (if self.markets.traded_only { "only markets being traded" } else { "all markets" }).to_string(),
                );
            }
            (View::Markets, KeyCode::Char('m')) if !self.feed.models.is_empty() => {
                self.markets.model = (self.markets.model + 1) % self.feed.models.len();
                if self.markets.sort == Sort::Edge {
                    self.rebuild_rows();
                }
                self.toast(format!("edge vs model: {}", self.model().unwrap_or("-")));
            }
            (View::Markets, KeyCode::Char('v')) => {
                self.markets.chart = self.markets.chart.next();
                self.toast(format!("chart: {}", self.markets.chart.name()));
            }
            (View::Strategies, KeyCode::Char('b')) => self.breakdown += 1,
            (View::Strategies, KeyCode::Enter) => {
                if let Some((sid, _)) = self.selected_strategy() {
                    self.slots.strategy = Some(sid);
                    self.slots.selected = 0;
                    self.rebuild_slots();
                    self.view = View::Slots;
                }
            }
            (View::Slots, KeyCode::Char('s')) => {
                self.slots.sort = self.slots.sort.next();
                self.rebuild_slots();
                self.toast(format!("sort: {}", self.slots.sort.name()));
            }
            (View::Slots, KeyCode::Enter) => {
                if let Some(t) = self.selected_slot().map(|s| s.ticker.clone()) {
                    self.show_market(&t);
                }
            }
            (View::Log, KeyCode::Char(' ')) => {
                self.log.paused_at = match self.log.paused_at {
                    Some(_) => None,
                    None => Some(self.feed.events_total),
                };
                self.log.offset = 0;
            }
            (View::Log, KeyCode::Char('l')) => {
                self.log.min_level = match self.log.min_level {
                    Level::Info => Level::Warn,
                    Level::Warn => Level::Error,
                    Level::Error => Level::Info,
                };
                self.log.offset = 0;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::proto::parse;
    use crate::proto::tests::FIXTURE;

    pub fn contract(ticker: &str, event: &str, sub: &str, close: i64) -> Msg {
        Msg::Contract(Contract {
            ticker: ticker.into(),
            event_ticker: event.into(),
            series: "KXNFLGAME".into(),
            title: format!("{sub} wins"),
            subtitle: sub.into(),
            status: "open".into(),
            close_ts_ms: Some(close + 100_000),
            event_ts_ms: Some(close),
            result: None,
        })
    }

    /// v1-only feed: three markets, one book, one print.
    pub fn loaded() -> App {
        let mut app = App::new("test.sock".into());
        app.on_msg(parse(r#"{"t":"hello","venue":"kalshi","env":"prod","source":"live","version":1}"#).unwrap(), 0);
        app.on_msg(contract("KXNFLGAME-26OCT26DALPHI-PHI", "KXNFLGAME-26OCT26DALPHI", "Philadelphia", 2_000), 0);
        app.on_msg(contract("KXNFLGAME-26OCT26DALPHI-DAL", "KXNFLGAME-26OCT26DALPHI", "Dallas", 2_000), 0);
        app.on_msg(contract("KXNFLGAME-26OCT19KCLV-KC", "KXNFLGAME-26OCT19KCLV", "Kansas City", 1_000), 0);
        app.on_msg(
            parse(r#"{"t":"book","ticker":"KXNFLGAME-26OCT26DALPHI-PHI","bids":[[5500,10000],[5400,2500]],"asks":[[5700,400]],"ts_ms":1}"#)
                .unwrap(),
            10,
        );
        app.on_msg(
            parse(r#"{"t":"trade","ticker":"KXNFLGAME-26OCT26DALPHI-PHI","price":5600,"qty":300,"taker_side":"bid","ts_ms":2}"#).unwrap(),
            10,
        );
        app.tick(10);
        app
    }

    /// The shared v2 fixture, received at `now`, plus a second of heatmap samples.
    pub fn v2(now: i64) -> App {
        let mut app = App::new("test.sock".into());
        for line in FIXTURE.lines() {
            app.on_msg(parse(line).unwrap(), now - 5_000);
        }
        app.tick(now - 1_000);
        app.tick(now);
        app
    }

    fn key(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)
    }

    fn code(c: KeyCode) -> KeyEvent {
        KeyEvent::new(c, KeyModifiers::NONE)
    }

    #[test]
    fn rows_sort_by_close_then_event() {
        let app = loaded();
        assert_eq!(
            app.markets.rows,
            vec!["KXNFLGAME-26OCT19KCLV-KC", "KXNFLGAME-26OCT26DALPHI-DAL", "KXNFLGAME-26OCT26DALPHI-PHI"]
        );
    }

    #[test]
    fn deltas_reset_and_negative_levels() {
        let mut app = loaded();
        let phi = "KXNFLGAME-26OCT26DALPHI-PHI";
        app.on_msg(
            parse(&format!(r#"{{"t":"delta","ticker":"{phi}","side":"bid","price":5600,"delta":100,"ts_ms":3}}"#))
                .unwrap(),
            20,
        );
        assert_eq!(app.feed.markets[phi].book.as_ref().unwrap().best_bid(), Some((5600, 100)));
        assert_eq!(app.feed.markets[phi].history.back(), Some(&(20, 5650.0)));
        app.on_msg(
            parse(&format!(r#"{{"t":"delta","ticker":"{phi}","side":"ask","price":5700,"delta":-401,"ts_ms":4}}"#))
                .unwrap(),
            30,
        );
        assert!(app.feed.markets[phi].book.is_none(), "out-of-sync book is dropped");
        app.on_msg(parse(&format!(r#"{{"t":"book","ticker":"{phi}","bids":[],"asks":[],"ts_ms":5}}"#)).unwrap(), 40);
        app.on_msg(parse(r#"{"t":"reset","reason":"gap","ts_ms":6}"#).unwrap(), 50);
        assert_eq!(app.feed.books_live(), 0);
    }

    #[test]
    fn filter_and_selection_follow_ticker() {
        let mut app = loaded();
        app.on_key(key('G'), 10);
        assert_eq!(app.selected_market().unwrap().contract.subtitle, "Philadelphia");
        app.on_key(key('s'), 10); // volume: PHI traded, sorts first
        assert_eq!(app.markets.selected, 0);
        assert_eq!(app.selected_market().unwrap().contract.subtitle, "Philadelphia");
        app.on_key(key('/'), 10);
        for ch in "kansas".chars() {
            app.on_key(key(ch), 10);
        }
        assert_eq!(app.markets.rows, vec!["KXNFLGAME-26OCT19KCLV-KC"]);
        app.on_key(code(KeyCode::Esc), 10);
        assert_eq!(app.markets.rows.len(), 3);
        assert_eq!(app.mode, Mode::Normal);
    }

    #[test]
    fn hello_clears_state_and_link_down_drops_books() {
        let mut app = loaded();
        app.set_link(Link::Down("closed".into()));
        assert_eq!(app.feed.books_live(), 0);
        assert_eq!(app.feed.markets.len(), 3);
        app.on_msg(parse(r#"{"t":"hello","venue":"kalshi","env":"demo","source":"poll","version":1}"#).unwrap(), 0);
        app.tick(0);
        assert!(app.feed.markets.is_empty() && app.markets.rows.is_empty());
    }

    #[test]
    fn views_switch_and_keep_their_own_state() {
        let mut app = v2(100_000);
        app.on_key(key('4'), 10);
        assert_eq!(app.view, View::Slots);
        assert_eq!(app.slots.rows.len(), 2);
        app.on_key(code(KeyCode::Tab), 10);
        assert_eq!(app.view, View::Log);
        app.on_key(code(KeyCode::BackTab), 10);
        app.on_key(code(KeyCode::BackTab), 10);
        assert_eq!(app.view, View::Strategies);
        // Enter on a strategy shows only its slots.
        app.on_key(code(KeyCode::Enter), 10);
        assert_eq!((app.view, app.slots.strategy.as_deref()), (View::Slots, Some("mm")));
        assert_eq!(app.slots.rows, ["mm/DEMO-NFL-26OCT18HOUJAC-HOU"]);
        // Enter on a slot jumps to its market.
        app.on_key(code(KeyCode::Enter), 10);
        assert_eq!(app.view, View::Markets);
        assert_eq!(app.selected_market().unwrap().contract.ticker, "DEMO-NFL-26OCT18HOUJAC-HOU");
    }

    #[test]
    fn markets_traded_only_edge_sort_and_model_cycle() {
        let mut app = v2(100_000);
        assert_eq!(app.model(), Some("fair"));
        app.on_key(key('t'), 10);
        app.tick(100_000);
        // HOU has a working slot; B68's slot is halted but still a slot.
        assert_eq!(app.markets.rows, ["DEMO-NFL-26OCT18HOUJAC-HOU", "DEMO-HIGHNY-26OCT18-B68"]);
        app.on_key(key('t'), 10);
        app.on_key(key('m'), 10);
        assert_eq!(app.model(), Some("mid"));
        while app.markets.sort != Sort::Edge {
            app.on_key(key('s'), 10);
        }
        app.on_key(key('m'), 10);
        assert_eq!(app.markets.rows[0], "DEMO-NFL-26OCT18HOUJAC-HOU", "the only market with a fair");
    }

    #[test]
    fn mode_and_range_switch() {
        let mut app = v2(100_000);
        app.on_key(key('3'), 10);
        assert_eq!(app.visible_strategies(), [("mm".to_string(), "paper".to_string())]);
        app.on_key(key('p'), 10);
        assert_eq!(app.visible_strategies(), [("smoke".to_string(), "live".to_string())]);
        app.on_key(key('p'), 10);
        assert_eq!(app.visible_strategies().len(), 2);
        app.on_key(key(']'), 10);
        app.on_key(key(']'), 10);
        app.on_key(key(']'), 10);
        app.on_key(key(']'), 10);
        assert_eq!(app.range, Range::All);
        app.on_key(key('['), 10);
        assert_eq!(app.range, Range::Month);
    }

    #[test]
    fn log_pauses_scrolls_and_filters_by_level() {
        let mut app = v2(100_000);
        app.on_key(key('5'), 10);
        app.on_key(key('k'), 10);
        assert!(app.log.paused_at.is_some() && app.log.offset == 1);
        app.on_key(key('j'), 10);
        app.on_key(key('G'), 10);
        assert!(app.log.paused_at.is_none() && app.log.offset == 0);
        app.on_key(key('l'), 10);
        assert_eq!(app.log.min_level, Level::Warn);
        app.on_key(key(' '), 10);
        assert!(app.log.paused_at.is_some());
    }

    #[test]
    fn value_sorts_refresh_on_a_schedule_not_every_frame() {
        let mut app = v2(100_000);
        while app.markets.sort != Sort::Volume {
            app.on_key(key('s'), 10);
        }
        let (hou, jac) = ("DEMO-NFL-26OCT18HOUJAC-HOU", "DEMO-NFL-26OCT18HOUJAC-JAC");
        let print = |t: &str, q: i64| {
            parse(&format!(r#"{{"t":"trade","ticker":"{t}","price":4300,"qty":{q},"taker_side":"bid","ts_ms":9}}"#))
                .unwrap()
        };
        app.on_msg(print(jac, 90_000), 100_000);
        app.tick(100_100);
        assert_eq!(app.markets.rows[0], jac, "the last sort was long ago: re-sorted right away");
        app.on_msg(print(hou, 500_000), 100_200);
        app.tick(100_300);
        assert_eq!(app.markets.rows[0], jac, "at most one value re-sort per interval");
        app.tick(100_100 + RESORT_MS);
        assert_eq!(app.markets.rows[0], hou, "re-sorted once the interval passed");
        // Time sort doesn't depend on live values: prints never trigger a re-sort.
        while app.markets.sort != Sort::Time {
            app.on_key(key('s'), 10);
        }
        app.on_msg(print(jac, 1), 200_000);
        app.tick(300_000);
        assert!(app.feed.values_changed, "nothing to re-sort for");
    }

    #[test]
    fn heatmap_records_selected_and_traded_markets() {
        let app = v2(100_000);
        let hou = &app.feed.markets["DEMO-NFL-26OCT18HOUJAC-HOU"];
        assert_eq!(hou.heat.len(), 2, "traded: sampled each second");
        assert!(app.feed.markets["DEMO-NFL-26OCT18HOUJAC-JAC"].heat.is_empty());
    }
}
