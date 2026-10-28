//! View 1 — Markets. Left: the market list (sized to its rows, scrolling inside), then the
//! selected contract's detail, its slots, and an activity tape of prints and your fills.
//! Right: the L2 book with bids and asks side by side, best prices on top, and a chart that
//! cycles price / depth / heatmap.

use std::collections::HashMap;

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Margin, Rect};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Cell, Chart, Dataset, GraphType, LegendPosition, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Table, TableState,
};

use super::{Col, cell, fit, mode_badge, panel, position, state_style};
use crate::app::{App, ChartMode, Link, Sort};
use crate::charts::{Heatmap, bounds};
use crate::feed::Market;
use crate::fmt::{self, cents, cents_f, qty, qty_short, trunc};
use crate::proto::Side;
use crate::theme::Theme;

pub const KEYS: &[(&str, &str)] = &[
    ("j/k", "move"),
    ("/", "filter"),
    ("v", "chart: price · depth · heatmap"),
    ("m", "edge vs next model"),
    ("t", "only markets being traded"),
    ("s", "sort: time · volume · spread · edge · ticker"),
    ("c", "show / hide closed"),
];

const CHART_WINDOW_MS: i64 = 60 * 60 * 1000;
/// Depth and imbalance look this far from each side's best.
const NEAR: i64 = 500;

pub fn draw(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    if app.feed.markets.is_empty() {
        draw_waiting(f, app, th, area);
        return;
    }
    let [left, right] = Layout::horizontal([Constraint::Percentage(50), Constraint::Fill(1)]).areas(area);
    draw_left(f, app, th, left, now_ms);
    draw_right(f, app, th, right, now_ms);
}

/// Market list (sized to its rows, at most ~45% of the column, scrolling inside), then the
/// selected contract's detail, its slots, and venue positions.
fn draw_left(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let feed = &app.feed;
    let m = app.selected_market();
    let list_h = (app.markets.rows.len() as u16 + 3).clamp(6, (area.height * 45 / 100).max(6));
    let here = m.map_or(0, |m| feed.slots_on(&m.contract.ticker).count()) as u16;
    let slots_h = match (feed.slots.is_empty(), here) {
        (true, _) => 0,
        (false, 0) => 3,
        (false, n) => n.min(5) + 3,
    };
    let positions = feed.account.as_ref().map_or(0, |a| a.positions.len()) as u16;
    let pos_h = if positions > 0 && area.height >= 34 { positions.min(5) + 3 } else { 0 };
    let lines = m.map(|m| detail_lines(app, m, th, area.width.saturating_sub(2) as usize, now_ms));
    let want = lines.as_ref().map_or(3, |l| l.len() as u16 + 2);
    // The detail takes what it needs; leftover height becomes the activity tape.
    let avail = area.height.saturating_sub(list_h + slots_h + pos_h);
    let (detail_h, tape_h) = if avail >= want + 6 { (want, avail - want) } else { (avail, 0) };
    let [list, detail, slots, tape, pos] = Layout::vertical([
        Constraint::Length(list_h),
        Constraint::Length(detail_h),
        Constraint::Length(slots_h),
        Constraint::Length(tape_h),
        Constraint::Length(pos_h),
    ])
    .areas(area);
    draw_table(f, app, th, list, now_ms);
    let block = panel(" Contract ", th);
    match lines {
        Some(lines) => f.render_widget(Paragraph::new(lines).block(block), detail),
        None => f.render_widget(Paragraph::new(Line::styled("no contract selected", th.muted)).block(block), detail),
    }
    if slots_h > 0 {
        draw_market_slots(f, app, m, th, slots);
    }
    if tape_h > 0 {
        draw_tape(f, app, m, th, tape);
    }
    if pos_h > 0 {
        draw_positions(f, app, th, pos);
    }
}

/// The book on top, the chart below.
fn draw_right(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let m = app.selected_market();
    let h = area.height;
    let chart_h = if h >= 30 {
        h * 45 / 100
    } else if h >= 22 {
        10
    } else {
        0
    };
    let [book, bottom] = Layout::vertical([Constraint::Fill(1), Constraint::Length(chart_h)]).areas(area);
    draw_ladder(f, app, m, th, book, now_ms);
    if chart_h == 0 {
        return;
    }
    draw_chart(f, app, m, th, bottom, now_ms);
}

fn draw_waiting(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let lines = match &app.link {
        Link::Up => vec![Line::styled("connected — waiting for markets…", th.muted)],
        _ => vec![
            Line::styled(format!("waiting for the feed at {}", app.socket), th.text),
            Line::default(),
            Line::from(vec![
                Span::styled("start it with ", th.muted),
                Span::styled("pmx up", th.accent),
                Span::styled(" (or ", th.muted),
                Span::styled("pmx up --demo", th.accent),
                Span::styled("); this viewer reconnects on its own", th.muted),
            ]),
        ],
    };
    let y = area.y + area.height / 2 - 1;
    let r = Rect::new(area.x, y, area.width, 3.min(area.height));
    f.render_widget(Paragraph::new(lines).alignment(Alignment::Center), r);
}

fn draw_table(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let feed = &app.feed;
    let model = app.model();
    let any_traded = feed.markets.keys().any(|t| feed.traded(t));
    let any_linked = !feed.entities.is_empty();
    let mut cols = vec![
        Col::new("Event", 14, false, 9),
        Col::new("Contract", 0, false, 0),
        Col::new("Pos", 10, true, if any_traded { 1 } else { 255 }),
        Col::new("Bid", 6, true, 0),
        Col::new("Ask", 6, true, 0),
        Col::new("Sprd", 5, true, 3),
        Col::new("Last", 7, true, 4),
    ];
    let first_model = cols.len();
    for _ in &feed.models {
        cols.push(Col::new("", 6, true, 6));
    }
    cols.push(Col::new("Edge", 6, true, if model.is_some() { 2 } else { 255 }));
    cols.push(Col::new("Linked", 24, false, if any_linked { 5 } else { 255 }));
    cols.push(Col::new("Vol", 7, true, 7));
    cols.push(Col::new("Ends", 7, true, 8));
    let mut keep = fit(&cols, area.width.saturating_sub(2));
    for (k, c) in keep.iter_mut().zip(&cols) {
        *k &= c.prio != 255;
    }
    let header_names: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if i >= first_model && i < first_model + feed.models.len() {
                trunc(&feed.models[i - first_model], 6)
            } else if c.name == "Edge" {
                model.map_or("Edge".into(), |m| format!("Δ{}", trunc(m, 5)))
            } else {
                c.name.to_string()
            }
        })
        .collect();

    // Only the rows in the visible window are built, so cost follows the screen, not the
    // number of markets. The window moves just enough to keep the selection in view.
    let visible = (area.height.saturating_sub(3) as usize).max(1);
    let (n, sel) = (app.markets.rows.len(), app.markets.selected);
    let mut off = app.markets.offset.get().min(n.saturating_sub(visible));
    if sel < off {
        off = sel;
    } else if sel >= off + visible {
        off = sel + 1 - visible;
    }
    app.markets.offset.set(off);
    let mut prev_event = "";
    let rows: Vec<Row> = app.markets.rows[off..(off + visible).min(n)]
        .iter()
        .filter_map(|t| feed.markets.get(t))
        .map(|m| {
            let c = &m.contract;
            let book = m.book.as_ref();
            let dim = if book.is_none() || !c.is_open() { th.muted } else { th.text };
            let mut cells: Vec<Cell> = vec![];
            let ev = c.event_code();
            let first = app.markets.sort != Sort::Time || ev != prev_event;
            prev_event = ev;
            cells.push(cell(if first { ev.to_string() } else { String::new() }, th.muted, false));
            let slots: Vec<_> = feed.slots_on(&c.ticker).collect();
            let mut label = c.label().to_string();
            if !slots.is_empty() {
                label = format!("●{} {label}", slots.len());
            }
            cells.push(cell(label, dim, false));
            let mut pos: Vec<String> =
                slots.iter().filter(|s| s.position != 0).map(|s| position(s.position, &s.mode)).collect();
            if let Some(p) = feed.position(&c.ticker).filter(|&p| p != 0) {
                pos.push(format!("{}{}V", if p > 0 { "+" } else { "" }, qty(p)));
            }
            let live = slots.iter().any(|s| s.mode == "live" && s.position != 0);
            cells.push(cell(pos.join(" "), if live { th.live } else { th.strong }, true));
            let px = |lvl: Option<(i64, i64)>| lvl.map_or("-".into(), |(p, _)| cents(p));
            let (bid, ask) = m.bbo();
            cells.push(cell(px(bid), if book.is_some() { th.bid } else { th.muted }, true));
            cells.push(cell(px(ask), if book.is_some() { th.ask } else { th.muted }, true));
            cells.push(cell(bid.zip(ask).map_or("-".into(), |(b, a)| cents(a.0 - b.0)), dim, true));
            let (arrow, style) = match m.tick {
                1 => ("▲", th.up),
                -1 => ("▼", th.down),
                _ => (" ", dim),
            };
            cells.push(cell(m.last().map_or("-".into(), |t| format!("{}{arrow}", cents(t.price))), style, true));
            for (i, name) in feed.models.iter().enumerate() {
                let v = m.fairs.get(name).map_or("-".into(), |fc| format!("{:.1}", fc.p * 100.0));
                cells.push(cell(v, th.series(i), true));
            }
            let edge = model.and_then(|md| m.edge(md));
            cells.push(cell(
                edge.map_or("-".into(), |e| fmt::signed_cents(e).trim_end_matches('¢').to_string()),
                edge.map_or(th.muted, |e| th.signed(e)),
                true,
            ));
            let linked = feed.entity_for(&c.ticker).map_or(String::new(), |e| e.summary.clone());
            cells.push(cell(linked, th.muted, false));
            cells.push(cell(if m.volume > 0 { qty_short(m.volume) } else { "-".into() }, dim, true));
            let ends = match (c.is_open(), c.ends_ts()) {
                (false, _) => c.status.clone(),
                (true, Some(ts)) => fmt::until(ts, now_ms),
                (true, None) => "-".into(),
            };
            cells.push(cell(ends, th.muted, true));
            Row::new(cells.into_iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c))
        })
        .collect();

    let header = Row::new(
        cols.iter()
            .zip(&header_names)
            .zip(&keep)
            .filter(|(_, k)| **k)
            .map(|((c, n), _)| cell(n.clone(), th.header, c.right)),
    );
    let widths: Vec<Constraint> = cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c.constraint()).collect();
    let mut title =
        vec![Span::styled(format!(" Markets {}/{} ", app.markets.rows.len(), feed.markets.len()), th.title)];
    title.push(Span::styled(format!("sort:{} ", app.markets.sort.name()), th.muted));
    if app.markets.traded_only {
        title.push(Span::styled("traded only ", th.warn));
    }
    if !app.markets.filter.is_empty() {
        title.push(Span::styled(format!("/{} ", app.markets.filter), th.warn));
    }
    let table = Table::new(rows, widths)
        .header(header)
        .block(panel(Line::from(title), th))
        .row_highlight_style(th.selected)
        .column_spacing(1);
    let sel = (n > 0).then_some(sel - off);
    f.render_stateful_widget(table, area, &mut TableState::default().with_selected(sel));
    if app.markets.rows.len() > visible {
        let bar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None)
            .track_style(th.border)
            .thumb_style(th.accent);
        let mut state = ScrollbarState::new(app.markets.rows.len()).position(app.markets.selected);
        f.render_stateful_widget(bar, area.inner(Margin { vertical: 2, horizontal: 0 }), &mut state);
    }
}

fn draw_positions(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let Some(acct) = &app.feed.account else { return };
    let rows = acct.positions.iter().map(|p| {
        let m = app.feed.markets.get(&p.ticker);
        let label = m.map_or(p.ticker.as_str(), |m| m.contract.label());
        let (side, style) = if p.qty >= 0 { ("YES", th.bid) } else { ("NO", th.ask) };
        let mid = m.and_then(Market::mid).map_or("-".into(), cents_f);
        Row::new([
            Cell::from(label.to_string()),
            Cell::from(Line::styled(format!("{side} {}", qty(p.qty.abs())), style).right_aligned()),
            Cell::from(Line::from(mid).right_aligned()),
            Cell::from(Line::styled(fmt::money(p.realized_pnl - p.fees), th.muted).right_aligned()),
        ])
    });
    let header = Row::new(["Contract", "Position", "Mid", "Realized"].map(|h| Cell::from(Line::styled(h, th.header))));
    let widths = [Constraint::Fill(1), Constraint::Length(12), Constraint::Length(7), Constraint::Length(11)];
    let table = Table::new(rows, widths).header(header).block(panel(" Venue positions ", th));
    f.render_widget(table, area);
}

/// `bid ███████░░░ ask`: bid share of nearby depth, by symbol as well as color.
fn imbalance_bar<'a>(imb: f64, width: usize, th: &Theme) -> Vec<Span<'a>> {
    let n = (((imb + 1.0) / 2.0) * width as f64).round() as usize;
    vec![
        Span::styled("bid ", th.muted),
        Span::styled("█".repeat(n.min(width)), th.bid),
        Span::styled("░".repeat(width - n.min(width)), th.ask),
        Span::styled(" ask ", th.muted),
        Span::styled(format!("{:+.0}%", imb * 100.0), th.signed(imb)),
    ]
}

fn detail_lines<'a>(app: &'a App, m: &'a Market, th: &Theme, w: usize, now_ms: i64) -> Vec<Line<'a>> {
    let feed = &app.feed;
    let c = &m.contract;
    let status = match &c.result {
        Some(r) => format!("{} → {}", c.status, r.to_uppercase()),
        None => c.status.clone(),
    };
    let ends = match c.ends_ts() {
        Some(ts) if ts > now_ms => format!("  resolves ~{} (in {})", fmt::date_time(ts), fmt::until(ts, now_ms)),
        Some(ts) => format!("  resolved ~{}", fmt::date_time(ts)),
        None => String::new(),
    };
    let mut lines = vec![Line::styled(trunc(&c.title, w), th.strong)];
    let id = vec![Span::styled(c.ticker.clone(), th.accent), Span::styled(format!("  {status}"), th.muted)];
    // Narrow panels get the resolution time on its own line instead of clipped.
    if c.ticker.chars().count() + status.chars().count() + ends.chars().count() + 2 <= w {
        lines.push(Line::from([id, vec![Span::styled(ends, th.muted)]].concat()));
    } else {
        lines.push(Line::from(id));
        if !ends.is_empty() {
            lines.push(Line::styled(ends.trim_start().to_string(), th.muted));
        }
    }
    match &m.book {
        Some(b) => {
            let lvl = |l: Option<(i64, i64)>| l.map_or("-".into(), |(p, q)| format!("{} × {}", cents(p), qty_short(q)));
            lines.push(Line::from(vec![
                Span::styled("bid ", th.muted),
                Span::styled(lvl(b.best_bid()), th.bid),
                Span::styled("   ask ", th.muted),
                Span::styled(lvl(b.best_ask()), th.ask),
                Span::styled("   mid ", th.muted),
                Span::styled(b.mid().map_or("-".into(), cents_f), th.strong),
                Span::styled("   spread ", th.muted),
                Span::styled(b.spread().map_or("-".into(), cents), th.text),
            ]));
            let mut micro = vec![
                Span::styled("micro ", th.muted),
                Span::styled(b.microprice().map_or("-".into(), |p| format!("{:.1}¢", p / 100.0)), th.text),
                Span::raw("   "),
            ];
            if let Some(imb) = b.imbalance(NEAR) {
                micro.extend(imbalance_bar(imb, if w >= 70 { 12 } else { 6 }, th));
            }
            let (bd, ad) = b.depth_within(NEAR);
            micro.push(Span::styled(format!("   ±5¢ {} / {}", qty_short(bd), qty_short(ad)), th.muted));
            if w >= 80 {
                let age = now_ms - m.book_rx_ms;
                micro.push(Span::styled(format!("   updated {} ago", fmt::duration(age)), th.muted));
            }
            lines.push(Line::from(micro));
        }
        None => lines.push(Line::styled("waiting for a book snapshot…", th.warn)),
    }
    let (bought, sold) = m.flow(now_ms - 5 * 60_000);
    lines.push(Line::from(vec![
        Span::styled("5m flow ", th.muted),
        Span::styled(format!("▲ {}", qty_short(bought)), th.up),
        Span::styled(" bought  ", th.muted),
        Span::styled(format!("▼ {}", qty_short(sold)), th.down),
        Span::styled(" sold   last ", th.muted),
        Span::styled(m.last().map_or("-".into(), |t| cents(t.price)), th.text),
        Span::styled("   vol ", th.muted),
        Span::styled(qty_short(m.volume), th.text),
        Span::styled(if w >= 90 { " since connect" } else { "" }, th.muted),
    ]));
    if !m.fairs.is_empty() {
        let mut spans = vec![Span::styled("models ", th.muted)];
        for (i, name) in feed.models.iter().enumerate() {
            let Some(fc) = m.fairs.get(name) else { continue };
            spans.push(Span::styled(format!("{name} {}", fmt::prob(fc.p)), th.series(i)));
            if let Some(e) = m.edge(name) {
                spans.push(Span::styled(format!(" {}", fmt::signed_cents(e)), th.signed(e)));
            }
            spans.push(Span::raw("   "));
        }
        lines.push(Line::from(spans));
    }
    for a in feed.advice_on(&c.ticker, now_ms).into_iter().take(2) {
        let mut spans = vec![Span::styled("advice ", th.muted), Span::styled(format!("{} ", a.advisor), th.accent)];
        if let Some(p) = a.p {
            spans.push(Span::styled(format!("{} ", fmt::prob(p)), th.strong));
        }
        if let Some(d) = &a.direction {
            spans.push(Span::styled(format!("{d} "), th.strong));
        }
        let left = fmt::duration(a.expires_ms - now_ms);
        spans.push(Span::styled(format!("conf {:.2} · {left} left · ", a.confidence), th.muted));
        let used: usize = spans.iter().map(|s| s.width()).sum();
        spans.push(Span::styled(trunc(&a.rationale, w.saturating_sub(used)), th.text));
        lines.push(Line::from(spans));
    }
    if let Some(e) = feed.entity_for(&c.ticker) {
        lines.push(Line::from(vec![Span::styled("◆ ", th.accent), Span::styled(trunc(&e.summary, w - 2), th.strong)]));
        let per_line = if w >= 90 { 2 } else { 1 };
        for chunk in e.fields.chunks(per_line) {
            let mut spans = vec![];
            for (k, v) in chunk {
                spans.push(Span::styled(format!("  {k} "), th.muted));
                spans.push(Span::styled(trunc(v, w / per_line - k.len() - 4), th.text));
            }
            lines.push(Line::from(spans));
        }
    }
    lines
}

/// The selected market's slots, one row each.
fn draw_market_slots(f: &mut Frame, app: &App, m: Option<&Market>, th: &Theme, area: Rect) {
    let slots: Vec<_> = m.map_or(vec![], |m| app.feed.slots_on(&m.contract.ticker).collect());
    let title =
        Line::from(vec![Span::styled(" Slots on this market ", th.title), Span::styled("4 all slots ", th.muted)]);
    if slots.is_empty() {
        let msg = Line::styled("none — no strategy is running on this market", th.muted);
        f.render_widget(Paragraph::new(msg).block(panel(title, th)), area);
        return;
    }
    let cols = [
        Col::new("", 7, false, 0),
        Col::new("Strategy", 0, false, 0),
        Col::new("State", 7, false, 1),
        Col::new("Pos", 8, true, 0),
        Col::new("Work", 4, true, 3),
        Col::new("Net", 9, true, 0),
        Col::new("Last action", 22, false, 2),
    ];
    let keep = fit(&cols, area.width.saturating_sub(2));
    let rows = slots.iter().map(|s| {
        let cells = [
            Cell::from(Line::from(mode_badge(&s.mode, th))),
            cell(s.strategy.clone(), th.strong, false),
            cell(s.state.clone(), state_style(&s.state, th), false),
            cell(position(s.position, &s.mode), if s.mode == "live" { th.live } else { th.text }, true),
            cell(s.working.to_string(), th.muted, true),
            cell(fmt::money_signed(s.net()), th.signed(s.net() as f64), true),
            cell(s.last_action.clone(), th.muted, false),
        ];
        Row::new(cells.into_iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c))
    });
    let header = Row::new(cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| cell(c.name, th.header, c.right)));
    let widths: Vec<Constraint> = cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c.constraint()).collect();
    f.render_widget(Table::new(rows, widths).header(header).block(panel(title, th)).column_spacing(1), area);
}

/// Bar growing rightward, in eighths of a cell.
fn bar(frac: f64, width: usize) -> String {
    const PARTS: [&str; 8] = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"];
    let eighths = (frac.clamp(0.0, 1.0) * width as f64 * 8.0).round() as usize;
    let mut s = "█".repeat(eighths / 8);
    s.push_str(PARTS[eighths % 8]);
    format!("{s:<width$}")
}

/// Bar growing leftward from the right edge, in halves of a cell (`▐` is the only
/// right-aligned partial block).
fn bar_leftward(frac: f64, width: usize) -> String {
    let halves = (frac.clamp(0.0, 1.0) * width as f64 * 2.0).round() as usize;
    let s = format!("{}{}", if halves % 2 == 1 { "▐" } else { "" }, "█".repeat(halves / 2));
    format!("{s:>width$}")
}

/// L2 book, bids left and asks right on shared rows, best prices on the top row and depth
/// going down; size bars grow outward from the center line.
fn draw_ladder(f: &mut Frame, app: &App, m: Option<&Market>, th: &Theme, area: Rect, now_ms: i64) {
    let mut title = vec![Span::styled(" Book · YES ", th.title)];
    if let Some(b) = m.and_then(|m| m.book.as_ref()) {
        title.push(Span::styled(
            match (b.spread(), b.mid(), b.microprice()) {
                (Some(s), Some(mid), Some(mp)) => {
                    format!("spread {} · mid {} · micro {:.1}¢ ", cents(s), cents_f(mid), mp / 100.0)
                }
                _ => "one-sided ".into(),
            },
            th.muted,
        ));
    }
    let block = panel(Line::from(title), th);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let Some((m, book)) = m.and_then(|m| Some((m, m.book.as_ref()?))) else {
        f.render_widget(Paragraph::new(Line::styled("no book", th.muted)).alignment(Alignment::Center), inner);
        return;
    };
    // Our working orders by level; paper orders aren't in the venue's book, so their
    // levels are added if missing.
    let mut mine: HashMap<(Side, i64), (i64, bool)> = HashMap::new();
    for o in app.feed.working_on(&m.contract.ticker) {
        let e = mine.entry((o.side, o.price)).or_default();
        e.0 += o.qty - o.filled;
        e.1 |= o.mode == "live";
    }
    let rows = inner.height.saturating_sub(1) as usize;
    let side_levels = |side: Side| -> Vec<(i64, i64)> {
        let mut lv: Vec<(i64, i64)> = book.levels(side).collect();
        for &(s, p) in mine.keys() {
            if s == side && !lv.iter().any(|&(lp, _)| lp == p) {
                lv.push((p, 0));
            }
        }
        lv.sort_by_key(|&(p, _)| if side == Side::Bid { -p } else { p });
        lv.truncate(rows);
        lv
    };
    let (bids, asks) = (side_levels(Side::Bid), side_levels(Side::Ask));
    let max_q = bids.iter().chain(&asks).map(|&(_, q)| q).max().unwrap_or(1).max(1) as f64;
    let half = (inner.width as usize).saturating_sub(1) / 2;
    let show_mine = !mine.is_empty() && half >= 22;
    let show_cum = half >= 32;
    // per half: [mine 6][cum 8][size 8][bar][space][flash][price 6], mirrored for asks
    let fixed = 8 + 8 + if show_cum { 8 } else { 0 } + if show_mine { 6 } else { 0 };
    let bar_w = half.saturating_sub(fixed);
    let last = m.last().map(|t| t.price);

    let gutter = |side: Side, p: i64| match (m.flash_at(side, p, now_ms), mine.get(&(side, p))) {
        (Some(d), _) if d > 0 => Span::styled("▴", th.up),
        (Some(_), _) => Span::styled("▾", th.down),
        // Without room for the "mine" column, our levels are marked here instead.
        (None, Some(&(_, live))) if !show_mine => Span::styled("●", if live { th.live } else { th.paper }),
        (None, _) if last == Some(p) => Span::styled("◆", th.accent),
        (None, _) => Span::raw(" "),
    };
    let mine_cell = |side: Side, p: i64| match mine.get(&(side, p)) {
        Some(&(q, live)) => Span::styled(
            format!(" {:>4}{}", qty(q), if live { "L" } else { "p" }),
            if live { th.live } else { th.paper },
        ),
        None => Span::raw("      "),
    };
    let half_row = |side: Side, lvl: Option<(i64, i64)>, cum: i64, best: bool| -> Vec<Span> {
        let Some((p, q)) = lvl else { return vec![Span::raw(" ".repeat(half))] };
        let mut px_style = if side == Side::Bid { th.bid } else { th.ask };
        if best {
            px_style = px_style.add_modifier(ratatui::style::Modifier::BOLD);
        }
        let size_style = if m.flash_at(side, p, now_ms).is_some() { th.strong } else { th.text };
        let bar_style = if side == Side::Bid { th.bid } else { th.ask };
        let frac = q as f64 / max_q;
        let shown = if q > 0 { qty_short(q) } else { "-".into() };
        let size = if side == Side::Bid {
            Span::styled(format!("{shown:>7} "), size_style)
        } else {
            Span::styled(format!(" {shown:>7}"), size_style)
        };
        let cum = show_cum.then(|| Span::styled(format!(" {:>7}", qty_short(cum)), th.muted));
        let own = show_mine.then(|| mine_cell(side, p));
        if side == Side::Bid {
            let mut out: Vec<Span> = own.into_iter().chain(cum).collect();
            out.push(size);
            out.push(Span::styled(bar_leftward(frac, bar_w), bar_style));
            out.push(Span::raw(" "));
            out.push(gutter(side, p));
            out.push(Span::styled(format!("{:>6}", cents(p)), px_style));
            out
        } else {
            let mut out = vec![
                Span::styled(format!("{:<6}", cents(p)), px_style),
                gutter(side, p),
                Span::raw(" "),
                Span::styled(bar(frac, bar_w), bar_style),
                size,
            ];
            out.extend(cum.into_iter().chain(own));
            out
        }
    };
    let pad = " ".repeat(bar_w);
    let opt = |on: bool, w: usize, s: &str| if on { format!("{s:>w$}") } else { String::new() };
    let bid_head = format!("{}{}{:>7} {pad}{:>8}", opt(show_mine, 6, "mine"), opt(show_cum, 8, "cum"), "size", "BID");
    let ask_head = format!("{:<8}{pad}{:>8}{}{}", "ASK", "size", opt(show_cum, 8, "cum"), opt(show_mine, 6, "mine"));
    let mut lines = vec![Line::from(vec![
        Span::styled(bid_head, th.header),
        Span::styled("│", th.border),
        Span::styled(ask_head, th.header),
    ])];
    let (mut bid_cum, mut ask_cum) = (0, 0);
    for i in 0..rows.min(bids.len().max(asks.len())) {
        let (b, a) = (bids.get(i).copied(), asks.get(i).copied());
        bid_cum += b.map_or(0, |l| l.1);
        ask_cum += a.map_or(0, |l| l.1);
        let mut spans = half_row(Side::Bid, b, bid_cum, i == 0);
        spans.push(Span::styled("│", th.border));
        spans.extend(half_row(Side::Ask, a, ask_cum, i == 0));
        lines.push(Line::from(spans));
    }
    f.render_widget(Paragraph::new(lines), inner);
}

/// Public prints and our own fills on this market, newest first. Ours are labelled with
/// their mode so they stand out without color.
fn draw_tape(f: &mut Frame, app: &App, m: Option<&Market>, th: &Theme, area: Rect) {
    let title = Line::from(vec![Span::styled(" Activity ", th.title), Span::styled("prints + your fills ", th.muted)]);
    let block = panel(title, th);
    let Some(m) = m else {
        f.render_widget(Paragraph::new(Line::styled("no contract selected", th.muted)).block(block), area);
        return;
    };
    let n = area.height.saturating_sub(2) as usize;
    let mine = app.feed.fills.iter().rev().filter(|fl| fl.ticker == m.contract.ticker).take(n);
    let mut events: Vec<(i64, Row)> = m
        .trades
        .iter()
        .rev()
        .take(n)
        .map(|t| {
            let (arrow, style) = match t.taker_side {
                Side::Bid => ("▲ buy", th.bid),
                Side::Ask => ("▼ sell", th.ask),
            };
            let row = Row::new([
                Cell::from(Line::styled(fmt::clock(t.ts_ms), th.muted)),
                Cell::from(Line::styled(arrow, style)),
                Cell::from(Line::styled(cents(t.price), style).right_aligned()),
                Cell::from(Line::from(qty(t.qty)).right_aligned()),
                Cell::from(""),
            ]);
            (t.ts_ms, row)
        })
        .collect();
    events.extend(mine.map(|fl| {
        let mode = if fl.mode.is_empty() { "venue" } else { fl.mode.as_str() };
        let (arrow, style) = match fl.side {
            Side::Bid => ("▲ YOU bought", th.up),
            Side::Ask => ("▼ YOU sold", th.down),
        };
        let row = Row::new([
            Cell::from(Line::styled(fmt::clock(fl.ts_ms), th.muted)),
            Cell::from(Line::styled(arrow, style)),
            Cell::from(Line::styled(cents(fl.price), style).right_aligned()),
            Cell::from(Line::styled(qty(fl.qty), th.strong).right_aligned()),
            Cell::from(Line::from(vec![mode_badge(mode, th), Span::styled(format!(" {}", fl.slot), th.muted)])),
        ]);
        (fl.ts_ms, row)
    }));
    if events.is_empty() {
        f.render_widget(Paragraph::new(Line::styled("no prints since connect", th.muted)).block(block), area);
        return;
    }
    events.sort_by_key(|(ts, _)| std::cmp::Reverse(*ts));
    let rows = events.into_iter().take(n).map(|(_, r)| r);
    let widths = [
        Constraint::Length(8),
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(8),
        Constraint::Fill(1),
    ];
    f.render_widget(Table::new(rows, widths).block(block).column_spacing(1), area);
}

fn chart_title<'a>(mode: ChartMode, th: &Theme) -> Line<'a> {
    let mut spans = vec![Span::styled(" Chart ", th.title), Span::styled("[v] ", th.accent)];
    for c in [ChartMode::Price, ChartMode::Depth, ChartMode::Heat] {
        spans.push(Span::styled(format!("{} ", c.name()), if c == mode { th.strong } else { th.muted }));
    }
    Line::from(spans)
}

fn draw_chart(f: &mut Frame, app: &App, m: Option<&Market>, th: &Theme, area: Rect, now_ms: i64) {
    let block = panel(chart_title(app.markets.chart, th), th);
    let Some(m) = m else {
        f.render_widget(Paragraph::new(Line::styled("no contract selected", th.muted)).block(block), area);
        return;
    };
    match app.markets.chart {
        ChartMode::Price => draw_price(f, app, m, th, block, area, now_ms),
        ChartMode::Depth => draw_depth(f, m, th, block, area),
        ChartMode::Heat => {
            let inner = block.inner(area);
            f.render_widget(block, area);
            if m.heat.is_empty() {
                let msg = "recording… (one column per second while this market is selected or traded)";
                f.render_widget(Paragraph::new(Line::styled(msg, th.muted)), inner);
                return;
            }
            let prints: Vec<(i64, i64, Side)> = m.prints.iter().copied().collect();
            f.render_widget(Heatmap { cols: &m.heat, prints: &prints, tick: 100, th }, inner);
        }
    }
}

fn draw_price(
    f: &mut Frame,
    app: &App,
    m: &Market,
    th: &Theme,
    block: ratatui::widgets::Block,
    area: Rect,
    now_ms: i64,
) {
    let since = now_ms - CHART_WINDOW_MS;
    let x = |ts: i64| (ts - now_ms) as f64 / 60_000.0;
    let cents_of = |p: f64| p / 100.0;
    let mut mids: Vec<(f64, f64)> =
        m.history.iter().filter(|&&(t, _)| t >= since).map(|&(t, p)| (x(t), cents_of(p))).collect();
    // Carry the current mid to "now" so a quiet market still draws a line.
    if let Some(&(_, p)) = m.history.back()
        && m.book.is_some()
    {
        mids.push((0.0, cents_of(p)));
    }
    type Fair<'a> = (usize, &'a String, Vec<(f64, f64)>);
    let fairs: Vec<Fair> = app
        .feed
        .models
        .iter()
        .enumerate()
        .filter_map(|(i, name)| {
            let h = m.fair_history.get(name)?;
            let mut pts: Vec<(f64, f64)> =
                h.iter().filter(|&&(t, _)| t >= since).map(|&(t, p)| (x(t), p * 100.0)).collect();
            if let Some(&(_, p)) = h.back() {
                pts.push((0.0, p * 100.0));
            }
            Some((i, name, pts))
        })
        .collect();
    let prints = |side: Side| -> Vec<(f64, f64)> {
        m.prints
            .iter()
            .filter(|&&(t, _, s)| s == side && t >= since)
            .map(|&(t, p, _)| (x(t), p as f64 / 100.0))
            .collect()
    };
    let (buys, sells) = (prints(Side::Bid), prints(Side::Ask));
    let fills = |side: Side| -> Vec<(f64, f64)> {
        app.feed
            .fills
            .iter()
            .filter(|fl| fl.ticker == m.contract.ticker && fl.side == side && fl.ts_ms >= since)
            .map(|fl| (x(fl.ts_ms), fl.price as f64 / 100.0))
            .collect()
    };
    let (my_buys, my_sells) = (fills(Side::Bid), fills(Side::Ask));
    let all = || mids.iter().chain(&buys).chain(&sells).chain(fairs.iter().flat_map(|(_, _, p)| p));
    let Some([lo, hi]) = bounds(all().map(|p| p.1), 0.0, 2.0) else {
        f.render_widget(Paragraph::new(Line::styled("no price history in the last hour", th.muted)).block(block), area);
        return;
    };
    let (lo, hi) = ((lo - 1.0).max(0.0).floor(), (hi + 1.0).min(100.0).ceil());
    let x0 = all().map(|p| p.0).fold(0.0, f64::min).min(-1.0);

    let mut datasets = vec![
        Dataset::default()
            .name("book mid")
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(th.accent)
            .data(&mids),
    ];
    for (i, name, pts) in &fairs {
        datasets.push(
            Dataset::default()
                .name(name.to_string())
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(th.series(*i))
                .data(pts),
        );
    }
    datasets.push(Dataset::default().marker(Marker::Dot).graph_type(GraphType::Scatter).style(th.bid).data(&buys));
    datasets.push(Dataset::default().marker(Marker::Dot).graph_type(GraphType::Scatter).style(th.ask).data(&sells));
    if !my_buys.is_empty() {
        datasets.push(
            Dataset::default()
                .name("my buy")
                .marker(Marker::Custom('▲'))
                .graph_type(GraphType::Scatter)
                .style(th.up)
                .data(&my_buys),
        );
    }
    if !my_sells.is_empty() {
        datasets.push(
            Dataset::default()
                .name("my sell")
                .marker(Marker::Custom('▼'))
                .graph_type(GraphType::Scatter)
                .style(th.down)
                .data(&my_sells),
        );
    }
    let chart = Chart::new(datasets)
        .block(block)
        .legend_position(Some(LegendPosition::TopLeft))
        .hidden_legend_constraints((Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)))
        .x_axis(
            Axis::default()
                .bounds([x0, 0.0])
                .labels([Span::styled(format!("{x0:.0}m"), th.muted), Span::styled("now", th.muted)])
                .style(th.border),
        )
        .y_axis(
            Axis::default()
                .bounds([lo, hi])
                .labels([Span::styled(format!("{lo:.0}¢"), th.muted), Span::styled(format!("{hi:.0}¢"), th.muted)])
                .style(th.border),
        );
    f.render_widget(chart, area);
}

/// Cumulative resting size outward from the touch, as steps (x = price ¢, y = contracts).
fn depth_steps(levels: impl Iterator<Item = (i64, i64)>) -> Vec<(f64, f64)> {
    let mut out = vec![];
    let mut cum = 0.0;
    for (p, q) in levels {
        let x = p as f64 / 100.0;
        out.push((x, cum));
        cum += q as f64 / 100.0;
        out.push((x, cum));
    }
    out
}

fn draw_depth(f: &mut Frame, m: &Market, th: &Theme, block: ratatui::widgets::Block, area: Rect) {
    let Some(book) = &m.book else {
        f.render_widget(Paragraph::new(Line::styled("no book", th.muted)).block(block), area);
        return;
    };
    let Some(mid) = book.mid() else {
        f.render_widget(Paragraph::new(Line::styled("one-sided book", th.muted)).block(block), area);
        return;
    };
    let near = |&(p, _): &(i64, i64)| (p as f64 - mid).abs() <= 2_500.0;
    let bids = depth_steps(book.levels(Side::Bid).filter(near));
    let asks = depth_steps(book.levels(Side::Ask).filter(near));
    let top = bids.iter().chain(&asks).map(|p| p.1).fold(1.0, f64::max) * 1.05;
    let xs = bids.iter().chain(&asks).map(|p| p.0);
    let [x_lo, x_hi] = bounds(xs, 0.02, 2.0).unwrap_or([0.0, 100.0]);
    let mid_line = [(mid / 100.0, 0.0), (mid / 100.0, top)];
    let datasets = vec![
        Dataset::default().name("bids").marker(Marker::Braille).graph_type(GraphType::Line).style(th.bid).data(&bids),
        Dataset::default().name("asks").marker(Marker::Braille).graph_type(GraphType::Line).style(th.ask).data(&asks),
        Dataset::default().marker(Marker::Dot).graph_type(GraphType::Line).style(th.muted).data(&mid_line),
    ];
    let chart = Chart::new(datasets)
        .block(block)
        .legend_position(Some(LegendPosition::TopRight))
        .x_axis(
            Axis::default()
                .bounds([x_lo, x_hi])
                .labels([
                    Span::styled(format!("{x_lo:.0}¢"), th.muted),
                    Span::styled(format!("mid {:.1}¢", mid / 100.0), th.muted),
                    Span::styled(format!("{x_hi:.0}¢"), th.muted),
                ])
                .style(th.border),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, top])
                .labels([Span::styled("0", th.muted), Span::styled(qty_short(top.round() as i64 * 100), th.muted)])
                .style(th.border),
        );
    f.render_widget(chart, area);
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::app::tests::{loaded, v2};
    use crate::ui::tests::{NOW, SIZES, render};

    fn key(app: &mut App, c: char) {
        app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE), 10);
    }

    #[test]
    fn v1_layout_still_renders() {
        let mut app = loaded();
        key(&mut app, 'G');
        let big = render(&app, 120, 40);
        assert!(big.contains("Philadelphia"));
        assert!(big.contains("55¢") && big.contains("57¢"));
        assert!(big.contains("spread 2¢ · mid 56¢"));
    }

    #[test]
    fn markets_show_models_edge_linked_data_and_slots() {
        let app = v2(NOW);
        let s = render(&app, 200, 60);
        assert!(s.contains("fair") && s.contains("Δfair"), "per-model and edge columns");
        assert!(s.contains("●1 Houston"), "slot marker on the row");
        assert!(s.contains("+5p"), "paper position");
        assert!(s.contains("Q3 4:12 HOU 17-14 JAC"), "linked entity summary");
        assert!(s.contains("possession") && s.contains("HOU, 3rd & 7"), "entity fields in the detail");
        assert!(s.contains("models fair 58.2¢ +1.7¢   mid 56.0¢ -0.5¢"), "every model vs mid: {s}");
        assert!(s.contains("micro") && s.contains("bid ") && s.contains(" ask "), "microstructure line");
        assert!(s.contains("mine") && s.contains("5p"), "our working order on the ladder");
        assert!(s.contains("PAPER") && s.contains("quote 55¢ / 57¢"), "slot line");
        for (w, h) in SIZES {
            let s = render(&app, w, h);
            assert!(s.contains("Houston"), "{w}x{h}");
            assert!(s.contains("YES"), "ladder at {w}x{h}");
        }
    }

    #[test]
    fn ladder_marks_flashes_and_the_last_print() {
        let mut app = v2(NOW);
        let hou = "DEMO-NFL-26OCT18HOUJAC-HOU";
        app.on_msg(
            crate::proto::parse(&format!(
                r#"{{"t":"delta","ticker":"{hou}","side":"bid","price":5400,"delta":500,"ts_ms":9}}"#
            ))
            .unwrap(),
            NOW - 200,
        );
        let s = render(&app, 200, 60);
        assert!(s.contains("▴   54¢│"), "a level that just grew: {s}");
        assert!(s.contains("◆   56¢│"), "last print marker");
    }

    #[test]
    fn book_puts_both_sides_side_by_side_best_on_top() {
        let app = v2(NOW);
        let s = render(&app, 200, 60);
        let lines: Vec<&str> = s.lines().collect();
        let head = lines.iter().position(|l| l.contains("BID│ASK")).expect("bid and ask headers meet in the middle");
        assert!(lines[head + 1].contains("56¢│57¢"), "best bid and best ask share the top row: {}", lines[head + 1]);
        assert!(lines[head + 2].contains("55¢│58¢"), "depth goes down both sides: {}", lines[head + 2]);
        assert!(lines[head + 3].contains("54¢│"), "a side with more levels keeps going");
        assert!(lines[head + 1].contains("5p"), "our working bid is on its level");
    }

    #[test]
    fn left_column_stacks_list_contract_slots_activity() {
        let app = v2(NOW);
        let s = render(&app, 200, 60);
        let lines: Vec<&str> = s.lines().collect();
        let row_of = |needle: &str| {
            lines
                .iter()
                .position(|l| l.find(needle).is_some_and(|c| c < 100))
                .unwrap_or_else(|| panic!("{needle} in the left half"))
        };
        let (list, contract, slots, activity) =
            (row_of("Markets 3/3"), row_of("Contract"), row_of("Slots on this market"), row_of("Activity"));
        assert!(list < contract && contract < slots && slots < activity, "{list} {contract} {slots} {activity}");
        assert!(contract - list <= 7, "the list is only as tall as its rows");
        assert!(
            s.contains("YOU bought") && s.contains("mm/DEMO-NFL-26OCT18HOUJAC-HOU"),
            "our fill in the activity tape"
        );
    }

    #[test]
    fn long_market_lists_scroll_inside_a_short_box() {
        let mut app = v2(NOW);
        for i in 0..60 {
            let t = format!("DEMO-X-{i:02}");
            app.on_msg(crate::app::tests::contract(&t, "DEMO-X", &format!("Thing {i:02}"), 9_000_000_000_000 + i), NOW);
        }
        app.tick(NOW);
        app.on_key(KeyEvent::new(KeyCode::Char('G'), KeyModifiers::NONE), 10);
        let s = render(&app, 200, 60);
        let lines: Vec<&str> = s.lines().collect();
        let list = lines.iter().position(|l| l.contains("Markets 63/63")).unwrap();
        let contract =
            lines.iter().position(|l| l.chars().take(100).collect::<String>().contains("╭ Contract")).unwrap();
        assert!(contract - list <= 26, "the list takes at most ~45% of the column");
        assert!(lines[list..contract].iter().any(|l| l.contains("Thing 59")), "the selection scrolls into view");
        assert!(lines[list..contract].iter().any(|l| l.contains('█')), "a scrollbar thumb shows there is more");
    }

    #[test]
    fn narrow_detail_wraps_instead_of_clipping() {
        let app = v2(NOW);
        let s = render(&app, 120, 40);
        assert!(s.contains("│DEMO-NFL-26OCT18HOUJAC-HOU  open"), "{s}");
        assert!(s.contains("│resolves ~"), "resolution time on its own line: {s}");
    }

    #[test]
    fn narrow_book_marks_our_levels_in_the_gutter() {
        let app = v2(NOW);
        let s = render(&app, 80, 24);
        assert!(s.contains("●   55¢│"), "our 55¢ bid, marked where the price is: {s}");
        assert!(!s.contains("mine"), "no room for the column at this width");
    }

    #[test]
    fn chart_cycles_price_depth_heatmap() {
        let mut app = v2(NOW);
        let s = render(&app, 200, 60);
        assert!(s.contains("[v] price depth heatmap"));
        key(&mut app, 'v');
        let s = render(&app, 200, 60);
        assert!(s.contains("mid 56.5¢") && s.contains("bids"), "depth chart: {s}");
        key(&mut app, 'v');
        let s = render(&app, 200, 60);
        assert!(s.contains('▓') || s.contains('█') || s.contains('▒'), "heatmap cells");
    }

    #[test]
    fn traded_only_hides_everything_else() {
        let mut app = v2(NOW);
        key(&mut app, 't');
        app.tick(NOW);
        let s = render(&app, 120, 40);
        assert!(s.contains("Markets 2/3") && s.contains("traded only"), "{s}");
    }
}
