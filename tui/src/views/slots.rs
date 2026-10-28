//! View 4 — Slots: each strategy × market, with its own position, orders, fills, rejects
//! and logs.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};

use super::{Col, cell, fit, mode_badge, panel, position, state_style};
use crate::app::App;
use crate::fmt::{self, cents, qty};
use crate::proto::Side;
use crate::theme::Theme;

pub const KEYS: &[(&str, &str)] = &[
    ("j/k", "move"),
    ("/", "filter"),
    ("Enter", "its market"),
    ("s", "sort: slot · pnl · position"),
    ("Esc", "clear strategy / filter"),
];

pub fn draw(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    if app.feed.slots.is_empty() {
        let msg = "no slots — a slot is one strategy running on one market; they appear when the engine runs";
        f.render_widget(Paragraph::new(Line::styled(msg, th.muted)).block(panel(" Slots ", th)), area);
        return;
    }
    let detail_h = if area.height >= 30 { area.height * 2 / 5 } else { area.height / 2 };
    let [table, detail] = Layout::vertical([Constraint::Fill(1), Constraint::Length(detail_h)]).areas(area);
    draw_table(f, app, th, table);
    draw_detail(f, app, th, detail, now_ms);
}

fn draw_table(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let cols = [
        Col::new("Strategy", 10, false, 0),
        Col::new("Market", 0, false, 0),
        Col::new("Mode", 7, false, 0),
        Col::new("State", 7, false, 1),
        Col::new("Pos", 8, true, 0),
        Col::new("Work", 4, true, 3),
        Col::new("Realized", 10, true, 4),
        Col::new("Unreal", 10, true, 4),
        Col::new("Fees", 8, true, 5),
        Col::new("Net", 10, true, 1),
        Col::new("Last action", 26, false, 2),
    ];
    let keep = fit(&cols, area.width.saturating_sub(2));
    let rows: Vec<Row> = app
        .slots
        .rows
        .iter()
        .filter_map(|id| app.feed.slots.get(id))
        .map(|s| {
            let label = app.feed.markets.get(&s.ticker).map_or(s.ticker.as_str(), |m| m.contract.label());
            let money = |v: i64| cell(fmt::money_signed(v), th.signed(v as f64), true);
            let cells = [
                cell(s.strategy.clone(), th.strong, false),
                cell(label.to_string(), th.text, false),
                Cell::from(Line::from(mode_badge(&s.mode, th))),
                cell(s.state.clone(), state_style(&s.state, th), false),
                cell(position(s.position, &s.mode), if s.mode == "live" { th.live } else { th.text }, true),
                cell(s.working.to_string(), th.muted, true),
                money(s.realized),
                money(s.unrealized),
                cell(fmt::money(s.fees), th.muted, true),
                money(s.net()),
                cell(s.last_action.clone(), th.muted, false),
            ];
            Row::new(cells.into_iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c))
        })
        .collect();
    let header = Row::new(cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| cell(c.name, th.header, c.right)));
    let widths: Vec<Constraint> = cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c.constraint()).collect();
    let mut title = vec![
        Span::styled(format!(" Slots {}/{} ", app.slots.rows.len(), app.feed.slots.len()), th.title),
        Span::styled(format!("sort:{} ", app.slots.sort.name()), th.muted),
    ];
    if let Some(sid) = &app.slots.strategy {
        title.push(Span::styled(format!("strategy {sid} "), th.warn));
    }
    if !app.slots.filter.is_empty() {
        title.push(Span::styled(format!("/{} ", app.slots.filter), th.warn));
    }
    let table = Table::new(rows, widths)
        .header(header)
        .block(panel(Line::from(title), th))
        .row_highlight_style(th.selected)
        .column_spacing(1);
    let sel = (!app.slots.rows.is_empty()).then_some(app.slots.selected);
    f.render_stateful_widget(table, area, &mut TableState::default().with_selected(sel));
}

fn side_word(side: Side) -> (&'static str, fn(&Theme) -> ratatui::style::Style) {
    match side {
        Side::Bid => ("buy ", |th| th.bid),
        Side::Ask => ("sell", |th| th.ask),
    }
}

fn draw_detail(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let Some(s) = app.selected_slot() else {
        f.render_widget(Paragraph::new(Line::styled("no slot selected", th.muted)).block(panel(" Slot ", th)), area);
        return;
    };
    let n = area.height.saturating_sub(2) as usize;
    let orders: Vec<Line> = app
        .feed
        .working
        .values()
        .filter(|o| o.slot == s.slot)
        .take(n)
        .map(|o| {
            let (word, style) = side_word(o.side);
            Line::from(vec![
                Span::styled(format!("{word} "), style(th)),
                Span::styled(format!("{} @ {}", qty(o.qty - o.filled), cents(o.price)), th.text),
                Span::styled(format!("  {} · {}", o.status, fmt::duration(now_ms - o.ts_ms)), th.muted),
            ])
        })
        .collect();
    let fills: Vec<Line> = app
        .feed
        .slot_fills(&s.slot)
        .take(n)
        .map(|fl| {
            let (word, style) = side_word(fl.side);
            Line::from(vec![
                Span::styled(format!("{} ", fmt::clock(fl.ts_ms)), th.muted),
                Span::styled(format!("{word} "), style(th)),
                Span::styled(format!("{} @ {}", qty(fl.qty), cents(fl.price)), th.text),
                Span::styled(
                    format!(" {} fee {}", if fl.is_taker { "taker" } else { "maker" }, fmt::money(fl.fee)),
                    th.muted,
                ),
            ])
        })
        .collect();
    let mut notes: Vec<Line> = app
        .feed
        .slot_risks(&s.slot)
        .take(n)
        .map(|r| {
            Line::from(vec![
                Span::styled(format!("{} ", fmt::clock(r.ts_ms)), th.muted),
                Span::styled(format!("REJECT {} ", r.rule), th.warn),
                Span::styled(r.detail.clone(), th.text),
            ])
        })
        .collect();
    notes.extend(app.feed.slot_events(&s.slot).take(n.saturating_sub(notes.len())).map(|e| {
        Line::from(vec![
            Span::styled(format!("{} ", fmt::clock(e.ts_ms)), th.muted),
            Span::styled(e.text.clone(), th.text),
        ])
    }));
    let empty = |lines: Vec<Line<'static>>, what: &'static str| {
        if lines.is_empty() { vec![Line::styled(what, th.muted)] } else { lines }
    };
    let title = |name: &str| {
        Line::from(vec![Span::styled(format!(" {name} "), th.title), Span::styled(format!("{} ", s.slot), th.muted)])
    };
    let panes = [
        (title("Working"), empty(orders, "no working orders")),
        (title("Fills"), empty(fills, "no fills yet")),
        (title("Rejects + log"), empty(notes, "nothing logged")),
    ];
    let areas: Vec<Rect> = if area.width >= 120 {
        Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1), Constraint::Fill(1)]).split(area).to_vec()
    } else {
        Layout::horizontal([Constraint::Fill(1), Constraint::Fill(1)]).split(area).to_vec()
    };
    for ((t, lines), a) in panes.into_iter().zip(areas) {
        f.render_widget(Paragraph::new(lines).block(panel(t, th)), a);
    }
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{KeyCode, KeyEvent};

    use crate::app::View;
    use crate::app::tests::v2;
    use crate::ui::tests::{NOW, SIZES, render};

    #[test]
    fn slots_table_and_detail() {
        let mut app = v2(NOW);
        app.view = View::Slots;
        let s = render(&app, 200, 60);
        assert!(s.contains("Slots 2/2"));
        assert!(s.contains("mm") && s.contains("Houston") && s.contains("active") && s.contains("+5p"));
        assert!(s.contains("smoke") && s.contains("LIVE") && s.contains("halted"));
        assert!(s.contains("buy  5 @ 55¢"), "working order: {s}");
        assert!(s.contains("maker fee $0.00"), "fill");
        for (w, h) in SIZES {
            assert!(render(&app, w, h).contains("Houston"), "{w}x{h}");
        }
        app.on_key(KeyEvent::from(KeyCode::Char('j')), 10);
        let s = render(&app, 200, 60);
        assert!(s.contains("REJECT max_loss") && s.contains("slot halted: max_loss"), "rejects + logs: {s}");
    }
}
