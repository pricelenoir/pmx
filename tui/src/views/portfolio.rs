//! View 2 — Portfolio: how the whole book is doing and how much risk it carries. Paper
//! and live are never summed; BOTH puts them side by side.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, LineGauge, Paragraph, Row, Table, TableState};

use super::{Col, cell, fit, mode_badge, panel, signed_money, state_style, verdict_badge};
use crate::app::App;
use crate::charts::{Curve, draw_equity};
use crate::fmt;
use crate::proto::{LimitUsage, ScopeStats};
use crate::theme::Theme;

pub const KEYS: &[(&str, &str)] =
    &[("p", "paper · live · both"), ("[ ]", "range: today · 7d · 30d · all"), ("j/k", "scroll exposures")];

pub fn draw(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let Some(p) = &app.feed.portfolio else {
        let msg = "no portfolio yet — it comes from a running engine (`pmx run`, or `pmx tui --demo` to look around)";
        f.render_widget(Paragraph::new(Line::styled(msg, th.muted)).block(panel(" Portfolio ", th)), area);
        return;
    };
    let modes = app.mode_sel.modes();
    let backtest = app.feed.session.as_ref().is_some_and(|s| s.is_backtest());
    let kpi_h = 3 + modes.len() as u16;
    let bottom_h = if area.height >= 30 { area.height * 2 / 5 } else { area.height.saturating_sub(kpi_h) / 2 };
    let [kpi, middle, bottom] =
        Layout::vertical([Constraint::Length(kpi_h), Constraint::Fill(1), Constraint::Length(bottom_h)]).areas(area);
    let breaches = p.limits.iter().filter(|l| l.breached).count();
    let show_limits = !backtest && middle.width >= 110 && !p.limits.is_empty();
    draw_kpis(f, app, th, kpi, modes, if show_limits { 0 } else { breaches });

    let [chart, limits] = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(if show_limits { (middle.width * 2 / 5).min(64) } else { 0 }),
    ])
    .areas(middle);
    let curves: Vec<Curve> = modes
        .iter()
        .map(|&m| Curve {
            name: format!("{} equity", m),
            style: if m == "live" { th.down } else { th.accent },
            points: app.feed.equity_for("portfolio", m),
        })
        .collect();
    let title = Line::from(vec![
        Span::styled(" Equity ", th.title),
        Span::styled(format!("{} · {} · ", app.mode_sel.name(), app.range.name()), th.muted),
        Span::styled("── ", th.muted),
        Span::styled("high-water mark ", th.muted),
    ]);
    let start = app.range.start_ms(fmt::midnight(now_ms));
    draw_equity(f, chart, th, panel(title, th), &curves, &[], (start, now_ms));
    if limits.width > 0 {
        draw_limits(f, th, limits, &p.limits);
    }

    let show_alloc = bottom.width >= 120;
    let [exp, alloc] = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(if show_alloc { (bottom.width * 2 / 5).min(72) } else { 0 }),
    ])
    .areas(bottom);
    draw_exposures(f, app, th, exp, modes);
    if alloc.width > 0 {
        draw_allocation(f, app, th, alloc, modes);
    }
}

fn kpi_cols() -> [Col; 10] {
    [
        Col::new("", 7, false, 0),
        Col::new("Equity", 13, true, 0),
        Col::new("Today", 13, true, 0),
        Col::new("Capital at risk", 15, true, 1),
        Col::new("Worst case", 13, true, 1),
        Col::new("Drawdown", 13, true, 2),
        Col::new("Unrealized", 13, true, 3),
        Col::new("Realized", 13, true, 4),
        Col::new("EV (model)", 13, true, 5),
        Col::new("Fees", 11, true, 6),
    ]
}

fn draw_kpis(f: &mut Frame, app: &App, th: &Theme, area: Rect, modes: &[&str], hidden_breaches: usize) {
    let p = app.feed.portfolio.as_ref().expect("checked by draw");
    let cols = kpi_cols();
    let keep = fit(&cols, area.width.saturating_sub(2));
    let rows: Vec<Row> = modes
        .iter()
        .map(|&mode| {
            let Some(s) = p.scope("portfolio", mode) else {
                return Row::new([
                    cell(format!(" {} ", mode.to_uppercase()), th.mode(mode), false),
                    cell("no data", th.muted, false),
                ]);
            };
            let loss = |v: i64| {
                cell(
                    if v > 0 { format!("▼ -{}", fmt::money(v)) } else { "  $0.00".into() },
                    if v > 0 { th.down } else { th.text },
                    true,
                )
            };
            let signed = |v: i64| Cell::from(Line::from(signed_money(v, th)).right_aligned());
            let cells = [
                Cell::from(Line::from(mode_badge(mode, th))),
                signed(s.equity),
                signed(s.today),
                cell(fmt::money(s.capital_at_risk), th.text, true),
                signed(s.worst_case),
                loss(s.drawdown),
                signed(s.unrealized),
                signed(s.realized),
                s.ev_model.map_or(cell("-", th.muted, true), signed),
                cell(fmt::money(s.fees), th.muted, true),
            ];
            Row::new(cells.into_iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c))
        })
        .collect();
    let header = Row::new(cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| cell(c.name, th.header, c.right)));
    let widths: Vec<Constraint> = cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c.constraint()).collect();
    let mut title = vec![Span::styled(" Portfolio ", th.title)];
    if let Some(s) = modes.iter().filter_map(|m| p.scope("portfolio", m)).find(|s| s.stale) {
        title.push(Span::styled(format!("{} marks stale ", s.mode), th.warn));
    }
    if hidden_breaches > 0 {
        title.push(Span::styled(format!(" ■ {hidden_breaches} limit breached (widen for gauges) "), th.halt));
    }
    f.render_widget(
        Table::new(rows, widths).header(header).block(panel(Line::from(title), th)).column_spacing(1),
        area,
    );
}

fn limit_label(l: &LimitUsage) -> String {
    let what = match l.limit.as_str() {
        "max_capital_at_risk" => "capital at risk",
        "max_worst_case" => "worst case",
        "max_entity_risk" => "entity risk",
        "max_drawdown" => "drawdown",
        "max_loss" => "day loss",
        "max_position" => "position",
        "max_open_orders" => "open orders",
        other => other.strip_prefix("max_").unwrap_or(other),
    };
    let scope = l.scope.strip_prefix("strategy:").unwrap_or(&l.scope);
    format!("{scope} · {what}")
}

fn draw_limits(f: &mut Frame, th: &Theme, area: Rect, limits: &[LimitUsage]) {
    let block = panel(" Limits ", th);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let mut sorted: Vec<&LimitUsage> = limits.iter().collect();
    sorted.sort_by(|a, b| {
        let r = |l: &LimitUsage| if l.cap > 0 { l.used as f64 / l.cap as f64 } else { 0.0 };
        r(b).total_cmp(&r(a))
    });
    let label_w = 28.min(inner.width / 2);
    for (i, l) in sorted.iter().take(inner.height as usize).enumerate() {
        let row = Rect::new(inner.x, inner.y + i as u16, inner.width, 1);
        let [name, gauge, value] =
            Layout::horizontal([Constraint::Length(label_w), Constraint::Fill(1), Constraint::Length(9)]).areas(row);
        let ratio = if l.cap > 0 { (l.used as f64 / l.cap as f64).clamp(0.0, 1.0) } else { 0.0 };
        let style = if l.breached {
            th.error
        } else if ratio >= 0.7 {
            th.warn
        } else {
            th.ok
        };
        f.render_widget(Paragraph::new(Line::styled(fmt::trunc(&limit_label(l), label_w as usize - 1), th.text)), name);
        let money = l.limit != "max_open_orders" && l.limit != "max_position";
        let amount = |v: i64| if money { fmt::money_short(v) } else { v.to_string() };
        let shown = format!("{} / {}", amount(l.used), amount(l.cap));
        f.render_widget(
            LineGauge::default()
                .ratio(ratio)
                .filled_style(style)
                .unfilled_style(th.muted)
                .label(Span::styled(shown, th.muted)),
            gauge,
        );
        let tag = if l.breached { "BREACH".to_string() } else { format!("{:>3.0}%", ratio * 100.0) };
        f.render_widget(Paragraph::new(Line::styled(format!(" {tag}"), style)), value);
    }
}

/// Outcome PnL in the domain's order — or, when that doesn't fit `width`, worst first
/// with a count of what's left out, so the outcome that hurts is never the one hidden.
fn scenario_spans<'a>(scen: &[(String, i64)], width: usize, th: &Theme) -> Vec<Span<'a>> {
    let piece = |o: &str, v: i64| format!("{o} {}", fmt::money_short_signed(v));
    let full: usize = scen.iter().map(|(o, v)| piece(o, *v).chars().count() + 3).sum();
    let mut order: Vec<&(String, i64)> = scen.iter().collect();
    if full > width + 3 {
        order.sort_by_key(|(_, v)| *v);
    }
    let mut spans = vec![];
    let mut used = 0;
    for (i, (o, v)) in order.iter().enumerate() {
        let sep = if i > 0 { " │ " } else { "" };
        let w = sep.chars().count() + piece(o, *v).chars().count();
        let left = order.len() - i;
        let reserve = if left > 1 { 8 } else { 0 };
        if i > 0 && used + w + reserve > width {
            let more = if width.saturating_sub(used) >= 8 { format!(" +{left} more") } else { format!(" +{left}") };
            spans.push(Span::styled(more, th.muted));
            break;
        }
        used += w;
        spans.push(Span::styled(format!("{sep}{o} "), th.muted));
        spans.push(Span::styled(fmt::money_short_signed(*v), th.signed(*v as f64)));
    }
    spans
}

fn draw_exposures(f: &mut Frame, app: &App, th: &Theme, area: Rect, modes: &[&str]) {
    let p = app.feed.portfolio.as_ref().expect("checked by draw");
    let mut rows: Vec<_> = p.exposures.iter().filter(|e| modes.contains(&e.mode.as_str())).collect();
    rows.sort_by_key(|e| e.worst_case);
    let cols = [
        Col::new("", 7, false, if modes.len() > 1 { 0 } else { 255 }),
        Col::new("Entity", 26, false, 0),
        Col::new("Strategies", 12, false, 4),
        Col::new("At risk", 10, true, 1),
        Col::new("Worst", 10, true, 0),
        Col::new("PnL if…", 0, false, 2),
        Col::new("EV model", 10, true, 3),
        Col::new("EV mkt", 10, true, 3),
    ];
    let mut keep = fit(&cols, area.width.saturating_sub(2));
    for (k, c) in keep.iter_mut().zip(&cols) {
        *k &= c.prio != 255;
    }
    let fixed: u16 = cols.iter().zip(&keep).filter(|(c, k)| **k && c.width > 0).map(|(c, _)| c.width + 1).sum();
    let scen_w = area.width.saturating_sub(2 + fixed) as usize;
    let table_rows: Vec<Row> = rows
        .iter()
        .map(|e| {
            let name = app.feed.entities.get(&e.entity).map_or(e.entity.clone(), |en| en.summary.clone());
            let scen = scenario_spans(&e.scenarios, scen_w, th);
            let ev = |v: Option<i64>| {
                v.map_or(cell("-", th.muted, true), |v| cell(fmt::money_short_signed(v), th.signed(v as f64), true))
            };
            let cells = [
                Cell::from(Line::from(mode_badge(&e.mode, th))),
                cell(name, th.text, false),
                cell(e.strategies.join(","), th.muted, false),
                cell(fmt::money_short(e.capital_at_risk), th.text, true),
                cell(fmt::money_short_signed(e.worst_case), th.signed(e.worst_case as f64), true),
                Cell::from(Line::from(scen)),
                ev(e.ev_model),
                ev(e.ev_market),
            ];
            Row::new(cells.into_iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c))
        })
        .collect();
    let header = Row::new(cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| cell(c.name, th.header, c.right)));
    let widths: Vec<Constraint> = cols.iter().zip(&keep).filter(|(_, k)| **k).map(|(c, _)| c.constraint()).collect();
    let title = Line::from(vec![
        Span::styled(format!(" Exposure by entity {} ", rows.len()), th.title),
        Span::styled("sorted by worst case ", th.muted),
    ]);
    let table = Table::new(table_rows, widths)
        .header(header)
        .block(panel(title, th))
        .row_highlight_style(th.selected)
        .column_spacing(1);
    let sel = (!rows.is_empty()).then_some(app.exposure_scroll.min(rows.len().saturating_sub(1)));
    f.render_stateful_widget(table, area, &mut TableState::default().with_selected(sel));
}

fn draw_allocation(f: &mut Frame, app: &App, th: &Theme, area: Rect, modes: &[&str]) {
    let p = app.feed.portfolio.as_ref().expect("checked by draw");
    let strategies: Vec<(&ScopeStats, &str)> = p
        .scopes
        .iter()
        .filter(|s| modes.contains(&s.mode.as_str()))
        .filter_map(|s| Some((s, s.scope.strip_prefix("strategy:")?)))
        .collect();
    let rows = strategies.iter().map(|(s, id)| {
        let total_risk = p.scope("portfolio", &s.mode).map_or(0, |t| t.capital_at_risk);
        let share = if total_risk > 0 {
            format!("{:.0}%", 100.0 * s.capital_at_risk as f64 / total_risk as f64)
        } else {
            "-".into()
        };
        let verdict = app.feed.perf_for(id, &s.mode, app.range.name()).map(|pf| pf.verdict.verdict.as_str());
        let states: Vec<&str> = app
            .feed
            .slots
            .values()
            .filter(|sl| sl.strategy == *id && sl.mode == s.mode)
            .map(|sl| sl.state.as_str())
            .collect();
        let state = ["error", "halted", "active", "idle"].into_iter().find(|st| states.contains(st)).unwrap_or("-");
        Row::new([
            cell(id.to_string(), th.strong, false),
            Cell::from(Line::from(mode_badge(&s.mode, th))),
            cell(fmt::money_short_signed(s.today), th.signed(s.today as f64), true),
            cell(fmt::money_short_signed(s.equity), th.signed(s.equity as f64), true),
            cell(fmt::money_short(s.capital_at_risk), th.text, true),
            cell(share, th.muted, true),
            Cell::from(
                Line::from(verdict.map_or(Span::styled("-", th.muted), |v| verdict_badge(v, th, false))).centered(),
            ),
            cell(state, state_style(state, th), false),
        ])
    });
    let header = Row::new(
        [
            ("Strategy", false),
            ("", false),
            ("Today", true),
            ("Total", true),
            ("At risk", true),
            ("Risk%", true),
            ("V", false),
            ("State", false),
        ]
        .map(|(h, r)| cell(h, th.header, r)),
    );
    let widths = [
        Constraint::Fill(1),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(5),
        Constraint::Length(1),
        Constraint::Length(6),
    ];
    f.render_widget(Table::new(rows, widths).header(header).block(panel(" Allocation ", th)).column_spacing(1), area);
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::app::View;
    use crate::app::tests::v2;
    use crate::ui::tests::{NOW, SIZES, render};

    fn app() -> App {
        let mut app = v2(NOW);
        app.view = View::Portfolio;
        app
    }

    #[test]
    fn kpis_chart_limits_exposure_and_allocation() {
        let a = app();
        let s = render(&a, 200, 60);
        assert!(s.contains("Equity") && s.contains("Capital at risk") && s.contains("Worst case"));
        assert!(s.contains("▲ +$1.20"), "equity, signed with an arrow: {s}");
        assert!(s.contains("▼ -$2.75"), "worst case");
        assert!(s.contains("high-water mark"));
        assert!(s.contains("drawdown"));
        assert!(s.contains("Limits") && s.contains("smoke · day loss") && s.contains("BREACH"));
        assert!(s.contains("Q3 4:12 HOU 17-14 JAC"), "exposure labelled by the entity's summary");
        assert!(s.contains("HOU +$2.25") && s.contains("JAC -$2.75") && s.contains("tie -$2.75"), "scenarios: {s}");
        assert!(s.contains("Allocation") && s.contains("mm"));
        assert!(s.contains("$2.75 / $1.00k"), "gauges show used / cap");
        for (w, h) in SIZES {
            assert!(render(&a, w, h).contains("Equity"), "{w}x{h}");
        }
        let small = render(&a, 80, 24);
        assert!(small.contains("1 limit breached"), "breaches surface when gauges don't fit: {small}");
    }

    #[test]
    fn both_shows_paper_and_live_side_by_side() {
        let mut a = app();
        let k = |a: &mut App| a.on_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE), 10);
        k(&mut a);
        let live = render(&a, 200, 60);
        assert!(live.contains("LIVE") && live.contains("▼ -$0.13"), "live today");
        assert!(live.contains("live marks stale"));
        k(&mut a);
        let both = render(&a, 200, 60);
        let rows: Vec<&str> = both.lines().filter(|l| l.contains(" PAPER ") || l.contains(" LIVE ")).collect();
        assert!(rows.len() >= 2, "one KPI row per mode, never a sum");
    }

    #[test]
    fn scenarios_that_dont_fit_show_the_worst_first() {
        let th = Theme::new(false);
        let scen: Vec<(String, i64)> = [
            ("65° or below", 14_730_000),
            ("66° to 67°", 14_730_000),
            ("68° to 69°", 9_000_000),
            ("70° or above", -15_270_000),
        ]
        .map(|(o, v)| (o.to_string(), v))
        .to_vec();
        let text = |w| scenario_spans(&scen, w, &th).iter().map(|s| s.content.to_string()).collect::<String>();
        assert!(text(200).starts_with("65° or below +$14.73 │ 66° to 67°"), "domain order when it fits");
        let narrow = text(50);
        assert!(narrow.starts_with("70° or above -$15.27"), "{narrow}");
        assert!(narrow.ends_with("more") && narrow.chars().count() <= 50, "{narrow}");
        let tight = text(22);
        assert!(tight.starts_with("70° or above -$15.27") && tight.ends_with("+3"), "{tight}");
    }

    #[test]
    fn no_portfolio_says_where_it_comes_from() {
        let mut a = crate::app::tests::loaded();
        a.view = View::Portfolio;
        assert!(render(&a, 120, 40).contains("no portfolio yet"));
    }
}
