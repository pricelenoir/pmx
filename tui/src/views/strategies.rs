//! View 3 — Strategies: is each strategy profitable, why, and how sure are we. Every
//! number comes from analytics (`perf` messages); nothing is computed here.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState, Wrap};

use super::{cell, mode_badge, panel, verdict_badge};
use crate::app::App;
use crate::charts::{Curve, SignedBars, draw_equity, hbar};
use crate::fmt;
use crate::proto::{Perf, PerfStats, Side};
use crate::theme::Theme;

pub const KEYS: &[(&str, &str)] = &[
    ("j/k", "strategy"),
    ("Enter", "its slots"),
    ("b", "breakdown tables"),
    ("p", "paper · live · both"),
    ("[ ]", "range: today · 7d · 30d · all"),
];

pub fn draw(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let list = app.visible_strategies();
    if list.is_empty() {
        let msg = format!(
            "no {} strategies yet — they appear when a running engine reports slots or performance",
            app.mode_sel.name().to_lowercase()
        );
        f.render_widget(Paragraph::new(Line::styled(msg, th.muted)).block(panel(" Strategies ", th)), area);
        return;
    }
    let list_w = (area.width / 3).clamp(24, 38);
    let [left, right] = Layout::horizontal([Constraint::Length(list_w), Constraint::Fill(1)]).areas(area);
    draw_list(f, app, th, left, &list);
    let Some((sid, mode)) = app.selected_strategy() else { return };
    let perf = app.feed.perf_for(&sid, &mode, app.range.name());
    let verdict_h =
        perf.map_or(3, |p| 3 + p.verdict.flags.len().min(2) as u16 + u16::from(!p.verdict.needed.is_empty()));
    let [verdict, body] = Layout::vertical([Constraint::Length(verdict_h), Constraint::Fill(1)]).areas(right);
    draw_verdict(f, app, th, verdict, &sid, &mode, perf);
    let Some(perf) = perf else {
        let msg = format!("no performance for {} yet — analytics publishes every 30 s", app.range.name());
        f.render_widget(Paragraph::new(Line::styled(msg, th.muted)).block(panel(" Performance ", th)), body);
        return;
    };
    let kinds = &perf.breakdowns;
    let pick = app.breakdown % (kinds.len() + 1);
    if pick > 0 {
        draw_breakdown(f, th, body, &kinds[pick - 1].0, &kinds[pick - 1].1);
        return;
    }
    let wide = body.width >= 90;
    let stats_w = if wide { (body.width * 2 / 5).clamp(46, 64) } else { body.width };
    if !wide {
        let chart_h = if body.height >= 30 { body.height / 2 } else { 0 };
        let [chart, stats] = Layout::vertical([Constraint::Length(chart_h), Constraint::Fill(1)]).areas(body);
        if chart_h > 0 {
            draw_cum(f, app, th, chart, &sid, &mode, now_ms);
        }
        draw_stats(f, th, stats, &perf.stats);
        return;
    }
    let [top, bottom] = Layout::vertical([Constraint::Fill(3), Constraint::Fill(2)]).areas(body);
    let [chart, stats] = Layout::horizontal([Constraint::Fill(1), Constraint::Length(stats_w)]).areas(top);
    let [daily, edge] = Layout::horizontal([Constraint::Fill(1), Constraint::Length(stats_w)]).areas(bottom);
    draw_cum(f, app, th, chart, &sid, &mode, now_ms);
    draw_stats(f, th, stats, &perf.stats);
    draw_daily(f, th, daily, &perf.stats);
    draw_edge(f, th, edge, perf);
}

fn draw_list(f: &mut Frame, app: &App, th: &Theme, area: Rect, list: &[(String, String)]) {
    let rows = list.iter().map(|(sid, mode)| {
        let perf = app.feed.perf_for(sid, mode, app.range.name());
        let net = perf.map(|p| p.stats.net);
        Row::new([
            cell(sid.clone(), th.strong, false),
            Cell::from(Line::from(mode_badge(mode, th))),
            net.map_or(cell("-", th.muted, true), |n| cell(fmt::money_short_signed(n), th.signed(n as f64), true)),
            Cell::from(Line::from(
                perf.map_or(Span::styled("-", th.muted), |p| verdict_badge(&p.verdict.verdict, th, false)),
            )),
        ])
    });
    let widths = [Constraint::Fill(1), Constraint::Length(7), Constraint::Length(9), Constraint::Length(1)];
    let title = Line::from(vec![
        Span::styled(" Strategies ", th.title),
        Span::styled(format!("{} · {} ", app.mode_sel.name(), app.range.name()), th.muted),
    ]);
    let table = Table::new(rows, widths).block(panel(title, th)).row_highlight_style(th.selected).column_spacing(1);
    let sel = Some(app.strategy_selected.min(list.len().saturating_sub(1)));
    f.render_stateful_widget(table, area, &mut TableState::default().with_selected(sel));
}

fn draw_verdict(f: &mut Frame, app: &App, th: &Theme, area: Rect, sid: &str, mode: &str, perf: Option<&Perf>) {
    let slots: Vec<_> = app.feed.slots.values().filter(|s| s.strategy == sid && s.mode == mode).collect();
    let active = slots.iter().filter(|s| s.state == "active").count();
    let halted = slots.iter().filter(|s| s.state == "halted" || s.state == "error").count();
    let mut head = vec![Span::styled(format!(" {sid} "), th.strong), mode_badge(mode, th), Span::raw("  ")];
    let mut lines = vec![];
    match perf {
        Some(p) => {
            head.push(verdict_badge(&p.verdict.verdict, th, true));
            head.push(Span::styled(format!("  {}", p.verdict.reason), th.text));
            lines.push(Line::from(head));
            for flag in p.verdict.flags.iter().take(2) {
                lines.push(Line::styled(format!(" ⚠ {flag}"), th.warn));
            }
            if !p.verdict.needed.is_empty() {
                lines.push(Line::styled(format!(" to decide: {}", p.verdict.needed), th.muted));
            }
        }
        None => {
            head.push(verdict_badge("inconclusive", th, true));
            head.push(Span::styled("  no data for this range", th.muted));
            lines.push(Line::from(head));
        }
    }
    let title = Line::from(vec![
        Span::styled(" Verdict ", th.title),
        Span::styled(format!("{} slots · {active} active", slots.len()), th.muted),
        Span::styled(if halted > 0 { format!(" · {halted} halted ") } else { " ".into() }, th.error),
    ]);
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }).block(panel(title, th)), area);
}

fn draw_cum(f: &mut Frame, app: &App, th: &Theme, area: Rect, sid: &str, mode: &str, now_ms: i64) {
    let prefix = format!("{sid}/");
    let fills: Vec<(i64, Side)> = app
        .feed
        .fills
        .iter()
        .filter(|fl| fl.slot.starts_with(&prefix) && fl.mode == mode)
        .map(|fl| (fl.ts_ms, fl.side))
        .collect();
    let scope = format!("strategy:{sid}");
    let curves = [Curve { name: "PnL".into(), style: th.accent, points: app.feed.equity_for(&scope, mode) }];
    let title = Line::from(vec![
        Span::styled(" Cumulative PnL ", th.title),
        Span::styled(format!("{} · ▲ buy ▼ sell ", app.range.name()), th.muted),
    ]);
    let start = app.range.start_ms(fmt::midnight(now_ms));
    draw_equity(f, area, th, panel(title, th), &curves, &fills, (start, now_ms));
}

fn opt<T>(v: Option<T>, f: impl Fn(T) -> String) -> String {
    v.map_or("-".into(), f)
}

fn draw_stats(f: &mut Frame, th: &Theme, area: Rect, s: &PerfStats) {
    let money = |v: i64| fmt::money_signed(v);
    let c = |v: f64| format!("{v:+.2}¢");
    let rows: Vec<(&str, Vec<Span>)> = vec![
        (
            "net",
            vec![
                Span::styled(money(s.net), th.signed(s.net as f64)),
                Span::styled(format!("  gross {}  fees {}", money(s.gross), fmt::money(s.fees)), th.muted),
            ],
        ),
        ("expectancy", {
            let mut v = vec![Span::styled(
                opt(s.expectancy_c, |e| format!("{}/contract", c(e))),
                s.expectancy_c.map_or(th.muted, |e| th.signed(e)),
            )];
            if let Some((lo, hi)) = s.expectancy_ci {
                v.push(Span::styled(format!("  95% CI [{lo:+.2}, {hi:+.2}]"), th.strong));
            }
            v
        }),
        ("P(edge ≤ 0)", vec![Span::styled(opt(s.p_nonpositive, fmt::pct), th.text)]),
        (
            "contracts",
            vec![Span::styled(
                format!("{}  fills {}  episodes {}", fmt::qty(s.contracts), s.fills, s.episodes),
                th.text,
            )],
        ),
        (
            "win rate",
            vec![Span::styled(
                format!("{}  profit factor {}", opt(s.win_rate, fmt::pct), opt(s.profit_factor, |v| format!("{v:.2}"))),
                th.text,
            )],
        ),
        (
            "avg win/loss",
            vec![
                Span::styled(opt(s.avg_win, money), th.up),
                Span::styled(" / ", th.muted),
                Span::styled(opt(s.avg_loss, money), th.down),
            ],
        ),
        (
            "edge entry",
            vec![Span::styled(
                format!(
                    "{} → realized {}  capture {}",
                    opt(s.edge_entry_c, c),
                    opt(s.edge_realized_c, c),
                    opt(s.capture, fmt::pct)
                ),
                th.text,
            )],
        ),
        (
            "markouts",
            s.markouts_c
                .iter()
                .flat_map(|(k, v)| {
                    [Span::styled(format!("{k} "), th.muted), Span::styled(format!("{} ", c(*v)), th.signed(*v))]
                })
                .collect(),
        ),
        (
            "max drawdown",
            vec![
                Span::styled(fmt::money(s.max_drawdown), th.down),
                Span::styled(format!("  under water {}", fmt::duration(s.time_under_water_ms)), th.muted),
            ],
        ),
        (
            "sharpe/day",
            vec![Span::styled(
                format!(
                    "{} ({} days)  days up {}",
                    opt(s.sharpe_day, |v| format!("{v:.2}")),
                    s.days,
                    opt(s.pct_days_positive, fmt::pct)
                ),
                th.text,
            )],
        ),
        (
            "brier",
            vec![Span::styled(
                format!(
                    "model {}  market {}",
                    opt(s.brier_model, |v| format!("{v:.4}")),
                    opt(s.brier_market, |v| format!("{v:.4}"))
                ),
                th.text,
            )],
        ),
        (
            "maker share",
            vec![Span::styled(
                format!("{}  fill rate {}", opt(s.maker_share, fmt::pct), opt(s.fill_rate, fmt::pct)),
                th.text,
            )],
        ),
        (
            "return/risk",
            vec![Span::styled(
                format!("{}  max at risk {}", opt(s.return_on_risk, fmt::pct), fmt::money_short(s.max_capital_at_risk)),
                th.text,
            )],
        ),
    ];
    let lines: Vec<Line> = rows
        .into_iter()
        .map(|(k, mut v)| {
            v.insert(0, Span::styled(format!("{k:>12} "), th.muted));
            Line::from(v)
        })
        .collect();
    f.render_widget(Paragraph::new(lines).block(panel(" Stats ", th)), area);
}

fn draw_daily(f: &mut Frame, th: &Theme, area: Rect, s: &PerfStats) {
    let values: Vec<i64> = s.daily.iter().map(|d| d.1).collect();
    let best = values.iter().copied().max().unwrap_or(0);
    let worst = values.iter().copied().min().unwrap_or(0);
    let title = Line::from(vec![
        Span::styled(format!(" Daily PnL {} days ", values.len()), th.title),
        Span::styled("best ", th.muted),
        Span::styled(fmt::money_short_signed(best), th.up),
        Span::styled(" worst ", th.muted),
        Span::styled(fmt::money_short_signed(worst), th.down),
        Span::raw(" "),
    ]);
    let block = panel(title, th);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if values.is_empty() {
        f.render_widget(Paragraph::new(Line::styled("no settled days yet", th.muted)), inner);
        return;
    }
    let bar_width = ((inner.width as usize / values.len()).saturating_sub(1)).clamp(1, 6) as u16;
    f.render_widget(SignedBars { values: &values, up: th.up, down: th.down, axis: th.border, bar_width }, inner);
}

fn draw_edge(f: &mut Frame, th: &Theme, area: Rect, perf: &Perf) {
    let block = panel(" Realized ¢/contract by predicted edge ", th);
    let inner = block.inner(area);
    f.render_widget(block, area);
    let Some((_, rows)) = perf.breakdowns.iter().find(|(k, _)| k == "edge") else {
        f.render_widget(Paragraph::new(Line::styled("no edge buckets (needs model fairs at entry)", th.muted)), inner);
        return;
    };
    let max = rows.iter().filter_map(|r| r.cpc).fold(0.0, |m: f64, v| m.max(v.abs()));
    let half = (inner.width as usize).saturating_sub(24) / 2;
    let lines: Vec<Line> = rows
        .iter()
        .map(|r| {
            let mut spans = vec![Span::styled(format!("{:>6} ", fmt::trunc(&r.key, 6)), th.muted)];
            spans.extend(hbar(r.cpc.unwrap_or(0.0), max, half.max(2), th));
            spans.push(Span::styled(
                format!(" {:>6}", opt(r.cpc, |v| format!("{v:+.1}¢"))),
                r.cpc.map_or(th.muted, |v| th.signed(v)),
            ));
            spans.push(Span::styled(format!(" n={}", r.fills), th.muted));
            Line::from(spans)
        })
        .collect();
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_breakdown(f: &mut Frame, th: &Theme, area: Rect, kind: &str, rows: &[crate::proto::BucketRow]) {
    let header = Row::new(
        [
            ("By ".to_string() + kind, false),
            ("Fills".into(), true),
            ("Contracts".into(), true),
            ("PnL".into(), true),
            ("¢/contract".into(), true),
            ("Edge at entry".into(), true),
        ]
        .map(|(h, r)| cell(h, th.header, r)),
    );
    let body = rows.iter().map(|r| {
        Row::new([
            cell(r.key.clone(), th.text, false),
            cell(r.fills.to_string(), th.muted, true),
            cell(fmt::qty(r.contracts), th.text, true),
            cell(fmt::money_signed(r.pnl), th.signed(r.pnl as f64), true),
            cell(opt(r.cpc, |v| format!("{v:+.2}¢")), r.cpc.map_or(th.muted, |v| th.signed(v)), true),
            cell(opt(r.edge_c, |v| format!("{v:.1}¢")), th.muted, true),
        ])
    });
    let widths = [
        Constraint::Fill(1),
        Constraint::Length(7),
        Constraint::Length(10),
        Constraint::Length(12),
        Constraint::Length(11),
        Constraint::Length(14),
    ];
    let title = Line::from(vec![
        Span::styled(format!(" Breakdown by {kind} "), th.title),
        Span::styled("[b] next ", th.accent),
    ]);
    f.render_widget(Table::new(body, widths).header(header).block(panel(title, th)).column_spacing(1), area);
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::app::View;
    use crate::app::tests::v2;
    use crate::ui::tests::{NOW, SIZES, render};

    fn key(app: &mut App, c: char) {
        app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE), 10);
    }

    fn app() -> App {
        let mut app = v2(NOW);
        app.view = View::Strategies;
        key(&mut app, ']'); // the fixture's perf is for 7d
        app
    }

    #[test]
    fn verdict_stats_charts_and_buckets() {
        let a = app();
        let s = render(&a, 200, 60);
        assert!(s.contains("? INCONCLUSIVE") && s.contains("95% CI [-1.5, +5.5]"), "verdict with its reason: {s}");
        assert!(s.contains("⚠ one entity drives > 50% of PnL"), "flags");
        assert!(s.contains("to decide: about 40 more entities"));
        assert!(
            s.contains("expectancy") && s.contains("+2.00¢/contract") && s.contains("95% CI [-1.50, +5.50]"),
            "{s}"
        );
        assert!(s.contains("Cumulative PnL") && s.contains("Daily PnL 2 days"));
        assert!(s.contains("2-4¢") && s.contains("+1.0¢") && s.contains("n=8"), "edge buckets");
        for (w, h) in SIZES {
            let s = render(&a, w, h);
            assert!(s.contains("INCONCLUSIVE") && s.contains("expectancy"), "{w}x{h}: {s}");
        }
    }

    #[test]
    fn breakdowns_cycle_and_ranges_without_data_say_so() {
        let mut a = app();
        key(&mut a, 'b');
        assert!(render(&a, 160, 50).contains("Breakdown by edge"));
        key(&mut a, 'b');
        assert!(render(&a, 160, 50).contains("Breakdown by market"));
        key(&mut a, 'b');
        assert!(render(&a, 160, 50).contains("Cumulative PnL"), "back to charts");
        key(&mut a, '[');
        assert!(render(&a, 160, 50).contains("no performance for today yet"));
    }
}
