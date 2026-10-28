//! The frame: header (tabs, health, PnL), the active view, footer (keys), help overlay.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Clear, Paragraph};

use crate::app::{App, Link, Mode, View};
use crate::fmt;
use crate::theme::Theme;
use crate::views::{self, panel};

pub const MIN_W: u16 = 60;
pub const MIN_H: u16 = 15;

/// Rows visible in a view's main table, for page-up/down.
pub fn page_size(area: Rect) -> usize {
    area.height.saturating_sub(7) as usize
}

pub fn draw(f: &mut Frame, app: &App, th: &Theme, now_ms: i64) {
    let area = f.area();
    if area.width < MIN_W || area.height < MIN_H {
        let msg = format!("terminal too small: {}x{} (need {MIN_W}x{MIN_H})", area.width, area.height);
        f.render_widget(Paragraph::new(msg).style(th.warn).alignment(Alignment::Center), area);
        return;
    }
    let [tabs, status, body, footer] =
        Layout::vertical([Constraint::Length(1), Constraint::Length(1), Constraint::Fill(1), Constraint::Length(1)])
            .areas(area);
    draw_tabs(f, app, th, tabs, now_ms);
    draw_status(f, app, th, status);
    match app.view {
        View::Markets => views::markets::draw(f, app, th, body, now_ms),
        View::Portfolio => views::portfolio::draw(f, app, th, body, now_ms),
        View::Strategies => views::strategies::draw(f, app, th, body, now_ms),
        View::Slots => views::slots::draw(f, app, th, body, now_ms),
        View::Log => views::log::draw(f, app, th, body),
    }
    draw_footer(f, app, th, footer);
    if app.mode == Mode::Help {
        draw_help(f, app, th, area);
    }
}

fn draw_tabs(f: &mut Frame, app: &App, th: &Theme, area: Rect, now_ms: i64) {
    let mut spans = vec![Span::styled(" pmx ", th.title)];
    if let Some(s) = &app.feed.session {
        if s.is_demo() {
            spans.push(Span::styled(" DEMO · invented data ", th.demo));
            spans.push(Span::raw(" "));
        } else if s.is_backtest() {
            spans.push(Span::styled(format!(" BACKTEST {} ", s.run), th.backtest));
            spans.push(Span::raw(" "));
        }
    }
    let long = area.width >= 100;
    for v in View::ALL {
        let i = View::ALL.iter().position(|&x| x == v).unwrap_or(0) + 1;
        let label = format!(" {i} {} ", if long { v.name() } else { v.short() });
        spans.push(Span::styled(label, if v == app.view { th.tab_active } else { th.tab }));
    }
    let right = Line::from(Span::styled(fmt::clock(now_ms) + " ", th.text));
    let [l, r] = Layout::horizontal([Constraint::Fill(1), Constraint::Length(right.width() as u16)]).areas(area);
    f.render_widget(Paragraph::new(Line::from(spans)), l);
    f.render_widget(Paragraph::new(right), r);
}

/// Feed + source health, paper and live PnL today (never summed), any halt. `detail` 3
/// shows everything; lower levels drop the session text, then names of healthy sources,
/// then the "today" labels — PnL, down sources, and halts always stay.
fn status_spans<'a>(app: &'a App, th: &Theme, detail: u8) -> Vec<Span<'a>> {
    let feed = &app.feed;
    let mut spans = vec![Span::raw(" ")];
    let (dot, text) = match &app.link {
        Link::Up => (th.ok, "feed".to_string()),
        Link::Connecting => (th.warn, "connecting".to_string()),
        Link::Down(e) => (th.error, format!("feed down: {e}")),
    };
    spans.push(Span::styled("● ", dot));
    spans.push(Span::styled(text, th.text));
    if let Some(s) = feed.session.as_ref().filter(|_| detail >= 3) {
        spans.push(Span::styled(format!(" {}·{}·{}", s.venue, s.env, s.source), th.muted));
    }
    // Sources: ● up, ○ down — shape and color; a down source is always named.
    let venue = feed.session.as_ref().map_or("venue", |s| s.venue.as_str());
    let sources = feed
        .upstream
        .iter()
        .map(|(up, _)| (venue, *up))
        .chain(feed.sources.iter().map(|(n, (up, _))| (n.as_str(), *up)));
    spans.push(Span::raw(" "));
    for (name, up) in sources {
        spans.push(Span::styled(if up { " ●" } else { " ○" }, if up { th.ok } else { th.error }));
        if !up || detail >= 2 {
            spans.push(Span::styled(format!(" {name}"), if up { th.muted } else { th.error }));
        }
    }
    if let Some(p) = &feed.portfolio {
        for mode in ["paper", "live"] {
            if let Some(s) = p.scope("portfolio", mode) {
                spans.push(Span::raw("  "));
                spans.push(views::mode_badge(mode, th));
                spans.push(Span::raw(" "));
                spans.push(views::signed_money_short(s.today, th));
                if detail >= 1 {
                    spans.push(Span::styled(" today", th.muted));
                }
            }
        }
    }
    let halted = feed.halts.len();
    if halted > 0 {
        spans.push(Span::styled(format!("  ■ {halted} halted"), th.error));
    }
    if feed.dropped > 0 {
        spans.push(Span::styled(format!("  ⚠ {} dropped", feed.dropped), th.warn));
    }
    spans
}

fn draw_status(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let feed = &app.feed;
    if let Some(h) = feed.global_halt() {
        let text = format!(" ■ HALTED — everything: {} ", h.reason);
        f.render_widget(Paragraph::new(Line::styled(text, th.halt)), area);
        return;
    }
    let left = (0..=3)
        .rev()
        .map(|d| Line::from(status_spans(app, th, d)))
        .find(|l| l.width() as u16 <= area.width)
        .unwrap_or_else(|| Line::from(status_spans(app, th, 0)));
    let mut right = format!("{} mkts · {} books · {} msg/s ", feed.markets.len(), feed.books_live(), app.msg_rate);
    if app.debug {
        let p = &app.perf;
        right += &format!(
            "· frame {:.1}/{:.1} ms · apply {:.2} µs ",
            p.frame_avg().as_secs_f64() * 1e3,
            p.frame_max.as_secs_f64() * 1e3,
            p.apply_us()
        );
    }
    let right = Line::styled(right, th.muted);
    let rw = if left.width() as u16 + right.width() as u16 <= area.width { right.width() as u16 } else { 0 };
    let [l, r] = Layout::horizontal([Constraint::Fill(1), Constraint::Length(rw)]).areas(area);
    f.render_widget(Paragraph::new(left), l);
    if rw > 0 {
        f.render_widget(Paragraph::new(right), r);
    }
}

/// Keys for the active view, then the global ones.
pub fn view_keys(view: View) -> &'static [(&'static str, &'static str)] {
    match view {
        View::Markets => views::markets::KEYS,
        View::Portfolio => views::portfolio::KEYS,
        View::Strategies => views::strategies::KEYS,
        View::Slots => views::slots::KEYS,
        View::Log => views::log::KEYS,
    }
}

fn draw_footer(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let left: Vec<Span> = if app.mode == Mode::Filter {
        let text = match app.view {
            View::Markets => &app.markets.filter,
            View::Slots => &app.slots.filter,
            _ => &app.log.filter,
        };
        vec![
            Span::styled(" /", th.accent),
            Span::styled(text.clone(), th.strong),
            Span::styled("█", th.accent),
            Span::styled("   Enter", th.accent),
            Span::styled(" keep  ", th.muted),
            Span::styled("Esc", th.accent),
            Span::styled(" clear", th.muted),
        ]
    } else {
        // Help and quit always show; the view's keys fill what room is left.
        let width = |keys: &[(&str, &str)]| {
            keys.iter().map(|(k, w)| k.chars().count() + w.chars().count() + 3).sum::<usize>() + 1
        };
        let global: &[(&str, &str)] = if area.width >= 100 {
            &[("1-5", "view"), ("?", "help"), ("q", "quit")]
        } else {
            &[("?", "help"), ("q", "quit")]
        };
        let mut keys: Vec<(&str, &str)> = vec![];
        for &k in view_keys(app.view).iter().take(5) {
            keys.push(k);
            if width(&keys) + width(global) > area.width as usize {
                keys.pop();
                break;
            }
        }
        let mut spans = vec![Span::raw(" ")];
        for (k, what) in keys.iter().chain(global) {
            spans.push(Span::styled(*k, th.accent));
            spans.push(Span::styled(format!(" {what}  "), th.muted));
        }
        spans
    };
    let left = Line::from(left);
    let lw = (left.width() as u16).min(area.width);
    let right = match &app.toast {
        Some((t, _)) => Line::from(Span::styled(format!("{t} "), th.warn)),
        None => Line::default(),
    };
    let [l, r] = Layout::horizontal([Constraint::Length(lw), Constraint::Fill(1)]).areas(area);
    f.render_widget(Paragraph::new(left), l);
    f.render_widget(Paragraph::new(right).alignment(Alignment::Right), r);
}

fn draw_help(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let global = [
        ("1 … 5  Tab", "Markets · Portfolio · Strategies · Slots · Log"),
        ("j / k  ↓ / ↑", "move selection"),
        ("g / G  PgUp / PgDn", "first / last · page"),
        ("Esc", "clear filter / close"),
        ("?", "this help"),
        ("q  Ctrl-c", "quit the viewer (pmx keeps running)"),
    ];
    let line = |(k, d): &(&str, &str)| {
        Line::from(vec![Span::styled(format!("  {k:<20}"), th.accent), Span::styled(d.to_string(), th.text)])
    };
    let mut lines = vec![Line::styled(format!(" {}", app.view.name()), th.strong)];
    lines.extend(view_keys(app.view).iter().map(line));
    lines.push(Line::default());
    lines.push(Line::styled(" Everywhere", th.strong));
    lines.extend(global.iter().map(line));
    lines.push(Line::default());
    lines.push(Line::styled("  Prices are YES; NO = 100¢ − YES. Sizes in contracts.", th.muted));
    lines.push(Line::styled("  Paper and live are never summed. This view is read-only.", th.muted));
    lines.push(Line::styled("  Quitting only closes this viewer: pmx keeps running (pmx down stops it).", th.muted));
    let w = 72.min(area.width);
    let h = (lines.len() as u16 + 2).min(area.height);
    let r = Rect::new(area.x + (area.width - w) / 2, area.y + (area.height - h) / 2, w, h);
    f.render_widget(Clear, r);
    f.render_widget(Paragraph::new(Text::from(lines)).block(panel(" Keys ", th)), r);
}

#[cfg(test)]
pub(crate) mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::app::tests::{loaded, v2};

    pub const NOW: i64 = 1_792_000_100_000;
    pub const SIZES: [(u16, u16); 3] = [(200, 60), (120, 40), (80, 24)];

    pub fn render(app: &App, w: u16, h: u16) -> String {
        render_at(app, w, h, NOW)
    }

    pub fn render_at(app: &App, w: u16, h: u16, now: i64) -> String {
        let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
        term.draw(|f| draw(f, app, &Theme::new(false), now)).unwrap();
        let buf = term.backend().buffer();
        (0..h).map(|y| (0..w).map(|x| buf[(x, y)].symbol()).collect::<String>() + "\n").collect()
    }

    #[test]
    fn every_view_renders_at_every_size() {
        let mut app = v2(NOW);
        for v in View::ALL {
            app.view = v;
            for (w, h) in [(200, 60), (120, 40), (80, 24), (60, 15), (40, 10), (1, 1)] {
                render(&app, w, h);
            }
            app.mode = Mode::Help;
            render(&app, 80, 24);
            app.mode = Mode::Normal;
        }
        assert!(render(&app, 40, 10).contains("terminal too small"));
    }

    #[test]
    fn header_shows_tabs_health_and_pnl_by_mode() {
        let app = v2(NOW);
        let s = render(&app, 200, 60);
        let top: Vec<&str> = s.lines().take(2).collect();
        assert!(top[0].contains("1 Markets") && top[0].contains("5 Log"), "{}", top[0]);
        assert!(top[1].contains("○ weather"), "a down source is hollow: {}", top[1]);
        assert!(top[1].contains("PAPER") && top[1].contains("+$0.31 today"), "{}", top[1]);
        assert!(top[1].contains("LIVE") && top[1].contains("-$0.13 today"), "{}", top[1]);
        assert!(top[1].contains("1 halted"));
        let small = render(&app, 80, 24);
        assert!(small.lines().next().unwrap().contains("1 Mkts"));
        let status = small.lines().nth(1).unwrap();
        assert!(status.contains("○ weather") && status.contains("+$0.31") && status.contains("-$0.13"), "{status}");
        assert!(small.lines().last().unwrap().contains("? help  q quit"), "help and quit never fall off");
    }

    #[test]
    fn demo_and_backtest_are_badged_and_global_halt_takes_the_status_line() {
        let mut app = loaded();
        let hello = |src: &str| {
            crate::proto::parse(&format!(
                r#"{{"t":"hello","venue":"kalshi","env":"prod","source":"{src}","version":2,"run":"r7"}}"#
            ))
            .unwrap()
        };
        app.on_msg(hello("demo"), 0);
        assert!(render(&app, 120, 40).lines().next().unwrap().contains("DEMO · invented data"));
        app.on_msg(hello("backtest"), 0);
        assert!(render(&app, 120, 40).lines().next().unwrap().contains("BACKTEST r7"));
        app.on_msg(
            crate::proto::parse(r#"{"t":"halt","scope":"","reason":"kill switch","active":true,"ts_ms":1}"#).unwrap(),
            0,
        );
        assert!(render(&app, 120, 40).lines().nth(1).unwrap().contains("HALTED — everything: kill switch"));
    }

    #[test]
    fn waiting_screen_names_the_socket() {
        let app = App::new("/x/pmx.sock".into());
        assert!(render(&app, 100, 30).contains("waiting for the feed at /x/pmx.sock"));
    }
}

/// Scale benchmark with the ARCHITECTURE budgets: `cargo test --release scale -- --ignored --nocapture`.
/// Fails when a budget is exceeded by more than 2× (measured on an M1 Pro; see the budget table).
#[cfg(test)]
mod scale {
    use std::time::{Duration, Instant};

    use super::tests::{NOW, render};
    use crate::app::App;
    use crate::proto::parse;

    /// An app holding `n` markets with books, as a v3 feed builds it.
    fn loaded(n: usize) -> App {
        let mut app = App::new("x".into());
        app.on_msg(parse(r#"{"t":"hello","venue":"kalshi","env":"prod","source":"live","version":3}"#).unwrap(), 0);
        for i in 0..n {
            let (t, e) = (format!("T-{i}"), format!("E-{}", i / 2));
            app.on_msg(crate::app::tests::contract(&t, &e, &format!("Market {i}"), 1_000 + i as i64), 0);
            let line = format!(
                r#"{{"t":"book","ticker":"T-{i}","bids":[[5500,1000],[5400,900],[5300,800]],"asks":[[5700,1000],[5800,900]],"ts_ms":1}}"#
            );
            app.on_msg(parse(&line).unwrap(), 0);
        }
        app
    }

    fn per<T: Copy>(f: impl Fn(T), items: &[T]) -> Duration {
        let t = Instant::now();
        for &x in items {
            f(x);
        }
        t.elapsed() / items.len() as u32
    }

    #[test]
    #[ignore]
    fn ten_thousand_markets_within_budget() {
        for n in [5_000, 10_000] {
            let mut app = loaded(n);
            let t = Instant::now();
            app.tick(NOW);
            let sort = t.elapsed();
            let frame = per(|_| drop(render(&app, 200, 60)), &[(); 10]);
            println!("{n} markets: rebuild_rows (sort) {sort:?}, full frame 200x60 {frame:?}");
            assert!(frame < Duration::from_millis(10), "TUI frame budget 5 ms at 10k markets, 2× = 10 ms: {frame:?}");
            assert!(sort < Duration::from_millis(15), "re-sort (≤ 2/s) ~7.4 ms at 10k, 2× = 15 ms: {sort:?}");
        }
        let mut app = loaded(5_000);
        let lines = |make: &dyn Fn(usize) -> String, n: usize| (0..n).map(make).collect::<Vec<String>>();
        let apply = |app: &mut App, lines: &[String]| {
            let t = Instant::now();
            for (i, l) in lines.iter().enumerate() {
                app.on_msg(parse(l).unwrap(), i as i64);
            }
            t.elapsed() / lines.len() as u32
        };
        let deltas = lines(
            &|i| {
                let d = if i % 2 == 0 { 1 } else { -1 };
                format!(r#"{{"t":"delta","ticker":"T-{}","side":"bid","price":5500,"delta":{d},"ts_ms":2}}"#, i % 5_000)
            },
            100_000,
        );
        let delta = apply(&mut app, &deltas);
        let levels = |base: i64, step: i64| {
            (0..10).map(|k| format!("[{},{}]", base + k * step, 100 * (k + 1))).collect::<Vec<_>>().join(",")
        };
        let (bids, asks) = (levels(5500, -100), levels(5600, 100));
        let books = lines(
            &|i| format!(r#"{{"t":"book","ticker":"T-{}","bids":[{bids}],"asks":[{asks}],"ts_ms":3}}"#, i % 5_000),
            20_000,
        );
        let book = apply(&mut app, &books);
        let tops = lines(
            &|i| {
                format!(
                    r#"{{"t":"top","ticker":"T-{}","bid":[5500,100],"ask":[5600,{}],"last":[5600,100,"bid"],"fairs":{{"fair":0.{},"mid":0.555}},"position":{{"paper":500}},"ts_ms":4}}"#,
                    i % 5_000,
                    100 + i % 7,
                    500 + i % 100
                )
            },
            20_000,
        );
        let top = apply(&mut app, &tops);
        println!("parse+apply: delta {delta:?}, 10-level book {book:?} ({} bytes), top {top:?}", books[0].len());
        assert!(delta < Duration::from_nanos(1_100), "delta apply ~0.5 µs, 2× = 1.1 µs: {delta:?}");
        assert!(book < Duration::from_nanos(6_500), "book apply ~3.2 µs, 2× = 6.5 µs: {book:?}");
        assert!(top < Duration::from_nanos(2_600), "top apply ~1.3 µs, 2× = 2.6 µs: {top:?}");
    }
}
