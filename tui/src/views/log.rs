//! View 5 — Log: fills, risk rejections, halts, slot errors, source health, and log lines,
//! newest at the bottom. Pausing freezes the view while lines keep arriving.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::panel;
use crate::app::App;
use crate::feed::{Event, Level};
use crate::fmt::{self, trunc};
use crate::theme::Theme;

pub const KEYS: &[(&str, &str)] = &[
    ("space", "pause"),
    ("l", "level: all · warn+ · error"),
    ("/", "filter"),
    ("j/k", "scroll (pauses)"),
    ("G", "follow live"),
];

fn level_word(l: Level) -> &'static str {
    match l {
        Level::Info => "INFO ",
        Level::Warn => "WARN ",
        Level::Error => "ERROR",
    }
}

pub fn draw(f: &mut Frame, app: &App, th: &Theme, area: Rect) {
    let feed = &app.feed;
    let terms: Vec<String> = app.log.filter.to_lowercase().split_whitespace().map(str::to_string).collect();
    let shown = |e: &&Event| {
        e.level >= app.log.min_level && {
            let hay = format!("{} {} {}", e.kind, e.source, e.text).to_lowercase();
            terms.iter().all(|t| hay.contains(t))
        }
    };
    // While paused, ignore what arrived since.
    let arrived = app.log.paused_at.map_or(0, |at| feed.events_total.saturating_sub(at) as usize);
    let upto = feed.events.len().saturating_sub(arrived);
    let matching: Vec<&Event> = feed.events.iter().take(upto).filter(shown).collect();
    let rows = area.height.saturating_sub(2) as usize;
    let end = matching.len().saturating_sub(app.log.offset);
    let start = end.saturating_sub(rows);
    let src_w = (area.width as usize / 5).clamp(12, 32);
    let lines: Vec<Line> = matching[start..end]
        .iter()
        .map(|e| {
            let style = match e.level {
                Level::Info => th.text,
                Level::Warn => th.warn,
                Level::Error => th.error,
            };
            let kind_style = match e.kind {
                "FILL" | "ADV" => th.accent,
                "HALT" | "SLOT" => th.error,
                "RISK" => th.warn,
                _ => th.muted,
            };
            Line::from(vec![
                Span::styled(format!(" {} ", fmt::clock(e.ts_ms)), th.muted),
                Span::styled(format!("{} ", level_word(e.level)), style),
                Span::styled(format!("{:<5} ", e.kind), kind_style),
                Span::styled(format!("{:<src_w$} ", trunc(&e.source, src_w)), th.muted),
                Span::styled(e.text.clone(), if e.level == Level::Info { th.text } else { style }),
            ])
        })
        .collect();
    let mut title = vec![Span::styled(format!(" Log {} ", matching.len()), th.title)];
    title.push(Span::styled(
        match app.log.min_level {
            Level::Info => "all levels ",
            Level::Warn => "warn + error ",
            Level::Error => "errors only ",
        },
        th.muted,
    ));
    if !app.log.filter.is_empty() {
        title.push(Span::styled(format!("/{} ", app.log.filter), th.warn));
    }
    if app.log.paused_at.is_some() {
        title.push(Span::styled(format!(" ⏸ PAUSED · {arrived} new "), th.demo));
    }
    let body = if lines.is_empty() { vec![Line::styled(" nothing logged yet", th.muted)] } else { lines };
    f.render_widget(Paragraph::new(body).block(panel(Line::from(title), th)), area);
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{KeyCode, KeyEvent};

    use crate::app::View;
    use crate::app::tests::v2;
    use crate::proto::parse;
    use crate::ui::tests::{NOW, SIZES, render};

    #[test]
    fn log_lists_levels_and_pauses() {
        let mut app = v2(NOW);
        app.view = View::Log;
        let s = render(&app, 200, 60);
        for needle in
            ["FILL", "RISK", "HALT", "SRC", "weather", "max_loss rejected buy 10 @ 32¢", "PAPER bought 5 @ 55¢"]
        {
            assert!(s.contains(needle), "{needle}: {s}");
        }
        for (w, h) in SIZES {
            render(&app, w, h);
        }
        app.on_key(KeyEvent::from(KeyCode::Char('l')), 10);
        let warn = render(&app, 200, 60);
        assert!(warn.contains("warn + error") && !warn.contains("PAPER bought"));
        app.on_key(KeyEvent::from(KeyCode::Char(' ')), 10);
        app.on_msg(parse(r#"{"t":"log","level":"error","source":"x","text":"late arrival","ts_ms":1}"#).unwrap(), 0);
        let paused = render(&app, 200, 60);
        assert!(paused.contains("PAUSED · 1 new") && !paused.contains("late arrival"));
        app.on_key(KeyEvent::from(KeyCode::Char('G')), 10);
        assert!(render(&app, 200, 60).contains("late arrival"));
    }
}
