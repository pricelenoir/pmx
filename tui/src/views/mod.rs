//! One module per view, plus what they share: panels, columns that collapse by priority,
//! badges, and signed values that read without color.

pub mod log;
pub mod markets;
pub mod portfolio;
pub mod slots;
pub mod strategies;

use ratatui::layout::Constraint;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Cell};

use crate::fmt;
use crate::theme::Theme;

pub fn panel<'a>(title: impl Into<Line<'a>>, th: &Theme) -> Block<'a> {
    Block::bordered().border_type(BorderType::Rounded).border_style(th.border).title(title.into().style(th.title))
}

pub fn cell<'a>(s: impl Into<String>, style: Style, right: bool) -> Cell<'a> {
    let line = Line::styled(s.into(), style);
    Cell::from(if right { line.right_aligned() } else { line })
}

/// A table column that drops out (highest `prio` first) when the panel is too narrow.
pub struct Col {
    pub name: &'static str,
    /// 0 = takes the remaining width (at least `FILL_MIN`).
    pub width: u16,
    pub right: bool,
    pub prio: u8,
}

const FILL_MIN: u16 = 14;

impl Col {
    pub const fn new(name: &'static str, width: u16, right: bool, prio: u8) -> Self {
        Col { name, width, right, prio }
    }

    pub fn constraint(&self) -> Constraint {
        if self.width == 0 { Constraint::Fill(1) } else { Constraint::Length(self.width) }
    }
}

/// Which of `cols` fit in `width` (with 1-cell spacing), in their original order.
pub fn fit(cols: &[Col], width: u16) -> Vec<bool> {
    let mut keep = vec![true; cols.len()];
    let need = |keep: &[bool]| -> u16 {
        let (n, w) = cols
            .iter()
            .zip(keep)
            .filter(|(_, k)| **k)
            .fold((0u16, 0u16), |(n, w), (c, _)| (n + 1, w + if c.width == 0 { FILL_MIN } else { c.width }));
        w + n.saturating_sub(1)
    };
    let mut order: Vec<usize> = (0..cols.len()).collect();
    // Highest priority number first; among equals, the rightmost column goes first.
    order.sort_by_key(|&i| (std::cmp::Reverse(cols[i].prio), std::cmp::Reverse(i)));
    for i in order {
        if need(&keep) <= width {
            break;
        }
        if cols[i].prio > 0 {
            keep[i] = false;
        }
    }
    keep
}

/// `PAPER` / `LIVE` (live is loud: bold red reversed).
pub fn mode_badge<'a>(mode: &str, th: &Theme) -> Span<'a> {
    let label = if mode == "live" { " LIVE " } else { " PAPER " };
    Span::styled(label, th.mode(mode))
}

/// ▲ +$12.40 / ▼ -$8.10: sign as arrow, text sign, and color.
pub fn signed_money<'a>(v: i64, th: &Theme) -> Span<'a> {
    let arrow = match v.signum() {
        1 => "▲ ",
        -1 => "▼ ",
        _ => "  ",
    };
    Span::styled(format!("{arrow}{}", fmt::money_signed(v)), th.signed(v as f64))
}

pub fn signed_money_short<'a>(v: i64, th: &Theme) -> Span<'a> {
    Span::styled(fmt::money_short_signed(v), th.signed(v as f64))
}

/// ✓ profitable / ✗ unprofitable / ? inconclusive — symbol, word, and color.
pub fn verdict_badge<'a>(verdict: &str, th: &Theme, long: bool) -> Span<'a> {
    let (sym, word, style) = match verdict {
        "profitable" => ("✓", "PROFITABLE", th.up),
        "unprofitable" => ("✗", "UNPROFITABLE", th.down),
        _ => ("?", "INCONCLUSIVE", th.warn),
    };
    Span::styled(if long { format!("{sym} {word}") } else { sym.to_string() }, style)
}

pub fn state_style(state: &str, th: &Theme) -> Style {
    match state {
        "active" => th.ok,
        "warming" => th.warn,
        "halted" => th.error,
        "error" => th.error,
        _ => th.muted,
    }
}

/// Signed YES position with its mode: +5p (paper), -1L (live).
pub fn position(q: i64, mode: &str) -> String {
    let tag = if mode == "live" { "L" } else { "p" };
    format!("{}{}{tag}", if q > 0 { "+" } else { "" }, fmt::qty(q))
}
