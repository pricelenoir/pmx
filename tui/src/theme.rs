//! Semantic styles on the 16 ANSI colors, so the terminal theme decides the actual
//! shades. `NO_COLOR` keeps only modifiers; bids/asks stay distinct by position.

use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, Copy)]
pub struct Theme {
    pub text: Style,
    pub muted: Style,
    pub strong: Style,
    pub accent: Style,
    pub bid: Style,
    pub ask: Style,
    pub up: Style,
    pub down: Style,
    pub warn: Style,
    pub error: Style,
    pub ok: Style,
    pub selected: Style,
    pub border: Style,
    pub title: Style,
    pub header: Style,
    pub tab: Style,
    pub tab_active: Style,
    pub paper: Style,
    pub live: Style,
    pub demo: Style,
    pub backtest: Style,
    pub halt: Style,
    /// Model lines and columns, in model order.
    pub series: [Style; 5],
}

impl Theme {
    pub fn new(color: bool) -> Self {
        let fg = |c: Color| if color { Style::new().fg(c) } else { Style::new() };
        let bold = Style::new().add_modifier(Modifier::BOLD);
        let dim = Style::new().add_modifier(Modifier::DIM);
        Theme {
            text: Style::new(),
            muted: if color { fg(Color::DarkGray) } else { dim },
            strong: bold,
            accent: fg(Color::Cyan),
            bid: fg(Color::Green),
            ask: fg(Color::Red),
            up: fg(Color::Green).add_modifier(Modifier::BOLD),
            down: fg(Color::Red).add_modifier(Modifier::BOLD),
            warn: fg(Color::Yellow),
            error: fg(Color::Red).add_modifier(Modifier::BOLD),
            ok: fg(Color::Green),
            selected: Style::new().add_modifier(Modifier::REVERSED),
            border: if color { fg(Color::DarkGray) } else { Style::new() },
            title: fg(Color::Cyan).add_modifier(Modifier::BOLD),
            header: if color { fg(Color::DarkGray).add_modifier(Modifier::BOLD) } else { bold },
            tab: if color { fg(Color::DarkGray) } else { dim },
            tab_active: bold.add_modifier(Modifier::REVERSED),
            paper: fg(Color::Cyan).add_modifier(Modifier::REVERSED),
            live: fg(Color::Red).add_modifier(Modifier::BOLD | Modifier::REVERSED),
            demo: fg(Color::Yellow).add_modifier(Modifier::BOLD | Modifier::REVERSED),
            backtest: fg(Color::Magenta).add_modifier(Modifier::BOLD | Modifier::REVERSED),
            halt: fg(Color::Red).add_modifier(Modifier::BOLD | Modifier::REVERSED),
            series: [
                fg(Color::Yellow),
                fg(Color::Magenta),
                fg(Color::Blue),
                fg(Color::LightGreen),
                fg(Color::LightRed),
            ],
        }
    }

    pub fn series(&self, i: usize) -> Style {
        self.series[i % self.series.len()]
    }

    /// Style for a value by sign: gains `up`, losses `down`, zero plain.
    pub fn signed(&self, v: f64) -> Style {
        if v > 0.0 {
            self.up
        } else if v < 0.0 {
            self.down
        } else {
            self.text
        }
    }

    pub fn mode(&self, mode: &str) -> Style {
        if mode == "live" { self.live } else { self.paper }
    }

    pub fn from_env() -> Self {
        Theme::new(std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()))
    }
}
