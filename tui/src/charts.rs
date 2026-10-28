//! Chart helpers shared by the views: axis bounds, point thinning, signed bars (ratatui's
//! `BarChart` can't go negative), the order-book heatmap, and the equity panel.

use std::collections::VecDeque;

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Chart, Dataset, GraphType, LegendPosition, Paragraph, Sparkline, Widget};

use crate::feed::HeatCol;
use crate::fmt;
use crate::proto::{EquityPoint, Side};
use crate::theme::Theme;

/// `[lo, hi]` of `values` widened by `pad` × span on each side; a single value or a flat
/// series gets a span of `min_span` so it draws mid-panel. None when empty.
pub fn bounds(values: impl IntoIterator<Item = f64>, pad: f64, min_span: f64) -> Option<[f64; 2]> {
    let (lo, hi) =
        values.into_iter().filter(|v| v.is_finite()).fold((f64::MAX, f64::MIN), |(lo, hi), v| (lo.min(v), hi.max(v)));
    if lo > hi {
        return None;
    }
    let span = (hi - lo).max(min_span);
    let mid = (lo + hi) / 2.0;
    let (lo, hi) = (mid - span / 2.0, mid + span / 2.0);
    Some([lo - span * pad, hi + span * pad])
}

/// At most ~`n` points, keeping each bucket's min and max (in x order) so peaks and
/// drawdowns survive thinning.
pub fn thin(points: &[(f64, f64)], n: usize) -> Vec<(f64, f64)> {
    if points.len() <= n.max(2) {
        return points.to_vec();
    }
    let buckets = (n / 2).max(1);
    let per = points.len().div_ceil(buckets);
    let mut out = Vec::with_capacity(buckets * 2 + 1);
    for chunk in points.chunks(per) {
        let lo = chunk.iter().min_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
        let hi = chunk.iter().max_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
        if lo.0 <= hi.0 {
            out.extend([*lo, *hi]);
        } else {
            out.extend([*hi, *lo]);
        }
    }
    out.dedup();
    if let (Some(last), Some(end)) = (out.last(), points.last())
        && last != end
    {
        out.push(*end);
    }
    out
}

/// Bucket maxima of `values` into `width` columns (for sparklines).
pub fn columns(values: &[u64], width: usize) -> Vec<u64> {
    if values.len() <= width || width == 0 {
        return values.to_vec();
    }
    let per = values.len().div_ceil(width);
    values.chunks(per).map(|c| c.iter().copied().max().unwrap_or(0)).collect()
}

const EIGHTHS_UP: [char; 9] = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
const POS: char = '█';
const NEG: char = '▒';

/// Vertical bars around a zero line, newest on the right: gains rise in `█` (eighth-cell
/// resolution), losses hang in `▒`, so sign reads without color.
pub struct SignedBars<'a> {
    pub values: &'a [i64],
    pub up: Style,
    pub down: Style,
    pub axis: Style,
    pub bar_width: u16,
}

impl Widget for SignedBars<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height < 2 || area.width == 0 || self.values.is_empty() {
            return;
        }
        let max_pos = self.values.iter().copied().max().unwrap_or(0).max(0) as f64;
        let max_neg = -(self.values.iter().copied().min().unwrap_or(0).min(0)) as f64;
        let rows = area.height - 1; // one row is the zero line
        let pos_rows = if max_pos + max_neg == 0.0 {
            rows
        } else {
            ((rows as f64) * max_pos / (max_pos + max_neg)).round() as u16
        };
        let neg_rows = rows - pos_rows;
        let axis_y = area.y + pos_rows;
        for x in area.x..area.right() {
            buf[(x, axis_y)].set_char('─').set_style(self.axis);
        }
        let step = self.bar_width + 1;
        let fit = (area.width / step).max(1) as usize;
        let shown = &self.values[self.values.len().saturating_sub(fit)..];
        let x0 = area.right().saturating_sub(shown.len() as u16 * step) + 1;
        for (i, &v) in shown.iter().enumerate() {
            let x = x0 + i as u16 * step;
            for dx in 0..self.bar_width {
                let x = x + dx;
                if x >= area.right() {
                    continue;
                }
                if v > 0 && max_pos > 0.0 {
                    let eighths = ((v as f64 / max_pos) * pos_rows as f64 * 8.0).round().max(1.0) as u16;
                    for r in 0..pos_rows {
                        let fill = eighths.saturating_sub(r * 8).min(8);
                        if fill == 0 {
                            break;
                        }
                        buf[(x, axis_y - 1 - r)].set_char(EIGHTHS_UP[fill as usize]).set_style(self.up);
                    }
                } else if v < 0 && max_neg > 0.0 {
                    let cells = ((-v as f64 / max_neg) * neg_rows as f64).round().max(1.0) as u16;
                    for r in 0..cells.min(neg_rows) {
                        buf[(x, axis_y + 1 + r)].set_char(NEG).set_style(self.down);
                    }
                }
            }
        }
    }
}

/// One signed horizontal bar `half` cells each side of a `│` axis: losses `▒` to the left,
/// gains `█` to the right.
pub fn hbar<'a>(v: f64, max_abs: f64, half: usize, th: &Theme) -> Vec<Span<'a>> {
    let n = if max_abs > 0.0 { ((v.abs() / max_abs) * half as f64).round() as usize } else { 0 };
    let n = n.min(half).max(usize::from(v != 0.0));
    let (left, right) = if v < 0.0 {
        (format!("{}{}", " ".repeat(half - n), NEG.to_string().repeat(n)), " ".repeat(half))
    } else {
        (" ".repeat(half), format!("{}{}", POS.to_string().repeat(n), " ".repeat(half - n)))
    };
    vec![Span::styled(left, th.down), Span::styled("│", th.border), Span::styled(right, th.up)]
}

const SHADES: [char; 5] = [' ', '░', '▒', '▓', '█'];

/// Resting size by price (rows, one per `tick`) over time (columns, newest right).
/// Bids and asks keep their own color and sit on opposite sides of the mid; prints are
/// `▲` (bought) / `▼` (sold).
pub struct Heatmap<'a> {
    pub cols: &'a VecDeque<HeatCol>,
    /// Prints as (wall ms, price, taker side).
    pub prints: &'a [(i64, i64, Side)],
    pub tick: i64,
    pub th: &'a Theme,
}

impl Heatmap<'_> {
    const LABEL_W: u16 = 5;

    /// Price of the top row, centering the newest column's mid.
    fn top(&self, rows: u16) -> Option<i64> {
        let last = self.cols.back()?;
        let mid = match (last.bids.first(), last.asks.first()) {
            (Some(&(b, _)), Some(&(a, _))) => (a + b) / 2,
            (Some(&(b, _)), None) => b,
            (None, Some(&(a, _))) => a,
            (None, None) => return None,
        };
        let top = mid / self.tick * self.tick + (rows as i64 / 2) * self.tick;
        Some(top.min(10_000 - self.tick).max(rows as i64 * self.tick))
    }
}

impl Widget for Heatmap<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let th = self.th;
        if area.width <= Self::LABEL_W + 2 || area.height < 3 {
            return;
        }
        let Some(top) = self.top(area.height) else {
            buf.set_string(area.x, area.y, "no book yet", th.muted);
            return;
        };
        let width = (area.width - Self::LABEL_W) as usize;
        let cols: Vec<&HeatCol> = self.cols.iter().skip(self.cols.len().saturating_sub(width)).collect();
        let x0 = area.right() - cols.len() as u16;
        let row_of = |p: i64| -> Option<u16> {
            let r = (top - p) / self.tick;
            (p <= top && r < area.height as i64).then_some(r as u16)
        };
        let max = cols
            .iter()
            .flat_map(|c| c.bids.iter().chain(&c.asks))
            .filter(|&&(p, _)| row_of(p).is_some())
            .map(|&(_, q)| q)
            .max()
            .unwrap_or(1)
            .max(1) as f64;
        for r in (0..area.height).step_by(4) {
            let label = fmt::cents(top - r as i64 * self.tick);
            buf.set_string(area.x, area.y + r, format!("{label:>4}"), th.muted);
        }
        for (i, c) in cols.iter().enumerate() {
            let x = x0 + i as u16;
            for (levels, style) in [(&c.bids, th.bid), (&c.asks, th.ask)] {
                for &(p, q) in levels.iter() {
                    if let Some(r) = row_of(p) {
                        // sqrt so mid-sized levels stay visible next to one huge one
                        let shade = ((q as f64 / max).sqrt() * 4.0).ceil().clamp(1.0, 4.0) as usize;
                        buf[(x, area.y + r)].set_char(SHADES[shade]).set_style(style);
                    }
                }
            }
        }
        let Some(t0) = cols.first().map(|c| c.ts_ms) else { return };
        for &(ts, p, side) in self.prints.iter().filter(|&&(ts, _, _)| ts >= t0) {
            let i = cols.partition_point(|c| c.ts_ms < ts).min(cols.len() - 1);
            if let Some(r) = row_of(p) {
                let (ch, style) = if side == Side::Bid { ('▲', th.up) } else { ('▼', th.down) };
                buf[(x0 + i as u16, area.y + r)].set_char(ch).set_style(style);
            }
        }
    }
}

/// One line on the equity panel.
pub struct Curve<'a> {
    pub name: String,
    pub style: Style,
    /// Oldest first.
    pub points: &'a [EquityPoint],
}

/// A curve ready to draw, in dollars.
struct Drawn {
    name: String,
    style: Style,
    equity: Vec<(f64, f64)>,
    hwm: Vec<(f64, f64)>,
}

/// Equity curves with their high-water marks over `window` = (start, now) ms, fill
/// markers (`▲` buy, `▼` sell, at their time and the curve's value), and a drawdown
/// sparkline for the first curve beneath. Money axis in dollars.
pub fn draw_equity(
    f: &mut Frame,
    area: Rect,
    th: &Theme,
    block: Block,
    curves: &[Curve],
    fills: &[(i64, Side)],
    window: (i64, i64),
) {
    let (start_ms, now_ms) = window;
    let inner = block.inner(area);
    f.render_widget(block, area);
    let in_range =
        |pts: &'_ [EquityPoint]| -> Vec<EquityPoint> { pts[pts.partition_point(|p| p.0 < start_ms)..].to_vec() };
    let series: Vec<(&Curve, Vec<EquityPoint>)> =
        curves.iter().map(|c| (c, in_range(c.points))).filter(|(_, p)| !p.is_empty()).collect();
    if series.is_empty() {
        f.render_widget(Paragraph::new(Line::styled("no equity history in this range yet", th.muted)), inner);
        return;
    }
    let dd_rows = if inner.height >= 10 { 3 } else { 0 };
    let [chart_area, dd_area] = Layout::vertical([Constraint::Fill(1), Constraint::Length(dd_rows)]).areas(inner);
    let width = chart_area.width.max(10) as usize * 2;
    let dollars = |m: i64| m as f64 / 1e6;
    let lines: Vec<Drawn> = series
        .iter()
        .map(|(c, pts)| {
            let eq: Vec<(f64, f64)> = pts.iter().map(|&(t, e, _)| (t as f64, dollars(e))).collect();
            let hwm: Vec<(f64, f64)> = pts.iter().map(|&(t, e, d)| (t as f64, dollars(e + d))).collect();
            Drawn { name: c.name.clone(), style: c.style, equity: thin(&eq, width), hwm: thin(&hwm, width) }
        })
        .collect();
    let x0 = series.iter().map(|(_, p)| p[0].0).min().unwrap_or(start_ms).max(start_ms.min(now_ms - 60_000));
    let ys = lines.iter().flat_map(|l| l.equity.iter().chain(&l.hwm).map(|p| p.1));
    let [lo, hi] = bounds(ys, 0.05, 0.02).unwrap_or([0.0, 1.0]);
    // Fills sit on the first curve at their time.
    let first = &series[0].1;
    let at = |ts: i64| first[first.partition_point(|p| p.0 <= ts).saturating_sub(1)].1;
    let buys: Vec<(f64, f64)> =
        fills.iter().filter(|f| f.1 == Side::Bid && f.0 >= x0).map(|f| (f.0 as f64, dollars(at(f.0)))).collect();
    let sells: Vec<(f64, f64)> =
        fills.iter().filter(|f| f.1 == Side::Ask && f.0 >= x0).map(|f| (f.0 as f64, dollars(at(f.0)))).collect();
    let mut datasets = vec![];
    for l in &lines {
        datasets
            .push(Dataset::default().marker(Marker::Braille).graph_type(GraphType::Line).style(th.muted).data(&l.hwm));
        datasets.push(
            Dataset::default()
                .name(l.name.clone())
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(l.style)
                .data(&l.equity),
        );
    }
    if !buys.is_empty() {
        datasets.push(
            Dataset::default()
                .name("buy")
                .marker(Marker::Custom('▲'))
                .graph_type(GraphType::Scatter)
                .style(th.up)
                .data(&buys),
        );
    }
    if !sells.is_empty() {
        datasets.push(
            Dataset::default()
                .name("sell")
                .marker(Marker::Custom('▼'))
                .graph_type(GraphType::Scatter)
                .style(th.down)
                .data(&sells),
        );
    }
    let span = now_ms - x0;
    let label = |ts: i64| {
        if span > 2 * 86_400_000 { fmt::day(ts) } else { fmt::clock(ts).chars().take(5).collect() }
    };
    let money = |v: f64| fmt::money_short((v * 1e6) as i64);
    let chart = Chart::new(datasets)
        .legend_position(Some(LegendPosition::TopLeft))
        .hidden_legend_constraints((Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)))
        .x_axis(
            Axis::default()
                .bounds([x0 as f64, now_ms as f64])
                .labels([Span::styled(label(x0), th.muted), Span::styled("now", th.muted)])
                .style(th.border),
        )
        .y_axis(
            Axis::default()
                .bounds([lo, hi])
                .labels([Span::styled(money(lo), th.muted), Span::styled(money(hi), th.muted)])
                .style(th.border),
        );
    f.render_widget(chart, chart_area);
    if dd_area.height > 0 {
        let dd: Vec<u64> = first.iter().map(|&(_, _, d)| (d.max(0) / 10_000) as u64).collect();
        let worst = first.iter().map(|p| p.2).max().unwrap_or(0);
        let [label_area, spark_area] = Layout::horizontal([Constraint::Length(14), Constraint::Fill(1)]).areas(dd_area);
        f.render_widget(
            Paragraph::new(vec![
                Line::styled("drawdown", th.muted),
                Line::styled(format!("max {}", fmt::money_short(worst)), th.down),
            ]),
            label_area,
        );
        let data = columns(&dd, spark_area.width as usize);
        f.render_widget(Sparkline::default().data(&data).style(th.down), spark_area);
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    #[test]
    fn bounds_pad_and_never_collapse() {
        assert_eq!(bounds([], 0.1, 1.0), None);
        assert_eq!(bounds([5.0], 0.0, 2.0), Some([4.0, 6.0]), "single point centers");
        assert_eq!(bounds([0.0, 10.0], 0.1, 1.0), Some([-1.0, 11.0]));
        assert_eq!(bounds([-3.0, -1.0], 0.0, 0.5), Some([-3.0, -1.0]), "negative-only");
        assert_eq!(bounds([f64::NAN, 1.0], 0.0, 0.0), Some([1.0, 1.0]));
    }

    #[test]
    fn thin_keeps_extremes_and_the_end() {
        let pts: Vec<(f64, f64)> = (0..1_000).map(|i| (i as f64, if i == 500 { -50.0 } else { i as f64 })).collect();
        let t = thin(&pts, 40);
        assert!(t.len() <= 42);
        assert!(t.contains(&(500.0, -50.0)), "the dip survives");
        assert_eq!(t.last(), Some(&(999.0, 999.0)));
        assert_eq!(thin(&pts[..3], 40), pts[..3].to_vec());
        assert!(thin(&[], 10).is_empty());
    }

    #[test]
    fn columns_bucket_maxima() {
        assert_eq!(columns(&[1, 5, 2, 2, 9, 0], 3), [5, 2, 9]);
        assert_eq!(columns(&[1, 2], 10), [1, 2]);
    }

    fn render(w: impl Widget, width: u16, height: u16) -> Vec<String> {
        let mut term = Terminal::new(TestBackend::new(width, height)).unwrap();
        term.draw(|f| f.render_widget(w, f.area())).unwrap();
        let buf = term.backend().buffer();
        (0..height).map(|y| (0..width).map(|x| buf[(x, y)].symbol()).collect()).collect()
    }

    #[test]
    fn signed_bars_split_around_zero() {
        let th = Theme::new(false);
        let bars = |v: &[i64], h| {
            render(SignedBars { values: v, up: th.up, down: th.down, axis: th.border, bar_width: 1 }, 8, h)
        };
        let rows = bars(&[4, -2, 0, 2], 4);
        assert_eq!(rows.len(), 4);
        let axis = rows.iter().position(|r| r.contains('─')).unwrap();
        assert!(rows[..axis].iter().any(|r| r.contains('█')), "gains above the line");
        assert!(rows[axis + 1..].iter().any(|r| r.contains('▒')), "losses below it");
        let neg = bars(&[-1, -3], 4);
        assert_eq!(neg[0].trim().chars().next(), Some('─'), "negative-only: axis at the top");
        assert!(bars(&[], 4).iter().all(|r| r.trim().is_empty()));
        assert!(bars(&[7], 4).concat().contains('█'), "a single bar draws");
    }

    #[test]
    fn hbar_sides() {
        let th = Theme::new(false);
        let text = |v| hbar(v, 2.0, 4, &th).iter().map(|s| s.content.to_string()).collect::<String>();
        assert_eq!(text(2.0), "    │████");
        assert_eq!(text(-1.0), "  ▒▒│    ");
        assert_eq!(text(0.0), "    │    ");
        assert_eq!(text(0.01), "    │█   ", "a nonzero value always shows");
    }

    #[test]
    fn heatmap_draws_levels_prints_and_labels() {
        let th = Theme::new(false);
        let cols: VecDeque<HeatCol> = (0..3)
            .map(|i| HeatCol { ts_ms: i * 1000, bids: vec![(5500, 100), (5400, 900)], asks: vec![(5700, 400)] })
            .collect();
        let prints = [(1_500, 5700, Side::Bid)];
        let rows = render(Heatmap { cols: &cols, prints: &prints, tick: 100, th: &th }, 12, 6);
        let all = rows.concat();
        assert!(all.contains('▲'), "the print");
        assert!(all.contains('█'), "the biggest level is solid");
        assert!(rows[0].starts_with(" 59¢"), "labels from the top row: {rows:?}");
        let empty = VecDeque::new();
        let rows = render(Heatmap { cols: &empty, prints: &[], tick: 100, th: &th }, 20, 4);
        assert!(rows[0].starts_with("no book yet"));
    }
}
