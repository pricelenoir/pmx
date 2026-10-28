//! Display formatting for the integer units.

use chrono::{Local, TimeZone};

/// 56¢, 56.5¢, 0.25¢ — same as `pmx.core.units.cents`.
pub fn cents(p: i64) -> String {
    let (whole, frac) = (p.div_euclid(100), p.rem_euclid(100));
    if frac == 0 { format!("{whole}¢") } else { format!("{}¢", format!("{whole}.{frac:02}").trim_end_matches('0')) }
}

pub fn cents_f(p: f64) -> String {
    cents((p * 2.0).round() as i64 / 2)
}

fn grouped(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Contracts: 1,203 / 0.5 / 12.25.
pub fn qty(q: i64) -> String {
    let sign = if q < 0 { "-" } else { "" };
    let (whole, frac) = (q.unsigned_abs() / 100, q.unsigned_abs() % 100);
    if frac == 0 {
        format!("{sign}{}", grouped(whole))
    } else {
        format!("{sign}{}.{}", grouped(whole), format!("{frac:02}").trim_end_matches('0'))
    }
}

/// Contracts, at most ~6 chars: 950, 12.3k, 1.03M.
pub fn qty_short(q: i64) -> String {
    let c = q as f64 / 100.0;
    match c.abs() {
        a if a >= 1e6 => format!("{:.2}M", c / 1e6),
        a if a >= 1e4 => format!("{:.1}k", c / 1e3),
        _ => qty(q),
    }
}

/// $1,234.56 from micro-dollars, rounded to the cent.
pub fn money(micros: i64) -> String {
    let c = (micros.unsigned_abs() + 5_000) / 10_000;
    let sign = if micros < 0 && c > 0 { "-" } else { "" };
    format!("{sign}${}.{:02}", grouped(c / 100), c % 100)
}

/// +$12.40 / -$8.10; zero has no sign.
pub fn money_signed(micros: i64) -> String {
    let s = money(micros);
    if s.starts_with('-') || s == "$0.00" { s } else { format!("+{s}") }
}

/// Money in at most ~7 chars: $12.40, $1.23k, -$12.3k, $1.20M.
pub fn money_short(micros: i64) -> String {
    let d = micros as f64 / 1e6;
    let sign = if d < 0.0 && d.abs() >= 0.005 { "-" } else { "" };
    match d.abs() {
        a if a >= 1e6 => format!("{sign}${:.2}M", a / 1e6),
        a if a >= 1e4 => format!("{sign}${:.1}k", a / 1e3),
        a if a >= 1e3 => format!("{sign}${:.2}k", a / 1e3),
        a => format!("{sign}${a:.2}"),
    }
}

/// `money_short` with an explicit + for gains.
pub fn money_short_signed(micros: i64) -> String {
    let s = money_short(micros);
    if s.starts_with('-') || s == "$0.00" { s } else { format!("+{s}") }
}

/// A probability as a YES price: 58.2¢.
pub fn prob(p: f64) -> String {
    format!("{:.1}¢", p * 100.0)
}

/// A price difference (1e-4 $ units) in signed cents: +2.2¢, -0.5¢.
pub fn signed_cents(d: f64) -> String {
    let c = d / 100.0;
    if c.abs() < 0.05 { "0.0¢".into() } else { format!("{c:+.1}¢") }
}

pub fn pct(f: f64) -> String {
    format!("{:.0}%", f * 100.0)
}

/// 45s, 12m, 3h 12m, 2d 4h.
pub fn duration(ms: i64) -> String {
    let s = ms.max(0) / 1000;
    match (s / 86_400, s % 86_400 / 3_600, s % 3_600 / 60) {
        (0, 0, 0) => format!("{s}s"),
        (0, 0, m) => format!("{m}m"),
        (0, h, m) => format!("{h}h {m:02}m"),
        (d, h, _) => format!("{d}d {h}h"),
    }
}

/// Mon 10/12.
pub fn day(ts_ms: i64) -> String {
    Local.timestamp_millis_opt(ts_ms).single().map(|t| t.format("%a %m/%d").to_string()).unwrap_or_default()
}

/// Local midnight today, in ms.
pub fn midnight(now_ms: i64) -> i64 {
    Local
        .timestamp_millis_opt(now_ms)
        .single()
        .and_then(|t| t.date_naive().and_hms_opt(0, 0, 0))
        .and_then(|t| t.and_local_timezone(Local).earliest())
        .map_or(now_ms - now_ms.rem_euclid(86_400_000), |t| t.timestamp_millis())
}

/// Compact time until `ts_ms`: 9d 3h, 3h 12m, 12m, <1m.
pub fn until(ts_ms: i64, now_ms: i64) -> String {
    let s = (ts_ms - now_ms) / 1000;
    if s <= 0 {
        return "past".into();
    }
    let (d, h, m) = (s / 86_400, s % 86_400 / 3_600, s % 3_600 / 60);
    match (d, h, m) {
        (0, 0, 0) => "<1m".into(),
        (0, 0, m) => format!("{m}m"),
        (0, h, m) => format!("{h}h {m:02}m"),
        (d, h, _) => format!("{d}d {h}h"),
    }
}

pub fn clock(ts_ms: i64) -> String {
    Local.timestamp_millis_opt(ts_ms).single().map(|t| t.format("%H:%M:%S").to_string()).unwrap_or_default()
}

pub fn date_time(ts_ms: i64) -> String {
    Local.timestamp_millis_opt(ts_ms).single().map(|t| t.format("%a %b %-d %H:%M").to_string()).unwrap_or_default()
}

/// Truncate to `width` display chars with an ellipsis.
pub fn trunc(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        return s.to_string();
    }
    if width == 0 {
        return String::new();
    }
    let mut out: String = s.chars().take(width - 1).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats() {
        assert_eq!(cents(5600), "56¢");
        assert_eq!(cents(5650), "56.5¢");
        assert_eq!(cents(25), "0.25¢");
        assert_eq!(cents_f(5650.0), "56.5¢");
        assert_eq!(qty(120_300), "1,203");
        assert_eq!(qty(50), "0.5");
        assert_eq!(qty(-1225), "-12.25");
        assert_eq!(qty_short(102_936_600), "1.03M");
        assert_eq!(qty_short(1_234_500), "12.3k");
        assert_eq!(money(1_234_567_890), "$1,234.57");
        assert_eq!(money(-4_999), "$0.00");
        assert_eq!(money(-5_000), "-$0.01");
        assert_eq!(until(10_000 + 3 * 86_400_000 + 4 * 3_600_000, 10_000), "3d 4h");
        assert_eq!(until(3_660_000, 0), "1h 01m");
        assert_eq!(until(0, 1), "past");
        assert_eq!(trunc("Philadelphia", 6), "Phila…");
        assert_eq!(money_signed(12_400_000), "+$12.40");
        assert_eq!(money_signed(-8_100_000), "-$8.10");
        assert_eq!(money_signed(1_000), "$0.00");
        assert_eq!(money_short(1_234_560_000), "$1.23k");
        assert_eq!(money_short(-12_345_000_000), "-$12.3k");
        assert_eq!(money_short_signed(5_000_000), "+$5.00");
        assert_eq!(prob(0.5823), "58.2¢");
        assert_eq!(signed_cents(220.0), "+2.2¢");
        assert_eq!(signed_cents(-3.0), "0.0¢");
        assert_eq!(duration(45_000), "45s");
        assert_eq!(duration(3 * 3_600_000 + 12 * 60_000), "3h 12m");
        let now = 1_792_000_000_000;
        assert!(midnight(now) <= now && now - midnight(now) < 86_400_000);
    }
}
