//! Feed protocol from the feed process (see `src/pmx/feed/proto.py`): one JSON object per
//! line, tagged by `t`. Integer units — price 1e-4 $, qty 1e-2 contracts, money 1e-6 $ —
//! and everything quoted from the YES side. Unknown tags parse as `Unknown` and missing
//! fields default, so either side can add to the protocol. v3 conflates market data: each
//! changed market's `top` and top-10 `book` at most 4/s (10/s when traded), no deltas.

use std::fmt;
use std::marker::PhantomData;

use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, Visitor};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Contract {
    pub ticker: String,
    pub event_ticker: String,
    pub series: String,
    pub title: String,
    pub subtitle: String,
    pub status: String,
    /// Trading stops (for some markets, a backstop days after the event).
    pub close_ts_ms: Option<i64>,
    /// When the underlying is expected to resolve.
    #[serde(default)]
    pub event_ts_ms: Option<i64>,
    pub result: Option<String>,
}

impl Contract {
    pub fn label(&self) -> &str {
        if self.subtitle.is_empty() { &self.title } else { &self.subtitle }
    }

    /// Event ticker without its series prefix: `KXNFLGAME-26OCT26DALPHI` -> `26OCT26DALPHI`.
    pub fn event_code(&self) -> &str {
        self.event_ticker
            .strip_prefix(self.series.as_str())
            .map(|s| s.trim_start_matches('-'))
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.event_ticker)
    }

    pub fn ends_ts(&self) -> Option<i64> {
        self.event_ts_ms.or(self.close_ts_ms)
    }

    pub fn is_open(&self) -> bool {
        self.status == "open"
    }
}

/// One market at a glance (v3), sent with its conflated `book`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Top {
    pub ticker: String,
    pub bid: Option<(i64, i64)>,
    pub ask: Option<(i64, i64)>,
    /// (price, qty, taker side) of the last print.
    pub last: Option<(i64, i64, Side)>,
    /// Model -> P(YES), in the sender's order.
    #[serde(default, deserialize_with = "pairs")]
    pub fairs: Vec<(String, f64)>,
    /// Mode -> signed YES position summed over the slots on this market (nonzero only).
    #[serde(default, deserialize_with = "pairs")]
    pub position: Vec<(String, i64)>,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Trade {
    pub ticker: String,
    pub price: i64,
    pub qty: i64,
    pub taker_side: Side,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Fill {
    pub ticker: String,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub fee: i64,
    pub is_taker: bool,
    pub ts_ms: i64,
    /// Empty when not attributed to a slot (v1 `pmx serve`).
    #[serde(default)]
    pub slot: String,
    #[serde(default)]
    pub mode: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Position {
    pub ticker: String,
    pub qty: i64,
    pub realized_pnl: i64,
    pub fees: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Account {
    pub balance: i64,
    pub positions: Vec<Position>,
    pub ts_ms: i64,
}

/// Outside data linked to markets, as its domain chose to display it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Entity {
    pub key: String,
    pub kind: String,
    pub tickers: Vec<String>,
    pub summary: String,
    #[serde(default)]
    pub fields: Vec<(String, String)>,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Forecast {
    pub model: String,
    pub ticker: String,
    /// P(YES), 0..1.
    pub p: f64,
    pub ts_ms: i64,
    pub data_ts_ms: i64,
}

/// An advisor's latest word on a ticker or an entity; `expires_ms` before `ts_ms` withdraws it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Advice {
    pub advisor: String,
    pub target: String,
    pub p: Option<f64>,
    /// buy | sell | flat
    pub direction: Option<String>,
    pub confidence: f64,
    pub expires_ms: i64,
    pub rationale: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Slot {
    pub slot: String,
    pub strategy: String,
    pub ticker: String,
    pub mode: String,
    /// warming | idle | active | halted | error
    pub state: String,
    pub position: i64,
    pub realized: i64,
    pub unrealized: i64,
    pub fees: i64,
    pub working: i64,
    pub last_action: String,
    pub ts_ms: i64,
}

impl Slot {
    pub fn net(&self) -> i64 {
        self.realized + self.unrealized - self.fees
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Order {
    pub slot: String,
    pub mode: String,
    pub client_order_id: String,
    pub order_id: Option<String>,
    pub ticker: String,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub filled: i64,
    /// queued (awaiting write capacity) | pending | open | filled | canceled | rejected
    pub status: String,
    pub ts_ms: i64,
    #[serde(default)]
    pub reason: String,
}

impl Order {
    pub fn working(&self) -> bool {
        matches!(self.status.as_str(), "queued" | "pending" | "open")
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Risk {
    pub slot: String,
    pub ticker: String,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub rule: String,
    pub detail: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Log {
    /// info | warn | error
    pub level: String,
    pub source: String,
    pub text: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Halt {
    /// Slot id, strategy id, or "" for everything.
    pub scope: String,
    pub reason: String,
    pub active: bool,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ScopeStats {
    /// "portfolio" | "strategy:<id>" | "slot:<id>"
    pub scope: String,
    pub mode: String,
    pub realized: i64,
    pub unrealized: i64,
    pub fees: i64,
    pub equity: i64,
    pub today: i64,
    pub hwm: i64,
    pub drawdown: i64,
    pub capital_at_risk: i64,
    pub worst_case: i64,
    pub ev_model: Option<i64>,
    pub stdev_model: Option<i64>,
    pub gross_exposure: i64,
    pub working_exposure: i64,
    pub open_positions: i64,
    #[serde(default)]
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Exposure {
    pub entity: String,
    /// Paper and live exposure are never netted.
    pub mode: String,
    pub tickers: Vec<String>,
    pub strategies: Vec<String>,
    pub capital_at_risk: i64,
    /// Outcome -> total PnL if it happens, in the domain's order.
    #[serde(deserialize_with = "pairs")]
    pub scenarios: Vec<(String, i64)>,
    pub worst_case: i64,
    pub ev_model: Option<i64>,
    pub ev_market: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LimitUsage {
    pub scope: String,
    pub limit: String,
    pub used: i64,
    pub cap: i64,
    pub breached: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Portfolio {
    pub ts_ms: i64,
    pub scopes: Vec<ScopeStats>,
    pub exposures: Vec<Exposure>,
    pub limits: Vec<LimitUsage>,
}

impl Portfolio {
    pub fn scope(&self, scope: &str, mode: &str) -> Option<&ScopeStats> {
        self.scopes.iter().find(|s| s.scope == scope && s.mode == mode)
    }
}

/// (ts_ms, equity, drawdown), micros.
pub type EquityPoint = (i64, i64, i64);

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Equity {
    pub scope: String,
    pub mode: String,
    pub points: Vec<EquityPoint>,
    /// The whole history (sent on connect) rather than new points.
    #[serde(default)]
    pub replace: bool,
}

/// One strategy's performance over a range. Money in micros, `*_c` in cents per contract;
/// None = not enough data to say.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct PerfStats {
    pub net: i64,
    pub gross: i64,
    pub fees: i64,
    pub realized: i64,
    pub open: i64,
    pub orders: i64,
    pub fills: i64,
    pub contracts: i64,
    pub maker_share: Option<f64>,
    pub fill_rate: Option<f64>,
    pub episodes: i64,
    pub markets: i64,
    pub entities: i64,
    pub win_rate: Option<f64>,
    pub avg_win: Option<i64>,
    pub avg_loss: Option<i64>,
    pub profit_factor: Option<f64>,
    pub expectancy_c: Option<f64>,
    pub expectancy_ci: Option<(f64, f64)>,
    pub pnl_ci: Option<(i64, i64)>,
    pub p_nonpositive: Option<f64>,
    pub best_episode: Option<i64>,
    pub worst_episode: Option<i64>,
    pub edge_entry_c: Option<f64>,
    pub edge_realized_c: Option<f64>,
    pub capture: Option<f64>,
    #[serde(deserialize_with = "pairs")]
    pub markouts_c: Vec<(String, f64)>,
    pub max_drawdown: i64,
    pub time_under_water_ms: i64,
    pub max_capital_at_risk: i64,
    pub return_on_risk: Option<f64>,
    pub daily: Vec<(i64, i64)>,
    pub sharpe_day: Option<f64>,
    pub days: i64,
    pub pct_days_positive: Option<f64>,
    pub brier_model: Option<f64>,
    pub brier_market: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BucketRow {
    pub key: String,
    pub fills: i64,
    pub contracts: i64,
    pub pnl: i64,
    #[serde(default)]
    pub cpc: Option<f64>,
    #[serde(default)]
    pub edge_c: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Verdict {
    /// profitable | unprofitable | inconclusive
    pub verdict: String,
    pub reason: String,
    #[serde(default)]
    pub flags: Vec<String>,
    #[serde(default)]
    pub needed: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Perf {
    pub strategy: String,
    pub mode: String,
    /// today | 7d | 30d | all
    pub range: String,
    pub ts_ms: i64,
    pub stats: PerfStats,
    /// "market" | "entity" | "phase" | "edge" | "day" -> rows, in the sender's order.
    #[serde(deserialize_with = "pairs")]
    pub breakdowns: Vec<(String, Vec<BucketRow>)>,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "t", rename_all = "lowercase")]
pub enum Msg {
    Hello {
        venue: String,
        env: String,
        source: String,
        version: u32,
        #[serde(default)]
        run: String,
    },
    /// The full state sent on connect (or a resync) is complete; live updates follow.
    Synced,
    Status {
        connected: bool,
        detail: String,
        ts_ms: i64,
        /// "" = the venue; "engine" = the engine's link to the feed process.
        #[serde(default)]
        source: String,
        /// Engine events that never reached the feed (it was slow or down).
        #[serde(default)]
        dropped: i64,
    },
    Contract(Contract),
    Book {
        ticker: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        ts_ms: i64,
    },
    Delta {
        ticker: String,
        side: Side,
        price: i64,
        delta: i64,
        ts_ms: i64,
    },
    Top(Top),
    Stale {
        ticker: String,
    },
    Reset {
        reason: String,
        ts_ms: i64,
    },
    Trade(Trade),
    Fill(Fill),
    Account(Account),
    Entity(Entity),
    Forecast(Forecast),
    Advice(Advice),
    Models {
        names: Vec<String>,
    },
    Slot(Slot),
    Order(Order),
    Risk(Risk),
    Log(Log),
    Halt(Halt),
    Portfolio(Box<Portfolio>),
    Equity(Equity),
    Perf(Box<Perf>),
    #[serde(other)]
    Unknown,
}

pub fn parse(line: &str) -> Result<Msg, serde_json::Error> {
    serde_json::from_str(line)
}

/// A JSON object as ordered `(key, value)` pairs — keeps the sender's order (outcomes,
/// breakdown kinds) that a map type would lose.
fn pairs<'de, D, V>(d: D) -> Result<Vec<(String, V)>, D::Error>
where
    D: Deserializer<'de>,
    V: Deserialize<'de>,
{
    struct Pairs<V>(PhantomData<V>);
    impl<'de, V: Deserialize<'de>> Visitor<'de> for Pairs<V> {
        type Value = Vec<(String, V)>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("an object")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(map.size_hint().unwrap_or(0));
            while let Some(kv) = map.next_entry()? {
                out.push(kv);
            }
            Ok(out)
        }
    }
    d.deserialize_map(Pairs(PhantomData))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const FIXTURE: &str = include_str!("../../tests/fixtures/feed/v2.ndjson");
    pub const FIXTURE_V3: &str = include_str!("../../tests/fixtures/feed/v3.ndjson");

    #[test]
    fn parses_every_v1_message_type() {
        let lines = [
            r#"{"t":"hello","venue":"kalshi","env":"prod","source":"poll","version":1}"#,
            r#"{"t":"status","connected":true,"detail":"ok","ts_ms":1}"#,
            r#"{"t":"contract","ticker":"KXNFLGAME-26OCT26DALPHI-PHI","event_ticker":"KXNFLGAME-26OCT26DALPHI","series":"KXNFLGAME","title":"Philadelphia wins","subtitle":"Philadelphia","status":"open","close_ts_ms":1793232900000,"result":null}"#,
            r#"{"t":"book","ticker":"X","bids":[[3500,12433]],"asks":[[3700,100]],"ts_ms":2}"#,
            r#"{"t":"delta","ticker":"X","side":"ask","price":3700,"delta":-100,"ts_ms":3}"#,
            r#"{"t":"stale","ticker":"X"}"#,
            r#"{"t":"reset","reason":"gap","ts_ms":4}"#,
            r#"{"t":"trade","ticker":"X","price":4900,"qty":5517,"taker_side":"bid","ts_ms":5}"#,
            r#"{"t":"fill","ticker":"X","side":"bid","price":100,"qty":100,"fee":0,"is_taker":false,"ts_ms":6}"#,
            r#"{"t":"account","balance":1000000,"positions":[{"ticker":"X","qty":-500,"realized_pnl":0,"fees":10}],"ts_ms":7}"#,
            r#"{"t":"something_new","x":1}"#,
        ];
        let msgs: Vec<Msg> = lines.iter().map(|l| parse(l).unwrap()).collect();
        assert!(matches!(&msgs[1], Msg::Status { source, .. } if source.is_empty()));
        assert!(
            matches!(&msgs[2], Msg::Contract(c) if c.event_code() == "26OCT26DALPHI" && c.label() == "Philadelphia")
        );
        assert!(matches!(&msgs[3], Msg::Book { bids, .. } if bids == &vec![(3500, 12433)]));
        assert!(matches!(&msgs[4], Msg::Delta { side: Side::Ask, delta: -100, .. }));
        assert!(matches!(&msgs[8], Msg::Fill(f) if f.slot.is_empty()));
        assert!(matches!(&msgs[9], Msg::Account(a) if a.positions[0].qty == -500));
        assert_eq!(msgs[10], Msg::Unknown);
    }

    #[test]
    fn parses_the_shared_v2_fixture() {
        let msgs: Vec<Msg> = FIXTURE.lines().map(|l| parse(l).unwrap_or_else(|e| panic!("{e}: {l}"))).collect();
        assert!(!msgs.contains(&Msg::Unknown), "every fixture tag is known");
        let perf = msgs.iter().find_map(|m| if let Msg::Perf(p) = m { Some(p) } else { None }).unwrap();
        assert_eq!(perf.breakdowns.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), ["edge", "market"]);
        assert_eq!(perf.stats.expectancy_ci, Some((-1.5, 5.5)));
        assert_eq!(perf.stats.markouts_c[2], ("5m".to_string(), -0.1));
        let port = msgs.iter().find_map(|m| if let Msg::Portfolio(p) = m { Some(p) } else { None }).unwrap();
        let outcomes: Vec<&str> = port.exposures[0].scenarios.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(outcomes, ["HOU", "JAC", "tie"], "outcomes keep the domain's order");
        assert!(port.scope("portfolio", "live").unwrap().stale);
        assert!(msgs.iter().any(|m| matches!(m, Msg::Status { source, connected: false, .. } if source == "weather")));
        assert!(msgs.iter().any(|m| matches!(m, Msg::Fill(f) if f.slot.starts_with("mm/") && f.mode == "paper")));
    }

    #[test]
    fn parses_the_shared_v3_fixture() {
        let msgs: Vec<Msg> = FIXTURE_V3.lines().map(|l| parse(l).unwrap_or_else(|e| panic!("{e}: {l}"))).collect();
        assert!(!msgs.contains(&Msg::Unknown), "every fixture tag is known");
        assert!(matches!(&msgs[0], Msg::Hello { version: 3, .. }));
        let tops: Vec<&Top> = msgs.iter().filter_map(|m| if let Msg::Top(t) = m { Some(t) } else { None }).collect();
        let hou = tops.iter().rev().find(|t| t.ticker.ends_with("-HOU")).unwrap();
        assert_eq!((hou.bid, hou.ask), (Some((5500, 10000)), Some((5700, 300))), "the held-back delta is in");
        assert_eq!(hou.last, Some((5600, 300, Side::Bid)));
        assert_eq!(hou.fairs, [("fair".to_string(), 0.582), ("mid".to_string(), 0.56)]);
        assert_eq!(hou.position, [("paper".to_string(), 500)]);
        let books = msgs.iter().filter_map(|m| if let Msg::Book { bids, .. } = m { Some(bids.len()) } else { None });
        assert!(books.max() == Some(10), "conflated books carry the top 10 levels");
        assert!(tops.iter().any(|t| t.bid.is_none() && t.fairs.is_empty()), "an empty side and no models");
        assert!(msgs.iter().any(|m| matches!(m, Msg::Status { source, dropped: 1234, .. } if source == "engine")));
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Delta { .. } | Msg::Forecast(_))), "v3 sends neither");
        let advice = msgs.iter().find_map(|m| if let Msg::Advice(a) = m { Some(a) } else { None }).unwrap();
        assert_eq!((advice.p, advice.direction.as_deref()), (Some(0.61), Some("buy")));
    }
}
