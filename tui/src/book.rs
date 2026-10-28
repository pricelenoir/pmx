//! Aggregated YES-side L2 book, sparse by price (subpenny grids exist).

use std::collections::BTreeMap;

use crate::proto::Side;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub ts_ms: i64,
}

impl Book {
    pub fn from_levels(bids: &[(i64, i64)], asks: &[(i64, i64)], ts_ms: i64) -> Self {
        let keep = |levels: &[(i64, i64)]| levels.iter().copied().filter(|&(_, q)| q > 0).collect();
        Book { bids: keep(bids), asks: keep(asks), ts_ms }
    }

    /// Err when the level would go negative — the book is out of sync.
    pub fn apply(&mut self, side: Side, price: i64, delta: i64, ts_ms: i64) -> Result<(), String> {
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let q = levels.get(&price).copied().unwrap_or(0) + delta;
        match q {
            q if q < 0 => return Err(format!("{side:?} {price} would go to {q}")),
            0 => {
                levels.remove(&price);
            }
            q => {
                levels.insert(price, q);
            }
        }
        if ts_ms > 0 {
            self.ts_ms = ts_ms;
        }
        Ok(())
    }

    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.iter().next_back().map(|(&p, &q)| (p, q))
    }

    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.iter().next().map(|(&p, &q)| (p, q))
    }

    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()?.0 + self.best_ask()?.0) as f64 / 2.0)
    }

    pub fn spread(&self) -> Option<i64> {
        Some(self.best_ask()?.0 - self.best_bid()?.0)
    }

    /// Touch mid weighted toward the side with less size (where the next print is likelier).
    pub fn microprice(&self) -> Option<f64> {
        let ((bp, bq), (ap, aq)) = (self.best_bid()?, self.best_ask()?);
        if bq + aq == 0 {
            return self.mid();
        }
        Some((bp as f64 * aq as f64 + ap as f64 * bq as f64) / (bq + aq) as f64)
    }

    /// (bid size, ask size) resting within `range` price units of each side's best.
    pub fn depth_within(&self, range: i64) -> (i64, i64) {
        let sum = |side: Side| match self.levels(side).next() {
            Some((best, _)) => self.levels(side).take_while(|&(p, _)| (p - best).abs() <= range).map(|(_, q)| q).sum(),
            None => 0,
        };
        (sum(Side::Bid), sum(Side::Ask))
    }

    /// (bid − ask) / (bid + ask) depth within `range`: +1 all bids, −1 all asks.
    pub fn imbalance(&self, range: i64) -> Option<f64> {
        let (b, a) = self.depth_within(range);
        (b + a > 0).then(|| (b - a) as f64 / (b + a) as f64)
    }

    /// Best-first.
    pub fn levels(&self, side: Side) -> Box<dyn Iterator<Item = (i64, i64)> + '_> {
        match side {
            Side::Bid => Box::new(self.bids.iter().rev().map(|(&p, &q)| (p, q))),
            Side::Ask => Box::new(self.asks.iter().map(|(&p, &q)| (p, q))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_then_deltas() {
        let mut b = Book::from_levels(&[(5500, 100), (5400, 0)], &[(5700, 50)], 1);
        assert_eq!(b.best_bid(), Some((5500, 100)));
        assert_eq!(b.bids.len(), 1);
        assert_eq!(b.spread(), Some(200));
        assert_eq!(b.mid(), Some(5600.0));

        b.apply(Side::Bid, 5600, 30, 2).unwrap();
        assert_eq!(b.best_bid(), Some((5600, 30)));
        b.apply(Side::Ask, 5700, -50, 3).unwrap();
        assert_eq!(b.best_ask(), None);
        assert_eq!(b.spread(), None);
        assert!(b.apply(Side::Bid, 5600, -31, 4).is_err());
        assert_eq!(b.levels(Side::Bid).collect::<Vec<_>>(), vec![(5600, 30), (5500, 100)]);
    }

    #[test]
    fn microstructure() {
        let b = Book::from_levels(&[(5500, 300), (5400, 100), (5000, 999)], &[(5700, 100), (5900, 100)], 1);
        // More size on the bid pulls the microprice toward the ask.
        assert_eq!(b.microprice(), Some((5500.0 * 100.0 + 5700.0 * 300.0) / 400.0));
        assert_eq!(b.depth_within(200), (400, 200));
        assert_eq!(b.imbalance(200), Some(200.0 / 600.0));
        let empty = Book::default();
        assert_eq!((empty.microprice(), empty.imbalance(500), empty.depth_within(500)), (None, None, (0, 0)));
    }
}
