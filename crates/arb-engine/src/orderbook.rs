use arb_core::types::L1Quote;
use arb_core::VenueId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthDiffEvent {
    pub first_update_id: u64, // U
    pub final_update_id: u64, // u
    pub prev_final_update_id: u64, // pu
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub event_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

pub struct BinanceBookReconciler {
    symbol: String,
    synced: bool,
    last_update_id: u64,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    buffered_diffs: Vec<DepthDiffEvent>,
}

impl BinanceBookReconciler {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            synced: false,
            last_update_id: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            buffered_diffs: Vec::new(),
        }
    }

    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// Handle incoming WebSocket diff update
    pub fn handle_diff(&mut self, event: DepthDiffEvent) {
        if !self.synced {
            self.buffered_diffs.push(event);
            return;
        }

        // Drop outdated events
        if event.final_update_id <= self.last_update_id {
            return;
        }

        // Sequence integrity check: pu must equal previous last_update_id
        if event.prev_final_update_id != self.last_update_id {
            // Gap detected! Mark unsynced, trigger snapshot resync
            self.synced = false;
            self.bids.clear();
            self.asks.clear();
            self.buffered_diffs.clear();
            self.buffered_diffs.push(event);
            return;
        }

        self.apply_diff(&event);
        self.last_update_id = event.final_update_id;
    }

    /// Apply REST snapshot and replay buffered diffs
    pub fn apply_snapshot(&mut self, snapshot: DepthSnapshot) -> bool {
        self.bids.clear();
        self.asks.clear();

        for (p, q) in snapshot.bids {
            if q > Decimal::ZERO {
                self.bids.insert(p, q);
            }
        }
        for (p, q) in snapshot.asks {
            if q > Decimal::ZERO {
                self.asks.insert(p, q);
            }
        }

        self.last_update_id = snapshot.last_update_id;

        // Replay buffered events
        let mut first_match = false;
        let diffs = std::mem::take(&mut self.buffered_diffs);

        for event in diffs {
            if event.final_update_id <= self.last_update_id {
                continue;
            }

            if !first_match {
                // First processed event should bridge the snapshot:
                // U <= lastUpdateId AND u >= lastUpdateId
                if event.first_update_id <= self.last_update_id && event.final_update_id >= self.last_update_id {
                    self.apply_diff(&event);
                    self.last_update_id = event.final_update_id;
                    first_match = true;
                }
            } else {
                if event.prev_final_update_id == self.last_update_id {
                    self.apply_diff(&event);
                    self.last_update_id = event.final_update_id;
                } else {
                    // Sequence broke during replay
                    self.synced = false;
                    return false;
                }
            }
        }

        self.synced = true;
        true
    }

    fn apply_diff(&mut self, diff: &DepthDiffEvent) {
        for (p, q) in &diff.bids {
            if *q == Decimal::ZERO {
                self.bids.remove(p);
            } else {
                self.bids.insert(*p, *q);
            }
        }
        for (p, q) in &diff.asks {
            if *q == Decimal::ZERO {
                self.asks.remove(p);
            } else {
                self.asks.insert(*p, *q);
            }
        }
    }

    /// Extract current best bid and ask (L1 quote)
    pub fn to_l1_quote(&self, timestamp_ns: u64) -> Option<L1Quote> {
        let (&bid_price, &bid_qty) = self.bids.iter().next_back()?;
        let (&ask_price, &ask_qty) = self.asks.iter().next()?;

        Some(L1Quote {
            venue: VenueId::BinanceFutures,
            symbol: self.symbol.clone(),
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
            timestamp_ns,
            local_recv_ns: timestamp_ns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_binance_book_reconciler_lifecycle() {
        let mut reconciler = BinanceBookReconciler::new("SKHYUSDT".into());
        assert!(!reconciler.is_synced());

        // 1. Buffer diff events arriving before snapshot
        let diff1 = DepthDiffEvent {
            first_update_id: 100,
            final_update_id: 105,
            prev_final_update_id: 99,
            bids: vec![(dec!(10.00), dec!(50))],
            asks: vec![(dec!(10.10), dec!(50))],
            event_time_ms: 1000,
        };
        let diff2 = DepthDiffEvent {
            first_update_id: 106,
            final_update_id: 110,
            prev_final_update_id: 105,
            bids: vec![(dec!(10.02), dec!(30))],
            asks: vec![(dec!(10.08), dec!(20))],
            event_time_ms: 1100,
        };
        reconciler.handle_diff(diff1);
        reconciler.handle_diff(diff2);
        assert!(!reconciler.is_synced());

        // 2. Apply REST snapshot with last_update_id = 104
        let snapshot = DepthSnapshot {
            last_update_id: 104,
            bids: vec![(dec!(9.99), dec!(100))],
            asks: vec![(dec!(10.12), dec!(100))],
        };

        let synced = reconciler.apply_snapshot(snapshot);
        assert!(synced);
        assert!(reconciler.is_synced());

        // 3. Verify top of book after replay:
        // diff1 (100..105) spanned 104, diff2 (106..110) updated bids with 10.02, asks with 10.08
        let l1 = reconciler.to_l1_quote(1200).expect("Should produce L1 quote");
        assert_eq!(l1.bid_price, dec!(10.02));
        assert_eq!(l1.ask_price, dec!(10.08));

        // 4. Sequential live diff update
        let diff3 = DepthDiffEvent {
            first_update_id: 111,
            final_update_id: 115,
            prev_final_update_id: 110,
            bids: vec![(dec!(10.05), dec!(40))],
            asks: vec![],
            event_time_ms: 1200,
        };
        reconciler.handle_diff(diff3);
        assert!(reconciler.is_synced());
        let l1_updated = reconciler.to_l1_quote(1300).unwrap();
        assert_eq!(l1_updated.bid_price, dec!(10.05));

        // 5. Gap detection: prev_final_update_id != last_update_id (115)
        let gap_diff = DepthDiffEvent {
            first_update_id: 120,
            final_update_id: 125,
            prev_final_update_id: 119, // Gap: expected 115
            bids: vec![],
            asks: vec![],
            event_time_ms: 1300,
        };
        reconciler.handle_diff(gap_diff);
        assert!(!reconciler.is_synced(), "Sequence gap should trip unsynced state");
    }
}

