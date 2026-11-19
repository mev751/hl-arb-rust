use crate::state::EngineSnapshot;
use crate::types::{FillEvent, OrderIntent, TradeRoundTrip};
use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

pub struct WalManager {
    base_dir: PathBuf,
    intents_file: File,
    events_file: File,
    trades_file: File,
}

impl WalManager {
    pub fn new<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let base_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("Failed to create WAL directory: {:?}", base_dir))?;

        let intents_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base_dir.join("wal-intents.jsonl"))
            .context("Failed to open wal-intents.jsonl")?;

        let events_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base_dir.join("wal-events.jsonl"))
            .context("Failed to open wal-events.jsonl")?;

        let trades_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base_dir.join("trades.jsonl"))
            .context("Failed to open trades.jsonl")?;

        Ok(Self {
            base_dir,
            intents_file,
            events_file,
            trades_file,
        })
    }

    /// Append order intent before submission over the wire.
    pub fn log_intent(&mut self, intent: &OrderIntent) -> Result<()> {
        let line = serde_json::to_string(intent)?;
        writeln!(self.intents_file, "{}", line)?;
        self.intents_file.flush()?;
        debug!(intent_id = %intent.intent_id, "WAL logged intent");
        Ok(())
    }

    /// Append fill or execution event.
    pub fn log_fill(&mut self, fill: &FillEvent) -> Result<()> {
        let line = serde_json::to_string(fill)?;
        writeln!(self.events_file, "{}", line)?;
        self.events_file.flush()?;
        debug!(fill_id = %fill.fill_id, "WAL logged fill");
        Ok(())
    }

    /// Append completed trade roundtrip.
    pub fn log_trade(&mut self, trade: &TradeRoundTrip) -> Result<()> {
        let line = serde_json::to_string(trade)?;
        writeln!(self.trades_file, "{}", line)?;
        self.trades_file.flush()?;
        info!(trade_id = %trade.trade_id, net_pnl = %trade.net_pnl_usdt, "WAL logged trade");
        Ok(())
    }

    /// Atomically persist current engine snapshot to `state.json` via temp file rename.
    pub fn save_state(&self, snapshot: &EngineSnapshot) -> Result<()> {
        let tmp_path = self.base_dir.join("state.json.tmp");
        let final_path = self.base_dir.join("state.json");

        let data = serde_json::to_vec_pretty(snapshot)?;
        fs::write(&tmp_path, data)
            .with_context(|| format!("Failed to write tmp state to {:?}", tmp_path))?;

        fs::rename(&tmp_path, &final_path)
            .with_context(|| format!("Failed to atomically rename {:?} to {:?}", tmp_path, final_path))?;

        debug!("Atomic state saved to {:?}", final_path);
        Ok(())
    }

    /// Load existing snapshot from `state.json` if available.
    pub fn load_state(&self) -> Result<Option<EngineSnapshot>> {
        let state_path = self.base_dir.join("state.json");
        if !state_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&state_path)
            .with_context(|| format!("Failed to read state file {:?}", state_path))?;
        let snapshot: EngineSnapshot = serde_json::from_str(&content)
            .with_context(|| "Failed to parse state.json")?;

        info!("Loaded existing engine snapshot from {:?}", state_path);
        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::StrategyState;
    use crate::types::{OrderSide, OrderType, VenueId};
    use rust_decimal_macros::dec;

    #[test]
    fn test_wal_lifecycle() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("wal_test_{}", uuid::Uuid::new_v4()));
        let mut wal = WalManager::new(&temp_dir)?;

        let intent = OrderIntent {
            intent_id: "test_intent_1".into(),
            client_order_id: "cl_1".into(),
            venue: VenueId::Hyperliquid,
            symbol: "xyz:SKHY".into(),
            side: OrderSide::Buy,
            order_type: OrderType::ImmediateOrCancel,
            price: Some(dec!(10.50)),
            qty: dec!(10),
            created_at_ns: 1000,
        };
        wal.log_intent(&intent)?;

        let mut snapshot = EngineSnapshot::new("xyz:SKHY".into(), "SKHYUSDT".into());
        snapshot.state = StrategyState::Open;
        wal.save_state(&snapshot)?;

        let loaded = wal.load_state()?.expect("Should find saved state");
        assert_eq!(loaded.state, StrategyState::Open);

        let _ = fs::remove_dir_all(&temp_dir);
        Ok(())
    }
}
