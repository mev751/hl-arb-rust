use arb_core::RiskConfig;
use arb_strategy::StrategyConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub engine: EngineSettings,
    pub strategy: StrategySettings,
    pub risk: RiskSettings,
    pub venues: VenueSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSettings {
    pub dry_run: bool,
    pub wal_dir: String,
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySettings {
    pub symbol_hl: String,
    pub symbol_bn: String,
    pub entry_spread_bps: Decimal,
    pub exit_spread_bps: Decimal,
    pub order_chunk_qty: Decimal,
    #[serde(default)]
    pub order_chunk_notional_usd: Option<Decimal>,
    pub max_position_qty: Decimal,
    pub hl_slippage_bps: Decimal,
    pub hl_tick_size: Decimal,
    pub hl_step_size: Decimal,
    pub bn_step_size: Decimal,
    pub roundtrip_fees_bps: Decimal,
}

impl StrategySettings {
    pub fn to_strategy_config(&self) -> StrategyConfig {
        StrategyConfig {
            symbol_hl: self.symbol_hl.clone(),
            symbol_bn: self.symbol_bn.clone(),
            entry_spread_bps: self.entry_spread_bps,
            exit_spread_bps: self.exit_spread_bps,
            order_chunk_qty: self.order_chunk_qty,
            order_chunk_notional_usd: self.order_chunk_notional_usd,
            max_position_qty: self.max_position_qty,
            hl_slippage_bps: self.hl_slippage_bps,
            hl_tick_size: self.hl_tick_size,
            hl_step_size: self.hl_step_size,
            bn_step_size: self.bn_step_size,
            roundtrip_fees_bps: self.roundtrip_fees_bps,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskSettings {
    pub max_position_qty: Decimal,
    pub max_order_chunk_qty: Decimal,
    pub max_unhedged_qty: Decimal,
    pub max_drawdown_usdt: Decimal,
    pub max_consecutive_failures: u32,
    pub cooldown_ms: u64,
}

impl RiskSettings {
    pub fn to_risk_config(&self) -> RiskConfig {
        RiskConfig {
            max_position_qty: self.max_position_qty,
            max_order_chunk_qty: self.max_order_chunk_qty,
            max_unhedged_qty: self.max_unhedged_qty,
            max_drawdown_usdt: self.max_drawdown_usdt,
            max_consecutive_failures: self.max_consecutive_failures,
            cooldown_ms: self.cooldown_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueSettings {
    pub binance: BinanceVenueConfig,
    pub hyperliquid: HyperliquidVenueConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceVenueConfig {
    pub api_key: String,
    pub api_secret: String,
    pub rest_url: String,
    pub ws_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidVenueConfig {
    pub private_key: String,
    pub wallet_address: String,
    pub rest_url: String,
    pub ws_url: String,
    pub is_mainnet: bool,
}

impl AppConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
