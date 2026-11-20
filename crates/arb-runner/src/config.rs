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
        load_dotenv();
        let content = std::fs::read_to_string(path)?;
        let mut config: AppConfig = toml::from_str(&content)?;
        config.resolve_env_overrides();
        Ok(config)
    }

    pub fn resolve_env_overrides(&mut self) {
        resolve_field(&mut self.venues.hyperliquid.private_key, &["HL_PRIVATE_KEY", "ENV_HL_PRIVATE_KEY", "HYPERLIQUID_PRIVATE_KEY"]);
        resolve_field(&mut self.venues.hyperliquid.wallet_address, &["HL_WALLET_ADDRESS", "ENV_HL_WALLET_ADDRESS", "HYPERLIQUID_WALLET_ADDRESS"]);
        resolve_field(&mut self.venues.binance.api_key, &["BINANCE_API_KEY", "ENV_BINANCE_API_KEY"]);
        resolve_field(&mut self.venues.binance.api_secret, &["BINANCE_API_SECRET", "ENV_BINANCE_API_SECRET"]);
    }
}

fn resolve_field(field: &mut String, fallback_env_names: &[&str]) {
    let trimmed = field.trim();
    // 1. Check if format is ${VAR} or $VAR
    if let Some(var_name) = trimmed.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        if let Ok(val) = std::env::var(var_name) {
            *field = val;
            return;
        }
    } else if let Some(var_name) = trimmed.strip_prefix('$') {
        if let Ok(val) = std::env::var(var_name) {
            *field = val;
            return;
        }
    } else if trimmed.starts_with("ENV_") {
        if let Ok(val) = std::env::var(trimmed) {
            *field = val;
            return;
        }
        if let Some(stripped) = trimmed.strip_prefix("ENV_") {
            if let Ok(val) = std::env::var(stripped) {
                *field = val;
                return;
            }
        }
    }

    // 2. Check fallback standard env names
    for &name in fallback_env_names {
        if let Ok(val) = std::env::var(name) {
            if !val.trim().is_empty() {
                *field = val;
                return;
            }
        }
    }
}

/// Automatically loads KEY=VALUE pairs from a local .env file if present
pub fn load_dotenv() {
    let dotenv_paths = [".env", "config/.env", "../.env"];
    for p in &dotenv_paths {
        if let Ok(content) = std::fs::read_to_string(p) {
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = line.split_once('=') {
                    let key = k.trim();
                    let val = v.trim().trim_matches('"').trim_matches('\'');
                    if !key.is_empty() && std::env::var(key).is_err() {
                        std::env::set_var(key, val);
                    }
                }
            }
            break;
        }
    }
}
