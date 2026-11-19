use crate::types::{FillEvent, OrderSide, Position, TradeRoundTrip};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StrategyState {
    Idle,
    HlSubmitting,
    Hedging,
    Open,
    Closing,
    Unwind,
    Halted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveArbitrageSession {
    pub session_id: String,
    pub leg_hl_side: OrderSide,
    pub leg_bn_side: OrderSide,
    pub target_qty: Decimal,
    pub hl_client_order_id: Option<String>,
    pub bn_client_order_id: Option<String>,
    pub hl_fill: Option<FillEvent>,
    pub bn_fill: Option<FillEvent>,
    pub entry_basis_bps: Decimal,
    pub start_time_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshot {
    pub state: StrategyState,
    pub active_session: Option<ActiveArbitrageSession>,
    pub pos_hl: Position,
    pub pos_bn: Position,
    pub completed_trades: Vec<TradeRoundTrip>,
    pub total_realized_pnl_usdt: Decimal,
    pub total_fees_usdt: Decimal,
    pub last_updated_ns: u64,
}

impl EngineSnapshot {
    pub fn new(symbol_hl: String, symbol_bn: String) -> Self {
        use crate::types::VenueId;
        Self {
            state: StrategyState::Idle,
            active_session: None,
            pos_hl: Position::new(VenueId::Hyperliquid, symbol_hl),
            pos_bn: Position::new(VenueId::BinanceFutures, symbol_bn),
            completed_trades: Vec::new(),
            total_realized_pnl_usdt: Decimal::ZERO,
            total_fees_usdt: Decimal::ZERO,
            last_updated_ns: 0,
        }
    }
}
