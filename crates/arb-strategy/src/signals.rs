use arb_core::{OrderSide, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StrategySignal {
    /// Enter cross-venue arbitrage: buy on HL, hedge on BN (or vice-versa)
    Enter {
        session_id: String,
        leg_hl_side: OrderSide,
        leg_bn_side: OrderSide,
        target_qty: Decimal,
        target_spread_bps: Decimal,
        hl_limit_price: Decimal,
    },
    /// Immediate market hedge on opposite leg upon receipt of first leg fill
    Hedge {
        session_id: String,
        venue: VenueId,
        side: OrderSide,
        qty: Decimal,
        reason: String,
    },
    /// Exit cross-venue position when basis converges
    Exit {
        session_id: String,
        leg_hl_side: OrderSide,
        leg_bn_side: OrderSide,
        qty: Decimal,
        current_spread_bps: Decimal,
    },
    /// Emergency unwind of unhedged residual position
    Unwind {
        venue: VenueId,
        side: OrderSide,
        qty: Decimal,
        reason: String,
    },
}
