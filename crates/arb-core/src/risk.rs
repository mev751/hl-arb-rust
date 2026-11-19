use crate::clocks::unix_now_ms;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RiskError {
    #[error("Order quantity {qty} exceeds max allowed chunk size {max_qty}")]
    ExceedsMaxOrderQty { qty: Decimal, max_qty: Decimal },

    #[error("Total position {current} + requested {requested} exceeds max position {limit}")]
    ExceedsMaxPosition {
        current: Decimal,
        requested: Decimal,
        limit: Decimal,
    },

    #[error("Unhedged delta {unhedged} exceeds exposure tolerance {tolerance}")]
    UnhedgedDeltaExceeded {
        unhedged: Decimal,
        tolerance: Decimal,
    },

    #[error("Circuit breaker active: {reason}")]
    CircuitBreakerActive { reason: String },

    #[error("Cooling down: remaining {remaining_ms}ms")]
    CoolingDown { remaining_ms: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_position_qty: Decimal,
    pub max_order_chunk_qty: Decimal,
    pub max_unhedged_qty: Decimal,
    pub max_drawdown_usdt: Decimal,
    pub max_consecutive_failures: u32,
    pub cooldown_ms: u64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_qty: Decimal::from(100),
            max_order_chunk_qty: Decimal::from(10),
            max_unhedged_qty: Decimal::from(1),
            max_drawdown_usdt: Decimal::from(500),
            max_consecutive_failures: 3,
            cooldown_ms: 1_000,
        }
    }
}

#[derive(Debug, Default)]
struct RiskState {
    reserved_qty: Decimal,
    consecutive_failures: u32,
    circuit_broken: bool,
    circuit_break_reason: Option<String>,
    last_trade_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RiskManager {
    config: RiskConfig,
    state: Arc<RwLock<RiskState>>,
}

impl RiskManager {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(RiskState::default())),
        }
    }

    /// Check whether a new entry order of size `qty` is compliant with risk parameters.
    pub fn can_open_entry(
        &self,
        qty: Decimal,
        current_hl_pos: Decimal,
        current_bn_pos: Decimal,
    ) -> Result<(), RiskError> {
        let state = self.state.read();

        // 1. Check circuit breaker
        if state.circuit_broken {
            return Err(RiskError::CircuitBreakerActive {
                reason: state
                    .circuit_break_reason
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string()),
            });
        }

        // 2. Check cooldown
        let now_ms = unix_now_ms();
        if now_ms < state.last_trade_ms + self.config.cooldown_ms {
            let remaining = (state.last_trade_ms + self.config.cooldown_ms) - now_ms;
            return Err(RiskError::CoolingDown {
                remaining_ms: remaining,
            });
        }

        // 3. Check single order chunk size
        if qty > self.config.max_order_chunk_qty {
            return Err(RiskError::ExceedsMaxOrderQty {
                qty,
                max_qty: self.config.max_order_chunk_qty,
            });
        }

        // 4. Check unhedged delta
        let unhedged = (current_hl_pos.abs() - current_bn_pos.abs()).abs();
        if unhedged > self.config.max_unhedged_qty {
            return Err(RiskError::UnhedgedDeltaExceeded {
                unhedged,
                tolerance: self.config.max_unhedged_qty,
            });
        }

        // 5. Check aggregate position including reservations
        let total_planned = current_hl_pos.abs() + state.reserved_qty + qty;
        if total_planned > self.config.max_position_qty {
            return Err(RiskError::ExceedsMaxPosition {
                current: current_hl_pos.abs() + state.reserved_qty,
                requested: qty,
                limit: self.config.max_position_qty,
            });
        }

        Ok(())
    }

    /// Reserve risk capacity before dispatching order to venue.
    pub fn reserve(&self, qty: Decimal) {
        let mut state = self.state.write();
        state.reserved_qty += qty;
    }

    /// Release reserved risk capacity on rejection or cancellation.
    pub fn release(&self, qty: Decimal) {
        let mut state = self.state.write();
        state.reserved_qty = (state.reserved_qty - qty).max(Decimal::ZERO);
    }

    /// Commit fill: subtract from reservation and record last trade time.
    pub fn commit_fill(&self, qty: Decimal) {
        let mut state = self.state.write();
        state.reserved_qty = (state.reserved_qty - qty).max(Decimal::ZERO);
        state.consecutive_failures = 0;
        state.last_trade_ms = unix_now_ms();
    }

    /// Register a trade or execution failure; trips breaker if threshold exceeded.
    pub fn record_failure(&self, reason: String) {
        let mut state = self.state.write();
        state.consecutive_failures += 1;
        if state.consecutive_failures >= self.config.max_consecutive_failures {
            state.circuit_broken = true;
            state.circuit_break_reason = Some(format!(
                "Triggered after {} consecutive failures. Last error: {}",
                state.consecutive_failures, reason
            ));
        }
    }

    /// Operator manual reset of circuit breaker.
    pub fn reset_circuit_breaker(&self) {
        let mut state = self.state.write();
        state.circuit_broken = false;
        state.circuit_break_reason = None;
        state.consecutive_failures = 0;
    }

    pub fn is_halted(&self) -> bool {
        self.state.read().circuit_broken
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_risk_manager_checks() {
        let config = RiskConfig {
            max_position_qty: dec!(50),
            max_order_chunk_qty: dec!(10),
            max_unhedged_qty: dec!(2),
            max_drawdown_usdt: dec!(500),
            max_consecutive_failures: 2,
            cooldown_ms: 100,
        };
        let rm = RiskManager::new(config);

        // Valid order
        assert!(rm.can_open_entry(dec!(5), dec!(0), dec!(0)).is_ok());

        // Order exceeding chunk limit
        assert!(rm.can_open_entry(dec!(15), dec!(0), dec!(0)).is_err());

        // Reserve 45 units
        rm.reserve(dec!(45));
        // Trying to open 10 more exceeds 50
        assert!(rm.can_open_entry(dec!(10), dec!(0), dec!(0)).is_err());

        // Release reservation
        rm.release(dec!(45));
        assert!(rm.can_open_entry(dec!(10), dec!(0), dec!(0)).is_ok());

        // Consecutive failures
        rm.record_failure("error 1".into());
        assert!(!rm.is_halted());
        rm.record_failure("error 2".into());
        assert!(rm.is_halted());
        assert!(rm.can_open_entry(dec!(5), dec!(0), dec!(0)).is_err());

        // Reset
        rm.reset_circuit_breaker();
        assert!(!rm.is_halted());
    }
}
