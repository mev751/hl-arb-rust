use crate::executor::ExecutionClient;
use arb_core::{EngineSnapshot, StrategyState, VenueId};
use rust_decimal::Decimal;
use tracing::{info, warn};

/// Result of startup reconciliation containing both derived state and verified live venue positions.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconcileOutcome {
    pub initial_state: StrategyState,
    pub live_hl_pos: Decimal,
    pub live_bn_pos: Decimal,
}

pub struct StartupReconciler;

impl StartupReconciler {
    /// Reconcile state from disk against live exchange positions.
    pub async fn reconcile_on_startup<E: ExecutionClient + ?Sized>(
        executor: &E,
        snapshot: Option<&EngineSnapshot>,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<StrategyState, anyhow::Error> {
        let outcome = Self::reconcile_detailed(executor, snapshot, symbol_hl, symbol_bn).await?;
        Ok(outcome.initial_state)
    }

    /// Reconcile state and return live venue quantities along with initial strategy state.
    pub async fn reconcile_detailed<E: ExecutionClient + ?Sized>(
        executor: &E,
        snapshot: Option<&EngineSnapshot>,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<ReconcileOutcome, anyhow::Error> {
        let live_hl_pos = executor.query_position(VenueId::Hyperliquid, symbol_hl).await?;
        let live_bn_pos = executor.query_position(VenueId::BinanceFutures, symbol_bn).await?;

        info!(
            live_hl = %live_hl_pos,
            live_bn = %live_bn_pos,
            "Reconciling live positions against local WAL snapshot"
        );

        let initial_state = Self::evaluate_positions(snapshot, live_hl_pos, live_bn_pos);

        Ok(ReconcileOutcome {
            initial_state,
            live_hl_pos,
            live_bn_pos,
        })
    }

    /// Pure state derivation from optional snapshot and live HL/BN positions.
    pub fn evaluate_positions(
        snapshot: Option<&EngineSnapshot>,
        live_hl_pos: Decimal,
        live_bn_pos: Decimal,
    ) -> StrategyState {
        if let Some(snap) = snapshot {
            info!(
                saved_state = ?snap.state,
                saved_hl_pos = %snap.pos_hl.net_qty,
                saved_bn_pos = %snap.pos_bn.net_qty,
                "Found prior engine snapshot on disk"
            );

            // Check if crashed during mid-flight submission or hedge
            match snap.state {
                StrategyState::HlSubmitting => {
                    warn!("Previous crash during HL IOC submission. Reconciling pending order/position...");
                    if live_hl_pos.abs() > Decimal::ZERO && live_bn_pos.abs().is_zero() {
                        warn!("HL order was filled prior to crash, but BN hedge was not completed! Triggering hedge/unwind.");
                        return StrategyState::Hedging;
                    }
                }
                StrategyState::Hedging => {
                    warn!("Previous crash during BN Hedging. Checking if BN fill completed...");
                    let unhedged = (live_hl_pos.abs() - live_bn_pos.abs()).abs();
                    if unhedged.is_zero() && !live_hl_pos.is_zero() {
                        info!("Both legs confirmed on venue. Position is safely OPEN.");
                        return StrategyState::Open;
                    } else {
                        warn!(unhedged = %unhedged, "Unhedged mismatch detected on startup! Operator attention needed.");
                        return StrategyState::Unwind;
                    }
                }
                _ => {}
            }
        }

        // If positions are flat, state is Idle
        if live_hl_pos.is_zero() && live_bn_pos.is_zero() {
            info!("Venue positions are completely flat. State is IDLE.");
            return StrategyState::Idle;
        }

        // Require HL long (>0), BN short (<0), and residual delta strictly < 0.05 tokens
        let residual = (live_hl_pos + live_bn_pos).abs();
        if live_hl_pos > Decimal::ZERO
            && live_bn_pos < Decimal::ZERO
            && residual < Decimal::new(5, 2)
        {
            info!("Venue holds balanced hedged position. State is OPEN.");
            return StrategyState::Open;
        }

        warn!(
            live_hl = %live_hl_pos,
            live_bn = %live_bn_pos,
            residual = %residual,
            "Unbalanced positions found on startup without active hedge session! State is UNWIND."
        );
        StrategyState::Unwind
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_startup_reconciler_unbalanced_detection() {
        // Flat positions -> Idle
        assert_eq!(
            StartupReconciler::evaluate_positions(None, dec!(0), dec!(0)),
            StrategyState::Idle
        );

        // Matched Long HL + Short BN -> Open
        assert_eq!(
            StartupReconciler::evaluate_positions(None, dec!(0.3), dec!(-0.30)),
            StrategyState::Open
        );

        // Unbalanced (0.0 HL, -0.1 BN) -> MUST be Unwind, NEVER Open
        assert_eq!(
            StartupReconciler::evaluate_positions(None, dec!(0), dec!(-0.10)),
            StrategyState::Unwind
        );

        // Unbalanced (+0.1 HL, 0.0 BN) -> MUST be Unwind, NEVER Open
        assert_eq!(
            StartupReconciler::evaluate_positions(None, dec!(0.1), dec!(0)),
            StrategyState::Unwind
        );

        // Same-side or mismatched residual >= 0.05 -> Unwind
        assert_eq!(
            StartupReconciler::evaluate_positions(None, dec!(0.2), dec!(-0.10)),
            StrategyState::Unwind
        );
    }
}
