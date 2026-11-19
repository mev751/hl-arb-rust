use crate::executor::ExecutionClient;
use arb_core::{EngineSnapshot, StrategyState, VenueId};
use rust_decimal::Decimal;
use tracing::{info, warn};

pub struct StartupReconciler;

impl StartupReconciler {
    /// Reconcile state from disk against live exchange positions.
    pub async fn reconcile_on_startup<E: ExecutionClient>(
        executor: &E,
        snapshot: Option<&EngineSnapshot>,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<StrategyState, anyhow::Error> {
        let live_hl_pos = executor.query_position(VenueId::Hyperliquid, symbol_hl).await?;
        let live_bn_pos = executor.query_position(VenueId::BinanceFutures, symbol_bn).await?;

        info!(
            live_hl = %live_hl_pos,
            live_bn = %live_bn_pos,
            "Reconciling live positions against local WAL snapshot"
        );

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
                        return Ok(StrategyState::Hedging);
                    }
                }
                StrategyState::Hedging => {
                    warn!("Previous crash during BN Hedging. Checking if BN fill completed...");
                    let unhedged = (live_hl_pos.abs() - live_bn_pos.abs()).abs();
                    if unhedged.is_zero() && !live_hl_pos.is_zero() {
                        info!("Both legs confirmed on venue. Position is safely OPEN.");
                        return Ok(StrategyState::Open);
                    } else {
                        warn!(unhedged = %unhedged, "Unhedged mismatch detected on startup! Operator attention needed.");
                        return Ok(StrategyState::Unwind);
                    }
                }
                _ => {}
            }
        }

        // If positions are flat, state is Idle
        if live_hl_pos.is_zero() && live_bn_pos.is_zero() {
            info!("Venue positions are completely flat. State is IDLE.");
            return Ok(StrategyState::Idle);
        }

        // If balanced hedged position exists
        if (live_hl_pos + live_bn_pos).abs() <= Decimal::from(1) {
            info!("Venue holds balanced hedged position. State is OPEN.");
            return Ok(StrategyState::Open);
        }

        warn!("Unbalanced positions found on startup without active hedge session! State is UNWIND.");
        Ok(StrategyState::Unwind)
    }
}
