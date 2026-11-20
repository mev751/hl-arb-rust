use crate::executor::{ExecutionClient, ExecutionError};
use arb_core::{
    clocks::unix_now_ns, EngineSnapshot, L1Quote, OrderIntent, OrderType,
    RiskManager, StrategyState, TradeRoundTrip, VenueId, WalManager,
};
use arb_strategy::{ArbitrageStrategy, CrossVenueArbStrategy, StrategyConfig, StrategySignal};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct ArbitrageEngine<E: ExecutionClient> {
    strategy: CrossVenueArbStrategy,
    risk: RiskManager,
    wal: Mutex<WalManager>,
    executor: Arc<E>,
    snapshot: EngineSnapshot,
    symbol_hl: String,
    symbol_bn: String,
}

impl<E: ExecutionClient> ArbitrageEngine<E> {
    pub fn new(
        config: StrategyConfig,
        risk: RiskManager,
        wal: WalManager,
        executor: Arc<E>,
    ) -> Self {
        let symbol_hl = config.symbol_hl.clone();
        let symbol_bn = config.symbol_bn.clone();
        let strategy = CrossVenueArbStrategy::new(config);
        let snapshot = EngineSnapshot::new(symbol_hl.clone(), symbol_bn.clone());

        Self {
            strategy,
            risk,
            wal: Mutex::new(wal),
            executor,
            snapshot,
            symbol_hl,
            symbol_bn,
        }
    }

    pub fn state(&self) -> StrategyState {
        self.strategy.state()
    }

    pub fn snapshot(&self) -> &EngineSnapshot {
        &self.snapshot
    }

    pub fn set_state(&mut self, state: StrategyState) {
        self.strategy.set_state(state);
        self.snapshot.state = state;
    }

    /// Trigger a test trade for a specified quantity (size) across both legs.
    pub async fn trigger_test_order(
        &mut self,
        size: Decimal,
        hl_limit_price: Decimal,
    ) -> Result<(), anyhow::Error> {
        let session_id = format!("test_{}", unix_now_ns());
        self.set_state(StrategyState::HlSubmitting);
        let signal = StrategySignal::Enter {
            session_id,
            leg_hl_side: arb_core::OrderSide::Buy,
            leg_bn_side: arb_core::OrderSide::Sell,
            target_qty: size,
            target_spread_bps: rust_decimal_macros::dec!(50.0),
            hl_limit_price,
        };
        self.handle_signal(signal).await
    }

    pub async fn process_quote(&mut self, quote: &L1Quote) -> Result<(), anyhow::Error> {
        let signal = self.strategy.on_quote(quote);
        if let Some(sig) = signal {
            self.handle_signal(sig).await?;
        }
        Ok(())
    }

    async fn handle_signal(&mut self, mut signal: StrategySignal) -> Result<(), anyhow::Error> {
        loop {
            match signal {
                StrategySignal::Enter {
                    session_id,
                    leg_hl_side,
                    leg_bn_side: _,
                    target_qty,
                    target_spread_bps: _,
                    hl_limit_price,
                } => {
                    // 1. Risk Pre-Trade Check
                    if let Err(e) = self.risk.can_open_entry(
                        target_qty,
                        self.snapshot.pos_hl.net_qty,
                        self.snapshot.pos_bn.net_qty,
                    ) {
                        warn!(error = %e, "Risk check failed for new entry. Skipping opportunity.");
                        self.strategy.set_state(StrategyState::Idle);
                        return Ok(());
                    }

                    // 2. Reserve Risk
                    self.risk.reserve(target_qty);

                    // 3. Log Intent in WAL before network call
                    let client_order_id = format!("hl_{}_{}", session_id, unix_now_ns());
                    let intent = OrderIntent {
                        intent_id: format!("intent_{}", session_id),
                        client_order_id: client_order_id.clone(),
                        venue: VenueId::Hyperliquid,
                        symbol: self.symbol_hl.clone(),
                        side: leg_hl_side,
                        order_type: OrderType::ImmediateOrCancel,
                        price: Some(hl_limit_price),
                        qty: target_qty,
                        created_at_ns: unix_now_ns(),
                    };

                    {
                        let mut wal = self.wal.lock();
                        wal.log_intent(&intent)?;
                    }

                    info!(
                        session_id = %session_id,
                        cl_id = %client_order_id,
                        qty = %target_qty,
                        price = %hl_limit_price,
                        "Submitting Leg 1: Hyperliquid IOC order"
                    );

                    // 4. Dispatch HL IOC to executor
                    match self
                        .executor
                        .submit_hl_ioc(
                            &self.symbol_hl,
                            leg_hl_side,
                            target_qty,
                            hl_limit_price,
                            &client_order_id,
                        )
                        .await
                    {
                        Ok(hl_fill) => {
                            info!(
                                fill_id = %hl_fill.fill_id,
                                qty = %hl_fill.qty,
                                price = %hl_fill.price,
                                "Leg 1 HL IOC filled successfully!"
                            );

                            // Log fill in WAL
                            {
                                let mut wal = self.wal.lock();
                                wal.log_fill(&hl_fill)?;
                            }
                            self.snapshot.pos_hl.apply_fill(&hl_fill);
                            self.risk.commit_fill(hl_fill.qty);

                            // Feed fill to strategy -> triggers immediate Hedge signal
                            if let Some(hedge_sig) = self.strategy.on_fill(&hl_fill) {
                                signal = hedge_sig;
                                continue;
                            }
                            break;
                        }
                        Err(ExecutionError::ZeroFill) => {
                            info!("HL IOC expired with zero fill. Releasing risk reservation.");
                            self.risk.release(target_qty);
                            self.strategy.set_state(StrategyState::Idle);
                            break;
                        }
                        Err(err) => {
                            error!(error = %err, "HL IOC order failed or rejected!");
                            self.risk.release(target_qty);
                            self.risk.record_failure(err.to_string());
                            self.strategy.set_state(StrategyState::Idle);
                            break;
                        }
                    }
                }

                StrategySignal::Hedge {
                    session_id,
                    venue,
                    side,
                    qty,
                    reason,
                } => {
                    info!(
                        session_id = %session_id,
                        qty = %qty,
                        venue = %venue,
                        side = ?side,
                        reason = %reason,
                        "Submitting Leg 2: Binance Market Hedge"
                    );

                    let client_order_id = format!("bn_hedge_{}_{}", session_id, unix_now_ns());
                    let intent = OrderIntent {
                        intent_id: format!("intent_hedge_{}", session_id),
                        client_order_id: client_order_id.clone(),
                        venue,
                        symbol: self.symbol_bn.clone(),
                        side,
                        order_type: OrderType::Market,
                        price: None,
                        qty,
                        created_at_ns: unix_now_ns(),
                    };

                    {
                        let mut wal = self.wal.lock();
                        wal.log_intent(&intent)?;
                    }

                    match self
                        .executor
                        .submit_bn_market(&self.symbol_bn, side, qty, &client_order_id)
                        .await
                    {
                        Ok(bn_fill) => {
                            info!(
                                fill_id = %bn_fill.fill_id,
                                qty = %bn_fill.qty,
                                price = %bn_fill.price,
                                "Leg 2 Binance Hedge filled successfully!"
                            );

                            {
                                let mut wal = self.wal.lock();
                                wal.log_fill(&bn_fill)?;
                            }
                            self.snapshot.pos_bn.apply_fill(&bn_fill);

                            // Inform strategy that hedge fill is secured
                            self.strategy.on_fill(&bn_fill);
                            self.set_state(StrategyState::Open);

                            // Save snapshot
                            let wal = self.wal.lock();
                            wal.save_state(&self.snapshot)?;
                        }
                        Err(err) => {
                            error!(error = %err, "CRITICAL: Binance hedge execution failed! Entering UNWIND state.");
                            self.risk.record_failure(err.to_string());
                            self.set_state(StrategyState::Unwind);

                            let wal = self.wal.lock();
                            wal.save_state(&self.snapshot)?;
                        }
                    }
                    break;
                }

                StrategySignal::Exit {
                    session_id,
                    leg_hl_side,
                    leg_bn_side,
                    qty,
                    current_spread_bps,
                } => {
                    info!(
                        session_id = %session_id,
                        qty = %qty,
                        spread = %current_spread_bps,
                        "Unwinding cross-venue positions (Exit)"
                    );

                    // Close HL
                    let hl_cl_id = format!("exit_hl_{}", unix_now_ns());
                    let hl_fill_res = self
                        .executor
                        .submit_hl_ioc(&self.symbol_hl, leg_hl_side, qty, Decimal::ZERO, &hl_cl_id)
                        .await;

                    // Close BN
                    let bn_cl_id = format!("exit_bn_{}", unix_now_ns());
                    let bn_fill_res = self
                        .executor
                        .submit_bn_market(&self.symbol_bn, leg_bn_side, qty, &bn_cl_id)
                        .await;

                    if let (Ok(hl_fill), Ok(bn_fill)) = (hl_fill_res, bn_fill_res) {
                        info!("Both legs successfully closed on exit.");
                        self.snapshot.pos_hl.apply_fill(&hl_fill);
                        self.snapshot.pos_bn.apply_fill(&bn_fill);

                        // Compute trade roundtrip record
                        let total_fees = hl_fill.fee + bn_fill.fee;
                        let gross_pnl = (bn_fill.price - hl_fill.price) * qty;
                        let net_pnl = gross_pnl - total_fees;

                        let trade = TradeRoundTrip {
                            trade_id: format!("trade_{}", session_id),
                            symbol_hl: self.symbol_hl.clone(),
                            symbol_bn: self.symbol_bn.clone(),
                            entry_hl_price: hl_fill.price,
                            entry_bn_price: bn_fill.price,
                            entry_qty: qty,
                            exit_hl_price: Some(hl_fill.price),
                            exit_bn_price: Some(bn_fill.price),
                            exit_qty: Some(qty),
                            entry_basis_bps: current_spread_bps,
                            exit_basis_bps: Some(current_spread_bps),
                            gross_pnl_usdt: gross_pnl,
                            total_fees_usdt: total_fees,
                            net_pnl_usdt: net_pnl,
                            entry_time_ns: unix_now_ns(),
                            exit_time_ns: Some(unix_now_ns()),
                        };

                        {
                            let mut wal = self.wal.lock();
                            wal.log_trade(&trade)?;
                            self.snapshot.completed_trades.push(trade);
                            self.snapshot.total_realized_pnl_usdt += net_pnl;
                            self.snapshot.total_fees_usdt += total_fees;
                            wal.save_state(&self.snapshot)?;
                        }

                        self.set_state(StrategyState::Idle);
                    } else {
                        error!("Error closing one or both legs on exit! Check positions.");
                        self.set_state(StrategyState::Unwind);
                    }
                    break;
                }

                StrategySignal::Unwind { venue, side, qty, reason } => {
                    warn!(venue = %venue, qty = %qty, reason = %reason, "Executing emergency unwind");
                    // Attempt emergency close
                    let cl_id = format!("unwind_{}", unix_now_ns());
                    match venue {
                        VenueId::Hyperliquid => {
                            let _ = self.executor.submit_hl_ioc(&self.symbol_hl, side, qty, Decimal::ZERO, &cl_id).await;
                        }
                        VenueId::BinanceFutures => {
                            let _ = self.executor.submit_bn_market(&self.symbol_bn, side, qty, &cl_id).await;
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}
