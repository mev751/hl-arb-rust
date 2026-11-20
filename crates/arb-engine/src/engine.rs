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

pub struct ArbitrageEngine<E: ExecutionClient + ?Sized> {
    strategy: CrossVenueArbStrategy,
    risk: RiskManager,
    wal: Mutex<WalManager>,
    executor: Arc<E>,
    snapshot: EngineSnapshot,
    symbol_hl: String,
    symbol_bn: String,
    accumulated_fees: Decimal,
}

impl<E: ExecutionClient + ?Sized> ArbitrageEngine<E> {
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
            accumulated_fees: Decimal::ZERO,
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

    pub fn restore_snapshot(&mut self, snapshot: EngineSnapshot) {
        self.strategy.set_state(snapshot.state);
        self.strategy
            .sync_positions(snapshot.pos_hl.net_qty, snapshot.pos_bn.net_qty);
        self.snapshot = snapshot;
    }

    /// Manually flatten and close any open positions across both venues.
    pub async fn manual_close(&mut self) -> Result<Option<TradeRoundTrip>, anyhow::Error> {
        let hl_qty = self.snapshot.pos_hl.net_qty;
        let bn_qty = self.snapshot.pos_bn.net_qty;

        if hl_qty.is_zero() && bn_qty.is_zero() {
            info!("No open positions to close. Snapshot indicates both sides are zero.");
            return Ok(None);
        }

        let qty = hl_qty.abs().max(bn_qty.abs());
        let leg_hl_side = if hl_qty > Decimal::ZERO {
            arb_core::OrderSide::Sell
        } else {
            arb_core::OrderSide::Buy
        };
        let leg_bn_side = if bn_qty > Decimal::ZERO {
            arb_core::OrderSide::Sell
        } else {
            arb_core::OrderSide::Buy
        };

        info!(
            hl_qty = %hl_qty,
            bn_qty = %bn_qty,
            close_qty = %qty,
            hl_side = ?leg_hl_side,
            bn_side = ?leg_bn_side,
            "Executing manual flatten / close across both venues"
        );

        let session_id = format!("manual_close_{}", unix_now_ns());
        let signal = StrategySignal::Exit {
            session_id,
            leg_hl_side,
            leg_bn_side,
            qty,
            current_spread_bps: Decimal::ZERO,
        };

        self.handle_signal(signal).await?;
        Ok(self.snapshot.completed_trades.last().cloned())
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
                            self.accumulated_fees += hl_fill.fee;

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

                    let client_order_id = format!("bn_{}", unix_now_ns());
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
                            self.accumulated_fees += bn_fill.fee;

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
                    qty: _,
                    current_spread_bps,
                } => {
                    let hl_qty_to_close = self.snapshot.pos_hl.net_qty.abs();
                    let bn_qty_to_close = self.snapshot.pos_bn.net_qty.abs();
                    let entry_hl_price = self.snapshot.pos_hl.entry_price;
                    let entry_bn_price = self.snapshot.pos_bn.entry_price;

                    info!(
                        session_id = %session_id,
                        hl_qty = %hl_qty_to_close,
                        bn_qty = %bn_qty_to_close,
                        entry_hl = %entry_hl_price,
                        entry_bn = %entry_bn_price,
                        spread = %current_spread_bps,
                        "Unwinding cross-venue positions (Exit)"
                    );

                    // Concurrently close both venues with tokio::join! to eliminate sequential lag
                    let hl_fut = async {
                        if hl_qty_to_close > Decimal::ZERO {
                            let hl_cl_id = format!("exit_hl_{}", unix_now_ns());
                            Some(self.executor.submit_hl_ioc(&self.symbol_hl, leg_hl_side, hl_qty_to_close, Decimal::ZERO, &hl_cl_id).await)
                        } else {
                            None
                        }
                    };

                    let bn_fut = async {
                        if bn_qty_to_close > Decimal::ZERO {
                            let bn_cl_id = format!("exit_bn_{}", unix_now_ns());
                            Some(self.executor.submit_bn_market(&self.symbol_bn, leg_bn_side, bn_qty_to_close, &bn_cl_id).await)
                        } else {
                            None
                        }
                    };

                    let (hl_fill_res, bn_fill_res) = tokio::join!(hl_fut, bn_fut);

                    let mut hl_price = Decimal::ZERO;
                    let mut bn_price = Decimal::ZERO;

                    if let Some(res) = hl_fill_res {
                        match res {
                            Ok(fill) => {
                                info!("HL position closed: qty={} price={}", fill.qty, fill.price);
                                self.snapshot.pos_hl.apply_fill(&fill);
                                self.accumulated_fees += fill.fee;
                                hl_price = fill.price;
                            }
                            Err(e) => error!(error = %e, "Failed to close HL position on exit"),
                        }
                    }

                    if let Some(res) = bn_fill_res {
                        match res {
                            Ok(fill) => {
                                info!("BN position closed: qty={} price={}", fill.qty, fill.price);
                                self.snapshot.pos_bn.apply_fill(&fill);
                                self.accumulated_fees += fill.fee;
                                bn_price = fill.price;
                            }
                            Err(e) => error!(error = %e, "Failed to close BN position on exit"),
                        }
                    }

                    if self.snapshot.pos_hl.net_qty.is_zero() && self.snapshot.pos_bn.net_qty.is_zero() {
                        info!("Both legs successfully flattened to zero.");
                        self.set_state(StrategyState::Idle);

                        let close_qty = hl_qty_to_close.max(bn_qty_to_close);
                        let hl_pnl = if entry_hl_price > Decimal::ZERO {
                            (hl_price - entry_hl_price) * close_qty
                        } else {
                            Decimal::ZERO
                        };
                        let bn_pnl = if entry_bn_price > Decimal::ZERO {
                            (entry_bn_price - bn_price) * close_qty
                        } else {
                            Decimal::ZERO
                        };
                        let gross_pnl = hl_pnl + bn_pnl;
                        let total_roundtrip_fees = self.accumulated_fees;
                        let net_pnl = gross_pnl - total_roundtrip_fees;
                        self.accumulated_fees = Decimal::ZERO;

                        let outcome = if net_pnl > Decimal::ZERO { "WIN" } else { "LOSS" };

                        let trade = TradeRoundTrip {
                            trade_id: format!("trade_{}", session_id),
                            symbol_hl: self.symbol_hl.clone(),
                            symbol_bn: self.symbol_bn.clone(),
                            entry_hl_price,
                            entry_bn_price,
                            entry_qty: close_qty,
                            exit_hl_price: Some(hl_price),
                            exit_bn_price: Some(bn_price),
                            exit_qty: Some(close_qty),
                            entry_basis_bps: current_spread_bps,
                            exit_basis_bps: Some(current_spread_bps),
                            gross_pnl_usdt: gross_pnl,
                            total_fees_usdt: total_roundtrip_fees,
                            net_pnl_usdt: net_pnl,
                            entry_time_ns: unix_now_ns(),
                            exit_time_ns: Some(unix_now_ns()),
                        };

                        info!("==================================================");
                        info!(
                            outcome = outcome,
                            trade_id = %trade.trade_id,
                            gross_pnl = %trade.gross_pnl_usdt,
                            total_fees = %trade.total_fees_usdt,
                            net_pnl = %trade.net_pnl_usdt,
                            "[TRADE {}] Net PnL: {} USDT | Gross PnL: {} USDT | Total Fees: {} USDT",
                            outcome, net_pnl, gross_pnl, total_roundtrip_fees
                        );
                        info!("  * Outcome         : [{}]", outcome);
                        info!("  * Traded Size     : {} tokens", close_qty);
                        info!(
                            "  * HL Leg (Buy -> Sell) : {} -> {} (Gross PnL: {:.5} USDT)",
                            entry_hl_price, hl_price, hl_pnl
                        );
                        info!(
                            "  * BN Leg (Sell -> Buy) : {} -> {} (Gross PnL: {:.5} USDT)",
                            entry_bn_price, bn_price, bn_pnl
                        );
                        info!("  * Total Fees      : {} USDT", total_roundtrip_fees);
                        info!("  * Net PnL (Real)  : {} USDT", net_pnl);
                        info!("==================================================");

                        let mut wal = self.wal.lock();
                        wal.log_trade(&trade)?;
                        self.snapshot.completed_trades.push(trade);
                        self.snapshot.total_fees_usdt += total_roundtrip_fees;
                        self.snapshot.total_realized_pnl_usdt += net_pnl;
                        wal.save_state(&self.snapshot)?;
                    } else {
                        error!("Position remaining after exit attempt. pos_hl={} pos_bn={}", self.snapshot.pos_hl.net_qty, self.snapshot.pos_bn.net_qty);
                        self.set_state(StrategyState::Unwind);
                        let wal = self.wal.lock();
                        wal.save_state(&self.snapshot)?;
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
