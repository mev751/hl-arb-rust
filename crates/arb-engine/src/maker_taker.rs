use crate::executor::{ExecutionClient, ExecutionError};
use crate::ws_feeds::{BnOrderUpdate, EngineEvent, FeedKind};
use arb_core::{
    clocks::{unix_now_ms, unix_now_ns},
    compute_hl_ioc_limit_price, compute_mid_basis_bps, quantize_price, quantize_qty,
    ArbDirection, EngineSnapshot, FillEvent, L1Quote, OrderSide, RiskManager, StrategyState,
    TradeRoundTrip, VenueId, WalManager,
};
use arb_strategy::StrategyConfig;
use parking_lot::Mutex;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

const CANCEL_WATCHDOG_MS: u64 = 2000;
const SEEN_TRADES_CAP: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderPhase {
    Resting,
    Canceling { sent_at_ms: u64 },
}

#[derive(Debug, Clone)]
pub struct ActiveBnOrder {
    pub client_order_id: String,
    pub side: OrderSide,
    pub price: Decimal,
    pub orig_qty: Decimal,
    pub cum_filled_qty: Decimal,
    pub is_entry: bool,
    pub reduce_only: bool,
    pub phase: OrderPhase,
}

#[derive(Debug, Clone)]
struct DesiredQuote {
    side: OrderSide,
    price: Decimal,
    qty: Decimal,
    is_entry: bool,
    reduce_only: bool,
}

#[derive(Debug, Default)]
struct FeedHealthState {
    bn_market_ok: bool,
    bn_user_ok: bool,
    hl_market_ok: bool,
    hl_trade_ok: bool,
    hl_last_quote_wall_ms: u64,
    bn_last_quote_wall_ms: u64,
}

impl FeedHealthState {
    fn all_ws_ok(&self) -> bool {
        self.bn_market_ok && self.bn_user_ok && self.hl_market_ok && self.hl_trade_ok
    }
}

#[derive(Debug, Default)]
struct RoundAccumulator {
    active: bool,
    direction: Option<ArbDirection>,
    entry_time_ns: u64,
    entry_basis_bps: Decimal,
    entry_hl_notional: Decimal,
    entry_hl_qty: Decimal,
    entry_bn_notional: Decimal,
    entry_bn_qty: Decimal,
    exit_hl_notional: Decimal,
    exit_hl_qty: Decimal,
    exit_bn_notional: Decimal,
    exit_bn_qty: Decimal,
    total_fees: Decimal,
}

impl RoundAccumulator {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Event-driven arbitrage engine implementing:
/// - Entry: Binance GTX Post-Only Maker Sell (0% fee) -> Hyperliquid IOC Taker Buy (0.86 bps via WS `post`)
/// - Exit:  Binance GTX Post-Only Maker Buy  (0% fee) -> Hyperliquid IOC Taker Sell (0.86 bps via WS `post`)
pub struct MakerTakerEngine<E: ExecutionClient + ?Sized> {
    config: StrategyConfig,
    risk: RiskManager,
    wal: Mutex<WalManager>,
    executor: Arc<E>,
    snapshot: EngineSnapshot,

    hl_quote: Option<L1Quote>,
    bn_quote: Option<L1Quote>,
    feeds: FeedHealthState,

    active_bn_order: Option<ActiveBnOrder>,
    last_order_action_ms: u64,
    seen_trade_ids: HashSet<(i64, i64)>,
    seen_trade_ring: VecDeque<(i64, i64)>,
    order_accounted_qty: HashMap<String, Decimal>,
    order_accounted_ring: VecDeque<String>,

    // Rolling EMA basis tracker (event-time driven)
    ema_basis_bps: Option<f64>,
    last_ema_event_ms: u64,
    latest_mid_basis_bps: Decimal,

    // Extreme 1s price jump guard (NO L1 OBI cancel)
    bn_mid_hist: VecDeque<(u64, Decimal)>,
    last_hist_event_ms: u64,

    // Round PnL accounting & hold timer
    round: RoundAccumulator,
    first_open_wall_ms: Option<u64>,
}

impl<E: ExecutionClient + ?Sized> MakerTakerEngine<E> {
    pub fn new(
        config: StrategyConfig,
        risk: RiskManager,
        wal: WalManager,
        executor: Arc<E>,
    ) -> Self {
        let snapshot = EngineSnapshot::new(config.symbol_hl.clone(), config.symbol_bn.clone());
        Self {
            config,
            risk,
            wal: Mutex::new(wal),
            executor,
            snapshot,
            hl_quote: None,
            bn_quote: None,
            feeds: FeedHealthState::default(),
            active_bn_order: None,
            last_order_action_ms: 0,
            seen_trade_ids: HashSet::new(),
            seen_trade_ring: VecDeque::new(),
            order_accounted_qty: HashMap::new(),
            order_accounted_ring: VecDeque::new(),
            ema_basis_bps: None,
            last_ema_event_ms: 0,
            latest_mid_basis_bps: Decimal::ZERO,
            bn_mid_hist: VecDeque::new(),
            last_hist_event_ms: 0,
            round: RoundAccumulator::default(),
            first_open_wall_ms: None,
        }
    }

    pub fn snapshot(&self) -> &EngineSnapshot {
        &self.snapshot
    }

    pub fn state(&self) -> StrategyState {
        self.snapshot.state
    }

    pub fn active_bn_order(&self) -> Option<&ActiveBnOrder> {
        self.active_bn_order.as_ref()
    }

    pub fn ema_basis_bps(&self) -> Option<f64> {
        self.ema_basis_bps
    }

    pub fn latest_quotes(&self) -> (Option<&L1Quote>, Option<&L1Quote>) {
        (self.hl_quote.as_ref(), self.bn_quote.as_ref())
    }

    pub fn restore_snapshot(&mut self, snapshot: EngineSnapshot) {
        self.snapshot = snapshot;
        if !self.snapshot.pos_hl.net_qty.is_zero() || !self.snapshot.pos_bn.net_qty.is_zero() {
            self.first_open_wall_ms = Some(unix_now_ms());
            self.round.active = true;
            self.round.direction = if self.snapshot.pos_bn.net_qty > Decimal::ZERO
                || self.snapshot.pos_hl.net_qty < Decimal::ZERO
            {
                Some(ArbDirection::LongSpread)
            } else {
                Some(ArbDirection::ShortSpread)
            };
            self.round.entry_time_ns = unix_now_ns();
            let hq = self.snapshot.pos_hl.net_qty.abs();
            let bq = self.snapshot.pos_bn.net_qty.abs();
            self.round.entry_hl_qty = hq;
            self.round.entry_hl_notional = hq * self.snapshot.pos_hl.entry_price;
            self.round.entry_bn_qty = bq;
            self.round.entry_bn_notional = bq * self.snapshot.pos_bn.entry_price;
        }
    }

    pub fn reset_circuit_breaker(&self) {
        self.risk.reset_circuit_breaker();
    }

    /// Convenience helper (primarily for dry-run & tests) to set all 4 feed health flags at once.
    pub fn set_all_feeds_healthy(&mut self, healthy: bool) {
        self.feeds.bn_market_ok = healthy;
        self.feeds.bn_user_ok = healthy;
        self.feeds.hl_market_ok = healthy;
        self.feeds.hl_trade_ok = healthy;
    }

    pub async fn process_event(&mut self, event: EngineEvent) -> Result<(), anyhow::Error> {
        match event {
            EngineEvent::FeedHealth { feed, healthy } => {
                self.on_feed_health(feed, healthy).await?;
            }
            EngineEvent::Quote(quote) => {
                self.on_quote(quote).await?;
            }
            EngineEvent::BnOrderUpdate(upd) => {
                self.on_bn_order_update(upd).await?;
            }
        }
        Ok(())
    }

    /// Update feed connectivity status; immediately cancel any resting BN order if any feed drops.
    pub async fn on_feed_health(&mut self, feed: FeedKind, healthy: bool) -> Result<(), anyhow::Error> {
        match feed {
            FeedKind::BnMarket => self.feeds.bn_market_ok = healthy,
            FeedKind::BnUser => self.feeds.bn_user_ok = healthy,
            FeedKind::HlMarket => self.feeds.hl_market_ok = healthy,
            FeedKind::HlTrade => self.feeds.hl_trade_ok = healthy,
        }

        if !healthy {
            warn!(feed = ?feed, "Feed unhealthy; pulling active Binance Maker quote and pausing quoting");
            self.cancel_active_bn_order("feed_unhealthy").await;
            if feed == FeedKind::BnUser {
                // Safety net: User Data Stream down means cancel ACKs may not arrive via WS
                let _ = self.executor.cancel_bn_all_open_orders(&self.config.symbol_bn).await;
                self.active_bn_order = None;
            }
        }
        self.refresh_state();
        Ok(())
    }

    /// Periodic maintenance tick (e.g., watchdog for stuck canceling orders or max_hold_secs).
    pub async fn on_maintenance_tick(&mut self) -> Result<(), anyhow::Error> {
        self.check_cancel_watchdog().await;
        if !self.feeds_are_fresh(unix_now_ms()) && self.active_bn_order.is_some() {
            warn!("Quotes became stale; canceling active Binance Maker order");
            self.cancel_active_bn_order("stale_quotes").await;
        }
        self.check_max_hold_emergency().await?;
        self.refresh_state();
        Ok(())
    }

    /// Gracefully cancel open BN orders, clean entry dust if flat on HL, and save WAL state.
    pub async fn shutdown(&mut self) -> Result<(), anyhow::Error> {
        info!("Shutting down MakerTakerEngine gracefully...");
        let _ = self
            .executor
            .cancel_bn_all_open_orders(&self.config.symbol_bn)
            .await;
        self.active_bn_order = None;
        self.cleanup_entry_dust_if_needed().await?;
        self.refresh_state();
        let wal = self.wal.lock();
        wal.save_state(&self.snapshot)?;
        Ok(())
    }

    async fn on_quote(&mut self, quote: L1Quote) -> Result<(), anyhow::Error> {
        let wall_ms = unix_now_ms();
        let event_ms = if quote.local_recv_ns > 0 {
            quote.local_recv_ns / 1_000_000
        } else {
            wall_ms
        };

        match quote.venue {
            VenueId::Hyperliquid if quote.symbol == self.config.symbol_hl => {
                self.hl_quote = Some(quote);
                self.feeds.hl_last_quote_wall_ms = wall_ms;
            }
            VenueId::BinanceFutures if quote.symbol == self.config.symbol_bn => {
                self.bn_quote = Some(quote);
                self.feeds.bn_last_quote_wall_ms = wall_ms;
            }
            _ => return Ok(()),
        }

        // First handle cancel watchdog & max hold checks
        self.check_cancel_watchdog().await;
        self.check_max_hold_emergency().await?;

        let (hl, bn) = match (&self.hl_quote, &self.bn_quote) {
            (Some(h), Some(b)) => (h.clone(), b.clone()),
            _ => return Ok(()),
        };

        // Update 2h rolling EMA basis & 1s momentum window
        let mom_1s_bps = self.update_basis_and_momentum(event_ms, &hl, &bn);

        // If halted or feeds unhealthy/stale, pull resting order and stop quoting
        if self.risk.is_halted() || self.snapshot.state == StrategyState::Unwind {
            self.cancel_active_bn_order("risk_halted_or_unwind").await;
            self.refresh_state();
            return Ok(());
        }

        if !self.feeds.all_ws_ok() || !self.feeds_are_fresh(wall_ms) {
            if self.active_bn_order.is_some() {
                self.cancel_active_bn_order("feeds_unhealthy_or_stale").await;
            }
            self.refresh_state();
            return Ok(());
        }

        // If cancel is currently in flight, wait strictly for terminal WS state before placing a new order
        if let Some(ref active) = self.active_bn_order {
            if matches!(active.phase, OrderPhase::Canceling { .. }) {
                return Ok(());
            }
        }

        // Extreme 1s price jump guard (bypasses reprice cooldown)
        if let Some(ref active) = self.active_bn_order {
            let oppose_sell = active.side == OrderSide::Sell && mom_1s_bps > self.config.extreme_jump_bps;
            let oppose_buy = active.side == OrderSide::Buy && mom_1s_bps < -self.config.extreme_jump_bps;
            if oppose_sell || oppose_buy {
                warn!(
                    side = ?active.side,
                    mom_1s_bps = %mom_1s_bps,
                    limit_bps = %self.config.extreme_jump_bps,
                    "Extreme 1s price jump detected; pulling BN Maker order immediately"
                );
                self.cancel_active_bn_order("extreme_price_jump").await;
                self.refresh_state();
                return Ok(());
            }
        }

        let desired = self.compute_desired_quote(&hl, &bn, mom_1s_bps);
        self.reconcile_bn_order(desired, wall_ms).await?;
        self.refresh_state();
        Ok(())
    }

    fn feeds_are_fresh(&self, now_wall_ms: u64) -> bool {
        let stale = self.config.stale_quote_ms;
        if stale == 0 {
            return true;
        }
        if self.feeds.hl_last_quote_wall_ms == 0 || self.feeds.bn_last_quote_wall_ms == 0 {
            return false;
        }
        now_wall_ms.saturating_sub(self.feeds.hl_last_quote_wall_ms) <= stale
            && now_wall_ms.saturating_sub(self.feeds.bn_last_quote_wall_ms) <= stale
    }

    fn update_basis_and_momentum(&mut self, event_ms: u64, hl: &L1Quote, bn: &L1Quote) -> Decimal {
        if let (Some(hl_mid), Some(bn_mid)) = (hl.mid_price(), bn.mid_price()) {
            if let Some(mid_basis) = compute_mid_basis_bps(bn_mid, hl_mid) {
                self.latest_mid_basis_bps = mid_basis;
                if let Some(mb_f) = mid_basis.to_f64() {
                    match self.ema_basis_bps {
                        None => {
                            self.ema_basis_bps = Some(mb_f);
                            self.last_ema_event_ms = event_ms;
                        }
                        Some(prev) => {
                            if event_ms >= self.last_ema_event_ms + 1000 {
                                let dt_s = ((event_ms - self.last_ema_event_ms) as f64 / 1000.0).min(60.0);
                                let tau = (self.config.ema_tau_secs.max(1)) as f64;
                                let alpha = 1.0 - (-dt_s / tau).exp();
                                self.ema_basis_bps = Some(prev + alpha * (mb_f - prev));
                                self.last_ema_event_ms = event_ms;
                            }
                        }
                    }
                }
            }

            if event_ms >= self.last_hist_event_ms + 100 || self.bn_mid_hist.is_empty() {
                self.bn_mid_hist.push_back((event_ms, bn_mid));
                self.last_hist_event_ms = event_ms;
                while self.bn_mid_hist.len() > 1 {
                    if let Some(&(old_ts, _)) = self.bn_mid_hist.front() {
                        if event_ms.saturating_sub(old_ts) > 1200 {
                            self.bn_mid_hist.pop_front();
                            continue;
                        }
                    }
                    break;
                }
            }

            if let Some(&(_, old_mid)) = self.bn_mid_hist.front() {
                if bn_mid > Decimal::ZERO {
                    return ((bn_mid - old_mid) / bn_mid) * dec!(10_000);
                }
            }
        }
        Decimal::ZERO
    }

    fn effective_mode1_thresholds(&self) -> (Decimal, Decimal) {
        self.config.effective_mode1_thresholds(self.ema_basis_bps)
    }

    fn effective_mode2_thresholds(&self) -> (Decimal, Decimal) {
        self.config.effective_mode2_thresholds(self.ema_basis_bps)
    }

    fn compute_chunk_qty(&self, ref_price: Decimal) -> Decimal {
        let raw_qty = match self.config.order_chunk_notional_usd {
            Some(notional) if notional > Decimal::ZERO && ref_price > Decimal::ZERO => {
                notional / ref_price
            }
            _ => self.config.order_chunk_qty,
        };
        quantize_qty(raw_qty, self.config.hl_step_size)
    }

    fn compute_desired_quote(
        &self,
        hl: &L1Quote,
        bn: &L1Quote,
        mom_1s_bps: Decimal,
    ) -> Option<DesiredQuote> {
        let tick = self.config.hl_tick_size;
        let max_dist = Decimal::from(self.config.max_quote_dist_ticks) * tick;
        let bps_denom = dec!(10_000);

        let pos_bn = self.snapshot.pos_bn.net_qty;
        let pos_hl = self.snapshot.pos_hl.net_qty;

        // =========================================================================
        // PRIORITY 1: EXIT EXISTING POSITIONS
        // =========================================================================

        // 1A: Exit Mode 1 (Short BN, Long HL): Buy BN Maker (reduce_only = true)
        if pos_bn < Decimal::ZERO
            && (pos_bn.abs() >= self.config.hl_step_size || !pos_hl.is_zero())
            && mom_1s_bps >= -self.config.extreme_jump_bps
        {
            let (_, eff_exit_bps) = self.effective_mode1_thresholds();
            let target_buy = quantize_price(
                hl.bid_price * (Decimal::ONE + eff_exit_bps / bps_denom),
                tick,
                false,
            );
            let q_buy = target_buy.min(bn.ask_price - tick);
            if q_buy > Decimal::ZERO && q_buy >= bn.bid_price - max_dist {
                let base_chunk = self.compute_chunk_qty(hl.bid_price).max(self.config.hl_step_size);
                let exit_qty = quantize_qty(base_chunk.min(pos_bn.abs()), self.config.bn_step_size);
                if exit_qty > Decimal::ZERO {
                    return Some(DesiredQuote {
                        side: OrderSide::Buy,
                        price: q_buy,
                        qty: exit_qty,
                        is_entry: false,
                        reduce_only: true,
                    });
                }
            }
        }

        // 1B: Exit Mode 2 (Long BN, Short HL): Sell BN Maker (reduce_only = true)
        if pos_bn > Decimal::ZERO
            && (pos_bn.abs() >= self.config.hl_step_size || !pos_hl.is_zero())
            && mom_1s_bps <= self.config.extreme_jump_bps
        {
            let (_, eff_rev_exit_bps) = self.effective_mode2_thresholds();
            let target_sell = quantize_price(
                hl.ask_price * (Decimal::ONE + eff_rev_exit_bps / bps_denom),
                tick,
                true,
            );
            let q_sell = target_sell.max(bn.bid_price + tick);
            if q_sell > Decimal::ZERO && q_sell <= bn.ask_price + max_dist {
                let base_chunk = self.compute_chunk_qty(hl.ask_price).max(self.config.hl_step_size);
                let exit_qty = quantize_qty(base_chunk.min(pos_bn.abs()), self.config.bn_step_size);
                if exit_qty > Decimal::ZERO {
                    return Some(DesiredQuote {
                        side: OrderSide::Sell,
                        price: q_sell,
                        qty: exit_qty,
                        is_entry: false,
                        reduce_only: true,
                    });
                }
            }
        }

        // =========================================================================
        // PRIORITY 2: ENTER NEW POSITIONS / SCALE IN
        // =========================================================================
        let eff_pos = pos_bn.abs().max(pos_hl.abs());

        // 2A: Mode 1 Entry: Short BN (Sell GTX, reduce_only = false)
        if pos_bn <= Decimal::ZERO && pos_hl >= Decimal::ZERO {
            let entry_qty = self.compute_chunk_qty(hl.ask_price);
            if entry_qty > Decimal::ZERO
                && eff_pos + entry_qty <= self.config.max_position_qty
                && mom_1s_bps <= self.config.extreme_jump_bps
                && self.risk.can_open_entry(entry_qty, pos_hl, pos_bn).is_ok()
            {
                let (eff_entry_bps, _) = self.effective_mode1_thresholds();
                let target_sell = quantize_price(
                    hl.ask_price * (Decimal::ONE + eff_entry_bps / bps_denom),
                    tick,
                    true,
                );
                let q_sell = target_sell.max(bn.bid_price + tick);
                if q_sell > Decimal::ZERO && q_sell <= bn.ask_price + max_dist {
                    return Some(DesiredQuote {
                        side: OrderSide::Sell,
                        price: q_sell,
                        qty: entry_qty,
                        is_entry: true,
                        reduce_only: false,
                    });
                }
            }
        }

        // 2B: Mode 2 Entry: Long BN (Buy GTX, reduce_only = false)
        if self.config.enable_reverse_arb && pos_bn >= Decimal::ZERO && pos_hl <= Decimal::ZERO {
            let entry_qty = self.compute_chunk_qty(hl.bid_price);
            if entry_qty > Decimal::ZERO
                && eff_pos + entry_qty <= self.config.max_position_qty
                && mom_1s_bps >= -self.config.extreme_jump_bps
                && self.risk.can_open_entry(entry_qty, pos_hl, pos_bn).is_ok()
            {
                let (eff_rev_entry_bps, _) = self.effective_mode2_thresholds();
                let target_buy = quantize_price(
                    hl.bid_price * (Decimal::ONE + eff_rev_entry_bps / bps_denom),
                    tick,
                    false,
                );
                let q_buy = target_buy.min(bn.ask_price - tick);
                if q_buy > Decimal::ZERO && q_buy >= bn.bid_price - max_dist {
                    return Some(DesiredQuote {
                        side: OrderSide::Buy,
                        price: q_buy,
                        qty: entry_qty,
                        is_entry: true,
                        reduce_only: false,
                    });
                }
            }
        }

        None
    }

    async fn reconcile_bn_order(
        &mut self,
        desired: Option<DesiredQuote>,
        now_ms: u64,
    ) -> Result<(), anyhow::Error> {
        let tick = self.config.hl_tick_size;

        match (&self.active_bn_order, &desired) {
            (Some(active), None) => {
                if now_ms.saturating_sub(self.last_order_action_ms) >= self.config.reprice_cooldown_ms {
                    debug!(
                        cl_id = %active.client_order_id,
                        "Desired quote cleared; canceling resting BN Maker order"
                    );
                    self.cancel_active_bn_order("out_of_range").await;
                }
            }
            (Some(active), Some(want)) => {
                let price_changed = (active.price - want.price).abs() >= tick;
                let side_changed = active.side != want.side;
                if (side_changed || price_changed)
                    && now_ms.saturating_sub(self.last_order_action_ms) >= self.config.reprice_cooldown_ms
                {
                    debug!(
                        cl_id = %active.client_order_id,
                        old_px = %active.price,
                        new_px = %want.price,
                        "Repricing BN Maker order (cancel-first)"
                    );
                    self.cancel_active_bn_order("reprice").await;
                }
            }
            (None, Some(want)) => {
                if now_ms.saturating_sub(self.last_order_action_ms) >= self.config.reprice_cooldown_ms {
                    self.place_bn_post_only(want, now_ms).await;
                }
            }
            (None, None) => {}
        }
        Ok(())
    }

    async fn place_bn_post_only(&mut self, want: &DesiredQuote, now_ms: u64) {
        let cl_id = format!("bn_mk_{}_{}", if want.is_entry { "en" } else { "ex" }, unix_now_ns());
        self.last_order_action_ms = now_ms;

        match self
            .executor
            .submit_bn_post_only(
                &self.config.symbol_bn,
                want.side,
                want.qty,
                want.price,
                want.reduce_only,
                &cl_id,
            )
            .await
        {
            Ok(ack) => {
                debug!(
                    cl_id = %cl_id,
                    order_id = ack.order_id,
                    side = ?want.side,
                    price = %want.price,
                    qty = %want.qty,
                    reduce_only = want.reduce_only,
                    "Placed BN GTX Maker order"
                );
                self.active_bn_order = Some(ActiveBnOrder {
                    client_order_id: cl_id,
                    side: want.side,
                    price: want.price,
                    orig_qty: want.qty,
                    cum_filled_qty: ack.executed_qty,
                    is_entry: want.is_entry,
                    reduce_only: want.reduce_only,
                    phase: OrderPhase::Resting,
                });
            }
            Err(ExecutionError::PostOnlyRejected(msg)) => {
                debug!(msg = %msg, price = %want.price, "BN GTX rejected (would take); skipping without risk penalty");
            }
            Err(e) => {
                warn!(error = %e, "Failed to place BN GTX order");
                self.risk.record_failure(e.to_string());
            }
        }
    }

    async fn cancel_active_bn_order(&mut self, reason: &str) {
        let Some(mut active) = self.active_bn_order.take() else {
            return;
        };
        if matches!(active.phase, OrderPhase::Canceling { .. }) {
            self.active_bn_order = Some(active);
            return;
        }

        let now_ms = unix_now_ms();
        self.last_order_action_ms = now_ms;

        match self
            .executor
            .cancel_order(
                VenueId::BinanceFutures,
                &self.config.symbol_bn,
                &active.client_order_id,
            )
            .await
        {
            Ok(()) => {
                debug!(cl_id = %active.client_order_id, reason = reason, "Sent cancel for BN Maker order; waiting terminal WS status");
                active.phase = OrderPhase::Canceling { sent_at_ms: now_ms };
                self.active_bn_order = Some(active);
            }
            Err(ExecutionError::OrderNotFound(msg)) => {
                // Likely filled or already closed in race; reconcile via REST query!
                debug!(cl_id = %active.client_order_id, msg = %msg, "BN cancel returned OrderNotFound (-2011); querying final status");
                self.active_bn_order = Some(active);
                self.reconcile_bn_order_via_rest().await;
            }
            Err(e) => {
                warn!(cl_id = %active.client_order_id, error = %e, "BN cancel failed; querying order state");
                active.phase = OrderPhase::Canceling { sent_at_ms: now_ms };
                self.active_bn_order = Some(active);
            }
        }
    }

    async fn check_cancel_watchdog(&mut self) {
        let timed_out = matches!(
            self.active_bn_order.as_ref().map(|o| o.phase),
            Some(OrderPhase::Canceling { sent_at_ms }) if unix_now_ms().saturating_sub(sent_at_ms) > CANCEL_WATCHDOG_MS
        );
        if timed_out {
            warn!("BN Canceling order exceeded 2s watchdog without WS terminal event; querying REST");
            self.reconcile_bn_order_via_rest().await;
        }
    }

    fn order_tracking_key(client_order_id: &str, order_id: i64) -> String {
        if !client_order_id.is_empty() {
            client_order_id.to_string()
        } else {
            format!("oid_{}", order_id)
        }
    }

    fn order_accounted_qty(&self, key: &str) -> Decimal {
        self.order_accounted_qty
            .get(key)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    fn record_order_accounted_qty(&mut self, key: &str, new_cum: Decimal) {
        let prev = self.order_accounted_qty(key);
        if new_cum <= prev {
            return;
        }
        let is_new = !self.order_accounted_qty.contains_key(key);
        self.order_accounted_qty.insert(key.to_string(), new_cum);
        if is_new {
            self.order_accounted_ring.push_back(key.to_string());
            while self.order_accounted_ring.len() > SEEN_TRADES_CAP {
                if let Some(old) = self.order_accounted_ring.pop_front() {
                    self.order_accounted_qty.remove(&old);
                }
            }
        }
    }

    async fn reconcile_bn_order_via_rest(&mut self) {
        let Some(active) = self.active_bn_order.clone() else {
            return;
        };
        match self
            .executor
            .query_bn_order(&self.config.symbol_bn, &active.client_order_id)
            .await
        {
            Ok(ack) => {
                let terminal = matches!(
                    ack.status.as_str(),
                    "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
                );
                let key = Self::order_tracking_key(&active.client_order_id, ack.order_id);
                let prev_accounted = self.order_accounted_qty(&key).max(active.cum_filled_qty);
                if ack.executed_qty > prev_accounted {
                    let missing = ack.executed_qty - prev_accounted;
                    self.record_order_accounted_qty(&key, ack.executed_qty);
                    let synthetic = FillEvent {
                        fill_id: format!("bn_reconcile_{}_{}", ack.order_id, unix_now_ns()),
                        client_order_id: active.client_order_id.clone(),
                        venue: VenueId::BinanceFutures,
                        symbol: self.config.symbol_bn.clone(),
                        side: active.side,
                        price: if ack.price > Decimal::ZERO { ack.price } else { active.price },
                        qty: missing,
                        fee: Decimal::ZERO,
                        fee_asset: "USDT".to_string(),
                        is_maker: true,
                        timestamp_ns: unix_now_ns(),
                    };
                    if let Err(e) = self.record_bn_fill(synthetic) {
                        error!(error = %e, "Failed recording reconciled BN fill");
                    }
                    if let Some(ref mut cur) = self.active_bn_order {
                        cur.cum_filled_qty = ack.executed_qty;
                    }
                } else if prev_accounted > Decimal::ZERO {
                    self.record_order_accounted_qty(&key, prev_accounted);
                }
                if terminal {
                    self.active_bn_order = None;
                    let _ = self.hedge_delta_if_needed().await;
                    let _ = self.cleanup_entry_dust_if_needed().await;
                }
            }
            Err(ExecutionError::OrderNotFound(_)) => {
                // Unknown to Binance -> clear local resting handle
                self.active_bn_order = None;
                let _ = self.hedge_delta_if_needed().await;
                let _ = self.cleanup_entry_dust_if_needed().await;
            }
            Err(e) => {
                warn!(error = %e, "REST query_bn_order failed during watchdog reconciliation");
            }
        }
    }

    async fn on_bn_order_update(&mut self, upd: BnOrderUpdate) -> Result<(), anyhow::Error> {
        let key = Self::order_tracking_key(&upd.client_order_id, upd.order_id);
        let matches_active = self
            .active_bn_order
            .as_ref()
            .map(|o| o.client_order_id == upd.client_order_id)
            .unwrap_or(false);

        if upd.exec_type == "TRADE" && upd.last_filled_qty > Decimal::ZERO {
            let dedup_key = (upd.order_id, upd.trade_id);
            let is_dup = upd.trade_id > 0 && self.seen_trade_ids.contains(&dedup_key);
            if !is_dup {
                if upd.trade_id > 0 {
                    self.seen_trade_ids.insert(dedup_key);
                    self.seen_trade_ring.push_back(dedup_key);
                    while self.seen_trade_ring.len() > SEEN_TRADES_CAP {
                        if let Some(old) = self.seen_trade_ring.pop_front() {
                            self.seen_trade_ids.remove(&old);
                        }
                    }
                }

                let prev_accounted = self.order_accounted_qty(&key);
                let effective_cum = if upd.cum_filled_qty > Decimal::ZERO {
                    upd.cum_filled_qty
                } else {
                    prev_accounted + upd.last_filled_qty
                };

                if effective_cum <= prev_accounted {
                    debug!(
                        key = %key,
                        cum = %effective_cum,
                        prev = %prev_accounted,
                        "Ignoring already-accounted BN WS TRADE update"
                    );
                } else {
                    let delta_qty = effective_cum - prev_accounted;
                    self.record_order_accounted_qty(&key, effective_cum);
                    let px = if upd.last_filled_px > Decimal::ZERO {
                        upd.last_filled_px
                    } else {
                        upd.avg_price
                    };
                    let fill = FillEvent {
                        fill_id: format!("bn_{}_{}", upd.order_id, upd.trade_id),
                        client_order_id: upd.client_order_id.clone(),
                        venue: VenueId::BinanceFutures,
                        symbol: self.config.symbol_bn.clone(),
                        side: upd.side,
                        price: px,
                        qty: delta_qty,
                        fee: upd.commission,
                        fee_asset: upd.commission_asset.clone(),
                        is_maker: upd.is_maker,
                        timestamp_ns: unix_now_ns(),
                    };
                    self.record_bn_fill(fill)?;

                    if matches_active {
                        if let Some(ref mut act) = self.active_bn_order {
                            act.cum_filled_qty = act.cum_filled_qty.max(effective_cum);
                        }
                    }
                }
            }
        }

        let is_terminal = matches!(
            upd.order_status.as_str(),
            "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
        );

        if is_terminal && matches_active {
            if let Some(active) = self.active_bn_order.take() {
                // Reconcile any fill missed prior to terminal event
                let prev_accounted = self.order_accounted_qty(&key).max(active.cum_filled_qty);
                if upd.cum_filled_qty > prev_accounted {
                    let missing = upd.cum_filled_qty - prev_accounted;
                    self.record_order_accounted_qty(&key, upd.cum_filled_qty);
                    let px = if upd.avg_price > Decimal::ZERO {
                        upd.avg_price
                    } else if upd.last_filled_px > Decimal::ZERO {
                        upd.last_filled_px
                    } else {
                        active.price
                    };
                    let missing_fill = FillEvent {
                        fill_id: format!("bn_miss_{}_{}", upd.order_id, unix_now_ns()),
                        client_order_id: active.client_order_id.clone(),
                        venue: VenueId::BinanceFutures,
                        symbol: self.config.symbol_bn.clone(),
                        side: active.side,
                        price: px,
                        qty: missing,
                        fee: Decimal::ZERO,
                        fee_asset: "USDT".to_string(),
                        is_maker: true,
                        timestamp_ns: unix_now_ns(),
                    };
                    self.record_bn_fill(missing_fill)?;
                }
            }
        }

        // Trigger immediate HL Taker hedge on any BN fill delta >= hl_step_size
        self.hedge_delta_if_needed().await?;

        if is_terminal {
            self.cleanup_entry_dust_if_needed().await?;
        }

        self.refresh_state();
        Ok(())
    }

    fn record_bn_fill(&mut self, fill: FillEvent) -> Result<(), anyhow::Error> {
        info!(
            venue = "BINANCE",
            side = ?fill.side,
            qty = %fill.qty,
            price = %fill.price,
            fee = %fill.fee,
            is_maker = fill.is_maker,
            "BN Maker fill recorded"
        );
        {
            let mut wal = self.wal.lock();
            wal.log_fill(&fill)?;
        }

        if !self.round.active {
            self.round.active = true;
            self.round.entry_time_ns = fill.timestamp_ns;
            self.round.entry_basis_bps = self.latest_mid_basis_bps;
            self.round.direction = if fill.side == OrderSide::Sell {
                Some(ArbDirection::ShortSpread)
            } else {
                Some(ArbDirection::LongSpread)
            };
            self.first_open_wall_ms = Some(unix_now_ms());
        }

        let is_entry = match self.round.direction {
            Some(ArbDirection::ShortSpread) => fill.side == OrderSide::Sell,
            Some(ArbDirection::LongSpread) => fill.side == OrderSide::Buy,
            None => fill.side == OrderSide::Sell,
        };

        if is_entry {
            self.round.entry_bn_notional += fill.qty * fill.price;
            self.round.entry_bn_qty += fill.qty;
        } else {
            self.round.exit_bn_notional += fill.qty * fill.price;
            self.round.exit_bn_qty += fill.qty;
        }
        self.round.total_fees += fill.fee;
        self.snapshot.pos_bn.apply_fill(&fill);
        self.maybe_finalize_round()?;
        Ok(())
    }

    async fn hedge_delta_if_needed(&mut self) -> Result<(), anyhow::Error> {
        let step = self.config.hl_step_size;
        let slippages = if self.config.hl_retry_slippages_bps.is_empty() {
            vec![dec!(10.0), dec!(25.0), dec!(50.0)]
        } else {
            self.config.hl_retry_slippages_bps.clone()
        };

        loop {
            // Target: pos_hl + pos_bn == 0 => delta = pos_hl + pos_bn
            // Example: pos_hl = 0, pos_bn = -0.10 => delta = -0.10 => Buy 0.1 HL
            // Example: pos_hl = 0.1, pos_bn = 0.00 => delta = +0.10 => Sell 0.1 HL
            let delta = self.snapshot.pos_hl.net_qty + self.snapshot.pos_bn.net_qty;
            if delta.abs() < step {
                break;
            }

            let is_buy = delta < Decimal::ZERO;
            let side = if is_buy { OrderSide::Buy } else { OrderSide::Sell };
            let reduce_only = if is_buy {
                self.snapshot.pos_hl.net_qty < Decimal::ZERO
            } else {
                self.snapshot.pos_hl.net_qty > Decimal::ZERO
            };
            let hedge_qty = quantize_qty(delta.abs(), step);
            if hedge_qty <= Decimal::ZERO {
                break;
            }

            // Cap reduce_only qty to current HL position size so reduceOnly never rejects
            let hedge_qty = if reduce_only {
                quantize_qty(hedge_qty.min(self.snapshot.pos_hl.net_qty.abs()), step)
            } else {
                hedge_qty
            };
            if hedge_qty <= Decimal::ZERO {
                break;
            }

            let best_px = match (&self.hl_quote, is_buy) {
                (Some(q), true) => q.ask_price,
                (Some(q), false) => q.bid_price,
                (None, _) => Decimal::ZERO,
            };

            let mut hedged = false;
            let pre_hl_pos = self.snapshot.pos_hl.net_qty;

            for (attempt_idx, &slip_bps) in slippages.iter().enumerate() {
                let limit_price = if best_px > Decimal::ZERO {
                    compute_hl_ioc_limit_price(best_px, slip_bps, self.config.hl_tick_size, is_buy)
                } else {
                    Decimal::ZERO
                };
                let cl_id = format!("hl_h_{}_{}", attempt_idx + 1, unix_now_ns());

                info!(
                    attempt = attempt_idx + 1,
                    side = ?side,
                    qty = %hedge_qty,
                    limit_price = %limit_price,
                    slip_bps = %slip_bps,
                    reduce_only = reduce_only,
                    "Dispatching HL IOC Leg 2 hedge"
                );

                match self
                    .executor
                    .submit_hl_ioc_with_opts(
                        &self.config.symbol_hl,
                        side,
                        hedge_qty,
                        limit_price,
                        reduce_only,
                        &cl_id,
                    )
                    .await
                {
                    Ok(hl_fill) => {
                        self.apply_hl_fill(hl_fill, is_buy)?;
                        hedged = true;
                        break;
                    }
                    Err(ExecutionError::ZeroFill) => {
                        warn!(
                            attempt = attempt_idx + 1,
                            slip_bps = %slip_bps,
                            "HL IOC ZeroFill; widening slippage on next retry"
                        );
                        continue;
                    }
                    Err(ExecutionError::Timeout(msg)) | Err(ExecutionError::Network(msg)) => {
                        warn!(
                            attempt = attempt_idx + 1,
                            error = %msg,
                            "HL IOC transport uncertain; querying live HL position before retry"
                        );
                        if let Ok(live_hl) = self
                            .executor
                            .query_position(VenueId::Hyperliquid, &self.config.symbol_hl)
                            .await
                        {
                            let filled_delta = live_hl - pre_hl_pos;
                            let expected_sign = if is_buy { filled_delta } else { -filled_delta };
                            if expected_sign >= step {
                                let est_px = if best_px > Decimal::ZERO {
                                    best_px
                                } else {
                                    self.snapshot.pos_hl.entry_price
                                };
                                let synth = FillEvent {
                                    fill_id: format!("hl_verified_{}", unix_now_ns()),
                                    client_order_id: cl_id,
                                    venue: VenueId::Hyperliquid,
                                    symbol: self.config.symbol_hl.clone(),
                                    side,
                                    price: est_px,
                                    qty: expected_sign,
                                    fee: est_px * expected_sign * Decimal::new(86, 6),
                                    fee_asset: "USDC".to_string(),
                                    is_maker: false,
                                    timestamp_ns: unix_now_ns(),
                                };
                                info!(qty = %expected_sign, "Verified HL fill landed on-chain after timeout");
                                self.apply_hl_fill(synth, is_buy)?;
                                hedged = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!(attempt = attempt_idx + 1, error = %e, "HL IOC hedge error");
                        break;
                    }
                }
            }

            if !hedged {
                error!(
                    unhedged_qty = %hedge_qty,
                    "CRITICAL: All HL hedge retries exhausted! Flattening unhedged BN leg and tripping circuit breaker."
                );
                let unwind_side = if is_buy { OrderSide::Buy } else { OrderSide::Sell };
                let cl_unwind = format!("bn_unwind_{}", unix_now_ns());
                match self
                    .executor
                    .submit_bn_market_with_opts(
                        &self.config.symbol_bn,
                        unwind_side,
                        hedge_qty,
                        is_buy, // reduce_only when buying back BN short
                        &cl_unwind,
                    )
                    .await
                {
                    Ok(u_fill) => {
                        self.record_order_accounted_qty(&cl_unwind, u_fill.qty);
                        let _ = self.wal.lock().log_fill(&u_fill);
                        self.round.total_fees += u_fill.fee;
                        self.snapshot.pos_bn.apply_fill(&u_fill);
                        let _ = self.maybe_finalize_round();
                    }
                    Err(ue) => {
                        error!(error = %ue, "Emergency BN unwind failed!");
                    }
                }
                self.risk.record_failure("HL Leg 2 hedge exhausted all slippage retries".into());
                // Trip breaker cleanly
                while !self.risk.is_halted() {
                    self.risk.record_failure("HL Leg 2 hedge circuit trip".into());
                }
                self.snapshot.state = StrategyState::Halted;
                let wal = self.wal.lock();
                wal.save_state(&self.snapshot)?;
                break;
            }
        }
        Ok(())
    }

    fn apply_hl_fill(&mut self, fill: FillEvent, is_buy: bool) -> Result<(), anyhow::Error> {
        {
            let mut wal = self.wal.lock();
            wal.log_fill(&fill)?;
        }
        let is_entry = match self.round.direction {
            Some(ArbDirection::ShortSpread) => is_buy,
            Some(ArbDirection::LongSpread) => !is_buy,
            None => is_buy,
        };

        if is_entry {
            self.round.entry_hl_notional += fill.qty * fill.price;
            self.round.entry_hl_qty += fill.qty;
            self.risk.commit_fill(fill.qty);
        } else {
            self.round.exit_hl_notional += fill.qty * fill.price;
            self.round.exit_hl_qty += fill.qty;
        }
        self.round.total_fees += fill.fee;
        self.snapshot.pos_hl.apply_fill(&fill);
        self.maybe_finalize_round()?;

        let wal = self.wal.lock();
        wal.save_state(&self.snapshot)?;
        Ok(())
    }

    /// Clean up sub-`hl_step_size` Binance entry dust when HL is flat and no BN order is resting.
    async fn cleanup_entry_dust_if_needed(&mut self) -> Result<(), anyhow::Error> {
        if self.active_bn_order.is_some() {
            return Ok(());
        }
        let hl_qty = self.snapshot.pos_hl.net_qty;
        let bn_qty = self.snapshot.pos_bn.net_qty;
        if hl_qty.is_zero() && !bn_qty.is_zero() && bn_qty.abs() < self.config.hl_step_size {
            let dust_qty = bn_qty.abs();
            let close_side = if bn_qty < Decimal::ZERO {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let cl_id = format!("bn_dust_{}", unix_now_ns());
            warn!(
                dust_qty = %dust_qty,
                side = ?close_side,
                "Flattening sub-HL-step entry dust on Binance with reduceOnly=true"
            );
            match self
                .executor
                .submit_bn_market_with_opts(
                    &self.config.symbol_bn,
                    close_side,
                    dust_qty,
                    true,
                    &cl_id,
                )
                .await
            {
                Ok(fill) => {
                    self.record_order_accounted_qty(&cl_id, fill.qty);
                    {
                        let mut wal = self.wal.lock();
                        wal.log_fill(&fill)?;
                    }
                    if fill.side == OrderSide::Buy {
                        self.round.exit_bn_notional += fill.qty * fill.price;
                        self.round.exit_bn_qty += fill.qty;
                    }
                    self.round.total_fees += fill.fee;
                    self.snapshot.pos_bn.apply_fill(&fill);
                    self.maybe_finalize_round()?;
                    let wal = self.wal.lock();
                    wal.save_state(&self.snapshot)?;
                }
                Err(e) => {
                    error!(error = %e, dust_qty = %dust_qty, "Failed to flatten entry dust on Binance");
                }
            }
        }
        Ok(())
    }

    async fn check_max_hold_emergency(&mut self) -> Result<(), anyhow::Error> {
        if self.config.max_hold_secs == 0 {
            return Ok(());
        }
        let Some(opened_at) = self.first_open_wall_ms else {
            return Ok(());
        };
        if self.snapshot.pos_hl.net_qty.is_zero() && self.snapshot.pos_bn.net_qty.is_zero() {
            self.first_open_wall_ms = None;
            return Ok(());
        }
        let elapsed_secs = unix_now_ms().saturating_sub(opened_at) / 1000;
        if elapsed_secs < self.config.max_hold_secs {
            return Ok(());
        }

        warn!(
            elapsed_secs = elapsed_secs,
            max_hold_secs = self.config.max_hold_secs,
            "Max hold duration exceeded; executing Taker emergency close"
        );
        self.cancel_active_bn_order("max_hold_close").await;
        let _ = self
            .executor
            .cancel_bn_all_open_orders(&self.config.symbol_bn)
            .await;
        self.active_bn_order = None;

        let hl_qty = self.snapshot.pos_hl.net_qty;
        let bn_qty = self.snapshot.pos_bn.net_qty;
        if !hl_qty.is_zero() {
            let is_long = hl_qty > Decimal::ZERO;
            let hl_side = if is_long { OrderSide::Sell } else { OrderSide::Buy };
            let lim = self
                .hl_quote
                .as_ref()
                .map(|q| {
                    if is_long {
                        compute_hl_ioc_limit_price(q.bid_price, dec!(50.0), self.config.hl_tick_size, false)
                    } else {
                        compute_hl_ioc_limit_price(q.ask_price, dec!(50.0), self.config.hl_tick_size, true)
                    }
                })
                .unwrap_or(Decimal::ZERO);
            let cl_hl = format!("maxhold_hl_{}", unix_now_ns());
            if let Ok(f) = self
                .executor
                .submit_hl_ioc_with_opts(&self.config.symbol_hl, hl_side, hl_qty.abs(), lim, true, &cl_hl)
                .await
            {
                let _ = self.wal.lock().log_fill(&f);
                self.round.exit_hl_notional += f.qty * f.price;
                self.round.exit_hl_qty += f.qty;
                self.round.total_fees += f.fee;
                self.snapshot.pos_hl.apply_fill(&f);
            }
        }
        if !bn_qty.is_zero() {
            let side = if bn_qty < Decimal::ZERO { OrderSide::Buy } else { OrderSide::Sell };
            let cl_bn = format!("maxhold_bn_{}", unix_now_ns());
            if let Ok(f) = self
                .executor
                .submit_bn_market_with_opts(&self.config.symbol_bn, side, bn_qty.abs(), true, &cl_bn)
                .await
            {
                self.record_order_accounted_qty(&cl_bn, f.qty);
                let _ = self.wal.lock().log_fill(&f);
                self.round.exit_bn_notional += f.qty * f.price;
                self.round.exit_bn_qty += f.qty;
                self.round.total_fees += f.fee;
                self.snapshot.pos_bn.apply_fill(&f);
            }
        }
        self.maybe_finalize_round()?;
        let wal = self.wal.lock();
        wal.save_state(&self.snapshot)?;
        Ok(())
    }

    fn maybe_finalize_round(&mut self) -> Result<(), anyhow::Error> {
        if !self.snapshot.pos_hl.net_qty.is_zero() || !self.snapshot.pos_bn.net_qty.is_zero() {
            return Ok(());
        }
        self.first_open_wall_ms = None;
        if !self.round.active {
            return Ok(());
        }

        let entry_hl_px = if self.round.entry_hl_qty > Decimal::ZERO {
            self.round.entry_hl_notional / self.round.entry_hl_qty
        } else {
            Decimal::ZERO
        };
        let exit_hl_px = if self.round.exit_hl_qty > Decimal::ZERO {
            self.round.exit_hl_notional / self.round.exit_hl_qty
        } else {
            Decimal::ZERO
        };
        let entry_bn_px = if self.round.entry_bn_qty > Decimal::ZERO {
            self.round.entry_bn_notional / self.round.entry_bn_qty
        } else {
            Decimal::ZERO
        };
        let exit_bn_px = if self.round.exit_bn_qty > Decimal::ZERO {
            self.round.exit_bn_notional / self.round.exit_bn_qty
        } else {
            Decimal::ZERO
        };

        // Exact cashflow PnL across both legs
        let direction = self.round.direction.unwrap_or(ArbDirection::ShortSpread);
        let (hl_pnl, bn_pnl) = match direction {
            ArbDirection::ShortSpread => {
                // Short BN (entry sell, exit buy), Long HL (entry buy, exit sell)
                let hl_pnl = self.round.exit_hl_notional - self.round.entry_hl_notional;
                let bn_pnl = self.round.entry_bn_notional - self.round.exit_bn_notional;
                (hl_pnl, bn_pnl)
            }
            ArbDirection::LongSpread => {
                // Long BN (entry buy, exit sell), Short HL (entry sell, exit buy)
                let hl_pnl = self.round.entry_hl_notional - self.round.exit_hl_notional;
                let bn_pnl = self.round.exit_bn_notional - self.round.entry_bn_notional;
                (hl_pnl, bn_pnl)
            }
        };
        let gross_pnl = hl_pnl + bn_pnl;
        let fees = self.round.total_fees;
        let net_pnl = gross_pnl - fees;
        let traded_qty = self.round.entry_bn_qty.max(self.round.entry_hl_qty);
        let outcome = if net_pnl > Decimal::ZERO { "WIN" } else { "LOSS" };

        let trade = TradeRoundTrip {
            trade_id: format!("trade_mt_{}", unix_now_ns()),
            symbol_hl: self.config.symbol_hl.clone(),
            symbol_bn: self.config.symbol_bn.clone(),
            direction: Some(direction),
            entry_hl_price: entry_hl_px,
            entry_bn_price: entry_bn_px,
            entry_qty: traded_qty,
            exit_hl_price: Some(exit_hl_px),
            exit_bn_price: Some(exit_bn_px),
            exit_qty: Some(self.round.exit_bn_qty.max(self.round.exit_hl_qty)),
            entry_basis_bps: self.round.entry_basis_bps,
            exit_basis_bps: Some(self.latest_mid_basis_bps),
            gross_pnl_usdt: gross_pnl,
            total_fees_usdt: fees,
            net_pnl_usdt: net_pnl,
            entry_time_ns: self.round.entry_time_ns,
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
            outcome, net_pnl, gross_pnl, fees
        );

        {
            let mut wal = self.wal.lock();
            wal.log_trade(&trade)?;
            self.snapshot.completed_trades.push(trade);
            self.snapshot.total_fees_usdt += fees;
            self.snapshot.total_realized_pnl_usdt += net_pnl;
            wal.save_state(&self.snapshot)?;
        }
        self.round.reset();
        Ok(())
    }

    fn refresh_state(&mut self) {
        if self.risk.is_halted() {
            self.snapshot.state = StrategyState::Halted;
            return;
        }
        if self.snapshot.state == StrategyState::Unwind {
            if self.snapshot.pos_hl.net_qty.is_zero() && self.snapshot.pos_bn.net_qty.is_zero() {
                self.snapshot.state = StrategyState::Idle;
            }
            return;
        }
        let flat = self.snapshot.pos_hl.net_qty.is_zero() && self.snapshot.pos_bn.net_qty.is_zero();
        self.snapshot.state = match (flat, &self.active_bn_order) {
            (true, Some(o)) if o.is_entry => StrategyState::QuotingEntry,
            (true, _) => StrategyState::Idle,
            (false, Some(o)) if !o.is_entry => StrategyState::QuotingExit,
            (false, _) => StrategyState::Open,
        };
    }
}
