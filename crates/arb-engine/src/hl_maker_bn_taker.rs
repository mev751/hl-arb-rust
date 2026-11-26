use crate::executor::{ExecutionClient, ExecutionError};
use crate::maker_taker::OrderPhase;
use crate::ws_feeds::{EngineEvent, FeedKind, HlOrderUpdate, HlUserFill};
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

const CANCEL_WATCHDOG_MS: u64 = 3000;
const SEEN_TRADES_CAP: usize = 10_000;

#[derive(Debug, Clone)]
pub struct ActiveHlOrder {
    pub oid: u64,
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
    hl_market_ok: bool,
    hl_trade_ok: bool,
    hl_user_ok: bool,
    hl_last_quote_wall_ms: u64,
    bn_last_quote_wall_ms: u64,
}

impl FeedHealthState {
    fn all_ws_ok(&self) -> bool {
        self.bn_market_ok && self.hl_market_ok && self.hl_trade_ok && self.hl_user_ok
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

/// Event-driven reversed-leg arbitrage engine implementing:
/// - Entry: Hyperliquid ALO Post-Only Maker (~0.29 bps) -> Binance USD-M Market Taker (~20ms, 4.0 bps)
/// - Exit:  Hyperliquid ALO Post-Only Maker (~0.29 bps) -> Binance USD-M Market Taker (~20ms, 4.0 bps)
pub struct HlMakerBnTakerEngine<E: ExecutionClient + ?Sized> {
    config: StrategyConfig,
    risk: RiskManager,
    wal: Mutex<WalManager>,
    executor: Arc<E>,
    snapshot: EngineSnapshot,

    hl_quote: Option<L1Quote>,
    bn_quote: Option<L1Quote>,
    feeds: FeedHealthState,

    active_hl_order: Option<ActiveHlOrder>,
    last_order_action_ms: u64,
    seen_tids: HashSet<u64>,
    seen_tid_ring: VecDeque<u64>,
    order_accounted_qty: HashMap<u64, Decimal>,
    order_accounted_ring: VecDeque<u64>,
    terminal_accounted_oids: HashSet<u64>,
    terminal_oid_ring: VecDeque<u64>,

    // Rolling EMA basis tracker (event-time driven)
    ema_basis_bps: Option<f64>,
    last_ema_event_ms: u64,
    latest_mid_basis_bps: Decimal,

    // Extreme 1s price jump guard on Binance mid
    bn_mid_hist: VecDeque<(u64, Decimal)>,
    last_hist_event_ms: u64,

    // Round PnL accounting & hold timer
    round: RoundAccumulator,
    first_open_wall_ms: Option<u64>,
}

impl<E: ExecutionClient + ?Sized> HlMakerBnTakerEngine<E> {
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
            active_hl_order: None,
            last_order_action_ms: 0,
            seen_tids: HashSet::new(),
            seen_tid_ring: VecDeque::new(),
            order_accounted_qty: HashMap::new(),
            order_accounted_ring: VecDeque::new(),
            terminal_accounted_oids: HashSet::new(),
            terminal_oid_ring: VecDeque::new(),
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

    pub fn active_hl_order(&self) -> Option<&ActiveHlOrder> {
        self.active_hl_order.as_ref()
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
        self.feeds.hl_market_ok = healthy;
        self.feeds.hl_trade_ok = healthy;
        self.feeds.hl_user_ok = healthy;
    }

    pub async fn process_event(&mut self, event: EngineEvent) -> Result<(), anyhow::Error> {
        match event {
            EngineEvent::FeedHealth { feed, healthy } => {
                self.on_feed_health(feed, healthy).await?;
            }
            EngineEvent::Quote(quote) => {
                self.on_quote(quote).await?;
            }
            EngineEvent::HlUserFill(fill) => {
                self.on_hl_user_fill(fill).await?;
            }
            EngineEvent::HlOrderUpdate(upd) => {
                self.on_hl_order_update(upd).await?;
            }
            EngineEvent::BnOrderUpdate(_) => {}
        }
        Ok(())
    }

    /// Update feed connectivity status; immediately cancel any resting HL order if any required feed drops.
    pub async fn on_feed_health(&mut self, feed: FeedKind, healthy: bool) -> Result<(), anyhow::Error> {
        match feed {
            FeedKind::BnMarket => self.feeds.bn_market_ok = healthy,
            FeedKind::HlMarket => self.feeds.hl_market_ok = healthy,
            FeedKind::HlTrade => self.feeds.hl_trade_ok = healthy,
            FeedKind::HlUser => self.feeds.hl_user_ok = healthy,
            FeedKind::BnUser => return Ok(()),
        }

        if !healthy {
            warn!(feed = ?feed, "Feed unhealthy; pulling active Hyperliquid Maker quote and pausing quoting");
            self.cancel_active_hl_order("feed_unhealthy").await;
            if feed == FeedKind::HlUser {
                let _ = self
                    .executor
                    .cancel_hl_all_open_orders(&self.config.symbol_hl)
                    .await;
                self.active_hl_order = None;
            }
        }
        self.refresh_state();
        Ok(())
    }

    /// Periodic maintenance tick (watchdog for stuck canceling orders, stale quotes, max hold, residual delta).
    pub async fn on_maintenance_tick(&mut self) -> Result<(), anyhow::Error> {
        self.check_cancel_watchdog().await;
        if !self.feeds_are_fresh(unix_now_ms()) && self.active_hl_order.is_some() {
            warn!("Quotes became stale; canceling active Hyperliquid Maker order");
            self.cancel_active_hl_order("stale_quotes").await;
        }
        self.check_max_hold_emergency().await?;
        if self.active_hl_order.is_none() {
            let delta = self.snapshot.pos_hl.net_qty + self.snapshot.pos_bn.net_qty;
            if delta.abs() >= self.config.bn_step_size {
                warn!(delta = %delta, "Maintenance tick detected unhedged delta; hedging on Binance now");
                let _ = self.hedge_delta_if_needed().await;
            }
            let _ = self.cleanup_entry_dust_if_needed().await;
        }
        self.refresh_state();
        Ok(())
    }

    /// Gracefully cancel open HL orders for this symbol, clean entry dust if flat on BN, and save WAL state.
    pub async fn shutdown(&mut self) -> Result<(), anyhow::Error> {
        info!("Shutting down HlMakerBnTakerEngine gracefully...");
        let _ = self
            .executor
            .cancel_hl_all_open_orders(&self.config.symbol_hl)
            .await;
        self.active_hl_order = None;
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

        self.check_cancel_watchdog().await;
        self.check_max_hold_emergency().await?;

        let (hl, bn) = match (&self.hl_quote, &self.bn_quote) {
            (Some(h), Some(b)) => (h.clone(), b.clone()),
            _ => return Ok(()),
        };

        let mom_1s_bps = self.update_basis_and_momentum(event_ms, &hl, &bn);

        if self.risk.is_halted() || self.snapshot.state == StrategyState::Unwind {
            self.cancel_active_hl_order("risk_halted_or_unwind").await;
            self.refresh_state();
            return Ok(());
        }

        if !self.feeds.all_ws_ok() || !self.feeds_are_fresh(wall_ms) {
            if self.active_hl_order.is_some() {
                self.cancel_active_hl_order("feeds_unhealthy_or_stale").await;
            }
            self.refresh_state();
            return Ok(());
        }

        if let Some(ref active) = self.active_hl_order {
            if matches!(active.phase, OrderPhase::Canceling { .. }) {
                return Ok(());
            }
        }

        // Extreme 1s price jump guard on Binance mid protects our resting HL Maker order
        if let Some(ref active) = self.active_hl_order {
            let oppose_sell = active.side == OrderSide::Sell && mom_1s_bps > self.config.extreme_jump_bps;
            let oppose_buy = active.side == OrderSide::Buy && mom_1s_bps < -self.config.extreme_jump_bps;
            if oppose_sell || oppose_buy {
                warn!(
                    side = ?active.side,
                    mom_1s_bps = %mom_1s_bps,
                    limit_bps = %self.config.extreme_jump_bps,
                    "Extreme 1s BN price jump detected; pulling HL Maker order immediately"
                );
                self.cancel_active_hl_order("extreme_price_jump").await;
                self.refresh_state();
                return Ok(());
            }
        }

        let desired = self.compute_desired_quote(&hl, &bn, mom_1s_bps);
        self.reconcile_hl_order(desired, wall_ms).await?;
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
        // PRIORITY 1: EXIT EXISTING POSITIONS (HL Maker -> BN Market Taker)
        // =========================================================================

        // 1A: Exit Mode 1 (Long HL, Short BN): Sell HL Maker (reduce_only = true)
        if pos_hl > Decimal::ZERO
            && (pos_hl.abs() >= self.config.bn_step_size || !pos_bn.is_zero())
            && mom_1s_bps <= self.config.extreme_jump_bps
        {
            let (_, eff_exit_bps) = self.effective_mode1_thresholds();
            let denom = Decimal::ONE + eff_exit_bps / bps_denom;
            if denom > Decimal::ZERO {
                let target_sell = quantize_price(bn.ask_price / denom, tick, true);
                let q_sell = target_sell.max(hl.bid_price + tick);
                if q_sell > Decimal::ZERO && q_sell <= hl.ask_price + max_dist {
                    let base_chunk = self.compute_chunk_qty(hl.ask_price).max(self.config.hl_step_size);
                    let exit_qty = quantize_qty(base_chunk.min(pos_hl.abs()), self.config.hl_step_size);
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
        }

        // 1B: Exit Mode 2 (Short HL, Long BN): Buy HL Maker (reduce_only = true)
        if pos_hl < Decimal::ZERO
            && (pos_hl.abs() >= self.config.bn_step_size || !pos_bn.is_zero())
            && mom_1s_bps >= -self.config.extreme_jump_bps
        {
            let (_, eff_rev_exit_bps) = self.effective_mode2_thresholds();
            let denom = Decimal::ONE + eff_rev_exit_bps / bps_denom;
            if denom > Decimal::ZERO {
                let target_buy = quantize_price(bn.bid_price / denom, tick, false);
                let q_buy = target_buy.min(hl.ask_price - tick);
                if q_buy > Decimal::ZERO && q_buy >= hl.bid_price - max_dist {
                    let base_chunk = self.compute_chunk_qty(hl.bid_price).max(self.config.hl_step_size);
                    let exit_qty = quantize_qty(base_chunk.min(pos_hl.abs()), self.config.hl_step_size);
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
        }

        // =========================================================================
        // PRIORITY 2: ENTER NEW POSITIONS (STRICT: Flat positions only)
        // =========================================================================
        let is_flat = pos_bn.is_zero() && pos_hl.is_zero();
        if !is_flat {
            return None;
        }

        // 2A: Mode 1 Entry (ShortSpread: Long HL Maker + Short BN Taker) -> HL Maker Buy
        {
            let entry_qty = self.compute_chunk_qty(hl.bid_price);
            if entry_qty > Decimal::ZERO
                && entry_qty <= self.config.max_position_qty
                && mom_1s_bps >= -self.config.extreme_jump_bps
                && self.risk.can_open_entry(entry_qty, pos_hl, pos_bn).is_ok()
            {
                let (eff_entry_bps, _) = self.effective_mode1_thresholds();
                let denom = Decimal::ONE + eff_entry_bps / bps_denom;
                if denom > Decimal::ZERO {
                    let target_buy = quantize_price(bn.bid_price / denom, tick, false);
                    let q_buy = target_buy.min(hl.ask_price - tick);
                    if q_buy > Decimal::ZERO && q_buy >= hl.bid_price - max_dist {
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
        }

        // 2B: Mode 2 Entry (LongSpread: Short HL Maker + Long BN Taker) -> HL Maker Sell
        if self.config.enable_reverse_arb {
            let entry_qty = self.compute_chunk_qty(hl.ask_price);
            if entry_qty > Decimal::ZERO
                && entry_qty <= self.config.max_position_qty
                && mom_1s_bps <= self.config.extreme_jump_bps
                && self.risk.can_open_entry(entry_qty, pos_hl, pos_bn).is_ok()
            {
                let (eff_rev_entry_bps, _) = self.effective_mode2_thresholds();
                let denom = Decimal::ONE + eff_rev_entry_bps / bps_denom;
                if denom > Decimal::ZERO {
                    let target_sell = quantize_price(bn.ask_price / denom, tick, true);
                    let q_sell = target_sell.max(hl.bid_price + tick);
                    if q_sell > Decimal::ZERO && q_sell <= hl.ask_price + max_dist {
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
        }

        None
    }

    async fn reconcile_hl_order(
        &mut self,
        desired: Option<DesiredQuote>,
        now_ms: u64,
    ) -> Result<(), anyhow::Error> {
        let tick = self.config.hl_tick_size;

        match (&self.active_hl_order, &desired) {
            (Some(active), None) => {
                if now_ms.saturating_sub(self.last_order_action_ms) >= self.config.reprice_cooldown_ms {
                    debug!(
                        oid = active.oid,
                        "Desired quote cleared; canceling resting HL Maker order"
                    );
                    self.cancel_active_hl_order("out_of_range").await;
                }
            }
            (Some(active), Some(want)) => {
                let price_changed = (active.price - want.price).abs() >= tick;
                let side_changed = active.side != want.side || active.is_entry != want.is_entry;
                if (side_changed || price_changed)
                    && now_ms.saturating_sub(self.last_order_action_ms) >= self.config.reprice_cooldown_ms
                {
                    debug!(
                        oid = active.oid,
                        old_px = %active.price,
                        new_px = %want.price,
                        "Repricing HL Maker order (cancel-first)"
                    );
                    self.cancel_active_hl_order("reprice").await;
                }
            }
            (None, Some(want)) => {
                if now_ms.saturating_sub(self.last_order_action_ms) >= self.config.reprice_cooldown_ms {
                    self.place_hl_post_only(want, now_ms).await?;
                }
            }
            (None, None) => {}
        }
        Ok(())
    }

    async fn place_hl_post_only(&mut self, want: &DesiredQuote, now_ms: u64) -> Result<(), anyhow::Error> {
        self.last_order_action_ms = now_ms;

        match self
            .executor
            .submit_hl_post_only(
                &self.config.symbol_hl,
                want.side,
                want.qty,
                want.price,
                want.reduce_only,
            )
            .await
        {
            Ok(ack) => {
                debug!(
                    oid = ack.oid,
                    status = %ack.status,
                    side = ?want.side,
                    price = %ack.price,
                    qty = %want.qty,
                    reduce_only = want.reduce_only,
                    "Placed HL ALO Maker order"
                );
                if ack.status == "resting" {
                    self.active_hl_order = Some(ActiveHlOrder {
                        oid: ack.oid,
                        side: want.side,
                        price: ack.price,
                        orig_qty: want.qty,
                        cum_filled_qty: ack.executed_qty,
                        is_entry: want.is_entry,
                        reduce_only: want.reduce_only,
                        phase: OrderPhase::Resting,
                    });
                } else if ack.executed_qty > Decimal::ZERO {
                    self.record_order_accounted_qty(ack.oid, ack.executed_qty);
                    self.mark_oid_terminal_accounted(ack.oid);
                    let est_fee = ack.price * ack.executed_qty * Decimal::new(288, 7);
                    let fill = FillEvent {
                        fill_id: format!("hl_alo_{}_{}", ack.oid, unix_now_ns()),
                        client_order_id: format!("hl_alo_{}", ack.oid),
                        venue: VenueId::Hyperliquid,
                        symbol: self.config.symbol_hl.clone(),
                        side: want.side,
                        price: ack.price,
                        qty: ack.executed_qty,
                        fee: est_fee,
                        fee_asset: "USDC".to_string(),
                        is_maker: true,
                        timestamp_ns: unix_now_ns(),
                    };
                    self.record_hl_fill(fill)?;
                    self.hedge_delta_if_needed().await?;
                }
            }
            Err(ExecutionError::PostOnlyRejected(msg)) => {
                debug!(msg = %msg, price = %want.price, "HL ALO rejected (would take); skipping without risk penalty");
            }
            Err(ExecutionError::Rejected(msg)) if msg.to_lowercase().contains("minimum value") => {
                warn!(
                    msg = %msg,
                    qty = %want.qty,
                    price = %want.price,
                    "HL ALO rejected due to $10 minimum value; skipping without risk penalty"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to place HL ALO order");
                self.risk.record_failure(e.to_string());
            }
        }
        Ok(())
    }

    async fn cancel_active_hl_order(&mut self, reason: &str) {
        let Some(mut active) = self.active_hl_order.take() else {
            return;
        };
        if matches!(active.phase, OrderPhase::Canceling { .. }) {
            self.active_hl_order = Some(active);
            return;
        }

        let now_ms = unix_now_ms();
        self.last_order_action_ms = now_ms;

        match self
            .executor
            .cancel_hl_order(&self.config.symbol_hl, active.oid)
            .await
        {
            Ok(()) => {
                debug!(oid = active.oid, reason = reason, "Sent cancel for HL Maker order; waiting terminal WS status");
                active.phase = OrderPhase::Canceling { sent_at_ms: now_ms };
                self.active_hl_order = Some(active);
            }
            Err(ExecutionError::OrderNotFound(msg)) => {
                debug!(oid = active.oid, msg = %msg, "HL cancel returned OrderNotFound; reconciling via REST orderStatus");
                self.active_hl_order = Some(active);
                self.reconcile_hl_order_via_rest().await;
            }
            Err(e) => {
                warn!(oid = active.oid, error = %e, "HL cancel failed; entering canceling state for watchdog");
                active.phase = OrderPhase::Canceling { sent_at_ms: now_ms };
                self.active_hl_order = Some(active);
            }
        }
    }

    async fn check_cancel_watchdog(&mut self) {
        let timed_out = matches!(
            self.active_hl_order.as_ref().map(|o| o.phase),
            Some(OrderPhase::Canceling { sent_at_ms }) if unix_now_ms().saturating_sub(sent_at_ms) > CANCEL_WATCHDOG_MS
        );
        if timed_out {
            warn!("HL Canceling order exceeded 3s watchdog without WS terminal event; querying REST orderStatus");
            self.reconcile_hl_order_via_rest().await;
        }
    }

    fn order_accounted_qty(&self, oid: u64) -> Decimal {
        self.order_accounted_qty
            .get(&oid)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    fn record_order_accounted_qty(&mut self, oid: u64, new_cum: Decimal) {
        if oid == 0 {
            return;
        }
        let prev = self.order_accounted_qty(oid);
        if new_cum <= prev {
            return;
        }
        let is_new = !self.order_accounted_qty.contains_key(&oid);
        self.order_accounted_qty.insert(oid, new_cum);
        if is_new {
            self.order_accounted_ring.push_back(oid);
            while self.order_accounted_ring.len() > SEEN_TRADES_CAP {
                if let Some(old) = self.order_accounted_ring.pop_front() {
                    self.order_accounted_qty.remove(&old);
                }
            }
        }
    }

    fn mark_oid_terminal_accounted(&mut self, oid: u64) {
        if oid == 0 || self.terminal_accounted_oids.contains(&oid) {
            return;
        }
        self.terminal_accounted_oids.insert(oid);
        self.terminal_oid_ring.push_back(oid);
        while self.terminal_oid_ring.len() > SEEN_TRADES_CAP {
            if let Some(old) = self.terminal_oid_ring.pop_front() {
                self.terminal_accounted_oids.remove(&old);
            }
        }
    }

    async fn reconcile_hl_order_via_rest(&mut self) {
        let Some(active) = self.active_hl_order.clone() else {
            return;
        };
        match self.executor.query_hl_order(active.oid).await {
            Ok(ack) => {
                let terminal = !matches!(ack.status.as_str(), "open" | "triggered");
                let prev_accounted = self.order_accounted_qty(active.oid).max(active.cum_filled_qty);
                if ack.executed_qty > prev_accounted {
                    let missing = ack.executed_qty - prev_accounted;
                    self.record_order_accounted_qty(active.oid, ack.executed_qty);
                    let px = if ack.price > Decimal::ZERO {
                        ack.price
                    } else {
                        active.price
                    };
                    let est_fee = px * missing * Decimal::new(288, 7);
                    let synthetic = FillEvent {
                        fill_id: format!("hl_reconcile_{}_{}", active.oid, unix_now_ns()),
                        client_order_id: format!("hl_alo_{}", active.oid),
                        venue: VenueId::Hyperliquid,
                        symbol: self.config.symbol_hl.clone(),
                        side: active.side,
                        price: px,
                        qty: missing,
                        fee: est_fee,
                        fee_asset: "USDC".to_string(),
                        is_maker: true,
                        timestamp_ns: unix_now_ns(),
                    };
                    if let Err(e) = self.record_hl_fill(synthetic) {
                        error!(error = %e, "Failed recording reconciled HL fill");
                    }
                    if let Some(ref mut cur) = self.active_hl_order {
                        cur.cum_filled_qty = ack.executed_qty;
                    }
                } else if prev_accounted > Decimal::ZERO {
                    self.record_order_accounted_qty(active.oid, prev_accounted);
                }

                if terminal {
                    self.mark_oid_terminal_accounted(active.oid);
                    self.active_hl_order = None;
                    let _ = self.hedge_delta_if_needed().await;
                    let _ = self.cleanup_entry_dust_if_needed().await;
                }
            }
            Err(ExecutionError::OrderNotFound(_)) => {
                self.active_hl_order = None;
                let _ = self.hedge_delta_if_needed().await;
                let _ = self.cleanup_entry_dust_if_needed().await;
            }
            Err(e) => {
                warn!(error = %e, oid = active.oid, "REST query_hl_order failed during watchdog reconciliation");
            }
        }
    }

    async fn on_hl_user_fill(&mut self, fill: HlUserFill) -> Result<(), anyhow::Error> {
        if fill.tid > 0 && self.seen_tids.contains(&fill.tid) {
            debug!(tid = fill.tid, oid = fill.oid, "Ignoring duplicate HL userFills tid");
            return Ok(());
        }
        if fill.oid > 0 && self.terminal_accounted_oids.contains(&fill.oid) {
            debug!(oid = fill.oid, tid = fill.tid, "Ignoring HL userFill for already-reconciled terminal oid");
            return Ok(());
        }

        if fill.tid > 0 {
            self.seen_tids.insert(fill.tid);
            self.seen_tid_ring.push_back(fill.tid);
            while self.seen_tid_ring.len() > SEEN_TRADES_CAP {
                if let Some(old) = self.seen_tid_ring.pop_front() {
                    self.seen_tids.remove(&old);
                }
            }
        }

        let matches_active = self
            .active_hl_order
            .as_ref()
            .map(|o| o.oid == fill.oid)
            .unwrap_or(false);

        let delta_qty = if fill.oid > 0 {
            let prev = self.order_accounted_qty(fill.oid);
            let allowed = if matches_active {
                let orig = self.active_hl_order.as_ref().map(|o| o.orig_qty).unwrap_or(fill.qty);
                fill.qty.min((orig - prev).max(Decimal::ZERO))
            } else {
                fill.qty
            };
            if allowed <= Decimal::ZERO {
                debug!(oid = fill.oid, prev = %prev, "Ignoring HL userFill exceeding accounted/orig qty");
                return Ok(());
            }
            self.record_order_accounted_qty(fill.oid, prev + allowed);
            allowed
        } else {
            fill.qty
        };

        let fee = if delta_qty == fill.qty {
            fill.fee
        } else if fill.qty > Decimal::ZERO {
            fill.fee * (delta_qty / fill.qty)
        } else {
            Decimal::ZERO
        };

        let fill_ev = FillEvent {
            fill_id: format!("hl_mk_{}_{}", fill.oid, fill.tid),
            client_order_id: format!("hl_alo_{}", fill.oid),
            venue: VenueId::Hyperliquid,
            symbol: self.config.symbol_hl.clone(),
            side: fill.side,
            price: fill.price,
            qty: delta_qty,
            fee,
            fee_asset: fill.fee_token.clone(),
            is_maker: fill.is_maker,
            timestamp_ns: unix_now_ns(),
        };
        self.record_hl_fill(fill_ev)?;

        if matches_active {
            let mut full_fill = false;
            if let Some(ref mut act) = self.active_hl_order {
                act.cum_filled_qty += delta_qty;
                if act.cum_filled_qty >= act.orig_qty {
                    full_fill = true;
                }
            }
            if full_fill {
                self.mark_oid_terminal_accounted(fill.oid);
                self.active_hl_order = None;
            }
        }

        self.hedge_delta_if_needed().await?;
        self.refresh_state();
        Ok(())
    }

    async fn on_hl_order_update(&mut self, upd: HlOrderUpdate) -> Result<(), anyhow::Error> {
        let matches_active = self
            .active_hl_order
            .as_ref()
            .map(|o| o.oid == upd.oid)
            .unwrap_or(false);

        if !matches_active {
            return Ok(());
        }

        let is_terminal = !matches!(upd.status.as_str(), "open" | "triggered");
        if !is_terminal {
            return Ok(());
        }

        if let Some(active) = self.active_hl_order.take() {
            let executed = (upd.orig_qty - upd.remaining_qty).max(Decimal::ZERO);
            let prev_accounted = self.order_accounted_qty(upd.oid).max(active.cum_filled_qty);
            if executed > prev_accounted {
                let missing = executed - prev_accounted;
                self.record_order_accounted_qty(upd.oid, executed);
                let px = if upd.limit_px > Decimal::ZERO {
                    upd.limit_px
                } else {
                    active.price
                };
                let est_fee = px * missing * Decimal::new(288, 7);
                let missing_fill = FillEvent {
                    fill_id: format!("hl_miss_{}_{}", upd.oid, unix_now_ns()),
                    client_order_id: format!("hl_alo_{}", upd.oid),
                    venue: VenueId::Hyperliquid,
                    symbol: self.config.symbol_hl.clone(),
                    side: active.side,
                    price: px,
                    qty: missing,
                    fee: est_fee,
                    fee_asset: "USDC".to_string(),
                    is_maker: true,
                    timestamp_ns: unix_now_ns(),
                };
                self.record_hl_fill(missing_fill)?;
            }
            self.mark_oid_terminal_accounted(upd.oid);
        }

        self.hedge_delta_if_needed().await?;
        self.cleanup_entry_dust_if_needed().await?;
        self.refresh_state();
        Ok(())
    }

    fn record_hl_fill(&mut self, fill: FillEvent) -> Result<(), anyhow::Error> {
        info!(
            venue = "HYPERLIQUID",
            side = ?fill.side,
            qty = %fill.qty,
            price = %fill.price,
            fee = %fill.fee,
            is_maker = fill.is_maker,
            "HL Maker fill recorded"
        );
        {
            let mut wal = self.wal.lock();
            wal.log_fill(&fill)?;
        }

        if !self.round.active {
            self.round.active = true;
            self.round.entry_time_ns = fill.timestamp_ns;
            self.round.entry_basis_bps = self.latest_mid_basis_bps;
            self.round.direction = if fill.side == OrderSide::Buy {
                Some(ArbDirection::ShortSpread)
            } else {
                Some(ArbDirection::LongSpread)
            };
            self.first_open_wall_ms = Some(unix_now_ms());
        }

        let is_entry = match self.round.direction {
            Some(ArbDirection::ShortSpread) => fill.side == OrderSide::Buy,
            Some(ArbDirection::LongSpread) => fill.side == OrderSide::Sell,
            None => fill.side == OrderSide::Buy,
        };

        if is_entry {
            self.round.entry_hl_notional += fill.qty * fill.price;
            self.round.entry_hl_qty += fill.qty;
        } else {
            self.round.exit_hl_notional += fill.qty * fill.price;
            self.round.exit_hl_qty += fill.qty;
        }
        self.round.total_fees += fill.fee;
        self.snapshot.pos_hl.apply_fill(&fill);
        self.maybe_finalize_round()?;
        Ok(())
    }

    async fn hedge_delta_if_needed(&mut self) -> Result<(), anyhow::Error> {
        let step = self.config.bn_step_size;
        let max_attempts = 5u32;

        loop {
            let delta = self.snapshot.pos_hl.net_qty + self.snapshot.pos_bn.net_qty;
            if delta.abs() < step {
                break;
            }

            // If delta > 0 (net Long HL), Sell BN Market; if delta < 0 (net Short HL), Buy BN Market.
            let bn_side = if delta > Decimal::ZERO {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            let is_bn_buy = bn_side == OrderSide::Buy;
            let reduce_only = if is_bn_buy {
                self.snapshot.pos_bn.net_qty < Decimal::ZERO
            } else {
                self.snapshot.pos_bn.net_qty > Decimal::ZERO
            };

            let hedge_qty = quantize_qty(delta.abs(), step);
            let hedge_qty = if reduce_only {
                quantize_qty(hedge_qty.min(self.snapshot.pos_bn.net_qty.abs()), step)
            } else {
                hedge_qty
            };
            if hedge_qty <= Decimal::ZERO {
                break;
            }

            let bn_ref_px = match (&self.bn_quote, is_bn_buy) {
                (Some(q), true) => q.ask_price,
                (Some(q), false) => q.bid_price,
                (None, _) => self
                    .hl_quote
                    .as_ref()
                    .and_then(|q| q.mid_price())
                    .unwrap_or(Decimal::ZERO),
            };

            // Binance Futures rejects non-reduce-only orders below $5 MIN_NOTIONAL
            if !reduce_only && bn_ref_px > Decimal::ZERO && (hedge_qty * bn_ref_px) < dec!(5.0) {
                warn!(
                    hedge_qty = %hedge_qty,
                    bn_ref_px = %bn_ref_px,
                    notional = %(hedge_qty * bn_ref_px),
                    active_hl = self.active_hl_order.is_some(),
                    "BN hedge notional below $5 minimum; deferring until more HL fill or dust cleanup"
                );
                break;
            }

            let pre_bn_pos = self.snapshot.pos_bn.net_qty;
            let mut attempt = 0u32;
            let mut hedged = false;

            while attempt < max_attempts {
                attempt += 1;
                let cl_id = format!("bn_h_{}_{}", attempt, unix_now_ns());

                info!(
                    attempt = attempt,
                    side = ?bn_side,
                    qty = %hedge_qty,
                    reduce_only = reduce_only,
                    "Dispatching Binance Market Leg 2 hedge"
                );

                match self
                    .executor
                    .submit_bn_market_with_opts(
                        &self.config.symbol_bn,
                        bn_side,
                        hedge_qty,
                        reduce_only,
                        &cl_id,
                    )
                    .await
                {
                    Ok(bn_fill) => {
                        self.apply_bn_fill(bn_fill, is_bn_buy)?;
                        hedged = true;
                        break;
                    }
                    Err(e) => {
                        let is_transport = matches!(
                            e,
                            ExecutionError::Network(_) | ExecutionError::Timeout(_)
                        );
                        warn!(
                            attempt = attempt,
                            error = %e,
                            transport = is_transport,
                            "BN Market hedge error; checking live BN position before retry"
                        );
                        if let Ok(live_bn) = self
                            .executor
                            .query_position(VenueId::BinanceFutures, &self.config.symbol_bn)
                            .await
                        {
                            let filled_delta = live_bn - pre_bn_pos;
                            let expected_sign = if is_bn_buy { filled_delta } else { -filled_delta };
                            if expected_sign >= step {
                                let est_px = if bn_ref_px > Decimal::ZERO {
                                    bn_ref_px
                                } else {
                                    self.snapshot.pos_bn.entry_price
                                };
                                let synth = FillEvent {
                                    fill_id: format!("bn_verified_{}", unix_now_ns()),
                                    client_order_id: cl_id,
                                    venue: VenueId::BinanceFutures,
                                    symbol: self.config.symbol_bn.clone(),
                                    side: bn_side,
                                    price: est_px,
                                    qty: expected_sign,
                                    fee: est_px * expected_sign * dec!(0.0004),
                                    fee_asset: "USDT".to_string(),
                                    is_maker: false,
                                    timestamp_ns: unix_now_ns(),
                                };
                                info!(qty = %expected_sign, "Verified BN hedge fill landed after error");
                                self.apply_bn_fill(synth, is_bn_buy)?;
                                hedged = true;
                                break;
                            }
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
                    }
                }
            }

            if !hedged {
                error!(
                    side = ?bn_side,
                    qty = %hedge_qty,
                    "Failed BN Market hedge after {} attempts; recording risk failure",
                    max_attempts
                );
                self.risk.record_failure(format!(
                    "BN Market hedge failed after {} attempts",
                    max_attempts
                ));
                break;
            }
        }
        Ok(())
    }

    fn apply_bn_fill(&mut self, fill: FillEvent, is_bn_buy: bool) -> Result<(), anyhow::Error> {
        {
            let mut wal = self.wal.lock();
            wal.log_fill(&fill)?;
        }
        let is_entry = match self.round.direction {
            Some(ArbDirection::ShortSpread) => !is_bn_buy, // Short BN on entry
            Some(ArbDirection::LongSpread) => is_bn_buy,   // Long BN on entry
            None => !is_bn_buy,
        };

        if is_entry {
            self.round.entry_bn_notional += fill.qty * fill.price;
            self.round.entry_bn_qty += fill.qty;
            self.risk.commit_fill(fill.qty);
        } else {
            self.round.exit_bn_notional += fill.qty * fill.price;
            self.round.exit_bn_qty += fill.qty;
        }
        self.round.total_fees += fill.fee;
        self.snapshot.pos_bn.apply_fill(&fill);
        self.maybe_finalize_round()?;

        let wal = self.wal.lock();
        wal.save_state(&self.snapshot)?;
        Ok(())
    }

    /// Clean up HL entry dust below BN min notional / step size when BN is flat and no HL order is resting.
    async fn cleanup_entry_dust_if_needed(&mut self) -> Result<(), anyhow::Error> {
        if self.active_hl_order.is_some() {
            return Ok(());
        }
        let hl_qty = self.snapshot.pos_hl.net_qty;
        let bn_qty = self.snapshot.pos_bn.net_qty;
        let ref_px = self
            .hl_quote
            .as_ref()
            .map(|q| if hl_qty > Decimal::ZERO { q.bid_price } else { q.ask_price })
            .unwrap_or(Decimal::ZERO);
        let below_bn_min_notional = ref_px > Decimal::ZERO && (hl_qty.abs() * ref_px) < dec!(5.0);
        if bn_qty.is_zero()
            && !hl_qty.is_zero()
            && (hl_qty.abs() < self.config.bn_step_size || below_bn_min_notional)
        {
            let dust_qty = quantize_qty(hl_qty.abs(), self.config.hl_step_size);
            if dust_qty <= Decimal::ZERO {
                return Ok(());
            }
            let close_side = if hl_qty > Decimal::ZERO {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            let is_buy = close_side == OrderSide::Buy;
            let lim = if ref_px > Decimal::ZERO {
                compute_hl_ioc_limit_price(ref_px, dec!(200.0), self.config.hl_tick_size, is_buy)
            } else {
                Decimal::ZERO
            };
            let cl_id = format!("hl_dust_{}", unix_now_ns());
            warn!(
                dust_qty = %dust_qty,
                side = ?close_side,
                "Flattening sub-BN-min entry dust on Hyperliquid with reduceOnly=true"
            );
            match self
                .executor
                .submit_hl_ioc_with_opts(
                    &self.config.symbol_hl,
                    close_side,
                    dust_qty,
                    lim,
                    true,
                    &cl_id,
                )
                .await
            {
                Ok(fill) => {
                    {
                        let mut wal = self.wal.lock();
                        wal.log_fill(&fill)?;
                    }
                    self.round.exit_hl_notional += fill.qty * fill.price;
                    self.round.exit_hl_qty += fill.qty;
                    self.round.total_fees += fill.fee;
                    self.snapshot.pos_hl.apply_fill(&fill);
                    self.maybe_finalize_round()?;
                    let wal = self.wal.lock();
                    wal.save_state(&self.snapshot)?;
                }
                Err(e) => {
                    error!(error = %e, dust_qty = %dust_qty, "Failed to flatten entry dust on Hyperliquid");
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
        self.cancel_active_hl_order("max_hold_close").await;
        let _ = self
            .executor
            .cancel_hl_all_open_orders(&self.config.symbol_hl)
            .await;
        self.active_hl_order = None;

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
                        compute_hl_ioc_limit_price(q.bid_price, dec!(200.0), self.config.hl_tick_size, false)
                    } else {
                        compute_hl_ioc_limit_price(q.ask_price, dec!(200.0), self.config.hl_tick_size, true)
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

        let direction = self.round.direction.unwrap_or(ArbDirection::ShortSpread);
        let (hl_pnl, bn_pnl) = match direction {
            ArbDirection::ShortSpread => {
                // Long HL (entry buy, exit sell), Short BN (entry sell, exit buy)
                let hl_pnl = self.round.exit_hl_notional - self.round.entry_hl_notional;
                let bn_pnl = self.round.entry_bn_notional - self.round.exit_bn_notional;
                (hl_pnl, bn_pnl)
            }
            ArbDirection::LongSpread => {
                // Short HL (entry sell, exit buy), Long BN (entry buy, exit sell)
                let hl_pnl = self.round.entry_hl_notional - self.round.exit_hl_notional;
                let bn_pnl = self.round.exit_bn_notional - self.round.entry_bn_notional;
                (hl_pnl, bn_pnl)
            }
        };
        let gross_pnl = hl_pnl + bn_pnl;
        let fees = self.round.total_fees;
        let net_pnl = gross_pnl - fees;
        let traded_qty = self.round.entry_hl_qty.max(self.round.entry_bn_qty);
        let outcome = if net_pnl > Decimal::ZERO { "WIN" } else { "LOSS" };

        let trade = TradeRoundTrip {
            trade_id: format!("trade_hlmk_{}", unix_now_ns()),
            symbol_hl: self.config.symbol_hl.clone(),
            symbol_bn: self.config.symbol_bn.clone(),
            direction: Some(direction),
            entry_hl_price: entry_hl_px,
            entry_bn_price: entry_bn_px,
            entry_qty: traded_qty,
            exit_hl_price: Some(exit_hl_px),
            exit_bn_price: Some(exit_bn_px),
            exit_qty: Some(self.round.exit_hl_qty.max(self.round.exit_bn_qty)),
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
        self.snapshot.state = match (flat, &self.active_hl_order) {
            (true, Some(o)) if o.is_entry => StrategyState::QuotingEntry,
            (true, _) => StrategyState::Idle,
            (false, Some(o)) if !o.is_entry => StrategyState::QuotingExit,
            (false, _) => StrategyState::Open,
        };
    }
}
