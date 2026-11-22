use crate::funding::FundingCalibrator;
use crate::signals::StrategySignal;
use crate::traits::ArbitrageStrategy;
use arb_core::{
    compute_hl_ioc_limit_price, compute_long_hl_exec_spread_bps, compute_mid_basis_bps,
    compute_net_edge_bps, quantize_qty, FillEvent, L1Quote, OrderSide, StrategyState, VenueId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
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
    #[serde(default = "default_execution_mode")]
    pub execution_mode: String,
    #[serde(default = "default_true")]
    pub adaptive_baseline: bool,
    #[serde(default = "default_ema_tau_secs")]
    pub ema_tau_secs: u64,
    #[serde(default = "default_adaptive_entry_delta_bps")]
    pub adaptive_entry_delta_bps: Decimal,
    #[serde(default = "default_adaptive_exit_delta_bps")]
    pub adaptive_exit_delta_bps: Decimal,
    #[serde(default = "default_reprice_cooldown_ms")]
    pub reprice_cooldown_ms: u64,
    #[serde(default = "default_max_quote_dist_ticks")]
    pub max_quote_dist_ticks: u32,
    #[serde(default = "default_extreme_jump_bps")]
    pub extreme_jump_bps: Decimal,
    #[serde(default = "default_hl_retry_slippages_bps")]
    pub hl_retry_slippages_bps: Vec<Decimal>,
    #[serde(default = "default_stale_quote_ms")]
    pub stale_quote_ms: u64,
    #[serde(default)]
    pub max_hold_secs: u64,
    #[serde(default = "default_true")]
    pub enable_reverse_arb: bool,
    #[serde(default = "default_reverse_entry_spread_bps")]
    pub reverse_entry_spread_bps: Decimal,
    #[serde(default = "default_reverse_exit_spread_bps")]
    pub reverse_exit_spread_bps: Decimal,
    #[serde(default = "default_adaptive_reverse_entry_delta_bps")]
    pub adaptive_reverse_entry_delta_bps: Decimal,
    #[serde(default = "default_adaptive_reverse_exit_delta_bps")]
    pub adaptive_reverse_exit_delta_bps: Decimal,
}

fn default_execution_mode() -> String {
    "bn_maker_hl_taker".to_string()
}

fn default_true() -> bool {
    true
}

fn default_reverse_entry_spread_bps() -> Decimal {
    Decimal::new(50, 1)
}

fn default_reverse_exit_spread_bps() -> Decimal {
    Decimal::new(180, 1)
}

fn default_adaptive_reverse_entry_delta_bps() -> Decimal {
    Decimal::new(140, 1)
}

fn default_adaptive_reverse_exit_delta_bps() -> Decimal {
    Decimal::new(10, 1)
}

fn default_ema_tau_secs() -> u64 {
    7200
}

fn default_adaptive_entry_delta_bps() -> Decimal {
    Decimal::new(50, 1)
}

fn default_adaptive_exit_delta_bps() -> Decimal {
    Decimal::new(40, 1)
}

fn default_reprice_cooldown_ms() -> u64 {
    150
}

fn default_max_quote_dist_ticks() -> u32 {
    3
}

fn default_extreme_jump_bps() -> Decimal {
    Decimal::new(120, 1)
}

fn default_hl_retry_slippages_bps() -> Vec<Decimal> {
    vec![
        Decimal::new(100, 1),
        Decimal::new(250, 1),
        Decimal::new(500, 1),
    ]
}

fn default_stale_quote_ms() -> u64 {
    2000
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            symbol_hl: "xyz:SKHY".to_string(),
            symbol_bn: "SKHYUSDT".to_string(),
            entry_spread_bps: Decimal::new(250, 1),
            exit_spread_bps: Decimal::new(160, 1),
            order_chunk_qty: Decimal::new(1, 1),
            order_chunk_notional_usd: None,
            max_position_qty: Decimal::new(30, 1),
            hl_slippage_bps: Decimal::new(100, 1),
            hl_tick_size: Decimal::new(1, 2),
            hl_step_size: Decimal::new(1, 1),
            bn_step_size: Decimal::new(1, 2),
            roundtrip_fees_bps: Decimal::new(172, 2),
            execution_mode: default_execution_mode(),
            adaptive_baseline: default_true(),
            ema_tau_secs: default_ema_tau_secs(),
            adaptive_entry_delta_bps: default_adaptive_entry_delta_bps(),
            adaptive_exit_delta_bps: default_adaptive_exit_delta_bps(),
            reprice_cooldown_ms: default_reprice_cooldown_ms(),
            max_quote_dist_ticks: default_max_quote_dist_ticks(),
            extreme_jump_bps: default_extreme_jump_bps(),
            hl_retry_slippages_bps: default_hl_retry_slippages_bps(),
            stale_quote_ms: default_stale_quote_ms(),
            max_hold_secs: 0,
            enable_reverse_arb: default_true(),
            reverse_entry_spread_bps: default_reverse_entry_spread_bps(),
            reverse_exit_spread_bps: default_reverse_exit_spread_bps(),
            adaptive_reverse_entry_delta_bps: default_adaptive_reverse_entry_delta_bps(),
            adaptive_reverse_exit_delta_bps: default_adaptive_reverse_exit_delta_bps(),
        }
    }
}

impl StrategyConfig {
    pub fn effective_mode1_thresholds(&self, ema_basis_bps: Option<f64>) -> (Decimal, Decimal) {
        if self.adaptive_baseline {
            if let Some(ema_f) = ema_basis_bps {
                if let Ok(ema_dec) = Decimal::try_from(ema_f) {
                    let ema_rounded = ema_dec.round_dp(4);
                    return (
                        ema_rounded + self.adaptive_entry_delta_bps,
                        ema_rounded - self.adaptive_exit_delta_bps,
                    );
                }
            }
        }
        (self.entry_spread_bps, self.exit_spread_bps)
    }

    pub fn effective_mode2_thresholds(&self, ema_basis_bps: Option<f64>) -> (Decimal, Decimal) {
        if self.adaptive_baseline {
            if let Some(ema_f) = ema_basis_bps {
                if let Ok(ema_dec) = Decimal::try_from(ema_f) {
                    let ema_rounded = ema_dec.round_dp(4);
                    return (
                        ema_rounded - self.adaptive_reverse_entry_delta_bps,
                        ema_rounded - self.adaptive_reverse_exit_delta_bps,
                    );
                }
            }
        }
        (self.reverse_entry_spread_bps, self.reverse_exit_spread_bps)
    }
}

pub struct CrossVenueArbStrategy {
    config: StrategyConfig,
    state: StrategyState,
    hl_quote: Option<L1Quote>,
    bn_quote: Option<L1Quote>,
    funding: FundingCalibrator,
    active_session_id: Option<String>,
    current_hl_pos: Decimal,
    current_bn_pos: Decimal,
}

impl CrossVenueArbStrategy {
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            state: StrategyState::Idle,
            hl_quote: None,
            bn_quote: None,
            funding: FundingCalibrator::new(),
            active_session_id: None,
            current_hl_pos: Decimal::ZERO,
            current_bn_pos: Decimal::ZERO,
        }
    }

    pub fn state(&self) -> StrategyState {
        self.state
    }

    pub fn set_state(&mut self, state: StrategyState) {
        if state == StrategyState::Idle {
            self.current_hl_pos = Decimal::ZERO;
            self.current_bn_pos = Decimal::ZERO;
            self.active_session_id = None;
        }
        self.state = state;
    }

    pub fn sync_positions(&mut self, hl_pos: Decimal, bn_pos: Decimal) {
        self.current_hl_pos = hl_pos;
        self.current_bn_pos = bn_pos;
    }
}

impl ArbitrageStrategy for CrossVenueArbStrategy {
    fn name(&self) -> &str {
        "BinanceFutures_vs_Hyperliquid_PerpArb"
    }

    fn on_start(&mut self) {
        info!(
            strategy = self.name(),
            hl = %self.config.symbol_hl,
            bn = %self.config.symbol_bn,
            entry_bps = %self.config.entry_spread_bps,
            exit_bps = %self.config.exit_spread_bps,
            "Arbitrage Strategy initialized"
        );
    }

    fn on_quote(&mut self, quote: &L1Quote) -> Option<StrategySignal> {
        // Cache latest quotes
        match quote.venue {
            VenueId::Hyperliquid if quote.symbol == self.config.symbol_hl => {
                self.hl_quote = Some(quote.clone());
            }
            VenueId::BinanceFutures if quote.symbol == self.config.symbol_bn => {
                self.bn_quote = Some(quote.clone());
            }
            _ => return None,
        }

        let (hl_quote, bn_quote) = match (&self.hl_quote, &self.bn_quote) {
            (Some(hl), Some(bn)) => (hl, bn),
            _ => return None,
        };

        let hl_mid = hl_quote.mid_price()?;
        let bn_mid = bn_quote.mid_price()?;
        let mid_basis_bps = compute_mid_basis_bps(bn_mid, hl_mid)?;
        let avg_mid = (hl_mid + bn_mid) / Decimal::from(2);

        match self.state {
            StrategyState::Idle => {
                // Check Long HL / Short BN opportunity
                // HL Buy at ask, BN Sell at bid
                let exec_spread_bps =
                    compute_long_hl_exec_spread_bps(hl_quote.ask_price, bn_quote.bid_price, avg_mid)?;

                let net_edge_bps =
                    compute_net_edge_bps(exec_spread_bps, self.config.roundtrip_fees_bps);

                debug!(
                    mid_basis = %mid_basis_bps,
                    exec_spread = %exec_spread_bps,
                    net_edge = %net_edge_bps,
                    "Tick evaluated in Idle state"
                );

                if exec_spread_bps >= self.config.entry_spread_bps && net_edge_bps > Decimal::ZERO {
                    let raw_qty = match self.config.order_chunk_notional_usd {
                        Some(notional) if notional > Decimal::ZERO => {
                            notional / hl_quote.ask_price
                        }
                        _ => self.config.order_chunk_qty,
                    };
                    let target_qty = quantize_qty(raw_qty, self.config.hl_step_size);
                    if target_qty <= Decimal::ZERO {
                        return None;
                    }

                    let hl_limit_price = compute_hl_ioc_limit_price(
                        hl_quote.ask_price,
                        self.config.hl_slippage_bps,
                        self.config.hl_tick_size,
                        true,
                    );

                    let session_id = uuid::Uuid::new_v4().to_string();
                    self.active_session_id = Some(session_id.clone());
                    self.state = StrategyState::HlSubmitting;

                    info!(
                        session_id = %session_id,
                        exec_spread = %exec_spread_bps,
                        target_qty = %target_qty,
                        limit_price = %hl_limit_price,
                        "Triggering Entry signal: Buy HL IOC, prepare Sell BN hedge"
                    );

                    Some(StrategySignal::Enter {
                        session_id,
                        leg_hl_side: OrderSide::Buy,
                        leg_bn_side: OrderSide::Sell,
                        target_qty,
                        target_spread_bps: exec_spread_bps,
                        hl_limit_price,
                    })
                } else {
                    None
                }
            }
            StrategyState::Open => {
                // Monitor for exit signal: closing Long HL (sell at bid) and closing Short BN (buy at ask)
                // Exit spread = (hl_bid - bn_ask) / avg_mid * 10000
                // When basis converges, bn_ask - hl_bid decreases, so (hl_bid - bn_ask) becomes less negative
                let current_spread_bps = ((hl_quote.bid_price - bn_quote.ask_price) / avg_mid)
                    * Decimal::from(10_000);

                // Exit condition: spread <= exit_spread_bps (e.g. basis has converged back near parity)
                if current_spread_bps >= -self.config.exit_spread_bps {
                    let session_id = self
                        .active_session_id
                        .clone()
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    self.state = StrategyState::Closing;

                    info!(
                        session_id = %session_id,
                        current_spread = %current_spread_bps,
                        exit_hurdle = %self.config.exit_spread_bps,
                        "Triggering Exit signal: Unwinding hedged pair"
                    );

                    Some(StrategySignal::Exit {
                        session_id,
                        leg_hl_side: OrderSide::Sell,
                        leg_bn_side: OrderSide::Buy,
                        qty: self.current_hl_pos.abs(),
                        current_spread_bps,
                    })
                } else {
                    // Check if we can scale into more position if spread is still >= entry_spread_bps
                    let exec_spread_bps =
                        compute_long_hl_exec_spread_bps(hl_quote.ask_price, bn_quote.bid_price, avg_mid)?;
                    let net_edge_bps =
                        compute_net_edge_bps(exec_spread_bps, self.config.roundtrip_fees_bps);

                    if exec_spread_bps >= self.config.entry_spread_bps && net_edge_bps > Decimal::ZERO {
                        let raw_qty = match self.config.order_chunk_notional_usd {
                            Some(notional) if notional > Decimal::ZERO => {
                                notional / hl_quote.ask_price
                            }
                            _ => self.config.order_chunk_qty,
                        };
                        let target_qty = quantize_qty(raw_qty, self.config.hl_step_size);
                        if target_qty > Decimal::ZERO
                            && (self.current_hl_pos.abs() + target_qty) <= self.config.max_position_qty
                        {
                            let hl_limit_price = compute_hl_ioc_limit_price(
                                hl_quote.ask_price,
                                self.config.hl_slippage_bps,
                                self.config.hl_tick_size,
                                true,
                            );

                            let session_id = uuid::Uuid::new_v4().to_string();
                            self.active_session_id = Some(session_id.clone());
                            self.state = StrategyState::HlSubmitting;

                            info!(
                                session_id = %session_id,
                                exec_spread = %exec_spread_bps,
                                current_pos = %self.current_hl_pos,
                                target_qty = %target_qty,
                                max_pos = %self.config.max_position_qty,
                                "Scaling into position: Buy HL IOC, prepare Sell BN hedge"
                            );

                            Some(StrategySignal::Enter {
                                session_id,
                                leg_hl_side: OrderSide::Buy,
                                leg_bn_side: OrderSide::Sell,
                                target_qty,
                                target_spread_bps: exec_spread_bps,
                                hl_limit_price,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
            _ => None,
        }
    }

    fn on_fill(&mut self, fill: &FillEvent) -> Option<StrategySignal> {
        info!(
            venue = %fill.venue,
            side = ?fill.side,
            qty = %fill.qty,
            price = %fill.price,
            state = ?self.state,
            "Strategy observed fill event"
        );

        match (self.state, fill.venue) {
            (StrategyState::HlSubmitting, VenueId::Hyperliquid) => {
                // HL leg filled! Immediately dispatch market hedge on Binance
                self.current_hl_pos += fill.qty;
                let session_id = self
                    .active_session_id
                    .clone()
                    .unwrap_or_else(|| "sess_hedge".into());
                self.state = StrategyState::Hedging;

                Some(StrategySignal::Hedge {
                    session_id,
                    venue: VenueId::BinanceFutures,
                    side: OrderSide::Sell,
                    qty: fill.qty,
                    reason: "HL IOC filled; hedging on Binance".into(),
                })
            }
            (StrategyState::Hedging, VenueId::BinanceFutures) => {
                // BN hedge filled! Both legs secured, position is OPEN
                self.current_bn_pos -= fill.qty;
                self.state = StrategyState::Open;
                info!(
                    pos_hl = %self.current_hl_pos,
                    pos_bn = %self.current_bn_pos,
                    "Cross-venue hedge completed. State is now OPEN"
                );
                None
            }
            (StrategyState::Closing, VenueId::Hyperliquid) => {
                self.current_hl_pos -= fill.qty;
                if self.current_hl_pos.is_zero() && self.current_bn_pos.is_zero() {
                    self.state = StrategyState::Idle;
                    self.active_session_id = None;
                    info!("Positions fully closed. State transitioned back to IDLE");
                }
                None
            }
            (StrategyState::Closing, VenueId::BinanceFutures) => {
                self.current_bn_pos += fill.qty;
                if self.current_hl_pos.is_zero() && self.current_bn_pos.is_zero() {
                    self.state = StrategyState::Idle;
                    self.active_session_id = None;
                    info!("Positions fully closed. State transitioned back to IDLE");
                }
                None
            }
            _ => None,
        }
    }

    fn on_funding_rate(&mut self, venue: VenueId, symbol: &str, rate: Decimal) {
        self.funding.update_rate(venue, symbol, rate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::clocks::unix_now_ns;
    use rust_decimal_macros::dec;

    #[test]
    fn test_cross_venue_arb_lifecycle() {
        let config = StrategyConfig {
            symbol_hl: "xyz:SKHY".into(),
            symbol_bn: "SKHYUSDT".into(),
            entry_spread_bps: dec!(40.0),
            exit_spread_bps: dec!(10.0),
            order_chunk_qty: dec!(10.0),
            order_chunk_notional_usd: None,
            max_position_qty: dec!(50.0),
            hl_slippage_bps: dec!(10.0),
            hl_tick_size: dec!(0.01),
            hl_step_size: dec!(0.1),
            bn_step_size: dec!(0.1),
            roundtrip_fees_bps: dec!(12.0),
            ..Default::default()
        };

        let mut strategy = CrossVenueArbStrategy::new(config);
        strategy.on_start();

        // 1. Send quotes where BN is higher than HL by 50 bps
        // HL: bid 10.00, ask 10.02
        // BN: bid 10.07, ask 10.09
        let quote_hl = L1Quote {
            venue: VenueId::Hyperliquid,
            symbol: "xyz:SKHY".into(),
            bid_price: dec!(10.00),
            bid_qty: dec!(100),
            ask_price: dec!(10.02),
            ask_qty: dec!(100),
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };
        assert!(strategy.on_quote(&quote_hl).is_none());

        let quote_bn = L1Quote {
            venue: VenueId::BinanceFutures,
            symbol: "SKHYUSDT".into(),
            bid_price: dec!(10.07),
            bid_qty: dec!(100),
            ask_price: dec!(10.09),
            ask_qty: dec!(100),
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };

        // (10.07 - 10.02) / 10.045 * 10000 = 0.05 / 10.045 * 10000 ≈ 49.77 bps > 40.0 bps
        let signal = strategy.on_quote(&quote_bn);
        assert!(signal.is_some());
        assert_eq!(strategy.state(), StrategyState::HlSubmitting);

        // 2. HL Fill arrives
        let fill_hl = FillEvent {
            fill_id: "hl_f1".into(),
            client_order_id: "cl_hl_1".into(),
            venue: VenueId::Hyperliquid,
            symbol: "xyz:SKHY".into(),
            side: OrderSide::Buy,
            price: dec!(10.02),
            qty: dec!(10.0),
            fee: dec!(0.01),
            fee_asset: "USDC".into(),
            is_maker: false,
            timestamp_ns: unix_now_ns(),
        };
        let hedge_sig = strategy.on_fill(&fill_hl);
        assert!(hedge_sig.is_some());
        assert_eq!(strategy.state(), StrategyState::Hedging);

        // 3. BN Hedge Fill arrives
        let fill_bn = FillEvent {
            fill_id: "bn_f1".into(),
            client_order_id: "cl_bn_1".into(),
            venue: VenueId::BinanceFutures,
            symbol: "SKHYUSDT".into(),
            side: OrderSide::Sell,
            price: dec!(10.07),
            qty: dec!(10.0),
            fee: dec!(0.04),
            fee_asset: "USDT".into(),
            is_maker: false,
            timestamp_ns: unix_now_ns(),
        };
        assert!(strategy.on_fill(&fill_bn).is_none());
        assert_eq!(strategy.state(), StrategyState::Open);
    }
}
