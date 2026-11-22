use arb_core::{
    clocks::unix_now_ns, L1Quote, OrderSide, RiskConfig, RiskManager, StrategyState, VenueId,
    WalManager,
};
use arb_engine::{
    BnOrderUpdate, DryRunExecutor, EngineEvent, ExecutionClient, FeedKind, MakerTakerEngine,
    OrderPhase,
};
use arb_strategy::StrategyConfig;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

fn make_quote(
    venue: VenueId,
    symbol: &str,
    bid: Decimal,
    bid_sz: Decimal,
    ask: Decimal,
    ask_sz: Decimal,
    ts_ns: u64,
) -> L1Quote {
    L1Quote {
        venue,
        symbol: symbol.to_string(),
        bid_price: bid,
        bid_qty: bid_sz,
        ask_price: ask,
        ask_qty: ask_sz,
        timestamp_ns: ts_ns,
        local_recv_ns: ts_ns,
    }
}

fn base_config() -> StrategyConfig {
    StrategyConfig {
        symbol_hl: "xyz:SKHY".into(),
        symbol_bn: "SKHYUSDT".into(),
        entry_spread_bps: dec!(25.0),
        exit_spread_bps: dec!(15.0),
        order_chunk_qty: dec!(0.1),
        order_chunk_notional_usd: None,
        max_position_qty: dec!(3.0),
        hl_slippage_bps: dec!(10.0),
        hl_tick_size: dec!(0.01),
        hl_step_size: dec!(0.1),
        bn_step_size: dec!(0.01),
        roundtrip_fees_bps: dec!(1.72),
        execution_mode: "bn_maker_hl_taker".into(),
        adaptive_baseline: false,
        ema_tau_secs: 7200,
        adaptive_entry_delta_bps: dec!(5.0),
        adaptive_exit_delta_bps: dec!(4.0),
        reprice_cooldown_ms: 0,
        max_quote_dist_ticks: 3,
        extreme_jump_bps: dec!(12.0),
        hl_retry_slippages_bps: vec![dec!(10.0), dec!(25.0), dec!(50.0)],
        stale_quote_ms: 60_000,
        max_hold_secs: 0,
    }
}

fn base_risk() -> RiskManager {
    RiskManager::new(RiskConfig {
        max_position_qty: dec!(3.0),
        max_order_chunk_qty: dec!(1.0),
        max_unhedged_qty: dec!(1.0),
        max_drawdown_usdt: dec!(500),
        max_consecutive_failures: 3,
        cooldown_ms: 0,
    })
}

async fn push_quotes(
    engine: &mut MakerTakerEngine<DryRunExecutor>,
    exec: &DryRunExecutor,
    hl_bid: Decimal,
    hl_ask: Decimal,
    bn_bid: Decimal,
    bn_ask: Decimal,
    ts_ns: u64,
) -> anyhow::Result<()> {
    let q_hl = make_quote(VenueId::Hyperliquid, "xyz:SKHY", hl_bid, dec!(10), hl_ask, dec!(10), ts_ns);
    let q_bn = make_quote(VenueId::BinanceFutures, "SKHYUSDT", bn_bid, dec!(10), bn_ask, dec!(10), ts_ns);
    exec.update_quote(q_hl.clone());
    exec.update_quote(q_bn.clone());
    engine.process_event(EngineEvent::Quote(q_hl)).await?;
    engine.process_event(EngineEvent::Quote(q_bn)).await?;
    Ok(())
}

#[tokio::test]
async fn test_maker_taker_full_roundtrip_and_exact_fees() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_rt_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = MakerTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    // HL bid = 100.00, ask = 100.01, entry = 25 bps -> target_sell = ceil(100.260025) = 100.27
    // BN bid = 100.28, ask = 100.30 -> q_sell = max(100.27, 100.29) = 100.29 (<= 100.30 + 0.03)
    push_quotes(&mut engine, &exec, dec!(100.00), dec!(100.01), dec!(100.28), dec!(100.30), now).await?;

    let ord = engine.active_bn_order().expect("Entry GTX Sell should be placed").clone();
    assert_eq!(ord.side, OrderSide::Sell);
    assert_eq!(ord.price, dec!(100.29));
    assert_eq!(ord.orig_qty, dec!(0.1));
    assert!(!ord.reduce_only);
    assert_eq!(engine.state(), StrategyState::QuotingEntry);

    // Simulate BN Maker fill (0 commission) -> should immediately trigger HL IOC Buy @ 100.00
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord.client_order_id, OrderSide::Sell, dec!(0.1), true);
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord.client_order_id.clone(),
            order_id: 1,
            side: OrderSide::Sell,
            exec_type: "TRADE".into(),
            order_status: "FILLED".into(),
            last_filled_qty: dec!(0.1),
            cum_filled_qty: dec!(0.1),
            last_filled_px: dec!(100.29),
            avg_price: dec!(100.29),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 1001,
            event_time_ms: 1,
        }))
        .await?;

    assert!(engine.active_bn_order().is_none());
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(engine.state(), StrategyState::Open);

    // Move spread down near 15 bps exit target (BN drops first as spread compresses):
    // HL bid = 100.00, exit = 15 bps -> target_buy = floor(100.15) = 100.15
    // BN bid = 100.15, ask = 100.17 -> q_buy = min(100.15, 100.16) = 100.15 (>= 100.15 - 0.03)
    let q_bn_exit = make_quote(VenueId::BinanceFutures, "SKHYUSDT", dec!(100.15), dec!(10), dec!(100.17), dec!(10), now + 2_000_000_000);
    let q_hl_exit = make_quote(VenueId::Hyperliquid, "xyz:SKHY", dec!(100.00), dec!(10), dec!(100.01), dec!(10), now + 2_000_000_000);
    exec.update_quote(q_bn_exit.clone());
    exec.update_quote(q_hl_exit.clone());
    engine.process_event(EngineEvent::Quote(q_bn_exit)).await?;
    engine.process_event(EngineEvent::Quote(q_hl_exit)).await?;

    let exit_ord = engine.active_bn_order().expect("Exit GTX Buy should be placed").clone();
    assert_eq!(exit_ord.side, OrderSide::Buy);
    assert_eq!(exit_ord.price, dec!(100.15));
    assert_eq!(exit_ord.orig_qty, dec!(0.1));
    assert!(exit_ord.reduce_only);
    assert_eq!(engine.state(), StrategyState::QuotingExit);

    // Simulate BN Maker Buy fill (0 commission) -> should immediately trigger HL IOC Sell @ 100.00
    exec.apply_bn_order_fill_state("SKHYUSDT", &exit_ord.client_order_id, OrderSide::Buy, dec!(0.1), true);
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: exit_ord.client_order_id.clone(),
            order_id: 2,
            side: OrderSide::Buy,
            exec_type: "TRADE".into(),
            order_status: "FILLED".into(),
            last_filled_qty: dec!(0.1),
            cum_filled_qty: dec!(0.1),
            last_filled_px: dec!(100.15),
            avg_price: dec!(100.15),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: true,
            trade_id: 1002,
            event_time_ms: 2,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(0));
    assert_eq!(engine.state(), StrategyState::Idle);
    assert_eq!(engine.snapshot().completed_trades.len(), 1);

    let tr = &engine.snapshot().completed_trades[0];
    // Gross PnL = (100.29 - 100.15)*0.1 + (100.00 - 100.01)*0.1 = 0.014 - 0.001 = 0.013 USDT
    // Fees = (100.01 + 100.00) * 0.1 * 0.000086 = 0.001720086 USDT (exact 1.72 bps roundtrip!)
    assert_eq!(tr.gross_pnl_usdt, dec!(0.013));
    assert_eq!(tr.total_fees_usdt, dec!(0.001720086));
    assert_eq!(tr.net_pnl_usdt, dec!(0.011279914));

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_partial_fill_dust_cleanup_and_cancel_race() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_dust_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = MakerTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.24), dec!(100.26), now).await?;
    let ord1 = engine.active_bn_order().unwrap().clone();

    // Case A: Partial fill 0.04 (< HL step_size 0.1), then CANCELED
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord1.client_order_id, OrderSide::Sell, dec!(0.04), false);
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord1.client_order_id.clone(),
            order_id: 10,
            side: OrderSide::Sell,
            exec_type: "TRADE".into(),
            order_status: "PARTIALLY_FILLED".into(),
            last_filled_qty: dec!(0.04),
            cum_filled_qty: dec!(0.04),
            last_filled_px: dec!(100.25),
            avg_price: dec!(100.25),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 2001,
            event_time_ms: 10,
        }))
        .await?;

    // HL must NOT hedge 0.04 (since 0.04 < 0.1 step_size)
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.04));

    // Order gets CANCELED -> automatic cleanup_entry_dust_if_needed flattens BN -0.04 -> 0
    exec.cancel_order(VenueId::BinanceFutures, "SKHYUSDT", &ord1.client_order_id).await.ok();
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord1.client_order_id.clone(),
            order_id: 10,
            side: OrderSide::Sell,
            exec_type: "CANCELED".into(),
            order_status: "CANCELED".into(),
            last_filled_qty: dec!(0),
            cum_filled_qty: dec!(0.04),
            last_filled_px: dec!(0),
            avg_price: dec!(100.25),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: false,
            reduce_only: false,
            trade_id: 0,
            event_time_ms: 11,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(0));
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0));
    assert_eq!(engine.state(), StrategyState::Idle);

    // Case B: Cancel race! Place order, move quotes to trigger Canceling, then receive terminal FILLED event
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.24), dec!(100.26), now + 2_000_000_000).await?;
    let ord2 = engine.active_bn_order().unwrap().clone();

    // Move BN further down (bn_ask = 100.20, max_dist = 0.03 -> 100.23 < 100.25) so engine sends cancel (OrderPhase::Canceling)
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.18), dec!(100.20), now + 2_200_000_000).await?;
    assert!(matches!(
        engine.active_bn_order().unwrap().phase,
        OrderPhase::Canceling { .. }
    ));

    // Despite cancel in flight, order actually filled on Binance! Terminal CANCELED/FILLED delivers cum_filled_qty = 0.1
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord2.client_order_id, OrderSide::Sell, dec!(0.1), true);
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord2.client_order_id.clone(),
            order_id: 11,
            side: OrderSide::Sell,
            exec_type: "CANCELED".into(),
            order_status: "CANCELED".into(),
            last_filled_qty: dec!(0),
            cum_filled_qty: dec!(0.1), // missed TRADE caught via cum_filled_qty reconciliation!
            last_filled_px: dec!(100.25),
            avg_price: dec!(100.25),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 0,
            event_time_ms: 12,
        }))
        .await?;

    // HL hedge MUST trigger for the caught cancel-race fill!
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(engine.state(), StrategyState::Open);

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_hl_trade_ws_unhealthy_pulls_bn_order_and_blocks_quoting() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_health_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = MakerTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.24), dec!(100.26), now).await?;
    let ord = engine.active_bn_order().expect("Order should rest when healthy").clone();
    assert_eq!(exec.open_bn_orders().len(), 1);

    // HL trade WS disconnects! Engine MUST immediately cancel BN resting order
    engine
        .process_event(EngineEvent::FeedHealth {
            feed: FeedKind::HlTrade,
            healthy: false,
        })
        .await?;

    assert_eq!(exec.open_bn_orders().len(), 0);
    assert!(matches!(
        engine.active_bn_order().unwrap().phase,
        OrderPhase::Canceling { .. }
    ));

    // Terminal cancel arrives
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord.client_order_id,
            order_id: 20,
            side: OrderSide::Sell,
            exec_type: "CANCELED".into(),
            order_status: "CANCELED".into(),
            last_filled_qty: dec!(0),
            cum_filled_qty: dec!(0),
            last_filled_px: dec!(0),
            avg_price: dec!(0),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: false,
            reduce_only: false,
            trade_id: 0,
            event_time_ms: 20,
        }))
        .await?;
    assert!(engine.active_bn_order().is_none());

    // While unhealthy, new quotes MUST NOT place any BN order
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.24), dec!(100.26), now + 1_000_000_000).await?;
    assert!(engine.active_bn_order().is_none());
    assert_eq!(exec.open_bn_orders().len(), 0);

    // Recovery -> quoting resumes!
    engine
        .process_event(EngineEvent::FeedHealth {
            feed: FeedKind::HlTrade,
            healthy: true,
        })
        .await?;
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.24), dec!(100.26), now + 2_000_000_000).await?;
    assert!(engine.active_bn_order().is_some());
    assert_eq!(exec.open_bn_orders().len(), 1);

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_adaptive_ema_extreme_jump_and_no_obi_false_cancel() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_ema_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut cfg = base_config();
    cfg.adaptive_baseline = true;
    cfg.ema_tau_secs = 30; // fast tau in unit test to observe regime tracking cleanly
    cfg.adaptive_entry_delta_bps = dec!(6.0);
    cfg.adaptive_exit_delta_bps = dec!(4.0);
    cfg.max_quote_dist_ticks = 2;
    cfg.extreme_jump_bps = dec!(12.0);

    let mut engine = MakerTakerEngine::new(cfg, base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    // 1. Seed EMA around 15 bps (HL mid = 100.00, BN mid = 100.15), then shift regime up smoothly to 20 bps
    let mut ts = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.01), dec!(100.14), dec!(100.16), ts).await?;
    assert!((engine.ema_basis_bps().unwrap() - 14.99).abs() < 0.2);

    // Step through 17 bps (first 30s) then 20 bps (next 30s) so EMA tracks up cleanly without crossing +6 bps hurdle
    for i in 0..30 {
        ts += 2_000_000_000;
        let bn_b = if i < 15 { dec!(100.16) } else { dec!(100.19) };
        let bn_a = if i < 15 { dec!(100.18) } else { dec!(100.21) };
        push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.01), bn_b, bn_a, ts).await?;
    }
    let adapted_ema = engine.ema_basis_bps().unwrap();
    assert!(adapted_ema > 18.0 && adapted_ema < 20.5, "EMA should adapt upward to ~18.6 bps, got {}", adapted_ema);
    assert!(engine.active_bn_order().is_none(), "No stray order should rest before entry hurdle");

    // 2. Now basis spikes to +25 bps (EMA + 6 = ~25 bps) WITH massive L1 OBI imbalance (bid_sz=999, ask_sz=1)
    // MUST quote immediately and MUST NOT cancel or skip due to L1 OBI imbalance!
    ts += 1_000_000_000;
    let q_hl_spike = make_quote(VenueId::Hyperliquid, "xyz:SKHY", dec!(99.99), dec!(10), dec!(100.01), dec!(10), ts);
    let q_bn_obi = make_quote(VenueId::BinanceFutures, "SKHYUSDT", dec!(100.24), dec!(999), dec!(100.26), dec!(1), ts);
    exec.update_quote(q_hl_spike.clone());
    exec.update_quote(q_bn_obi.clone());
    engine.process_event(EngineEvent::Quote(q_hl_spike)).await?;
    engine.process_event(EngineEvent::Quote(q_bn_obi)).await?;

    assert!(engine.active_bn_order().is_some(), "Should quote immediately ignoring OBI imbalance");
    // Feed another tick with extreme OBI=0.999 -> resting order MUST stay Resting!
    ts += 500_000_000;
    let q_bn_obi2 = make_quote(VenueId::BinanceFutures, "SKHYUSDT", dec!(100.24), dec!(999), dec!(100.26), dec!(1), ts);
    exec.update_quote(q_bn_obi2.clone());
    engine.process_event(EngineEvent::Quote(q_bn_obi2)).await?;
    assert!(matches!(engine.active_bn_order().unwrap().phase, OrderPhase::Resting));

    // Now simulate an extreme > 12 bps upward jump in 200ms (BN jumps 100.25 -> 100.45 = +20 bps)
    ts += 200_000_000;
    let q_jump = make_quote(VenueId::BinanceFutures, "SKHYUSDT", dec!(100.44), dec!(10), dec!(100.46), dec!(10), ts);
    exec.update_quote(q_jump.clone());
    engine.process_event(EngineEvent::Quote(q_jump)).await?;

    assert!(
        matches!(engine.active_bn_order().unwrap().phase, OrderPhase::Canceling { .. }),
        "Extreme +20 bps jump must trigger immediate cancel on resting Sell Maker"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_hl_tiered_slippage_retry_recovers_on_third_attempt() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_retry_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = MakerTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.28), dec!(100.30), now).await?;
    let ord = engine.active_bn_order().unwrap().clone();

    // First 2 HL IOC tiers (10 bps, 25 bps) return ZeroFill; 3rd tier (50 bps) succeeds!
    exec.force_hl_zero_fills(2);
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord.client_order_id, OrderSide::Sell, dec!(0.1), true);
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord.client_order_id,
            order_id: 99,
            side: OrderSide::Sell,
            exec_type: "TRADE".into(),
            order_status: "FILLED".into(),
            last_filled_qty: dec!(0.1),
            cum_filled_qty: dec!(0.1),
            last_filled_px: dec!(100.29),
            avg_price: dec!(100.29),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 9901,
            event_time_ms: 99,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(engine.state(), StrategyState::Open);

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_rest_reconcile_and_delayed_ws_trade_no_double_hedge() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_race_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = MakerTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.28), dec!(100.30), now).await?;
    let ord = engine.active_bn_order().expect("Entry GTX Sell should be placed").clone();

    // Exchange fills order before cancel arrives, while WS ORDER_TRADE_UPDATE is delayed in transit.
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord.client_order_id, OrderSide::Sell, dec!(0.1), true);

    // Out-of-range quotes trigger cancel -> OrderNotFound -> REST reconcile records fill and hedges HL 0.1
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.18), dec!(100.20), now + 2_000_000_000).await?;

    assert!(engine.active_bn_order().is_none());
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(exec.query_position(VenueId::Hyperliquid, "xyz:SKHY").await?, dec!(0.1));

    // Delayed WS TRADE arrives AFTER REST reconcile already cleared active_bn_order and hedged HL
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord.client_order_id,
            order_id: 77,
            side: OrderSide::Sell,
            exec_type: "TRADE".into(),
            order_status: "FILLED".into(),
            last_filled_qty: dec!(0.1),
            cum_filled_qty: dec!(0.1),
            last_filled_px: dec!(100.29),
            avg_price: dec!(100.29),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 9001,
            event_time_ms: 77,
        }))
        .await?;

    // Must NOT double-record BN fill or fire a second HL IOC hedge!
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(exec.query_position(VenueId::Hyperliquid, "xyz:SKHY").await?, dec!(0.1));
    assert_eq!(engine.state(), StrategyState::Open);

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_partial_fill_rest_reconcile_and_delayed_ws_no_double_count() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_mt_prace_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = MakerTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.28), dec!(100.30), now).await?;
    let ord = engine.active_bn_order().expect("Entry GTX Sell should be placed").clone();

    // Step 1: Partial fill 0.04 arrives normally via WS
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord.client_order_id, OrderSide::Sell, dec!(0.04), false);
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord.client_order_id.clone(),
            order_id: 88,
            side: OrderSide::Sell,
            exec_type: "TRADE".into(),
            order_status: "PARTIALLY_FILLED".into(),
            last_filled_qty: dec!(0.04),
            cum_filled_qty: dec!(0.04),
            last_filled_px: dec!(100.29),
            avg_price: dec!(100.29),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 9101,
            event_time_ms: 88,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.04));
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0));

    // Step 2: Remaining 0.06 fills on exchange; cancel races -> OrderNotFound -> REST reconcile records 0.06 & hedges 0.1
    exec.apply_bn_order_fill_state("SKHYUSDT", &ord.client_order_id, OrderSide::Sell, dec!(0.06), true);
    push_quotes(&mut engine, &exec, dec!(99.99), dec!(100.00), dec!(100.18), dec!(100.20), now + 2_000_000_000).await?;

    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(exec.query_position(VenueId::Hyperliquid, "xyz:SKHY").await?, dec!(0.1));

    // Step 3: Delayed WS TRADE for the 0.06 leg arrives after REST reconcile
    engine
        .process_event(EngineEvent::BnOrderUpdate(BnOrderUpdate {
            symbol: "SKHYUSDT".into(),
            client_order_id: ord.client_order_id,
            order_id: 88,
            side: OrderSide::Sell,
            exec_type: "TRADE".into(),
            order_status: "FILLED".into(),
            last_filled_qty: dec!(0.06),
            cum_filled_qty: dec!(0.10),
            last_filled_px: dec!(100.29),
            avg_price: dec!(100.29),
            commission: Decimal::ZERO,
            commission_asset: "USDT".into(),
            is_maker: true,
            reduce_only: false,
            trade_id: 9102,
            event_time_ms: 89,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.1));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.1));
    assert_eq!(exec.query_position(VenueId::Hyperliquid, "xyz:SKHY").await?, dec!(0.1));

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}
