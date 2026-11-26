use arb_core::{
    clocks::unix_now_ns, ArbDirection, L1Quote, OrderSide, RiskConfig, RiskManager, StrategyState,
    VenueId, WalManager,
};
use arb_engine::{
    DryRunExecutor, EngineEvent, ExecutionClient, FeedKind, HlMakerBnTakerEngine, HlOrderUpdate,
    HlUserFill, OrderPhase,
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
        symbol_hl: "xyz:SOXL".into(),
        symbol_bn: "SOXLUSDT".into(),
        entry_spread_bps: dec!(25.0),
        exit_spread_bps: dec!(10.0),
        order_chunk_qty: dec!(0.3),
        order_chunk_notional_usd: None,
        max_position_qty: dec!(1.2),
        hl_slippage_bps: dec!(200.0),
        hl_tick_size: dec!(0.001),
        hl_step_size: dec!(0.01),
        bn_step_size: dec!(0.01),
        roundtrip_fees_bps: dec!(8.6),
        execution_mode: "hl_maker_bn_taker".into(),
        adaptive_baseline: false,
        ema_tau_secs: 1800,
        adaptive_entry_delta_bps: dec!(18.0),
        adaptive_exit_delta_bps: dec!(0.0),
        reprice_cooldown_ms: 0,
        max_quote_dist_ticks: 50,
        extreme_jump_bps: dec!(12.0),
        hl_retry_slippages_bps: vec![dec!(200.0), dec!(300.0), dec!(500.0)],
        stale_quote_ms: 60_000,
        max_hold_secs: 0,
        enable_reverse_arb: true,
        reverse_entry_spread_bps: dec!(-18.0),
        reverse_exit_spread_bps: dec!(7.0),
        adaptive_reverse_entry_delta_bps: dec!(18.0),
        adaptive_reverse_exit_delta_bps: dec!(0.0),
    }
}

fn base_risk() -> RiskManager {
    RiskManager::new(RiskConfig {
        max_position_qty: dec!(5.0),
        max_order_chunk_qty: dec!(1.0),
        max_unhedged_qty: dec!(2.0),
        max_drawdown_usdt: dec!(500),
        max_consecutive_failures: 3,
        cooldown_ms: 0,
    })
}

async fn push_quotes(
    engine: &mut HlMakerBnTakerEngine<DryRunExecutor>,
    exec: &DryRunExecutor,
    hl_bid: Decimal,
    hl_ask: Decimal,
    bn_bid: Decimal,
    bn_ask: Decimal,
    ts_ns: u64,
) -> anyhow::Result<()> {
    let q_hl = make_quote(VenueId::Hyperliquid, "xyz:SOXL", hl_bid, dec!(10), hl_ask, dec!(10), ts_ns);
    let q_bn = make_quote(VenueId::BinanceFutures, "SOXLUSDT", bn_bid, dec!(10), bn_ask, dec!(10), ts_ns);
    exec.update_quote(q_hl.clone());
    exec.update_quote(q_bn.clone());
    engine.process_event(EngineEvent::Quote(q_bn)).await?;
    engine.process_event(EngineEvent::Quote(q_hl)).await?;
    Ok(())
}

#[tokio::test]
async fn test_hl_maker_bn_taker_mode1_full_roundtrip() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_hlmk_m1_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = HlMakerBnTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    // Mode 1 Entry: target_buy = floor(33.800 / 1.0025, 0.001) = 33.715 <= 33.719 (hl_ask - tick)
    push_quotes(&mut engine, &exec, dec!(33.700), dec!(33.720), dec!(33.800), dec!(33.820), now).await?;

    let ord = engine.active_hl_order().expect("Entry ALO Buy should be placed").clone();
    assert_eq!(ord.side, OrderSide::Buy);
    assert_eq!(ord.price, dec!(33.715));
    assert_eq!(ord.orig_qty, dec!(0.3));
    assert!(!ord.reduce_only);
    assert_eq!(engine.state(), StrategyState::QuotingEntry);

    // Simulate HL Maker fill -> immediate BN Market Sell @ 33.800
    exec.apply_hl_order_fill_state("xyz:SOXL", ord.oid, OrderSide::Buy, dec!(0.3), true);
    engine
        .process_event(EngineEvent::HlUserFill(HlUserFill {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            tid: 1001,
            side: OrderSide::Buy,
            price: dec!(33.715),
            qty: dec!(0.3),
            fee: dec!(0.00029130),
            fee_token: "USDC".into(),
            is_maker: true,
            time_ms: 1000,
        }))
        .await?;
    engine
        .process_event(EngineEvent::HlOrderUpdate(HlOrderUpdate {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            side: OrderSide::Buy,
            limit_px: dec!(33.715),
            orig_qty: dec!(0.3),
            remaining_qty: Decimal::ZERO,
            status: "filled".into(),
            status_time_ms: 1001,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.3));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.3));
    assert_eq!(engine.state(), StrategyState::Open);
    assert!(engine.active_hl_order().is_none());

    // Mode 1 Exit: target_sell = ceil(33.780 / 1.001, 0.001) = 33.747 >= 33.741 (hl_bid + tick)
    push_quotes(
        &mut engine,
        &exec,
        dec!(33.740),
        dec!(33.760),
        dec!(33.760),
        dec!(33.780),
        now + 500_000_000,
    )
    .await?;

    let exit_ord = engine.active_hl_order().expect("Exit ALO Sell should be placed").clone();
    assert_eq!(exit_ord.side, OrderSide::Sell);
    assert_eq!(exit_ord.price, dec!(33.747));
    assert!(exit_ord.reduce_only);
    assert_eq!(engine.state(), StrategyState::QuotingExit);

    // Simulate HL Maker exit fill -> immediate BN Market Buy @ 33.780
    exec.apply_hl_order_fill_state("xyz:SOXL", exit_ord.oid, OrderSide::Sell, dec!(0.3), true);
    engine
        .process_event(EngineEvent::HlUserFill(HlUserFill {
            coin: "xyz:SOXL".into(),
            oid: exit_ord.oid,
            tid: 1002,
            side: OrderSide::Sell,
            price: dec!(33.747),
            qty: dec!(0.3),
            fee: dec!(0.00029160),
            fee_token: "USDC".into(),
            is_maker: true,
            time_ms: 2000,
        }))
        .await?;

    assert_eq!(engine.state(), StrategyState::Idle);
    assert_eq!(engine.snapshot().pos_hl.net_qty, Decimal::ZERO);
    assert_eq!(engine.snapshot().pos_bn.net_qty, Decimal::ZERO);
    assert_eq!(engine.snapshot().completed_trades.len(), 1);
    let tr = &engine.snapshot().completed_trades[0];
    assert_eq!(tr.direction, Some(ArbDirection::ShortSpread));
    assert!(tr.net_pnl_usdt > Decimal::ZERO, "Expected positive net PnL, got {}", tr.net_pnl_usdt);

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_hl_maker_bn_taker_mode2_reverse_roundtrip() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_hlmk_m2_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut cfg = base_config();
    cfg.entry_spread_bps = dec!(32.0);
    let mut engine = HlMakerBnTakerEngine::new(cfg, base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    // Negative basis: HL 33.820/33.840, BN 33.740/33.760
    // Mode 2 Entry Sell: ceil(33.760 / 0.9982, 0.001) = 33.821 >= 33.821
    push_quotes(&mut engine, &exec, dec!(33.820), dec!(33.840), dec!(33.740), dec!(33.760), now).await?;

    let ord = engine.active_hl_order().expect("Mode 2 ALO Sell should be placed").clone();
    assert_eq!(ord.side, OrderSide::Sell);
    assert_eq!(ord.price, dec!(33.821));
    assert!(!ord.reduce_only);

    exec.apply_hl_order_fill_state("xyz:SOXL", ord.oid, OrderSide::Sell, dec!(0.3), true);
    engine
        .process_event(EngineEvent::HlUserFill(HlUserFill {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            tid: 2001,
            side: OrderSide::Sell,
            price: dec!(33.821),
            qty: dec!(0.3),
            fee: dec!(0.000292),
            fee_token: "USDC".into(),
            is_maker: true,
            time_ms: 1000,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(-0.3));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(0.3));

    // Basis reverts positive: HL 33.780/33.800, BN 33.820/33.840
    // Mode 2 Exit Buy: floor(33.820 / 1.0007, 0.001) = 33.796 <= 33.799
    push_quotes(
        &mut engine,
        &exec,
        dec!(33.780),
        dec!(33.800),
        dec!(33.820),
        dec!(33.840),
        now + 500_000_000,
    )
    .await?;

    let exit_ord = engine.active_hl_order().expect("Mode 2 Exit ALO Buy should be placed").clone();
    assert_eq!(exit_ord.side, OrderSide::Buy);
    assert_eq!(exit_ord.price, dec!(33.796));
    assert!(exit_ord.reduce_only);

    exec.apply_hl_order_fill_state("xyz:SOXL", exit_ord.oid, OrderSide::Buy, dec!(0.3), true);
    engine
        .process_event(EngineEvent::HlUserFill(HlUserFill {
            coin: "xyz:SOXL".into(),
            oid: exit_ord.oid,
            tid: 2002,
            side: OrderSide::Buy,
            price: dec!(33.796),
            qty: dec!(0.3),
            fee: dec!(0.000292),
            fee_token: "USDC".into(),
            is_maker: true,
            time_ms: 2000,
        }))
        .await?;

    assert_eq!(engine.state(), StrategyState::Idle);
    assert_eq!(engine.snapshot().completed_trades.len(), 1);
    let tr = &engine.snapshot().completed_trades[0];
    assert_eq!(tr.direction, Some(ArbDirection::LongSpread));
    assert!(tr.net_pnl_usdt > Decimal::ZERO, "Expected positive net PnL, got {}", tr.net_pnl_usdt);

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_hl_maker_bn_taker_dedup_race() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_hlmk_dedup_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = HlMakerBnTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(33.700), dec!(33.720), dec!(33.800), dec!(33.820), now).await?;
    let ord = engine.active_hl_order().unwrap().clone();

    // Case 1: orderUpdates arrives BEFORE userFills
    exec.apply_hl_order_fill_state("xyz:SOXL", ord.oid, OrderSide::Buy, dec!(0.3), true);
    engine
        .process_event(EngineEvent::HlOrderUpdate(HlOrderUpdate {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            side: OrderSide::Buy,
            limit_px: ord.price,
            orig_qty: dec!(0.3),
            remaining_qty: Decimal::ZERO,
            status: "filled".into(),
            status_time_ms: 1000,
        }))
        .await?;

    // Delayed userFills arrives with same oid -> MUST NOT double hedge!
    engine
        .process_event(EngineEvent::HlUserFill(HlUserFill {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            tid: 3001,
            side: OrderSide::Buy,
            price: ord.price,
            qty: dec!(0.3),
            fee: dec!(0.00029),
            fee_token: "USDC".into(),
            is_maker: true,
            time_ms: 1001,
        }))
        .await?;
    // Duplicate tid as well
    engine
        .process_event(EngineEvent::HlUserFill(HlUserFill {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            tid: 3001,
            side: OrderSide::Buy,
            price: ord.price,
            qty: dec!(0.3),
            fee: dec!(0.00029),
            fee_token: "USDC".into(),
            is_maker: true,
            time_ms: 1001,
        }))
        .await?;

    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.3));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.3));

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_hl_maker_bn_taker_extreme_jump_and_feed_unhealthy() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_hlmk_guard_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = HlMakerBnTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(33.700), dec!(33.720), dec!(33.800), dec!(33.820), now).await?;
    let ord = engine.active_hl_order().expect("Entry Buy should rest").clone();

    // BN drops > 12 bps in 300ms -> should immediately pull resting HL Buy order
    let q_bn_drop = make_quote(
        VenueId::BinanceFutures,
        "SOXLUSDT",
        dec!(33.730),
        dec!(10),
        dec!(33.750),
        dec!(10),
        now + 300_000_000,
    );
    exec.update_quote(q_bn_drop.clone());
    engine.process_event(EngineEvent::Quote(q_bn_drop)).await?;
    assert!(matches!(
        engine.active_hl_order().map(|o| o.phase),
        Some(OrderPhase::Canceling { .. })
    ));
    assert!(exec.open_hl_orders().is_empty());

    // Simulate cancel confirmation
    engine
        .process_event(EngineEvent::HlOrderUpdate(HlOrderUpdate {
            coin: "xyz:SOXL".into(),
            oid: ord.oid,
            side: OrderSide::Buy,
            limit_px: ord.price,
            orig_qty: dec!(0.3),
            remaining_qty: dec!(0.3),
            status: "canceled".into(),
            status_time_ms: 1002,
        }))
        .await?;
    assert!(engine.active_hl_order().is_none());

    // Place a new order after window settles, then test HlUser feed unhealthy
    let t2 = now + 2_000_000_000;
    push_quotes(&mut engine, &exec, dec!(33.700), dec!(33.720), dec!(33.800), dec!(33.820), t2).await?;
    assert!(engine.active_hl_order().is_some());

    engine
        .process_event(EngineEvent::FeedHealth {
            feed: FeedKind::HlUser,
            healthy: false,
        })
        .await?;
    assert!(engine.active_hl_order().is_none());
    assert!(exec.open_hl_orders().is_empty());

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

#[tokio::test]
async fn test_hl_maker_bn_taker_cancel_watchdog_reconciles_rest() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_hlmk_wd_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;
    let exec = Arc::new(DryRunExecutor::new());
    let mut engine = HlMakerBnTakerEngine::new(base_config(), base_risk(), wal, exec.clone());
    engine.set_all_feeds_healthy(true);

    let now = unix_now_ns();
    push_quotes(&mut engine, &exec, dec!(33.700), dec!(33.720), dec!(33.800), dec!(33.820), now).await?;
    let ord = engine.active_hl_order().expect("Entry Buy should rest").clone();

    // Simulate fill occurring in a race before cancel arrives, and cancel returns OrderNotFound
    exec.apply_hl_order_fill_state("xyz:SOXL", ord.oid, OrderSide::Buy, dec!(0.3), true);
    engine
        .process_event(EngineEvent::FeedHealth {
            feed: FeedKind::HlMarket,
            healthy: false,
        })
        .await?;

    // OrderNotFound during cancel triggers immediate REST query_hl_order reconciliation -> hedged!
    assert!(engine.active_hl_order().is_none());
    assert_eq!(engine.snapshot().pos_hl.net_qty, dec!(0.3));
    assert_eq!(engine.snapshot().pos_bn.net_qty, dec!(-0.3));
    assert_eq!(exec.query_position(VenueId::BinanceFutures, "SOXLUSDT").await?, dec!(-0.3));

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}
