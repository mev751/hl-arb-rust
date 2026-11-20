use arb_core::{
    clocks::unix_now_ns, L1Quote, RiskConfig, RiskManager, StrategyState, VenueId, WalManager,
};
use arb_engine::{ArbitrageEngine, DryRunExecutor};
use arb_strategy::StrategyConfig;
use rust_decimal_macros::dec;
use std::sync::Arc;

#[tokio::test]
async fn test_full_arbitrage_lifecycle() -> anyhow::Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("arb_integ_test_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&temp_dir)?;

    let risk_config = RiskConfig {
        max_position_qty: dec!(100),
        max_order_chunk_qty: dec!(10),
        max_unhedged_qty: dec!(1),
        max_drawdown_usdt: dec!(500),
        max_consecutive_failures: 3,
        cooldown_ms: 0, // no cooldown for unit test
    };
    let risk_manager = RiskManager::new(risk_config);

    let strategy_config = StrategyConfig {
        symbol_hl: "xyz:SKHY".into(),
        symbol_bn: "SKHYUSDT".into(),
        entry_spread_bps: dec!(40.0),
        exit_spread_bps: dec!(10.0),
        order_chunk_qty: dec!(10.0),
        order_chunk_notional_usd: None,
        max_position_qty: dec!(100.0),
        hl_slippage_bps: dec!(10.0),
        hl_tick_size: dec!(0.01),
        hl_step_size: dec!(0.1),
        bn_step_size: dec!(0.1),
        roundtrip_fees_bps: dec!(12.0),
    };

    let executor = Arc::new(DryRunExecutor::new());
    let mut engine = ArbitrageEngine::new(strategy_config, risk_manager, wal, executor.clone());

    assert_eq!(engine.state(), StrategyState::Idle);

    // 1. Send normal spread (no arbitrage)
    let q_hl_1 = L1Quote {
        venue: VenueId::Hyperliquid,
        symbol: "xyz:SKHY".into(),
        bid_price: dec!(10.00),
        bid_qty: dec!(100),
        ask_price: dec!(10.02),
        ask_qty: dec!(100),
        timestamp_ns: unix_now_ns(),
        local_recv_ns: unix_now_ns(),
    };
    executor.update_quote(q_hl_1.clone());
    engine.process_quote(&q_hl_1).await?;

    let q_bn_1 = L1Quote {
        venue: VenueId::BinanceFutures,
        symbol: "SKHYUSDT".into(),
        bid_price: dec!(10.03),
        bid_qty: dec!(100),
        ask_price: dec!(10.05),
        ask_qty: dec!(100),
        timestamp_ns: unix_now_ns(),
        local_recv_ns: unix_now_ns(),
    };
    executor.update_quote(q_bn_1.clone());
    engine.process_quote(&q_bn_1).await?;

    // Spread = (10.03 - 10.02) / 10.025 * 10000 ≈ 10 bps < 40 bps -> Idle
    assert_eq!(engine.state(), StrategyState::Idle);

    // 2. Wide divergence occurs!
    // HL drops: ask = 9.95
    // BN stays high: bid = 10.05
    // Spread = (10.05 - 9.95) / 10.00 * 10000 = 100 bps > 40 bps -> Triggers Entry!
    let q_hl_diverge = L1Quote {
        venue: VenueId::Hyperliquid,
        symbol: "xyz:SKHY".into(),
        bid_price: dec!(9.93),
        bid_qty: dec!(100),
        ask_price: dec!(9.95),
        ask_qty: dec!(100),
        timestamp_ns: unix_now_ns(),
        local_recv_ns: unix_now_ns(),
    };
    executor.update_quote(q_hl_diverge.clone());
    engine.process_quote(&q_hl_diverge).await?;

    let q_bn_diverge = L1Quote {
        venue: VenueId::BinanceFutures,
        symbol: "SKHYUSDT".into(),
        bid_price: dec!(10.05),
        bid_qty: dec!(100),
        ask_price: dec!(10.07),
        ask_qty: dec!(100),
        timestamp_ns: unix_now_ns(),
        local_recv_ns: unix_now_ns(),
    };
    executor.update_quote(q_bn_diverge.clone());
    engine.process_quote(&q_bn_diverge).await?;

    // Engine should have executed HL IOC buy, received fill, hedged on BN, and entered Open state!
    assert_eq!(engine.state(), StrategyState::Open);

    // 3. Basis converges!
    // HL bid = 10.04, BN ask = 10.04
    // Exit spread = (10.04 - 10.04) / 10.04 * 10000 = 0 bps >= -10 bps -> Triggers Exit!
    let q_hl_converge = L1Quote {
        venue: VenueId::Hyperliquid,
        symbol: "xyz:SKHY".into(),
        bid_price: dec!(10.04),
        bid_qty: dec!(100),
        ask_price: dec!(10.06),
        ask_qty: dec!(100),
        timestamp_ns: unix_now_ns(),
        local_recv_ns: unix_now_ns(),
    };
    executor.update_quote(q_hl_converge.clone());
    engine.process_quote(&q_hl_converge).await?;

    let q_bn_converge = L1Quote {
        venue: VenueId::BinanceFutures,
        symbol: "SKHYUSDT".into(),
        bid_price: dec!(10.03),
        bid_qty: dec!(100),
        ask_price: dec!(10.04),
        ask_qty: dec!(100),
        timestamp_ns: unix_now_ns(),
        local_recv_ns: unix_now_ns(),
    };
    executor.update_quote(q_bn_converge.clone());
    engine.process_quote(&q_bn_converge).await?;

    // Engine should have unwound both legs and returned to Idle!
    assert_eq!(engine.state(), StrategyState::Idle);

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}
