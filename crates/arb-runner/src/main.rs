mod cli;
mod config;

use clap::Parser;
use cli::{Cli, Commands};
use config::AppConfig;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use arb_core::{L1Quote, RiskManager, WalManager};
use arb_engine::{
    ArbitrageEngine, DryRunExecutor, ExecutionClient, LiveExecutor, LiveExecutorConfig,
    StartupReconciler,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = match AppConfig::from_file(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config file {:?}: {}", cli.config, e);
            return Err(e);
        }
    };

    // Initialize tracing using RUST_LOG or config.engine.log_level (defaults to info)
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| config.engine.log_level.clone());
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(&log_level))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(log_level = %log_level, "Starting Nautilus Arbitrage Engine...");

    let command = cli.command.unwrap_or(Commands::Run);

    match command {
        Commands::Run => run_engine(config).await?,
        Commands::Check => check_connectivity(config).await?,
        Commands::Reconcile => reconcile_state(config).await?,
        Commands::Close { live } => manual_close(config, live).await?,
        Commands::TestTrade { size, amount, price, live, close } => test_trade(config, size, amount, price, live, close).await?,
        Commands::Backtest { quotes_file } => backtest_replay(config, quotes_file).await?,
    }

    Ok(())
}

async fn run_engine(config: AppConfig) -> anyhow::Result<()> {
    info!(
        dry_run = config.engine.dry_run,
        symbol_hl = %config.strategy.symbol_hl,
        symbol_bn = %config.strategy.symbol_bn,
        "Initializing Arbitrage Engine"
    );

    let wal = WalManager::new(&config.engine.wal_dir)?;
    let existing_snapshot = wal.load_state()?;

    let risk_manager = RiskManager::new(config.risk.to_risk_config());
    let strategy_config = config.strategy.to_strategy_config();

    // Use DryRunExecutor or LiveExecutor based on config
    let executor: Arc<dyn ExecutionClient> = if config.engine.dry_run {
        Arc::new(DryRunExecutor::new())
    } else {
        let hl_dex = if let Some((prefix, _)) = config.strategy.symbol_hl.split_once(':') {
            prefix.to_string()
        } else {
            String::new()
        };
        let live_config = LiveExecutorConfig {
            bn_api_key: config.venues.binance.api_key.clone(),
            bn_api_secret: config.venues.binance.api_secret.clone(),
            bn_rest_url: config.venues.binance.rest_url.clone(),
            hl_wallet: config.venues.hyperliquid.wallet_address.clone(),
            hl_private_key: config.venues.hyperliquid.private_key.clone(),
            hl_rest_url: config.venues.hyperliquid.rest_url.clone(),
            hl_is_mainnet: config.venues.hyperliquid.is_mainnet,
            hl_dex,
        };
        Arc::new(LiveExecutor::new(live_config)?)
    };

    // Reconcile on startup
    let initial_state = StartupReconciler::reconcile_on_startup(
        executor.as_ref(),
        existing_snapshot.as_ref(),
        &config.strategy.symbol_hl,
        &config.strategy.symbol_bn,
    )
    .await?;

    info!(initial_state = ?initial_state, "Startup reconciliation completed");

    let mut engine = ArbitrageEngine::new(
        strategy_config,
        risk_manager,
        wal,
        executor.clone(),
    );

    if let Some(state) = existing_snapshot {
        let mut snap = engine.snapshot().clone();
        snap.state = initial_state;
        snap.pos_hl = state.pos_hl;
        snap.pos_bn = state.pos_bn;
        snap.completed_trades = state.completed_trades;
        snap.total_realized_pnl_usdt = state.total_realized_pnl_usdt;
        snap.total_fees_usdt = state.total_fees_usdt;
        engine.restore_snapshot(snap);
    } else {
        engine.set_state(initial_state);
    }

    engine.reset_circuit_breaker();
    info!("Risk manager circuit breaker initialized and active.");

    info!("Engine continuous event loop started. Polling quotes every 200ms...");

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
    let mut tick_count: u64 = 0;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received (Ctrl+C). Stopping engine gracefully.");
                break;
            }
            _ = interval.tick() => {
                tick_count += 1;
                match executor.fetch_quotes(&config.strategy.symbol_hl, &config.strategy.symbol_bn).await {
                    Ok((q_hl, q_bn)) => {
                        let avg_mid = (q_hl.ask_price + q_bn.bid_price) / rust_decimal_macros::dec!(2.0);
                        let spread_bps = if avg_mid > rust_decimal::Decimal::ZERO {
                            ((q_bn.bid_price - q_hl.ask_price) / avg_mid) * rust_decimal_macros::dec!(10000.0)
                        } else {
                            rust_decimal::Decimal::ZERO
                        };

                        if tick_count % 30 == 0 {
                            info!(
                                tick = tick_count,
                                state = ?engine.state(),
                                pos_hl = %engine.snapshot().pos_hl.net_qty,
                                pos_bn = %engine.snapshot().pos_bn.net_qty,
                                hl_ask = %q_hl.ask_price,
                                bn_bid = %q_bn.bid_price,
                                spread_bps = %format!("{:.2}", spread_bps),
                                "[HEARTBEAT] Engine running normally"
                            );
                        }

                        if let Err(e) = engine.process_quote(&q_hl).await {
                            tracing::warn!(error = %e, "Engine error processing HL quote");
                        }
                        if let Err(e) = engine.process_quote(&q_bn).await {
                            tracing::warn!(error = %e, "Engine error processing BN quote");
                        }
                    }
                    Err(e) => {
                        if tick_count % 30 == 0 {
                            tracing::warn!(error = %e, "Failed to fetch quotes from venues");
                        }
                    }
                }
            }
        }
    }

    info!("Arbitrage Engine shut down safely.");
    Ok(())
}

async fn check_connectivity(config: AppConfig) -> anyhow::Result<()> {
    println!("==================================================");
    println!("=== Checking Venue Configuration & Credentials ===");
    println!("==================================================");

    fn mask_secret(s: &str) -> String {
        if s.len() <= 8 {
            "***".to_string()
        } else {
            format!("{}...{}", &s[..4], &s[s.len() - 4..])
        }
    }

    let hl_pk = &config.venues.hyperliquid.private_key;
    let hl_wallet = &config.venues.hyperliquid.wallet_address;
    let is_hl_pk_set = !hl_pk.is_empty() && !hl_pk.starts_with("ENV_");
    let is_hl_wallet_set = !hl_wallet.is_empty() && !hl_wallet.starts_with("ENV_");

    println!("Hyperliquid:");
    println!("  * REST URL   : {}", config.venues.hyperliquid.rest_url);
    println!("  * WS URL     : {}", config.venues.hyperliquid.ws_url);
    println!("  * Wallet     : {}", if is_hl_wallet_set { hl_wallet.clone() } else { format!("[UNSET: {}]", hl_wallet) });
    println!("  * Private Key: {}", if is_hl_pk_set { mask_secret(hl_pk) } else { format!("[UNSET: {}]", hl_pk) });

    let bn_key = &config.venues.binance.api_key;
    let bn_secret = &config.venues.binance.api_secret;
    let is_bn_key_set = !bn_key.is_empty() && !bn_key.starts_with("ENV_");
    let is_bn_secret_set = !bn_secret.is_empty() && !bn_secret.starts_with("ENV_");

    println!("Binance Futures:");
    println!("  * REST URL   : {}", config.venues.binance.rest_url);
    println!("  * WS URL     : {}", config.venues.binance.ws_url);
    println!("  * API Key    : {}", if is_bn_key_set { mask_secret(bn_key) } else { format!("[UNSET: {}]", bn_key) });
    println!("  * API Secret : {}", if is_bn_secret_set { mask_secret(bn_secret) } else { format!("[UNSET: {}]", bn_secret) });
    println!("--------------------------------------------------");

    if is_hl_pk_set && is_hl_wallet_set && is_bn_key_set && is_bn_secret_set {
        println!("All credentials loaded successfully!");
    } else {
        println!("Notice: Some credentials are still placeholders.");
        println!("You can set them via a .env file or environment variables:");
        println!("  export HL_PRIVATE_KEY=\"0x...\"");
        println!("  export HL_WALLET_ADDRESS=\"0x...\"");
        println!("  export BINANCE_API_KEY=\"...\"");
        println!("  export BINANCE_API_SECRET=\"...\"");
    }

    Ok(())
}

async fn reconcile_state(config: AppConfig) -> anyhow::Result<()> {
    let wal = WalManager::new(&config.engine.wal_dir)?;
    if let Some(snap) = wal.load_state()? {
        println!("State snapshot: {:?}", snap.state);
        println!("HL Position: {:?}", snap.pos_hl.net_qty);
        println!("BN Position: {:?}", snap.pos_bn.net_qty);
        println!("Completed Trades: {}", snap.completed_trades.len());
        println!("Total Realized PnL: {} USDT", snap.total_realized_pnl_usdt);
    } else {
        println!("No state snapshot found in {}", config.engine.wal_dir);
    }
    Ok(())
}

async fn manual_close(config: AppConfig, force_live: bool) -> anyhow::Result<()> {
    use arb_core::{clocks::unix_now_ns, L1Quote, VenueId};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    let is_live = force_live || !config.engine.dry_run;

    println!("==================================================");
    println!("=== Manually Flattening Cross-Venue Positions ===");
    println!("==================================================");
    println!("Execution Mode  : {}", if is_live { "LIVE (Real Exchange Orders)" } else { "DRY-RUN (Simulated Fills)" });
    println!("WAL Directory   : {}", config.engine.wal_dir);

    let wal = WalManager::new(&config.engine.wal_dir)?;
    let initial_state = wal.load_state()?;

    let (hl_pos, bn_pos) = if let Some(ref snap) = initial_state {
        (snap.pos_hl.net_qty, snap.pos_bn.net_qty)
    } else {
        (Decimal::ZERO, Decimal::ZERO)
    };

    println!("Current HL Position: {}", hl_pos);
    println!("Current BN Position: {}", bn_pos);

    if hl_pos.is_zero() && bn_pos.is_zero() {
        println!("No open positions to close. Both venue positions are zero.");
        return Ok(());
    }

    let risk_manager = RiskManager::new(config.risk.to_risk_config());
    let strategy_config = config.strategy.to_strategy_config();

    let executor: Arc<dyn ExecutionClient> = if is_live {
        let hl_dex = if let Some((prefix, _)) = config.strategy.symbol_hl.split_once(':') {
            prefix.to_string()
        } else {
            String::new()
        };
        let live_config = LiveExecutorConfig {
            bn_api_key: config.venues.binance.api_key.clone(),
            bn_api_secret: config.venues.binance.api_secret.clone(),
            bn_rest_url: config.venues.binance.rest_url.clone(),
            hl_wallet: config.venues.hyperliquid.wallet_address.clone(),
            hl_private_key: config.venues.hyperliquid.private_key.clone(),
            hl_rest_url: config.venues.hyperliquid.rest_url.clone(),
            hl_is_mainnet: config.venues.hyperliquid.is_mainnet,
            hl_dex,
        };
        Arc::new(LiveExecutor::new(live_config)?)
    } else {
        let dry_executor = Arc::new(DryRunExecutor::new());
        let q_hl = L1Quote {
            venue: VenueId::Hyperliquid,
            symbol: config.strategy.symbol_hl.clone(),
            bid_price: dec!(10.04),
            bid_qty: dec!(1000),
            ask_price: dec!(10.05),
            ask_qty: dec!(1000),
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };
        let q_bn = L1Quote {
            venue: VenueId::BinanceFutures,
            symbol: config.strategy.symbol_bn.clone(),
            bid_price: dec!(10.05),
            bid_qty: dec!(1000),
            ask_price: dec!(10.06),
            ask_qty: dec!(1000),
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };
        dry_executor.update_quote(q_hl);
        dry_executor.update_quote(q_bn);
        dry_executor
    };

    let mut engine = ArbitrageEngine::new(
        strategy_config,
        risk_manager,
        wal,
        executor,
    );

    if let Some(state) = initial_state {
        let mut snap = engine.snapshot().clone();
        snap.state = state.state;
        snap.pos_hl = state.pos_hl;
        snap.pos_bn = state.pos_bn;
        snap.completed_trades = state.completed_trades;
        snap.total_realized_pnl_usdt = state.total_realized_pnl_usdt;
        snap.total_fees_usdt = state.total_fees_usdt;
        engine.restore_snapshot(snap);
    }

    println!("--------------------------------------------------");
    println!("Submitting market close orders on both venues...");
    match engine.manual_close().await? {
        Some(trade) => {
            println!("Manual flatten completed successfully!");
            println!("  * Final State       : {:?}", engine.state());
            println!("  * HL Final Position : {}", engine.snapshot().pos_hl.net_qty);
            println!("  * BN Final Position : {}", engine.snapshot().pos_bn.net_qty);
            println!("  * Unhedged Delta    : 0.0");
            println!("  * Trade Net PnL     : {} USDT", trade.net_pnl_usdt);
            println!("  * Total Fees        : {} USDT", trade.total_fees_usdt);
            println!("==================================================");
            println!("WAL state updated and persisted.");
        }
        None => {
            println!("No positions were closed.");
        }
    }

    Ok(())
}

async fn test_trade(
    config: AppConfig,
    size_opt: Option<rust_decimal::Decimal>,
    amount_opt: Option<rust_decimal::Decimal>,
    ref_price_opt: Option<rust_decimal::Decimal>,
    force_live: bool,
    auto_close: bool,
) -> anyhow::Result<()> {
    use arb_core::{clocks::unix_now_ns, math::quantize_qty, L1Quote, VenueId};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    let is_live = force_live || !config.engine.dry_run;

    println!("==================================================");
    println!("=== Testing Two-Leg Arbitrage Order Execution ===");
    println!("==================================================");
    println!("Execution Mode  : {}", if is_live { "LIVE (Real Exchange Orders)" } else { "DRY-RUN (Simulated Top-of-Book Fills)" });
    println!("Leg 1 (HL)      : {} (IOC Buy)", config.strategy.symbol_hl);
    println!("Leg 2 (BN)      : {} (Market Sell Hedge)", config.strategy.symbol_bn);
    println!("WAL Directory   : {}", config.engine.wal_dir);

    let wal = WalManager::new(&config.engine.wal_dir)?;
    let risk_manager = RiskManager::new(config.risk.to_risk_config());
    let strategy_config = config.strategy.to_strategy_config();

    let (executor, q_hl, _q_bn): (Arc<dyn ExecutionClient>, L1Quote, L1Quote) = if is_live {
        let hl_dex = if let Some((prefix, _)) = config.strategy.symbol_hl.split_once(':') {
            prefix.to_string()
        } else {
            String::new()
        };
        let live_config = LiveExecutorConfig {
            bn_api_key: config.venues.binance.api_key.clone(),
            bn_api_secret: config.venues.binance.api_secret.clone(),
            bn_rest_url: config.venues.binance.rest_url.clone(),
            hl_wallet: config.venues.hyperliquid.wallet_address.clone(),
            hl_private_key: config.venues.hyperliquid.private_key.clone(),
            hl_rest_url: config.venues.hyperliquid.rest_url.clone(),
            hl_is_mainnet: config.venues.hyperliquid.is_mainnet,
            hl_dex,
        };
        let live_exec = Arc::new(LiveExecutor::new(live_config)?);
        println!("Fetching live L1 quotes from Hyperliquid & Binance...");
        let (quotes_hl, quotes_bn) = live_exec.fetch_live_quotes(&config.strategy.symbol_hl, &config.strategy.symbol_bn).await?;
        println!("Live Top-of-Book Quotes:");
        println!("  * Hyperliquid ({}) : Bid {} | Ask {}", config.strategy.symbol_hl, quotes_hl.bid_price, quotes_hl.ask_price);
        println!("  * Binance     ({}) : Bid {} | Ask {}", config.strategy.symbol_bn, quotes_bn.bid_price, quotes_bn.ask_price);
        (live_exec, quotes_hl, quotes_bn)
    } else {
        let dry_executor = Arc::new(DryRunExecutor::new());
        let dry_hl = L1Quote {
            venue: VenueId::Hyperliquid,
            symbol: config.strategy.symbol_hl.clone(),
            bid_price: dec!(10.00),
            bid_qty: dec!(1000),
            ask_price: dec!(10.02),
            ask_qty: dec!(1000),
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };
        let dry_bn = L1Quote {
            venue: VenueId::BinanceFutures,
            symbol: config.strategy.symbol_bn.clone(),
            bid_price: dec!(10.08),
            bid_qty: dec!(1000),
            ask_price: dec!(10.10),
            ask_qty: dec!(1000),
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };
        dry_executor.update_quote(dry_hl.clone());
        dry_executor.update_quote(dry_bn.clone());
        (dry_executor, dry_hl, dry_bn)
    };

    let step_size = config.strategy.hl_step_size;
    let base_price = ref_price_opt.unwrap_or(q_hl.ask_price);

    let (trade_qty, notional_info) = if let Some(amount) = amount_opt {
        let raw_qty = amount / base_price;
        let quantized = quantize_qty(raw_qty, step_size);
        let actual_notional = quantized * base_price;
        let info = format!("Specified Notional: {} USDT | Ref Price: {} USDT -> Quantized Qty: {} (Actual Notional: {:.2} USDT)",
            amount, base_price, quantized, actual_notional);
        (quantized, info)
    } else if let Some(size) = size_opt {
        let quantized = quantize_qty(size, step_size);
        let actual_notional = quantized * base_price;
        let info = format!("Specified Size: {} | Ref Price: {} USDT -> Estimated Notional: {:.2} USDT",
            quantized, base_price, actual_notional);
        (quantized, info)
    } else if let Some(cfg_notional) = config.strategy.order_chunk_notional_usd {
        let raw_qty = cfg_notional / base_price;
        let quantized = quantize_qty(raw_qty, step_size);
        let actual_notional = quantized * base_price;
        let info = format!("Config Notional: {} USDT | Ref Price: {} USDT -> Quantized Qty: {} (Actual Notional: {:.2} USDT)",
            cfg_notional, base_price, quantized, actual_notional);
        (quantized, info)
    } else {
        let size = quantize_qty(config.strategy.order_chunk_qty, step_size);
        let actual_notional = size * base_price;
        let info = format!("Default Config Size: {} | Ref Price: {} USDT -> Estimated Notional: {:.2} USDT",
            size, base_price, actual_notional);
        (size, info)
    };

    if trade_qty <= Decimal::ZERO {
        anyhow::bail!("Calculated trade quantity is zero. Please increase order amount/size or check step_size (current: {}).", step_size);
    }

    println!("Execution Param : {}", notional_info);
    println!("Target Trade Qty: {} per leg", trade_qty);
    println!("--------------------------------------------------");

    let mut engine = ArbitrageEngine::new(
        strategy_config,
        risk_manager,
        wal,
        executor.clone(),
    );

    // Allow slippage buffer above ask for guaranteed IOC execution
    let slippage_mult = Decimal::ONE + (config.strategy.hl_slippage_bps / dec!(10000.0)).max(dec!(0.005));
    let hl_limit_price = (base_price * slippage_mult).round_dp(2);

    println!("[Step 1] Triggering Leg 1 (HL IOC Buy qty={}, limit_price={}) & Leg 2 (BN Market Sell Hedge)...", trade_qty, hl_limit_price);
    match engine.trigger_test_order(trade_qty, hl_limit_price).await {
        Ok(_) => {
            let snap = engine.snapshot();
            println!("[Step 2] Entry Succeeded!");
            println!("  * Engine State  : {:?}", engine.state());
            println!("  * HL Net Pos    : {}", snap.pos_hl.net_qty);
            println!("  * BN Net Pos    : {}", snap.pos_bn.net_qty);
            println!("  * Unhedged Delta: {}", (snap.pos_hl.net_qty.abs() - snap.pos_bn.net_qty.abs()).abs());
        }
        Err(e) => {
            println!("[Error] Order execution failed: {}", e);
            return Err(e);
        }
    }

    if auto_close {
        println!("--------------------------------------------------");
        println!("[Step 3] Auto-close enabled: Closing both legs...");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        match engine.manual_close().await? {
            Some(trade) => {
                let snap = engine.snapshot();
                println!("  * Close Succeeded!");
                println!("  * Final State   : {:?}", engine.state());
                println!("  * Final HL Pos  : {}", snap.pos_hl.net_qty);
                println!("  * Final BN Pos  : {}", snap.pos_bn.net_qty);
                println!("  * Trade Net PnL : {} USDT", trade.net_pnl_usdt);
                println!("  * Total Fees    : {} USDT", trade.total_fees_usdt);
            }
            None => {
                println!("  * Warning: No open positions were found to close.");
            }
        }
    }

    println!("==================================================");
    println!("Test trade completed successfully. WAL records persisted.");
    Ok(())
}

async fn backtest_replay(config: AppConfig, path: std::path::PathBuf) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    info!(path = ?path, "Starting historical quote replay backtest");
    println!("=== Nautilus Arbitrage Engine: Historical Backtest ===");
    println!("Loading quote feed from: {:?}", path);

    let file = File::open(&path)?;
    let reader = BufReader::new(file);

    let wal_dir = std::env::temp_dir().join(format!("arb_backtest_wal_{}", uuid::Uuid::new_v4()));
    let wal = WalManager::new(&wal_dir)?;
    let risk_manager = RiskManager::new(config.risk.to_risk_config());
    let strategy_config = config.strategy.to_strategy_config();

    let dry_executor = Arc::new(DryRunExecutor::new());
    let mut engine = ArbitrageEngine::new(
        strategy_config,
        risk_manager,
        wal,
        dry_executor.clone(),
    );

    let mut quote_count: usize = 0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        if let Ok(quote) = serde_json::from_str::<L1Quote>(&line) {
            dry_executor.update_quote(quote.clone());
            engine.process_quote(&quote).await?;
            quote_count += 1;
        }
    }

    println!("--------------------------------------------------");
    println!("Processed Quotes: {}", quote_count);
    println!("Final Engine State: {:?}", engine.state());
    println!("Backtest complete.");
    println!("==================================================");

    let _ = std::fs::remove_dir_all(&wal_dir);
    Ok(())
}
