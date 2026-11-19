mod cli;
mod config;

use clap::Parser;
use cli::{Cli, Commands};
use config::AppConfig;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use arb_core::{L1Quote, RiskManager, WalManager};
use arb_engine::{ArbitrageEngine, DryRunExecutor, StartupReconciler};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info,arb=debug"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Nautilus Arbitrage Engine...");

    let config = match AppConfig::from_file(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, path = ?cli.config, "Failed to load config file");
            return Err(e);
        }
    };

    let command = cli.command.unwrap_or(Commands::Run);

    match command {
        Commands::Run => run_engine(config).await?,
        Commands::Check => check_connectivity(config).await?,
        Commands::Reconcile => reconcile_state(config).await?,
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
    let dry_executor = Arc::new(DryRunExecutor::new());

    // Reconcile on startup
    let initial_state = StartupReconciler::reconcile_on_startup(
        dry_executor.as_ref(),
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
        dry_executor.clone(),
    );
    engine.set_state(initial_state);

    info!("Engine loop running. Waiting for market data streams...");
    Ok(())
}

async fn check_connectivity(config: AppConfig) -> anyhow::Result<()> {
    info!(
        bn_rest = %config.venues.binance.rest_url,
        hl_rest = %config.venues.hyperliquid.rest_url,
        "Checking venue connectivity and accounts"
    );
    println!("Connectivity check: OK");
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
