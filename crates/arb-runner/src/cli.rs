use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "arb")]
#[command(about = "High-Performance Cross-Venue Arbitrage Engine", long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value = "config/config.toml")]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the arbitrage engine (Live or Dry-Run according to config)
    Run,

    /// Check connectivity and account balances on Binance & Hyperliquid
    Check,

    /// Reconcile open orders and positions against local WAL
    Reconcile,

    /// Test two-leg order placement with a specified quantity (size)
    TestTrade {
        /// Order size / quantity to trade on both sides
        #[arg(short, long, default_value = "1.0")]
        size: rust_decimal::Decimal,

        /// Automatically unwind (close) both legs after test entry
        #[arg(long, default_value_t = false)]
        close: bool,
    },

    /// Replay historical JSONL quote data for backtesting
    Backtest {
        #[arg(long)]
        quotes_file: PathBuf,
    },
}
