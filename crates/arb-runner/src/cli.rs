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

    /// Manually flatten / close all open positions on both venues immediately
    Close {
        /// Force LIVE execution mode (sends real orders to exchanges, overrides config.dry_run)
        #[arg(long, default_value_t = false)]
        live: bool,
    },

    /// Rebalance an unhedged residual delta on whichever leg has excess position (reduceOnly)
    Rebalance {
        /// Force LIVE execution mode (sends real reduceOnly order to flatten excess residual)
        #[arg(long, default_value_t = false)]
        live: bool,
    },

    /// Test two-leg order placement with a specified quantity (size) or notional amount (USDT/USD)
    TestTrade {
        /// Order size / token quantity to trade on each side (e.g. --size 2.5)
        #[arg(short, long)]
        size: Option<rust_decimal::Decimal>,

        /// Target notional amount (每边下单金额 in USDT/USD, e.g. --amount 100 or --notional 100)
        #[arg(short, long, alias = "notional")]
        amount: Option<rust_decimal::Decimal>,

        /// Reference price for converting amount to token quantity (optional, defaults to market quote)
        #[arg(short, long)]
        price: Option<rust_decimal::Decimal>,

        /// Force LIVE execution mode (sends real orders to exchanges, overrides config.dry_run)
        #[arg(long, default_value_t = false)]
        live: bool,

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
