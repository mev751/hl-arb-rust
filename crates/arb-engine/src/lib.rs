pub mod dry_run;
pub mod engine;
pub mod executor;
pub mod hl_ws;
pub mod live;
pub mod maker_taker;
pub mod orderbook;
pub mod reconcile;
pub mod ws_feeds;

pub use dry_run::DryRunExecutor;
pub use engine::ArbitrageEngine;
pub use executor::{BnOrderAck, ExecutionClient, ExecutionError};
pub use hl_ws::HlWsTradeClient;
pub use live::{LiveExecutor, LiveExecutorConfig};
pub use maker_taker::{ActiveBnOrder, MakerTakerEngine, OrderPhase};
pub use orderbook::{BinanceBookReconciler, DepthDiffEvent, DepthSnapshot};
pub use reconcile::{ReconcileOutcome, StartupReconciler};
pub use ws_feeds::{
    spawn_bn_book_ticker_feed, spawn_bn_user_data_feed, spawn_hl_l2book_feed, BnOrderUpdate,
    EngineEvent, FeedKind,
};
