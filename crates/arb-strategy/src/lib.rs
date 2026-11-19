pub mod funding;
pub mod nautilus_adapter;
pub mod signals;
pub mod strategy;
pub mod traits;

pub use funding::{FundingCalibrator, FundingSnapshot};
pub use nautilus_adapter::NautilusStrategyAdapter;
pub use signals::StrategySignal;
pub use strategy::{CrossVenueArbStrategy, StrategyConfig};
pub use traits::ArbitrageStrategy;
