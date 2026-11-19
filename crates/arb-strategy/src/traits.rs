use crate::signals::StrategySignal;
use arb_core::{FillEvent, L1Quote, VenueId};
use rust_decimal::Decimal;

pub trait ArbitrageStrategy: Send + Sync {
    fn name(&self) -> &str;
    fn on_start(&mut self);
    fn on_quote(&mut self, quote: &L1Quote) -> Option<StrategySignal>;
    fn on_fill(&mut self, fill: &FillEvent) -> Option<StrategySignal>;
    fn on_funding_rate(&mut self, venue: VenueId, symbol: &str, rate: Decimal);
}
