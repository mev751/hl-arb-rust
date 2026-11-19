use crate::signals::StrategySignal;
use crate::strategy::CrossVenueArbStrategy;
use crate::traits::ArbitrageStrategy;
use arb_core::{FillEvent, L1Quote, OrderSide, VenueId};
use rust_decimal::Decimal;
use tracing::info;

/// NautilusTrader adapter that forwards Nautilus events into the cross-venue arbitrage strategy.
pub struct NautilusStrategyAdapter {
    strategy: CrossVenueArbStrategy,
}

impl NautilusStrategyAdapter {
    pub fn new(strategy: CrossVenueArbStrategy) -> Self {
        Self { strategy }
    }

    pub fn inner(&self) -> &CrossVenueArbStrategy {
        &self.strategy
    }

    pub fn inner_mut(&mut self) -> &mut CrossVenueArbStrategy {
        &mut self.strategy
    }

    /// Corresponds to NautilusTrader `on_start`
    pub fn on_start(&mut self) {
        info!("NautilusStrategyAdapter started");
        self.strategy.on_start();
    }

    /// Corresponds to NautilusTrader `on_quote_tick`
    pub fn on_quote_tick(
        &mut self,
        venue: VenueId,
        symbol: &str,
        bid_price: Decimal,
        bid_qty: Decimal,
        ask_price: Decimal,
        ask_qty: Decimal,
        timestamp_ns: u64,
    ) -> Option<StrategySignal> {
        let quote = L1Quote {
            venue,
            symbol: symbol.to_string(),
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
            timestamp_ns,
            local_recv_ns: timestamp_ns,
        };
        self.strategy.on_quote(&quote)
    }

    /// Corresponds to NautilusTrader `on_order_filled`
    pub fn on_order_filled(
        &mut self,
        fill_id: String,
        client_order_id: String,
        venue: VenueId,
        symbol: String,
        side: OrderSide,
        price: Decimal,
        qty: Decimal,
        fee: Decimal,
        fee_asset: String,
        is_maker: bool,
        timestamp_ns: u64,
    ) -> Option<StrategySignal> {
        let fill = FillEvent {
            fill_id,
            client_order_id,
            venue,
            symbol,
            side,
            price,
            qty,
            fee,
            fee_asset,
            is_maker,
            timestamp_ns,
        };
        self.strategy.on_fill(&fill)
    }
}
