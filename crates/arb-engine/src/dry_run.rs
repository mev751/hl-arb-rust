use crate::executor::{ExecutionClient, ExecutionError};
use arb_core::{clocks::unix_now_ns, FillEvent, L1Quote, OrderSide, VenueId};
use async_trait::async_trait;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

#[derive(Default)]
struct DryRunState {
    quotes: HashMap<(VenueId, String), L1Quote>,
    positions: HashMap<(VenueId, String), Decimal>,
}

#[derive(Clone, Default)]
pub struct DryRunExecutor {
    state: Arc<RwLock<DryRunState>>,
    taker_fee_rate: Decimal,
}

impl DryRunExecutor {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(DryRunState::default())),
            taker_fee_rate: dec!(0.0004), // 4 bps default taker fee
        }
    }

    pub fn update_quote(&self, quote: L1Quote) {
        let mut state = self.state.write();
        state
            .quotes
            .insert((quote.venue, quote.symbol.clone()), quote);
    }
}

#[async_trait]
impl ExecutionClient for DryRunExecutor {
    async fn submit_hl_ioc(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        let quote = {
            let state = self.state.read();
            state
                .quotes
                .get(&(VenueId::Hyperliquid, symbol.to_string()))
                .cloned()
                .ok_or_else(|| ExecutionError::Other(format!("No HL quote available for {}", symbol)))?
        };

        // Check if limit price crosses top-of-book (limit_price <= 0 means aggressive/market fill)
        let fill_price = match side {
            OrderSide::Buy => {
                if limit_price <= Decimal::ZERO || limit_price >= quote.ask_price {
                    quote.ask_price
                } else {
                    return Err(ExecutionError::ZeroFill);
                }
            }
            OrderSide::Sell => {
                if limit_price <= Decimal::ZERO || limit_price <= quote.bid_price {
                    quote.bid_price
                } else {
                    return Err(ExecutionError::ZeroFill);
                }
            }
        };

        let notional = fill_price * qty;
        let fee = notional * self.taker_fee_rate;

        {
            let mut state = self.state.write();
            let pos = state
                .positions
                .entry((VenueId::Hyperliquid, symbol.to_string()))
                .or_insert(Decimal::ZERO);
            match side {
                OrderSide::Buy => *pos += qty,
                OrderSide::Sell => *pos -= qty,
            }
        }

        info!(
            venue = "HYPERLIQUID",
            cl_id = client_order_id,
            side = ?side,
            price = %fill_price,
            qty = %qty,
            "[DRY-RUN] Simulating HL IOC fill"
        );

        Ok(FillEvent {
            fill_id: format!("sim_hl_{}", uuid::Uuid::new_v4()),
            client_order_id: client_order_id.to_string(),
            venue: VenueId::Hyperliquid,
            symbol: symbol.to_string(),
            side,
            price: fill_price,
            qty,
            fee,
            fee_asset: "USDC".into(),
            is_maker: false,
            timestamp_ns: unix_now_ns(),
        })
    }

    async fn submit_bn_market(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        let quote = {
            let state = self.state.read();
            state
                .quotes
                .get(&(VenueId::BinanceFutures, symbol.to_string()))
                .cloned()
                .ok_or_else(|| ExecutionError::Other(format!("No BN quote available for {}", symbol)))?
        };

        let fill_price = match side {
            OrderSide::Buy => quote.ask_price,
            OrderSide::Sell => quote.bid_price,
        };

        let notional = fill_price * qty;
        let fee = notional * self.taker_fee_rate;

        {
            let mut state = self.state.write();
            let pos = state
                .positions
                .entry((VenueId::BinanceFutures, symbol.to_string()))
                .or_insert(Decimal::ZERO);
            match side {
                OrderSide::Buy => *pos += qty,
                OrderSide::Sell => *pos -= qty,
            }
        }

        info!(
            venue = "BINANCE",
            cl_id = client_order_id,
            side = ?side,
            price = %fill_price,
            qty = %qty,
            "[DRY-RUN] Simulating BN Market fill"
        );

        Ok(FillEvent {
            fill_id: format!("sim_bn_{}", uuid::Uuid::new_v4()),
            client_order_id: client_order_id.to_string(),
            venue: VenueId::BinanceFutures,
            symbol: symbol.to_string(),
            side,
            price: fill_price,
            qty,
            fee,
            fee_asset: "USDT".into(),
            is_maker: false,
            timestamp_ns: unix_now_ns(),
        })
    }

    async fn cancel_order(
        &self,
        _venue: VenueId,
        _symbol: &str,
        _client_order_id: &str,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    async fn query_position(
        &self,
        venue: VenueId,
        symbol: &str,
    ) -> Result<Decimal, ExecutionError> {
        let state = self.state.read();
        Ok(state
            .positions
            .get(&(venue, symbol.to_string()))
            .cloned()
            .unwrap_or(Decimal::ZERO))
    }
}
