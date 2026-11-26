use crate::executor::{BnOrderAck, ExecutionClient, ExecutionError, HlOrderAck};
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
    open_bn_orders: HashMap<String, BnOrderAck>,
    closed_bn_orders: HashMap<String, BnOrderAck>,
    open_hl_orders: HashMap<u64, HlOrderAck>,
    closed_hl_orders: HashMap<u64, HlOrderAck>,
    next_order_id: i64,
    next_hl_oid: u64,
    hl_forced_zero_fills: usize,
}

#[derive(Clone, Default)]
pub struct DryRunExecutor {
    state: Arc<RwLock<DryRunState>>,
    bn_taker_fee_rate: Decimal,
    hl_taker_fee_rate: Decimal,
}

impl DryRunExecutor {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(DryRunState {
                next_order_id: 1,
                next_hl_oid: 1,
                ..Default::default()
            })),
            bn_taker_fee_rate: dec!(0.0004),          // 4.00 bps Binance taker fee
            hl_taker_fee_rate: Decimal::new(86, 6),   // 0.86 bps Hyperliquid taker fee
        }
    }

    pub fn update_quote(&self, quote: L1Quote) {
        let mut state = self.state.write();
        state
            .quotes
            .insert((quote.venue, quote.symbol.clone()), quote);
    }

    /// Force the next `count` HL IOC submissions to return `ExecutionError::ZeroFill` (for retry tests).
    pub fn force_hl_zero_fills(&self, count: usize) {
        self.state.write().hl_forced_zero_fills = count;
    }

    /// Snapshot of currently open simulated Binance post-only orders.
    pub fn open_bn_orders(&self) -> Vec<BnOrderAck> {
        self.state.read().open_bn_orders.values().cloned().collect()
    }

    /// Record a simulated Binance Maker fill in dry-run position state and order tracking.
    pub fn record_bn_maker_fill(&self, symbol: &str, side: OrderSide, qty: Decimal) {
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

    /// Update simulated BN order status (e.g., partial/full fill or cancel race) in dry-run state.
    pub fn apply_bn_order_fill_state(
        &self,
        symbol: &str,
        client_order_id: &str,
        side: OrderSide,
        fill_qty: Decimal,
        is_terminal: bool,
    ) {
        self.record_bn_maker_fill(symbol, side, fill_qty);
        let mut state = self.state.write();
        if let Some(mut ord) = state.open_bn_orders.get(client_order_id).cloned() {
            ord.executed_qty += fill_qty;
            if is_terminal || ord.executed_qty >= ord.orig_qty {
                ord.status = "FILLED".to_string();
                state.open_bn_orders.remove(client_order_id);
                state.closed_bn_orders.insert(client_order_id.to_string(), ord);
            } else {
                ord.status = "PARTIALLY_FILLED".to_string();
                state.open_bn_orders.insert(client_order_id.to_string(), ord);
            }
        }
    }

    /// Snapshot of currently open simulated Hyperliquid ALO post-only orders.
    pub fn open_hl_orders(&self) -> Vec<HlOrderAck> {
        self.state.read().open_hl_orders.values().cloned().collect()
    }

    /// Record a simulated Hyperliquid Maker fill in dry-run position state.
    pub fn record_hl_maker_fill(&self, symbol: &str, side: OrderSide, qty: Decimal) {
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

    /// Update simulated HL order status (partial/full fill) in dry-run state.
    pub fn apply_hl_order_fill_state(
        &self,
        symbol: &str,
        oid: u64,
        side: OrderSide,
        fill_qty: Decimal,
        is_terminal: bool,
    ) {
        self.record_hl_maker_fill(symbol, side, fill_qty);
        let mut state = self.state.write();
        if let Some(mut ord) = state.open_hl_orders.get(&oid).cloned() {
            ord.executed_qty += fill_qty;
            if is_terminal || ord.executed_qty >= ord.orig_qty {
                ord.status = "filled".to_string();
                state.open_hl_orders.remove(&oid);
                state.closed_hl_orders.insert(oid, ord);
            } else {
                state.open_hl_orders.insert(oid, ord);
            }
        }
    }
}

#[async_trait]
impl ExecutionClient for DryRunExecutor {
    async fn fetch_quotes(
        &self,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<(L1Quote, L1Quote), ExecutionError> {
        let state = self.state.read();
        let q_hl = state
            .quotes
            .get(&(VenueId::Hyperliquid, symbol_hl.to_string()))
            .cloned()
            .unwrap_or_else(|| L1Quote {
                venue: VenueId::Hyperliquid,
                symbol: symbol_hl.to_string(),
                bid_price: dec!(100.00),
                bid_qty: dec!(100),
                ask_price: dec!(100.01),
                ask_qty: dec!(100),
                timestamp_ns: unix_now_ns(),
                local_recv_ns: unix_now_ns(),
            });
        let q_bn = state
            .quotes
            .get(&(VenueId::BinanceFutures, symbol_bn.to_string()))
            .cloned()
            .unwrap_or_else(|| L1Quote {
                venue: VenueId::BinanceFutures,
                symbol: symbol_bn.to_string(),
                bid_price: dec!(100.04),
                bid_qty: dec!(100),
                ask_price: dec!(100.05),
                ask_qty: dec!(100),
                timestamp_ns: unix_now_ns(),
                local_recv_ns: unix_now_ns(),
            });
        Ok((q_hl, q_bn))
    }

    async fn submit_hl_ioc_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        _reduce_only: bool,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        {
            let mut st = self.state.write();
            if st.hl_forced_zero_fills > 0 {
                st.hl_forced_zero_fills -= 1;
                return Err(ExecutionError::ZeroFill);
            }
        }

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
        let fee = notional * self.hl_taker_fee_rate;

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

    async fn submit_bn_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        _reduce_only: bool,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError> {
        let mut state = self.state.write();
        if let Some(q_bn) = state.quotes.get(&(VenueId::BinanceFutures, symbol.to_string())) {
            match side {
                OrderSide::Sell if price <= q_bn.bid_price => {
                    return Err(ExecutionError::PostOnlyRejected(format!(
                        "Sell price {} <= bid {}",
                        price, q_bn.bid_price
                    )));
                }
                OrderSide::Buy if price >= q_bn.ask_price => {
                    return Err(ExecutionError::PostOnlyRejected(format!(
                        "Buy price {} >= ask {}",
                        price, q_bn.ask_price
                    )));
                }
                _ => {}
            }
        }

        state.next_order_id += 1;
        let ack = BnOrderAck {
            order_id: state.next_order_id,
            client_order_id: client_order_id.to_string(),
            status: "NEW".to_string(),
            price,
            orig_qty: qty,
            executed_qty: Decimal::ZERO,
        };
        state
            .open_bn_orders
            .insert(client_order_id.to_string(), ack.clone());
        Ok(ack)
    }

    async fn submit_bn_market_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        _reduce_only: bool,
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
        let fee = notional * self.bn_taker_fee_rate;

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
        venue: VenueId,
        _symbol: &str,
        client_order_id: &str,
    ) -> Result<(), ExecutionError> {
        if venue == VenueId::BinanceFutures {
            let mut state = self.state.write();
            if let Some(mut ord) = state.open_bn_orders.remove(client_order_id) {
                ord.status = "CANCELED".to_string();
                state
                    .closed_bn_orders
                    .insert(client_order_id.to_string(), ord);
                return Ok(());
            } else {
                return Err(ExecutionError::OrderNotFound(format!(
                    "Simulated order {} not open",
                    client_order_id
                )));
            }
        }
        Ok(())
    }

    async fn cancel_bn_all_open_orders(&self, _symbol: &str) -> Result<(), ExecutionError> {
        let mut state = self.state.write();
        let drained: Vec<(String, BnOrderAck)> = state.open_bn_orders.drain().collect();
        for (cl_id, mut ord) in drained {
            ord.status = "CANCELED".to_string();
            state.closed_bn_orders.insert(cl_id, ord);
        }
        Ok(())
    }

    async fn query_bn_order(
        &self,
        _symbol: &str,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError> {
        let state = self.state.read();
        if let Some(ord) = state.open_bn_orders.get(client_order_id) {
            return Ok(ord.clone());
        }
        if let Some(ord) = state.closed_bn_orders.get(client_order_id) {
            return Ok(ord.clone());
        }
        Err(ExecutionError::OrderNotFound(format!(
            "Simulated order {} not found",
            client_order_id
        )))
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

    async fn submit_hl_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        _reduce_only: bool,
    ) -> Result<HlOrderAck, ExecutionError> {
        let mut state = self.state.write();
        if let Some(q_hl) = state.quotes.get(&(VenueId::Hyperliquid, symbol.to_string())) {
            match side {
                OrderSide::Buy if price >= q_hl.ask_price => {
                    return Err(ExecutionError::PostOnlyRejected(format!(
                        "HL Buy price {} >= ask {}",
                        price, q_hl.ask_price
                    )));
                }
                OrderSide::Sell if price <= q_hl.bid_price => {
                    return Err(ExecutionError::PostOnlyRejected(format!(
                        "HL Sell price {} <= bid {}",
                        price, q_hl.bid_price
                    )));
                }
                _ => {}
            }
        }

        state.next_hl_oid += 1;
        let oid = state.next_hl_oid;
        let ack = HlOrderAck {
            oid,
            status: "resting".to_string(),
            price,
            orig_qty: qty,
            executed_qty: Decimal::ZERO,
        };
        state.open_hl_orders.insert(oid, ack.clone());
        Ok(ack)
    }

    async fn cancel_hl_order(&self, _symbol: &str, oid: u64) -> Result<(), ExecutionError> {
        let mut state = self.state.write();
        if let Some(mut ord) = state.open_hl_orders.remove(&oid) {
            ord.status = "canceled".to_string();
            state.closed_hl_orders.insert(oid, ord);
            Ok(())
        } else {
            Err(ExecutionError::OrderNotFound(format!(
                "Simulated HL order {} not open",
                oid
            )))
        }
    }

    async fn cancel_hl_all_open_orders(&self, _symbol: &str) -> Result<(), ExecutionError> {
        let mut state = self.state.write();
        let drained: Vec<(u64, HlOrderAck)> = state.open_hl_orders.drain().collect();
        for (oid, mut ord) in drained {
            ord.status = "canceled".to_string();
            state.closed_hl_orders.insert(oid, ord);
        }
        Ok(())
    }

    async fn query_hl_order(&self, oid: u64) -> Result<HlOrderAck, ExecutionError> {
        let state = self.state.read();
        if let Some(ord) = state.open_hl_orders.get(&oid) {
            return Ok(ord.clone());
        }
        if let Some(ord) = state.closed_hl_orders.get(&oid) {
            return Ok(ord.clone());
        }
        Err(ExecutionError::OrderNotFound(format!(
            "Simulated HL order {} not found",
            oid
        )))
    }
}
