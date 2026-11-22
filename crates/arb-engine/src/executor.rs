use arb_core::{FillEvent, OrderSide, VenueId};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Order rejected by exchange: {0}")]
    Rejected(String),

    #[error("Post-only (GTX) order rejected because it would immediately match: {0}")]
    PostOnlyRejected(String),

    #[error("Order not found / already closed: {0}")]
    OrderNotFound(String),

    #[error("Order expired/IOC cancelled with zero fill")]
    ZeroFill,

    #[error("Network error: {0}")]
    Network(String),

    #[error("Network or transport timeout: {0}")]
    Timeout(String),

    #[error("Exchange API error code {code}: {msg}")]
    ApiError { code: i64, msg: String },

    #[error("Execution failure: {0}")]
    Other(String),
}

/// Acknowledgment or status snapshot of a Binance USD-M Futures limit/maker order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BnOrderAck {
    pub order_id: i64,
    pub client_order_id: String,
    pub status: String,
    pub price: Decimal,
    pub orig_qty: Decimal,
    pub executed_qty: Decimal,
}

#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn fetch_quotes(
        &self,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<(arb_core::L1Quote, arb_core::L1Quote), ExecutionError>;

    async fn submit_hl_ioc(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        self.submit_hl_ioc_with_opts(symbol, side, qty, limit_price, false, client_order_id)
            .await
    }

    async fn submit_hl_ioc_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError>;

    async fn submit_bn_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError>;

    async fn submit_bn_market(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        self.submit_bn_market_with_opts(symbol, side, qty, false, client_order_id)
            .await
    }

    async fn submit_bn_market_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError>;

    async fn cancel_order(
        &self,
        venue: VenueId,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<(), ExecutionError>;

    async fn cancel_bn_all_open_orders(&self, symbol: &str) -> Result<(), ExecutionError>;

    async fn query_bn_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError>;

    async fn query_position(
        &self,
        venue: VenueId,
        symbol: &str,
    ) -> Result<Decimal, ExecutionError>;
}

#[async_trait]
impl<T: ExecutionClient + ?Sized> ExecutionClient for std::sync::Arc<T> {
    async fn fetch_quotes(
        &self,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<(arb_core::L1Quote, arb_core::L1Quote), ExecutionError> {
        (**self).fetch_quotes(symbol_hl, symbol_bn).await
    }

    async fn submit_hl_ioc(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        (**self)
            .submit_hl_ioc(symbol, side, qty, limit_price, client_order_id)
            .await
    }

    async fn submit_hl_ioc_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        (**self)
            .submit_hl_ioc_with_opts(symbol, side, qty, limit_price, reduce_only, client_order_id)
            .await
    }

    async fn submit_bn_post_only(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError> {
        (**self)
            .submit_bn_post_only(symbol, side, qty, price, reduce_only, client_order_id)
            .await
    }

    async fn submit_bn_market(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        (**self)
            .submit_bn_market(symbol, side, qty, client_order_id)
            .await
    }

    async fn submit_bn_market_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        (**self)
            .submit_bn_market_with_opts(symbol, side, qty, reduce_only, client_order_id)
            .await
    }

    async fn cancel_order(
        &self,
        venue: VenueId,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<(), ExecutionError> {
        (**self).cancel_order(venue, symbol, client_order_id).await
    }

    async fn cancel_bn_all_open_orders(&self, symbol: &str) -> Result<(), ExecutionError> {
        (**self).cancel_bn_all_open_orders(symbol).await
    }

    async fn query_bn_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError> {
        (**self).query_bn_order(symbol, client_order_id).await
    }

    async fn query_position(
        &self,
        venue: VenueId,
        symbol: &str,
    ) -> Result<Decimal, ExecutionError> {
        (**self).query_position(venue, symbol).await
    }
}
