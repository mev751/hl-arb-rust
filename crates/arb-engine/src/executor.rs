use arb_core::{FillEvent, OrderSide, VenueId};
use async_trait::async_trait;
use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Order rejected by exchange: {0}")]
    Rejected(String),

    #[error("Order expired/IOC cancelled with zero fill")]
    ZeroFill,

    #[error("Network or transport timeout: {0}")]
    Timeout(String),

    #[error("Exchange API error code {code}: {msg}")]
    ApiError { code: i64, msg: String },

    #[error("Execution failure: {0}")]
    Other(String),
}

#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn submit_hl_ioc(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError>;

    async fn submit_bn_market(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError>;

    async fn cancel_order(
        &self,
        venue: VenueId,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<(), ExecutionError>;

    async fn query_position(
        &self,
        venue: VenueId,
        symbol: &str,
    ) -> Result<Decimal, ExecutionError>;
}
