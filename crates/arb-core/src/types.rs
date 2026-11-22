use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VenueId {
    BinanceFutures,
    Hyperliquid,
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VenueId::BinanceFutures => write!(f, "BINANCE"),
            VenueId::Hyperliquid => write!(f, "HYPERLIQUID"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn opposite(&self) -> Self {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
    ImmediateOrCancel, // IOC
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc, // Good 'til Canceled
    Ioc, // Immediate or Cancel
    Fok, // Fill or Kill
    PostOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1Quote {
    pub venue: VenueId,
    pub symbol: String,
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
    pub timestamp_ns: u64,
    pub local_recv_ns: u64,
}

impl L1Quote {
    pub fn mid_price(&self) -> Option<Decimal> {
        if self.bid_price > Decimal::ZERO && self.ask_price > Decimal::ZERO {
            Some((self.bid_price + self.ask_price) / Decimal::from(2))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub intent_id: String,
    pub client_order_id: String,
    pub venue: VenueId,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub qty: Decimal,
    pub created_at_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvent {
    pub fill_id: String,
    pub client_order_id: String,
    pub venue: VenueId,
    pub symbol: String,
    pub side: OrderSide,
    pub price: Decimal,
    pub qty: Decimal,
    pub fee: Decimal,
    pub fee_asset: String,
    pub is_maker: bool,
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub venue: VenueId,
    pub symbol: String,
    pub net_qty: Decimal,
    pub entry_price: Decimal,
    pub realized_pnl: Decimal,
    pub updated_at: DateTime<Utc>,
}

impl Position {
    pub fn new(venue: VenueId, symbol: String) -> Self {
        Self {
            venue,
            symbol,
            net_qty: Decimal::ZERO,
            entry_price: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        }
    }

    pub fn apply_fill(&mut self, fill: &FillEvent) {
        let signed_qty = match fill.side {
            OrderSide::Buy => fill.qty,
            OrderSide::Sell => -fill.qty,
        };

        if self.net_qty.is_zero() {
            self.net_qty = signed_qty;
            self.entry_price = fill.price;
        } else if (self.net_qty > Decimal::ZERO && signed_qty > Decimal::ZERO)
            || (self.net_qty < Decimal::ZERO && signed_qty < Decimal::ZERO)
        {
            // Adding to position: weighted average entry price
            let total_cost = (self.net_qty.abs() * self.entry_price) + (fill.qty * fill.price);
            let total_qty = self.net_qty.abs() + fill.qty;
            if total_qty > Decimal::ZERO {
                self.entry_price = total_cost / total_qty;
            }
            self.net_qty += signed_qty;
        } else {
            // Reducing position
            let closing_qty = signed_qty.abs().min(self.net_qty.abs());
            let pnl = if self.net_qty > Decimal::ZERO {
                // Long closed with Sell
                (fill.price - self.entry_price) * closing_qty
            } else {
                // Short closed with Buy
                (self.entry_price - fill.price) * closing_qty
            };
            self.realized_pnl += pnl - fill.fee;
            self.net_qty += signed_qty;
            if self.net_qty.is_zero() {
                self.entry_price = Decimal::ZERO;
            }
        }
        self.updated_at = Utc::now();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArbDirection {
    /// Mode 1: Short BN Maker + Long HL Taker (Entry spread >= threshold, Exit spread <= threshold)
    ShortSpread,
    /// Mode 2: Long BN Maker + Short HL Taker (Entry spread <= threshold, Exit spread >= threshold)
    LongSpread,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRoundTrip {
    pub trade_id: String,
    pub symbol_hl: String,
    pub symbol_bn: String,
    #[serde(default)]
    pub direction: Option<ArbDirection>,
    pub entry_hl_price: Decimal,
    pub entry_bn_price: Decimal,
    pub entry_qty: Decimal,
    pub exit_hl_price: Option<Decimal>,
    pub exit_bn_price: Option<Decimal>,
    pub exit_qty: Option<Decimal>,
    pub entry_basis_bps: Decimal,
    pub exit_basis_bps: Option<Decimal>,
    pub gross_pnl_usdt: Decimal,
    pub total_fees_usdt: Decimal,
    pub net_pnl_usdt: Decimal,
    pub entry_time_ns: u64,
    pub exit_time_ns: Option<u64>,
}
