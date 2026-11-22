use crate::live::LiveExecutor;
use arb_core::{clocks::unix_now_ns, L1Quote, OrderSide, VenueId};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{debug, info, warn};

const HL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HL_PONG_WATCHDOG_INTERVAL: Duration = Duration::from_secs(2);
const HL_PONG_TIMEOUT: Duration = Duration::from_secs(6);
const BN_LISTEN_KEY_KEEPALIVE: Duration = Duration::from_secs(25 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedKind {
    BnMarket,
    BnUser,
    HlMarket,
    HlTrade,
}

/// Real-time Binance USD-M Futures `ORDER_TRADE_UPDATE` execution event.
#[derive(Debug, Clone)]
pub struct BnOrderUpdate {
    pub symbol: String,
    pub client_order_id: String,
    pub order_id: i64,
    pub side: OrderSide,
    pub exec_type: String,
    pub order_status: String,
    pub last_filled_qty: Decimal,
    pub cum_filled_qty: Decimal,
    pub last_filled_px: Decimal,
    pub avg_price: Decimal,
    pub commission: Decimal,
    pub commission_asset: String,
    pub is_maker: bool,
    pub reduce_only: bool,
    pub trade_id: i64,
    pub event_time_ms: u64,
}

/// Unified event dispatched into `MakerTakerEngine` from WebSocket feeds.
#[derive(Debug, Clone)]
pub enum EngineEvent {
    Quote(L1Quote),
    BnOrderUpdate(BnOrderUpdate),
    FeedHealth { feed: FeedKind, healthy: bool },
}

fn parse_dec_field(obj: &serde_json::Value, key: &str) -> Decimal {
    obj.get(key)
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<Decimal>().ok())
                .or_else(|| v.as_f64().and_then(|f| Decimal::try_from(f).ok()))
        })
        .unwrap_or(Decimal::ZERO)
}

/// Parse a Binance `bookTicker` WebSocket JSON payload into an `L1Quote`.
pub fn parse_bn_book_ticker(txt: &str, configured_symbol: &str) -> Option<L1Quote> {
    let val: serde_json::Value = serde_json::from_str(txt).ok()?;
    let data = val.get("data").unwrap_or(&val);

    let bid_price = parse_dec_field(data, "b");
    let bid_qty = parse_dec_field(data, "B");
    let ask_price = parse_dec_field(data, "a");
    let ask_qty = parse_dec_field(data, "A");

    if bid_price <= Decimal::ZERO || ask_price <= Decimal::ZERO {
        return None;
    }

    let now_ns = unix_now_ns();
    let event_ms = data.get("E").and_then(|v| v.as_u64()).unwrap_or(0);
    let timestamp_ns = if event_ms > 0 {
        event_ms.saturating_mul(1_000_000)
    } else {
        now_ns
    };

    Some(L1Quote {
        venue: VenueId::BinanceFutures,
        symbol: configured_symbol.to_string(),
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        timestamp_ns,
        local_recv_ns: now_ns,
    })
}

/// Parsed User Data Stream frame outcome.
#[derive(Debug, Clone)]
pub enum BnUserStreamMsg {
    OrderUpdate(BnOrderUpdate),
    ListenKeyExpired,
    Other,
}

/// Parse a Binance USD-M Futures User Data Stream message.
pub fn parse_bn_user_message(txt: &str, expected_symbol_upper: &str) -> Option<BnUserStreamMsg> {
    let val: serde_json::Value = serde_json::from_str(txt).ok()?;
    let event_type = val.get("e").and_then(|v| v.as_str()).unwrap_or("");
    match event_type {
        "listenKeyExpired" => Some(BnUserStreamMsg::ListenKeyExpired),
        "ORDER_TRADE_UPDATE" => {
            let o = val.get("o")?;
            let symbol = o.get("s").and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
            if !expected_symbol_upper.is_empty() && symbol != expected_symbol_upper {
                return Some(BnUserStreamMsg::Other);
            }
            let side = match o.get("S").and_then(|v| v.as_str()).unwrap_or("") {
                "BUY" => OrderSide::Buy,
                "SELL" => OrderSide::Sell,
                _ => return None,
            };

            let update = BnOrderUpdate {
                symbol,
                client_order_id: o
                    .get("c")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                order_id: o.get("i").and_then(|v| v.as_i64()).unwrap_or(0),
                side,
                exec_type: o
                    .get("x")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                order_status: o
                    .get("X")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                last_filled_qty: parse_dec_field(o, "l"),
                cum_filled_qty: parse_dec_field(o, "z"),
                last_filled_px: parse_dec_field(o, "L"),
                avg_price: parse_dec_field(o, "ap"),
                commission: parse_dec_field(o, "n"),
                commission_asset: o
                    .get("N")
                    .and_then(|v| v.as_str())
                    .unwrap_or("USDT")
                    .to_string(),
                is_maker: o.get("m").and_then(|v| v.as_bool()).unwrap_or(false),
                reduce_only: o.get("R").and_then(|v| v.as_bool()).unwrap_or(false),
                trade_id: o.get("t").and_then(|v| v.as_i64()).unwrap_or(0),
                event_time_ms: val
                    .get("E")
                    .and_then(|v| v.as_u64())
                    .or_else(|| o.get("T").and_then(|v| v.as_u64()))
                    .unwrap_or(0),
            };
            Some(BnUserStreamMsg::OrderUpdate(update))
        }
        _ => Some(BnUserStreamMsg::Other),
    }
}

/// Parse a Hyperliquid `l2Book` WS message into an `L1Quote` (also signals `pong`).
#[derive(Debug, Clone)]
pub enum HlMarketMsg {
    Pong,
    Quote(L1Quote),
    Other,
}

pub fn parse_hl_l2book(txt: &str, configured_symbol: &str) -> Option<HlMarketMsg> {
    let val: serde_json::Value = serde_json::from_str(txt).ok()?;
    let channel = val.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    match channel {
        "pong" => Some(HlMarketMsg::Pong),
        "l2Book" => {
            let data = val.get("data")?;
            let coin = data.get("coin").and_then(|c| c.as_str()).unwrap_or("");
            let clean_cfg = configured_symbol.split(':').last().unwrap_or(configured_symbol);
            if coin != configured_symbol && coin != clean_cfg {
                return Some(HlMarketMsg::Other);
            }

            let levels = data.get("levels").and_then(|l| l.as_array())?;
            let best_bid = levels.first().and_then(|b| b.as_array()).and_then(|a| a.first())?;
            let best_ask = levels.get(1).and_then(|a| a.as_array()).and_then(|a| a.first())?;

            let bid_price = parse_dec_field(best_bid, "px");
            let bid_qty = parse_dec_field(best_bid, "sz");
            let ask_price = parse_dec_field(best_ask, "px");
            let ask_qty = parse_dec_field(best_ask, "sz");

            if bid_price <= Decimal::ZERO || ask_price <= Decimal::ZERO {
                return Some(HlMarketMsg::Other);
            }

            let now_ns = unix_now_ns();
            let ex_ms = data.get("time").and_then(|v| v.as_u64()).unwrap_or(0);
            let timestamp_ns = if ex_ms > 0 {
                ex_ms.saturating_mul(1_000_000)
            } else {
                now_ns
            };

            Some(HlMarketMsg::Quote(L1Quote {
                venue: VenueId::Hyperliquid,
                symbol: configured_symbol.to_string(),
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                timestamp_ns,
                local_recv_ns: now_ns,
            }))
        }
        _ => Some(HlMarketMsg::Other),
    }
}

/// Spawn Binance `<symbol>@bookTicker` WebSocket feed with automatic reconnect.
pub fn spawn_bn_book_ticker_feed(
    bn_ws_url: String,
    symbol_bn: String,
    tx: mpsc::Sender<EngineEvent>,
) {
    tokio::spawn(async move {
        let backoffs_ms = [200u64, 500, 1000, 2000, 5000];
        let mut attempt = 0usize;
        let stream_url = format!(
            "{}/{}@bookTicker",
            bn_ws_url.trim_end_matches('/'),
            symbol_bn.to_lowercase()
        );

        loop {
            info!(url = %stream_url, "Connecting Binance bookTicker WS...");
            let connected_at = Instant::now();

            match connect_async_tls_with_config(stream_url.as_str(), None, true, None).await {
                Ok((ws_stream, _)) => {
                    info!(symbol = %symbol_bn, "Binance bookTicker WS connected");
                    if tx
                        .send(EngineEvent::FeedHealth {
                            feed: FeedKind::BnMarket,
                            healthy: true,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }

                    let (mut ws_sink, mut ws_rx) = ws_stream.split();
                    while let Some(msg) = ws_rx.next().await {
                        match msg {
                            Ok(Message::Text(txt)) => {
                                if let Some(q) = parse_bn_book_ticker(&txt, &symbol_bn) {
                                    if tx.send(EngineEvent::Quote(q)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Ok(Message::Ping(p)) => {
                                if ws_sink.send(Message::Pong(p)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }

                    if connected_at.elapsed() >= Duration::from_secs(10) {
                        attempt = 0;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to connect Binance bookTicker WS");
                }
            }

            if tx
                .send(EngineEvent::FeedHealth {
                    feed: FeedKind::BnMarket,
                    healthy: false,
                })
                .await
                .is_err()
            {
                return;
            }

            let delay = backoffs_ms[attempt.min(backoffs_ms.len() - 1)];
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    });
}

/// Build the Binance USD-M Futures private User Data Stream URL (`.../private/ws/<listenKey>`).
pub fn build_bn_user_stream_url(bn_ws_url: &str, listen_key: &str) -> String {
    let trimmed = bn_ws_url.trim_end_matches('/');
    let base = if trimmed.ends_with("/private/ws") {
        trimmed.to_string()
    } else if let Some(prefix) = trimmed.strip_suffix("/public/ws") {
        format!("{}/private/ws", prefix)
    } else if let Some(prefix) = trimmed.strip_suffix("/ws") {
        format!("{}/private/ws", prefix)
    } else {
        format!("{}/private/ws", trimmed)
    };
    format!("{}/{}", base, listen_key)
}

/// Spawn Binance Futures User Data Stream WebSocket with listenKey lifecycle and automatic reconnect.
pub fn spawn_bn_user_data_feed(
    live_exec: Arc<LiveExecutor>,
    bn_ws_url: String,
    symbol_bn: String,
    tx: mpsc::Sender<EngineEvent>,
) {
    tokio::spawn(async move {
        let backoffs_ms = [200u64, 500, 1000, 2000, 5000];
        let mut attempt = 0usize;
        let expected_upper = symbol_bn.to_uppercase();

        loop {
            let listen_key = match live_exec.create_bn_listen_key().await {
                Ok(k) => k,
                Err(e) => {
                    warn!(error = %e, "Failed to create Binance listenKey");
                    if tx
                        .send(EngineEvent::FeedHealth {
                            feed: FeedKind::BnUser,
                            healthy: false,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let delay = backoffs_ms[attempt.min(backoffs_ms.len() - 1)];
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
            };

            let stream_url = build_bn_user_stream_url(&bn_ws_url, &listen_key);
            info!(endpoint = "private/ws", "Connecting Binance User Data Stream WS...");
            let connected_at = Instant::now();

            match connect_async_tls_with_config(stream_url.as_str(), None, true, None).await {
                Ok((ws_stream, _)) => {
                    info!(endpoint = "private/ws", "Binance User Data Stream WS connected");
                    if tx
                        .send(EngineEvent::FeedHealth {
                            feed: FeedKind::BnUser,
                            healthy: true,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }

                    let (mut ws_sink, mut ws_rx) = ws_stream.split();
                    let mut keepalive_interval = tokio::time::interval(BN_LISTEN_KEY_KEEPALIVE);
                    keepalive_interval.tick().await;

                    loop {
                        tokio::select! {
                            maybe_msg = ws_rx.next() => {
                                match maybe_msg {
                                    Some(Ok(Message::Text(txt))) => {
                                        match parse_bn_user_message(&txt, &expected_upper) {
                                            Some(BnUserStreamMsg::OrderUpdate(upd)) => {
                                                if tx.send(EngineEvent::BnOrderUpdate(upd)).await.is_err() {
                                                    return;
                                                }
                                            }
                                            Some(BnUserStreamMsg::ListenKeyExpired) => {
                                                warn!("Binance listenKey expired; renewing User Data Stream");
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        if ws_sink.send(Message::Pong(p)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                    _ => {}
                                }
                            }
                            _ = keepalive_interval.tick() => {
                                if let Err(e) = live_exec.keepalive_bn_listen_key(&listen_key).await {
                                    warn!(error = %e, "Binance listenKey keepalive failed; reconnecting");
                                    break;
                                }
                                debug!("Binance listenKey keepalive succeeded");
                            }
                        }
                    }

                    if connected_at.elapsed() >= Duration::from_secs(10) {
                        attempt = 0;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to connect Binance User Data Stream WS");
                }
            }

            if tx
                .send(EngineEvent::FeedHealth {
                    feed: FeedKind::BnUser,
                    healthy: false,
                })
                .await
                .is_err()
            {
                return;
            }

            let delay = backoffs_ms[attempt.min(backoffs_ms.len() - 1)];
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    });
}

/// Spawn Hyperliquid `l2Book` WebSocket market data feed with 20s JSON ping/pong keepalive.
pub fn spawn_hl_l2book_feed(
    hl_ws_url: String,
    symbol_hl: String,
    tx: mpsc::Sender<EngineEvent>,
) {
    tokio::spawn(async move {
        let backoffs_ms = [200u64, 500, 1000, 2000, 5000];
        let mut attempt = 0usize;

        loop {
            info!(url = %hl_ws_url, symbol = %symbol_hl, "Connecting Hyperliquid l2Book WS...");
            let connected_at = Instant::now();

            match connect_async_tls_with_config(hl_ws_url.as_str(), None, true, None).await {
                Ok((ws_stream, _)) => {
                    let (mut ws_sink, mut ws_rx) = ws_stream.split();
                    let sub_msg = serde_json::json!({
                        "method": "subscribe",
                        "subscription": {
                            "type": "l2Book",
                            "coin": symbol_hl,
                        }
                    });
                    if let Err(e) = ws_sink.send(Message::Text(sub_msg.to_string())).await {
                        warn!(error = %e, "Failed to send HL l2Book subscription");
                    } else {
                        info!(symbol = %symbol_hl, "Hyperliquid l2Book WS connected & subscribed");
                        if tx
                            .send(EngineEvent::FeedHealth {
                                feed: FeedKind::HlMarket,
                                healthy: true,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }

                        let mut ping_interval = tokio::time::interval(HL_HEARTBEAT_INTERVAL);
                        ping_interval.tick().await;
                        let mut watchdog_interval = tokio::time::interval(HL_PONG_WATCHDOG_INTERVAL);
                        watchdog_interval.tick().await;
                        let mut awaiting_pong = false;
                        let mut last_ping = Instant::now();

                        loop {
                            tokio::select! {
                                maybe_msg = ws_rx.next() => {
                                    match maybe_msg {
                                        Some(Ok(Message::Text(txt))) => {
                                            match parse_hl_l2book(&txt, &symbol_hl) {
                                                Some(HlMarketMsg::Pong) => {
                                                    awaiting_pong = false;
                                                    debug!("HL l2Book WS pong received");
                                                }
                                                Some(HlMarketMsg::Quote(q)) => {
                                                    if tx.send(EngineEvent::Quote(q)).await.is_err() {
                                                        return;
                                                    }
                                                }
                                                _ => {}
                                            }
                                        }
                                        Some(Ok(Message::Ping(p))) => {
                                            if ws_sink.send(Message::Pong(p)).await.is_err() {
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    if awaiting_pong && last_ping.elapsed() > HL_PONG_TIMEOUT {
                                        warn!("HL l2Book WS pong missing; reconnecting");
                                        break;
                                    }
                                    if ws_sink.send(Message::Text(r#"{"method":"ping"}"#.to_string())).await.is_err() {
                                        break;
                                    }
                                    awaiting_pong = true;
                                    last_ping = Instant::now();
                                }
                                _ = watchdog_interval.tick() => {
                                    if awaiting_pong && last_ping.elapsed() > HL_PONG_TIMEOUT {
                                        warn!("HL l2Book WS pong timeout (>6s); forcing reconnect");
                                        break;
                                    }
                                }
                            }
                        }

                        if connected_at.elapsed() >= Duration::from_secs(10) {
                            attempt = 0;
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to connect Hyperliquid l2Book WS");
                }
            }

            if tx
                .send(EngineEvent::FeedHealth {
                    feed: FeedKind::HlMarket,
                    healthy: false,
                })
                .await
                .is_err()
            {
                return;
            }

            let delay = backoffs_ms[attempt.min(backoffs_ms.len() - 1)];
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_parse_bn_book_ticker() {
        let raw = r#"{"e":"bookTicker","u":123,"E":1795237199197,"s":"SKHYUSDT","b":"104.03","B":"1.69","a":"104.04","A":"12.5"}"#;
        let q = parse_bn_book_ticker(raw, "SKHYUSDT").unwrap();
        assert_eq!(q.venue, VenueId::BinanceFutures);
        assert_eq!(q.symbol, "SKHYUSDT");
        assert_eq!(q.bid_price, dec!(104.03));
        assert_eq!(q.bid_qty, dec!(1.69));
        assert_eq!(q.ask_price, dec!(104.04));
        assert_eq!(q.ask_qty, dec!(12.5));
        assert_eq!(q.timestamp_ns, 1795237199197u64 * 1_000_000);
    }

    #[test]
    fn test_parse_bn_order_trade_update_and_expire() {
        let raw = r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1795237200123,
            "o": {
                "s": "SKHYUSDT",
                "c": "bn_mk_1",
                "S": "SELL",
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "i": 2828886544,
                "l": "0.04",
                "z": "0.04",
                "L": "104.25",
                "ap": "104.25",
                "n": "0",
                "N": "USDT",
                "t": 999888,
                "m": true,
                "R": false
            }
        }"#;
        match parse_bn_user_message(raw, "SKHYUSDT").unwrap() {
            BnUserStreamMsg::OrderUpdate(upd) => {
                assert_eq!(upd.client_order_id, "bn_mk_1");
                assert_eq!(upd.side, OrderSide::Sell);
                assert_eq!(upd.exec_type, "TRADE");
                assert_eq!(upd.order_status, "PARTIALLY_FILLED");
                assert_eq!(upd.last_filled_qty, dec!(0.04));
                assert_eq!(upd.cum_filled_qty, dec!(0.04));
                assert_eq!(upd.last_filled_px, dec!(104.25));
                assert_eq!(upd.commission, dec!(0));
                assert!(upd.is_maker);
                assert!(!upd.reduce_only);
                assert_eq!(upd.trade_id, 999888);
            }
            other => panic!("Unexpected: {:?}", other),
        }

        let exp = r#"{"e":"listenKeyExpired","E":1795237200999}"#;
        assert!(matches!(
            parse_bn_user_message(exp, "SKHYUSDT").unwrap(),
            BnUserStreamMsg::ListenKeyExpired
        ));
    }

    #[test]
    fn test_parse_hl_l2book_and_pong() {
        assert!(matches!(
            parse_hl_l2book(r#"{"channel":"pong"}"#, "xyz:SKHY").unwrap(),
            HlMarketMsg::Pong
        ));

        let book = r#"{
            "channel": "l2Book",
            "data": {
                "coin": "xyz:SKHY",
                "time": 1795237199335,
                "levels": [
                    [{"px": "103.79", "sz": "20.99", "n": 2}],
                    [{"px": "103.82", "sz": "6.02", "n": 1}]
                ]
            }
        }"#;
        match parse_hl_l2book(book, "xyz:SKHY").unwrap() {
            HlMarketMsg::Quote(q) => {
                assert_eq!(q.venue, VenueId::Hyperliquid);
                assert_eq!(q.bid_price, dec!(103.79));
                assert_eq!(q.ask_price, dec!(103.82));
            }
            other => panic!("Unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_build_bn_user_stream_url() {
        assert_eq!(
            build_bn_user_stream_url("wss://fstream.binance.com/ws", "KEY123"),
            "wss://fstream.binance.com/private/ws/KEY123"
        );
        assert_eq!(
            build_bn_user_stream_url("wss://fstream.binance.com/ws/", "KEY123"),
            "wss://fstream.binance.com/private/ws/KEY123"
        );
        assert_eq!(
            build_bn_user_stream_url("wss://fstream.binance.com/public/ws", "KEY123"),
            "wss://fstream.binance.com/private/ws/KEY123"
        );
        assert_eq!(
            build_bn_user_stream_url("wss://fstream.binance.com/private/ws", "KEY123"),
            "wss://fstream.binance.com/private/ws/KEY123"
        );
        assert_eq!(
            build_bn_user_stream_url("wss://fstream.binance.com", "KEY123"),
            "wss://fstream.binance.com/private/ws/KEY123"
        );
    }
}
