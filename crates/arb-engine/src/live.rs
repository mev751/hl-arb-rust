use crate::executor::{BnOrderAck, ExecutionClient, ExecutionError};
use crate::hl_ws::HlWsTradeClient;
use arb_core::{clocks::unix_now_ns, FillEvent, OrderSide, VenueId};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use k256::ecdsa::SigningKey;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::Sha256;
use sha3::{Digest, Keccak256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct LiveExecutorConfig {
    pub bn_api_key: String,
    pub bn_api_secret: String,
    pub bn_rest_url: String,
    pub bn_ws_url: String,
    pub hl_wallet: String,
    pub hl_private_key: String,
    pub hl_rest_url: String,
    pub hl_ws_url: String,
    pub hl_is_mainnet: bool,
    pub hl_dex: String,
}

#[derive(Clone, Debug)]
pub struct CachedHlIoc {
    pub symbol: String,
    pub side: OrderSide,
    pub qty: Decimal,
    pub limit_price: Decimal,
    pub reduce_only: bool,
    pub nonce: u64,
    pub payload: serde_json::Value,
    pub generated_at: std::time::Instant,
}

pub struct LiveExecutor {
    http: reqwest::Client,
    config: LiveExecutorConfig,
    hl_signing_key: Option<SigningKey>,
    hl_asset_info: parking_lot::RwLock<Option<(u64, u32)>>,
    hl_master_wallet: parking_lot::RwLock<Option<String>>,
    hl_trade_ws: parking_lot::RwLock<Option<HlWsTradeClient>>,
    last_nonce: AtomicU64,
    cached_ioc: parking_lot::RwLock<Option<CachedHlIoc>>,
}

impl LiveExecutor {
    pub fn new(config: LiveExecutorConfig) -> Result<Self, anyhow::Error> {
        let http = reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(90))
            .timeout(Duration::from_secs(10))
            .build()?;

        let hl_signing_key = if !config.hl_private_key.is_empty() && !config.hl_private_key.starts_with("ENV_") {
            let key_str = config.hl_private_key.trim().trim_start_matches("0x");
            let key_bytes = hex::decode(key_str)?;
            let key = SigningKey::from_slice(&key_bytes)?;
            Some(key)
        } else {
            None
        };

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(Self {
            http,
            config,
            hl_signing_key,
            hl_asset_info: parking_lot::RwLock::new(None),
            hl_master_wallet: parking_lot::RwLock::new(None),
            hl_trade_ws: parking_lot::RwLock::new(None),
            last_nonce: AtomicU64::new(now_ms),
            cached_ioc: parking_lot::RwLock::new(None),
        })
    }

    /// Attach a persistent Hyperliquid WebSocket `post` client for low-latency order execution.
    pub fn attach_hl_trade_ws(&self, ws: HlWsTradeClient) {
        *self.hl_trade_ws.write() = Some(ws);
    }

    pub fn is_hl_trade_ws_healthy(&self) -> bool {
        self.hl_trade_ws
            .read()
            .as_ref()
            .map(|ws| ws.is_healthy())
            .unwrap_or(false)
    }

    /// Resolve master wallet address when configured `hl_wallet` is an API/agent signer address.
    pub async fn resolve_hl_master_wallet(&self) -> Result<String, ExecutionError> {
        if let Some(cached) = self.hl_master_wallet.read().clone() {
            return Ok(cached);
        }

        let resp = self
            .http
            .post(format!("{}/info", self.config.hl_rest_url))
            .json(&serde_json::json!({
                "type": "userRole",
                "user": self.config.hl_wallet,
            }))
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("HL userRole network error: {}", e)))?;

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ExecutionError::Network(format!("Parse HL userRole failed: {}", e)))?;

        let role = json.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let resolved = if role == "agent" {
            json.get("data")
                .and_then(|d| d.get("user"))
                .and_then(|u| u.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(|master| {
                    info!(
                        agent_wallet = %self.config.hl_wallet,
                        master_wallet = %master,
                        "Resolved Hyperliquid master wallet from agent address"
                    );
                    master.to_string()
                })
                .unwrap_or_else(|| self.config.hl_wallet.clone())
        } else {
            self.config.hl_wallet.clone()
        };

        *self.hl_master_wallet.write() = Some(resolved.clone());
        Ok(resolved)
    }

    fn sign_bn_query(&self, query: &str) -> Result<String, ExecutionError> {
        let mut mac = HmacSha256::new_from_slice(self.config.bn_api_secret.as_bytes())
            .map_err(|e| ExecutionError::Other(format!("HMAC key init error: {}", e)))?;
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn parse_bn_error(status: reqwest::StatusCode, body: &str) -> ExecutionError {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
            if let Some(code) = v.get("code").and_then(|c| c.as_i64()) {
                let msg = v
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or(body)
                    .to_string();
                return match code {
                    -5022 => ExecutionError::PostOnlyRejected(format!("code {}: {}", code, msg)),
                    -2011 | -2013 => ExecutionError::OrderNotFound(format!("code {}: {}", code, msg)),
                    _ => ExecutionError::ApiError { code, msg },
                };
            }
        }
        ExecutionError::Rejected(format!("Binance HTTP {}: {}", status, body))
    }

    fn parse_bn_order_ack(resp_json: &serde_json::Value) -> BnOrderAck {
        BnOrderAck {
            order_id: resp_json.get("orderId").and_then(|v| v.as_i64()).unwrap_or(0),
            client_order_id: resp_json
                .get("clientOrderId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            status: resp_json
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            price: resp_json
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(Decimal::ZERO),
            orig_qty: resp_json
                .get("origQty")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(Decimal::ZERO),
            executed_qty: resp_json
                .get("executedQty")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(Decimal::ZERO),
        }
    }

    /// Create a Binance USD-M Futures User Data Stream listenKey (`POST /fapi/v1/listenKey`).
    pub async fn create_bn_listen_key(&self) -> Result<String, ExecutionError> {
        let url = format!("{}/fapi/v1/listenKey", self.config.bn_rest_url);
        let resp = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.config.bn_api_key)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance listenKey create error: {}", e)))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExecutionError::Network(format!("Read Binance listenKey body failed: {}", e)))?;

        if !status.is_success() {
            return Err(Self::parse_bn_error(status, &body));
        }

        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| ExecutionError::Other(format!("Parse listenKey json failed: {}", e)))?;

        json.get("listenKey")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| ExecutionError::Other(format!("Missing listenKey in response: {}", body)))
    }

    /// Keepalive an active Binance USD-M Futures User Data Stream listenKey (`PUT /fapi/v1/listenKey`).
    pub async fn keepalive_bn_listen_key(&self, _listen_key: &str) -> Result<(), ExecutionError> {
        let url = format!("{}/fapi/v1/listenKey", self.config.bn_rest_url);
        let resp = self
            .http
            .put(&url)
            .header("X-MBX-APIKEY", &self.config.bn_api_key)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance listenKey keepalive error: {}", e)))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Self::parse_bn_error(status, &body));
        }
        Ok(())
    }

    fn next_nonce(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut current = self.last_nonce.load(Ordering::Relaxed);
        loop {
            let next = if now <= current { current + 1 } else { now };
            match self.last_nonce.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(actual) => current = actual,
            }
        }
    }

    /// Resolve Hyperliquid AssetID and szDecimals (HIP-3 DEX or standard perps)
    pub async fn resolve_hl_asset_info(&self, coin_symbol: &str) -> Result<(u64, u32), ExecutionError> {
        if let Some(info) = *self.hl_asset_info.read() {
            return Ok(info);
        }

        let dex = if !self.config.hl_dex.is_empty() {
            self.config.hl_dex.clone()
        } else if let Some((prefix, _)) = coin_symbol.split_once(':') {
            prefix.to_string()
        } else {
            String::new()
        };

        let mut dex_idx = 0usize;
        let mut found_dex = false;

        if !dex.is_empty() {
            let resp = self
                .http
                .post(format!("{}/info", self.config.hl_rest_url))
                .json(&serde_json::json!({ "type": "perpDexs" }))
                .send()
                .await
                .map_err(|e| ExecutionError::Network(format!("Query perpDexs failed: {}", e)))?;

            let dexs: Vec<serde_json::Value> = resp
                .json()
                .await
                .map_err(|e| ExecutionError::Network(format!("Parse perpDexs failed: {}", e)))?;

            for (i, item) in dexs.iter().enumerate() {
                if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                    if name == dex {
                        dex_idx = i;
                        found_dex = true;
                        break;
                    }
                }
            }

            if !found_dex {
                return Err(ExecutionError::Other(format!("DEX {} not found on Hyperliquid", dex)));
            }
        }

        let mut req_body = serde_json::json!({ "type": "meta" });
        if !dex.is_empty() {
            req_body["dex"] = serde_json::Value::String(dex.clone());
        }

        let meta_resp = self
            .http
            .post(format!("{}/info", self.config.hl_rest_url))
            .json(&req_body)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Query HL meta failed: {}", e)))?;

        let meta_json: serde_json::Value = meta_resp
            .json()
            .await
            .map_err(|e| ExecutionError::Network(format!("Parse HL meta failed: {}", e)))?;

        let clean_symbol = coin_symbol.split(':').last().unwrap_or(coin_symbol);

        if let Some(universe) = meta_json.get("universe").and_then(|u| u.as_array()) {
            for (idx, token) in universe.iter().enumerate() {
                if let Some(name) = token.get("name").and_then(|n| n.as_str()) {
                    if name == coin_symbol || name == clean_symbol {
                        let asset_id = if dex.is_empty() {
                            idx as u64
                        } else {
                            100_000 + (dex_idx as u64 * 10_000) + idx as u64
                        };
                        let sz_decimals = token.get("szDecimals").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
                        info!(coin = coin_symbol, asset_id = asset_id, sz_decimals = sz_decimals, dex = dex, "Resolved Hyperliquid asset ID & szDecimals");
                        *self.hl_asset_info.write() = Some((asset_id, sz_decimals));
                        return Ok((asset_id, sz_decimals));
                    }
                }
            }
        }

        Err(ExecutionError::Other(format!(
            "Coin {} not found in Hyperliquid meta universe (dex={})",
            coin_symbol, dex
        )))
    }

    /// Fetch live top-of-book L1 quotes for both venues via REST
    pub async fn fetch_live_quotes(
        &self,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<(arb_core::L1Quote, arb_core::L1Quote), ExecutionError> {
        // 1. Fetch Binance bookTicker
        let bn_url = format!("{}/fapi/v1/ticker/bookTicker?symbol={}", self.config.bn_rest_url, symbol_bn.to_uppercase());
        let bn_resp = self
            .http
            .get(&bn_url)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance ticker network error: {}", e)))?;

        let bn_json: serde_json::Value = bn_resp
            .json()
            .await
            .map_err(|e| ExecutionError::Network(format!("Parse Binance ticker failed: {}", e)))?;

        let bn_bid: Decimal = bn_json.get("bidPrice").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
        let bn_ask: Decimal = bn_json.get("askPrice").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
        let bn_bid_qty: Decimal = bn_json.get("bidQty").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
        let bn_ask_qty: Decimal = bn_json.get("askQty").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);

        let q_bn = arb_core::L1Quote {
            venue: VenueId::BinanceFutures,
            symbol: symbol_bn.to_string(),
            bid_price: bn_bid,
            bid_qty: bn_bid_qty,
            ask_price: bn_ask,
            ask_qty: bn_ask_qty,
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };

        // 2. Fetch Hyperliquid book
        let hl_url = format!("{}/info", self.config.hl_rest_url);
        let hl_resp = self
            .http
            .post(&hl_url)
            .json(&serde_json::json!({ "type": "l2Book", "coin": symbol_hl }))
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("HL l2Book network error: {}", e)))?;

        let hl_json: serde_json::Value = hl_resp
            .json()
            .await
            .map_err(|e| ExecutionError::Network(format!("Parse HL l2Book failed: {}", e)))?;

        let mut hl_bid = Decimal::ZERO;
        let mut hl_bid_qty = Decimal::ZERO;
        let mut hl_ask = Decimal::ZERO;
        let mut hl_ask_qty = Decimal::ZERO;

        if let Some(levels) = hl_json.get("levels").and_then(|l| l.as_array()) {
            if let Some(bids) = levels.get(0).and_then(|b| b.as_array()) {
                if let Some(first_bid) = bids.first() {
                    hl_bid = first_bid.get("px").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
                    hl_bid_qty = first_bid.get("sz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
                }
            }
            if let Some(asks) = levels.get(1).and_then(|a| a.as_array()) {
                if let Some(first_ask) = asks.first() {
                    hl_ask = first_ask.get("px").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
                    hl_ask_qty = first_ask.get("sz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
                }
            }
        }

        if hl_bid.is_zero() || hl_ask.is_zero() {
            hl_bid = bn_bid;
            hl_ask = bn_ask;
            hl_bid_qty = Decimal::from(100);
            hl_ask_qty = Decimal::from(100);
        }

        let q_hl = arb_core::L1Quote {
            venue: VenueId::Hyperliquid,
            symbol: symbol_hl.to_string(),
            bid_price: hl_bid,
            bid_qty: hl_bid_qty,
            ask_price: hl_ask,
            ask_qty: hl_ask_qty,
            timestamp_ns: unix_now_ns(),
            local_recv_ns: unix_now_ns(),
        };

        Ok((q_hl, q_bn))
    }
}

// -----------------------------------------------------------------------------
// Hyperliquid Wire Structs & EIP-712 Signing
// -----------------------------------------------------------------------------

#[derive(Serialize, Debug)]
pub struct HlOrderAction<'a> {
    #[serde(rename = "type")]
    pub action_type: &'a str,
    pub orders: Vec<HlOrderWire<'a>>,
    pub grouping: &'a str,
}

#[derive(Serialize, Debug)]
pub struct HlOrderWire<'a> {
    pub a: u64,
    pub b: bool,
    pub p: &'a str,
    pub s: &'a str,
    pub r: bool,
    pub t: HlOrderTypeWire<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<&'a str>,
}

#[derive(Serialize, Debug)]
pub struct HlOrderTypeWire<'a> {
    pub limit: HlLimitOrderWire<'a>,
}

#[derive(Serialize, Debug)]
pub struct HlLimitOrderWire<'a> {
    pub tif: &'a str,
}

#[derive(Serialize, Debug)]
pub struct HlSignature {
    pub r: String,
    pub s: String,
    pub v: u32,
}

pub fn format_hl_price(px: Decimal, sz_decimals: u32) -> String {
    let px_f = px.to_f64().unwrap_or(0.0);
    if px_f <= 0.0 {
        return "0".to_string();
    }
    let max_dec = if 6 >= sz_decimals { 6 - sz_decimals } else { 0 };
    let exp = px_f.log10().floor();
    let sig_decimals = (4.0 - exp).max(0.0) as u32;
    let final_decimals = sig_decimals.min(max_dec);
    let formatted = format!("{:.*}", final_decimals as usize, px_f);
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-0" {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn format_hl_qty(qty: Decimal, sz_decimals: u32) -> String {
    let formatted = format!("{:.*}", sz_decimals as usize, qty);
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-0" {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn build_l1_action_hash<T: Serialize>(action: &T, nonce: u64) -> Result<[u8; 32], anyhow::Error> {
    let mut buf = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut buf).with_struct_map();
    action.serialize(&mut serializer)?;

    let mut hasher = Keccak256::new();
    hasher.update(&buf);
    hasher.update(&nonce.to_be_bytes());
    hasher.update(&[0x00]); // no vault

    let hash: [u8; 32] = hasher.finalize().into();
    Ok(hash)
}

pub fn sign_phantom_agent(
    signing_key: &SigningKey,
    connection_id: &[u8; 32],
    is_mainnet: bool,
) -> Result<HlSignature, anyhow::Error> {
    let source = if is_mainnet { "a" } else { "b" };

    // 1. Domain separator
    // EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)
    let domain_type_hash = Keccak256::digest(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    let name_hash = Keccak256::digest(b"Exchange");
    let version_hash = Keccak256::digest(b"1");
    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[30] = 0x05;
    chain_id_bytes[31] = 0x39; // 1337 in big-endian uint256
    let address_bytes = [0u8; 32]; // 0x0 address

    let mut domain_hasher = Keccak256::new();
    domain_hasher.update(&domain_type_hash);
    domain_hasher.update(&name_hash);
    domain_hasher.update(&version_hash);
    domain_hasher.update(&chain_id_bytes);
    domain_hasher.update(&address_bytes);
    let domain_separator = domain_hasher.finalize();

    // 2. Message struct hash
    // Agent(string source,bytes32 connectionId)
    let agent_type_hash = Keccak256::digest(b"Agent(string source,bytes32 connectionId)");
    let source_hash = Keccak256::digest(source.as_bytes());

    let mut msg_hasher = Keccak256::new();
    msg_hasher.update(&agent_type_hash);
    msg_hasher.update(&source_hash);
    msg_hasher.update(connection_id);
    let msg_hash = msg_hasher.finalize();

    // 3. Digest: keccak256("\x19\x01" + domainSeparator + msgHash)
    let mut digest_hasher = Keccak256::new();
    digest_hasher.update(b"\x19\x01");
    digest_hasher.update(&domain_separator);
    digest_hasher.update(&msg_hash);
    let digest: [u8; 32] = digest_hasher.finalize().into();

    // 4. ECDSA Secp256k1 signature
    let (sig, recid) = signing_key.sign_prehash_recoverable(&digest)?;
    let (r, s) = sig.split_bytes();

    let v = recid.to_byte() as u32 + 27;

    Ok(HlSignature {
        r: format!("0x{}", hex::encode(r)),
        s: format!("0x{}", hex::encode(s)),
        v,
    })
}

impl LiveExecutor {
    fn bn_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn bn_signed_url(&self, endpoint: &str, query_string: &str) -> Result<String, ExecutionError> {
        let signature = self.sign_bn_query(query_string)?;
        Ok(format!(
            "{}{}?{}&signature={}",
            self.config.bn_rest_url, endpoint, query_string, signature
        ))
    }

    async fn resolve_hl_ioc_fallback_price(
        &self,
        symbol: &str,
        is_buy: bool,
        limit_price: Decimal,
    ) -> Result<Decimal, ExecutionError> {
        let hl_url = format!("{}/info", self.config.hl_rest_url);
        let hl_resp = self
            .http
            .post(&hl_url)
            .json(&serde_json::json!({ "type": "l2Book", "coin": symbol }))
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("HL l2Book network error: {}", e)))?;
        let hl_json: serde_json::Value = hl_resp
            .json()
            .await
            .map_err(|e| ExecutionError::Network(format!("Parse HL l2Book failed: {}", e)))?;

        let mut best_px = Decimal::ZERO;
        if let Some(levels) = hl_json.get("levels").and_then(|l| l.as_array()) {
            let level_idx = if is_buy { 1 } else { 0 };
            if let Some(book_side) = levels.get(level_idx).and_then(|b| b.as_array()) {
                if let Some(top) = book_side.first() {
                    best_px = top
                        .get("px")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(Decimal::ZERO);
                }
            }
        }
        if best_px > Decimal::ZERO {
            if is_buy {
                Ok(best_px * rust_decimal_macros::dec!(1.02))
            } else {
                Ok(best_px * rust_decimal_macros::dec!(0.98))
            }
        } else {
            Ok(limit_price)
        }
    }
    pub fn build_signed_hl_ioc_payload(
        &self,
        _symbol: &str,
        side: OrderSide,
        qty: Decimal,
        effective_limit_price: Decimal,
        reduce_only: bool,
        asset_id: u64,
        sz_decimals: u32,
    ) -> Result<(serde_json::Value, u64), ExecutionError> {
        let signing_key = self
            .hl_signing_key
            .as_ref()
            .ok_or_else(|| ExecutionError::Other("Hyperliquid private key is missing or not configured".into()))?;

        let is_buy = side == OrderSide::Buy;
        let px_str = format_hl_price(effective_limit_price, sz_decimals);
        let qty_str = format_hl_qty(qty, sz_decimals);

        let action = HlOrderAction {
            action_type: "order",
            orders: vec![HlOrderWire {
                a: asset_id,
                b: is_buy,
                p: &px_str,
                s: &qty_str,
                r: reduce_only,
                t: HlOrderTypeWire {
                    limit: HlLimitOrderWire { tif: "Ioc" },
                },
                c: None,
            }],
            grouping: "na",
        };

        let nonce = self.next_nonce();
        let conn_id = build_l1_action_hash(&action, nonce)
            .map_err(|e| ExecutionError::Other(format!("Build action hash failed: {}", e)))?;

        let signature = sign_phantom_agent(signing_key, &conn_id, self.config.hl_is_mainnet)
            .map_err(|e| ExecutionError::Other(format!("EIP-712 signing failed: {}", e)))?;

        let payload = serde_json::json!({
            "action": action,
            "nonce": nonce,
            "signature": signature,
        });

        Ok((payload, nonce))
    }
}

/// Parse a Hyperliquid `/exchange` or WS `post` order response payload into a `FillEvent`.
pub fn parse_hl_order_response(
    resp_json: &serde_json::Value,
    symbol: &str,
    side: OrderSide,
    qty: Decimal,
    limit_price: Decimal,
    client_order_id: &str,
) -> Result<FillEvent, ExecutionError> {
    if resp_json.get("status").and_then(|v| v.as_str()) != Some("ok") {
        return Err(ExecutionError::Rejected(format!(
            "HL order rejected: {}",
            resp_json
        )));
    }

    let statuses = resp_json
        .get("response")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.get("statuses"))
        .and_then(|s| s.as_array());

    if let Some(arr) = statuses {
        if let Some(first) = arr.first() {
            if let Some(err_msg) = first.get("error").and_then(|e| e.as_str()) {
                if err_msg.contains("could not immediately match")
                    || err_msg.to_lowercase().contains("zero fill")
                {
                    return Err(ExecutionError::ZeroFill);
                }
                return Err(ExecutionError::Rejected(format!(
                    "HL order error: {}",
                    err_msg
                )));
            }

            if let Some(filled) = first.get("filled") {
                let total_sz: Decimal = filled
                    .get("totalSz")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(qty);

                let avg_px: Decimal = filled
                    .get("avgPx")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(limit_price);

                let oid = filled.get("oid").and_then(|o| o.as_i64()).unwrap_or(0);

                let notional = avg_px * total_sz;
                let fee = notional * Decimal::new(86, 6); // 0.0086% (0.86 bps) taker fee

                info!(
                    symbol = symbol,
                    side = ?side,
                    qty = %total_sz,
                    price = %avg_px,
                    oid = oid,
                    "Hyperliquid LIVE order filled!"
                );

                return Ok(FillEvent {
                    fill_id: format!("hl_{}_{}", oid, unix_now_ns()),
                    client_order_id: client_order_id.to_string(),
                    venue: VenueId::Hyperliquid,
                    symbol: symbol.to_string(),
                    side,
                    price: avg_px,
                    qty: total_sz,
                    fee,
                    fee_asset: "USDC".to_string(),
                    is_maker: false,
                    timestamp_ns: unix_now_ns(),
                });
            }
        }
    }

    Err(ExecutionError::ZeroFill)
}

// -----------------------------------------------------------------------------
// ExecutionClient Implementation
// -----------------------------------------------------------------------------

#[async_trait]
impl ExecutionClient for LiveExecutor {
    async fn fetch_quotes(
        &self,
        symbol_hl: &str,
        symbol_bn: &str,
    ) -> Result<(arb_core::L1Quote, arb_core::L1Quote), ExecutionError> {
        self.fetch_live_quotes(symbol_hl, symbol_bn).await
    }

    async fn prepare_hl_ioc_cache(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        limit_price: Decimal,
        reduce_only: bool,
    ) {
        if self.hl_signing_key.is_none() || limit_price <= Decimal::ZERO || qty <= Decimal::ZERO {
            return;
        }

        // Fast check if current cache is already fresh and matches
        {
            let cache = self.cached_ioc.read();
            if let Some(c) = cache.as_ref() {
                let same_target = c.symbol == symbol && c.side == side && c.qty == qty && c.reduce_only == reduce_only;
                let price_close = (c.limit_price - limit_price).abs() <= Decimal::ZERO;
                let fresh = c.generated_at.elapsed() < Duration::from_millis(1500);
                if same_target && price_close && fresh {
                    return;
                }
            }
        }

        let Ok((asset_id, sz_decimals)) = self.resolve_hl_asset_info(symbol).await else {
            return;
        };

        match self.build_signed_hl_ioc_payload(
            symbol,
            side,
            qty,
            limit_price,
            reduce_only,
            asset_id,
            sz_decimals,
        ) {
            Ok((payload, nonce)) => {
                let item = CachedHlIoc {
                    symbol: symbol.to_string(),
                    side,
                    qty,
                    limit_price,
                    reduce_only,
                    nonce,
                    payload,
                    generated_at: std::time::Instant::now(),
                };
                *self.cached_ioc.write() = Some(item);
                debug!(
                    symbol = symbol,
                    side = ?side,
                    qty = %qty,
                    price = %limit_price,
                    nonce = nonce,
                    "Pre-signed HL IOC order cached"
                );
            }
            Err(e) => {
                debug!(error = %e, "Failed to pre-sign HL IOC order");
            }
        }
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
        let (asset_id, sz_decimals) = self.resolve_hl_asset_info(symbol).await?;
        let is_buy = side == OrderSide::Buy;

        // Only hit REST l2Book when caller left limit_price <= 0 (e.g. manual emergency close)
        let effective_limit_price = if limit_price <= Decimal::ZERO {
            self.resolve_hl_ioc_fallback_price(symbol, is_buy, limit_price)
                .await?
        } else {
            limit_price
        };

        // Check if pre-signed cache matches
        let cached_hit = {
            let mut lock = self.cached_ioc.write();
            if let Some(c) = lock.take() {
                let same_target = c.symbol == symbol && c.side == side && c.qty == qty && c.reduce_only == reduce_only;
                let price_ok = if is_buy {
                    c.limit_price >= effective_limit_price
                } else {
                    c.limit_price <= effective_limit_price
                };
                let fresh = c.generated_at.elapsed() <= Duration::from_secs(4);
                if same_target && price_ok && fresh {
                    info!(
                        symbol = symbol,
                        side = ?side,
                        qty = %qty,
                        price = %c.limit_price,
                        nonce = c.nonce,
                        age_ms = c.generated_at.elapsed().as_millis(),
                        "HL IOC Leg 2 dispatched via PRE-SIGNED CACHE (0ms crypto latency)"
                    );
                    Some((c.payload, c.limit_price))
                } else {
                    None
                }
            } else {
                None
            }
        };

        let (payload, final_limit_px) = match cached_hit {
            Some((p, px)) => (p, px),
            None => {
                let (p, _) = self.build_signed_hl_ioc_payload(
                    symbol,
                    side,
                    qty,
                    effective_limit_price,
                    reduce_only,
                    asset_id,
                    sz_decimals,
                )?;
                (p, effective_limit_price)
            }
        };

        let px_str = format_hl_price(final_limit_px, sz_decimals);
        let qty_str = format_hl_qty(qty, sz_decimals);

        debug!(
            asset_id = asset_id,
            side = ?side,
            qty = %qty_str,
            price = %px_str,
            reduce_only = reduce_only,
            cloid = client_order_id,
            "Submitting Hyperliquid IOC order"
        );

        // Primary path: persistent Hyperliquid WebSocket `post`
        let ws_opt = self.hl_trade_ws.read().clone();
        if let Some(ws) = ws_opt {
            if ws.is_healthy() {
                match ws.post_action(payload.clone(), 5000).await {
                    Ok(resp_json) => {
                        return parse_hl_order_response(
                            &resp_json,
                            symbol,
                            side,
                            qty,
                            final_limit_px,
                            client_order_id,
                        );
                    }
                    Err(ExecutionError::Network(err)) => {
                        warn!(
                            error = %err,
                            "HL WS post transport failed before delivery; falling back to REST /exchange"
                        );
                    }
                    Err(ExecutionError::Timeout(err)) => {
                        warn!(
                            error = %err,
                            "HL WS post timed out after 5000ms; falling back to REST /exchange"
                        );
                    }
                    Err(other) => {
                        // Propagate ZeroFill / Rejected without blind duplicate submission
                        return Err(other);
                    }
                }
            }
        }

        // Fallback / standalone REST path
        let resp = self
            .http
            .post(format!("{}/exchange", self.config.hl_rest_url))
            .json(&payload)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("HL /exchange network error: {}", e)))?;

        let status_code = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExecutionError::Network(format!("Read HL response body failed: {}", e)))?;

        if !status_code.is_success() {
            return Err(ExecutionError::Rejected(format!(
                "HL /exchange HTTP {}: {}",
                status_code, body
            )));
        }

        let resp_json: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
            ExecutionError::Other(format!("Parse HL json response failed: {}, body: {}", e, body))
        })?;

        parse_hl_order_response(
            &resp_json,
            symbol,
            side,
            qty,
            effective_limit_price,
            client_order_id,
        )
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
        let timestamp = Self::bn_timestamp_ms();
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        let clean_symbol = symbol.to_uppercase();
        let px_str = format!("{:.2}", price.round_dp(2));
        let qty_str = format_hl_qty(qty, 2);

        let query_string = if reduce_only {
            format!(
                "newClientOrderId={}&price={}&quantity={}&recvWindow=5000&reduceOnly=true&side={}&symbol={}&timeInForce=GTX&timestamp={}&type=LIMIT",
                client_order_id, px_str, qty_str, side_str, clean_symbol, timestamp
            )
        } else {
            format!(
                "newClientOrderId={}&price={}&quantity={}&recvWindow=5000&side={}&symbol={}&timeInForce=GTX&timestamp={}&type=LIMIT",
                client_order_id, px_str, qty_str, side_str, clean_symbol, timestamp
            )
        };

        let url = self.bn_signed_url("/fapi/v1/order", &query_string)?;

        debug!(
            symbol = %clean_symbol,
            side = side_str,
            qty = %qty_str,
            price = %px_str,
            reduce_only = reduce_only,
            cloid = client_order_id,
            "Posting GTX Post-Only order to Binance USD-M"
        );

        let resp = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.config.bn_api_key)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance GTX post error: {}", e)))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExecutionError::Network(format!("Read Binance GTX body failed: {}", e)))?;

        if !status.is_success() {
            return Err(Self::parse_bn_error(status, &body));
        }

        let resp_json: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
            ExecutionError::Other(format!("Parse Binance GTX json failed: {}, body: {}", e, body))
        })?;
        let ack = Self::parse_bn_order_ack(&resp_json);

        if (ack.status == "EXPIRED" || ack.status == "REJECTED") && ack.executed_qty.is_zero() {
            return Err(ExecutionError::PostOnlyRejected(format!(
                "Binance GTX immediately expired/rejected ({}): {}",
                ack.status, body
            )));
        }

        Ok(ack)
    }

    async fn submit_bn_market_with_opts(
        &self,
        symbol: &str,
        side: OrderSide,
        qty: Decimal,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<FillEvent, ExecutionError> {
        let timestamp = Self::bn_timestamp_ms();
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let clean_symbol = symbol.to_uppercase();
        let qty_str = format_hl_qty(qty, 2);

        // Build query string in sorted deterministic order
        let query_string = if reduce_only {
            format!(
                "newClientOrderId={}&newOrderRespType=RESULT&quantity={}&recvWindow=5000&reduceOnly=true&side={}&symbol={}&timestamp={}&type=MARKET",
                client_order_id, qty_str, side_str, clean_symbol, timestamp
            )
        } else {
            format!(
                "newClientOrderId={}&newOrderRespType=RESULT&quantity={}&recvWindow=5000&side={}&symbol={}&timestamp={}&type=MARKET",
                client_order_id, qty_str, side_str, clean_symbol, timestamp
            )
        };

        let url = self.bn_signed_url("/fapi/v1/order", &query_string)?;

        debug!(
            symbol = symbol,
            side = side_str,
            qty = %qty_str,
            reduce_only = reduce_only,
            cloid = client_order_id,
            "Posting market order to Binance USD-M"
        );

        let resp = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.config.bn_api_key)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance network error: {}", e)))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExecutionError::Network(format!("Read Binance response body failed: {}", e)))?;

        if !status.is_success() {
            return Err(Self::parse_bn_error(status, &body));
        }

        let resp_json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| ExecutionError::Other(format!("Parse Binance json failed: {}, body: {}", e, body)))?;

        let order_status = resp_json.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if order_status != "FILLED" && order_status != "NEW" && order_status != "PARTIALLY_FILLED" {
            return Err(ExecutionError::Rejected(format!(
                "Binance unexpected order status: {}, body: {}",
                order_status, body
            )));
        }

        let executed_qty: Decimal = resp_json
            .get("executedQty")
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(qty);

        let avg_price: Decimal = resp_json
            .get("avgPrice")
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse().ok())
            .filter(|p| *p > Decimal::ZERO)
            .or_else(|| {
                let cum_quote: Decimal = resp_json
                    .get("cumQuote")
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse().ok())?;
                if executed_qty > Decimal::ZERO {
                    Some(cum_quote / executed_qty)
                } else {
                    None
                }
            })
            .unwrap_or(Decimal::ZERO);

        let final_avg_price = if avg_price.is_zero() {
            let ticker_url = format!("{}/fapi/v1/ticker/bookTicker?symbol={}", self.config.bn_rest_url, clean_symbol);
            if let Ok(ticker_resp) = self.http.get(&ticker_url).send().await {
                if let Ok(ticker_json) = ticker_resp.json::<serde_json::Value>().await {
                    let field = if side == OrderSide::Buy { "askPrice" } else { "bidPrice" };
                    ticker_json.get(field).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                }
            } else {
                Decimal::ZERO
            }
        } else {
            avg_price
        };

        let order_id = resp_json.get("orderId").and_then(|o| o.as_i64()).unwrap_or(0);
        let notional = final_avg_price * executed_qty;
        let fee = notional * Decimal::new(4, 4); // 0.04% (4 bps) taker fee

        info!(
            symbol = symbol,
            side = side_str,
            qty = %executed_qty,
            avg_price = %final_avg_price,
            order_id = order_id,
            "Binance LIVE market hedge filled!"
        );

        Ok(FillEvent {
            fill_id: format!("bn_{}_{}", order_id, unix_now_ns()),
            client_order_id: client_order_id.to_string(),
            venue: VenueId::BinanceFutures,
            symbol: symbol.to_string(),
            side,
            price: final_avg_price,
            qty: executed_qty,
            fee,
            fee_asset: "USDT".to_string(),
            is_maker: false,
            timestamp_ns: unix_now_ns(),
        })
    }

    async fn cancel_order(
        &self,
        venue: VenueId,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<(), ExecutionError> {
        match venue {
            VenueId::BinanceFutures => {
                let timestamp = Self::bn_timestamp_ms();
                let query_string = format!(
                    "origClientOrderId={}&recvWindow=5000&symbol={}&timestamp={}",
                    client_order_id, symbol.to_uppercase(), timestamp
                );
                let url = self.bn_signed_url("/fapi/v1/order", &query_string)?;

                let resp = self
                    .http
                    .delete(&url)
                    .header("X-MBX-APIKEY", &self.config.bn_api_key)
                    .send()
                    .await
                    .map_err(|e| ExecutionError::Network(format!("Binance cancel network error: {}", e)))?;

                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(Self::parse_bn_error(status, &text));
                }
                Ok(())
            }
            VenueId::Hyperliquid => Ok(()),
        }
    }

    async fn cancel_bn_all_open_orders(&self, symbol: &str) -> Result<(), ExecutionError> {
        let timestamp = Self::bn_timestamp_ms();
        let query_string = format!(
            "recvWindow=5000&symbol={}&timestamp={}",
            symbol.to_uppercase(),
            timestamp
        );
        let url = self.bn_signed_url("/fapi/v1/allOpenOrders", &query_string)?;

        let resp = self
            .http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.config.bn_api_key)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance cancelAll network error: {}", e)))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Self::parse_bn_error(status, &text));
        }
        Ok(())
    }

    async fn query_bn_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<BnOrderAck, ExecutionError> {
        let timestamp = Self::bn_timestamp_ms();
        let query_string = format!(
            "origClientOrderId={}&recvWindow=5000&symbol={}&timestamp={}",
            client_order_id,
            symbol.to_uppercase(),
            timestamp
        );
        let url = self.bn_signed_url("/fapi/v1/order", &query_string)?;

        let resp = self
            .http
            .get(&url)
            .header("X-MBX-APIKEY", &self.config.bn_api_key)
            .send()
            .await
            .map_err(|e| ExecutionError::Network(format!("Binance query order error: {}", e)))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExecutionError::Network(format!("Read Binance order query body failed: {}", e)))?;

        if !status.is_success() {
            return Err(Self::parse_bn_error(status, &body));
        }

        let resp_json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| ExecutionError::Other(format!("Parse Binance order query json failed: {}", e)))?;

        Ok(Self::parse_bn_order_ack(&resp_json))
    }

    async fn query_position(&self, venue: VenueId, symbol: &str) -> Result<Decimal, ExecutionError> {
        match venue {
            VenueId::BinanceFutures => {
                let timestamp = Self::bn_timestamp_ms();
                let query_string = format!(
                    "recvWindow=5000&symbol={}&timestamp={}",
                    symbol.to_uppercase(), timestamp
                );
                let url = self.bn_signed_url("/fapi/v2/positionRisk", &query_string)?;

                let resp = self
                    .http
                    .get(&url)
                    .header("X-MBX-APIKEY", &self.config.bn_api_key)
                    .send()
                    .await
                    .map_err(|e| ExecutionError::Network(format!("Binance positionRisk error: {}", e)))?;

                let json: Vec<serde_json::Value> = resp.json().await.map_err(|e| ExecutionError::Other(e.to_string()))?;
                for p in json {
                    if p.get("symbol").and_then(|s| s.as_str()) == Some(&symbol.to_uppercase()) {
                        let pos_amt: Decimal = p.get("positionAmt").and_then(|s| s.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
                        return Ok(pos_amt);
                    }
                }
                Ok(Decimal::ZERO)
            }
            VenueId::Hyperliquid => {
                let master_wallet = self.resolve_hl_master_wallet().await?;
                let dex = if !self.config.hl_dex.is_empty() {
                    self.config.hl_dex.as_str()
                } else {
                    symbol.split_once(':').map(|(p, _)| p).unwrap_or("")
                };

                let mut req_body = serde_json::json!({
                    "type": "clearinghouseState",
                    "user": master_wallet,
                });
                if !dex.is_empty() {
                    req_body["dex"] = serde_json::Value::String(dex.to_string());
                }

                let resp = self
                    .http
                    .post(format!("{}/info", self.config.hl_rest_url))
                    .json(&req_body)
                    .send()
                    .await
                    .map_err(|e| ExecutionError::Network(format!("HL clearinghouseState error: {}", e)))?;

                let json: serde_json::Value = resp.json().await.map_err(|e| ExecutionError::Other(e.to_string()))?;
                let clean_sym = symbol.split(':').last().unwrap_or(symbol);
                if let Some(positions) = json.get("assetPositions").and_then(|a| a.as_array()) {
                    for pos in positions {
                        if let Some(pos_obj) = pos.get("position") {
                            let coin = pos_obj.get("coin").and_then(|c| c.as_str()).unwrap_or("");
                            if coin == symbol || coin == clean_sym {
                                let szo: Decimal = pos_obj.get("szi").and_then(|s| s.as_str()).and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO);
                                return Ok(szo);
                            }
                        }
                    }
                }
                Ok(Decimal::ZERO)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_format_hl_price_and_qty() {
        assert_eq!(format_hl_price(dec!(127.654321), 2), "127.65");
        assert_eq!(format_hl_price(dec!(12.3456), 2), "12.346");
        assert_eq!(format_hl_price(dec!(10.02), 1), "10.02");
        assert_eq!(format_hl_qty(dec!(2.900), 1), "2.9");
    }

    #[test]
    fn test_hl_signing_deterministic() {
        // Test signing key from hex
        let key_bytes = hex::decode("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
        let signing_key = SigningKey::from_slice(&key_bytes).unwrap();

        let action = HlOrderAction {
            action_type: "order",
            orders: vec![HlOrderWire {
                a: 110099,
                b: true,
                p: "127.5",
                s: "1",
                r: false,
                t: HlOrderTypeWire {
                    limit: HlLimitOrderWire { tif: "Ioc" },
                },
                c: Some("0x0123456789abcdef0123456789abcdef"),
            }],
            grouping: "na",
        };

        let conn_id = build_l1_action_hash(&action, 1700000000000).unwrap();
        let sig = sign_phantom_agent(&signing_key, &conn_id, true).unwrap();

        assert!(sig.r.starts_with("0x"));
        assert!(sig.s.starts_with("0x"));
        assert!(sig.v == 27 || sig.v == 28);
    }

    #[test]
    fn test_parse_hl_order_response_variants() {
        let filled_json = serde_json::json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{
                        "filled": {
                            "totalSz": "0.1",
                            "avgPx": "104.25",
                            "oid": 612183889731i64
                        }
                    }]
                }
            }
        });
        let fill = parse_hl_order_response(
            &filled_json,
            "xyz:SKHY",
            OrderSide::Buy,
            dec!(0.1),
            dec!(104.35),
            "cl_1",
        )
        .unwrap();
        assert_eq!(fill.qty, dec!(0.1));
        assert_eq!(fill.price, dec!(104.25));
        // 104.25 * 0.1 * 0.000086 = 0.00089655 USDC (0.86 bps)
        assert_eq!(fill.fee, dec!(0.00089655));

        let zero_fill_json = serde_json::json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{
                        "error": "Order could not immediately match against any resting orders."
                    }]
                }
            }
        });
        assert!(matches!(
            parse_hl_order_response(
                &zero_fill_json,
                "xyz:SKHY",
                OrderSide::Buy,
                dec!(0.1),
                dec!(104.25),
                "cl_2",
            ),
            Err(ExecutionError::ZeroFill)
        ));

        let rejected_json = serde_json::json!({
            "status": "err",
            "response": "Insufficient margin"
        });
        assert!(matches!(
            parse_hl_order_response(
                &rejected_json,
                "xyz:SKHY",
                OrderSide::Buy,
                dec!(0.1),
                dec!(104.25),
                "cl_3",
            ),
            Err(ExecutionError::Rejected(_))
        ));
    }

    #[tokio::test]
    async fn test_hl_ioc_pre_signed_cache() {
        let config = LiveExecutorConfig {
            bn_api_key: "dummy".into(),
            bn_api_secret: "dummy".into(),
            bn_rest_url: "http://dummy".into(),
            bn_ws_url: "ws://dummy".into(),
            hl_private_key: "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            hl_wallet: "0x54ca605d37b1ef9ff7b131239a54e78fa2b24dc9".into(),
            hl_rest_url: "http://dummy".into(),
            hl_ws_url: "ws://dummy".into(),
            hl_is_mainnet: true,
            hl_dex: "xyz".into(),
        };
        let executor = LiveExecutor::new(config).unwrap();
        *executor.hl_asset_info.write() = Some((110099, 2));

        // 1. Prepare cache for Buy 0.1 at 107.50
        executor
            .prepare_hl_ioc_cache("xyz:SKHY", OrderSide::Buy, dec!(0.1), dec!(107.50), false)
            .await;

        let cache = executor.cached_ioc.read().clone().unwrap();
        assert_eq!(cache.side, OrderSide::Buy);
        assert_eq!(cache.qty, dec!(0.1));
        assert_eq!(cache.limit_price, dec!(107.50));
        assert!(!cache.reduce_only);
        assert!(cache.payload.get("signature").is_some());

        // 2. Calling prepare again with same price and qty should be a no-op (keep same nonce)
        let old_nonce = cache.nonce;
        executor
            .prepare_hl_ioc_cache("xyz:SKHY", OrderSide::Buy, dec!(0.1), dec!(107.50), false)
            .await;
        assert_eq!(executor.cached_ioc.read().as_ref().unwrap().nonce, old_nonce);

        // 3. Price change updates the cached order with fresh nonce
        executor
            .prepare_hl_ioc_cache("xyz:SKHY", OrderSide::Buy, dec!(0.1), dec!(107.55), false)
            .await;
        let new_cache = executor.cached_ioc.read().clone().unwrap();
        assert_eq!(new_cache.limit_price, dec!(107.55));
        assert!(new_cache.nonce > old_nonce);
    }
}
