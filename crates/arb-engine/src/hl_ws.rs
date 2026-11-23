use crate::executor::ExecutionError;
use futures_util::{SinkExt, StreamExt};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{debug, info, warn};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const PONG_WATCHDOG_INTERVAL: Duration = Duration::from_secs(2);
const PONG_TIMEOUT: Duration = Duration::from_secs(6);

/// Parsed inbound message from the Hyperliquid trade WebSocket.
#[derive(Debug, Clone, PartialEq)]
pub enum HlWsInbound {
    Pong,
    PostAction {
        id: u64,
        payload: serde_json::Value,
    },
    PostError {
        id: u64,
        message: String,
    },
    Other,
}

/// Build the Hyperliquid WebSocket `post` action envelope wrapping a signed `/exchange` payload.
pub fn build_ws_post_action_envelope(id: u64, payload: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "method": "post",
        "id": id,
        "request": {
            "type": "action",
            "payload": payload,
        }
    })
}

/// Parse a raw JSON text frame received from the Hyperliquid trade WebSocket.
pub fn parse_ws_post_message(txt: &str) -> Result<HlWsInbound, serde_json::Error> {
    let val: serde_json::Value = serde_json::from_str(txt)?;
    let channel = val.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    match channel {
        "pong" => Ok(HlWsInbound::Pong),
        "post" => {
            let Some(data) = val.get("data") else {
                return Ok(HlWsInbound::Other);
            };
            let Some(id) = data.get("id").and_then(|v| v.as_u64()) else {
                return Ok(HlWsInbound::Other);
            };
            let Some(resp) = data.get("response") else {
                return Ok(HlWsInbound::Other);
            };
            let resp_type = resp.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match resp_type {
                "action" => {
                    let payload = resp
                        .get("payload")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    Ok(HlWsInbound::PostAction { id, payload })
                }
                "error" => {
                    let message = resp
                        .get("payload")
                        .and_then(|p| p.as_str().map(str::to_string).or_else(|| Some(p.to_string())))
                        .unwrap_or_else(|| "Unknown HL WS post error".to_string());
                    Ok(HlWsInbound::PostError { id, message })
                }
                _ => Ok(HlWsInbound::Other),
            }
        }
        _ => Ok(HlWsInbound::Other),
    }
}

type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<serde_json::Value, ExecutionError>>>>>;

/// Persistent Hyperliquid WebSocket client dedicated to `method="post"` order submission
/// with 20s JSON ping/pong keepalive and automatic reconnect/backoff.
#[derive(Clone)]
pub struct HlWsTradeClient {
    ws_url: String,
    healthy: Arc<AtomicBool>,
    next_id: Arc<AtomicU64>,
    out_tx: Arc<RwLock<Option<mpsc::UnboundedSender<String>>>>,
    pending: PendingMap,
}

impl HlWsTradeClient {
    /// Spawn the background WebSocket supervisor task and return a shareable handle.
    pub fn spawn(ws_url: String, health_tx: Option<mpsc::Sender<bool>>) -> Self {
        let client = Self {
            ws_url: ws_url.clone(),
            healthy: Arc::new(AtomicBool::new(false)),
            next_id: Arc::new(AtomicU64::new(1)),
            out_tx: Arc::new(RwLock::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
        };

        let bg = client.clone();
        tokio::spawn(async move {
            bg.run_supervisor(health_tx).await;
        });

        client
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire) && self.out_tx.read().is_some()
    }

    /// Submit a signed `/exchange` payload over the persistent WebSocket and await its matched reply.
    pub async fn post_action(
        &self,
        payload: serde_json::Value,
        timeout_ms: u64,
    ) -> Result<serde_json::Value, ExecutionError> {
        if !self.is_healthy() {
            return Err(ExecutionError::Network(
                "HL trade WS is not connected".to_string(),
            ));
        }

        let tx = self.out_tx.read().clone().ok_or_else(|| {
            ExecutionError::Network("HL trade WS sender unavailable".to_string())
        })?;

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let envelope = build_ws_post_action_envelope(id, payload);
        let frame = serde_json::to_string(&envelope)
            .map_err(|e| ExecutionError::Other(format!("Serialize HL WS envelope failed: {}", e)))?;

        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending.lock().insert(id, resp_tx);

        if tx.send(frame).is_err() {
            self.pending.lock().remove(&id);
            return Err(ExecutionError::Network(
                "HL trade WS outbound channel closed".to_string(),
            ));
        }

        match tokio::time::timeout(Duration::from_millis(timeout_ms), resp_rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(ExecutionError::Network(
                "HL trade WS response channel dropped".to_string(),
            )),
            Err(_) => {
                self.pending.lock().remove(&id);
                Err(ExecutionError::Timeout(format!(
                    "HL WS post id={} timed out after {}ms",
                    id, timeout_ms
                )))
            }
        }
    }

    async fn run_supervisor(&self, health_tx: Option<mpsc::Sender<bool>>) {
        let backoffs_ms = [200u64, 500, 1000, 2000, 5000];
        let mut attempt = 0usize;

        loop {
            info!(url = %self.ws_url, "Connecting Hyperliquid trade WebSocket...");
            let connected_at = Instant::now();

            match connect_async_tls_with_config(self.ws_url.as_str(), None, true, None).await {
                Ok((ws_stream, _)) => {
                    info!(url = %self.ws_url, "Hyperliquid trade WebSocket connected (TCP_NODELAY=true)");
                    let (mut ws_sink, mut ws_rx) = ws_stream.split();
                    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

                    *self.out_tx.write() = Some(out_tx);
                    self.healthy.store(true, Ordering::Release);
                    if let Some(ref htx) = health_tx {
                        let _ = htx.send(true).await;
                    }

                    let mut ping_interval = tokio::time::interval(HEARTBEAT_INTERVAL);
                    ping_interval.tick().await;
                    let mut watchdog_interval = tokio::time::interval(PONG_WATCHDOG_INTERVAL);
                    watchdog_interval.tick().await;

                    let mut awaiting_pong = false;
                    let mut last_ping = Instant::now();
                    let mut pong_count: u64 = 0;

                    loop {
                        tokio::select! {
                            maybe_out = out_rx.recv() => {
                                match maybe_out {
                                    Some(txt) => {
                                        if let Err(e) = ws_sink.send(Message::Text(txt)).await {
                                            warn!(error = %e, "HL trade WS send error");
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                            maybe_in = ws_rx.next() => {
                                match maybe_in {
                                    Some(Ok(Message::Text(txt))) => {
                                        match parse_ws_post_message(&txt) {
                                            Ok(HlWsInbound::Pong) => {
                                                awaiting_pong = false;
                                                pong_count = pong_count.wrapping_add(1);
                                                if pong_count % 3 == 0 {
                                                    info!(
                                                        pongs = pong_count,
                                                        "Hyperliquid trade WS keepalive active: received {{\"channel\":\"pong\"}} (20s heartbeat OK)"
                                                    );
                                                } else {
                                                    debug!(pongs = pong_count, "HL trade WS pong received");
                                                }
                                            }
                                            Ok(HlWsInbound::PostAction { id, payload }) => {
                                                if let Some(waiter) = self.pending.lock().remove(&id) {
                                                    let _ = waiter.send(Ok(payload));
                                                }
                                            }
                                            Ok(HlWsInbound::PostError { id, message }) => {
                                                if let Some(waiter) = self.pending.lock().remove(&id) {
                                                    let _ = waiter.send(Err(ExecutionError::Rejected(message)));
                                                }
                                            }
                                            Ok(HlWsInbound::Other) => {}
                                            Err(e) => {
                                                debug!(error = %e, "Ignoring unparseable HL trade WS frame");
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        if let Err(e) = ws_sink.send(Message::Pong(p)).await {
                                            warn!(error = %e, "HL trade WS pong reply failed");
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        warn!(close = ?frame, "HL trade WS closed by server");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        warn!(error = %e, "HL trade WS stream error");
                                        break;
                                    }
                                    None => {
                                        warn!("HL trade WS stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            _ = ping_interval.tick() => {
                                if awaiting_pong && last_ping.elapsed() > PONG_TIMEOUT {
                                    warn!("HL trade WS heartbeat pong missing on ping tick; reconnecting");
                                    break;
                                }
                                if let Err(e) = ws_sink.send(Message::Text(r#"{"method":"ping"}"#.to_string())).await {
                                    warn!(error = %e, "HL trade WS ping send error");
                                    break;
                                }
                                awaiting_pong = true;
                                last_ping = Instant::now();
                            }
                            _ = watchdog_interval.tick() => {
                                if awaiting_pong && last_ping.elapsed() > PONG_TIMEOUT {
                                    warn!("HL trade WS pong timeout (>6s); forcing reconnect");
                                    break;
                                }
                            }
                        }
                    }

                    // Reset backoff if connection stayed up long enough
                    if connected_at.elapsed() >= Duration::from_secs(10) {
                        attempt = 0;
                    }
                }
                Err(e) => {
                    warn!(url = %self.ws_url, error = %e, "Failed to connect HL trade WebSocket");
                }
            }

            // Cleanup on disconnect
            self.healthy.store(false, Ordering::Release);
            *self.out_tx.write() = None;
            {
                let mut map = self.pending.lock();
                for (_, waiter) in map.drain() {
                    let _ = waiter.send(Err(ExecutionError::Timeout(
                        "HL trade WS disconnected while request in flight".to_string(),
                    )));
                }
            }
            if let Some(ref htx) = health_tx {
                let _ = htx.send(false).await;
            }

            let delay_ms = backoffs_ms[attempt.min(backoffs_ms.len() - 1)];
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_ws_post_action_envelope() {
        let payload = serde_json::json!({
            "action": { "type": "order", "grouping": "na" },
            "nonce": 1700000000000u64,
            "signature": { "r": "0x1", "s": "0x2", "v": 27 }
        });
        let env = build_ws_post_action_envelope(42, payload.clone());
        assert_eq!(env["method"], "post");
        assert_eq!(env["id"], 42);
        assert_eq!(env["request"]["type"], "action");
        assert_eq!(env["request"]["payload"], payload);
    }

    #[test]
    fn test_parse_ws_post_messages() {
        let pong = r#"{"channel":"pong"}"#;
        assert_eq!(parse_ws_post_message(pong).unwrap(), HlWsInbound::Pong);

        let action_resp = r#"{
            "channel": "post",
            "data": {
                "id": 7,
                "response": {
                    "type": "action",
                    "payload": {
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": [{ "filled": { "totalSz": "0.1", "avgPx": "104.25", "oid": 999 } }] } }
                    }
                }
            }
        }"#;
        match parse_ws_post_message(action_resp).unwrap() {
            HlWsInbound::PostAction { id, payload } => {
                assert_eq!(id, 7);
                assert_eq!(payload["status"], "ok");
            }
            other => panic!("Unexpected parsed message: {:?}", other),
        }

        let err_resp = r#"{
            "channel": "post",
            "data": {
                "id": 8,
                "response": {
                    "type": "error",
                    "payload": "Invalid nonce"
                }
            }
        }"#;
        assert_eq!(
            parse_ws_post_message(err_resp).unwrap(),
            HlWsInbound::PostError {
                id: 8,
                message: "Invalid nonce".to_string()
            }
        );
    }
}
