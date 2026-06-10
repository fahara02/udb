//! Pluggable ws:// signalling bridge (feature `ws-signalling`).
//!
//! Some real-time clients/engines cannot speak UDB's gRPC `SignalingService`
//! (e.g. Unreal Engine Pixel Streaming, whose streamer plugin and browser
//! frontend are hard-wired to a JSON-over-WebSocket protocol). This module hosts
//! a `ws://` server that speaks those external protocols so they can still
//! negotiate WebRTC through UDB and reuse UDB's TURN credential minting.
//!
//! Extensibility is the point: a protocol is a [`SignalingProtocol`]
//! implementation. Pixel Streaming is the first ([`PixelStreamingProtocol`]);
//! adding another (e.g. a custom JSON or Socket.IO dialect) is a new trait impl
//! plus a match arm in [`SignalingServer::from_env`] — no server rewrite.
//!
//! Runtime activation: build with `--features ws-signalling`, then set
//! `UDB_WS_SIGNALLING_ADDR` (e.g. `0.0.0.0:8888`). ICE/TURN servers advertised to
//! clients come from the same `UDB_TURN_URLS` / `UDB_TURN_SECRET` env the native
//! WebRTC `TurnService` uses, so one TURN deployment serves both.
//!
//! Isolation model: this optional bridge is deployment-scoped. Its Pixel
//! Streaming and JSON relay protocols do not receive a validated UDB tenant or
//! project context during the WebSocket handshake, so one bridge instance should
//! be exposed for one tenant/room trust boundary (or placed behind an external
//! gateway that performs that split). `UDB_WS_SIGNALLING_TOKEN` is admission
//! control for that deployment, not a multi-tenant authorization token.

mod json_relay;
mod pixel_streaming;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::net::TcpStream;

pub use json_relay::JsonRelayProtocol;
pub use pixel_streaming::PixelStreamingProtocol;

/// One accepted, upgraded WebSocket connection handed to a protocol; the protocol
/// owns it (read + write) for the connection's lifetime.
pub type WsStream = tokio_tungstenite::WebSocketStream<TcpStream>;

/// A pluggable real-time signalling protocol served over `ws://`. Implementations
/// hold whatever cross-connection state they need (e.g. a streamer/player
/// registry) behind `Arc`/interior mutability.
#[async_trait]
pub trait SignalingProtocol: Send + Sync {
    /// Stable protocol id, used in config selection and logs (e.g. `pixelstreaming`).
    fn id(&self) -> &'static str;
    /// Drive one connection to completion.
    async fn handle(&self, ws: WsStream, peer: SocketAddr);
}

/// ICE/TURN servers advertised to clients during a protocol handshake. Read from
/// the same env as the native WebRTC service; when a TURN secret is present, each
/// handshake mints a fresh short-lived coturn REST credential.
#[derive(Clone, Debug)]
pub struct IceConfig {
    /// `stun:`/`turn:` URLs (mixed; split by scheme when building the payload).
    urls: Vec<String>,
    /// coturn shared secret; `None` → only STUN advertised (no credentials).
    turn_secret: Option<Vec<u8>>,
    /// Credential lifetime in seconds.
    ttl: i64,
}

impl IceConfig {
    pub fn from_env() -> Self {
        let urls = std::env::var("UDB_TURN_URLS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.split(',')
                    .map(|u| u.trim().to_string())
                    .filter(|u| !u.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec!["stun:stun.l.google.com:19302".to_string()]);
        // Shared resolver: fails closed in production (STUN-only) when no secret
        // is configured, rather than leaking a dev secret into ICE handshakes.
        let turn_secret = crate::runtime::security::resolve_turn_secret();
        let ttl = std::env::var("UDB_TURN_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(86_400);
        Self {
            urls,
            turn_secret,
            ttl,
        }
    }

    /// Build the `iceServers` JSON array for a protocol handshake, minting a fresh
    /// TURN credential for `principal` when a secret is configured. STUN URLs are
    /// advertised credential-free; TURN URLs carry the minted username/credential.
    pub fn ice_servers_json(&self, principal: &str, now_unix: i64) -> serde_json::Value {
        let mut servers = Vec::new();
        let stun: Vec<&String> = self.urls.iter().filter(|u| u.starts_with("stun")).collect();
        let turn: Vec<&String> = self.urls.iter().filter(|u| u.starts_with("turn")).collect();
        if !stun.is_empty() {
            servers.push(json!({ "urls": stun }));
        }
        match (&self.turn_secret, turn.is_empty()) {
            (Some(secret), false) => {
                let expiry = now_unix + self.ttl;
                let (username, credential) =
                    crate::runtime::security::turn_rest_credential(secret, principal, expiry);
                servers.push(json!({
                    "urls": turn,
                    "username": username,
                    "credential": credential,
                }));
            }
            (_, false) => servers.push(json!({ "urls": turn })),
            _ => {}
        }
        json!(servers)
    }
}

/// The ws:// signalling server: binds an address and dispatches each accepted
/// connection to the configured [`SignalingProtocol`].
pub struct SignalingServer {
    addr: SocketAddr,
    protocol: Arc<dyn SignalingProtocol>,
}

impl SignalingServer {
    /// Construct from the environment, or `None` when not configured
    /// (`UDB_WS_SIGNALLING_ADDR` unset/invalid). `UDB_WS_SIGNALLING_PROTOCOL`
    /// selects the protocol (default `pixelstreaming`).
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("UDB_WS_SIGNALLING_ADDR").ok()?;
        let addr: SocketAddr = match raw.trim().parse() {
            Ok(addr) => addr,
            Err(err) => {
                tracing::warn!(value = %raw, error = %err, "invalid UDB_WS_SIGNALLING_ADDR; ws signalling disabled");
                return None;
            }
        };
        let ice = IceConfig::from_env();
        let which = std::env::var("UDB_WS_SIGNALLING_PROTOCOL")
            .unwrap_or_else(|_| "pixelstreaming".to_string());
        let protocol: Arc<dyn SignalingProtocol> = match which.trim().to_ascii_lowercase().as_str()
        {
            "pixelstreaming" | "ps" | "" => Arc::new(PixelStreamingProtocol::new(ice)),
            "json-relay" | "relay" => Arc::new(JsonRelayProtocol::new()),
            other => {
                tracing::warn!(
                    protocol = %other,
                    "unknown UDB_WS_SIGNALLING_PROTOCOL; defaulting to pixelstreaming"
                );
                Arc::new(PixelStreamingProtocol::new(ice))
            }
        };
        Some(Self { addr, protocol })
    }

    /// Accept loop: one task per connection. Runs until the process exits.
    pub async fn serve(self) -> std::io::Result<()> {
        self.serve_with_shutdown(std::future::pending::<()>()).await
    }

    /// Accept loop: one task per connection, stopping when `shutdown` resolves.
    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> std::io::Result<()>
    where
        F: Future<Output = ()> + Send,
    {
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        // Optional shared-secret admission: when `UDB_WS_SIGNALLING_TOKEN` is set,
        // a client must present it as `?token=…` (or the `Sec-WebSocket-Protocol`)
        // or the handshake is rejected with 401. This keeps a deployment-scoped
        // bridge from being open to the public internet; it is not per-tenant
        // routing or authorization.
        let token = std::env::var("UDB_WS_SIGNALLING_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty());
        tracing::info!(
            addr = %self.addr,
            protocol = self.protocol.id(),
            admission = if token.is_some() { "token-required" } else { "open" },
            "ws signalling bridge listening"
        );
        let mut shutdown = Box::pin(shutdown);
        loop {
            let accepted = tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!(addr = %self.addr, protocol = self.protocol.id(), "ws signalling bridge shutting down");
                    break;
                }
                accepted = listener.accept() => accepted,
            };
            let (stream, peer) = match accepted {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(error = %err, "ws signalling accept failed");
                    continue;
                }
            };
            let protocol = self.protocol.clone();
            let token = token.clone();
            tokio::spawn(async move {
                let accepted = if let Some(expected) = token {
                    use tokio_tungstenite::tungstenite::handshake::server::{
                        ErrorResponse, Request, Response,
                    };
                    tokio_tungstenite::accept_hdr_async(
                        stream,
                        move |req: &Request, resp: Response| {
                            if request_token(req).as_deref() == Some(expected.as_str()) {
                                Ok(resp)
                            } else {
                                let mut err = ErrorResponse::new(Some(
                                    "missing or invalid signalling token".to_string(),
                                ));
                                *err.status_mut() =
                                    tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                                Err(err)
                            }
                        },
                    )
                    .await
                } else {
                    tokio_tungstenite::accept_async(stream).await
                };
                match accepted {
                    Ok(ws) => protocol.handle(ws, peer).await,
                    Err(err) => tracing::debug!(%peer, error = %err, "ws handshake rejected"),
                }
            });
        }
        Ok(())
    }

    /// Spawn the server as a detached background task if configured. Returns the
    /// handle (or `None` when `UDB_WS_SIGNALLING_ADDR` is unset). A signalling
    /// failure logs but never brings down the broker's data plane.
    pub fn spawn_from_env() -> Option<tokio::task::JoinHandle<()>> {
        Self::spawn_from_env_with_shutdown(std::future::pending::<()>())
    }

    /// Spawn the server with a shutdown signal, mirroring tonic's
    /// `serve_with_shutdown` behavior for the optional bridge.
    pub fn spawn_from_env_with_shutdown<F>(shutdown: F) -> Option<tokio::task::JoinHandle<()>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let server = Self::from_env()?;
        Some(tokio::spawn(async move {
            if let Err(err) = server.serve_with_shutdown(shutdown).await {
                tracing::error!(error = %err, "ws signalling bridge exited");
            }
        }))
    }
}

/// Extract a presented admission token from a ws upgrade request: the `token`
/// query parameter (`?token=…`), else the `Sec-WebSocket-Protocol` header.
fn request_token(
    req: &tokio_tungstenite::tungstenite::handshake::server::Request,
) -> Option<String> {
    if let Some(query) = req.uri().query() {
        for pair in query.split('&') {
            if let Some(v) = pair.strip_prefix("token=") {
                return Some(v.to_string());
            }
        }
    }
    req.headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
}

#[cfg(test)]
mod it_tests {
    //! Live end-to-end integration test of the Pixel Streaming protocol over real
    //! localhost WebSocket sockets: bind a TCP listener, accept connections into a
    //! shared `PixelStreamingProtocol` hub, then drive a streamer + player through
    //! a full register → list → subscribe → offer relay over `connect_async`.

    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message;

    type ClientWs = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    fn test_ice() -> IceConfig {
        IceConfig {
            urls: vec!["stun:stun.example:3478".into()],
            turn_secret: None,
            ttl: 60,
        }
    }

    /// Read client frames until a Text frame parses to JSON, with a timeout so a
    /// missing message fails fast instead of hanging.
    async fn read_json(client: &mut ClientWs) -> serde_json::Value {
        let fut = async {
            while let Some(next) = client.next().await {
                match next.expect("ws read error") {
                    Message::Text(t) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(t.as_str()) {
                            return v;
                        }
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Close(_) => panic!("connection closed before a JSON frame arrived"),
                    _ => {}
                }
            }
            panic!("stream ended before a JSON frame arrived");
        };
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .expect("timed out waiting for a JSON frame")
    }

    /// Read frames until one has the expected `type`, returning it.
    async fn read_until_type(client: &mut ClientWs, want: &str) -> serde_json::Value {
        let fut = async {
            loop {
                let v = read_json(client).await;
                if v.get("type").and_then(|t| t.as_str()) == Some(want) {
                    return v;
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for type={want}"))
    }

    async fn send(client: &mut ClientWs, json: serde_json::Value) {
        client
            .send(Message::text(json.to_string()))
            .await
            .expect("ws send failed");
    }

    #[tokio::test]
    async fn pixel_streaming_end_to_end_over_real_sockets() {
        // 1. Bind a real listener on an ephemeral port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        // 2. Accept loop: every connection shares ONE protocol instance so the
        //    streamer and player land in the same hub.
        let protocol = Arc::new(PixelStreamingProtocol::new(test_ice()));
        let server = tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let protocol = protocol.clone();
                tokio::spawn(async move {
                    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                        protocol.handle(ws, peer).await;
                    }
                });
            }
        });

        let url = format!("ws://{addr}/");

        // 3. Connect two clients.
        let (mut streamer, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("streamer connect");
        let (mut player, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("player connect");

        // 4. Streamer: drain config + identify, register, await confirm.
        assert_eq!(read_json(&mut streamer).await["type"], "config");
        assert_eq!(read_json(&mut streamer).await["type"], "identify");
        send(&mut streamer, json!({ "type": "endpointId", "id": "s1" })).await;
        let confirm = read_until_type(&mut streamer, "endpointIdConfirm").await;
        assert_eq!(confirm["committedId"], "s1");

        // 5. Player: drain config + identify, list streamers (expect "s1"), subscribe.
        assert_eq!(read_json(&mut player).await["type"], "config");
        assert_eq!(read_json(&mut player).await["type"], "identify");
        send(&mut player, json!({ "type": "listStreamers" })).await;
        let list = read_until_type(&mut player, "streamerList").await;
        let ids: Vec<String> = list["ids"]
            .as_array()
            .expect("ids array")
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert!(
            ids.contains(&"s1".to_string()),
            "streamerList missing s1: {ids:?}"
        );
        send(
            &mut player,
            json!({ "type": "subscribe", "streamerId": "s1" }),
        )
        .await;

        // 6. Streamer learns of the player and sends it an offer.
        let connected = read_until_type(&mut streamer, "playerConnected").await;
        let player_id = connected["playerId"]
            .as_str()
            .expect("playerId")
            .to_string();
        send(
            &mut streamer,
            json!({ "type": "offer", "sdp": "X", "playerId": player_id }),
        )
        .await;

        // 7. Player receives the relayed offer.
        let offer = read_until_type(&mut player, "offer").await;
        assert_eq!(offer["type"], "offer");
        assert_eq!(offer["sdp"], "X");

        // Clean up.
        let _ = streamer.close(None).await;
        let _ = player.close(None).await;
        server.abort();
    }
}
