//! Unreal Engine **Pixel Streaming** signalling protocol over `ws://`.
//!
//! Implements the streamer ⇄ signalling-server ⇄ player message router defined by
//! Epic's Pixel Streaming Infrastructure (the protocol Epic's reference "Wilbur"
//! server speaks). UDB acts as the intermediary that brokers SDP/ICE — it never
//! touches media (media is direct UE↔browser WebRTC). Routing fields (`type`,
//! `playerId`, `streamerId`, `id`) are read; the rest of each JSON message is
//! forwarded verbatim, which keeps the router resilient across UE versions.
//!
//! Conformance: the message set matches Epic's documented signalling protocol;
//! the routing state machine is unit-tested below. Full end-to-end conformance is
//! validated by running Epic's `Extras/SS_Test` protocol tester against a live
//! endpoint (requires Node + the PixelStreamingInfrastructure repo) — a
//! deployment-time check, not a build-time one.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::{IceConfig, SignalingProtocol, WsStream};

fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// What a connection turned out to be. Every connection starts as an
/// (unsubscribed) player; a `endpointId` message promotes it to a streamer.
#[derive(Clone)]
enum Role {
    Streamer(String),
    Player {
        id: String,
        subscribed: Option<String>,
    },
}

/// Bounded per-connection outbound queue. A slow/blocked socket drops frames once
/// full instead of growing an unbounded backlog (backpressure, no memory blowup).
const OUTBOUND_CAP: usize = 256;

#[derive(Default)]
struct HubState {
    /// conn_id → outbound sender (to that socket's writer task).
    senders: HashMap<u64, mpsc::Sender<Message>>,
    /// conn_id → role.
    roles: HashMap<u64, Role>,
    /// streamerId → conn_id.
    streamers: HashMap<String, u64>,
}

fn send_json(state: &HubState, conn_id: u64, value: Value) {
    if let Some(tx) = state.senders.get(&conn_id) {
        // Drop on a full queue rather than block the shared hub lock.
        let _ = tx.try_send(Message::text(value.to_string()));
    }
}

/// Shared cross-connection registry + router for one deployment-scoped Pixel
/// Streaming endpoint. There is no tenant-aware join context in this WebSocket
/// protocol path, so all registered streamers and players in one hub share the
/// same trust boundary.
struct PsHub {
    ice: IceConfig,
    next_id: AtomicU64,
    state: Mutex<HubState>,
}

impl PsHub {
    fn new(ice: IceConfig) -> Self {
        Self {
            ice,
            next_id: AtomicU64::new(1),
            state: Mutex::new(HubState::default()),
        }
    }

    /// Register a connection, send the PS `config` (with freshly-minted ICE/TURN
    /// servers) + `identify` handshake, and return its id.
    fn add_connection(&self, tx: mpsc::Sender<Message>) -> u64 {
        let conn_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut st = self.state.lock().unwrap();
        st.senders.insert(conn_id, tx);
        st.roles.insert(
            conn_id,
            Role::Player {
                id: conn_id.to_string(),
                subscribed: None,
            },
        );
        let ice = self.ice.ice_servers_json(&conn_id.to_string(), now_unix());
        send_json(
            &st,
            conn_id,
            json!({ "type": "config", "peerConnectionOptions": { "iceServers": ice } }),
        );
        send_json(&st, conn_id, json!({ "type": "identify" }));
        conn_id
    }

    /// Route one inbound text message. Unknown/ignored types (stats, pong,
    /// layerPreference, …) are dropped.
    fn on_message(&self, conn_id: u64, text: &str) {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let mtype = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let mut st = self.state.lock().unwrap();
        match mtype {
            // Streamer registers itself.
            "endpointId" => {
                let id = v
                    .get("id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                if id.is_empty() {
                    return;
                }
                // Reject a duplicate streamer id held by another live connection —
                // a socket must not hijack/overwrite an existing streamer.
                if let Some(&existing) = st.streamers.get(&id)
                    && existing != conn_id
                {
                    send_json(
                        &st,
                        conn_id,
                        json!({ "type": "error", "message": "streamer id already in use" }),
                    );
                    return;
                }
                st.streamers.insert(id.clone(), conn_id);
                st.roles.insert(conn_id, Role::Streamer(id.clone()));
                send_json(
                    &st,
                    conn_id,
                    json!({ "type": "endpointIdConfirm", "committedId": id }),
                );
            }
            // Player asks for available streamers.
            "listStreamers" => {
                let ids: Vec<String> = st.streamers.keys().cloned().collect();
                send_json(&st, conn_id, json!({ "type": "streamerList", "ids": ids }));
            }
            // Player attaches to a streamer.
            "subscribe" => {
                let sid = v
                    .get("streamerId")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Some(&streamer_conn) = st.streamers.get(&sid) {
                    let player_id = conn_id.to_string();
                    st.roles.insert(
                        conn_id,
                        Role::Player {
                            id: player_id.clone(),
                            subscribed: Some(sid),
                        },
                    );
                    send_json(
                        &st,
                        streamer_conn,
                        json!({
                            "type": "playerConnected",
                            "playerId": player_id,
                            "dataChannel": true,
                            "sfu": false,
                        }),
                    );
                } else {
                    let ids: Vec<String> = st.streamers.keys().cloned().collect();
                    send_json(&st, conn_id, json!({ "type": "streamerList", "ids": ids }));
                }
            }
            "unsubscribe" => {
                if let Some(Role::Player { id, subscribed }) = st.roles.get(&conn_id).cloned() {
                    if let Some(sid) = subscribed.as_ref() {
                        if let Some(&sc) = st.streamers.get(sid) {
                            send_json(
                                &st,
                                sc,
                                json!({ "type": "playerDisconnected", "playerId": id }),
                            );
                        }
                    }
                    st.roles.insert(
                        conn_id,
                        Role::Player {
                            id,
                            subscribed: None,
                        },
                    );
                }
            }
            // SDP/ICE relay. Player→streamer messages are tagged with playerId;
            // streamer→player messages carry the target playerId already.
            "offer" | "answer" | "iceCandidate" => match st.roles.get(&conn_id).cloned() {
                Some(Role::Player {
                    id,
                    subscribed: Some(sid),
                }) => {
                    if let Some(&sc) = st.streamers.get(&sid) {
                        let mut fwd = v.clone();
                        if let Some(obj) = fwd.as_object_mut() {
                            obj.insert("playerId".to_string(), json!(id));
                        }
                        send_json(&st, sc, fwd);
                    }
                }
                Some(Role::Streamer(_)) => {
                    if let Some(target) = v
                        .get("playerId")
                        .and_then(|x| x.as_str())
                        .and_then(|p| p.parse::<u64>().ok())
                    {
                        send_json(&st, target, v.clone());
                    }
                }
                _ => {}
            },
            // Streamer drops a player.
            "disconnectPlayer" => {
                if let Some(target) = v
                    .get("playerId")
                    .and_then(|x| x.as_str())
                    .and_then(|p| p.parse::<u64>().ok())
                {
                    send_json(&st, target, json!({ "type": "streamerDisconnected" }));
                }
            }
            // Application-level keepalive.
            "ping" => {
                let time = v.get("time").cloned().unwrap_or_else(|| json!(now_unix()));
                send_json(&st, conn_id, json!({ "type": "pong", "time": time }));
            }
            _ => {}
        }
    }

    /// Tear down a connection and notify its counterpart(s).
    fn remove_connection(&self, conn_id: u64) {
        let mut st = self.state.lock().unwrap();
        match st.roles.remove(&conn_id) {
            Some(Role::Streamer(sid)) => {
                st.streamers.remove(&sid);
                let affected: Vec<u64> = st
                    .roles
                    .iter()
                    .filter_map(|(cid, r)| match r {
                        Role::Player {
                            subscribed: Some(s),
                            ..
                        } if *s == sid => Some(*cid),
                        _ => None,
                    })
                    .collect();
                let ids: Vec<String> = st.streamers.keys().cloned().collect();
                for cid in affected {
                    send_json(&st, cid, json!({ "type": "streamerList", "ids": ids }));
                }
            }
            Some(Role::Player {
                id,
                subscribed: Some(sid),
            }) => {
                if let Some(&sc) = st.streamers.get(&sid) {
                    send_json(
                        &st,
                        sc,
                        json!({ "type": "playerDisconnected", "playerId": id }),
                    );
                }
            }
            _ => {}
        }
        st.senders.remove(&conn_id);
    }

    /// Own one upgraded WebSocket for its lifetime: split it, pump an outbound
    /// channel to the sink, and route inbound text frames.
    async fn run(self: Arc<Self>, ws: WsStream) {
        let (mut sink, mut stream) = ws.split();
        let (tx, mut rx) = mpsc::channel::<Message>(OUTBOUND_CAP);
        let writer = tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                if sink.send(m).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        let conn_id = self.add_connection(tx.clone());
        while let Some(next) = stream.next().await {
            match next {
                Ok(Message::Text(t)) => self.on_message(conn_id, t.as_str()),
                Ok(Message::Ping(p)) => {
                    let _ = tx.try_send(Message::Pong(p));
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        self.remove_connection(conn_id);
        writer.abort();
    }
}

/// The Pixel Streaming [`SignalingProtocol`]. Holds the shared streamer/player hub.
pub struct PixelStreamingProtocol {
    hub: Arc<PsHub>,
}

impl PixelStreamingProtocol {
    pub fn new(ice: IceConfig) -> Self {
        Self {
            hub: Arc::new(PsHub::new(ice)),
        }
    }
}

#[async_trait]
impl SignalingProtocol for PixelStreamingProtocol {
    fn id(&self) -> &'static str {
        "pixelstreaming"
    }

    async fn handle(&self, ws: WsStream, _peer: SocketAddr) {
        self.hub.clone().run(ws).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ice() -> IceConfig {
        IceConfig {
            urls: vec!["stun:stun.example:3478".to_string()],
            turn_secret: None,
            ttl: 60,
        }
    }

    /// Add a connection and drain the two handshake messages (config + identify).
    fn connect(hub: &Arc<PsHub>) -> (u64, mpsc::Receiver<Message>) {
        let (tx, mut rx) = mpsc::channel(OUTBOUND_CAP);
        let id = hub.add_connection(tx);
        // config + identify
        assert_eq!(next_type(&mut rx).as_deref(), Some("config"));
        assert_eq!(next_type(&mut rx).as_deref(), Some("identify"));
        (id, rx)
    }

    fn next_json(rx: &mut mpsc::Receiver<Message>) -> Option<Value> {
        match rx.try_recv().ok()? {
            Message::Text(t) => serde_json::from_str(t.as_str()).ok(),
            _ => None,
        }
    }

    fn next_type(rx: &mut mpsc::Receiver<Message>) -> Option<String> {
        next_json(rx).and_then(|v| v.get("type").and_then(|t| t.as_str()).map(String::from))
    }

    #[test]
    fn streamer_register_player_subscribe_and_sdp_relay() {
        let hub = Arc::new(PsHub::new(test_ice()));

        // streamer registers
        let (streamer, mut srx) = connect(&hub);
        hub.on_message(streamer, r#"{"type":"endpointId","id":"streamer_a"}"#);
        assert_eq!(next_type(&mut srx).as_deref(), Some("endpointIdConfirm"));

        // player lists + subscribes
        let (player, mut prx) = connect(&hub);
        hub.on_message(player, r#"{"type":"listStreamers"}"#);
        let list = next_json(&mut prx).unwrap();
        assert_eq!(list["type"], "streamerList");
        assert_eq!(list["ids"][0], "streamer_a");

        hub.on_message(player, r#"{"type":"subscribe","streamerId":"streamer_a"}"#);
        // streamer is told a player connected, tagged with the player's id
        let connected = next_json(&mut srx).unwrap();
        assert_eq!(connected["type"], "playerConnected");
        assert_eq!(connected["playerId"], player.to_string());

        // streamer → player offer (addressed by playerId)
        hub.on_message(
            streamer,
            &format!(r#"{{"type":"offer","sdp":"o","playerId":"{player}"}}"#),
        );
        let offer = next_json(&mut prx).unwrap();
        assert_eq!(offer["type"], "offer");
        assert_eq!(offer["sdp"], "o");

        // player → streamer answer (server injects playerId)
        hub.on_message(player, r#"{"type":"answer","sdp":"a"}"#);
        let answer = next_json(&mut srx).unwrap();
        assert_eq!(answer["type"], "answer");
        assert_eq!(answer["sdp"], "a");
        assert_eq!(answer["playerId"], player.to_string());

        // player leaves → streamer notified
        hub.remove_connection(player);
        let gone = next_json(&mut srx).unwrap();
        assert_eq!(gone["type"], "playerDisconnected");
        assert_eq!(gone["playerId"], player.to_string());
    }

    #[test]
    fn ping_is_answered_with_pong() {
        let hub = Arc::new(PsHub::new(test_ice()));
        let (conn, mut rx) = connect(&hub);
        hub.on_message(conn, r#"{"type":"ping","time":123}"#);
        let pong = next_json(&mut rx).unwrap();
        assert_eq!(pong["type"], "pong");
        assert_eq!(pong["time"], 123);
    }

    #[test]
    fn config_handshake_advertises_ice_servers() {
        let hub = Arc::new(PsHub::new(test_ice()));
        let (tx, mut rx) = mpsc::channel(OUTBOUND_CAP);
        hub.add_connection(tx);
        let config = next_json(&mut rx).unwrap();
        assert_eq!(config["type"], "config");
        assert_eq!(
            config["peerConnectionOptions"]["iceServers"][0]["urls"][0],
            "stun:stun.example:3478"
        );
    }
}
