//! Generic JSON **room-broadcast** signalling protocol over `ws://`.
//!
//! A minimal mesh relay that proves the [`SignalingProtocol`] trait is enough to
//! add a second protocol without touching the server. Every text frame is JSON
//! carrying a `"room"` field (e.g. `{"room":"r","sdp":"…"}`). The first frame a
//! connection sends binds it to that room; thereafter the server **broadcasts
//! each received frame verbatim to every OTHER connection in the same room** —
//! it never echoes back to the sender and applies no SDP semantics. This is the
//! generic counterpart to the SDP-aware `pixelstreaming` router and demonstrates
//! the extension point: a new trait impl plus a `from_env` match arm, no server
//! rewrite.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::{self, Sender};
use tokio_tungstenite::tungstenite::Message;

use super::{SignalingProtocol, WsStream};

/// Bounded per-connection outbound queue: a slow socket drops frames once full
/// rather than growing an unbounded backlog (backpressure, no memory blowup).
const OUTBOUND_CAP: usize = 256;

/// Shared cross-connection registry. `rooms` maps a room id to its members'
/// outbound senders (keyed by connection id), so a broadcast can address every
/// member except the sender.
#[derive(Default)]
struct RelayHub {
    next_id: AtomicU64,
    rooms: Mutex<HashMap<String, HashMap<u64, Sender<Message>>>>,
}

impl RelayHub {
    fn next_conn_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Bind `conn_id` (with its writer `tx`) to `room`, creating the room if
    /// needed. Idempotent: re-binding the same connection just refreshes its
    /// sender.
    fn join(&self, room: &str, conn_id: u64, tx: Sender<Message>) {
        let mut rooms = self.rooms.lock().unwrap();
        rooms
            .entry(room.to_string())
            .or_default()
            .insert(conn_id, tx);
    }

    /// Deliver `msg` to every member of `room` except `from`.
    fn broadcast(&self, room: &str, from: u64, msg: &Message) {
        let rooms = self.rooms.lock().unwrap();
        if let Some(members) = rooms.get(room) {
            for (&cid, tx) in members.iter() {
                if cid != from {
                    // Drop on a full queue rather than block the shared hub lock.
                    let _ = tx.try_send(msg.clone());
                }
            }
        }
    }

    /// Remove `conn_id` from `room`, dropping the room when it becomes empty.
    fn leave(&self, room: &str, conn_id: u64) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(members) = rooms.get_mut(room) {
            members.remove(&conn_id);
            if members.is_empty() {
                rooms.remove(room);
            }
        }
    }
}

/// Extract the `"room"` field from a JSON text frame, if present and non-empty.
fn room_of(text: &str) -> Option<String> {
    let v: Value = serde_json::from_str(text).ok()?;
    let room = v.get("room")?.as_str()?;
    if room.is_empty() {
        None
    } else {
        Some(room.to_string())
    }
}

/// The generic room-broadcast [`SignalingProtocol`]. Holds the shared room hub.
pub struct JsonRelayProtocol {
    hub: Arc<RelayHub>,
}

impl JsonRelayProtocol {
    pub fn new() -> Self {
        Self {
            hub: Arc::new(RelayHub::default()),
        }
    }
}

impl Default for JsonRelayProtocol {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SignalingProtocol for JsonRelayProtocol {
    fn id(&self) -> &'static str {
        "json-relay"
    }

    async fn handle(&self, ws: WsStream, _peer: SocketAddr) {
        let hub = self.hub.clone();
        let conn_id = hub.next_conn_id();

        // Split the socket; pump an outbound channel to the sink so the hub (and
        // the Ping handler) can write without owning the sink.
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

        // The first JSON frame with a "room" binds this connection.
        let mut room: Option<String> = None;
        while let Some(next) = stream.next().await {
            match next {
                Ok(Message::Text(t)) => {
                    let text = t.as_str();
                    match &room {
                        Some(r) => hub.broadcast(r, conn_id, &Message::text(text.to_string())),
                        None => {
                            if let Some(r) = room_of(text) {
                                hub.join(&r, conn_id, tx.clone());
                                // The binding frame is itself relayed to peers
                                // already in the room.
                                hub.broadcast(&r, conn_id, &Message::text(text.to_string()));
                                room = Some(r);
                            }
                        }
                    }
                }
                Ok(Message::Ping(p)) => {
                    let _ = tx.try_send(Message::Pong(p));
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }

        if let Some(r) = room {
            hub.leave(&r, conn_id);
        }
        writer.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two connections in room "r": a message from A reaches B but is not echoed
    /// back to A.
    #[test]
    fn broadcast_reaches_others_not_sender() {
        let hub = RelayHub::default();
        let a = hub.next_conn_id();
        let b = hub.next_conn_id();
        let (atx, mut arx) = mpsc::channel::<Message>(OUTBOUND_CAP);
        let (btx, mut brx) = mpsc::channel::<Message>(OUTBOUND_CAP);

        hub.join("r", a, atx);
        hub.join("r", b, btx);

        let msg = Message::text(r#"{"room":"r","hello":"world"}"#.to_string());
        hub.broadcast("r", a, &msg);

        // B receives it...
        match brx.try_recv().expect("B should receive the broadcast") {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                assert_eq!(v["room"], "r");
                assert_eq!(v["hello"], "world");
            }
            other => panic!("unexpected message: {other:?}"),
        }
        // ...A does not (no echo to sender).
        assert!(
            arx.try_recv().is_err(),
            "sender must not receive its own message"
        );
    }

    #[test]
    fn leaving_empties_and_drops_the_room() {
        let hub = RelayHub::default();
        let a = hub.next_conn_id();
        let (atx, _arx) = mpsc::channel::<Message>(OUTBOUND_CAP);
        hub.join("r", a, atx);
        assert!(hub.rooms.lock().unwrap().contains_key("r"));
        hub.leave("r", a);
        assert!(!hub.rooms.lock().unwrap().contains_key("r"));
    }

    #[test]
    fn room_of_parses_field() {
        assert_eq!(room_of(r#"{"room":"abc"}"#).as_deref(), Some("abc"));
        assert_eq!(room_of(r#"{"room":""}"#), None);
        assert_eq!(room_of(r#"{"no":"room"}"#), None);
        assert_eq!(room_of("not json"), None);
    }
}
