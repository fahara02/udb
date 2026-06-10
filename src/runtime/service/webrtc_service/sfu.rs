//! Feature-gated embedded WebRTC SFU foundation.
//!
//! This module is compiled only with `--features webrtc`. It owns live
//! `RTCPeerConnection` handles for the embedded media plane while durable
//! room/peer/track metadata stays in `udb_webrtc.*`.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use webrtc_rs::api::APIBuilder;
use webrtc_rs::api::media_engine::MediaEngine;
use webrtc_rs::peer_connection::RTCPeerConnection;
use webrtc_rs::peer_connection::configuration::RTCConfiguration;
use webrtc_rs::peer_connection::sdp::session_description::RTCSessionDescription;

#[derive(Default)]
pub(crate) struct EmbeddedSfu {
    peers: Mutex<HashMap<String, Arc<RTCPeerConnection>>>,
    tracks: Mutex<HashMap<String, SfuTrack>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct SfuTrack {
    room_id: String,
    peer_id: String,
    kind: String,
}

impl EmbeddedSfu {
    pub(crate) fn enabled_from_env() -> bool {
        std::env::var("UDB_WEBRTC_SFU_ENABLED")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }

    fn peer_key(room_id: &str, peer_id: &str) -> String {
        format!("{room_id}:{peer_id}")
    }

    /// Accept an SDP offer from a peer, create an embedded `webrtc-rs`
    /// PeerConnection, and return the SFU's SDP answer. RTP fan-out is attached
    /// to these stored connections by later publish/subscribe handling; the
    /// important boundary here is that UDB now owns real media-plane peer
    /// connections only when the `webrtc` feature is enabled.
    pub(crate) async fn accept_offer(
        &self,
        room_id: &str,
        peer_id: &str,
        offer_sdp: &str,
    ) -> Result<String, String> {
        let mut media = MediaEngine::default();
        media
            .register_default_codecs()
            .map_err(|err| format!("register WebRTC codecs failed: {err}"))?;
        let api = APIBuilder::new().with_media_engine(media).build();
        let pc = Arc::new(
            api.new_peer_connection(RTCConfiguration::default())
                .await
                .map_err(|err| format!("create SFU peer connection failed: {err}"))?,
        );

        let offer = RTCSessionDescription::offer(offer_sdp.to_string())
            .map_err(|err| format!("invalid SDP offer: {err}"))?;
        pc.set_remote_description(offer)
            .await
            .map_err(|err| format!("set SFU remote description failed: {err}"))?;
        let answer = pc
            .create_answer(None)
            .await
            .map_err(|err| format!("create SFU answer failed: {err}"))?;
        let answer_sdp = answer.sdp.clone();
        pc.set_local_description(answer)
            .await
            .map_err(|err| format!("set SFU local description failed: {err}"))?;

        self.peers
            .lock()
            .await
            .insert(Self::peer_key(room_id, peer_id), pc);
        Ok(answer_sdp)
    }

    pub(crate) async fn register_published_track(
        &self,
        room_id: &str,
        peer_id: &str,
        track_id: &str,
        kind: &str,
    ) {
        self.tracks.lock().await.insert(
            track_id.to_string(),
            SfuTrack {
                room_id: room_id.to_string(),
                peer_id: peer_id.to_string(),
                kind: kind.to_string(),
            },
        );
    }

    pub(crate) async fn unregister_track(&self, track_id: &str) {
        self.tracks.lock().await.remove(track_id);
    }

    pub(crate) async fn close_peer(&self, room_id: &str, peer_id: &str) {
        if let Some(pc) = self
            .peers
            .lock()
            .await
            .remove(&Self::peer_key(room_id, peer_id))
        {
            if let Err(err) = pc.close().await {
                tracing::warn!(room_id, peer_id, error = %err, "embedded SFU peer close failed");
            }
        }
    }
}
