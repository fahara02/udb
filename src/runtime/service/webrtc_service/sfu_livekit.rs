//! LiveKit-backed SFU bridge.
//!
//! The broker does not proxy LiveKit media. It mints short-lived LiveKit access
//! tokens bound to the verified UDB tenant/room/peer tuple and uses LiveKit's
//! RoomService API for lifecycle hooks when `http-client` is available.

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;

use super::{SfuBridge, SfuJoinToken};

#[derive(Clone)]
pub(crate) struct LiveKitSfuBridge {
    url: String,
    api_key: String,
    api_secret: String,
}

impl std::fmt::Debug for LiveKitSfuBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveKitSfuBridge")
            .field("url", &self.url)
            .field("api_key", &self.api_key)
            .field("api_secret", &"[redacted]")
            .finish()
    }
}

#[derive(Serialize)]
struct LiveKitClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    exp: i64,
    nbf: i64,
    video: LiveKitVideoGrant<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<&'a str>,
}

#[derive(Serialize)]
struct LiveKitVideoGrant<'a> {
    #[serde(rename = "roomJoin", skip_serializing_if = "is_false")]
    room_join: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    room: Option<&'a str>,
    #[serde(rename = "canPublish", skip_serializing_if = "is_false")]
    can_publish: bool,
    #[serde(rename = "canSubscribe", skip_serializing_if = "is_false")]
    can_subscribe: bool,
    #[serde(rename = "roomAdmin", skip_serializing_if = "is_false")]
    room_admin: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl LiveKitSfuBridge {
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        let url = std::env::var("UDB_LIVEKIT_URL").ok();
        let api_key = std::env::var("UDB_LIVEKIT_API_KEY").ok();
        let api_secret = std::env::var("UDB_LIVEKIT_API_SECRET").ok();
        let allow_insecure = env_bool("UDB_LIVEKIT_ALLOW_INSECURE");
        if url.as_deref().unwrap_or("").trim().is_empty()
            && api_key.as_deref().unwrap_or("").trim().is_empty()
            && api_secret.as_deref().unwrap_or("").trim().is_empty()
        {
            return Ok(None);
        }
        let url = required("UDB_LIVEKIT_URL", url)?;
        let api_key = required("UDB_LIVEKIT_API_KEY", api_key)?;
        let api_secret = required("UDB_LIVEKIT_API_SECRET", api_secret)?;
        Self::new_with_insecure(url, api_key, api_secret, allow_insecure).map(Some)
    }

    fn new(url: String, api_key: String, api_secret: String) -> Result<Self, String> {
        Self::new_with_insecure(url, api_key, api_secret, false)
    }

    fn new_with_insecure(
        url: String,
        api_key: String,
        api_secret: String,
        allow_insecure: bool,
    ) -> Result<Self, String> {
        let url = url.trim().trim_end_matches('/').to_string();
        let tls = url.starts_with("https://") || url.starts_with("wss://");
        let plaintext = url.starts_with("http://") || url.starts_with("ws://");
        if !tls && !(allow_insecure && plaintext) {
            return if plaintext {
                Err(
                    "UDB_LIVEKIT_URL uses plaintext; set UDB_LIVEKIT_ALLOW_INSECURE=1 only for local LiveKit tests"
                        .to_string(),
                )
            } else {
                Err("UDB_LIVEKIT_URL must use https:// or wss://".to_string())
            };
        }
        Ok(Self {
            url,
            api_key: api_key.trim().to_string(),
            api_secret: api_secret.trim().to_string(),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(url: &str, api_key: &str, api_secret: &str) -> Self {
        Self::new(url.to_string(), api_key.to_string(), api_secret.to_string())
            .expect("test LiveKit config")
    }

    #[cfg(test)]
    pub(crate) fn for_test_insecure(url: &str, api_key: &str, api_secret: &str) -> Self {
        Self::new_with_insecure(
            url.to_string(),
            api_key.to_string(),
            api_secret.to_string(),
            true,
        )
        .expect("test insecure LiveKit config")
    }

    #[cfg(feature = "http-client")]
    fn room_service_base(&self) -> String {
        if let Some(rest) = self.url.strip_prefix("wss://") {
            format!("https://{rest}")
        } else if let Some(rest) = self.url.strip_prefix("ws://") {
            format!("http://{rest}")
        } else {
            self.url.clone()
        }
        .trim_end_matches('/')
        .to_string()
    }

    fn participant_identity(tenant_id: &str, room_id: &str, peer_id: &str) -> String {
        format!(
            "udb:{}:{}:{}",
            tenant_id.trim(),
            room_id.trim(),
            peer_id.trim()
        )
    }

    fn participant_metadata(tenant_id: &str, room_id: &str, peer_id: &str) -> String {
        serde_json::json!({
            "tenant_id": tenant_id.trim(),
            "room_id": room_id.trim(),
            "peer_id": peer_id.trim(),
        })
        .to_string()
    }

    fn sign_token(
        &self,
        subject: &str,
        ttl_seconds: i64,
        now_unix: i64,
        grant: LiveKitVideoGrant<'_>,
        metadata: Option<&str>,
    ) -> Result<SfuJoinToken, String> {
        let ttl_seconds = ttl_seconds.max(1);
        let claims = LiveKitClaims {
            iss: &self.api_key,
            sub: subject,
            exp: now_unix + ttl_seconds,
            nbf: now_unix.saturating_sub(5),
            video: grant,
            metadata,
        };
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(self.api_secret.as_bytes()),
        )
        .map_err(|err| format!("LiveKit token signing failed: {err}"))?;
        Ok(SfuJoinToken {
            url: self.url.clone(),
            token,
            expires_at: claims.exp,
        })
    }

    fn join_token(
        &self,
        tenant_id: &str,
        room_id: &str,
        peer_id: &str,
        ttl_seconds: i64,
        now_unix: i64,
    ) -> Result<SfuJoinToken, String> {
        let identity = Self::participant_identity(tenant_id, room_id, peer_id);
        let metadata = Self::participant_metadata(tenant_id, room_id, peer_id);
        self.sign_token(
            &identity,
            ttl_seconds,
            now_unix,
            LiveKitVideoGrant {
                room_join: true,
                room: Some(room_id.trim()),
                can_publish: true,
                can_subscribe: true,
                room_admin: false,
            },
            Some(&metadata),
        )
    }

    #[cfg(feature = "http-client")]
    fn admin_token(&self, room_id: &str, now_unix: i64) -> Result<String, String> {
        let token = self.sign_token(
            "udb:sfu-admin",
            300,
            now_unix,
            LiveKitVideoGrant {
                room_join: false,
                room: Some(room_id.trim()),
                can_publish: false,
                can_subscribe: false,
                room_admin: true,
            },
            None,
        )?;
        Ok(token.token)
    }

    #[cfg(feature = "http-client")]
    async fn post_room_service(
        &self,
        method: &str,
        room_id: &str,
        body: serde_json::Value,
    ) -> Result<(), String> {
        let token = self.admin_token(room_id, chrono::Utc::now().timestamp())?;
        let url = format!(
            "{}/twirp/livekit.RoomService/{method}",
            self.room_service_base()
        );
        let response = reqwest::Client::new()
            .post(url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("LiveKit {method} request failed: {err}"))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(format!(
            "LiveKit {method} failed with HTTP {status}: {body}"
        ))
    }
}

fn required(name: &str, value: Option<String>) -> Result<String, String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{name} is required when LiveKit SFU is configured"))
}

#[async_trait::async_trait]
impl SfuBridge for LiveKitSfuBridge {
    async fn accept_offer(
        &self,
        _room_id: &str,
        _peer_id: &str,
        _offer_sdp: &str,
    ) -> Result<String, String> {
        Err(
            "LiveKit uses client-side WebRTC join tokens; UDB does not proxy SDP offers"
                .to_string(),
        )
    }

    async fn mint_join_token(
        &self,
        tenant_id: &str,
        room_id: &str,
        peer_id: &str,
        ttl_seconds: i64,
        now_unix: i64,
    ) -> Result<Option<SfuJoinToken>, String> {
        self.join_token(tenant_id, room_id, peer_id, ttl_seconds, now_unix)
            .map(Some)
    }

    async fn register_published_track(
        &self,
        _room_id: &str,
        _peer_id: &str,
        _track_id: &str,
        _kind: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn unregister_track(&self, _track_id: &str) -> Result<(), String> {
        Ok(())
    }

    async fn kick_peer(&self, tenant_id: &str, room_id: &str, peer_id: &str) -> Result<(), String> {
        #[cfg(feature = "http-client")]
        {
            let identity = Self::participant_identity(tenant_id, room_id, peer_id);
            return self
                .post_room_service(
                    "RemoveParticipant",
                    room_id,
                    serde_json::json!({
                        "room": room_id,
                        "identity": identity,
                    }),
                )
                .await;
        }
        #[cfg(not(feature = "http-client"))]
        {
            let _ = (tenant_id, room_id, peer_id);
            Err("LiveKit peer kick requires the http-client feature".to_string())
        }
    }

    async fn close_room_hook(&self, room_id: &str) -> Result<(), String> {
        #[cfg(feature = "http-client")]
        {
            return self
                .post_room_service(
                    "DeleteRoom",
                    room_id,
                    serde_json::json!({ "room": room_id }),
                )
                .await;
        }
        #[cfg(not(feature = "http-client"))]
        {
            let _ = room_id;
            Err("LiveKit room close requires the http-client feature".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    use serde::Deserialize;

    #[derive(Clone, Debug, Deserialize)]
    struct DecodedClaims {
        iss: String,
        sub: String,
        exp: i64,
        video: serde_json::Value,
        metadata: String,
    }

    #[tokio::test]
    async fn livekit_join_token_binds_tenant_room_and_peer() {
        let bridge = LiveKitSfuBridge::for_test("wss://livekit.example", "api-key", "secret");
        let token = bridge
            .mint_join_token("tenant-a", "room-a", "peer-a", 60, 1_700_000_000)
            .await
            .expect("token minted")
            .expect("LiveKit bridge returns a token");

        assert_eq!(token.url, "wss://livekit.example");
        assert_eq!(token.expires_at, 1_700_000_060);
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        let decoded = decode::<DecodedClaims>(
            &token.token,
            &DecodingKey::from_secret(b"secret"),
            &validation,
        )
        .expect("valid LiveKit JWT");
        assert_eq!(decoded.claims.iss, "api-key");
        assert_eq!(decoded.claims.sub, "udb:tenant-a:room-a:peer-a");
        assert_eq!(decoded.claims.exp, 1_700_000_060);
        assert_eq!(decoded.claims.video["roomJoin"], true);
        assert_eq!(decoded.claims.video["room"], "room-a");
        assert_eq!(decoded.claims.video["canPublish"], true);
        assert_eq!(decoded.claims.video["canSubscribe"], true);
        assert_eq!(
            decoded.claims.metadata,
            r#"{"peer_id":"peer-a","room_id":"room-a","tenant_id":"tenant-a"}"#
        );
    }

    #[test]
    fn plaintext_livekit_url_requires_explicit_local_opt_in() {
        let err = LiveKitSfuBridge::new(
            "ws://livekit:7880".to_string(),
            "devkey".to_string(),
            "secret".to_string(),
        )
        .expect_err("plaintext LiveKit must be opt-in only");
        assert!(err.contains("UDB_LIVEKIT_ALLOW_INSECURE=1"));

        let bridge = LiveKitSfuBridge::for_test_insecure("ws://livekit:7880", "devkey", "secret");
        assert_eq!(bridge.url, "ws://livekit:7880");
    }

    #[cfg(feature = "http-client")]
    #[test]
    fn room_service_base_maps_websocket_urls_to_http_twirp_base() {
        let tls = LiveKitSfuBridge::for_test("wss://livekit.example", "api-key", "secret");
        assert_eq!(tls.room_service_base(), "https://livekit.example");

        let local = LiveKitSfuBridge::for_test_insecure("ws://livekit:7880", "devkey", "secret");
        assert_eq!(local.room_service_base(), "http://livekit:7880");
    }
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}
