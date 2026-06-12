//! Native WebRTC streaming control plane — proto-driven Postgres CRUD over the
//! UDB-owned `udb_webrtc.{rooms,peers,tracks}` tables, plus short-lived TURN
//! (coturn REST) credential issuance and a bidirectional signaling relay.
//!
//! Mirrors `tenant_service`: no in-memory canonical store, no hand-mapped schema
//! — table/column identifiers resolve from the embedded proto manifest via
//! [`NativeModel`]. Durable room/peer/track state lives in Postgres; only the
//! transient signaling fan-out (live socket routing) is held in memory, which is
//! unavoidable for a signaling server and is not canonical data.
//!
//! Scope = Layer A by default: signaling + TURN credentials + NAT traversal +
//! data channels (mesh). When built with the `webrtc` Cargo feature, an embedded
//! `webrtc-rs` SFU foundation can be enabled with `UDB_WEBRTC_SFU_ENABLED=1`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::{PgPool, Row};
use tokio::sync::{Mutex, broadcast};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::native_helpers::{
    MAX_LIST_ROWS, admit_on as native_admit_on, emit_payload_event, native_service_context,
    validate_request_tenant,
};
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalSort, LogicalValue, NullOrder, SortDirection,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::webrtc::entity::v1 as webrtc_entity_pb;
use crate::proto::udb::core::webrtc::services::v1 as webrtc_pb;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

use webrtc_pb::peer_service_server::PeerService;
use webrtc_pb::room_service_server::RoomService;
use webrtc_pb::signaling_service_server::SignalingService;
use webrtc_pb::track_service_server::TrackService;
use webrtc_pb::turn_service_server::TurnService;

pub use webrtc_pb::peer_service_server::PeerServiceServer;
pub use webrtc_pb::room_service_server::RoomServiceServer;
pub use webrtc_pb::signaling_service_server::SignalingServiceServer;
pub use webrtc_pb::track_service_server::TrackServiceServer;
pub use webrtc_pb::turn_service_server::TurnServiceServer;

use super::DataBrokerService;

#[cfg(feature = "webrtc")]
mod sfu;

const ROOM_MSG: &str = "udb.core.webrtc.entity.v1.Room";
const PEER_MSG: &str = "udb.core.webrtc.entity.v1.Peer";
const TRACK_MSG: &str = "udb.core.webrtc.entity.v1.Track";

/// Default TURN credential lifetime (1 day) when `UDB_TURN_TTL_SECONDS` is unset.
const DEFAULT_TURN_TTL_SECS: i64 = 86_400;
/// Default ICE server advertised when `UDB_TURN_URLS` is unset.
const DEFAULT_STUN_URL: &str = "stun:stun.l.google.com:19302";
/// Action bound into TURN REST credentials; the credential only authorizes media relay.
const TURN_RELAY_ACTION: &str = "webrtc.turn.relay";
/// Per-room signaling broadcast channel capacity (buffered SDP/ICE fan-out).
const SIGNALING_BROADCAST_CAPACITY: usize = 256;
/// Inbound signaling pump: re-validate room membership and refresh the peer's
/// last-seen heartbeat after this many relayed frames…
const SIGNAL_MEMBERSHIP_RECHECK_FRAMES: u64 = 64;
/// …or after this many seconds since the last check, whichever comes first.
/// Must be smaller than the stale-peer timeout so live peers keep their
/// heartbeat fresh.
const SIGNAL_MEMBERSHIP_RECHECK_SECS: u64 = 20;
/// Default interval between stale-peer reaper sweeps
/// (`UDB_WEBRTC_REAP_INTERVAL_SECS` overrides; 0 disables the reaper).
const STALE_PEER_REAP_INTERVAL_SECS_DEFAULT: u64 = 60;
/// Default heartbeat age after which a CONNECTED peer with no signaling
/// activity is reaped (`UDB_WEBRTC_STALE_PEER_TIMEOUT_SECS` overrides;
/// 0 disables the reaper).
const STALE_PEER_TIMEOUT_SECS_DEFAULT: i64 = 120;
/// Bounded batch per stale-peer reaper sweep.
const STALE_PEER_REAP_BATCH_SIZE: i64 = 500;

/// TURN (coturn) configuration, read from the environment once at build time.
///
/// Credentials follow the long-term-secret REST scheme (RFC 5766-bis / coturn
/// `use-auth-secret`): the broker and coturn share `secret`; the broker mints
/// short-lived `username = "<expiry-unix>:<principal>"` and
/// `credential = base64(HMAC-SHA1(secret, username))`.
#[derive(Clone, Debug)]
struct TurnConfig {
    /// Shared coturn secret, or `None` when unconfigured in production (fail
    /// closed — `IssueCredentials` then rejects rather than minting with a
    /// well-known dev secret). Resolved via `security::resolve_turn_secret`.
    secret: Option<Vec<u8>>,
    urls: Vec<String>,
    default_ttl: i64,
}

impl TurnConfig {
    fn from_env() -> Self {
        let secret = crate::runtime::security::resolve_turn_secret();
        let urls = std::env::var("UDB_TURN_URLS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.split(',')
                    .map(|u| u.trim().to_string())
                    .filter(|u| !u.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![DEFAULT_STUN_URL.to_string()]);
        Self {
            secret,
            urls,
            default_ttl: DEFAULT_TURN_TTL_SECS,
        }
    }
}

/// One broadcast frame on a room's signaling channel: a relayed signaling
/// payload, a targeted peer-closed marker that ends ONLY the addressed peer's
/// subscriber stream (`leave_room`), or the terminal room-closed marker that
/// ends every subscriber stream (`close_room`). There are no proto-level
/// PeerClosed/RoomClosed signal payloads, so termination is hub-internal and
/// surfaces to clients as end-of-stream.
#[derive(Clone)]
enum HubFrame {
    Signal(webrtc_pb::SignalResponse),
    /// End-of-stream for the one peer whose `peer_id` this carries; every
    /// other subscriber skips the frame and keeps relaying.
    PeerClosed(String),
    RoomClosed,
}

/// Per-room live channel: the broadcast sender plus a closed flag the inbound
/// pumps consult before relaying. Once a room is closed, racing senders become
/// no-ops instead of delivering frames into a dead room.
#[derive(Clone)]
struct RoomChannel {
    tx: broadcast::Sender<HubFrame>,
    closed: Arc<AtomicBool>,
}

impl RoomChannel {
    fn send_signal(&self, resp: webrtc_pb::SignalResponse) {
        if !self.closed.load(Ordering::SeqCst) {
            let _ = self.tx.send(HubFrame::Signal(resp));
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn subscribe(&self) -> broadcast::Receiver<HubFrame> {
        self.tx.subscribe()
    }
}

/// Transient per-room signaling fan-out. Each room gets a broadcast channel;
/// every connected peer subscribes and publishes SDP/ICE through it. This is
/// live-connection routing state, not canonical data — it is rebuilt on connect
/// and lost on restart by design.
#[derive(Default)]
struct SignalingHub {
    rooms: Mutex<HashMap<String, RoomChannel>>,
}

impl SignalingHub {
    async fn channel(&self, room_id: &str) -> RoomChannel {
        let mut guard = self.rooms.lock().await;
        guard
            .entry(room_id.to_string())
            .or_insert_with(|| RoomChannel {
                tx: broadcast::channel(SIGNALING_BROADCAST_CAPACITY).0,
                closed: Arc::new(AtomicBool::new(false)),
            })
            .clone()
    }

    async fn prune_if_idle(&self, room_id: &str) {
        let mut guard = self.rooms.lock().await;
        let should_remove = guard
            .get(room_id)
            .map(|ch| ch.tx.receiver_count() == 0)
            .unwrap_or(false);
        if should_remove {
            guard.remove(room_id);
        }
    }

    /// Terminate a room's signaling: mark the channel closed (racing senders
    /// become no-ops), broadcast the terminal [`HubFrame::RoomClosed`] so every
    /// subscriber stream ends, and drop the hub entry. Whole-room teardown —
    /// `close_room` only; a single peer leaving uses [`Self::close_peer`].
    async fn close(&self, room_id: &str) {
        let entry = { self.rooms.lock().await.remove(room_id) };
        if let Some(ch) = entry {
            ch.closed.store(true, Ordering::SeqCst);
            let _ = ch.tx.send(HubFrame::RoomClosed);
        }
    }

    /// Terminate ONLY one peer's subscriber stream: broadcast the targeted
    /// [`HubFrame::PeerClosed`] which the outbound pump treats as end-of-stream
    /// for the addressed peer and a no-op for everyone else. The room channel
    /// stays open so the remaining peers keep signaling (`leave_room` path).
    async fn close_peer(&self, room_id: &str, peer_id: &str) {
        let entry = { self.rooms.lock().await.get(room_id).cloned() };
        if let Some(ch) = entry
            && !ch.is_closed()
        {
            let _ = ch.tx.send(HubFrame::PeerClosed(peer_id.to_string()));
        }
    }
}

/// Postgres-backed WebRTC control-plane handler. One struct implements all five
/// tonic services (Room/Peer/Track/Turn/Signaling); it is cloned per server when
/// registered on the control-plane listener.
#[derive(Clone)]
pub struct WebrtcServiceImpl {
    pg_pool: Option<PgPool>,
    /// Runtime handle for the typed native-entity data-plane path
    /// (`native_entity_read_for_service`). Room/peer/track CRUD reads + simple
    /// inserts ride the IR compiler → backend dispatch (extend_udb.md P4); the
    /// genuinely atomic/arithmetic lifecycle writes keep their bespoke SQL on
    /// `pg_pool` (capability-gated escape hatch, P4.D). `None` in bare test
    /// construction — `build_webrtc_service` always wires it in production.
    runtime: Option<Arc<DataBrokerRuntime>>,
    turn: TurnConfig,
    signaling: Arc<SignalingHub>,
    #[cfg(feature = "webrtc")]
    sfu: Arc<sfu::EmbeddedSfu>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka → consumers. `None` = no domain-event emission.
    outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Join/publish/signal entry points acquire a
    /// per-tenant `Admin` budget through this so one tenant can't starve the
    /// shared signaling/control plane. `None` only in test construction (no
    /// runtime wired) — `build_webrtc_service` always wires it in production.
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl WebrtcServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            turn: TurnConfig::from_env(),
            signaling: Arc::new(SignalingHub::default()),
            #[cfg(feature = "webrtc")]
            sfu: Arc::new(sfu::EmbeddedSfu::default()),
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    /// Wire the shared per-tenant fair-admission manager (same one the data plane
    /// uses) so join/publish/signal entry points are bounded per tenant.
    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    /// Per-tenant fair admission for a webrtc control-plane mutation. Acquires the
    /// shared `Admin` channel budget SCOPED to the validated tenant so a single
    /// tenant cannot starve the shared signaling/control plane — mirrors the data
    /// plane's `execute_with_channel_scoped`. On exhaustion returns the same
    /// `Status::resource_exhausted` backpressure. The returned [`ChannelPermit`]
    /// is dropped (released) as soon as admission succeeds for the streaming
    /// `Signal` RPC — it gates ENTRY only and is NOT held across the long-lived
    /// stream, so it can never deadlock the relay. `None` channels (test mode)
    /// admit without a permit.
    ///
    /// `tenant` MUST be the VALIDATED tenant (post `validate_request_*`).
    async fn admit(&self, tenant: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webrtc",
            OperationChannel::Admin,
            tenant,
            None,
        )
        .await
    }

    /// Lighter per-tenant fair admission for a READ RPC (get/list room/peer/track).
    /// Acquires the cheap `Read` channel budget scoped to the validated tenant so
    /// one tenant cannot exhaust the shared pool with reads.
    async fn admit_read(&self, tenant: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webrtc",
            OperationChannel::Read,
            tenant,
            None,
        )
        .await
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Wire the runtime used for the typed native-entity path (room/peer/track
    /// CRUD reads + simple inserts compile through the IR and dispatch to the
    /// proto-bound backend). `build_webrtc_service` always wires it.
    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    /// The typed-path runtime; fail closed when absent (bare test construction).
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            Status::failed_precondition("webrtc service requires runtime native entity dispatch")
        })
    }

    /// Wire the transactional outbox so room/peer/track mutations publish domain
    /// events to Kafka (via the CDC relay). `relation` is the schema-qualified
    /// table, e.g. `"udb_system"."outbox_events"` (`CdcConfig::outbox_relation`).
    pub fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    /// Room/peer/track CRUD is durable-only: fail closed without a Postgres pool.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "webrtc service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    async fn require_active_peer_membership(
        &self,
        pool: &PgPool,
        tenant_id: Uuid,
        room_id: Uuid,
        peer_id: Uuid,
    ) -> Result<(), Status> {
        let pm = peer_model();
        let rm = room_model();
        // Transitional P4 gate: membership validation is a cross-entity join with
        // state guards; keep it on the Postgres-native path until typed native
        // reads expose a portable join/capability contract.
        let member: Option<i32> = sqlx::query_scalar(&format!(
            "SELECT 1 FROM {prel} p \
             JOIN {rrel} r ON r.{rrid} = p.{prid} AND r.{rtid} = p.{ptid} \
             WHERE p.{ppid} = $1::UUID AND p.{prid} = $2::UUID AND p.{ptid} = $3::UUID \
               AND p.{pdel} IS NULL AND p.{pstate} = 'CONNECTED' \
               AND r.{rdel} IS NULL AND r.{rstate} = 'ACTIVE' \
             LIMIT 1",
            prel = pm.relation,
            rrel = rm.relation,
            rrid = rm.q("room_id"),
            rtid = rm.q("tenant_id"),
            prid = pm.q("room_id"),
            ptid = pm.q("tenant_id"),
            ppid = pm.q("peer_id"),
            pdel = pm.q("deleted_at"),
            pstate = pm.q("state"),
            rdel = rm.q("deleted_at"),
            rstate = rm.q("state"),
        ))
        .bind(peer_id)
        .bind(room_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("verify peer membership failed: {err}")))?;
        if member.is_none() {
            return Err(Status::failed_precondition(
                "peer is not an active member of this room",
            ));
        }
        Ok(())
    }

    pub(crate) async fn require_active_signal_membership(
        &self,
        req: &webrtc_pb::SignalRequest,
    ) -> Result<(String, String, String), Status> {
        let bound_room = req.room_id.trim().to_string();
        let bound_peer = req.peer_id.trim().to_string();
        let bound_tenant = req.tenant_id.trim().to_string();
        if bound_room.is_empty() {
            return Err(Status::invalid_argument(
                "room_id is required on the first signaling message",
            ));
        }
        if bound_peer.is_empty() {
            return Err(Status::invalid_argument(
                "peer_id is required on the first signaling message",
            ));
        }
        if bound_tenant.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id is required on the first signaling message",
            ));
        }
        let tenant_uuid = parse_uuid("tenant_id", &bound_tenant)?;
        let room_uuid = parse_uuid("room_id", &bound_room)?;
        let peer_uuid = parse_uuid("peer_id", &bound_peer)?;
        let pool = self.require_pool()?;
        self.require_active_peer_membership(pool, tenant_uuid, room_uuid, peer_uuid)
            .await
            .map_err(|err| {
                if err.code() == tonic::Code::FailedPrecondition {
                    Status::permission_denied(err.message().to_string())
                } else {
                    err
                }
            })?;
        Ok((bound_tenant, bound_room, bound_peer))
    }

    #[cfg(feature = "webrtc")]
    async fn sfu_answer_for_offer(
        &self,
        req: &webrtc_pb::SignalRequest,
        offer_sdp: &str,
    ) -> Option<webrtc_pb::SignalResponse> {
        if !sfu::EmbeddedSfu::enabled_from_env() {
            return None;
        }
        if req.room_id.trim().is_empty() || req.peer_id.trim().is_empty() {
            tracing::warn!("embedded SFU offer ignored: room_id and peer_id are required");
            return None;
        }
        match self
            .sfu
            .accept_offer(&req.room_id, &req.peer_id, offer_sdp)
            .await
        {
            Ok(answer_sdp) => Some(webrtc_pb::SignalResponse {
                payload: Some(webrtc_pb::signal_response::Payload::AnswerSdp(answer_sdp)),
            }),
            Err(err) => {
                tracing::warn!(
                    room_id = %req.room_id,
                    peer_id = %req.peer_id,
                    error = %err,
                    "embedded SFU failed to accept offer; falling back to mesh relay"
                );
                None
            }
        }
    }

    async fn signal_response_for(
        &self,
        req: &webrtc_pb::SignalRequest,
    ) -> Option<webrtc_pb::SignalResponse> {
        #[cfg(feature = "webrtc")]
        if let Some(webrtc_pb::signal_request::Payload::OfferSdp(sdp)) = req.payload.as_ref()
            && let Some(answer) = self.sfu_answer_for_offer(req, sdp).await
        {
            return Some(answer);
        }
        signal_to_response(req)
    }

    /// Idempotently release a live peer from a room after signaling teardown.
    /// The peer update is guarded by `state = 'CONNECTED'`, so concurrent
    /// inbound/outbound stream endings can both call this while only one releases
    /// the participant slot.
    async fn disconnect_bound_peer(
        &self,
        pool: &PgPool,
        tenant_id: Uuid,
        room_id: Uuid,
        peer_id: Uuid,
        reason: &str,
    ) -> Result<bool, Status> {
        let pm = peer_model();
        let rm = room_model();
        let mut tx = pool.begin().await.map_err(|err| {
            Status::internal(format!("disconnect peer transaction failed: {err}"))
        })?;
        let result = sqlx::query(&disconnect_peer_sql(&pm))
            .bind(peer_id)
            .bind(room_id)
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("disconnect peer failed: {err}")))?;
        let disconnected = result.rows_affected() > 0;
        if disconnected {
            sqlx::query(&decrement_participant_count_sql(&rm))
                .bind(room_id)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    Status::internal(format!("release room participant failed: {err}"))
                })?;
        }
        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("commit disconnect peer failed: {err}")))?;

        if disconnected {
            let tenant_id = tenant_id.to_string();
            let room_id = room_id.to_string();
            let peer_id = peer_id.to_string();
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                "udb.webrtc.peer.left.v1",
                &room_id,
                serde_json::json!({
                    "room_id": room_id.clone(),
                    "peer_id": peer_id.clone(),
                    "tenant_id": tenant_id.clone(),
                    "reason": reason,
                }),
                Some(&self.metrics),
            )
            .await;
            #[cfg(feature = "webrtc")]
            self.sfu.close_peer(&room_id, &peer_id).await;
        }
        Ok(disconnected)
    }

    /// Refresh the signaling heartbeat while also revalidating that the peer is
    /// still a connected member of an active room. Returns `false` when the pump
    /// should stop relaying frames.
    async fn touch_bound_peer_membership(
        &self,
        pool: &PgPool,
        tenant_id: Uuid,
        room_id: Uuid,
        peer_id: Uuid,
    ) -> Result<bool, Status> {
        let pm = peer_model();
        let rm = room_model();
        let result = sqlx::query(&touch_peer_membership_sql(&pm, &rm))
            .bind(peer_id)
            .bind(room_id)
            .bind(tenant_id)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("refresh peer heartbeat failed: {err}")))?;
        Ok(result.rows_affected() > 0)
    }

    async fn reap_stale_peers(
        &self,
        stale_after_secs: i64,
        batch_size: i64,
    ) -> Result<usize, Status> {
        if stale_after_secs <= 0 || batch_size <= 0 {
            return Ok(0);
        }
        let pool = self.require_pool()?;
        let pm = peer_model();
        let rm = room_model();
        let mut tx = pool.begin().await.map_err(|err| {
            Status::internal(format!("stale peer reaper transaction failed: {err}"))
        })?;
        let rows = sqlx::query(&stale_peer_reap_sql(&pm))
            .bind(stale_after_secs as f64)
            .bind(batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("stale peer reaper failed: {err}")))?;
        for row in &rows {
            let room_id = row
                .try_get::<String, _>("room_id")
                .ok()
                .and_then(|id| Uuid::parse_str(&id).ok());
            if let Some(room_id) = room_id {
                sqlx::query(&decrement_participant_count_sql(&rm))
                    .bind(room_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|err| {
                        Status::internal(format!(
                            "release stale peer room participant failed: {err}"
                        ))
                    })?;
            }
        }
        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("commit stale peer reaper failed: {err}")))?;

        for row in &rows {
            let peer_id = row.try_get::<String, _>("peer_id").unwrap_or_default();
            let room_id = row.try_get::<String, _>("room_id").unwrap_or_default();
            let tenant_id = row.try_get::<String, _>("tenant_id").unwrap_or_default();
            if !room_id.is_empty() {
                emit_payload_event(
                    pool,
                    self.outbox_relation.as_deref(),
                    "udb.webrtc.peer.left.v1",
                    &room_id,
                    serde_json::json!({
                        "room_id": room_id.clone(),
                        "peer_id": peer_id.clone(),
                        "tenant_id": tenant_id.clone(),
                        "reason": "stale_heartbeat",
                    }),
                    Some(&self.metrics),
                )
                .await;
                #[cfg(feature = "webrtc")]
                self.sfu.close_peer(&room_id, &peer_id).await;
            }
        }

        Ok(rows.len())
    }
}

impl Default for WebrtcServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

// ── models ────────────────────────────────────────────────────────────────────

fn room_model() -> NativeModel {
    native_model(
        ROOM_MSG,
        &[
            "room_id",
            "tenant_id",
            "name",
            "state",
            "max_participants",
            "participant_count",
            "config",
            "created_by",
            "deleted_at",
        ],
    )
}

fn peer_model() -> NativeModel {
    native_model(
        PEER_MSG,
        &[
            "peer_id",
            "room_id",
            "tenant_id",
            "display_name",
            "state",
            "metadata",
            "user_agent",
            "joined_at",
            "left_at",
            "deleted_at",
            // Auto-injected audit column (Peer has `audit_fields: true`); used as
            // the signaling last-seen heartbeat by the stale-peer reaper, not a
            // proto field on the Peer message.
            "updated_at",
        ],
    )
}

/// Idempotent CONNECTED→DISCONNECTED transition for a peer whose signaling
/// stream died without a LeaveRoom RPC. The `state = 'CONNECTED'` guard means
/// only the FIRST caller (inbound or outbound stream half) wins, so the
/// participant-count decrement runs exactly once.
fn disconnect_peer_sql(pm: &NativeModel) -> String {
    format!(
        "UPDATE {rel} SET {state} = 'DISCONNECTED', \
           {left_at} = COALESCE({left_at}, CURRENT_TIMESTAMP), \
           {updated_at} = CURRENT_TIMESTAMP \
         WHERE {peer_id} = $1::UUID AND {room_id} = $2::UUID AND {tenant_id} = $3::UUID \
           AND {deleted} IS NULL AND {state} = 'CONNECTED'",
        rel = pm.relation,
        state = pm.q("state"),
        left_at = pm.q("left_at"),
        updated_at = pm.q("updated_at"),
        peer_id = pm.q("peer_id"),
        room_id = pm.q("room_id"),
        tenant_id = pm.q("tenant_id"),
        deleted = pm.q("deleted_at"),
    )
}

/// Release one participant slot on a room (same SQL `leave_room` uses).
fn decrement_participant_count_sql(rm: &NativeModel) -> String {
    format!(
        "UPDATE {room_rel} SET {count} = GREATEST({count} - 1, 0) \
         WHERE {rid} = $1::UUID AND {rdeleted} IS NULL",
        room_rel = rm.relation,
        count = rm.q("participant_count"),
        rid = rm.q("room_id"),
        rdeleted = rm.q("deleted_at"),
    )
}

/// Combined heartbeat + membership re-validation for the signaling pump: bump
/// the peer's last-seen (`updated_at`) ONLY while the peer is still a CONNECTED
/// member of an ACTIVE room. 0 rows ⇒ membership is gone (left/kicked peer or
/// closed room) and the pump must stop relaying.
fn touch_peer_membership_sql(pm: &NativeModel, rm: &NativeModel) -> String {
    format!(
        "UPDATE {prel} p SET {pupdated} = CURRENT_TIMESTAMP \
         FROM {rrel} r \
         WHERE p.{ppid} = $1::UUID AND p.{prid} = $2::UUID AND p.{ptid} = $3::UUID \
           AND p.{pdel} IS NULL AND p.{pstate} = 'CONNECTED' \
           AND r.{rrid} = p.{prid} AND r.{rtid} = p.{ptid} \
           AND r.{rdel} IS NULL AND r.{rstate} = 'ACTIVE'",
        prel = pm.relation,
        rrel = rm.relation,
        pupdated = pm.q("updated_at"),
        ppid = pm.q("peer_id"),
        prid = pm.q("room_id"),
        ptid = pm.q("tenant_id"),
        pdel = pm.q("deleted_at"),
        pstate = pm.q("state"),
        rrid = rm.q("room_id"),
        rtid = rm.q("tenant_id"),
        rdel = rm.q("deleted_at"),
        rstate = rm.q("state"),
    )
}

/// Bounded stale-peer sweep: CONNECTED peers whose last-seen heartbeat
/// (`updated_at`) is older than the cutoff transition to DISCONNECTED. Returns
/// the disconnected (peer, room, tenant) tuples so the caller can release the
/// participant slots and emit events.
fn stale_peer_reap_sql(pm: &NativeModel) -> String {
    format!(
        "WITH doomed AS ( \
            SELECT {peer_id} AS doomed_peer_id FROM {rel} \
            WHERE {state} = 'CONNECTED' AND {deleted} IS NULL \
              AND {updated} < NOW() - make_interval(secs => $1::DOUBLE PRECISION) \
            ORDER BY {updated} \
            LIMIT $2 \
         ) \
         UPDATE {rel} p SET {state} = 'DISCONNECTED', \
           {left_at} = COALESCE({left_at}, CURRENT_TIMESTAMP), \
           {updated} = CURRENT_TIMESTAMP \
         FROM doomed d \
         WHERE p.{peer_id} = d.doomed_peer_id AND p.{state} = 'CONNECTED' \
         RETURNING p.{peer_id}::TEXT AS peer_id, p.{room_id}::TEXT AS room_id, \
                   p.{tenant_id}::TEXT AS tenant_id",
        rel = pm.relation,
        peer_id = pm.q("peer_id"),
        room_id = pm.q("room_id"),
        tenant_id = pm.q("tenant_id"),
        state = pm.q("state"),
        left_at = pm.q("left_at"),
        deleted = pm.q("deleted_at"),
        updated = pm.q("updated_at"),
    )
}

fn track_model() -> NativeModel {
    native_model(
        TRACK_MSG,
        &[
            "track_id",
            "room_id",
            "peer_id",
            "tenant_id",
            "kind",
            "label",
            "state",
            "settings",
            "metadata",
        ],
    )
}

use super::native_helpers::{non_empty_json, parse_uuid};

// ── enum<->db (VARCHAR(20), short token via the proto_enum serializer) ─────────

fn room_state_from_db(value: &str) -> i32 {
    use webrtc_entity_pb::RoomState as S;
    match value {
        "ACTIVE" | "ROOM_STATE_ACTIVE" => S::Active as i32,
        "IDLE" | "ROOM_STATE_IDLE" => S::Idle as i32,
        "CLOSED" | "ROOM_STATE_CLOSED" => S::Closed as i32,
        _ => S::Unspecified as i32,
    }
}

fn room_state_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "ACTIVE" | "ROOM_STATE_ACTIVE" => "ACTIVE",
        "IDLE" | "ROOM_STATE_IDLE" => "IDLE",
        "CLOSED" | "ROOM_STATE_CLOSED" => "CLOSED",
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown room state: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

fn peer_state_from_db(value: &str) -> i32 {
    use webrtc_entity_pb::PeerState as S;
    match value {
        "NEW" | "PEER_STATE_NEW" => S::New as i32,
        "CONNECTING" | "PEER_STATE_CONNECTING" => S::Connecting as i32,
        "CONNECTED" | "PEER_STATE_CONNECTED" => S::Connected as i32,
        "DISCONNECTED" | "PEER_STATE_DISCONNECTED" => S::Disconnected as i32,
        "FAILED" | "PEER_STATE_FAILED" => S::Failed as i32,
        "CLOSED" | "PEER_STATE_CLOSED" => S::Closed as i32,
        _ => S::Unspecified as i32,
    }
}

fn peer_state_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "NEW" | "PEER_STATE_NEW" => "NEW",
        "CONNECTING" | "PEER_STATE_CONNECTING" => "CONNECTING",
        "CONNECTED" | "PEER_STATE_CONNECTED" => "CONNECTED",
        "DISCONNECTED" | "PEER_STATE_DISCONNECTED" => "DISCONNECTED",
        "FAILED" | "PEER_STATE_FAILED" => "FAILED",
        "CLOSED" | "PEER_STATE_CLOSED" => "CLOSED",
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown peer state: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

fn track_kind_from_db(value: &str) -> i32 {
    use webrtc_entity_pb::TrackKind as K;
    match value {
        "AUDIO" | "TRACK_KIND_AUDIO" => K::Audio as i32,
        "VIDEO" | "TRACK_KIND_VIDEO" => K::Video as i32,
        "SCREEN" | "TRACK_KIND_SCREEN" => K::Screen as i32,
        "DATA" | "TRACK_KIND_DATA" => K::Data as i32,
        _ => K::Unspecified as i32,
    }
}

fn track_kind_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "AUDIO" | "TRACK_KIND_AUDIO" => "AUDIO",
        "VIDEO" | "TRACK_KIND_VIDEO" => "VIDEO",
        "SCREEN" | "TRACK_KIND_SCREEN" => "SCREEN",
        "DATA" | "TRACK_KIND_DATA" => "DATA",
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown track kind: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

fn track_state_from_db(value: &str) -> i32 {
    use webrtc_entity_pb::TrackState as S;
    match value {
        "ACTIVE" | "TRACK_STATE_ACTIVE" => S::Active as i32,
        "MUTED" | "TRACK_STATE_MUTED" => S::Muted as i32,
        "ENDED" | "TRACK_STATE_ENDED" => S::Ended as i32,
        _ => S::Unspecified as i32,
    }
}

// ── projections + row mappers ─────────────────────────────────────────────────

fn room_select_projection(m: &NativeModel) -> String {
    [
        m.text("room_id"),
        m.text("tenant_id"),
        m.select("name"),
        m.text_or_empty("state"),
        m.select("max_participants"),
        m.select("participant_count"),
        m.text_or_empty("config"),
        m.text_or_empty("created_by"),
    ]
    .join(", ")
}

fn room_from_row(row: &sqlx::postgres::PgRow) -> Result<webrtc_entity_pb::Room, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode room failed: {e}"));
    Ok(webrtc_entity_pb::Room {
        room_id: row.try_get("room_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        name: row.try_get("name").map_err(map)?,
        state: room_state_from_db(&row.try_get::<String, _>("state").map_err(map)?),
        max_participants: row.try_get::<i32, _>("max_participants").map_err(map)?,
        participant_count: row.try_get::<i32, _>("participant_count").map_err(map)?,
        config: row.try_get("config").map_err(map)?,
        created_by: row.try_get("created_by").map_err(map)?,
        ..Default::default()
    })
}

fn peer_select_projection(m: &NativeModel) -> String {
    [
        m.text("peer_id"),
        m.text("room_id"),
        m.text("tenant_id"),
        m.text_or_empty("display_name"),
        m.text_or_empty("state"),
        m.text_or_empty("metadata"),
        m.text_or_empty("user_agent"),
    ]
    .join(", ")
}

fn peer_from_row(row: &sqlx::postgres::PgRow) -> Result<webrtc_entity_pb::Peer, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode peer failed: {e}"));
    Ok(webrtc_entity_pb::Peer {
        peer_id: row.try_get("peer_id").map_err(map)?,
        room_id: row.try_get("room_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        display_name: row.try_get("display_name").map_err(map)?,
        state: peer_state_from_db(&row.try_get::<String, _>("state").map_err(map)?),
        metadata: row.try_get("metadata").map_err(map)?,
        user_agent: row.try_get("user_agent").map_err(map)?,
        ..Default::default()
    })
}

// `track_select_projection` / `track_from_row` (the raw PgRow track mappers)
// were removed when ListTracks/GetTrack moved to the typed read path below; the
// track row→proto mapping now lives in `track_from_json`. Room/peer keep their
// PgRow mappers because list_rooms + join_room still issue raw SQL (aggregate /
// atomic CAS — see P4.D).

// ── typed native-entity READ path (extend_udb.md P4) ──────────────────────────
// Room/peer/track READS (get_room/get_peer/list_peers/list_tracks) compile through
// the IR (`native_entity_read_for_service`) and dispatch to the proto-bound
// backend, instead of bespoke Postgres SELECTs — the same path
// `tenant_service::get_tenant` rides. The two STANDALONE inserts (create_room,
// publish_track) likewise ride the typed WRITE path (`native_entity_write_for_service`,
// JSONB via `LogicalValue::Json` + UUID via String, proven by storage/asset).
// Still raw (P4.D — not IR-expressible): the atomic/arithmetic lifecycle (join CAS,
// close-room txn, stale-peer reap, participant_count math, cross-table gates), the
// blind conditional UPDATEs (update_room/mute/unpublish/leave — the IR has no
// UPDATE-only op, only insert/upsert-on-PK), and `list_rooms`' exact `total_count`
// aggregate (precedent: `ListTenants`).

fn wv_eq(field: &str, value: &str) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String(value.to_string()),
    }
}

fn wv_asc(field: &str) -> LogicalSort {
    LogicalSort {
        field: field.to_string(),
        direction: SortDirection::Asc,
        nulls: NullOrder::Default,
    }
}

/// Read a string-ish field from a typed-read JSON row. JSONB columns come back
/// as nested JSON (object/array) and are re-stringified, matching the proto
/// `string` shape — the same coercion `tenant_service::json_string_field` uses.
fn webrtc_json_str(row: &serde_json::Value, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(value @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => {
            value.to_string()
        }
        _ => String::new(),
    }
}

fn webrtc_json_i32(row: &serde_json::Value, key: &str) -> i32 {
    row.get(key)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0) as i32
}

fn room_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "room_id",
            "tenant_id",
            "name",
            "state",
            "max_participants",
            "participant_count",
            "config",
            "created_by",
        ]
        .into_iter()
        .map(String::from),
    )
}

fn room_read_by_id(room_id: &str, tenant_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: ROOM_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            wv_eq("room_id", room_id),
            wv_eq("tenant_id", tenant_id),
            LogicalFilter::IsNull("deleted_at".to_string()),
        ])),
        projection: Some(room_projection()),
        sort: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn room_from_json(row: &serde_json::Value) -> webrtc_entity_pb::Room {
    webrtc_entity_pb::Room {
        room_id: webrtc_json_str(row, "room_id"),
        tenant_id: webrtc_json_str(row, "tenant_id"),
        name: webrtc_json_str(row, "name"),
        state: room_state_from_db(&webrtc_json_str(row, "state")),
        max_participants: webrtc_json_i32(row, "max_participants"),
        participant_count: webrtc_json_i32(row, "participant_count"),
        config: webrtc_json_str(row, "config"),
        created_by: webrtc_json_str(row, "created_by"),
        ..Default::default()
    }
}

fn peer_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "peer_id",
            "room_id",
            "tenant_id",
            "display_name",
            "state",
            "metadata",
            "user_agent",
        ]
        .into_iter()
        .map(String::from),
    )
}

fn peer_read_by_id(peer_id: &str, tenant_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: PEER_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            wv_eq("peer_id", peer_id),
            wv_eq("tenant_id", tenant_id),
            LogicalFilter::IsNull("deleted_at".to_string()),
        ])),
        projection: Some(peer_projection()),
        sort: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn peer_list_read(tenant_id: &str, room_id: &str, state_filter: &str) -> LogicalRead {
    let mut clauses = vec![
        wv_eq("tenant_id", tenant_id),
        wv_eq("room_id", room_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ];
    if !state_filter.trim().is_empty() {
        clauses.push(wv_eq("state", state_filter));
    }
    LogicalRead {
        message_type: PEER_MSG.to_string(),
        filter: Some(LogicalFilter::And(clauses)),
        projection: Some(peer_projection()),
        sort: vec![wv_asc("display_name")],
        pagination: Some(LogicalPagination::limit(MAX_LIST_ROWS as u32)),
    }
}

fn peer_from_json(row: &serde_json::Value) -> webrtc_entity_pb::Peer {
    webrtc_entity_pb::Peer {
        peer_id: webrtc_json_str(row, "peer_id"),
        room_id: webrtc_json_str(row, "room_id"),
        tenant_id: webrtc_json_str(row, "tenant_id"),
        display_name: webrtc_json_str(row, "display_name"),
        state: peer_state_from_db(&webrtc_json_str(row, "state")),
        metadata: webrtc_json_str(row, "metadata"),
        user_agent: webrtc_json_str(row, "user_agent"),
        ..Default::default()
    }
}

fn track_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "track_id",
            "room_id",
            "peer_id",
            "tenant_id",
            "kind",
            "label",
            "state",
            "settings",
            "metadata",
        ]
        .into_iter()
        .map(String::from),
    )
}

fn track_list_read(
    tenant_id: &str,
    room_id: &str,
    kind_filter: &str,
    peer_filter: &str,
) -> LogicalRead {
    let mut clauses = vec![wv_eq("tenant_id", tenant_id), wv_eq("room_id", room_id)];
    if !kind_filter.trim().is_empty() {
        clauses.push(wv_eq("kind", kind_filter));
    }
    if !peer_filter.trim().is_empty() {
        clauses.push(wv_eq("peer_id", peer_filter));
    }
    LogicalRead {
        message_type: TRACK_MSG.to_string(),
        filter: Some(LogicalFilter::And(clauses)),
        projection: Some(track_projection()),
        sort: vec![wv_asc("label")],
        pagination: Some(LogicalPagination::limit(MAX_LIST_ROWS as u32)),
    }
}

fn track_from_json(row: &serde_json::Value) -> webrtc_entity_pb::Track {
    webrtc_entity_pb::Track {
        track_id: webrtc_json_str(row, "track_id"),
        room_id: webrtc_json_str(row, "room_id"),
        peer_id: webrtc_json_str(row, "peer_id"),
        tenant_id: webrtc_json_str(row, "tenant_id"),
        kind: track_kind_from_db(&webrtc_json_str(row, "kind")),
        label: webrtc_json_str(row, "label"),
        state: track_state_from_db(&webrtc_json_str(row, "state")),
        settings: webrtc_json_str(row, "settings"),
        metadata: webrtc_json_str(row, "metadata"),
        ..Default::default()
    }
}

// ── typed native-entity WRITE records (extend_udb.md P4) ──────────────────────
// Only the two STANDALONE inserts (create_room, publish_track) ride the typed
// write path — the IR's `LogicalWrite` is insert/upsert-keyed-on-PK, so a blind
// conditional `UPDATE … WHERE` (update_room / mute / unpublish / leave's peer
// close) and the atomic lifecycle (join CAS / close-room txn / reap CTE) are NOT
// expressible without an unwanted INSERT or lost atomicity — those stay raw (P4.D).

/// Empty → SQL NULL (UUID columns reject `''::uuid`), else the id as a `String`
/// the per-backend compiler binds/casts to UUID (proven live by `storage_service`).
fn wv_uuid_or_null(value: &str) -> LogicalValue {
    let value = value.trim();
    if value.is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.to_string())
    }
}

/// A JSONB column value: parse the (always-valid, `{}`-defaulted) JSON text into
/// `LogicalValue::Json`, which the compiler binds as jsonb — the idiom
/// `asset_service`/`authz` already use. Invalid JSON fails closed exactly as the
/// raw `$n::JSONB` bind did.
fn wv_json(value: &str) -> Result<LogicalValue, Status> {
    serde_json::from_str::<serde_json::Value>(value)
        .map(LogicalValue::Json)
        .map_err(|err| Status::invalid_argument(format!("webrtc JSON field is invalid: {err}")))
}

fn room_create_record(
    room_id: &str,
    tenant_id: &str,
    req: &webrtc_pb::CreateRoomRequest,
) -> Result<LogicalRecord, Status> {
    let mut record = LogicalRecord::new();
    record.insert(
        "room_id".to_string(),
        LogicalValue::String(room_id.to_string()),
    );
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant_id.to_string()),
    );
    record.insert("name".to_string(), LogicalValue::String(req.name.clone()));
    record.insert(
        "state".to_string(),
        LogicalValue::String("ACTIVE".to_string()),
    );
    record.insert(
        "max_participants".to_string(),
        LogicalValue::Int(req.max_participants as i64),
    );
    record.insert("participant_count".to_string(), LogicalValue::Int(0));
    record.insert("config".to_string(), wv_json(&non_empty_json(&req.config))?);
    record.insert("created_by".to_string(), wv_uuid_or_null(&req.created_by));
    // `audit_info` (JSONB) + `deleted_*` omitted → DB defaults ('{}'::jsonb / NULL).
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
fn track_create_record(
    track_id: &str,
    tenant_id: &str,
    room_id: &str,
    peer_id: &str,
    req: &webrtc_pb::PublishTrackRequest,
    kind: &str,
) -> Result<LogicalRecord, Status> {
    let mut record = LogicalRecord::new();
    record.insert(
        "track_id".to_string(),
        LogicalValue::String(track_id.to_string()),
    );
    record.insert(
        "room_id".to_string(),
        LogicalValue::String(room_id.to_string()),
    );
    record.insert(
        "peer_id".to_string(),
        LogicalValue::String(peer_id.to_string()),
    );
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant_id.to_string()),
    );
    record.insert("kind".to_string(), LogicalValue::String(kind.to_string()));
    record.insert("label".to_string(), LogicalValue::String(req.label.clone()));
    record.insert(
        "state".to_string(),
        LogicalValue::String("ACTIVE".to_string()),
    );
    record.insert(
        "settings".to_string(),
        wv_json(&non_empty_json(&req.settings))?,
    );
    record.insert(
        "metadata".to_string(),
        wv_json(&non_empty_json(&req.metadata))?,
    );
    // `audit_info` (JSONB, NOT NULL DEFAULT '{}') omitted → DB default applies.
    Ok(record)
}

// ── RoomService ────────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl RoomService for WebrtcServiceImpl {
    async fn create_room(
        &self,
        request: Request<webrtc_pb::CreateRoomRequest>,
    ) -> Result<Response<webrtc_pb::CreateRoomResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        if req.tenant_id.trim().is_empty() || req.name.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id and name are required"));
        }
        // Per-tenant fair admission (held for the whole RPC).
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let room_id = Uuid::new_v4().to_string();
        let context = native_service_context(&metadata, &tenant_id, "");
        // Typed native-entity insert (extend_udb.md P4): the row (incl. its JSONB
        // `config` + UUID columns) compiles through the IR and dispatches to the
        // proto-bound backend — no bespoke SQL. `Error` conflict = plain INSERT.
        self.require_runtime()?
            .native_entity_write_for_service(
                "webrtc.room",
                &context,
                ROOM_MSG,
                room_create_record(&room_id, &tenant_id, &req)?,
                ConflictStrategy::Error,
            )
            .await?;
        // The outbox event is emitted post-insert on the pool (the raw path did
        // the same — not in the insert txn, so no atomicity change).
        if let Ok(pool) = self.require_pool() {
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                "udb.webrtc.room.created.v1",
                &room_id,
                serde_json::json!({
                    "room_id": room_id.clone(),
                    "tenant_id": tenant_id.clone(),
                    "name": req.name.clone(),
                }),
                Some(&self.metrics),
            )
            .await;
        }
        Ok(Response::new(webrtc_pb::CreateRoomResponse {
            room_id,
            message: "room created".to_string(),
            error: None,
        }))
    }

    async fn get_room(
        &self,
        request: Request<webrtc_pb::GetRoomRequest>,
    ) -> Result<Response<webrtc_pb::GetRoomResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let room_id = parse_uuid("room_id", &req.room_id)?.to_string();
        let context = native_service_context(&metadata, &tenant_id, "");
        let room = self
            .require_runtime()?
            .native_entity_read_for_service(
                "webrtc.room",
                &context,
                room_read_by_id(&room_id, &tenant_id),
            )
            .await?
            .first()
            .map(room_from_json)
            .ok_or_else(|| Status::not_found("room not found"))?;
        Ok(Response::new(webrtc_pb::GetRoomResponse {
            room: Some(room),
            error: None,
        }))
    }

    async fn update_room(
        &self,
        request: Request<webrtc_pb::UpdateRoomRequest>,
    ) -> Result<Response<webrtc_pb::UpdateRoomResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let room_id = parse_uuid("room_id", &req.room_id)?;
        let pool = self.require_pool()?;
        let m = room_model();
        let rel = m.relation.clone();
        let state = room_state_to_db(&req.state, "")?;
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {name} = COALESCE(NULLIF($3, ''), {name}), \
               {state} = COALESCE(NULLIF($4, ''), {state}), \
               {config} = CASE WHEN $5 = '' THEN {config} ELSE $5::JSONB END \
             WHERE {room_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            name = m.q("name"),
            state = m.q("state"),
            config = m.q("config"),
            room_id = m.q("room_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .bind(&req.name)
        .bind(&state)
        .bind(req.config.trim())
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("update room failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("room not found"));
        }
        Ok(Response::new(webrtc_pb::UpdateRoomResponse {
            message: "room updated".to_string(),
            error: None,
        }))
    }

    async fn close_room(
        &self,
        request: Request<webrtc_pb::CloseRoomRequest>,
    ) -> Result<Response<webrtc_pb::CloseRoomResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let room_id = parse_uuid("room_id", &req.room_id)?;
        let pool = self.require_pool()?;
        let m = room_model();
        let pm = peer_model();
        let tm = track_model();
        // Transitional P4 gate: close_room is a multi-entity transactional state
        // transition plus event fan-out. It must stay on a backend that can
        // express the room/peer/track updates atomically.
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| Status::internal(format!("close room transaction failed: {err}")))?;
        let peer_rows = sqlx::query(&format!(
            "SELECT {peer_id}::TEXT AS peer_id FROM {rel} \
             WHERE {room_id} = $1::UUID AND {tenant_id} = $2::UUID \
               AND {deleted} IS NULL AND {state} = 'CONNECTED' \
             ORDER BY {peer_id}",
            rel = pm.relation,
            peer_id = pm.q("peer_id"),
            room_id = pm.q("room_id"),
            tenant_id = pm.q("tenant_id"),
            deleted = pm.q("deleted_at"),
            state = pm.q("state"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("list room peers failed: {err}")))?;
        let track_rows = sqlx::query(&format!(
            "SELECT {track_id}::TEXT AS track_id FROM {rel} \
             WHERE {room_id} = $1::UUID AND {tenant_id} = $2::UUID AND {state} <> 'ENDED' \
             ORDER BY {track_id}",
            rel = tm.relation,
            track_id = tm.q("track_id"),
            room_id = tm.q("room_id"),
            tenant_id = tm.q("tenant_id"),
            state = tm.q("state"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("list room tracks failed: {err}")))?;
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {state} = 'CLOSED', {count} = 0 \
             WHERE {room_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            rel = m.relation,
            state = m.q("state"),
            count = m.q("participant_count"),
            room_id = m.q("room_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("close room failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("room not found"));
        }
        sqlx::query(&format!(
            "UPDATE {rel} SET {state} = 'CLOSED', {left_at} = COALESCE({left_at}, CURRENT_TIMESTAMP), \
               {deleted} = COALESCE({deleted}, CURRENT_TIMESTAMP) \
             WHERE {room_id} = $1::UUID AND {tenant_id} = $2::UUID \
               AND {deleted} IS NULL AND {state} <> 'CLOSED'",
            rel = pm.relation,
            state = pm.q("state"),
            left_at = pm.q("left_at"),
            deleted = pm.q("deleted_at"),
            room_id = pm.q("room_id"),
            tenant_id = pm.q("tenant_id"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("close room peers failed: {err}")))?;
        sqlx::query(&format!(
            "UPDATE {rel} SET {state} = 'ENDED' \
             WHERE {room_id} = $1::UUID AND {tenant_id} = $2::UUID AND {state} <> 'ENDED'",
            rel = tm.relation,
            state = tm.q("state"),
            room_id = tm.q("room_id"),
            tenant_id = tm.q("tenant_id"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("end room tracks failed: {err}")))?;
        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("commit close room failed: {err}")))?;
        let peer_ids = peer_rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>("peer_id").ok())
            .collect::<Vec<_>>();
        let track_ids = track_rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>("track_id").ok())
            .collect::<Vec<_>>();
        let tx = self.signaling.channel(&req.room_id).await;
        for peer_id in &peer_ids {
            tx.send_signal(webrtc_pb::SignalResponse {
                payload: Some(webrtc_pb::signal_response::Payload::PeerLeft(
                    peer_id.clone(),
                )),
            });
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                "udb.webrtc.peer.left.v1",
                &req.room_id,
                serde_json::json!({
                    "room_id": req.room_id.clone(),
                    "peer_id": peer_id,
                    "tenant_id": req.tenant_id.clone(),
                    "reason": "room_closed",
                }),
                Some(&self.metrics),
            )
            .await;
            #[cfg(feature = "webrtc")]
            self.sfu.close_peer(&req.room_id, peer_id).await;
        }
        for track_id in &track_ids {
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                "udb.webrtc.track.ended.v1",
                &req.room_id,
                serde_json::json!({
                    "room_id": req.room_id.clone(),
                    "track_id": track_id,
                    "tenant_id": req.tenant_id.clone(),
                    "reason": "room_closed",
                }),
                Some(&self.metrics),
            )
            .await;
            #[cfg(feature = "webrtc")]
            self.sfu.unregister_track(track_id).await;
        }
        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            "udb.webrtc.room.closed.v1",
            &req.room_id,
            serde_json::json!({
                "room_id": req.room_id.clone(),
                "tenant_id": req.tenant_id.clone(),
                "closed_peer_count": peer_ids.len(),
                "ended_track_count": track_ids.len(),
            }),
            Some(&self.metrics),
        )
        .await;
        drop(tx);
        self.signaling.close(&req.room_id).await;
        Ok(Response::new(webrtc_pb::CloseRoomResponse {
            message: "room closed".to_string(),
            error: None,
        }))
    }

    async fn list_rooms(
        &self,
        request: Request<webrtc_pb::ListRoomsRequest>,
    ) -> Result<Response<webrtc_pb::ListRoomsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let pool = self.require_pool()?;
        let m = room_model();
        let rel = m.relation.clone();
        let projection = room_select_projection(&m);
        let state_filter = room_state_to_db(&req.state, "")?;
        let page_size = if req.page_size > 0 { req.page_size } else { 50 }.min(500) as i64;
        let page = if req.page > 0 { req.page } else { 1 } as i64;
        let offset = (page - 1) * page_size;
        let where_clause = format!(
            "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL AND ($2 = '' OR {state} = $2)",
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
            state = m.q("state"),
        );
        // P4.D: kept raw. ListRooms returns an exact `total_count`; the typed
        // `LogicalRead` helper has no aggregate-count surface yet, so keep the
        // SQL count+page (precedent: `tenant_service::list_tenants`).
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(tenant_id)
            .bind(&state_filter)
            .fetch_one(pool)
            .await
            .map_err(|err| Status::internal(format!("count rooms failed: {err}")))?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} ORDER BY {name} LIMIT $3 OFFSET $4",
            name = m.q("name"),
        ))
        .bind(tenant_id)
        .bind(&state_filter)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list rooms failed: {err}")))?;
        let mut rooms = Vec::with_capacity(rows.len());
        for row in &rows {
            rooms.push(room_from_row(row)?);
        }
        Ok(Response::new(webrtc_pb::ListRoomsResponse {
            rooms,
            total_count: total as i32,
            error: None,
        }))
    }
}

// ── PeerService ──────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl PeerService for WebrtcServiceImpl {
    async fn join_room(
        &self,
        request: Request<webrtc_pb::JoinRoomRequest>,
    ) -> Result<Response<webrtc_pb::JoinRoomResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC) — one tenant's join
        // flood can't starve the shared control plane.
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let room_id = parse_uuid("room_id", &req.room_id)?;
        let pool = self.require_pool()?;
        let m = peer_model();
        let rel = m.relation.clone();
        let peer_id = Uuid::new_v4().to_string();
        let metadata = non_empty_json(&req.metadata);

        // Atomically claim a participant slot: increment participant_count only if
        // the room exists, belongs to the tenant, is open, and has capacity. 0 rows
        // ⇒ reject — no peer is inserted into a missing/foreign/closed/full room,
        // and capacity is enforced atomically (no TOCTOU).
        // Transitional P4 gate: this compare-and-increment is intentionally not
        // lowered to generic typed CRUD until the native path has a CAS/update
        // capability that every eligible backend can declare honestly.
        let rm = room_model();
        let claimed = sqlx::query(&format!(
            "UPDATE {room_rel} SET {count} = {count} + 1 \
             WHERE {rid} = $1::UUID AND {rtid} = $2::UUID AND {deleted} IS NULL \
               AND {state} <> 'CLOSED' AND ({max} = 0 OR {count} < {max})",
            room_rel = rm.relation,
            count = rm.q("participant_count"),
            rid = rm.q("room_id"),
            rtid = rm.q("tenant_id"),
            deleted = rm.q("deleted_at"),
            state = rm.q("state"),
            max = rm.q("max_participants"),
        ))
        .bind(room_id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("claim room slot failed: {err}")))?;
        if claimed.rows_affected() == 0 {
            return Err(Status::failed_precondition(
                "room not found, closed, or at capacity",
            ));
        }

        // Insert the peer; release the claimed slot on failure.
        if let Err(err) = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({peer_id}, {room_id}, {tenant_id}, {display_name}, {state}, {metadata}, {user_agent}, {joined_at}) \
             VALUES ($1::UUID, $2::UUID, $3::UUID, $4, 'CONNECTED', $5::JSONB, $6, CURRENT_TIMESTAMP)",
            peer_id = m.q("peer_id"),
            room_id = m.q("room_id"),
            tenant_id = m.q("tenant_id"),
            display_name = m.q("display_name"),
            state = m.q("state"),
            metadata = m.q("metadata"),
            user_agent = m.q("user_agent"),
            joined_at = m.q("joined_at"),
        ))
        .bind(&peer_id)
        .bind(room_id)
        .bind(tenant_id)
        .bind(&req.display_name)
        .bind(&metadata)
        .bind(&req.user_agent)
        .execute(pool)
        .await
        {
            let _ = sqlx::query(&format!(
                "UPDATE {room_rel} SET {count} = GREATEST({count} - 1, 0) \
                 WHERE {rid} = $1::UUID AND {deleted} IS NULL",
                room_rel = rm.relation,
                count = rm.q("participant_count"),
                rid = rm.q("room_id"),
                deleted = rm.q("deleted_at"),
            ))
            .bind(room_id)
            .execute(pool)
            .await;
            return Err(Status::internal(format!("join room failed: {err}")));
        }

        // Fetch the freshly-created peer + the other peers already in the room.
        let projection = peer_select_projection(&m);
        let self_row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {peer_id} = $1::UUID",
            peer_id = m.q("peer_id"),
        ))
        .bind(parse_uuid("peer_id", &peer_id)?)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("reload peer failed: {err}")))?;
        let peer = peer_from_row(&self_row)?;

        let other_rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {room_id} = $1::UUID AND {peer_id} <> $2::UUID \
               AND {deleted} IS NULL AND {state} <> 'CLOSED' \
             ORDER BY {display_name}",
            room_id = m.q("room_id"),
            peer_id = m.q("peer_id"),
            deleted = m.q("deleted_at"),
            state = m.q("state"),
            display_name = m.q("display_name"),
        ))
        .bind(room_id)
        .bind(parse_uuid("peer_id", &peer_id)?)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list existing peers failed: {err}")))?;
        let mut existing_peers = Vec::with_capacity(other_rows.len());
        for row in &other_rows {
            existing_peers.push(peer_from_row(row)?);
        }

        // Notify the room's signaling subscribers that a peer joined.
        let tx = self.signaling.channel(&req.room_id).await;
        tx.send_signal(webrtc_pb::SignalResponse {
            payload: Some(webrtc_pb::signal_response::Payload::PeerJoined(
                peer_id.clone(),
            )),
        });

        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            "udb.webrtc.peer.joined.v1",
            &req.room_id,
            serde_json::json!({
                "room_id": req.room_id.clone(),
                "peer_id": peer_id.clone(),
                "tenant_id": req.tenant_id.clone(),
            }),
            Some(&self.metrics),
        )
        .await;

        Ok(Response::new(webrtc_pb::JoinRoomResponse {
            peer: Some(peer),
            existing_peers,
            error: None,
        }))
    }

    async fn leave_room(
        &self,
        request: Request<webrtc_pb::LeaveRoomRequest>,
    ) -> Result<Response<webrtc_pb::LeaveRoomResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let room_id = parse_uuid("room_id", &req.room_id)?;
        let peer_id = parse_uuid("peer_id", &req.peer_id)?;
        let pool = self.require_pool()?;
        let m = peer_model();
        let rel = m.relation.clone();
        // Transitional P4 gate: leave_room couples a guarded peer transition with
        // a room counter decrement and signaling teardown; keep the durable part
        // on the current transactional Postgres path.
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {state} = 'CLOSED', {left_at} = CURRENT_TIMESTAMP, \
               {deleted} = CURRENT_TIMESTAMP \
             WHERE {peer_id} = $1::UUID AND {room_id} = $2::UUID AND {tenant_id} = $3::UUID \
               AND {deleted} IS NULL",
            state = m.q("state"),
            left_at = m.q("left_at"),
            deleted = m.q("deleted_at"),
            peer_id = m.q("peer_id"),
            room_id = m.q("room_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(peer_id)
        .bind(room_id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("leave room failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Ok(Response::new(webrtc_pb::LeaveRoomResponse {
                success: false,
                error: None,
            }));
        }
        let rm = room_model();
        let _ = sqlx::query(&format!(
            "UPDATE {room_rel} SET {count} = GREATEST({count} - 1, 0) \
             WHERE {rid} = $1::UUID AND {rdeleted} IS NULL",
            room_rel = rm.relation,
            count = rm.q("participant_count"),
            rid = rm.q("room_id"),
            rdeleted = rm.q("deleted_at"),
        ))
        .bind(room_id)
        .execute(pool)
        .await;

        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            "udb.webrtc.peer.left.v1",
            &req.room_id,
            serde_json::json!({
                "room_id": req.room_id.clone(),
                "peer_id": req.peer_id.clone(),
                "tenant_id": req.tenant_id.clone(),
            }),
            Some(&self.metrics),
        )
        .await;

        let tx = self.signaling.channel(&req.room_id).await;
        tx.send_signal(webrtc_pb::SignalResponse {
            payload: Some(webrtc_pb::signal_response::Payload::PeerLeft(
                req.peer_id.clone(),
            )),
        });
        drop(tx);
        // Targeted teardown: end ONLY the leaving peer's subscriber stream.
        // The room's signaling hub stays up for every other connected peer —
        // whole-hub teardown belongs to `close_room` alone.
        self.signaling.close_peer(&req.room_id, &req.peer_id).await;
        self.signaling.prune_if_idle(&req.room_id).await;
        #[cfg(feature = "webrtc")]
        self.sfu
            .close_peer(&room_id.to_string(), &peer_id.to_string())
            .await;

        Ok(Response::new(webrtc_pb::LeaveRoomResponse {
            success: true,
            error: None,
        }))
    }

    async fn get_peer(
        &self,
        request: Request<webrtc_pb::GetPeerRequest>,
    ) -> Result<Response<webrtc_pb::GetPeerResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let peer_id = parse_uuid("peer_id", &req.peer_id)?.to_string();
        let context = native_service_context(&metadata, &tenant_id, "");
        let peer = self
            .require_runtime()?
            .native_entity_read_for_service(
                "webrtc.room",
                &context,
                peer_read_by_id(&peer_id, &tenant_id),
            )
            .await?
            .first()
            .map(peer_from_json)
            .ok_or_else(|| Status::not_found("peer not found"))?;
        Ok(Response::new(webrtc_pb::GetPeerResponse {
            peer: Some(peer),
            error: None,
        }))
    }

    async fn list_peers(
        &self,
        request: Request<webrtc_pb::ListPeersRequest>,
    ) -> Result<Response<webrtc_pb::ListPeersResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let room_id = parse_uuid("room_id", &req.room_id)?.to_string();
        // Validated/normalized into the short DB token before it reaches the IR
        // filter ("" = no state filter), preserving the raw path's semantics.
        let state_filter = peer_state_to_db(&req.state, "")?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let peers = self
            .require_runtime()?
            .native_entity_read_for_service(
                "webrtc.room",
                &context,
                peer_list_read(&tenant_id, &room_id, &state_filter),
            )
            .await?
            .iter()
            .map(peer_from_json)
            .collect();
        Ok(Response::new(webrtc_pb::ListPeersResponse {
            peers,
            error: None,
        }))
    }
}

// ── TrackService ─────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl TrackService for WebrtcServiceImpl {
    async fn publish_track(
        &self,
        request: Request<webrtc_pb::PublishTrackRequest>,
    ) -> Result<Response<webrtc_pb::PublishTrackResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC).
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let room_id = parse_uuid("room_id", &req.room_id)?;
        let peer_id = parse_uuid("peer_id", &req.peer_id)?;
        let pool = self.require_pool()?;
        self.require_active_peer_membership(pool, tenant_id, room_id, peer_id)
            .await?;
        let track_id = Uuid::new_v4().to_string();
        let kind = track_kind_to_db(&req.kind, "VIDEO")?;
        let tenant_str = tenant_id.to_string();
        let context = native_service_context(&metadata, &tenant_str, "");
        // Typed native-entity insert (extend_udb.md P4): the track row incl. its
        // JSONB settings/metadata + UUID columns compiles through the IR and
        // dispatches to the proto-bound backend. The peer-membership gate above
        // stays raw (cross-table JOIN, not IR-expressible — P4.D).
        self.require_runtime()?
            .native_entity_write_for_service(
                "webrtc.room",
                &context,
                TRACK_MSG,
                track_create_record(
                    &track_id,
                    &tenant_str,
                    &room_id.to_string(),
                    &peer_id.to_string(),
                    &req,
                    &kind,
                )?,
                ConflictStrategy::Error,
            )
            .await?;

        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            "udb.webrtc.track.published.v1",
            &req.room_id,
            serde_json::json!({
                "room_id": req.room_id.clone(),
                "peer_id": req.peer_id.clone(),
                "track_id": track_id.clone(),
                "kind": kind.clone(),
            }),
            Some(&self.metrics),
        )
        .await;

        let tx = self.signaling.channel(&req.room_id).await;
        tx.send_signal(webrtc_pb::SignalResponse {
            payload: Some(webrtc_pb::signal_response::Payload::TrackPublished(
                track_id.clone(),
            )),
        });
        #[cfg(feature = "webrtc")]
        self.sfu
            .register_published_track(&req.room_id, &req.peer_id, &track_id, &kind)
            .await;

        Ok(Response::new(webrtc_pb::PublishTrackResponse {
            track_id,
            message: "track published".to_string(),
            error: None,
        }))
    }

    async fn unpublish_track(
        &self,
        request: Request<webrtc_pb::UnpublishTrackRequest>,
    ) -> Result<Response<webrtc_pb::UnpublishTrackResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let track_id = parse_uuid("track_id", &req.track_id)?;
        let pool = self.require_pool()?;
        let m = track_model();
        let rel = m.relation.clone();
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {state} = 'ENDED' \
             WHERE {track_id} = $1::UUID AND {tenant_id} = $2::UUID",
            state = m.q("state"),
            track_id = m.q("track_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(track_id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("unpublish track failed: {err}")))?;
        #[cfg(feature = "webrtc")]
        if result.rows_affected() > 0 {
            self.sfu.unregister_track(&req.track_id).await;
        }
        Ok(Response::new(webrtc_pb::UnpublishTrackResponse {
            success: result.rows_affected() > 0,
            error: None,
        }))
    }

    async fn mute_track(
        &self,
        request: Request<webrtc_pb::MuteTrackRequest>,
    ) -> Result<Response<webrtc_pb::MuteTrackResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let track_id = parse_uuid("track_id", &req.track_id)?;
        let pool = self.require_pool()?;
        let m = track_model();
        let rel = m.relation.clone();
        let new_state = if req.muted { "MUTED" } else { "ACTIVE" };
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {state} = $3 \
             WHERE {track_id} = $1::UUID AND {tenant_id} = $2::UUID AND {state} <> 'ENDED'",
            state = m.q("state"),
            track_id = m.q("track_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(track_id)
        .bind(tenant_id)
        .bind(new_state)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("mute track failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("track not found or ended"));
        }
        Ok(Response::new(webrtc_pb::MuteTrackResponse {
            message: format!("track {new_state}"),
            error: None,
        }))
    }

    async fn list_tracks(
        &self,
        request: Request<webrtc_pb::ListTracksRequest>,
    ) -> Result<Response<webrtc_pb::ListTracksResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let room_id = parse_uuid("room_id", &req.room_id)?.to_string();
        // Normalize the optional kind filter to its short DB token ("" = any);
        // the optional peer filter stays the raw id (empty = any), matching the
        // raw path which only applied it when non-empty.
        let kind_filter = track_kind_to_db(&req.kind, "")?;
        let peer_filter = req.peer_id.trim().to_string();
        let context = native_service_context(&metadata, &tenant_id, "");
        let tracks = self
            .require_runtime()?
            .native_entity_read_for_service(
                "webrtc.room",
                &context,
                track_list_read(&tenant_id, &room_id, &kind_filter, &peer_filter),
            )
            .await?
            .iter()
            .map(track_from_json)
            .collect();
        Ok(Response::new(webrtc_pb::ListTracksResponse {
            tracks,
            error: None,
        }))
    }
}

// ── TurnService ──────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl TurnService for WebrtcServiceImpl {
    async fn issue_credentials(
        &self,
        request: Request<webrtc_pb::IssueCredentialsRequest>,
    ) -> Result<Response<webrtc_pb::IssueCredentialsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = self.admit(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let room_id = parse_uuid("room_id", &req.room_id)?;
        let peer_id = parse_uuid("peer_id", &req.peer_id)?;
        let pool = self.require_pool()?;
        self.require_active_peer_membership(pool, tenant_id, room_id, peer_id)
            .await?;
        let ttl = if req.ttl_seconds > 0 {
            (req.ttl_seconds as i64).min(self.turn.default_ttl)
        } else {
            self.turn.default_ttl
        };
        let now = chrono::Utc::now().timestamp();
        let expiry = now + ttl;
        // username = "<expiry>:<principal>" per the coturn long-term-secret REST
        // scheme; bind the principal to the verified tenant/room/peer tuple and
        // the only action this credential authorizes: TURN media relay.
        let principal = format!(
            "{}:{}:{}:{}",
            req.tenant_id.trim(),
            req.room_id.trim(),
            req.peer_id.trim(),
            TURN_RELAY_ACTION
        );
        // Fail closed when no TURN secret is configured (production with no
        // UDB_TURN_SECRET) rather than minting against a well-known dev secret.
        let secret = self.turn.secret.as_deref().ok_or_else(|| {
            Status::failed_precondition(
                "TURN secret not configured; set UDB_TURN_SECRET to issue credentials",
            )
        })?;
        let (username, credential) =
            crate::runtime::security::turn_rest_credential(secret, &principal, expiry);
        let ice = webrtc_pb::IceServer {
            urls: self.turn.urls.clone(),
            username: username.clone(),
            credential: credential.clone(),
        };
        Ok(Response::new(webrtc_pb::IssueCredentialsResponse {
            ice_servers: vec![ice],
            username,
            credential,
            ttl_seconds: ttl as i32,
            expires_at: Some(prost_types::Timestamp {
                seconds: expiry,
                nanos: 0,
            }),
            allowed_action: TURN_RELAY_ACTION.to_string(),
            error: None,
        }))
    }
}

// ── SignalingService (bidi relay) ──────────────────────────────────────────────

/// Map an inbound signaling request to the response broadcast to the room. A
/// `ping` is answered with `pong`; SDP/ICE payloads are forwarded verbatim so
/// every peer in the room sees them (mesh signaling — clients address the
/// relevant peer at the application layer).
fn signal_to_response(req: &webrtc_pb::SignalRequest) -> Option<webrtc_pb::SignalResponse> {
    use webrtc_pb::signal_request::Payload as In;
    use webrtc_pb::signal_response::Payload as Out;
    let out = match req.payload.as_ref()? {
        In::OfferSdp(s) => Out::OfferSdp(s.clone()),
        In::AnswerSdp(s) => Out::AnswerSdp(s.clone()),
        In::IceCandidate(s) => Out::IceCandidate(s.clone()),
        In::Ping(_) => Out::Pong(true),
    };
    Some(webrtc_pb::SignalResponse { payload: Some(out) })
}

/// What the outbound pump does with one received hub frame.
enum FrameDisposition {
    /// Relay the signaling payload to this subscriber.
    Yield(webrtc_pb::SignalResponse),
    /// Frame addressed to a different peer — keep this subscriber's stream alive.
    Skip,
    /// End THIS subscriber's stream.
    End,
}

/// Outbound-pump frame filter: [`HubFrame::PeerClosed`] ends ONLY the stream
/// of the peer it addresses (`leave_room` targeted teardown), while
/// [`HubFrame::RoomClosed`] ends every subscriber (`close_room` whole-hub
/// teardown). `bound_peer` is the peer_id this subscriber stream is bound to.
fn dispose_hub_frame(frame: HubFrame, bound_peer: &str) -> FrameDisposition {
    match frame {
        HubFrame::Signal(resp) => FrameDisposition::Yield(resp),
        HubFrame::PeerClosed(peer_id) if peer_id == bound_peer => FrameDisposition::End,
        HubFrame::PeerClosed(_) => FrameDisposition::Skip,
        HubFrame::RoomClosed => FrameDisposition::End,
    }
}

#[tonic::async_trait]
impl SignalingService for WebrtcServiceImpl {
    type SignalStream =
        Pin<Box<dyn Stream<Item = Result<webrtc_pb::SignalResponse, Status>> + Send>>;

    async fn signal(
        &self,
        request: Request<tonic::Streaming<webrtc_pb::SignalRequest>>,
    ) -> Result<Response<Self::SignalStream>, Status> {
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        // The first message establishes which room this peer is signaling in.
        let first = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("signaling stream error: {e}")))?
            .ok_or_else(|| Status::invalid_argument("empty signaling stream"))?;
        validate_request_tenant(&metadata, &first.tenant_id)?;
        // Per-tenant fair admission at stream ENTRY only: bound the RATE of new
        // signaling connections a single tenant can open so one tenant can't
        // exhaust the shared signaling/control plane. The permit is dropped
        // immediately below (released before the long-lived relay starts) — it is
        // NOT held across the stream, so it cannot deadlock the Signal RPC.
        let _admit = self.admit(&first.tenant_id).await?;
        // Admission: the connection binds to (tenant_id, room_id, peer_id) from
        // the first frame; the peer must be connected in an active room.
        let (bound_tenant, bound_room, bound_peer) =
            self.require_active_signal_membership(&first).await?;
        let bound_tenant_uuid = parse_uuid("tenant_id", &bound_tenant)?;
        let bound_room_uuid = parse_uuid("room_id", &bound_room)?;
        let bound_peer_uuid = parse_uuid("peer_id", &bound_peer)?;
        let pool = self.require_pool()?.clone();
        if !self
            .touch_bound_peer_membership(&pool, bound_tenant_uuid, bound_room_uuid, bound_peer_uuid)
            .await?
        {
            return Err(Status::permission_denied(
                "peer is not an active member of this room",
            ));
        }
        drop(_admit); // release the entry permit; do NOT hold it across the stream
        let tx = self.signaling.channel(&bound_room).await;
        let rx = tx.subscribe();

        // Publish the first message, then pump the rest of the inbound stream —
        // ignoring frames that try to switch the bound (room_id, peer_id) tuple.
        if let Some(resp) = self.signal_response_for(&first).await {
            tx.send_signal(resp);
        }
        let tx_in = tx.clone();
        let svc = self.clone();
        let hub_in = self.signaling.clone();
        let cleanup_room_in = bound_room.clone();
        let pool_in = pool.clone();
        let bound_tenant_in = bound_tenant.clone();
        let bound_room_in = bound_room.clone();
        let bound_peer_in = bound_peer.clone();
        tokio::spawn(async move {
            let mut frames_since_check = 0_u64;
            let mut heartbeat =
                tokio::time::interval(Duration::from_secs(SIGNAL_MEMBERSHIP_RECHECK_SECS));
            loop {
                if tx_in.is_closed() {
                    break;
                }
                tokio::select! {
                    maybe_msg = inbound.message() => {
                        let msg = match maybe_msg {
                            Ok(Some(msg)) => msg,
                            Ok(None) => break,
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    room_id = %bound_room_in,
                                    peer_id = %bound_peer_in,
                                    "signaling inbound stream failed"
                                );
                                break;
                            }
                        };
                        let same_room =
                            msg.room_id.trim().is_empty()
                                || msg.room_id.trim() == bound_room_in.as_str();
                        let same_peer =
                            msg.peer_id.trim().is_empty()
                                || msg.peer_id.trim() == bound_peer_in.as_str();
                        let same_tenant = msg.tenant_id.trim().is_empty()
                            || msg.tenant_id.trim() == bound_tenant_in.as_str();
                        if !same_room || !same_peer || !same_tenant {
                            continue; // reject frames that rebind to another room/peer
                        }
                        frames_since_check = frames_since_check.saturating_add(1);
                        if frames_since_check >= SIGNAL_MEMBERSHIP_RECHECK_FRAMES {
                            match svc
                                .touch_bound_peer_membership(
                                    &pool_in,
                                    bound_tenant_uuid,
                                    bound_room_uuid,
                                    bound_peer_uuid,
                                )
                                .await
                            {
                                Ok(true) => {
                                    frames_since_check = 0;
                                }
                                Ok(false) => break,
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        room_id = %bound_room_in,
                                        peer_id = %bound_peer_in,
                                        "signaling membership revalidation failed"
                                    );
                                    break;
                                }
                            }
                        }
                        if let Some(resp) = svc.signal_response_for(&msg).await {
                            tx_in.send_signal(resp);
                        }
                    }
                    _ = heartbeat.tick() => {
                        match svc
                            .touch_bound_peer_membership(
                                &pool_in,
                                bound_tenant_uuid,
                                bound_room_uuid,
                                bound_peer_uuid,
                            )
                            .await
                        {
                            Ok(true) => {
                                frames_since_check = 0;
                            }
                            Ok(false) => break,
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    room_id = %bound_room_in,
                                    peer_id = %bound_peer_in,
                                    "signaling heartbeat failed"
                                );
                                break;
                            }
                        }
                    }
                }
            }
            match svc
                .disconnect_bound_peer(
                    &pool_in,
                    bound_tenant_uuid,
                    bound_room_uuid,
                    bound_peer_uuid,
                    "signal_stream_ended",
                )
                .await
            {
                Ok(true) => tx_in.send_signal(webrtc_pb::SignalResponse {
                    payload: Some(webrtc_pb::signal_response::Payload::PeerLeft(
                        bound_peer_in.clone(),
                    )),
                }),
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    error = %err,
                    room_id = %bound_room_in,
                    peer_id = %bound_peer_in,
                    "signaling inbound cleanup failed"
                ),
            }
            drop(tx_in);
            hub_in.prune_if_idle(&cleanup_room_in).await;
        });

        let hub_out = self.signaling.clone();
        let cleanup_room_out = bound_room.clone();
        let svc_out = self.clone();
        let pool_out = pool.clone();
        let bound_peer_out = bound_peer.clone();
        let out = async_stream::try_stream! {
            let mut rx = rx;
            loop {
                match rx.recv().await {
                    Ok(frame) => match dispose_hub_frame(frame, &bound_peer_out) {
                        FrameDisposition::Yield(resp) => yield resp,
                        FrameDisposition::Skip => continue,
                        FrameDisposition::End => break,
                    },
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            match svc_out
                .disconnect_bound_peer(
                    &pool_out,
                    bound_tenant_uuid,
                    bound_room_uuid,
                    bound_peer_uuid,
                    "signal_stream_outbound_ended",
                )
                .await
            {
                Ok(true) => tx.send_signal(webrtc_pb::SignalResponse {
                    payload: Some(webrtc_pb::signal_response::Payload::PeerLeft(
                        bound_peer_out,
                    )),
                }),
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    error = %err,
                    room_id = %cleanup_room_out,
                    peer_id = %bound_peer_out,
                    "signaling outbound cleanup failed"
                ),
            }
            drop(rx);
            drop(tx);
            hub_out.prune_if_idle(&cleanup_room_out).await;
        };
        Ok(Response::new(Box::pin(out)))
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[tokio::test]
    async fn signaling_hub_close_broadcasts_terminal_frame_and_removes_room() {
        let hub = SignalingHub::default();
        let channel = hub.channel("room-a").await;
        let mut rx = channel.subscribe();

        hub.close("room-a").await;

        assert!(channel.is_closed());
        match tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("terminal frame should arrive")
            .expect("broadcast should be open long enough for terminal frame")
        {
            HubFrame::RoomClosed => {}
            HubFrame::Signal(_) | HubFrame::PeerClosed(_) => {
                panic!("expected room-closed terminal frame")
            }
        }
        assert!(!hub.rooms.lock().await.contains_key("room-a"));

        let replacement = hub.channel("room-a").await;
        assert!(!replacement.is_closed());
    }

    /// FIX-43: `leave_room` performs a TARGETED teardown via
    /// `SignalingHub::close_peer` — only the leaving peer's subscriber stream
    /// ends; the other peers' streams keep relaying and the room hub survives.
    #[tokio::test]
    async fn leave_room_targeted_close_ends_only_the_leaving_peers_stream() {
        let hub = SignalingHub::default();
        let channel = hub.channel("room-a").await;
        let mut rx_leaver = channel.subscribe();
        let mut rx_stayer = channel.subscribe();

        // The hub-side call leave_room makes for the leaving peer only.
        hub.close_peer("room-a", "peer-leaving").await;

        // Both subscribers receive the broadcast frame, but the outbound-pump
        // filter ends ONLY the addressed peer's stream…
        let frame_leaver = tokio::time::timeout(Duration::from_millis(100), rx_leaver.recv())
            .await
            .expect("targeted frame should arrive")
            .expect("broadcast should stay open");
        assert!(matches!(
            dispose_hub_frame(frame_leaver, "peer-leaving"),
            FrameDisposition::End
        ));
        // …while the staying peer skips it and keeps its stream alive.
        let frame_stayer = tokio::time::timeout(Duration::from_millis(100), rx_stayer.recv())
            .await
            .expect("targeted frame should arrive")
            .expect("broadcast should stay open");
        assert!(matches!(
            dispose_hub_frame(frame_stayer, "peer-staying"),
            FrameDisposition::Skip
        ));

        // The room hub is NOT torn down: later signals still reach the
        // remaining peer (whole-hub teardown stays with close_room).
        assert!(!channel.is_closed());
        assert!(hub.rooms.lock().await.contains_key("room-a"));
        channel.send_signal(webrtc_pb::SignalResponse {
            payload: Some(webrtc_pb::signal_response::Payload::Pong(true)),
        });
        let frame_next = tokio::time::timeout(Duration::from_millis(100), rx_stayer.recv())
            .await
            .expect("relayed signal should arrive")
            .expect("broadcast should stay open");
        assert!(matches!(
            dispose_hub_frame(frame_next, "peer-staying"),
            FrameDisposition::Yield(_)
        ));

        // RoomClosed (close_room) still ends EVERY subscriber's stream.
        hub.close("room-a").await;
        let frame_closed = tokio::time::timeout(Duration::from_millis(100), rx_stayer.recv())
            .await
            .expect("terminal frame should arrive")
            .expect("broadcast should stay open");
        assert!(matches!(
            dispose_hub_frame(frame_closed, "peer-staying"),
            FrameDisposition::End
        ));
    }

    /// FIX-42: stream-death cleanup — when a signaling pump ends without a
    /// LeaveRoom RPC, `disconnect_bound_peer` (the REAL cleanup function both
    /// pump halves call) must mark the peer DISCONNECTED, release exactly one
    /// participant slot, and stay idempotent when the racing inbound/outbound
    /// halves both invoke it. Needs live Postgres (the function is one SQL
    /// transaction), so this follows the `live_tests` env-gated pattern: same
    /// DSN env chain as `live_tests::support::live_pg_dsn`, idempotent
    /// proto-generated DDL (`CREATE … IF NOT EXISTS`).
    #[tokio::test]
    #[ignore = "requires live Postgres; run with cargo test --lib live_postgres_webrtc_stream_death_cleanup_disconnects_peer -- --ignored --nocapture"]
    async fn live_postgres_webrtc_stream_death_cleanup_disconnects_peer() {
        let dsn = std::env::var("UDB_LIVE_NATIVE_PG_DSN")
            .or_else(|_| std::env::var("UDB_LIVE_AUTH_PG_DSN"))
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&dsn)
            .await
            .unwrap_or_else(|err| panic!("connect live webrtc postgres at {dsn}: {err}"));
        for stmt in crate::runtime::native_catalog::native_service_catalog_ddl() {
            sqlx::raw_sql(&stmt)
                .execute(&pool)
                .await
                .unwrap_or_else(|err| panic!("native service DDL failed: {err}\nSQL:\n{stmt}"));
        }
        let svc = WebrtcServiceImpl::new().with_postgres(Some(pool.clone()));
        let tenant_id = Uuid::new_v4().to_string();

        let room = svc
            .create_room(Request::new(webrtc_pb::CreateRoomRequest {
                tenant_id: tenant_id.clone(),
                name: "stream-death-cleanup".to_string(),
                max_participants: 10,
                ..Default::default()
            }))
            .await
            .expect("create_room")
            .into_inner();
        let join = svc
            .join_room(Request::new(webrtc_pb::JoinRoomRequest {
                tenant_id: tenant_id.clone(),
                room_id: room.room_id.clone(),
                display_name: "Ada".to_string(),
                ..Default::default()
            }))
            .await
            .expect("join_room")
            .into_inner();
        let peer = join.peer.expect("peer");

        let tenant_uuid = Uuid::parse_str(&tenant_id).expect("tenant uuid");
        let room_uuid = Uuid::parse_str(&room.room_id).expect("room uuid");
        let peer_uuid = Uuid::parse_str(&peer.peer_id).expect("peer uuid");

        // The pump-end cleanup path (one stream half wins)…
        let first = svc
            .disconnect_bound_peer(
                &pool,
                tenant_uuid,
                room_uuid,
                peer_uuid,
                "signal_stream_ended",
            )
            .await
            .expect("disconnect bound peer");
        assert!(first, "first cleanup must transition the peer and win");
        // …and the racing other half is an idempotent no-op.
        let second = svc
            .disconnect_bound_peer(
                &pool,
                tenant_uuid,
                room_uuid,
                peer_uuid,
                "signal_stream_outbound_ended",
            )
            .await
            .expect("idempotent disconnect");
        assert!(!second, "second cleanup must not double-release the slot");

        let got_peer = svc
            .get_peer(Request::new(webrtc_pb::GetPeerRequest {
                tenant_id: tenant_id.clone(),
                peer_id: peer.peer_id.clone(),
                ..Default::default()
            }))
            .await
            .expect("get_peer")
            .into_inner()
            .peer
            .expect("peer");
        assert_eq!(
            got_peer.state,
            webrtc_entity_pb::PeerState::Disconnected as i32,
            "stream-death cleanup must mark the peer DISCONNECTED"
        );
        let got_room = svc
            .get_room(Request::new(webrtc_pb::GetRoomRequest {
                tenant_id: tenant_id.clone(),
                room_id: room.room_id.clone(),
            }))
            .await
            .expect("get_room")
            .into_inner();
        assert_eq!(
            got_room.room.expect("room").participant_count,
            0,
            "stream-death cleanup must release exactly one participant slot"
        );
    }

    #[test]
    fn disconnect_sql_is_connected_only_and_updates_last_seen_fields() {
        let pm = peer_model();
        let rm = room_model();
        let disconnect = disconnect_peer_sql(&pm);
        assert!(disconnect.contains("DISCONNECTED"));
        assert!(disconnect.contains("COALESCE"));
        assert!(disconnect.contains("CURRENT_TIMESTAMP"));
        assert!(disconnect.contains("AND \"state\" = 'CONNECTED'"));

        let decrement = decrement_participant_count_sql(&rm);
        assert!(decrement.contains("GREATEST"));
        assert!(decrement.contains("- 1"));
    }

    #[test]
    fn heartbeat_and_stale_reaper_sql_are_bounded_and_membership_gated() {
        let pm = peer_model();
        let rm = room_model();
        let touch = touch_peer_membership_sql(&pm, &rm);
        assert!(touch.contains("CURRENT_TIMESTAMP"));
        assert!(touch.contains("p.\"state\" = 'CONNECTED'"));
        assert!(touch.contains("r.\"state\" = 'ACTIVE'"));

        let reap = stale_peer_reap_sql(&pm);
        assert!(reap.contains("make_interval"));
        assert!(reap.contains("LIMIT $2"));
        assert!(reap.contains("RETURNING"));
        assert!(reap.contains("DISCONNECTED"));
    }

    /// A caller scoped to tenant-a must not read another tenant's room by putting a
    /// foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any pool/DB access (no Postgres needed).
    #[tokio::test]
    async fn get_room_rejects_cross_tenant_body() {
        let svc = WebrtcServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(webrtc_pb::GetRoomRequest {
            tenant_id: "tenant-b".to_string(),
            room_id: "00000000-0000-0000-0000-000000000001".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_room(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

impl DataBrokerService {
    /// Build the native WebRTC service, wired to the broker's Postgres pool.
    pub(crate) fn build_webrtc_service(&self) -> WebrtcServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("webrtc.room", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        let svc = WebrtcServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime.clone()))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone());

        let interval_secs = std::env::var("UDB_WEBRTC_REAP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(STALE_PEER_REAP_INTERVAL_SECS_DEFAULT);
        let stale_after_secs = std::env::var("UDB_WEBRTC_STALE_PEER_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(STALE_PEER_TIMEOUT_SECS_DEFAULT);
        if interval_secs > 0 && stale_after_secs > 0 && svc.pg_pool.is_some() {
            let reaper = svc.clone();
            let singleton_pool = svc.pg_pool.clone().expect("checked above");
            let singleton_relation = runtime.config().cdc.lock_log_relation();
            crate::runtime::service::native_runtime::NativeWorkerHost::spawn_while_leader(
                crate::runtime::singleton::WORKER_WEBRTC_STALE_PEER_REAPER,
                "webrtc stale-peer reaper disconnected peers",
                singleton_pool,
                singleton_relation,
                Duration::from_secs(interval_secs),
                move || {
                    let reaper_once = reaper.clone();
                    async move {
                        reaper_once
                            .reap_stale_peers(stale_after_secs, STALE_PEER_REAP_BATCH_SIZE)
                            .await
                            .map(|n| n as i64)
                    }
                },
            );
        }

        svc
    }
}
