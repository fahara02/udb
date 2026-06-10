//! `ControlPlaneService` — Phase 9 versioned, ACK/NACK, nonce-paired, ordered
//! control-plane policy/config distribution (xDS-style).
//!
//! Structure mirrors the other native services (`idp`, `tenant_service`): proto
//! is the source of truth, all SQL goes through [`native_catalog`], NO in-memory
//! state (the versioned registry and the per-node ACK/NACK ledger are both
//! Postgres-backed), and every RPC has a real handler that fails closed without a
//! pool. Submodules:
//!   * [`resources`] — pure resource model, dependency ordering, content hashing.
//!   * [`store`]     — durable registry + node-state CRUD over `udb_control.*`.
//!
//! Streaming model (`StreamResources`): the node opens an aggregated stream and
//! sends `DiscoveryRequest`s that double as ACK (version_info + matching nonce)
//! or NACK (error_detail present). The server pushes `DiscoveryResponse`s per
//! resource_type in [`resources::ordered_resource_types`] order — backend-target
//! DEFINITIONS before the routing/RLS POLICIES that reference them
//! (make-before-break) — each carrying a fresh nonce, only when the node's
//! accepted version differs from the current registry version. A poll loop on a
//! short interval wakes the stream to push new versions; an ACK advances the
//! node's applied version, a NACK keeps its last-good version (it does NOT
//! silently diverge).

pub(crate) mod canary;
pub(crate) mod resources;
pub(crate) mod scaling;
pub(crate) mod sourcing;
pub(crate) mod store;
pub(crate) mod subscriber;

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::proto::udb::core::control::entity::v1::ResourceType;
use crate::proto::udb::core::control::services::v1 as control_pb;
use control_pb::control_plane_service_server::ControlPlaneService;

pub use crate::proto::udb::core::control::services::v1::control_plane_service_server::ControlPlaneServiceServer;

use super::DataBrokerService;
use super::mappings::{bounded_page_response, bounded_page_window, timestamp_from_unix};

use resources::{ResourceModel, aggregate_version, ordered_resource_types, resource_type_from_i32};

/// How often the aggregated stream re-checks the registry for a new version to
/// push (the change-notify hook). A focused follow-up replaces this poll with a
/// debounced watch/broadcast notify; the poll keeps the core honest meanwhile.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Postgres-backed `ControlPlaneService` handler.
#[derive(Clone)]
pub struct ControlPlaneServiceImpl {
    pg_pool: Option<PgPool>,
    /// Phase 9 scaling: shared across all node streams so the push loop can wake
    /// the instant the registry changes (instead of only on the poll tick).
    reload: Option<Arc<tokio::sync::Notify>>,
    /// Phase 9 scaling: shared concurrent-push throttle (+ debounce) so a fleet
    /// of nodes cannot stampede the registry on a burst of version bumps.
    scaler: Option<Arc<scaling::PushScaler>>,
}

impl ControlPlaneServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            reload: None,
            scaler: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Wire the reload-notify so the push loop wakes on registry changes.
    pub fn with_reload(mut self, reload: Arc<tokio::sync::Notify>) -> Self {
        self.reload = Some(reload);
        self
    }

    /// Wire the shared push debouncer/throttle (Phase 9 scaling knobs).
    pub fn with_scaler(mut self, scaler: Arc<scaling::PushScaler>) -> Self {
        self.scaler = Some(scaler);
        self
    }

    /// Control-plane distribution is durable-only: fail closed without a pool.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "control-plane service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for ControlPlaneServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

// ── proto projections ──────────────────────────────────────────────────────────

fn resource_to_pb(model: &ResourceModel) -> control_pb::Resource {
    control_pb::Resource {
        name: model.name.clone(),
        version: if model.version.is_empty() {
            model.content_hash.clone()
        } else {
            model.version.clone()
        },
        payload_json: model.payload_json.clone(),
        resource_type: model.resource_type_enum() as i32,
    }
}

fn node_state_to_pb(row: &store::NodeStateRow, current_world: &str) -> control_pb::NodeAckState {
    control_pb::NodeAckState {
        node_id: row.node_id.clone(),
        resource_type: store::resource_type_of(&row.resource_type) as i32,
        subscribed_names: store::parse_subscribed_names(&row.subscribed_names_json),
        accepted_version: row.accepted_version.clone(),
        last_good_version: row.last_good_version.clone(),
        last_response_nonce: row.last_response_nonce.clone(),
        nack_error_detail: row.nack_error_detail.clone(),
        in_sync: !row.accepted_version.is_empty() && row.accepted_version == current_world,
        updated_at: timestamp_from_unix(row.updated_at_unix.max(0) as u64),
    }
}

type DiscoveryStream =
    Pin<Box<dyn Stream<Item = Result<control_pb::DiscoveryResponse, Status>> + Send>>;
type DeltaStream =
    Pin<Box<dyn Stream<Item = Result<control_pb::DeltaDiscoveryResponse, Status>> + Send>>;

// ── handler impl ───────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl ControlPlaneService for ControlPlaneServiceImpl {
    type StreamResourcesStream = DiscoveryStream;
    type DeltaResourcesStream = DeltaStream;

    /// Aggregated state-of-the-world distribution with xDS ACK/NACK + ordering.
    async fn stream_resources(
        &self,
        request: Request<tonic::Streaming<control_pb::DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamResourcesStream>, Status> {
        let pool = self.require_pool()?.clone();
        let mut inbound = request.into_inner();

        // The first request establishes the node identity (and any initial
        // subscription). Everything after it is an ACK/NACK or a subscription
        // change for the same node.
        let first = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("control stream error: {e}")))?
            .ok_or_else(|| Status::invalid_argument("empty control discovery stream"))?;
        let node_id = first.node_id.trim().to_string();
        if node_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id is required on the first DiscoveryRequest",
            ));
        }

        // Seed ledger rows for every distributed type so the poll loop has a
        // baseline, then apply the first request as an ACK/NACK/subscription.
        for rt in ordered_resource_types() {
            store::ensure_node_state(&pool, &node_id, *rt, &[]).await?;
        }
        apply_inbound(&pool, &node_id, &first).await?;

        // Inbound pump: durably record every subsequent ACK/NACK/subscription.
        let inbound_pool = pool.clone();
        let inbound_node = node_id.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                // Bind to the first node_id; ignore frames that try to rebind.
                if !msg.node_id.trim().is_empty() && msg.node_id.trim() != inbound_node {
                    continue;
                }
                let _ = apply_inbound(&inbound_pool, &inbound_node, &msg).await;
            }
        });

        // Outbound push loop: for each resource_type in dependency order, when the
        // node's accepted version differs from the current world version, send a
        // fresh response (definitions first, then dependent policies).
        let out_pool = pool.clone();
        let out_node = node_id.clone();
        let reload = self.reload.clone();
        let scaler = self.scaler.clone();
        let out = async_stream::try_stream! {
            // Track what we last SENT (pending, not yet acked) per type so we do
            // not re-emit the same version on every poll tick.
            let mut last_sent: std::collections::BTreeMap<i32, String> =
                std::collections::BTreeMap::new();
            loop {
                for rt in ordered_resource_types() {
                    let rt = *rt;
                    // The node's current subscription set + applied version.
                    let state = store::get_node_state(&out_pool, &out_node, rt)
                        .await?
                        .unwrap_or_default();
                    let names = store::parse_subscribed_names(&state.subscribed_names_json);
                    let resources = store::list_resources(&out_pool, rt, None, &names).await?;
                    let world = aggregate_version(&resources);

                    let already_applied = state.accepted_version == world;
                    let already_sent = last_sent.get(&(rt as i32)) == Some(&world);
                    if already_applied || already_sent {
                        continue;
                    }
                    // make-before-break: allocate a fresh nonce, mark it as the
                    // node's last_response_nonce, and push this type's resources.
                    let nonce = store::next_response_nonce(&out_pool, &out_node, rt).await?;
                    let pb_resources: Vec<control_pb::Resource> =
                        resources.iter().map(resource_to_pb).collect();
                    // Phase 9 scaling: a concurrent-push permit caps how many node
                    // streams push simultaneously (held across the yield/send).
                    let _permit = match scaler.as_ref() {
                        Some(s) => Some(s.throttle.acquire().await),
                        None => None,
                    };
                    last_sent.insert(rt as i32, world.clone());
                    yield control_pb::DiscoveryResponse {
                        resource_type: rt as i32,
                        version_info: world,
                        nonce,
                        resources: pb_resources,
                        removed_resources: Vec::new(),
                    };
                }
                // Wake on the poll tick OR an out-of-band registry-change notify
                // (the subscriber fires the latter so changes propagate promptly).
                // When a notify arrives, route it through the scaler's debouncer so
                // a BURST of registry bumps coalesces into a single push (within
                // DEBOUNCE_MS) bounded by the DEBOUNCE_MAX_MS ceiling — this is what
                // makes `inc_control_debounce_coalesced` fire and stops a churning
                // registry from stampeding every open node stream.
                match reload.as_ref() {
                    Some(n) => {
                        tokio::select! {
                            _ = tokio::time::sleep(STREAM_POLL_INTERVAL) => {}
                            _ = n.notified() => {
                                if let Some(s) = scaler.as_ref() {
                                    // Record this bump, then coalesce the rest of the
                                    // burst before recomputing/pushing below.
                                    s.debouncer.notify();
                                    s.debouncer.wait().await;
                                }
                            }
                        }
                    }
                    None => tokio::time::sleep(STREAM_POLL_INTERVAL).await,
                }
            }
        };
        Ok(Response::new(Box::pin(out)))
    }

    /// Incremental/delta distribution. Computes the minimal add/remove set against
    /// the versions the node already holds (`initial_resource_versions`).
    async fn delta_resources(
        &self,
        request: Request<tonic::Streaming<control_pb::DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaResourcesStream>, Status> {
        let pool = self.require_pool()?.clone();
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("control delta stream error: {e}")))?
            .ok_or_else(|| Status::invalid_argument("empty control delta stream"))?;
        let node_id = first.node_id.trim().to_string();
        if node_id.is_empty() {
            return Err(Status::invalid_argument(
                "node_id is required on the first DeltaDiscoveryRequest",
            ));
        }
        for rt in ordered_resource_types() {
            store::ensure_node_state(&pool, &node_id, *rt, &[]).await?;
        }
        apply_inbound_delta(&pool, &node_id, &first).await?;

        // The node's last-known per-resource versions, seeded from the first
        // request and namespaced by resource_type (the first frame's type).
        let seed_rt = resource_type_from_i32(first.resource_type);
        let mut known: std::collections::HashMap<String, String> = first
            .initial_resource_versions
            .iter()
            .map(|(name, ver)| (delta_key(seed_rt, name), ver.clone()))
            .collect();

        let inbound_pool = pool.clone();
        let inbound_node = node_id.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                if !msg.node_id.trim().is_empty() && msg.node_id.trim() != inbound_node {
                    continue;
                }
                let _ = apply_inbound_delta(&inbound_pool, &inbound_node, &msg).await;
            }
        });

        let out_pool = pool.clone();
        let out_node = node_id.clone();
        let out = async_stream::try_stream! {
            loop {
                for rt in ordered_resource_types() {
                    let rt = *rt;
                    let state = store::get_node_state(&out_pool, &out_node, rt)
                        .await?
                        .unwrap_or_default();
                    let names = store::parse_subscribed_names(&state.subscribed_names_json);
                    let resources = store::list_resources(&out_pool, rt, None, &names).await?;
                    let world = aggregate_version(&resources);

                    // Compute the minimal delta vs the node's known versions.
                    let mut changed: Vec<control_pb::Resource> = Vec::new();
                    let live_names: std::collections::BTreeSet<String> =
                        resources.iter().map(|r| r.name.clone()).collect();
                    for r in &resources {
                        let key = delta_key(rt, &r.name);
                        let current = if r.version.is_empty() {
                            r.content_hash.clone()
                        } else {
                            r.version.clone()
                        };
                        if known.get(&key).map(String::as_str) != Some(current.as_str()) {
                            changed.push(resource_to_pb(r));
                            known.insert(key, current);
                        }
                    }
                    // Anything the node knew (for this type) that is no longer live
                    // is removed.
                    let removed: Vec<String> = known
                        .keys()
                        .filter(|k| k.starts_with(&delta_prefix(rt)))
                        .map(|k| delta_name(rt, k))
                        .filter(|name| !live_names.contains(name))
                        .collect();
                    for name in &removed {
                        known.remove(&delta_key(rt, name));
                    }
                    if changed.is_empty() && removed.is_empty() {
                        continue;
                    }
                    let nonce = store::next_response_nonce(&out_pool, &out_node, rt).await?;
                    yield control_pb::DeltaDiscoveryResponse {
                        resource_type: rt as i32,
                        nonce,
                        resources: changed,
                        removed_resources: removed,
                        system_version_info: world,
                    };
                }
                tokio::time::sleep(STREAM_POLL_INTERVAL).await;
            }
        };
        Ok(Response::new(Box::pin(out)))
    }

    /// On-demand fetch (incl. by tenant) — no streaming, no ACK bookkeeping.
    async fn get_resources(
        &self,
        request: Request<control_pb::GetResourcesRequest>,
    ) -> Result<Response<control_pb::GetResourcesResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        let rt = resource_type_from_i32(req.resource_type);
        if rt == ResourceType::Unspecified {
            return Err(Status::invalid_argument("resource_type is required"));
        }
        let tenant = if req.tenant_id.trim().is_empty() {
            None
        } else {
            Some(req.tenant_id.as_str())
        };
        let resources = store::list_resources(pool, rt, tenant, &req.resource_names).await?;
        let version = aggregate_version(&resources);
        let (limit, offset, _) = bounded_page_window(req.page.as_ref());
        let total = resources.len();
        let windowed: Vec<control_pb::Resource> = resources
            .iter()
            .skip(offset)
            .take(limit)
            .map(resource_to_pb)
            .collect();
        Ok(Response::new(control_pb::GetResourcesResponse {
            resources: windowed,
            version_info: version,
            page: Some(bounded_page_response(total, req.page.as_ref())),
        }))
    }

    async fn list_node_states(
        &self,
        request: Request<control_pb::ListNodeStatesRequest>,
    ) -> Result<Response<control_pb::ListNodeStatesResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        let rt = resource_type_from_i32(req.resource_type);
        let node = if req.node_id.trim().is_empty() {
            None
        } else {
            Some(req.node_id.as_str())
        };
        let (limit, offset, _) = bounded_page_window(req.page.as_ref());
        let (rows, total) =
            store::list_node_states(pool, node, rt, limit as i64, offset as i64).await?;
        // Resolve each row's current world version for the in_sync flag.
        let mut states = Vec::with_capacity(rows.len());
        for row in &rows {
            let row_rt = store::resource_type_of(&row.resource_type);
            let names = store::parse_subscribed_names(&row.subscribed_names_json);
            let world = store::world_version(pool, row_rt, None, &names).await?;
            states.push(node_state_to_pb(row, &world));
        }
        Ok(Response::new(control_pb::ListNodeStatesResponse {
            node_states: states,
            page: Some(bounded_page_response(total as usize, req.page.as_ref())),
        }))
    }

    async fn ack_status(
        &self,
        request: Request<control_pb::AckStatusRequest>,
    ) -> Result<Response<control_pb::AckStatusResponse>, Status> {
        let pool = self.require_pool()?;
        let req = request.into_inner();
        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }
        let rt = resource_type_from_i32(req.resource_type);
        if rt == ResourceType::Unspecified {
            return Err(Status::invalid_argument("resource_type is required"));
        }
        let row = store::get_node_state(pool, req.node_id.trim(), rt)
            .await?
            .ok_or_else(|| Status::not_found("no node state for this (node, resource_type)"))?;
        let names = store::parse_subscribed_names(&row.subscribed_names_json);
        let world = store::world_version(pool, rt, None, &names).await?;
        let acknowledged = row.accepted_version == world && row.nack_error_detail.is_empty();
        let nacked = !row.nack_error_detail.is_empty();
        Ok(Response::new(control_pb::AckStatusResponse {
            node_state: Some(node_state_to_pb(&row, &world)),
            current_version: world,
            acknowledged,
            nacked,
        }))
    }
}

// ── inbound application (ACK / NACK / subscription) ─────────────────────────────

/// Apply one aggregated `DiscoveryRequest` against the durable ledger:
///   * subscription change — `resource_names` non-empty re-seeds the node's
///     subscription for this type;
///   * NACK — `error_detail` present → record_nack (keeps last_good, does NOT
///     advance accepted_version);
///   * ACK — `version_info` + matching `response_nonce` → record_ack (advances
///     accepted_version + last_good, clears nack).
async fn apply_inbound(
    pool: &PgPool,
    node_id: &str,
    req: &control_pb::DiscoveryRequest,
) -> Result<(), Status> {
    let rt = resource_type_from_i32(req.resource_type);
    if rt == ResourceType::Unspecified {
        // A bare first frame with no type just establishes the node; nothing to do.
        return Ok(());
    }
    // (Re)establish the subscription set for this type when the node sent one.
    if !req.resource_names.is_empty() {
        store::ensure_node_state(pool, node_id, rt, &req.resource_names).await?;
    } else {
        store::ensure_node_state(pool, node_id, rt, &[]).await?;
    }
    match &req.error_detail {
        Some(err) if !err.message.trim().is_empty() || err.code != 0 => {
            let detail =
                serde_json::json!({ "code": err.code, "message": err.message }).to_string();
            store::record_nack(pool, node_id, rt, &req.response_nonce, &detail).await?;
        }
        _ if !req.version_info.trim().is_empty() => {
            store::record_ack(pool, node_id, rt, &req.version_info, &req.response_nonce).await?;
        }
        _ => {}
    }
    Ok(())
}

/// Delta variant of [`apply_inbound`]: same ACK/NACK semantics; subscription
/// changes are additive/subtractive (subscribe/unsubscribe name lists).
async fn apply_inbound_delta(
    pool: &PgPool,
    node_id: &str,
    req: &control_pb::DeltaDiscoveryRequest,
) -> Result<(), Status> {
    let rt = resource_type_from_i32(req.resource_type);
    if rt == ResourceType::Unspecified {
        return Ok(());
    }
    // Merge subscribe/unsubscribe into the durable subscription set.
    let existing = store::get_node_state(pool, node_id, rt)
        .await?
        .map(|r| store::parse_subscribed_names(&r.subscribed_names_json))
        .unwrap_or_default();
    let mut set: std::collections::BTreeSet<String> = existing.into_iter().collect();
    for name in &req.resource_names_subscribe {
        if !name.trim().is_empty() {
            set.insert(name.trim().to_string());
        }
    }
    for name in &req.resource_names_unsubscribe {
        set.remove(name.trim());
    }
    let names: Vec<String> = set.into_iter().collect();
    store::ensure_node_state(pool, node_id, rt, &names).await?;

    match &req.error_detail {
        Some(err) if !err.message.trim().is_empty() || err.code != 0 => {
            let detail =
                serde_json::json!({ "code": err.code, "message": err.message }).to_string();
            store::record_nack(pool, node_id, rt, &req.response_nonce, &detail).await?;
        }
        _ if !req.response_nonce.trim().is_empty() => {
            // A delta ACK acknowledges the nonce; the applied version is the
            // current world for the node's subscription (computed at ack time).
            let world = store::world_version(pool, rt, None, &names).await?;
            store::record_ack(pool, node_id, rt, &world, &req.response_nonce).await?;
        }
        _ => {}
    }
    Ok(())
}

fn delta_prefix(rt: ResourceType) -> String {
    format!("{}\u{1f}", rt as i32)
}

fn delta_key(rt: ResourceType, name: &str) -> String {
    format!("{}\u{1f}{}", rt as i32, name)
}

fn delta_name(_rt: ResourceType, key: &str) -> String {
    key.split('\u{1f}').nth(1).unwrap_or("").to_string()
}

// ── service wiring ──────────────────────────────────────────────────────────────

impl DataBrokerService {
    /// Build the native `ControlPlaneService`, wired to the broker's Postgres
    /// pool (the versioned registry + per-node ACK/NACK ledger live there). Fails
    /// closed (every RPC errors) when no pool is configured.
    pub(crate) fn build_control_plane_service(&self) -> ControlPlaneServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        // Phase 9 scaling knobs (debounce/coalesce + concurrent-push throttle),
        // shared across every node stream so a fleet cannot stampede the registry.
        let scaler = Arc::new(scaling::PushScaler::from_env(Some(self.metrics.clone())));
        let mut svc = ControlPlaneServiceImpl::new()
            .with_postgres(pg_pool.clone())
            .with_scaler(scaler);
        if let Some(pool) = pg_pool {
            let config = Arc::new(runtime.config().clone());
            // Populate the versioned registry from real config at startup so the
            // service distributes actual backend-targets / native-enablement /
            // method-security / routing / RLS policy (not an empty registry).
            let seed_pool = pool.clone();
            let seed_cfg = config.clone();
            tokio::spawn(async move {
                if let Err(e) = sourcing::resync(&seed_pool, seed_cfg.as_ref()).await {
                    tracing::warn!(error = %e, "control-plane initial resource sync failed");
                }
            });
            // Keep the registry fresh and WAKE the push streams the instant a
            // change is applied (the subscriber owns the shared reload notify).
            // Wire the authz bundle-version probe so an authz-only policy/tuple
            // change (which need not alter the sourced RLS/method-security
            // resources) still flips the change signal and triggers a fleet
            // push — this is exactly the gap the subscriber exists to close
            // (`udb.authz.policy.bundle.invalidated.v1` emitted, no node reloads).
            let abac_cell = self.abac_snapshot();
            let authz_version: subscriber::AuthzVersionFn = Arc::new(move || {
                abac_cell
                    .read()
                    .map(|snapshot| {
                        format!("{}:{}", snapshot.version, snapshot.relationship_version)
                    })
                    .unwrap_or_default()
            });
            let handle = subscriber::SubscriberHandle::new(pool, config)
                .with_metrics(self.metrics.clone())
                .with_authz_version(authz_version);
            svc = svc.with_reload(handle.reload_notify());
            subscriber::spawn_control_plane_subscriber(handle);
        }
        svc
    }
}
