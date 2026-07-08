//! Typed consistency model (U6).
//!
//! Pre-U6, "consistency" was a stringly-typed grab-bag passed through
//! `RequestContext.consistency: String`, then re-parsed at six different
//! call sites (replica router, cache, projection lag check, ABAC export
//! controls, etc.). The 4-mode lint allowlist in `generation::lint`
//! diverged from the runtime's 2-mode check in `core::accessors`. Adding a
//! sixth mode meant editing strings in six places and hoping you found
//! all of them.
//!
//! U6 centralises consistency as a typed [`ConsistencyMode`] with seven
//! pinned variants, plus the two primitives that make cross-backend
//! "read-your-writes" actually work:
//!
//! - [`WriteReceipt`] — what the broker hands back on every write. Carries
//!   the Postgres WAL LSN (for replica fencing), the outbox sequence
//!   number (for projection fencing), and the set of projection task IDs
//!   spawned by the write.
//! - [`ReadFence`] — what a reading client can ATTACH to a follow-up read
//!   to demand "wait until the active replica has caught up past this
//!   LSN AND all these projection tasks have completed, otherwise return
//!   a stale-read warning."
//!
//! These types are intentionally executor-agnostic; the runtime layer
//! decides how to honour them per backend (Postgres replica router waits
//! on LSN; Mongo / Qdrant projection-backed reads wait on the projection
//! task ID; Redis cache check-and-refresh against the manifest checksum).

use serde::{Deserialize, Serialize};

/// The seven pinned consistency modes a request may declare.
///
/// **Pinned**: the `as_str` tokens are public contract — SDK enums,
/// `x-udb-consistency` header values, lint allowlist, ABAC policy
/// matching all key on them. Don't rename a variant without updating
/// every SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyMode {
    /// Read from the primary, no replica fallback. Default for writes
    /// and for reads that explicitly demand linearizability.
    Strong,
    /// Read your own writes — broker routes to primary OR fences the
    /// replica past the caller's `WriteReceipt.source_lsn`. The most
    /// common production read mode for "I just wrote, give me what I
    /// wrote" sessions.
    ReadYourWrites,
    /// Read from any replica that's caught up within
    /// `RequestContext.max_replica_lag_ms`. Bounded staleness in the
    /// sense the Spanner / Cosmos DB papers use it.
    BoundedStaleness,
    /// **REPLICA_BOUNDED** — explicitly route the read to a physical read
    /// REPLICA within a bounded staleness budget, and FAIL OVER to the
    /// primary (never error, never serve silently-stale) when the replica
    /// cannot be proven fresh in time.
    ///
    /// Distinct from [`Self::BoundedStaleness`] in two ways:
    /// 1. It targets SQL read replicas only — it does NOT opt into
    ///    projection-backed targets (`allows_projection()` is `false`), so a
    ///    bounded read here is always served by a real replica or the
    ///    primary, never a Mongo/Qdrant/ClickHouse projection.
    /// 2. On a backend with no real replication-position token (object
    ///    store, plain cache) it FAILS OVER to the primary rather than
    ///    refusing — the primary trivially satisfies the read.
    ///
    /// ## Timeout / failover contract
    ///
    /// Honoured by
    /// [`crate::runtime::replica::PgReplicaManager::choose_bounded_replica`]:
    /// pick a replica within `max_replica_lag_ms` lag, then WAIT on its REAL
    /// applied replication position (`pg_last_wal_replay_lsn()`, a GTID, or a
    /// monotone outbox seq — NEVER a wall clock) until it passes the caller's
    /// write LSN, using `max_replica_lag_ms` as the wait budget. If the fence
    /// does not clear inside that budget (or no replica is eligible), the
    /// broker FAILS OVER to the primary and ALWAYS attaches a
    /// [`StaleReadWarning`] (`ReplicaLagExceeded` when no replica was
    /// eligible, `FenceTimedOut` when the chosen replica didn't catch up) —
    /// a failed-over read is never returned silently. Decided on the real
    /// replication token, NOT a wall clock.
    ReplicaBounded,
    /// Read from any healthy replica or projection target. No fence,
    /// no lag bound. The cheapest mode; required for high-fanout reads.
    Eventual,
    /// Reads served by projection-backed targets (Mongo / Qdrant /
    /// ClickHouse / Neo4j) are acceptable. Distinct from `Eventual`
    /// because projection-OK reads still wait on any
    /// `ReadFence.projection_task_ids` the caller supplies — the
    /// caller opts into staleness BUT not past their own writes.
    ProjectionOk,
    /// Redis cache hits are acceptable. Even cheaper than
    /// `ProjectionOk` because cache reads skip the broker → backend
    /// round-trip entirely, but the cache layer verifies the entry's
    /// `manifest_checksum` matches the current catalog before serving
    /// (no stale-schema responses on this path).
    CacheOk,
}

impl Default for ConsistencyMode {
    fn default() -> Self {
        Self::Strong
    }
}

impl ConsistencyMode {
    /// Pinned wire token. SDK enums and `x-udb-consistency` header use
    /// these exact strings; changing one breaks every client.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::ReadYourWrites => "read_your_writes",
            Self::BoundedStaleness => "bounded_staleness",
            Self::ReplicaBounded => "replica_bounded",
            Self::Eventual => "eventual",
            Self::ProjectionOk => "projection_ok",
            Self::CacheOk => "cache_ok",
        }
    }

    /// Parse a wire token. Accepts the canonical `as_str` form plus a
    /// handful of legacy aliases (`linearizable` ↔ `strong`,
    /// `eventual_consistency` ↔ `eventual`) that pre-date U6. Returns
    /// `None` for unknown tokens so the caller can surface
    /// `InvalidArgument` rather than silently defaulting.
    pub fn parse(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "strong" | "linearizable" | "primary" => Some(Self::Strong),
            "read_your_writes" | "ryw" | "read-your-writes" => Some(Self::ReadYourWrites),
            "bounded_staleness" | "bounded-staleness" => Some(Self::BoundedStaleness),
            "replica_bounded" | "replica-bounded" => Some(Self::ReplicaBounded),
            "eventual" | "eventual_consistency" => Some(Self::Eventual),
            "projection_ok" | "projection-ok" => Some(Self::ProjectionOk),
            "cache_ok" | "cache-ok" => Some(Self::CacheOk),
            _ => None,
        }
    }

    /// Lenient parse that falls back to [`ConsistencyMode::default`]
    /// (`Strong`) when the token is unknown or empty. The legacy
    /// runtime paths default-to-strong on blank values; preserves that.
    pub fn parse_or_default(token: &str) -> Self {
        Self::parse(token).unwrap_or_default()
    }

    /// True when the request is willing to serve replica reads.
    pub fn allows_replica(self) -> bool {
        !matches!(self, Self::Strong | Self::ReadYourWrites)
    }

    /// True when the request is willing to read from a
    /// projection-backed target (Mongo doc, Qdrant vector, etc).
    pub fn allows_projection(self) -> bool {
        matches!(
            self,
            Self::Eventual | Self::ProjectionOk | Self::CacheOk | Self::BoundedStaleness
        )
    }

    /// True when the request will accept a Redis cache hit.
    pub fn allows_cache(self) -> bool {
        matches!(self, Self::CacheOk | Self::Eventual)
    }

    /// True when the runtime must honour any [`ReadFence`] the request
    /// carries. Strong/ReadYourWrites always honour; CacheOk skips the
    /// fence (the cache entry's own checksum is its consistency proof).
    pub fn honours_fence(self) -> bool {
        !matches!(self, Self::CacheOk)
    }

    pub(crate) fn from_proto_i32(mode: i32) -> Option<Self> {
        match mode {
            1 => Some(Self::Strong),
            2 => Some(Self::ReadYourWrites),
            3 => Some(Self::BoundedStaleness),
            4 => Some(Self::ReplicaBounded),
            5 => Some(Self::Eventual),
            6 => Some(Self::ProjectionOk),
            7 => Some(Self::CacheOk),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn to_proto_i32(self) -> i32 {
        match self {
            Self::Strong => 1,
            Self::ReadYourWrites => 2,
            Self::BoundedStaleness => 3,
            Self::ReplicaBounded => 4,
            Self::Eventual => 5,
            Self::ProjectionOk => 6,
            Self::CacheOk => 7,
        }
    }
}

/// What the broker hands back on every write so the next read can fence
/// against the same logical state. SDKs cache the most recent receipt
/// per `(tenant, project)` and attach it as a [`ReadFence`] on follow-up
/// reads — that's how the same connection sees its own writes even when
/// it routes through replicas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteReceipt {
    /// PostgreSQL WAL LSN at commit time. Replica router waits for
    /// `pg_last_wal_replay_lsn() >= this` before serving the read.
    /// Stored as a string (matches PG's `pg_lsn` textual form like
    /// `"0/1A2B3C4D"`).
    pub source_lsn: String,
    /// Outbox sequence number for the write. Projection-backed reads
    /// wait for `outbox_consumer_seq >= this`.
    pub outbox_seq: u64,
    /// Projection task IDs spawned by this write. Reads to projection
    /// targets wait for these tasks to complete before serving.
    pub projection_task_ids: Vec<String>,
    /// Manifest checksum at write time. Cache reads compare against
    /// this — a schema migration invalidates every receipt.
    pub manifest_checksum: String,
    /// Unix milliseconds at commit time. Used for `BoundedStaleness`
    /// budget calculation (`now - written_at <= max_replica_lag_ms`).
    pub written_at_unix_ms: i64,
}

impl WriteReceipt {
    /// Stub receipt for tests / no-op write paths. The runtime fills in
    /// real values from the WAL LSN + outbox row + projection enqueue.
    pub fn empty() -> Self {
        Self {
            source_lsn: String::new(),
            outbox_seq: 0,
            projection_task_ids: Vec::new(),
            manifest_checksum: String::new(),
            written_at_unix_ms: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.source_lsn.is_empty()
            && self.outbox_seq == 0
            && self.projection_task_ids.is_empty()
            && self.manifest_checksum.is_empty()
    }

    pub(crate) fn to_proto(&self) -> crate::proto::WriteReceipt {
        crate::proto::WriteReceipt {
            source_lsn: self.source_lsn.clone(),
            outbox_seq: self.outbox_seq,
            projection_task_ids: self.projection_task_ids.clone(),
            manifest_checksum: self.manifest_checksum.clone(),
            written_at_unix_ms: self.written_at_unix_ms,
        }
    }

    pub(crate) fn from_proto(proto: &crate::proto::WriteReceipt) -> Self {
        Self {
            source_lsn: proto.source_lsn.clone(),
            outbox_seq: proto.outbox_seq,
            projection_task_ids: proto.projection_task_ids.clone(),
            manifest_checksum: proto.manifest_checksum.clone(),
            written_at_unix_ms: proto.written_at_unix_ms,
        }
    }
}

/// Attached to a read request to demand "wait until the active backend
/// has caught up past this LSN AND this set of projection task IDs."
/// Derived from a previous `WriteReceipt`; SDKs do the conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadFence {
    /// Minimum WAL LSN the replica must have applied before serving.
    /// Empty when not fencing against PG.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub min_outbox_lsn: String,
    /// Projection task IDs that must be `COMPLETED` before the read
    /// is served. Empty when not fencing against projections.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projection_task_ids: Vec<String>,
    /// How long the broker may block waiting for the fence to clear
    /// before returning a [`StaleReadWarning::FenceTimedOut`] (with
    /// stale data) or a `tonic::Code::DeadlineExceeded` error,
    /// depending on the consistency mode. `0` means "no wait — return
    /// immediately with whatever's available."
    #[serde(default)]
    pub max_wait_ms: u64,
}

impl ReadFence {
    /// Build a fence from a write receipt. Used by SDKs to convert
    /// "I just got this WriteReceipt" into "fence my next read against
    /// it."
    pub fn from_receipt(receipt: &WriteReceipt, max_wait_ms: u64) -> Self {
        Self {
            min_outbox_lsn: receipt.source_lsn.clone(),
            projection_task_ids: receipt.projection_task_ids.clone(),
            max_wait_ms,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.min_outbox_lsn.is_empty() && self.projection_task_ids.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn to_proto(&self) -> crate::proto::ReadFence {
        crate::proto::ReadFence {
            min_outbox_lsn: self.min_outbox_lsn.clone(),
            projection_task_ids: self.projection_task_ids.clone(),
            max_wait_ms: self.max_wait_ms,
        }
    }

    pub(crate) fn from_proto(proto: &crate::proto::ReadFence) -> Self {
        Self {
            min_outbox_lsn: proto.min_outbox_lsn.clone(),
            projection_task_ids: proto.projection_task_ids.clone(),
            max_wait_ms: proto.max_wait_ms,
        }
    }
}

/// Per-row warning attached to a response when the broker served stale
/// data the caller didn't explicitly permit. SDKs surface these so apps
/// can log or alert without crashing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StaleReadWarning {
    /// The configured fence timed out and the broker returned the
    /// most-recent-available data anyway. Includes how far behind
    /// the source the served value was.
    FenceTimedOut {
        backend: String,
        instance: String,
        lag_ms: u64,
    },
    /// The replica's lag exceeded `max_replica_lag_ms` and the broker
    /// fell back to the primary (Strong mode) or served the stale
    /// replica anyway (BoundedStaleness over budget).
    ReplicaLagExceeded {
        instance: String,
        lag_ms: u64,
        budget_ms: u64,
    },
    /// A projection target was missing the requested row entirely
    /// (the projection hadn't run yet) and the broker fell back to
    /// the canonical source.
    ProjectionMissing { backend: String, resource: String },
    /// The cache entry's `manifest_checksum` didn't match the active
    /// catalog and the entry was discarded mid-request.
    CacheStale {
        backend: String,
        cache_key_prefix: String,
    },
}

impl StaleReadWarning {
    /// Stable token for `phase=` span attributes and the audit log.
    pub fn kind_token(&self) -> &'static str {
        match self {
            Self::FenceTimedOut { .. } => "fence_timed_out",
            Self::ReplicaLagExceeded { .. } => "replica_lag_exceeded",
            Self::ProjectionMissing { .. } => "projection_missing",
            Self::CacheStale { .. } => "cache_stale",
        }
    }
}

/// Resolved per-request consistency policy: the mode plus any optional
/// fence the caller attached. The runtime's executors check this once
/// at entry and respect it on every backend path.
#[derive(Debug, Clone, Default)]
pub struct ConsistencyPolicy {
    pub mode: ConsistencyMode,
    pub fence: ReadFence,
    /// `max_replica_lag_ms` carried over from `RequestContext`. Honoured
    /// by `BoundedStaleness` mode.
    pub max_replica_lag_ms: u64,
}

impl ConsistencyPolicy {
    /// Build from the existing `RequestContext` string-typed fields
    /// during the migration window. The runtime call sites that still
    /// pass `RequestContext` directly can call this once and operate
    /// on the typed result.
    pub fn from_request_context(
        consistency_token: &str,
        max_replica_lag_ms: u64,
        primary_read: bool,
        eventual_consistency_allowed: bool,
    ) -> Self {
        // Honour the legacy boolean flags: `primary_read` upgrades to
        // Strong, `eventual_consistency_allowed` downgrades blank tokens
        // to Eventual. Otherwise honour the explicit mode.
        let mode = if primary_read {
            ConsistencyMode::Strong
        } else if consistency_token.trim().is_empty() && eventual_consistency_allowed {
            ConsistencyMode::Eventual
        } else {
            ConsistencyMode::parse_or_default(consistency_token)
        };
        Self {
            mode,
            fence: ReadFence::default(),
            max_replica_lag_ms,
        }
    }

    pub fn with_fence(mut self, fence: ReadFence) -> Self {
        self.fence = fence;
        self
    }

    /// True when the runtime should ROUTE to the primary even though
    /// the caller didn't explicitly demand it (e.g. `ReadYourWrites`
    /// with no fence is treated like `Strong`).
    pub fn force_primary(&self) -> bool {
        matches!(self.mode, ConsistencyMode::Strong)
            || (matches!(self.mode, ConsistencyMode::ReadYourWrites) && self.fence.is_empty())
    }
}

// ── 6.4 REPLICA_BOUNDED read routing ──────────────────────────────────────────
//
// A bounded-staleness read may be served from a read replica ONLY IF the
// broker can prove the replica has caught up past the caller's write — and
// "prove" means waiting on the backend's REAL replication position
// (`CanonicalStore::wait_for_token` over a PG WAL LSN, a MySQL GTID/binlog
// position, or a monotone outbox `event_seq`). A backend whose only "version"
// is a wall clock (object-store `LastModified`, cache TTL stamp) cannot be
// fenced honestly: `wait_for_token` on a wall-clock value returns the instant
// `now()` passes the stamp, which is a *vacuous* fence — it clears immediately
// and serves stale data while claiming freshness. Such backends are REFUSED a
// bounded read, never faked.

/// Classifies a backend's durability token for bounded-read eligibility.
///
/// `RealPosition` backends mint a monotone replication/write position that a
/// later read can wait on (this is what `CanonicalStore::current_durability_token`
/// returns for every UDB canonical store — verified per impl). `WallClock`
/// backends have no such position; a fence on them is vacuous.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityTokenClass {
    /// Monotone replication/write position (PG WAL LSN, MySQL GTID or
    /// `file:<file>:<pos>`, SQLite `data_version`, or a durable monotone
    /// outbox `event_seq` counter on MSSQL / MongoDB / Redis / Qdrant /
    /// ClickHouse / Neo4j / Cassandra / Weaviate / Pinecone / Elasticsearch).
    /// A `wait_for_token` against one of these blocks until the store has
    /// actually applied past it — a real fence.
    RealPosition,
    /// No real replication position — only a wall clock / opaque timestamp
    /// (object stores, plain caches). Bounded reads are refused for these.
    WallClock,
}

/// Classify a backend label (`CanonicalStore::backend_label`, the
/// `BackendKind::as_str` token, or a SDK-supplied alias). The allowlist
/// names every backend whose `current_durability_token` was verified to
/// return a monotone position; everything else — object stores (s3, minio,
/// azureblob, gcs), plain caches (memcached), and unknown labels — is
/// `WallClock` and therefore refused a bounded read.
pub fn durability_token_class(backend_label: &str) -> DurabilityTokenClass {
    match backend_label.trim().to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" | "pg" | "mysql" | "mariadb" | "mssql" | "sqlserver"
        | "sqlite" | "mongodb" | "mongo" | "redis" | "qdrant" | "clickhouse" | "neo4j"
        | "cassandra" | "scylla" | "weaviate" | "pinecone" | "elasticsearch" => {
            DurabilityTokenClass::RealPosition
        }
        // Object stores / caches / unknown: no real replication position.
        _ => DurabilityTokenClass::WallClock,
    }
}

/// True when `backend_label` mints a real replication position and can
/// therefore honour a bounded-staleness fence on a replica.
pub fn supports_bounded_reads(backend_label: &str) -> bool {
    matches!(
        durability_token_class(backend_label),
        DurabilityTokenClass::RealPosition
    )
}

/// Typed refusal returned (inside [`ReadRouting::RefusedBounded`]) when a
/// caller asks for bounded staleness against a backend that has no real
/// replication-position token. The handler surfaces this as
/// `tonic::Code::FailedPrecondition` rather than silently serving the
/// primary or — worse — a vacuously-fenced stale replica read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedReadRefused {
    /// Backend label that was asked for a bounded read.
    pub backend: String,
    /// Why the bounded read was refused (stable operator-facing string).
    pub reason: &'static str,
}

impl std::fmt::Display for BoundedReadRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bounded-staleness read refused for backend '{}': {}",
            self.backend, self.reason
        )
    }
}

impl std::error::Error for BoundedReadRefused {}

/// The pure routing decision for a single read/write request, computed once
/// at handler entry from the [`ConsistencyPolicy`], whether the op mutates,
/// and the target backend. The runtime maps each variant to an action:
///
/// - [`ReadRouting::Primary`] → use the primary pool.
/// - [`ReadRouting::ReplicaBounded`] → `PgReplicaManager::choose_bounded_replica`
///   (pick a replica within `max_staleness_ms` lag, WAIT on its real applied
///   LSN past `min_lsn`, FAIL OVER to primary on timeout).
/// - [`ReadRouting::ReplicaUnfenced`] → any healthy replica, no fence.
/// - [`ReadRouting::RefusedBounded`] → return the typed error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadRouting {
    /// Serve from the primary. Writes ALWAYS land here; so do `Strong`
    /// reads and `ReadYourWrites` reads with no fence.
    Primary,
    /// Route to a read replica and fence on the backend's REAL replication
    /// position. `min_lsn` is the caller's write position (`None` ⇒
    /// lag-bounded selection only, no LSN wait). `max_staleness_ms` is the
    /// wait budget; the caller fails over to the primary on timeout.
    ReplicaBounded {
        max_staleness_ms: u64,
        min_lsn: Option<String>,
    },
    /// Route to any healthy replica with no fence (`Eventual`,
    /// `ProjectionOk`, `CacheOk`). No staleness bound.
    ReplicaUnfenced,
    /// Bounded staleness was requested for a backend with no real
    /// replication-position token — refused, never faked.
    RefusedBounded(BoundedReadRefused),
}

impl ConsistencyPolicy {
    /// Resolve the pure routing decision for this request. `is_write`
    /// short-circuits to [`ReadRouting::Primary`] — writes NEVER route to a
    /// replica. `backend_label` is the canonical-store backend the read
    /// targets; it gates whether a bounded read is honourable or refused.
    pub fn route_read(&self, is_write: bool, backend_label: &str) -> ReadRouting {
        // 1) Writes are primary-only, unconditionally.
        if is_write {
            return ReadRouting::Primary;
        }
        // 2) Strong, and ReadYourWrites-without-a-fence, force the primary.
        if self.force_primary() {
            return ReadRouting::Primary;
        }
        match self.mode {
            // Reached here only when RYW carries a fence (force_primary was
            // false): route to a replica and wait on the fence LSN, with the
            // fence's own max_wait_ms as the budget.
            ConsistencyMode::ReadYourWrites => self.bounded_or_refuse(backend_label, false),
            // Explicit bounded staleness: honour on a real-position backend,
            // refuse on a wall-clock backend.
            ConsistencyMode::BoundedStaleness => self.bounded_or_refuse(backend_label, true),
            // REPLICA_BOUNDED: same replica + LSN-fence + lag-budget routing as
            // BoundedStaleness, but FAILS OVER to the primary on a wall-clock
            // backend (refuse_on_wallclock = false) instead of refusing — the
            // mode degrades to the primary, it never errors.
            ConsistencyMode::ReplicaBounded => self.bounded_or_refuse(backend_label, false),
            // No fence, no bound — any replica/projection/cache target.
            ConsistencyMode::Eventual
            | ConsistencyMode::ProjectionOk
            | ConsistencyMode::CacheOk => ReadRouting::ReplicaUnfenced,
            // force_primary already handled Strong.
            ConsistencyMode::Strong => ReadRouting::Primary,
        }
    }

    /// Shared tail for the two fenced read modes. `refuse_on_wallclock`
    /// distinguishes BoundedStaleness (explicitly asked for a replica
    /// bounded read ⇒ REFUSE if we can't fence honestly) from RYW (the
    /// primary trivially satisfies read-your-writes ⇒ fall back to primary
    /// rather than error).
    fn bounded_or_refuse(&self, backend_label: &str, refuse_on_wallclock: bool) -> ReadRouting {
        let min_lsn = {
            let lsn = self.fence.min_outbox_lsn.trim();
            (!lsn.is_empty()).then(|| lsn.to_string())
        };
        let max_staleness_ms = match self.mode {
            // Bounded-staleness family: the budget is the caller's lag bound.
            ConsistencyMode::BoundedStaleness | ConsistencyMode::ReplicaBounded => {
                self.max_replica_lag_ms
            }
            // RYW: the budget is the fence's own wait window.
            _ => self.fence.max_wait_ms,
        };
        if supports_bounded_reads(backend_label) {
            ReadRouting::ReplicaBounded {
                max_staleness_ms,
                min_lsn,
            }
        } else if refuse_on_wallclock {
            ReadRouting::RefusedBounded(BoundedReadRefused {
                backend: backend_label.to_string(),
                reason: "backend mints no real replication-position token; a bounded-staleness \
                         fence on it would be a vacuous wall-clock fence",
            })
        } else {
            // RYW against a wall-clock backend: the primary always has the
            // caller's writes, so route there instead of erroring.
            ReadRouting::Primary
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consistency_golden_value() -> serde_json::Value {
        let receipt = WriteReceipt {
            source_lsn: "0/1A2B3C4D".to_string(),
            outbox_seq: 42,
            projection_task_ids: vec![
                "projection-task-a".to_string(),
                "projection-task-b".to_string(),
            ],
            manifest_checksum: "sha256:0123456789abcdef".to_string(),
            written_at_unix_ms: 1_735_689_600_000,
        };
        let fence = ReadFence::from_receipt(&receipt, 2_500);
        serde_json::json!({
            "write_receipt": receipt,
            "read_fence": fence,
        })
    }

    #[test]
    fn consistency_golden_json_matches_serde_contract() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let committed: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(root.join("docs/generated/consistency-golden.json"))
                .expect("consistency golden JSON should be readable"),
        )
        .expect("consistency golden JSON should parse");

        assert_eq!(
            committed,
            consistency_golden_value(),
            "docs/generated/consistency-golden.json drifted from WriteReceipt/ReadFence serde"
        );
    }

    /// Pinned wire tokens — SDKs and the `x-udb-consistency` header
    /// reference these by string. Changing one is a breaking change.
    #[test]
    fn mode_tokens_are_pinned() {
        assert_eq!(ConsistencyMode::Strong.as_str(), "strong");
        assert_eq!(ConsistencyMode::ReadYourWrites.as_str(), "read_your_writes");
        assert_eq!(
            ConsistencyMode::BoundedStaleness.as_str(),
            "bounded_staleness"
        );
        assert_eq!(ConsistencyMode::ReplicaBounded.as_str(), "replica_bounded");
        assert_eq!(ConsistencyMode::Eventual.as_str(), "eventual");
        assert_eq!(ConsistencyMode::ProjectionOk.as_str(), "projection_ok");
        assert_eq!(ConsistencyMode::CacheOk.as_str(), "cache_ok");
    }

    #[test]
    fn legacy_aliases_parse() {
        // Back-compat with the pre-U6 string vocabulary.
        assert_eq!(
            ConsistencyMode::parse("linearizable"),
            Some(ConsistencyMode::Strong)
        );
        assert_eq!(
            ConsistencyMode::parse("primary"),
            Some(ConsistencyMode::Strong)
        );
        assert_eq!(
            ConsistencyMode::parse("eventual_consistency"),
            Some(ConsistencyMode::Eventual)
        );
        assert_eq!(
            ConsistencyMode::parse("ryw"),
            Some(ConsistencyMode::ReadYourWrites)
        );
        assert_eq!(ConsistencyMode::parse("unknown_mode"), None);
        assert_eq!(ConsistencyMode::parse(""), None);
        assert_eq!(
            ConsistencyMode::parse_or_default("unknown"),
            ConsistencyMode::Strong
        );
    }

    /// Mode capability matrix — these are what every runtime call site
    /// reads to decide "may I route this to the replica / projection /
    /// cache?" Pinned to lock the policy.
    #[test]
    fn mode_capability_matrix_is_correct() {
        // Strong: primary only.
        assert!(!ConsistencyMode::Strong.allows_replica());
        assert!(!ConsistencyMode::Strong.allows_projection());
        assert!(!ConsistencyMode::Strong.allows_cache());

        // ReadYourWrites: primary only (replica only with a fence).
        assert!(!ConsistencyMode::ReadYourWrites.allows_replica());
        assert!(!ConsistencyMode::ReadYourWrites.allows_projection());
        assert!(!ConsistencyMode::ReadYourWrites.allows_cache());

        // BoundedStaleness: replica + projection, but no cache (cache
        // doesn't carry a lag bound).
        assert!(ConsistencyMode::BoundedStaleness.allows_replica());
        assert!(ConsistencyMode::BoundedStaleness.allows_projection());
        assert!(!ConsistencyMode::BoundedStaleness.allows_cache());

        // Eventual: anything goes.
        assert!(ConsistencyMode::Eventual.allows_replica());
        assert!(ConsistencyMode::Eventual.allows_projection());
        assert!(ConsistencyMode::Eventual.allows_cache());

        // ProjectionOk: projection yes, cache no (cache is even
        // weaker — must be opted into explicitly via CacheOk).
        assert!(ConsistencyMode::ProjectionOk.allows_replica());
        assert!(ConsistencyMode::ProjectionOk.allows_projection());
        assert!(!ConsistencyMode::ProjectionOk.allows_cache());

        // CacheOk: cache yes (the whole point).
        assert!(ConsistencyMode::CacheOk.allows_replica());
        assert!(ConsistencyMode::CacheOk.allows_projection());
        assert!(ConsistencyMode::CacheOk.allows_cache());
    }

    /// CacheOk skips the read fence because the cache entry's own
    /// `manifest_checksum` is the consistency proof — fencing past it
    /// would force a backend round-trip on every read and defeat the
    /// cache.
    #[test]
    fn cache_ok_skips_fence_others_honour_it() {
        assert!(!ConsistencyMode::CacheOk.honours_fence());
        assert!(ConsistencyMode::Strong.honours_fence());
        assert!(ConsistencyMode::ReadYourWrites.honours_fence());
        assert!(ConsistencyMode::Eventual.honours_fence());
    }

    /// A WriteReceipt feeds straight into a ReadFence — SDKs use this
    /// to implement "read-your-writes" sessions without any plumbing
    /// other than caching the receipt.
    #[test]
    fn write_receipt_converts_to_read_fence() {
        let receipt = WriteReceipt {
            source_lsn: "0/1A2B3C".into(),
            outbox_seq: 42,
            projection_task_ids: vec!["task-a".into(), "task-b".into()],
            manifest_checksum: "abc123".into(),
            written_at_unix_ms: 1_700_000_000_000,
        };
        let fence = ReadFence::from_receipt(&receipt, 5_000);
        assert_eq!(fence.min_outbox_lsn, "0/1A2B3C");
        assert_eq!(
            fence.projection_task_ids,
            vec!["task-a".to_string(), "task-b".to_string()]
        );
        assert_eq!(fence.max_wait_ms, 5_000);
        assert!(!fence.is_empty());
    }

    #[test]
    fn write_receipt_proto_round_trip_preserves_serde_shape() {
        let receipt = WriteReceipt {
            source_lsn: "0/1A2B3C".into(),
            outbox_seq: 42,
            projection_task_ids: vec!["task-a".into(), "task-b".into()],
            manifest_checksum: "abc123".into(),
            written_at_unix_ms: 1_700_000_000_000,
        };

        let proto = receipt.to_proto();
        let decoded = WriteReceipt::from_proto(&proto);

        assert_eq!(decoded, receipt);
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::json!({
                "source_lsn": "0/1A2B3C",
                "outbox_seq": 42,
                "projection_task_ids": ["task-a", "task-b"],
                "manifest_checksum": "abc123",
                "written_at_unix_ms": 1_700_000_000_000i64,
            })
        );
    }

    #[test]
    fn read_fence_proto_round_trip_preserves_serde_shape() {
        let fence = ReadFence {
            min_outbox_lsn: "0/1A2B3C".into(),
            projection_task_ids: vec!["task-a".into()],
            max_wait_ms: 2_500,
        };

        let proto = fence.to_proto();
        let decoded = ReadFence::from_proto(&proto);

        assert_eq!(decoded, fence);
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::json!({
                "min_outbox_lsn": "0/1A2B3C",
                "projection_task_ids": ["task-a"],
                "max_wait_ms": 2_500,
            })
        );
    }

    #[test]
    fn consistency_mode_proto_numbers_match_pinned_wire_tokens() {
        for mode in [
            ConsistencyMode::Strong,
            ConsistencyMode::ReadYourWrites,
            ConsistencyMode::BoundedStaleness,
            ConsistencyMode::ReplicaBounded,
            ConsistencyMode::Eventual,
            ConsistencyMode::ProjectionOk,
            ConsistencyMode::CacheOk,
        ] {
            assert_eq!(
                ConsistencyMode::from_proto_i32(mode.to_proto_i32()),
                Some(mode),
                "{} must round-trip through the proto enum number",
                mode.as_str()
            );
        }
        assert_eq!(ConsistencyMode::from_proto_i32(0), None);
        assert_eq!(ConsistencyMode::from_proto_i32(999), None);
    }

    #[test]
    fn empty_receipts_and_fences_are_detectable() {
        assert!(WriteReceipt::empty().is_empty());
        assert!(ReadFence::default().is_empty());
    }

    /// The `from_request_context` adapter is what the runtime call
    /// sites use during the migration window — they still read the
    /// pre-U6 stringly typed `RequestContext` fields but get back a
    /// typed `ConsistencyPolicy`.
    #[test]
    fn from_request_context_honours_legacy_flags() {
        // `primary_read=true` upgrades any mode to Strong.
        let p = ConsistencyPolicy::from_request_context("eventual", 0, true, false);
        assert_eq!(p.mode, ConsistencyMode::Strong);

        // Blank consistency + `eventual_consistency_allowed=true`
        // downgrades to Eventual.
        let p = ConsistencyPolicy::from_request_context("", 0, false, true);
        assert_eq!(p.mode, ConsistencyMode::Eventual);

        // Explicit mode wins over the flags.
        let p = ConsistencyPolicy::from_request_context("read_your_writes", 0, false, true);
        assert_eq!(p.mode, ConsistencyMode::ReadYourWrites);
    }

    /// `force_primary` is what the replica router checks: Strong always
    /// forces, ReadYourWrites forces ONLY when no fence is attached.
    /// With a fence, the replica router can route to a replica and wait
    /// on the LSN.
    #[test]
    fn force_primary_respects_ryw_fence_state() {
        let strong = ConsistencyPolicy {
            mode: ConsistencyMode::Strong,
            ..Default::default()
        };
        assert!(strong.force_primary());

        let ryw_no_fence = ConsistencyPolicy {
            mode: ConsistencyMode::ReadYourWrites,
            ..Default::default()
        };
        assert!(
            ryw_no_fence.force_primary(),
            "RYW with no fence behaves like Strong"
        );

        let ryw_with_fence = ConsistencyPolicy {
            mode: ConsistencyMode::ReadYourWrites,
            fence: ReadFence {
                min_outbox_lsn: "0/100".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            !ryw_with_fence.force_primary(),
            "RYW with fence allows replica routing once the fence is satisfied"
        );

        let eventual = ConsistencyPolicy {
            mode: ConsistencyMode::Eventual,
            ..Default::default()
        };
        assert!(!eventual.force_primary());
    }

    /// Stale-read warning tokens are referenced by the audit log and
    /// the per-tenant dashboard; pin them.
    #[test]
    fn stale_read_warning_tokens_are_pinned() {
        let cases = [
            (
                StaleReadWarning::FenceTimedOut {
                    backend: "mongodb".into(),
                    instance: "default".into(),
                    lag_ms: 1234,
                },
                "fence_timed_out",
            ),
            (
                StaleReadWarning::ReplicaLagExceeded {
                    instance: "replica-1".into(),
                    lag_ms: 5_000,
                    budget_ms: 1_000,
                },
                "replica_lag_exceeded",
            ),
            (
                StaleReadWarning::ProjectionMissing {
                    backend: "qdrant".into(),
                    resource: "customers_vec".into(),
                },
                "projection_missing",
            ),
            (
                StaleReadWarning::CacheStale {
                    backend: "redis".into(),
                    cache_key_prefix: "udb:billing".into(),
                },
                "cache_stale",
            ),
        ];
        for (warning, token) in cases {
            assert_eq!(warning.kind_token(), token);
        }
    }

    /// Round-trip every mode through serde so `x-udb-consistency`
    /// header parsing matches `as_str` exactly.
    #[test]
    fn serde_round_trip_matches_wire_tokens() {
        for mode in [
            ConsistencyMode::Strong,
            ConsistencyMode::ReadYourWrites,
            ConsistencyMode::BoundedStaleness,
            ConsistencyMode::ReplicaBounded,
            ConsistencyMode::Eventual,
            ConsistencyMode::ProjectionOk,
            ConsistencyMode::CacheOk,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let token = json.trim_matches('"');
            assert_eq!(token, mode.as_str());
            let back: ConsistencyMode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, mode);
        }
    }

    // ── 6.4 REPLICA_BOUNDED routing decision ──────────────────────────────

    /// Every backend with a verified monotone position token classifies as
    /// `RealPosition`; object stores / caches / unknown labels are
    /// `WallClock` and therefore refused a bounded read.
    #[test]
    fn durability_token_class_is_pinned() {
        for real in [
            "postgres",
            "postgresql",
            "mysql",
            "mariadb",
            "mssql",
            "sqlserver",
            "sqlite",
            "mongodb",
            "redis",
            "qdrant",
            "clickhouse",
            "neo4j",
            "cassandra",
            "weaviate",
            "pinecone",
            "elasticsearch",
        ] {
            assert_eq!(
                durability_token_class(real),
                DurabilityTokenClass::RealPosition,
                "{real} mints a real position token"
            );
            assert!(
                supports_bounded_reads(real),
                "{real} supports bounded reads"
            );
        }
        for wall in [
            "s3",
            "minio",
            "azureblob",
            "gcs",
            "memcached",
            "weird-future-backend",
        ] {
            assert_eq!(
                durability_token_class(wall),
                DurabilityTokenClass::WallClock,
                "{wall} has no real position token"
            );
            assert!(
                !supports_bounded_reads(wall),
                "{wall} must be refused a bounded read"
            );
        }
        // Case / whitespace insensitive.
        assert!(supports_bounded_reads("  Postgres "));
    }

    /// Writes NEVER route to a replica — regardless of the declared mode,
    /// even the cheapest `Eventual`.
    #[test]
    fn writes_always_route_to_primary() {
        for mode in [
            ConsistencyMode::Strong,
            ConsistencyMode::ReadYourWrites,
            ConsistencyMode::BoundedStaleness,
            ConsistencyMode::ReplicaBounded,
            ConsistencyMode::Eventual,
            ConsistencyMode::ProjectionOk,
            ConsistencyMode::CacheOk,
        ] {
            let policy = ConsistencyPolicy {
                mode,
                max_replica_lag_ms: 1_000,
                ..Default::default()
            };
            assert_eq!(
                policy.route_read(true, "postgres"),
                ReadRouting::Primary,
                "write under {mode:?} must route to primary"
            );
        }
    }

    /// Strong reads and RYW-without-fence force the primary.
    #[test]
    fn strong_and_unfenced_ryw_route_to_primary() {
        let strong = ConsistencyPolicy {
            mode: ConsistencyMode::Strong,
            ..Default::default()
        };
        assert_eq!(strong.route_read(false, "postgres"), ReadRouting::Primary);

        let ryw = ConsistencyPolicy {
            mode: ConsistencyMode::ReadYourWrites,
            ..Default::default()
        };
        assert_eq!(ryw.route_read(false, "postgres"), ReadRouting::Primary);
    }

    /// Bounded staleness against a real-position backend routes to a replica
    /// with the LSN fence and the lag budget carried through.
    #[test]
    fn bounded_staleness_routes_to_replica_with_real_token() {
        let policy = ConsistencyPolicy {
            mode: ConsistencyMode::BoundedStaleness,
            max_replica_lag_ms: 750,
            fence: ReadFence {
                min_outbox_lsn: "0/1A2B3C".into(),
                max_wait_ms: 9_999,
                ..Default::default()
            },
        };
        assert_eq!(
            policy.route_read(false, "postgres"),
            ReadRouting::ReplicaBounded {
                max_staleness_ms: 750,
                min_lsn: Some("0/1A2B3C".to_string()),
            }
        );

        // Real-position backend whose token is an outbox seq, not an LSN —
        // still bounded-eligible.
        let policy = ConsistencyPolicy {
            mode: ConsistencyMode::BoundedStaleness,
            max_replica_lag_ms: 200,
            fence: ReadFence::default(),
        };
        assert_eq!(
            policy.route_read(false, "mongodb"),
            ReadRouting::ReplicaBounded {
                max_staleness_ms: 200,
                // No fence LSN ⇒ lag-bounded selection only.
                min_lsn: None,
            }
        );
    }

    /// Bounded staleness against a wall-clock backend is REFUSED with a
    /// typed error — never faked, never silently downgraded.
    #[test]
    fn bounded_staleness_refused_on_wallclock_backend() {
        let policy = ConsistencyPolicy {
            mode: ConsistencyMode::BoundedStaleness,
            max_replica_lag_ms: 500,
            fence: ReadFence::default(),
        };
        for backend in ["s3", "azureblob", "gcs", "memcached", "unknown"] {
            match policy.route_read(false, backend) {
                ReadRouting::RefusedBounded(refused) => {
                    assert_eq!(refused.backend, backend);
                    assert!(!refused.reason.is_empty());
                    // It's a real std::error::Error with a useful message.
                    let err: &dyn std::error::Error = &refused;
                    assert!(err.to_string().contains(backend));
                }
                other => panic!("expected RefusedBounded for {backend}, got {other:?}"),
            }
        }
    }

    /// RYW WITH a fence routes to a replica on a real-position backend
    /// (waiting on the fence LSN), but falls back to the PRIMARY — not a
    /// refusal — on a wall-clock backend, since the primary trivially
    /// satisfies read-your-writes.
    #[test]
    fn fenced_ryw_routes_to_replica_or_primary_never_refused() {
        let policy = ConsistencyPolicy {
            mode: ConsistencyMode::ReadYourWrites,
            max_replica_lag_ms: 0,
            fence: ReadFence {
                min_outbox_lsn: "0/200".into(),
                max_wait_ms: 1_500,
                ..Default::default()
            },
        };
        assert_eq!(
            policy.route_read(false, "postgres"),
            ReadRouting::ReplicaBounded {
                max_staleness_ms: 1_500,
                min_lsn: Some("0/200".to_string()),
            }
        );
        // Wall-clock backend: primary, not RefusedBounded.
        assert_eq!(policy.route_read(false, "s3"), ReadRouting::Primary);
    }

    /// Eventual / ProjectionOk / CacheOk route to an unfenced replica on any
    /// backend (no real-position requirement — there's no fence to honour).
    #[test]
    fn eventual_family_routes_to_unfenced_replica() {
        for mode in [
            ConsistencyMode::Eventual,
            ConsistencyMode::ProjectionOk,
            ConsistencyMode::CacheOk,
        ] {
            let policy = ConsistencyPolicy {
                mode,
                ..Default::default()
            };
            assert_eq!(
                policy.route_read(false, "s3"),
                ReadRouting::ReplicaUnfenced,
                "{mode:?} routes to an unfenced replica"
            );
        }
    }

    // ── 6.4 REPLICA_BOUNDED distinct mode ─────────────────────────────────

    /// The REPLICA_BOUNDED wire token + aliases parse to the distinct
    /// variant, and it is its own mode (not an alias of BoundedStaleness).
    #[test]
    fn replica_bounded_token_and_aliases_parse() {
        assert_eq!(ConsistencyMode::ReplicaBounded.as_str(), "replica_bounded");
        assert_eq!(
            ConsistencyMode::parse("replica_bounded"),
            Some(ConsistencyMode::ReplicaBounded)
        );
        assert_eq!(
            ConsistencyMode::parse("replica-bounded"),
            Some(ConsistencyMode::ReplicaBounded)
        );
        // Distinct from BoundedStaleness.
        assert_ne!(
            ConsistencyMode::ReplicaBounded,
            ConsistencyMode::BoundedStaleness
        );
    }

    /// REPLICA_BOUNDED capability matrix: it is replica-eligible and
    /// honours the fence, but — unlike BoundedStaleness — it is REPLICA-only
    /// (no projection-backed targets) and never accepts a cache hit.
    #[test]
    fn replica_bounded_is_replica_only() {
        assert!(ConsistencyMode::ReplicaBounded.allows_replica());
        assert!(ConsistencyMode::ReplicaBounded.honours_fence());
        assert!(
            !ConsistencyMode::ReplicaBounded.allows_projection(),
            "REPLICA_BOUNDED targets SQL replicas only, never projections"
        );
        assert!(!ConsistencyMode::ReplicaBounded.allows_cache());
    }

    /// REPLICA_BOUNDED routing: on a real-position backend it routes to a
    /// replica with the LSN fence (the REAL token, not a wall clock) and the
    /// lag budget; on a wall-clock backend it FAILS OVER to the primary
    /// rather than refusing (the key difference from BoundedStaleness, which
    /// refuses). Writes always route to the primary.
    #[test]
    fn replica_bounded_routes_to_replica_then_fails_over_to_primary() {
        let policy = ConsistencyPolicy {
            mode: ConsistencyMode::ReplicaBounded,
            max_replica_lag_ms: 500,
            fence: ReadFence {
                min_outbox_lsn: "0/1A2B3C".into(),
                max_wait_ms: 9_999,
                ..Default::default()
            },
        };
        // Real-position backend: replica + LSN fence + lag budget.
        assert_eq!(
            policy.route_read(false, "postgres"),
            ReadRouting::ReplicaBounded {
                max_staleness_ms: 500,
                min_lsn: Some("0/1A2B3C".to_string()),
            }
        );
        // Wall-clock backend: FAIL OVER to primary — NOT RefusedBounded.
        assert_eq!(policy.route_read(false, "s3"), ReadRouting::Primary);
        // Writes never route to a replica, even under REPLICA_BOUNDED.
        assert_eq!(policy.route_read(true, "postgres"), ReadRouting::Primary);
    }
}
