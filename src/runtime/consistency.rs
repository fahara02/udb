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
//! U6 centralises consistency as a typed [`ConsistencyMode`] with six
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

/// The six pinned consistency modes a request may declare.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
