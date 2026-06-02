//! `Neo4jCanonicalStore` — B.10b PHASE 1. Neo4j-backed
//! [`CanonicalStore`](super::CanonicalStore) implementation over the EXISTING
//! HTTP transactional Cypher executor
//! ([`Neo4jExecutor`](crate::runtime::executors::neo4j::Neo4jExecutor)). No new
//! Bolt driver dependency: every operation is Cypher over the HTTP
//! `/db/<db>/tx/commit` endpoint, which runs a list of statements atomically as
//! one transaction.
//!
//! This is the **base** canonical-store surface only: durability token, outbox,
//! advisory leases, and `ensure_system_tables`. The four system-store traits
//! (`ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
//! `MigrationAuditStore`) are PHASE 2 and are NOT implemented here, so this
//! store is not yet registered into the runtime `CanonicalStoreRegistry` (that
//! happens only after the full `SystemStores` conformance passes).
//!
//! ## Why reuse `Neo4jExecutor`, not a second connection layer
//!
//! UDB's Neo4j executor already wraps a configured `reqwest::Client` and knows
//! how to (a) run a single auto-commit Cypher with parameters
//! (`cypher_rows`) and (b) run a LIST of statements atomically in ONE HTTP
//! transaction (`cypher_tx_rows`). This store reuses those `pub(crate)` helpers
//! exactly as the Cassandra store reuses `CassandraClient`'s CQL helpers and the
//! MongoDB store reuses `MongoDbExecutor::native_database()`.
//!
//! ## How Neo4j's model maps onto the SQL canonical core
//!
//! Neo4j has no tables, no sequences, and no `INSERT … ON CONFLICT`. The
//! mapping:
//!
//! - **No auto-increment / sequence.** The outbox sequence is a single
//!   `(:UdbCounter {id:'outbox_seq', seq})` node. `enqueue_outbox_event`
//!   `MERGE`s that node and bumps `seq` *inside the same Cypher statement* that
//!   creates the `(:UdbOutbox)` event node — the HTTP transaction makes the
//!   bump-and-create atomic, so the counter never advances without a matching
//!   event row and two concurrent enqueues serialise on the node's write lock.
//! - **No `ON CONFLICT … WHERE expired OR same-owner`.** Neo4j expresses the
//!   PG disjunction directly: `MERGE` the `(:UdbLease)` node, then a
//!   `WITH l WHERE l.owner_id = $o OR l.expires_at <= $now` gate. When the gate
//!   fails (a live lease held by a different owner) the row is filtered out, the
//!   `SET` does not run, and `RETURN` yields no rows → `Ok(false)`. See the
//!   per-case reasoning on `try_acquire_advisory_lease`.
//!
//! ## Test isolation (`run_tag`)
//!
//! Neo4j Community Edition has a single database, so the live conformance test
//! cannot provision a throwaway DB the way the SQL/Mongo/Cassandra tests do.
//! Instead the store carries an opaque `run_tag` that is stamped onto every node
//! it creates AND ANDed into every MATCH/MERGE, so two concurrent conformance
//! runs (and any pre-existing operator data) never collide. Production callers
//! pass an empty tag (the default), which still scopes cleanly because the
//! counter id / lease name carry the addressing.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value as Json, json};

use super::{CanonicalStore, DurabilityToken};
use crate::runtime::executors::neo4j::Neo4jExecutor;

/// Well-known id of the single outbox-sequence counter node.
const OUTBOX_SEQ_ID: &str = "outbox_seq";

pub struct Neo4jCanonicalStore {
    // B.10b phase 2: the four system-store trait impls live in sibling files
    // (`neo4j_projection.rs`, `neo4j_saga.rs`, `neo4j_admin_audit.rs`,
    // `neo4j_migration_audit.rs`). They reach the executor + run-tag through
    // these `pub(super)` fields / accessors, mirroring how the Cassandra
    // siblings reach `CassandraCanonicalStore`.
    pub(super) executor: Neo4jExecutor,
    pub(super) instance_name: String,
    /// Opaque per-store tag stamped onto every node and ANDed into every
    /// MATCH/MERGE for isolation (see module docs). Empty for production.
    pub(super) run_tag: String,
}

impl Neo4jCanonicalStore {
    pub fn new(executor: Neo4jExecutor, instance_name: impl Into<String>) -> Self {
        Self {
            executor,
            instance_name: instance_name.into(),
            run_tag: String::new(),
        }
    }

    /// B.10b conformance only: scope every node this store touches with a unique
    /// `run_tag`. Two concurrent live runs against the SAME Neo4j database thus
    /// never see each other's counter/outbox/lease nodes.
    pub fn with_run_tag(mut self, run_tag: impl Into<String>) -> Self {
        self.run_tag = run_tag.into();
        self
    }

    /// The run tag as a JSON string param value.
    fn tag(&self) -> Json {
        json!(self.run_tag)
    }

    /// B.10b phase 2: executor accessor for the sibling system-store impls.
    pub(super) fn executor(&self) -> &Neo4jExecutor {
        &self.executor
    }

    /// B.10b phase 2: run-tag accessor for the sibling system-store impls.
    /// Returned as the raw `&str` so callers can wrap it in their own param
    /// builders; use [`Neo4jCanonicalStore::tag`]-equivalent `json!(run_tag())`
    /// at the bind site.
    pub(super) fn run_tag(&self) -> &str {
        &self.run_tag
    }

    /// Unix milliseconds now. Used for the advisory-lease `expires_at`
    /// comparison. We compute `now`/`expires_at` client-side (rather than
    /// Neo4j's `timestamp()`) so the lease math is identical to the SQL/Cassandra
    /// stores and unit-testable without a server. `timestamp()` is still used
    /// server-side for the outbox `created_at` (a non-load-bearing audit field).
    fn now_unix_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

#[async_trait]
impl CanonicalStore for Neo4jCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "neo4j"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        // Neo4j has no tables; the "schema" is indexes. We deliberately use
        // composite RANGE INDEXES — not uniqueness constraints — because:
        //   * Composite UNIQUENESS constraints / NODE KEY require Neo4j
        //     ENTERPRISE; this store must run on Community Edition.
        //   * Single-property uniqueness on e.g. `lease_name` alone would
        //     collide across `run_tag`-scoped conformance runs (and across
        //     operators sharing a DB), so it is the wrong invariant anyway.
        // The logical singleton (one counter / lease node per (run_tag, key)) is
        // enforced by the `MERGE (… {run_tag, id})` pattern, not the index — the
        // index only makes the MATCH/MERGE lookups cheap. `CREATE INDEX IF NOT
        // EXISTS` is idempotent (Neo4j 5 syntax) and available on Community.
        //
        // Each statement runs in its own request: Neo4j 5 forbids mixing schema
        // (CREATE INDEX) and data (the counter seed) in one transaction, so the
        // indexes go first, then the seed.
        let indexes = [
            "CREATE INDEX udb_counter_idx IF NOT EXISTS \
             FOR (c:UdbCounter) ON (c.run_tag, c.id)",
            "CREATE INDEX udb_outbox_event_idx IF NOT EXISTS \
             FOR (o:UdbOutbox) ON (o.run_tag, o.event_id)",
            "CREATE INDEX udb_lease_name_idx IF NOT EXISTS \
             FOR (l:UdbLease) ON (l.run_tag, l.lease_name)",
        ];
        for ddl in indexes {
            self.executor.cypher_rows(ddl, json!({})).await?;
        }

        // Seed the counter node if absent (idempotent MERGE; ON CREATE seeds 0
        // so a fresh store reports outbox_max_seq == 0 — the contract's
        // freshness assertion).
        self.executor
            .cypher_rows(
                "MERGE (c:UdbCounter {run_tag:$tag, id:$id}) \
                 ON CREATE SET c.seq = 0",
                json!({ "tag": self.tag(), "id": OUTBOX_SEQ_ID }),
            )
            .await?;
        Ok(())
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        // Atomic within ONE HTTP transaction: MERGE+bump the counter, then CREATE
        // the event node carrying the freshly-allocated seq. Once the counter
        // node exists, MERGE takes a write lock on it, so concurrent enqueues
        // serialise and every event gets a distinct, monotone seq. The payload
        // is stored as a JSON string (Neo4j node properties cannot hold nested
        // maps).
        //
        // KNOWN PHASE-1 CAVEAT: on Community Edition there is no uniqueness
        // constraint backing the counter (composite uniqueness is Enterprise-
        // only), so a first-ever enqueue racing another first-ever enqueue could
        // briefly create two counter nodes. `ensure_system_tables` seeds the
        // counter exactly once before any enqueue, which closes that window in
        // the normal startup path; a hardened phase-2 path can pre-seed under a
        // single-property constraint or an explicit lock node.
        let payload_text = serde_json::to_string(payload)
            .map_err(|e| format!("outbox payload serialise failed: {e}"))?;
        let cypher = "MERGE (c:UdbCounter {run_tag:$tag, id:$id}) \
             ON CREATE SET c.seq = 1 \
             ON MATCH SET c.seq = c.seq + 1 \
             WITH c.seq AS s \
             CREATE (:UdbOutbox {run_tag:$tag, event_seq:s, event_id:$eid, \
                 topic:$topic, partition_key:$pk, payload:$payload, \
                 created_at:timestamp()}) \
             RETURN s";
        let rows = self
            .executor
            .cypher_rows(
                cypher,
                json!({
                    "tag": self.tag(),
                    "id": OUTBOX_SEQ_ID,
                    "eid": event_id,
                    "topic": topic,
                    "pk": partition_key,
                    "payload": payload_text,
                }),
            )
            .await?;
        let seq = rows
            .first()
            .and_then(|r| r.get("s"))
            .and_then(Json::as_i64)
            .ok_or_else(|| "outbox enqueue did not return a sequence".to_string())?;
        Ok(seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        // The counter node is the authoritative high-water mark (it advances in
        // the same tx that creates the event node and never regresses). 0 when
        // the counter is absent / freshly seeded.
        let rows = self
            .executor
            .cypher_rows(
                "MATCH (c:UdbCounter {run_tag:$tag, id:$id}) RETURN c.seq AS seq",
                json!({ "tag": self.tag(), "id": OUTBOX_SEQ_ID }),
            )
            .await?;
        let seq = rows
            .first()
            .and_then(|r| r.get("seq"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        Ok(seq)
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let seq = self.outbox_max_seq().await?;
        Ok(DurabilityToken::new("neo4j", seq.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("neo4j") {
            return Err(format!(
                "Neo4jCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token
            .value
            .parse()
            .map_err(|e| format!("malformed neo4j durability token '{}': {e}", token.value))?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, super::NEO4J_DURABILITY_POLL_MS);
        loop {
            if self.outbox_max_seq().await? >= target {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        // Neo4j has no tables; the lease "table" is a composite RANGE INDEX on
        // (run_tag, lease_name) (Community-compatible; see `ensure_system_tables`
        // for why NOT a uniqueness constraint). Idempotent; safe to call
        // independently of `ensure_system_tables`.
        self.executor
            .cypher_rows(
                "CREATE INDEX udb_lease_name_idx IF NOT EXISTS \
                 FOR (l:UdbLease) ON (l.run_tag, l.lease_name)",
                json!({}),
            )
            .await?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, String> {
        // Single-statement atomic acquire mapping PG's
        //   INSERT … ON CONFLICT DO UPDATE … WHERE expired OR same-owner.
        //
        // `MERGE (l {lease_name})` either creates the node (fresh) or matches the
        // existing one. The `WITH l WHERE l.owner_id = $o OR l.expires_at <= $now`
        // gate is the disjunction: when it FAILS (a live lease held by a
        // DIFFERENT owner) the row is filtered out → SET does not run → RETURN
        // yields no rows → Ok(false). When it PASSES we SET owner+expires and
        // RETURN the (now-our) owner → Ok(true).
        //
        // The six contract cases, verified against this Cypher:
        //   1. fresh (no node)            → MERGE creates, ON CREATE sets owner=$o;
        //                                    gate `owner=$o` true → RETURN $o → true.
        //   2. live, different owner      → MERGE matches; gate `owner=$o`(no) OR
        //                                    `expires<=now`(no) → filtered → no row → false.
        //   3. same owner, refresh        → MERGE matches; gate `owner=$o` true →
        //                                    SET refreshes expires → RETURN $o → true.
        //   4. wrong-owner release        → handled in release (owner-scoped DELETE).
        //   5. owner release then reacquire→ release DELETEs the node → next acquire
        //                                    is case 1 (fresh) → true.
        //   6. zero-ttl takeover          → ttl=0 makes expires_at = now; a later
        //                                    acquirer sees `expires<=now` true →
        //                                    SET takes over → RETURN new owner → true.
        // (And the live-different-owner DENY of case 2/5 is the false branch.)
        let now = Self::now_unix_ms();
        let new_expires = now + (ttl.as_millis() as i64);
        let cypher = "MERGE (l:UdbLease {run_tag:$tag, lease_name:$n}) \
             ON CREATE SET l.owner_id = $o, l.expires_at = $exp \
             WITH l WHERE l.owner_id = $o OR l.expires_at <= $now \
             SET l.owner_id = $o, l.expires_at = $exp \
             RETURN l.owner_id AS owner";
        let rows = self
            .executor
            .cypher_rows(
                cypher,
                json!({
                    "tag": self.tag(),
                    "n": lease_name,
                    "o": owner_id,
                    "exp": new_expires,
                    "now": now,
                }),
            )
            .await?;
        let acquired = rows
            .first()
            .and_then(|r| r.get("owner"))
            .and_then(Json::as_str)
            .map(|owner| owner == owner_id)
            .unwrap_or(false);
        Ok(acquired)
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        // Owner-scoped DELETE: the `WHERE l.owner_id = $o` makes a wrong-owner
        // release a no-op (contract case 4) instead of yanking another worker's
        // lease, and an idempotent delete for the right owner.
        self.executor
            .cypher_rows(
                "MATCH (l:UdbLease {run_tag:$tag, lease_name:$n}) \
                 WHERE l.owner_id = $o DELETE l",
                json!({ "tag": self.tag(), "n": lease_name, "o": owner_id }),
            )
            .await?;
        Ok(())
    }
}

// ── B.10b phase 2: shared node-label constants + row-cell helpers ─────────────
//
// The four system-store sibling files (`neo4j_projection.rs`, `neo4j_saga.rs`,
// `neo4j_admin_audit.rs`, `neo4j_migration_audit.rs`) all map one domain row to
// one labeled node carrying `run_tag` + the domain id, and all parse the
// column-keyed `Json` rows `cypher_rows`/`cypher_tx_rows` return back into typed
// fields. These constants + helpers are the single source of truth for both, so
// the siblings can't drift on label names or row-cell decoding.

/// Node label for one projection task (`:UdbProjectionTask`).
pub(super) const LABEL_PROJECTION_TASK: &str = "UdbProjectionTask";
/// Node label for one saga (`:UdbSaga`).
pub(super) const LABEL_SAGA: &str = "UdbSaga";
/// Node label for one admin-audit-log row (`:UdbAuditLog`).
pub(super) const LABEL_AUDIT_LOG: &str = "UdbAuditLog";
/// Node label for one migration run (`:UdbMigrationRun`).
pub(super) const LABEL_MIGRATION_RUN: &str = "UdbMigrationRun";
/// Node label for one migration op-ledger entry (`:UdbMigrationOp`).
pub(super) const LABEL_MIGRATION_OP: &str = "UdbMigrationOp";

/// Unix milliseconds now, as a free function so the phase-2 sibling files can
/// stamp client-computed `created_at`/`updated_at`/`finished_at`/… epoch-millis
/// integers identically to the phase-1 lease math.
pub(super) fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Read a string property from a column-keyed row, defaulting to empty.
///
/// The HTTP API returns a node map projection (`t{.*}`) as a JSON object whose
/// keys are the property names, so the siblings RETURN `node{.*} AS <alias>`
/// and look properties up by name here.
pub(super) fn prop_str(node: &Json, key: &str) -> String {
    node.get(key)
        .and_then(Json::as_str)
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Read an `i64` property (epoch-millis / counter / retry-count) → 0 when
/// absent/non-numeric. Neo4j integer math returns JSON numbers.
pub(super) fn prop_i64(node: &Json, key: &str) -> i64 {
    node.get(key).and_then(Json::as_i64).unwrap_or(0)
}

/// Read an `i32` property (retry_count / current_step / operation_index).
pub(super) fn prop_i32(node: &Json, key: &str) -> i32 {
    prop_i64(node, key) as i32
}

/// Read a `Uuid` property from a string cell. The siblings store the domain id
/// as a node property string (e.g. `task_id`/`saga_id`/`audit_id`/`run_id`).
pub(super) fn prop_uuid(
    node: &Json,
    key: &str,
) -> Result<uuid::Uuid, super::system_store::SystemStoreError> {
    let raw = node.get(key).and_then(Json::as_str).ok_or_else(|| {
        super::system_store::SystemStoreError::InvalidInput(format!(
            "neo4j row missing string uuid property '{key}'"
        ))
    })?;
    uuid::Uuid::parse_str(raw).map_err(|e| {
        super::system_store::SystemStoreError::InvalidInput(format!(
            "neo4j row property '{key}' is not a uuid '{raw}': {e}"
        ))
    })
}

/// Read a REQUIRED epoch-millis property → `DateTime<Utc>`, falling back to
/// `Utc::now()` when absent/unparseable (matching the SQL impls' tolerant
/// `unwrap_or_else(|_| Utc::now())` on `created_at`/`updated_at`/`started_at`).
pub(super) fn prop_dt(node: &Json, key: &str) -> chrono::DateTime<chrono::Utc> {
    prop_opt_dt(node, key).unwrap_or_else(chrono::Utc::now)
}

/// Read an OPTIONAL epoch-millis property: `None` when the property is absent or
/// JSON-null, `Some` when a millis integer is present. This is the round-trip
/// discipline the MSSQL bug flagged — a missing `finished_at`/`applied_at`/
/// `completed_at`/`next_retry_at` must parse back to `None` (not `Some(now)`).
pub(super) fn prop_opt_dt(node: &Json, key: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis = node.get(key).and_then(Json::as_i64)?;
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
}

/// Decode a JSON-shaped string property back into `serde_json::Value`. The
/// siblings store JSON columns as Cypher string properties (Neo4j node
/// properties cannot hold nested maps), so this parses the stored text.
/// Absent/null/unparseable → `default`.
pub(super) fn prop_json(node: &Json, key: &str, default: Json) -> Json {
    match node.get(key).and_then(Json::as_str) {
        Some(s) => serde_json::from_str(s).unwrap_or(default),
        None => default,
    }
}

/// Map an executor error string to a typed `SystemStoreError::Io` for the
/// `"neo4j"` backend. `op` names the failed operation. The siblings funnel
/// every `cypher_rows`/`cypher_tx_rows` error through this so the typed-error
/// surface matches the SQL/Cassandra impls.
pub(super) fn neo_err(
    op: &str,
    err: impl std::fmt::Display,
) -> super::system_store::SystemStoreError {
    super::system_store::SystemStoreError::Io {
        backend: "neo4j",
        source: format!("{op}: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::executors::neo4j::Neo4jConfig;

    fn dummy_store() -> Neo4jCanonicalStore {
        let exec = Neo4jExecutor::new(Neo4jConfig {
            http_base: "http://localhost:7474".to_string(),
            username: "neo4j".to_string(),
            password: "secret".to_string(),
            database: "neo4j".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        });
        Neo4jCanonicalStore::new(exec, "primary")
    }

    /// Pin: backend label is `"neo4j"` exactly (registry key + token identity).
    #[test]
    fn backend_label_is_pinned() {
        let store = dummy_store();
        assert_eq!(store.backend_label(), "neo4j");
        assert_eq!(store.instance_name(), "primary");
    }

    /// Pin: a foreign-backend token is rejected before any HTTP call.
    #[tokio::test]
    async fn wait_for_token_rejects_foreign_backend() {
        let store = dummy_store();
        let foreign = DurabilityToken::new("postgres", "0/100");
        let err = store
            .wait_for_token(&foreign, Duration::from_millis(1))
            .await
            .expect_err("foreign token must be rejected");
        assert!(err.contains("cannot wait on"));
    }

    /// Pin: a malformed (non-integer) neo4j token surfaces as an error, not a
    /// silent hang.
    #[tokio::test]
    async fn wait_for_token_rejects_malformed_value() {
        let store = dummy_store();
        let bad = DurabilityToken::new("neo4j", "not-an-int");
        let err = store
            .wait_for_token(&bad, Duration::from_millis(1))
            .await
            .expect_err("malformed token must error");
        assert!(err.contains("malformed"));
    }
}
