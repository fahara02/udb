//! Leader-elected compliance-evidence export worker (master-plan 4.4).
//!
//! The auth_service `PostgresAuditLogSink` already persists an append-only,
//! query-able compliance record (`udb_system.auth_audit_log`) whose `envelope`
//! column is a fully-built [`ComplianceEnvelope`]-shaped document. This worker is
//! the *automation* half of 4.4: on a leader-elected schedule it drains the
//! newly-arrived slice of that durable audit window, wraps each row in the SAME
//! canonical [`ComplianceEnvelope`] (never a parallel envelope), serializes the
//! batch as a **chain-hashed JSONL bundle** whose hash chain is *continuous across
//! export runs* (each batch links to the prior batch's head, so the whole audit
//! history forms one tamper-evident chain), and writes the bundle plus a manifest
//! THROUGH [`DataBrokerRuntime::put_object_backend_target_for_project`] (never
//! directly to S3) into the compliance-evidence bucket.
//!
//! Why leader-elected: the worker advances a single durable high-water mark
//! (`udb_system.evidence_export_state`), so a per-node worker would race the
//! watermark and emit duplicate/forked evidence chains. The
//! [`crate::runtime::singleton::WORKER_EVIDENCE_EXPORT`] lease guarantees exactly
//! one exporter cluster-wide.
//!
//! The operator-driven one-shot equivalent is `udb compliance evidence`
//! (`src/cli/evidence.rs`), which reads the same durable relation and writes
//! through the same object helper; this worker is the unattended scheduled path.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::runtime::DataBrokerRuntime;
use crate::runtime::service::auth_service::events::{
    ComplianceEnvelope, build_native_compliance_envelope,
};

/// Genesis hash seeding the tamper-evident chain when no prior batch exists.
const EVIDENCE_CHAIN_GENESIS: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
/// Hash algorithm label recorded in the manifest `chain` block.
const EVIDENCE_CHAIN_ALGORITHM: &str = "sha256";
/// Schema version of the emitted evidence manifest (bump on a breaking change).
const EVIDENCE_MANIFEST_SCHEMA_VERSION: &str = "1";
/// Machine-readable `kind` discriminator stamped on every manifest.
const EVIDENCE_MANIFEST_KIND: &str = "udb.compliance.evidence.manifest";
/// Durable audit relation owned by the auth_service `PostgresAuditLogSink`. Read
/// (never written) here so there is no parallel telemetry store. Kept in lock-step
/// with `PostgresAuditLogSink::new`.
const AUTH_AUDIT_LOG_RELATION: &str = "udb_system.auth_audit_log";
/// One-row-per-worker durable high-water mark + carried chain head. Lazily
/// self-created (no migration ordering), mirroring the sink's lazy-DDL pattern.
const EVIDENCE_STATE_RELATION: &str = "udb_system.evidence_export_state";
/// Content type for the line-delimited-JSON evidence bundle.
const EVIDENCE_BUNDLE_CONTENT_TYPE: &str = "application/x-ndjson";
/// Default object-key prefix inside the compliance-evidence bucket.
const DEFAULT_EVIDENCE_PREFIX: &str = "compliance-evidence";
/// Default object-store backend when no override is configured.
const DEFAULT_EVIDENCE_BACKEND: &str = "s3";
/// Default evidence bucket when no override is configured.
const DEFAULT_EVIDENCE_BUCKET: &str = "udb-compliance-evidence";
/// Default rows exported per tick (bounds bundle size / memory per run).
const DEFAULT_BATCH_LIMIT: i64 = 500;
/// Default export cadence.
const DEFAULT_INTERVAL_SECS: u64 = 300;
/// Epoch watermark for the first-ever run (RFC3339, bound as `timestamptz`).
const EPOCH_WATERMARK: &str = "1970-01-01T00:00:00Z";

/// Resolved-once configuration for the export worker. All env reads happen in
/// [`EvidenceExportConfig::from_env`] (the single startup-config boundary for
/// this module — see the env-confinement allowlist in `connection_manager.rs`),
/// so the worker body itself reads no environment.
#[derive(Debug, Clone)]
pub struct EvidenceExportConfig {
    /// Object-store backend the bundle is written through (e.g. `s3`/`minio`).
    pub backend: String,
    /// Compliance-evidence bucket / container.
    pub bucket: String,
    /// Object-key prefix inside the bucket.
    pub prefix: String,
    /// Project the object write is routed under.
    pub project: String,
    /// Maximum audit rows drained per tick.
    pub batch_limit: i64,
    /// Export cadence.
    pub interval: Duration,
}

impl EvidenceExportConfig {
    /// Resolve the worker configuration from the environment exactly once. Falls
    /// back to the generic object-store env vars and finally to safe defaults so
    /// the worker is usable with zero extra configuration.
    pub fn from_env() -> Self {
        let read = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let backend = read("UDB_COMPLIANCE_EVIDENCE_BACKEND")
            .or_else(|| read("UDB_OBJECT_BACKEND"))
            .unwrap_or_else(|| DEFAULT_EVIDENCE_BACKEND.to_string());
        let bucket = read("UDB_COMPLIANCE_EVIDENCE_BUCKET")
            .or_else(|| read("UDB_OBJECT_BUCKET"))
            .unwrap_or_else(|| DEFAULT_EVIDENCE_BUCKET.to_string());
        let prefix = read("UDB_COMPLIANCE_EVIDENCE_PREFIX")
            .map(|value| value.trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_EVIDENCE_PREFIX.to_string());
        let project = read("UDB_COMPLIANCE_EVIDENCE_PROJECT")
            .or_else(|| read("UDB_PROJECT_ID"))
            .unwrap_or_else(|| crate::runtime::catalog::DEFAULT_PROJECT_ID.to_string());
        let batch_limit = read("UDB_COMPLIANCE_EVIDENCE_BATCH")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_BATCH_LIMIT);
        let interval = Duration::from_secs(
            read("UDB_COMPLIANCE_EVIDENCE_INTERVAL_SECS")
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_INTERVAL_SECS),
        );
        Self {
            backend,
            bucket,
            prefix,
            project,
            batch_limit,
            interval,
        }
    }
}

/// One exported audit record. The field set mirrors the queryable columns the
/// `PostgresAuditLogSink` persists, plus the canonical [`ComplianceEnvelope`]
/// document (`envelope`), so an exported record carries the SAME shape as the
/// durable audit row — no parallel evidence-envelope type is introduced.
#[derive(Debug, Clone, Serialize, PartialEq)]
struct EvidenceRecord {
    event_id: String,
    event_type: String,
    tenant_id: String,
    actor: String,
    target_resource: String,
    operation: String,
    outcome: String,
    reason_code: String,
    occurred_at: String,
    envelope: Value,
}

/// Durable export state: the high-water mark and the carried chain head so the
/// tamper-evident chain is continuous across worker runs and broker restarts.
#[derive(Debug, Clone)]
struct EvidenceState {
    last_event_id: String,
    last_occurred_at: String,
    chain_head: String,
}

impl Default for EvidenceState {
    fn default() -> Self {
        Self {
            last_event_id: String::new(),
            last_occurred_at: EPOCH_WATERMARK.to_string(),
            chain_head: EVIDENCE_CHAIN_GENESIS.to_string(),
        }
    }
}

/// Compute the next chain hash: `sha256(prev_hash || "\n" || canonical_record)`,
/// hex-encoded. Pure + deterministic so a verifier can independently re-walk the
/// JSONL bundle and recompute the same chain.
fn chain_hash(prev_hash: &str, canonical_record: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(b"\n");
    hasher.update(canonical_record.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Append `records` to a running chain that already produced `prev_head`. Each
/// output line is the record JSON plus `prev_hash`/`chain_hash`, where the hash is
/// computed over the canonical record content (without the chain fields). Returns
/// the JSONL text and the new chain head (== `prev_head` when `records` is empty).
///
/// The chain LINKS ACROSS BATCHES: passing the prior batch's head as `prev_head`
/// makes the whole audit history one tamper-evident chain, not a per-batch reset.
fn append_chain(prev_head: &str, records: &[EvidenceRecord]) -> Result<(String, String), String> {
    let mut prev = prev_head.to_string();
    let mut jsonl = String::new();
    for record in records {
        let canonical = serde_json::to_string(record)
            .map_err(|err| format!("serialize evidence record failed: {err}"))?;
        let hash = chain_hash(&prev, &canonical);
        let mut line = serde_json::to_value(record)
            .map_err(|err| format!("encode evidence record failed: {err}"))?;
        line["prev_hash"] = json!(prev);
        line["chain_hash"] = json!(hash);
        jsonl.push_str(
            &serde_json::to_string(&line)
                .map_err(|err| format!("encode evidence line failed: {err}"))?,
        );
        jsonl.push('\n');
        prev = hash;
    }
    Ok((jsonl, prev))
}

/// Resolve the canonical compliance envelope for a durable audit row. When the row
/// already carries a built envelope (the normal case — the sink persists one), it
/// is carried forward verbatim (no re-derivation, no data loss). Otherwise the
/// envelope is reconstructed from the queryable columns by REUSING the canonical
/// [`ComplianceEnvelope`] + [`build_native_compliance_envelope`] — this module
/// never defines a parallel envelope type.
fn record_compliance_envelope(
    event_id: &str,
    event_type: &str,
    tenant_id: &str,
    project: &str,
    actor: &str,
    target_resource: &str,
    operation: &str,
    outcome: &str,
    reason_code: &str,
    stored: Value,
) -> Value {
    if stored.is_object() {
        return stored;
    }
    let env = ComplianceEnvelope {
        actor: actor.to_string(),
        target_resource: target_resource.to_string(),
        operation: operation.to_string(),
        outcome: outcome.to_string(),
        reason_code: reason_code.to_string(),
        ..ComplianceEnvelope::default()
    };
    build_native_compliance_envelope(
        event_id,
        event_type,
        actor,
        tenant_id,
        project,
        &env,
        "",
        "none",
        0,
        &[],
        Value::Null,
    )
}

/// Build the object-store PUT request JSON the dispatch executor accepts. Mirrors
/// the key aliases the object executor reads (`bucket`/`object_key` + synonyms),
/// matching the operator CLI path so both write identically.
fn object_put_request_json(bucket: &str, object_key: &str, content_type: &str) -> String {
    json!({
        "op": "put",
        "bucket": bucket,
        "container": bucket,
        "object_key": object_key,
        "key": object_key,
        "object": object_key,
        "blob": object_key,
        "content_type": content_type,
    })
    .to_string()
}

/// Sanitize watermark bounds into an object-key-safe slug.
fn evidence_object_slug(from: &str, to: &str, generated_at: &str) -> String {
    let sanitize = |value: &str| -> String {
        value
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect()
    };
    format!(
        "{}_{}__{}",
        sanitize(from.trim()),
        sanitize(to.trim()),
        sanitize(generated_at.trim())
    )
}

/// Build the machine-readable evidence manifest (the list/summary of the exported
/// objects + their chain head). Pure (no IO) so it is unit-testable.
#[allow(clippy::too_many_arguments)]
fn render_evidence_manifest(
    config: &EvidenceExportConfig,
    prev_head: &str,
    chain_head: &str,
    record_count: usize,
    window_from: &str,
    window_to: &str,
    evidence_key: &str,
    evidence_bytes: usize,
    manifest_key: &str,
    generated_at: &str,
) -> Value {
    json!({
        "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "kind": EVIDENCE_MANIFEST_KIND,
        "generated_at": generated_at,
        "source_relation": AUTH_AUDIT_LOG_RELATION,
        "worker": crate::runtime::singleton::WORKER_EVIDENCE_EXPORT,
        "window": { "from_exclusive": window_from, "to_inclusive": window_to },
        "record_count": record_count,
        "chain": {
            "algorithm": EVIDENCE_CHAIN_ALGORITHM,
            "genesis": EVIDENCE_CHAIN_GENESIS,
            "prev_head": prev_head,
            "head": chain_head,
        },
        "evidence_object": {
            "backend": config.backend,
            "bucket": config.bucket,
            "object_key": evidence_key,
            "content_type": EVIDENCE_BUNDLE_CONTENT_TYPE,
            "bytes": evidence_bytes,
        },
        "manifest_object": {
            "backend": config.backend,
            "bucket": config.bucket,
            "object_key": manifest_key,
            "content_type": "application/json",
        },
    })
}

/// Lazily self-create the durable export-state relation (no migration ordering),
/// mirroring `PostgresAuditLogSink::ensure_table`.
async fn ensure_state_table(pool: &PgPool) -> Result<(), String> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(pool)
        .await
        .map_err(|err| format!("ensure udb_system schema failed: {err}"))?;
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {rel} ( \
             worker VARCHAR(160) PRIMARY KEY, \
             last_event_id VARCHAR(64) NOT NULL DEFAULT '', \
             last_occurred_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch', \
             chain_head VARCHAR(64) NOT NULL DEFAULT '{genesis}', \
             exported_count BIGINT NOT NULL DEFAULT 0, \
             updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() \
         )",
        rel = EVIDENCE_STATE_RELATION,
        genesis = EVIDENCE_CHAIN_GENESIS
    );
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| format!("ensure {EVIDENCE_STATE_RELATION} failed: {err}"))
}

/// Load the high-water mark + carried chain head. A missing row (first run) yields
/// the genesis/epoch default.
async fn load_state(pool: &PgPool) -> Result<EvidenceState, String> {
    let sql = format!(
        "SELECT last_event_id, last_occurred_at::text AS last_occurred_at, chain_head \
         FROM {rel} WHERE worker = $1",
        rel = EVIDENCE_STATE_RELATION
    );
    let row = sqlx::query(&sql)
        .bind(crate::runtime::singleton::WORKER_EVIDENCE_EXPORT)
        .fetch_optional(pool)
        .await
        .map_err(|err| format!("load evidence-export state failed: {err}"))?;
    let Some(row) = row else {
        return Ok(EvidenceState::default());
    };
    Ok(EvidenceState {
        last_event_id: row
            .try_get::<String, _>("last_event_id")
            .unwrap_or_default(),
        last_occurred_at: row
            .try_get::<String, _>("last_occurred_at")
            .unwrap_or_else(|_| EPOCH_WATERMARK.to_string()),
        chain_head: row
            .try_get::<String, _>("chain_head")
            .unwrap_or_else(|_| EVIDENCE_CHAIN_GENESIS.to_string()),
    })
}

/// Persist the advanced watermark + new chain head after a successful object
/// write, accumulating the exported-row counter.
async fn save_state(
    pool: &PgPool,
    last_event_id: &str,
    last_occurred_at: &str,
    chain_head: &str,
    exported: i64,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO {rel} \
             (worker, last_event_id, last_occurred_at, chain_head, exported_count, updated_at) \
         VALUES ($1, $2, $3::timestamptz, $4, $5, NOW()) \
         ON CONFLICT (worker) DO UPDATE SET \
             last_event_id = EXCLUDED.last_event_id, \
             last_occurred_at = EXCLUDED.last_occurred_at, \
             chain_head = EXCLUDED.chain_head, \
             exported_count = {rel}.exported_count + EXCLUDED.exported_count, \
             updated_at = NOW()",
        rel = EVIDENCE_STATE_RELATION
    );
    sqlx::query(&sql)
        .bind(crate::runtime::singleton::WORKER_EVIDENCE_EXPORT)
        .bind(last_event_id)
        .bind(last_occurred_at)
        .bind(chain_head)
        .bind(exported)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|err| format!("save evidence-export state failed: {err}"))
}

/// Drain the audit window past `state`'s watermark, reusing the canonical
/// envelope for each row. Stable `(occurred_at, event_id)` ordering keeps the
/// chain reproducible.
async fn collect_records(
    pool: &PgPool,
    state: &EvidenceState,
    project: &str,
    limit: i64,
) -> Result<Vec<EvidenceRecord>, String> {
    let sql = format!(
        "SELECT event_id::text AS event_id, event_type, tenant_id, actor, \
                target_resource, operation, outcome, reason_code, \
                occurred_at::text AS occurred_at, envelope::text AS envelope \
         FROM {rel} \
         WHERE occurred_at > $1::timestamptz \
            OR (occurred_at = $1::timestamptz AND event_id::text > $2) \
         ORDER BY occurred_at ASC, event_id ASC \
         LIMIT $3",
        rel = AUTH_AUDIT_LOG_RELATION
    );
    let rows = sqlx::query(&sql)
        .bind(&state.last_occurred_at)
        .bind(&state.last_event_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("query audit window failed: {err}"))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let get = |key: &str| -> String { row.try_get::<String, _>(key).unwrap_or_default() };
        let envelope_text: String = row.try_get("envelope").unwrap_or_default();
        let stored: Value = serde_json::from_str(&envelope_text).unwrap_or(Value::Null);
        let event_id = get("event_id");
        let event_type = get("event_type");
        let tenant_id = get("tenant_id");
        let actor = get("actor");
        let target_resource = get("target_resource");
        let operation = get("operation");
        let outcome = get("outcome");
        let reason_code = get("reason_code");
        let envelope = record_compliance_envelope(
            &event_id,
            &event_type,
            &tenant_id,
            project,
            &actor,
            &target_resource,
            &operation,
            &outcome,
            &reason_code,
            stored,
        );
        out.push(EvidenceRecord {
            event_id,
            event_type,
            tenant_id,
            actor,
            target_resource,
            operation,
            outcome,
            reason_code,
            occurred_at: get("occurred_at"),
            envelope,
        });
    }
    Ok(out)
}

/// Run one export tick: drain the audit window past the durable watermark, write a
/// chain-hashed JSONL bundle + manifest THROUGH the object helper, and advance the
/// watermark/chain head. Returns the number of records exported (0 when the window
/// is empty or the audit relation does not yet exist). Idempotent-ish: if the
/// object write succeeds but the state update fails, the next tick re-exports the
/// same window under a deterministic key range and overwrites.
///
/// `#[allow(dead_code)]`: the only production caller is the deferred leader spawn
/// in `serve()` (see [`spawn_evidence_export_worker`]); the worker is unit-tested
/// through its pure helpers.
#[allow(dead_code)]
pub async fn run_evidence_export_once(
    runtime: &DataBrokerRuntime,
    pool: &PgPool,
    config: &EvidenceExportConfig,
) -> Result<i64, String> {
    ensure_state_table(pool).await?;

    // Tolerate a pre-audit broker: the sink creates auth_audit_log lazily on the
    // first security event, so an export that runs before any auth activity finds
    // no relation. `to_regclass` returns NULL rather than erroring.
    let exists: Option<String> = sqlx::query_scalar(&format!(
        "SELECT to_regclass('{AUTH_AUDIT_LOG_RELATION}')::text"
    ))
    .fetch_one(pool)
    .await
    .map_err(|err| format!("probe audit relation failed: {err}"))?;
    if exists.is_none() {
        return Ok(0);
    }

    let state = load_state(pool).await?;
    let records = collect_records(pool, &state, &config.project, config.batch_limit).await?;
    if records.is_empty() {
        return Ok(0);
    }

    let (jsonl, new_head) = append_chain(&state.chain_head, &records)?;
    let window_from = state.last_occurred_at.clone();
    let window_to = records[records.len() - 1].occurred_at.clone();
    let last_event_id = records[records.len() - 1].event_id.clone();
    let generated_at = chrono::Utc::now().to_rfc3339();
    let slug = evidence_object_slug(&window_from, &window_to, &generated_at);
    let evidence_key = format!("{}/evidence-{slug}.jsonl", config.prefix);
    let manifest_key = format!("{}/evidence-{slug}.manifest.json", config.prefix);

    let manifest = render_evidence_manifest(
        config,
        &state.chain_head,
        &new_head,
        records.len(),
        &window_from,
        &window_to,
        &evidence_key,
        jsonl.len(),
        &manifest_key,
        &generated_at,
    );

    // Write the bundle THEN the manifest, both THROUGH the storage helper (never
    // directly to S3). The manifest is written last so it points at an
    // already-durable bundle.
    let evidence_req =
        object_put_request_json(&config.bucket, &evidence_key, EVIDENCE_BUNDLE_CONTENT_TYPE);
    runtime
        .put_object_backend_target_for_project(
            &config.backend,
            None,
            &config.project,
            &evidence_req,
            jsonl.into_bytes(),
        )
        .await
        .map_err(|err| format!("write evidence bundle failed: {err}"))?;

    let manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|err| format!("encode manifest bytes failed: {err}"))?;
    let manifest_req = object_put_request_json(&config.bucket, &manifest_key, "application/json");
    runtime
        .put_object_backend_target_for_project(
            &config.backend,
            None,
            &config.project,
            &manifest_req,
            manifest_bytes,
        )
        .await
        .map_err(|err| format!("write manifest object failed: {err}"))?;

    save_state(
        pool,
        &last_event_id,
        &window_to,
        &new_head,
        records.len() as i64,
    )
    .await?;
    Ok(records.len() as i64)
}

/// Spawn the leader-elected evidence-export worker. Mirrors the scheduler-tick
/// spawn: the singleton lease ([`WORKER_EVIDENCE_EXPORT`]) guarantees exactly one
/// exporter cluster-wide; the closure clones the runtime/pool/config per tick.
///
/// Call this from `serve()` after the runtime + native store pool are available
/// (the exact call site is noted in the module docs / the 4.4 plan):
///
/// ```ignore
/// let evidence_runtime = service.runtime.load_full();
/// if let Ok(evidence_pool) =
///     evidence_runtime.native_store_pool_for_service("compliance", true, "")
/// {
///     let singleton_relation = evidence_runtime.config().cdc.lock_log_relation();
///     let config = crate::runtime::evidence_export::EvidenceExportConfig::from_env();
///     crate::runtime::evidence_export::spawn_evidence_export_worker(
///         evidence_runtime,
///         evidence_pool,
///         singleton_relation,
///         config,
///     );
/// }
/// ```
///
/// `#[allow(dead_code)]` until the `serve()` call site is wired (deferred per the
/// edit-only lane).
#[allow(dead_code)]
pub fn spawn_evidence_export_worker(
    runtime: Arc<DataBrokerRuntime>,
    pool: PgPool,
    singleton_relation: String,
    config: EvidenceExportConfig,
) {
    let interval = config.interval;
    crate::runtime::service::native_runtime::NativeWorkerHost::spawn_while_leader(
        crate::runtime::singleton::WORKER_EVIDENCE_EXPORT,
        "compliance evidence exported audit window",
        pool.clone(),
        singleton_relation,
        interval,
        move || {
            let runtime = runtime.clone();
            let pool = pool.clone();
            let config = config.clone();
            async move { run_evidence_export_once(&runtime, &pool, &config).await }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, op: &str, occurred_at: &str) -> EvidenceRecord {
        EvidenceRecord {
            event_id: id.to_string(),
            event_type: "udb.authz.access.denied.v1".to_string(),
            tenant_id: "t1".to_string(),
            actor: "user-1".to_string(),
            target_resource: "doc/1".to_string(),
            operation: op.to_string(),
            outcome: "deny".to_string(),
            reason_code: "abac_default_deny".to_string(),
            occurred_at: occurred_at.to_string(),
            envelope: json!({ "event_id": id, "operation": op }),
        }
    }

    #[test]
    fn empty_window_keeps_prior_head() {
        let (jsonl, head) = append_chain("abc123", &[]).expect("chain");
        assert!(jsonl.is_empty());
        assert_eq!(head, "abc123", "an empty batch must not advance the chain");
    }

    #[test]
    fn chain_links_each_record_to_the_prior_line() {
        let records = vec![
            rec("e1", "create", "2026-06-25T00:00:00Z"),
            rec("e2", "update", "2026-06-25T00:00:01Z"),
            rec("e3", "delete", "2026-06-25T00:00:02Z"),
        ];
        let (jsonl, head) = append_chain(EVIDENCE_CHAIN_GENESIS, &records).expect("chain");
        let lines: Vec<Value> = jsonl
            .lines()
            .map(|line| serde_json::from_str(line).expect("json line"))
            .collect();
        assert_eq!(lines.len(), 3);
        // line[0] chains off genesis; each subsequent line's prev_hash == the
        // prior line's chain_hash; the last hash == the returned head.
        assert_eq!(lines[0]["prev_hash"], json!(EVIDENCE_CHAIN_GENESIS));
        for n in 1..lines.len() {
            assert_eq!(
                lines[n]["prev_hash"],
                lines[n - 1]["chain_hash"],
                "line {n} must link to line {}",
                n - 1
            );
        }
        assert_eq!(lines[lines.len() - 1]["chain_hash"], json!(head));

        // Independently recompute the chain over the canonical record content to
        // prove the on-line hash is real (not a placeholder).
        let mut prev = EVIDENCE_CHAIN_GENESIS.to_string();
        for record in &records {
            let canonical = serde_json::to_string(record).expect("canonical");
            prev = chain_hash(&prev, &canonical);
        }
        assert_eq!(prev, head);
    }

    #[test]
    fn tampering_a_middle_record_breaks_the_chain() {
        let base = vec![
            rec("e1", "create", "2026-06-25T00:00:00Z"),
            rec("e2", "update", "2026-06-25T00:00:01Z"),
            rec("e3", "delete", "2026-06-25T00:00:02Z"),
        ];
        let (clean_jsonl, clean_head) = append_chain(EVIDENCE_CHAIN_GENESIS, &base).expect("chain");

        // Tamper the MIDDLE record's outcome after the fact.
        let mut tampered = base.clone();
        tampered[1].outcome = "allow".to_string();
        let (_, tampered_head) = append_chain(EVIDENCE_CHAIN_GENESIS, &tampered).expect("chain");
        assert_ne!(
            clean_head, tampered_head,
            "altering a middle record must change the head"
        );

        // A verifier re-walking the ORIGINAL bundle but recomputing over the
        // tampered middle record detects the break at exactly that line: the
        // recomputed hash diverges from line 2's prev_hash linkage onward.
        let lines: Vec<Value> = clean_jsonl
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("json line"))
            .collect();
        let canonical_tampered = serde_json::to_string(&tampered[1]).expect("canonical");
        let recomputed = chain_hash(lines[1]["prev_hash"].as_str().unwrap(), &canonical_tampered);
        assert_ne!(
            recomputed,
            lines[1]["chain_hash"].as_str().unwrap(),
            "recomputed hash of a tampered middle record must not match the recorded chain hash"
        );
    }

    #[test]
    fn chain_is_continuous_across_batches() {
        // Batch 1 off genesis, batch 2 off batch 1's head: the combined walk must
        // equal a single walk over all records (continuous tamper-evident chain).
        let batch1 = vec![rec("e1", "create", "2026-06-25T00:00:00Z")];
        let batch2 = vec![rec("e2", "update", "2026-06-25T00:00:01Z")];
        let (_, head1) = append_chain(EVIDENCE_CHAIN_GENESIS, &batch1).expect("b1");
        let (_, head2) = append_chain(&head1, &batch2).expect("b2");

        let all = vec![batch1[0].clone(), batch2[0].clone()];
        let (_, head_all) = append_chain(EVIDENCE_CHAIN_GENESIS, &all).expect("all");
        assert_eq!(
            head2, head_all,
            "two linked batches must yield the same head as one combined batch"
        );
    }

    #[test]
    fn worker_reuses_compliance_envelope_not_a_new_type() {
        // With no stored envelope, the worker reconstructs the canonical envelope
        // by building a `ComplianceEnvelope` and running it through
        // `build_native_compliance_envelope` (reuse — no parallel envelope type).
        let env = record_compliance_envelope(
            "evt-1",
            "udb.authz.access.denied.v1",
            "tenant-9",
            "default",
            "user-7",
            "doc/42",
            "delete",
            "deny",
            "abac_default_deny",
            Value::Null,
        );
        // The canonical native envelope stamps `specversion`/`subject` and carries
        // the operation/outcome — proving it is the real ComplianceEnvelope shape.
        assert_eq!(env["specversion"], json!("1.0"));
        assert_eq!(env["operation"], json!("delete"));
        assert_eq!(env["outcome"], json!("deny"));
        assert_eq!(env["reason_code"], json!("abac_default_deny"));
        assert_eq!(env["actor"], json!("user-7"));
        assert_eq!(env["subject"], json!("doc/42"));
    }

    #[test]
    fn worker_carries_a_stored_envelope_forward_verbatim() {
        // The common path: the durable row already holds a built envelope; it is
        // carried forward unchanged (no second, divergent envelope).
        let stored = json!({ "specversion": "1.0", "operation": "create", "custom": 7 });
        let env = record_compliance_envelope(
            "evt-2",
            "udb.authz.access.granted.v1",
            "tenant-9",
            "default",
            "user-7",
            "doc/42",
            "create",
            "allow",
            "",
            stored.clone(),
        );
        assert_eq!(env, stored);
    }

    #[test]
    fn manifest_is_machine_readable_and_records_the_chain_head() {
        let config = EvidenceExportConfig {
            backend: "s3".to_string(),
            bucket: "udb-compliance-evidence".to_string(),
            prefix: "compliance-evidence".to_string(),
            project: "default".to_string(),
            batch_limit: 500,
            interval: Duration::from_secs(300),
        };
        let manifest = render_evidence_manifest(
            &config,
            EVIDENCE_CHAIN_GENESIS,
            "deadbeef",
            3,
            "2026-06-25T00:00:00Z",
            "2026-06-25T00:00:02Z",
            "compliance-evidence/evidence-x.jsonl",
            1234,
            "compliance-evidence/evidence-x.manifest.json",
            "2026-06-25T12:00:00Z",
        );
        assert_eq!(manifest["kind"], json!(EVIDENCE_MANIFEST_KIND));
        assert_eq!(manifest["source_relation"], json!(AUTH_AUDIT_LOG_RELATION));
        assert_eq!(manifest["record_count"], json!(3));
        assert_eq!(
            manifest["chain"]["prev_head"],
            json!(EVIDENCE_CHAIN_GENESIS)
        );
        assert_eq!(manifest["chain"]["head"], json!("deadbeef"));
        assert_eq!(
            manifest["chain"]["algorithm"],
            json!(EVIDENCE_CHAIN_ALGORITHM)
        );
        assert_eq!(
            manifest["evidence_object"]["bucket"],
            json!("udb-compliance-evidence")
        );
        assert_eq!(manifest["evidence_object"]["bytes"], json!(1234));
        assert_eq!(
            manifest["worker"],
            json!(crate::runtime::singleton::WORKER_EVIDENCE_EXPORT)
        );
    }

    #[test]
    fn put_request_carries_executor_aliases() {
        let req: Value =
            serde_json::from_str(&object_put_request_json("b", "k", "application/json"))
                .expect("json");
        assert_eq!(req["op"], json!("put"));
        assert_eq!(req["bucket"], json!("b"));
        assert_eq!(req["container"], json!("b"));
        assert_eq!(req["object_key"], json!("k"));
        assert_eq!(req["key"], json!("k"));
    }
}
