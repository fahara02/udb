//! `udb compliance evidence` — master-plan 4.4 compliance-evidence automation.
//!
//! Thin CLI over existing machinery. It drains the durable auth-audit window
//! (the append-only `udb_system.auth_audit_log` relation OWNED by the
//! auth_service `PostgresAuditLogSink`) into a chain-hashed JSONL evidence
//! bundle, writes that bundle plus a machine-readable manifest THROUGH the
//! storage-service object helpers
//! (`DataBrokerRuntime::put_object_backend_target_for_project` — never directly
//! to S3), and prints the manifest.
//!
//! The per-record JSON is the same redacted `ComplianceEnvelope` shape the sink
//! persists (read back from the `envelope` JSONB column), so the bundle carries
//! no second, divergent telemetry envelope.
//!
//! The leader-elected worker that runs this on a schedule
//! (`udb::runtime::singleton::WORKER_EVIDENCE_EXPORT`) is a follow-up; this
//! command is the operator-driven / one-shot entry point over the same durable
//! record and the same storage path.

use super::*;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;

/// Schema version of the emitted evidence manifest. Bump on a breaking change to
/// the manifest JSON shape so older consumers can detect incompatibility.
const EVIDENCE_MANIFEST_SCHEMA_VERSION: &str = "1";
/// Machine-readable `kind` discriminator stamped on every manifest.
const EVIDENCE_MANIFEST_KIND: &str = "udb.compliance.evidence.manifest";
/// Genesis hash seeding the per-bundle tamper-evident chain (no prior record).
const EVIDENCE_CHAIN_GENESIS: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
/// Hash algorithm label recorded in the manifest `chain` block.
const EVIDENCE_CHAIN_ALGORITHM: &str = "sha256";
/// Durable audit relation written by the auth_service `PostgresAuditLogSink`.
/// This command READS the same relation the sink owns rather than introducing a
/// parallel telemetry store (4.4 doctrine: reuse the audit sink, no parallel
/// envelope). Kept in lock-step with `PostgresAuditLogSink::new`.
const AUTH_AUDIT_LOG_RELATION: &str = "udb_system.auth_audit_log";
/// Default object-key prefix inside the compliance-evidence bucket.
const DEFAULT_EVIDENCE_PREFIX: &str = "compliance-evidence";
/// Default object-store backend when neither `--backend` nor `UDB_OBJECT_BACKEND`
/// is set.
const DEFAULT_EVIDENCE_BACKEND: &str = "s3";
/// Default evidence bucket when neither `--bucket` nor an object-bucket env var
/// is set.
const DEFAULT_EVIDENCE_BUCKET: &str = "udb-compliance-evidence";
/// Content type for the line-delimited-JSON evidence bundle.
const EVIDENCE_BUNDLE_CONTENT_TYPE: &str = "application/x-ndjson";

/// One exported audit record. The field set mirrors the queryable columns the
/// `PostgresAuditLogSink` persists, plus the full redacted `ComplianceEnvelope`
/// JSON (`envelope`), so an exported record carries the SAME shape as the
/// durable audit row.
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

/// Compute the next chain hash: `sha256(prev_hash || "\n" || canonical_record)`,
/// hex-encoded. Pure + deterministic so the manifest head is reproducible and a
/// verifier can re-walk the JSONL bundle independently.
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

/// Render the records as a chain-hashed JSONL bundle. Each output line is the
/// record JSON plus `prev_hash`/`chain_hash`, where the hash is computed over the
/// canonical record content (without the chain fields). Returns the JSONL text
/// and the final chain head (the genesis hash when the window is empty).
fn build_evidence_bundle(records: &[EvidenceRecord]) -> Result<(String, String), String> {
    let mut prev = EVIDENCE_CHAIN_GENESIS.to_string();
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

/// Build the machine-readable evidence manifest value. Pure (no IO) so it is
/// unit-testable; the live path serializes it both to the local `--out` file and
/// to the storage-service object.
#[allow(clippy::too_many_arguments)]
fn render_evidence_manifest(
    since: &str,
    until: &str,
    tenant: &str,
    record_count: usize,
    chain_head: &str,
    backend: &str,
    bucket: &str,
    evidence_key: &str,
    evidence_bytes: usize,
    manifest_key: &str,
    written: bool,
    generated_at: &str,
) -> Value {
    let opt = |value: &str| {
        if value.trim().is_empty() {
            Value::Null
        } else {
            json!(value)
        }
    };
    json!({
        "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "kind": EVIDENCE_MANIFEST_KIND,
        "generated_at": generated_at,
        "source_relation": AUTH_AUDIT_LOG_RELATION,
        "window": { "since": opt(since), "until": opt(until) },
        "tenant_filter": opt(tenant),
        "record_count": record_count,
        "chain": {
            "algorithm": EVIDENCE_CHAIN_ALGORITHM,
            "genesis": EVIDENCE_CHAIN_GENESIS,
            "head": chain_head,
        },
        "evidence_object": {
            "backend": backend,
            "bucket": bucket,
            "object_key": evidence_key,
            "content_type": EVIDENCE_BUNDLE_CONTENT_TYPE,
            "bytes": evidence_bytes,
            "written": written,
        },
        "manifest_object": {
            "backend": backend,
            "bucket": bucket,
            "object_key": manifest_key,
            "content_type": "application/json",
            "written": written,
        },
    })
}

/// Build the object-store PUT request JSON the dispatch executor accepts. The
/// canonical builder (`runtime::core::setup_data::object_request_json`) is
/// crate-internal and unreachable from the binary crate, so this mirrors the
/// same key aliases the executor reads (`bucket`/`object_key` + synonyms).
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

/// Sanitize a window bound / timestamp into an object-key-safe slug.
fn evidence_object_slug(since: &str, until: &str, generated_at: &str) -> String {
    let sanitize = |value: &str| -> String {
        value
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect()
    };
    let since = if since.trim().is_empty() {
        "begin".to_string()
    } else {
        sanitize(since.trim())
    };
    let until = if until.trim().is_empty() {
        "now".to_string()
    } else {
        sanitize(until.trim())
    };
    format!("{since}_{until}__{}", sanitize(generated_at))
}

fn resolve_backend(flag: &str) -> String {
    let flag = flag.trim();
    if !flag.is_empty() {
        return flag.to_string();
    }
    env::var("UDB_OBJECT_BACKEND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_EVIDENCE_BACKEND.to_string())
}

fn resolve_bucket(flag: &str) -> String {
    let flag = flag.trim();
    if !flag.is_empty() {
        return flag.to_string();
    }
    env::var("UDB_COMPLIANCE_EVIDENCE_BUCKET")
        .or_else(|_| env::var("UDB_OBJECT_BUCKET"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_EVIDENCE_BUCKET.to_string())
}

fn resolve_project(flag: &str) -> String {
    let flag = flag.trim();
    if !flag.is_empty() {
        return flag.to_string();
    }
    env::var("UDB_PROJECT_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| udb::runtime::catalog::DEFAULT_PROJECT_ID.to_string())
}

/// Read the audit window from the durable audit-log relation. Optional filters
/// (`since`/`until`/`tenant`) are NULL/empty-tolerant so callers can export a
/// full or partial window; ordering is stable so the chain is reproducible.
async fn collect_evidence_records(
    pool: &sqlx::PgPool,
    since: &str,
    until: &str,
    tenant: &str,
    limit: i64,
) -> Result<Vec<EvidenceRecord>, String> {
    let since_opt = (!since.trim().is_empty()).then(|| since.trim().to_string());
    let until_opt = (!until.trim().is_empty()).then(|| until.trim().to_string());
    let tenant = tenant.trim().to_string();
    let row_limit = if limit <= 0 { i64::MAX } else { limit };
    let sql = format!(
        "SELECT event_id::text AS event_id, event_type, tenant_id, actor, \
                target_resource, operation, outcome, reason_code, \
                occurred_at::text AS occurred_at, envelope::text AS envelope \
         FROM {rel} \
         WHERE ($1::timestamptz IS NULL OR occurred_at >= $1::timestamptz) \
           AND ($2::timestamptz IS NULL OR occurred_at <= $2::timestamptz) \
           AND ($3 = '' OR tenant_id = $3) \
         ORDER BY occurred_at ASC, event_id ASC \
         LIMIT $4",
        rel = AUTH_AUDIT_LOG_RELATION
    );
    let rows = sqlx::query(&sql)
        .bind(since_opt)
        .bind(until_opt)
        .bind(&tenant)
        .bind(row_limit)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("query audit window failed: {err}"))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let get = |key: &str| -> String { row.try_get::<String, _>(key).unwrap_or_default() };
        let envelope_text: String = row.try_get("envelope").unwrap_or_default();
        let envelope: Value = serde_json::from_str(&envelope_text).unwrap_or(Value::Null);
        out.push(EvidenceRecord {
            event_id: get("event_id"),
            event_type: get("event_type"),
            tenant_id: get("tenant_id"),
            actor: get("actor"),
            target_resource: get("target_resource"),
            operation: get("operation"),
            outcome: get("outcome"),
            reason_code: get("reason_code"),
            occurred_at: get("occurred_at"),
            envelope,
        });
    }
    Ok(out)
}

pub(crate) fn run_compliance_command(command: ComplianceCommand) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("compliance: failed to create tokio runtime: {err}");
            return 1;
        }
    };
    match runtime.block_on(run_compliance_command_async(command)) {
        Ok(value) => {
            output_json(&value, "compliance evidence manifest");
            0
        }
        Err(err) => {
            eprintln!("compliance: {err}");
            1
        }
    }
}

async fn run_compliance_command_async(command: ComplianceCommand) -> Result<Value, String> {
    match command {
        ComplianceCommand::Evidence {
            since,
            until,
            tenant,
            backend,
            bucket,
            prefix,
            project,
            limit,
            out,
            dry_run,
        } => {
            // Mirror the admin runtime-CLI pattern (e.g. `admin verify-audit`):
            // build the in-process runtime so we can both read the durable audit
            // pool AND write through the object executor. A pure gRPC dial cannot
            // reach the storage-service object helper (no evidence RPC exists).
            let rt = udb::DataBrokerRuntime::from_env().await;
            let Some(pool) = rt.pg_pool_clone() else {
                return Err(
                    "PostgreSQL is not configured (set UDB_PG_DSN or DATABASE_URL)".to_string(),
                );
            };

            let records = collect_evidence_records(&pool, &since, &until, &tenant, limit).await?;
            let (jsonl, chain_head) = build_evidence_bundle(&records)?;

            let backend = resolve_backend(&backend);
            let bucket = resolve_bucket(&bucket);
            let project = resolve_project(&project);
            let generated_at = chrono::Utc::now().to_rfc3339();
            let prefix = {
                let trimmed = prefix.trim();
                if trimmed.is_empty() {
                    DEFAULT_EVIDENCE_PREFIX.to_string()
                } else {
                    trimmed.trim_end_matches('/').to_string()
                }
            };
            let slug = evidence_object_slug(&since, &until, &generated_at);
            let evidence_key = format!("{prefix}/evidence-{slug}.jsonl");
            let manifest_key = format!("{prefix}/evidence-{slug}.manifest.json");

            let manifest = render_evidence_manifest(
                &since,
                &until,
                &tenant,
                records.len(),
                &chain_head,
                &backend,
                &bucket,
                &evidence_key,
                jsonl.len(),
                &manifest_key,
                !dry_run,
                &generated_at,
            );

            // Optional local copy for CI artifact capture, independent of the
            // object-store write.
            if !out.trim().is_empty() {
                let pretty = serde_json::to_string_pretty(&manifest)
                    .map_err(|err| format!("render local manifest failed: {err}"))?;
                std::fs::write(out.trim(), pretty).map_err(|err| {
                    format!("write local manifest '{}' failed: {err}", out.trim())
                })?;
            }

            if dry_run {
                return Ok(manifest);
            }

            // Write the bundle THEN the manifest, both THROUGH the storage helper
            // (never directly to S3). The manifest is written last so it points at
            // an already-durable bundle.
            let evidence_req =
                object_put_request_json(&bucket, &evidence_key, EVIDENCE_BUNDLE_CONTENT_TYPE);
            rt.put_object_backend_target_for_project(
                &backend,
                None,
                &project,
                &evidence_req,
                jsonl.into_bytes(),
            )
            .await
            .map_err(|err| format!("write evidence bundle failed: {err}"))?;

            let manifest_bytes = serde_json::to_vec(&manifest)
                .map_err(|err| format!("encode manifest bytes failed: {err}"))?;
            let manifest_req = object_put_request_json(&bucket, &manifest_key, "application/json");
            rt.put_object_backend_target_for_project(
                &backend,
                None,
                &project,
                &manifest_req,
                manifest_bytes,
            )
            .await
            .map_err(|err| format!("write manifest object failed: {err}"))?;

            Ok(manifest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, op: &str) -> EvidenceRecord {
        EvidenceRecord {
            event_id: id.to_string(),
            event_type: "udb.authz.access.denied.v1".to_string(),
            tenant_id: "t1".to_string(),
            actor: "user-1".to_string(),
            target_resource: "doc/1".to_string(),
            operation: op.to_string(),
            outcome: "deny".to_string(),
            reason_code: "abac_default_deny".to_string(),
            occurred_at: "2026-06-25T00:00:00Z".to_string(),
            envelope: json!({ "event_id": id, "operation": op }),
        }
    }

    #[test]
    fn empty_window_chains_to_genesis() {
        let (jsonl, head) = build_evidence_bundle(&[]).expect("bundle");
        assert!(jsonl.is_empty());
        assert_eq!(head, EVIDENCE_CHAIN_GENESIS);
    }

    #[test]
    fn chain_is_deterministic_and_links_records() {
        let records = vec![rec("e1", "create"), rec("e2", "delete")];
        let (jsonl, head) = build_evidence_bundle(&records).expect("bundle");
        let (jsonl2, head2) = build_evidence_bundle(&records).expect("bundle");
        assert_eq!(jsonl, jsonl2, "bundle must be deterministic");
        assert_eq!(head, head2);

        let lines: Vec<Value> = jsonl
            .lines()
            .map(|line| serde_json::from_str(line).expect("valid json line"))
            .collect();
        assert_eq!(lines.len(), 2);
        // First record chains off genesis; second chains off the first's hash;
        // the last hash equals the manifest head.
        assert_eq!(lines[0]["prev_hash"], json!(EVIDENCE_CHAIN_GENESIS));
        assert_eq!(lines[1]["prev_hash"], lines[0]["chain_hash"]);
        assert_eq!(lines[1]["chain_hash"], json!(head));
        assert_ne!(lines[0]["chain_hash"], lines[1]["chain_hash"]);
    }

    #[test]
    fn tampering_an_early_record_changes_the_head() {
        let base = vec![rec("e1", "create"), rec("e2", "delete")];
        let mut tampered = base.clone();
        tampered[0].outcome = "allow".to_string();
        let (_, head_a) = build_evidence_bundle(&base).expect("bundle");
        let (_, head_b) = build_evidence_bundle(&tampered).expect("bundle");
        assert_ne!(
            head_a, head_b,
            "altering an early record must change the head"
        );
    }

    #[test]
    fn manifest_shape_is_machine_readable() {
        let manifest = render_evidence_manifest(
            "2026-06-01T00:00:00Z",
            "",
            "tenant-9",
            3,
            "deadbeef",
            "s3",
            "udb-compliance-evidence",
            "compliance-evidence/evidence-x.jsonl",
            1234,
            "compliance-evidence/evidence-x.manifest.json",
            true,
            "2026-06-25T12:00:00Z",
        );
        assert_eq!(
            manifest["schema_version"],
            json!(EVIDENCE_MANIFEST_SCHEMA_VERSION)
        );
        assert_eq!(manifest["kind"], json!(EVIDENCE_MANIFEST_KIND));
        assert_eq!(manifest["source_relation"], json!(AUTH_AUDIT_LOG_RELATION));
        assert_eq!(manifest["record_count"], json!(3));
        assert_eq!(manifest["window"]["since"], json!("2026-06-01T00:00:00Z"));
        assert_eq!(manifest["window"]["until"], Value::Null);
        assert_eq!(manifest["tenant_filter"], json!("tenant-9"));
        assert_eq!(
            manifest["chain"]["algorithm"],
            json!(EVIDENCE_CHAIN_ALGORITHM)
        );
        assert_eq!(manifest["chain"]["head"], json!("deadbeef"));
        assert_eq!(
            manifest["evidence_object"]["bucket"],
            json!("udb-compliance-evidence")
        );
        assert_eq!(manifest["evidence_object"]["bytes"], json!(1234));
        assert_eq!(manifest["evidence_object"]["written"], json!(true));
    }

    #[test]
    fn object_put_request_carries_executor_aliases() {
        let req: Value =
            serde_json::from_str(&object_put_request_json("b", "k", "application/json"))
                .expect("valid json");
        assert_eq!(req["op"], json!("put"));
        assert_eq!(req["bucket"], json!("b"));
        assert_eq!(req["container"], json!("b"));
        assert_eq!(req["object_key"], json!("k"));
        assert_eq!(req["key"], json!("k"));
        assert_eq!(req["content_type"], json!("application/json"));
    }

    #[test]
    fn arg_parse_evidence_reads_flags() {
        let args: Vec<String> = [
            "compliance",
            "evidence",
            "--since",
            "2026-01-01T00:00:00Z",
            "--tenant",
            "t9",
            "--bucket",
            "ev",
            "--prefix",
            "ev-prefix",
            "--limit",
            "50",
            "--dry-run",
        ]
        .iter()
        .map(|value| value.to_string())
        .collect();
        let (command, _, _, _) = parse_args(&args);
        match command {
            Command::Compliance(ComplianceCommand::Evidence {
                since,
                tenant,
                bucket,
                prefix,
                limit,
                dry_run,
                ..
            }) => {
                assert_eq!(since, "2026-01-01T00:00:00Z");
                assert_eq!(tenant, "t9");
                assert_eq!(bucket, "ev");
                assert_eq!(prefix, "ev-prefix");
                assert_eq!(limit, 50);
                assert!(dry_run);
            }
            _ => panic!("expected compliance evidence command"),
        }
    }
}
