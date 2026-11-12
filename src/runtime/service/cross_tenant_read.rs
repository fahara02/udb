//! An audited read of one other tenant's rows.
//!
//! A platform operator names the tenant and the reason on the call. Only a verified platform
//! authority holding `udb:cross_tenant:read` may do it, and the record of the read commits to
//! the durable audit log before any row is read: with no record, there is no read. Masking,
//! soft delete and row security apply as they would to that tenant's own read.

use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::runtime::error_reasons::{Refusal, SCOPE_MISSING};
use crate::runtime::executor_utils::invalid_argument_fields;
use crate::runtime::security::SecurityContext;
use crate::runtime::service::auth_service::audit_export::{AuditExportSink, PostgresAuditLogSink};
use crate::runtime::service::method_security::{claim_context_present, current_claim_context};

pub(crate) const TARGET_HEADER: &str = "x-udb-cross-tenant-target";
pub(crate) const REASON_HEADER: &str = "x-udb-cross-tenant-reason";
pub(crate) const CROSS_TENANT_READ_SCOPE: &str = "udb:cross_tenant:read";
const MAX_REASON_CHARS: usize = 500;

pub(crate) struct CrossTenantRead {
    pub(crate) target: String,
    reason: String,
}

fn header(metadata: &MetadataMap, name: &str) -> String {
    metadata
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// The read the call asks for, if it asks for one.
pub(crate) fn requested(metadata: &MetadataMap) -> Result<Option<CrossTenantRead>, Status> {
    let target = header(metadata, TARGET_HEADER);
    let reason = header(metadata, REASON_HEADER);
    if target.is_empty() {
        if reason.is_empty() {
            return Ok(None);
        }
        return Err(invalid_argument_fields(
            format!("{REASON_HEADER} needs {TARGET_HEADER}"),
            [(TARGET_HEADER, "name the tenant to read")],
        ));
    }
    if reason.is_empty() || reason.chars().count() > MAX_REASON_CHARS {
        return Err(invalid_argument_fields(
            format!("a cross-tenant read needs a reason of 1 to {MAX_REASON_CHARS} characters"),
            [(REASON_HEADER, "say why this tenant is being read")],
        ));
    }
    Ok(Some(CrossTenantRead { target, reason }))
}

/// Whether this verified caller may read another tenant. Scope and platform authority come
/// from the claim, never from request metadata.
fn authority_refusal() -> Option<Status> {
    let refused = |message: &str| {
        Some(
            Refusal::new(SCOPE_MISSING, message)
                .missing("scope", CROSS_TENANT_READ_SCOPE)
                .into_status(),
        )
    };
    if !claim_context_present() {
        return refused("a cross-tenant read needs a verified credential");
    }
    let claim = current_claim_context();
    if !claim.is_cross_tenant_admin() {
        return refused("a cross-tenant read needs platform authority");
    }
    if !claim
        .scopes
        .iter()
        .any(|scope| scope.trim().eq_ignore_ascii_case(CROSS_TENANT_READ_SCOPE))
    {
        return refused("a cross-tenant read needs its own scope");
    }
    None
}

/// Check the caller and record the attempt. `Ok` means the record is durable and the read may
/// run. A refused attempt is recorded too, best effort, since the refusal stands either way.
pub(crate) async fn admit(
    pool: Option<sqlx::PgPool>,
    security: &SecurityContext,
    read: &CrossTenantRead,
    message_type: &str,
    decision_id: &str,
) -> Result<(), Status> {
    let refusal = authority_refusal();
    let audit_failure = |detail: &str| {
        tracing::error!(error = %detail, "cross-tenant read refused: its audit record failed");
        crate::runtime::executor_utils::retryable_status(
            "postgres",
            "cross_tenant_read_audit",
            1_000,
            "a cross-tenant read cannot run without its audit record",
        )
    };
    let Some(pool) = pool else {
        return Err(refusal.unwrap_or_else(|| audit_failure("no audit database")));
    };
    let outcome = if refusal.is_some() {
        "denied"
    } else {
        "allowed"
    };
    let envelope = serde_json::json!({
        "event_id": uuid::Uuid::new_v4().to_string(),
        "actor": security.user_id,
        "actor_tenant": security.tenant_id,
        "target_tenant": read.target,
        "target_resource": message_type,
        "operation": "cross_tenant_read",
        "outcome": outcome,
        "reason": read.reason,
        "correlation_id": security.correlation_id,
        "trace_id": security.trace_id,
        "decision_id": decision_id,
    });
    let mut envelope = envelope;
    let audit = audit_sink_for(&pool);
    if let Some(widths) = audit.widths().await {
        let truncated = fit_envelope_to_widths(&mut envelope, widths);
        if !truncated.is_empty() {
            tracing::warn!(
                columns = ?truncated,
                relation = AUTH_AUDIT_RELATION,
                "cross-tenant read audit: value(s) longer than the audit column were cut to the \
                 column width; the full values are kept in the record's envelope under `untruncated`"
            );
        }
    }
    let written = audit
        .sink
        .export("udb.data.cross_tenant_read", &envelope)
        .await;
    if let Some(refusal) = refusal {
        return Err(refusal);
    }
    written.map_err(|err| audit_failure(&err))
}

/// The relation [`PostgresAuditLogSink::new`] writes. Kept in lock-step with it.
const AUTH_AUDIT_RELATION: &str = "udb_system.auth_audit_log";

/// Envelope keys the sink copies into bounded columns, and the column each lands
/// in. The sink reads exactly these keys (`actor_tenant` becomes `tenant_id`).
const BOUNDED_ENVELOPE_COLUMNS: [(&str, &str); 8] = [
    ("actor_tenant", "tenant_id"),
    ("actor", "actor"),
    ("target_resource", "target_resource"),
    ("operation", "operation"),
    ("outcome", "outcome"),
    ("correlation_id", "correlation_id"),
    ("trace_id", "trace_id"),
    ("decision_id", "decision_id"),
];

/// Cut each bounded value to its column's declared width so an over-long value
/// (a caller-supplied correlation id, a long principal name) cannot fail the
/// audit INSERT, which on this path would refuse the read on every retry. The
/// change is never silent: each original is kept in full under `untruncated` in
/// the same envelope (stored as JSONB, unbounded), and the caller logs the
/// columns. Returns the envelope keys that were cut.
fn fit_envelope_to_widths(
    envelope: &mut serde_json::Value,
    widths: &std::collections::HashMap<String, usize>,
) -> Vec<&'static str> {
    let mut originals = serde_json::Map::new();
    let mut truncated = Vec::new();
    for (key, column) in BOUNDED_ENVELOPE_COLUMNS {
        let Some(width) = widths.get(column).copied() else {
            continue;
        };
        let Some(value) = envelope.get(key).and_then(serde_json::Value::as_str) else {
            continue;
        };
        if let Some(cut) = crate::runtime::core::audit::truncate_to_width(value, width) {
            originals.insert(
                key.to_string(),
                serde_json::Value::String(value.to_string()),
            );
            envelope[key] = serde_json::Value::String(cut);
            truncated.push(key);
        }
    }
    if !originals.is_empty() {
        envelope["untruncated"] = serde_json::Value::Object(originals);
    }
    truncated
}

/// The audit sink for one data-plane pool, built once and reused by every
/// cross-tenant read on that pool, with the audit table's declared column widths.
///
/// Building a fresh `PostgresAuditLogSink` per call reset its "table ensured"
/// latch, so every cross-tenant read re-ran `CREATE SCHEMA` + `CREATE TABLE IF NOT
/// EXISTS` + the shape query before its INSERT.
struct PoolAuditSink {
    /// Identity of the pool this sink belongs to: clones of one pool share this
    /// `Arc`, a rebuilt pool gets a new one.
    options: std::sync::Arc<sqlx::postgres::PgConnectOptions>,
    pool: sqlx::PgPool,
    sink: PostgresAuditLogSink,
    widths: std::sync::OnceLock<std::collections::HashMap<String, usize>>,
}

impl PoolAuditSink {
    /// Declared widths of the audit table, read once. On a cold database the
    /// table is created first so the very first record is fitted too. `None` when
    /// they cannot be read: the INSERT then runs unfitted and any overflow refuses
    /// the read loudly, as before.
    async fn widths(&self) -> Option<&std::collections::HashMap<String, usize>> {
        if let Some(widths) = self.widths.get() {
            return Some(widths);
        }
        let read =
            || crate::runtime::core::audit::declared_column_widths(&self.pool, AUTH_AUDIT_RELATION);
        let mut widths = read().await;
        if matches!(&widths, Ok(found) if found.is_empty()) {
            if let Err(err) = self.sink.ensure_table().await {
                tracing::warn!(error = %err, "cross-tenant read audit: could not create the audit table");
                return None;
            }
            widths = read().await;
        }
        match widths {
            // Cached even when empty: the table exists by now, it just has no
            // bounded columns, and asking again would re-run the DDL per call.
            Ok(found) => {
                let _ = self.widths.set(found);
                self.widths.get()
            }
            Err(err) => {
                tracing::warn!(error = %err, "cross-tenant read audit: could not read column widths");
                None
            }
        }
    }
}

/// One slot: the broker has one data-plane pool at a time. A different pool (a
/// runtime reload) replaces the slot, which also releases the old pool.
static POOL_AUDIT_SINK: std::sync::Mutex<Option<std::sync::Arc<PoolAuditSink>>> =
    std::sync::Mutex::new(None);

fn audit_sink_for(pool: &sqlx::PgPool) -> std::sync::Arc<PoolAuditSink> {
    let options = pool.connect_options();
    let mut slot = POOL_AUDIT_SINK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(cached) = slot.as_ref()
        && std::sync::Arc::ptr_eq(&cached.options, &options)
    {
        return cached.clone();
    }
    let fresh = std::sync::Arc::new(PoolAuditSink {
        options,
        pool: pool.clone(),
        sink: PostgresAuditLogSink::new(pool.clone()),
        widths: std::sync::OnceLock::new(),
    });
    *slot = Some(fresh.clone());
    fresh
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(pairs: &[(&'static str, &str)]) -> MetadataMap {
        let mut map = MetadataMap::new();
        for (key, value) in pairs {
            map.insert(*key, value.parse().unwrap());
        }
        map
    }

    #[test]
    fn a_target_needs_a_reason_and_a_reason_needs_a_target() {
        assert!(requested(&metadata(&[])).unwrap().is_none());
        assert!(requested(&metadata(&[(TARGET_HEADER, "t")])).is_err());
        assert!(requested(&metadata(&[(REASON_HEADER, "why")])).is_err());
        let read = requested(&metadata(&[(TARGET_HEADER, "t"), (REASON_HEADER, "why")]))
            .unwrap()
            .unwrap();
        assert_eq!(read.target, "t");
        let long = "x".repeat(MAX_REASON_CHARS + 1);
        assert!(requested(&metadata(&[(TARGET_HEADER, "t"), (REASON_HEADER, &long)])).is_err());
    }

    /// An over-long value is cut to its column width, the original is kept in
    /// full under `untruncated`, and a value that fits is not touched.
    #[test]
    fn over_long_audit_values_are_fitted_and_the_original_is_kept() {
        let widths: std::collections::HashMap<String, usize> = [
            ("correlation_id".to_string(), 120),
            ("tenant_id".to_string(), 64),
            ("actor".to_string(), 200),
        ]
        .into_iter()
        .collect();
        let long_corr = "c".repeat(300);
        let mut envelope = serde_json::json!({
            "actor": "ops@example.com",
            "actor_tenant": "t".repeat(64),
            "correlation_id": long_corr,
            "reason": "r".repeat(500),
        });
        let cut = fit_envelope_to_widths(&mut envelope, &widths);
        assert_eq!(cut, vec!["correlation_id"]);
        assert_eq!(envelope["correlation_id"].as_str().unwrap().len(), 120);
        assert_eq!(
            envelope["untruncated"]["correlation_id"],
            long_corr.as_str()
        );
        // Fits exactly: untouched, not recorded.
        assert_eq!(envelope["actor_tenant"].as_str().unwrap().len(), 64);
        assert!(envelope["untruncated"].get("actor_tenant").is_none());
        assert_eq!(envelope["actor"], "ops@example.com");
        // Not a bounded column: never cut.
        assert_eq!(envelope["reason"].as_str().unwrap().len(), 500);

        let mut fits = serde_json::json!({ "actor": "a", "correlation_id": "c" });
        assert!(fit_envelope_to_widths(&mut fits, &widths).is_empty());
        assert!(
            fits.get("untruncated").is_none(),
            "no marker when nothing was cut"
        );
    }

    /// Against real Postgres: one sink per pool (no per-call DDL), and an
    /// over-long caller-supplied correlation id no longer fails the audit INSERT.
    /// The call is unauthenticated, so it is refused, but its "denied" record is
    /// still written, which is what this checks.
    #[tokio::test]
    #[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_cross_tenant_audit_reuses_its_sink_and_fits_long_values -- --ignored --nocapture"]
    async fn live_cross_tenant_audit_reuses_its_sink_and_fits_long_values() {
        use crate::runtime::service::live_tests::support::{
            live_native_service_db_lock, live_pg_pool,
        };
        let _guard = live_native_service_db_lock().lock().await;
        let pool = live_pg_pool().await;

        let first = audit_sink_for(&pool);
        let again = audit_sink_for(&pool.clone());
        assert!(
            std::sync::Arc::ptr_eq(&first, &again),
            "clones of one pool share one sink"
        );

        let decision_id = format!("ctr-live-{}", uuid::Uuid::new_v4());
        let long_corr = "c".repeat(300);
        let security = SecurityContext {
            user_id: "ops-live".to_string(),
            tenant_id: "tenant-live".to_string(),
            correlation_id: long_corr.clone(),
            ..SecurityContext::default()
        };
        let read = CrossTenantRead {
            target: "tenant-target".to_string(),
            reason: "live width check".to_string(),
        };
        let refused = admit(
            Some(pool.clone()),
            &security,
            &read,
            "acme.Order",
            &decision_id,
        )
        .await
        .expect_err("an unauthenticated cross-tenant read is refused");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied, "{refused:?}");

        let (corr, original): (String, Option<String>) = sqlx::query_as(
            "SELECT correlation_id, envelope->'untruncated'->>'correlation_id' \
             FROM udb_system.auth_audit_log WHERE decision_id = $1",
        )
        .bind(&decision_id)
        .fetch_one(&pool)
        .await
        .expect("the denied attempt is recorded despite the over-long correlation id");
        assert_eq!(corr.chars().count(), 120, "cut to the column width");
        assert_eq!(
            original.as_deref(),
            Some(long_corr.as_str()),
            "full value kept"
        );
        assert!(
            first.widths.get().is_some(),
            "widths are read once and cached on the shared sink"
        );
    }
}
