//! Typed service-account grants + mTLS certificate bindings (fix_plan Phases
//! 2+3, UDB-AUTH-003/006/007).
//!
//! Security policy for service accounts no longer lives in the mutable
//! free-form `users.profile_attributes_json`: a `udb_authn.service_account_grants`
//! row is the single authoritative source of an account's immutable service
//! identity, tenant/project binding, and operator-approved scope set, and a
//! `udb_authn.certificate_bindings` row maps a TLS peer-certificate selector to
//! that grant. This module holds:
//!
//! - the CENTRAL scope-validation helper ([`validate_service_scopes`]) every
//!   grant/binding write AND every request-time read goes through;
//! - the Postgres store functions for both tables (all identifiers resolve
//!   through [`native_model`], mirroring `idp::store` / `control_plane::store`);
//! - the deterministic profile→typed-grant migration
//!   ([`migrate_profile_grants`]);
//! - request-time certificate resolution ([`resolve_certificate_grant`]) —
//!   fail-closed on every ambiguity;
//! - free handler functions for the 8 new `AuthnService` grant/binding RPCs
//!   (the trait wiring happens inside `authn/mod.rs`, not here).
//!
//! ## Wiring contract (pool + service handle)
//!
//! `AuthnServiceImpl`'s `pg_pool` field and its `require_pool()` helper are
//! private to the `authn` module, so the handlers here take the pool as an
//! explicit parameter: the trait impl calls
//! `grants::create_service_account_grant(self, self.require_pool()?, request)`.
//! HIGH-6: every mutation handler runs its store write AND its audit event in
//! ONE sqlx transaction via the service's `emit_event_in_tx` seam, so grants
//! publish through the SAME outbox sink as every other auth event and an
//! event-write failure rolls the mutation back.

use sqlx::{PgPool, Row};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

use crate::backend::BackendKind;
use crate::ir::compile::CompileContext;
use crate::ir::{ComparisonOp, LogicalAssignment, LogicalFilter, LogicalUpdate, LogicalValue};
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::runtime::native_catalog::native_model;

use super::authn::AuthnServiceImpl;
use super::events::{AuthEvent, ComplianceEnvelope};
use super::mappings::timestamp_from_unix;

/// Fully-qualified proto message names — the manifest keys `native_model`
/// resolves table/column identifiers from (proto is the source of truth).
pub(crate) const GRANT_MSG: &str = "udb.core.authn.entity.v1.ServiceAccountGrant";
pub(crate) const BINDING_MSG: &str = "udb.core.authn.entity.v1.CertificateBinding";
const USER_MSG: &str = "udb.core.authn.entity.v1.User";

/// Row lifecycle statuses shared by grants and bindings (proto column default
/// vocabulary). Only ACTIVE rows authenticate.
pub(crate) const STATUS_ACTIVE: &str = "ACTIVE";
pub(crate) const STATUS_REVOKED: &str = "REVOKED";

/// `users.account_kind` value for service accounts (mirrors the resolver in
/// `auth_service::mod`).
const ACCOUNT_KIND_SERVICE_ACCOUNT: &str = "SERVICE_ACCOUNT";
/// `users.status` value for active accounts.
const USER_STATUS_ACTIVE: &str = "ACTIVE";

/// Certificate selector kinds (proto `CertificateBinding.selector_kind`
/// vocabulary). SPIFFE is preferred; FINGERPRINT pins one exact DER.
pub(crate) const SELECTOR_KIND_SPIFFE_URI: &str = "SPIFFE_URI";
pub(crate) const SELECTOR_KIND_DNS_SAN: &str = "DNS_SAN";
pub(crate) const SELECTOR_KIND_SUBJECT_CN: &str = "SUBJECT_CN";
pub(crate) const SELECTOR_KIND_FINGERPRINT_SHA256: &str = "FINGERPRINT_SHA256";
const SELECTOR_KINDS: [&str; 4] = [
    SELECTOR_KIND_SPIFFE_URI,
    SELECTOR_KIND_DNS_SAN,
    SELECTOR_KIND_SUBJECT_CN,
    SELECTOR_KIND_FINGERPRINT_SHA256,
];

/// Domain-event topics for grant/binding mutations (dot-separated, same
/// convention as `events::topics`). No existing topic covers the grant plane,
/// so these are the module's own constants.
pub(crate) const TOPIC_GRANT_CHANGED: &str = "udb.authn.grant.changed.v1";
pub(crate) const TOPIC_CERTIFICATE_BINDING_CHANGED: &str =
    "udb.authn.certificate_binding.changed.v1";

/// Provenance stamped by [`migrate_profile_grants`] on migrated rows.
const MIGRATION_UPDATED_BY: &str = "migration";
const MIGRATION_REASON: &str = "migrated from profile_attributes";

/// Paging bounds for the List RPCs: clamp 1..=200, default 50. The max is
const LIST_PAGE_SIZE_DEFAULT: i64 = 50;
const LIST_PAGE_SIZE_MAX: i64 = 200;

// ── Status helpers (all through executor_utils, mirroring apikey.rs) ──────────

fn grants_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn grants_failed_precondition_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::failed_precondition_fields(message, fields)
}

fn grants_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn grant_policy_denied(policy_decision_id: &'static str, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        Code::PermissionDenied,
        "grant_validation",
        policy_decision_id,
        message,
    )
}

fn required_field_status(field: &'static str) -> Status {
    grants_invalid_fields(
        format!("{field} is required"),
        [(field, "must be non-empty")],
    )
}

fn uuid_field_status(field: &str) -> Status {
    grants_invalid_fields(
        format!("{field} must be a UUID"),
        [(field.to_string(), "must be a valid UUID".to_string())],
    )
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value.trim()).map_err(|_| uuid_field_status(field))
}

fn map_err(context: &'static str) -> impl Fn(sqlx::Error) -> Status {
    // Classify recognised SQLSTATEs to typed gRPC codes (same as idp/store.rs).
    move |err| crate::runtime::executor_utils::sqlx_error_to_status(context, &err)
}

/// The violated constraint name when `err` is a Postgres unique violation
/// (SQLSTATE 23505), else `None`. Lets the creates map per-index collisions to
/// typed `FailedPrecondition` instead of a generic AlreadyExists.
fn unique_violation_constraint(err: &sqlx::Error) -> Option<String> {
    let db = err.as_database_error()?;
    if db.code().as_deref() == Some("23505") {
        Some(db.constraint().unwrap_or_default().to_string())
    } else {
        None
    }
}

// ── Central scope validation ─────────────────────────────────────────────────

/// Normalize a raw scope list: trim, drop empties, dedupe case-insensitively
/// (first occurrence wins, original casing preserved).
pub(crate) fn normalized_scope_list(raw: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for scope in raw {
        let trimmed = scope.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_ascii_lowercase()) {
            out.push(trimmed.to_string());
        }
    }
    out
}

/// A scope no service account may EVER hold, requested or approved: wildcard /
/// broker-admin / owner-role scopes would let a workload credential administer
/// the deployment.
fn is_forbidden_service_scope(scope: &str) -> bool {
    let lower = scope.to_ascii_lowercase();
    let normalized_alias = lower.replace(['-', '.'], "_");
    lower.split(':').any(|segment| {
        matches!(
            segment,
            "*" | "admin" | "administrator" | "owner" | "superuser" | "root"
        )
    }) || matches!(
        normalized_alias.as_str(),
        "organization_owner"
            | "organisation_owner"
            | "tenant_owner"
            | "account_owner"
            | "org_owner"
    )
}

/// THE central scope validator (UDB-AUTH-003): every grant create/replace,
/// every binding attenuation, and every request-time resolution funnels
/// through here — write-time AND read-time (defense in depth).
///
/// - Both lists are normalized ([`normalized_scope_list`]).
/// - Any forbidden scope (wildcard/admin/owner) on EITHER side ⇒
///   `PermissionDenied` (`forbidden_service_scope`) — fail closed even when
///   the poisoned entry was only in the approved set.
/// - `requested` empty ⇒ the full approved set is returned (validated).
/// - Otherwise every requested scope must be present in `approved`
///   (case-insensitive); the returned subset carries the APPROVED entries'
///   original casing. Any miss ⇒ `PermissionDenied` (`scope_outside_grant`).
pub(crate) fn validate_service_scopes(
    requested: &[String],
    approved: &[String],
) -> Result<Vec<String>, Status> {
    let requested = normalized_scope_list(requested);
    let approved = normalized_scope_list(approved);
    for scope in requested.iter().chain(approved.iter()) {
        if is_forbidden_service_scope(scope) {
            return Err(grant_policy_denied(
                "forbidden_service_scope",
                format!("scope '{scope}' can never be granted to a service account"),
            ));
        }
    }
    if requested.is_empty() {
        return Ok(approved);
    }
    let approved_by_lower: std::collections::BTreeMap<String, &String> = approved
        .iter()
        .map(|scope| (scope.to_ascii_lowercase(), scope))
        .collect();
    let mut out = Vec::with_capacity(requested.len());
    for scope in &requested {
        match approved_by_lower.get(&scope.to_ascii_lowercase()) {
            // Preserve the approved entry's casing — the grant row is canonical.
            Some(approved_entry) => out.push((*approved_entry).clone()),
            None => {
                return Err(grant_policy_denied(
                    "scope_outside_grant",
                    format!("scope '{scope}' is outside the approved grant"),
                ));
            }
        }
    }
    Ok(out)
}

// ── Typed-IR plumbing (copied idiom from idp/store.rs) ───────────────────────

fn native_compile_context() -> CompileContext<'static> {
    CompileContext::new(crate::runtime::native_catalog::native_manifest())
}

fn eq(field: &str, value: LogicalValue) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value,
    }
}

fn uuid_value(value: &str, field: &str) -> Result<LogicalValue, Status> {
    Ok(LogicalValue::String(parse_uuid(value, field)?.to_string()))
}

// HIGH-6: executor-generic (`&PgPool` for standalone writes, `&mut *tx` when
// the caller needs the mutation atomic with its audit event).
async fn execute_typed_update<'c, E>(
    executor: E,
    op: LogicalUpdate,
) -> Result<(u64, Vec<serde_json::Value>), Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_update_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )?;
    execute_compiled_mutation(executor, &compiled.spec_json).await
}

async fn execute_compiled_mutation<'c, E>(
    executor: E,
    spec_json: &str,
) -> Result<(u64, Vec<serde_json::Value>), Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let spec: serde_json::Value = serde_json::from_str(spec_json).map_err(|err| {
        grants_internal_status(
            "typed_native_mutation_json",
            format!("typed native mutation JSON failed: {err}"),
        )
    })?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            grants_internal_status(
                "typed_native_mutation_compile",
                "typed native mutation did not compile to SQL",
            )
        })?;
    crate::runtime::core::validate_pg_mutation_sql(sql)?;
    let params = crate::runtime::core::dispatch_params(&spec)?;
    let param_types = crate::runtime::core::dispatch_param_types(&spec)?;
    let return_rows = sql.to_ascii_lowercase().contains(" returning ");
    if return_rows {
        let rows = crate::runtime::core::bind_typed_generic_pg_params(
            sqlx::query(sql),
            &params,
            param_types.as_deref(),
        )?
        .fetch_all(executor)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "typed native mutation failed",
                &err,
            )
        })?;
        let rows = crate::runtime::core::pg_rows_to_json(rows)?;
        Ok((rows.len() as u64, rows))
    } else {
        let result = crate::runtime::core::bind_typed_generic_pg_params(
            sqlx::query(sql),
            &params,
            param_types.as_deref(),
        )?
        .execute(executor)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "typed native mutation failed",
                &err,
            )
        })?;
        Ok((result.rows_affected(), Vec::new()))
    }
}

// ── ServiceAccountGrant store ────────────────────────────────────────────────

/// Runtime view of one `service_account_grants` row.
#[derive(Debug, Clone, Default)]
pub(crate) struct GrantRecord {
    pub grant_id: String,
    pub user_id: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub status: String,
    pub revision: i64,
    pub updated_by: String,
    pub reason: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

impl GrantRecord {
    /// Map to the proto entity. `approved_scopes_json` carries the scope list
    /// re-serialized as a JSON array string.
    pub(crate) fn to_pb(&self) -> authn_entity_pb::ServiceAccountGrant {
        authn_entity_pb::ServiceAccountGrant {
            grant_id: self.grant_id.clone(),
            user_id: self.user_id.clone(),
            service_identity: self.service_identity.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            approved_scopes_json: scopes_to_json(&self.scopes),
            status: self.status.clone(),
            revision: self.revision,
            updated_by: self.updated_by.clone(),
            reason: self.reason.clone(),
            created_at: timestamp_from_unix(self.created_at_unix),
            updated_at: timestamp_from_unix(self.updated_at_unix),
        }
    }
}

/// Serialize a scope list to the stored JSON array string form.
fn scopes_to_json(scopes: &[String]) -> String {
    serde_json::to_string(scopes).unwrap_or_else(|_| "[]".to_string())
}

/// Parse a stored JSON array string back into a scope list (tolerant: a
/// malformed column degrades to empty — which fails closed everywhere scopes
/// are consumed, because empty effective scopes never authenticate).
fn scopes_from_json(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw.trim()).unwrap_or_default()
}

fn grant_select_columns() -> Vec<&'static str> {
    vec![
        "grant_id",
        "user_id",
        "service_identity",
        "tenant_id",
        "project_id",
        "approved_scopes_json",
        "status",
        "revision",
        "updated_by",
        "reason",
        "created_at",
        "updated_at",
    ]
}

/// SELECT clause aliasing every grant column to its runtime name; timestamps
/// render as unix-epoch bigints (same idiom as `idp::store`).
fn grant_select_clause() -> String {
    let m = native_model(GRANT_MSG, &grant_select_columns());
    [
        m.text_or_empty_as("grant_id", "grant_id"),
        m.text_or_empty_as("user_id", "user_id"),
        m.text_or_empty_as("service_identity", "service_identity"),
        m.text_or_empty_as("tenant_id", "tenant_id"),
        m.text_or_empty_as("project_id", "project_id"),
        m.json_text_as("approved_scopes_json", "approved_scopes_json"),
        m.text_or_empty_as("status", "status"),
        format!("{} AS revision", m.q("revision")),
        m.text_or_empty_as("updated_by", "updated_by"),
        m.text_or_empty_as("reason", "reason"),
        m.timestamp_unix_as("created_at", "created_at_unix"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
    ]
    .join(", ")
}

fn grant_row_from(row: &sqlx::postgres::PgRow) -> GrantRecord {
    GrantRecord {
        grant_id: row.try_get("grant_id").unwrap_or_default(),
        user_id: row.try_get("user_id").unwrap_or_default(),
        service_identity: row.try_get("service_identity").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        scopes: scopes_from_json(
            &row.try_get::<String, _>("approved_scopes_json")
                .unwrap_or_else(|_| "[]".to_string()),
        ),
        status: row.try_get("status").unwrap_or_default(),
        revision: row.try_get("revision").unwrap_or(0),
        updated_by: row.try_get("updated_by").unwrap_or_default(),
        reason: row.try_get("reason").unwrap_or_default(),
        created_at_unix: row.try_get::<i64, _>("created_at_unix").unwrap_or(0).max(0) as u64,
        updated_at_unix: row.try_get::<i64, _>("updated_at_unix").unwrap_or(0).max(0) as u64,
    }
}

/// Create the (single) grant for a service account. The scope set goes through
/// [`validate_service_scopes`] and must be non-empty — a scope-less grant can
/// never authenticate, so writing one would be a capability lie.
///
/// Unique collisions map to typed `FailedPrecondition`:
/// - `user_id` unique ⇒ "grant already exists" (one grant per account);
/// - `service_identity` unique ⇒ "identity already bound" (deployment-wide).
#[allow(clippy::too_many_arguments)]
/// CRIT-3: the owner facts a grant creation must verify, read under a row lock.
struct GrantOwner {
    project_id: String,
}

/// Load-and-lock the grant owner, requiring an ACTIVE `SERVICE_ACCOUNT` in the
/// SAME tenant. Missing / wrong-kind / inactive / cross-tenant owners are a
/// typed `FailedPrecondition` — a grant can never target them. Runs on the
/// caller's transaction connection so the `FOR UPDATE` lock actually spans the
/// grant INSERT (HIGH-6: `create_grant` is always called inside a tx).
async fn verify_grant_owner(
    conn: &mut sqlx::PgConnection,
    user_id: &uuid::Uuid,
    tenant_id: &str,
) -> Result<GrantOwner, Status> {
    let um = native_model(
        "udb.core.authn.entity.v1.User",
        &[
            "user_id",
            "tenant_id",
            "project_id",
            "account_kind",
            "status",
        ],
    );
    let rel = um.relation.clone();
    // Raw escape hatch: `FOR UPDATE` row-locking is not expressible in the
    // typed read IR; the lock is what prevents a concurrent deactivation from
    // racing the grant INSERT in this same transaction scope.
    // `text_or_empty_as` already emits `COALESCE(col::TEXT,'') AS <alias>`, so the
    // select list must NOT add a second `AS <alias>` (that renders `… AS x AS x`,
    // a Postgres syntax error at the second `AS`).
    let sql = format!(
        "SELECT {project}, {kind}, {status} \
         FROM {rel} WHERE {uid} = $1 AND {tenant} = $2 FOR UPDATE",
        project = um.text_or_empty_as("project_id", "project_id"),
        kind = um.text_or_empty_as("account_kind", "account_kind"),
        status = um.text_or_empty_as("status", "status"),
        uid = um.q("user_id"),
        tenant = um.q("tenant_id"),
    );
    let row = sqlx::query(&sql)
        .bind(user_id)
        .bind(tenant_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|error| grants_internal_status("grant_owner_load", error.to_string()))?;
    let Some(row) = row else {
        return Err(crate::runtime::executor_utils::failed_precondition_fields(
            "grant owner must be an existing service account in the grant tenant",
            [(
                "user_id".to_string(),
                "no matching user in this tenant".to_string(),
            )],
        ));
    };
    let kind: String = row.try_get("account_kind").unwrap_or_default();
    let status: String = row.try_get("status").unwrap_or_default();
    if !kind.eq_ignore_ascii_case("SERVICE_ACCOUNT") {
        return Err(crate::runtime::executor_utils::failed_precondition_fields(
            "grants may only target service accounts",
            [(
                "user_id".to_string(),
                "account kind must be SERVICE_ACCOUNT".to_string(),
            )],
        ));
    }
    if !status.eq_ignore_ascii_case("ACTIVE") {
        return Err(crate::runtime::executor_utils::failed_precondition_fields(
            "grant owner service account must be ACTIVE",
            [("user_id".to_string(), "account is not ACTIVE".to_string())],
        ));
    }
    Ok(GrantOwner {
        project_id: row.try_get("project_id").unwrap_or_default(),
    })
}

// HIGH-6: takes the caller's transaction connection — the owner FOR-UPDATE
// lock, the INSERT, and the caller's audit-event outbox row commit atomically.
fn validated_service_identity(raw: &str) -> Result<&str, Status> {
    let identity = raw.trim();
    if identity.is_empty() {
        return Err(required_field_status("service_identity"));
    }
    if identity.len() > 512 || identity.chars().any(char::is_control) {
        return Err(grants_invalid_fields(
            "service_identity is malformed",
            [(
                "service_identity",
                "must be at most 512 bytes and contain no control characters",
            )],
        ));
    }
    Ok(identity)
}

fn validated_tenant_id(raw: &str) -> Result<&str, Status> {
    let tenant_id = raw.trim();
    if tenant_id.is_empty() {
        return Err(required_field_status("tenant_id"));
    }
    if tenant_id.len() > 120 || tenant_id.chars().any(char::is_control) {
        return Err(grants_invalid_fields(
            "tenant_id is malformed",
            [(
                "tenant_id",
                "must be at most 120 bytes and contain no control characters",
            )],
        ));
    }
    Ok(tenant_id)
}

pub(crate) async fn create_grant(
    conn: &mut sqlx::PgConnection,
    user_id: &str,
    service_identity: &str,
    tenant_id: &str,
    project_id: &str,
    scopes: &[String],
    updated_by: &str,
    reason: &str,
) -> Result<GrantRecord, Status> {
    let user = parse_uuid(user_id, "user_id")?;
    let tenant = validated_tenant_id(tenant_id)?;
    let service_identity = validated_service_identity(service_identity)?;
    // requested = empty ⇒ validate the approved set itself (the central helper).
    let approved = validate_service_scopes(&[], scopes)?;
    if approved.is_empty() {
        return Err(grants_invalid_fields(
            "approved_scopes must contain at least one scope",
            [("approved_scopes", "must be a non-empty scope list")],
        ));
    }
    // CRIT-3: a grant may only target an ACTIVE `SERVICE_ACCOUNT` in the SAME
    // tenant — never a human account, an inactive account, or a cross-tenant
    // owner. The row is locked FOR UPDATE so a concurrent deactivation cannot
    // race the grant creation.
    let owner = verify_grant_owner(&mut *conn, &user, tenant).await?;
    if !project_id.trim().is_empty() && owner.project_id.trim() != project_id.trim() {
        return Err(crate::runtime::executor_utils::failed_precondition_fields(
            "grant project must match the service account's project binding",
            [(
                "project_id".to_string(),
                format!("service account is bound to project '{}'", owner.project_id),
            )],
        ));
    }
    let m = native_model(GRANT_MSG, &grant_select_columns());
    // Raw escape hatch: the create must classify WHICH unique index collided
    // (user vs identity) into a typed FailedPrecondition — the typed-write error
    // mapping cannot surface the constraint name.
    let sql = format!(
        "INSERT INTO {rel} \
            ({gid}, {uid}, {identity}, {tenant}, {project}, {scopes}, {status}, {revision}, \
             {uby}, {reason}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5::JSONB, $6, 1, $7, $8) \
         RETURNING {cols}",
        rel = &m.relation,
        gid = m.q("grant_id"),
        uid = m.q("user_id"),
        identity = m.q("service_identity"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        scopes = m.q("approved_scopes_json"),
        status = m.q("status"),
        revision = m.q("revision"),
        uby = m.q("updated_by"),
        reason = m.q("reason"),
        cols = grant_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(service_identity)
        .bind(tenant)
        .bind(owner.project_id.trim())
        .bind(scopes_to_json(&approved))
        .bind(STATUS_ACTIVE)
        .bind(updated_by)
        .bind(reason)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| match unique_violation_constraint(&err) {
            Some(constraint) if constraint.contains("identity") => {
                grants_failed_precondition_fields(
                    "service identity is already bound to another service account",
                    [("service_identity", "must be unique across the deployment")],
                )
            }
            Some(_) => grants_failed_precondition_fields(
                "a grant already exists for this service account",
                [(
                    "user_id",
                    "must not already hold a grant (replace it instead)",
                )],
            ),
            None => map_err("service-account grant insert failed")(err),
        })?;
    Ok(grant_row_from(&row))
}

/// Fetch the grant for one service account, tenant-scoped. Executor-generic
/// (HIGH-6): pass `&PgPool` for standalone reads or `&mut *tx` to read inside
/// a mutation transaction.
pub(crate) async fn get_grant_by_user<'c, E>(
    executor: E,
    tenant_id: &str,
    user_id: &str,
) -> Result<Option<GrantRecord>, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let user = parse_uuid(user_id, "user_id")?;
    let tenant = validated_tenant_id(tenant_id)?;
    let m = native_model(GRANT_MSG, &["user_id", "tenant_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {uid} = $1 AND {tenant} = $2 LIMIT 1",
        cols = grant_select_clause(),
        rel = &m.relation,
        uid = m.q("user_id"),
        tenant = m.q("tenant_id"),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(tenant)
        .fetch_optional(executor)
        .await
        .map_err(map_err("service-account grant get failed"))?;
    Ok(row.map(|r| grant_row_from(&r)))
}

/// Fetch the ACTIVE grant for a service identity. Deliberately deployment-wide
/// (no tenant filter): `service_identity` is globally unique, and the callers
/// (certificate resolution, duplicate detection) authenticate BEFORE a tenant
/// is known.
pub(crate) async fn get_active_grant_by_identity(
    pool: &PgPool,
    service_identity: &str,
) -> Result<Option<GrantRecord>, Status> {
    let service_identity = service_identity.trim();
    if service_identity.is_empty() {
        return Ok(None);
    }
    let m = native_model(GRANT_MSG, &["service_identity", "status"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {identity} = $1 AND {status} = $2 LIMIT 1",
        cols = grant_select_clause(),
        rel = &m.relation,
        identity = m.q("service_identity"),
        status = m.q("status"),
    );
    let row = sqlx::query(&sql)
        .bind(service_identity)
        .bind(STATUS_ACTIVE)
        .fetch_optional(pool)
        .await
        .map_err(map_err("service-account grant identity lookup failed"))?;
    Ok(row.map(|r| grant_row_from(&r)))
}

/// List a tenant's grants, newest first (user_id tiebreak keeps paging stable).
pub(crate) async fn list_grants(
    pool: &PgPool,
    tenant_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<GrantRecord>, Status> {
    let tenant = validated_tenant_id(tenant_id)?;
    let m = native_model(GRANT_MSG, &["tenant_id", "created_at", "user_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 \
         ORDER BY {created} DESC, {uid} ASC LIMIT $2 OFFSET $3",
        cols = grant_select_clause(),
        rel = &m.relation,
        tenant = m.q("tenant_id"),
        created = m.q("created_at"),
        uid = m.q("user_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant)
        .bind(limit.max(1))
        .bind(offset.max(0))
        .fetch_all(pool)
        .await
        .map_err(map_err("service-account grant list failed"))?;
    Ok(rows.iter().map(grant_row_from).collect())
}

/// Replace a grant's scope set (and optionally project) under optimistic
/// concurrency: a single UPDATE guarded by `revision = expected_revision`, so
/// two concurrent reviews can never silently interleave. The revision bump is
/// what invalidates stale certificate bindings (their `grant_revision` no
/// longer matches ⇒ [`resolve_certificate_grant`] fails closed).
// HIGH-6: executor-generic so the RPC handler can run the CAS UPDATE and its
// audit event on one transaction.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replace_grant(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    user_id: &str,
    scopes: &[String],
    project_id: &str,
    updated_by: &str,
    reason: &str,
    expected_revision: i64,
) -> Result<GrantRecord, Status> {
    let user = parse_uuid(user_id, "user_id")?;
    let tenant = validated_tenant_id(tenant_id)?;
    let owner = verify_grant_owner(&mut *conn, &user, tenant).await?;
    if !project_id.trim().is_empty() && owner.project_id.trim() != project_id.trim() {
        return Err(crate::runtime::executor_utils::failed_precondition_fields(
            "grant project must match the service account's project binding",
            [(
                "project_id".to_string(),
                format!("service account is bound to project '{}'", owner.project_id),
            )],
        ));
    }
    // requested = empty ⇒ validate the new approved set itself.
    let approved = validate_service_scopes(&[], scopes)?;
    if approved.is_empty() {
        return Err(grants_invalid_fields(
            "approved_scopes must contain at least one scope",
            [("approved_scopes", "must be a non-empty scope list")],
        ));
    }
    let m = native_model(GRANT_MSG, &grant_select_columns());
    // Raw escape hatch: the revision CAS (`revision = revision + 1` guarded by
    // `revision = $expected`) and the keep-when-empty project CASE are not
    // expressible in LogicalUpdate.
    let sql = format!(
        "UPDATE {rel} SET \
            {scopes} = $3::JSONB, \
            {project} = $4, \
            {revision} = {revision} + 1, \
            {uby} = $5, {reason} = $6, {updated} = NOW() \
         WHERE {uid} = $1 AND {tenant} = $2 AND {status} = $7 AND {revision} = $8 \
         RETURNING {cols}",
        rel = m.relation,
        scopes = m.q("approved_scopes_json"),
        project = m.q("project_id"),
        revision = m.q("revision"),
        uby = m.q("updated_by"),
        reason = m.q("reason"),
        updated = m.q("updated_at"),
        uid = m.q("user_id"),
        tenant = m.q("tenant_id"),
        status = m.q("status"),
        cols = grant_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(tenant)
        .bind(scopes_to_json(&approved))
        .bind(owner.project_id.trim())
        .bind(updated_by)
        .bind(reason)
        .bind(STATUS_ACTIVE)
        .bind(expected_revision)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("service-account grant replace failed"))?;
    // 0 rows = the grant is missing OR another writer already bumped the
    // revision — both are the same client error: re-read and retry.
    row.map(|r| grant_row_from(&r)).ok_or_else(|| {
        grants_failed_precondition_fields(
            "grant is inactive, missing, or its revision changed",
            [(
                "expected_revision",
                "must match the grant's current revision (re-read the grant and retry)",
            )],
        )
    })
}

/// Explicit service-identity rotation. Identity is immutable through ordinary
/// grant replacement; this audited CAS is the sole mutation path. Incrementing
/// the grant revision invalidates every API key and certificate binding
/// reviewed against the old identity, while current-grant JWT/session checks
/// reject the old service identity immediately.
pub(crate) async fn rotate_grant_identity(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    user_id: &str,
    new_service_identity: &str,
    updated_by: &str,
    reason: &str,
    expected_revision: i64,
) -> Result<(GrantRecord, String), Status> {
    let user = parse_uuid(user_id, "user_id")?;
    let tenant = validated_tenant_id(tenant_id)?;
    let new_service_identity = validated_service_identity(new_service_identity)?;
    verify_grant_owner(&mut *conn, &user, tenant).await?;
    let m = native_model(
        GRANT_MSG,
        &[
            "user_id",
            "tenant_id",
            "service_identity",
            "status",
            "revision",
            "updated_by",
            "reason",
            "updated_at",
        ],
    );
    let prior_sql = format!(
        "SELECT {identity} AS service_identity FROM {rel} \
         WHERE {uid} = $1 AND {tenant} = $2 AND {status} = $3 AND {revision} = $4 \
         FOR UPDATE",
        identity = m.q("service_identity"),
        rel = &m.relation,
        uid = m.q("user_id"),
        tenant = m.q("tenant_id"),
        status = m.q("status"),
        revision = m.q("revision"),
    );
    let prior = sqlx::query(&prior_sql)
        .bind(user)
        .bind(tenant)
        .bind(STATUS_ACTIVE)
        .bind(expected_revision)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("service-account identity rotation load failed"))?
        .ok_or_else(|| {
            grants_failed_precondition_fields(
                "grant is inactive, missing, or its revision changed",
                [(
                    "expected_revision",
                    "must match the grant's current revision (re-read the grant and retry)",
                )],
            )
        })?;
    let previous_service_identity: String = prior.try_get("service_identity").unwrap_or_default();
    if previous_service_identity == new_service_identity {
        return Err(grants_invalid_fields(
            "new_service_identity must differ from the current identity",
            [(
                "new_service_identity",
                "must name a new immutable service identity",
            )],
        ));
    }
    let sql = format!(
        "UPDATE {rel} SET {identity} = $3, {revision} = {revision} + 1, \
            {uby} = $4, {reason} = $5, {updated} = NOW() \
         WHERE {uid} = $1 AND {tenant} = $2 AND {status} = $6 AND {revision} = $7 \
         RETURNING {cols}",
        rel = &m.relation,
        identity = m.q("service_identity"),
        revision = m.q("revision"),
        uby = m.q("updated_by"),
        reason = m.q("reason"),
        updated = m.q("updated_at"),
        uid = m.q("user_id"),
        tenant = m.q("tenant_id"),
        status = m.q("status"),
        cols = grant_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(tenant)
        .bind(new_service_identity)
        .bind(updated_by)
        .bind(reason)
        .bind(STATUS_ACTIVE)
        .bind(expected_revision)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|error| match unique_violation_constraint(&error) {
            Some(constraint) if constraint.contains("identity") => {
                grants_failed_precondition_fields(
                    "service identity is already bound to another service account",
                    [(
                        "new_service_identity",
                        "must be unique across the deployment",
                    )],
                )
            }
            _ => map_err("service-account identity rotation failed")(error),
        })?
        .ok_or_else(|| {
            grants_failed_precondition_fields(
                "grant revision changed during identity rotation",
                [("expected_revision", "must match the current grant revision")],
            )
        })?;
    Ok((grant_row_from(&row), previous_service_identity))
}

/// Transfer an ACTIVE grant's stable service identity from one service account
/// to another by RE-POINTING the grant's `user_id`, atomically and under
/// revision CAS. The identity and its approved scopes ride the SAME row to the
/// destination, so neither the deployment-wide `service_identity` unique index
/// nor the per-user `user_id` unique index (both of which retain revoked rows)
/// is ever violated — unlike a revoke-then-create, which would collide on the
/// retained rows and leave a window with no owner. The source account is left
/// with no grant (its credentials no longer resolve to the identity), and the
/// move is a deterministic inverse of itself (transfer back, from → destination,
/// to → source). Returns the moved grant (now owned by the destination) and the
/// previous owner's `user_id` for a reverse transfer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn transfer_grant_identity(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    from_user_id: &str,
    to_user_id: &str,
    updated_by: &str,
    reason: &str,
    expected_revision: i64,
) -> Result<(GrantRecord, String), Status> {
    let from_user = parse_uuid(from_user_id, "from_user_id")?;
    let to_user = parse_uuid(to_user_id, "to_user_id")?;
    let tenant = validated_tenant_id(tenant_id)?;
    if from_user == to_user {
        return Err(grants_invalid_fields(
            "from_user_id and to_user_id must name different service accounts",
            [("to_user_id", "must differ from from_user_id")],
        ));
    }
    // Lock + validate BOTH accounts: each must be an ACTIVE SERVICE_ACCOUNT in
    // the tenant. The FOR UPDATE locks prevent a concurrent deactivation from
    // racing the re-point.
    verify_grant_owner(&mut *conn, &from_user, tenant).await?;
    let to_owner = verify_grant_owner(&mut *conn, &to_user, tenant).await?;
    // The destination must hold NO grant: the per-user unique index (which
    // retains revoked rows) would otherwise reject the re-point. Fail closed with
    // a precise reason before attempting the write.
    if get_grant_by_user(&mut *conn, tenant, to_user_id)
        .await?
        .is_some()
    {
        return Err(grants_failed_precondition_fields(
            "destination service account already has a grant",
            [(
                "to_user_id",
                "must have no existing grant (including a revoked one); create a fresh service account",
            )],
        ));
    }
    // Load + lock the source grant under CAS; capture the identity + project for
    // the audit event, the response, and the same-project guard.
    let m = native_model(
        GRANT_MSG,
        &[
            "user_id",
            "tenant_id",
            "service_identity",
            "project_id",
            "status",
            "revision",
            "updated_by",
            "reason",
            "updated_at",
        ],
    );
    let prior_sql = format!(
        "SELECT {identity} AS service_identity, {project} \
         FROM {rel} \
         WHERE {uid} = $1 AND {tenant} = $2 AND {status} = $3 AND {revision} = $4 \
         FOR UPDATE",
        identity = m.q("service_identity"),
        project = m.text_or_empty_as("project_id", "project_id"),
        rel = &m.relation,
        uid = m.q("user_id"),
        tenant = m.q("tenant_id"),
        status = m.q("status"),
        revision = m.q("revision"),
    );
    let prior = sqlx::query(&prior_sql)
        .bind(from_user)
        .bind(tenant)
        .bind(STATUS_ACTIVE)
        .bind(expected_revision)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("service-account grant transfer load failed"))?
        .ok_or_else(|| {
            grants_failed_precondition_fields(
                "source grant is inactive, missing, or its revision changed",
                [(
                    "expected_revision",
                    "must match the source grant's current revision (re-read the grant and retry)",
                )],
            )
        })?;
    let previous_service_identity: String = prior.try_get("service_identity").unwrap_or_default();
    let grant_project: String = prior.try_get("project_id").unwrap_or_default();
    // The identity is project-scoped: refuse a cross-project move so the
    // destination cannot silently re-home the identity into another project.
    if grant_project.trim() != to_owner.project_id.trim() {
        return Err(grants_failed_precondition_fields(
            "destination service account is in a different project than the grant",
            [(
                "to_user_id",
                "must belong to the same project as the transferred grant",
            )],
        ));
    }
    let _ = previous_service_identity;
    let sql = format!(
        "UPDATE {rel} SET {uid} = $3, {revision} = {revision} + 1, \
            {uby} = $4, {reason} = $5, {updated} = NOW() \
         WHERE {uid} = $1 AND {tenant} = $2 AND {status} = $6 AND {revision} = $7 \
         RETURNING {cols}",
        rel = &m.relation,
        uid = m.q("user_id"),
        revision = m.q("revision"),
        uby = m.q("updated_by"),
        reason = m.q("reason"),
        updated = m.q("updated_at"),
        tenant = m.q("tenant_id"),
        status = m.q("status"),
        cols = grant_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(from_user)
        .bind(tenant)
        .bind(to_user)
        .bind(updated_by)
        .bind(reason)
        .bind(STATUS_ACTIVE)
        .bind(expected_revision)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|error| match unique_violation_constraint(&error) {
            Some(constraint) if constraint.contains("user") => grants_failed_precondition_fields(
                "destination service account already has a grant",
                [(
                    "to_user_id",
                    "must have no existing grant (including a revoked one)",
                )],
            ),
            _ => map_err("service-account grant transfer failed")(error),
        })?
        .ok_or_else(|| {
            grants_failed_precondition_fields(
                "source grant revision changed during transfer",
                [("expected_revision", "must match the current grant revision")],
            )
        })?;
    Ok((grant_row_from(&row), from_user_id.trim().to_string()))
}

/// Revoke a grant (status ⇒ REVOKED). Returns `true` when an ACTIVE row was
/// revoked; `false` when the grant is missing or already revoked (idempotent).
/// HIGH-6: executor-generic so the handler can pair it with its audit event in
/// one transaction.
pub(crate) async fn revoke_grant<'c, E>(
    executor: E,
    tenant_id: &str,
    user_id: &str,
    updated_by: &str,
    reason: &str,
) -> Result<bool, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(
        "status".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(STATUS_REVOKED.to_string()),
        },
    );
    assignments.insert(
        "updated_by".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(updated_by.to_string()),
        },
    );
    assignments.insert(
        "reason".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(reason.to_string()),
        },
    );
    assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
    let (affected, _) = execute_typed_update(
        executor,
        LogicalUpdate {
            message_type: GRANT_MSG.to_string(),
            filter: LogicalFilter::And(vec![
                eq("user_id", uuid_value(user_id, "user_id")?),
                eq(
                    "tenant_id",
                    LogicalValue::String(validated_tenant_id(tenant_id)?.to_string()),
                ),
                eq("status", LogicalValue::String(STATUS_ACTIVE.to_string())),
            ]),
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    Ok(affected > 0)
}

// ── CertificateBinding store ─────────────────────────────────────────────────

/// Runtime view of one `certificate_bindings` row. Timestamp fields are unix
/// seconds; 0 means "column NULL" (no bound / never revoked).
#[derive(Debug, Clone, Default)]
pub(crate) struct BindingRecord {
    pub binding_id: String,
    pub selector_kind: String,
    pub selector_value: String,
    pub user_id: String,
    pub tenant_id: String,
    pub grant_revision: i64,
    pub scope_subset: Vec<String>,
    pub status: String,
    pub not_before_unix: u64,
    pub not_after_unix: u64,
    pub revoked_at_unix: u64,
    pub revoke_reason: String,
    pub updated_by: String,
    pub reason: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

impl BindingRecord {
    pub(crate) fn to_pb(&self) -> authn_entity_pb::CertificateBinding {
        authn_entity_pb::CertificateBinding {
            binding_id: self.binding_id.clone(),
            selector_kind: self.selector_kind.clone(),
            selector_value: self.selector_value.clone(),
            user_id: self.user_id.clone(),
            tenant_id: self.tenant_id.clone(),
            grant_revision: self.grant_revision,
            scope_subset_json: scopes_to_json(&self.scope_subset),
            status: self.status.clone(),
            not_before: timestamp_from_unix(self.not_before_unix),
            not_after: timestamp_from_unix(self.not_after_unix),
            revoked_at: timestamp_from_unix(self.revoked_at_unix),
            revoke_reason: self.revoke_reason.clone(),
            updated_by: self.updated_by.clone(),
            reason: self.reason.clone(),
            created_at: timestamp_from_unix(self.created_at_unix),
            updated_at: timestamp_from_unix(self.updated_at_unix),
        }
    }
}

fn binding_select_columns() -> Vec<&'static str> {
    vec![
        "binding_id",
        "selector_kind",
        "selector_value",
        "user_id",
        "tenant_id",
        "grant_revision",
        "scope_subset_json",
        "status",
        "not_before",
        "not_after",
        "revoked_at",
        "revoke_reason",
        "updated_by",
        "reason",
        "created_at",
        "updated_at",
    ]
}

fn binding_select_clause() -> String {
    let m = native_model(BINDING_MSG, &binding_select_columns());
    [
        m.text_or_empty_as("binding_id", "binding_id"),
        m.text_or_empty_as("selector_kind", "selector_kind"),
        m.text_or_empty_as("selector_value", "selector_value"),
        m.text_or_empty_as("user_id", "user_id"),
        m.text_or_empty_as("tenant_id", "tenant_id"),
        format!("{} AS grant_revision", m.q("grant_revision")),
        m.json_text_as("scope_subset_json", "scope_subset_json"),
        m.text_or_empty_as("status", "status"),
        m.timestamp_unix_as("not_before", "not_before_unix"),
        m.timestamp_unix_as("not_after", "not_after_unix"),
        m.timestamp_unix_as("revoked_at", "revoked_at_unix"),
        m.text_or_empty_as("revoke_reason", "revoke_reason"),
        m.text_or_empty_as("updated_by", "updated_by"),
        m.text_or_empty_as("reason", "reason"),
        m.timestamp_unix_as("created_at", "created_at_unix"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
    ]
    .join(", ")
}

fn binding_row_from(row: &sqlx::postgres::PgRow) -> BindingRecord {
    BindingRecord {
        binding_id: row.try_get("binding_id").unwrap_or_default(),
        selector_kind: row.try_get("selector_kind").unwrap_or_default(),
        selector_value: row.try_get("selector_value").unwrap_or_default(),
        user_id: row.try_get("user_id").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        grant_revision: row.try_get("grant_revision").unwrap_or(0),
        scope_subset: scopes_from_json(
            &row.try_get::<String, _>("scope_subset_json")
                .unwrap_or_else(|_| "[]".to_string()),
        ),
        status: row.try_get("status").unwrap_or_default(),
        not_before_unix: row.try_get::<i64, _>("not_before_unix").unwrap_or(0).max(0) as u64,
        not_after_unix: row.try_get::<i64, _>("not_after_unix").unwrap_or(0).max(0) as u64,
        revoked_at_unix: row.try_get::<i64, _>("revoked_at_unix").unwrap_or(0).max(0) as u64,
        revoke_reason: row.try_get("revoke_reason").unwrap_or_default(),
        updated_by: row.try_get("updated_by").unwrap_or_default(),
        reason: row.try_get("reason").unwrap_or_default(),
        created_at_unix: row.try_get::<i64, _>("created_at_unix").unwrap_or(0).max(0) as u64,
        updated_at_unix: row.try_get::<i64, _>("updated_at_unix").unwrap_or(0).max(0) as u64,
    }
}

/// Canonicalize + validate a selector kind against the closed vocabulary.
fn canonical_selector_kind(raw: &str) -> Result<&'static str, Status> {
    let trimmed = raw.trim();
    SELECTOR_KINDS
        .iter()
        .find(|kind| trimmed.eq_ignore_ascii_case(kind))
        .copied()
        .ok_or_else(|| {
            grants_invalid_fields(
                format!("selector_kind '{trimmed}' is not supported"),
                [(
                    "selector_kind",
                    "must be one of SPIFFE_URI, DNS_SAN, SUBJECT_CN, FINGERPRINT_SHA256",
                )],
            )
        })
}

/// Validate and canonicalize selector values at the write/read boundary so a
/// malformed or differently-cased selector cannot become an unreachable
/// security record. The same function is used by binding creation and request
/// resolution.
fn canonical_selector_value(kind: &str, raw: &str) -> Result<String, Status> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(required_field_status("selector_value"));
    }
    let valid_dns_name = |name: &str| {
        let name = name.trim_end_matches('.');
        !name.is_empty()
            && name.len() <= 253
            && name.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && (label == "*"
                        || (label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                            && !label.starts_with('-')
                            && !label.ends_with('-')))
            })
            && (!name.contains('*') || name.starts_with("*."))
    };
    let canonical = match kind {
        SELECTOR_KIND_FINGERPRINT_SHA256
            if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            value.to_ascii_lowercase()
        }
        SELECTOR_KIND_SPIFFE_URI => {
            let Some(rest) = value.strip_prefix("spiffe://") else {
                return Err(grants_invalid_fields(
                    "SPIFFE_URI selector must be a canonical spiffe:// URI",
                    [("selector_value", "must start with spiffe://")],
                ));
            };
            let trust_domain = rest.split('/').next().unwrap_or_default();
            if rest.bytes().any(|byte| byte.is_ascii_whitespace())
                || rest.contains('?')
                || rest.contains('#')
                || !valid_dns_name(trust_domain)
            {
                return Err(grants_invalid_fields(
                    "SPIFFE_URI selector is malformed",
                    [(
                        "selector_value",
                        "must contain a valid trust domain and no query, fragment, or whitespace",
                    )],
                ));
            }
            value.to_string()
        }
        SELECTOR_KIND_DNS_SAN if valid_dns_name(value) => {
            value.trim_end_matches('.').to_ascii_lowercase()
        }
        SELECTOR_KIND_SUBJECT_CN if value.len() <= 1024 && !value.chars().any(char::is_control) => {
            value.to_string()
        }
        SELECTOR_KIND_FINGERPRINT_SHA256 => {
            return Err(grants_invalid_fields(
                "FINGERPRINT_SHA256 selector is malformed",
                [(
                    "selector_value",
                    "must be exactly 64 hexadecimal characters",
                )],
            ));
        }
        SELECTOR_KIND_DNS_SAN => {
            return Err(grants_invalid_fields(
                "DNS_SAN selector is malformed",
                [("selector_value", "must be a valid DNS name")],
            ));
        }
        SELECTOR_KIND_SUBJECT_CN => {
            return Err(grants_invalid_fields(
                "SUBJECT_CN selector is malformed",
                [(
                    "selector_value",
                    "must be at most 1024 characters and contain no control characters",
                )],
            ));
        }
        _ => {
            return Err(grants_invalid_fields(
                "selector kind is not supported",
                [("selector_kind", "must use the closed selector vocabulary")],
            ));
        }
    };
    Ok(canonical)
}

/// Create an mTLS certificate binding for a service account.
///
/// Preconditions (all fail closed):
/// - the account must hold an ACTIVE grant ("service account has no active
///   grant" otherwise) — a binding can never precede its grant;
/// - `scope_subset` must validate as a subset of that grant (the stored subset
///   is the caller's NORMALIZED request — empty means "the full CURRENT grant",
///   resolved dynamically at request time, per the proto contract);
/// - the binding pins the grant's current `revision`, so a later grant replace
///   forces an operator re-review before the certificate authenticates again.
///
/// HIGH-3: a selector whose existing row is REVOKED is SUPERSEDED in place —
/// an audited replacement that preserves the row id (returned `bool` is `true`)
/// — while an ACTIVE existing row keeps failing with `FailedPrecondition`.
/// HIGH-6: runs on the caller's transaction connection so the existing-row
/// check, the write, and the caller's audit event commit atomically.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_binding(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    user_id: &str,
    selector_kind: &str,
    selector_value: &str,
    scope_subset: &[String],
    not_before_unix: u64,
    not_after_unix: u64,
    updated_by: &str,
    reason: &str,
) -> Result<(BindingRecord, bool), Status> {
    let user = parse_uuid(user_id, "user_id")?;
    let tenant = validated_tenant_id(tenant_id)?;
    let kind = canonical_selector_kind(selector_kind)?;
    let selector_value = canonical_selector_value(kind, selector_value)?;
    if not_before_unix != 0 && not_after_unix != 0 && not_after_unix <= not_before_unix {
        return Err(grants_invalid_fields(
            "certificate binding validity window is invalid",
            [("not_after", "must be later than not_before")],
        ));
    }
    let owner = verify_grant_owner(&mut *conn, &user, tenant).await?;
    let grant = get_grant_by_user(&mut *conn, tenant_id, user_id)
        .await?
        .filter(|grant| grant.status == STATUS_ACTIVE)
        .ok_or_else(|| {
            grants_failed_precondition_fields(
                "service account has no active grant",
                [(
                    "user_id",
                    "must reference a service account with an ACTIVE grant",
                )],
            )
        })?;
    if grant.project_id.trim() != owner.project_id.trim() {
        return Err(grants_failed_precondition_fields(
            "service-account grant project no longer matches its owner",
            [(
                "user_id",
                "replace the grant against the owner's current project before binding a certificate",
            )],
        ));
    }
    // Validate the attenuation against the grant (the central helper). The
    // stored value stays the normalized SUBSET (not the expansion), so an empty
    // subset keeps meaning "the full current grant" at resolution time.
    let _ = validate_service_scopes(scope_subset, &grant.scopes)?;
    let stored_subset = normalized_scope_list(scope_subset);
    let m = native_model(BINDING_MSG, &binding_select_columns());
    // HIGH-3: check-first under a row lock — the unique index on
    // (selector_kind, selector_value) covers revoked rows too, so a
    // revoke-then-create would otherwise always collide. Locking the existing
    // row (if any) FOR UPDATE inside this transaction serializes concurrent
    // re-reviews of the same selector.
    // Raw escape hatch: `FOR UPDATE` is not expressible in the typed read IR.
    let existing_sql = format!(
        "SELECT {cols} FROM {rel} WHERE {skind} = $1 AND {svalue} = $2 LIMIT 1 FOR UPDATE",
        cols = binding_select_clause(),
        rel = m.relation,
        skind = m.q("selector_kind"),
        svalue = m.q("selector_value"),
    );
    let existing = sqlx::query(&existing_sql)
        .bind(kind)
        .bind(&selector_value)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("certificate binding conflict check failed"))?
        .map(|row| binding_row_from(&row));
    if let Some(existing) = existing {
        if existing.status != STATUS_REVOKED {
            // ACTIVE (or any non-revoked lifecycle state): unchanged posture.
            return Err(grants_failed_precondition_fields(
                "certificate selector is already bound",
                [(
                    "selector_value",
                    "must not already be bound (revoke the existing binding first)",
                )],
            ));
        }
        // HIGH-3: supersede the REVOKED row in place — same binding_id, fresh
        // ownership/revision/subset/audit fields, revocation metadata cleared.
        let binding = parse_uuid(&existing.binding_id, "binding_id")?;
        let supersede_sql = format!(
            "UPDATE {rel} SET \
                {uid} = $2, {tenant} = $3, {grev} = $4, {subset} = $5::JSONB, \
                {nbf} = CASE WHEN $6 = 0 THEN NULL ELSE to_timestamp($6::DOUBLE PRECISION) END, \
                {naf} = CASE WHEN $7 = 0 THEN NULL ELSE to_timestamp($7::DOUBLE PRECISION) END, \
                {status} = $8, {revoked_at} = NULL, {revoke_reason} = '', \
                {uby} = $9, {reason} = $10, \
                {updated} = NOW() \
             WHERE {bid} = $1 \
             RETURNING {cols}",
            rel = m.relation,
            uid = m.q("user_id"),
            tenant = m.q("tenant_id"),
            grev = m.q("grant_revision"),
            subset = m.q("scope_subset_json"),
            nbf = m.q("not_before"),
            naf = m.q("not_after"),
            status = m.q("status"),
            revoked_at = m.q("revoked_at"),
            revoke_reason = m.q("revoke_reason"),
            uby = m.q("updated_by"),
            reason = m.q("reason"),
            updated = m.q("updated_at"),
            bid = m.q("binding_id"),
            cols = binding_select_clause(),
        );
        let row = sqlx::query(&supersede_sql)
            .bind(binding)
            .bind(user)
            .bind(tenant)
            .bind(grant.revision)
            .bind(scopes_to_json(&stored_subset))
            .bind(not_before_unix as i64)
            .bind(not_after_unix as i64)
            .bind(STATUS_ACTIVE)
            .bind(updated_by)
            .bind(reason)
            .fetch_one(&mut *conn)
            .await
            .map_err(map_err("certificate binding supersede failed"))?;
        return Ok((binding_row_from(&row), true));
    }
    // Raw escape hatch: the unique-selector collision must map to a typed
    // FailedPrecondition (constraint-name classification), like `create_grant`.
    // A 23505 can still surface here when two fresh creates race past the
    // check above — same typed error as the ACTIVE-conflict path.
    let sql = format!(
        "INSERT INTO {rel} \
            ({bid}, {kind}, {value}, {uid}, {tenant}, {grev}, {subset}, {nbf}, {naf}, {status}, {uby}, {reason}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6::JSONB, \
                 CASE WHEN $7 = 0 THEN NULL ELSE to_timestamp($7::DOUBLE PRECISION) END, \
                 CASE WHEN $8 = 0 THEN NULL ELSE to_timestamp($8::DOUBLE PRECISION) END, \
                 $9, $10, $11) \
         RETURNING {cols}",
        rel = m.relation,
        bid = m.q("binding_id"),
        kind = m.q("selector_kind"),
        value = m.q("selector_value"),
        uid = m.q("user_id"),
        tenant = m.q("tenant_id"),
        grev = m.q("grant_revision"),
        subset = m.q("scope_subset_json"),
        nbf = m.q("not_before"),
        naf = m.q("not_after"),
        status = m.q("status"),
        uby = m.q("updated_by"),
        reason = m.q("reason"),
        cols = binding_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(kind)
        .bind(&selector_value)
        .bind(user)
        .bind(tenant)
        .bind(grant.revision)
        .bind(scopes_to_json(&stored_subset))
        .bind(not_before_unix as i64)
        .bind(not_after_unix as i64)
        .bind(STATUS_ACTIVE)
        .bind(updated_by)
        .bind(reason)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| match unique_violation_constraint(&err) {
            Some(_) => grants_failed_precondition_fields(
                "certificate selector is already bound",
                [(
                    "selector_value",
                    "must not already be bound (revoke the existing binding first)",
                )],
            ),
            None => map_err("certificate binding insert failed")(err),
        })?;
    Ok((binding_row_from(&row), false))
}

/// List a tenant's certificate bindings, newest first.
pub(crate) async fn list_bindings(
    pool: &PgPool,
    tenant_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<BindingRecord>, Status> {
    let tenant = validated_tenant_id(tenant_id)?;
    let m = native_model(BINDING_MSG, &["tenant_id", "created_at", "binding_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 \
         ORDER BY {created} DESC, {bid} ASC LIMIT $2 OFFSET $3",
        cols = binding_select_clause(),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        created = m.q("created_at"),
        bid = m.q("binding_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant)
        .bind(limit.max(1))
        .bind(offset.max(0))
        .fetch_all(pool)
        .await
        .map_err(map_err("certificate binding list failed"))?;
    Ok(rows.iter().map(binding_row_from).collect())
}

async fn get_binding_by_id<'c, E>(
    executor: E,
    tenant_id: &str,
    binding_id: &str,
) -> Result<Option<BindingRecord>, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let tenant = validated_tenant_id(tenant_id)?;
    let binding = parse_uuid(binding_id, "binding_id")?;
    let m = native_model(BINDING_MSG, &["tenant_id", "binding_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {bid} = $2 LIMIT 1",
        cols = binding_select_clause(),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        bid = m.q("binding_id"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant)
        .bind(binding)
        .fetch_optional(executor)
        .await
        .map_err(map_err("certificate binding get failed"))?;
    Ok(row.map(|row| binding_row_from(&row)))
}

/// Revoke a binding (status ⇒ REVOKED, revocation metadata stamped). Returns
/// `true` when an ACTIVE row was revoked; `false` when missing/already revoked.
/// HIGH-6: executor-generic so the handler can pair it with its audit event in
/// one transaction.
pub(crate) async fn revoke_binding<'c, E>(
    executor: E,
    tenant_id: &str,
    binding_id: &str,
    updated_by: &str,
    reason: &str,
) -> Result<bool, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let mut assignments = std::collections::BTreeMap::new();
    assignments.insert(
        "status".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(STATUS_REVOKED.to_string()),
        },
    );
    assignments.insert("revoked_at".to_string(), LogicalAssignment::ServerNow);
    assignments.insert(
        "revoke_reason".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(reason.to_string()),
        },
    );
    assignments.insert(
        "updated_by".to_string(),
        LogicalAssignment::Set {
            value: LogicalValue::String(updated_by.to_string()),
        },
    );
    assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
    let (affected, _) = execute_typed_update(
        executor,
        LogicalUpdate {
            message_type: BINDING_MSG.to_string(),
            filter: LogicalFilter::And(vec![
                eq("binding_id", uuid_value(binding_id, "binding_id")?),
                eq(
                    "tenant_id",
                    LogicalValue::String(validated_tenant_id(tenant_id)?.to_string()),
                ),
                eq("status", LogicalValue::String(STATUS_ACTIVE.to_string())),
            ]),
            assignments,
            return_fields: Vec::new(),
            require_affected: false,
        },
    )
    .await?;
    Ok(affected > 0)
}

/// Resolve one ACTIVE binding by its selector, enforcing the validity window in
/// SQL against the SERVER clock (`NOW()`), so a skewed client can't stretch a
/// window. Deliberately no tenant filter — this is the request-time
/// authentication path where the tenant is DERIVED from the binding, never
/// supplied by the caller.
pub(crate) async fn find_active_binding(
    pool: &PgPool,
    selector_kind: &str,
    selector_value: &str,
) -> Result<Option<BindingRecord>, Status> {
    let kind = canonical_selector_kind(selector_kind)?;
    let selector_value = match canonical_selector_value(kind, selector_value) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let m = native_model(
        BINDING_MSG,
        &[
            "selector_kind",
            "selector_value",
            "status",
            "not_before",
            "not_after",
        ],
    );
    let sql = format!(
        "SELECT {cols} FROM {rel} \
         WHERE {skind} = $1 AND {svalue} = $2 AND {status} = $3 \
           AND ({nbf} IS NULL OR {nbf} <= NOW()) \
           AND ({naf} IS NULL OR {naf} > NOW()) \
         LIMIT 1",
        cols = binding_select_clause(),
        rel = m.relation,
        skind = m.q("selector_kind"),
        svalue = m.q("selector_value"),
        status = m.q("status"),
        nbf = m.q("not_before"),
        naf = m.q("not_after"),
    );
    let row = sqlx::query(&sql)
        .bind(kind)
        .bind(&selector_value)
        .bind(STATUS_ACTIVE)
        .fetch_optional(pool)
        .await
        .map_err(map_err("certificate binding lookup failed"))?;
    Ok(row.map(|r| binding_row_from(&r)))
}

/// Resolve a selector regardless of lifecycle state and report whether the row
/// is usable at the database server's current time. Request-time certificate
/// resolution uses this instead of filtering inactive rows out: once a strong
/// selector (for example SPIFFE URI or fingerprint) has a known binding, an
/// expired or revoked row must deny authentication rather than fall through to
/// a weaker DNS/CN selector on the same certificate.
async fn find_binding_candidate(
    pool: &PgPool,
    selector_kind: &str,
    selector_value: &str,
) -> Result<Option<(BindingRecord, bool)>, Status> {
    let kind = canonical_selector_kind(selector_kind)?;
    let selector_value = match canonical_selector_value(kind, selector_value) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let m = native_model(
        BINDING_MSG,
        &[
            "selector_kind",
            "selector_value",
            "status",
            "not_before",
            "not_after",
        ],
    );
    let sql = format!(
        "SELECT {cols}, \
           ({status} = $3 AND ({nbf} IS NULL OR {nbf} <= NOW()) \
             AND ({naf} IS NULL OR {naf} > NOW())) AS binding_usable \
         FROM {rel} WHERE {skind} = $1 AND {svalue} = $2 LIMIT 1",
        cols = binding_select_clause(),
        rel = m.relation,
        skind = m.q("selector_kind"),
        svalue = m.q("selector_value"),
        status = m.q("status"),
        nbf = m.q("not_before"),
        naf = m.q("not_after"),
    );
    let row = sqlx::query(&sql)
        .bind(kind)
        .bind(&selector_value)
        .bind(STATUS_ACTIVE)
        .fetch_optional(pool)
        .await
        .map_err(map_err("certificate binding lookup failed"))?;
    Ok(row.map(|row| {
        let usable = row.try_get::<bool, _>("binding_usable").unwrap_or(false);
        (binding_row_from(&row), usable)
    }))
}

// ── Request-time certificate resolution (UDB-AUTH-007) ───────────────────────

/// The principal derived from a verified peer certificate: everything comes
/// from SERVER-SIDE state (binding → grant), never from certificate metadata
/// or caller headers.
pub(crate) struct ResolvedCertificatePrincipal {
    /// The bound service-account user id.
    pub subject: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    /// The binding id — recorded as the authenticating credential.
    pub credential_id: String,
}

/// Resolve a verified client-certificate DER to its server-controlled
/// principal.
///
/// Candidate selectors, in order: every SPIFFE URI SAN, the exact-DER SHA-256
/// fingerprint pin, every DNS SAN, and the subject CN. The FIRST binding
/// hit decides the outcome — a binding whose grant checks fail returns
/// `Ok(None)` (fail closed) rather than falling through to a weaker selector,
/// so a revoked/stale pin can never be shadowed by a looser name match.
///
/// Fail-closed set (all ⇒ `Ok(None)`): no grant for the bound user, grant not
/// ACTIVE, binding reviewed against a different grant revision (stale review),
/// scope validation failure, or empty effective scopes.
/// CRIT-4: attenuate an API key's stored scopes against the owner's CURRENT
/// typed grant at AUTHENTICATION time, so a key can never outlive a grant
/// replacement or revocation:
///   * owner has NO typed grant, grant is revoked, owner is no longer an ACTIVE
///     service account, or the
///     intersection of key scopes with the current grant is empty → `None`
///     (deny — the key is dead until re-issued under the new grant);
///   * otherwise → the intersection (never wider than either side).
pub(crate) async fn attenuate_key_scopes_against_grant(
    pool: &PgPool,
    tenant_id: &str,
    owner_user_id: &str,
    reviewed_grant_revision: i64,
    key_scopes: &[String],
) -> Result<Option<Vec<String>>, String> {
    let grant = get_grant_by_user(pool, tenant_id, owner_user_id)
        .await
        .map_err(|status| {
            crate::runtime::executor_utils::store_string_from_status("grant lookup failed", &status)
        })?;
    let Some(grant) = grant else {
        return Ok(None);
    };
    if grant.status != STATUS_ACTIVE {
        return Ok(None);
    }
    if reviewed_grant_revision <= 0 || reviewed_grant_revision != grant.revision {
        return Ok(None);
    }
    if !owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id).await? {
        return Ok(None);
    }
    let approved: std::collections::BTreeSet<String> = grant
        .scopes
        .iter()
        .map(|scope| scope.trim().to_ascii_lowercase())
        .collect();
    let effective: Vec<String> = key_scopes
        .iter()
        .filter(|scope| approved.contains(&scope.trim().to_ascii_lowercase()))
        .cloned()
        .collect();
    if effective.is_empty() {
        return Ok(None);
    }
    Ok(Some(effective))
}

/// Validate a service bearer/session principal against CURRENT durable grant
/// and account state. This is the shared read-time guard used after JWT
/// signature validation: revoking the grant/account or narrowing its scopes
/// invalidates already-issued service credentials immediately.
pub(crate) async fn validate_service_principal_against_grant(
    pool: &PgPool,
    tenant_id: &str,
    owner_user_id: &str,
    project_id: &str,
    service_identity: &str,
    credential_scopes: &[String],
) -> Result<bool, String> {
    if tenant_id.trim().is_empty()
        || owner_user_id.trim().is_empty()
        || service_identity.trim().is_empty()
        || Uuid::parse_str(owner_user_id.trim()).is_err()
    {
        return Ok(false);
    }
    let grant = get_grant_by_user(pool, tenant_id, owner_user_id)
        .await
        .map_err(|status| {
            crate::runtime::executor_utils::store_string_from_status("grant lookup failed", &status)
        })?;
    let Some(grant) = grant.filter(|grant| grant.status == STATUS_ACTIVE) else {
        return Ok(false);
    };
    if grant.project_id.trim() != project_id.trim()
        || grant.service_identity.trim() != service_identity.trim()
        || !owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id).await?
    {
        return Ok(false);
    }
    Ok(validate_service_scopes(credential_scopes, &grant.scopes).is_ok())
}

/// CRIT-3 request-time check: is the account STILL an ACTIVE service account?
/// Errors are stringly (this path's contract) and mean fail closed upstream.
async fn owner_is_active_service_account(
    pool: &PgPool,
    user_id: &str,
    tenant_id: &str,
) -> Result<bool, String> {
    let user = uuid::Uuid::parse_str(user_id.trim())
        .map_err(|_| "grant owner id is not a UUID".to_string())?;
    let tenant = validated_tenant_id(tenant_id)
        .map_err(|status| format!("grant tenant id is invalid: {status}"))?;
    let um = native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "tenant_id", "account_kind", "status"],
    );
    let rel = um.relation.clone();
    let sql = format!(
        "SELECT 1 FROM {rel} WHERE {uid} = $1 AND {tenant} = $2 \
           AND {kind} = 'SERVICE_ACCOUNT' AND {status} = 'ACTIVE'",
        uid = um.q("user_id"),
        tenant = um.q("tenant_id"),
        kind = um.q("account_kind"),
        status = um.q("status"),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(tenant)
        .fetch_optional(pool)
        .await
        .map_err(|error| {
            crate::runtime::executor_utils::sqlx_error_to_tagged_string(
                "grant owner status check failed",
                &error,
            )
        })?;
    Ok(row.is_some())
}

pub(crate) async fn resolve_certificate_grant(
    pool: &PgPool,
    cert_der: &[u8],
) -> Result<Option<ResolvedCertificatePrincipal>, String> {
    use sha2::{Digest, Sha256};

    if cert_der.is_empty() {
        return Ok(None);
    }
    let fingerprint: String = Sha256::digest(cert_der)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    // HIGH-2: the certificate is parsed ONCE into typed selectors — every URI
    // SAN matches only SPIFFE_URI bindings, every DNS SAN only DNS_SAN
    // bindings, the subject CN only SUBJECT_CN bindings. A URI value can never
    // satisfy a DNS/CN binding (or vice versa).
    let selectors = crate::runtime::security::certificate_selectors_from_der(cert_der);
    let mut candidates: Vec<(&'static str, String)> = Vec::new();
    for uri in &selectors.uri_sans {
        candidates.push((SELECTOR_KIND_SPIFFE_URI, uri.clone()));
    }
    candidates.push((SELECTOR_KIND_FINGERPRINT_SHA256, fingerprint));
    for dns in &selectors.dns_sans {
        candidates.push((SELECTOR_KIND_DNS_SAN, dns.clone()));
    }
    if let Some(cn) = &selectors.subject_cn {
        candidates.push((SELECTOR_KIND_SUBJECT_CN, cn.clone()));
    }

    for (kind, value) in candidates {
        let binding = find_binding_candidate(pool, kind, &value)
            .await
            .map_err(|status| {
                crate::runtime::executor_utils::store_string_from_status(
                    "certificate binding lookup failed",
                    &status,
                )
            })?;
        let Some((binding, binding_usable)) = binding else {
            continue;
        };
        if !binding_usable {
            return Ok(None);
        }
        // Binding hit: resolve the CURRENT grant through the owner pool (no
        // tenant GUC — this runs before any tenant context exists; the grant
        // read is still tenant-scoped by the binding's own tenant column).
        let grant = get_grant_by_user(pool, &binding.tenant_id, &binding.user_id)
            .await
            .map_err(|status| {
                crate::runtime::executor_utils::store_string_from_status(
                    "service-account grant lookup failed",
                    &status,
                )
            })?;
        let Some(grant) = grant else {
            // Fail closed: an orphaned binding never authenticates.
            return Ok(None);
        };
        if grant.status != STATUS_ACTIVE {
            // Fail closed: the grant was revoked after the binding was created.
            return Ok(None);
        }
        if binding.grant_revision != grant.revision {
            // Fail closed: the grant changed since this binding was reviewed —
            // an operator must re-issue the binding against the new revision.
            return Ok(None);
        }
        // Effective scopes = the binding's attenuation validated against the
        // grant (empty subset ⇒ the full current grant). Any validation error
        // (poisoned stored data) fails closed instead of authenticating.
        let scopes = match validate_service_scopes(&binding.scope_subset, &grant.scopes) {
            Ok(scopes) => scopes,
            Err(_) => return Ok(None),
        };
        if scopes.is_empty() {
            // Fail closed: a scope-less principal can never authenticate.
            return Ok(None);
        }
        // CRIT-3: re-check the CURRENT account at request time — deactivating
        // or re-kinding the service account must immediately disable its mTLS
        // authentication, regardless of the (still-ACTIVE) grant/binding rows.
        if !owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id).await? {
            return Ok(None);
        }
        return Ok(Some(ResolvedCertificatePrincipal {
            subject: grant.user_id,
            service_identity: grant.service_identity,
            tenant_id: grant.tenant_id,
            project_id: grant.project_id,
            scopes,
            credential_id: binding.binding_id,
        }));
    }
    Ok(None)
}

// ── Profile → typed-grant migration ──────────────────────────────────────────

/// Outcome of one [`migrate_profile_grants`] run. `rejected` entries are
/// formatted `"user_id=<..> identity=<..>: <reason>"`.
#[derive(Debug, Default)]
pub(crate) struct MigrationReport {
    pub migrated: usize,
    pub skipped_existing: usize,
    pub rejected: Vec<String>,
}

/// One migration candidate scanned from `udb_authn.users`.
struct ProfileGrantCandidate {
    user_id: String,
    tenant_id: String,
    project_id: String,
    profile_attributes_json: String,
    external_subject: String,
}

/// Deterministically migrate legacy profile-attribute scope grants into typed
/// `service_account_grants` rows.
///
/// Scan: ACTIVE, non-deleted SERVICE_ACCOUNT users, ordered by `user_id` so
/// re-runs and dry runs walk the identical sequence. Per user:
/// - identity = the profile's service identity, falling back to
///   `external_subject`; users with no identity or no profile scopes are
///   silently out of scope (nothing to migrate);
/// - the scope set must pass [`validate_service_scopes`] — a failure is
///   RECORDED as a rejection, never partially written;
/// - a user that already holds a grant row is counted `skipped_existing`
///   (re-runs are idempotent);
/// - an identity already claimed — by an earlier candidate in THIS run or by
///   an existing ACTIVE grant of another account — is a rejection (ambiguous
///   registrations must never authenticate).
///
/// `dry_run` walks the same decisions and counts what WOULD migrate without
/// writing anything.
pub(crate) async fn migrate_profile_grants(
    pool: &PgPool,
    dry_run: bool,
) -> Result<MigrationReport, Status> {
    let m = native_model(
        USER_MSG,
        &[
            "user_id",
            "tenant_id",
            "project_id",
            "account_kind",
            "status",
            "profile_attributes_json",
            "external_subject",
            "deleted_at",
        ],
    );
    let sql = format!(
        "SELECT {uid} , {tenant}, {project}, {profile}, {esub} \
         FROM {rel} \
         WHERE {ak} = $1 AND {status} = $2 AND {del} IS NULL \
         ORDER BY {uid_col} ASC",
        uid = m.text_or_empty_as("user_id", "user_id"),
        tenant = m.text_or_empty_as("tenant_id", "tenant_id"),
        project = m.text_or_empty_as("project_id", "project_id"),
        profile = m.json_text_as("profile_attributes_json", "profile_attributes_json"),
        esub = m.text_or_empty_as("external_subject", "external_subject"),
        rel = m.relation,
        ak = m.q("account_kind"),
        status = m.q("status"),
        del = m.q("deleted_at"),
        uid_col = m.q("user_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(ACCOUNT_KIND_SERVICE_ACCOUNT)
        .bind(USER_STATUS_ACTIVE)
        .fetch_all(pool)
        .await
        .map_err(map_err("service-account migration scan failed"))?;
    let candidates: Vec<ProfileGrantCandidate> = rows
        .iter()
        .map(|row| ProfileGrantCandidate {
            user_id: row.try_get("user_id").unwrap_or_default(),
            tenant_id: row.try_get("tenant_id").unwrap_or_default(),
            project_id: row.try_get("project_id").unwrap_or_default(),
            profile_attributes_json: row
                .try_get("profile_attributes_json")
                .unwrap_or_else(|_| "{}".to_string()),
            external_subject: row.try_get("external_subject").unwrap_or_default(),
        })
        .collect();

    let mut report = MigrationReport::default();
    // Identities claimed within THIS run (case-insensitive), so two candidates
    // sharing an identity reject deterministically on the second.
    let mut claimed_identities = std::collections::BTreeSet::new();

    for candidate in candidates {
        let identity = {
            let from_profile =
                crate::runtime::authn::profile::service_identity_from_profile_attributes_json(
                    &candidate.profile_attributes_json,
                );
            if from_profile.trim().is_empty() {
                candidate.external_subject.trim().to_string()
            } else {
                from_profile.trim().to_string()
            }
        };
        if identity.is_empty() {
            // No managed identity ⇒ not a migration candidate (nothing to bind).
            continue;
        }
        let profile_scopes =
            crate::runtime::authn::profile::service_scopes_from_profile_attributes_json(
                &candidate.profile_attributes_json,
            );
        if normalized_scope_list(&profile_scopes).is_empty() {
            // No reviewed scope grant in the profile ⇒ nothing to migrate.
            continue;
        }
        let reject = |report: &mut MigrationReport, reason: &str| {
            report.rejected.push(format!(
                "user_id={} identity={identity}: {reason}",
                candidate.user_id
            ));
        };
        // Central validation (requested = empty ⇒ validate the set itself).
        let approved = match validate_service_scopes(&[], &profile_scopes) {
            Ok(approved) => approved,
            Err(status) => {
                reject(&mut report, status.message());
                continue;
            }
        };
        // Idempotency: an account that already holds a grant is left untouched.
        match get_grant_by_user(pool, &candidate.tenant_id, &candidate.user_id).await {
            Ok(Some(_)) => {
                report.skipped_existing += 1;
                claimed_identities.insert(identity.to_ascii_lowercase());
                continue;
            }
            Ok(None) => {}
            Err(status) => {
                reject(&mut report, status.message());
                continue;
            }
        }
        // Ambiguity guards: identity taken earlier in this run, or already
        // bound to a DIFFERENT account's active grant.
        if !claimed_identities.insert(identity.to_ascii_lowercase()) {
            reject(&mut report, "duplicate service identity in migration set");
            continue;
        }
        match get_active_grant_by_identity(pool, &identity).await {
            Ok(Some(existing)) if existing.user_id != candidate.user_id => {
                reject(&mut report, "identity already bound to another account");
                continue;
            }
            Ok(_) => {}
            Err(status) => {
                reject(&mut report, status.message());
                continue;
            }
        }
        if dry_run {
            report.migrated += 1;
            continue;
        }
        // HIGH-6: `create_grant` now takes a transaction connection (owner
        // FOR-UPDATE lock + INSERT are atomic); each candidate commits on its
        // own tx so one failure never poisons the remaining candidates.
        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(err) => {
                reject(&mut report, &format!("migration tx begin failed: {err}"));
                continue;
            }
        };
        match create_grant(
            &mut *tx,
            &candidate.user_id,
            &identity,
            &candidate.tenant_id,
            &candidate.project_id,
            &approved,
            MIGRATION_UPDATED_BY,
            MIGRATION_REASON,
        )
        .await
        {
            Ok(_) => match tx.commit().await {
                Ok(()) => report.migrated += 1,
                Err(err) => reject(&mut report, &format!("migration tx commit failed: {err}")),
            },
            // Never partially write: a failed insert is a rejection entry (the
            // dropped tx rolls back), the run continues with the remaining
            // candidates.
            Err(status) => reject(&mut report, status.message()),
        }
    }
    Ok(report)
}

// ── Handler helpers ──────────────────────────────────────────────────────────

/// The caller principal for `updated_by`, from the method-security verified
/// claim context (the SAME source apikey.rs uses); empty when no claim context
/// is installed (e.g. internal/bootstrap paths).
fn caller_updated_by() -> String {
    if !crate::runtime::service::method_security::claim_context_present() {
        return String::new();
    }
    crate::runtime::service::method_security::current_claim_context().subject
}

fn require_tenant(tenant_id: &str) -> Result<(), Status> {
    if tenant_id.trim().is_empty() {
        return Err(grants_invalid_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }
    // CRIT-2: the body tenant may only ECHO the validated bearer's tenant — a
    // tenant-A principal carrying grant-management scopes must never operate on
    // tenant-B grants or certificate bindings. `enforce_body_tenant_matches_claim`
    // is the SAME fail-closed binder every other authn admin handler uses (a
    // cross-tenant admin claim passes; an in-process caller with no claim
    // context installed — the offline CLI / test path — is exempt by design).
    let claim_ctx = crate::runtime::service::method_security::current_claim_context();
    crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
        &claim_ctx, tenant_id, "",
    )?;
    Ok(())
}

fn request_timestamp_unix(
    timestamp: Option<prost_types::Timestamp>,
    field: &'static str,
) -> Result<u64, Status> {
    let Some(timestamp) = timestamp else {
        return Ok(0);
    };
    if timestamp.seconds < 0 || !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(grants_invalid_fields(
            format!("{field} is not a valid protobuf timestamp"),
            [(
                field,
                "must have non-negative seconds and valid nanoseconds",
            )],
        ));
    }
    Ok(timestamp.seconds as u64)
}

/// Clamp `page_size` to 1..=200 (default 50) and parse the numeric-offset
/// `page_token` (unparseable ⇒ 0). Returns `(limit, offset)`.
fn page_window(page_size: i32, page_token: &str) -> (i64, i64) {
    let limit = if page_size <= 0 {
        LIST_PAGE_SIZE_DEFAULT
    } else {
        (page_size as i64).min(LIST_PAGE_SIZE_MAX)
    };
    let offset = page_token.trim().parse::<i64>().unwrap_or(0).max(0);
    (limit, offset)
}

/// Next-page token: the next numeric offset when the page came back full,
/// empty when this was the last page.
fn next_page_token(returned: usize, limit: i64, offset: i64) -> String {
    if returned as i64 == limit {
        (offset + limit).to_string()
    } else {
        String::new()
    }
}

/// The `auth_method` compliance field from the current otel context (defaults
/// to "bearer" when no method was recorded).
fn event_auth_method() -> String {
    let method = crate::runtime::otel::current_auth_method();
    if method.trim().is_empty() {
        "bearer".to_string()
    } else {
        method
    }
}

/// Build the grant-changed domain event. HIGH-6: the handlers persist this via
/// `AuthnServiceImpl::emit_event_in_tx` on the SAME transaction as the store
/// mutation (topic/payload unchanged from the previous best-effort emit).
fn build_grant_event(
    grant_id: &str,
    tenant_id: &str,
    user_id: &str,
    service_identity: &str,
    revision: i64,
    operation: &str,
    reason_code: &str,
    actor: &str,
) -> AuthEvent {
    AuthEvent::new(
        TOPIC_GRANT_CHANGED,
        grant_id.to_string(),
        tenant_id.to_string(),
        serde_json::json!({
            "grant_id": grant_id,
            "user_id": user_id,
            "service_identity": service_identity,
            "revision": revision,
            "operation": operation,
        }),
    )
    .with_correlation(Uuid::new_v4().to_string())
    .with_compliance(ComplianceEnvelope {
        actor: actor.to_string(),
        target_resource: format!("service_account_grant:{grant_id}"),
        target_tenant: tenant_id.to_string(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        reason_code: reason_code.to_string(),
        auth_method: event_auth_method(),
        ..ComplianceEnvelope::default()
    })
}

/// Build the certificate-binding-changed domain event (persisted via
/// `emit_event_in_tx`, same HIGH-6 contract as [`build_grant_event`]).
#[allow(clippy::too_many_arguments)]
fn build_binding_event(
    binding_id: &str,
    tenant_id: &str,
    user_id: &str,
    selector_kind: &str,
    selector_value: &str,
    operation: &str,
    reason_code: &str,
    actor: &str,
) -> AuthEvent {
    AuthEvent::new(
        TOPIC_CERTIFICATE_BINDING_CHANGED,
        binding_id.to_string(),
        tenant_id.to_string(),
        serde_json::json!({
            "binding_id": binding_id,
            "user_id": user_id,
            "selector_kind": selector_kind,
            "selector_value": selector_value,
            "operation": operation,
        }),
    )
    .with_correlation(Uuid::new_v4().to_string())
    .with_compliance(ComplianceEnvelope {
        actor: actor.to_string(),
        target_resource: format!("certificate_binding:{binding_id}"),
        target_tenant: tenant_id.to_string(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        reason_code: reason_code.to_string(),
        auth_method: event_auth_method(),
        ..ComplianceEnvelope::default()
    })
}

/// Begin the one HIGH-6 mutation transaction (typed internal Status on failure).
async fn begin_grants_tx(
    pool: &PgPool,
    operation: &'static str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, Status> {
    pool.begin().await.map_err(|err| {
        grants_internal_status(operation, format!("transaction begin failed: {err}"))
    })
}

/// Commit the HIGH-6 mutation transaction (typed internal Status on failure).
async fn commit_grants_tx(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    operation: &'static str,
) -> Result<(), Status> {
    tx.commit().await.map_err(|err| {
        grants_internal_status(operation, format!("transaction commit failed: {err}"))
    })
}

// ── AuthnService handler impls (free fns; trait wiring lives in authn/mod.rs) ─

pub(crate) async fn create_service_account_grant(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::CreateServiceAccountGrantRequest>,
) -> Result<Response<authn_pb::CreateServiceAccountGrantResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let claim_ctx = crate::runtime::service::method_security::current_claim_context();
    let (tenant_id, project_id) =
        crate::runtime::service::method_security::resolve_body_tenant_scope(
            &claim_ctx,
            &req.tenant_id,
            &req.project_id,
        )?;
    let updated_by = caller_updated_by();
    // HIGH-6: grant INSERT + audit-event outbox row commit in ONE transaction —
    // an event-write failure rolls the mutation back.
    let mut tx = begin_grants_tx(pool, "grant_create_tx").await?;
    let record = create_grant(
        &mut *tx,
        &req.user_id,
        &req.service_identity,
        &tenant_id,
        &project_id,
        &req.approved_scopes,
        &updated_by,
        &req.reason,
    )
    .await?;
    svc.emit_event_in_tx(
        &mut *tx,
        build_grant_event(
            &record.grant_id,
            &record.tenant_id,
            &record.user_id,
            &record.service_identity,
            record.revision,
            "create",
            "grant_created",
            &updated_by,
        ),
    )
    .await?;
    commit_grants_tx(tx, "grant_create_tx").await?;
    Ok(Response::new(authn_pb::CreateServiceAccountGrantResponse {
        message: "service-account grant created".to_string(),
        error: None,
        grant: Some(record.to_pb()),
    }))
}

pub(crate) async fn get_service_account_grant(
    _svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::GetServiceAccountGrantRequest>,
) -> Result<Response<authn_pb::GetServiceAccountGrantResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let record = get_grant_by_user(pool, &req.tenant_id, &req.user_id)
        .await?
        .ok_or_else(|| {
            crate::runtime::executor_utils::schema_status(
                Code::NotFound,
                "authn",
                "service_account_grant_get",
                "service_account_grant_not_found",
                "service account grant not found",
            )
        })?;
    Ok(Response::new(authn_pb::GetServiceAccountGrantResponse {
        message: "service-account grant".to_string(),
        error: None,
        grant: Some(record.to_pb()),
    }))
}

pub(crate) async fn list_service_account_grants(
    _svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::ListServiceAccountGrantsRequest>,
) -> Result<Response<authn_pb::ListServiceAccountGrantsResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let (limit, offset) = page_window(req.page_size, &req.page_token);
    let records = list_grants(pool, &req.tenant_id, limit, offset).await?;
    let next_page_token = next_page_token(records.len(), limit, offset);
    Ok(Response::new(authn_pb::ListServiceAccountGrantsResponse {
        message: "service-account grants".to_string(),
        error: None,
        grants: records.iter().map(GrantRecord::to_pb).collect(),
        next_page_token,
    }))
}

pub(crate) async fn replace_service_account_grant(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::ReplaceServiceAccountGrantRequest>,
) -> Result<Response<authn_pb::ReplaceServiceAccountGrantResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let claim_ctx = crate::runtime::service::method_security::current_claim_context();
    let (tenant_id, project_id) =
        crate::runtime::service::method_security::resolve_body_tenant_scope(
            &claim_ctx,
            &req.tenant_id,
            &req.project_id,
        )?;
    let updated_by = caller_updated_by();
    // HIGH-6: CAS UPDATE + audit-event outbox row commit in ONE transaction.
    let mut tx = begin_grants_tx(pool, "grant_replace_tx").await?;
    let record = replace_grant(
        &mut *tx,
        &tenant_id,
        &req.user_id,
        &req.approved_scopes,
        &project_id,
        &updated_by,
        &req.reason,
        req.expected_revision,
    )
    .await?;
    svc.emit_event_in_tx(
        &mut *tx,
        build_grant_event(
            &record.grant_id,
            &record.tenant_id,
            &record.user_id,
            &record.service_identity,
            record.revision,
            "replace",
            "grant_replaced",
            &updated_by,
        ),
    )
    .await?;
    commit_grants_tx(tx, "grant_replace_tx").await?;
    Ok(Response::new(
        authn_pb::ReplaceServiceAccountGrantResponse {
            message: "service-account grant replaced".to_string(),
            error: None,
            grant: Some(record.to_pb()),
        },
    ))
}

pub(crate) async fn rotate_service_account_identity(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::RotateServiceAccountIdentityRequest>,
) -> Result<Response<authn_pb::RotateServiceAccountIdentityResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let updated_by = caller_updated_by();
    let mut tx = begin_grants_tx(pool, "grant_identity_rotate_tx").await?;
    let (record, previous_service_identity) = rotate_grant_identity(
        &mut *tx,
        &req.tenant_id,
        &req.user_id,
        &req.new_service_identity,
        &updated_by,
        &req.reason,
        req.expected_revision,
    )
    .await?;
    let mut event = build_grant_event(
        &record.grant_id,
        &record.tenant_id,
        &record.user_id,
        &record.service_identity,
        record.revision,
        "rotate_identity",
        "service_identity_rotated",
        &updated_by,
    );
    if let Some(body) = event.body.as_object_mut() {
        body.insert(
            "previous_service_identity".to_string(),
            serde_json::Value::String(previous_service_identity.clone()),
        );
    }
    svc.emit_event_in_tx(&mut *tx, event).await?;
    commit_grants_tx(tx, "grant_identity_rotate_tx").await?;
    Ok(Response::new(
        authn_pb::RotateServiceAccountIdentityResponse {
            grant: Some(record.to_pb()),
            previous_service_identity,
            message: "service-account identity rotated; dependent credentials invalidated"
                .to_string(),
            error: None,
        },
    ))
}

pub(crate) async fn transfer_service_account_grant(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::TransferServiceAccountGrantRequest>,
) -> Result<Response<authn_pb::TransferServiceAccountGrantResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let updated_by = caller_updated_by();
    // HIGH-6: the grant re-point + audit-event outbox row commit in ONE
    // transaction — an event-write failure rolls the transfer back.
    let mut tx = begin_grants_tx(pool, "grant_transfer_tx").await?;
    let (record, previous_user_id) = transfer_grant_identity(
        &mut *tx,
        &req.tenant_id,
        &req.from_user_id,
        &req.to_user_id,
        &updated_by,
        &req.reason,
        req.expected_revision,
    )
    .await?;
    let mut event = build_grant_event(
        &record.grant_id,
        &record.tenant_id,
        &record.user_id,
        &record.service_identity,
        record.revision,
        "transfer",
        "service_identity_transferred",
        &updated_by,
    );
    if let Some(body) = event.body.as_object_mut() {
        body.insert(
            "previous_user_id".to_string(),
            serde_json::Value::String(previous_user_id.clone()),
        );
    }
    svc.emit_event_in_tx(&mut *tx, event).await?;
    commit_grants_tx(tx, "grant_transfer_tx").await?;
    Ok(Response::new(
        authn_pb::TransferServiceAccountGrantResponse {
            grant: Some(record.to_pb()),
            previous_user_id,
            message: "service-account grant transferred; source credentials no longer resolve to \
                      the identity"
                .to_string(),
            error: None,
        },
    ))
}

pub(crate) async fn revoke_service_account_grant(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::RevokeServiceAccountGrantRequest>,
) -> Result<Response<authn_pb::RevokeServiceAccountGrantResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let updated_by = caller_updated_by();
    // HIGH-6: status UPDATE + audit-event outbox row commit in ONE transaction.
    let mut tx = begin_grants_tx(pool, "grant_revoke_tx").await?;
    // Resolve first so the audit event carries the real grant id/identity.
    let existing = get_grant_by_user(&mut *tx, &req.tenant_id, &req.user_id).await?;
    let revoked = revoke_grant(
        &mut *tx,
        &req.tenant_id,
        &req.user_id,
        &updated_by,
        &req.reason,
    )
    .await?;
    if revoked {
        if let Some(grant) = existing {
            svc.emit_event_in_tx(
                &mut *tx,
                build_grant_event(
                    &grant.grant_id,
                    &grant.tenant_id,
                    &grant.user_id,
                    &grant.service_identity,
                    grant.revision,
                    "revoke",
                    "grant_revoked",
                    &updated_by,
                ),
            )
            .await?;
        }
    }
    commit_grants_tx(tx, "grant_revoke_tx").await?;
    Ok(Response::new(authn_pb::RevokeServiceAccountGrantResponse {
        message: "service-account grant revoked".to_string(),
        error: None,
        revoked,
    }))
}

pub(crate) async fn create_certificate_binding(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::CreateCertificateBindingRequest>,
) -> Result<Response<authn_pb::CreateCertificateBindingResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let not_before_unix = request_timestamp_unix(req.not_before.clone(), "not_before")?;
    let not_after_unix = request_timestamp_unix(req.not_after.clone(), "not_after")?;
    let updated_by = caller_updated_by();
    // HIGH-6: binding write (fresh INSERT or HIGH-3 supersede-in-place of a
    // REVOKED row) + audit-event outbox row commit in ONE transaction.
    let mut tx = begin_grants_tx(pool, "certificate_binding_create_tx").await?;
    let (record, superseded) = create_binding(
        &mut *tx,
        &req.tenant_id,
        &req.user_id,
        &req.selector_kind,
        &req.selector_value,
        &req.scope_subset,
        not_before_unix,
        not_after_unix,
        &updated_by,
        &req.reason,
    )
    .await?;
    // HIGH-3: an audited replacement of a revoked selector emits the same
    // binding event with operation "supersede".
    let (operation, reason_code) = if superseded {
        ("supersede", "certificate_binding_superseded")
    } else {
        ("create", "certificate_binding_created")
    };
    svc.emit_event_in_tx(
        &mut *tx,
        build_binding_event(
            &record.binding_id,
            &record.tenant_id,
            &record.user_id,
            &record.selector_kind,
            &record.selector_value,
            operation,
            reason_code,
            &updated_by,
        ),
    )
    .await?;
    commit_grants_tx(tx, "certificate_binding_create_tx").await?;
    Ok(Response::new(authn_pb::CreateCertificateBindingResponse {
        message: if superseded {
            "certificate binding superseded".to_string()
        } else {
            "certificate binding created".to_string()
        },
        error: None,
        binding: Some(record.to_pb()),
    }))
}

pub(crate) async fn list_certificate_bindings(
    _svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::ListCertificateBindingsRequest>,
) -> Result<Response<authn_pb::ListCertificateBindingsResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let (limit, offset) = page_window(req.page_size, &req.page_token);
    let records = list_bindings(pool, &req.tenant_id, limit, offset).await?;
    let next_page_token = next_page_token(records.len(), limit, offset);
    Ok(Response::new(authn_pb::ListCertificateBindingsResponse {
        message: "certificate bindings".to_string(),
        error: None,
        bindings: records.iter().map(BindingRecord::to_pb).collect(),
        next_page_token,
    }))
}

pub(crate) async fn revoke_certificate_binding(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::RevokeCertificateBindingRequest>,
) -> Result<Response<authn_pb::RevokeCertificateBindingResponse>, Status> {
    let req = request.into_inner();
    require_tenant(&req.tenant_id)?;
    let updated_by = caller_updated_by();
    // HIGH-6: status UPDATE + audit-event outbox row commit in ONE transaction.
    let mut tx = begin_grants_tx(pool, "certificate_binding_revoke_tx").await?;
    let existing = get_binding_by_id(&mut *tx, &req.tenant_id, &req.binding_id).await?;
    let revoked = revoke_binding(
        &mut *tx,
        &req.tenant_id,
        &req.binding_id,
        &updated_by,
        &req.reason,
    )
    .await?;
    if revoked {
        if let Some(binding) = existing {
            svc.emit_event_in_tx(
                &mut *tx,
                build_binding_event(
                    &binding.binding_id,
                    &binding.tenant_id,
                    &binding.user_id,
                    &binding.selector_kind,
                    &binding.selector_value,
                    "revoke",
                    "certificate_binding_revoked",
                    &updated_by,
                ),
            )
            .await?;
        }
    }
    commit_grants_tx(tx, "certificate_binding_revoke_tx").await?;
    Ok(Response::new(authn_pb::RevokeCertificateBindingResponse {
        message: "certificate binding revoked".to_string(),
        error: None,
        revoked,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn normalized_scope_list_trims_dedupes_and_preserves_casing() {
        let raw = strings(&[" DB:Read ", "db:read", "", "db:write", "DB:WRITE", "  "]);
        assert_eq!(
            normalized_scope_list(&raw),
            strings(&["DB:Read", "db:write"])
        );
    }

    #[test]
    fn tenant_ids_follow_the_canonical_authn_string_contract() {
        assert_eq!(validated_tenant_id(" acme ").unwrap(), "acme");
        assert_eq!(
            validated_tenant_id("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert!(validated_tenant_id("").is_err());
        assert!(validated_tenant_id("bad\ntenant").is_err());
        assert!(validated_tenant_id(&"x".repeat(121)).is_err());
    }

    #[test]
    fn validate_service_scopes_rejects_forbidden_scopes_on_either_side() {
        for forbidden in [
            "*",
            "udb:*",
            "UDB:ADMIN",
            "tenant:administrator",
            "billing:superuser",
            "data:*",
            "role:owner:acme",
            "org-owner",
            "Organization_Owner",
        ] {
            let err = validate_service_scopes(&[], &strings(&["db:read", forbidden]))
                .expect_err("forbidden approved scope must be rejected");
            assert_eq!(err.code(), Code::PermissionDenied);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Policy as i32);
            assert_eq!(detail.operation, "grant_validation");
            assert_eq!(detail.policy_decision_id, "forbidden_service_scope");

            let err = validate_service_scopes(&strings(&[forbidden]), &strings(&["db:read"]))
                .expect_err("forbidden requested scope must be rejected");
            assert_eq!(err.code(), Code::PermissionDenied);
        }
    }

    #[test]
    fn validate_service_scopes_empty_request_returns_full_approved_set() {
        let approved = strings(&["db:read", "db:write"]);
        assert_eq!(
            validate_service_scopes(&[], &approved).expect("approved set validates"),
            approved
        );
    }

    #[test]
    fn validate_service_scopes_subset_matches_case_insensitively_with_approved_casing() {
        let approved = strings(&["DB:Read", "db:write"]);
        let requested = strings(&["db:read"]);
        // The returned entry carries the APPROVED casing (the grant row is canonical).
        assert_eq!(
            validate_service_scopes(&requested, &approved).expect("subset validates"),
            strings(&["DB:Read"])
        );
    }

    #[test]
    fn validate_service_scopes_rejects_scope_outside_grant() {
        let err = validate_service_scopes(&strings(&["db:delete"]), &strings(&["db:read"]))
            .expect_err("scope outside the grant must be rejected");
        assert_eq!(err.code(), Code::PermissionDenied);
        let detail = decode_detail(&err);
        assert_eq!(detail.policy_decision_id, "scope_outside_grant");
    }

    #[test]
    fn canonical_selector_kind_accepts_vocabulary_and_rejects_unknown() {
        assert_eq!(
            canonical_selector_kind("spiffe_uri").expect("case-insensitive accept"),
            SELECTOR_KIND_SPIFFE_URI
        );
        let err = canonical_selector_kind("SERIAL_NUMBER").expect_err("unknown kind rejected");
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn selector_values_are_validated_and_canonicalized() {
        assert_eq!(
            canonical_selector_value(SELECTOR_KIND_DNS_SAN, "API.Example.COM.")
                .expect("valid DNS SAN"),
            "api.example.com"
        );
        assert_eq!(
            canonical_selector_value(
                SELECTOR_KIND_FINGERPRINT_SHA256,
                "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
            )
            .expect("valid fingerprint"),
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
        assert!(
            canonical_selector_value(SELECTOR_KIND_SPIFFE_URI, "https://example.com/service")
                .is_err()
        );
        assert!(canonical_selector_value(SELECTOR_KIND_DNS_SAN, "bad..name").is_err());
        assert!(canonical_selector_value(SELECTOR_KIND_FINGERPRINT_SHA256, "abcd").is_err());
    }

    #[test]
    fn page_window_clamps_and_parses_token() {
        assert_eq!(page_window(0, ""), (50, 0));
        assert_eq!(page_window(-5, "junk"), (50, 0));
        assert_eq!(page_window(1000, "150"), (200, 150));
        assert_eq!(page_window(25, "-9"), (25, 0));
    }

    #[test]
    fn next_page_token_only_on_full_pages() {
        assert_eq!(next_page_token(50, 50, 0), "50");
        assert_eq!(next_page_token(49, 50, 0), "");
        assert_eq!(next_page_token(0, 50, 100), "");
    }

    #[test]
    fn scopes_json_roundtrip_and_tolerant_parse() {
        let scopes = strings(&["db:read", "db:write"]);
        assert_eq!(scopes_from_json(&scopes_to_json(&scopes)), scopes);
        // Malformed stored JSON degrades to empty ⇒ fails closed downstream.
        assert!(scopes_from_json("{not-json").is_empty());
    }
}
