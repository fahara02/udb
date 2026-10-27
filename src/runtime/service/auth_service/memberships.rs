//! Cross-tenant service tokens: service-account tenant memberships.
//!
//! A service account lives in exactly one (home) tenant with exactly one typed
//! grant there. An administrator of ANOTHER tenant can approve it to act in
//! their tenant by writing a `udb_authn.service_account_tenant_memberships`
//! row. The account then exchanges its home bearer for a short-lived token in
//! the target tenant (`ExchangeServiceTokenForTenant`):
//!
//! - subject = the service account, tenant/project = the target, scopes =
//!   the requested subset of the membership's approved scopes, service
//!   identity = the home grant's;
//! - the `home_tenant_id` claim names the home tenant and the `mbr` claim pins
//!   `"<membership_id>:<revision>"`;
//! - lifetime at most [`MEMBERSHIP_TOKEN_MAX_TTL_SECS`] and never past the
//!   membership's expiry; no refresh token, no session;
//! - every use re-checks the membership (ACTIVE, unexpired, same revision,
//!   same tenant/project/subject/home, scopes still approved), the home grant
//!   (ACTIVE, same service identity) and the account (ACTIVE service account)
//!   — [`validate_membership_principal`], called from the shared JWT
//!   persisted-state check.
//!
//! Only an administrator OF THE TARGET tenant (claim tenant == target) creates,
//! lists or revokes memberships; a home-tenant administrator alone cannot.
//! Every mutation commits with its audit events (one in each tenant) in one
//! transaction; every exchange is published in both tenants.

use sqlx::{PgPool, Row};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::runtime::native_catalog::native_model;
// The `auth_method` claim of every exchanged token.
use crate::runtime::security::MEMBERSHIP_TOKEN_AUTH_METHOD;

use super::authn::AuthnServiceImpl;
use super::events::{AuthEvent, ComplianceEnvelope};
use super::grants::{
    STATUS_ACTIVE, STATUS_REVOKED, begin_grants_tx, caller_updated_by, commit_grants_tx,
    event_auth_method, get_grant_by_user, grants_invalid_fields, map_err, next_page_token,
    owner_is_active_service_account, page_window, parse_uuid, scopes_from_json, scopes_to_json,
    unique_violation_constraint, validate_service_scopes,
};
use super::mappings::timestamp_from_unix;
use super::now_unix;

/// Manifest key `native_model` resolves the membership table from.
pub(crate) const MEMBERSHIP_MSG: &str = "udb.core.authn.entity.v1.ServiceAccountTenantMembership";

/// The scope a service account's HOME bearer must carry, literally, to
/// exchange it for a target-tenant token. Never approvable in a membership,
/// so an exchanged token can never be exchanged again.
pub(crate) const EXCHANGE_TENANT_TOKEN_SCOPE: &str = "udb:authn:exchange-tenant-token";

/// `jti` prefix of every exchanged token; a bearer carrying it is never a home
/// bearer.
pub(crate) const MEMBERSHIP_TOKEN_JTI_PREFIX: &str = "mbx_";

/// Hard cap on an exchanged token's lifetime.
pub(crate) const MEMBERSHIP_TOKEN_MAX_TTL_SECS: u64 = 15 * 60;

/// A membership may be approved for at most one year.
pub(crate) const MEMBERSHIP_MAX_LIFETIME_SECS: u64 = 365 * 24 * 60 * 60;

/// Status of an ACTIVE row whose `expires_at` passed, set when a new approval
/// for the same (account, tenant, project) replaces it.
const STATUS_EXPIRED: &str = "EXPIRED";

/// Membership created / revoked (published in the target AND home tenant).
pub(crate) const TOPIC_MEMBERSHIP_CHANGED: &str = "udb.authn.service.membership.changed.v1";
/// A home bearer was exchanged for a target-tenant token (published in both
/// tenants).
pub(crate) const TOPIC_TENANT_TOKEN_EXCHANGED: &str = "udb.authn.service.token.exchanged.v1";

// ── Statuses ─────────────────────────────────────────────────────────────────

/// The ONE refusal for "no usable membership": none, expired, revoked, other
/// project, or the caller's own tenant. Identical so the exchange reveals
/// nothing about other tenants' approvals.
fn membership_required_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        Code::PermissionDenied,
        "exchange_service_token_for_tenant",
        "service_tenant_membership_required",
        "no active membership lets this service account act in the requested tenant and project",
    )
}

fn home_service_bearer_required_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        Code::PermissionDenied,
        "exchange_service_token_for_tenant",
        "service_tenant_exchange_requires_home_bearer",
        "exchanging for a tenant token requires the service account's own home bearer with udb:authn:exchange-tenant-token",
    )
}

fn target_admin_required_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        Code::PermissionDenied,
        "service_tenant_membership",
        "service_tenant_membership_target_admin_required",
        "only an administrator of the target tenant can manage its service-account memberships",
    )
}

/// One refusal for every reason an account cannot be approved (unknown,
/// another tenant, not a service account, inactive, no active grant), so the
/// target-tenant admin learns nothing more about the home tenant.
fn account_ineligible_status() -> Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "the service account cannot be approved: it must be an ACTIVE service account of home_tenant_id with an ACTIVE grant",
        [(
            "user_id",
            "must be an ACTIVE service account of home_tenant_id with an ACTIVE grant",
        )],
    )
}

fn membership_not_found_status() -> Status {
    crate::runtime::executor_utils::schema_status(
        Code::NotFound,
        "authn",
        "service_tenant_membership_revoke",
        "service_tenant_membership_not_found",
        "service-account tenant membership not found",
    )
}

// ── Pure validation ──────────────────────────────────────────────────────────

/// A canonical tenant UUID (the target tenant of a membership).
pub(crate) fn validated_target_tenant(raw: &str) -> Result<String, Status> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(grants_invalid_fields(
            "target_tenant_id is required",
            [("target_tenant_id", "must be a non-empty tenant id")],
        ));
    }
    Uuid::parse_str(raw)
        .map(|tenant| tenant.to_string())
        .map_err(|_| {
            grants_invalid_fields(
                "target_tenant_id must be a tenant UUID",
                [("target_tenant_id", "must be the canonical tenant UUID")],
            )
        })
}

fn validated_bounded_text(
    raw: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<String, Status> {
    let value = raw.trim();
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(grants_invalid_fields(
            format!("{field} is malformed"),
            [(
                field,
                format!("must be at most {max_bytes} bytes and contain no control characters"),
            )],
        ));
    }
    Ok(value.to_string())
}

/// The approved scope set of a membership: validated like every service
/// scope (no admin, owner, wildcard or platform scope), non-empty, and never
/// the exchange scope itself (an exchanged token is never exchanged again).
pub(crate) fn membership_approved_scopes(raw: &[String]) -> Result<Vec<String>, Status> {
    let approved = validate_service_scopes(&[], raw)?;
    if approved.is_empty() {
        return Err(grants_invalid_fields(
            "approved_scopes must contain at least one scope",
            [("approved_scopes", "must be a non-empty scope list")],
        ));
    }
    if approved
        .iter()
        .any(|scope| scope.eq_ignore_ascii_case(EXCHANGE_TENANT_TOKEN_SCOPE))
    {
        return Err(grants_invalid_fields(
            "approved_scopes must not include udb:authn:exchange-tenant-token",
            [(
                "approved_scopes",
                "must not include udb:authn:exchange-tenant-token",
            )],
        ));
    }
    Ok(approved)
}

/// `expires_at` of a new membership: required, in the future, at most
/// [`MEMBERSHIP_MAX_LIFETIME_SECS`] away. Returns unix seconds.
pub(crate) fn validated_membership_expiry(
    expires_at: Option<&prost_types::Timestamp>,
    now: u64,
) -> Result<u64, Status> {
    let invalid = |description: &str| {
        grants_invalid_fields(
            format!("expires_at {description}"),
            [("expires_at", description.to_string())],
        )
    };
    let Some(expires_at) = expires_at else {
        return Err(invalid("is required"));
    };
    if expires_at.seconds <= 0 || !(0..1_000_000_000).contains(&expires_at.nanos) {
        return Err(invalid("must be a valid future timestamp"));
    }
    let expires = expires_at.seconds as u64;
    if expires <= now {
        return Err(invalid("must be in the future"));
    }
    if expires - now > MEMBERSHIP_MAX_LIFETIME_SECS {
        return Err(invalid("must be at most one year away"));
    }
    Ok(expires)
}

/// The `mbr` claim: `"<membership_id>:<revision>"`.
pub(crate) fn membership_pin(membership_id: &str, revision: i64) -> String {
    format!("{}:{revision}", membership_id.trim())
}

/// Parse an `mbr` claim; `None` for anything that is not a UUID and a
/// positive revision.
pub(crate) fn parse_membership_pin(raw: &str) -> Option<(Uuid, i64)> {
    let (id, revision) = raw.trim().rsplit_once(':')?;
    let id = Uuid::parse_str(id).ok()?;
    let revision = revision.parse::<i64>().ok()?;
    (revision > 0).then_some((id, revision))
}

/// An exchanged token's lifetime: at most [`MEMBERSHIP_TOKEN_MAX_TTL_SECS`],
/// never past the membership's expiry. `None` once the membership expired.
pub(crate) fn membership_token_ttl(expires_at_unix: u64, now: u64) -> Option<u64> {
    let left = expires_at_unix.checked_sub(now)?;
    (left > 0).then(|| left.min(MEMBERSHIP_TOKEN_MAX_TTL_SECS))
}

// ── Store ────────────────────────────────────────────────────────────────────

/// Runtime view of one `service_account_tenant_memberships` row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MembershipRecord {
    pub membership_id: String,
    pub user_id: String,
    pub home_tenant_id: String,
    pub target_tenant_id: String,
    pub target_project_id: String,
    pub scopes: Vec<String>,
    pub status: String,
    pub revision: i64,
    pub approved_by: String,
    pub reason: String,
    pub expires_at_unix: u64,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

impl MembershipRecord {
    pub(crate) fn to_pb(&self) -> authn_entity_pb::ServiceAccountTenantMembership {
        authn_entity_pb::ServiceAccountTenantMembership {
            membership_id: self.membership_id.clone(),
            user_id: self.user_id.clone(),
            home_tenant_id: self.home_tenant_id.clone(),
            target_tenant_id: self.target_tenant_id.clone(),
            target_project_id: self.target_project_id.clone(),
            approved_scopes_json: scopes_to_json(&self.scopes),
            status: self.status.clone(),
            revision: self.revision,
            approved_by: self.approved_by.clone(),
            reason: self.reason.clone(),
            expires_at: timestamp_from_unix(self.expires_at_unix),
            created_at: timestamp_from_unix(self.created_at_unix),
            updated_at: timestamp_from_unix(self.updated_at_unix),
        }
    }

    /// ACTIVE and not yet expired at `now`.
    pub(crate) fn is_live(&self, now: u64) -> bool {
        self.status == STATUS_ACTIVE && self.expires_at_unix > now
    }
}

fn membership_select_columns() -> Vec<&'static str> {
    vec![
        "membership_id",
        "user_id",
        "home_tenant_id",
        "target_tenant_id",
        "target_project_id",
        "approved_scopes_json",
        "status",
        "revision",
        "approved_by",
        "reason",
        "expires_at",
        "created_at",
        "updated_at",
    ]
}

fn membership_select_clause() -> String {
    let m = native_model(MEMBERSHIP_MSG, &membership_select_columns());
    [
        m.text_or_empty_as("membership_id", "membership_id"),
        m.text_or_empty_as("user_id", "user_id"),
        m.text_or_empty_as("home_tenant_id", "home_tenant_id"),
        m.text_or_empty_as("target_tenant_id", "target_tenant_id"),
        m.text_or_empty_as("target_project_id", "target_project_id"),
        m.json_text_as("approved_scopes_json", "approved_scopes_json"),
        m.text_or_empty_as("status", "status"),
        format!("{} AS revision", m.q("revision")),
        m.text_or_empty_as("approved_by", "approved_by"),
        m.text_or_empty_as("reason", "reason"),
        m.timestamp_unix_as("expires_at", "expires_at_unix"),
        m.timestamp_unix_as("created_at", "created_at_unix"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
    ]
    .join(", ")
}

/// A row that fails to decode degrades to empty fields — an empty status is
/// never ACTIVE, so every consumer fails closed on it.
fn membership_row_from(row: &sqlx::postgres::PgRow) -> MembershipRecord {
    MembershipRecord {
        membership_id: row.try_get("membership_id").unwrap_or_default(),
        user_id: row.try_get("user_id").unwrap_or_default(),
        home_tenant_id: row.try_get("home_tenant_id").unwrap_or_default(),
        target_tenant_id: row.try_get("target_tenant_id").unwrap_or_default(),
        target_project_id: row.try_get("target_project_id").unwrap_or_default(),
        scopes: scopes_from_json(
            &row.try_get::<String, _>("approved_scopes_json")
                .unwrap_or_else(|_| "[]".to_string()),
        ),
        status: row.try_get("status").unwrap_or_default(),
        revision: row.try_get("revision").unwrap_or(0),
        approved_by: row.try_get("approved_by").unwrap_or_default(),
        reason: row.try_get("reason").unwrap_or_default(),
        expires_at_unix: row.try_get::<i64, _>("expires_at_unix").unwrap_or(0).max(0) as u64,
        created_at_unix: row.try_get::<i64, _>("created_at_unix").unwrap_or(0).max(0) as u64,
        updated_at_unix: row.try_get::<i64, _>("updated_at_unix").unwrap_or(0).max(0) as u64,
    }
}

/// Lock the account and require an ACTIVE `SERVICE_ACCOUNT` of `home_tenant`
/// holding an ACTIVE grant there. Every miss is the same
/// [`account_ineligible_status`].
async fn verify_membership_account(
    conn: &mut sqlx::PgConnection,
    user: &Uuid,
    home_tenant: &str,
) -> Result<(), Status> {
    let um = native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "tenant_id", "account_kind", "status"],
    );
    // Raw escape hatch: `FOR UPDATE` keeps a concurrent deactivation from
    // racing the approval inside this transaction.
    let sql = format!(
        "SELECT 1 FROM {rel} WHERE {uid} = $1 AND {tenant} = $2 \
           AND {kind} = 'SERVICE_ACCOUNT' AND {status} = 'ACTIVE' FOR UPDATE",
        rel = um.relation,
        uid = um.q("user_id"),
        tenant = um.q("tenant_id"),
        kind = um.q("account_kind"),
        status = um.q("status"),
    );
    let account = sqlx::query(&sql)
        .bind(user)
        .bind(home_tenant)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("service tenant membership account load failed"))?;
    if account.is_none() {
        return Err(account_ineligible_status());
    }
    let grant = get_grant_by_user(&mut *conn, home_tenant, &user.to_string()).await?;
    if !grant.is_some_and(|grant| grant.status == STATUS_ACTIVE) {
        return Err(account_ineligible_status());
    }
    Ok(())
}

/// Mark an ACTIVE but lapsed membership for the same (account, tenant,
/// project) EXPIRED, so a fresh approval can replace it.
async fn expire_lapsed_membership(
    conn: &mut sqlx::PgConnection,
    user: &Uuid,
    target_tenant: &str,
    target_project: &str,
) -> Result<(), Status> {
    let m = native_model(MEMBERSHIP_MSG, &membership_select_columns());
    let sql = format!(
        "UPDATE {rel} SET {status} = $4, {revision} = {revision} + 1, {updated} = now() \
         WHERE {uid} = $1 AND {target} = $2 AND {project} = $3 \
           AND {status} = $5 AND {expires} <= now()",
        rel = m.relation,
        status = m.q("status"),
        revision = m.q("revision"),
        updated = m.q("updated_at"),
        uid = m.q("user_id"),
        target = m.q("target_tenant_id"),
        project = m.q("target_project_id"),
        expires = m.q("expires_at"),
    );
    sqlx::query(&sql)
        .bind(user)
        .bind(target_tenant)
        .bind(target_project)
        .bind(STATUS_EXPIRED)
        .bind(STATUS_ACTIVE)
        .execute(&mut *conn)
        .await
        .map_err(map_err("service tenant membership expiry failed"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_membership(
    conn: &mut sqlx::PgConnection,
    user: &Uuid,
    home_tenant: &str,
    target_tenant: &str,
    target_project: &str,
    scopes: &[String],
    approved_by: &str,
    reason: &str,
    expires_at_unix: u64,
) -> Result<MembershipRecord, Status> {
    let m = native_model(MEMBERSHIP_MSG, &membership_select_columns());
    // Raw escape hatch: the partial unique index collision must map to a
    // typed FailedPrecondition, which the typed-write error mapping cannot
    // surface.
    let sql = format!(
        "INSERT INTO {rel} \
            ({mid}, {uid}, {home}, {target}, {project}, {scopes}, {status}, {revision}, \
             {approved_by}, {reason}, {expires}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5::JSONB, $6, 1, $7, $8, \
                 to_timestamp($9::DOUBLE PRECISION)) \
         RETURNING {cols}",
        rel = m.relation,
        mid = m.q("membership_id"),
        uid = m.q("user_id"),
        home = m.q("home_tenant_id"),
        target = m.q("target_tenant_id"),
        project = m.q("target_project_id"),
        scopes = m.q("approved_scopes_json"),
        status = m.q("status"),
        revision = m.q("revision"),
        approved_by = m.q("approved_by"),
        reason = m.q("reason"),
        expires = m.q("expires_at"),
        cols = membership_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(home_tenant)
        .bind(target_tenant)
        .bind(target_project)
        .bind(scopes_to_json(scopes))
        .bind(STATUS_ACTIVE)
        .bind(approved_by)
        .bind(reason)
        .bind(expires_at_unix as f64)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| match unique_violation_constraint(&err) {
            Some(_) => crate::runtime::executor_utils::failed_precondition_fields(
                "an active membership already exists for this service account, tenant and project",
                [(
                    "user_id",
                    "must not already hold an active membership for target_tenant_id and target_project_id (revoke it first)",
                )],
            ),
            None => map_err("service tenant membership insert failed")(err),
        })?;
    Ok(membership_row_from(&row))
}

/// One membership by id (any tenant: the caller compares the tenants).
pub(crate) async fn get_membership_by_id<'c, E>(
    executor: E,
    membership_id: &Uuid,
) -> Result<Option<MembershipRecord>, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let m = native_model(MEMBERSHIP_MSG, &["membership_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {mid} = $1 LIMIT 1",
        cols = membership_select_clause(),
        rel = m.relation,
        mid = m.q("membership_id"),
    );
    let row = sqlx::query(&sql)
        .bind(membership_id)
        .fetch_optional(executor)
        .await
        .map_err(map_err("service tenant membership get failed"))?;
    Ok(row.map(|row| membership_row_from(&row)))
}

/// The ACTIVE membership of `user` (homed in `home_tenant`) for the target
/// tenant/project, if any. Expiry is the caller's check.
async fn find_active_membership(
    pool: &PgPool,
    user: &Uuid,
    home_tenant: &str,
    target_tenant: &str,
    target_project: &str,
) -> Result<Option<MembershipRecord>, Status> {
    let m = native_model(MEMBERSHIP_MSG, &membership_select_columns());
    let sql = format!(
        "SELECT {cols} FROM {rel} \
         WHERE {uid} = $1 AND {home} = $2 AND {target} = $3 AND {project} = $4 \
           AND {status} = $5 LIMIT 1",
        cols = membership_select_clause(),
        rel = m.relation,
        uid = m.q("user_id"),
        home = m.q("home_tenant_id"),
        target = m.q("target_tenant_id"),
        project = m.q("target_project_id"),
        status = m.q("status"),
    );
    let row = sqlx::query(&sql)
        .bind(user)
        .bind(home_tenant)
        .bind(target_tenant)
        .bind(target_project)
        .bind(STATUS_ACTIVE)
        .fetch_optional(pool)
        .await
        .map_err(map_err("service tenant membership lookup failed"))?;
    Ok(row.map(|row| membership_row_from(&row)))
}

async fn list_memberships(
    pool: &PgPool,
    target_tenant: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<MembershipRecord>, Status> {
    let m = native_model(MEMBERSHIP_MSG, &membership_select_columns());
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {target} = $1 \
         ORDER BY {created} DESC, {mid} ASC LIMIT $2 OFFSET $3",
        cols = membership_select_clause(),
        rel = m.relation,
        target = m.q("target_tenant_id"),
        created = m.q("created_at"),
        mid = m.q("membership_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(target_tenant)
        .bind(limit.max(1))
        .bind(offset.max(0))
        .fetch_all(pool)
        .await
        .map_err(map_err("service tenant membership list failed"))?;
    Ok(rows.iter().map(membership_row_from).collect())
}

/// Revoke an ACTIVE membership of `target_tenant`, bumping its revision so
/// every token pinned to the old revision stops validating. `None` when no
/// ACTIVE row matched.
async fn revoke_membership(
    conn: &mut sqlx::PgConnection,
    membership_id: &Uuid,
    target_tenant: &str,
    approved_by: &str,
    reason: &str,
) -> Result<Option<MembershipRecord>, Status> {
    let m = native_model(MEMBERSHIP_MSG, &membership_select_columns());
    let sql = format!(
        "UPDATE {rel} SET {status} = $3, {revision} = {revision} + 1, \
            {approved_by} = $4, {reason} = $5, {updated} = now() \
         WHERE {mid} = $1 AND {target} = $2 AND {status} = $6 \
         RETURNING {cols}",
        rel = m.relation,
        status = m.q("status"),
        revision = m.q("revision"),
        approved_by = m.q("approved_by"),
        reason = m.q("reason"),
        updated = m.q("updated_at"),
        mid = m.q("membership_id"),
        target = m.q("target_tenant_id"),
        cols = membership_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(membership_id)
        .bind(target_tenant)
        .bind(STATUS_REVOKED)
        .bind(approved_by)
        .bind(reason)
        .bind(STATUS_ACTIVE)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("service tenant membership revoke failed"))?;
    Ok(row.map(|row| membership_row_from(&row)))
}

// ── Request-time validation of exchanged tokens ──────────────────────────────

/// The claims of an exchanged token that its persisted-state check compares.
pub(crate) struct MembershipTokenClaims<'a> {
    pub pin: &'a str,
    pub subject: &'a str,
    pub tenant_id: &'a str,
    pub project_id: &'a str,
    pub home_tenant_id: &'a str,
    pub service_identity: &'a str,
    pub scopes: &'a [String],
}

/// Is an exchanged token still backed by live state? The membership it is
/// pinned to must be ACTIVE, unexpired and at the pinned revision, for the
/// same subject, home tenant, target tenant and project, with every token
/// scope still approved; the home grant must be ACTIVE with the token's
/// service identity; the account must still be an ACTIVE service account.
/// `Err` is a store failure (the caller fails closed on it).
pub(crate) async fn validate_membership_principal(
    pool: &PgPool,
    claims: &MembershipTokenClaims<'_>,
    now: u64,
) -> Result<bool, String> {
    let Some((membership_id, revision)) = parse_membership_pin(claims.pin) else {
        return Ok(false);
    };
    let Ok(subject) = Uuid::parse_str(claims.subject.trim()) else {
        return Ok(false);
    };
    let home = claims.home_tenant_id.trim();
    if home.is_empty()
        || claims.tenant_id.trim().is_empty()
        || claims.service_identity.trim().is_empty()
        || claims.scopes.is_empty()
    {
        return Ok(false);
    }
    let store_err = |context: &str, status: Status| {
        crate::runtime::executor_utils::store_string_from_status(context, &status)
    };
    let Some(membership) = get_membership_by_id(pool, &membership_id)
        .await
        .map_err(|status| store_err("service tenant membership lookup failed", status))?
    else {
        return Ok(false);
    };
    let same_subject = Uuid::parse_str(membership.user_id.trim()).ok() == Some(subject);
    if !membership.is_live(now)
        || membership.revision != revision
        || !same_subject
        || membership.home_tenant_id.trim() != home
        || membership.target_tenant_id.trim() != claims.tenant_id.trim()
        || membership.target_project_id.trim() != claims.project_id.trim()
        || validate_service_scopes(claims.scopes, &membership.scopes).is_err()
    {
        return Ok(false);
    }
    let grant = get_grant_by_user(pool, home, &subject.to_string())
        .await
        .map_err(|status| store_err("service tenant membership grant lookup failed", status))?;
    let Some(grant) = grant.filter(|grant| grant.status == STATUS_ACTIVE) else {
        return Ok(false);
    };
    if grant.service_identity.trim() != claims.service_identity.trim() {
        return Ok(false);
    }
    owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id).await
}

// ── Events ───────────────────────────────────────────────────────────────────

/// One membership-changed event, published in `event_tenant` (the target and
/// the home tenant each get one). The actor is always the target tenant's
/// administrator.
fn build_membership_event(
    event_tenant: &str,
    record: &MembershipRecord,
    operation: &str,
    reason_code: &str,
    actor: &str,
) -> AuthEvent {
    AuthEvent::new(
        TOPIC_MEMBERSHIP_CHANGED,
        record.membership_id.clone(),
        event_tenant.to_string(),
        serde_json::json!({
            "membership_id": record.membership_id,
            "user_id": record.user_id,
            "home_tenant_id": record.home_tenant_id,
            "target_tenant_id": record.target_tenant_id,
            "target_project_id": record.target_project_id,
            "revision": record.revision,
            "status": record.status,
            "operation": operation,
            "actor_tenant_id": record.target_tenant_id,
        }),
    )
    .with_correlation(format!(
        "service_tenant_membership:{}:{}",
        record.membership_id, record.revision
    ))
    .with_compliance(ComplianceEnvelope {
        actor: actor.to_string(),
        target_resource: format!("service_account_tenant_membership:{}", record.membership_id),
        target_tenant: record.target_tenant_id.clone(),
        target_project: record.target_project_id.clone(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        reason_code: reason_code.to_string(),
        auth_method: event_auth_method(),
        ..ComplianceEnvelope::default()
    })
}

/// One token-exchanged event, published in `event_tenant`: actor = the
/// service account (acting from its home tenant), target = the target tenant.
#[allow(clippy::too_many_arguments)]
fn build_exchange_event(
    event_tenant: &str,
    membership: &MembershipRecord,
    subject: &str,
    service_identity: &str,
    jti: &str,
    scopes: &[String],
    expires_at_unix: i64,
) -> AuthEvent {
    AuthEvent::new(
        TOPIC_TENANT_TOKEN_EXCHANGED,
        membership.membership_id.clone(),
        event_tenant.to_string(),
        serde_json::json!({
            "membership_id": membership.membership_id,
            "membership_revision": membership.revision,
            "user_id": subject,
            "home_tenant_id": membership.home_tenant_id,
            "target_tenant_id": membership.target_tenant_id,
            "target_project_id": membership.target_project_id,
            "jti": jti,
            "scopes": scopes,
            "expires_at_unix": expires_at_unix,
        }),
    )
    .with_correlation(format!("service_tenant_exchange:{jti}"))
    .with_compliance(ComplianceEnvelope {
        actor: subject.to_string(),
        target_resource: format!(
            "service_account_tenant_membership:{}",
            membership.membership_id
        ),
        target_tenant: membership.target_tenant_id.clone(),
        target_project: membership.target_project_id.clone(),
        operation: "exchange_service_token_for_tenant".to_string(),
        outcome: "success".to_string(),
        reason_code: "membership_token_issued".to_string(),
        auth_method: event_auth_method(),
        credential_id: jti.to_string(),
        service_identity: service_identity.to_string(),
        ..ComplianceEnvelope::default()
    })
}

// ── Authorization ────────────────────────────────────────────────────────────

/// The target tenant of a membership RPC, when the caller administers it: the
/// verified claim tenant must BE the target. A home-tenant administrator, or
/// any other tenant's, is refused; so is a claim with no tenant. In-process
/// calls with no claim context (offline CLI, tests) are trusted, as for every
/// grant RPC.
fn require_target_tenant_admin(raw_target: &str) -> Result<String, Status> {
    let target = validated_target_tenant(raw_target)?;
    if crate::runtime::service::method_security::claim_context_present() {
        let ctx = crate::runtime::service::method_security::current_claim_context();
        if !ctx.authenticated || ctx.tenant_id.trim() != target {
            tracing::warn!(
                target: "udb.audit.authz",
                subject = %ctx.subject,
                claim_tenant = %ctx.tenant_id,
                target_tenant = %target,
                "DENY: service-account membership management outside the caller's tenant"
            );
            return Err(target_admin_required_status());
        }
    }
    Ok(target)
}

// ── AuthnService handler impls (trait wiring lives in authn/mod.rs) ──────────

pub(crate) async fn create_service_account_tenant_membership(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::CreateServiceAccountTenantMembershipRequest>,
) -> Result<Response<authn_pb::CreateServiceAccountTenantMembershipResponse>, Status> {
    let req = request.into_inner();
    let target = require_target_tenant_admin(&req.target_tenant_id)?;
    let user = parse_uuid(&req.user_id, "user_id")?;
    let home = validated_bounded_text(&req.home_tenant_id, "home_tenant_id", 120)?;
    if home.is_empty() {
        return Err(grants_invalid_fields(
            "home_tenant_id is required",
            [("home_tenant_id", "must be the service account's tenant")],
        ));
    }
    if home.eq_ignore_ascii_case(&target) {
        return Err(grants_invalid_fields(
            "a membership must target a tenant other than the service account's own",
            [("target_tenant_id", "must differ from home_tenant_id")],
        ));
    }
    let target_project = validated_bounded_text(&req.target_project_id, "target_project_id", 255)?;
    let scopes = membership_approved_scopes(&req.approved_scopes)?;
    let reason = validated_bounded_text(&req.reason, "reason", 1024)?;
    let expires_at = validated_membership_expiry(req.expires_at.as_ref(), now_unix())?;
    let approved_by = caller_updated_by();
    // The approval, its lapsed predecessor's expiry and both tenants' audit
    // events commit in ONE transaction.
    let mut tx = begin_grants_tx(pool, "service_tenant_membership_create_tx").await?;
    verify_membership_account(&mut *tx, &user, &home).await?;
    expire_lapsed_membership(&mut *tx, &user, &target, &target_project).await?;
    let record = insert_membership(
        &mut *tx,
        &user,
        &home,
        &target,
        &target_project,
        &scopes,
        &approved_by,
        &reason,
        expires_at,
    )
    .await?;
    for event_tenant in [&record.target_tenant_id, &record.home_tenant_id] {
        svc.emit_event_in_tx(
            &mut *tx,
            build_membership_event(
                event_tenant,
                &record,
                "create",
                "service_tenant_membership_created",
                &approved_by,
            ),
        )
        .await?;
    }
    commit_grants_tx(tx, "service_tenant_membership_create_tx").await?;
    Ok(Response::new(
        authn_pb::CreateServiceAccountTenantMembershipResponse {
            membership: Some(record.to_pb()),
            message: "service-account tenant membership created".to_string(),
            error: None,
        },
    ))
}

pub(crate) async fn list_service_account_tenant_memberships(
    _svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::ListServiceAccountTenantMembershipsRequest>,
) -> Result<Response<authn_pb::ListServiceAccountTenantMembershipsResponse>, Status> {
    let req = request.into_inner();
    let target = require_target_tenant_admin(&req.target_tenant_id)?;
    let (limit, offset) = page_window(req.page_size, &req.page_token);
    let records = list_memberships(pool, &target, limit, offset).await?;
    let next_page_token = next_page_token(records.len(), limit, offset);
    Ok(Response::new(
        authn_pb::ListServiceAccountTenantMembershipsResponse {
            memberships: records.iter().map(MembershipRecord::to_pb).collect(),
            next_page_token,
            message: "service-account tenant memberships".to_string(),
            error: None,
        },
    ))
}

pub(crate) async fn revoke_service_account_tenant_membership(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::RevokeServiceAccountTenantMembershipRequest>,
) -> Result<Response<authn_pb::RevokeServiceAccountTenantMembershipResponse>, Status> {
    let req = request.into_inner();
    let target = require_target_tenant_admin(&req.target_tenant_id)?;
    let membership_id = parse_uuid(&req.membership_id, "membership_id")?;
    let reason = validated_bounded_text(&req.reason, "reason", 1024)?;
    let approved_by = caller_updated_by();
    let mut tx = begin_grants_tx(pool, "service_tenant_membership_revoke_tx").await?;
    let revoked =
        revoke_membership(&mut *tx, &membership_id, &target, &approved_by, &reason).await?;
    match &revoked {
        Some(record) => {
            for event_tenant in [&record.target_tenant_id, &record.home_tenant_id] {
                svc.emit_event_in_tx(
                    &mut *tx,
                    build_membership_event(
                        event_tenant,
                        record,
                        "revoke",
                        "service_tenant_membership_revoked",
                        &approved_by,
                    ),
                )
                .await?;
            }
        }
        // Not ACTIVE: already revoked/expired in this tenant (idempotent), or
        // not a membership of this tenant at all (NOT_FOUND).
        None => {
            let existing = get_membership_by_id(&mut *tx, &membership_id).await?;
            if !existing.is_some_and(|record| record.target_tenant_id.trim() == target) {
                return Err(membership_not_found_status());
            }
        }
    }
    commit_grants_tx(tx, "service_tenant_membership_revoke_tx").await?;
    Ok(Response::new(
        authn_pb::RevokeServiceAccountTenantMembershipResponse {
            revoked: revoked.is_some(),
            message: "service-account tenant membership revoked".to_string(),
            error: None,
        },
    ))
}

pub(crate) async fn exchange_service_token_for_tenant(
    svc: &AuthnServiceImpl,
    pool: &PgPool,
    request: Request<authn_pb::ExchangeServiceTokenForTenantRequest>,
) -> Result<Response<authn_pb::ExchangeServiceTokenForTenantResponse>, Status> {
    let req = request.into_inner();
    // There is no home bearer to exchange without a verified claim.
    if !crate::runtime::service::method_security::claim_context_present() {
        return Err(home_service_bearer_required_status());
    }
    let ctx = crate::runtime::service::method_security::current_claim_context();
    // Service accounts only, with the exchange scope granted literally, and
    // never with a token this RPC minted (no chained exchanges).
    if !ctx.is_service_account_bearer()
        || !ctx.has_exact_scope(EXCHANGE_TENANT_TOKEN_SCOPE)
        || ctx.credential_id.starts_with(MEMBERSHIP_TOKEN_JTI_PREFIX)
        || ctx
            .auth_method
            .eq_ignore_ascii_case(MEMBERSHIP_TOKEN_AUTH_METHOD)
    {
        return Err(home_service_bearer_required_status());
    }
    let Ok(subject) = Uuid::parse_str(ctx.subject.trim()) else {
        return Err(home_service_bearer_required_status());
    };
    let home = ctx.tenant_id.trim().to_string();
    let target = validated_target_tenant(&req.target_tenant_id)?;
    let target_project = validated_bounded_text(&req.target_project_id, "target_project_id", 255)?;
    if home.is_empty() || home.eq_ignore_ascii_case(&target) {
        return Err(membership_required_status());
    }
    // The home grant this bearer was minted from must still stand, for this
    // exact identity and project.
    let grant = get_grant_by_user(pool, &home, &subject.to_string()).await?;
    let Some(grant) = grant.filter(|grant| {
        grant.status == STATUS_ACTIVE
            && grant.service_identity.trim() == ctx.service_identity.trim()
            && grant.project_id.trim() == ctx.project_id.trim()
    }) else {
        return Err(home_service_bearer_required_status());
    };
    if !owner_is_active_service_account(pool, &grant.user_id, &grant.tenant_id)
        .await
        .map_err(|error| {
            crate::runtime::executor_utils::internal_status(
                "authn",
                "exchange_service_token_for_tenant_account",
                error,
            )
        })?
    {
        return Err(home_service_bearer_required_status());
    }
    let now = now_unix();
    let membership = find_active_membership(pool, &subject, &home, &target, &target_project)
        .await?
        .filter(|membership| membership.is_live(now))
        .ok_or_else(membership_required_status)?;
    let scopes = validate_service_scopes(&req.requested_scopes, &membership.scopes)?;
    if scopes.is_empty() {
        return Err(membership_required_status());
    }
    let ttl = membership_token_ttl(membership.expires_at_unix, now)
        .ok_or_else(membership_required_status)?;
    let jti = format!("{MEMBERSHIP_TOKEN_JTI_PREFIX}{}", Uuid::new_v4().simple());
    let pin = membership_pin(&membership.membership_id, membership.revision);
    let subject = subject.to_string();
    let (access_token, expires_at_unix) = svc
        .issue_membership_access_token(
            &subject,
            &target,
            &target_project,
            &scopes,
            &grant.service_identity,
            &jti,
            crate::runtime::security::MembershipPin {
                home_tenant_id: &home,
                membership: &pin,
            },
            now,
            ttl,
        )
        .await?;
    tracing::info!(
        target: "udb.audit.authn",
        user_id = %subject,
        home_tenant_id = %home,
        target_tenant_id = %target,
        target_project_id = %target_project,
        membership_id = %membership.membership_id,
        jti = %jti,
        "service account exchanged its home bearer for a tenant-membership token"
    );
    // Published in BOTH tenants: the home tenant sees its account act
    // elsewhere, the target tenant sees who acts in it.
    for event_tenant in [&home, &target] {
        svc.emit_event(build_exchange_event(
            event_tenant,
            &membership,
            &subject,
            &grant.service_identity,
            &jti,
            &scopes,
            expires_at_unix,
        ))
        .await;
    }
    Ok(Response::new(
        authn_pb::ExchangeServiceTokenForTenantResponse {
            access_token,
            expires_at_unix,
            tenant_id: target,
            project_id: target_project,
            scopes,
            membership_id: membership.membership_id,
            message: "tenant-membership token issued".to_string(),
            error: None,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn target_tenant_must_be_a_canonical_uuid() {
        let tenant = "6F9619FF-8B86-D011-B42D-00C04FC964FF";
        assert_eq!(
            validated_target_tenant(tenant).unwrap(),
            tenant.to_ascii_lowercase()
        );
        for raw in ["", "  ", "acme", "6f9619ff-8b86-d011-b42d"] {
            assert_eq!(
                validated_target_tenant(raw).unwrap_err().code(),
                Code::InvalidArgument,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn approved_scopes_reject_admin_wildcard_empty_and_the_exchange_scope() {
        assert_eq!(
            membership_approved_scopes(&strings(&["udb:read", " udb:write "])).unwrap(),
            strings(&["udb:read", "udb:write"])
        );
        for bad in [
            &["udb:admin"][..],
            &["udb:*"][..],
            &["udb:platform_admin"][..],
            &["tenant:owner"][..],
        ] {
            assert_eq!(
                membership_approved_scopes(&strings(bad))
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied,
                "{bad:?}"
            );
        }
        assert_eq!(
            membership_approved_scopes(&[]).unwrap_err().code(),
            Code::InvalidArgument
        );
        assert_eq!(
            membership_approved_scopes(&strings(&["udb:read", EXCHANGE_TENANT_TOKEN_SCOPE]))
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn expiry_is_required_future_and_within_a_year() {
        let now = 1_800_000_000;
        let at = |seconds: i64| prost_types::Timestamp { seconds, nanos: 0 };
        assert_eq!(
            validated_membership_expiry(Some(&at(now as i64 + 60)), now).unwrap(),
            now + 60
        );
        assert_eq!(
            validated_membership_expiry(
                Some(&at((now + MEMBERSHIP_MAX_LIFETIME_SECS) as i64)),
                now
            )
            .unwrap(),
            now + MEMBERSHIP_MAX_LIFETIME_SECS
        );
        for bad in [
            None,
            Some(at(now as i64)),
            Some(at(now as i64 - 1)),
            Some(at((now + MEMBERSHIP_MAX_LIFETIME_SECS) as i64 + 1)),
            Some(at(-5)),
        ] {
            assert_eq!(
                validated_membership_expiry(bad.as_ref(), now)
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn membership_pin_round_trips_and_rejects_garbage() {
        let id = Uuid::new_v4();
        let pin = membership_pin(&id.to_string(), 3);
        assert_eq!(parse_membership_pin(&pin), Some((id, 3)));
        for bad in [
            "",
            "3",
            "not-a-uuid:3",
            &format!("{id}:0"),
            &format!("{id}:-1"),
            &format!("{id}:x"),
            &format!("{id}"),
        ] {
            assert_eq!(parse_membership_pin(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn token_ttl_is_capped_and_never_outlives_the_membership() {
        let now = 1_800_000_000;
        assert_eq!(
            membership_token_ttl(now + 86_400, now),
            Some(MEMBERSHIP_TOKEN_MAX_TTL_SECS)
        );
        assert_eq!(membership_token_ttl(now + 120, now), Some(120));
        assert_eq!(membership_token_ttl(now, now), None);
        assert_eq!(membership_token_ttl(now - 1, now), None);
    }

    #[test]
    fn exchanged_scopes_are_the_requested_subset_of_the_membership() {
        let approved = strings(&["udb:read", "udb:write"]);
        assert_eq!(
            validate_service_scopes(&[], &approved).unwrap(),
            approved,
            "empty request: every approved scope"
        );
        assert_eq!(
            validate_service_scopes(&strings(&["UDB:READ"]), &approved).unwrap(),
            strings(&["udb:read"])
        );
        assert!(validate_service_scopes(&strings(&["udb:delete"]), &approved).is_err());
    }

    #[test]
    fn a_record_is_live_only_while_active_and_unexpired() {
        let now = 1_800_000_000;
        let record = MembershipRecord {
            status: STATUS_ACTIVE.to_string(),
            expires_at_unix: now + 1,
            ..MembershipRecord::default()
        };
        assert!(record.is_live(now));
        assert!(!record.is_live(now + 1));
        for status in [STATUS_REVOKED, STATUS_EXPIRED, ""] {
            let record = MembershipRecord {
                status: status.to_string(),
                ..record.clone()
            };
            assert!(!record.is_live(now), "{status:?}");
        }
    }

    #[tokio::test]
    async fn a_home_tenant_admin_cannot_manage_another_tenants_memberships() {
        use crate::runtime::service::method_security::{
            scope_claim_context_for_test, test_claim_context,
        };
        let target = Uuid::new_v4().to_string();
        let home_admin = test_claim_context(
            "home-admin",
            "home-tenant",
            "",
            &["udb:authn:manage-grants"],
            &[],
        );
        let err = scope_claim_context_for_test(home_admin, async {
            require_target_tenant_admin(&target)
        })
        .await
        .expect_err("a home-tenant admin is not an admin of the target tenant");
        assert_eq!(err.code(), Code::PermissionDenied);

        let target_admin = test_claim_context(
            "target-admin",
            &target,
            "",
            &["udb:authn:manage-grants"],
            &[],
        );
        let resolved = scope_claim_context_for_test(target_admin, async {
            require_target_tenant_admin(&target)
        })
        .await
        .expect("the target tenant's admin manages its memberships");
        assert_eq!(resolved, target);
    }

    #[tokio::test]
    async fn exchange_requires_a_home_service_bearer_with_the_exchange_scope() {
        use crate::runtime::service::method_security::{
            VerifiedClaimContext, scope_claim_context_for_test,
        };
        let svc = AuthnServiceImpl::new(
            crate::runtime::authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        );
        let service = VerifiedClaimContext {
            subject: Uuid::new_v4().to_string(),
            service_identity: "spiffe://home/svc".to_string(),
            tenant_id: "home-tenant".to_string(),
            project_id: "p".to_string(),
            scopes: strings(&[EXCHANGE_TENANT_TOKEN_SCOPE]),
            roles: Vec::new(),
            // CREDENTIAL_TYPE_SERVICE_ACCOUNT
            credential_type: 4,
            credential_id: "sess_x".to_string(),
            auth_method: "pwd".to_string(),
            authenticated: true,
        };
        let refused = [
            // A human bearer.
            VerifiedClaimContext {
                service_identity: String::new(),
                credential_type: 1,
                ..service.clone()
            },
            // No exchange scope (admin scopes do not imply it).
            VerifiedClaimContext {
                scopes: strings(&["udb:admin", "udb:*"]),
                ..service.clone()
            },
            // An exchanged token presented again.
            VerifiedClaimContext {
                credential_id: format!("{MEMBERSHIP_TOKEN_JTI_PREFIX}abc"),
                ..service.clone()
            },
            VerifiedClaimContext {
                auth_method: MEMBERSHIP_TOKEN_AUTH_METHOD.to_string(),
                ..service.clone()
            },
        ];
        // Refused before any store access: this service has no pool, and the
        // pool handed in is never touched on these paths.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")
            .expect("lazy pool");
        for ctx in refused {
            let err = scope_claim_context_for_test(
                ctx,
                exchange_service_token_for_tenant(
                    &svc,
                    &pool,
                    Request::new(authn_pb::ExchangeServiceTokenForTenantRequest {
                        target_tenant_id: Uuid::new_v4().to_string(),
                        ..Default::default()
                    }),
                ),
            )
            .await
            .expect_err("only a home service bearer may exchange");
            assert_eq!(err.code(), Code::PermissionDenied);
        }
        // Without any verified claim there is nothing to exchange.
        let err = exchange_service_token_for_tenant(
            &svc,
            &pool,
            Request::new(authn_pb::ExchangeServiceTokenForTenantRequest::default()),
        )
        .await
        .expect_err("no claim, no exchange");
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    #[tokio::test]
    async fn persisted_state_rejects_membership_claims_out_of_shape() {
        use super::super::authn::JwtPersistedState;
        let svc = AuthnServiceImpl::new(
            crate::runtime::authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        );
        let now = 1_800_000_000;
        let membership = membership_pin(&Uuid::new_v4().to_string(), 1);
        let base = crate::runtime::security::SecurityClaims {
            sub: Some(Uuid::new_v4().to_string()),
            tenant_id: Some(Uuid::new_v4().to_string()),
            project_id: Some("clinic".to_string()),
            scopes: Some(strings(&["udb:read"])),
            service_identity: Some("spiffe://home/svc".to_string()),
            home_tenant_id: Some("home-tenant".to_string()),
            mbr: Some(membership),
            jti: Some(format!("{MEMBERSHIP_TOKEN_JTI_PREFIX}abc")),
            ..Default::default()
        };
        // Membership claims without a service identity, or one of the pair
        // without the other, are not a token UDB mints: rejected outright.
        for claims in [
            crate::runtime::security::SecurityClaims {
                service_identity: None,
                ..base.clone()
            },
            crate::runtime::security::SecurityClaims {
                home_tenant_id: None,
                ..base.clone()
            },
            crate::runtime::security::SecurityClaims {
                mbr: None,
                ..base.clone()
            },
        ] {
            assert!(matches!(
                svc.jwt_persisted_state_user(&claims, now)
                    .await
                    .expect("shape rejection needs no store"),
                JwtPersistedState::Rejected
            ));
        }
        // A well-formed membership token is checked against the store; with
        // none wired it fails closed (an error), never valid.
        assert!(svc.jwt_persisted_state_user(&base, now).await.is_err());
    }
}
