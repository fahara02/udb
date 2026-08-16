//! Device, session, token-revocation, MFA-challenge, factor, and WebAuthn
//! credential lifecycle handlers (Phase 3 / I2.3, I2.4, I2.6, I2.7).
//!
//! All state is Postgres-backed via the proto-driven `native_model` descriptors
//! (no hand-maintained schema). Credential-shaped values (jti, fingerprint) are
//! stored only as keyed-HMAC digests; IPs are masked to a network prefix.

use super::*;
use crate::runtime::native_catalog::{NativeModel, native_model};
use crate::runtime::service::native_helpers::{
    native_next_page_token_for_total, native_offset_page_window,
};
use sqlx::Row;

#[cfg(any(feature = "redis", test))]
fn first_unrevoked_tenant_issue_second(cutoff_unix: u64) -> u64 {
    cutoff_unix.saturating_add(1)
}

/// JWT `iat` is encoded in whole seconds and the tenant kill boundary is
/// intentionally inclusive (`iat <= cutoff`). After a successful Redis cutoff
/// publication, do not report the revoke complete until a subsequently-started
/// login can only mint in a strictly later second.
#[cfg(feature = "redis")]
async fn wait_until_after_tenant_revoke_cutoff(cutoff_unix: u64) {
    let resume_at = first_unrevoked_tenant_issue_second(cutoff_unix);
    while now_unix() < resume_at {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn lifecycle_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn lifecycle_policy_status_with_code(
    operation: impl Into<String>,
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        policy_decision_id,
        message,
    )
}

fn lifecycle_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn revoke_device_tenant_scope_required_status() -> Status {
    lifecycle_policy_status_with_code(
        "revoke_device",
        "tenant_scoped_bearer_required",
        "device revoke requires a tenant-scoped bearer token or a cross-tenant admin role",
    )
}

fn device_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.Device",
        &[
            "device_id",
            "user_id",
            "tenant_id",
            "project_id",
            "device_name",
            "device_type",
            "fingerprint_hash",
            "last_ip_masked",
            "last_user_agent_hash",
            "last_seen_at",
            "created_at",
            "revoked_at",
            "revoked_by",
        ],
    )
}

fn revocation_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.TokenRevocation",
        &[
            "jti_hash",
            "token_type",
            "tenant_id",
            "expires_at",
            "revoked_at",
            "revoked_by",
            "reason",
        ],
    )
}

fn token_family_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.TokenFamily",
        &[
            "family_id",
            "session_id",
            "user_id",
            "principal_id",
            "tenant_id",
            "project_id",
            "device_id",
            "current_refresh_jti_hash",
            "previous_refresh_jti_hash",
            "reuse_detected_at",
            "revoked_at",
            "revocation_reason",
        ],
    )
}

fn mfa_challenge_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.MfaChallenge",
        &[
            "challenge_id",
            "user_id",
            "tenant_id",
            "project_id",
            "factor_kind",
            "purpose",
            "device_fingerprint_hash",
            "ip_address_masked",
            "attempt_count",
            "expires_at",
            "consumed_at",
            "created_at",
        ],
    )
}

/// Mask a source IP to a network prefix (IPv4 /24, IPv6 /48) so the full client
/// address never lands in the device/challenge rows. Returns the input trimmed
/// for non-IP strings.
pub(super) fn mask_ip(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(std::net::IpAddr::V4(v4)) = trimmed.parse::<std::net::IpAddr>() {
        let o = v4.octets();
        return format!("{}.{}.{}.0/24", o[0], o[1], o[2]);
    }
    if let Ok(std::net::IpAddr::V6(v6)) = trimmed.parse::<std::net::IpAddr>() {
        let s = v6.segments();
        return format!("{:x}:{:x}:{:x}::/48", s[0], s[1], s[2]);
    }
    trimmed.chars().take(64).collect()
}

impl AuthnServiceImpl {
    pub(super) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            authn_capability_status(
                "postgres_auth_store",
                "native_postgres_auth_store",
                "this operation requires the native Postgres auth store",
            )
        })
    }

    fn device_fingerprint_hash(&self, fingerprint: &str) -> String {
        authn::hash_secret(&format!("device:{fingerprint}"), &self.hash_key())
    }

    /// Resolve the neutral-runtime routing scope from the decoded request body
    /// and the validated bearer claim. Transport requests always install a claim
    /// context, so an omitted body scope inherits the claim while a conflicting
    /// tenant/project is rejected. Direct in-process callers retain the explicit
    /// body scope, matching the shared method-security contract.
    fn request_authn_context(
        &self,
        request_context: Option<&crate::proto::udb::core::common::v1::RequestContext>,
    ) -> Result<crate::RequestContext, Status> {
        let tenant = request_context.and_then(|context| context.tenant.as_ref());
        let body_tenant = tenant.map(|scope| scope.tenant_id.as_str()).unwrap_or_default();
        let body_project = tenant
            .map(|scope| scope.project_id.as_str())
            .unwrap_or_default();
        let claim = crate::runtime::service::method_security::current_claim_context();
        let (tenant_id, project_id) =
            crate::runtime::service::method_security::resolve_body_tenant_scope(
                &claim,
                body_tenant,
                body_project,
            )?;
        Ok(self.authn_context(&tenant_id, &project_id))
    }

    /// D3 — authorize a per-user device/credential operation against the VALIDATED
    /// bearer claim (never the request body): the caller may target `user_id` only
    /// when the target user belongs to the caller's claim tenant, OR the caller IS
    /// that user, OR the caller holds a genuine cross-tenant/platform-admin role.
    /// Fails closed so a tenant-A admin token cannot list/revoke devices for a
    /// tenant-B user by passing that user's id. The DENY is auditable (subject +
    /// target tenant) via the shared body-tenant guard's tracing.
    pub(super) async fn authorize_target_user(&self, user_id: &str) -> Result<(), Status> {
        // No claim context (in-process / trusted caller, not over the wire): the
        // shared guards bypass too, so there is nothing to enforce here.
        if !crate::runtime::service::method_security::claim_context_present() {
            return Ok(());
        }
        let ctx = crate::runtime::service::method_security::current_claim_context();
        if ctx.is_cross_tenant_admin() {
            return Ok(());
        }
        // The caller acting on their own account is always allowed.
        if !ctx.subject.trim().is_empty() && ctx.subject.trim() == user_id.trim() {
            return Ok(());
        }
        // Otherwise the target user must live in the caller's claim tenant. Resolve
        // the user's tenant and compare it to the validated claim tenant via the
        // shared D1 guard (which also denies a tenantless non-admin caller).
        let target = self
            .users
            .get_user_by_id(user_id)
            .await
            .map_err(|err| {
                lifecycle_internal_status("authorize_target_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &ctx,
            &target.tenant_id,
            &target.project_id,
        )
    }

    fn jti_hash(&self, jti: &str) -> String {
        authn::hash_secret(&format!("jti:{jti}"), &self.hash_key())
    }

    /// Best-effort SET of a revoked `jti_hash` onto the short-TTL cluster denylist
    /// (Redis). The TTL is the token's own remaining lifetime (`expires_at_unix -
    /// now`) so the entry expires exactly when the token would — no unbounded
    /// growth; when no expiry is known the denylist's default (access-token max
    /// lifetime) is used. A Redis failure is logged but NEVER fails the revoke —
    /// the durable `token_revocations` row remains the source of truth. No-op
    /// when Redis is not configured (`jti_denylist` is `None`) or in the slim
    /// (no-`redis`) build.
    async fn denylist_revoked_jti(&self, jti_hash: &str, expires_at_unix: u64) {
        #[cfg(feature = "redis")]
        {
            let Some(denylist) = self.jti_denylist.as_ref() else {
                return;
            };
            // Token's remaining life → TTL. `0` lets `JtiDenylist::add` fall back
            // to its default (access-token max lifetime), covering tokens revoked
            // without a known expiry (e.g. session handles).
            let ttl = if expires_at_unix > 0 {
                expires_at_unix.saturating_sub(now_unix())
            } else {
                0
            };
            if let Err(err) = denylist.add(jti_hash, ttl).await {
                // Best-effort: the durable DB row already committed; the denylist
                // is only an accelerator, so a Redis miss degrades to DB-only.
                tracing::warn!(
                    error = %err,
                    "jti denylist SET failed; revocation still durable in token_revocations"
                );
            }
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (jti_hash, expires_at_unix);
        }
    }

    // ── Cluster-wide revocation (I2.3) ──────────────────────────────────────

    /// Durably revoke a token by jti: insert its keyed digest into the
    /// `token_revocations` deny list so every node rejects it before expiry.
    pub(super) async fn revoke_token_jti(
        &self,
        jti: &str,
        token_type: &str,
        tenant_id: &str,
        expires_at_unix: u64,
        revoked_by: &str,
        reason: &str,
    ) -> Result<(), Status> {
        if jti.trim().is_empty() {
            return Ok(());
        }
        let propagation_started = std::time::Instant::now();
        let pool = self.require_pool()?;
        let m = revocation_model();
        let sql = format!(
            "INSERT INTO {rel} ({jti}, {ttype}, {tenant}, {expires}, {by}, {reason}) \
             VALUES ($1, $2, $3, CASE WHEN $4::BIGINT > 0 THEN to_timestamp($4::DOUBLE PRECISION) ELSE NULL END, $5, $6) \
             ON CONFLICT ({jti}) DO NOTHING",
            rel = m.relation,
            jti = m.q("jti_hash"),
            ttype = m.q("token_type"),
            tenant = m.q("tenant_id"),
            expires = m.q("expires_at"),
            by = m.q("revoked_by"),
            reason = m.q("reason"),
        );
        let event = AuthEvent::new(
            topics::TOKEN_REVOKED,
            format!("revocation:{}", Uuid::new_v4().simple()),
            tenant_id.to_string(),
            serde_json::json!({
                "token_type": token_type,
                "tenant_id": tenant_id,
                "reason": reason,
            }),
        );
        // The keyed-HMAC digest is computed once: it is both the durable row key
        // and the cluster denylist key (NEVER the raw jti).
        let jti_hash = self.jti_hash(jti);
        // Atomicity (§7): the revocation insert and its audit/outbox event land in
        // ONE transaction — either both commit or neither does. A failed audit
        // write aborts the revocation rather than leaving it without its event.
        let mut tx = pool.begin().await.map_err(|err| {
            lifecycle_internal_status(
                "token_revocation_tx_begin",
                format!("token revocation tx begin failed: {err}"),
            )
        })?;
        sqlx::query(&sql)
            .bind(&jti_hash)
            .bind(token_type)
            .bind(tenant_id)
            .bind(expires_at_unix as i64)
            .bind(revoked_by)
            .bind(reason)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "token_revocation_insert",
                    format!("token revocation insert failed: {err}"),
                )
            })?;
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            lifecycle_internal_status(
                "token_revocation_commit",
                format!("token revocation commit failed: {err}"),
            )
        })?;
        // Populate the fast cluster denylist AFTER the durable row commits, so
        // every node can reject this jti immediately (before its DB read). Best-
        // effort: a Redis failure never fails the revoke (the DB row stands).
        self.denylist_revoked_jti(&jti_hash, expires_at_unix).await;
        self.metrics
            .observe_revocation_propagation_seconds(propagation_started.elapsed().as_secs_f64());
        Ok(())
    }

    /// Is this jti on the revocation deny list? Returns (revoked, reason).
    ///
    /// Tier-1 #13 — fast cross-node revocation: the short-TTL cluster denylist
    /// (Redis) is consulted FIRST. A denylist HIT short-circuits to revoked
    /// (skipping the DB read). A MISS falls through to the durable
    /// `token_revocations` read (the source of truth). The denylist is purely an
    /// accelerator: when Redis is absent the behavior is the unchanged DB-only
    /// lookup.
    ///
    /// Phase 5 fail-closed hardening (applies to BOTH layers): on a denylist OR a
    /// DB **lookup error** the outcome depends on
    /// [`crate::runtime::security::fail_closed_mode`]. In a hardened/production
    /// posture the token is treated as revoked (deny) so a store outage cannot
    /// silently let a possibly-revoked token through; in dev/test the legacy
    /// fail-open+warn behavior is kept (a denylist error falls through to the DB
    /// read). The happy path (found revoked / not revoked) is unchanged. A
    /// missing pool (no deny list wired at all) still returns `(false, _)`.
    ///
    /// Cluster-wide token kill 3.3 — TENANT-level half: alongside the per-jti
    /// denylist this also consults the tenant denylist (`tenant_denied_after`).
    /// When the token's `tenant_id` has a recorded kill cutoff and the token's
    /// `iat` is at/before that cutoff, the token is revoked. `tenant_id` and `iat`
    /// MUST come from the VALIDATED claim, never a request body. `iat == 0` (issue
    /// time unknown) or an empty `tenant_id` skips the tenant comparison so a
    /// tenant kill never spuriously denies a token whose age we can't establish —
    /// the per-jti and durable layers still apply. Both fast-path consultations
    /// (jti, then tenant) run BEFORE the durable `token_revocations` read; a miss
    /// or (in dev) a Redis outage falls through to that durable PG read
    /// (availability-first — Postgres stays authoritative).
    pub(super) async fn is_token_revoked(
        &self,
        jti: &str,
        tenant_id: &str,
        principal_id: &str,
        iat: u64,
    ) -> (bool, String) {
        if jti.trim().is_empty() {
            return (false, String::new());
        }
        // The keyed-HMAC digest keys both the cluster denylist and the DB row.
        let jti_hash = self.jti_hash(jti);
        // ── Fast path: consult the cluster denylist first ──────────────────
        #[cfg(feature = "redis")]
        if let Some(denylist) = self.jti_denylist.as_ref() {
            let fail_closed = crate::runtime::security::fail_closed_mode();
            // (a) Per-jti denylist.
            let decision = denylist.check(&jti_hash).await;
            if matches!(
                decision,
                crate::runtime::authn::revocation::DenylistDecision::Error
            ) {
                self.metrics.inc_revocation_lookup_failure();
                if fail_closed {
                    tracing::error!(
                        "jti denylist lookup failed; failing closed (treating token as revoked)"
                    );
                } else {
                    tracing::warn!(
                        "jti denylist lookup failed; falling through to durable DB read"
                    );
                }
            }
            if let Some(outcome) =
                crate::runtime::authn::revocation::denylist_check_outcome(decision, fail_closed)
            {
                // Denylist alone decided: a hit (revoked), or an error under
                // fail-closed. A miss / dev-mode error returns None → fall through.
                return outcome;
            }
            // (b) Tenant-level denylist. Skipped when the tenant or issue time is
            // unknown (iat == 0) so a tenant kill only acts on a token whose age
            // can actually be compared to the cutoff.
            if !tenant_id.trim().is_empty() && iat > 0 {
                let tenant_decision = denylist.tenant_denied_after(tenant_id).await;
                if matches!(
                    tenant_decision,
                    crate::runtime::authn::revocation::TenantDenylistDecision::Error
                ) {
                    self.metrics.inc_revocation_lookup_failure();
                    if fail_closed {
                        tracing::error!(
                            "tenant denylist lookup failed; failing closed (treating token as revoked)"
                        );
                    } else {
                        tracing::warn!(
                            "tenant denylist lookup failed; falling through to durable DB read"
                        );
                    }
                }
                if let Some(outcome) =
                    crate::runtime::authn::revocation::tenant_denylist_check_outcome(
                        tenant_decision,
                        iat,
                        fail_closed,
                    )
                {
                    // Tenant denylist alone decided: a cutoff >= iat (revoked), or
                    // an error under fail-closed. A miss, a strictly-newer token,
                    // or a dev-mode error returns None → durable DB read.
                    return outcome;
                }
            }
            // (c) Principal-level denylist (account hard-delete). Same cutoff-vs-iat
            // semantics as the tenant kill — reuses `tenant_denylist_check_outcome`.
            // Skipped when the principal or issue time is unknown.
            if !principal_id.trim().is_empty() && iat > 0 {
                let principal_decision = denylist.principal_denied_after(principal_id).await;
                if matches!(
                    principal_decision,
                    crate::runtime::authn::revocation::TenantDenylistDecision::Error
                ) {
                    self.metrics.inc_revocation_lookup_failure();
                    if fail_closed {
                        tracing::error!(
                            "principal denylist lookup failed; failing closed (treating token as revoked)"
                        );
                    } else {
                        tracing::warn!(
                            "principal denylist lookup failed; falling through to durable DB read"
                        );
                    }
                }
                if let Some(outcome) =
                    crate::runtime::authn::revocation::tenant_denylist_check_outcome(
                        principal_decision,
                        iat,
                        fail_closed,
                    )
                {
                    return outcome;
                }
            }
        }
        #[cfg(not(feature = "redis"))]
        let _ = (tenant_id, principal_id, iat);
        // ── Source of truth: durable `token_revocations` read ──────────────
        let Some(pool) = self.pg_pool.as_ref() else {
            return (false, String::new());
        };
        let m = revocation_model();
        let sql = format!(
            "SELECT COALESCE({reason}, '')::TEXT AS reason FROM {rel} WHERE {jti} = $1 LIMIT 1",
            reason = m.q("reason"),
            rel = m.relation,
            jti = m.q("jti_hash"),
        );
        match sqlx::query(&sql).bind(&jti_hash).fetch_optional(pool).await {
            Ok(Some(row)) => {
                let reason: String = row.try_get("reason").unwrap_or_default();
                (true, reason)
            }
            Ok(None) => (false, String::new()),
            Err(err) => {
                self.metrics.inc_revocation_lookup_failure();
                let fail_closed = crate::runtime::security::fail_closed_mode();
                if fail_closed {
                    tracing::error!(
                        error = %err,
                        "token revocation lookup failed; failing closed (treating token as revoked)"
                    );
                } else {
                    tracing::warn!(error = %err, "token revocation lookup failed; failing open");
                }
                crate::runtime::authn::revocation::revocation_lookup_error_outcome(fail_closed)
            }
        }
    }

    // ── Devices (I2.4) ──────────────────────────────────────────────────────

    /// P3 (bug_report.md): register (or refresh) the caller's device at login so
    /// the device lifecycle is reachable — without this NO RPC ever inserts a
    /// `devices` row, so ListDevices is always empty and the client can never get
    /// a `device_id` to pass to RevokeDevice. Idempotent per (user, fingerprint):
    /// returns the existing non-revoked device's id when the same fingerprint has
    /// logged in before (refreshing `last_seen_at`), else inserts a new row.
    /// `fingerprint` is the client `LoginRequest.device_id`; only its keyed-HMAC
    /// digest is stored. Best-effort: any failure returns `None` and never blocks
    /// login (so a missing/legacy `devices` table can't break auth).
    pub(super) async fn register_login_device(
        &self,
        user_id: &str,
        tenant_id: &str,
        project_id: &str,
        fingerprint: &str,
        device_name: &str,
        ip_raw: &str,
    ) -> Option<String> {
        if fingerprint.trim().is_empty() {
            return None;
        }
        let pool = self.require_pool().ok()?;
        let m = device_model();
        let fp_hash = self.device_fingerprint_hash(fingerprint);
        let ip_masked = mask_ip(ip_raw);
        let existing: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {id}::TEXT FROM {rel} WHERE {user} = $1 AND {fp} = $2 AND {revoked} IS NULL \
             ORDER BY {created} DESC LIMIT 1",
            id = m.q("device_id"),
            rel = m.relation,
            user = m.q("user_id"),
            fp = m.q("fingerprint_hash"),
            revoked = m.q("revoked_at"),
            created = m.q("created_at"),
        ))
        .bind(user_id)
        .bind(&fp_hash)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if let Some(id) = existing {
            let _ = sqlx::query(&format!(
                "UPDATE {rel} SET {seen} = NOW(), {ip} = $2 WHERE {id} = $1::UUID",
                rel = m.relation,
                seen = m.q("last_seen_at"),
                ip = m.q("last_ip_masked"),
                id = m.q("device_id"),
            ))
            .bind(&id)
            .bind(ip_masked)
            .execute(pool)
            .await;
            return Some(id);
        }
        let new_id = uuid::Uuid::new_v4().to_string();
        sqlx::query_scalar(&format!(
            "INSERT INTO {rel} ({id}, {user}, {tenant}, {project}, {name}, {fp}, {ip}, {seen}) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, NOW()) RETURNING {id}::TEXT",
            rel = m.relation,
            id = m.q("device_id"),
            user = m.q("user_id"),
            tenant = m.q("tenant_id"),
            project = m.q("project_id"),
            name = m.q("device_name"),
            fp = m.q("fingerprint_hash"),
            ip = m.q("last_ip_masked"),
            seen = m.q("last_seen_at"),
        ))
        .bind(&new_id)
        .bind(user_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(device_name)
        .bind(&fp_hash)
        .bind(ip_masked)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
    }

    pub(super) async fn list_devices_impl(
        &self,
        request: Request<authn_pb::ListDevicesRequest>,
    ) -> Result<Response<authn_pb::ListDevicesResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "user_id is required",
                [("user_id", "must be a non-empty user id")],
            ));
        }
        // D3: bind the target user to the validated bearer claim — a tenant-A token
        // cannot enumerate a tenant-B user's devices by passing that user's id.
        self.authorize_target_user(&req.user_id).await?;
        let pool = self.require_pool()?;
        let m = device_model();
        let (limit, offset, _) = bounded_page_window(req.page.as_ref());
        let sql = format!(
            "SELECT {id}::TEXT AS device_id, {user}::TEXT AS user_id, \
                    COALESCE({tenant}::TEXT,'') AS tenant_id, COALESCE({project}::TEXT,'') AS project_id, \
                    COALESCE({name}::TEXT,'') AS device_name, COALESCE({dtype}::TEXT,'') AS device_type, \
                    COALESCE({ip}::TEXT,'') AS last_ip_masked, \
                    {seen}, {created}, {revoked} AS revoked_at \
             FROM {rel} WHERE {user} = $1 AND {revoked} IS NULL \
             ORDER BY {created_col} DESC OFFSET $2 LIMIT $3",
            id = m.q("device_id"),
            user = m.q("user_id"),
            tenant = m.q("tenant_id"),
            project = m.q("project_id"),
            name = m.q("device_name"),
            dtype = m.q("device_type"),
            ip = m.q("last_ip_masked"),
            seen = m.timestamp_unix_as("last_seen_at", "last_seen_at"),
            created = m.timestamp_unix_as("created_at", "created_at"),
            // ORDER BY must use the BARE column, NOT the `{created}` projection — that
            // one ends in `AS "created_at"`, and an `AS alias` inside ORDER BY is a
            // syntax error ("at or near AS"). This is the §1 ListDevices bug.
            created_col = m.q("created_at"),
            revoked = m.q("revoked_at"),
            rel = m.relation,
        );
        let rows = sqlx::query(&sql)
            .bind(&req.user_id)
            .bind(offset as i64)
            .bind(limit as i64)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "list_devices_query",
                    format!("list devices failed: {err}"),
                )
            })?;
        let devices = rows
            .iter()
            .map(|row| authn_entity_pb::Device {
                device_id: row.try_get("device_id").unwrap_or_default(),
                user_id: row.try_get("user_id").unwrap_or_default(),
                tenant_id: row.try_get("tenant_id").unwrap_or_default(),
                project_id: row.try_get("project_id").unwrap_or_default(),
                device_name: row.try_get("device_name").unwrap_or_default(),
                device_type: authn_entity_pb::DeviceType::try_from(parse_device_type(
                    &row.try_get::<String, _>("device_type").unwrap_or_default(),
                ))
                .unwrap_or(authn_entity_pb::DeviceType::Web) as i32,
                // Fingerprint digest is STORAGE_ONLY: never surfaced through reads.
                fingerprint_hash: String::new(),
                last_ip_masked: row.try_get("last_ip_masked").unwrap_or_default(),
                last_user_agent_hash: String::new(),
                last_seen_at: timestamp_from_unix(
                    row.try_get::<i64, _>("last_seen_at").unwrap_or(0).max(0) as u64,
                ),
                created_at: timestamp_from_unix(
                    row.try_get::<i64, _>("created_at").unwrap_or(0).max(0) as u64,
                ),
                revoked_at: None,
                revoked_by: String::new(),
            })
            .collect();
        Ok(Response::new(authn_pb::ListDevicesResponse {
            devices,
            page: Some(bounded_page_response(rows.len(), req.page.as_ref())),
        }))
    }

    pub(super) async fn revoke_device_impl(
        &self,
        request: Request<authn_pb::RevokeDeviceRequest>,
    ) -> Result<Response<authn_pb::RevokeDeviceResponse>, Status> {
        let req = request.into_inner();
        // bug_report.md B1: validate device_id is a UUID at the boundary so a
        // malformed id never reaches the `WHERE {id} = $1::UUID` query (which
        // would leak a raw Postgres `22P02` error as Internal).
        Self::require_uuid_arg(&req.device_id, "device_id")?;
        let propagation_started = std::time::Instant::now();
        // D3: derive the authorized tenant from the VALIDATED bearer claim, never
        // from the request. A tenant-scoped caller may only revoke devices owned by
        // its own claim tenant; a genuine cross-tenant/platform admin may revoke any
        // device. The tenant predicate is pushed into the UPDATE/lookup so a
        // tenant-A token revoking a tenant-B device simply matches no row (fails
        // closed → not_found) rather than crossing the boundary. When no claim
        // context is installed (in-process / trusted caller, not over the wire) the
        // gate is bypassed — real transport requests always carry a context.
        let claim_ctx = crate::runtime::service::method_security::current_claim_context();
        let context_present = crate::runtime::service::method_security::claim_context_present();
        let cross_tenant_admin = !context_present || claim_ctx.is_cross_tenant_admin();
        let claim_tenant = claim_ctx.tenant_id.trim().to_string();
        if context_present && !cross_tenant_admin && claim_tenant.is_empty() {
            return Err(revoke_device_tenant_scope_required_status());
        }
        let pool = self.require_pool()?;
        let m = device_model();
        let actor = req
            .context
            .as_ref()
            .and_then(|c| {
                [
                    c.user_id.as_str(),
                    c.principal_id.as_str(),
                    c.service_identity.as_str(),
                ]
                .into_iter()
                .find(|value| !value.trim().is_empty())
                .map(str::to_string)
            })
            .unwrap_or_else(|| crate::runtime::otel::current_actor());
        let auth_method = crate::runtime::otel::current_auth_method();
        let trace = req
            .context
            .as_ref()
            .map(|c| {
                (
                    c.trace_id.clone(),
                    c.span_id.clone(),
                    c.ip_address.clone(),
                    c.user_agent.clone(),
                )
            })
            .unwrap_or_default();
        // Mark the device revoked and revoke every token family bound to it so
        // future refresh + session validation is blocked. D3: the tenant predicate
        // (`{tenant} = $3 OR $4`) binds the revoke to the caller's CLAIM tenant for
        // a non-admin — a cross-tenant device id matches no row → not_found.
        let sql = format!(
            "UPDATE {rel} SET {revoked} = NOW(), {by} = $2 WHERE {id} = $1::UUID AND {revoked} IS NULL \
             AND ({tenant} = $3 OR $4) \
             RETURNING {tenant}::TEXT AS tenant_id",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            by = m.q("revoked_by"),
            id = m.q("device_id"),
            tenant = m.q("tenant_id"),
        );
        let mut tx = pool.begin().await.map_err(|err| {
            lifecycle_internal_status(
                "revoke_device_tx_begin",
                format!("revoke device tx begin failed: {err}"),
            )
        })?;
        let row = sqlx::query(&sql)
            .bind(&req.device_id)
            .bind(&actor)
            .bind(&claim_tenant)
            .bind(cross_tenant_admin)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "revoke_device_update",
                    format!("revoke device failed: {err}"),
                )
            })?;
        let Some(row) = row else {
            return Err(super::authn_device_not_found_status());
        };
        let tenant_id: String = row.try_get("tenant_id").unwrap_or_default();
        let families = self
            .revoke_families_for_device(&mut *tx, &req.device_id, "device_revoked")
            .await?;
        // Atomicity (§7): the device-revoke UPDATE, the family revokes, and the
        // audit event all commit together — or the transaction rolls back.
        // Device revocation is a security-sensitive operation (I2.7): attach the
        // full compliance envelope (resolved actor, auth method, source IP, UA, and
        // trace/span correlation) so the audit record identifies WHO revoked the
        // device, HOW they authenticated, and from WHERE — the same provenance the
        // emergency-revoke path records.
        self.emit_event_in_tx(
            &mut *tx,
            AuthEvent::new(
                topics::DEVICE_REVOKED,
                req.device_id.clone(),
                tenant_id.clone(),
                serde_json::json!({
                    "device_id": req.device_id.clone(),
                    "reason": req.reason.clone(),
                    "families_revoked": families,
                }),
            )
            .with_correlation(format!("device-revoke:{}", req.device_id))
            .with_compliance(ComplianceEnvelope {
                actor,
                target_resource: format!("device:{}", req.device_id),
                target_tenant: tenant_id,
                operation: "device_revoke".to_string(),
                outcome: "success".to_string(),
                reason_code: if req.reason.trim().is_empty() {
                    "device_revoked".to_string()
                } else {
                    req.reason.clone()
                },
                auth_method,
                source_ip: trace.2,
                user_agent: trace.3,
                trace_id: trace.0,
                span_id: trace.1,
                ..ComplianceEnvelope::default()
            }),
        )
        .await?;
        tx.commit().await.map_err(|err| {
            lifecycle_internal_status(
                "revoke_device_commit",
                format!("revoke device commit failed: {err}"),
            )
        })?;
        self.metrics
            .observe_revocation_propagation_seconds(propagation_started.elapsed().as_secs_f64());
        Ok(Response::new(authn_pb::RevokeDeviceResponse {
            revoked: true,
            device_id: req.device_id,
            sessions_revoked: families as i64,
        }))
    }

    async fn revoke_families_for_device<'c, E>(
        &self,
        executor: E,
        device_id: &str,
        reason: &str,
    ) -> Result<u64, Status>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let m = token_family_model();
        let sql = format!(
            "UPDATE {rel} SET {revoked} = NOW(), {reason_col} = $2 WHERE {device} = $1 AND {revoked} IS NULL",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            reason_col = m.q("revocation_reason"),
            device = m.q("device_id"),
        );
        let res = sqlx::query(&sql)
            .bind(device_id)
            .bind(reason)
            .execute(executor)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "revoke_device_families",
                    format!("revoke device families failed: {err}"),
                )
            })?;
        Ok(res.rows_affected())
    }

    // ── Admin session revocation (I2.4) ─────────────────────────────────────

    pub(super) async fn admin_revoke_session_impl(
        &self,
        request: Request<authn_pb::AdminRevokeSessionRequest>,
    ) -> Result<Response<authn_pb::AdminRevokeSessionResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "user_id is required",
                [("user_id", "must be a non-empty user id")],
            ));
        }
        let propagation_started = std::time::Instant::now();
        // Revoking by user id revokes the user's sessions (the public session
        // handle is one-way, so a specific-session revoke targets the principal).
        let now = now_unix();
        let event = AuthEvent::new(
            topics::SESSION_REVOKED,
            format!("admin-revoke:{}", Uuid::new_v4().simple()),
            String::new(),
            serde_json::json!({ "user_id": req.user_id.clone(), "reason": req.reason.clone() }),
        );
        // Atomicity (§7): session revoke + family revoke + audit event commit together.
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            lifecycle_internal_status(
                "admin_revoke_session_tx_begin",
                format!("admin revoke tx begin failed: {err}"),
            )
        })?;
        let count = self
            .sessions
            .revoke_all_for_principal_in_tx(&mut *tx, &req.user_id, now)
            .await
            .map_err(|err| {
                lifecycle_internal_status("admin_revoke_session_store", err.to_string())
            })?;
        self.revoke_all_user_families_on(&mut *tx, &req.user_id, "admin_revoke")
            .await?;
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            lifecycle_internal_status(
                "admin_revoke_session_commit",
                format!("admin revoke commit failed: {err}"),
            )
        })?;
        self.metrics
            .observe_revocation_propagation_seconds(propagation_started.elapsed().as_secs_f64());
        Ok(Response::new(authn_pb::AdminRevokeSessionResponse {
            revoked: count > 0,
            sessions_revoked: count as i64,
        }))
    }

    pub(super) async fn admin_revoke_all_user_sessions_impl(
        &self,
        request: Request<authn_pb::AdminRevokeAllUserSessionsRequest>,
    ) -> Result<Response<authn_pb::AdminRevokeAllUserSessionsResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "user_id is required",
                [("user_id", "must be a non-empty user id")],
            ));
        }
        let propagation_started = std::time::Instant::now();
        let now = now_unix();
        let event = AuthEvent::new(
            topics::SESSION_REVOKED,
            format!("admin-revoke-all:{}", Uuid::new_v4().simple()),
            String::new(),
            serde_json::json!({ "user_id": req.user_id.clone(), "reason": req.reason.clone() }),
        );
        // Atomicity (§7): session revoke + family revoke + audit event commit together.
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            lifecycle_internal_status(
                "admin_revoke_all_sessions_tx_begin",
                format!("admin revoke-all tx begin failed: {err}"),
            )
        })?;
        let count = self
            .sessions
            .revoke_all_for_principal_in_tx(&mut *tx, &req.user_id, now)
            .await
            .map_err(|err| {
                lifecycle_internal_status("admin_revoke_all_sessions_store", err.to_string())
            })?;
        self.revoke_all_user_families_on(&mut *tx, &req.user_id, "admin_revoke_all")
            .await?;
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            lifecycle_internal_status(
                "admin_revoke_all_sessions_commit",
                format!("admin revoke-all commit failed: {err}"),
            )
        })?;
        self.metrics
            .observe_revocation_propagation_seconds(propagation_started.elapsed().as_secs_f64());
        Ok(Response::new(
            authn_pb::AdminRevokeAllUserSessionsResponse {
                sessions_revoked: count as i64,
            },
        ))
    }

    pub(super) async fn admin_revoke_all_tenant_sessions_impl(
        &self,
        request: Request<authn_pb::AdminRevokeAllTenantSessionsRequest>,
    ) -> Result<Response<authn_pb::AdminRevokeAllTenantSessionsResponse>, Status> {
        let req = request.into_inner();
        if req.tenant_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "tenant_id is required",
                [("tenant_id", "must be a non-empty tenant id")],
            ));
        }
        let propagation_started = std::time::Instant::now();
        // D3: the bulk tenant revoke targets `req.tenant_id`. Without this guard a
        // tenant-A admin token could revoke tenant B simply by setting body
        // `tenant_id=B`. Bind the target to the VALIDATED bearer tenant: a
        // tenant-scoped caller may only revoke its own tenant; only a genuine
        // cross-tenant/platform admin may target an arbitrary tenant. Fails closed.
        let claim_ctx = crate::runtime::service::method_security::current_claim_context();
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &claim_ctx,
            &req.tenant_id,
            "",
        )?;
        let pool = self.require_pool()?;
        // Bulk-revoke sessions + token families for the tenant in two scoped
        // UPDATEs (proto-driven relations).
        let session_m = native_model(
            "udb.core.authn.entity.v1.Session",
            &["tenant_id", "is_active", "revoked_by", "revoke_reason"],
        );
        let session_sql = format!(
            "UPDATE {rel} SET {active} = FALSE, {reason} = 'admin_revoke_tenant' \
             WHERE {tenant} = $1 AND {active} = TRUE",
            rel = session_m.relation,
            active = session_m.q("is_active"),
            reason = session_m.q("revoke_reason"),
            tenant = session_m.q("tenant_id"),
        );
        let fam = token_family_model();
        let fam_sql = format!(
            "UPDATE {rel} SET {revoked} = NOW(), {reason} = 'admin_revoke_tenant' \
             WHERE {tenant} = $1 AND {revoked} IS NULL",
            rel = fam.relation,
            revoked = fam.q("revoked_at"),
            reason = fam.q("revocation_reason"),
            tenant = fam.q("tenant_id"),
        );
        let event = AuthEvent::new(
            topics::SESSION_REVOKED,
            format!("admin-revoke-tenant:{}", Uuid::new_v4().simple()),
            req.tenant_id.clone(),
            serde_json::json!({ "tenant_id": req.tenant_id.clone(), "reason": req.reason.clone() }),
        );
        // Atomicity (§7): both bulk revokes and the audit event commit together —
        // either all land or the transaction rolls back.
        let mut tx = pool.begin().await.map_err(|err| {
            lifecycle_internal_status(
                "revoke_tenant_tx_begin",
                format!("revoke tenant tx begin failed: {err}"),
            )
        })?;
        let sessions = sqlx::query(&session_sql)
            .bind(&req.tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "revoke_tenant_sessions",
                    format!("revoke tenant sessions failed: {err}"),
                )
            })?
            .rows_affected();
        sqlx::query(&fam_sql)
            .bind(&req.tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "revoke_tenant_families",
                    format!("revoke tenant families failed: {err}"),
                )
            })?;
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            lifecycle_internal_status(
                "revoke_tenant_commit",
                format!("revoke tenant commit failed: {err}"),
            )
        })?;
        // Cluster-wide fast-path kill: the durable bulk-revoke above is the source
        // of truth; publishing the tenant cutoff to the denylist propagates the
        // kill to every replica's hot-path token check immediately (a token with
        // `iat <= now` is denied, mirroring the `tenant_denied_after` check in
        // `is_token_revoked`). Best-effort / availability-first — a denylist error
        // must NEVER fail the revoke that already durably committed.
        #[cfg(feature = "redis")]
        if let Some(denylist) = self.jti_denylist.as_ref() {
            let cutoff_unix = now_unix();
            match denylist
                .deny_tenant_after(&req.tenant_id, cutoff_unix)
                .await
            {
                Ok(()) => {
                    // The cutoff is inclusive and JWT `iat` has seconds
                    // precision. Begin the completion barrier only after Redis
                    // acknowledged the cutoff, so a login begun after this RPC
                    // returns is provably issued with `iat > cutoff_unix`.
                    wait_until_after_tenant_revoke_cutoff(cutoff_unix).await;
                }
                Err(err) => {
                    tracing::warn!(
                        tenant_id = %req.tenant_id,
                        error = %err,
                        "tenant denylist cutoff publish failed after durable tenant revoke \
                         (availability-first; durable revoke already committed)"
                    );
                }
            }
        }
        self.metrics
            .observe_revocation_propagation_seconds(propagation_started.elapsed().as_secs_f64());
        Ok(Response::new(
            authn_pb::AdminRevokeAllTenantSessionsResponse {
                sessions_revoked: sessions as i64,
            },
        ))
    }

    /// `revoke_all_user_families` over any executor — `pool` for a standalone
    /// revoke, or a `&mut PgConnection` so it commits in the caller's transaction.
    async fn revoke_all_user_families_on<'c, E>(
        &self,
        executor: E,
        user_id: &str,
        reason: &str,
    ) -> Result<u64, Status>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let m = token_family_model();
        let sql = format!(
            "UPDATE {rel} SET {revoked} = NOW(), {reason_col} = $2 \
             WHERE ({user} = $1 OR {principal} = $1) AND {revoked} IS NULL",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            reason_col = m.q("revocation_reason"),
            user = m.q("user_id"),
            principal = m.q("principal_id"),
        );
        let res = sqlx::query(&sql)
            .bind(user_id)
            .bind(reason)
            .execute(executor)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "revoke_user_families",
                    format!("revoke user families failed: {err}"),
                )
            })?;
        Ok(res.rows_affected())
    }

    pub(super) async fn emergency_revoke_impl(
        &self,
        request: Request<authn_pb::EmergencyRevokeRequest>,
    ) -> Result<Response<authn_pb::EmergencyRevokeResponse>, Status> {
        let req = request.into_inner();
        let has_selector = !req.signing_key_id.trim().is_empty()
            || !req.token_family_id.trim().is_empty()
            || !req.tenant_id.trim().is_empty()
            || !req.principal_id.trim().is_empty();
        if !has_selector {
            return Err(lifecycle_invalid_fields(
                "at least one selector is required (signing_key_id/token_family_id/tenant_id/principal_id)",
                [(
                    "selectors",
                    "must include at least one of signing_key_id, token_family_id, tenant_id, or principal_id",
                )],
            ));
        }
        let propagation_started = std::time::Instant::now();
        let actor = req
            .context
            .as_ref()
            .and_then(|c| {
                [
                    c.user_id.as_str(),
                    c.principal_id.as_str(),
                    c.service_identity.as_str(),
                ]
                .into_iter()
                .find(|value| !value.trim().is_empty())
                .map(str::to_string)
            })
            .unwrap_or_else(|| crate::runtime::otel::current_actor());
        let auth_method = crate::runtime::otel::current_auth_method();
        let trace = req
            .context
            .as_ref()
            .map(|c| {
                (
                    c.trace_id.clone(),
                    c.span_id.clone(),
                    c.ip_address.clone(),
                    c.user_agent.clone(),
                )
            })
            .unwrap_or_default();
        let now = now_unix();
        let mut families_revoked = 0u64;
        let mut sessions_revoked = 0u64;
        let mut keys_compromised = 0u64;

        // Atomicity (§7): every durable emergency mutation (signing-key compromise,
        // family revokes, principal session+family revokes, and the tenant-wide
        // bulk revoke) plus the emergency audit/outbox event commit in ONE
        // transaction — a partial break-glass that is not fully durable, or whose
        // audit event cannot be written, rolls the whole operation back rather than
        // leaving the system in a half-revoked state without its event.
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            lifecycle_internal_status(
                "emergency_revoke_tx_begin",
                format!("emergency revoke tx begin failed: {err}"),
            )
        })?;

        if !req.signing_key_id.trim().is_empty() {
            keys_compromised = self
                .compromise_signing_key_on(&mut *tx, req.signing_key_id.trim(), &actor)
                .await?;
        }
        if !req.token_family_id.trim().is_empty() {
            families_revoked += self
                .revoke_family_by_id_on(&mut *tx, req.token_family_id.trim(), "emergency_revoke")
                .await?;
        }
        if !req.principal_id.trim().is_empty() {
            families_revoked += self
                .revoke_all_user_families_on(&mut *tx, req.principal_id.trim(), "emergency_revoke")
                .await?;
            sessions_revoked += self
                .sessions
                .revoke_all_for_principal_in_tx(&mut *tx, req.principal_id.trim(), now)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "emergency_revoke_principal_sessions",
                        err.to_string(),
                    )
                })? as u64;
        }
        if !req.tenant_id.trim().is_empty() {
            // Inline the tenant-wide bulk revoke (mirrors
            // `admin_revoke_all_tenant_sessions_impl`) onto THIS emergency tx so the
            // whole break-glass is one atomic unit (rather than recursing into a
            // handler with its own independent transaction).
            let session_m = native_model(
                "udb.core.authn.entity.v1.Session",
                &["tenant_id", "is_active", "revoked_by", "revoke_reason"],
            );
            let session_sql = format!(
                "UPDATE {rel} SET {active} = FALSE, {reason} = 'emergency_revoke_tenant' \
                 WHERE {tenant} = $1 AND {active} = TRUE",
                rel = session_m.relation,
                active = session_m.q("is_active"),
                reason = session_m.q("revoke_reason"),
                tenant = session_m.q("tenant_id"),
            );
            let fam = token_family_model();
            let fam_sql = format!(
                "UPDATE {rel} SET {revoked} = NOW(), {reason} = 'emergency_revoke_tenant' \
                 WHERE {tenant} = $1 AND {revoked} IS NULL",
                rel = fam.relation,
                revoked = fam.q("revoked_at"),
                reason = fam.q("revocation_reason"),
                tenant = fam.q("tenant_id"),
            );
            let tenant_sessions = sqlx::query(&session_sql)
                .bind(req.tenant_id.trim())
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "emergency_revoke_tenant_sessions",
                        format!("emergency revoke tenant sessions failed: {err}"),
                    )
                })?
                .rows_affected();
            let tenant_families = sqlx::query(&fam_sql)
                .bind(req.tenant_id.trim())
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "emergency_revoke_tenant_families",
                        format!("emergency revoke tenant families failed: {err}"),
                    )
                })?
                .rows_affected();
            sessions_revoked += tenant_sessions;
            families_revoked += tenant_families;
        }

        let operation_id = Uuid::new_v4().to_string();
        let reason_code = if req.reason.trim().is_empty() {
            "emergency_revoke".to_string()
        } else {
            req.reason.clone()
        };
        // Shared compliance envelope for the umbrella event and every operation-
        // plane facet event below — same actor/trace/outcome, only the
        // operation/target/reason differ. Cloned per-event so trace material is
        // reused (it is consumed by the final umbrella event).
        let ops_envelope =
            |operation: &str, target_resource: String, reason: &str| ComplianceEnvelope {
                actor: actor.clone(),
                actor_project: String::new(),
                target_resource,
                target_tenant: req.tenant_id.clone(),
                target_project: String::new(),
                operation: operation.to_string(),
                outcome: "success".to_string(),
                reason_code: reason.to_string(),
                auth_method: auth_method.clone(),
                source_ip: trace.2.clone(),
                user_agent: trace.3.clone(),
                trace_id: trace.0.clone(),
                span_id: trace.1.clone(),
                ..ComplianceEnvelope::default()
            };

        // ── Operation-plane facet events ────────────────────────────────────────
        // The break-glass umbrella event (OPS_EMERGENCY_REVOKE) records the whole
        // operation; these facets name the specific operations that actually fired
        // so the ops/audit stream carries the distinct, individually-subscribable
        // signals. Emitted in the SAME tx as the mutations (§7 atomicity).

        // A compromised signing key was rotated OUT of the signing registry — that
        // is a key-rotation event (the registry's ACTIVE/VERIFYING set changes, and
        // tokens signed by the compromised key stop verifying). Emit both the
        // ops-plane rotation signal and the authn signing-key-rotated event.
        if keys_compromised > 0 {
            let key_target = format!("signing_key:{}", req.signing_key_id.trim());
            self.emit_event_in_tx(
                &mut *tx,
                AuthEvent::new(
                    topics::OPS_KEY_ROTATION,
                    operation_id.clone(),
                    req.tenant_id.clone(),
                    serde_json::json!({
                        "operation_id": operation_id,
                        "signing_key_id": req.signing_key_id.clone(),
                        "keys_compromised": keys_compromised,
                        "trigger": "emergency_revoke",
                    }),
                )
                .with_correlation(operation_id.clone())
                .with_compliance(ops_envelope(
                    "key_rotation",
                    key_target.clone(),
                    "signing_key_compromised",
                )),
            )
            .await?;
            self.emit_event_in_tx(
                &mut *tx,
                AuthEvent::new(
                    topics::SIGNING_KEY_ROTATED,
                    req.signing_key_id.trim().to_string(),
                    req.tenant_id.clone(),
                    serde_json::json!({
                        "signing_key_id": req.signing_key_id.clone(),
                        "new_state": "compromised",
                        "rotation_reason": "emergency_revoke",
                    }),
                )
                .with_correlation(operation_id.clone())
                .with_compliance(ops_envelope(
                    "signing_key_rotated",
                    key_target,
                    "signing_key_compromised",
                )),
            )
            .await?;
        }

        // A tenant-wide revoke denied every active session + token family in the
        // tenant — the tenant is effectively suspended (no live credentials remain
        // until re-provisioned). Emit the tenant-suspension lifecycle event AND the
        // fleet-scope emergency deny-all signal (a tenant is the broadest blast
        // radius this RPC supports — "deny everything for this tenant").
        if !req.tenant_id.trim().is_empty() {
            let tenant_target = format!("tenant:{}", req.tenant_id.trim());
            self.emit_event_in_tx(
                &mut *tx,
                AuthEvent::new(
                    topics::OPS_TENANT_SUSPENDED,
                    operation_id.clone(),
                    req.tenant_id.clone(),
                    serde_json::json!({
                        "operation_id": operation_id,
                        "tenant_id": req.tenant_id.clone(),
                        "sessions_revoked": sessions_revoked,
                        "families_revoked": families_revoked,
                        "trigger": "emergency_revoke",
                    }),
                )
                .with_correlation(operation_id.clone())
                .with_compliance(ops_envelope(
                    "tenant_suspended",
                    tenant_target.clone(),
                    &reason_code,
                )),
            )
            .await?;
            self.emit_event_in_tx(
                &mut *tx,
                AuthEvent::new(
                    topics::OPS_EMERGENCY_DENY_ALL,
                    operation_id.clone(),
                    req.tenant_id.clone(),
                    serde_json::json!({
                        "operation_id": operation_id,
                        "tenant_id": req.tenant_id.clone(),
                        "sessions_revoked": sessions_revoked,
                        "families_revoked": families_revoked,
                        "scope": "tenant",
                    }),
                )
                .with_correlation(operation_id.clone())
                .with_compliance(ops_envelope(
                    "emergency_deny_all",
                    tenant_target,
                    &reason_code,
                )),
            )
            .await?;
        }

        let event = AuthEvent::new(
            topics::OPS_EMERGENCY_REVOKE,
            operation_id.clone(),
            req.tenant_id.clone(),
            serde_json::json!({
                "operation_id": operation_id,
                "signing_key_id": req.signing_key_id.clone(),
                "token_family_id": req.token_family_id.clone(),
                "tenant_id": req.tenant_id.clone(),
                "principal_id": req.principal_id.clone(),
                "reason": req.reason.clone(),
                "families_revoked": families_revoked,
                "sessions_revoked": sessions_revoked,
                "keys_compromised": keys_compromised,
            }),
        )
        .with_correlation(operation_id.clone())
        .with_compliance(ops_envelope(
            "emergency_revoke",
            if req.tenant_id.trim().is_empty() {
                req.principal_id.clone()
            } else {
                format!("tenant:{}", req.tenant_id)
            },
            &reason_code,
        ));
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            lifecycle_internal_status(
                "emergency_revoke_commit",
                format!("emergency revoke commit failed: {err}"),
            )
        })?;
        self.metrics
            .observe_revocation_propagation_seconds(propagation_started.elapsed().as_secs_f64());

        Ok(Response::new(authn_pb::EmergencyRevokeResponse {
            families_revoked: families_revoked as i64,
            sessions_revoked: sessions_revoked as i64,
            keys_compromised: keys_compromised as i64,
            operation_id,
        }))
    }

    /// `revoke_family_by_id` over any executor — `pool` for a standalone revoke,
    /// or a `&mut PgConnection` so it commits in the caller's transaction (used
    /// by `emergency_revoke_impl` for §7 atomicity).
    async fn revoke_family_by_id_on<'c, E>(
        &self,
        executor: E,
        family_id: &str,
        reason: &str,
    ) -> Result<u64, Status>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let m = token_family_model();
        let sql = format!(
            "UPDATE {rel} SET {revoked} = NOW(), {reason_col} = $2 WHERE {id} = $1::UUID AND {revoked} IS NULL",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            reason_col = m.q("revocation_reason"),
            id = m.q("family_id"),
        );
        let res = sqlx::query(&sql)
            .bind(family_id)
            .bind(reason)
            .execute(executor)
            .await
            .map_err(|err| {
                lifecycle_internal_status("revoke_family", format!("revoke family failed: {err}"))
            })?;
        Ok(res.rows_affected())
    }

    // ── MFA challenge lifecycle (I2.6) ──────────────────────────────────────

    pub(super) async fn issue_mfa_challenge_impl(
        &self,
        request: Request<authn_pb::IssueMfaChallengeRequest>,
    ) -> Result<Response<authn_pb::IssueMfaChallengeResponse>, Status> {
        let req = request.into_inner();
        // A9: bind the target user_id to the validated claim (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                lifecycle_internal_status("issue_mfa_challenge_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        let pool = self.require_pool()?;
        let m = mfa_challenge_model();
        let factor = if req.factor_kind == 0 {
            authn_entity_pb::AuthFactorKind::Totp as i32
        } else {
            req.factor_kind
        };
        let purpose = if req.purpose == 0 {
            authn_entity_pb::MfaChallengePurpose::LoginStepUp as i32
        } else {
            req.purpose
        };
        let ttl = std::env::var("UDB_MFA_CHALLENGE_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|t| *t > 0)
            .unwrap_or(300);
        let now = now_unix();
        let expires = now.saturating_add(ttl);
        let challenge_id = Uuid::new_v4().to_string();
        let fp_hash = if req.device_fingerprint.trim().is_empty() {
            String::new()
        } else {
            self.device_fingerprint_hash(&req.device_fingerprint)
        };
        if let Ok(runtime) = self.authn_runtime() {
            let expires_at = unix_to_utc(expires).ok_or_else(|| {
                lifecycle_internal_status(
                    "issue_mfa_challenge_expiry",
                    "invalid MFA challenge expiry",
                )
            })?;
            let context = self.authn_context(&user.tenant_id, &user.project_id);
            let record = authn_record([
                ("challenge_id", LogicalValue::String(challenge_id.clone())),
                ("user_id", LogicalValue::String(user.user_id.clone())),
                ("tenant_id", LogicalValue::String(user.tenant_id.clone())),
                ("project_id", LogicalValue::String(user.project_id.clone())),
                ("factor_kind", LogicalValue::String(auth_factor_db(factor))),
                ("purpose", LogicalValue::String(mfa_purpose_db(purpose))),
                (
                    "device_fingerprint_hash",
                    LogicalValue::String(fp_hash.clone()),
                ),
                (
                    "ip_address_masked",
                    LogicalValue::String(mask_ip(&req.ip_address)),
                ),
                ("expires_at", LogicalValue::Timestamp(expires_at)),
            ]);
            runtime
                .native_entity_write_for_service(
                    "authn",
                    &context,
                    "udb.core.authn.entity.v1.MfaChallenge",
                    record,
                    crate::ir::ConflictStrategy::Error,
                )
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "issue_mfa_challenge_runtime_write",
                        format!("issue MFA challenge failed: {err}"),
                    )
                })?;
            return Ok(Response::new(authn_pb::IssueMfaChallengeResponse {
                challenge_id,
                expires_at_unix: expires as i64,
                factor_kind: factor,
            }));
        }
        let sql = format!(
            "INSERT INTO {rel} ({id}, {user}, {tenant}, {project}, {factor}, {purpose}, {fp}, {ip}, {expires}) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8, to_timestamp($9::DOUBLE PRECISION))",
            rel = m.relation,
            id = m.q("challenge_id"),
            user = m.q("user_id"),
            tenant = m.q("tenant_id"),
            project = m.q("project_id"),
            factor = m.q("factor_kind"),
            purpose = m.q("purpose"),
            fp = m.q("device_fingerprint_hash"),
            ip = m.q("ip_address_masked"),
            expires = m.q("expires_at"),
        );
        sqlx::query(&sql)
            .bind(&challenge_id)
            .bind(&user.user_id)
            .bind(&user.tenant_id)
            .bind(&user.project_id)
            .bind(auth_factor_db(factor))
            .bind(mfa_purpose_db(purpose))
            .bind(&fp_hash)
            .bind(mask_ip(&req.ip_address))
            .bind(expires as f64)
            .execute(pool)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "issue_mfa_challenge_pg_insert",
                    format!("issue MFA challenge failed: {err}"),
                )
            })?;
        Ok(Response::new(authn_pb::IssueMfaChallengeResponse {
            challenge_id,
            expires_at_unix: expires as i64,
            factor_kind: factor,
        }))
    }

    pub(super) async fn verify_mfa_challenge_impl(
        &self,
        request: Request<authn_pb::VerifyMfaChallengeRequest>,
    ) -> Result<Response<authn_pb::VerifyMfaChallengeResponse>, Status> {
        let req = request.into_inner();
        // bug_report.md B1/B4: validate the id is a UUID at the boundary so a
        // malformed challenge_id never reaches native dispatch / the
        // `WHERE {id} = $1::UUID` query (whose inner InvalidArgument was being
        // re-wrapped as Internal).
        Self::require_uuid_arg(&req.challenge_id, "challenge_id")?;
        let context = self.request_authn_context(req.context.as_ref())?;
        if let Ok(runtime) = self.authn_runtime() {
            let now = unix_to_utc(now_unix()).ok_or_else(|| {
                lifecycle_internal_status(
                    "verify_mfa_challenge_time",
                    "invalid MFA verification time",
                )
            })?;
            let mut assignments = std::collections::BTreeMap::new();
            assignments.insert("consumed_at".to_string(), LogicalAssignment::ServerNow);
            assignments.insert(
                "attempt_count".to_string(),
                LogicalAssignment::Increment {
                    by: LogicalValue::Int(1),
                },
            );
            let op = LogicalUpdate {
                message_type: "udb.core.authn.entity.v1.MfaChallenge".to_string(),
                filter: authn_and(vec![
                    authn_eq(
                        "challenge_id",
                        LogicalValue::String(req.challenge_id.clone()),
                    ),
                    LogicalFilter::IsNull("consumed_at".to_string()),
                    authn_cmp("expires_at", ComparisonOp::Gt, LogicalValue::Timestamp(now)),
                    authn_cmp("attempt_count", ComparisonOp::Lt, LogicalValue::Int(5)),
                ]),
                assignments,
                return_fields: vec![
                    "user_id".to_string(),
                    "factor_kind".to_string(),
                    "device_fingerprint_hash".to_string(),
                ],
                require_affected: false,
            };
            let (_, rows) = runtime
                .native_entity_update_for_service("authn", &context, op)
                .await
                .map_err(|err| {
                    crate::runtime::executor_utils::prefix_status(
                        "verify MFA challenge failed",
                        err,
                    )
                })?;
            let Some(row) = rows.first() else {
                return Ok(Response::new(authn_pb::VerifyMfaChallengeResponse {
                    verified: false,
                    user_id: String::new(),
                }));
            };
            let user_id = row
                .get("user_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let fp_stored = row
                .get("device_fingerprint_hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            if !fp_stored.is_empty() {
                let presented = if req.device_fingerprint.trim().is_empty() {
                    String::new()
                } else {
                    self.device_fingerprint_hash(&req.device_fingerprint)
                };
                if presented != fp_stored {
                    return Ok(Response::new(authn_pb::VerifyMfaChallengeResponse {
                        verified: false,
                        user_id: String::new(),
                    }));
                }
            }
            let factor_db = row
                .get("factor_kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let verified = self
                .verify_mfa_proof(&user_id, &factor_db, &req.code, now_unix())
                .await?;
            return Ok(Response::new(authn_pb::VerifyMfaChallengeResponse {
                verified,
                user_id: if verified { user_id } else { String::new() },
            }));
        }
        let pool = self.require_pool()?;
        let m = mfa_challenge_model();
        // Atomically consume the challenge: the UPDATE matches only an unexpired,
        // unconsumed, under-attempt-limit row and stamps consumed_at in the same
        // statement, so a replay or concurrent verify loses the race (single-use).
        let consume_sql = format!(
            "UPDATE {rel} SET {consumed} = NOW(), {attempts} = {attempts} + 1 \
             WHERE {id} = $1::UUID AND {consumed} IS NULL AND {expires} > NOW() AND {attempts} < 5 \
             RETURNING {user}::TEXT AS user_id, {factor}::TEXT AS factor_kind, \
                       COALESCE({fp}::TEXT,'') AS fp",
            rel = m.relation,
            consumed = m.q("consumed_at"),
            attempts = m.q("attempt_count"),
            id = m.q("challenge_id"),
            expires = m.q("expires_at"),
            user = m.q("user_id"),
            factor = m.q("factor_kind"),
            fp = m.q("device_fingerprint_hash"),
        );
        let Some(row) = sqlx::query(&consume_sql)
            .bind(&req.challenge_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "verify MFA challenge failed",
                    &err,
                )
            })?
        else {
            // Either expired, already consumed (replay), or over the attempt cap.
            return Ok(Response::new(authn_pb::VerifyMfaChallengeResponse {
                verified: false,
                user_id: String::new(),
            }));
        };
        let user_id: String = row.try_get("user_id").unwrap_or_default();
        let fp_stored: String = row.try_get("fp").unwrap_or_default();
        // Device binding: when a fingerprint was bound at issuance, the verifier
        // must present the same device.
        if !fp_stored.is_empty() {
            let presented = if req.device_fingerprint.trim().is_empty() {
                String::new()
            } else {
                self.device_fingerprint_hash(&req.device_fingerprint)
            };
            if presented != fp_stored {
                return Ok(Response::new(authn_pb::VerifyMfaChallengeResponse {
                    verified: false,
                    user_id: String::new(),
                }));
            }
        }
        let factor_db: String = row.try_get("factor_kind").unwrap_or_default();
        // Verify the proof against the user's factor. TOTP and recovery codes are
        // checked against the stored secret / code hashes (reusing existing
        // helpers); email/SMS OTP challenges verify against the OTP store via code.
        let now = now_unix();
        let verified = self
            .verify_mfa_proof(&user_id, &factor_db, &req.code, now)
            .await?;
        Ok(Response::new(authn_pb::VerifyMfaChallengeResponse {
            verified,
            user_id: if verified { user_id } else { String::new() },
        }))
    }

    async fn verify_mfa_proof(
        &self,
        user_id: &str,
        factor_db: &str,
        code: &str,
        now: u64,
    ) -> Result<bool, Status> {
        let user = self
            .users
            .get_user_by_id(user_id)
            .await
            .map_err(|err| {
                lifecycle_internal_status("verify_mfa_proof_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        let upper = factor_db.to_ascii_uppercase();
        if upper.contains("TOTP") {
            let ok = authn::totp::decrypt_secret(&user.totp_secret_hash, &self.otp_hash_key()?)
                .map(|secret| authn::totp::verify(&secret, code, now))
                .unwrap_or(false);
            return Ok(ok);
        }
        if upper.contains("RECOVERY") {
            let hash = authn::hash_recovery_code(code, &self.otp_hash_key()?);
            return self
                .users
                .consume_recovery_code(user_id, &hash, now)
                .await
                .map_err(|err| {
                    lifecycle_internal_status("verify_mfa_recovery_code", err.to_string())
                });
        }
        // For OTP-style factors the `code` is treated as a previously-issued OTP
        // id+code is out of band; without that we cannot verify here.
        Ok(false)
    }

    // ── MFA factor management (I2.6) ────────────────────────────────────────

    pub(super) async fn list_mfa_factors_impl(
        &self,
        request: Request<authn_pb::ListMfaFactorsRequest>,
    ) -> Result<Response<authn_pb::ListMfaFactorsResponse>, Status> {
        let req = request.into_inner();
        // A8: bind the target user_id to the validated claim (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                lifecycle_internal_status("list_mfa_factors_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        let mut factors = Vec::new();
        factors.push(authn_pb::MfaFactorSummary {
            factor_kind: authn_entity_pb::AuthFactorKind::Totp as i32,
            enabled: !user.totp_secret_hash.is_empty() && user.mfa_enabled,
            label: "Authenticator app".to_string(),
        });
        // WebAuthn factor presence (best-effort count of registered credentials).
        let passkeys = self.count_webauthn_credentials(&user.user_id).await;
        factors.push(authn_pb::MfaFactorSummary {
            factor_kind: authn_entity_pb::AuthFactorKind::Webauthn as i32,
            enabled: passkeys > 0,
            label: format!("{passkeys} passkey(s)"),
        });
        let page_window = native_offset_page_window(1, req.page_size, &req.page_token, 50);
        let total = factors.len() as i64;
        let factors = factors
            .into_iter()
            .skip(page_window.offset)
            .take(page_window.limit)
            .collect();
        Ok(Response::new(authn_pb::ListMfaFactorsResponse {
            factors,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
        }))
    }

    pub(super) async fn disable_mfa_factor_impl(
        &self,
        request: Request<authn_pb::DisableMfaFactorRequest>,
    ) -> Result<Response<authn_pb::DisableMfaFactorResponse>, Status> {
        let req = request.into_inner();
        // A5: bind the target user_id to the validated claim — a bearer must not
        // strip another tenant's user's second factor (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                lifecycle_internal_status("disable_mfa_factor_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        let now = now_unix();
        if req.factor_kind == authn_entity_pb::AuthFactorKind::Totp as i32 {
            user.totp_secret_hash = String::new();
            user.mfa_enabled = false;
            user.updated_at_unix = now;
            self.users.put_user(user.clone()).await.map_err(|err| {
                lifecycle_internal_status("disable_mfa_factor_store", err.to_string())
            })?;
        } else if req.factor_kind == authn_entity_pb::AuthFactorKind::Webauthn as i32 {
            self.delete_all_webauthn_credentials(&req.user_id).await?;
        }
        self.emit_event(
            AuthEvent::new(
                topics::MFA_FACTOR_DISABLED,
                req.user_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({ "user_id": req.user_id.clone(), "factor_kind": req.factor_kind }),
            )
            .with_correlation(format!("mfa_disable:{}", req.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: req.user_id.clone(),
                target_resource: req.user_id.clone(),
                operation: "mfa_disable".to_string(),
                outcome: "success".to_string(),
                reason_code: "factor_disabled".to_string(),
                auth_method: "mfa".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::DisableMfaFactorResponse {
            disabled: true,
        }))
    }

    pub(super) async fn revoke_recovery_codes_impl(
        &self,
        request: Request<authn_pb::RevokeRecoveryCodesRequest>,
    ) -> Result<Response<authn_pb::RevokeRecoveryCodesResponse>, Status> {
        let req = request.into_inner();
        // A7: bind the target user_id to the validated claim (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                lifecycle_internal_status("revoke_recovery_codes_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        // Replace with an empty set → all prior codes invalidated.
        self.users
            .replace_recovery_codes(&req.user_id, &user.tenant_id, &[])
            .await
            .map_err(|err| {
                lifecycle_internal_status("revoke_recovery_codes_replace", err.to_string())
            })?;
        Ok(Response::new(authn_pb::RevokeRecoveryCodesResponse {
            revoked_count: 0,
        }))
    }

    pub(super) async fn admin_reset_mfa_impl(
        &self,
        request: Request<authn_pb::AdminResetMfaRequest>,
    ) -> Result<Response<authn_pb::AdminResetMfaResponse>, Status> {
        let req = request.into_inner();
        // A5: bind the target user_id to the validated claim — cross-tenant admin
        // reset only for a genuine cross-tenant admin (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| lifecycle_internal_status("admin_reset_mfa_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        let now = now_unix();
        // Clear all factors: TOTP secret, recovery codes, WebAuthn credentials.
        user.totp_secret_hash = String::new();
        user.mfa_enabled = false;
        user.updated_at_unix = now;
        let tenant = user.tenant_id.clone();
        self.users
            .put_user(user)
            .await
            .map_err(|err| lifecycle_internal_status("admin_reset_mfa_store", err.to_string()))?;
        let _ = self
            .users
            .replace_recovery_codes(&req.user_id, &tenant, &[])
            .await;
        self.delete_all_webauthn_credentials(&req.user_id)
            .await
            .ok();
        // Admin recovery path is audited (does not bypass audit, I2.7).
        let admin_actor = req
            .context
            .as_ref()
            .map(|c| c.user_id.clone())
            .filter(|a| !a.trim().is_empty())
            .unwrap_or_else(|| req.user_id.clone());
        self.emit_event(
            AuthEvent::new(
                topics::MFA_RESET,
                req.user_id.clone(),
                tenant,
                serde_json::json!({
                    "user_id": req.user_id.clone(),
                    "actor": req.context.as_ref().map(|c| c.user_id.clone()).unwrap_or_default(),
                    "reason": req.reason.clone(),
                }),
            )
            .with_correlation(format!("mfa_reset:{}", req.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: admin_actor,
                target_resource: req.user_id.clone(),
                operation: "mfa_reset".to_string(),
                outcome: "success".to_string(),
                reason_code: if req.reason.trim().is_empty() {
                    "admin_mfa_reset".to_string()
                } else {
                    req.reason.clone()
                },
                auth_method: "admin".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::AdminResetMfaResponse {
            reset: true,
        }))
    }

    // ── WebAuthn credential lifecycle (I2.7) ────────────────────────────────

    async fn count_webauthn_credentials(&self, user_id: &str) -> i64 {
        let Some(pool) = self.pg_pool.as_ref() else {
            return 0;
        };
        let m = native_model(
            "udb.core.authn.entity.v1.WebAuthnCredential",
            &["credential_id", "user_id"],
        );
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*)::bigint FROM {rel} WHERE {user} = $1::UUID",
            rel = m.relation,
            user = m.q("user_id"),
        ))
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap_or(0)
    }

    async fn delete_all_webauthn_credentials(&self, user_id: &str) -> Result<u64, Status> {
        if let Ok(runtime) = self.authn_runtime() {
            let context = self.authn_context("", "");
            let op = LogicalDelete {
                message_type: "udb.core.authn.entity.v1.WebAuthnCredential".to_string(),
                filter: authn_eq("user_id", LogicalValue::String(user_id.to_string())),
                return_fields: vec!["credential_id".to_string()],
            };
            let rows = runtime
                .native_entity_delete_rows_for_service("authn", &context, op)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "delete_webauthn_credentials_runtime",
                        format!("delete WebAuthn credentials failed: {err}"),
                    )
                })?;
            return Ok(rows.len() as u64);
        }
        let pool = self.require_pool()?;
        let m = native_model(
            "udb.core.authn.entity.v1.WebAuthnCredential",
            &["credential_id", "user_id"],
        );
        let res = sqlx::query(&format!(
            "DELETE FROM {rel} WHERE {user} = $1::UUID",
            rel = m.relation,
            user = m.q("user_id"),
        ))
        .bind(user_id)
        .execute(pool)
        .await
        .map_err(|err| {
            lifecycle_internal_status(
                "delete_webauthn_credentials_pg",
                format!("delete WebAuthn credentials failed: {err}"),
            )
        })?;
        Ok(res.rows_affected())
    }

    pub(super) async fn list_web_authn_credentials_impl(
        &self,
        request: Request<authn_pb::ListWebAuthnCredentialsRequest>,
    ) -> Result<Response<authn_pb::ListWebAuthnCredentialsResponse>, Status> {
        let req = request.into_inner();
        // A8: bind the target user_id to the validated claim (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        if req.user_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "user_id is required",
                [("user_id", "must be a non-empty user id")],
            ));
        }
        let pool = self.require_pool()?;
        let m = native_model(
            "udb.core.authn.entity.v1.WebAuthnCredential",
            &[
                "credential_id",
                "user_id",
                "label",
                "created_at",
                "last_used_at",
            ],
        );
        let sql = format!(
            "SELECT {id}::TEXT AS credential_id, COALESCE({label}::TEXT,'') AS label, \
                    {created}, {last} \
             FROM {rel} WHERE {user} = $1::UUID ORDER BY {created_col} ASC",
            id = m.q("credential_id"),
            label = m.q("label"),
            created = m.timestamp_unix_as("created_at", "created_at"),
            created_col = m.q("created_at"),
            last = m.timestamp_unix_as("last_used_at", "last_used_at"),
            rel = m.relation,
            user = m.q("user_id"),
        );
        let rows = sqlx::query(&sql)
            .bind(&req.user_id)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                lifecycle_internal_status(
                    "list_webauthn_credentials",
                    format!("list WebAuthn credentials failed: {err}"),
                )
            })?;
        let page_window = native_offset_page_window(1, req.page_size, &req.page_token, 50);
        let total = rows.len() as i64;
        let credentials = rows
            .iter()
            .skip(page_window.offset)
            .take(page_window.limit)
            .map(|row| authn_pb::WebAuthnCredentialSummary {
                credential_id: row.try_get("credential_id").unwrap_or_default(),
                label: row.try_get("label").unwrap_or_default(),
                created_at_unix: row.try_get::<i64, _>("created_at").unwrap_or(0),
                last_used_at_unix: row.try_get::<i64, _>("last_used_at").unwrap_or(0),
            })
            .collect();
        Ok(Response::new(authn_pb::ListWebAuthnCredentialsResponse {
            credentials,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
        }))
    }

    pub(super) async fn delete_web_authn_credential_impl(
        &self,
        request: Request<authn_pb::DeleteWebAuthnCredentialRequest>,
    ) -> Result<Response<authn_pb::DeleteWebAuthnCredentialResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() || req.credential_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "user_id and credential_id are required",
                [
                    ("user_id", "must be a non-empty user id"),
                    (
                        "credential_id",
                        "must be a non-empty WebAuthn credential id",
                    ),
                ],
            ));
        }
        // A7: bind the target user_id to the validated claim (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        if let Ok(runtime) = self.authn_runtime() {
            let target = self
                .users
                .get_user_by_id(&req.user_id)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "delete_webauthn_credential_user_load",
                        err.to_string(),
                    )
                })?
                .ok_or_else(super::authn_user_not_found_status)?;
            let context = self.authn_context(&target.tenant_id, &target.project_id);
            let op = LogicalDelete {
                message_type: "udb.core.authn.entity.v1.WebAuthnCredential".to_string(),
                filter: authn_and(vec![
                    authn_eq(
                        "credential_id",
                        LogicalValue::String(req.credential_id.clone()),
                    ),
                    authn_eq("user_id", LogicalValue::String(req.user_id.clone())),
                ]),
                return_fields: vec!["credential_id".to_string()],
            };
            let rows = runtime
                .native_entity_delete_rows_for_service("authn", &context, op)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "delete_webauthn_credential_runtime",
                        format!("delete WebAuthn credential failed: {err}"),
                    )
                })?;
            return Ok(Response::new(authn_pb::DeleteWebAuthnCredentialResponse {
                deleted: !rows.is_empty(),
            }));
        }
        let pool = self.require_pool()?;
        let m = native_model(
            "udb.core.authn.entity.v1.WebAuthnCredential",
            &["credential_id", "user_id"],
        );
        let res = sqlx::query(&format!(
            "DELETE FROM {rel} WHERE {id} = $1 AND {user} = $2::UUID",
            rel = m.relation,
            id = m.q("credential_id"),
            user = m.q("user_id"),
        ))
        .bind(&req.credential_id)
        .bind(&req.user_id)
        .execute(pool)
        .await
        .map_err(|err| {
            lifecycle_internal_status(
                "delete_webauthn_credential_pg",
                format!("delete WebAuthn credential failed: {err}"),
            )
        })?;
        Ok(Response::new(authn_pb::DeleteWebAuthnCredentialResponse {
            deleted: res.rows_affected() > 0,
        }))
    }

    pub(super) async fn rename_passkey_impl(
        &self,
        request: Request<authn_pb::RenamePasskeyRequest>,
    ) -> Result<Response<authn_pb::RenamePasskeyResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() || req.credential_id.trim().is_empty() {
            return Err(lifecycle_invalid_fields(
                "user_id and credential_id are required",
                [
                    ("user_id", "must be a non-empty user id"),
                    (
                        "credential_id",
                        "must be a non-empty WebAuthn credential id",
                    ),
                ],
            ));
        }
        // A7: bind the target user_id to the validated claim (no-op in-process).
        self.authorize_target_user(&req.user_id).await?;
        if let Ok(runtime) = self.authn_runtime() {
            let target = self
                .users
                .get_user_by_id(&req.user_id)
                .await
                .map_err(|err| {
                    lifecycle_internal_status("rename_passkey_user_load", err.to_string())
                })?
                .ok_or_else(super::authn_user_not_found_status)?;
            let context = self.authn_context(&target.tenant_id, &target.project_id);
            let mut assignments = std::collections::BTreeMap::new();
            assignments.insert(
                "label".to_string(),
                authn_set(LogicalValue::String(req.new_label.clone())),
            );
            assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
            let op = LogicalUpdate {
                message_type: "udb.core.authn.entity.v1.WebAuthnCredential".to_string(),
                filter: authn_and(vec![
                    authn_eq(
                        "credential_id",
                        LogicalValue::String(req.credential_id.clone()),
                    ),
                    authn_eq("user_id", LogicalValue::String(req.user_id.clone())),
                ]),
                assignments,
                return_fields: vec!["credential_id".to_string()],
                require_affected: false,
            };
            let (_, rows) = runtime
                .native_entity_update_for_service("authn", &context, op)
                .await
                .map_err(|err| {
                    lifecycle_internal_status(
                        "rename_passkey_runtime",
                        format!("rename passkey failed: {err}"),
                    )
                })?;
            return Ok(Response::new(authn_pb::RenamePasskeyResponse {
                renamed: !rows.is_empty(),
            }));
        }
        let pool = self.require_pool()?;
        let m = native_model(
            "udb.core.authn.entity.v1.WebAuthnCredential",
            &["credential_id", "user_id", "label", "updated_at"],
        );
        let res = sqlx::query(&format!(
            "UPDATE {rel} SET {label} = $3, {updated} = NOW() WHERE {id} = $1 AND {user} = $2::UUID",
            rel = m.relation,
            label = m.q("label"),
            updated = m.q("updated_at"),
            id = m.q("credential_id"),
            user = m.q("user_id"),
        ))
        .bind(&req.credential_id)
        .bind(&req.user_id)
        .bind(&req.new_label)
        .execute(pool)
        .await
        .map_err(|err| lifecycle_internal_status("rename_passkey_pg", format!("rename passkey failed: {err}")))?;
        Ok(Response::new(authn_pb::RenamePasskeyResponse {
            renamed: res.rows_affected() > 0,
        }))
    }
}

/// DB string form of an `AuthFactorKind` enum value.
fn auth_factor_db(kind: i32) -> String {
    authn_entity_pb::AuthFactorKind::try_from(kind)
        .unwrap_or(authn_entity_pb::AuthFactorKind::Unspecified)
        .as_str_name()
        .to_string()
}

/// DB string form of an `MfaChallengePurpose` enum value.
fn mfa_purpose_db(purpose: i32) -> String {
    authn_entity_pb::MfaChallengePurpose::try_from(purpose)
        .unwrap_or(authn_entity_pb::MfaChallengePurpose::Unspecified)
        .as_str_name()
        .to_string()
}

/// Parse the DB device-type string back to the proto enum tag.
fn parse_device_type(value: &str) -> i32 {
    authn_entity_pb::DeviceType::from_str_name(&value.to_ascii_uppercase())
        .or_else(|| {
            authn_entity_pb::DeviceType::from_str_name(&format!(
                "DEVICE_TYPE_{}",
                value.to_ascii_uppercase()
            ))
        })
        .unwrap_or(authn_entity_pb::DeviceType::Web) as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    #[test]
    fn mask_ip_truncates_v4_to_24() {
        assert_eq!(mask_ip("203.0.113.42"), "203.0.113.0/24");
        // Masked output never contains the final host octet.
        assert!(!mask_ip("203.0.113.42").contains("42"));
    }

    #[test]
    fn mask_ip_truncates_v6_to_48() {
        let masked = mask_ip("2001:db8:abcd:1234::1");
        assert!(masked.ends_with("::/48"));
        assert!(masked.starts_with("2001:db8:abcd"));
    }

    #[test]
    fn mask_ip_handles_empty_and_nonip() {
        assert_eq!(mask_ip(""), "");
        assert_eq!(mask_ip("not-an-ip"), "not-an-ip");
    }

    #[test]
    fn device_type_parse_accepts_short_and_full_forms() {
        assert_eq!(
            parse_device_type("WEB"),
            authn_entity_pb::DeviceType::Web as i32
        );
        assert_eq!(
            parse_device_type("MOBILE"),
            authn_entity_pb::DeviceType::Mobile as i32
        );
        assert_eq!(
            parse_device_type("DEVICE_TYPE_API"),
            authn_entity_pb::DeviceType::Api as i32
        );
        // Unknown falls back to WEB rather than panicking.
        assert_eq!(
            parse_device_type("???"),
            authn_entity_pb::DeviceType::Web as i32
        );
    }

    fn deny_test_service() -> AuthnServiceImpl {
        // No-pool service: the D3 body-tenant guard fires BEFORE `require_pool`, so
        // a cross-tenant call is denied without a live Postgres.
        let config = crate::runtime::authn::AuthnConfig {
            session_hash_secret: "deny-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        };
        AuthnServiceImpl::new(config, crate::runtime::security::SecurityConfig::default())
    }

    fn body_scope(
        tenant_id: &str,
        project_id: &str,
    ) -> crate::proto::udb::core::common::v1::RequestContext {
        crate::proto::udb::core::common::v1::RequestContext {
            tenant: Some(crate::proto::udb::core::common::v1::TenantContext {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn request_authn_context_inherits_validated_claim_scope() {
        let service = deny_test_service();
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-a",
            "tenant-a",
            "project-a",
            &["udb:authn:write"],
            &[],
        );
        let context = crate::runtime::service::method_security::scope_claim_context_for_test(
            claim,
            async { service.request_authn_context(None) },
        )
        .await
        .expect("an omitted body scope inherits the validated claim");

        assert_eq!(context.tenant_id, "tenant-a");
        assert_eq!(context.project_id, "project-a");
    }

    #[tokio::test]
    async fn request_authn_context_rejects_foreign_body_scope() {
        let service = deny_test_service();
        let claim = crate::runtime::service::method_security::test_claim_context(
            "user-a",
            "tenant-a",
            "project-a",
            &["udb:authn:write"],
            &[],
        );
        let request_context = body_scope("tenant-b", "project-b");
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            claim,
            async { service.request_authn_context(Some(&request_context)) },
        )
        .await
        .expect_err("request body scope must not override the validated bearer claim");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_fields(status: &Status, expected: &[(&str, &str)]) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), expected.len());
        for (actual, (field, description)) in detail.field_violations.iter().zip(expected) {
            assert_eq!(actual.field, *field);
            assert_eq!(actual.description, *description);
        }
    }

    fn assert_capability_detail(
        status: &Status,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn lifecycle_internal_status_carries_typed_detail() {
        let status = lifecycle_internal_status("list_devices_query", "list devices failed");
        assert_internal_detail(&status, "list_devices_query", "list devices failed");
    }

    #[test]
    fn authn_missing_postgres_store_capability_carries_typed_detail() {
        let svc = deny_test_service();
        let err = match svc.require_pool() {
            Err(status) => status,
            Ok(_) => panic!("pool-less authn service must fail closed"),
        };

        assert_capability_detail(
            &err,
            "postgres_auth_store",
            "native_postgres_auth_store",
            "this operation requires the native Postgres auth store",
        );
    }

    #[tokio::test]
    async fn list_devices_missing_user_id_carries_field_violation() {
        let err = deny_test_service()
            .list_devices(Request::new(authn_pb::ListDevicesRequest::default()))
            .await
            .expect_err("missing user_id must fail before pool access");

        assert_eq!(err.message(), "user_id is required");
        assert_validation_fields(&err, &[("user_id", "must be a non-empty user id")]);
    }

    #[tokio::test]
    async fn admin_revoke_session_missing_user_id_carries_field_violation() {
        let err = deny_test_service()
            .admin_revoke_session(Request::new(authn_pb::AdminRevokeSessionRequest::default()))
            .await
            .expect_err("missing user_id must fail before pool access");

        assert_eq!(err.message(), "user_id is required");
        assert_validation_fields(&err, &[("user_id", "must be a non-empty user id")]);
    }

    #[tokio::test]
    async fn admin_revoke_all_user_sessions_missing_user_id_carries_field_violation() {
        let err = deny_test_service()
            .admin_revoke_all_user_sessions(Request::new(
                authn_pb::AdminRevokeAllUserSessionsRequest::default(),
            ))
            .await
            .expect_err("missing user_id must fail before pool access");

        assert_eq!(err.message(), "user_id is required");
        assert_validation_fields(&err, &[("user_id", "must be a non-empty user id")]);
    }

    #[tokio::test]
    async fn admin_revoke_all_tenant_sessions_missing_tenant_id_carries_field_violation() {
        let err = deny_test_service()
            .admin_revoke_all_tenant_sessions(Request::new(
                authn_pb::AdminRevokeAllTenantSessionsRequest::default(),
            ))
            .await
            .expect_err("missing tenant_id must fail before claim or pool access");

        assert_eq!(err.message(), "tenant_id is required");
        assert_validation_fields(&err, &[("tenant_id", "must be a non-empty tenant id")]);
    }

    #[tokio::test]
    async fn emergency_revoke_missing_selector_carries_field_violation() {
        let err = deny_test_service()
            .emergency_revoke(Request::new(authn_pb::EmergencyRevokeRequest::default()))
            .await
            .expect_err("missing selector must fail before pool access");

        assert_eq!(
            err.message(),
            "at least one selector is required (signing_key_id/token_family_id/tenant_id/principal_id)"
        );
        assert_validation_fields(
            &err,
            &[(
                "selectors",
                "must include at least one of signing_key_id, token_family_id, tenant_id, or principal_id",
            )],
        );
    }

    #[tokio::test]
    async fn list_webauthn_credentials_missing_user_id_carries_field_violation() {
        let err = deny_test_service()
            .list_web_authn_credentials(Request::new(
                authn_pb::ListWebAuthnCredentialsRequest::default(),
            ))
            .await
            .expect_err("missing user_id must fail before pool access");

        assert_eq!(err.message(), "user_id is required");
        assert_validation_fields(&err, &[("user_id", "must be a non-empty user id")]);
    }

    #[tokio::test]
    async fn delete_webauthn_credential_missing_identity_carries_field_violations() {
        let err = deny_test_service()
            .delete_web_authn_credential(Request::new(
                authn_pb::DeleteWebAuthnCredentialRequest::default(),
            ))
            .await
            .expect_err("missing credential identity must fail before runtime/pool access");

        assert_eq!(err.message(), "user_id and credential_id are required");
        assert_validation_fields(
            &err,
            &[
                ("user_id", "must be a non-empty user id"),
                (
                    "credential_id",
                    "must be a non-empty WebAuthn credential id",
                ),
            ],
        );
    }

    #[tokio::test]
    async fn rename_passkey_missing_identity_carries_field_violations() {
        let err = deny_test_service()
            .rename_passkey(Request::new(authn_pb::RenamePasskeyRequest::default()))
            .await
            .expect_err("missing credential identity must fail before runtime/pool access");

        assert_eq!(err.message(), "user_id and credential_id are required");
        assert_validation_fields(
            &err,
            &[
                ("user_id", "must be a non-empty user id"),
                (
                    "credential_id",
                    "must be a non-empty WebAuthn credential id",
                ),
            ],
        );
    }

    #[tokio::test]
    async fn revoke_device_tenantless_non_admin_carries_policy_detail() {
        let svc = deny_test_service();
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "",
            "",
            &["udb:authn:write"],
            &[],
        );
        let req = Request::new(authn_pb::RevokeDeviceRequest {
            device_id: "11111111-1111-4111-8111-111111111111".to_string(),
            reason: "lost".to_string(),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.revoke_device_impl(req),
        )
        .await
        .expect_err("tenantless non-admin device revoke must fail before pool access");

        assert_policy_detail(
            &err,
            "revoke_device",
            "tenant_scoped_bearer_required",
            "device revoke requires a tenant-scoped bearer token or a cross-tenant admin role",
        );
    }

    #[tokio::test]
    async fn admin_revoke_all_tenant_sessions_denies_cross_tenant_body() {
        // D3: a tenant-A admin token cannot revoke tenant B by setting body
        // tenant_id=tenant-b. The claim-bound guard denies before any DB access.
        let svc = deny_test_service();
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "admin-a",
            "tenant-a",
            "",
            &["udb:authn:write"],
            &[],
        );
        let req = Request::new(authn_pb::AdminRevokeAllTenantSessionsRequest {
            tenant_id: "tenant-b".to_string(),
            reason: "attack".to_string(),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.admin_revoke_all_tenant_sessions_impl(req),
        )
        .await
        .expect_err("cross-tenant tenant-revoke must be denied");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn cross_tenant_admin_may_revoke_other_tenant_sessions() {
        // A genuine platform admin is allowed past the D3 guard; it then proceeds to
        // the DB stage (no pool wired → failed_precondition, NOT permission_denied),
        // which proves the guard did not block the legitimate cross-tenant admin.
        let svc = deny_test_service();
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "ops-1",
            "tenant-a",
            "",
            &[],
            &["platform_admin"],
        );
        let req = Request::new(authn_pb::AdminRevokeAllTenantSessionsRequest {
            tenant_id: "tenant-b".to_string(),
            reason: "legit".to_string(),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.admin_revoke_all_tenant_sessions_impl(req),
        )
        .await
        .expect_err("no pool wired → reaches DB stage and fails precondition");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn completed_tenant_revoke_resumes_strictly_after_inclusive_cutoff() {
        use crate::runtime::authn::revocation::{
            TenantDenylistDecision, tenant_denylist_check_outcome,
        };

        let cutoff = 1_000;
        let resume_at = first_unrevoked_tenant_issue_second(cutoff);
        assert_eq!(resume_at, cutoff + 1);
        assert!(
            tenant_denylist_check_outcome(
                TenantDenylistDecision::Cutoff(cutoff),
                cutoff,
                true,
            )
            .is_some(),
            "same-second tokens remain denied"
        );
        assert!(
            tenant_denylist_check_outcome(
                TenantDenylistDecision::Cutoff(cutoff),
                resume_at,
                true,
            )
            .is_none(),
            "issuance after the completed-revoke barrier is fresh"
        );
    }

    #[test]
    fn auth_factor_and_purpose_db_roundtrip_through_proto_names() {
        // The DB string is the canonical proto enum name; verify it parses back.
        let factor = auth_factor_db(authn_entity_pb::AuthFactorKind::Totp as i32);
        assert_eq!(factor, "AUTH_FACTOR_KIND_TOTP");
        assert_eq!(
            authn_entity_pb::AuthFactorKind::from_str_name(&factor),
            Some(authn_entity_pb::AuthFactorKind::Totp)
        );
        let purpose = mfa_purpose_db(authn_entity_pb::MfaChallengePurpose::LoginStepUp as i32);
        assert_eq!(purpose, "MFA_CHALLENGE_PURPOSE_LOGIN_STEP_UP");
        assert_eq!(
            authn_entity_pb::MfaChallengePurpose::from_str_name(&purpose),
            Some(authn_entity_pb::MfaChallengePurpose::LoginStepUp)
        );
    }
}
