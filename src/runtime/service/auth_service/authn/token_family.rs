//! Refresh-token rotation lineage (Phase 3 / I2.2).
//!
//! Each refresh-token chain is one `token_families` row. A refresh token is an
//! opaque credential of the form `rt_<family_id>.<jti>`; only the keyed-HMAC
//! digest of `<jti>` is persisted (`current_refresh_jti_hash`). On refresh the
//! family is rotated **atomically** (current → previous, a fresh current minted)
//! and a brand-new `rt_…` value is returned; the presented value is now stale.
//!
//! Reuse detection: presenting a refresh token whose jti hash matches the row's
//! `previous_refresh_jti_hash` (an already-rotated value), or presenting any
//! value against an already-revoked family, is treated as theft — the whole
//! family is revoked, every session/refresh chain for the principal is killed,
//! and a high-severity `REFRESH_REUSE_DETECTED` audit event is emitted.
//!
//! This replaces the previous behaviour of treating the server-side session id
//! as the refresh credential (I2.2: "stop treating server-side session IDs as
//! refresh credentials"). The minted family is bound to family/device/tenant/
//! project/principal so device or tenant revocation also kills the chain.

use super::*;
use crate::runtime::native_catalog::{NativeModel, native_model};
use sqlx::Row;

/// Result of a refresh-token rotation attempt.
pub(super) enum RotateOutcome {
    /// Rotation succeeded: the freshly-minted refresh token plus the family row's
    /// bound principal context (used to mint the new access token).
    Rotated {
        new_refresh_token: String,
        family: TokenFamilyRow,
    },
    /// The presented token did not match any live family (unknown / already
    /// rotated-away current that is neither current nor previous).
    NotFound,
    /// Reuse of a superseded refresh token (or a revoked family) — the family has
    /// already been revoked, the principal's sessions killed, and a high-severity
    /// audit event emitted as a side effect inside `rotate_refresh_family`. A
    /// distinct variant (vs `NotFound`) so a caller can fail closed AND tell a theft
    /// signal apart from an unknown token; it carries no payload because every
    /// remediation is complete before it is returned.
    Reuse,
}

/// Principal/binding context carried on a token-family row. The bound session
/// handle is intentionally NOT surfaced here: a refresh-reuse response kills EVERY
/// active session for the principal (full theft remediation), not just the one
/// bound to this family, so the per-family session handle has no consumer.
pub(super) struct TokenFamilyRow {
    pub family_id: String,
    pub user_id: String,
    pub principal_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub device_id: String,
    pub revoked: bool,
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
            "updated_at",
        ],
    )
}

fn token_family_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

impl AuthnServiceImpl {
    fn require_family_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            authn_capability_status(
                "refresh_token_rotation",
                "native_postgres_auth_store",
                "refresh-token rotation requires the native Postgres auth store",
            )
        })
    }

    /// Keyed-HMAC digest of a refresh jti. STORAGE_ONLY: only this digest is ever
    /// persisted; the raw jti is credential-equivalent for a live token.
    fn refresh_jti_hash(&self, jti: &str) -> String {
        authn::hash_secret(&format!("refresh-jti:{jti}"), &self.hash_key())
    }

    /// Mint a new refresh-token family for a freshly-authenticated principal and
    /// return the opaque `rt_<family>.<jti>` refresh credential. Binds the family
    /// to session/device/tenant/project/user/principal (I2.2). No-op fallback
    /// (empty string) when no Postgres pool is wired — the caller then omits the
    /// refresh token rather than emitting an unbacked one.
    pub(super) async fn mint_refresh_family(
        &self,
        user_id: &str,
        principal_id: &str,
        tenant_id: &str,
        project_id: &str,
        device_id: &str,
        session_handle: &str,
        now: u64,
    ) -> Result<String, Status> {
        let _ = now;
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(String::new());
        };
        let m = token_family_model();
        let family_id = Uuid::new_v4().to_string();
        let jti = Uuid::new_v4().simple().to_string();
        let jti_hash = self.refresh_jti_hash(&jti);
        // The session handle is hashed before storage so the family row never
        // carries the raw session id (which is itself a credential).
        let session_hash = if session_handle.trim().is_empty() {
            String::new()
        } else {
            authn::hash_secret(session_handle, &self.hash_key())
        };
        if let Ok(runtime) = self.authn_runtime() {
            let context = self.authn_context(tenant_id, project_id);
            let record = authn_record([
                ("family_id", LogicalValue::String(family_id.clone())),
                ("session_id", LogicalValue::String(session_hash.clone())),
                ("user_id", LogicalValue::String(user_id.to_string())),
                (
                    "principal_id",
                    LogicalValue::String(principal_id.to_string()),
                ),
                ("tenant_id", LogicalValue::String(tenant_id.to_string())),
                ("project_id", LogicalValue::String(project_id.to_string())),
                ("device_id", LogicalValue::String(device_id.to_string())),
                (
                    "current_refresh_jti_hash",
                    LogicalValue::String(jti_hash.clone()),
                ),
            ]);
            runtime
                .native_entity_write_for_service(
                    "authn",
                    &context,
                    "udb.core.authn.entity.v1.TokenFamily",
                    record,
                    crate::ir::ConflictStrategy::Error,
                )
                .await
                .map_err(|err| {
                    token_family_internal_status(
                        "mint_refresh_family",
                        format!("mint refresh family failed: {err}"),
                    )
                })?;
            return Ok(authn::token_family::format_refresh_token(&family_id, &jti));
        }
        let sql = format!(
            "INSERT INTO {rel} ({id}, {session}, {user}, {principal}, {tenant}, {project}, {device}, {cur}) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8)",
            rel = m.relation,
            id = m.q("family_id"),
            session = m.q("session_id"),
            user = m.q("user_id"),
            principal = m.q("principal_id"),
            tenant = m.q("tenant_id"),
            project = m.q("project_id"),
            device = m.q("device_id"),
            cur = m.q("current_refresh_jti_hash"),
        );
        sqlx::query(&sql)
            .bind(&family_id)
            .bind(&session_hash)
            .bind(user_id)
            .bind(principal_id)
            .bind(tenant_id)
            .bind(project_id)
            .bind(device_id)
            .bind(&jti_hash)
            .execute(pool)
            .await
            .map_err(|err| {
                token_family_internal_status(
                    "mint_refresh_family",
                    format!("mint refresh family failed: {err}"),
                )
            })?;
        Ok(authn::token_family::format_refresh_token(&family_id, &jti))
    }

    /// Revoke every live refresh-token family bound to `session_handle`, so a
    /// refresh token cannot outlive the session it was minted under (a logged-out
    /// session must not keep minting access tokens via `RefreshToken`). The handle
    /// is hashed with the same key `mint_refresh_family` used, so the stored
    /// `session_id` digest matches. No-op (0) without a Postgres pool.
    pub(super) async fn revoke_families_for_session(
        &self,
        session_handle: &str,
    ) -> Result<u64, Status> {
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(0);
        };
        if session_handle.trim().is_empty() {
            return Ok(0);
        }
        let session_hash = authn::hash_secret(session_handle, &self.hash_key());
        if let Ok(runtime) = self.authn_runtime() {
            let context = self.authn_context("", "");
            let mut assignments = std::collections::BTreeMap::new();
            assignments.insert("revoked_at".to_string(), LogicalAssignment::ServerNow);
            assignments.insert(
                "revocation_reason".to_string(),
                authn_set(LogicalValue::String("logout".to_string())),
            );
            assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
            let op = LogicalUpdate {
                message_type: "udb.core.authn.entity.v1.TokenFamily".to_string(),
                filter: authn_and(vec![
                    authn_eq("session_id", LogicalValue::String(session_hash.clone())),
                    LogicalFilter::IsNull("revoked_at".to_string()),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            };
            return runtime
                .native_entity_update_for_service("authn", &context, op)
                .await
                .map(|(affected, _)| affected)
                .map_err(|err| {
                    token_family_internal_status(
                        "revoke_families_for_session",
                        format!("revoke families for session failed: {err}"),
                    )
                });
        }
        let m = token_family_model();
        let sql = format!(
            "UPDATE {rel} SET {revoked} = COALESCE({revoked}, NOW()), \
                    {reason} = COALESCE(NULLIF({reason}, ''), 'logout'), {updated} = NOW() \
             WHERE {session} = $1 AND {revoked} IS NULL",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            reason = m.q("revocation_reason"),
            updated = m.q("updated_at"),
            session = m.q("session_id"),
        );
        Ok(sqlx::query(&sql)
            .bind(&session_hash)
            .execute(pool)
            .await
            .map_err(|err| {
                token_family_internal_status(
                    "revoke_families_for_session",
                    format!("revoke families for session failed: {err}"),
                )
            })?
            .rows_affected())
    }

    /// Revoke every live refresh-token family for a principal (all-sessions
    /// logout), so no refresh token survives a logout-all. No-op (0) without a
    /// Postgres pool.
    pub(super) async fn revoke_families_for_principal(
        &self,
        principal_id: &str,
    ) -> Result<u64, Status> {
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(0);
        };
        if principal_id.trim().is_empty() {
            return Ok(0);
        }
        if let Ok(runtime) = self.authn_runtime() {
            let context = self.authn_context("", "");
            let mut total = 0u64;
            for field in ["user_id", "principal_id"] {
                let mut assignments = std::collections::BTreeMap::new();
                assignments.insert("revoked_at".to_string(), LogicalAssignment::ServerNow);
                assignments.insert(
                    "revocation_reason".to_string(),
                    authn_set(LogicalValue::String("logout_all".to_string())),
                );
                assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
                let op = LogicalUpdate {
                    message_type: "udb.core.authn.entity.v1.TokenFamily".to_string(),
                    filter: authn_and(vec![
                        authn_eq(field, LogicalValue::String(principal_id.to_string())),
                        LogicalFilter::IsNull("revoked_at".to_string()),
                    ]),
                    assignments,
                    return_fields: Vec::new(),
                    require_affected: false,
                };
                let (affected, _) = runtime
                    .native_entity_update_for_service("authn", &context, op)
                    .await
                    .map_err(|err| {
                        token_family_internal_status(
                            "revoke_families_for_principal",
                            format!("revoke families for principal failed: {err}"),
                        )
                    })?;
                total += affected;
            }
            return Ok(total);
        }
        let m = token_family_model();
        let sql = format!(
            "UPDATE {rel} SET {revoked} = COALESCE({revoked}, NOW()), \
                    {reason} = COALESCE(NULLIF({reason}, ''), 'logout_all'), {updated} = NOW() \
             WHERE ({user} = $1 OR {principal} = $1) AND {revoked} IS NULL",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            reason = m.q("revocation_reason"),
            updated = m.q("updated_at"),
            user = m.q("user_id"),
            principal = m.q("principal_id"),
        );
        Ok(sqlx::query(&sql)
            .bind(principal_id)
            .execute(pool)
            .await
            .map_err(|err| {
                token_family_internal_status(
                    "revoke_families_for_principal",
                    format!("revoke families for principal failed: {err}"),
                )
            })?
            .rows_affected())
    }

    /// Atomically rotate a presented refresh token. See module docs for the
    /// reuse-detection semantics. On success the presented jti is retired and a
    /// new `rt_…` is returned; on reuse the family is revoked here.
    pub(super) async fn rotate_refresh_family(
        &self,
        family_id: &str,
        presented_jti: &str,
        now: u64,
    ) -> Result<RotateOutcome, Status> {
        let _ = now;
        let pool = self.require_family_pool()?;
        let m = token_family_model();
        let presented_hash = self.refresh_jti_hash(presented_jti);
        let new_jti = Uuid::new_v4().simple().to_string();
        let new_hash = self.refresh_jti_hash(&new_jti);

        // Single atomic UPDATE: only a live family whose CURRENT jti matches the
        // presented value rotates (current → previous, new current minted). The
        // WHERE clause is the concurrency guard — a second concurrent rotation
        // with the same (now-previous) value cannot match `current`, so it loses
        // the race and is handled as reuse below.
        let rotate_sql = format!(
            "UPDATE {rel} SET {prev} = {cur}, {cur} = $3, {updated} = NOW() \
             WHERE {id} = $1::UUID AND {cur} = $2 AND {revoked} IS NULL \
             RETURNING {user}::TEXT AS user_id, {principal}::TEXT AS principal_id, \
                       {tenant}::TEXT AS tenant_id, COALESCE({project}::TEXT,'') AS project_id, \
                       COALESCE({device}::TEXT,'') AS device_id",
            rel = m.relation,
            prev = m.q("previous_refresh_jti_hash"),
            cur = m.q("current_refresh_jti_hash"),
            updated = m.q("updated_at"),
            id = m.q("family_id"),
            revoked = m.q("revoked_at"),
            user = m.q("user_id"),
            principal = m.q("principal_id"),
            tenant = m.q("tenant_id"),
            project = m.q("project_id"),
            device = m.q("device_id"),
        );
        if let Some(row) = sqlx::query(&rotate_sql)
            .bind(family_id)
            .bind(&presented_hash)
            .bind(&new_hash)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                token_family_internal_status(
                    "rotate_refresh_family",
                    format!("rotate refresh family failed: {err}"),
                )
            })?
        {
            let family = TokenFamilyRow {
                family_id: family_id.to_string(),
                user_id: row.try_get("user_id").unwrap_or_default(),
                principal_id: row.try_get("principal_id").unwrap_or_default(),
                tenant_id: row.try_get("tenant_id").unwrap_or_default(),
                project_id: row.try_get("project_id").unwrap_or_default(),
                device_id: row.try_get("device_id").unwrap_or_default(),
                revoked: false,
            };
            return Ok(RotateOutcome::Rotated {
                new_refresh_token: authn::token_family::format_refresh_token(family_id, &new_jti),
                family,
            });
        }

        // The CURRENT jti did not match. Inspect the family to distinguish reuse
        // of a superseded (previous) jti — or any presentation against a revoked
        // family — from a wholly unknown token.
        let inspect_sql = format!(
            "SELECT {user}::TEXT AS user_id, {principal}::TEXT AS principal_id, \
                    {tenant}::TEXT AS tenant_id, COALESCE({project}::TEXT,'') AS project_id, \
                    COALESCE({device}::TEXT,'') AS device_id, \
                    ({prev} = $2) AS is_previous, ({revoked} IS NOT NULL) AS is_revoked \
             FROM {rel} WHERE {id} = $1::UUID",
            user = m.q("user_id"),
            principal = m.q("principal_id"),
            tenant = m.q("tenant_id"),
            project = m.q("project_id"),
            device = m.q("device_id"),
            prev = m.q("previous_refresh_jti_hash"),
            revoked = m.q("revoked_at"),
            rel = m.relation,
            id = m.q("family_id"),
        );
        let Some(row) = sqlx::query(&inspect_sql)
            .bind(family_id)
            .bind(&presented_hash)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                token_family_internal_status(
                    "inspect_refresh_family",
                    format!("inspect refresh family failed: {err}"),
                )
            })?
        else {
            return Ok(RotateOutcome::NotFound);
        };
        let is_previous: bool = row.try_get("is_previous").unwrap_or(false);
        let is_revoked: bool = row.try_get("is_revoked").unwrap_or(false);
        let family = TokenFamilyRow {
            family_id: family_id.to_string(),
            user_id: row.try_get("user_id").unwrap_or_default(),
            principal_id: row.try_get("principal_id").unwrap_or_default(),
            tenant_id: row.try_get("tenant_id").unwrap_or_default(),
            project_id: row.try_get("project_id").unwrap_or_default(),
            device_id: row.try_get("device_id").unwrap_or_default(),
            revoked: is_revoked,
        };
        if is_previous || is_revoked {
            // Reuse of a rotated-away token (or a token from an already-revoked
            // family) — revoke the whole family. A revoked family that is hit
            // again is idempotent (already revoked), but we still surface Reuse so
            // the caller fails closed.
            self.revoke_family_on_reuse(&family).await?;
            return Ok(RotateOutcome::Reuse);
        }
        Ok(RotateOutcome::NotFound)
    }

    /// Revoke a family on reuse detection and kill the principal's sessions, then
    /// emit the high-severity audit/event (I2.2). Idempotent: stamping
    /// `reuse_detected_at`/`revoked_at` only when not already set.
    async fn revoke_family_on_reuse(&self, family: &TokenFamilyRow) -> Result<(), Status> {
        let pool = self.require_family_pool()?;
        let m = token_family_model();
        let sql = format!(
            "UPDATE {rel} SET {revoked} = COALESCE({revoked}, NOW()), \
                    {reuse} = COALESCE({reuse}, NOW()), \
                    {reason} = COALESCE({reason}, 'refresh_reuse_detected') \
             WHERE {id} = $1::UUID",
            rel = m.relation,
            revoked = m.q("revoked_at"),
            reuse = m.q("reuse_detected_at"),
            reason = m.q("revocation_reason"),
            id = m.q("family_id"),
        );
        sqlx::query(&sql)
            .bind(&family.family_id)
            .execute(pool)
            .await
            .map_err(|err| {
                token_family_internal_status(
                    "revoke_reused_family",
                    format!("revoke reused family failed: {err}"),
                )
            })?;
        // Kill every active session for the principal — refresh-token theft means
        // the chain is compromised; the legitimate user must re-authenticate.
        let principal = if family.principal_id.is_empty() {
            family.user_id.clone()
        } else {
            family.principal_id.clone()
        };
        if !principal.is_empty() {
            let _ = self
                .sessions
                .revoke_all_for_principal(&principal, now_unix())
                .await;
        }
        // First-detection gate: the family-revoke UPDATE above is idempotent, but the
        // high-severity audit event and the token-theft metric must fire ONCE — on the
        // first reuse detection, not on every subsequent replay against the now-dead
        // family (which would spam security dashboards and inflate the theft counter).
        // `family.revoked` is the family's revocation state as observed at inspect time:
        // when it was ALREADY revoked, this presentation is a replay against a family
        // we already alerted on, so we skip the metric + audit (the caller still fails
        // closed via the returned Reuse outcome).
        if family.revoked {
            return Ok(());
        }
        // Phase L metric: count every refresh-family reuse detection so security
        // dashboards can alert on token-theft spikes.
        self.metrics.record_refresh_reuse_detected();
        // High-severity audit + domain event (no raw token material in the body).
        self.emit_event(
            AuthEvent::new(
                topics::REFRESH_REUSE_DETECTED,
                family.family_id.clone(),
                family.tenant_id.clone(),
                serde_json::json!({
                    "family_id": family.family_id,
                    "principal_id": family.principal_id,
                    "user_id": family.user_id,
                    "tenant_id": family.tenant_id,
                    "project_id": family.project_id,
                    "device_id": family.device_id,
                    "severity": "high",
                    "reason": "refresh_token_reuse",
                }),
            )
            .with_correlation(format!("refresh-reuse:{}", family.family_id)),
        )
        .await;
        Ok(())
    }
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
    fn token_family_internal_status_carries_typed_detail() {
        let status = token_family_internal_status(
            "rotate_refresh_family",
            "rotate refresh family failed: store closed",
        );
        assert_internal_detail(
            &status,
            "rotate_refresh_family",
            "rotate refresh family failed: store closed",
        );
    }

    #[test]
    fn refresh_family_missing_postgres_store_capability_carries_typed_detail() {
        let svc = AuthnServiceImpl::new(
            authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        );
        let err = match svc.require_family_pool() {
            Err(status) => status,
            Ok(_) => panic!("pool-less refresh family service must fail closed"),
        };

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "refresh-token rotation requires the native Postgres auth store"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, "refresh_token_rotation");
        assert_eq!(detail.capability_required, "native_postgres_auth_store");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }
}
