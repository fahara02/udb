//! DB-backed JWT signing-key registry (Phase 3 / I2.1).
//!
//! Replaces the single process-global RS256 key with a `udb_authn.signing_keys`
//! registry. The ACTIVE key signs newly-issued access tokens; ACTIVE + VERIFYING
//! public keys are published in JWKS so tokens signed by a just-rotated key are
//! still verifiable during the rotation overlap window. RETIRED/COMPROMISED keys
//! are excluded from JWKS (their tokens stop verifying).
//!
//! The env key (`UDB_JWT_PRIVATE_KEY` / `UDB_JWT_PUBLIC_KEY`) is kept as a dev
//! seed: when the registry is empty it is inserted as the ACTIVE key on startup
//! so existing single-key deployments keep working with no config change.

use super::*;
use crate::runtime::authn::signing_keys as signing_key_domain;
use crate::runtime::core::DataBrokerRuntime;
use crate::runtime::native_catalog::native_model;
use crate::runtime::security::UDB_RS256_KID;
use sqlx::Row;

// Master-plan 3.4: the signing-key source is abstracted behind a `KeyProvider`
// (see `key_provider.rs`). The seed path resolves its private PEM through the
// active provider (default `EnvKeyProvider` = today's env/config path, selected
// once by `UDB_SIGNING_KEY_PROVIDER`), so the source can become a KMS/HSM without
// touching the seal/seed logic below. The seal-at-rest step
// ([`DataBrokerRuntime::encrypt_secret_at_rest`]) stays *downstream* of the
// provider — a provider supplies cleartext private material; it does not seal.
use super::key_provider::active_key_provider;

/// One published signing key (public material + kid + algorithm) for JWKS. Only
/// ACTIVE/VERIFYING keys ever become a `RegistryKey` — the publishability gate is
/// applied while reading the row (the `state` is consumed there), so the published
/// key need not carry it further.
pub(super) struct RegistryKey {
    pub key_id: String,
    pub algorithm: String,
    pub public_material: String,
}

fn signing_key_model() -> crate::runtime::native_catalog::NativeModel {
    native_model(
        "udb.core.authn.entity.v1.SigningKey",
        &[
            "key_id",
            "tenant_id",
            "algorithm",
            "public_material",
            "encrypted_private_material",
            "state",
            "created_by",
            "retired_at",
            "retired_by",
            "rotation_reason",
        ],
    )
}

fn signing_key_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

impl AuthnServiceImpl {
    /// Seed the registry from the env key when it is empty (dev/default seed).
    /// Idempotent: a no-op once any key row exists. Never fails the caller — a
    /// seed error is logged and the env-key signing path remains the fallback.
    ///
    /// Phase 5 (encryption-at-rest): the RS256 private PEM is sealed via
    /// [`DataBrokerRuntime::encrypt_secret_at_rest`] before it is written, so the
    /// `encrypted_private_material` column is genuinely an AEAD envelope (it was a
    /// name lie before — plaintext PEM). In dev (no encryption key configured) the
    /// helper passes the PEM through verbatim, so the env-seed key keeps working.
    pub(crate) async fn seed_signing_key_registry(&self, runtime: &DataBrokerRuntime) {
        self.seed_signing_key_registry_inner(runtime).await;
        // Whether we just seeded the env key or rows already existed (e.g. an
        // operator-rotated ACTIVE key), publish the live registry to the
        // process-global signing-key snapshot so the synchronous issuer signs with
        // the ACTIVE key's PEM + `kid` and the validator accepts ACTIVE/VERIFYING
        // keys during the overlap window. No-op (env fallback) when empty.
        self.refresh_signing_key_cache(runtime).await;
    }

    /// Idempotent env-key seed: inserts the env key as the ACTIVE row only when the
    /// registry is empty. Split out so [`Self::seed_signing_key_registry`] can
    /// always refresh the in-process signing-key cache afterwards regardless of
    /// whether a seed was needed.
    async fn seed_signing_key_registry_inner(&self, runtime: &DataBrokerRuntime) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let Some(public) = self.env_public_pem() else {
            return;
        };
        let m = signing_key_model();
        // Only seed when the registry has no key yet (idempotent startup seed).
        let existing: Result<i64, _> = sqlx::query_scalar(&format!(
            "SELECT COUNT(*)::bigint FROM {rel}",
            rel = m.relation
        ))
        .fetch_one(pool)
        .await;
        if !matches!(existing, Ok(0)) {
            return;
        }
        // Resolve the signing private material via the active `KeyProvider`
        // (default `EnvKeyProvider` = today's env/config path, selected once by
        // `UDB_SIGNING_KEY_PROVIDER`). Byte-identical to the prior
        // `self.security.jwt_private_pem().unwrap_or_default()` for the env path:
        // `EnvKeyProvider::signing_key_pem` returns `Ok(pem)` when configured and
        // `Ok("")` when absent/empty, so `private_plain` is exactly the PEM-or-""
        // it was before, and the seal step below is unchanged. A provider `Err` is
        // only reachable for a non-env/misconfigured provider (the env path is
        // infallible); we log and skip the seed rather than writing an empty key,
        // leaving the env-key signing path as the fallback.
        let private_plain = match active_key_provider(&self.security).signing_key_pem() {
            Ok(pem) => pem,
            Err(err) => {
                tracing::warn!(error = %err, "skipping signing-key seed: signing-key provider unavailable");
                return;
            }
        };
        // Seal the private material at rest. On error (fail-closed mode with no key
        // configured) we refuse to seed rather than write the PEM in the clear.
        let private = match runtime.encrypt_secret_at_rest(&private_plain) {
            Ok(sealed) => sealed,
            Err(err) => {
                tracing::warn!(error = %err, "skipping signing-key seed: cannot seal private material at rest");
                return;
            }
        };
        if let Ok(authn_runtime) = self.authn_runtime() {
            let context = self.authn_context("", "");
            let record = authn_record([
                ("key_id", LogicalValue::String(UDB_RS256_KID.to_string())),
                (
                    "algorithm",
                    LogicalValue::String(signing_key_domain::DEFAULT_SIGNING_ALGORITHM.to_string()),
                ),
                ("public_material", LogicalValue::String(public.clone())),
                (
                    "encrypted_private_material",
                    LogicalValue::String(private.clone()),
                ),
                (
                    "state",
                    LogicalValue::String(signing_key_domain::STATE_ACTIVE.to_string()),
                ),
                ("created_by", LogicalValue::String("env-seed".to_string())),
            ]);
            if let Err(err) = authn_runtime
                .native_entity_write_for_service(
                    "authn",
                    &context,
                    "udb.core.authn.entity.v1.SigningKey",
                    record,
                    crate::ir::ConflictStrategy::Ignore,
                )
                .await
            {
                tracing::warn!(error = %err, "failed to seed signing-key registry from env key");
            }
            return;
        }
        let sql = format!(
            "INSERT INTO {rel} ({kid}, {alg}, {pubm}, {privm}, {state}, {by}) \
             VALUES ($1, $2, $3, $4, $5, 'env-seed') ON CONFLICT ({kid}) DO NOTHING",
            rel = m.relation,
            kid = m.q("key_id"),
            alg = m.q("algorithm"),
            pubm = m.q("public_material"),
            privm = m.q("encrypted_private_material"),
            state = m.q("state"),
            by = m.q("created_by"),
        );
        if let Err(err) = sqlx::query(&sql)
            .bind(UDB_RS256_KID)
            .bind(signing_key_domain::DEFAULT_SIGNING_ALGORITHM)
            .bind(&public)
            .bind(&private)
            .bind(signing_key_domain::STATE_ACTIVE)
            .execute(pool)
            .await
        {
            tracing::warn!(error = %err, "failed to seed signing-key registry from env key");
        }
    }

    /// Public PEM resolved from the configured env public key (inline or path).
    fn env_public_pem(&self) -> Option<String> {
        self.security.jwt_public_pem()
    }

    /// ACTIVE + VERIFYING registry keys for JWKS publication.
    ///
    /// Returns `Ok(vec![])` when there is no pool or simply no rows — an empty
    /// registry is not an error; the caller then falls back to the configured env
    /// public key. A *store/DB error*, however, is surfaced as `Err`:
    ///   - in [`fail_closed_mode`](crate::runtime::security::fail_closed_mode) the
    ///     caller MUST refuse to silently publish the legacy env key (FAIL CLOSED),
    ///   - in dev the caller logs and degrades to the env key (FAIL OPEN), matching
    ///     prior behavior.
    pub(super) async fn jwks_registry_keys(&self) -> Result<Vec<RegistryKey>, Status> {
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(Vec::new());
        };
        let m = signing_key_model();
        let sql = format!(
            "SELECT {kid}::TEXT AS key_id, {alg}::TEXT AS algorithm, \
                    {pubm}::TEXT AS public_material, {state}::TEXT AS state \
             FROM {rel} WHERE {state} IN ($1, $2) ORDER BY {state}, {kid}",
            kid = m.q("key_id"),
            alg = m.q("algorithm"),
            pubm = m.q("public_material"),
            state = m.q("state"),
            rel = m.relation,
        );
        let rows = match sqlx::query(&sql)
            .bind(signing_key_domain::STATE_ACTIVE)
            .bind(signing_key_domain::STATE_VERIFYING)
            .fetch_all(pool)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                return Err(signing_key_internal_status(
                    "jwks_registry_read",
                    format!("signing-key registry read failed: {err}"),
                ));
            }
        };
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let state: String = row.try_get("state").ok()?;
                if !signing_key_domain::jwks_publishable_state(&state) {
                    return None;
                }
                Some(RegistryKey {
                    key_id: row.try_get("key_id").ok()?,
                    algorithm: row.try_get("algorithm").ok()?,
                    public_material: row.try_get("public_material").ok()?,
                })
            })
            .collect())
    }

    /// Load the ACTIVE signing key's `kid` + *decrypted* private material (Phase 5
    /// read path / [`Self::seed_signing_key_registry`] write-path inverse).
    ///
    /// The `encrypted_private_material` column is an AES-256-GCM-SIV envelope when
    /// an encryption key is configured; this returns the plaintext PEM ready to
    /// build an `EncodingKey`, paired with the key's `kid` so the issuer stamps
    /// the JWT header with the key that actually signs. Legacy plaintext rows pass
    /// through unchanged. The decrypt is performed here so the cleartext PEM never
    /// lives in the DB read surface. Returns `None` when no pool / no ACTIVE row.
    ///
    /// Wired into [`Self::refresh_signing_key_cache`], which publishes the result
    /// to the process-global signing-key snapshot consumed by the synchronous
    /// `crate::runtime::security::sign_access_token_with_key` issuer path.
    pub(super) async fn load_active_signing_private_key(
        &self,
        runtime: &DataBrokerRuntime,
    ) -> Result<Option<(String, String)>, Status> {
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(None);
        };
        let m = signing_key_model();
        let sql = format!(
            "SELECT {kid}::TEXT AS key_id, \
                    {privm}::TEXT AS encrypted_private_material \
             FROM {rel} WHERE {state} = $1 ORDER BY {kid} LIMIT 1",
            kid = m.q("key_id"),
            privm = m.q("encrypted_private_material"),
            rel = m.relation,
            state = m.q("state"),
        );
        let row = sqlx::query(&sql)
            .bind(signing_key_domain::STATE_ACTIVE)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                signing_key_internal_status(
                    "active_signing_key_read",
                    format!("active signing-key read failed: {err}"),
                )
            })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let key_id: String = row.try_get("key_id").unwrap_or_default();
        let stored: String = row
            .try_get("encrypted_private_material")
            .unwrap_or_default();
        if key_id.is_empty() || stored.is_empty() {
            return Ok(None);
        }
        let pem = runtime.decrypt_secret_at_rest(&stored).map_err(|err| {
            signing_key_internal_status(
                "active_signing_key_decrypt",
                format!("active signing-key decrypt failed: {err}"),
            )
        })?;
        Ok(Some((key_id, pem)))
    }

    /// Resolve the live signing-key registry (ACTIVE private + ACTIVE/VERIFYING
    /// public) and publish it to the process-global snapshot in
    /// [`crate::runtime::security`], which the synchronous issuer/validator paths
    /// read. This is what makes rotation REAL: once an operator promotes a new
    /// ACTIVE key, this refresh makes newly-issued tokens sign with that key's PEM
    /// and stamp its `kid`, while the previous key (now VERIFYING) keeps verifying
    /// during the overlap window.
    ///
    /// Called at startup seed and should be re-run on rotation. Best-effort: on a
    /// store/decrypt error we leave the previous snapshot in place rather than
    /// blanking the active key (which would force the env fallback mid-flight).
    /// When the registry is empty (`active = None`), the env key + `UDB_RS256_KID`
    /// remain the signing/validation fallback.
    pub(crate) async fn refresh_signing_key_cache(&self, runtime: &DataBrokerRuntime) {
        if self.pg_pool.is_none() {
            return;
        }
        let active = match self.load_active_signing_private_key(runtime).await {
            Ok(active) => active,
            Err(err) => {
                tracing::warn!(error = %err, "signing-key cache refresh: active key load failed; keeping previous snapshot");
                return;
            }
        };
        let public_by_kid = match self.jwks_registry_keys().await {
            Ok(keys) => keys
                .into_iter()
                .map(|k| (k.key_id, k.public_material))
                .collect(),
            Err(err) => {
                tracing::warn!(error = %err, "signing-key cache refresh: public-key load failed; keeping previous snapshot");
                return;
            }
        };
        crate::runtime::security::install_signing_key_registry_snapshot(
            crate::runtime::security::SigningKeyRegistrySnapshot {
                active,
                public_by_kid,
            },
        );
    }

    /// `compromise_signing_key` over any executor — `pool` for a standalone
    /// write, or a `&mut PgConnection` so it commits inside the caller's
    /// transaction (used by `emergency_revoke_impl` for §7 atomicity).
    pub(super) async fn compromise_signing_key_on<'c, E>(
        &self,
        executor: E,
        key_id: &str,
        by: &str,
    ) -> Result<u64, Status>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let m = signing_key_model();
        let sql = format!(
            "UPDATE {rel} SET {state} = $3, {retired_at} = NOW(), {retired_by} = $2, \
                    {reason} = $4 \
             WHERE {kid} = $1 AND {state} <> $3",
            rel = m.relation,
            state = m.q("state"),
            retired_at = m.q("retired_at"),
            retired_by = m.q("retired_by"),
            reason = m.q("rotation_reason"),
            kid = m.q("key_id"),
        );
        let res = sqlx::query(&sql)
            .bind(key_id)
            .bind(by)
            .bind(signing_key_domain::STATE_COMPROMISED)
            .bind(signing_key_domain::EMERGENCY_COMPROMISE_REASON)
            .execute(executor)
            .await
            .map_err(|err| {
                signing_key_internal_status(
                    "compromise_signing_key",
                    format!("compromise signing key failed: {err}"),
                )
            })?;
        Ok(res.rows_affected())
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
    fn signing_key_internal_status_carries_typed_detail() {
        let status = signing_key_internal_status(
            "active_signing_key_decrypt",
            "active signing-key decrypt failed: missing key",
        );
        assert_internal_detail(
            &status,
            "active_signing_key_decrypt",
            "active signing-key decrypt failed: missing key",
        );
    }
}
