//! Native `VaultService` (master-plan 9.1, flagship) — secrets management built
//! into the broker.
//!
//! Mirrors `tenant_service`/`lock_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. Three engines share ONE crypto stack — the broker
//! AES-256-GCM-SIV envelope reused from [`crate::runtime::encryption`] via
//! [`DataBrokerRuntime::encrypt_secret_at_rest`] /
//! [`DataBrokerRuntime::decrypt_secret_at_rest`]:
//!
//!   * KV      — versioned, envelope-encrypted secrets at hierarchical paths
//!               (`tenant/<t>/app/<path>`), with compare-and-swap, soft delete +
//!               undelete (restore), and crypto-shred destroy.
//!   * Transit — encrypt/decrypt (+ batch encrypt/decrypt)/sign/verify/hmac by
//!               key NAME (+ envelope
//!               generate-data-key/rewrap and, for ed25519 signing keys,
//!               get-transit-public-key); private key material is never exported
//!               (only the ed25519 PUBLIC key is); versioned keys with an
//!               ACTIVE/VERIFYING rotation overlap modelled on the auth-service
//!               signing-key registry.
//!   * Seal    — every handler checks the seal state FIRST and fails closed
//!               (`failed_precondition`) when the master key is unavailable;
//!               `SealStatus` reports the state.
//!
//! Envelope encryption: the secret VALUE (and each transit operation's payload)
//! is sealed under a random per-secret / per-key data-encryption key (DEK) with
//! the SAME `aes_gcm_siv` primitive `encryption.rs` uses; the DEK is itself
//! wrapped by the master key-encryption key (KEK) via `encrypt_secret_at_rest`.
//! Only ciphertext is stored. Every secret-bearing type carries a redacting
//! `Debug` from day one (mirroring `encryption.rs::RedactedKeyVersions`).
//!
//! Doctrine: tenant is taken from the VERIFIED claim (cross-tenant request
//! bodies are rejected by `validate_request_tenant`); per-RPC authorization is
//! enforced by the `method_security` tower from each RPC's
//! `endpoint_security.decision_resource` (the same casbin decision engine
//! `auth_service::decide_action_native` consults) before the handler runs; the
//! sensitive reads (`GetSecret`, `Decrypt`) are audited via the outbox
//! compliance envelope.
//!
//! Module layout (no god file): [`config`] the statics + audit topics + the
//! lease-reaper cadence, [`errors`] the typed statuses + field-violation helpers,
//! [`crypto`] the ISOLATED crypto stack (DEK wrap/seal/open, HMAC, envelope
//! parse, algorithm allow-list) + the redacting secret-bearing types, [`model`]
//! the durable-row DTOs + JSON decoders + version selectors, [`store`] the
//! neutral-IR read/record builders, [`dynamic`] the dynamic DB-credential engine
//! helpers, [`events`] the audit emit, [`workers`] the leader-elected lease
//! reaper, [`handlers`] the twenty RPCs — `mod.rs` keeps only the struct, the
//! builders/require-guard/seal-gate/read helpers, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultServiceServer;

use super::DataBrokerService;

mod config;
mod crypto;
mod dynamic;
mod errors;
mod events;
mod handlers;
mod model;
mod quota;
mod store;
#[cfg(test)]
mod tests;
mod workers;

use config::{SEAL_PROBE, VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE, VAULT_RUNTIME_REQUIRED_MESSAGE};
use errors::{vault_capability_status, vault_master_key_unavailable_status};
use model::{
    StoredSecret, StoredTransitKey, stored_secret_from_json, stored_transit_key_from_json,
};
use store::{secret_path_read, transit_key_read};

// Re-exported at the module root for `serve()`, which spawns the leader-elected
// Vault DB-credential lease reaper and reads the env-resolved cadence/batch knobs.
// `VAULT_DB_LEASE_REAPER_BATCH` is retained as the byte-stable default; new code
// should read the env-governed `vault_db_lease_reaper_batch()`.
// TODO(leader-wire): the reaper-batch value passed at the spawn in
// `service/mod.rs` still uses the const `VAULT_DB_LEASE_REAPER_BATCH`; switch it
// to `vault_db_lease_reaper_batch()` so the spawn honors
// `UDB_VAULT_DB_LEASE_REAPER_BATCH` (in-service clamp in `workers.rs` already does).
pub use config::{
    VAULT_DB_LEASE_REAPER_BATCH, vault_db_lease_reaper_batch, vault_db_lease_reaper_interval,
};
pub use workers::run_vault_db_lease_reaper_once;

/// Postgres-backed `VaultService` handler.
pub struct VaultServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `vault`).
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for the master-key envelope (KEK wrap/unwrap) and typed
    /// native-entity dispatch.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    /// Test-only seal override. `Some(true)` forces SEALED (exercise the
    /// fail-closed gate); `Some(false)` forces UNSEALED (exercise the
    /// cross-tenant guard without a configured master key); `None` in production
    /// probes the real master KEK. Never set by `build_vault_service`.
    pub(crate) seal_override: Option<bool>,
}

impl VaultServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            seal_override: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_seal_override(mut self, sealed: bool) -> Self {
        self.seal_override = Some(sealed);
        self
    }

    /// Vault state is durable-only: fail closed when no runtime dispatch exists.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            vault_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                VAULT_RUNTIME_REQUIRED_MESSAGE,
            )
        })
    }

    /// The SEAL GATE. Every operating handler calls this FIRST: a sealed vault
    /// (master key unavailable) refuses to serve — never degrade to plaintext.
    /// In production this probes the real master KEK via `encrypt_secret_at_rest`
    /// (which fails closed when `fail_closed_mode` is on and no key is set). The
    /// test override short-circuits the probe.
    pub(crate) fn check_seal(&self) -> Result<(), Status> {
        match self.seal_override {
            Some(true) => {
                return Err(vault_master_key_unavailable_status(
                    VAULT_MASTER_KEY_UNAVAILABLE_MESSAGE,
                ));
            }
            Some(false) => return Ok(()),
            None => {}
        }
        let runtime = self.runtime.as_deref().ok_or_else(|| {
            vault_master_key_unavailable_status("vault is sealed: no runtime / master key wired")
        })?;
        runtime
            .encrypt_secret_at_rest(SEAL_PROBE)
            .map(|_| ())
            .map_err(|err| {
                vault_master_key_unavailable_status(format!(
                    "vault is sealed: master key unavailable ({err})"
                ))
            })
    }

    /// Whether the vault is currently sealed (for `SealStatus`).
    pub(crate) fn is_sealed(&self) -> bool {
        self.check_seal().is_err()
    }

    /// Whether a REAL master KEK is configured (vs the dev passthrough where
    /// `encrypt_secret_at_rest` returns the cleartext unchanged). Used by
    /// `SealStatus` to report honest posture.
    pub(crate) fn kek_configured(&self) -> bool {
        if self.seal_override.is_some() {
            return false;
        }
        match self.runtime.as_deref() {
            Some(runtime) => runtime
                .encrypt_secret_at_rest(SEAL_PROBE)
                .map(|env| env.starts_with("udb-aead:"))
                .unwrap_or(false),
            None => false,
        }
    }

    /// All durable secret versions for one path (bounded scan).
    pub(crate) async fn read_secret_versions(
        &self,
        runtime: &DataBrokerRuntime,
        context: &crate::RequestContext,
        tenant_id: &str,
        secret_path: &str,
    ) -> Result<Vec<StoredSecret>, Status> {
        let rows = runtime
            .native_entity_read_for_service(
                "vault",
                context,
                secret_path_read(tenant_id, secret_path),
            )
            .await?;
        Ok(rows.iter().map(stored_secret_from_json).collect())
    }

    /// All durable transit-key versions for one key name (bounded scan).
    pub(crate) async fn read_transit_versions(
        &self,
        runtime: &DataBrokerRuntime,
        context: &crate::RequestContext,
        tenant_id: &str,
        key_name: &str,
    ) -> Result<Vec<StoredTransitKey>, Status> {
        let rows = runtime
            .native_entity_read_for_service("vault", context, transit_key_read(tenant_id, key_name))
            .await?;
        Ok(rows.iter().map(stored_transit_key_from_json).collect())
    }
}

impl Default for VaultServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl VaultService for VaultServiceImpl {
    // ── KV engine ─────────────────────────────────────────────────────────────

    async fn put_secret(
        &self,
        request: Request<vault_pb::PutSecretRequest>,
    ) -> Result<Response<vault_pb::PutSecretResponse>, Status> {
        handlers::put_secret(self, request).await
    }

    async fn get_secret(
        &self,
        request: Request<vault_pb::GetSecretRequest>,
    ) -> Result<Response<vault_pb::GetSecretResponse>, Status> {
        handlers::get_secret(self, request).await
    }

    async fn list_secrets(
        &self,
        request: Request<vault_pb::ListSecretsRequest>,
    ) -> Result<Response<vault_pb::ListSecretsResponse>, Status> {
        handlers::list_secrets(self, request).await
    }

    async fn delete_secret(
        &self,
        request: Request<vault_pb::DeleteSecretRequest>,
    ) -> Result<Response<vault_pb::DeleteSecretResponse>, Status> {
        handlers::delete_secret(self, request).await
    }

    async fn undelete_secret(
        &self,
        request: Request<vault_pb::UndeleteSecretRequest>,
    ) -> Result<Response<vault_pb::UndeleteSecretResponse>, Status> {
        handlers::undelete_secret(self, request).await
    }

    async fn destroy_secret(
        &self,
        request: Request<vault_pb::DestroySecretRequest>,
    ) -> Result<Response<vault_pb::DestroySecretResponse>, Status> {
        handlers::destroy_secret(self, request).await
    }

    // ── Transit engine ────────────────────────────────────────────────────────

    async fn create_transit_key(
        &self,
        request: Request<vault_pb::CreateTransitKeyRequest>,
    ) -> Result<Response<vault_pb::CreateTransitKeyResponse>, Status> {
        handlers::create_transit_key(self, request).await
    }

    async fn rotate_transit_key(
        &self,
        request: Request<vault_pb::RotateTransitKeyRequest>,
    ) -> Result<Response<vault_pb::RotateTransitKeyResponse>, Status> {
        handlers::rotate_transit_key(self, request).await
    }

    async fn encrypt(
        &self,
        request: Request<vault_pb::EncryptRequest>,
    ) -> Result<Response<vault_pb::EncryptResponse>, Status> {
        handlers::encrypt(self, request).await
    }

    async fn decrypt(
        &self,
        request: Request<vault_pb::DecryptRequest>,
    ) -> Result<Response<vault_pb::DecryptResponse>, Status> {
        handlers::decrypt(self, request).await
    }

    async fn batch_encrypt(
        &self,
        request: Request<vault_pb::BatchEncryptRequest>,
    ) -> Result<Response<vault_pb::BatchEncryptResponse>, Status> {
        handlers::batch_encrypt(self, request).await
    }

    async fn batch_decrypt(
        &self,
        request: Request<vault_pb::BatchDecryptRequest>,
    ) -> Result<Response<vault_pb::BatchDecryptResponse>, Status> {
        handlers::batch_decrypt(self, request).await
    }

    async fn generate_data_key(
        &self,
        request: Request<vault_pb::GenerateDataKeyRequest>,
    ) -> Result<Response<vault_pb::GenerateDataKeyResponse>, Status> {
        handlers::generate_data_key(self, request).await
    }

    async fn rewrap(
        &self,
        request: Request<vault_pb::RewrapRequest>,
    ) -> Result<Response<vault_pb::RewrapResponse>, Status> {
        handlers::rewrap(self, request).await
    }

    async fn get_transit_public_key(
        &self,
        request: Request<vault_pb::GetTransitPublicKeyRequest>,
    ) -> Result<Response<vault_pb::GetTransitPublicKeyResponse>, Status> {
        handlers::get_transit_public_key(self, request).await
    }

    async fn sign(
        &self,
        request: Request<vault_pb::SignRequest>,
    ) -> Result<Response<vault_pb::SignResponse>, Status> {
        handlers::sign(self, request).await
    }

    async fn verify(
        &self,
        request: Request<vault_pb::VerifyRequest>,
    ) -> Result<Response<vault_pb::VerifyResponse>, Status> {
        handlers::verify(self, request).await
    }

    async fn hmac(
        &self,
        request: Request<vault_pb::HmacRequest>,
    ) -> Result<Response<vault_pb::HmacResponse>, Status> {
        handlers::hmac(self, request).await
    }

    // ── Seal engine ─────────────────────────────────────────────────────────

    async fn seal_status(
        &self,
        request: Request<vault_pb::SealStatusRequest>,
    ) -> Result<Response<vault_pb::SealStatusResponse>, Status> {
        handlers::seal_status(self, request).await
    }

    // ── Dynamic database credentials ─────────────────────────────────────────

    async fn generate_database_credentials(
        &self,
        request: Request<vault_pb::GenerateDatabaseCredentialsRequest>,
    ) -> Result<Response<vault_pb::GenerateDatabaseCredentialsResponse>, Status> {
        handlers::generate_database_credentials(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `VaultService`, wired to the broker's Postgres pool, the
    /// master-key envelope runtime, and the shared outbox.
    pub(crate) fn build_vault_service(&self) -> VaultServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then
        // a health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("vault", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        VaultServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
