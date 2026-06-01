//! Stage 1 auth gRPC handlers, split across files so authn/authz can be worked
//! on in parallel:
//! - [`mappings`]: proto↔runtime conversions + Postgres row helpers.
//! - [`authn`]: [`AuthnServiceImpl`] implementing `AuthnService`.
//! - [`authz`]: [`AuthzServiceImpl`] implementing `AuthzService`.
//!
//! Scope is the Stage-1 control-plane surface from `AUTH_NATIVE_ACCESS_PLAN.md`
//! plus the Stage-2 native-access/policy-bundle additions: `Authenticate`,
//! session lifecycle, JWT signing + refresh, TOTP MFA, CSRF validation,
//! `Authorize`/`GetNativeAccess`/`GetPolicyBundle`, the `Put*`/`Lint` mutators,
//! the snapshot-backed policy/check endpoints, role/policy CRUD, and API-key
//! lifecycle + usage stats. Every RPC in these services is implemented; the only
//! deferred authn primitive is outbound OTP delivery (needs an external channel).
//!
//! gRPC handlers return the concrete generated response types; the REST
//! `ApiResponse`/`RawJsonResponse` envelope (`core/common/v1`) is applied by the
//! gateway transcoding layer, not inside these handlers.

mod apikey;
mod authn;
mod authz;
mod events;
mod mappings;

#[cfg(test)]
mod tests;

use std::sync::Arc;

pub use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyServiceServer;
pub use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnServiceServer;
pub use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzServiceServer;

pub use apikey::ApiKeyServiceImpl;
pub use authn::AuthnServiceImpl;
pub use authz::AuthzServiceImpl;

#[cfg(feature = "redis")]
use crate::runtime::authn::RedisSessionStore;
use crate::runtime::authn::{
    AuthnConfig, PostgresApiKeyStore, PostgresSessionStore, PostgresUserStore, SessionStore,
};
use crate::runtime::authz::AuthzSnapshot;
use crate::runtime::security::SecurityConfig;

use super::DataBrokerService;

/// Current unix time in seconds.
pub(super) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl DataBrokerService {
    /// Build the Stage-1 auth services for the gRPC server, seeding the authz
    /// snapshot from the broker's currently-loaded ABAC policies so both share
    /// one policy view during migration. When a Postgres pool is available the
    /// authz service reads/writes its durable policy/role/relationship tables.
    pub(crate) fn build_auth_services(
        &self,
    ) -> (AuthnServiceImpl, AuthzServiceImpl, ApiKeyServiceImpl) {
        let policies = self
            .abac_policies
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let mut snapshot = AuthzSnapshot::from_abac_policies("live-abac", &policies);
        snapshot.default_allow = self.abac_default_allow;
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        let authn_config = AuthnConfig::from_env();
        let security = SecurityConfig::current();
        // Wire the outbox-backed event sink so native auth mutations publish
        // their domain events to Kafka via the CDC relay. Falls back to a no-op
        // when no Postgres pool is available (the events have nowhere durable to
        // land without the outbox table).
        let event_sink: Arc<dyn events::AuthEventSink> = match pg_pool.clone() {
            Some(pool) => Arc::new(events::OutboxAuthEventSink::new(
                pool,
                runtime.config().cdc.outbox_relation(),
            )),
            None => events::noop_sink(),
        };
        let (authn_service, api_key_service) = if let Some(pool) = pg_pool.clone() {
            let api_key_store = Arc::new(PostgresApiKeyStore::new(pool.clone(), ""));
            let session_store: Arc<dyn SessionStore> =
                if authn_config.session_backend.eq_ignore_ascii_case("redis") {
                    #[cfg(feature = "redis")]
                    {
                        if let Some(redis) = runtime.redis_clone() {
                            Arc::new(RedisSessionStore::new(
                                redis,
                                "udb:authn",
                                authn_config.session_ttl_secs,
                            ))
                        } else {
                            Arc::new(PostgresSessionStore::new(pool.clone(), ""))
                        }
                    }
                    #[cfg(not(feature = "redis"))]
                    {
                        Arc::new(PostgresSessionStore::new(pool.clone(), ""))
                    }
                } else {
                    Arc::new(PostgresSessionStore::new(pool.clone(), ""))
                };
            (
                AuthnServiceImpl::with_stores(
                    authn_config.clone(),
                    security,
                    session_store,
                    api_key_store.clone(),
                    Arc::new(PostgresUserStore::new(pool.clone(), "")),
                )
                .with_event_sink(event_sink.clone()),
                ApiKeyServiceImpl::with_store(authn_config, api_key_store)
                    .with_postgres(Some(pool.clone()))
                    .with_event_sink(event_sink.clone()),
            )
        } else {
            (
                AuthnServiceImpl::new(authn_config.clone(), security)
                    .with_event_sink(event_sink.clone()),
                ApiKeyServiceImpl::new(authn_config).with_event_sink(event_sink.clone()),
            )
        };
        (
            authn_service,
            AuthzServiceImpl::new(snapshot)
                .with_postgres(pg_pool)
                .with_event_sink(event_sink),
            api_key_service,
        )
    }
}
