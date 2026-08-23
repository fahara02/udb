//! The data-plane client.
//!
//! Every method routes through [`UdbClient::request`], which is the single point
//! that applies connection metadata. That is deliberate: if applying tenant scope
//! were the caller's job, forgetting it once would be a cross-tenant read, and
//! nothing in the type system would object.

use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::error::{CallPolicy, UdbError};
use crate::metadata::Metadata;
use crate::proto::udb::entity::v1 as entity;
use crate::proto::udb::services::v1::data_broker_client::DataBrokerClient;

/// A connected UDB data-plane client bound to one identity.
#[derive(Clone, Debug)]
pub struct UdbClient {
    inner: DataBrokerClient<Channel>,
    meta: Metadata,
    /// Overrides the per-RPC default when set. Reads default to a retrying
    /// policy and mutations to a single attempt, so an override is only needed
    /// to tighten a deadline or to opt a mutation into retries the CALLER knows
    /// are safe (an idempotency key, say).
    policy: Option<CallPolicy>,
}

/// Generate an RPC wrapper with identical retry and deadline handling.
///
/// A macro rather than a generic helper because the closure would have to hand a
/// `&mut` client into an async block per attempt, and the lifetime dance costs
/// more clarity than the repetition saves. `$default` is the policy used when the
/// client carries no override.
macro_rules! rpc {
    ($(#[$m:meta])* $name:ident, $rpc:ident, $req:ty, $resp:ty, $path:expr) => {
        $(#[$m])*
        pub async fn $name(&mut self, req: $req) -> Result<$resp, UdbError> {
            let policy = self.policy.unwrap_or_else(|| CallPolicy::from_contract($path));
            let mut attempt: u32 = 1;
            loop {
                let mut request = self.request(req.clone())?;
                if let Some(deadline) = policy.deadline {
                    request.set_timeout(deadline);
                }
                match self.inner.$rpc(request).await {
                    Ok(response) => return Ok(response.into_inner()),
                    Err(status) => {
                        let err = UdbError::from_status(status);
                        if policy.should_retry(attempt, &err) {
                            tokio::time::sleep(policy.backoff_for(attempt, &err)).await;
                            attempt += 1;
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
        }
    };
}

impl UdbClient {
    /// Connect to a broker's data-plane listener (`:50051` by default).
    ///
    /// Note the port: UDB runs SEPARATE listeners with different authorization
    /// models — the data plane authorizes through Casbin, while the native
    /// service listener uses scope-based endpoint security. A credential accepted
    /// by one is not automatically accepted by the other, and the mismatch
    /// presents as a permissions error rather than a wrong-address error.
    pub async fn connect(
        endpoint: impl Into<String>,
        meta: Metadata,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self::with_channel(channel, meta))
    }

    /// Wrap an already-configured channel — use this for TLS, custom timeouts,
    /// load balancing, or interceptors.
    pub fn with_channel(channel: Channel, meta: Metadata) -> Self {
        Self {
            inner: DataBrokerClient::new(channel),
            meta,
            policy: None,
        }
    }

    /// The connection's metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.meta
    }

    /// A copy of this client carrying request-scoped audit fields.
    ///
    /// Identity is intentionally not settable here; see [`Metadata`].
    pub fn with_audit(
        &self,
        purpose: impl Into<String>,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            inner: self.inner.clone(),
            meta: self.meta.clone().with_audit(purpose, correlation_id),
            policy: self.policy,
        }
    }

    /// Replace the bearer credential, e.g. after a token refresh.
    pub fn with_bearer_token(&self, token: impl Into<String>) -> Self {
        Self {
            inner: self.inner.clone(),
            meta: self.meta.clone().with_bearer_token(token),
            policy: self.policy,
        }
    }

    /// Wrap a message in a request carrying this connection's metadata.
    ///
    /// Public so callers can reach an RPC this wrapper does not expose yet
    /// without hand-rolling — and without hand-rolling the headers.
    pub fn request<T>(&self, message: T) -> Result<Request<T>, Status> {
        let mut req = Request::new(message);
        self.meta.apply(&mut req)?;
        Ok(req)
    }

    /// The raw generated client, for RPCs this wrapper does not cover.
    ///
    /// Pair it with [`UdbClient::request`] so metadata is still applied.
    pub fn raw(&mut self) -> &mut DataBrokerClient<Channel> {
        &mut self.inner
    }

    /// Apply a deadline/retry policy to every call this client makes.
    pub fn with_policy(&self, policy: CallPolicy) -> Self {
        Self {
            inner: self.inner.clone(),
            meta: self.meta.clone(),
            policy: Some(policy),
        }
    }

    rpc!(
        /// Read. Retry policy comes from the contract, not this wrapper.
        select,
        select,
        entity::SelectRequest,
        entity::RecordSet,
        "/udb.services.v1.DataBroker/Select"
    );
    rpc!(
        /// Write. The contract declares it replayable, so it retries.
        upsert,
        upsert,
        entity::UpsertRequest,
        entity::MutationResponse,
        "/udb.services.v1.DataBroker/Upsert"
    );
    rpc!(
        /// Write. Contract-declared replayable.
        update,
        update,
        entity::UpdateRequest,
        entity::MutationResponse,
        "/udb.services.v1.DataBroker/Update"
    );
    rpc!(
        /// Write. Contract-declared replayable.
        delete,
        delete,
        entity::DeleteRequest,
        entity::MutationResponse,
        "/udb.services.v1.DataBroker/Delete"
    );
    rpc!(
        /// Compare-and-swap. The contract does NOT declare it replayable: a CAS
        /// that timed out may have committed, and a repeat would compare against
        /// state its own first attempt wrote.
        bulk_cas,
        bulk_cas,
        entity::BulkCasRequest,
        entity::BulkCasResponse,
        "/udb.services.v1.DataBroker/BulkCas"
    );
    rpc!(
        /// Read.
        vector_search,
        vector_search,
        entity::VectorSearchRequest,
        entity::VectorSet,
        "/udb.services.v1.DataBroker/VectorSearch"
    );
    rpc!(
        /// Write. Not contract-declared replayable.
        vector_upsert,
        vector_upsert,
        entity::VectorUpsertRequest,
        entity::MutationResponse,
        "/udb.services.v1.DataBroker/VectorUpsert"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::headers;

    // `connect_lazy` registers with the Tokio reactor even though it dials
    // nothing, so these must run inside a runtime. They exercise metadata
    // assembly, not transport: nothing needs to be listening.
    fn client() -> UdbClient {
        let channel = Endpoint::from_static("http://127.0.0.1:50051").connect_lazy();
        UdbClient::with_channel(
            channel,
            Metadata::new("tenant-1")
                .with_project("proj-9")
                .with_bearer_token("tok"),
        )
    }

    #[tokio::test]
    async fn request_carries_connection_identity() {
        let c = client();
        let req = c.request(()).expect("metadata applies");
        let md = req.metadata();
        assert_eq!(md.get(headers::TENANT_ID).unwrap(), "tenant-1");
        assert_eq!(md.get(headers::PROJECT_ID).unwrap(), "proj-9");
        assert_eq!(md.get(headers::AUTHORIZATION).unwrap(), "Bearer tok");
    }

    #[tokio::test]
    async fn with_audit_keeps_identity_and_adds_audit() {
        let c = client().with_audit("billing", "corr-7");
        let req = c.request(()).expect("metadata applies");
        let md = req.metadata();
        assert_eq!(md.get(headers::TENANT_ID).unwrap(), "tenant-1");
        assert_eq!(md.get(headers::PURPOSE).unwrap(), "billing");
        assert_eq!(md.get(headers::CORRELATION_ID).unwrap(), "corr-7");
    }

    #[test]
    fn wrapped_rpc_paths_exist_in_the_registry() {
        // A typo in a path literal would make `from_contract` fall through to the
        // conservative default and silently stop retrying that RPC — a change
        // nothing else would catch.
        for path in [
            "/udb.services.v1.DataBroker/Select",
            "/udb.services.v1.DataBroker/Upsert",
            "/udb.services.v1.DataBroker/Update",
            "/udb.services.v1.DataBroker/Delete",
            "/udb.services.v1.DataBroker/BulkCas",
            "/udb.services.v1.DataBroker/VectorSearch",
            "/udb.services.v1.DataBroker/VectorUpsert",
        ] {
            assert!(
                crate::generated_rpcs::spec_for_path(path).is_some(),
                "{path} is not in the generated registry"
            );
        }
    }

    #[test]
    fn contract_drives_retry_and_disagrees_with_naive_naming() {
        use crate::generated_rpcs::is_retry_safe;
        // Reads: obviously repeatable.
        assert!(is_retry_safe("/udb.services.v1.DataBroker/Select"));
        // Mutations the CONTRACT declares replayable. A name-based guess would
        // refuse these, which is exactly the mistake this replaced.
        assert!(is_retry_safe("/udb.services.v1.DataBroker/Upsert"));
        assert!(is_retry_safe("/udb.services.v1.DataBroker/Delete"));
        // And ones it does not.
        assert!(!is_retry_safe("/udb.services.v1.DataBroker/BulkCas"));
        assert!(!is_retry_safe("/udb.services.v1.DataBroker/VectorUpsert"));
    }

    #[tokio::test]
    async fn with_bearer_token_replaces_only_the_credential() {
        let c = client().with_bearer_token("rotated");
        assert_eq!(c.metadata().tenant_id, "tenant-1");
        let req = c.request(()).expect("metadata applies");
        assert_eq!(
            req.metadata().get(headers::AUTHORIZATION).unwrap(),
            "Bearer rotated"
        );
    }
}
