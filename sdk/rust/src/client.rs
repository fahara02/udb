//! The data-plane client.
//!
//! Every method routes through [`UdbClient::request`], which is the single point
//! that applies connection metadata. That is deliberate: if applying tenant scope
//! were the caller's job, forgetting it once would be a cross-tenant read, and
//! nothing in the type system would object.

use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::metadata::Metadata;
use crate::proto::udb::entity::v1 as entity;
use crate::proto::udb::services::v1::data_broker_client::DataBrokerClient;

/// A connected UDB data-plane client bound to one identity.
#[derive(Clone, Debug)]
pub struct UdbClient {
    inner: DataBrokerClient<Channel>,
    meta: Metadata,
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
        }
    }

    /// Replace the bearer credential, e.g. after a token refresh.
    pub fn with_bearer_token(&self, token: impl Into<String>) -> Self {
        Self {
            inner: self.inner.clone(),
            meta: self.meta.clone().with_bearer_token(token),
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

    pub async fn select(
        &mut self,
        req: entity::SelectRequest,
    ) -> Result<entity::RecordSet, Status> {
        let req = self.request(req)?;
        Ok(self.inner.select(req).await?.into_inner())
    }

    pub async fn upsert(
        &mut self,
        req: entity::UpsertRequest,
    ) -> Result<entity::MutationResponse, Status> {
        let req = self.request(req)?;
        Ok(self.inner.upsert(req).await?.into_inner())
    }

    pub async fn update(
        &mut self,
        req: entity::UpdateRequest,
    ) -> Result<entity::MutationResponse, Status> {
        let req = self.request(req)?;
        Ok(self.inner.update(req).await?.into_inner())
    }

    pub async fn delete(
        &mut self,
        req: entity::DeleteRequest,
    ) -> Result<entity::MutationResponse, Status> {
        let req = self.request(req)?;
        Ok(self.inner.delete(req).await?.into_inner())
    }

    pub async fn bulk_cas(
        &mut self,
        req: entity::BulkCasRequest,
    ) -> Result<entity::BulkCasResponse, Status> {
        let req = self.request(req)?;
        Ok(self.inner.bulk_cas(req).await?.into_inner())
    }

    pub async fn vector_search(
        &mut self,
        req: entity::VectorSearchRequest,
    ) -> Result<entity::VectorSet, Status> {
        let req = self.request(req)?;
        Ok(self.inner.vector_search(req).await?.into_inner())
    }

    pub async fn vector_upsert(
        &mut self,
        req: entity::VectorUpsertRequest,
    ) -> Result<entity::MutationResponse, Status> {
        let req = self.request(req)?;
        Ok(self.inner.vector_upsert(req).await?.into_inner())
    }
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
