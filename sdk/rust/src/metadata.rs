//! The request metadata every UDB call carries.
//!
//! The broker derives tenant isolation, project scoping, and the audit trail from
//! these headers — they are not optional decoration. A call without
//! `x-tenant-id` is not a call with a default tenant; it is a call the broker
//! will refuse or scope to nothing.

use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::Request;

/// Header names, kept in one place so a typo cannot silently drop a tenant scope.
pub mod headers {
    pub const TENANT_ID: &str = "x-tenant-id";
    pub const USER_ID: &str = "x-user-id";
    pub const PROJECT_ID: &str = "x-udb-project-id";
    pub const PURPOSE: &str = "x-purpose";
    pub const CORRELATION_ID: &str = "x-correlation-id";
    pub const SERVICE_IDENTITY: &str = "x-service-identity";
    pub const CLIENT_CATALOG_VERSION: &str = "x-udb-client-catalog-version";
    pub const SCOPES: &str = "x-scopes";
    pub const AUTHORIZATION: &str = "authorization";
}

/// Identity and audit context for a connection.
///
/// Split deliberately into two halves, mirroring the other UDB SDKs:
///
/// - **Identity** — tenant, user, project, scopes, service identity — is
///   authoritative for the connection and is never overridden per request.
///   Letting a caller pass a different tenant per call would make the isolation
///   boundary a client-side suggestion.
/// - **Audit** — purpose, correlation id, catalog version — is request-scoped and
///   may be varied per call via [`Metadata::with_audit`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    pub tenant_id: String,
    pub user_id: String,
    pub project_id: String,
    pub purpose: String,
    pub correlation_id: String,
    pub service_identity: String,
    pub client_catalog_version: String,
    pub scopes: Vec<String>,
    /// Bearer credential. Sent as `authorization: Bearer <token>` when non-empty.
    pub bearer_token: String,
}

impl Metadata {
    /// Metadata for `tenant_id`, which is the minimum the broker requires.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            ..Default::default()
        }
    }

    pub fn with_project(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = project_id.into();
        self
    }

    pub fn with_user(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = token.into();
        self
    }

    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// Request-scoped audit fields, leaving identity untouched.
    pub fn with_audit(
        mut self,
        purpose: impl Into<String>,
        correlation_id: impl Into<String>,
    ) -> Self {
        self.purpose = purpose.into();
        self.correlation_id = correlation_id.into();
        self
    }

    /// Apply every non-empty field to an outgoing request.
    ///
    /// Empty values are omitted rather than sent blank: the broker treats a
    /// present-but-empty header differently from an absent one on some paths, and
    /// sending `x-scopes: ""` is not the same as claiming no scopes.
    pub fn apply<T>(&self, request: &mut Request<T>) -> Result<(), tonic::Status> {
        self.apply_to_map(request.metadata_mut())
    }

    pub fn apply_to_map(&self, md: &mut MetadataMap) -> Result<(), tonic::Status> {
        let joined_scopes = self.scopes.join(" ");
        let pairs = [
            (headers::TENANT_ID, self.tenant_id.as_str()),
            (headers::USER_ID, self.user_id.as_str()),
            (headers::PROJECT_ID, self.project_id.as_str()),
            (headers::PURPOSE, self.purpose.as_str()),
            (headers::CORRELATION_ID, self.correlation_id.as_str()),
            (headers::SERVICE_IDENTITY, self.service_identity.as_str()),
            (
                headers::CLIENT_CATALOG_VERSION,
                self.client_catalog_version.as_str(),
            ),
            (headers::SCOPES, joined_scopes.as_str()),
        ];
        for (name, value) in pairs {
            if value.is_empty() {
                continue;
            }
            insert(md, name, value)?;
        }
        if !self.bearer_token.is_empty() {
            insert(
                md,
                headers::AUTHORIZATION,
                &format!("Bearer {}", self.bearer_token),
            )?;
        }
        Ok(())
    }
}

fn insert(md: &mut MetadataMap, name: &'static str, value: &str) -> Result<(), tonic::Status> {
    let parsed: MetadataValue<_> = value.parse().map_err(|_| {
        // Naming the header without echoing the value: these carry tenant and
        // credential material and this error may well be logged.
        tonic::Status::invalid_argument(format!(
            "metadata header `{name}` is not a valid ASCII header value"
        ))
    })?;
    md.insert(name, parsed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_of(meta: &Metadata) -> MetadataMap {
        let mut md = MetadataMap::new();
        meta.apply_to_map(&mut md).expect("valid metadata");
        md
    }

    #[test]
    fn empty_fields_are_omitted_not_sent_blank() {
        let md = map_of(&Metadata::new("tenant-1"));
        assert_eq!(md.get(headers::TENANT_ID).unwrap(), "tenant-1");
        assert!(
            md.get(headers::USER_ID).is_none(),
            "empty user must be absent"
        );
        assert!(
            md.get(headers::SCOPES).is_none(),
            "no scopes must be absent"
        );
        assert!(
            md.get(headers::AUTHORIZATION).is_none(),
            "absent token must not send an empty Bearer"
        );
    }

    #[test]
    fn bearer_token_is_prefixed() {
        let md = map_of(&Metadata::new("t").with_bearer_token("abc.def"));
        assert_eq!(md.get(headers::AUTHORIZATION).unwrap(), "Bearer abc.def");
    }

    #[test]
    fn scopes_are_space_joined() {
        let md = map_of(&Metadata::new("t").with_scopes(["read", "write"]));
        assert_eq!(md.get(headers::SCOPES).unwrap(), "read write");
    }

    #[test]
    fn audit_fields_do_not_disturb_identity() {
        let base = Metadata::new("tenant-1")
            .with_project("proj-9")
            .with_user("user-1");
        let scoped = base.clone().with_audit("billing", "corr-123");
        assert_eq!(scoped.tenant_id, base.tenant_id);
        assert_eq!(scoped.project_id, base.project_id);
        assert_eq!(scoped.user_id, base.user_id);
        assert_eq!(scoped.purpose, "billing");
        assert_eq!(scoped.correlation_id, "corr-123");
    }

    #[test]
    fn invalid_header_value_names_the_header_but_not_the_value() {
        let mut meta = Metadata::new("tenant-1");
        meta.bearer_token = "sekret\u{7f}".to_string(); // DEL is not a legal header byte
        let mut md = MetadataMap::new();
        let err = meta.apply_to_map(&mut md).expect_err("must reject");
        assert!(err.message().contains(headers::AUTHORIZATION));
        assert!(
            !err.message().contains("sekret"),
            "credential material must not reach the error text"
        );
    }
}
