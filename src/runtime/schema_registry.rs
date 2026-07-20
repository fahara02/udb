//! Runtime schema registry for data operations (U13).
//!
//! Pre-U13 the catalog had **two** schema-aware surfaces:
//!
//! - **CDC schema registry** (`runtime::cdc::SchemaRegistryMode`): registers
//!   event payload shapes with an external HTTP service for downstream
//!   consumers. Cares about event publishing, not RPC requests.
//! - **`CatalogManager::is_compatible`**: a yes/no compatibility check used
//!   only on the catalog activation / migration path.
//!
//! Neither covers what every gRPC RPC needs to know on the hot path:
//!
//! > For this `project_id` + `client_catalog_version`, am I allowed to
//! > read/write `Customer`, and what fields does the broker expect today?
//!
//! `SchemaRegistry` is that runtime surface. It:
//!
//! - Negotiates the caller's `client_catalog_version` against the
//!   active project catalog using the existing
//!   [`CatalogManager::compatibility_error`] ladder.
//! - Surfaces a per-message [`MessageDescriptor`] — field names, SQL types,
//!   primary key, manifest checksum — so SDKs can validate payloads at
//!   bind time and cache the descriptor per `(project, version, message)`.
//! - Returns typed [`NegotiationOutcome`] verdicts so handlers map
//!   compat failures to the right gRPC `tonic::Code` instead of mixing
//!   "not found" vs "incompatible" into one error.
//!
//! The U13 acceptance gate ("Old client can continue under
//! backward-compatible catalog changes and fails clearly under breaking
//! changes") is verified by the tests in this file: they pin the
//! compatibility ladder so a future regression doesn't silently widen
//! the compat envelope.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::generation::manifest::ManifestColumn;
use crate::runtime::catalog::{CatalogManager, CatalogState};

/// What the broker says about a message type a client is asking about.
/// SDKs cache by `(project, version, message_type)` to avoid round-tripping
/// on every RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageDescriptor {
    /// Canonical proto message name (`acme.billing.v1.Customer`).
    pub message_type: String,
    /// Project this descriptor was resolved against.
    pub project_id: String,
    /// Catalog version at lookup time. Pair with `manifest_checksum` to
    /// detect drift between the cache and the live broker.
    pub catalog_version: String,
    /// SHA-256 of the catalog manifest. Changes on every schema migration
    /// — clients use it as a cache key.
    pub manifest_checksum: String,
    /// Physical SQL location (used by some clients for direct queries; the
    /// broker still mediates all access, but it's useful for tooling).
    pub schema: String,
    pub table: String,
    pub primary_key: Vec<String>,
    pub fields: Vec<FieldDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDescriptor {
    pub name: String,
    pub column_name: String,
    pub proto_type: String,
    pub sql_type: String,
    pub not_null: bool,
    pub is_primary: bool,
    pub is_array: bool,
}

impl From<&ManifestColumn> for FieldDescriptor {
    fn from(col: &ManifestColumn) -> Self {
        Self {
            name: col.field_name.clone(),
            column_name: col.column_name.clone(),
            proto_type: col.proto_type.clone(),
            sql_type: col.sql_type.clone(),
            not_null: col.not_null,
            is_primary: col.is_primary,
            is_array: col.is_array,
        }
    }
}

/// Typed outcome of `negotiate_version`. The handler maps these to
/// `tonic::Code` directly — no string parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NegotiationOutcome {
    /// Client and broker agree (exact version match, or client passed no
    /// version so the broker's active version is implied).
    Match {
        active_version: String,
        manifest_checksum: String,
    },
    /// The client's version is older but the broker's compat policy says
    /// it's still allowed (`backward` mode, same major). The client may
    /// continue; the response should carry an upgrade hint.
    BackwardCompatible {
        active_version: String,
        client_version: String,
        manifest_checksum: String,
    },
    /// **Breaking** — the client's version doesn't satisfy the active
    /// catalog's compatibility policy. The handler should map this to
    /// `tonic::Code::FailedPrecondition` with a clear remediation string.
    Incompatible {
        active_version: String,
        client_version: String,
        reason: String,
    },
}

impl NegotiationOutcome {
    /// True when the client may proceed (Match or BackwardCompatible).
    pub fn may_proceed(&self) -> bool {
        !matches!(self, Self::Incompatible { .. })
    }
}

/// Errors `SchemaRegistry::lookup_message` returns. Distinct from
/// `NegotiationOutcome` because not-found and incompatible are different
/// failure shapes (one's `NotFound`, the other's `FailedPrecondition`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LookupError {
    /// The active manifest doesn't declare this message type.
    MessageNotFound {
        project_id: String,
        message_type: String,
    },
    /// A convenience name matched more than one composed-catalog entity.
    MessageAmbiguous {
        project_id: String,
        message_type: String,
        candidates: Vec<String>,
    },
    /// The client's version isn't compatible with the active catalog.
    Incompatible {
        project_id: String,
        message_type: String,
        client_version: String,
        active_version: String,
        reason: String,
    },
}

/// The runtime-side wrapper around `CatalogManager` that handles
/// per-RPC schema queries.
#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    catalog: Arc<CatalogManager>,
}

impl SchemaRegistry {
    pub fn new(catalog: Arc<CatalogManager>) -> Self {
        Self { catalog }
    }

    /// Negotiate the client's catalog version against the project's
    /// active catalog. Empty `client_version` is allowed (lenient mode)
    /// and resolves to `Match` against whatever the active catalog is.
    pub fn negotiate_version(&self, project_id: &str, client_version: &str) -> NegotiationOutcome {
        let active: Arc<CatalogState> = self.catalog.active_for(project_id);
        let active_version = active.metadata.version.clone();
        let manifest_checksum = active.metadata.checksum.clone();

        if client_version.trim().is_empty() {
            return NegotiationOutcome::Match {
                active_version,
                manifest_checksum,
            };
        }
        match self.catalog.compatibility_error(client_version, project_id) {
            None if active_version == client_version => NegotiationOutcome::Match {
                active_version,
                manifest_checksum,
            },
            None => NegotiationOutcome::BackwardCompatible {
                active_version,
                client_version: client_version.to_string(),
                manifest_checksum,
            },
            Some(reason) => NegotiationOutcome::Incompatible {
                active_version,
                client_version: client_version.to_string(),
                reason,
            },
        }
    }

    /// Look up a message descriptor for `(project_id, message_type)`
    /// honouring the client's compatibility envelope. SDKs cache the
    /// returned descriptor by `(project, version, message_type)`.
    pub fn lookup_message(
        &self,
        project_id: &str,
        message_type: &str,
        client_version: &str,
    ) -> Result<MessageDescriptor, LookupError> {
        // Compatibility check first — refuse to expose a descriptor a
        // client can't legally bind against.
        let outcome = self.negotiate_version(project_id, client_version);
        if let NegotiationOutcome::Incompatible {
            active_version,
            client_version,
            reason,
        } = &outcome
        {
            return Err(LookupError::Incompatible {
                project_id: project_id.to_string(),
                message_type: message_type.to_string(),
                client_version: client_version.clone(),
                active_version: active_version.clone(),
                reason: reason.clone(),
            });
        }

        let active = self.catalog.active_for(project_id);
        let table = match crate::broker::resolve_table_for_message(&active.manifest, message_type) {
            Ok(table) => table,
            Err(crate::broker::TableLookupError::Missing { .. }) => {
                return Err(LookupError::MessageNotFound {
                    project_id: project_id.to_string(),
                    message_type: message_type.to_string(),
                });
            }
            Err(crate::broker::TableLookupError::Ambiguous { candidates, .. }) => {
                return Err(LookupError::MessageAmbiguous {
                    project_id: project_id.to_string(),
                    message_type: message_type.to_string(),
                    candidates,
                });
            }
        };

        Ok(MessageDescriptor {
            message_type: table.message_name.clone(),
            project_id: project_id.to_string(),
            catalog_version: active.metadata.version.clone(),
            manifest_checksum: active.metadata.checksum.clone(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            primary_key: table.primary_key.clone(),
            fields: table.columns.iter().map(FieldDescriptor::from).collect(),
        })
    }

    /// Enumerate every message type the active catalog declares for
    /// `project_id` — used by the `ListMessageSchemas` admin RPC and the
    /// portal's schema browser.
    pub fn list_messages(&self, project_id: &str) -> Vec<String> {
        let active = self.catalog.active_for(project_id);
        active
            .manifest
            .tables
            .iter()
            .map(|t| t.message_name.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{CatalogManifest, ManifestColumn, ManifestTable};

    fn manifest_with_customer(checksum: &str) -> CatalogManifest {
        CatalogManifest {
            checksum_sha256: checksum.to_string(),
            tables: vec![ManifestTable {
                checksum_sha256: checksum.to_string(),
                message_name: "acme.billing.v1.Customer".to_string(),
                schema: "public".to_string(),
                table: "customers".to_string(),
                primary_key: vec!["id".to_string()],
                columns: vec![
                    ManifestColumn {
                        field_name: "id".into(),
                        column_name: "id".into(),
                        proto_type: "string".into(),
                        sql_type: "uuid".into(),
                        is_primary: true,
                        not_null: true,
                        ..Default::default()
                    },
                    ManifestColumn {
                        field_name: "email".into(),
                        column_name: "email".into(),
                        proto_type: "string".into(),
                        sql_type: "text".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn registry_with_project(version: &str, compat: &str) -> SchemaRegistry {
        let catalog = Arc::new(CatalogManager::new(manifest_with_customer("default")));
        catalog
            .stage_catalog(
                manifest_with_customer("billing-v1"),
                "billing".into(),
                version.into(),
                compat.into(),
            )
            .await
            .unwrap();
        catalog
            .activate_catalog_for("billing", version)
            .await
            .unwrap();
        SchemaRegistry::new(catalog)
    }

    #[tokio::test]
    async fn empty_client_version_matches_active() {
        let registry = registry_with_project("2.0.0", "backward").await;
        let outcome = registry.negotiate_version("billing", "");
        match outcome {
            NegotiationOutcome::Match { active_version, .. } => {
                assert_eq!(active_version, "2.0.0");
            }
            other => panic!("expected Match, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exact_version_match() {
        let registry = registry_with_project("2.0.0", "backward").await;
        let outcome = registry.negotiate_version("billing", "2.0.0");
        assert!(matches!(outcome, NegotiationOutcome::Match { .. }));
    }

    /// **Acceptance gate**: "Old client can continue under backward-
    /// compatible catalog changes." A 1.x client with a backward-compat
    /// 2.x active catalog gets `BackwardCompatible`, not `Incompatible`.
    #[tokio::test]
    async fn older_client_passes_under_backward_compat() {
        // Active: 2.5.0 in "backward" mode. Client: 2.1.0 (same major,
        // older minor). Must be BackwardCompatible.
        let registry = registry_with_project("2.5.0", "backward").await;
        let outcome = registry.negotiate_version("billing", "2.1.0");
        assert!(
            matches!(outcome, NegotiationOutcome::BackwardCompatible { .. }),
            "got {outcome:?}"
        );
        assert!(outcome.may_proceed());
    }

    /// **Acceptance gate**: "Fails clearly under breaking changes." A
    /// cross-major-version client gets `Incompatible` with a typed
    /// reason the handler maps to `FailedPrecondition`.
    #[tokio::test]
    async fn breaking_change_returns_incompatible_with_reason() {
        // Active: 2.5.0, client: 3.0.0 (new major — broker can't serve
        // unreleased client expectations).
        let registry = registry_with_project("2.5.0", "backward").await;
        let outcome = registry.negotiate_version("billing", "3.0.0");
        match outcome {
            NegotiationOutcome::Incompatible {
                client_version,
                reason,
                ..
            } => {
                assert_eq!(client_version, "3.0.0");
                assert!(reason.contains("backward-compatible") || reason.contains("compat"));
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    /// `exact` compat mode is stricter than `backward` — even minor-
    /// version drift fails. SDKs that need byte-exact behaviour can
    /// request this mode at catalog stage time.
    #[tokio::test]
    async fn exact_mode_rejects_any_drift() {
        let registry = registry_with_project("2.5.0", "exact").await;
        let outcome = registry.negotiate_version("billing", "2.5.1");
        assert!(matches!(outcome, NegotiationOutcome::Incompatible { .. }));
    }

    #[tokio::test]
    async fn lookup_returns_descriptor_with_fields() {
        let registry = registry_with_project("1.0.0", "backward").await;
        let descriptor = registry
            .lookup_message("billing", "acme.billing.v1.Customer", "1.0.0")
            .expect("descriptor");
        assert_eq!(descriptor.message_type, "acme.billing.v1.Customer");
        assert_eq!(descriptor.project_id, "billing");
        assert_eq!(descriptor.schema, "public");
        assert_eq!(descriptor.table, "customers");
        assert_eq!(descriptor.primary_key, vec!["id"]);
        assert_eq!(descriptor.fields.len(), 2);
        let pk = descriptor
            .fields
            .iter()
            .find(|f| f.is_primary)
            .expect("pk field");
        assert_eq!(pk.name, "id");
    }

    #[tokio::test]
    async fn lookup_unknown_message_returns_message_not_found() {
        let registry = registry_with_project("1.0.0", "backward").await;
        let err = registry
            .lookup_message("billing", "acme.billing.v1.UnknownThing", "1.0.0")
            .unwrap_err();
        assert!(matches!(err, LookupError::MessageNotFound { .. }));
    }

    #[tokio::test]
    async fn lookup_ambiguous_short_name_returns_candidates_and_exact_fqns_route() {
        let mut manifest = manifest_with_customer("ambiguous-default");
        let customer_columns = manifest.tables[0].columns.clone();
        manifest.tables.push(ManifestTable {
            message_name: "udb.core.billing.v1.Customer".to_string(),
            schema: "udb_billing".to_string(),
            table: "customers".to_string(),
            primary_key: vec!["id".to_string()],
            columns: customer_columns,
            ..Default::default()
        });
        let registry = SchemaRegistry::new(Arc::new(CatalogManager::new(manifest)));

        let error = registry
            .lookup_message("default", "Customer", "")
            .expect_err("colliding short names must not route first-wins");
        let LookupError::MessageAmbiguous { candidates, .. } = error else {
            panic!("short-name collision must remain typed as ambiguous");
        };
        assert_eq!(candidates.len(), 2);
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.contains("acme.billing.v1.Customer"))
        );
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.contains("udb.core.billing.v1.Customer"))
        );

        let consumer = registry
            .lookup_message("default", "acme.billing.v1.Customer", "")
            .expect("consumer FQN routes exactly");
        assert_eq!(consumer.schema, "public");
        let embedded = registry
            .lookup_message("default", "udb.core.billing.v1.Customer", "")
            .expect("embedded FQN routes exactly");
        assert_eq!(embedded.schema, "udb_billing");
    }

    #[tokio::test]
    async fn lookup_with_incompatible_client_returns_typed_error() {
        let registry = registry_with_project("2.0.0", "exact").await;
        let err = registry
            .lookup_message("billing", "acme.billing.v1.Customer", "2.5.0")
            .unwrap_err();
        assert!(matches!(err, LookupError::Incompatible { .. }));
    }

    #[tokio::test]
    async fn list_messages_enumerates_active_catalog() {
        let registry = registry_with_project("1.0.0", "backward").await;
        let messages = registry.list_messages("billing");
        assert_eq!(messages, vec!["acme.billing.v1.Customer".to_string()]);
    }

    /// Unknown project_id falls back to the default catalog (U4's
    /// back-compat shim). Schema registry inherits the same behaviour
    /// for free — a missing `x-udb-project-id` header doesn't poison
    /// lookups for callers running against the default project.
    #[tokio::test]
    async fn unknown_project_falls_back_to_default() {
        let registry = registry_with_project("1.0.0", "backward").await;
        // Lookup against unconfigured project — sees default's catalog,
        // which has the same Customer table (we seeded both projects
        // with the same fixture).
        let descriptor = registry
            .lookup_message("nonexistent-project", "acme.billing.v1.Customer", "")
            .expect("falls back to default");
        assert_eq!(descriptor.project_id, "nonexistent-project");
    }
}
