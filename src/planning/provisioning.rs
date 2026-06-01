use serde::{Deserialize, Serialize};

use crate::backend::{BackendCapability, BackendKind};
use crate::generation::{CatalogManifest, ManifestStore, ManifestTable, UnifiedDsn};
use crate::observability::{MetricLabels, TraceContext};

pub const ACTION_ENSURE: &str = "ensure";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProvisioningPlan {
    pub actions: Vec<ProvisioningAction>,
    pub trace: TraceContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProvisioningAction {
    pub action: String,
    pub rollback_action: String,
    pub trace: TraceContext,
    pub metric_labels: MetricLabels,
    pub tier: String,
    pub store_kind: String,
    pub backend: String,
    pub resource_kind: String,
    pub resource_name: String,
    pub resource_uri: String,
    pub owner_schema: String,
    pub owner_table: String,
    pub dsn: String,
    pub capability: BackendCapability,
    pub parameters: Vec<ProvisioningParameter>,
    /// Dependency-ordering key (proto `migration_order`). Within a tier, actions
    /// are provisioned in ascending `migration_order` so an FK target table is
    /// created before the table that references it. Stores default high so they
    /// follow their owning tables.
    pub migration_order: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProvisioningParameter {
    pub key: String,
    pub value: String,
}

pub fn build_provisioning_plan(
    manifest: &CatalogManifest,
    dsn_entries: &[UnifiedDsn],
) -> ProvisioningPlan {
    try_build_provisioning_plan(manifest, dsn_entries).expect("provisioning plan requires DSNs")
}

pub fn try_build_provisioning_plan(
    manifest: &CatalogManifest,
    dsn_entries: &[UnifiedDsn],
) -> Result<ProvisioningPlan, String> {
    let mut actions = Vec::new();

    for table in &manifest.tables {
        actions.push(sql_table_action(table, dsn_entries));
    }

    for store in &manifest.stores {
        actions.push(store_action(store, dsn_entries)?);
    }

    // Order by migration_order (within tier+backend) FIRST so dependency targets
    // are provisioned before dependents (e.g. an FK target table before the table
    // that references it); the remaining keys keep the ordering deterministic.
    actions.sort_by(|a, b| {
        (
            a.tier.as_str(),
            a.backend.as_str(),
            a.migration_order,
            a.owner_schema.as_str(),
            a.owner_table.as_str(),
            a.resource_kind.as_str(),
            a.resource_name.as_str(),
        )
            .cmp(&(
                b.tier.as_str(),
                b.backend.as_str(),
                b.migration_order,
                b.owner_schema.as_str(),
                b.owner_table.as_str(),
                b.resource_kind.as_str(),
                b.resource_name.as_str(),
            ))
    });
    Ok(ProvisioningPlan {
        actions,
        trace: TraceContext::default(),
    })
}

fn sql_table_action(table: &ManifestTable, dsn_entries: &[UnifiedDsn]) -> ProvisioningAction {
    let backend = BackendKind::Postgres;
    let dsn = dsn_entries
        .iter()
        .find(|entry| {
            entry.store_kind == "sql"
                && entry.owner_schema == table.schema
                && entry.owner_table == table.table
        })
        .map(|entry| entry.dsn.clone())
        .unwrap_or_else(|| {
            format!(
                "{}://env:UDB_SQL_DSN/{}/{}",
                backend.dsn_scheme(),
                table.schema,
                table.table
            )
        });

    ProvisioningAction {
        action: ACTION_ENSURE.to_string(),
        rollback_action: "drop_table_requires_review".to_string(),
        trace: TraceContext::default(),
        metric_labels: MetricLabels {
            tier: backend.tier().as_str().to_string(),
            backend: backend.as_str().to_string(),
            resource_kind: "table".to_string(),
            operation: ACTION_ENSURE.to_string(),
        },
        tier: backend.tier().as_str().to_string(),
        store_kind: "sql".to_string(),
        backend: backend.as_str().to_string(),
        resource_kind: "table".to_string(),
        resource_name: format!("{}.{}", table.schema, table.table),
        resource_uri: format!("sql://{}/{}", table.schema, table.table),
        owner_schema: table.schema.clone(),
        owner_table: table.table.clone(),
        dsn,
        capability: backend.capabilities(),
        parameters: vec![
            param("migration_order", table.migration_order.to_string()),
            param("enable_rls", table.enable_rls.to_string()),
            param("force_rls", table.force_rls.to_string()),
            param("soft_delete", table.soft_delete.to_string()),
            param("audit_fields", table.audit_fields.to_string()),
        ],
        migration_order: table.migration_order,
    }
}

fn store_action(
    store: &ManifestStore,
    dsn_entries: &[UnifiedDsn],
) -> Result<ProvisioningAction, String> {
    let backend = BackendKind::from_store_kind(&store.store_kind, &store.backend);
    let backend_label = backend
        .as_ref()
        .map(|kind| kind.as_str().to_string())
        .unwrap_or_else(|| {
            if store.backend.trim().is_empty() {
                "generic".to_string()
            } else {
                store.backend.clone()
            }
        });
    let tier = backend
        .as_ref()
        .map(|kind| kind.tier().as_str().to_string())
        .unwrap_or_else(|| store.store_kind.clone());
    let capability = backend
        .as_ref()
        .map(BackendKind::capabilities)
        .unwrap_or_default();
    let dsn = dsn_entries
        .iter()
        .find(|entry| {
            entry.store_kind == store.store_kind
                && entry.backend == backend_label
                && entry.owner_schema == store.owner_schema
                && entry.owner_table == store.owner_table
                // Exact final-path-segment match, not substring: a substring
                // `.contains("bucket")` also matches "bucket_data", binding the
                // wrong DSN unpredictably. Empty resource_name keeps the prior
                // "owner_schema/owner_table already pin it" passthrough.
                && (store.resource_name.is_empty()
                    || entry
                        .resource_uri
                        .rsplit('/')
                        .next()
                        .map(|seg| seg == store.resource_name)
                        .unwrap_or(false)
                    || entry.resource_uri == store.resource_name)
        })
        .map(|entry| entry.dsn.clone())
        .ok_or_else(|| {
            format!(
                "missing DSN for store {} backend {} resource {} owned by {}.{}",
                store.store_kind,
                backend_label,
                store.resource_name,
                store.owner_schema,
                store.owner_table
            )
        })?;

    let resource_kind = resource_kind(store);
    Ok(ProvisioningAction {
        action: ACTION_ENSURE.to_string(),
        rollback_action: rollback_action(store),
        trace: TraceContext::default(),
        metric_labels: MetricLabels {
            tier: tier.clone(),
            backend: backend_label.clone(),
            resource_kind: resource_kind.clone(),
            operation: ACTION_ENSURE.to_string(),
        },
        tier,
        store_kind: store.store_kind.clone(),
        backend: backend_label.clone(),
        resource_kind,
        resource_name: store.resource_name.clone(),
        resource_uri: format!(
            "{}://{}/{}",
            store.store_kind,
            backend_label,
            store_path(store)
        ),
        owner_schema: store.owner_schema.clone(),
        owner_table: store.owner_table.clone(),
        dsn,
        capability,
        parameters: store
            .options
            .iter()
            .map(|option| param(&option.key, option.value.clone()))
            .collect(),
        // Stores follow their owning tables; default high so SQL-tier tables
        // (ordered by their real migration_order) provision first.
        migration_order: crate::ast::DEFAULT_MIGRATION_ORDER,
    })
}

fn rollback_action(store: &ManifestStore) -> String {
    match store.store_kind.as_str() {
        "cache" | "kv" | "key_value" | "key-value" => "delete_keyspace_requires_review",
        "object" | "blob" | "storage" => "delete_bucket_requires_review",
        "vector" => "delete_vector_collection_requires_review",
        "graph" => "delete_graph_requires_review",
        "nosql" | "document" => "delete_collection_requires_review",
        "timeseries" | "time-series" => "delete_measurement_requires_review",
        "column" | "columnar" | "wide-column" => "drop_column_table_requires_review",
        "model_registry" => "delete_experiment_requires_review",
        _ => "delete_resource_requires_review",
    }
    .to_string()
}

fn resource_kind(store: &ManifestStore) -> String {
    match store.store_kind.as_str() {
        "sql" => "table",
        "nosql" | "document" => "collection",
        "vector" => "vector_collection",
        "graph" => "graph",
        "timeseries" | "time-series" => "measurement",
        "column" | "columnar" | "wide-column" => "column_table",
        "cache" | "kv" | "key_value" | "key-value" => "keyspace",
        "object" | "blob" | "storage" => "bucket",
        "model_registry" => "experiment",
        _ => "resource",
    }
    .to_string()
}

fn store_path(store: &ManifestStore) -> String {
    let mut parts = Vec::new();
    if !store.database_name.trim().is_empty() {
        parts.push(store.database_name.as_str());
    }
    if !store.namespace.trim().is_empty() {
        parts.push(store.namespace.as_str());
    }
    if !store.resource_name.trim().is_empty() {
        parts.push(store.resource_name.as_str());
    }
    if parts.is_empty() {
        format!("{}/{}", store.owner_schema, store.owner_table)
    } else {
        parts.join("/")
    }
}

fn param(key: &str, value: String) -> ProvisioningParameter {
    ProvisioningParameter {
        key: key.to_string(),
        value,
    }
}
