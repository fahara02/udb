//! Native-service catalog.
//!
//! UDB's own native-service entity protos (`proto/udb/core/**`) are embedded at
//! build time and parsed into a [`CatalogManifest`] here, so native-service
//! migrations flow through the *same* proto → manifest → DDL pipeline as user
//! tables. Proto is the single source of truth: there is no hand-written DDL for
//! native services. Tables/columns derive entirely from the `pg_table` /
//! `pg_column` annotations on the entity messages.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use include_dir::{Dir, include_dir};

use crate::ast::ProtoSchema;
use crate::generation::sql::{SqlGenerationConfig, generate_bootstrap_sql};
use crate::generation::{CatalogManifest, ManifestTable, ManifestTableSecurity};
use crate::parser::{ParserConfig, parse_proto_source};
use crate::runtime::config::NativeServicesSettings;
use crate::runtime::executor_utils::qi_runtime;

/// Embedded UDB native-service protos (`proto/udb/core/**`), compiled into the
/// binary so native migration works for any user project without copying protos.
static NATIVE_PROTO_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/proto/udb/core");

/// The *entire* UDB proto contract (`proto/udb/**`): the annotation contract
/// (`udb/core/**`), broker wire surface (`udb/entity/**`, `udb/services/**`),
/// and event envelopes (`udb/events/**`). Embedded so `udb proto export` and
/// `udb sdk generate` can materialize the full version-matched tree without a
/// repo clone or registry. (Deliberately a superset of `NATIVE_PROTO_DIR`; the
/// double-embed of `core/**` is a few tens of KB and keeps native-catalog schema
/// parsing scoped to `core/**` only.)
static FULL_PROTO_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/proto/udb");

/// Vendored third-party protos the UDB contract imports (`google/api/{annotations,
/// http,field_behavior}`). Embedded so an exported tree compiles offline with bare
/// `protoc -I <out>` — buf users resolve these via `deps` instead, but `protoc`
/// users have no registry. The google well-known types (`google/protobuf/*`) ship
/// with every protoc/buf toolchain and are intentionally not vendored.
static THIRD_PARTY_PROTO_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/third_party/googleapis");

/// Recursively parse every embedded `*.proto` into `ProtoSchema`s. Entity protos
/// yield table schemas via their `pg_table` annotations; service/event protos
/// simply yield none.
fn collect(dir: &Dir<'_>, config: &ParserConfig, out: &mut Vec<ProtoSchema>) -> Result<(), String> {
    for file in dir.files() {
        let path = file.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("proto") {
            continue;
        }
        let report =
            parse_proto_source(file.contents(), path.to_string_lossy().to_string(), config)
                .map_err(|err| {
                    format!(
                        "failed to parse embedded native proto {}: {err}",
                        path.display()
                    )
                })?;
        out.extend(report.schemas);
    }
    for sub in dir.dirs() {
        collect(sub, config, out)?;
    }
    Ok(())
}

/// Every embedded UDB proto file as `(import_path, contents)`, where
/// `import_path` is the path users write in an `import "…"` line — i.e. rooted
/// at `udb/core/…` (the embedded `NATIVE_PROTO_DIR` is `proto/udb/core`, so each
/// file's path is relative to that and re-prefixed with `udb/core/`).
///
/// This backs `udb proto export`: the annotation contract (`udb/core/common/v1/
/// db.proto` and the sibling types it references) and the broker/service protos
/// are compiled into the binary, so a user can materialize a version-matched
/// proto tree without cloning the repo or depending on a registry.
pub fn embedded_proto_files() -> Vec<(String, &'static [u8])> {
    fn walk(dir: &Dir<'static>, out: &mut Vec<(String, &'static [u8])>) {
        for file in dir.files() {
            if file.path().extension().and_then(|ext| ext.to_str()) == Some("proto") {
                let rel = file.path().to_string_lossy().replace('\\', "/");
                out.push((format!("udb/core/{rel}"), file.contents()));
            }
        }
        for sub in dir.dirs() {
            walk(sub, out);
        }
    }
    let mut out = Vec::new();
    walk(&NATIVE_PROTO_DIR, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Every embedded UDB proto file across the *full* contract as
/// `(import_path, contents)`, where `import_path` is rooted at `udb/…` — exactly
/// the string a user writes in an `import "…"` line. Superset of
/// [`embedded_proto_files`]; this is what `udb proto export` materializes so the
/// broker wire surface (`udb/entity`, `udb/services`, `udb/events`) is present,
/// not just the annotation contract.
pub fn embedded_broker_protos() -> Vec<(String, &'static [u8])> {
    fn walk(dir: &Dir<'static>, out: &mut Vec<(String, &'static [u8])>) {
        for file in dir.files() {
            if file.path().extension().and_then(|ext| ext.to_str()) == Some("proto") {
                let rel = file.path().to_string_lossy().replace('\\', "/");
                out.push((format!("udb/{rel}"), file.contents()));
            }
        }
        for sub in dir.dirs() {
            walk(sub, out);
        }
    }
    let mut out = Vec::new();
    walk(&FULL_PROTO_DIR, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Vendored third-party protos as `(import_path, contents)` rooted at the import
/// path the UDB contract uses (`google/api/annotations.proto`, …). Written
/// alongside the UDB tree by `udb proto export` so offline `protoc -I <out>`
/// resolves the google/api imports.
pub fn embedded_third_party_protos() -> Vec<(String, &'static [u8])> {
    fn walk(dir: &Dir<'static>, out: &mut Vec<(String, &'static [u8])>) {
        for file in dir.files() {
            if file.path().extension().and_then(|ext| ext.to_str()) == Some("proto") {
                let rel = file.path().to_string_lossy().replace('\\', "/");
                out.push((rel, file.contents()));
            }
        }
        for sub in dir.dirs() {
            walk(sub, out);
        }
    }
    let mut out = Vec::new();
    walk(&THIRD_PARTY_PROTO_DIR, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The prost-encoded `FileDescriptorSet` for the whole UDB wire contract, embedded
/// at build time (`OUT_DIR/udb_descriptor.bin`). `udb sdk generate` decodes this to
/// enumerate the RPC surface — proto stays the single source of truth, no second
/// hand-maintained RPC list.
pub fn embedded_file_descriptor_set() -> &'static [u8] {
    tonic::include_file_descriptor_set!("udb_descriptor")
}

/// The UDB wire-protocol version the running binary speaks (single source:
/// `crate::runtime::service::UDB_PROTOCOL_VERSION`). Exposed so the CLI (bin
/// crate) can stamp generated SDKs with a version that always matches the
/// binary, rather than re-declaring the constant.
pub fn protocol_version() -> &'static str {
    crate::runtime::service::UDB_PROTOCOL_VERSION
}

pub(crate) fn native_schemas() -> &'static Vec<ProtoSchema> {
    static SCHEMAS: OnceLock<Vec<ProtoSchema>> = OnceLock::new();
    SCHEMAS.get_or_init(|| {
        let config = ParserConfig::default();
        let mut schemas = Vec::new();
        collect(&NATIVE_PROTO_DIR, &config, &mut schemas)
            .expect("embedded native protos must parse");
        schemas
    })
}

static INSTALLED_NATIVE_SERVICES_SETTINGS: OnceLock<Mutex<NativeServicesSettings>> =
    OnceLock::new();

pub(crate) fn install_native_services_settings(settings: NativeServicesSettings) {
    let cell = INSTALLED_NATIVE_SERVICES_SETTINGS
        .get_or_init(|| Mutex::new(NativeServicesSettings::default()));
    if let Ok(mut guard) = cell.lock() {
        *guard = settings;
    }
}

pub(crate) fn current_native_services_settings() -> NativeServicesSettings {
    INSTALLED_NATIVE_SERVICES_SETTINGS
        .get()
        .and_then(|cell| cell.lock().ok().map(|guard| guard.clone()))
        .unwrap_or_else(|| {
            let mut settings = NativeServicesSettings::default();
            settings.merge_env();
            settings
        })
}

/// Whether native services are enabled for this deployment. Default on.
/// `UDB_NATIVE_SERVICES_ENABLED` is canonical; `UDB_NATIVE_AUTH` remains a
/// compatibility fallback for older deployments.
pub(crate) fn native_services_enabled() -> bool {
    current_native_services_settings().enabled
}

/// Distinct, sorted schema names declared by every table in a [`CatalogManifest`].
///
/// The SINGLE source of truth for schema enumeration across BOTH planes — the
/// native plane ([`native_manifest`]) and the user plane (a user-built
/// `CatalogManifest`). Schema CREATION (`native_service_catalog_ddl` and the
/// migration engine, which both build from a manifest) and schema ENUMERATION
/// (namespace pre-create, teardown) now derive from this ONE function, so they
/// cannot drift — the bug that previously hid `udb_control`/`udb_system` from
/// enumeration while the DDL still created them.
pub(crate) fn manifest_schema_names(manifest: &CatalogManifest) -> Vec<String> {
    let mut names: Vec<String> = manifest
        .tables
        .iter()
        .map(|table| table.schema.clone())
        .filter(|schema| !schema.trim().is_empty())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Distinct schema names the native plane owns, derived from the SAME embedded
/// proto manifest `native_service_catalog_ddl()` generates its DDL from — so
/// namespace pre-create / teardown can never drift from creation.
///
/// Deliberately NOT filtered by `migration_enabled`: that filter (which lives in
/// `merge_native_with_settings`, governing what the diff/apply engine migrates)
/// silently dropped always-present core schemas like `udb_control`/`udb_system`/
/// `udb_idp`/`udb_sdk_live` whose tables the bootstrap DDL still creates. The
/// per-service enable toggle is enforced at the migration layer, not by hiding
/// schemas from enumeration.
pub(crate) fn native_schema_names() -> Vec<String> {
    manifest_schema_names(native_manifest())
}

/// Merge the native-service entity schemas into a user schema set and rebuild a
/// single [`CatalogManifest`], so native tables migrate through the same
/// diff/apply engine as user tables. Returns the inputs unchanged when native
/// services are disabled or the merged manifest fails to build.
///
/// Public so the offline `udb plan` / `drift` / `manifest-export` CLI can build
/// the SAME effective manifest `serve` diffs and stores in the ledger. Without
/// it the CLI's "new" manifest omitted the embedded `udb_*` schemas and produced
/// spurious `DropTable udb_*` ops that could never hash-match the serve-side diff
/// (breaking the approval-plan workflow). Uses env-derived settings when no
/// runtime is installed, matching how `serve` resolves them.
pub fn merge_native(
    manifest: &CatalogManifest,
    schemas: &[ProtoSchema],
) -> (CatalogManifest, Vec<ProtoSchema>) {
    merge_native_with_settings(manifest, schemas, &current_native_services_settings())
}

pub(crate) fn merge_native_with_settings(
    manifest: &CatalogManifest,
    schemas: &[ProtoSchema],
    settings: &NativeServicesSettings,
) -> (CatalogManifest, Vec<ProtoSchema>) {
    if !settings.enabled || !settings.migrate_enabled {
        return (manifest.clone(), schemas.to_vec());
    }
    let config = crate::runtime::config::UdbConfig {
        native_services: settings.clone(),
        ..crate::runtime::config::UdbConfig::default()
    };
    let enabled_ids =
        crate::runtime::service::native_registry::migration_enabled_service_ids(&config);
    // Borrow the inputs and copy once: the user schemas are `to_vec`'d a single
    // time, then native ones appended (was a caller-side `to_vec()` plus an
    // internal `.clone()`); the manifest is only cloned on the fallback paths,
    // never on the success path where it is replaced by `merged` (#98).
    let mut all = schemas.to_vec();
    // Idempotent merge: skip native schemas that are ALREADY present in the
    // parsed input. When the proto root being served is the full UDB tree (it
    // contains the native-service protos), re-appending the embedded copies
    // would duplicate every native message — each duplicate yields a second
    // write-owner projection, tripping the `ambiguous_projection_write_owner`
    // lint for the whole native catalog. Deduplicating on message identity
    // (`proto_package` + `message_name`) keeps the merge a no-op for those
    // already-present schemas while still adding native ones the user omitted.
    let existing: std::collections::HashSet<(String, String)> = all
        .iter()
        .map(|schema| (schema.proto_package.clone(), schema.message_name.clone()))
        .collect();
    all.extend(
        native_schemas()
            .iter()
            .filter(|schema| native_schema_migration_enabled(schema, &enabled_ids))
            .filter(|schema| {
                !existing.contains(&(schema.proto_package.clone(), schema.message_name.clone()))
            })
            .cloned(),
    );
    match CatalogManifest::from_schemas(&all) {
        Ok(merged) => (merged, all),
        Err(err) => {
            tracing::error!(error = %err, "failed to merge native-service manifest; native tables will not migrate");
            (manifest.clone(), schemas.to_vec())
        }
    }
}

fn native_schema_migration_enabled(
    schema: &ProtoSchema,
    enabled_ids: &std::collections::BTreeSet<String>,
) -> bool {
    let direct_owner = native_service_id_for_proto_schema(schema);
    if !direct_owner.is_empty() {
        return enabled_ids.contains(direct_owner);
    }

    // Most native services own a unique `udb_<service>` schema. Resolve those
    // owners from the descriptor-derived registry rather than extending the
    // compatibility switch in `native_service_id_for_parts` every time a
    // service is added. Without this fallback, newly added services were
    // mounted and required by the system-catalog health gate while all of their
    // entity schemas were silently filtered out of the migration manifest.
    crate::runtime::service::native_registry::native_service_registry()
        .into_iter()
        .any(|entry| {
            enabled_ids.contains(&entry.service_id)
                && entry
                    .db_schema_prefixes
                    .iter()
                    .any(|prefix| prefix == &schema.schema_name)
        })
}

fn native_service_id_for_proto_schema(schema: &ProtoSchema) -> &str {
    native_service_id_for_parts(
        &schema.proto_package,
        &schema.message_name,
        &schema.schema_name,
        &schema.table_name,
    )
}

fn native_service_id_for_parts(
    proto_package: &str,
    message_name: &str,
    schema: &str,
    table: &str,
) -> &'static str {
    if proto_package.contains(".apikey.")
        || message_name.contains(".apikey.")
        || table == "api_keys"
        || table.starts_with("api_key")
    {
        return "apikey";
    }
    if proto_package.contains(".webrtc.") || message_name.contains(".webrtc.") {
        return match table {
            "peers" => "webrtc_peer",
            "tracks" => "webrtc_track",
            "turn_credentials" => "webrtc_turn",
            "signaling_messages" => "webrtc_signaling",
            _ => "webrtc_room",
        };
    }
    match schema {
        "udb_authn" => "authn",
        "udb_authz" => "authz",
        "udb_tenant" => "tenant",
        "udb_notification" => "notification",
        "udb_analytics" => "analytics",
        "udb_storage" => "storage",
        "udb_asset" => "asset",
        "udb_webrtc" => "webrtc_room",
        _ => "",
    }
}

/// The native-service `CatalogManifest`, built once from the embedded protos.
pub fn native_manifest() -> &'static CatalogManifest {
    static MANIFEST: OnceLock<CatalogManifest> = OnceLock::new();
    MANIFEST.get_or_init(|| {
        CatalogManifest::from_schemas(native_schemas())
            .expect("embedded native manifest must build")
    })
}

pub(crate) fn native_service_manifest() -> Result<&'static CatalogManifest, String> {
    Ok(native_manifest())
}

pub(crate) fn native_service_catalog_ddl() -> Vec<String> {
    static DDL: OnceLock<Vec<String>> = OnceLock::new();
    DDL.get_or_init(|| {
        generate_bootstrap_sql(native_schemas(), &SqlGenerationConfig::default())
            .map(|artifacts| {
                artifacts
                    .into_iter()
                    .map(|artifact| artifact.content)
                    .collect()
            })
            .unwrap_or_else(|err| {
                tracing::error!(error = %err, "failed to generate native-service bootstrap SQL");
                Vec::new()
            })
    })
    .clone()
}

pub(crate) fn native_relation(message_type: &str) -> Option<(String, String)> {
    crate::broker::resolve_table_for_message(native_manifest(), message_type)
        .ok()
        .map(|table| (table.schema.clone(), table.table.clone()))
}

/// Runtime view of a UDB-owned proto entity table.
///
/// Native auth CRUD must go through this descriptor so table and column names
/// come from the embedded proto manifest, not from hand-maintained Rust schema
/// copies. Field names passed to [`NativeModel::column`] are generated proto
/// model fields; the descriptor resolves them to the annotated DB column.
#[derive(Debug, Clone)]
pub(crate) struct NativeModel {
    pub message_type: String,
    pub relation: String,
    pub table_security: ManifestTableSecurity,
    pub tenant_column: Option<String>,
    pub project_column: Option<String>,
    pub soft_delete_column: Option<String>,
    columns_by_field: BTreeMap<String, String>,
}

impl NativeModel {
    fn from_table(message_type: &str, table: &ManifestTable) -> Self {
        let columns_by_field = table
            .columns
            .iter()
            .map(|column| (column.field_name.clone(), column.column_name.clone()))
            .collect();
        Self {
            message_type: message_type.to_string(),
            relation: relation(&table.schema, &table.table),
            table_security: table.table_security.clone(),
            tenant_column: model_column(&table.table_security.tenant_column, table),
            project_column: model_column(&table.table_security.project_column, table),
            soft_delete_column: table
                .soft_delete
                .then(|| table.soft_delete_column.clone())
                .filter(|column| !column.trim().is_empty()),
            columns_by_field,
        }
    }

    pub(crate) fn column(&self, field_name: &str) -> &str {
        self.columns_by_field.get(field_name).unwrap_or_else(|| {
            panic!(
                "native model {} is missing proto field {}",
                self.message_type, field_name
            )
        })
    }

    /// Manifest-declared table-security model for this entity (tenant isolation
    /// mode, RLS policy template, soft-delete mode). Source of truth for the
    /// security predicates below so call sites do not hand-maintain column names.
    pub(crate) fn table_security(&self) -> &ManifestTableSecurity {
        &self.table_security
    }

    /// `<soft_delete_column> IS NULL` predicate when the entity is soft-deletable,
    /// else `None`. Lets active-row queries derive the soft-delete filter from the
    /// manifest instead of hardcoding the `deleted_at` literal.
    pub(crate) fn soft_delete_is_null(&self) -> Option<String> {
        self.soft_delete_column
            .as_deref()
            .map(|column| format!("{} IS NULL", qi_runtime(column)))
    }

    /// Quoted tenant-isolation column from the manifest table-security model,
    /// returned only when the table actually declares an active tenant-isolation
    /// mode (so callers do not invent a filter on non-tenant tables).
    pub(crate) fn tenant_column(&self) -> Option<String> {
        if isolation_active(&self.table_security().tenant_isolation_mode) {
            self.tenant_column.as_deref().map(qi_runtime)
        } else {
            None
        }
    }

    /// Quoted project-isolation column from the manifest table-security model,
    /// returned only when the table declares an active project-isolation mode.
    pub(crate) fn project_column(&self) -> Option<String> {
        if isolation_active(&self.table_security().project_isolation_mode) {
            self.project_column.as_deref().map(qi_runtime)
        } else {
            None
        }
    }

    pub(crate) fn q(&self, field_name: &str) -> String {
        qi_runtime(self.column(field_name))
    }

    pub(crate) fn select(&self, field_name: &str) -> String {
        self.q(field_name)
    }

    pub(crate) fn select_as(&self, field_name: &str, alias: &str) -> String {
        format!("{} AS {}", self.q(field_name), qi_runtime(alias))
    }

    pub(crate) fn text(&self, field_name: &str) -> String {
        self.text_as(field_name, field_name)
    }

    pub(crate) fn text_as(&self, field_name: &str, alias: &str) -> String {
        format!("{}::TEXT AS {}", self.q(field_name), qi_runtime(alias))
    }

    pub(crate) fn text_or_empty(&self, field_name: &str) -> String {
        self.text_or_empty_as(field_name, field_name)
    }

    pub(crate) fn text_or_empty_as(&self, field_name: &str, alias: &str) -> String {
        format!(
            "COALESCE({}::TEXT, '') AS {}",
            self.q(field_name),
            qi_runtime(alias)
        )
    }

    pub(crate) fn json_text_as(&self, field_name: &str, alias: &str) -> String {
        format!("{}::TEXT AS {}", self.q(field_name), qi_runtime(alias))
    }

    pub(crate) fn json_get_as(&self, field_name: &str, key: &str, alias: &str) -> String {
        format!(
            "{}->>{} AS {}",
            self.q(field_name),
            sql_literal(key),
            qi_runtime(alias)
        )
    }

    pub(crate) fn json_coalesce_as(
        &self,
        field_name: &str,
        key: &str,
        default: &str,
        alias: &str,
    ) -> String {
        format!(
            "COALESCE({}->>{}, {}) AS {}",
            self.q(field_name),
            sql_literal(key),
            sql_literal(default),
            qi_runtime(alias)
        )
    }

    pub(crate) fn timestamp_unix_as(&self, field_name: &str, alias: &str) -> String {
        format!(
            "COALESCE(EXTRACT(EPOCH FROM {})::BIGINT, 0) AS {}",
            self.q(field_name),
            qi_runtime(alias)
        )
    }

    pub(crate) fn required_columns(&self, field_names: &[&str]) {
        for field_name in field_names {
            let _ = self.column(field_name);
        }
    }
}

/// A manifest isolation mode is "active" when it is set to anything other than
/// the empty/`none` sentinel, matching the control-plane sourcing convention.
fn isolation_active(mode: &str) -> bool {
    let mode = mode.trim();
    !mode.is_empty() && mode != "none"
}

fn model_column(name: &str, table: &ManifestTable) -> Option<String> {
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    table
        .columns
        .iter()
        .find(|column| column.column_name == name || column.field_name == name)
        .map(|column| column.column_name.clone())
}

pub(crate) fn native_model(message_type: &str, required_fields: &[&str]) -> NativeModel {
    let table = crate::broker::resolve_table_for_message(native_manifest(), message_type)
        .unwrap_or_else(|error| panic!("native model lookup failed: {error}"));
    let model = NativeModel::from_table(message_type, table);
    model.required_columns(required_fields);
    model
}

pub(crate) fn relation(schema: &str, table: &str) -> String {
    format!("{}.{}", qi_runtime(schema), qi_runtime(table))
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_services_global_switch_defaults_on() {
        assert!(NativeServicesSettings::default().enabled);
    }

    #[test]
    fn native_services_global_switch_accepts_false_tokens() {
        for value in ["0", "false", "FALSE", "no", "off", " off "] {
            let mut settings = NativeServicesSettings::default();
            settings.merge_enabled_env_values(Some(value), None);
            assert!(
                !settings.enabled,
                "expected {value:?} to disable native services"
            );
        }
        for value in ["1", "true", "yes", "on", "enabled", "anything-else"] {
            let mut settings = NativeServicesSettings::default();
            settings.merge_enabled_env_values(Some(value), None);
            assert!(
                settings.enabled,
                "expected {value:?} to enable native services"
            );
        }
    }

    #[test]
    fn native_services_global_switch_prefers_canonical_over_legacy() {
        let mut settings = NativeServicesSettings::default();
        settings.merge_enabled_env_values(None, Some("0"));
        assert!(!settings.enabled);

        let mut settings = NativeServicesSettings::default();
        settings.merge_enabled_env_values(Some("1"), Some("0"));
        assert!(settings.enabled);

        let mut settings = NativeServicesSettings::default();
        settings.merge_enabled_env_values(Some("0"), Some("1"));
        assert!(!settings.enabled);
    }

    #[test]
    fn native_manifest_has_auth_tables_from_proto() {
        let manifest = native_manifest();
        assert!(
            !manifest.tables.is_empty(),
            "embedded native protos must yield at least one annotated table"
        );
        // PolicyRule carries a `pg_table` annotation (table_name = policy_rules),
        // so it must appear in the generated manifest.
        let has_policy_rules = manifest
            .tables
            .iter()
            .any(|t| t.message_name == "PolicyRule" || t.table == "policy_rules");
        assert!(
            has_policy_rules,
            "expected the proto-annotated policy_rules table in the native manifest; got: {:?}",
            manifest.tables.iter().map(|t| &t.table).collect::<Vec<_>>()
        );
    }

    #[test]
    fn native_merge_migrates_every_enabled_registry_owned_schema() {
        let settings = NativeServicesSettings::default();
        let config = crate::runtime::config::UdbConfig {
            native_services: settings.clone(),
            ..crate::runtime::config::UdbConfig::default()
        };
        let enabled_ids =
            crate::runtime::service::native_registry::migration_enabled_service_ids(&config);
        let registry = crate::runtime::service::native_registry::native_service_registry();

        let expected: Vec<_> = native_manifest()
            .tables
            .iter()
            .filter(|table| {
                registry.iter().any(|entry| {
                    enabled_ids.contains(&entry.service_id)
                        && entry
                            .db_schema_prefixes
                            .iter()
                            .any(|prefix| prefix == &table.schema)
                })
            })
            .map(|table| (table.schema.clone(), table.table.clone()))
            .collect();
        assert!(
            !expected.is_empty(),
            "descriptor registry must own at least one native table"
        );

        let user_schemas: Vec<ProtoSchema> = Vec::new();
        let user_manifest =
            CatalogManifest::from_schemas(&user_schemas).expect("empty user manifest must build");
        let (merged, _) = merge_native_with_settings(&user_manifest, &user_schemas, &settings);
        let missing: Vec<_> = expected
            .into_iter()
            .filter(|(schema, table)| {
                !merged
                    .tables
                    .iter()
                    .any(|candidate| &candidate.schema == schema && &candidate.table == table)
            })
            .collect();
        assert!(
            missing.is_empty(),
            "enabled native-service tables were filtered out of the migration manifest: {missing:?}"
        );
    }

    #[test]
    fn embedded_native_manifest_passes_startup_manifest_validation() {
        // The broker runs this exact validation at `serve` startup
        // (PROTO_CHECKSUM_LINT). Any entry here makes `udb serve` dead-on-arrival
        // against a real database — the SDK/lib tests never boot the broker, so
        // this is the gate that catches the class of regression where a new
        // entity declares db_table_security tenant isolation but omits
        // `tenant_column: true` on its pg_column, or a JSONB column omits
        // `is_json: true` (UDB-CAT-003).
        let manifest = native_manifest();
        assert!(
            manifest.validation_errors.is_empty(),
            "embedded native catalog FAILS startup manifest validation \
             (`udb serve` would abort at PROTO_CHECKSUM_LINT):\n{}",
            manifest.validation_errors.join("\n")
        );
    }

    #[test]
    fn native_service_catalog_ddl_is_generated_from_embedded_proto() {
        let ddl = native_service_catalog_ddl();
        let joined = ddl.join("\n");
        for fragment in [
            "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"users\"",
            "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"sessions\"",
            "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"otps\"",
            "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"webauthn_credentials\"",
            "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"webauthn_challenges\"",
            "CREATE TABLE IF NOT EXISTS \"udb_authn\".\"api_keys\"",
            "CREATE TABLE IF NOT EXISTS \"udb_authz\".\"policy_rules\"",
            "CREATE TABLE IF NOT EXISTS \"udb_vault\".\"vault_db_credential_leases\"",
        ] {
            assert!(
                joined.contains(fragment),
                "missing generated DDL fragment {fragment}"
            );
        }
        assert!(
            joined.contains("UDB:proto_manifest_checksum"),
            "native DDL must carry proto manifest metadata"
        );
    }

    #[test]
    fn native_model_exposes_table_security_metadata_and_rls_ddl() {
        let model = native_model(
            "udb.core.authn.entity.v1.User",
            &["user_id", "tenant_id", "project_id"],
        );
        assert_eq!(model.tenant_column.as_deref(), Some("tenant_id"));
        assert!(
            !model.table_security.tenant_isolation_mode.trim().is_empty(),
            "native model must expose table_security from db_table_security"
        );
        assert!(
            model
                .table_security
                .rls_policy_template
                .contains("app.current_tenant_id"),
            "native model must expose RLS policy template"
        );

        let joined = native_service_catalog_ddl().join("\n");
        assert!(
            joined.contains("ALTER TABLE \"udb_authn\".\"users\" ENABLE ROW LEVEL SECURITY"),
            "native DDL must enable RLS for table-security protected users table"
        );
        assert!(
            joined.contains("CREATE POLICY"),
            "native DDL must render table-security RLS policies"
        );
    }

    #[test]
    fn native_ddl_includes_storage_asset_webrtc_tables_from_proto() {
        let joined = native_service_catalog_ddl().join("\n");
        for fragment in [
            "CREATE TABLE IF NOT EXISTS \"udb_storage\".\"files\"",
            "CREATE TABLE IF NOT EXISTS \"udb_asset\".\"assets\"",
            "CREATE TABLE IF NOT EXISTS \"udb_asset\".\"pipeline_definitions\"",
            "CREATE TABLE IF NOT EXISTS \"udb_asset\".\"pipeline_instances\"",
            "CREATE TABLE IF NOT EXISTS \"udb_asset\".\"pipeline_steps\"",
            "CREATE TABLE IF NOT EXISTS \"udb_webrtc\".\"rooms\"",
            "CREATE TABLE IF NOT EXISTS \"udb_webrtc\".\"peers\"",
            "CREATE TABLE IF NOT EXISTS \"udb_webrtc\".\"tracks\"",
        ] {
            assert!(
                joined.contains(fragment),
                "missing generated DDL fragment {fragment}"
            );
        }
    }

    #[test]
    fn native_models_resolve_storage_asset_webrtc_columns() {
        let cases = [
            (
                "udb.core.storage.entity.v1.File",
                &["file_id", "tenant_id", "object_key", "status", "file_type"][..],
            ),
            (
                "udb.core.asset.entity.v1.PipelineInstance",
                &[
                    "instance_id",
                    "definition_id",
                    "asset_id",
                    "correlation_id",
                    "status",
                ][..],
            ),
            (
                "udb.core.asset.entity.v1.PipelineStep",
                &["step_id", "instance_id", "tenant_id", "step_type", "status"][..],
            ),
            (
                "udb.core.webrtc.entity.v1.Room",
                &["room_id", "tenant_id", "state", "participant_count"][..],
            ),
            (
                "udb.core.webrtc.entity.v1.Track",
                &["track_id", "room_id", "peer_id", "kind", "state"][..],
            ),
        ];
        for (message_type, fields) in cases {
            let model = native_model(message_type, fields);
            for field in fields {
                assert!(
                    !model.column(field).trim().is_empty(),
                    "missing native proto column for {message_type}.{field}"
                );
            }
        }
    }

    #[test]
    fn native_relation_resolves_authn_and_apikey_models() {
        let cases = [
            ("udb.core.authn.entity.v1.User", "udb_authn", "users"),
            ("udb.core.authn.entity.v1.Session", "udb_authn", "sessions"),
            ("udb.core.authn.entity.v1.OTP", "udb_authn", "otps"),
            (
                "udb.core.authn.entity.v1.WebAuthnCredential",
                "udb_authn",
                "webauthn_credentials",
            ),
            (
                "udb.core.authn.entity.v1.WebAuthnChallenge",
                "udb_authn",
                "webauthn_challenges",
            ),
            ("udb.core.apikey.entity.v1.ApiKey", "udb_authn", "api_keys"),
        ];
        for (message, expected_schema, expected_table) in cases {
            assert_eq!(
                native_relation(message),
                Some((expected_schema.to_string(), expected_table.to_string())),
                "native relation mismatch for {message}"
            );
        }
    }

    #[test]
    fn native_models_expose_auth_crud_columns_from_proto_manifest() {
        let cases = [
            (
                "udb.core.authn.entity.v1.User",
                &[
                    "user_id",
                    "username",
                    "email",
                    "tenant_id",
                    "profile_attributes_json",
                ][..],
            ),
            (
                "udb.core.authn.entity.v1.Session",
                &[
                    "session_id",
                    "session_token_lookup",
                    "principal_id",
                    "expires_at",
                    "scopes_json",
                ][..],
            ),
            (
                "udb.core.authn.entity.v1.OTP",
                &[
                    "otp_id",
                    "user_id",
                    "delivery_address",
                    "expires_at",
                    "status",
                ][..],
            ),
            (
                "udb.core.authn.entity.v1.WebAuthnCredential",
                &[
                    "credential_id",
                    "user_id",
                    "passkey_json",
                    "tenant_id",
                    "last_used_at",
                ][..],
            ),
            (
                "udb.core.authn.entity.v1.WebAuthnChallenge",
                &[
                    "challenge_id",
                    "user_id",
                    "ceremony",
                    "state_json",
                    "expires_at",
                    "consumed_at",
                ][..],
            ),
            (
                "udb.core.apikey.entity.v1.ApiKey",
                &[
                    "key_prefix",
                    "key_hash",
                    "owner_id",
                    "scopes_json",
                    "status",
                ][..],
            ),
            (
                "udb.core.authz.entity.v1.PolicyRule",
                &[
                    "policy_id",
                    "subject",
                    "object",
                    "action",
                    "attributes_json",
                ][..],
            ),
            (
                "udb.core.authz.entity.v1.PolicyTuple",
                &["tuple_kind", "subject", "domain", "object", "action"][..],
            ),
            (
                "udb.core.authz.entity.v1.Role",
                &["role_id", "role_code", "domain", "metadata_json"][..],
            ),
            (
                "udb.core.authz.entity.v1.UserRole",
                &["user_role_id", "user_id", "role_id", "domain"][..],
            ),
            (
                "udb.core.authz.entity.v1.AccessDecisionAudit",
                &["decision_audit_id", "user_id", "object", "decision_source"][..],
            ),
        ];
        for (message_type, fields) in cases {
            let model = native_model(message_type, fields);
            for field in fields {
                assert!(
                    !model.column(field).trim().is_empty(),
                    "missing native proto column for {message_type}.{field}"
                );
            }
        }
    }
}
