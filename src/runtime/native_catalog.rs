//! Native-service catalog.
//!
//! UDB's own native-service entity protos (`proto/udb/core/**`) are embedded at
//! build time and parsed into a [`CatalogManifest`] here, so native-service
//! migrations flow through the *same* proto → manifest → DDL pipeline as user
//! tables. Proto is the single source of truth: there is no hand-written DDL for
//! native services. Tables/columns derive entirely from the `pg_table` /
//! `pg_column` annotations on the entity messages.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use include_dir::{Dir, include_dir};

use crate::ast::ProtoSchema;
use crate::generation::sql::{SqlGenerationConfig, generate_bootstrap_sql};
use crate::generation::{CatalogManifest, ManifestTable};
use crate::parser::{ParserConfig, parse_proto_source};
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
fn collect(dir: &Dir<'_>, config: &ParserConfig, out: &mut Vec<ProtoSchema>) {
    for file in dir.files() {
        let path = file.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("proto") {
            continue;
        }
        match parse_proto_source(file.contents(), path.to_string_lossy().to_string(), config) {
            Ok(report) => out.extend(report.schemas),
            Err(err) => {
                tracing::warn!(proto = %path.display(), error = %err, "skipped native proto");
            }
        }
    }
    for sub in dir.dirs() {
        collect(sub, config, out);
    }
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
        collect(&NATIVE_PROTO_DIR, &config, &mut schemas);
        schemas
    })
}

/// Whether native services (auth, …) are enabled for this deployment. Default
/// on; set `UDB_NATIVE_AUTH=0` to opt out of native-service migration/serving.
pub(crate) fn native_services_enabled() -> bool {
    std::env::var("UDB_NATIVE_AUTH")
        .map(|v| !matches!(v.as_str(), "0" | "false" | "no" | "off"))
        .unwrap_or(true)
}

/// Distinct schema names owned by native services, derived from the proto
/// manifest (e.g. `udb_authn`, `udb_authz`). Used to bootstrap the namespaces
/// before the migration engine creates the tables — proto stays the source of
/// truth for the schema set.
pub(crate) fn native_schema_names() -> Vec<String> {
    let mut names: Vec<String> = native_manifest()
        .tables
        .iter()
        .map(|table| table.schema.clone())
        .filter(|schema| !schema.trim().is_empty())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Merge the native-service entity schemas into a user schema set and rebuild a
/// single [`CatalogManifest`], so native tables migrate through the same
/// diff/apply engine as user tables. Returns the inputs unchanged when native
/// services are disabled or the merged manifest fails to build.
pub(crate) fn merge_native(
    manifest: &CatalogManifest,
    schemas: &[ProtoSchema],
) -> (CatalogManifest, Vec<ProtoSchema>) {
    if !native_services_enabled() {
        return (manifest.clone(), schemas.to_vec());
    }
    // Borrow the inputs and copy once: the user schemas are `to_vec`'d a single
    // time, then native ones appended (was a caller-side `to_vec()` plus an
    // internal `.clone()`); the manifest is only cloned on the fallback paths,
    // never on the success path where it is replaced by `merged` (#98).
    let mut all = schemas.to_vec();
    all.extend(native_schemas().iter().cloned());
    match CatalogManifest::from_schemas(&all) {
        Ok(merged) => (merged, all),
        Err(err) => {
            tracing::error!(error = %err, "failed to merge native-service manifest; native tables will not migrate");
            (manifest.clone(), schemas.to_vec())
        }
    }
}

/// The native-service `CatalogManifest`, built once from the embedded protos.
pub fn native_manifest() -> &'static CatalogManifest {
    static MANIFEST: OnceLock<CatalogManifest> = OnceLock::new();
    MANIFEST.get_or_init(|| CatalogManifest::from_schemas(native_schemas()).unwrap_or_default())
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
    crate::broker::table_for_message(native_manifest(), message_type)
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

pub(crate) fn native_model(message_type: &str, required_fields: &[&str]) -> NativeModel {
    let table =
        crate::broker::table_for_message(native_manifest(), message_type).unwrap_or_else(|| {
            panic!("native model {message_type} is missing from embedded proto manifest")
        });
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
