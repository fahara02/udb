use serde::{Deserialize, Serialize};
use std::env;

use crate::ast::ProtoSchema;
use crate::backend::BackendKind;

use super::manifest::{CatalogManifest, ManifestStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DsnGenerationConfig {
    pub sql_env_key: String,
    pub nosql_env_key: String,
    pub vector_env_key: String,
    pub graph_env_key: String,
    pub timeseries_env_key: String,
    pub column_env_key: String,
    pub cache_env_key: String,
    pub object_env_key: String,
    pub fallback_env_key: String,
}

impl Default for DsnGenerationConfig {
    fn default() -> Self {
        Self {
            sql_env_key: "UDB_SQL_DSN".to_string(),
            nosql_env_key: "UDB_NOSQL_DSN".to_string(),
            vector_env_key: "UDB_VECTOR_DSN".to_string(),
            graph_env_key: "UDB_GRAPH_DSN".to_string(),
            timeseries_env_key: "UDB_TIMESERIES_DSN".to_string(),
            column_env_key: "UDB_COLUMN_DSN".to_string(),
            cache_env_key: "UDB_CACHE_DSN".to_string(),
            object_env_key: "UDB_OBJECT_DSN".to_string(),
            fallback_env_key: "UDB_GENERIC_DSN".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UnifiedDsnCatalog {
    pub checksum_sha256: String,
    pub entries: Vec<UnifiedDsn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UnifiedDsn {
    pub id: String,
    pub message_name: String,
    pub store_kind: String,
    pub backend: String,
    pub env_key: String,
    pub dsn: String,
    pub resource_uri: String,
    pub owner_schema: String,
    pub owner_table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ParsedUnifiedDsn {
    pub scheme: String,
    pub tier: String,
    pub backend: String,
    pub env_key: String,
    pub resource_path: String,
    pub resource_parts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResolvedUnifiedDsn {
    pub id: String,
    pub dsn: String,
    pub env_key: String,
    pub base_dsn: String,
    pub redacted_base_dsn: String,
    pub resource_path: String,
    pub valid: bool,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DsnValidationReport {
    pub passed: bool,
    pub errors: Vec<String>,
}

impl UnifiedDsn {
    pub fn parsed(&self) -> Result<ParsedUnifiedDsn, String> {
        parse_unified_dsn(&self.dsn)
    }

    pub fn validate(&self) -> DsnValidationReport {
        validate_unified_dsn(self)
    }

    pub fn resolved_from_env(&self) -> ResolvedUnifiedDsn {
        resolve_unified_dsn(self)
    }
}

pub fn generate_unified_dsn_catalog(
    schemas: &[ProtoSchema],
    config: &DsnGenerationConfig,
) -> Result<UnifiedDsnCatalog, serde_json::Error> {
    let manifest = CatalogManifest::from_schemas(schemas)?;
    let mut entries = Vec::new();

    for table in &manifest.tables {
        let env_key = config.sql_env_key.clone();
        let backend = "postgres".to_string();
        entries.push(UnifiedDsn {
            id: format!("sql:{}:{}", table.schema, table.table),
            message_name: table.message_name.clone(),
            store_kind: "sql".to_string(),
            backend: backend.clone(),
            env_key: env_key.clone(),
            dsn: format!(
                "{}://env:{}/{}/{}",
                BackendKind::Postgres.dsn_scheme(),
                env_key,
                table.schema,
                table.table
            ),
            resource_uri: format!("sql://{}/{}", table.schema, table.table),
            owner_schema: table.schema.clone(),
            owner_table: table.table.clone(),
        });
    }

    for store in &manifest.stores {
        entries.push(dsn_from_store(store, config));
    }

    entries.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(UnifiedDsnCatalog {
        checksum_sha256: manifest.checksum_sha256,
        entries,
    })
}

pub fn parse_unified_dsn(dsn: &str) -> Result<ParsedUnifiedDsn, String> {
    let (scheme, rest) = dsn
        .split_once("://")
        .ok_or_else(|| "DSN must contain ://".to_string())?;
    let scheme_parts = scheme.split('+').collect::<Vec<_>>();
    if scheme_parts.len() != 3 || scheme_parts[0] != "udb" {
        return Err("DSN scheme must be udb+<tier>+<backend>".to_string());
    }
    let tier = scheme_parts[1].trim();
    let backend = scheme_parts[2].trim();
    if tier.is_empty() || backend.is_empty() {
        return Err("DSN tier and backend must be non-empty".to_string());
    }
    if BackendKind::from_store_kind(tier, backend).is_none() && backend != "generic" {
        return Err(format!(
            "unsupported backend '{}' for tier '{}'",
            backend, tier
        ));
    }

    let (authority, resource_path) = rest
        .split_once('/')
        .ok_or_else(|| "DSN must include env authority and resource path".to_string())?;
    let env_key = authority
        .strip_prefix("env:")
        .ok_or_else(|| "DSN authority must be env:<ENV_KEY>".to_string())?
        .trim();
    if env_key.is_empty() {
        return Err("DSN env key must be non-empty".to_string());
    }
    let resource_path = resource_path.trim_matches('/');
    if resource_path.is_empty() {
        return Err("DSN resource path must be non-empty".to_string());
    }

    Ok(ParsedUnifiedDsn {
        scheme: scheme.to_string(),
        tier: tier.to_string(),
        backend: backend.to_string(),
        env_key: env_key.to_string(),
        resource_path: resource_path.to_string(),
        resource_parts: resource_path
            .split('/')
            .filter(|part| !part.trim().is_empty())
            .map(str::to_string)
            .collect(),
    })
}

pub fn validate_unified_dsn(entry: &UnifiedDsn) -> DsnValidationReport {
    let mut errors = Vec::new();
    match parse_unified_dsn(&entry.dsn) {
        Ok(parsed) => {
            if parsed.backend != entry.backend {
                errors.push(format!(
                    "backend mismatch: entry={} dsn={}",
                    entry.backend, parsed.backend
                ));
            }
            if parsed.env_key != entry.env_key {
                errors.push(format!(
                    "env key mismatch: entry={} dsn={}",
                    entry.env_key, parsed.env_key
                ));
            }
            if !entry.owner_schema.trim().is_empty()
                && !parsed
                    .resource_parts
                    .iter()
                    .any(|part| part == &entry.owner_schema)
            {
                errors.push(format!(
                    "resource path '{}' does not include owner schema '{}'",
                    parsed.resource_path, entry.owner_schema
                ));
            }
        }
        Err(err) => errors.push(err),
    }
    DsnValidationReport {
        passed: errors.is_empty(),
        errors,
    }
}

pub fn resolve_unified_dsn(entry: &UnifiedDsn) -> ResolvedUnifiedDsn {
    match parse_unified_dsn(&entry.dsn) {
        Ok(parsed) => match env::var(&parsed.env_key) {
            Ok(base_dsn) => ResolvedUnifiedDsn {
                id: entry.id.clone(),
                dsn: entry.dsn.clone(),
                env_key: parsed.env_key,
                redacted_base_dsn: redact_dsn(&base_dsn),
                base_dsn,
                resource_path: parsed.resource_path,
                valid: true,
                error: String::new(),
            },
            Err(err) => ResolvedUnifiedDsn {
                id: entry.id.clone(),
                dsn: entry.dsn.clone(),
                env_key: parsed.env_key,
                resource_path: parsed.resource_path,
                valid: false,
                error: format!("{}: {}", entry.env_key, err),
                ..ResolvedUnifiedDsn::default()
            },
        },
        Err(err) => ResolvedUnifiedDsn {
            id: entry.id.clone(),
            dsn: entry.dsn.clone(),
            valid: false,
            error: err,
            ..ResolvedUnifiedDsn::default()
        },
    }
}

pub fn resolve_unified_dsn_catalog(catalog: &UnifiedDsnCatalog) -> Vec<ResolvedUnifiedDsn> {
    catalog.entries.iter().map(resolve_unified_dsn).collect()
}

pub fn redact_dsn(dsn: &str) -> String {
    let Some((scheme, rest)) = dsn.split_once("://") else {
        return dsn.to_string();
    };
    // Use split_once('@') instead of find('@') + byte-slice indexing.
    // Byte-slicing with find() is unsafe when the '@' symbol is followed by a
    // multi-byte UTF-8 character: `rest[at_idx + 1..]` would panic if at_idx+1
    // falls in the middle of a multi-byte sequence. split_once is always safe.
    let Some((auth, host_and_path)) = rest.split_once('@') else {
        return dsn.to_string();
    };
    if let Some((user, _password)) = auth.split_once(':') {
        format!("{scheme}://{user}:***@{host_and_path}")
    } else {
        format!("{scheme}://***@{host_and_path}")
    }
}

fn dsn_from_store(store: &ManifestStore, config: &DsnGenerationConfig) -> UnifiedDsn {
    let env_key = if !store.dsn_env_key.trim().is_empty() {
        store.dsn_env_key.clone()
    } else {
        env_key_for_kind(&store.store_kind, config).to_string()
    };
    let backend_kind = BackendKind::from_store_kind(&store.store_kind, &store.backend);
    let backend = backend_kind
        .as_ref()
        .map(|kind| kind.as_str().to_string())
        .unwrap_or_else(|| {
            if store.backend.trim().is_empty() {
                "generic".to_string()
            } else {
                store.backend.clone()
            }
        });
    let scheme = backend_kind
        .as_ref()
        .map(|kind| kind.dsn_scheme())
        .unwrap_or_else(|| format!("udb+{}+{}", store.store_kind, backend));
    let dsn = if !store.dsn.trim().is_empty() {
        store.dsn.clone()
    } else {
        format!("{}://env:{}/{}", scheme, env_key, store_path(store))
    };

    UnifiedDsn {
        id: format!(
            "{}:{}:{}:{}",
            store.store_kind, backend, store.owner_schema, store.resource_name
        ),
        message_name: store.logical_name.clone(),
        store_kind: store.store_kind.clone(),
        backend: backend.clone(),
        env_key,
        dsn,
        resource_uri: format!("{}://{}/{}", store.store_kind, backend, store_path(store)),
        owner_schema: store.owner_schema.clone(),
        owner_table: store.owner_table.clone(),
    }
}

fn env_key_for_kind<'a>(kind: &str, config: &'a DsnGenerationConfig) -> &'a str {
    match kind {
        "sql" => &config.sql_env_key,
        "nosql" | "document" => &config.nosql_env_key,
        "vector" => &config.vector_env_key,
        "graph" => &config.graph_env_key,
        "timeseries" | "time-series" => &config.timeseries_env_key,
        "column" | "columnar" | "wide-column" => &config.column_env_key,
        "cache" => &config.cache_env_key,
        "object" | "blob" | "storage" => &config.object_env_key,
        _ => &config.fallback_env_key,
    }
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
