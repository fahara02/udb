//! `#[doc(hidden)]` benchmarking shims (D.2) — feature `bench-internals` only.
//!
//! Criterion micro-benches live in the bench crate and may call only UDB's
//! public API, but the hot conversion/parse helpers profiled by D.2 are
//! `pub(crate)`. Rather than widen the real public API, these `pub` wrappers
//! (same crate → allowed to call `pub(crate)`) expose exactly those functions.
//! This is NOT a stable API; it exists solely so `benches/hotpath_bench.rs` and
//! `examples/dhat_hotpath.rs` can measure the true internal code paths.
//!
//! Each wrapper carries the SAME `#[cfg]` gate as the function it wraps, so the
//! module compiles under any feature subset (benches normally run with the full
//! default feature set plus `bench-internals`).

use serde_json::Value as JsonValue;

pub use crate::broker::RequestContext;
pub use crate::proto::RequestContext as ProtoRequestContext;
pub use prost_types::{Struct, Value as ProstValue};

use crate::runtime::executor_utils as eu;

pub use crate::runtime::security::{AbacPolicy, PolicyEffect};

// ── Struct / prost <-> JSON (read/write row conversion) ───────────────────────

pub fn struct_to_json(value: &Struct) -> JsonValue {
    eu::struct_to_json(value)
}

pub fn prost_value_to_json(value: &ProstValue) -> JsonValue {
    eu::prost_value_to_json(value)
}

pub fn json_to_struct(value: &JsonValue) -> Option<Struct> {
    eu::json_to_struct(value)
}

pub fn json_to_prost_value(value: &JsonValue) -> Option<ProstValue> {
    eu::json_to_prost_value(value)
}

/// D.3 consuming variant — moves keys/strings instead of cloning (for callers
/// that own the value). Benched against `json_to_struct` to show the delta.
pub fn json_into_struct(value: JsonValue) -> Option<Struct> {
    eu::json_into_struct(value)
}

// ── RequestContext merge (per-request hot path) ───────────────────────────────

pub fn merge_context(
    proto_context: Option<&ProtoRequestContext>,
    metadata_context: RequestContext,
) -> RequestContext {
    eu::merge_context(proto_context, metadata_context)
}

// ── Authz / method-security hot maps (D.2 allocation coverage) ───────────────

pub fn rebuild_authz_snapshot_from_abac(
    version: &str,
    policies: &[AbacPolicy],
) -> crate::runtime::authz::AuthzSnapshot {
    crate::runtime::authz::AuthzSnapshot::from_abac_policies(version, policies)
}

pub fn rebuild_method_security_scope_registry() -> usize {
    crate::runtime::service::build_method_security_registry()
        .values()
        .map(|security| {
            security.scopes.len()
                + security.roles.len()
                + security.tenant_required as usize
                + security.request_context_required as usize
                + security.internal_grpc_only as usize
        })
        .sum()
}

pub fn method_security_scope_paths() -> Vec<String> {
    let mut paths: Vec<String> = crate::runtime::service::method_security_registry()
        .iter()
        .filter(|(_, security)| !security.scopes.is_empty())
        .map(|(path, _)| path.clone())
        .collect();
    paths.sort();
    paths
}

pub fn method_security_scope_count(path: &str) -> usize {
    crate::runtime::service::method_security(path)
        .map(|security| security.scopes.len())
        .unwrap_or_default()
}

// ── Generic-dispatch request parsing ──────────────────────────────────────────

pub fn parse_sql_dispatch(request_json: &str) -> Result<(String, Vec<JsonValue>), tonic::Status> {
    eu::parse_sql_dispatch(request_json)
}

#[cfg(any(feature = "gcs", feature = "azureblob"))]
pub fn parse_object_dispatch(
    req: &str,
    bucket_keys: &[&str],
    object_keys: &[&str],
    bucket_label: &str,
) -> Result<(String, String, String, Option<String>), tonic::Status> {
    eu::parse_object_dispatch(req, bucket_keys, object_keys, bucket_label)
}

#[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
pub fn parse_rest_dispatch(
    req: &str,
) -> Result<(reqwest::Method, String, JsonValue), tonic::Status> {
    eu::parse_rest_dispatch(req)
}

// ── Object/blob codecs (base64 — the D.6 SIMD candidates) ─────────────────────

/// base64 DECODE path: `{"data_base64"|"content_base64":"…"}` → raw bytes.
pub fn object_bytes_from_json(value: &JsonValue) -> Result<Vec<u8>, tonic::Status> {
    eu::object_bytes_from_json(value)
}

/// base64 ENCODE path: raw bytes → `base64:<STANDARD>` cell string.
#[cfg(any(feature = "mysql", feature = "sqlite", feature = "mssql"))]
pub fn base64_cell(bytes: &[u8]) -> JsonValue {
    eu::base64_cell(bytes)
}

// ── D.6 decision bench: scalar vs maintained-SIMD-crate, measured side-by-side
// in ONE run (so the delta is immune to cross-run machine-load variance). Both
// paths go through the real impls the accel layer would dispatch to.

/// `simd-codecs` base64 scalar-vs-SIMD comparison handles.
#[cfg(feature = "simd-codecs")]
pub mod codec_bench {
    pub fn scalar_encode(data: &[u8]) -> String {
        crate::runtime::accel::scalar::base64_encode(data)
    }
    pub fn simd_encode(data: &[u8]) -> String {
        base64_simd::STANDARD.encode_to_string(data)
    }
    pub fn scalar_decode(s: &str) -> Vec<u8> {
        crate::runtime::accel::scalar::base64_decode(s).expect("valid base64")
    }
    pub fn simd_decode(s: &str) -> Vec<u8> {
        base64_simd::STANDARD
            .decode_to_vec(s.as_bytes())
            .expect("valid base64")
    }
}

/// `simd-checksum` crc32 scalar-vs-SIMD comparison handles.
#[cfg(feature = "simd-checksum")]
pub mod checksum_bench {
    pub fn scalar_crc32(data: &[u8]) -> u32 {
        crate::runtime::accel::scalar::crc32(data)
    }
    pub fn simd_crc32(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }
}

// ── Live-backend handles (D.2 end-to-end perf against the docker stack) ───────
//
// Thin handles that build a REAL UDB executor from a DSN and expose the uniform
// generic-dispatch ops (query/mutate, object put/get), so
// `benches/live_backends_bench.rs` measures UDB's actual per-backend data path
// end-to-end — parse dispatch JSON → execute on the live backend → row→JSON
// conversion — not just the CPU-bound helpers above. Errors are flattened to
// `String` so the bench crate need not depend on `tonic`. Each handle is gated
// on its backend feature; the bench skips a backend whose DSN env is unset.

/// Relational executor handle over the three SQL engines (Postgres / MySQL /
/// MSSQL), driven with `{"sql":..,"params":[..]}` generic-dispatch payloads.
pub enum SqlBench {
    #[cfg(feature = "postgres")]
    Postgres(crate::runtime::executors::postgres::PostgresExecutor),
    #[cfg(feature = "mysql")]
    MySql(crate::runtime::executors::mysql::MysqlExecutor),
    // Holds the (Clone) client so `raw_exec` can run DDL via `simple_batch`; the
    // validated executor is built on demand for query/mutate.
    #[cfg(feature = "mssql")]
    Mssql(crate::runtime::executors::mssql::MssqlClient),
}

impl SqlBench {
    #[cfg(feature = "postgres")]
    pub async fn connect_postgres(dsn: &str) -> Result<Self, String> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect(dsn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self::Postgres(
            crate::runtime::executors::postgres::PostgresExecutor::with_pool(pool),
        ))
    }

    #[cfg(feature = "mysql")]
    pub async fn connect_mysql(dsn: &str) -> Result<Self, String> {
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(8)
            .connect(dsn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self::MySql(
            crate::runtime::executors::mysql::MysqlExecutor::with_pool(pool),
        ))
    }

    #[cfg(feature = "mssql")]
    pub fn connect_mssql(ado_connection_string: &str) -> Self {
        Self::Mssql(crate::runtime::executors::mssql::MssqlClient::new(
            ado_connection_string,
        ))
    }

    pub async fn query(&self, request_json: &str) -> Result<String, String> {
        use crate::runtime::executors::QueryExecutor as _;
        let r = match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(e) => e.query(request_json).await,
            #[cfg(feature = "mysql")]
            Self::MySql(e) => e.query(request_json).await,
            #[cfg(feature = "mssql")]
            Self::Mssql(c) => {
                crate::runtime::executors::mssql::MssqlExecutor::new(c.clone())
                    .query(request_json)
                    .await
            }
        };
        r.map_err(|s| s.to_string())
    }

    pub async fn mutate(&self, request_json: &str) -> Result<String, String> {
        use crate::runtime::executors::MutationExecutor as _;
        let r = match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(e) => e.mutate(request_json).await,
            #[cfg(feature = "mysql")]
            Self::MySql(e) => e.mutate(request_json).await,
            #[cfg(feature = "mssql")]
            Self::Mssql(c) => {
                crate::runtime::executors::mssql::MssqlExecutor::new(c.clone())
                    .mutate(request_json)
                    .await
            }
        };
        r.map_err(|s| s.to_string())
    }

    /// Run raw SQL directly on the underlying driver, bypassing UDB's DML-only
    /// `mutate` validation — for bench *setup* (CREATE/DROP TABLE), which is not
    /// part of what we measure.
    pub async fn raw_exec(&self, sql: &str) -> Result<(), String> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(e) => sqlx::query(sql)
                .execute(&e.pool)
                .await
                .map(|_| ())
                .map_err(|x| x.to_string()),
            #[cfg(feature = "mysql")]
            Self::MySql(e) => sqlx::query(sql)
                .execute(&e.pool)
                .await
                .map(|_| ())
                .map_err(|x| x.to_string()),
            #[cfg(feature = "mssql")]
            Self::Mssql(c) => c.simple_batch(sql).await,
        }
    }
}

/// Object-store executor handle over S3/MinIO, driven with the same dispatch
/// JSON the typed object RPCs use.
#[cfg(feature = "s3")]
pub struct S3Bench(crate::runtime::executors::s3::S3Executor);

#[cfg(feature = "s3")]
impl S3Bench {
    pub fn connect(endpoint: &str, access: &str, secret: &str, region: &str) -> Self {
        use aws_sdk_s3::config::{Credentials, Region};
        let creds = Credentials::new(access, secret, None, None, "udb-bench");
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .credentials_provider(creds)
            .region(Region::new(region.to_string()))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        Self(crate::runtime::executors::s3::S3Executor(
            aws_sdk_s3::Client::from_conf(conf),
        ))
    }

    pub async fn ensure_bucket(&self, bucket: &str) -> Result<(), String> {
        use crate::runtime::executors::ResourceAdminExecutor as _;
        self.0
            .ensure_resource(bucket, "{}")
            .await
            .map_err(|s| s.to_string())
    }

    pub async fn put(&self, request_json: &str, bytes: Vec<u8>) -> Result<String, String> {
        use crate::runtime::executors::ObjectExecutor as _;
        self.0
            .put_object(request_json, bytes)
            .await
            .map_err(|s| s.to_string())
    }

    pub async fn get(&self, request_json: &str) -> Result<Vec<u8>, String> {
        use crate::runtime::executors::ObjectExecutor as _;
        self.0
            .get_object(request_json)
            .await
            .map_err(|s| s.to_string())
    }

    pub async fn delete(&self, request_json: &str) -> Result<(), String> {
        use crate::runtime::executors::ObjectExecutor as _;
        self.0
            .delete_object(request_json)
            .await
            .map_err(|s| s.to_string())
    }
}
