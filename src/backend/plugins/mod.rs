//! Per-backend plugin instances (U2).
//!
//! Each plugin is a zero-sized struct implementing [`crate::backend::Backend`]
//! and exposing a `pub static` instance. The `all()` function below is the
//! single inventory point — registry build, generation dispatch, and dispatch
//! resolution all iterate this list. Feature-gated backends sit behind
//! `#[cfg(feature = "…")]` here, so the slim build genuinely drops them
//! without changing any consumer.

use crate::backend::plugin::Backend;

#[cfg(feature = "azureblob")]
pub mod azureblob;
#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
#[cfg(feature = "gcs")]
pub mod gcs;
#[cfg(feature = "memcached")]
pub mod memcached;
#[cfg(feature = "s3")]
pub mod minio;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "neo4j")]
pub mod neo4j;
#[cfg(feature = "pinecone")]
pub mod pinecone;
pub mod postgres;
#[cfg(feature = "qdrant")]
pub mod qdrant;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "weaviate")]
pub mod weaviate;

/// All compiled-in backend plugins, in canonical order
/// (sql → cache → vector → object → document → graph → column).
///
/// Order matters for log/UI listings, not for correctness.
pub fn all() -> Vec<&'static dyn Backend> {
    let mut out: Vec<&'static dyn Backend> = Vec::new();
    out.push(&postgres::PLUGIN);
    // NW3-1 + NW3-2: MySQL and SQLite are first-class SQL-tier
    // plugins. They sit next to Postgres in the inventory so
    // capability_matrix() lists them as runtime-supported and
    // GetCapabilities surfaces them to clients.
    #[cfg(feature = "mysql")]
    out.push(&mysql::PLUGIN);
    #[cfg(feature = "sqlite")]
    out.push(&sqlite::PLUGIN);
    #[cfg(feature = "redis")]
    out.push(&redis::PLUGIN);
    #[cfg(feature = "qdrant")]
    out.push(&qdrant::PLUGIN);
    #[cfg(feature = "s3")]
    {
        out.push(&minio::PLUGIN);
        out.push(&s3::PLUGIN);
    }
    #[cfg(feature = "mongodb")]
    out.push(&mongodb::PLUGIN);
    #[cfg(feature = "neo4j")]
    out.push(&neo4j::PLUGIN);
    #[cfg(feature = "clickhouse")]
    out.push(&clickhouse::PLUGIN);
    // C9: Elasticsearch as a real plugin — HTTP+JSON over reqwest.
    #[cfg(feature = "elasticsearch")]
    out.push(&elasticsearch::PLUGIN);
    // C9: Memcached as a real plugin — `memcache` crate binary
    // protocol wrapped in spawn_blocking.
    #[cfg(feature = "memcached")]
    out.push(&memcached::PLUGIN);
    // C9: SQL Server as a real plugin via tiberius (TDS protocol).
    #[cfg(feature = "mssql")]
    out.push(&mssql::PLUGIN);
    // C9: Weaviate (REST + GraphQL via reqwest).
    #[cfg(feature = "weaviate")]
    out.push(&weaviate::PLUGIN);
    // C9: Pinecone (REST via reqwest).
    #[cfg(feature = "pinecone")]
    out.push(&pinecone::PLUGIN);
    // C9: Cassandra / ScyllaDB via the `scylla` driver.
    #[cfg(feature = "cassandra")]
    out.push(&cassandra::PLUGIN);
    // C9: Azure Blob Storage via the official Azure SDK.
    #[cfg(feature = "azureblob")]
    out.push(&azureblob::PLUGIN);
    // C9: Google Cloud Storage via the canonical SDK.
    #[cfg(feature = "gcs")]
    out.push(&gcs::PLUGIN);
    out
}
