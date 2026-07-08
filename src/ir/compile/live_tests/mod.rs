//! Env-gated live IR compiler semantics checks.
//!
//! The compile-time cross-backend tests prove render shape. These tests take
//! the next step: compile neutral IR operations, execute the emitted backend
//! wire form against real engines, and compare normalized result rows.

mod postgres_live;
mod support;

#[cfg(feature = "mysql")]
mod mysql_live;

#[cfg(feature = "mssql")]
mod mssql_live;

#[cfg(feature = "sqlite")]
mod sqlite_live;

#[cfg(feature = "elasticsearch")]
mod elasticsearch_live;

#[cfg(feature = "azureblob")]
mod azureblob_live;

#[cfg(feature = "cassandra")]
mod cassandra_live;

#[cfg(feature = "clickhouse")]
mod clickhouse_live;

#[cfg(feature = "gcs")]
mod gcs_live;

#[cfg(feature = "memcached")]
mod memcached_live;

#[cfg(feature = "mongodb")]
mod mongodb_live;

#[cfg(any(feature = "azureblob", feature = "gcs", feature = "s3"))]
mod object_live_support;

#[cfg(feature = "neo4j")]
mod neo4j_live;

#[cfg(feature = "qdrant")]
mod qdrant_live;

#[cfg(feature = "pinecone")]
mod pinecone_live;

#[cfg(feature = "redis")]
mod redis_live;

#[cfg(feature = "s3")]
mod s3_live;

#[cfg(feature = "weaviate")]
mod weaviate_live;
