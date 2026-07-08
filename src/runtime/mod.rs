pub mod accel; // D.5: scalar-first acceleration layer (codecs/checksum) + cached CPU detect
pub mod catalog;
pub mod cdc;

pub mod channels;
pub mod config;
pub mod connection_manager;
pub mod core;
pub mod descriptor_diff; // F6: native-service contract version + descriptor-diff classifier
pub mod descriptor_manifest;
mod encryption;
pub mod evidence_export; // 4.4: leader-elected compliance-evidence export worker (chain-hashed JSONL → object store)
pub(crate) mod executor_utils;
pub mod executors;
pub mod metrics;
pub mod migration_audit;
pub mod observability;
pub mod otel; // Phase 10: W3C trace-context extract/scope/inject + feature-gated OTLP export
pub mod pipeline;
pub mod pipeline_coverage; // U7: per-RPC pipeline-adoption tracker
pub(crate) mod postgres_helpers;
pub mod preflight; // UDB_FRICTION §2: one-shot enterprise prerequisite report (startup + doctor)

pub mod authn; // Stage 1 auth: UDB-owned authn primitives (sessions, API keys, identities, hashing)
pub mod authz; // Stage 1 auth: UDB-owned authz engine (Decision/Principal/ResourceRef, RBAC/ABAC/ReBAC)
pub mod backend_context; // NW-deep: uniform request-context applicator across all backends
pub mod canonical_store; // P2P: pluggable canonical-store trait + Postgres/MySQL/SQLite impls
pub mod consistency;
pub mod consistency_fence;
pub mod drift_reconciliation;
pub mod native_catalog; // Native-service entity protos (embedded) → CatalogManifest (proto-driven migration; no hand-written DDL)
#[cfg(feature = "runtime-logging")]
pub(crate) mod pretty_log;
pub mod project_backend_router; // U4 step 2: strict project-scoped instance routing
#[cfg(test)]
mod project_isolation_tests;
pub mod projection;
#[cfg(test)]
mod projection_acceptance_tests;
pub mod replica;
#[cfg(test)]
mod rls_context_tests;
pub mod saga;
pub mod saga_compensators; // U19: plugin-aware backend compensator dispatch
pub mod schema_registry;
pub mod sdk_manifest; // Embedded descriptor set → RPC manifest for `udb sdk generate` (proto = source of truth)
pub mod security;
pub mod service;
pub mod signal;
#[cfg(feature = "ws-signalling")]
pub mod signalling; // Pluggable ws:// signalling bridge (Pixel Streaming first protocol)
pub mod singleton;
pub mod slo; // Phase 10: SLO/error-budget catalog + unified readiness contract
pub mod system;
pub mod tenant_movement;
pub mod xa;
pub mod xa_postgres;
pub mod xa_recovery; // C6: in-doubt 2PC recovery worker (PG + MySQL participants)

pub use channels::{ChannelManager, OperationChannel};
pub use connection_manager::{ClientSnapshot, ClientState, ConnectionManager, PoolingMode};
pub use core::{
    ConfigReloadMode, ConfigReloadOptions, ConfigReloadReport, DataBrokerRuntime, RuntimeInitReport,
};
#[cfg(feature = "clickhouse")]
pub use executors::clickhouse::{ClickHouseConfig, ClickHouseExecutor};
#[cfg(feature = "mongodb")]
pub use executors::mongodb::{MongoDbConfig, MongoDbExecutor};
#[cfg(feature = "neo4j")]
pub use executors::neo4j::{Neo4jConfig, Neo4jExecutor};
pub use migration_audit::PostgresMigrationAuditSink;
