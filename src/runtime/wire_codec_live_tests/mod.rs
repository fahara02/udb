//! Wire-codec SERVED round-trip live tests for this session's codec fixes.
//!
//! Unlike a unit test of a decoder function, every test here reproduces the way
//! a CUSTOMER hit the corruption: it WRITES a value through the real data-plane
//! mutation path (the `bind_one` legacy Upsert bind, or the backend executor's
//! `MutationExecutor::mutate`) and READS it back through the real query path
//! (`QueryExecutor::query`, which funnels rows through the exact serializer the
//! Select RPC uses — `pg_rows_to_json` → `row_value_to_json`, the shared
//! `sqlx_row_to_json`, the MSSQL `row_to_json`, and the Cassandra `cql_to_json`).
//! Each asserts an EXACT round-trip, so reverting the fix (which re-collapses the
//! value to `null` / `[]` / a double-encoded string, or fails the bind) turns the
//! assertion — or the write itself — red.
//!
//! They are runtime-skipped when the matching `UDB_*_DSN` env var is unset, so a
//! default local `cargo test` stays green with zero external dependencies. The
//! required integration-CI lane sets `UDB_WIRE_CODEC_LIVE_TESTS=1`; under that
//! opt-in a missing DSN panics instead of silently reporting success. Each
//! per-backend file is named `*_tests.rs` so the
//! `connection_manager::runtime_env_reads_are_confined…` guardrail treats its
//! `std::env::var` reads as test config.

/// Preserve zero-infrastructure local test runs while making the provisioned CI
/// lane fail closed if a backend DSN is renamed, omitted, or otherwise drifts.
/// One shared gate prevents four backend-specific skip policies from diverging.
fn live_dsn(dsn: Option<String>, names: &str) -> Option<String> {
    if dsn.is_none()
        && std::env::var("UDB_WIRE_CODEC_LIVE_TESTS")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    {
        panic!("{names} unset while UDB_WIRE_CODEC_LIVE_TESTS requires served wire-codec coverage");
    }
    dsn
}

#[cfg(feature = "postgres")]
mod postgres_tests;

#[cfg(feature = "mysql")]
mod mysql_tests;

#[cfg(feature = "mssql")]
mod mssql_tests;

#[cfg(feature = "cassandra")]
mod cassandra_tests;
