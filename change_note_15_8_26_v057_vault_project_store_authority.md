# UDB v0.5.7 Vault project-store authority correction

Date: 2026-08-15
Status: implementation complete; GitHub CI pending

## Changed

- Pinned every served Vault secret/transit operation to one active-project
  Postgres write authority before its first typed or raw store access.
- Made `ListSecrets` use that same canonical authority instead of independently
  selecting a read target.
- Threaded the pinned request context into Vault outbox emission so weighted
  routing cannot split an operation and its audit event across instances.
- Added a real tonic-client regression using two projects and two separately
  provisioned PostgreSQL databases. It covers typed Put/Get/Create/Encrypt,
  raw List/Destroy/Rotate, cross-project survival, version independence, and
  project-local outbox placement.

## Safety posture

- Unknown or inactive projects fail closed before any Vault store is touched.
- The wire cannot supply the internal target-instance pin; the service derives
  it from the project-aware runtime authority.
- Vault reads use the write authority deliberately, avoiding stale or split key
  material across read/write topology.
- Dynamic DB-credential lease provenance and exact-instance reaping remain in
  the dedicated credential-lifecycle change because that work owns the lease
  entity contract and worker.

## Verification requested from GitHub CI

No local Cargo/build/test command was run, per operator direction. Required CI:

```bash
cargo fmt --all -- --check
cargo test --locked --lib
UDB_LIVE_AUTH_TESTS=1 \
UDB_INTEGRATION_PG_DSN=postgres://udb:udb@localhost:55432/udb \
UDB_ENCRYPTION_KEY=QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI= \
cargo test --locked --lib \
  runtime::service::vault_service::project_store_live::served_vault_pins_typed_raw_and_outbox_paths_to_each_project_instance \
  -- --ignored --nocapture --test-threads=1
```

The standard `ci.yml` native-service live-test step already supplies the live
Postgres and encryption environment and runs ignored library tests serially.
