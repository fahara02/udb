# Testing

The default Rust suite should not require external services. Live service tests
must be explicit, ignored, or environment-gated.

## Local Gates

```powershell
cargo test --lib
cargo test --workspace
cargo check --all-features
cargo check --no-default-features --features postgres
cargo test -p udb-portable
```

Recommended hygiene gates:

```powershell
cargo fmt --check
cargo clippy --workspace --all-targets --all-features
cargo deny check advisories bans licenses sources
```

## CLI Smoke

```powershell
cargo run --bin udb-proto-parser -- doctor --human
cargo run --bin udb-proto-parser -- lint proto
cargo run --bin udb-proto-parser -- system-ddl
cargo run --bin udb-proto-parser -- policy-lint
```

## Playground

```powershell
cargo run --bin udb-proto-parser -- dev up
cargo run --bin udb-proto-parser -- dev status
cargo run --bin udb-proto-parser -- dev smoke
cargo run --bin udb-proto-parser -- dev down
```

## Live Acceptance Matrix

| Area | Evidence to collect |
|---|---|
| Postgres canonical path | typed select/upsert/delete, migration apply, catalog activation |
| MySQL/SQLite canonical path | conformance tests and one live round trip when available |
| Optional backend plugins | feature enabled, env set, plugin appears in capabilities |
| CDC | outbox publish, offset resume, DLQ replay/quarantine |
| Saga recovery | retry, compensation, manual-review path |
| Reload | traffic continues through config/client replacement |
| Backup/restore | clean restore plus projection rebuild |
| Tenant fairness | queue waits and rate limits under noisy-neighbor traffic |

Do not write static performance numbers into docs unless they include date,
commit, feature set, hardware, service topology, and command.
