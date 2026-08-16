# Testing


```text
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│    ██    ██  ██████   ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│    ██    ██  ██   ██  ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│     ██████   ██████   ██████                                               │
│                                                                            │
│    UNIVERSAL DATA BROKER                                                   │
│    gRPC data plane | native control plane | tenant/project scope guard     │
│                                                                            │
│    crate v0.5.11 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This is the quick command reference for testing UDB. If you just want to know
"what do I run?", start at the top and work down: the fast checks need nothing
but the repo, and each later section adds more setup. For the fuller narrative
version with troubleshooting and platform notes, see [../TESTING.md](../TESTING.md).

## Fast Checks

Run these first. They need no databases and no `.env` file, so they finish
quickly and catch most mistakes before you push:

```bash
cargo test --lib
buf lint
buf build
node scripts/check-versions.mjs
node sdk-conformance/run.mjs
```

## Feature Sweeps

UDB compiles different backend drivers behind Cargo feature flags. These runs
compile and exercise those optional backends so a change to one store doesn't
quietly break another:

```bash
cargo test --all-features --lib
cargo test --no-default-features --features postgres --lib
cargo test --features clickhouse,mssql,cassandra --lib
```

## Descriptor And Docs

UDB's native services are described by a protobuf-derived contract. These
commands check that the contract still lints, that its docs are current, and let
you inspect the generated service manifest:

```bash
udb native lint
udb native docs
udb native manifest
```

## SDKs

Each language SDK has its own test suite. Run the one for the SDK you touched:

```bash
cd sdk/typescript && npm test
cd sdk/python && pytest
cd sdk/go && go test ./...
dotnet test sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj
mvn -f sdk/java/pom.xml test
cd sdk/php && composer test
```

## Live Backends

These tests talk to real databases, so they need matching infrastructure
running. Start the integration compose stack first, then set the live-test
environment variables. The last command tears the stack back down:

```bash
docker compose -f docker-compose.integration.yml up -d --wait
UDB_INTEGRATION_TESTS=1 cargo test --test integration_tests -- --nocapture
docker compose -f docker-compose.integration.yml down -v --remove-orphans
```
