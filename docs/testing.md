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
│    crate v0.4.13 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
## Fast Checks

```bash
cargo test --lib
buf lint
buf build
node scripts/check-versions.mjs
node sdk-conformance/run.mjs
```

## Feature Sweeps

```bash
cargo test --all-features --lib
cargo test --no-default-features --features postgres --lib
cargo test --features clickhouse,mssql,cassandra --lib
```

## Descriptor And Docs

```bash
udb native lint
udb native docs
udb native manifest
```

## SDKs

```bash
cd sdk/typescript && npm test
cd sdk/python && pytest
cd sdk/go && go test ./...
dotnet test sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj
mvn -f sdk/java/pom.xml test
cd sdk/php && composer test
```

## Live Backends

Live checks require matching infrastructure. Start the integration compose stack
before enabling live-test environment variables.

```bash
docker compose -f docker-compose.integration.yml up -d --wait
UDB_INTEGRATION_TESTS=1 cargo test --test integration_tests -- --nocapture
docker compose -f docker-compose.integration.yml down -v --remove-orphans
```
