# Operations


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
│    crate v0.4.14 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This page covers day-to-day operation, production readiness, SLOs, runbooks,
and validation for a UDB deployment.

## Runtime Shape

Run the public data-plane listener where application clients can reach it:

```bash
udb serve proto "" 0.0.0.0:50051
```

Set `UDB_GRPC_ADDR` or `UDB_GRPC_BIND_ADDR` when the address should come from
the environment.

Run the native control plane on an internal network or behind a trusted gateway:

```bash
$env:UDB_AUTH_GRPC_ADDR = "127.0.0.1:50052"
udb serve proto "" 0.0.0.0:50051
```

## Local Playground

```bash
udb dev up
udb dev smoke
udb dev logs
udb dev down
```

## Health And Diagnostics

```bash
udb doctor --human
udb health-check
udb compat-matrix
udb native list --json
```

Use `doctor` for operator-readable readiness, `health-check` for lightweight
probes, and `compat-matrix` for backend capability/configuration status.

## Production Checklist

| Area | Check |
|---|---|
| Transport | TLS for public traffic; internal or gateway-protected native listener |
| Identity | JWT issuer/audience/key source configured; MFA for privileged accounts |
| Metadata | SDKs or gateway middleware attach tenant, project, purpose, scopes, correlation id, and service identity |
| Backends | Every project backend is configured, reachable, and visible in `udb compat-matrix` |
| State | UDB system/native state has backups, restore practice, and migration ownership |
| Secrets | Keys, tokens, backend passwords, and policy bundle secrets live in a secret manager |
| Events | Audit, CDC, DLQ, replay, and retention are configured before production traffic |
| Scale | Broker replicas, admission limits, and backend pools are sized together |
| CI | Rust, proto, SDK, descriptor, and conformance checks run before release |
| Recovery | Runbooks exist for backend outage, auth failures, CDC lag, and policy rollback |

## Configuration

Primary config files:

- `configs/database.yaml`
- `configs/backends.yaml`
- `configs/services.yaml`
- `.env.example`

Prefer environment variables for secrets. Do not commit real credentials.

Important environment settings:

| Setting | Purpose |
|---|---|
| `UDB_GRPC_ADDR` / `UDB_GRPC_BIND_ADDR` | Public broker listener |
| `UDB_AUTH_GRPC_ADDR` | Native control-plane listener |
| `UDB_JWT_ISSUER` / `UDB_JWT_AUDIENCE` | JWT validation expectations |
| `UDB_JWT_PUBLIC_KEY` / `UDB_JWT_JWKS_URL` | JWT validation key source |
| `UDB_JWT_PRIVATE_KEY` | UDB-issued token signing |
| `UDB_POLICY_BUNDLE_SECRET` | Signed policy bundles |
| `UDB_REQUIRE_SECURE_TRANSPORT` | Require secure transport in strict deployments |
| `UDB_STORAGE_OBJECT_BACKEND` | Object backend for native storage |
| `UDB_WEBRTC_GRPC_ADDR` | Optional peer-facing WebRTC listener |
| `UDB_TURN_URLS` / `UDB_TURN_SECRET` | TURN credential configuration |
| `UDB_WS_SIGNALLING_ADDR` | Optional WebSocket signalling bridge |

Backend environment variables commonly used by deployments:

| Backend | Settings |
|---|---|
| Postgres | `UDB_DATABASE_URL`, `UDB_POSTGRES_DSN` |
| MySQL | `UDB_MYSQL_DSN` |
| SQLite | `UDB_SQLITE_PATH` |
| SQL Server | `UDB_MSSQL_DSN` |
| Redis | `UDB_REDIS_URL` |
| Memcached | `UDB_MEMCACHED_URL` |
| ClickHouse | `UDB_CLICKHOUSE_DSN`, `UDB_COLUMN_DSN`, `UDB_COLUMN_HTTP_URL` |
| Cassandra / Scylla | `UDB_CASSANDRA_DSN` |
| MongoDB | `UDB_MONGODB_DSN`, `UDB_NOSQL_DSN`, `UDB_NOSQL_API_URL` |
| Neo4j | `UDB_NEO4J_DSN`, `UDB_GRAPH_DSN`, `UDB_GRAPH_HTTP_URL` |
| Qdrant | `UDB_QDRANT_URL`, `UDB_QDRANT_API_KEY` |
| Weaviate | `UDB_WEAVIATE_URL`, `UDB_WEAVIATE_API_KEY` |
| Pinecone | `UDB_PINECONE_API_KEY`, `UDB_PINECONE_INDEX` |
| S3 / MinIO | `UDB_S3_BUCKET`, `UDB_MINIO_ENDPOINT`, `UDB_MINIO_ACCESS_KEY`, `UDB_MINIO_SECRET_KEY` |
| Azure Blob | `UDB_AZUREBLOB_DSN` |
| Google Cloud Storage | `UDB_GCS_DSN` |
| Kafka / CDC | `UDB_KAFKA_BROKERS` |

## Migrations And Database Ops

```bash
udb lint proto --human
udb plan proto --prior previous-manifest.json
udb dbops sync --backend postgres
```

Review generated SQL and migration artifacts before production apply.

Use separate credentials where possible for runtime access, migrations, and
short-lived native access.

## Deployment Profiles

| Profile | Shape |
|---|---|
| Local developer | SQLite or Postgres, `udb dev up`, insecure local gRPC |
| Reference SaaS | Postgres system state, configured object storage, TLS, audit events, SDK metadata injection |
| Multi-backend application | Project catalog routes relational, object, vector, cache, graph, and analytics operations to configured instances |
| Enterprise identity | Internal native listener, OIDC/SAML, SCIM, MFA, signed policy bundles, audit retention |
| High-availability broker | Multiple broker replicas, singleton leases for background workers, external load balancer, backend-specific pool sizing |

For Kubernetes, treat UDB objects such as broker deployments, project catalogs,
backend instances, migration runs, CDC streams, and projection workers as
separate operational concerns even when they are applied from one repository.

## Native Service Operations

```bash
udb native list
udb native doctor auth storage webrtc
udb app init --lang typescript --framework express --services auth,storage
```

Native services are descriptor-driven and can be enabled per deployment.

## SLO Lanes

| Lane | Useful signals |
|---|---|
| DataBroker reads/writes | latency, error rate, backend pool pressure, admission rejection |
| Authn | login, refresh, validate latency and failures |
| Authz | check/batch latency, denial rate, policy revision and bundle version |
| Storage | presign, finalize, list latency and object backend errors |
| Asset | pipeline start, step completion, executor failures |
| WebRTC | join, signalling latency, TURN issuance failures |
| CDC | lag, DLQ depth, publish failures, replay count |
| Policy distribution | ACK/NACK count, version lag, rollback count |

Keep separate objectives for public data-plane traffic and internal
control-plane traffic. Treat tenant isolation, audit, and method-security
failures as correctness incidents.

## Events And CDC

Configure Kafka/outbox settings before enabling CDC or native-service event
publishing. Monitor lag, DLQ depth, replay count, and publish failures.

CDC and event operations should track:

- outbox depth by tenant/project;
- publish latency and publish failure count;
- DLQ enqueue and replay count;
- topic-policy rejection count;
- consumer lag;
- schema/catalog version attached to emitted events.

## Common Runbooks

| Situation | First checks |
|---|---|
| Broker not ready | `udb doctor --human`, listener env vars, backend reachability |
| Backend operation rejected | `udb compat-matrix`, project catalog, operation capability |
| Auth login failures | JWT/IdP settings, native listener reachability, clock skew, audit events |
| Unexpected authorization denial | request scopes, tenant/project ids, policy revision, `CheckAccess` result |
| CDC lag | event sink health, outbox depth, DLQ depth, replay workers |
| Storage presign failure | object backend config, tenant quotas, native storage state |
| WebRTC join/signalling issue | TURN config, room/peer state, gRPC or WebSocket listener health |
| Policy rollout issue | active bundle version, ACK/NACK status, rollback command path |
| CDC backlog | outbox depth, consumer lag, broker publish errors, replay worker capacity |
| DLQ recovery | failed event reason, replay eligibility, topic policy, idempotency state |
| Native service dependency outage | native listener health, backing store health, secret/config availability |
| Object/vector backend partial failure | affected project bindings, backend capability matrix, degraded-operation policy |
| Leader-election failover | singleton lease holder, lease age, standby readiness, worker resume state |
| Bad policy rollout rollback | active and previous bundle versions, NACK reason, rollback audit event |
| Access review workflow | privileged principals, stale grants, service identities, approval evidence |

## Backup And Recovery

Back up the canonical store that owns UDB system state for the deployment.
Include:

- catalog versions and project bindings;
- migration run and operation ledgers;
- native auth/authz/tenant/storage/asset/WebRTC state;
- CDC outbox, offsets, DLQ, and topic policy state;
- saga and projection task state;
- audit/event retention stores.

Object bytes remain in object storage. Back up object metadata and object
storage according to the same retention policy so metadata and bytes can be
restored together.

## Load And Soak

Load profiles should exercise the shipped broker path, not only isolated helper
functions. Useful profiles:

| Profile | Goal |
|---|---|
| `read-heavy` | steady relational/document/vector reads |
| `write-heavy` | mutation latency, audit, and CDC behavior |
| `mixed-projection` | canonical writes plus projection/search refresh |
| `tenant-noisy-neighbor` | admission and fairness under one busy tenant |
| `backend-outage` | degraded backend behavior and refusal paths |
| `reload-during-traffic` | catalog/config reload behavior |
| `multi-project-smoke` | independent project routing through one broker |

Example local shape:

```bash
UDB_HOST=localhost:50051 CONCURRENCY=50 TOTAL_REQUESTS=10000 PROFILE=read-heavy ./scripts/load_test.sh
```

Native-service load helpers are also available:

```bash
./scripts/auth-load-test.sh
./scripts/native-load-test.sh
```

## Performance Baseline

Performance work should cite a measured before/after result. The benchmark
suite covers CPU hot paths and live backend execution paths:

```bash
python data/gen_bench_data.py --target-mb 512
cargo bench --features bench-internals --bench hotpath_bench
UDB_BENCH_LIVE=1 cargo bench --features bench-internals --bench live_backends_bench
python scripts/bench_snapshot.py --label "release-0.4.14"
```

Durable benchmark history lives under `bench-history/` when snapshots are
recorded. Generated Criterion output lives under `target/criterion/`.

## Validation

Fast checks:

```bash
cargo test --lib
buf lint
buf build
node scripts/check-versions.mjs
node sdk-conformance/run.mjs
```

Live backend, HA, and load checks require matching infrastructure and should be
run in an environment that mirrors production topology.

Readiness evidence should include the SDK conformance runner
(`sdk-conformance/run.mjs`), native service load coverage
(`scripts/native-load-test.sh` or `scripts/native-load-test.ps1`), a multi-node
broker exercise, and compliance-mode checks for audit, method security, and
redaction behavior.
