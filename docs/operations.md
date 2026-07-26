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
│    crate v0.4.28 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This is the guide for running UDB in production. If you operate a UDB
deployment — day to day, or on call when something breaks — this page is for
you. It covers how to run the broker, what to check before going live, the
service objectives (SLOs) to hold yourself to, the runbooks to reach for when
things go wrong, and how to validate that a release is ready.

## Runtime Shape

UDB runs two listeners. The **data plane** is the public gRPC endpoint your
application clients talk to. Run it where those clients can reach it:

```bash
udb serve proto "" 0.0.0.0:50051
```

If you'd rather pull the address from the environment, set `UDB_GRPC_ADDR` or
`UDB_GRPC_BIND_ADDR`.

The **control plane** — the native services that handle auth, storage, and the
rest — is more sensitive, so keep it off the public internet. Run it on an
internal network or behind a trusted gateway:

```bash
$env:UDB_AUTH_GRPC_ADDR = "127.0.0.1:50052"
udb serve proto "" 0.0.0.0:50051
```

## Local Playground

Want a broker running on your laptop in one command? These bring one up, poke it
with a smoke test, tail its logs, and tear it back down:

```bash
udb dev up
udb dev smoke
udb dev logs
udb dev down
```

## Health And Diagnostics

Three commands answer "is this broker healthy, and can it do what my projects
need?"

```bash
udb doctor --human
udb health-check
udb compat-matrix
udb native list --json
```

Reach for `doctor` when you want an operator-readable readiness report,
`health-check` for a lightweight liveness probe, and `compat-matrix` to see
whether each backend is configured and which operations it supports.

## Production Checklist

Walk this list before you send real traffic. Each row is one thing that will
hurt in production if it isn't handled up front.

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

Start from these files:

- `configs/database.yaml`
- `configs/backends.yaml`
- `configs/services.yaml`
- `.env.example`

Keep secrets in environment variables, and never commit real credentials to any
of these.

These environment settings control the broker's core wiring — listeners, JWT
validation, and the optional media services:

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

And these point the broker at whichever backends your deployment uses:

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

Schema changes flow from your proto definitions. Lint them, plan the change
against the previous manifest to see what will move, then sync it to a backend:

```bash
udb lint proto --human
udb plan proto --prior previous-manifest.json
udb dbops sync --backend postgres
```

Always read the generated SQL and migration artifacts before you apply them to
production.

Where you can, use separate credentials for runtime access, migrations, and
short-lived native access — a leaked runtime credential shouldn't be able to
rewrite your schema.

## Deployment Profiles

Most deployments look like one of these shapes. Find the one closest to yours
and use it as a starting point.

| Profile | Shape |
|---|---|
| Local developer | SQLite or Postgres, `udb dev up`, insecure local gRPC |
| Reference SaaS | Postgres system state, configured object storage, TLS, audit events, SDK metadata injection |
| Multi-backend application | Project catalog routes relational, object, vector, cache, graph, and analytics operations to configured instances |
| Enterprise identity | Internal native listener, OIDC/SAML, SCIM, MFA, signed policy bundles, audit retention |
| High-availability broker | Multiple broker replicas, singleton leases for background workers, external load balancer, backend-specific pool sizing |

On Kubernetes, treat UDB objects — broker deployments, project catalogs, backend
instances, migration runs, CDC streams, and projection workers — as separate
operational concerns, even when one repository applies them all together. They
fail and scale independently, so manage them that way.

## Native Service Operations

The native services are the control-plane building blocks (auth, storage, WebRTC,
and more). List what's running, check the health of specific ones, or scaffold a
client app wired to the services you name:

```bash
udb native list
udb native doctor auth storage webrtc
udb app init --lang typescript --framework express --services auth,storage
```

Each service is descriptor-driven, so you can enable exactly the ones a given
deployment needs.

## SLO Lanes

A service-level objective (SLO) is the performance and reliability target you
promise for a given slice of traffic. Track each lane below on its own — a
healthy read path doesn't tell you anything about CDC lag.

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

Set separate objectives for public data-plane traffic and internal control-plane
traffic — they have different users and different stakes. And treat any tenant
isolation, audit, or method-security failure not as a performance blip but as a
correctness incident: those are the guarantees UDB exists to keep.

## Events And CDC

CDC (change data capture) streams every write out as an event. Before you turn it
on — or enable native-service event publishing — configure your Kafka and outbox
settings. Once it's live, watch lag, DLQ (dead-letter queue) depth, replay count,
and publish failures.

In practice, track:

- outbox depth by tenant/project;
- publish latency and publish failure count;
- DLQ enqueue and replay count;
- topic-policy rejection count;
- consumer lag;
- schema/catalog version attached to emitted events.

## Common Runbooks

When something breaks, start here. Each row pairs a symptom with the first things
worth checking — not an exhaustive fix, but the fastest path to the cause.

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

Back up the canonical store that owns UDB system state for your deployment. Make
sure that backup includes:

- catalog versions and project bindings;
- migration run and operation ledgers;
- native auth/authz/tenant/storage/asset/WebRTC state;
- CDC outbox, offsets, DLQ, and topic policy state;
- saga and projection task state;
- audit/event retention stores.

The actual file bytes live in object storage, not in that store. Back up object
metadata and object storage under the same retention policy, so that when you
restore, metadata and bytes come back together and stay consistent.

## Load And Soak

Load tests only mean something if they hit the real broker path — the shipped
request pipeline, not isolated helper functions. These profiles each stress a
different part of that path:

| Profile | Goal |
|---|---|
| `read-heavy` | steady relational/document/vector reads |
| `write-heavy` | mutation latency, audit, and CDC behavior |
| `mixed-projection` | canonical writes plus projection/search refresh |
| `tenant-noisy-neighbor` | admission and fairness under one busy tenant |
| `backend-outage` | degraded backend behavior and refusal paths |
| `reload-during-traffic` | catalog/config reload behavior |
| `multi-project-smoke` | independent project routing through one broker |

A local run looks like this:

```bash
UDB_HOST=localhost:50051 CONCURRENCY=50 TOTAL_REQUESTS=10000 PROFILE=read-heavy ./scripts/load_test.sh
```

There are load helpers for the native services too:

```bash
./scripts/auth-load-test.sh
./scripts/native-load-test.sh
```

## Performance Baseline

Any performance claim should come with a measured before-and-after number, not a
hunch. The benchmark suite covers both CPU hot paths and live backend execution
paths:

```bash
python data/gen_bench_data.py --target-mb 512
cargo bench --features bench-internals --bench hotpath_bench
UDB_BENCH_LIVE=1 cargo bench --features bench-internals --bench live_backends_bench
python scripts/bench_snapshot.py --label "release-0.4.28"
```

Once you record a snapshot, its history is kept under `bench-history/`, and the
raw Criterion output lands under `target/criterion/`.

## Validation

These run quickly and catch most regressions before they leave your machine:

```bash
cargo test --lib
buf lint
buf build
node scripts/check-versions.mjs
node sdk-conformance/run.mjs
```

The heavier checks — live backends, high availability, and load — need matching
infrastructure, so run them in an environment that mirrors your production
topology rather than a laptop.

Before you call a release ready, gather evidence: the SDK conformance runner
(`sdk-conformance/run.mjs`), native-service load coverage
(`scripts/native-load-test.sh` or `scripts/native-load-test.ps1`), a multi-node
broker exercise, and compliance-mode checks that confirm audit, method security,
and redaction all behave.
