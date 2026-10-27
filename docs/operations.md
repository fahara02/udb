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
│    crate v0.6.3 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This is the guide for running UDB in production. If you operate a UDB
deployment — day to day, or on call when something breaks — this page is for
you. It covers how to run the broker, what to check before going live, the
service objectives (SLOs) to hold yourself to, the runbooks to reach for when
things go wrong, and how to validate that a release is ready.

## Check code coverage

Before deployment, check a service's generated table calls against the same
identity and policy declarations used to provision it:

```bash
udb check app-proto --code ./... --identities identities.yaml \
  --identity billing --purpose invoicing --policies policies.yaml
```

The identities file must name the canonical tenant UUID and the selected
service account's UUID, service identity, project and grant scopes. The checker
reads that grant rather than assuming wildcard scopes. Add `--role` when the
service has a known role binding; confirm those bindings with live authorization
checks after deployment.

The embedded analyzer requires Go on PATH and uses `go/parser` and `go/ast`.
It compiles only its own standard-library program, then parses application
source as data. Module, toolchain and dependency downloads are disabled. It
resolves generated table constructors, variable aliases, typed parameters and
receiver fields, including sibling-file declarations. Syntax/read failures and
symlinked source directories fail the scan instead of producing an empty pass.
A literal `WithMetadata(..., Metadata{Purpose: "..."})` overrides the deployment
purpose. A dynamic purpose, an ambiguous entity or a permission depending on
runtime attributes/relationships produces a finding. This checks the resolved
table call sites; it does not establish the deployment's live policy state.

## Runtime Shape

The control-plane distribution subscriber wakes on PostgreSQL notifications
from committed resource writes. It checks for missed changes every 30 seconds
(`UDB_CONTROL_RELOAD_INTERVAL_MS` overrides that fallback). An unchanged world
skips full config re-sourcing; `udb_control_resync_total` counts completed resyncs,
including the startup baseline. Listener reconnects check the durable world to
recover changes from a connection gap.

All gRPC listeners send HTTP/2 keepalive pings every 30 seconds and close a
connection if its peer does not acknowledge within 20 seconds. This applies to
the public, native control-plane, WebRTC peer and optional Unix socket listeners.
LiveQuery additionally sends an explicit `Heartbeat` payload while idle, at the
`UDB_LIVEQUERY_KEEPALIVE_SECS` cadence (default 30 seconds; zero disables it).
Heartbeats contain no data or resume cursor and should be ignored by row handlers.

LiveQuery subscriptions share one 500 ms journal poll per tenant, project and
topic. Each stream applies its own IN/OR predicates to snapshots and deltas.
The default concurrent-stream limits are 1,024 per tenant and 4,096 per process;
override them with `UDB_LIVEQUERY_MAX_STREAMS_PER_TENANT` and
`UDB_LIVEQUERY_MAX_STREAMS_GLOBAL`. Delta channels remain bounded, and lagging
streams catch up from their own journal watermark. Monitor
`udb_livequery_journal_scans_total{source="shared"}` and `{source="catch_up"}` to
distinguish the shared polling load from reconnect or slow-subscriber recovery.

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
export UDB_AUTH_GRPC_ADDR=127.0.0.1:50061   # :50052 is the metrics port
udb serve proto "" 0.0.0.0:50051
```

### Rate limits and version pins

The broker logs the effective rate-limit window, tenant default, operation
ceilings, bucket scope and Redis failure policy when its runtime starts. A build
without Redis support logs that rate limiting is disabled. With Redis support,
a missing backend uses the configured local fallback unless `open` is explicit.

Set `UDB_RATE_LIMIT_<OP>_MAX_PER_WINDOW` for a specific RPC, for example
`UDB_RATE_LIMIT_SELECT_MAX_PER_WINDOW=250`. `<OP>` is the uppercase RPC name
(`BEGINTX`, `BATCHSELECT`, `VECTORSEARCH`); these explicit ceilings also take
precedence over a per-key budget raise. The equivalent configuration map is
`service.rate_limit_max_per_operation`. Other RPCs retain the tenant default.

A refusal carries `UDB_RATE_LIMITED`, its effective `bucket`, `limit`, `window`
and verified `principal`, plus `retry_after_ms`. Batch lookups such as
`{"id":{"$in":["a","b"]}}` charge one Select instead of one request per id.
API-key Create/Get/List/Rotate responses report the persisted per-key budgets.

`UDB_EXPECTED_VERSION` pins the broker build (an optional `v` prefix is allowed).
A mismatch refuses `serve` before proto parsing or backend startup. Every gRPC
response carries `x-udb-version`, including refusals, so clients can identify the
build that answered.

Go clients send their compiled `SDKVersion` as `x-udb-sdk-version` on unary and
streaming calls. A broker in the same major and minor release is compatible,
including different patches and prereleases. A different, missing, malformed,
or repeated `x-udb-version` warns by default; `OnVersionWarning` replaces the
standard logger. Set `StrictServerVersion: true` in `Config`, `EnterpriseConfig`,
or low-level `Options` to return a typed `VersionMismatchError` instead. This
checks response headers: a write may already have completed when its mismatch
is detected, so the SDK never retries that refusal. Existing RPC failures retain
their original typed error and detail trailer.

### Auth-code delivery and invitation setup

Auth issuance queues `authn.email_verification`, `authn.password_reset`, and
`authn.otp` through the NotificationService mounted on the same listener.
Shipped English templates render `{{code}}`, `{{expires_in_minutes}}`, and
`{{user_name}}`; an active tenant template takes precedence. Configure a
notification channel provider to deliver the queued messages. If notification
queuing is unavailable, the configured `UDB_OTP_DELIVERY_WEBHOOK_URL` is the
fallback. Issuance remains a durable operation when delivery is unavailable.

Notification read/send responses redact auth-code subjects and bodies. The
worker retains the delivery material while retries are pending and scrubs it
when delivery reaches a terminal result. Channel opt-outs do not suppress
requested authentication codes. A terminal notification whose code has been
scrubbed cannot be manually retried; request a fresh authentication code.

Create an invitation with `password_setup_required=true` and an empty password.
Login returns `UDB_PASSWORD_SETUP_REQUIRED` until ResetPassword consumes the
emailed invitation code and sets the first password. ForgotPassword and
AdminResetPassword share the configured OTP cooldown; public repeat requests
keep the non-enumerating response shape and do not issue another code.
Cooldown checks and OTP persistence serialize on the durable user row, so
concurrent requests across broker replicas issue one code per user/type window.
SendOTP, ResendOTP and phone verification use the same issuance boundary.

Public auth RPCs also enforce a per-client-IP budget using the transport peer.
Forwarded IP headers affect the budget when `UDB_TRUST_PROXY_IP_HEADERS=true`
is configured for a trusted gateway. RPCs declaring the same `abuse_policy_ref` share an additional
budget; `UDB_ABUSE_POLICY_<REF>` sets its per-minute ceiling (uppercase the
reference and replace punctuation with underscores).

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

### Transaction refusals and replay

`BeginTx` emits a `TX_STATE_ERROR` frame with the original gRPC `code` and
`error_detail`, then ends with the same refusal in its trailer. Classify the
stable detail reason; an earlier `TX_STATE_OPEN` frame does not prove commit.
Any failed mutation rolls the whole transaction back.

Relational `upsert`, `update` and `delete` mutations may set an
`idempotency_key`. The receipt commits in the same PostgreSQL transaction as
the writes. An identical retry returns its original mutation ID and affected
count before checking CAS, without repeating entity writes, revisions,
projection tasks, CDC events or write audit. A changed payload, predicate,
precondition, conflict target or delivery/count requirement returns
`UDB_IDEMPOTENCY_REUSE`. Keys are scoped to tenant, project, entity and BeginTx
operation; unary keys have a separate namespace. A rolled-back transaction
keeps no fresh receipts, so retrying it can execute the writes.

Set `require_affected` to a non-zero exact count when a relational mutation must
match rows. A mismatch returns `NOT_FOUND`/`UDB_NO_ROWS_AFFECTED` and rolls the
transaction back. An upsert's `conflict_fields` select its conflict target and
the row its CAS precondition checks, with the same semantics as unary Upsert.

### Two-phase commit and MySQL mirrors

With `UDB_2PC_ENABLED=true`, a two-phase `BeginTx` commits PostgreSQL through
an XA ledger. By default PostgreSQL is the only participant. Configuring a
MySQL instance does not enrol it.

A MySQL participant is a **mirror**. It does not receive mutations of its own.
Between `XA START` and `XA END` it replays the transaction's PostgreSQL
statements, translated to MySQL, so the same rows land in both stores
atomically. That is only correct for a MySQL database that actually holds a copy
of those tables. To opt one in, name it explicitly:

| Setting | Purpose |
|---|---|
| `UDB_XA_MYSQL_MIRROR_INSTANCES` | Comma-separated MySQL instance names (for example `primary,reporting`) that mirror the relational tables and join two-phase `BeginTx` as replay participants. Unset or empty means no MySQL participant. |

How it behaves:

- The variable is read once at startup. Changing it needs a restart.
- Names are trimmed, de-duplicated and sorted, so the order participants are
  recorded in the XA ledger is stable.
- A listed name that is not a configured MySQL instance fails the transaction
  before any `XA PREPARE`, and the error names the missing instance. The broker
  never silently drops a participant you declared.
- Unlisted MySQL instances never take part, however many are configured.
- In-doubt transactions are resolved by the XA recovery worker. It runs on one
  replica at a time, under a fenced singleton lease, and drives each ledger row
  to commit or abort on the participants recorded for it.

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

All broker gRPC listeners send HTTP/2 keepalive pings every 30 seconds and wait
20 seconds for an acknowledgment. LiveQuery subscriptions additionally send an
explicit `Heartbeat` frame while idle. A heartbeat carries no row or event ID;
clients ignore it without advancing their resume cursor. The Go helper also
accepts the empty Change heartbeat from older brokers.

Use `AuthzService.PutAuthzPolicy` or `udb policy apply` to manage authorization.
`DataBroker.PutPolicy` and its `UDB_ALLOW_LEGACY_PUT_POLICY` override were removed
in 0.6.0: the legacy ABAC table it wrote never granted access.

The native services are the control-plane building blocks (auth, storage, WebRTC,
and more). List what's running, check the health of specific ones, or scaffold a
client app wired to the services you name:

```bash
udb native list
udb native doctor auth storage webrtc
udb app init --lang typescript --framework express --services auth,storage
```

Each service is descriptor-driven, so you can enable exactly the ones a given
deployment needs. The selection lives in the `native_services:` section of the
target directory's `udb.yaml` (`udb native add|remove` edit only that section).
Since 0.6.0 a leftover `.udb/native-services.json` is refused with directions to
move it there; `udb upgrade --check` flags it as well.

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
- topic-policy generation, age, and availability;
- consumer lag;
- schema/catalog version attached to emitted events.

Treat an authorization or topic-policy change as a stream-lifetime event, not
only a connect-time check. Open CDC streams periodically revalidate their
credential lineage, tenant status, scopes, and policy decision; revocation or a
changed scope set closes the stream and requires reconnect. Disabling the final
topic policy or failing to load a complete replacement generation makes CDC
unavailable. Do not "recover" a missing/pruned cursor by starting at epoch: ask
the caller to choose an explicit retained position or a new subscription.

### Journal retention

The CDC journal (`udb_system.udb_cdc_event_journal`) keeps every published
event for replay. The CDC leader prunes it about once a minute, in batches of
5,000 rows (at most 20 batches per pass).

| Setting | Default | Meaning |
| --- | --- | --- |
| `UDB_CDC_JOURNAL_RETENTION` | `7d` | Published and acknowledged rows older than this are deleted. Accepts seconds or `s`/`m`/`h`/`d` (`36h`, `30d`). |
| `UDB_CDC_JOURNAL_RETENTION_OVERRIDES` | `udb.lock.*=1h` | Comma-separated `pattern=duration` pairs for shorter retention on busy topics. Setting it replaces the default list; set it to an empty string to turn overrides off. |
| `UDB_CDC_KEEP_UNACKED_CONSUMER_ORIGIN` | unset | Read once, on the first 0.6.2 startup. When `1`, named consumers that never acknowledged anything stay at the start of the journal instead of moving to its head. |

What is never deleted:

- dead-lettered rows (`delivery_state = 'dlq'`);
- the newest row of each topic, so a client can always resume from a topic's
  latest event;
- the durable journal head, so positions never restart.

An override deletes a row only when it is also at or below the position of
every named consumer that could read that topic and acknowledged anything within
`UDB_CDC_JOURNAL_RETENTION`. A consumer that has been silent for longer than the
global retention no longer holds rows back.

An anonymous stream that resumes with `since_event_id` gets `NotFound` once
that event is pruned. Durable consumers (`consumer_name`) keep their position
across retention, and a progress marker's token (below) is always accepted.

### Consumer start and progress markers

A new `consumer_name` starts at the journal head: it receives only events
committed after its first subscription. Set
`CDCSubscriptionRequest.start = CDC_CONSUMER_START_EARLIEST` on that first
subscription to replay the retained history instead (Go
`ConsumeOptions{FromEarliest: true}`, TypeScript
`consume(…, { fromEarliest: true })`). A name that already has a position
ignores `start`.

With `progress_markers` set, a stream also sends progress markers:
`CDCEnvelope.progress_only = true`, topic `udb.cdc.progress.v1`, and
`journal_position` set to the position the stream has read through. One marker
follows the end of replay; later ones follow polls that passed events the
stream did not deliver, at most every 5 seconds or every 1,000 positions. A
durable consumer acknowledges the marker's `event_id` (a token, not an event id)
with `AckCdcEvents` after every earlier event, so a consumer of a quiet topic
keeps its stored position close to the head. The Go, TypeScript, Python and PHP
SDK consumers request markers and keep them away from your handlers. Publishing
to `udb.cdc.progress.v1` is refused.

### Broker journal readers

The webhook dispatcher and the cache invalidator each keep their position in
`udb_system.udb_journal_reader_cursors` (`webhook.dispatch`,
`cache.invalidate`) and read at most 5,000 journal positions per tick from
there. With no active webhook endpoint the dispatcher does not read the journal
at all; it moves its position to the head.

The embedding work emitter (`embedding.work`), the search freshness worker
(`search.freshness`) and the search reindex worker (`search.reindex`) keep
their positions in the same table. Each reads 5,000 positions per query and,
while a window holds nothing to do, moves on to the next one in the same tick,
up to 20. With no ACTIVE embedding source naming a CDC topic, no ACTIVE search
index naming one, or no search index that is not deleted, the matching worker
does not read the journal and moves its position to the head. A failed event
holds its worker's position just before it until it succeeds. A source or index
registered later does not receive changes published before its worker's
position; use `Backfill` or `Reindex`.

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

`BackupService` resolves the active project catalog and exactly one canonical
PostgreSQL write instance at operation start. It exports tenant tables inside
one `REPEATABLE READ READ ONLY` transaction and records the project, catalog
checksum, instance, snapshot/WAL provenance, destination, manifest key, and
checksums in durable run metadata. A project that spans multiple canonical
PostgreSQL instances is refused until a coordinated snapshot protocol exists;
do not describe that topology as an atomic backup.

Restore and retention must follow the immutable run metadata, never current
process defaults. Restore preflights project, catalog, and instance compatibility
before writing. Retention stops at the first provider error, preserves the
manifest and run journal for retry, and deletes the manifest only after every
referenced table artifact has been removed and verified.

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
python scripts/bench_snapshot.py --label "release-0.6.3"
```

Once you record a snapshot, its history is kept under `bench-history/`, and the
raw Criterion output lands under `target/criterion/`.

## Password CPU budget

Password hashes and verification run on Tokio blocking workers behind one shared
admission semaphore. Its default concurrency is the detected available CPU count
minus one, with a minimum of one. Detection happens once. Set
`UDB_PASSWORD_KDF_MAX_CONCURRENCY` to a positive integer to lower that budget;
values above the detected budget do not increase it. Invalid values refuse
password operations with a typed internal error.

Each Argon2id hash holds about 19 MiB. When the process runs under a cgroup
memory limit (`memory.max`, or v1 `memory.limit_in_bytes`) and no cap is set,
the default is also lowered so concurrent hashes fit in a quarter of that limit
(at least one), and the broker logs a startup warning naming
`UDB_PASSWORD_KDF_MAX_CONCURRENCY`. It also warns when the limit is under
512 MiB, and when an explicit cap exceeds the quarter-of-memory budget.

Available parallelism estimates affinity and container CPU limits where the host
exposes them. It does not measure VM steal or changing host contention. On a
throttled host, lower the operator cap and compare concurrent login/read latency
with CPU steal and quota measurements. Request cancellation releases queued work;
already running hashes retain their admission slot until computation finishes.

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

## Reviewed catalog transitions

A `backward` catalog transition may contain operations classified `RequiresReview`.
Use `udb catalog transition` with the matching broker/CLI release to review an
immutable candidate **before staging**. Ordinary `catalog stage` and `activate`
retain their compatibility checks. Blocked or data-destructive changes cannot use
this workflow.

`udb up --project PROJECT --tenant TENANT` drives this whole sequence. When the
ACTIVE catalog differs from the protos it runs `status` and `plan` (with an
idempotency key derived from the ACTIVE catalog and the target, so re-running
replays the same broker run), prints the operations and the review command, and
stops with exit code 3 and `Next: udb up … --approve RUN_ID`. Rerun with that
flag after reviewing to approve, apply, stage and activate; at a terminal you
can confirm instead. `--yes` never approves a reviewed plan. The response files
are kept under `.udb/transitions/`, and an existing file means its step is done.
The manual steps below are what it runs.

Set `UDB_AUTH_TOKEN` (or `UDB_BEARER_TOKEN`) to an authorized operator bearer,
`UDB_TENANT_ID` to its tenant and `UDB_GRPC_TARGET` to its project broker. For an
HTTPS endpoint, set `UDB_TLS_CA_FILE` to the trusted CA file. The broker verifies
the actual actor, tenant, project and permissions. Every mutation needs its own
stable `--idempotency-key`; retries of that same request reuse the same key.

1. Discover the current durable ACTIVE catalog with
   `udb catalog transition status --project PROJECT`. Retain its `catalog_id` and
   `manifest_integrity_sha256`. The latter is the stored **outer integrity hash**;
   `checksum_sha256` is a different selector and cannot substitute for it.
2. Plan the exact generated candidate:
   `udb catalog transition plan --project PROJECT --manifest candidate.json --expected-active-catalog-id ACTIVE_ID --expected-active-manifest-integrity-sha256 OUTER_HASH --idempotency-key PLAN_KEY --out plan.json`.
   The broker stores the candidate, original ACTIVE binding, complete operation
   hash and reviewed fingerprint set. It does not require a staged target.
3. Review the actual operations, safety classifications and fingerprints in that
   response. Then authorize that exact plan:
   `udb catalog transition approve --project PROJECT --plan plan.json --idempotency-key APPROVAL_KEY --out approval.json`.
   The broker persists approval from the verified actor. The opaque approval
   token is written to a new file with Unix mode `0600`; stdout omits it.
4. Apply or verify the real routed PostgreSQL target:
   `udb catalog transition apply --project PROJECT --approval approval.json --idempotency-key APPLY_KEY`.
   Continue only after the native run reaches `COMPLETED`. Actual broker DDL
   records `APPLIED`; an already-applied exact target records `VERIFIED` after
   native shape verification. A filesystem receipt cannot authorize either.
5. Stage the same immutable candidate with that run reference:
   `udb catalog transition stage --project PROJECT --manifest candidate.json --run-id RUN_ID --idempotency-key STAGE_KEY`.
   Retain the returned staged `catalog_id`.
6. Activate that exact staged catalog:
   `udb catalog transition activate --project PROJECT --catalog-id STAGED_ID --run-id RUN_ID --idempotency-key ACTIVATE_KEY`.
   Inspect the durable run with
   `udb catalog transition status --project PROJECT --run-id RUN_ID`.

The broker checks native approval and application evidence under the project
lock at staging and activation. Missing or foreign evidence, changed candidate
content, incomplete/failed application, altered target authority, or a changed
ACTIVE base are refused. After a base change, create and review a new plan;
reusing the old receipt cannot establish authority. Restart/retry consumes the
durable run and its verified native target receipts, preserving transactional
audit and idempotency.

The initial reviewed application path supports transactional PostgreSQL schema,
table and column changes, existing-table foreign-key additions, nullable
widening and plain-index changes, plus retention-class metadata changes.
Partial indexes require native PostgreSQL predicate verification. Concurrent or
expression indexes, custom operator classes/parameters, unsupported physical
objects and other table-security changes are refused until their application
and verification paths exist. Keep approval files private and retain the exact
candidate and plan for release review. Response files cache server evidence;
they are not canonical stores.
