# Native services tour — every UDB native service, on the real login path

UDB has two halves. The **data plane** (`DataBroker`) reads and writes your own
proto-defined tables. The **native control plane** is a set of 28 built-in gRPC
services for the plumbing most apps end up writing themselves: users and API keys,
roles and access checks, locks, feature flags, a job scheduler, workflows,
webhooks, secrets, notifications, usage metering, file storage, WebRTC rooms, and
more.

This folder is a tour of that control plane. One program logs in as an admin the
way a production app does, then makes at least one real call to **each of the 28
native services** — a create plus a read-back wherever the service has one — and
prints one numbered line per service. It is written three times, in Go,
TypeScript and Python. The three take the same steps, send the same requests and
print the same lines.

| Folder | Language | How it is checked |
| --- | --- | --- |
| [`go/`](go) | Go (the reference) | `go vet ./...`; run against a live broker |
| [`typescript/`](typescript) | TypeScript | `npx tsc --noEmit`; run against a live broker |
| [`python/`](python) | Python | `python -m py_compile main.py`; run against a live broker |
| [`php/`](php), [`csharp/`](csharp), [`java/`](java) | PHP, C#, Java | The tested tour is Go/TS/Python; these three are compile-checked older flows that use dev header mode. |

The PHP, C# and Java folders cover only users, roles, access checks and API keys.
They authorize by sending `x-scopes: udb:*` as a gRPC header, which the broker
accepts only when started with `UDB_ALLOW_HEADER_SCOPES=true`. Never run a
production broker that way.

## Two listeners: data on 50051, native services on 50061

A broker listens on two addresses:

- **Data plane** — `127.0.0.1:50051`, the public `DataBroker`. Your table reads
  and writes go here.
- **Control plane** — `UDB_AUTH_GRPC_ADDR`, by default the data port + 10:
  `127.0.0.1:50061`. **Every native service in this tour, including login, lives
  here.**

Call a native service on `:50051` and the broker answers `UNIMPLEMENTED`, naming
the control-plane address. The control plane binds to loopback by default; to
reach it from another host, set `UDB_AUTH_GRPC_ADDR=0.0.0.0:50061` on a trusted
interface. The SDKs take both addresses: `UDB_TARGET` (data) and
`UDB_AUTH_TARGET` (control plane).

## How a native call is authorized

The tour logs in with a username and password and gets back a signed token
(an RS256 JWT). The token carries the user's tenant and its scopes. The admin
created below is the organization owner, so its token carries `udb:*`.

Each native RPC requires one scope, named `udb:<service>:<method>` — for example
`udb:lock:acquire-lock`. `udb:*` covers them all. A real service account holds only
the scopes it needs (see the table below). This is a different gate from the data
plane, where access is decided by the policy rules in `udb_authz.policy_rules`: a
policy rule grants nothing on a native RPC, and a scope grants nothing on your
tables.

Tenants are UUIDs. You log in with the tenant's human code (`acme`); the token
carries the tenant's UUID, and every request after login uses the UUID. Native
services also check that the `tenant_id` in the request body matches the
token's tenant.

## Prerequisites

- Docker (for PostgreSQL) and `openssl`.
- The `udb` CLI (it is also the broker). Build it with
  `cargo build --release --bin udb`, or point `UDB_CLI` at a binary. The scripts
  look for `udb` on `PATH` and under `target/{release,debug}`.
- For the tour you run: Go 1.22+, or Node 18+, or Python 3.10+.

The tour reuses the broker setup from the TypeScript enterprise example
(`../ts_enterprise/scripts`). There is no second copy of it here.

## 1. Bootstrap (once)

Start PostgreSQL on host port 55432 and create the database the scripts use:

```bash
cd examples/php_quickstart && docker compose up -d && cd ../ts_enterprise
docker exec udb-php-quickstart-postgres-1 psql -U udb -d udb -c "CREATE DATABASE udb_enterprise;" || true
```

Then run the bootstrap script:

```bash
./scripts/bootstrap.sh
```

It works offline, straight against PostgreSQL. It:

- generates the RS256 key pair the broker signs tokens with;
- creates the admin user (`admin`, tenant code `acme`) as organization owner, so
  its token carries `udb:*`;
- applies three data-plane policy rules (not needed by this tour, used by the
  enterprise examples);
- writes everything to `examples/ts_enterprise/secrets/enterprise.env`, including
  `UDB_PASS`.

## 2. Serve

In the same folder, start the broker and leave this terminal open:

```bash
./scripts/serve.sh
```

Wait for `UDB DataBroker is ready`. The broker now serves data on
`127.0.0.1:50051` and the control plane on `127.0.0.1:50061`, with real token
login and no header-scope shortcut.

## 3. Run the tour

In a second terminal, load the values bootstrap wrote, then run the language you
want. Every tour reads the same environment.

```bash
cd examples/native-services
set -a; source ../ts_enterprise/secrets/enterprise.env; set +a
```

**Go**

```bash
cd go && go run .
```

**TypeScript**

```bash
cd typescript && npm install && npm start
```

**Python**

```bash
cd python && pip install -r requirements.txt && python main.py
```

| Env var | What it is | Default |
| --- | --- | --- |
| `UDB_PASS` | the admin password (in `enterprise.env`) | **required** |
| `UDB_TARGET` | data-plane address | `127.0.0.1:50051` |
| `UDB_AUTH_TARGET` | control-plane address | `127.0.0.1:50061` |
| `UDB_USER` | user to log in as | `admin` |
| `UDB_TENANT` | the tenant's human code | `acme` |
| `UDB_PROJECT` | project | `default` |

### What it prints

One line per service, numbered in the order the tour runs them, then a summary.
This is a real run against the broker `serve.sh` starts (Postgres only, so the
four services that need Redis, Kafka or an object store skip), in all three
languages:

```text
logged in as admin (tenant code "acme" -> 00000000-0000-0000-0000-0000000d0001)
01 AuthnService: GetUser -> "admin" (USER_STATUS_ACTIVE)
02 TenantService: UpdateTenantConfig + GetTenantConfig -> native_tour_max_users=200
03 AuthzService: CreateRole + GetRole + DeleteRole ok; CheckAccess(admin, Select billing.v1.Invoice) -> allowed=true
04 ApiKeyService: CreateApiKey(service account) + ValidateApiKey(data:read) -> valid=true; RevokeApiKey ok
05 IdentityProviderService: ListProviders -> 0 provider(s)
06 ConfigService: PutFlag + GetFlag -> bool_value=true; DeleteFlag ok
07 LockService: AcquireLock -> fencing token issued; GetLock -> found=true; ReleaseLock -> released=true
08 LiveQueryService: Subscribe(udb.core.lock.entity.v1.Lock, lock_name=...) -> snapshot with 1 row(s)
09 CacheService: skipped — UDB_CAPABILITY_UNSUPPORTED (native_service_enabled): native service 'cache' is not running: it needs Redis (UDB_REDIS_DSN)
10 SchedulerService: CreateJob(*/5 * * * *) + GetJob -> JOB_STATUS_ACTIVE; DeleteJob ok
11 WorkflowService: StartWorkflow + GetWorkflow -> WORKFLOW_STATUS_RUNNING; CancelWorkflow ok
12 WebhookService: CreateEndpoint + GetEndpoint -> https://example.com/udb-native-tour; DeleteEndpoint ok
13 VaultService: PutSecret + GetSecret -> version 1 round-trips; DeleteSecret ok
14 NotificationService: skipped — UDB_CAPABILITY_UNSUPPORTED (native_service_enabled): native service 'notification' is not running: it needs Kafka (UDB_KAFKA_BROKERS)
15 MeteringService: RecordUsage(3) + QueryUsage(1h) -> used=3
16 AnalyticsService: RecordPipelineMetric + GetThroughput -> total_requests >= 1
17 BackupService: PutBackupPolicy + GetBackupPolicy -> "0 3 * * *" retention 7 days; DeleteBackupPolicy ok
18 ControlPlaneService: ListNodeStates -> 0 node state(s)
19 StorageService: skipped — UDB_CAPABILITY_UNSUPPORTED (native_service_enabled): native service 'storage' is not running: it needs an S3/MinIO object store (UDB_MINIO_ENDPOINT)
20 AssetService: skipped — UDB_CAPABILITY_UNSUPPORTED (native_service_enabled): native service 'asset' is not running: it needs an S3/MinIO object store (UDB_MINIO_ENDPOINT)
21 SearchService: ListIndexes -> 0 index(es)
22 EmbeddingService: ListModels -> 0 model(s)
23 MessagingService: ListMessagingDevices(admin) -> 0 device(s)
24 RoomService: CreateRoom + GetRoom -> ROOM_STATE_ACTIVE
25 PeerService: JoinRoom + ListPeers -> 1 peer(s) in the room
26 TrackService: PublishTrack(audio) + ListTracks -> 1 track(s); UnpublishTrack ok
27 TurnService: IssueCredentials -> 1 ICE server(s), ttl 3600s
28 SignalingService: Signal(ping) -> pong; LeaveRoom + CloseRoom ok

OK 24/28 services called, 4 skipped (backend not configured)
```

How many services skip (`M`) depends on which backends your broker has; when
nothing fails, `N + M = 28`.

**Skipped services.** Some services need a backend that `serve.sh` does not set up
(Redis for the cache, an S3 or MinIO store for files, a TURN secret, and so on).
When the broker refuses a call because such a backend is missing, it says so with a
typed error, and the tour prints that error and moves on:

```text
NN Service: skipped — <REASON> (<what is missing>): <the broker's own message>
```

For example, with no Redis configured the cache service is not running, and the
tour prints:

```text
09 CacheService: skipped — UDB_CAPABILITY_UNSUPPORTED (native_service_enabled): native service 'cache' is not running: it needs Redis (UDB_REDIS_DSN)
```

Set what the message names, restart the broker, and run the tour again.

A service is only ever skipped for that reason: a `FAILED_PRECONDITION` refusal
that the broker marks as a missing capability, or a `*_NOT_CONFIGURED` /
`*_UNAVAILABLE` reason. Only the services marked "may skip" in the table below are
allowed to skip. Any other error is printed as `NN Service: FAILED — <code>
<reason>: <message>`, the tour carries on with the next service, and at the end it
prints every failure and exits with status 1.

The tour cleans up what it creates: roles, API keys, flags, locks, jobs, webhook
endpoints, secrets, cache namespaces, backup policies, files and rooms are
deleted, revoked, released or closed; the service account is deactivated and the
workflow cancelled. Records that are history by nature stay: usage and pipeline
metrics, the notification log, and the tenant config key `native_tour_max_users`.

## The 28 services

The RPCs in the second column are the ones the tour calls. Each needs the scope in
the third column, or `udb:*`. Every service except SignalingService stores its
state in PostgreSQL (`UDB_PG_DSN`); the "Also needs" column lists anything else a service needs, with the
environment variable that turns it on. The names of every RPC and its scope are in
[`docs/generated/udb-native-contract.json`](../../docs/generated/udb-native-contract.json)
and [`docs/generated/native-services.md`](../../docs/generated/native-services.md).

| # | Service | What the tour calls | Scopes | Also needs | May skip |
| --- | --- | --- | --- | --- | --- |
| 01 | AuthnService | `GetUser` (the admin) | `udb:authn:get-user` | — | no |
| 02 | TenantService | `UpdateTenantConfig`, `GetTenantConfig` | `udb:tenant:update-tenant-config`, `udb:tenant:get-tenant-config` | — | no |
| 03 | AuthzService | `CreateRole`, `GetRole`, `CheckAccess`, `DeleteRole` | `udb:authz:create-role`, `…:get-role`, `…:check-access`, `…:delete-role` | — | no |
| 04 | ApiKeyService | `AuthnService.CreateUser` (a service account), `ChangeUserStatus`, `CreateServiceAccountGrant`; then `CreateApiKey`, `ValidateApiKey`, `RevokeApiKey`; then `RevokeServiceAccountGrant` and deactivate | `udb:apikey:create-api-key`, `…:validate-api-key`, `…:revoke-api-key`, plus `udb:authn:create-user`, `udb:authn:change-user-status`, `udb:authn:manage-grants` | — | no |
| 05 | IdentityProviderService | `ListProviders` | `udb:idp:list-providers` | — | no |
| 06 | ConfigService | `PutFlag`, `GetFlag`, `DeleteFlag` | `udb:config:put-flag`, `…:get-flag`, `…:delete-flag` | — | no |
| 07 | LockService | `AcquireLock`, `GetLock`, `ReleaseLock` | `udb:lock:acquire-lock`, `…:get-lock`, `…:release-lock` | — | no |
| 08 | LiveQueryService | `Subscribe` to the lock table (server stream) and read the first snapshot; holds and releases a lock around it | `udb:livequery:subscribe` (+ the lock scopes) | — | no |
| 09 | CacheService | `CreateNamespace`, `Set`, `Get`, `DeleteNamespace` | `udb:cache:create-namespace`, `…:set`, `…:get`, `…:delete-namespace` | Redis: `UDB_REDIS_DSN` (or `REDIS_URL`) | yes |
| 10 | SchedulerService | `CreateJob` (cron), `GetJob`, `DeleteJob` | `udb:scheduler:create-job`, `…:get-job`, `…:delete-job` | — | no |
| 11 | WorkflowService | `StartWorkflow`, `GetWorkflow`, `CancelWorkflow` | `udb:workflow:start-workflow`, `…:get-workflow`, `…:cancel-workflow` | — | no |
| 12 | WebhookService | `CreateEndpoint` (`https://example.com/…`), `GetEndpoint`, `DeleteEndpoint` | `udb:webhook:create-endpoint`, `…:get-endpoint`, `…:delete-endpoint` | — | no |
| 13 | VaultService | `PutSecret`, `GetSecret`, `DeleteSecret` | `udb:vault:put-secret`, `…:get-secret`, `…:delete-secret` | a real master key: `UDB_ENCRYPTION_KEY` (the vault stays sealed without one) | yes |
| 14 | NotificationService | `UpsertTemplate`, `SendNotification` (email, to the admin), `GetNotification` | `udb:notification:upsert-template`, `…:send-notification`, `…:get-notification` | an active catalog for the project; Kafka `UDB_KAFKA_BROKERS` for event fan-out; `UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON` for real delivery | yes |
| 15 | MeteringService | `RecordUsage`, `QueryUsage` | `udb:metering:record-usage`, `…:query-usage` | — | yes |
| 16 | AnalyticsService | `RecordPipelineMetric`, `GetThroughput` | `udb:analytics:record-pipeline-metric`, `…:get-throughput` | — | no |
| 17 | BackupService | `PutBackupPolicy`, `GetBackupPolicy`, `DeleteBackupPolicy` | `udb:backup:put-backup-policy`, `…:get-backup-policy`, `…:delete-backup-policy` | — | no |
| 18 | ControlPlaneService | `ListNodeStates` | `udb:control:list-node-states` | — | no |
| 19 | StorageService | `RegisterUpload`, `GetFile`, `DeleteFile` | `udb:storage:register-upload`, `…:get-file`, `…:delete-file` | S3/MinIO: `UDB_MINIO_ENDPOINT`, `UDB_MINIO_ACCESS_KEY`, `UDB_MINIO_SECRET_KEY`; bucket `UDB_STORAGE_BUCKET` (default `udb-storage`) | yes |
| 20 | AssetService | `CreatePipelineDefinition`, `GetPipelineDefinition`, `ListAssets` | `udb:asset:create-pipeline-definition`, `…:get-pipeline-definition`, `…:list-assets` | the object store, as for StorageService | yes |
| 21 | SearchService | `ListIndexes` | `udb:search:list-indexes` | an active catalog; Qdrant `UDB_QDRANT_URL` to create and query vector indexes | yes |
| 22 | EmbeddingService | `ListModels` | `udb:embedding:list-models` | an active catalog; a vector store (`UDB_QDRANT_URL`) and Kafka to embed | yes |
| 23 | MessagingService | `ListMessagingDevices` (the admin's devices) | `udb:messaging:list-messaging-devices` | — | yes |
| 24 | RoomService | `CreateRoom`, `GetRoom` (closed at step 28) | `udb:webrtc:room:create-room`, `…:get-room`, `…:close-room` | — | yes |
| 25 | PeerService | `JoinRoom`, `ListPeers` (left at step 28) | `udb:webrtc:peer:join-room`, `…:list-peers`, `…:leave-room` | — | yes |
| 26 | TrackService | `PublishTrack` (audio), `ListTracks`, `UnpublishTrack` | `udb:webrtc:track:publish-track`, `…:list-tracks`, `…:unpublish-track` | — | yes |
| 27 | TurnService | `IssueCredentials` for the peer | `udb:webrtc:turn:issue-credentials` | `UDB_TURN_SECRET` (and `UDB_TURN_URLS`) | yes |
| 28 | SignalingService | joins a second peer, sends `ping` on the `Signal` stream, waits for `pong`; then `LeaveRoom` and `CloseRoom` | `udb:webrtc:signal` | — | yes |

The four WebRTC services after RoomService use its room. If RoomService (or
PeerService) is skipped, the services that need it print
`skipped — needs RoomService, which was skipped above`; if it failed, they are
reported as failures too.

## How each language reaches a native service

All three log in with the SDK's enterprise helper and then call the generated
client for each service over the control-plane connection. The SDK attaches the
token, the tenant UUID and the identity headers to every call and refreshes the
token in the background.

- **Go** — `udbclient.ConnectEnterprise(...)`, then
  `lockpb.NewLockServiceClient(session.NativeConn())` with
  `session.NativeContext(ctx)`. Requests are the generated Go structs.
- **TypeScript** — `UdbProject.connectEnterprise(...)`, then
  `udb.nativeCore.unary("udb.core.lock.services.v1.LockService", "AcquireLock", {...})`.
  Streams use `nativeCore.serverStream` and `nativeCore.bidiStream`. Requests are
  plain objects with the proto's snake_case field names.
- **Python** — `create_udb(...)` and `udb.login_and_adopt_tenant(...)`, then
  `udb.generated.LockService.acquire_lock(request)`. The tour builds each request
  from the same snake_case dict the TypeScript tour sends.

## Common mistakes

- **Calling a native service on the data port.** `UNIMPLEMENTED` from a native RPC
  means you dialed `:50051`. Use the control-plane address (`:50061`).
- **Using the tenant code instead of the UUID.** Log in with `acme`, then use the
  tenant UUID from the token in every request. A native service refuses a request
  whose `tenant_id` differs from the token's tenant.
- **`PERMISSION_DENIED` with reason `UDB_SCOPE_MISSING`.** The token lacks the
  RPC's scope (`udb:<service>:<method>`). Grant the scope; a data-plane policy rule
  does not help here.
- **Treating a skip as a bug.** `skipped — …` lines are the broker reporting a
  backend it does not have. Set the variable it names and run again.
- **An API key for a plain user.** API keys belong to service accounts, and a
  service account needs an active `CreateServiceAccountGrant` (its approved
  scopes) before it can hold a key. Step 04 shows the whole sequence.
