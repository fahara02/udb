# Integration


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
│    crate v0.6.22 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This guide walks you from a freshly installed SDK to your first tenant-scoped
calls against a running broker. Follow it if you're wiring an app into UDB for
the first time.

Get one thing right up front and you'll skip the most common first-week bug. You
refer to a tenant by its human **code** (for example, `acme`), but row-level
security — the database rule that keeps one tenant's rows invisible to another —
compares the canonical tenant **UUID** instead. The SDK's login/adopt step
resolves the code to that UUID for you. Skip it and pass a raw code, and your
integration reads back zero rows with no error to explain why.

## 1. Install An SDK

Pick your language and follow the install commands in
[../sdk/README.md](../sdk/README.md).

## 2. Export UDB Protos

Protos are the schema files that define UDB's API. Pull them into your app repo:

```bash
udb proto export --fmt
```

This creates or refreshes `proto/udb/**` in the application repo, and it can
merge the required `buf.yaml` entries for you.

## 3. Write App Protos

Describe your own entities in proto files, importing UDB's shared definitions:

```proto
import "udb/core/common/v1/db.proto";
```

Add storage and security annotations wherever you want UDB to manage routing or
metadata.

## 4. Configure Backends

Tell UDB which databases to talk to. Define backend instances in
`configs/backends.yaml` or in the deployment environment. To check that your
binary and runtime configuration line up, run `udb compat-matrix`.

## 5. Start The Broker

```bash
udb serve proto "" 0.0.0.0:50051
```

The data plane listens on `50051`. The native services (auth, API keys, storage,
authz, …) listen on a separate control-plane port, loopback `127.0.0.1:50061` (the
data port + 10) by default; set `UDB_AUTH_GRPC_ADDR=<internal-ip>:50061` when
callers run on other hosts. The data-plane port answers `UNIMPLEMENTED` for native
RPCs.

## 6. Give The Project A Catalog, Policies And A Credential

Authorization is default-deny, and a project with no ACTIVE catalog is refused:

```bash
udb auth bootstrap user --username admin --email admin@example.com \
  --password "$ADMIN_PASSWORD" --tenant acme --project billing   # prints the tenant UUID
udb up --project billing --tenant "$TENANT_UUID"                 # catalog + udb.yaml policies
# or: udb catalog bootstrap --project billing
#     udb policy apply -f policies.yaml --tenant "$TENANT_UUID"
udb auth api-key create --owner <service-account-id> --name billing-api --scope <scope>
```

See [security.md](security.md#seeding-data-plane-authorization-the-standard-path)
for the policy file and [service-accounts.md](service-accounts.md) for service
accounts and API keys.

## 7. Authenticate And Connect

Point the SDK at both listeners and give it a credential: an API key for a
service (the SDK exchanges it through `Authenticate{api_key}` for a
self-refreshing bearer), or a username and password through `connectEnterprise`
for a person:

```ts
const udb = await UdbProject.connect({
  target: "127.0.0.1:50051",
  authTarget: "127.0.0.1:50061",
  tenantId: process.env.UDB_TENANT_ID!, // the key's own tenant UUID
  credentials: { apiKey: process.env.UDB_API_KEY },
});
```

With a verified credential the broker derives the identity — tenant, project,
user, service identity and scopes — from the credential itself. A tenant or
project header may only repeat it; one that disagrees is refused. `x-scopes` and
`x-service-identity` are ignored for API keys and bearers, and header-supplied
scopes (`UDB_ALLOW_HEADER_SCOPES`) are refused in production.

## 8. Send Metadata

Every request should carry a set of context fields:

- tenant id;
- project id;
- purpose;
- correlation id;
- scopes;
- service identity;
- user id when an end user exists;
- client catalog/protocol version;
- optionally, a read fence and consistency mode for read-your-writes reads —
  the guarantee that a read sees your own just-committed write (see
  [native-services.md](native-services.md#consistency-write-receipts-and-read-fences)).

You rarely set these by hand: the SDKs attach these fields for you.

## 9. Call UDB

For everyday operations, reach for the SDK helpers. For advanced RPCs, use the
generated request and response types.

## 10. Loading Large Reference Data

Use `BulkUpsert` when you load a lot of rows at once: a country or tariff list,
a product catalogue, a nightly import of a few million rows. A plain `Upsert`
loop works, but it pays for every row separately: one transaction, one change
event, one projection task and one rate-limit unit per row. `BulkUpsert` writes
a **batch** of rows as one unit instead.

What a batch gives you:

- **All or nothing.** The batch is one transaction. If any row is refused (a bad
  value, a missing NOT NULL column, a row outside your tenant), nothing from
  that batch is stored.
- **The same rules as a single write.** Every row is checked, tenant-scoped and
  encrypted exactly as `Upsert` would do it, and you need the same `Upsert`
  permission on the table. A shared (global) table still needs a platform admin
  or the global-write scope.
- **Few statements.** Rows are written with multi-row statements, as many rows
  per statement as PostgreSQL allows. A 1000-row batch of a narrow table runs
  about four SQL statements, not four thousand. The response's
  `statement_count` tells you the exact number.
- **One event and one audit record per batch.** Subscribers see one change event
  with operation `bulk_upsert` and a summary payload (`batch_id`, `row_count`,
  `affected_rows`, `keys_sha256`, `first_key`, `last_key`) instead of one event
  per row. If your consumer reacts to row events, treat a `bulk_upsert` event as
  "these rows of this table changed" and reread what you need. The audit log
  gets one record naming the table, the batch id and the row count.
- **A size-aware rate charge.** A batch costs `ceil(rows / 1000)` units of your
  tenant's `BulkUpsert` budget (at least one), not one unit per row.

Rules for a batch:

- Every row in a batch carries the **same fields**. Send `null` for an empty
  value. A row with a different set of fields is refused with
  `UDB_BULK_ROW_SHAPE_MISMATCH`.
- A key appears **once** per batch. Two rows with the same primary key (or the
  same `conflict_fields` value) are refused with `UDB_BULK_DUPLICATE_KEY`.
- A batch holds at most **10 000 rows** by default (`UDB_BULK_UPSERT_MAX_ROWS`),
  and the request must fit the gRPC message limit (`UDB_GRPC_MAX_MESSAGE_BYTES`,
  10 MiB by default). A bigger batch is refused with `UDB_BULK_BATCH_TOO_LARGE`.

You don't build batches by hand. The SDK's bulk-load helper takes all your rows
(a list, an iterator or a stream), cuts them into batches of 1000 rows and at
most 4 MiB, and sends the batches one after another:

```go
res, err := table.BulkLoad(ctx, rows, udbclient.BulkLoadOptions{
    BatchIDPrefix:        "tariffs-2026-11",
    IdempotencyKeyPrefix: "tariffs-2026-11",
})
```

```ts
const res = await table.bulkLoad(rows, {
  batchIdPrefix: "tariffs-2026-11",
  idempotencyKeyPrefix: "tariffs-2026-11",
});
```

```python
res = table.bulk_load(
    rows,
    batch_id_prefix="tariffs-2026-11",
    idempotency_key_prefix="tariffs-2026-11",
)
```

The helper stops at the first failed batch and tells you how many batches were
committed before it. Batches already committed stay committed. With an
idempotency key prefix you can simply run the same load again: committed
batches are replayed (nothing is written twice, no second event), and the load
carries on from the batch that failed.

For a 7-million-row load that is about 7000 batches. Each one is a short
transaction, so the load never holds one huge transaction open, and other
traffic keeps running next to it.

Operator settings:

| Setting | Default | What it does |
|---|---|---|
| `UDB_BULK_UPSERT_MAX_ROWS` | `10000` | Most rows in one batch (1 to 100000). |
| `UDB_BULK_UPSERT_ROWS_PER_RATE_UNIT` | `1000` | Rows one rate-limit unit covers. |
| `UDB_RATE_LIMIT_BULKUPSERT_MAX_PER_WINDOW` | unset | Optional separate ceiling for the `BulkUpsert` bucket. |

## 11. Shared Cache Entries

Cache keys are normally private to your tenant: two tenants can both store
`fx:usd` and never see each other's value. Sometimes you want the opposite, one
value that every tenant reads, such as today's exchange rates, a feature
catalogue or a list of public holidays. Set `shared: true` on the request to
use the **shared** cache namespace instead of your tenant's.

`shared` works on both cache surfaces:

- the native `CacheService` (`Get`, `Set`, `Increment`, `Delete`, `Scan`,
  `CreateNamespace`, `DeleteNamespace`, `GetNamespaceStats`);
- the data-plane cache RPCs on `DataBroker` (`CacheGet`, `CacheSet`,
  `CacheDelete`, `CacheScan`), for Redis and Memcached cache backends.

Who may do what:

- **Reading** a shared entry (get, scan, namespace stats): any authenticated
  caller of any tenant.
- **Writing or deleting** a shared entry: only a platform admin
  (`udb:platform_admin`) or a holder of `udb:global:write`, the same rule as
  writing a shared (global) table. Anyone else gets `PERMISSION_DENIED` with
  reason `shared_cache_write_denied`. A typical setup is one loader service
  account with `udb:global:write` that refreshes the shared values, and every
  tenant reading them.

```json
{ "tenant_id": "<your tenant>", "namespace": "fx", "key": "usd",
  "value": "MS4wODQy", "shared": true }
```

Things worth knowing:

- Shared entries live under the reserved tenant segment `*shared*`
  (`udb:cache:*shared*:<namespace>:...` for `CacheService`,
  `udb:<project>:*shared*:...` for the data-plane RPCs). No real tenant can
  have that id, so shared keys never collide with a tenant's keys. A request
  whose own tenant is `*shared*` is refused.
- The same key can exist in your tenant namespace and in the shared namespace
  at once; they are two different entries. Pick one with `shared`.
- A shared `CacheService` namespace has its own byte budget, set with
  `CreateNamespace` and `shared: true`.
- Change events for shared writes carry `*shared*` as their tenant (and the
  writer's tenant as `actor_tenant_id`), the same way changes to shared table
  rows do. The data-plane cache RPCs emit no per-key events.
- Memcached cannot list keys, so a `CacheScan` against Memcached returns no
  entries, for the shared namespace as for your own.
