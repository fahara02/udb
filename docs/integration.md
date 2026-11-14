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
│    crate v0.6.17 | protocol v1.0.0                                          │
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
