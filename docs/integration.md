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
│    crate v0.4.29 | protocol v1.0.0                                          │
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

## 6. Send Metadata

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

## 7. Call UDB

For everyday operations, reach for the SDK helpers. For advanced RPCs, use the
generated request and response types.
