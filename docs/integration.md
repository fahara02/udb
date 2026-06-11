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
│    crate v0.3.3 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This guide shows the normal application integration flow.

## 1. Install An SDK

Use [../sdk/README.md](../sdk/README.md) for language-specific install commands.

## 2. Export UDB Protos

```bash
udb proto export --fmt
```

This creates or refreshes `proto/udb/**` in the application repo and can merge
the required `buf.yaml` entries.

## 3. Write App Protos

```proto
import "udb/core/common/v1/db.proto";
```

Add storage and security annotations where UDB should manage routing or
metadata.

## 4. Configure Backends

Define backend instances in `configs/backends.yaml` or the deployment
environment. Use `udb compat-matrix` to verify the current binary and runtime
configuration.

## 5. Start The Broker

```bash
udb serve proto "" 0.0.0.0:50051
```

## 6. Send Metadata

Every request should carry:

- tenant id;
- project id;
- purpose;
- correlation id;
- scopes;
- service identity;
- user id when an end user exists;
- client catalog/protocol version.

SDKs attach these fields for you.

## 7. Call UDB

Use SDK helpers for common operations and generated request/response types for
advanced RPCs.
