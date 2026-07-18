# UDB Proto Module

`buf.build/fahara02/udb` — the wire contract for talking to a UDB broker.

This directory holds the `.proto` files that define how a client and the broker
exchange requests and responses over the wire. It is a standalone buf module: a
versioned, publishable bundle of protos that other projects depend on the same
way they depend on a package from npm or crates.io. You reach for it when you're
writing your own `.proto` files and want your requests to carry UDB's broker
types.

One rule keeps this module clean: it contains *only* broker-contract protos.
**No user or domain types live here.** Your own domain types (an `Invoice`, a
`Customer`) belong in your own buf module, which imports from this one.

## Files

The public broker contract lives under stable, universal import paths on purpose,
so the path you import never shifts under you:

| Path | Purpose |
|---|---|
| `udb/entity/v1/context.proto` | Broker request context and routing metadata. |
| `udb/entity/v1/operation.proto` | Backend-neutral resource identity plus operation stats and warnings |
| `udb/entity/v1/relational.proto` | Relational read/write request and response shapes |
| `udb/entity/v1/vector.proto` | Vector and hybrid search/upsert shapes |
| `udb/entity/v1/blob.proto` | Object/blob upload, download, and presigned URL shapes |
| `udb/entity/v1/stores.proto` | Universal cache/KV, document, graph, time-series, and analytical store request shapes |
| `udb/entity/v1/tx.proto` | Cross-backend mutation stream and transaction status |
| `udb/entity/v1/cdc.proto` | CDC subscription and control-plane shapes |
| `udb/entity/v1/outbox.proto` | First-class outbox event enqueue shapes |
| `udb/entity/v1/admin.proto` | Catalog, migration, DLQ, saga, policy, project, health, and admin shapes |
| `udb/entity/v1/types.proto` | Convenience public-import rollup for entity messages |
| `udb/events/v1/udb_events.proto` | Kafka topic payloads: `CDCEnvelope`, `DriftDetectedEvent`, `ProvisioningCompletedEvent` |
| `udb/services/v1/data_broker.proto` | The gRPC `DataBroker` service definition |

## Consuming this module

To use these types, declare a dependency on the module in your own buf module's
`buf.yaml`:

```yaml
version: v2
modules:
  - path: proto
    name: buf.build/your-org/your-app
deps:
  - buf.build/fahara02/udb
```

Then in any of your `.proto` files:

```proto
syntax = "proto3";
package your.app.v1;
import "udb/entity/v1/types.proto";

message YourRequest {
  udb.entity.v1.RequestContext context = 1;
  // ...
}
```

Now `buf generate` produces code stubs for your own messages *plus* the UDB
messages they reference — no copy-paste required.

## Why a separate module?

Because copying these files into every consumer guarantees drift. If each team
forks the broker contract, one team upgrades to v2, another stays on v1, and the
broker eventually receives mixed-version traffic it can't reconcile. Depending on
a published buf module instead pins the version per-consumer, exactly the way
crates.io or Packagist pin code dependencies.

## Versioning

The `v1` in the proto package names (`udb.entity.v1`, `udb.events.v1`,
`udb.services.v1`) is the wire-protocol version. Breaking changes go into a new
`v2/` directory next to the existing `v1/`, so old clients keep working. The
module itself is published to the Buf Schema Registry (BSR) on every tag of the
parent UDB repo.

Current wire-protocol version: **1.0.0** (see `sdk/UDB_PROTOCOL_VERSION`).
