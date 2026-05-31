# UDB Proto Module

`buf.build/fahara02/udb` — the UDB-internal broker wire contract.

This directory is a standalone buf module. It contains only protos that define how clients talk to the UDB broker; **no user/domain types live here**. Project domain types (Patient, Doctor, Invoice, etc.) live in the user's own buf module and import from this one.

## Files

| Path | Purpose | LOC |
|---|---|---|
| `udb/entity/v1/types.proto` | Request / response messages: `RequestContext`, `SelectRequest`, `UpsertRequest`, `Mutation`, `TxStatus`, vector ops, blob ops, CDC envelopes, catalog/migration/DLQ/saga/policy/admin types | 870 |
| `udb/events/v1/udb_events.proto` | Kafka topic payloads: `CDCEnvelope`, `DriftDetectedEvent`, `ProvisioningCompletedEvent` | 37 |
| `udb/services/v1/data_broker.proto` | The gRPC `DataBroker` service definition (Select, BatchSelect, Upsert, Delete, Vector*, Blob, Tx, CDC, Event, Resource, Catalog, Migration, DLQ, Saga, Policy, Admin, Project RPCs) | 190 |

## Consuming this module

In your own buf module's `buf.yaml`:

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

`buf generate` produces stubs for your messages *plus* the UDB messages they reference, no copy-paste required.

## Why a separate module?

Forking the broker contract by copying these files into every consumer guarantees drift — one team upgrades to v2, another stays on v1, the broker eventually receives mixed-version traffic. A published buf module pins the version per-consumer the same way crates.io / Packagist pin code dependencies.

## Versioning

The package versions inside the protos (`udb.entity.v1`, `udb.events.v1`, `udb.services.v1`) are wire-protocol versions; breaking changes go to a new `v2/` directory next to the existing `v1/`. The module itself is published to BSR on every tag of the parent UDB repo.

Current wire-protocol version: **1.0.0** (see `sdk/UDB_PROTOCOL_VERSION`).
