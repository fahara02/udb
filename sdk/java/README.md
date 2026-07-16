# UDB Java SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.4.3 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

`dev.udb:udb-java-client` is the Java client for UDB. It wraps the DataBroker
gRPC client, attaches UDB metadata, and includes generated request/response
types for the broker and native control-plane services.

Current manifest version: `0.4.3-SNAPSHOT`

Release target: `0.4.3`

Runtime: Java 17+

Maven Central publishing is still release-pipeline work. Until the public
artifact is available, build from this checkout:

```bash
mvn -f sdk/java/pom.xml test
```

## CLI And Proto Export

The Java build also packages a version-matched `udb` CLI wrapper. Once installed
or built, use it from your application repo to export UDB's shared protos:

```bash
udb proto export --fmt
```

Then your app protos can import:

```proto
import "udb/core/common/v1/db.proto";
```

Run `udb proto fmt` after export or edits to keep long UDB field annotations on
one line for easier review.

## Connect And Query

```java
import com.udb.core.authz.services.v1.ResourceRef;
import com.udb.entity.v1.RecordSet;
import com.udb.entity.v1.SelectRequest;
import dev.udb.client.UdbAuthClient;
import dev.udb.client.UdbClient;
import dev.udb.client.UdbMetadata;
import java.util.List;

var meta = new UdbMetadata(
    "acme",
    "web.request",
    "request-001",
    List.of("udb:read", "udb:write"),
    "billing.api",
    "user-1",
    "billing",
    UdbClient.PROTOCOL_VERSION);

try (var udb = new UdbClient("localhost:50051", meta)) {
    RecordSet rows = udb.select(
        SelectRequest.newBuilder()
            .setMessageType("acme.billing.v1.Invoice")
            .setLimit(50)
            .build());
}

try (var auth = new UdbAuthClient("localhost:50051", meta)) {
    var decision = auth.can(
        ResourceRef.newBuilder()
            .setMessageType("acme.billing.v1.Invoice")
            .build(),
        "read",
        "");
}
```

## Consistency, Idempotency Replay, And Platform Services

A mutation returns a `WriteReceipt`; a follow-up read can carry a fence built
from it. A keyed replay-safe mutation the broker deduplicated reports
`was_duplicate` (full contract: [docs/native-services.md](../../docs/native-services.md)).

```java
import dev.udb.client.MutationOutcome;

var outcome = MutationOutcome.of(udb.upsert(upsertRequest));
if (outcome.wasDuplicate()) {
    // durable-idempotency replay — no new side effect occurred
}

// Read-your-writes: metadata carrying a fence derived from the receipt.
var fenced = meta.afterWrite(outcome.writeReceipt());
```

Retry contract: a replay-safe mutation is retried on transient errors **only
when the request carries a non-empty idempotency key** — keyless mutations fail
closed rather than risk a double apply.

Vault, Metering, Scheduler, Search, Webhook, Workflow, Lock, LiveQuery, Config,
Backup, and Embedding are reachable as flat typed methods on
`GeneratedUdbClient` (no workflow facade yet):

```java
import dev.udb.client.generated.GeneratedUdbClient;
import com.udb.core.vault.services.v1.EncryptRequest;

var gen = new GeneratedUdbClient("localhost:50051", meta);
var enc = gen.Encrypt(EncryptRequest.newBuilder()
    .setTenantId(tenant)
    .setKeyName("docs")
    .setPlaintext("plain text to encrypt")
    .build());
```

The per-RPC retry class and idempotency contract are listed in
[docs/generated/udb-native-contract.json](../../docs/generated/udb-native-contract.json).

## Notes For Users

Use `dev.udb.client.UdbClient` and `UdbAuthClient` for normal app code. The
`com.udb.*` packages provide the protobuf request/response types.

Generated RPC wrappers raise `GeneratedClientSupport.UdbRpcException`, which
keeps the gRPC status, raw `udb-error-detail-bin` bytes, decoded
`com.udb.entity.v1.ErrorDetail`, and `retryable`/`retryAfterMs`/`kind`
convenience accessors.

## Performance

Each `UdbClient` / `UdbAuthClient` / `UdbProject` / `GeneratedUdbClient` holds a
**single long-lived `ManagedChannel`** — construct the client once and reuse it
across every RPC. Never create a client per call: a fresh channel forces a
TCP+TLS+HTTP/2 handshake every time, which dominates per-RPC latency.

Channels are built via `dev.udb.client.UdbChannels`, which applies:

- **Keepalive** (`keepAliveTime(30s)`, `keepAliveTimeout(10s)`,
  `keepAliveWithoutCalls(true)`) so an idle connection stays warm instead of
  dropping to IDLE and re-handshaking.
- No channel-wide retry policy. Generated wrappers retry only read-only unary RPCs
  using proto-derived `operation_kind`; mutating RPCs are never replayed by default.

If you build a `ManagedChannel` yourself, pass it through
`UdbChannels.tune(builder)` (or use `UdbChannels.forTarget(target, tls)`) to get the
same behaviour, then hand the channel to the client constructor that accepts one.
