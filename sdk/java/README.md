# UDB Java SDK

`dev.udb:udb-java-client` is the Java client for UDB. It wraps the DataBroker
gRPC client, attaches UDB metadata, and includes generated request/response
types for the broker and native control-plane services.

Current manifest version: `0.3.1-SNAPSHOT`

Release target: `0.3.1`

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
udb proto export
```

Then your app protos can import:

```proto
import "udb/core/common/v1/db.proto";
```

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

## Notes For Users

Use `dev.udb.client.UdbClient` and `UdbAuthClient` for normal app code. The
`com.udb.*` packages provide the protobuf request/response types.
