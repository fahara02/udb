# UDB Java SDK

Artifact: `dev.udb:udb-java-client`

Current manifest version: `0.2.1-SNAPSHOT`

Release target: `0.2.1`

Runtime: Java 17+

This wrapper centralizes UDB gRPC metadata for Java services and compiles the
committed generated protobuf classes under `sdk/java/gen`.

Maven Central publishing is still release-pipeline work. Until the public
artifact is published, build and test from this checkout:

```bash
mvn -f sdk/java/pom.xml test
```

After proto changes, regenerate from the repo root:

```powershell
.\scripts\gen_sdk.ps1
```

```bash
./scripts/gen_sdk.sh
```

The wrapper expects generated classes for the DataBroker and native
control-plane services, including:

- `com.udb.entity.v1.Types`
- `com.udb.services.v1.DataBrokerGrpc`
- `com.udb.core.authn.services.v1.*`
- `com.udb.core.authz.services.v1.*`

Use the root README for a concise client example.
