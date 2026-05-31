# UDB Java SDK

This wrapper centralizes UDB gRPC metadata for Java services. Generate the Java
protobuf classes first:

```powershell
..\scripts\gen_sdk.ps1
```

```bash
../scripts/gen_sdk.sh
```

Generated classes are written to `sdk/java/gen/java`. Add that directory as a
generated source root in your build, or copy the generated package into your
application build.

The wrapper expects generated classes for:

- `com.udb.entity.v1.Types`
- `com.udb.services.v1.DataBrokerGrpc`

Run the example after generating stubs and starting UDB:

```bash
mvn -f sdk/java/pom.xml test
```
