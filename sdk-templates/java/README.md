# UDB Java SDK templates

These files are consumed by the Rust-driven generator (`udb sdk generate`,
implemented in `src/cli/sdk_gen.rs`). Every file here is materialized at the
mirror path under `sdk/java/`. A `.tmpl` suffix means "render placeholders, then
strip the suffix"; any other file is copied verbatim. The proto
`FileDescriptorSet` embedded in the `udb` binary is the source of truth, so the
RPC list is never hand-maintained.

## What renders where

| Template (`sdk-templates/java/…`) | Renders to (`sdk/java/…`) | Mode |
|---|---|---|
| `src/main/java/dev/udb/client/generated/GeneratedClientSupport.java` | same path | verbatim |
| `src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl` | `.../GeneratedUdbClient.java` | rendered |
| `src/main/java/dev/udb/cli/Launcher.java.tmpl` | `.../Launcher.java` | rendered |
| `README.md` | `sdk/java/README.generated.md`? | (this file is template-dir docs, not copied) |

> Note: this `README.md` documents the templates; the generator copies it to
> `sdk/java/README.md` only if you intend that. The hand-written
> `sdk/java/README.md` already exists and is **not** overwritten — these
> templates render to **new** files alongside it.

## Design — composes with, never clobbers

The hand-written layer in `sdk/java/src/main/java/dev/udb/client/`
(`UdbClient`, `UdbAuthClient`, `UdbMetadata`) is untouched. The generated client
lives in the **new** `dev.udb.client.generated` package:

- **`GeneratedClientSupport.java`** (verbatim) — the robustness core:
  - `CallTuning` (deadline, max attempts, backoff multiplier/ceiling),
  - `UdbRpcException` carrying the gRPC `Status` plus the decoded
    `udb-error-detail-bin` binary trailer bytes,
  - `unary(...)` with retry + exponential backoff + **full jitter** on
    `UNAVAILABLE` / `RESOURCE_EXHAUSTED`, plus `DEADLINE_EXCEEDED` only for
    read-only RPCs,
  - `serverStreaming(...)` (single attempt; drain-time errors mapped),
  - `clientStreaming(...)` / `bidiStreaming(...)` (never retried).

  It forwards through the buf-generated `<Service>Grpc.get<Rpc>Method()` static
  `MethodDescriptor` via `io.grpc.stub.ClientCalls`, attaching headers per
  attempt with `MetadataUtils.newAttachHeadersInterceptor`. This means it does
  **not** depend on the camel-cased instance method names of the generated
  stubs — only on the PascalCase descriptor getters, which match `{{RPC_NAME}}`
  exactly.

- **`GeneratedUdbClient.java`** (rendered) — one wrapper method per RPC across
  all services, emitted by four `@@UDB_RPC_BEGIN kind=…` blocks (unary /
  server_streaming / client_streaming / bidi). It **reuses**
  `UdbClient.headers(UdbMetadata)` for metadata and `UdbMetadata` for config, so
  there is no duplicate metadata logic. Wrapper methods are named after the proto
  RPC in **PascalCase** (e.g. `Select`, `VectorSearch`) so they never collide
  with the hand-written camelCase helpers on `UdbClient` (`select`, `upsert`).

### RPC coverage

`GeneratedUdbClient.java.tmpl` uses `kind=` filters so every RPC of every service
is emitted with the correct shape:

- `kind=unary` → `Resp Rpc(Req)` + `Resp Rpc(Req, Duration deadline)` (retried)
- `kind=server_streaming` → `Iterator<Resp> Rpc(Req[, Duration])`
- `kind=client_streaming` → `StreamObserver<Req> Rpc(StreamObserver<Resp>)`
- `kind=bidi` → `StreamObserver<Req> Rpc(StreamObserver<Resp>)`

Because the per-RPC block exposes `{{SERVICE_PKG}}` / `{{SERVICE_NAME}}` per
method, a single flat class dispatches to each service's `…Grpc` class —
no nested service blocks are needed. (Verified: no RPC method name is duplicated
across services, so the PascalCase wrapper names are collision-free.)

Java packages map from proto packages with a `com.` prefix
(`udb.services.v1` → `com.udb.services.v1`), matching the committed `gen/` tree
and `buf.gen.yaml`.

## CLI bundling

`Launcher.java` (rendered, with `{{UDB_VERSION}}` baked in) makes the
version-matched `udb` CLI available when you build/install the SDK:

1. honours `$UDB_BIN` if executable;
2. else uses a `udb` on `$PATH` **if** `udb --version` contains the pinned
   version;
3. else downloads the matching GitHub release asset from
   `github.com/fahara02/udb` (tag `v{{UDB_VERSION}}`, per-OS/arch asset name) into
   a per-user cache (`%LOCALAPPDATA%\udb\vX.Y.Z` on Windows, `$XDG_CACHE_HOME`
   or `~/.cache/udb/vX.Y.Z` elsewhere) and execs it, forwarding all args.

The generator additively edits `sdk/java/pom.xml` (no version bump) to add:

- `maven-assembly-plugin` → an executable `*-jar-with-dependencies.jar`
  (`Main-Class: dev.udb.cli.Launcher`), and
- `appassembler-maven-plugin` → a thin `udb` wrapper script under
  `target/appassembler/bin/`.

After `mvn -f sdk/java/pom.xml package`:

```bash
target/appassembler/bin/udb sdk generate --help        # via the wrapper script
java -jar target/udb-java-client-*-jar-with-dependencies.jar --version  # via the fat jar
```

## Customising

Do **not** edit the rendered files (they are overwritten on regen). Instead:

- subclass `GeneratedUdbClient` or wrap its return values in your own code under
  `sdk/java/src/main/java`;
- tune retries/deadlines by passing a `GeneratedClientSupport.CallTuning` to the
  constructor;
- to change the robustness core itself, edit
  `sdk-templates/java/.../GeneratedClientSupport.java` and regenerate.

## Placeholders used

Scalars: `{{UDB_VERSION}}`, `{{PROTOCOL_VERSION}}`, `{{LANG}}`, `{{RPC_COUNT}}`,
`{{SERVICE_COUNT}}`, `{{GENERATED_NOTE}}`.
Per-RPC: `{{RPC_NAME}}`, `{{RPC_INPUT}}`, `{{RPC_INPUT_PKG}}`, `{{RPC_OUTPUT}}`,
`{{RPC_OUTPUT_PKG}}`, `{{RPC_PATH}}`, `{{RPC_KIND}}`, `{{SERVICE_NAME}}`,
`{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`.
