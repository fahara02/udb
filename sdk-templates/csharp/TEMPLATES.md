# UDB C# SDK generation templates

These templates are consumed by the Rust-driven generator (`udb sdk generate`,
implemented in `src/cli/sdk_gen.rs`). Every file here is materialized at the
mirror path under `sdk/csharp/`. A `.tmpl` suffix means "render placeholders,
then strip `.tmpl`"; any other file is copied verbatim. Proto is the source of
truth — the generator reads the embedded `FileDescriptorSet`, so the per-RPC /
per-service blocks below expand automatically over every service and RPC.

## What renders where

| Template (`sdk-templates/csharp/…`)            | Renders to (`sdk/csharp/…`)                | Mode     |
| ---------------------------------------------- | ------------------------------------------ | -------- |
| `Udb.Client/GeneratedClient.cs.tmpl`           | `Udb.Client/GeneratedClient.cs`            | rendered |
| `Udb.Client/GeneratedClientRuntime.cs`         | `Udb.Client/GeneratedClientRuntime.cs`     | verbatim |
| `Udb.Cli/UdbCli.cs.tmpl`                        | `Udb.Cli/UdbCli.cs`                         | rendered |
| `Udb.Cli/Udb.Cli.csproj.tmpl`                   | `Udb.Cli/Udb.Cli.csproj`                    | rendered |
| `TEMPLATES.md`                                   | `sdk/csharp/TEMPLATES.md`                   | verbatim |

> This doc is named `TEMPLATES.md` (not `README.md`) on purpose: the generator
> mirrors every non-`.tmpl` file to the same relative path, and the committed
> SDK already ships a hand-written `sdk/csharp/README.md`. A distinct name keeps
> the mirror additive and never clobbers the existing README.

## How it composes with the existing SDK (no clobbering)

The hand-written layer is untouched and reused:

- `Udb.Client/UdbClient.cs` — `UdbClient`, `UdbMetadata`, `Headers()`.
- `Udb.Client/UdbAuthClient.cs` — auth/authz ergonomics.

The generated layer lands in **new** files under the `Udb.Client.Generated`
namespace and never redefines `UdbClient` / `UdbMetadata` / `UdbAuthClient`:

- `GeneratedClientRuntime.cs` (verbatim) — the stable robustness core:
  `UdbCallOptions`, `UdbRpcException` (decodes the `udb-error-detail-bin`
  trailer into raw bytes plus `DecodedErrorDetail`/`Retryable`/`RetryAfterMs`/
  `Kind` convenience properties), and `GeneratedServiceBase` (per-call deadline, retry with
  exponential backoff + jitter on `Unavailable` / `ResourceExhausted`, plus
  `DeadlineExceeded` only for read-only RPCs, metadata propagation, typed error
  mapping). Streaming calls are opened once and never retried mid-stream.
- `GeneratedClient.cs` (rendered) — one `Generated<Service>Client` per gRPC
  service, with one forwarding method per RPC.

### Why `dynamic` for the stub

The buf C# plugin PascalCases each proto-package segment when it picks the C#
namespace (`udb.services.v1` → `Udb.Services.V1`,
`udb.core.authn.services.v1` → `udb.core.Authn.Services.V1`). The generator's
`{{SERVICE_PKG}}` placeholder is the lowercase proto package, which cannot be
mechanically mapped to that PascalCased namespace by string substitution. So the
generated wrapper holds the stub as `dynamic` and forwards to the stub's
`<Rpc>Async` / `<Rpc>` method by name. The call object is boxed as `object` and
re-cast to `dynamic` inside the runtime, so the closed generic (e.g.
`AsyncUnaryCall<RecordSet>`) always stays concrete — no `dynamic`-to-generic
cast is ever attempted. Requests and responses remain the exact generated
protobuf message types at runtime.

### Compilation wiring (additive, no manifest edit needed)

`Udb.Client/Udb.Client.csproj` is an SDK-style project with default compile
items enabled, so `GeneratedClient.cs` and `GeneratedClientRuntime.cs` — which
render into the project directory — are picked up automatically. The library
csproj is intentionally **not** edited. `dynamic` and records compile on the
net8.0 default language version (C# 12); `Microsoft.CSharp` is implicit in the
net8.0 shared framework.

## Usage of the generated layer

```csharp
using Udb.Client;
using Udb.Client.Generated;
using Udb.Services.V1; // buf stub namespace for DataBroker

var meta = new UdbMetadata(
    TenantId: "t1", Purpose: "demo", CorrelationId: Guid.NewGuid().ToString(),
    Scopes: new[] { "read" }, ServiceIdentity: "svc");

await using var client = new UdbClient("https://localhost:50051", meta);

// Wrap the buf stub with the generated robustness layer, reusing UdbClient.Headers().
var broker = new GeneratedDataBrokerClient(client.Broker, client.Headers);

RecordSet rows = await broker.SelectAsync(new SelectRequest { /* … */ });
```

The wrapper accepts any constructed buf stub (`client.Broker`, or
`new AuthnService.AuthnServiceClient(channel)`, etc.) plus a metadata supplier.

## CLI bundling — installing the SDK gives you the `udb` CLI

`Udb.Cli` packs as a .NET tool whose console command is `udb`
(`<PackAsTool>true</PackAsTool>`, `<ToolCommandName>udb</ToolCommandName>`):

```powershell
dotnet tool install --global Udb.Cli --version {{UDB_VERSION}}
udb --help
```

On launch the bundled launcher (`UdbCli.cs`, version baked from
`{{UDB_VERSION}}`) resolves the matching binary in order:

1. an `udb` already on `PATH` whose `--version` matches;
2. a previously cached download under `%LOCALAPPDATA%\udb\<version>` (Windows)
   or `~/.cache/udb/<version>` (Unix, honoring `XDG_CACHE_HOME`);
3. otherwise downloads the per-OS/arch release asset for tag `v{{UDB_VERSION}}`
   from `https://github.com/fahara02/udb/releases` into the cache.

It forwards all arguments and propagates the child exit code. The asset naming
(`udb-<os>-<arch>[.exe]`, e.g. `udb-linux-x86_64`, `udb-windows-x86_64.exe`,
`udb-macos-aarch64`) is centralized in `UdbCli.AssetName()` — adjust there if the
release pipeline uses different names.

## Customizing

- Default deadlines / retry budget: edit `UdbCallOptions` defaults in
  `GeneratedClientRuntime.cs`, or pass a `UdbCallOptions` to the wrapper ctor.
- Retryable status set: `UdbCallOptions.RetryableCodes`.
- Per-RPC method shape: edit the `@@UDB_RPC_BEGIN kind=…` blocks in
  `GeneratedClient.cs.tmpl` (one block per kind: `unary`, `server_streaming`,
  `client_streaming`, `bidi`).
- Per-service class skeleton: the `@@UDB_SERVICE_BEGIN` block.

## Scalar placeholders used here

`{{UDB_VERSION}}`, `{{PROTOCOL_VERSION}}`, `{{LANG}}`, `{{RPC_COUNT}}`,
`{{SERVICE_COUNT}}`, `{{GENERATED_NOTE}}`. Per-RPC: `{{RPC_NAME}}`,
`{{RPC_PATH}}`, `{{RPC_KIND}}`, `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`,
`{{SERVICE_FULL}}`. Per-service: `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`,
`{{SERVICE_FULL}}`, `{{SERVICE_RPC_COUNT}}`.
