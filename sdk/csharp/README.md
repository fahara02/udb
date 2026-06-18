# UDB C# SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.3.6 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

`Udb.Client` is the .NET client for UDB. Use it from .NET services that need to
read/write through the broker or call UDB auth/authz with the right request
metadata.

## Install

```powershell
dotnet add package Udb.Client --version 0.3.6
```

Runtime: .NET 8

The companion CLI tool is `Udb.Cli`:

```powershell
dotnet tool install --global Udb.Cli --version 0.3.6
```

The tool exposes `udb` and resolves the version-matched UDB binary.

## Export UDB Protos For Your App

From your application repo:

```powershell
udb proto export --fmt
```

Then your app protos can import:

```proto
import "udb/core/common/v1/db.proto";
```

Run `udb proto fmt` after export or edits to keep long UDB field annotations on
one line for easier review.

## Connect And Query

```csharp
using Udb.Client;
using Udb.Entity.V1;
using AuthzV1 = udb.core.Authz.Services.V1;

var meta = new UdbMetadata(
    TenantId: "acme",
    Purpose: "web.request",
    CorrelationId: "request-001",
    Scopes: new[] { "udb:read", "udb:write" },
    ServiceIdentity: "billing.api",
    UserId: "user-1",
    ProjectId: "billing");

await using var udb = new UdbClient("http://localhost:50051", meta);

RecordSet rows = await udb.SelectAsync(new SelectRequest
{
    MessageType = "acme.billing.v1.Invoice",
    Limit = 50
});

await using var auth = new UdbAuthClient("http://localhost:50051", meta);
var (allowed, decision) = await auth.CanAsync(
    new AuthzV1.ResourceRef { MessageType = "acme.billing.v1.Invoice" },
    "read");
```

## Performance

Each `UdbClient` / `UdbAuthClient` / `UdbProject` holds a **single long-lived
`GrpcChannel`** — construct the client once and reuse it across every RPC. Never
create a client per call: a fresh channel forces a TCP+TLS+HTTP/2 handshake every
time, which dominates per-RPC latency.

Channels are created via `UdbChannel.ForAddress(...)`, which applies:

- **Keepalive** through a `SocketsHttpHandler` (`KeepAlivePingDelay = 30s`,
  `KeepAlivePingTimeout = 10s`) so an idle connection stays warm instead of dropping
  to IDLE and re-handshaking. `EnableMultipleHttp2Connections` is on, so the channel
  pools connections past the server's max-concurrent-streams without raising that
  server limit.
- No channel-wide retry policy. Generated wrappers retry only read-only unary RPCs
  using proto-derived `operation_kind`; mutating RPCs are never replayed by default.

Use `UdbChannel.DefaultOptions()` / `UdbChannel.ForAddress(...)` if you build a
`GrpcChannel` yourself and want the same behaviour.

## Local SDK Development

Consumers do not need this. Use it only when editing this repository:

```powershell
dotnet build sdk\csharp\Udb.Client\Udb.Client.csproj
dotnet build sdk\csharp\Udb.Cli\Udb.Cli.csproj
```
