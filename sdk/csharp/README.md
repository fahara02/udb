# UDB C# SDK

`Udb.Client` is the .NET client for UDB. Use it from .NET services that need to
read/write through the broker or call UDB auth/authz with the right request
metadata.

## Install

```powershell
dotnet add package Udb.Client --version 0.3.1
```

Runtime: .NET 8

The companion CLI tool is `Udb.Cli`:

```powershell
dotnet tool install --global Udb.Cli --version 0.3.1
```

The tool exposes `udb` and resolves the version-matched UDB binary.

## Export UDB Protos For Your App

From your application repo:

```powershell
udb proto export
```

Then your app protos can import:

```proto
import "udb/core/common/v1/db.proto";
```

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

## Local SDK Development

Consumers do not need this. Use it only when editing this repository:

```powershell
dotnet build sdk\csharp\Udb.Client\Udb.Client.csproj
dotnet build sdk\csharp\Udb.Cli\Udb.Cli.csproj
```
