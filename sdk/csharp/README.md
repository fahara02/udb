# UDB C# SDK

Package: `Udb.Client`

Current release: `0.2.1`

Target framework: `net8.0`

The C# SDK centralizes gRPC metadata and compiles the committed generated
protobuf classes under `sdk/csharp/gen`.

NuGet publishing is part of the release pipeline. Once published:

```powershell
dotnet add package Udb.Client --version 0.2.1
```

For local development from this checkout:

```powershell
dotnet build sdk\csharp\Udb.Client\Udb.Client.csproj
```

After proto changes, regenerate from the repo root:

```powershell
.\scripts\gen_sdk.ps1
```
