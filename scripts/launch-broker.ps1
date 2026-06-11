#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build and launch the local UDB DataBroker with the repo's .env.local loaded.

.DESCRIPTION
    The broker (`udb serve`) does NOT auto-load .env.local, and rdkafka-sys needs
    the Visual-Studio-bundled cmake (the PATH cmake lacks the VS generator). This
    script handles both: it exports every KEY=VALUE from the env file into the
    process, points CMAKE at the VS toolchain, builds the `udb` binary, optionally
    stops a broker already holding the port, then launches `udb serve`.

    Run it from a dedicated terminal — it stays in the foreground serving until you
    Ctrl+C.

.PARAMETER EnvFile
    Env file to load (default: .env.local at the repo root).

.PARAMETER Addr
    Listen address. Default: UDB_GRPC_ADDR from the env file, else 0.0.0.0:50051.

.PARAMETER Release
    Build/run the optimized release binary instead of dev (slower compile, faster
    runtime). Default: dev.

.PARAMETER StopExisting
    Kill whatever process is already listening on the broker port before launching
    (avoids "address in use").

.PARAMETER DisableHeaderScopes
    Force UDB_ALLOW_HEADER_SCOPES off for this run, so the coarse admin gate is
    satisfied only by the JWT's projected scope (the real Block 1 path) and NOT by
    the x-scopes dev crutch. Use this to actually validate the bootstrap-admin fix.

.PARAMETER NoBuild
    Skip the build and run the existing target binary (fast restart).

.EXAMPLE
    ./scripts/launch-broker.ps1 -StopExisting -DisableHeaderScopes
#>
[CmdletBinding()]
param(
    [string]$EnvFile = "",
    [string]$Addr = "",
    [switch]$Release,
    [switch]$StopExisting,
    [switch]$DisableHeaderScopes,
    [switch]$NoBuild,
    [string]$Features = "mongodb-native"
)

$ErrorActionPreference = "Stop"

# Repo root = parent of this script's directory.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($EnvFile)) { $EnvFile = Join-Path $RepoRoot ".env.local" }

# --- 1. rdkafka-sys cmake: prefer the VS-bundled cmake (PATH cmake 3.x lacks the
#        VS 18 generator). Auto-discover; honor a pre-set $env:CMAKE.
if ([string]::IsNullOrWhiteSpace($env:CMAKE)) {
    $vsCmake = Get-ChildItem `
        "C:\Program Files\Microsoft Visual Studio\*\*\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe" `
        -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($vsCmake) {
        $env:CMAKE = $vsCmake.FullName
        Write-Host "CMAKE -> $($env:CMAKE)" -ForegroundColor DarkGray
    } else {
        Write-Warning "VS-bundled cmake not found; rdkafka-sys may fail. Set `$env:CMAKE manually."
    }
}

# --- 2. Load the env file: split on the FIRST '=', skip comments/blank, strip
#        surrounding quotes. Values may contain '=' (DSNs, base64), so never split
#        on all '='.
if (Test-Path $EnvFile) {
    # FIRST-wins, matching the broker's own dotenvy::from_path() load. The file
    # intentionally lists local-docker DSNs first and cloud DSNs later for the
    # SAME keys; last-wins would pick the cloud values and then (as an OS env var)
    # override the broker's correct local choice. So keep only the first def.
    $loaded = 0
    $seen = New-Object System.Collections.Generic.HashSet[string]
    foreach ($raw in Get-Content -LiteralPath $EnvFile) {
        $line = $raw.Trim()
        if ($line.Length -eq 0 -or $line.StartsWith("#")) { continue }
        $eq = $line.IndexOf("=")
        if ($eq -lt 1) { continue }
        $key = $line.Substring(0, $eq).Trim()
        if (-not $seen.Add($key)) { continue }   # first definition wins
        $val = $line.Substring($eq + 1).Trim()
        if (($val.StartsWith('"') -and $val.EndsWith('"')) -or
            ($val.StartsWith("'") -and $val.EndsWith("'"))) {
            $val = $val.Substring(1, $val.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($key, $val, "Process")
        $loaded++
    }
    Write-Host "Loaded $loaded vars from $EnvFile (first-wins)" -ForegroundColor DarkGray
} else {
    Write-Warning "Env file not found: $EnvFile (continuing with current environment)"
}

if ($DisableHeaderScopes) {
    [Environment]::SetEnvironmentVariable("UDB_ALLOW_HEADER_SCOPES", $null, "Process")
    Write-Host "UDB_ALLOW_HEADER_SCOPES disabled for this run (JWT-only admin path)" -ForegroundColor Yellow
}

# --- 3. Resolve the listen address.
if ([string]::IsNullOrWhiteSpace($Addr)) {
    $Addr = if ($env:UDB_GRPC_ADDR) { $env:UDB_GRPC_ADDR } else { "0.0.0.0:50051" }
}
$port = ($Addr -split ":")[-1]

# --- 4. Optionally stop whatever holds the port.
if ($StopExisting) {
    $owners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($procId in $owners) {
        try {
            Write-Host "Stopping existing listener on :$port (PID $procId)" -ForegroundColor Yellow
            Stop-Process -Id $procId -Force -ErrorAction Stop
        } catch { Write-Warning "Could not stop PID ${procId}: $_" }
    }
    Start-Sleep -Milliseconds 500
}

$profileArgs = if ($Release) { @("--release") } else { @() }
# The live Mongo backend is a native wire DSN, which needs the `mongodb-native`
# feature (default build only has the Data-API `mongodb`). Appended on top of the
# default feature set.
if (-not [string]::IsNullOrWhiteSpace($Features)) { $profileArgs += @("--features", $Features) }

# --- 5. Build (unless -NoBuild), then launch `udb serve <addr>`.
if (-not $NoBuild) {
    Write-Host "Building udb broker ($(if ($Release){'release'}else{'dev'}), features='$Features') — first build can take 10-30 min..." -ForegroundColor Cyan
    & cargo build --bin udb @profileArgs
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed (exit $LASTEXITCODE)" }
}

$binDir = if ($Release) { "release" } else { "debug" }
$bin = Join-Path $RepoRoot "target\$binDir\udb.exe"
if (-not (Test-Path $bin)) { throw "broker binary not found at $bin (run without -NoBuild first)" }

# The CLI's positionals are (proto_root, namespace, serve_addr) — passing the addr
# bare makes it the proto_root. Feed the address via UDB_GRPC_ADDR and let
# proto_root default to ./proto (we run from the repo root).
$env:UDB_GRPC_ADDR = $Addr
Write-Host "Launching: $bin serve  (UDB_GRPC_ADDR=$Addr, proto_root=./proto)" -ForegroundColor Green
& $bin serve
exit $LASTEXITCODE
