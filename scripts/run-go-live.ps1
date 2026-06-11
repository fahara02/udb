#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the Go SDK live conformance E2E (TestLiveGeneratedRPCSurface) against a
    running UDB broker.

.DESCRIPTION
    The test is gated on UDB_LIVE_SDK_TESTS=1 and drives Login -> AuthenticateBearer
    -> RefreshToken -> GetCapabilities -> CRUD/backend E2E -> full RPC-surface probe.
    It is a pure client: it only needs the target endpoints + admin credentials, not
    the broker's own config. Any UDB_LIVE_* / UDB_GRPC_TARGET / UDB_AUTH_GRPC_TARGET
    you store (uncommented) in .env.local is loaded first; -params override.

.PARAMETER Broker     Data-plane gRPC target (default: env, else 127.0.0.1:50051).
.PARAMETER Auth       Auth control-plane gRPC target (default: env, else 127.0.0.1:50052).
.PARAMETER Username   Admin username   (default: env UDB_LIVE_USERNAME, else sdk-live-admin).
.PARAMETER Password   Admin password   (default: env UDB_LIVE_PASSWORD — required).
.PARAMETER Tenant     (default: sdk-live)      .PARAMETER Project (default: default)
.PARAMETER Backends   Required backends CSV    (default: postgres,mongodb,minio).
.PARAMETER Bucket     Live S3 bucket           (default: udb-live-sdk).
.PARAMETER Run        -run regex (default: TestLiveGeneratedRPCSurface).
.PARAMETER EnvFile    Env file to source UDB_LIVE_* from (default: .env.local).

.EXAMPLE
    ./scripts/run-go-live.ps1 -Password 'hunter2'
#>
[CmdletBinding()]
param(
    [string]$Broker = "",
    [string]$Auth = "",
    [string]$Username = "",
    [string]$Password = "",
    [string]$Tenant = "",
    [string]$Project = "",
    [string]$Backends = "",
    [string]$Bucket = "",
    [string]$Run = "TestLiveGeneratedRPCSurface",
    [string]$EnvFile = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($EnvFile)) { $EnvFile = Join-Path $RepoRoot ".env.local" }

# Source only the UDB_LIVE_* / target vars from the env file (first '=' split).
if (Test-Path $EnvFile) {
    foreach ($raw in Get-Content -LiteralPath $EnvFile) {
        $line = $raw.Trim()
        if ($line.Length -eq 0 -or $line.StartsWith("#")) { continue }
        $eq = $line.IndexOf("="); if ($eq -lt 1) { continue }
        $key = $line.Substring(0, $eq).Trim()
        if ($key -notmatch '^(UDB_LIVE_|UDB_GRPC_TARGET$|UDB_AUTH_GRPC_TARGET$)') { continue }
        $val = $line.Substring($eq + 1).Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($key, $val, "Process")
    }
}

# Param overrides win; then sensible defaults.
function Pick($paramVal, $envName, $default) {
    if (-not [string]::IsNullOrWhiteSpace($paramVal)) { return $paramVal }
    $e = [Environment]::GetEnvironmentVariable($envName, "Process")
    if (-not [string]::IsNullOrWhiteSpace($e)) { return $e }
    return $default
}

$env:UDB_LIVE_SDK_TESTS       = "1"
$env:UDB_GRPC_TARGET          = Pick $Broker   "UDB_GRPC_TARGET"          "127.0.0.1:50051"
$env:UDB_AUTH_GRPC_TARGET     = Pick $Auth     "UDB_AUTH_GRPC_TARGET"     "127.0.0.1:50061"
$env:UDB_LIVE_USERNAME        = Pick $Username "UDB_LIVE_USERNAME"        "sdk-live-admin"
$env:UDB_LIVE_PASSWORD        = Pick $Password "UDB_LIVE_PASSWORD"        ""
$env:UDB_LIVE_TENANT          = Pick $Tenant   "UDB_LIVE_TENANT"          "sdk-live"
$env:UDB_LIVE_PROJECT         = Pick $Project  "UDB_LIVE_PROJECT"         "default"
$env:UDB_LIVE_REQUIRED_BACKENDS = Pick $Backends "UDB_LIVE_REQUIRED_BACKENDS" "postgres,mongodb,minio"
$env:UDB_LIVE_S3_BUCKET       = Pick $Bucket   "UDB_LIVE_S3_BUCKET"       "udb-live-sdk"

if ([string]::IsNullOrWhiteSpace($env:UDB_LIVE_PASSWORD)) {
    throw "Admin password required: pass -Password, set `$env:UDB_LIVE_PASSWORD, or add UDB_LIVE_PASSWORD to $EnvFile"
}

Write-Host "broker=$($env:UDB_GRPC_TARGET) auth=$($env:UDB_AUTH_GRPC_TARGET) user=$($env:UDB_LIVE_USERNAME) tenant=$($env:UDB_LIVE_TENANT)" -ForegroundColor Cyan

Set-Location (Join-Path $RepoRoot "sdk\go")
& go test ./udbclient/ -run $Run -count=1 -v
exit $LASTEXITCODE
