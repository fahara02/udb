#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the Python SDK live conformance E2E (test_live_generated_rpc_surface)
    against a running UDB broker. Mirrors run-go-live.ps1.
#>
[CmdletBinding()]
param(
    [string]$Broker = "", [string]$Auth = "", [string]$Username = "", [string]$Password = "",
    [string]$Tenant = "", [string]$Project = "", [string]$Backends = "", [string]$Bucket = "",
    [string]$K = "test_live_generated_rpc_surface or test_rpc_surface", [string]$EnvFile = ""
)
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($EnvFile)) { $EnvFile = Join-Path $RepoRoot ".env.local" }

if (Test-Path $EnvFile) {
    $seen = New-Object System.Collections.Generic.HashSet[string]
    foreach ($raw in Get-Content -LiteralPath $EnvFile) {
        $line = $raw.Trim()
        if ($line.Length -eq 0 -or $line.StartsWith("#")) { continue }
        $eq = $line.IndexOf("="); if ($eq -lt 1) { continue }
        $key = $line.Substring(0, $eq).Trim()
        if ($key -notmatch '^(UDB_LIVE_|UDB_GRPC_TARGET$|UDB_AUTH_GRPC_TARGET$)') { continue }
        if (-not $seen.Add($key)) { continue }
        $val = $line.Substring($eq + 1).Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($key, $val, "Process")
    }
}
function Pick($p, $e, $d) {
    if (-not [string]::IsNullOrWhiteSpace($p)) { return $p }
    $v = [Environment]::GetEnvironmentVariable($e, "Process")
    if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
    return $d
}
$env:UDB_LIVE_SDK_TESTS         = "1"
$env:UDB_GRPC_TARGET            = Pick $Broker   "UDB_GRPC_TARGET"            "127.0.0.1:50051"
$env:UDB_AUTH_GRPC_TARGET       = Pick $Auth     "UDB_AUTH_GRPC_TARGET"       "127.0.0.1:50061"
$env:UDB_LIVE_USERNAME          = Pick $Username "UDB_LIVE_USERNAME"          "sdk-live-admin"
$env:UDB_LIVE_PASSWORD          = Pick $Password "UDB_LIVE_PASSWORD"          ""
$env:UDB_LIVE_TENANT            = Pick $Tenant   "UDB_LIVE_TENANT"            "sdk-live"
$env:UDB_LIVE_PROJECT           = Pick $Project  "UDB_LIVE_PROJECT"           "default"
$env:UDB_LIVE_REQUIRED_BACKENDS = Pick $Backends "UDB_LIVE_REQUIRED_BACKENDS" "postgres,mongodb,minio"
$env:UDB_LIVE_S3_BUCKET         = Pick $Bucket   "UDB_LIVE_S3_BUCKET"         "udb-live-sdk"
if ([string]::IsNullOrWhiteSpace($env:UDB_LIVE_PASSWORD)) { throw "Set -Password / UDB_LIVE_PASSWORD" }

Write-Host "broker=$($env:UDB_GRPC_TARGET) auth=$($env:UDB_AUTH_GRPC_TARGET) user=$($env:UDB_LIVE_USERNAME)" -ForegroundColor Cyan
Set-Location (Join-Path $RepoRoot "sdk\python")
& uv run pytest tests/test_live_conformance.py -k $K -v
exit $LASTEXITCODE
