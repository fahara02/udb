#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Bootstrap (or re-bind) the live-test admin via the offline `udb auth bootstrap`
    path, loading .env.local for the Postgres DSN + hash secrets.

.DESCRIPTION
    Offline root bootstrap: connects straight to Postgres (UDB_PG_DSN), creates the
    admin if absent, and binds it to the system `organization_owner` role so its
    Login JWT carries `udb:admin` (Block 1). Idempotent — re-running binds an
    already-created admin instead of failing on the username. Does NOT require the
    broker to be running, but DOES require the new binary to be built.

.PARAMETER Username  Default: env UDB_LIVE_USERNAME, else sdk-live-admin.
.PARAMETER Password  Default: env UDB_LIVE_PASSWORD (required; >= 8 chars).
.PARAMETER Email     Default: <username>@sdk.local.
.PARAMETER Tenant    Default: env UDB_LIVE_TENANT, else sdk-live.
.PARAMETER Project   Default: env UDB_LIVE_PROJECT, else default.
.PARAMETER EnvFile   Default: .env.local at the repo root.
.PARAMETER Cargo     Run via `cargo run` instead of the prebuilt target binary.

.EXAMPLE
    ./scripts/bootstrap-admin.ps1
#>
[CmdletBinding()]
param(
    [string]$Username = "",
    [string]$Password = "",
    [string]$Email = "",
    [string]$Tenant = "",
    [string]$Project = "",
    [string]$EnvFile = "",
    [switch]$Cargo
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
if ([string]::IsNullOrWhiteSpace($EnvFile)) { $EnvFile = Join-Path $RepoRoot ".env.local" }

# Load .env.local FIRST-wins (matching the broker's dotenvy load) so the local
# UDB_PG_DSN + hash secrets win over the cloud values listed later in the file.
if (Test-Path $EnvFile) {
    $seen = New-Object System.Collections.Generic.HashSet[string]
    foreach ($raw in Get-Content -LiteralPath $EnvFile) {
        $line = $raw.Trim()
        if ($line.Length -eq 0 -or $line.StartsWith("#")) { continue }
        $eq = $line.IndexOf("="); if ($eq -lt 1) { continue }
        $key = $line.Substring(0, $eq).Trim()
        if (-not $seen.Add($key)) { continue }   # first definition wins
        $val = $line.Substring($eq + 1).Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($key, $val, "Process")
    }
} else {
    Write-Warning "Env file not found: $EnvFile"
}

function Pick($p, $e, $d) {
    if (-not [string]::IsNullOrWhiteSpace($p)) { return $p }
    $v = [Environment]::GetEnvironmentVariable($e, "Process")
    if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
    return $d
}

$Username = Pick $Username "UDB_LIVE_USERNAME" "sdk-live-admin"
$Password = Pick $Password "UDB_LIVE_PASSWORD" ""
$Tenant   = Pick $Tenant   "UDB_LIVE_TENANT"   "sdk-live"
$Project  = Pick $Project  "UDB_LIVE_PROJECT"  "default"
if ([string]::IsNullOrWhiteSpace($Email)) { $Email = "$Username@sdk.local" }

if ([string]::IsNullOrWhiteSpace($Password)) {
    throw "Password required: pass -Password or set UDB_LIVE_PASSWORD in $EnvFile"
}
if ([string]::IsNullOrWhiteSpace($env:UDB_PG_DSN)) {
    throw "UDB_PG_DSN not set — ensure it is present (uncommented) in $EnvFile"
}

$bootArgs = @(
    "auth", "bootstrap", "user",
    "--username", $Username,
    "--email", $Email,
    "--password", $Password,
    "--tenant", $Tenant,
    "--project", $Project
)

Write-Host "Bootstrapping admin '$Username' (tenant=$Tenant project=$Project)" -ForegroundColor Cyan
if ($Cargo) {
    $env:CMAKE = $env:CMAKE  # honor a preset CMAKE if building
    & cargo run --bin udb -- @bootArgs
} else {
    $bin = Join-Path $RepoRoot "target\debug\udb.exe"
    if (-not (Test-Path $bin)) { throw "binary not found at $bin — build first (launch-broker.ps1) or use -Cargo" }
    & $bin @bootArgs
}
exit $LASTEXITCODE
