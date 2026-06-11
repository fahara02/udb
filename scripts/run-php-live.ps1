#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the PHP SDK live conformance E2E (tests/Live) in Docker (php:8.3 + grpc/
    protobuf pecl extensions) against the host-run UDB broker. The container
    reaches the host via host.docker.internal, so the broker's auth plane must be
    bound on 0.0.0.0 (UDB_AUTH_GRPC_ADDR=0.0.0.0:50061).
#>
[CmdletBinding()]
param(
    [string]$Username = "", [string]$Password = "", [string]$Tenant = "", [string]$Project = "",
    [string]$Backends = "", [string]$Bucket = "", [string]$EnvFile = "", [switch]$Build
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
        if ($key -notmatch '^UDB_LIVE_') { continue }
        if (-not $seen.Add($key)) { continue }
        [Environment]::SetEnvironmentVariable($key, $line.Substring($eq + 1).Trim().Trim('"').Trim("'"), "Process")
    }
}
function Pick($p, $e, $d) {
    if (-not [string]::IsNullOrWhiteSpace($p)) { return $p }
    $v = [Environment]::GetEnvironmentVariable($e, "Process")
    if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
    return $d
}
$u  = Pick $Username "UDB_LIVE_USERNAME"          "sdk-live-admin"
$pw = Pick $Password "UDB_LIVE_PASSWORD"          ""
$t  = Pick $Tenant   "UDB_LIVE_TENANT"            "sdk-live"
$pr = Pick $Project  "UDB_LIVE_PROJECT"           "default"
$bk = Pick $Backends "UDB_LIVE_REQUIRED_BACKENDS" "postgres,mongodb,minio"
$bu = Pick $Bucket   "UDB_LIVE_S3_BUCKET"         "udb-live-sdk"
if ([string]::IsNullOrWhiteSpace($pw)) { throw "Set -Password / UDB_LIVE_PASSWORD" }

if ($Build) {
    & docker build -f (Join-Path $RepoRoot "sdk\php\Dockerfile.live-test") -t udb-php-live (Join-Path $RepoRoot "sdk\php")
    if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
}

Write-Host "PHP live (docker) -> host.docker.internal:50051/50061 user=$u tenant=$t" -ForegroundColor Cyan
& docker run --rm `
    --add-host=host.docker.internal:host-gateway `
    -e UDB_LIVE_SDK_TESTS=1 `
    -e UDB_GRPC_TARGET=host.docker.internal:50051 `
    -e UDB_AUTH_GRPC_TARGET=host.docker.internal:50061 `
    -e "UDB_LIVE_USERNAME=$u" `
    -e "UDB_LIVE_PASSWORD=$pw" `
    -e "UDB_LIVE_TENANT=$t" `
    -e "UDB_LIVE_PROJECT=$pr" `
    -e "UDB_LIVE_REQUIRED_BACKENDS=$bk" `
    -e "UDB_LIVE_S3_BUCKET=$bu" `
    udb-php-live
exit $LASTEXITCODE
