#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run UDB proto-to-postgres migration steps.

.DESCRIPTION
    Runs in the <repo> repo root so that the UDB binary can find
    .env.local / .env.prod (load_project_dotenv walks up from cwd).

    Two Neon PostgreSQL databases are supported:
      primary  (udb)        → UDB_PG_DSN  (default)
      backup   (udb_backup) → UDB_PG_DSN_BACKUP

.PARAMETER Env
    Target environment: local (default) or prod.
    Sets APP_ENV so load_project_dotenv picks .env.local or .env.prod.

.PARAMETER Database
    Target database: primary (default) or backup.
    When "backup", UDB_PG_DSN is overridden with UDB_PG_DSN_BACKUP.

.PARAMETER Command
    UDB sub-command. Defaults to "sync-migrations --backend postgres".

.PARAMETER DryRun
    Print what would be executed without running it.

.EXAMPLE
    .\scripts\migrate.ps1
    .\scripts\migrate.ps1 -Env local -Database backup
    .\scripts\migrate.ps1 -Env prod -Command "plan --backend postgres"
    .\scripts\migrate.ps1 -DryRun
#>
param(
    [ValidateSet("local","prod")]
    [string]$Env = "local",

    [ValidateSet("primary","backup")]
    [string]$Database = "primary",

    [string]$Command = "sync-migrations --backend postgres",

    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Paths ────────────────────────────────────────────────────────────────────
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$udbDir   = Split-Path -Parent $PSScriptRoot   # E:\...\udb

# ── Env file ─────────────────────────────────────────────────────────────────
$envFile = Join-Path $repoRoot ".env.$Env"
if (-not (Test-Path $envFile)) {
    Write-Error "Env file not found: $envFile"
    exit 1
}

# ── Load env file to resolve DSN vars for display/override ───────────────────
Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*([A-Z0-9_]+)=(.*)$') {
        $k = $Matches[1]; $v = $Matches[2].Trim("'`"")
        if (-not [System.Environment]::GetEnvironmentVariable($k)) {
            [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
        }
    }
}

# ── DSN override for backup ───────────────────────────────────────────────────
if ($Database -eq "backup") {
    $backupDsn = [System.Environment]::GetEnvironmentVariable("UDB_PG_DSN_BACKUP")
    if (-not $backupDsn) {
        Write-Error "UDB_PG_DSN_BACKUP is not set in $envFile"
        exit 1
    }
    $env:UDB_PG_DSN = $backupDsn
}

# ── cargo command ─────────────────────────────────────────────────────────────
$cargoArgs = "run --bin udb-proto-parser -- $Command"

Write-Host ""
Write-Host "  UDB migration helper" -ForegroundColor Cyan
Write-Host "  Repo root : $repoRoot"
Write-Host "  Env file  : $envFile"
Write-Host "  Database  : $Database"
Write-Host "  Command   : cargo $cargoArgs"
Write-Host ""

if ($DryRun) {
    Write-Host "[dry-run] skipping execution" -ForegroundColor Yellow
    exit 0
}

# Run from udb dir (load_project_dotenv walks up to repo root to find env file)
Push-Location $udbDir
try {
    $env:APP_ENV = $Env
    Invoke-Expression "cargo $cargoArgs"
}
finally {
    Pop-Location
    Remove-Item Env:APP_ENV -ErrorAction SilentlyContinue
    if ($Database -eq "backup") {
        Remove-Item Env:UDB_PG_DSN -ErrorAction SilentlyContinue
    }
}
