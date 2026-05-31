# scripts/playground.ps1 — Start the UDB local playground (Windows / PowerShell)
# Phase 7.3 of the UDB Critical Feature Implementation Plan.
#
# Usage:
#   .\scripts\playground.ps1            # Start all services
#   .\scripts\playground.ps1 down       # Stop and remove containers
#   .\scripts\playground.ps1 logs       # Tail UDB logs
#   .\scripts\playground.ps1 status     # Show service status
#   .\scripts\playground.ps1 reset      # Nuke volumes and restart

param(
    [Parameter(Position = 0)]
    [ValidateSet("up", "start", "down", "stop", "logs", "status", "ps", "reset", "smoke", "test")]
    [string]$Command = "up",

    [Parameter(Position = 1)]
    [string]$Service = "udb"
)

$ErrorActionPreference = "Stop"

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot   = Split-Path -Parent $ScriptDir
$ComposeFile = Join-Path $RepoRoot "docker-compose.playground.yml"
$DC          = "docker", "compose", "-f", $ComposeFile

function Invoke-DC {
    param([string[]]$Args)
    & docker compose -f $ComposeFile @Args
    if ($LASTEXITCODE -ne 0) { throw "docker compose exited with $LASTEXITCODE" }
}

switch ($Command) {
    { $_ -in "up", "start" } {
        Write-Host "Starting UDB playground..." -ForegroundColor Cyan
        Invoke-DC @("up", "-d", "--build")
        Write-Host ""
        Invoke-DC @("ps")
        Write-Host ""
        Write-Host "PostgreSQL : localhost:5432  (udb/udb/udb_dev)"   -ForegroundColor Green
        Write-Host "Redis      : localhost:6379"                        -ForegroundColor Green
        Write-Host "Qdrant     : http://localhost:6333"                 -ForegroundColor Green
        Write-Host "MinIO API  : http://localhost:9000  (minioadmin)"   -ForegroundColor Green
        Write-Host "MinIO UI   : http://localhost:9001"                 -ForegroundColor Green
        Write-Host "Kafka      : localhost:9094"                        -ForegroundColor Green
        Write-Host "UDB gRPC   : localhost:50051"                       -ForegroundColor Green
    }

    { $_ -in "down", "stop" } {
        Write-Host "Stopping UDB playground..." -ForegroundColor Yellow
        Invoke-DC @("down")
    }

    "logs" {
        Write-Host "Tailing logs for service: $Service" -ForegroundColor Cyan
        Invoke-DC @("logs", "-f", $Service)
    }

    { $_ -in "status", "ps" } {
        Invoke-DC @("ps")
    }

    "reset" {
        Write-Host "Resetting UDB playground (all volumes will be deleted)..." -ForegroundColor Red
        $confirm = Read-Host "Type 'yes' to confirm"
        if ($confirm -ne "yes") { Write-Host "Aborted."; exit 0 }
        Invoke-DC @("down", "-v")
        Invoke-DC @("up", "-d", "--build")
    }

    { $_ -in "smoke", "test" } {
        & "$ScriptDir\smoke_test.ps1"
    }
}
