[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
docker compose --project-directory $ProjectRoot up -d postgres redis qdrant minio minio-init udb-broker
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Start-Sleep -Seconds 3
$brokerId = docker compose --project-directory $ProjectRoot ps -q udb-broker
if (-not $brokerId) {
    docker compose --project-directory $ProjectRoot logs --no-color --tail=80 udb-broker
    exit 1
}

$brokerState = docker inspect -f '{{.State.Status}}' $brokerId
if ($brokerState -ne "running") {
    docker compose --project-directory $ProjectRoot logs --no-color --tail=80 udb-broker
    exit 1
}
