[CmdletBinding()]
param(
    [ValidateSet("auto", "docker", "release")]
    [string] $Runner = $(if ($env:UDB_RUNNER) { $env:UDB_RUNNER } else { "auto" })
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $ProjectRoot
try {
    uv run --no-project --with grpcio-tools python scripts/generate_models.py
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    & "$PSScriptRoot/udb.ps1" -Runner $Runner sync-migrations proto --backend all --force-bootstrap
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
