[CmdletBinding()]
param(
    [ValidateSet("auto", "docker", "release")]
    [string] $Runner = $(if ($env:UDB_RUNNER) { $env:UDB_RUNNER } else { "auto" })
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $ProjectRoot
try {
    buf generate
    composer install
    & "$PSScriptRoot/udb.ps1" -Runner $Runner sync-migrations proto --backend all --force-bootstrap
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
