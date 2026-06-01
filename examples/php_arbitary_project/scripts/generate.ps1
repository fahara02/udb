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
    $ComposerTemp = Join-Path $ProjectRoot ".composer-tmp"
    New-Item -ItemType Directory -Force -Path $ComposerTemp | Out-Null
    $env:TMP = $ComposerTemp
    $env:TEMP = $ComposerTemp
    $env:TMPDIR = $ComposerTemp
    composer install --ignore-platform-req=ext-grpc
    & "$PSScriptRoot/udb.ps1" -Runner $Runner sync-migrations proto --backend all --force-bootstrap
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
