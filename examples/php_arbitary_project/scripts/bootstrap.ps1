[CmdletBinding()]
param(
    [ValidateSet("auto", "docker", "release")]
    [string] $Runner = $(if ($env:UDB_RUNNER) { $env:UDB_RUNNER } else { "auto" })
)

$ErrorActionPreference = "Stop"
& "$PSScriptRoot/udb.ps1" -Runner $Runner admin force-sync proto
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
