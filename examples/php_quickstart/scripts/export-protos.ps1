# Step 1 — bring the UDB protos into this project.
#
# `udb proto export` writes the full UDB annotation contract + broker wire
# surface into proto/udb/**, vendors google/api into third_party/, and merges a
# buf.yaml. Your customer.proto imports "udb/core/common/v1/db.proto" from here.
[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
. "$PSScriptRoot/_udb.ps1"
Push-Location $ProjectRoot
try {
    Invoke-Udb proto export --out proto
}
finally {
    Pop-Location
}
