# Step 2 — generate the PHP model + install the SDK.
#
# `buf generate --path proto/shop` emits PHP *only* for your customer.proto
# (the import udb/core/common/v1/db.proto is resolved from the exported tree but
# not re-generated — those classes ship inside the fahara02/udb-laravel SDK).
[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $ProjectRoot
try {
    buf generate --path proto/shop
    if ($LASTEXITCODE -ne 0) { throw "buf generate failed" }

    # ext-grpc is only needed at runtime, not to install the SDK package.
    composer install --ignore-platform-req=ext-grpc
    if ($LASTEXITCODE -ne 0) { throw "composer install failed" }
}
finally {
    Pop-Location
}
