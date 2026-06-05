# Dot-sourced helper: resolves the UDB CLI binary the other scripts call.
#
# Resolution order:
#   1. $env:UDB_CLI            — an explicit path to udb-proto-parser(.exe)
#   2. udb-proto-parser on PATH
#   3. target/release/udb-proto-parser.exe in this repo (cargo build --release)
#
# Usage from another script:
#   . "$PSScriptRoot/_udb.ps1"
#   Invoke-Udb proto export --out proto

function Resolve-UdbCli {
    if ($env:UDB_CLI -and (Test-Path $env:UDB_CLI)) {
        return (Resolve-Path $env:UDB_CLI).Path
    }
    $onPath = Get-Command udb-proto-parser -ErrorAction SilentlyContinue
    if ($onPath) {
        return $onPath.Source
    }
    $exe = if ($env:OS -eq "Windows_NT") { "udb-proto-parser.exe" } else { "udb-proto-parser" }
    # examples/php_quickstart/scripts -> repo root is three levels up.
    $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
    $built = Join-Path $repoRoot "target/release/$exe"
    if (Test-Path $built) {
        return $built
    }
    throw "Could not find the UDB CLI. Build it with `cargo build --release --bin udb-proto-parser`, or set `$env:UDB_CLI` to the binary path."
}

function Invoke-Udb {
    $cli = Resolve-UdbCli
    & $cli @args
}
