# Run the UDB broker on your host (foreground — keep this terminal open).
#
# It serves the protos in proto/ and connects to the Postgres + Redis you
# started with `docker compose up -d`. On boot it force-syncs the schema, so the
# "shop"."customers" table from customer.proto is created automatically.
[CmdletBinding()]
param(
    [string] $Address = "127.0.0.1:50051"
)
$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
. "$PSScriptRoot/_udb.ps1"

$pgPort    = if ($env:UDB_POSTGRES_PORT) { $env:UDB_POSTGRES_PORT } else { "55432" }
$redisPort = if ($env:UDB_REDIS_PORT)    { $env:UDB_REDIS_PORT }    else { "56379" }

# sslmode=disable: the local postgres:16-alpine image speaks plaintext only;
# UDB negotiates TLS by default, so opt out for the dev container.
$env:UDB_PG_DSN                 = "postgres://udb:udb@localhost:$pgPort/udb?sslmode=disable"
$env:UDB_CACHE_DSN              = "redis://localhost:$redisPort"
$env:UDB_REDIS_DSN              = "redis://localhost:$redisPort"
$env:UDB_ENCRYPTION_KEY         = "00000000000000000000000000000000"
$env:UDB_ALLOW_HEADER_SCOPES    = "true"
$env:UDB_TLS_REQUIRED           = "false"
$env:UDB_MTLS_REQUIRED          = "false"
# Relational + cache are live; vectors/objects/etc. are absent, so allow the
# broker to start in a degraded (relational-only) mode for the quickstart.
$env:UDB_ALLOW_DEGRADED_BACKENDS = "true"
$env:UDB_RATE_LIMIT_ENABLED      = "false"
# Default-allow keeps example 1 zero-config. Set this to "false" before running
# example 2 if you want to see the authz engine actually deny + allow.
if (-not $env:UDB_ABAC_DEFAULT_ALLOW) { $env:UDB_ABAC_DEFAULT_ALLOW = "true" }
if (-not $env:RUST_LOG) { $env:RUST_LOG = "info" }

Push-Location $ProjectRoot
try {
    Invoke-Udb serve proto "" $Address
}
finally {
    Pop-Location
}
