# Example 2 setup — mint a UDB API key for the PHP client to authenticate with.
#
# Talks to the running broker over gRPC and prints the one-time plain key.
# Copy the "plain_key" value from the JSON and export it:
#
#   $env:UDB_API_KEY = "<plain_key>"
#   php 02_crud_with_auth.php
[CmdletBinding()]
param(
    [string] $Owner  = "php-quickstart",
    [string] $Name   = "php-quickstart",
    [string[]] $Scopes = @("udb:read", "udb:write"),
    [string] $Target = "127.0.0.1:50051",
    [string] $Tenant = "quickstart"
)
$ErrorActionPreference = "Stop"
. "$PSScriptRoot/_udb.ps1"

$env:UDB_GRPC_TARGET = $Target
$env:UDB_TENANT_ID   = $Tenant
$env:UDB_PROJECT_ID  = "default"
$env:UDB_USER_ID     = $Owner
# Reveal the plain key once so we can hand it to the PHP client.
$env:UDB_SHOW_SECRET = "1"

$scopeArgs = @()
foreach ($s in $Scopes) { $scopeArgs += @("--scope", $s) }

Invoke-Udb auth api-key create --owner $Owner --name $Name @scopeArgs
