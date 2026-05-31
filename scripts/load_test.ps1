<#
.SYNOPSIS
UDB load/soak profiles using ghz (https://ghz.sh).

.EXAMPLE
$env:PROFILE = "read-heavy"; .\scripts\load_test.ps1 -UdbHost localhost:50000
#>

param (
    [string]$UdbHost = $(if ($env:UDB_HOST) { $env:UDB_HOST } else { "localhost:50000" }),
    [int]$Concurrency = $(if ($env:CONCURRENCY) { [int]$env:CONCURRENCY } else { 50 }),
    [int]$TotalRequests = $(if ($env:TOTAL_REQUESTS) { [int]$env:TOTAL_REQUESTS } else { 10000 }),
    [string]$Profile = $(if ($env:PROFILE) { $env:PROFILE } else { "read-heavy" }),
    [string]$ProtoRoot = $(if ($env:PROTO_ROOT) { $env:PROTO_ROOT } else { "../proto" }),
    [string]$Service = $(if ($env:SERVICE) { $env:SERVICE } else { "udb.services.v1.DataBroker" })
)

$ErrorActionPreference = "Stop"
$ProtoFile = Join-Path $ProtoRoot "udb/services/v1/data_broker.proto"

if (!(Get-Command ghz -ErrorAction SilentlyContinue)) {
    Write-Error "ghz is not installed or not in PATH. Install it from https://ghz.sh"
    exit 1
}

function Invoke-UdbLoadCase {
    param (
        [string]$Name,
        [string]$Call,
        [string]$Data,
        [string]$Scopes,
        [string]$Tenant = "test-tenant"
    )

    Write-Host ""
    Write-Host "[$Profile] $Name" -ForegroundColor Yellow
    $Metadata = "{`"x-tenant-id`":`"$Tenant`",`"x-purpose`":`"benchmark`",`"x-scopes`":`"$Scopes`",`"x-service-identity`":`"load.test`"}"
    ghz --insecure `
        --proto $ProtoFile `
        --import-path $ProtoRoot `
        --call "$Service.$Call" `
        -d $Data `
        -m $Metadata `
        -c $Concurrency `
        -n $TotalRequests `
        $UdbHost
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " UDB Load Test" -ForegroundColor Cyan
Write-Host " Host: $UdbHost"
Write-Host " Profile: $Profile"
Write-Host " Concurrency: $Concurrency"
Write-Host " Total Requests: $TotalRequests"
Write-Host " Proto: $ProtoFile"
Write-Host "==========================================" -ForegroundColor Cyan

switch ($Profile) {
    "read-heavy" {
        Invoke-UdbLoadCase "Select" "Select" '{"messageType":"DocumentExtraction","filter":{"fields":{"status":{"stringValue":"processed"}}},"limit":25}' "udb:read"
        Invoke-UdbLoadCase "Capabilities" "GetCapabilities" '{"projectId":"default"}' "udb:admin"
    }
    "write-heavy" {
        Invoke-UdbLoadCase "Upsert" "Upsert" '{"messageType":"ProcessingQueue","payload":{"fields":{"id":{"stringValue":"load-{{.RequestNumber}}"},"status":{"stringValue":"pending"}}},"idempotencyKey":"load-{{.RequestNumber}}"}' "udb:write"
        Invoke-UdbLoadCase "EnqueueOutboxEvent" "EnqueueOutboxEvent" '{"topic":"document.uploaded.v1","partitionKey":"doc-{{.RequestNumber}}","payload":{"fields":{"event_id":{"stringValue":"11111111-1111-4111-8111-111111111111"},"event_type":{"stringValue":"document.uploaded.v1"},"correlation_id":{"stringValue":"load"},"document_id":{"stringValue":"doc-{{.RequestNumber}}"}}},"idempotencyKey":"outbox-{{.RequestNumber}}"}' "udb:write"
    }
    "mixed-projection" {
        Invoke-UdbLoadCase "Upsert plus projection fanout" "Upsert" '{"messageType":"ProcessingQueue","payload":{"fields":{"id":{"stringValue":"projection-{{.RequestNumber}}"},"status":{"stringValue":"project"},"tenant_id":{"stringValue":"test-tenant"}}},"idempotencyKey":"projection-{{.RequestNumber}}"}' "udb:write"
        Invoke-UdbLoadCase "VectorSearch" "VectorSearch" '{"collection":"documents","vector":[0.1,0.2,0.3,0.4],"limit":10,"withPayload":true}' "udb:read"
    }
    "tenant-noisy-neighbor" {
        Invoke-UdbLoadCase "Tenant A write pressure" "Upsert" '{"messageType":"ProcessingQueue","payload":{"fields":{"id":{"stringValue":"a-{{.RequestNumber}}"},"status":{"stringValue":"pending"}}},"idempotencyKey":"tenant-a-{{.RequestNumber}}"}' "udb:write" "tenant-a"
        Invoke-UdbLoadCase "Tenant B read isolation" "Select" '{"messageType":"DocumentExtraction","limit":10}' "udb:read" "tenant-b"
    }
    "backend-outage" {
        Invoke-UdbLoadCase "Health report during degraded backend" "GetHealthReport" '{}' "udb:admin"
        Invoke-UdbLoadCase "Generic dry-run list resources" "GenericDispatch" '{"backend":"qdrant","operation":"list_resources","dryRun":true}' "udb:dispatch,udb:admin"
    }
    "reload-during-traffic" {
        Invoke-UdbLoadCase "Select while reload is triggered externally" "Select" '{"messageType":"DocumentExtraction","limit":25}' "udb:read"
        Invoke-UdbLoadCase "Health after reload" "GetHealthReport" '{}' "udb:admin"
    }
    "multi-project-smoke" {
        Invoke-UdbLoadCase "ACME billing writes" "Upsert" '{"context":{"projectId":"acme-billing"},"messageType":"acme.billing.v1.Invoice","payload":{"fields":{"invoice_id":{"stringValue":"inv-{{.RequestNumber}}"},"tenant_id":{"stringValue":"tenant-acme"},"status":{"stringValue":"open"}}},"idempotencyKey":"acme-{{.RequestNumber}}"}' "udb:write" "tenant-acme"
        Invoke-UdbLoadCase "Zen clinic reads" "Select" '{"context":{"projectId":"zen-clinic"},"messageType":"zen.clinic.v1.Appointment","filter":{"fields":{"status":{"stringValue":"scheduled"}}},"limit":10}' "udb:read" "tenant-zen"
    }
    default {
        Write-Error "Unknown profile '$Profile'. See docs/load_soak_profiles.md"
        exit 2
    }
}

Write-Host ""
Write-Host "Load test complete." -ForegroundColor Green
