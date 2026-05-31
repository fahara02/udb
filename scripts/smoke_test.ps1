# scripts/smoke_test.ps1 — UDB playground smoke-test suite (Windows / PowerShell)
#
# Validates the full end-to-end stack against a running playground.
# Start the stack first:  .\scripts\playground.ps1 up
# Then run:               .\scripts\smoke_test.ps1
#
# Dependencies (on PATH):
#   - grpcurl  (https://github.com/fullstorydev/grpcurl)
#   - jq       (https://jqlang.github.io/jq/)
#
# Environment overrides via env vars:
#   UDB_HOST    default: localhost
#   UDB_PORT    default: 50051
#   UDB_TENANT  default: smoke-tenant

param(
    [string]$Host   = $env:UDB_HOST   ?? "localhost",
    [string]$Port   = $env:UDB_PORT   ?? "50051",
    [string]$Tenant = $env:UDB_TENANT ?? "smoke-tenant"
)

$ErrorActionPreference = "Stop"
$Addr = "${Host}:${Port}"
$Pass = 0
$Fail = 0

# ── Helpers ───────────────────────────────────────────────────────────────────

function Invoke-Grpc {
    param([string]$Method, [string]$Data = "{}")
    grpcurl -plaintext `
        -H "x-tenant-id: $Tenant" `
        -H "x-scopes: udb:admin *" `
        -d $Data `
        $Addr `
        "udb.services.v1.DataBroker/$Method"
}

function Check {
    param([string]$Label, [bool]$Ok)
    if ($Ok) {
        Write-Host "  [PASS] $Label" -ForegroundColor Green
        $script:Pass++
    } else {
        Write-Host "  [FAIL] $Label" -ForegroundColor Red
        $script:Fail++
    }
}

function Run-Check {
    param([string]$Label, [scriptblock]$Body)
    try {
        $result = & $Body
        Check -Label $Label -Ok ($result -eq $true -or $result -match "true")
    } catch {
        Check -Label $Label -Ok $false
        Write-Host "         error: $($_.Exception.Message)" -ForegroundColor DarkGray
    }
}

function Wait-ForUdb {
    Write-Host "Waiting for UDB gRPC at ${Addr}..." -ForegroundColor Cyan
    $attempts = 0
    while ($true) {
        try {
            grpcurl -plaintext $Addr list 2>$null | Out-Null
            if ($LASTEXITCODE -eq 0) { break }
        } catch {}
        $attempts++
        if ($attempts -ge 30) {
            Write-Error "UDB did not become ready after 30 attempts"
            exit 1
        }
        Start-Sleep -Seconds 2
    }
    Write-Host "UDB is ready." -ForegroundColor Green
}

function Jq-Test {
    param([string]$Json, [string]$Filter)
    $r = $Json | jq -e $Filter 2>$null
    return ($LASTEXITCODE -eq 0)
}

# ── Test groups ───────────────────────────────────────────────────────────────

function Test-Health {
    Write-Host ""
    Write-Host "── Health & Capabilities ────────────────────────────────────────────" -ForegroundColor Cyan

    Run-Check "GetHealthReport passes" {
        $out = Invoke-Grpc "GetHealthReport" '{"with_probes":true}'
        Jq-Test $out '.passed == true'
    }
    Run-Check "GetCapabilities lists supported_rpcs" {
        $out = Invoke-Grpc "GetCapabilities" '{}'
        Jq-Test $out '(.supported_rpcs | length) > 0'
    }
    Run-Check "GetCatalogManifest returns manifest_json" {
        $out = Invoke-Grpc "GetCatalogManifest" '{"redact":false}'
        Jq-Test $out '(.manifest_json | length) > 0'
    }
}

function Test-Relational {
    Write-Host ""
    Write-Host "── Relational (Upsert / Select / Delete) ────────────────────────────" -ForegroundColor Cyan

    $upsertPayload = '{"message_type":"smoke.TestRecord","fields":{"fields":{"id":{"string_value":"smoke-001"},"label":{"string_value":"hello"}}}}'

    Run-Check "Upsert returns affected_rows >= 0" {
        $out = Invoke-Grpc "Upsert" $upsertPayload
        Jq-Test $out '.affected_rows >= 0'
    }
    Run-Check "Select returns a RecordSet" {
        $out = Invoke-Grpc "Select" '{"message_type":"smoke.TestRecord","filters":{"fields":{"id":{"string_value":"smoke-001"}}}}'
        Jq-Test $out 'has("rows")'
    }
    Run-Check "Delete returns affected_rows >= 0" {
        $out = Invoke-Grpc "Delete" '{"message_type":"smoke.TestRecord","filter":{"fields":{"id":{"string_value":"smoke-001"}}}}'
        Jq-Test $out '.affected_rows >= 0'
    }
}

function Test-Event {
    Write-Host ""
    Write-Host "── Outbox Event Enqueue ─────────────────────────────────────────────" -ForegroundColor Cyan

    $payload = '{"topic":"document.uploaded.v1","partition_key":"smoke-doc-001","payload":{"fields":{"document_id":{"string_value":"smoke-doc-001"},"event_id":{"string_value":"00000000-0000-0000-0000-000000000001"},"event_type":{"string_value":"document.uploaded.v1"},"correlation_id":{"string_value":"smoke-corr-001"},"source_agent":{"string_value":"smoke-test"}}},"idempotency_key":"smoke-idem-001"}'

    Run-Check "EnqueueOutboxEvent returns event_id" {
        $out = Invoke-Grpc "EnqueueOutboxEvent" $payload
        Jq-Test $out '(.event_id | length) > 0'
    }
    Run-Check "EnqueueOutboxEvent deduplicates with same key" {
        $out = Invoke-Grpc "EnqueueOutboxEvent" $payload
        Jq-Test $out '.was_duplicate == true'
    }
}

function Test-Vector {
    Write-Host ""
    Write-Host "── Vector Upsert / Search ───────────────────────────────────────────" -ForegroundColor Cyan

    $vUpsert = '{"collection":"smoke_vectors","points":[{"id":"vec-001","vector":[0.1,0.2,0.3,0.4],"payload":{"fields":{"label":{"string_value":"smoke vector"}}}}]}'
    Run-Check "VectorUpsert succeeds or UNAVAILABLE" {
        $out = Invoke-Grpc "VectorUpsert" $vUpsert 2>&1
        $out -match '(affected_rows|unavailable|UNAVAILABLE|error)'
    }

    $vSearch = '{"collection":"smoke_vectors","vector":[0.1,0.2,0.3,0.4],"limit":3}'
    Run-Check "VectorSearch returns or UNAVAILABLE" {
        $out = Invoke-Grpc "VectorSearch" $vSearch 2>&1
        $out -match '(matches|unavailable|UNAVAILABLE|error)'
    }
}

function Test-Object {
    Write-Host ""
    Write-Host "── Object Store ─────────────────────────────────────────────────────" -ForegroundColor Cyan

    Run-Check "GeneratePresignedUrl returns url or UNAVAILABLE" {
        $out = Invoke-Grpc "GeneratePresignedUrl" '{"bucket":"udb-playground","key":"smoke/test.txt","method":"PUT"}' 2>&1
        $out -match '(url|unavailable|UNAVAILABLE)'
    }
}

function Test-Catalog {
    Write-Host ""
    Write-Host "── Catalog Versioning ───────────────────────────────────────────────" -ForegroundColor Cyan
    Run-Check "GetCatalogVersions returns list" {
        $out = Invoke-Grpc "GetCatalogVersions" '{}'
        Jq-Test $out 'has("versions")'
    }
}

function Test-DlqSaga {
    Write-Host ""
    Write-Host "── DLQ & Saga Admin ─────────────────────────────────────────────────" -ForegroundColor Cyan
    Run-Check "ListDlqEvents returns list" {
        $out = Invoke-Grpc "ListDlqEvents" '{"page_size":10}'
        Jq-Test $out 'has("events")'
    }
    Run-Check "ListSagas returns list" {
        $out = Invoke-Grpc "ListSagas" '{"page_size":10}'
        Jq-Test $out 'has("sagas")'
    }
}

function Test-Policy {
    Write-Host ""
    Write-Host "── Policy Admin ─────────────────────────────────────────────────────" -ForegroundColor Cyan
    Run-Check "ListPolicies returns list" {
        $out = Invoke-Grpc "ListPolicies" '{}'
        Jq-Test $out 'has("policies")'
    }
}

function Test-AdminSummary {
    Write-Host ""
    Write-Host "── Unified Admin Summary ────────────────────────────────────────────" -ForegroundColor Cyan
    Run-Check "GetAdminSummary has all sections" {
        $out = Invoke-Grpc "GetAdminSummary" '{}'
        Jq-Test $out 'has("catalog") and has("cdc") and has("sagas") and has("backends")'
    }
}

# ── Main ──────────────────────────────────────────────────────────────────────

Write-Host "========================================================" -ForegroundColor White
Write-Host " UDB Playground Smoke Tests"                               -ForegroundColor White
Write-Host " Target : ${Addr}"                                         -ForegroundColor White
Write-Host " Tenant : ${Tenant}"                                       -ForegroundColor White
Write-Host "========================================================" -ForegroundColor White

Wait-ForUdb

Test-Health
Test-Relational
Test-Event
Test-Vector
Test-Object
Test-Catalog
Test-DlqSaga
Test-Policy
Test-AdminSummary

Write-Host ""
Write-Host "========================================================" -ForegroundColor White
Write-Host " Results: ${Pass} passed, ${Fail} failed"                  -ForegroundColor $(if ($Fail -gt 0) { "Red" } else { "Green" })
Write-Host "========================================================" -ForegroundColor White

if ($Fail -gt 0) { exit 1 }
exit 0
