param(
    [string]$Target = $(if ($env:UDB_TARGET) { $env:UDB_TARGET } else { "127.0.0.1:50051" }),
    [string]$Tenant = $(if ($env:UDB_LOAD_TENANT_ID) { $env:UDB_LOAD_TENANT_ID } else { "load-tenant" }),
    [string]$Project = $(if ($env:UDB_LOAD_PROJECT_ID) { $env:UDB_LOAD_PROJECT_ID } else { "load-project" }),
    [int]$Concurrency = $(if ($env:UDB_LOAD_CONCURRENCY) { [int]$env:UDB_LOAD_CONCURRENCY } else { 8 }),
    [int]$Total = $(if ($env:UDB_LOAD_TOTAL) { [int]$env:UDB_LOAD_TOTAL } else { 200 }),
    [switch]$Tls
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command ghz -ErrorAction SilentlyContinue)) {
    throw "ghz is required: https://ghz.sh"
}

# Import paths so protos that import sibling udb/** files and vendored google/api
# annotations resolve (the control-plane + data_broker protos pull both in).
$common = @("-c", "$Concurrency", "-n", "$Total", "--format", "summary", "-i", "proto,third_party/googleapis")
if (-not $Tls) {
    $common = @("--insecure") + $common
}

# Optional ids for the WRITE/fan-out cases (ghz cannot chain; a real bench supplies
# ids minted out-of-band). Defaults are stable placeholders so the case still
# exercises the RPC admission/validation path under load.
$FileId = $(if ($env:UDB_LOAD_FILE_ID) { $env:UDB_LOAD_FILE_ID } else { "00000000-0000-0000-0000-000000000001" })
$DefinitionId = $(if ($env:UDB_LOAD_DEFINITION_ID) { $env:UDB_LOAD_DEFINITION_ID } else { "00000000-0000-0000-0000-000000000002" })
$AssetId = $(if ($env:UDB_LOAD_ASSET_ID) { $env:UDB_LOAD_ASSET_ID } else { "00000000-0000-0000-0000-000000000003" })
$StepId = $(if ($env:UDB_LOAD_STEP_ID) { $env:UDB_LOAD_STEP_ID } else { "00000000-0000-0000-0000-000000000004" })
$RoomId = $(if ($env:UDB_LOAD_ROOM_ID) { $env:UDB_LOAD_ROOM_ID } else { "00000000-0000-0000-0000-000000000005" })
$PeerId = $(if ($env:UDB_LOAD_PEER_ID) { $env:UDB_LOAD_PEER_ID } else { "load-peer" })

$metadataObject = [ordered]@{
    "x-tenant-id" = $Tenant
    "x-udb-project-id" = $Project
}
if ($env:UDB_LOAD_BEARER) {
    $metadataObject["authorization"] = "Bearer $($env:UDB_LOAD_BEARER)"
}
$metadata = @("-m", ($metadataObject | ConvertTo-Json -Compress))

function Invoke-GhzCase {
    param(
        [string]$Name,
        [string]$Call,
        [string]$Proto,
        [string]$Data
    )
    Write-Host "== $Name =="
    & ghz @common --call $Call --proto $Proto -d $Data @metadata $Target
    if ($LASTEXITCODE -ne 0) {
        throw "ghz case failed: $Name"
    }
}

# ── storage ────────────────────────────────────────────────────────────────────
Invoke-GhzCase `
    -Name "storage register upload" `
    -Call "udb.core.storage.services.v1.StorageService.RegisterUpload" `
    -Proto "proto/udb/core/storage/services/v1/storage_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"project_id`":`"$Project`",`"filename`":`"phase13.bin`",`"content_type`":`"application/octet-stream`",`"file_type`":`"binary`",`"expires_in_minutes`":15,`"size_bytes`":16}"

# WRITE: finalize an upload. Supply UDB_LOAD_FILE_ID from a prior RegisterUpload for
# hits; otherwise this benches the finalize-path admission/validation.
Invoke-GhzCase `
    -Name "storage finalize upload" `
    -Call "udb.core.storage.services.v1.StorageService.FinalizeUpload" `
    -Proto "proto/udb/core/storage/services/v1/storage_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"file_id`":`"$FileId`",`"content_type`":`"application/octet-stream`",`"size_bytes`":16}"

# READ fan-out: list a tenant's objects (storage's list-objects RPC is ListFiles).
Invoke-GhzCase `
    -Name "storage list objects (ListFiles)" `
    -Call "udb.core.storage.services.v1.StorageService.ListFiles" `
    -Proto "proto/udb/core/storage/services/v1/storage_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"page`":1,`"page_size`":25}"

# ── asset ──────────────────────────────────────────────────────────────────────
Invoke-GhzCase `
    -Name "asset list" `
    -Call "udb.core.asset.services.v1.AssetService.ListAssets" `
    -Proto "proto/udb/core/asset/services/v1/asset_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"page_size`":25}"

# WRITE: start a processing pipeline for an asset (enqueues steps).
Invoke-GhzCase `
    -Name "asset start pipeline" `
    -Call "udb.core.asset.services.v1.AssetService.StartPipeline" `
    -Proto "proto/udb/core/asset/services/v1/asset_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"definition_id`":`"$DefinitionId`",`"asset_id`":`"$AssetId`",`"context`":`"{}`",`"correlation_id`":`"load-$AssetId`"}"

# WRITE: complete a pipeline step (advances the pipeline state machine).
Invoke-GhzCase `
    -Name "asset complete step" `
    -Call "udb.core.asset.services.v1.AssetService.CompleteStep" `
    -Proto "proto/udb/core/asset/services/v1/asset_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"step_id`":`"$StepId`",`"status`":`"COMPLETED`",`"result`":`"{}`"}"

# ── webrtc ─────────────────────────────────────────────────────────────────────
Invoke-GhzCase `
    -Name "webrtc list rooms" `
    -Call "udb.core.webrtc.services.v1.RoomService.ListRooms" `
    -Proto "proto/udb/core/webrtc/services/v1/webrtc_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"page_size`":25}"

# WRITE: join a room (allocates a peer / mints a session).
Invoke-GhzCase `
    -Name "webrtc join room" `
    -Call "udb.core.webrtc.services.v1.PeerService.JoinRoom" `
    -Proto "proto/udb/core/webrtc/services/v1/webrtc_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"room_id`":`"$RoomId`",`"display_name`":`"load`",`"metadata`":`"{}`",`"user_agent`":`"ghz`"}"

# FAN-OUT: the bidi Signal stream — a ping signal per stream open.
Invoke-GhzCase `
    -Name "webrtc signal fan-out" `
    -Call "udb.core.webrtc.services.v1.SignalingService.Signal" `
    -Proto "proto/udb/core/webrtc/services/v1/webrtc_service.proto" `
    -Data "[{`"tenant_id`":`"$Tenant`",`"room_id`":`"$RoomId`",`"peer_id`":`"$PeerId`",`"ping`":true}]"

# ── cdc ────────────────────────────────────────────────────────────────────────
Invoke-GhzCase `
    -Name "cdc stream admission" `
    -Call "udb.services.v1.DataBroker.PublishCDC" `
    -Proto "proto/udb/services/v1/data_broker.proto" `
    -Data "{`"topic_pattern`":`"udb.*`",`"since_event_id`":`"`"}"

# DLQ-throughput: inject events on a topic with no owning topic-policy so they are
# rejected by the CDC engine and routed to the DLQ. A fresh event_id per request
# ({{newUUID}}) avoids dedup so each request exercises the reject+DLQ path.
Invoke-GhzCase `
    -Name "cdc dlq throughput (rejected events)" `
    -Call "udb.services.v1.DataBroker.EnqueueOutboxEvent" `
    -Proto "proto/udb/services/v1/data_broker.proto" `
    -Data "{`"topic`":`"udb.load.rejected.unrouted.v1`",`"partition_key`":`"{{.RequestNumber}}`",`"payload`":{`"event_id`":`"{{newUUID}}`",`"event_type`":`"udb.load.rejected.unrouted.v1`",`"correlation_id`":`"load-dlq-{{.RequestNumber}}`",`"document_id`":`"{{.RequestNumber}}`"}}"

# ── policy distribution ────────────────────────────────────────────────────────
Invoke-GhzCase `
    -Name "policy revision read" `
    -Call "udb.core.authz.services.v1.AuthzService.GetAuthzRevision" `
    -Proto "proto/udb/core/authz/services/v1/authz_service.proto" `
    -Data "{`"tenant_id`":`"$Tenant`",`"project_id`":`"$Project`"}"

# FAN-OUT: the control-plane StreamResources bidi stream — one DiscoveryRequest per
# stream subscribes a node to a resource type; the server pushes the world.
Invoke-GhzCase `
    -Name "policy distribution push fan-out (StreamResources)" `
    -Call "udb.core.control.services.v1.ControlPlaneService.StreamResources" `
    -Proto "proto/udb/core/control/services/v1/control_plane_service.proto" `
    -Data "[{`"node_id`":`"load-{{.RequestNumber}}`",`"resource_type`":`"RESOURCE_TYPE_ROUTING_POLICY`",`"resource_names`":[]}]"
