#!/usr/bin/env bash
set -euo pipefail

# Native-service load harness for Phase 13 readiness.
# Requires ghz on PATH and a running UDB broker/native control plane.
#
# Environment:
#   UDB_TARGET              public DataBroker gRPC target, default 127.0.0.1:50051
#   UDB_NATIVE_TARGET       native control-plane target, default UDB_TARGET port + 10
#   UDB_INSECURE           true/false, default true
#   UDB_LOAD_TENANT_ID     tenant id to stamp in request metadata/body
#   UDB_LOAD_PROJECT_ID    project id to stamp in request metadata/body
#   UDB_LOAD_BEARER        optional bearer token
#   UDB_LOAD_CONCURRENCY   ghz concurrency, default 8
#   UDB_LOAD_TOTAL         total requests per scenario, default 200

if ! command -v ghz >/dev/null 2>&1; then
  echo "ghz is required: https://ghz.sh" >&2
  exit 2
fi

PUBLIC_TARGET="${UDB_TARGET:-127.0.0.1:50051}"
TENANT="${UDB_LOAD_TENANT_ID:-load-tenant}"
PROJECT="${UDB_LOAD_PROJECT_ID:-load-project}"
CONCURRENCY="${UDB_LOAD_CONCURRENCY:-8}"
TOTAL="${UDB_LOAD_TOTAL:-200}"
INSECURE="${UDB_INSECURE:-true}"

default_native_target() {
  local target="$1"
  local host="${target%:*}"
  local port="${target##*:}"
  if [[ -z "$host" || "$host" == "$target" || ! "$port" =~ ^[0-9]+$ ]]; then
    echo "127.0.0.1:50061"
    return
  fi
  echo "${host}:$((port + 10))"
}

NATIVE_TARGET="${UDB_NATIVE_TARGET:-$(default_native_target "$PUBLIC_TARGET")}"

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  printf '%s' "$value"
}

# Import paths so protos that import sibling udb/** files and vendored google/api
# annotations resolve (the control-plane + data_broker protos pull both in).
IMPORTS=(-i "proto,third_party/googleapis")
COMMON=(-c "$CONCURRENCY" -n "$TOTAL" --count-errors --format summary "${IMPORTS[@]}" --call)
if [[ "$INSECURE" == "true" || "$INSECURE" == "1" ]]; then
  COMMON=(--insecure "${COMMON[@]}")
fi
METADATA_JSON="{\"x-tenant-id\":\"$(json_escape "$TENANT")\",\"x-udb-project-id\":\"$(json_escape "$PROJECT")\""
if [[ -n "${UDB_LOAD_BEARER:-}" ]]; then
  METADATA_JSON+=",\"authorization\":\"Bearer $(json_escape "$UDB_LOAD_BEARER")\""
fi
METADATA_JSON+="}"
META=(-m "$METADATA_JSON")

target_for_call() {
  local call="$1"
  case "$call" in
    udb.services.v1.DataBroker.*) printf '%s' "$PUBLIC_TARGET" ;;
    *) printf '%s' "$NATIVE_TARGET" ;;
  esac
}

run_case() {
  local name="$1"
  local call="$2"
  shift 2
  local target
  target="$(target_for_call "$call")"
  echo "== ${name} =="
  ghz "${COMMON[@]}" "$call" "$@" "${META[@]}" "$target"
}

# Optional ids for the WRITE/fan-out cases (chaining is not possible in ghz, so a
# real bench supplies ids minted out-of-band). Defaults are stable placeholders so
# the case still exercises the RPC admission/validation path under load.
FILE_ID="${UDB_LOAD_FILE_ID:-00000000-0000-0000-0000-000000000001}"
DEFINITION_ID="${UDB_LOAD_DEFINITION_ID:-00000000-0000-0000-0000-000000000002}"
ASSET_ID="${UDB_LOAD_ASSET_ID:-00000000-0000-0000-0000-000000000003}"
STEP_ID="${UDB_LOAD_STEP_ID:-00000000-0000-0000-0000-000000000004}"
ROOM_ID="${UDB_LOAD_ROOM_ID:-00000000-0000-0000-0000-000000000005}"
PEER_ID="${UDB_LOAD_PEER_ID:-load-peer}"

# ── storage ────────────────────────────────────────────────────────────────────
run_case "storage register upload" \
  udb.core.storage.services.v1.StorageService.RegisterUpload \
  --proto proto/udb/core/storage/services/v1/storage_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"project_id\":\"${PROJECT}\",\"filename\":\"phase13.bin\",\"content_type\":\"application/octet-stream\",\"file_type\":\"binary\",\"expires_in_minutes\":15,\"size_bytes\":16}"

# WRITE: finalize an upload (commits an upload's metadata). Supply UDB_LOAD_FILE_ID
# from a prior RegisterUpload for hits; otherwise this benches the finalize-path
# admission/validation (not-found is the expected fast reject for the placeholder).
run_case "storage finalize upload" \
  udb.core.storage.services.v1.StorageService.FinalizeUpload \
  --proto proto/udb/core/storage/services/v1/storage_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"file_id\":\"${FILE_ID}\",\"content_type\":\"application/octet-stream\",\"size_bytes\":16}"

# READ fan-out: list a tenant's objects (storage's list-objects RPC is ListFiles).
run_case "storage list objects (ListFiles)" \
  udb.core.storage.services.v1.StorageService.ListFiles \
  --proto proto/udb/core/storage/services/v1/storage_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"page\":1,\"page_size\":25}"

# ── asset ──────────────────────────────────────────────────────────────────────
run_case "asset list" \
  udb.core.asset.services.v1.AssetService.ListAssets \
  --proto proto/udb/core/asset/services/v1/asset_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"page_size\":25}"

# WRITE: start a processing pipeline for an asset (enqueues steps).
run_case "asset start pipeline" \
  udb.core.asset.services.v1.AssetService.StartPipeline \
  --proto proto/udb/core/asset/services/v1/asset_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"definition_id\":\"${DEFINITION_ID}\",\"asset_id\":\"${ASSET_ID}\",\"context\":\"{}\",\"correlation_id\":\"load-${ASSET_ID}\"}"

# WRITE: complete a pipeline step (advances the pipeline state machine).
run_case "asset complete step" \
  udb.core.asset.services.v1.AssetService.CompleteStep \
  --proto proto/udb/core/asset/services/v1/asset_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"step_id\":\"${STEP_ID}\",\"status\":\"COMPLETED\",\"result\":\"{}\"}"

# ── webrtc ─────────────────────────────────────────────────────────────────────
run_case "webrtc list rooms" \
  udb.core.webrtc.services.v1.RoomService.ListRooms \
  --proto proto/udb/core/webrtc/services/v1/webrtc_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"page_size\":25}"

# WRITE: join a room (allocates a peer / mints a session).
run_case "webrtc join room" \
  udb.core.webrtc.services.v1.PeerService.JoinRoom \
  --proto proto/udb/core/webrtc/services/v1/webrtc_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"room_id\":\"${ROOM_ID}\",\"display_name\":\"load\",\"metadata\":\"{}\",\"user_agent\":\"ghz\"}"

# FAN-OUT: the bidi Signal stream — a ping signal per stream open. ghz sends the
# -d array as the client stream; the case measures signaling fan-out throughput.
run_case "webrtc signal fan-out" \
  udb.core.webrtc.services.v1.SignalingService.Signal \
  --proto proto/udb/core/webrtc/services/v1/webrtc_service.proto \
  -d "[{\"tenant_id\":\"${TENANT}\",\"room_id\":\"${ROOM_ID}\",\"peer_id\":\"${PEER_ID}\",\"ping\":true}]"

# ── cdc ────────────────────────────────────────────────────────────────────────
run_case "cdc stream admission" \
  udb.services.v1.DataBroker.PublishCDC \
  --proto proto/udb/services/v1/data_broker.proto \
  -d "{\"topic_pattern\":\"udb.*\",\"since_event_id\":\"\"}"

# DLQ-throughput: inject events on a topic with no owning topic-policy so they are
# rejected by the CDC engine and routed to the DLQ — measures the reject+DLQ path
# under load. Each call enqueues one outbox event; a fresh event_id per request is
# generated by ghz's {{newUUID}} template so they are not deduped.
run_case "cdc dlq throughput (rejected events)" \
  udb.services.v1.DataBroker.EnqueueOutboxEvent \
  --proto proto/udb/services/v1/data_broker.proto \
  -d "{\"topic\":\"udb.load.rejected.unrouted.v1\",\"partition_key\":\"{{.RequestNumber}}\",\"payload\":{\"event_id\":\"{{newUUID}}\",\"event_type\":\"udb.load.rejected.unrouted.v1\",\"correlation_id\":\"load-dlq-{{.RequestNumber}}\",\"document_id\":\"{{.RequestNumber}}\"}}"

# ── policy distribution ────────────────────────────────────────────────────────
run_case "policy revision read" \
  udb.core.authz.services.v1.AuthzService.GetAuthzRevision \
  --proto proto/udb/core/authz/services/v1/authz_service.proto \
  -d "{\"tenant_id\":\"${TENANT}\",\"project_id\":\"${PROJECT}\"}"

# FAN-OUT: the control-plane StreamResources bidi stream — one DiscoveryRequest per
# stream subscribes a node to a resource type; the server pushes the world. This
# benches the policy distribution-push fan-out (the xDS-style ADS stream).
run_case "policy distribution push fan-out (StreamResources)" \
  udb.core.control.services.v1.ControlPlaneService.StreamResources \
  --proto proto/udb/core/control/services/v1/control_plane_service.proto \
  -d "[{\"node_id\":\"load-{{.RequestNumber}}\",\"resource_type\":\"RESOURCE_TYPE_ROUTING_POLICY\",\"resource_names\":[]}]"
