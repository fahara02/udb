#!/usr/bin/env bash
# UDB load/soak profiles using ghz (https://ghz.sh).

set -euo pipefail

UDB_HOST=${UDB_HOST:-"localhost:50000"}
CONCURRENCY=${CONCURRENCY:-50}
TOTAL_REQUESTS=${TOTAL_REQUESTS:-10000}
PROFILE=${PROFILE:-"read-heavy"}
PROTO_ROOT=${PROTO_ROOT:-"../proto"}
PROTO_FILE=${PROTO_FILE:-"${PROTO_ROOT}/udb/services/v1/data_broker.proto"}
SERVICE=${SERVICE:-"udb.services.v1.DataBroker"}

if ! command -v ghz >/dev/null 2>&1; then
  echo "Error: ghz is not installed. Install it from https://ghz.sh" >&2
  exit 1
fi

run_case() {
  local name="$1"
  local call="$2"
  local data="$3"
  local scopes="$4"
  local tenant="${5:-test-tenant}"

  echo ""
  echo "[$PROFILE] $name"
  ghz --insecure \
    --proto "$PROTO_FILE" \
    --import-path "$PROTO_ROOT" \
    --call "${SERVICE}.${call}" \
    -d "$data" \
    -m "{\"x-tenant-id\":\"${tenant}\",\"x-purpose\":\"benchmark\",\"x-scopes\":\"${scopes}\",\"x-service-identity\":\"load.test\"}" \
    -c "$CONCURRENCY" \
    -n "$TOTAL_REQUESTS" \
    "$UDB_HOST"
}

echo "=========================================="
echo " UDB Load Test"
echo " Host: $UDB_HOST"
echo " Profile: $PROFILE"
echo " Concurrency: $CONCURRENCY"
echo " Total Requests: $TOTAL_REQUESTS"
echo " Proto: $PROTO_FILE"
echo "=========================================="

case "$PROFILE" in
  read-heavy)
    run_case "Select" "Select" \
      '{"messageType":"DocumentExtraction","filter":{"fields":{"status":{"stringValue":"processed"}}},"limit":25}' \
      "udb:read"
    run_case "Capabilities" "GetCapabilities" \
      '{"projectId":"default"}' \
      "udb:admin"
    ;;
  write-heavy)
    run_case "Upsert" "Upsert" \
      '{"messageType":"ProcessingQueue","payload":{"fields":{"id":{"stringValue":"load-{{.RequestNumber}}"},"status":{"stringValue":"pending"}}},"idempotencyKey":"load-{{.RequestNumber}}"}' \
      "udb:write"
    run_case "EnqueueOutboxEvent" "EnqueueOutboxEvent" \
      '{"topic":"document.uploaded.v1","partitionKey":"doc-{{.RequestNumber}}","payload":{"fields":{"event_id":{"stringValue":"11111111-1111-4111-8111-111111111111"},"event_type":{"stringValue":"document.uploaded.v1"},"correlation_id":{"stringValue":"load"},"document_id":{"stringValue":"doc-{{.RequestNumber}}"}}},"idempotencyKey":"outbox-{{.RequestNumber}}"}' \
      "udb:write"
    ;;
  mixed-projection)
    run_case "Upsert plus projection fanout" "Upsert" \
      '{"messageType":"ProcessingQueue","payload":{"fields":{"id":{"stringValue":"projection-{{.RequestNumber}}"},"status":{"stringValue":"project"},"tenant_id":{"stringValue":"test-tenant"}}},"idempotencyKey":"projection-{{.RequestNumber}}"}' \
      "udb:write"
    run_case "VectorSearch" "VectorSearch" \
      '{"collection":"documents","vector":[0.1,0.2,0.3,0.4],"limit":10,"withPayload":true}' \
      "udb:read"
    ;;
  tenant-noisy-neighbor)
    run_case "Tenant A write pressure" "Upsert" \
      '{"messageType":"ProcessingQueue","payload":{"fields":{"id":{"stringValue":"a-{{.RequestNumber}}"},"status":{"stringValue":"pending"}}},"idempotencyKey":"tenant-a-{{.RequestNumber}}"}' \
      "udb:write" "tenant-a"
    run_case "Tenant B read isolation" "Select" \
      '{"messageType":"DocumentExtraction","limit":10}' \
      "udb:read" "tenant-b"
    ;;
  backend-outage)
    run_case "Health report during degraded backend" "GetHealthReport" \
      '{}' \
      "udb:admin"
    run_case "Generic dry-run list resources" "GenericDispatch" \
      '{"backend":"qdrant","operation":"list_resources","dryRun":true}' \
      "udb:dispatch,udb:admin"
    ;;
  reload-during-traffic)
    run_case "Select while reload is triggered externally" "Select" \
      '{"messageType":"DocumentExtraction","limit":25}' \
      "udb:read"
    run_case "Health after reload" "GetHealthReport" \
      '{}' \
      "udb:admin"
    ;;
  multi-project-smoke)
    run_case "ACME billing writes" "Upsert" \
      '{"context":{"projectId":"acme-billing"},"messageType":"acme.billing.v1.Invoice","payload":{"fields":{"invoice_id":{"stringValue":"inv-{{.RequestNumber}}"},"tenant_id":{"stringValue":"tenant-acme"},"status":{"stringValue":"open"}}},"idempotencyKey":"acme-{{.RequestNumber}}"}' \
      "udb:write" "tenant-acme"
    run_case "Zen clinic reads" "Select" \
      '{"context":{"projectId":"zen-clinic"},"messageType":"zen.clinic.v1.Appointment","filter":{"fields":{"status":{"stringValue":"scheduled"}}},"limit":10}' \
      "udb:read" "tenant-zen"
    ;;
  *)
    echo "Unknown PROFILE '$PROFILE'. See docs/load_soak_profiles.md" >&2
    exit 2
    ;;
esac

echo ""
echo "Load test complete."
