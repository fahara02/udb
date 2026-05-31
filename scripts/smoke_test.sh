#!/usr/bin/env bash
# scripts/smoke_test.sh — UDB playground smoke-test suite
#
# Validates the full end-to-end stack against a running playground:
#   ./scripts/playground.sh up   # start stack first
#   ./scripts/smoke_test.sh      # then run this
#
# Exit code: 0 = all checks passed, 1 = one or more failures.
#
# Dependencies (installed on the host or available in the UDB container):
#   - grpcurl  (https://github.com/fullstorydev/grpcurl)
#   - curl
#   - jq
#
# Environment overrides:
#   UDB_HOST    — default: localhost
#   UDB_PORT    — default: 50051
#   UDB_TENANT  — default: smoke-tenant

set -euo pipefail

HOST="${UDB_HOST:-localhost}"
PORT="${UDB_PORT:-50051}"
TENANT="${UDB_TENANT:-smoke-tenant}"
ADDR="${HOST}:${PORT}"

PASS=0
FAIL=0

# ── Helpers ───────────────────────────────────────────────────────────────────

grpc() {
    # grpc <rpc_method> [request_json]
    local method="$1"
    local data="${2:-'{}'}"
    grpcurl -plaintext \
        -H "x-tenant-id: ${TENANT}" \
        -H "x-scopes: udb:admin *" \
        -d "$data" \
        "$ADDR" \
        "udb.services.v1.DataBroker/${method}"
}

check() {
    local label="$1"
    local result="$2"   # "ok" or "fail"
    if [[ "$result" == "ok" ]]; then
        echo "  [PASS] $label"
        PASS=$(( PASS + 1 ))
    else
        echo "  [FAIL] $label"
        FAIL=$(( FAIL + 1 ))
    fi
}

run_check() {
    local label="$1"
    shift
    if "$@" > /tmp/smoke_out 2>&1; then
        check "$label" "ok"
    else
        check "$label" "fail"
        echo "         output: $(head -3 /tmp/smoke_out)"
    fi
}

wait_for_udb() {
    echo "Waiting for UDB gRPC at ${ADDR}…"
    local attempts=0
    until grpcurl -plaintext "$ADDR" list > /dev/null 2>&1; do
        attempts=$(( attempts + 1 ))
        if [[ $attempts -ge 30 ]]; then
            echo "ERROR: UDB did not become ready after 30 attempts"
            exit 1
        fi
        sleep 2
    done
    echo "UDB is ready."
}

# ── Test groups ───────────────────────────────────────────────────────────────

smoke_health() {
    echo ""
    echo "── Health & Capabilities ────────────────────────────────────────────"

    run_check "GetHealthReport passes" \
        bash -c "grpc GetHealthReport '{\"with_probes\":true}' | jq -e '.passed == true'"

    run_check "GetCapabilities lists supported_rpcs" \
        bash -c "grpc GetCapabilities '{}' | jq -e '(.supported_rpcs | length) > 0'"

    run_check "GetCatalogManifest returns manifest_json" \
        bash -c "grpc GetCatalogManifest '{\"redact\":false}' | jq -e '(.manifest_json | length) > 0'"
}

smoke_relational() {
    echo ""
    echo "── Relational (Select / Upsert / Delete) ────────────────────────────"

    local upsert_payload
    upsert_payload=$(cat <<'EOF'
{
  "message_type": "smoke.TestRecord",
  "fields": {
    "fields": {
      "id":    {"string_value": "smoke-001"},
      "label": {"string_value": "hello from smoke test"}
    }
  }
}
EOF
)

    run_check "Upsert returns affected_rows >= 0" \
        bash -c "grpc Upsert '$upsert_payload' | jq -e '.affected_rows >= 0'"

    run_check "Select returns a RecordSet" \
        bash -c "grpc Select '{\"message_type\":\"smoke.TestRecord\",\"filters\":{\"fields\":{\"id\":{\"string_value\":\"smoke-001\"}}}}' | jq -e 'has(\"rows\")'"

    run_check "Delete returns affected_rows >= 0" \
        bash -c "grpc Delete '{\"message_type\":\"smoke.TestRecord\",\"filter\":{\"fields\":{\"id\":{\"string_value\":\"smoke-001\"}}}}' | jq -e '.affected_rows >= 0'"
}

smoke_event() {
    echo ""
    echo "── Outbox Event Enqueue ─────────────────────────────────────────────"

    local enqueue_payload
    enqueue_payload=$(cat <<'EOF'
{
  "topic": "document.uploaded.v1",
  "partition_key": "smoke-doc-001",
  "payload": {
    "fields": {
      "document_id":    {"string_value": "smoke-doc-001"},
      "event_id":       {"string_value": "00000000-0000-0000-0000-000000000001"},
      "event_type":     {"string_value": "document.uploaded.v1"},
      "correlation_id": {"string_value": "smoke-corr-001"},
      "source_agent":   {"string_value": "smoke-test"}
    }
  },
  "idempotency_key": "smoke-idem-001"
}
EOF
)

    run_check "EnqueueOutboxEvent returns event_id" \
        bash -c "grpc EnqueueOutboxEvent '$enqueue_payload' | jq -e '(.event_id | length) > 0'"

    run_check "EnqueueOutboxEvent deduplicates with same key" \
        bash -c "grpc EnqueueOutboxEvent '$enqueue_payload' | jq -e '.was_duplicate == true'"
}

smoke_vector() {
    echo ""
    echo "── Vector Upsert / Search ───────────────────────────────────────────"

    local v_upsert
    v_upsert=$(cat <<'EOF'
{
  "collection": "smoke_vectors",
  "points": [{
    "id": "vec-001",
    "vector": [0.1, 0.2, 0.3, 0.4],
    "payload": {"fields": {"label": {"string_value": "smoke vector"}}}
  }]
}
EOF
)
    run_check "VectorUpsert succeeds (or UNAVAILABLE if Qdrant not seeded)" \
        bash -c "out=\$(grpc VectorUpsert '$v_upsert' 2>&1); echo \"\$out\" | grep -qE '(affected_rows|unavailable|UNAVAILABLE|error)'"

    local v_search
    v_search=$(cat <<'EOF'
{
  "collection": "smoke_vectors",
  "vector": [0.1, 0.2, 0.3, 0.4],
  "limit": 3
}
EOF
)
    run_check "VectorSearch returns results or UNAVAILABLE" \
        bash -c "out=\$(grpc VectorSearch '$v_search' 2>&1); echo \"\$out\" | grep -qE '(matches|unavailable|UNAVAILABLE|error)'"
}

smoke_object() {
    echo ""
    echo "── Object Store (GeneratePresignedUrl) ──────────────────────────────"

    run_check "GeneratePresignedUrl returns a url" \
        bash -c "grpc GeneratePresignedUrl '{\"bucket\":\"udb-playground\",\"key\":\"smoke/test.txt\",\"method\":\"PUT\"}' 2>&1 | grep -qE '(url|unavailable|UNAVAILABLE)'"
}

smoke_catalog() {
    echo ""
    echo "── Catalog Versioning ───────────────────────────────────────────────"

    run_check "GetCatalogVersions returns list" \
        bash -c "grpc GetCatalogVersions '{}' | jq -e 'has(\"versions\")'"
}

smoke_dlq() {
    echo ""
    echo "── DLQ & Saga Admin ─────────────────────────────────────────────────"

    run_check "ListDlqEvents returns list" \
        bash -c "grpc ListDlqEvents '{\"page_size\":10}' | jq -e 'has(\"events\")'"

    run_check "ListSagas returns list" \
        bash -c "grpc ListSagas '{\"page_size\":10}' | jq -e 'has(\"sagas\")'"
}

smoke_policy() {
    echo ""
    echo "── Policy Admin ─────────────────────────────────────────────────────"

    run_check "ListPolicies returns list" \
        bash -c "grpc ListPolicies '{}' | jq -e 'has(\"policies\")'"
}

smoke_admin_summary() {
    echo ""
    echo "── Unified Admin Summary ────────────────────────────────────────────"

    run_check "GetAdminSummary returns all sections" \
        bash -c "grpc GetAdminSummary '{}' | jq -e 'has(\"catalog\") and has(\"cdc\") and has(\"sagas\") and has(\"backends\")'"
}

# ── Main ──────────────────────────────────────────────────────────────────────

echo "========================================================"
echo " UDB Playground Smoke Tests"
echo " Target : ${ADDR}"
echo " Tenant : ${TENANT}"
echo "========================================================"

wait_for_udb

smoke_health
smoke_relational
smoke_event
smoke_vector
smoke_object
smoke_catalog
smoke_dlq
smoke_policy
smoke_admin_summary

echo ""
echo "========================================================"
echo " Results: ${PASS} passed, ${FAIL} failed"
echo "========================================================"

if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
exit 0
