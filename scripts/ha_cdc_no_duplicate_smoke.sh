#!/usr/bin/env bash
set -euo pipefail

# Multi-process CDC no-duplicate smoke for master-plan 1.1.
#
# Starts the HA broker profile and proves a peer takeover does not republish a
# redelivered event_id:
#   1. wait for the CDC tailer row-level lock holder,
#   2. enqueue one outbox row and prove it reaches the CDC journal + Kafka once,
#   3. kill the CDC holder container and wait for the peer to acquire the lock,
#   4. reinsert the same event_id into the outbox,
#   5. assert the peer acks the duplicate via the durable journal and Kafka still
#      contains exactly one message for that event_id.

PROJECT="${UDB_HA_CDC_PROJECT:-udb-ha-cdc}"
COMPOSE_FILE="${UDB_HA_CDC_COMPOSE_FILE:-docker-compose.integration.yml}"
LOCK_RELATION="${UDB_HA_CDC_LOCK_RELATION:-udb_system.udb_cdc_lock_log}"
CDC_LOCK_KEY="${UDB_HA_CDC_LOCK_KEY:-33042945945068643}"
RUN_ID="${UDB_HA_CDC_RUN_ID:-$(date +%Y%m%d%H%M%S)-$$}"
TOPIC="${UDB_HA_CDC_TOPIC:-udb.ha.cdc.${RUN_ID}.v1}"
FAILOVER_TIMEOUT_SECS="${UDB_HA_CDC_FAILOVER_TIMEOUT_SECS:-95}"
PUBLISH_TIMEOUT_SECS="${UDB_HA_CDC_PUBLISH_TIMEOUT_SECS:-90}"
POLL_SECS="${UDB_HA_CDC_POLL_SECS:-2}"
KEEP_STACK="${UDB_HA_CDC_KEEP_STACK:-0}"

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 2
  fi
}

require_sql_token() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[A-Za-z0-9._:-]+$ ]]; then
    echo "${name} must contain only letters, digits, dot, colon, underscore, or dash: ${value}" >&2
    exit 2
  fi
}

require_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "${name} must be an unsigned integer: ${value}" >&2
    exit 2
  fi
}

psql_exec() {
  compose exec -T postgres psql -U udb -d udb -v ON_ERROR_STOP=1 -q -c "$1"
}

psql_scalar() {
  compose exec -T postgres psql -U udb -d udb -v ON_ERROR_STOP=1 -qAt -c "$1" | tr -d '\r'
}

cleanup_rows() {
  psql_exec "
    DELETE FROM udb_system.udb_cdc_event_journal WHERE topic = '${TOPIC}' AND payload->>'correlation_id' = '${RUN_ID}';
    DELETE FROM udb_system.outbox_events WHERE topic = '${TOPIC}' AND payload->>'correlation_id' = '${RUN_ID}';
  " >/dev/null 2>&1 || true
}

cleanup() {
  cleanup_rows
  if [[ "$KEEP_STACK" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_healthy() {
  local service="$1"
  local attempts="${2:-90}"
  for _ in $(seq 1 "$attempts"); do
    local cid
    cid="$(compose ps -q "$service" 2>/dev/null || true)"
    if [[ -n "$cid" ]]; then
      local state
      state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || true)"
      if [[ "$state" == "healthy" || "$state" == "running" ]]; then
        return 0
      fi
    fi
    sleep "$POLL_SECS"
  done
  echo "service $service did not become healthy" >&2
  compose ps >&2 || true
  exit 1
}

service_container_id() {
  compose ps -q "$1" 2>/dev/null || true
}

service_state() {
  local cid="$1"
  if [[ -z "$cid" ]]; then
    printf 'missing'
    return 0
  fi
  docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || printf 'missing'
}

assert_service_stopped() {
  local service="$1"
  local cid
  cid="$(service_container_id "$service")"
  local state
  state="$(service_state "$cid")"
  if [[ "$state" == "running" ]]; then
    echo "service ${service} is still running after SIGKILL; CDC takeover proof would not isolate the peer" >&2
    compose ps >&2 || true
    exit 1
  fi
}

assert_service_running_container() {
  local service="$1"
  local expected_cid="$2"
  local cid
  cid="$(service_container_id "$service")"
  if [[ -z "$cid" ]]; then
    echo "service ${service} has no container" >&2
    compose ps >&2 || true
    exit 1
  fi
  if [[ -n "$expected_cid" && "$cid" != "$expected_cid" ]]; then
    echo "service ${service} restarted during CDC takeover (${expected_cid} -> ${cid}); proof requires the original peer worker" >&2
    compose ps >&2 || true
    exit 1
  fi
  local state
  state="$(service_state "$cid")"
  if [[ "$state" != "running" ]]; then
    echo "service ${service} is not running (state=${state})" >&2
    compose ps >&2 || true
    exit 1
  fi
}

new_event_id() {
  psql_scalar "
    WITH digest AS (
      SELECT md5(clock_timestamp()::TEXT || random()::TEXT) AS h
    )
    SELECT (
      substr(h, 1, 8) || '-' ||
      substr(h, 9, 4) || '-4' ||
      substr(h, 14, 3) || '-8' ||
      substr(h, 17, 3) || '-' ||
      substr(h, 20, 12)
    )::UUID::TEXT
    FROM digest;
  "
}

insert_outbox_event() {
  local event_id="$1"
  local label="$2"
  psql_exec "
    INSERT INTO udb_system.outbox_events (event_id, topic, partition_key, payload, created_at)
    VALUES (
      '${event_id}'::UUID,
      '${TOPIC}',
      '${label}',
      jsonb_build_object(
        'event_id', '${event_id}',
        'event_type', '${TOPIC}',
        'correlation_id', '${RUN_ID}',
        'document_id', '${label}',
        'tenant_id', 'ha-cdc-tenant',
        'project_id', 'ha-cdc-project',
        'payload', jsonb_build_object('label', '${label}', 'source_agent', 'ha_cdc_no_duplicate_smoke')
      ),
      NOW()
    );
  " >/dev/null
}

active_cdc_owner() {
  psql_scalar "
    SELECT holder_host
    FROM ${LOCK_RELATION}
    WHERE lock_key = ${CDC_LOCK_KEY}
      AND acquired_at >= NOW() - INTERVAL '45 seconds'
    ORDER BY acquired_at DESC
    LIMIT 1;
  "
}

holder_service_from_owner() {
  local owner="$1"
  case "$owner" in
    udb-ha-a) echo "udb-ha-a" ;;
    udb-ha-b) echo "udb-ha-b" ;;
    *)
      echo "unexpected CDC holder hostname: ${owner}" >&2
      exit 1
      ;;
  esac
}

wait_for_cdc_owner() {
  local label="$1"
  local forbidden_owner="${2:-}"
  local deadline=$((SECONDS + FAILOVER_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    local owner
    owner="$(active_cdc_owner || true)"
    if [[ -n "$owner" && "$owner" != "$forbidden_owner" ]]; then
      echo "$owner"
      return 0
    fi
    sleep "$POLL_SECS"
  done
  echo "timed out waiting for ${label} CDC owner" >&2
  psql_scalar "SELECT lock_key, holder_host, acquired_at FROM ${LOCK_RELATION} ORDER BY acquired_at DESC LIMIT 10;" >&2 || true
  exit 1
}

wait_sql_equals() {
  local label="$1"
  local sql="$2"
  local expected="$3"
  local deadline=$((SECONDS + PUBLISH_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    local actual
    actual="$(psql_scalar "$sql" || true)"
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep "$POLL_SECS"
  done
  echo "timed out waiting for ${label}; expected ${expected}" >&2
  echo "last value: $(psql_scalar "$sql" || true)" >&2
  exit 1
}

wait_journal_count() {
  local event_id="$1"
  local expected="$2"
  wait_sql_equals \
    "CDC journal count ${expected} for ${event_id}" \
    "SELECT COUNT(*) FROM udb_system.udb_cdc_event_journal WHERE event_id = '${event_id}'::UUID AND delivery_state = 'published';" \
    "$expected"
}

wait_outbox_count() {
  local event_id="$1"
  local expected="$2"
  wait_sql_equals \
    "outbox count ${expected} for ${event_id}" \
    "SELECT COUNT(*) FROM udb_system.outbox_events WHERE event_id = '${event_id}'::UUID;" \
    "$expected"
}

kafka_topic() {
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

kafka_event_count() {
  local event_id="$1"
  compose exec -T kafka bash -lc "
    set +e
    /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic '${TOPIC}' \
      --from-beginning \
      --timeout-ms 5000 2>/dev/null \
      | grep -F '${event_id}' | wc -l
  " | tr -d '[:space:]'
}

assert_kafka_event_count() {
  local event_id="$1"
  local expected="$2"
  local actual
  actual="$(kafka_event_count "$event_id")"
  if [[ "$actual" != "$expected" ]]; then
    echo "expected Kafka event_id ${event_id} count ${expected}, got ${actual}" >&2
    exit 1
  fi
}

require_tool docker
require_sql_token UDB_HA_CDC_RUN_ID "$RUN_ID"
require_sql_token UDB_HA_CDC_TOPIC "$TOPIC"
require_integer UDB_HA_CDC_LOCK_KEY "$CDC_LOCK_KEY"

echo "Starting HA CDC no-duplicate stack (${PROJECT})..."
compose --profile broker-ha up -d --build postgres redis kafka qdrant minio udb-ha-a udb-ha-b

wait_healthy postgres
wait_healthy redis
wait_healthy kafka
wait_healthy qdrant
wait_healthy minio
wait_healthy udb-ha-a
wait_healthy udb-ha-b

cleanup_rows
kafka_topic --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null

owner_before="$(wait_for_cdc_owner "initial")"
holder_service="$(holder_service_from_owner "$owner_before")"
peer_service="udb-ha-a"
if [[ "$holder_service" == "udb-ha-a" ]]; then
  peer_service="udb-ha-b"
fi
holder_cid="$(service_container_id "$holder_service")"
peer_cid="$(service_container_id "$peer_service")"
if [[ -z "$holder_cid" || -z "$peer_cid" || "$holder_cid" == "$peer_cid" ]]; then
  echo "HA CDC smoke requires two distinct broker containers (holder=${holder_cid:-missing}, peer=${peer_cid:-missing})" >&2
  compose ps >&2 || true
  exit 1
fi

event_id="$(new_event_id)"
echo "Initial CDC holder: ${owner_before}; event_id=${event_id}"
insert_outbox_event "$event_id" "ha-cdc-${RUN_ID}"
wait_journal_count "$event_id" "1"
wait_outbox_count "$event_id" "0"
assert_kafka_event_count "$event_id" "1"

echo "Killing CDC holder container: ${holder_service}"
compose kill -s KILL "$holder_service" >/dev/null
assert_service_stopped "$holder_service"
assert_service_running_container "$peer_service" "$peer_cid"
owner_after="$(wait_for_cdc_owner "peer takeover" "$owner_before")"
if [[ "$owner_after" != "$peer_service" ]]; then
  echo "expected CDC peer ${peer_service}, got ${owner_after}" >&2
  exit 1
fi
assert_service_stopped "$holder_service"
assert_service_running_container "$peer_service" "$peer_cid"
echo "CDC peer owner: ${owner_after}"

insert_outbox_event "$event_id" "ha-cdc-redelivery-${RUN_ID}"
wait_outbox_count "$event_id" "0"
wait_journal_count "$event_id" "1"
assert_kafka_event_count "$event_id" "1"
assert_service_stopped "$holder_service"
assert_service_running_container "$peer_service" "$peer_cid"

echo "PASS: CDC peer takeover acked redelivered event_id without a duplicate Kafka publish"
