#!/usr/bin/env bash
set -euo pipefail

# CDC live fault smoke for master-plan 1.4.
#
# This is a real Docker/process/network fault rig, not a unit double:
#   1. kill Kafka while an outbox row is pending and prove the row is not acked,
#   2. restart Kafka and prove the same row reaches the CDC journal,
#   3. disconnect the broker from the compose network, insert another outbox row,
#   4. reconnect the broker and prove the tailer restarts and journals the row.
#
# The compose project is scoped by UDB_CDC_FAULT_PROJECT, so cleanup only touches
# containers/networks/volumes owned by this smoke.

PROJECT="${UDB_CDC_FAULT_PROJECT:-udb-cdc-fault}"
COMPOSE_FILE="${UDB_CDC_FAULT_COMPOSE_FILE:-docker-compose.integration.yml}"
BROKER_SERVICE="${UDB_CDC_FAULT_BROKER_SERVICE:-udb}"
POSTGRES_SERVICE="${UDB_CDC_FAULT_POSTGRES_SERVICE:-postgres}"
KAFKA_SERVICE="${UDB_CDC_FAULT_KAFKA_SERVICE:-kafka}"
REDIS_SERVICE="${UDB_CDC_FAULT_REDIS_SERVICE:-redis}"
QDRANT_SERVICE="${UDB_CDC_FAULT_QDRANT_SERVICE:-qdrant}"
MINIO_SERVICE="${UDB_CDC_FAULT_MINIO_SERVICE:-minio}"
COMPOSE_NETWORK="${UDB_CDC_FAULT_NETWORK:-${PROJECT}_default}"
TOPIC="${UDB_CDC_FAULT_TOPIC:-udb.cdc.fault.v1}"
RUN_ID="${UDB_CDC_FAULT_RUN_ID:-$(date +%Y%m%d%H%M%S)-$$}"
FAULT_TIMEOUT_SECS="${UDB_CDC_FAULT_TIMEOUT_SECS:-120}"
POLL_SECS="${UDB_CDC_FAULT_POLL_SECS:-2}"
KEEP_STACK="${UDB_CDC_FAULT_KEEP_STACK:-0}"

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
  if [[ ! "$value" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "${name} must contain only letters, digits, dot, underscore, or dash: ${value}" >&2
    exit 2
  fi
}

cleanup_rows() {
  psql_exec "
    DELETE FROM udb_system.udb_cdc_event_journal WHERE topic = '${TOPIC}' AND payload->>'correlation_id' = '${RUN_ID}';
    DELETE FROM udb_system.outbox_events WHERE topic = '${TOPIC}' AND payload->>'correlation_id' = '${RUN_ID}';
  " >/dev/null 2>&1 || true
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
    echo "service ${service} is still running after requested fault injection" >&2
    compose ps >&2 || true
    exit 1
  fi
}

assert_service_running() {
  local service="$1"
  local cid
  cid="$(service_container_id "$service")"
  local state
  state="$(service_state "$cid")"
  if [[ "$state" != "running" ]]; then
    echo "service ${service} is not running (state=${state})" >&2
    compose ps >&2 || true
    exit 1
  fi
}

container_has_network() {
  local cid="$1"
  [[ -n "$cid" ]] && docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$cid" 2>/dev/null \
    | grep -Fxq "$COMPOSE_NETWORK"
}

assert_broker_network_attached() {
  local cid
  cid="$(service_container_id "$BROKER_SERVICE")"
  if ! container_has_network "$cid"; then
    echo "broker ${BROKER_SERVICE} is not attached to ${COMPOSE_NETWORK}" >&2
    docker inspect "$cid" >&2 || true
    exit 1
  fi
}

assert_broker_network_detached() {
  local cid
  cid="$(service_container_id "$BROKER_SERVICE")"
  if container_has_network "$cid"; then
    echo "broker ${BROKER_SERVICE} is still attached to ${COMPOSE_NETWORK} after network fault injection" >&2
    docker inspect "$cid" >&2 || true
    exit 1
  fi
}

reconnect_broker() {
  local cid
  cid="$(service_container_id "$BROKER_SERVICE")"
  if [[ -n "$cid" ]]; then
    docker network connect "$COMPOSE_NETWORK" "$cid" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  reconnect_broker
  cleanup_rows
  if [[ "$KEEP_STACK" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

psql_exec() {
  compose exec -T "$POSTGRES_SERVICE" psql -U udb -d udb -v ON_ERROR_STOP=1 -q -c "$1"
}

psql_scalar() {
  compose exec -T "$POSTGRES_SERVICE" psql -U udb -d udb -v ON_ERROR_STOP=1 -qAt -c "$1" | tr -d '\r'
}

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

wait_sql_equals() {
  local label="$1"
  local sql="$2"
  local expected="$3"
  local deadline=$((SECONDS + FAULT_TIMEOUT_SECS))
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

wait_journaled() {
  local event_id="$1"
  wait_sql_equals \
    "journal row for ${event_id}" \
    "SELECT COUNT(*) FROM udb_system.udb_cdc_event_journal WHERE event_id = '${event_id}'::UUID AND delivery_state = 'published';" \
    "1"
}

assert_outbox_pending() {
  local event_id="$1"
  wait_sql_equals \
    "pending outbox row for ${event_id}" \
    "SELECT COALESCE(MAX(delivery_state), '') FROM udb_system.outbox_events WHERE event_id = '${event_id}'::UUID;" \
    "pending"
  wait_sql_equals \
    "no premature journal row for ${event_id}" \
    "SELECT COUNT(*) FROM udb_system.udb_cdc_event_journal WHERE event_id = '${event_id}'::UUID;" \
    "0"
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
        'tenant_id', 'fault-tenant',
        'project_id', 'fault-project',
        'payload', jsonb_build_object('label', '${label}', 'source_agent', 'cdc_fault_smoke')
      ),
      NOW()
    );
  " >/dev/null
}

disconnect_broker_network() {
  local cid
  cid="$(service_container_id "$BROKER_SERVICE")"
  docker network disconnect "$COMPOSE_NETWORK" "$cid"
}

assert_broker_running() {
  local cid
  cid="$(service_container_id "$BROKER_SERVICE")"
  local state
  state="$(service_state "$cid")"
  if [[ "$state" != "running" ]]; then
    echo "broker container exited during network fault: ${state}" >&2
    compose logs --tail=120 "$BROKER_SERVICE" >&2 || true
    exit 1
  fi
}

require_tool docker
require_sql_token UDB_CDC_FAULT_TOPIC "$TOPIC"
require_sql_token UDB_CDC_FAULT_RUN_ID "$RUN_ID"

echo "Starting CDC fault stack (${PROJECT})..."
compose --profile broker up -d --build \
  "$POSTGRES_SERVICE" "$REDIS_SERVICE" "$KAFKA_SERVICE" "$QDRANT_SERVICE" "$MINIO_SERVICE" "$BROKER_SERVICE"

wait_healthy "$POSTGRES_SERVICE"
wait_healthy "$REDIS_SERVICE"
wait_healthy "$KAFKA_SERVICE"
wait_healthy "$QDRANT_SERVICE"
wait_healthy "$MINIO_SERVICE"
wait_healthy "$BROKER_SERVICE"
assert_broker_network_attached

cleanup_rows

echo "Fault 1: kill Kafka, enqueue CDC outbox row, prove it stays pending..."
compose kill -s KILL "$KAFKA_SERVICE" >/dev/null
assert_service_stopped "$KAFKA_SERVICE"
kafka_fault_event="$(new_event_id)"
insert_outbox_event "$kafka_fault_event" "kafka-kill-${RUN_ID}"
sleep "$POLL_SECS"
assert_outbox_pending "$kafka_fault_event"

echo "Restarting Kafka and waiting for the same event to publish..."
compose start "$KAFKA_SERVICE" >/dev/null
wait_healthy "$KAFKA_SERVICE"
assert_service_running "$KAFKA_SERVICE"
wait_journaled "$kafka_fault_event"

echo "Fault 2: disconnect broker from the store/network, enqueue row, then reconnect..."
network_fault_event="$(new_event_id)"
disconnect_broker_network
assert_broker_network_detached
insert_outbox_event "$network_fault_event" "network-drop-${RUN_ID}"
sleep "$POLL_SECS"
assert_broker_running
assert_outbox_pending "$network_fault_event"
reconnect_broker
assert_broker_network_attached
wait_healthy "$BROKER_SERVICE"
wait_journaled "$network_fault_event"

echo "PASS: CDC Kafka-kill and broker-store network-drop faults preserved pending rows and recovered to published journal rows"
