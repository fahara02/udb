#!/usr/bin/env bash
set -euo pipefail

# Multi-process HA smoke for master-plan 1.1.
#
# Starts two real broker containers over the same Postgres/Kafka/Redis stack and
# proves singleton-worker failover through the durable CDC lock table:
#   1. exactly one broker owns WORKER_PROJECTION_MATERIALIZER,
#   2. killing that broker leaves no split-brain owner,
#   3. the peer takes over the same lock with a higher fencing token.

PROJECT="${UDB_HA_PROJECT:-udb-ha}"
COMPOSE_FILE="${UDB_HA_COMPOSE_FILE:-docker-compose.integration.yml}"
WORKER="${UDB_HA_WORKER:-udb:projection:materializer}"
LOCK_RELATION="${UDB_HA_LOCK_RELATION:-udb_system.udb_cdc_lock_log}"
FAILOVER_TIMEOUT_SECS="${UDB_HA_FAILOVER_TIMEOUT_SECS:-75}"
POLL_SECS="${UDB_HA_POLL_SECS:-2}"
KEEP_STACK="${UDB_HA_KEEP_STACK:-0}"

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "$KEEP_STACK" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 2
  fi
}

require_tool docker

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
    echo "service ${service} is still running after SIGKILL; failover proof would not isolate the peer" >&2
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
    echo "service ${service} restarted during failover (${expected_cid} -> ${cid}); proof requires the original peer worker" >&2
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

psql_scalar() {
  compose exec -T postgres psql -U udb -d udb -At -c "$1"
}

active_owner_row() {
  psql_scalar "
    SELECT holder_host || '|' || fencing_token::TEXT || '|' || lock_key::TEXT
    FROM ${LOCK_RELATION}
    WHERE holder_host LIKE 'udb-ha-%:%:${WORKER}'
      AND acquired_at >= NOW() - INTERVAL '45 seconds'
    ORDER BY acquired_at DESC
    LIMIT 1;
  " | tr -d '\r'
}

wait_for_owner() {
  local label="$1"
  local deadline=$((SECONDS + FAILOVER_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    local row
    row="$(active_owner_row || true)"
    if [[ -n "$row" ]]; then
      echo "$row"
      return 0
    fi
    sleep "$POLL_SECS"
  done
  echo "timed out waiting for ${label} owner of ${WORKER}" >&2
  psql_scalar "SELECT lock_key, holder_host, acquired_at, fencing_token FROM ${LOCK_RELATION} ORDER BY acquired_at DESC LIMIT 10;" >&2 || true
  exit 1
}

holder_service_from_owner() {
  local owner="$1"
  case "$owner" in
    udb-ha-a:*) echo "udb-ha-a" ;;
    udb-ha-b:*) echo "udb-ha-b" ;;
    *)
      echo "unexpected HA owner hostname in holder_host: $owner" >&2
      exit 1
      ;;
  esac
}

echo "Starting multi-process HA stack (${PROJECT})..."
compose --profile broker-ha up -d --build postgres redis kafka qdrant minio udb-ha-a udb-ha-b

wait_healthy postgres
wait_healthy redis
wait_healthy kafka
wait_healthy qdrant
wait_healthy minio
wait_healthy udb-ha-a
wait_healthy udb-ha-b

IFS='|' read -r owner_before token_before lock_key_before < <(wait_for_owner "initial")
holder_service="$(holder_service_from_owner "$owner_before")"
peer_service="udb-ha-a"
if [[ "$holder_service" == "udb-ha-a" ]]; then
  peer_service="udb-ha-b"
fi
holder_cid="$(service_container_id "$holder_service")"
peer_cid="$(service_container_id "$peer_service")"
if [[ -z "$holder_cid" || -z "$peer_cid" || "$holder_cid" == "$peer_cid" ]]; then
  echo "HA smoke requires two distinct broker containers (holder=${holder_cid:-missing}, peer=${peer_cid:-missing})" >&2
  compose ps >&2 || true
  exit 1
fi

echo "Initial ${WORKER} owner: ${owner_before} token=${token_before} lock=${lock_key_before}"
echo "Killing holder container: ${holder_service}"
compose kill -s KILL "$holder_service" >/dev/null
assert_service_stopped "$holder_service"
assert_service_running_container "$peer_service" "$peer_cid"

deadline=$((SECONDS + FAILOVER_TIMEOUT_SECS))
while (( SECONDS < deadline )); do
  row="$(active_owner_row || true)"
  if [[ -n "$row" ]]; then
    IFS='|' read -r owner_after token_after lock_key_after <<< "$row"
    if [[ "$owner_after" == "${peer_service}:"* ]] \
      && [[ "$lock_key_after" == "$lock_key_before" ]] \
      && (( token_after > token_before )); then
      assert_service_stopped "$holder_service"
      assert_service_running_container "$peer_service" "$peer_cid"
      echo "Failover owner: ${owner_after} token=${token_after} lock=${lock_key_after}"
      echo "PASS: ${WORKER} failed over from ${holder_service} to ${peer_service} with a higher fencing token"
      exit 0
    fi
  fi
  sleep "$POLL_SECS"
done

echo "FAIL: ${WORKER} did not fail over from ${holder_service} to ${peer_service} within ${FAILOVER_TIMEOUT_SECS}s" >&2
psql_scalar "SELECT lock_key, holder_host, acquired_at, fencing_token FROM ${LOCK_RELATION} ORDER BY acquired_at DESC LIMIT 10;" >&2 || true
exit 1
