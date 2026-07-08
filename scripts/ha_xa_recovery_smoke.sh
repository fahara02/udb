#!/usr/bin/env bash
set -euo pipefail

# Multi-process XA recovery smoke for master-plan 1.1 / 3.6.
#
# Runs two XA-enabled broker containers over shared Postgres + MySQL, kills one
# broker, then seeds a real in-doubt MySQL XA transaction plus a UDB XA ledger
# commit-intent row. The surviving broker's actual WORKER_XA_RECOVERY loop must
# commit the prepared MySQL transaction and mark the ledger committed.

PROJECT="${UDB_HA_XA_PROJECT:-udb-ha-xa}"
COMPOSE_FILES=(
  "${UDB_HA_XA_INTEGRATION_COMPOSE:-docker-compose.integration.yml}"
  "${UDB_HA_XA_CANONICAL_COMPOSE:-docker-compose.canonical.yml}"
  "${UDB_HA_XA_OVERLAY_COMPOSE:-docker-compose.xa-ha.yml}"
)
RUN_ID="${UDB_HA_XA_RUN_ID:-$(date +%Y%m%d%H%M%S)-$$}"
KILL_SERVICE="${UDB_HA_XA_KILL_SERVICE:-udb-xa-ha-a}"
SURVIVOR_SERVICE="${UDB_HA_XA_SURVIVOR_SERVICE:-udb-xa-ha-b}"
RECOVERY_TIMEOUT_SECS="${UDB_HA_XA_RECOVERY_TIMEOUT_SECS:-90}"
POLL_SECS="${UDB_HA_XA_POLL_SECS:-2}"
KEEP_STACK="${UDB_HA_XA_KEEP_STACK:-0}"

compose() {
  local args=()
  for file in "${COMPOSE_FILES[@]}"; do
    args+=("-f" "$file")
  done
  docker compose -p "$PROJECT" "${args[@]}" "$@"
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 2
  fi
}

require_run_id() {
  if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "UDB_HA_XA_RUN_ID must contain only letters, digits, dot, underscore, or dash: ${RUN_ID}" >&2
    exit 2
  fi
}

sql_suffix() {
  printf '%s' "$RUN_ID" | tr -c 'A-Za-z0-9_' '_' | cut -c 1-40
}

psql_exec() {
  compose exec -T postgres psql -U udb -d udb -v ON_ERROR_STOP=1 -q -c "$1"
}

psql_scalar() {
  compose exec -T postgres psql -U udb -d udb -v ON_ERROR_STOP=1 -qAt -c "$1" | tr -d '\r'
}

mysql_exec() {
  local database="$1"
  local sql="$2"
  compose exec -T mysql mysql -uudb -pudb -h 127.0.0.1 --batch --raw "$database" -e "$sql"
}

mysql_scalar() {
  local database="$1"
  local sql="$2"
  mysql_exec "$database" "$sql" | tail -n +2 | tr -d '\r'
}

mysql_root_exec() {
  local sql="$1"
  compose exec -T mysql mysql -uroot -pudb -h 127.0.0.1 --batch --raw -e "$sql"
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
    echo "service ${service} is still running after SIGKILL; XA recovery proof would not isolate the survivor" >&2
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
    echo "service ${service} restarted during XA recovery (${expected_cid} -> ${cid}); proof requires the original survivor worker" >&2
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

wait_sql_equals() {
  local label="$1"
  local sql="$2"
  local expected="$3"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT_SECS))
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

wait_mysql_equals() {
  local label="$1"
  local database="$2"
  local sql="$3"
  local expected="$4"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    local actual
    actual="$(mysql_scalar "$database" "$sql" || true)"
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep "$POLL_SECS"
  done
  echo "timed out waiting for ${label}; expected ${expected}" >&2
  echo "last value: $(mysql_scalar "$database" "$sql" || true)" >&2
  exit 1
}

cleanup() {
  local suffix="${SAFE_SUFFIX:-}"
  if [[ -n "$suffix" ]]; then
    psql_exec "DROP SCHEMA IF EXISTS udb_xa_pg_${suffix} CASCADE;" >/dev/null 2>&1 || true
    mysql_root_exec "DROP DATABASE IF EXISTS \`udb_xa_mysql_${suffix}\`;" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_STACK" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_tool docker
require_run_id
SAFE_SUFFIX="$(sql_suffix)"
PG_SCHEMA="udb_xa_pg_${SAFE_SUFFIX}"
MYSQL_DB="udb_xa_mysql_${SAFE_SUFFIX}"
ROW_ID="row_${SAFE_SUFFIX}"
XID="udb_xa_${SAFE_SUFFIX}"

if (( ${#XID} > 60 )); then
  echo "derived XA xid is too long for the smoke: ${XID}" >&2
  exit 2
fi

echo "Starting XA HA stack (${PROJECT})..."
compose --profile broker-xa-ha up -d --build \
  postgres redis kafka qdrant minio mysql "$KILL_SERVICE" "$SURVIVOR_SERVICE"

wait_healthy postgres
wait_healthy redis
wait_healthy kafka
wait_healthy qdrant
wait_healthy minio
wait_healthy mysql
wait_healthy "$KILL_SERVICE"
wait_healthy "$SURVIVOR_SERVICE"

KILL_CID="$(service_container_id "$KILL_SERVICE")"
SURVIVOR_CID="$(service_container_id "$SURVIVOR_SERVICE")"
if [[ -z "$KILL_CID" || -z "$SURVIVOR_CID" || "$KILL_CID" == "$SURVIVOR_CID" ]]; then
  echo "XA HA smoke requires two distinct broker containers (kill=${KILL_CID:-missing}, survivor=${SURVIVOR_CID:-missing})" >&2
  compose ps >&2 || true
  exit 1
fi

wait_sql_equals \
  "XA ledger bootstrap" \
  "SELECT (to_regclass('udb_system.udb_xa_ledger') IS NOT NULL)::TEXT;" \
  "true"

echo "Killing one broker before seeding the in-doubt XA row: ${KILL_SERVICE}"
compose kill -s KILL "$KILL_SERVICE" >/dev/null
assert_service_stopped "$KILL_SERVICE"
assert_service_running_container "$SURVIVOR_SERVICE" "$SURVIVOR_CID"

psql_exec "
  CREATE SCHEMA IF NOT EXISTS ${PG_SCHEMA};
  CREATE TABLE IF NOT EXISTS ${PG_SCHEMA}.xa_items (id TEXT PRIMARY KEY, value TEXT NOT NULL);
  INSERT INTO ${PG_SCHEMA}.xa_items (id, value) VALUES ('${ROW_ID}', 'pg-phase2')
  ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value;
" >/dev/null

mysql_root_exec "
  CREATE DATABASE IF NOT EXISTS \`${MYSQL_DB}\`;
  GRANT ALL PRIVILEGES ON \`${MYSQL_DB}\`.* TO 'udb'@'%';
  GRANT XA_RECOVER_ADMIN ON *.* TO 'udb'@'%';
  FLUSH PRIVILEGES;
" >/dev/null
mysql_exec "$MYSQL_DB" "
  CREATE TABLE IF NOT EXISTS xa_items (id VARCHAR(191) PRIMARY KEY, value TEXT NOT NULL);
  XA START '${XID}';
  INSERT INTO xa_items (id, value) VALUES ('${ROW_ID}', 'mysql-phase2')
    ON DUPLICATE KEY UPDATE value = VALUES(value);
  XA END '${XID}';
  XA PREPARE '${XID}';
" >/dev/null

psql_exec "
  INSERT INTO udb_system.udb_xa_ledger
    (xid, tenant_id, project_id, origin_rpc, correlation_id, participants, decision, reason, decided_at, updated_at)
  VALUES
    ('${XID}', 'tenant-a', 'billing', 'scripts/ha_xa_recovery_smoke.sh', '${RUN_ID}',
     '[\"mysql:primary\"]'::JSONB, 'in_doubt', 'commit decided; phase 2 in flight', NOW(), NOW())
  ON CONFLICT (xid) DO UPDATE SET
    participants = EXCLUDED.participants,
    decision = 'in_doubt',
    reason = EXCLUDED.reason,
    recovery_attempts = 0,
    updated_at = NOW();
" >/dev/null

echo "Waiting for surviving broker ${SURVIVOR_SERVICE} to drive XA recovery for ${XID}..."
assert_service_stopped "$KILL_SERVICE"
assert_service_running_container "$SURVIVOR_SERVICE" "$SURVIVOR_CID"
wait_sql_equals \
  "ledger committed for ${XID}" \
  "SELECT decision FROM udb_system.udb_xa_ledger WHERE xid = '${XID}';" \
  "committed"
wait_mysql_equals \
  "MySQL committed row for ${XID}" \
  "$MYSQL_DB" \
  "SELECT COALESCE((SELECT value FROM xa_items WHERE id = '${ROW_ID}'), '');" \
  "mysql-phase2"

if mysql_exec "$MYSQL_DB" "XA RECOVER;" | grep -Fq "$XID"; then
  echo "XA xid ${XID} is still prepared after broker recovery" >&2
  exit 1
fi
assert_service_stopped "$KILL_SERVICE"
assert_service_running_container "$SURVIVOR_SERVICE" "$SURVIVOR_CID"

echo "PASS: surviving broker drove in-doubt MySQL XA transaction to committed"
