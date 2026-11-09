#!/usr/bin/env bash
# Changing a model that already has data, run for real: create a table, put rows in
# it, then add a column, hold a change until it is approved, remove a column, and
# add a NOT NULL column, checking the broker and the database at every step.
#
#   UDB_CLI=/path/to/udb UDB_PG_DSN=postgres://... ./walkthrough.sh
#
# It needs an empty PostgreSQL database and a `udb` binary. Every step prints what
# it did and stops on the first result that differs from what the guide says.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UDB="${UDB_CLI:-udb}"
: "${UDB_PG_DSN:?set UDB_PG_DSN to an empty PostgreSQL database}"
ADDR="${UDB_WALKTHROUGH_ADDR:-127.0.0.1:55051}"
WORK="$(mktemp -d)"
# The work directory (protos, manifests, plans, broker logs) is kept when a step fails.
trap 'rc=$?; kill "${BROKER:-0}" 2>/dev/null || true; if [ $rc -eq 0 ]; then rm -rf "$WORK"; else echo "logs kept in $WORK" >&2; fi' EXIT

export UDB_PG_DSN
export UDB_ENCRYPTION_KEY="${UDB_ENCRYPTION_KEY:-$(openssl rand -base64 32)}"
export UDB_TLS_REQUIRED=false UDB_MTLS_REQUIRED=false UDB_ALLOW_DEGRADED_BACKENDS=true
export UDB_CDC_ENABLED=false UDB_RATE_LIMIT_ENABLED=false
# Its own ports, whatever the environment says, so it can run beside another broker.
export UDB_GRPC_ADDR="$ADDR"
export UDB_AUTH_GRPC_ADDR="${UDB_WALKTHROUGH_AUTH_ADDR:-127.0.0.1:55061}"
export UDB_METRICS_ADDR="${UDB_WALKTHROUGH_METRICS_ADDR:-127.0.0.1:55052}"

step() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
# PSQL overrides the client, e.g. PSQL="docker exec -i my-pg psql -U udb -d udb".
sql() {
  if [ -n "${PSQL:-}" ]; then $PSQL -tAc "$1"; else psql "$UDB_PG_DSN" -tAc "$1"; fi
}

# Put proto version $1 (v1, v2, ...) in place as the project's proto tree.
use_protos() {
  rm -rf "$WORK/proto"
  mkdir -p "$WORK/proto/shop/v1"
  cp "$HERE/proto/$1/orders.proto" "$WORK/proto/shop/v1/orders.proto"
}

# Start the broker on the current protos and wait until it is ready or has exited.
# Prints the broker log path; returns 0 when ready, 1 when it refused to start.
start_broker() {
  local log="$WORK/broker-$1.log"
  "$UDB" serve "$WORK/proto" "" "$ADDR" >"$log" 2>&1 &
  BROKER=$!
  for _ in $(seq 1 300); do
    if grep -q "DataBroker is ready" "$log"; then echo "$log"; return 0; fi
    if ! kill -0 "$BROKER" 2>/dev/null; then echo "$log"; return 1; fi
    sleep 1
  done
  fail "the broker neither started nor refused within 300s (see $log)"
}
stop_broker() { kill "$BROKER" 2>/dev/null || true; wait "$BROKER" 2>/dev/null || true; }

step "1. First start: the broker creates shop.orders from the protos"
use_protos v1
UDB_MIGRATE_ENABLED=true start_broker first >/dev/null || fail "first start refused"
stop_broker
[ "$(sql "SELECT to_regclass('shop.orders') IS NOT NULL")" = t ] || fail "shop.orders was not created"
echo "shop.orders exists"

step "2. Your application writes rows (here, straight into the table)"
sql "INSERT INTO shop.orders (order_id, tenant_id, status) VALUES
     (gen_random_uuid(), gen_random_uuid(), 'open'),
     (gen_random_uuid(), gen_random_uuid(), 'paid')" >/dev/null
[ "$(sql "SELECT count(*) FROM shop.orders")" = 2 ] || fail "rows were not written"
echo "2 rows"

step "3. Save the manifest of what is deployed, before you change anything"
UDB_MANIFEST_EXPORT_PATH="$WORK/prior.json" "$UDB" manifest-export "$WORK/proto" >/dev/null
[ -s "$WORK/prior.json" ] || fail "manifest-export wrote nothing"
echo "prior manifest saved"

# `udb drift` and `udb plan` read the protos under ./proto, so run them where the current
# version is.
drift() { (cd "$WORK" && "$UDB" drift --prior "$WORK/prior.json"); }
# `udb drift` exits 1 when it finds drift, so keep its report and print the summary.
drift_report() { drift >"$1" 2>&1 || true; grep -m1 "^drift" "$1"; }
plan() { (cd "$WORK" && "$UDB" plan --prior "$WORK/prior.json" --emit-approval-plan "$1"); }
save_prior() { UDB_MANIFEST_EXPORT_PATH="$WORK/prior.json" "$UDB" manifest-export "$WORK/proto" >/dev/null; }
column_count() {
  sql "SELECT count(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='orders' AND column_name='$1'"
}

step "4. Add a nullable column (v2) and see what it will do"
use_protos v2
set +e; "$UDB" verify --live --dsn "$UDB_PG_DSN" >"$WORK/verify-v2.txt" 2>&1; rc=$?; set -e
[ "$rc" -ne 0 ] || fail "verify --live should report the column the protos add"
echo "verify --live exit $rc: the database lacks the new column"
drift_report "$WORK/drift-v2.txt"
grep -q '"auto_safe_count": 1' "$WORK/drift-v2.txt" || fail "drift should count one auto-safe change"

step "5. With UDB_MIGRATE_ENABLED=false a start refuses to apply it"
if UDB_MIGRATE_ENABLED=false start_broker held >/dev/null; then
  stop_broker; fail "the start applied a held change"
fi
grep -o "UDB_MIGRATE_ENABLED=false and the protos change the project's schema[^.]*" \
  "$WORK/broker-held.log" | head -1
[ "$(column_count customer_note)" = 0 ] || fail "the column was added although the start refused"

step "6. Approve it: emit a plan and start with it"
plan "$WORK/plan-v2.json" >/dev/null
# The plan names each operation with its safety and whether it can lose data.
grep -q '"kind": "AddColumn"' "$WORK/plan-v2.json" || fail "the plan should hold AddColumn"
grep -q '"data_destructive": false' "$WORK/plan-v2.json" || fail "AddColumn is not destructive"
UDB_MIGRATE_ENABLED=false UDB_MIGRATION_APPROVAL_PLAN="$WORK/plan-v2.json" start_broker approved >/dev/null \
  || fail "the approved start refused (see $WORK/broker-approved.log)"
stop_broker
"$UDB" verify --live --dsn "$UDB_PG_DSN" >/dev/null || fail "verify --live still finds drift after the apply"
[ "$(column_count customer_note)" = 1 ] || fail "customer_note was not added"
[ "$(sql "SELECT count(*) FROM shop.orders")" = 2 ] || fail "rows changed"
echo "customer_note added, rows intact, verify --live clean"
save_prior

step "7. Delete the field without saying the data may go (v3-blocked): refused"
use_protos v3-blocked
drift_report "$WORK/drift-v3-blocked.txt"
grep -q '"blocked_count": 1' "$WORK/drift-v3-blocked.txt" || fail "the drop should be Blocked"
plan "$WORK/plan-v3-blocked.json" >/dev/null 2>&1 || true
if UDB_MIGRATE_ENABLED=true start_broker blocked >/dev/null; then
  stop_broker; fail "a Blocked drop was applied"
fi
grep -m1 -o "DropColumn shop.orders(customer_note) \[Blocked[^]]*\]" "$WORK/broker-blocked.log" || true
[ "$(column_count customer_note)" = 1 ] || fail "the column was dropped although the change is Blocked"
echo "refused; customer_note and its values are still there"

step "8. Delete it on purpose (v3: reserved number and name, allow_drop on the table)"
use_protos v3
drift_report "$WORK/drift-v3.txt"
grep -q '"blocked_count": 0' "$WORK/drift-v3.txt" || fail "allow_drop should unblock the drop"
plan "$WORK/plan-v3.json" >/dev/null
grep -q '"data_destructive": true' "$WORK/plan-v3.json" || fail "the plan should mark the drop data_destructive"
if UDB_MIGRATE_ENABLED=false start_broker drop-held >/dev/null; then
  stop_broker; fail "UDB_MIGRATE_ENABLED=false applied the drop without the plan"
fi
UDB_MIGRATE_ENABLED=false UDB_MIGRATION_APPROVAL_PLAN="$WORK/plan-v3.json" start_broker drop >/dev/null \
  || fail "the approved drop refused (see $WORK/broker-drop.log)"
stop_broker
[ "$(column_count customer_note)" = 0 ] || fail "the approved drop left the column"
[ "$(sql "SELECT count(*) FROM shop.orders")" = 2 ] || fail "rows changed"
echo "customer_note dropped by the approved plan; rows intact"
save_prior

step "9. Remove allow_drop again and add a NOT NULL column with a default (v4)"
use_protos v4
drift_report "$WORK/drift-v4.txt"
UDB_MIGRATE_ENABLED=true start_broker notnull >/dev/null \
  || fail "the NOT NULL column with a default refused (see $WORK/broker-notnull.log)"
stop_broker
[ "$(sql "SELECT count(*) FROM shop.orders WHERE region = 'unknown'")" = 2 ] || fail "existing rows did not get the default"
echo "region added; both existing rows hold 'unknown'"
save_prior

step "10. Rename a column and keep its values (v5: previous_column_name)"
use_protos v5
drift_report "$WORK/drift-v5.txt"
UDB_MIGRATE_ENABLED=true start_broker rename >/dev/null \
  || fail "the rename refused (see $WORK/broker-rename.log)"
stop_broker
[ "$(column_count status)" = 0 ] && [ "$(column_count state)" = 1 ] || fail "status was not renamed to state"
[ "$(sql "SELECT string_agg(state, ',' ORDER BY state) FROM shop.orders")" = "open,paid" ] \
  || fail "the renamed column lost its values"
echo "status renamed to state; values kept"

step "Done: every step behaved as described"
