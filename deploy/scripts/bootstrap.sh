#!/usr/bin/env bash
# First admin user + data-plane policies for a UDB compose deployment.
# Run once, after ./scripts/init.sh and before the first `docker compose up`.
#
#   ./scripts/bootstrap.sh [deployment-dir]
#
# Settings (environment, all optional):
#   UDB_ADMIN_USER      admin username            (default: admin)
#   UDB_ADMIN_EMAIL     admin email               (default: <user>@example.com)
#   UDB_ADMIN_PASSWORD  admin password            (default: generated)
#   UDB_TENANT          human tenant code         (default: acme)
#   UDB_PROJECT         project id                (default: default)
#
# Steps, all offline against Postgres through the pinned broker image:
#   1. `udb auth bootstrap user`: creates the tenant and the first user, bound
#      to organization_owner (udb:* scopes on login). Prints the canonical
#      tenant UUID.
#   2. `udb policy apply`: writes allow rules for Select / Upsert / Delete in
#      that tenant into udb_authz.policy_rules. Data-plane authorization is
#      default-deny: without these rules every data RPC is PERMISSION_DENIED,
#      even for the organization owner. Edit policies/<tenant-uuid>.json for
#      least privilege and re-run step 2 (README).
#
# Writes admin.env (owner-only, git-ignored): UDB_USER, UDB_PASS, UDB_TENANT,
# UDB_TENANT_ID. A second run finds admin.env, skips step 1 and re-applies the
# policies.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR="$(cd "${1:-$SCRIPT_DIR/..}" && pwd)"
cd "$DIR"

die() { echo "bootstrap: $*" >&2; exit 1; }
rand_hex() { od -An -N32 -tx1 /dev/urandom | tr -d ' \n'; }

[[ -f .env ]] || die "no .env in $DIR; run ./scripts/init.sh first"
grep -qE '^UDB_JWT_PRIVATE_KEY=.+' .env || die ".env has no generated secrets; run ./scripts/init.sh first"

ADMIN_USER="${UDB_ADMIN_USER:-admin}"
ADMIN_EMAIL="${UDB_ADMIN_EMAIL:-${ADMIN_USER}@example.com}"
TENANT_CODE="${UDB_TENANT:-acme}"
PROJECT="${UDB_PROJECT:-default}"

echo "bootstrap: starting postgres"
docker compose up -d --wait postgres

if [[ -f admin.env ]]; then
  echo "bootstrap: admin.env exists; skipping user creation"
  TENANT_ID="$(grep -E '^UDB_TENANT_ID=' admin.env | tail -n 1 | cut -d= -f2-)"
  [[ -n "$TENANT_ID" ]] || die "admin.env has no UDB_TENANT_ID"
else
  # Upper, lower, digit and symbol, so a password policy that wants all four accepts it.
  ADMIN_PASS="${UDB_ADMIN_PASSWORD:-Udb-$(rand_hex | cut -c1-24)-9x!}"

  echo "bootstrap: creating admin '$ADMIN_USER' in tenant '$TENANT_CODE'"
  if ! BOOT_OUT="$(docker compose run --rm -T udb auth bootstrap user \
      --username "$ADMIN_USER" --email "$ADMIN_EMAIL" \
      --password "$ADMIN_PASS" --tenant "$TENANT_CODE" --project "$PROJECT" 2>&1)"; then
    echo "$BOOT_OUT" >&2
    die "udb auth bootstrap user failed (output above)"
  fi
  echo "$BOOT_OUT"
  TENANT_ID="$(printf '%s' "$BOOT_OUT" \
    | grep -oE '"tenant_id"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n 1 \
    | sed -E 's/.*"tenant_id"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  [[ -n "$TENANT_ID" ]] || die "could not read the canonical tenant_id from the output above"

  umask 077
  {
    echo "# Written by scripts/bootstrap.sh. Secret: do not commit."
    echo "UDB_USER=$ADMIN_USER"
    echo "UDB_PASS=$ADMIN_PASS"
    echo "UDB_TENANT=$TENANT_CODE"
    echo "UDB_TENANT_ID=$TENANT_ID"
    echo "UDB_PROJECT=$PROJECT"
  } > admin.env
  umask 022
  echo "bootstrap: wrote admin.env (admin password inside)"
fi
echo "bootstrap: canonical tenant UUID $TENANT_ID"

# Data-plane policies. The action names are the operations the broker submits
# (Select / Upsert / Delete), not aliases; required_scopes split read and write.
mkdir -p policies
POLICY_FILE="policies/${TENANT_ID}.json"
if [[ ! -f "$POLICY_FILE" ]]; then
  cat > "$POLICY_FILE" <<JSON
[
  {"effect": "allow", "subject": "*", "tenant": "${TENANT_ID}", "resource": "*", "action": "Select", "required_scopes": ["udb:read"]},
  {"effect": "allow", "subject": "*", "tenant": "${TENANT_ID}", "resource": "*", "action": "Upsert", "required_scopes": ["udb:write"]},
  {"effect": "allow", "subject": "*", "tenant": "${TENANT_ID}", "resource": "*", "action": "Delete", "required_scopes": ["udb:write"]}
]
JSON
  echo "bootstrap: wrote $POLICY_FILE"
fi

echo "bootstrap: applying $POLICY_FILE"
if ! APPLY_OUT="$(docker compose run --rm -T \
    -v "$DIR/policies:/etc/udb/policies:ro" \
    udb policy apply -f "/etc/udb/policies/${TENANT_ID}.json" --tenant "$TENANT_ID" 2>&1)"; then
  echo "$APPLY_OUT" >&2
  die "udb policy apply failed (output above)"
fi
echo "$APPLY_OUT"

echo
echo "bootstrap: done. Admin credentials are in $DIR/admin.env."
echo "next: docker compose up -d --wait"
