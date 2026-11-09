#!/usr/bin/env bash
# One-time setup for the docker + nginx example. Run from anywhere:
#
#   ./scripts/setup.sh
#
#   1. certs.sh                         self-signed CA + server certificate for nginx
#   2. deploy/scripts/init.sh <here>    .env, generated secrets, RS256 key pair
#   3. deploy/scripts/bootstrap.sh <here>
#                                       first admin (admin.env) + data policies
#
# Re-running is safe: each step keeps what already exists. UDB_ADMIN_USER,
# UDB_ADMIN_PASSWORD and UDB_TENANT pass through to bootstrap.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_SCRIPTS="$(cd "$ROOT/../../deploy/scripts" && pwd)"

"$SCRIPT_DIR/certs.sh"
"$DEPLOY_SCRIPTS/init.sh" "$ROOT"
"$DEPLOY_SCRIPTS/bootstrap.sh" "$ROOT"

echo
echo "setup: done."
echo "next:  docker compose up -d --wait"
echo "then:  docker compose --profile client run --rm client"
