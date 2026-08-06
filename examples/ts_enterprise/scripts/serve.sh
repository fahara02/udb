#!/usr/bin/env bash
# Start the UDB broker in ENTERPRISE mode for the TypeScript example (foreground —
# keep this terminal open). Run ./scripts/bootstrap.sh first; this sources the
# secrets/values it produced.
#
# Enterprise posture (vs the dev quickstart): real RS256 JWT login, server-side
# sessions, the native auth control plane EXPOSED on :50061, and DEFAULT-DENY
# ABAC authorization driven by a seeded policy. NO header-scopes shortcut, NO
# UDB_ABAC_DEFAULT_ALLOW. (TLS is terminated at the edge in this example; add
# UDB_TLS_* / UDB_MTLS_* for in-broker TLS — see docs/enterprise-deployment.md.)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ADDRESS="${1:-127.0.0.1:50051}"

ENV_FILE="$ROOT/secrets/enterprise.env"
[[ -f "$ENV_FILE" ]] || { echo "missing $ENV_FILE — run ./scripts/bootstrap.sh first" >&2; exit 1; }
set -a; # shellcheck disable=SC1090
source "$ENV_FILE"; set +a

if [[ -n "${UDB_CLI:-}" && -x "${UDB_CLI}" ]]; then CLI="$UDB_CLI"
elif command -v udb >/dev/null 2>&1; then CLI="$(command -v udb)"
elif [[ -x "$REPO_ROOT/target/release/udb.exe" ]]; then CLI="$REPO_ROOT/target/release/udb.exe"
elif [[ -x "$REPO_ROOT/target/debug/udb.exe" ]]; then CLI="$REPO_ROOT/target/debug/udb.exe"
else echo "Could not find the UDB CLI (set UDB_CLI)." >&2; exit 1; fi

# Expose the auth control plane (authn/authz/apikey) so the SDK can reach Login
# on a trusted interface. Defaults to loopback-only otherwise.
export UDB_AUTH_GRPC_ADDR="${UDB_AUTH_GRPC_ADDR:-127.0.0.1:50061}"
export UDB_SESSION_ENABLED=true
export UDB_TLS_REQUIRED=false
export UDB_MTLS_REQUIRED=false
export UDB_ALLOW_DEGRADED_BACKENDS=true
export UDB_RATE_LIMIT_ENABLED=false
export UDB_ENCRYPTION_KEY="${UDB_ENCRYPTION_KEY:-00000000000000000000000000000000}"
# DELIBERATELY NOT SET: UDB_ALLOW_HEADER_SCOPES, UDB_ABAC_DEFAULT_ALLOW.
# Authorization is real: JWT scopes + the allow policies bootstrap.sh seeded into
# the udb_authz.policy_rules governance table (Casbin) — not an env override.
export RUST_LOG="${RUST_LOG:-info}"

cd "$ROOT"
echo "serving proto/billing in ENTERPRISE mode (auth plane on $UDB_AUTH_GRPC_ADDR)"
exec "$CLI" serve proto/billing "" "$ADDRESS"
