#!/usr/bin/env bash
# Auth-plane load test (Phase 6 / plan §"Add load tests for: login, validate
# token, refresh token, authorize, batch authorize").
#
# Drives the native auth control-plane gRPC listener with `ghz` to measure
# throughput and tail latency of the hot auth RPCs. Standalone — needs only `ghz`
# and the in-repo proto tree; it does NOT depend on the Rust build.
#
# Install ghz:  https://ghz.sh/docs/install  (e.g. `go install github.com/bojand/ghz/cmd/ghz@latest`)
#
# Usage:
#   AUTH_TARGET=127.0.0.1:50061 TOKEN="<admin-bearer-jwt>" TENANT=acme \
#     scripts/auth-load-test.sh
#
# Env knobs (all optional):
#   AUTH_TARGET   auth control-plane addr (default 127.0.0.1:50061 — public port+10)
#   TOKEN         admin/control-plane bearer JWT (required for non-public RPCs)
#   TENANT        tenant id for x-tenant-id (default acme)
#   USERNAME/PASSWORD  credentials for the Login RPC bench (default loadtest/...)
#   SESSION_TOKEN api-key/session token to bench ValidateToken (optional)
#   REFRESH_TOKEN token-family refresh token (rt_…) to bench RefreshToken (optional)
#   CONCURRENCY   concurrent workers (default 50)
#   TOTAL         total requests per RPC (default 5000)
#   INSECURE      "1" for plaintext (default 1; set 0 to use TLS)
#
# Each RPC's run prints ghz's summary (RPS + p50/p90/p95/p99). Treat the numbers
# as relative regression signals against `bench-history/` conventions, not
# absolutes — they depend on the host and the backing Postgres/Redis.
set -euo pipefail

AUTH_TARGET="${AUTH_TARGET:-127.0.0.1:50061}"
TENANT="${TENANT:-acme}"
TOKEN="${TOKEN:-}"
USERNAME="${USERNAME:-loadtest}"
PASSWORD="${PASSWORD:-CorrectHorse1!}"
SESSION_TOKEN="${SESSION_TOKEN:-}"
REFRESH_TOKEN="${REFRESH_TOKEN:-}"
CONCURRENCY="${CONCURRENCY:-50}"
TOTAL="${TOTAL:-5000}"
INSECURE="${INSECURE:-1}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
proto_root="$repo_root/proto"
third_party="$repo_root/third_party/googleapis"

command -v ghz >/dev/null 2>&1 || { echo "ERROR: ghz not found on PATH — see https://ghz.sh/docs/install"; exit 127; }
[ -d "$proto_root/udb" ] || { echo "ERROR: proto tree not found at $proto_root/udb"; exit 1; }

ghz_flags=(--concurrency "$CONCURRENCY" --total "$TOTAL"
  --import-paths "$proto_root,$third_party")
[ "$INSECURE" = "1" ] && ghz_flags+=(--insecure)
[ -n "$TOKEN" ] && ghz_flags+=(--metadata "{\"authorization\":\"Bearer $TOKEN\",\"x-tenant-id\":\"$TENANT\"}")

run() { # run <label> <proto-file> <fully.qualified/Method> <json-data>
  local label="$1" proto="$2" call="$3" data="$4"
  echo; echo "===== $label  ($call) ====="
  ghz "${ghz_flags[@]}" \
    --proto "$proto_root/$proto" \
    --call "$call" \
    --data "$data" \
    "$AUTH_TARGET" || echo "  (run failed — check target/token/proto)"
}

AUTHN_PROTO="udb/core/authn/services/v1/authn_service.proto"
AUTHZ_PROTO="udb/core/authz/services/v1/authz_service.proto"

# 1. Login (public bootstrap; password → session/JWT). Highest-cost path (Argon2id).
run "Login" "$AUTHN_PROTO" "udb.core.authn.services.v1.AuthnService.Login" \
  "{\"username\":\"$USERNAME\",\"password\":\"$PASSWORD\"}"

# 2. ValidateToken (hot per-request path). Benches a session/api-key token if given.
if [ -n "$SESSION_TOKEN" ]; then
  run "ValidateToken" "$AUTHN_PROTO" "udb.core.authn.services.v1.AuthnService.ValidateToken" \
    "{\"token\":\"$SESSION_TOKEN\",\"token_type\":\"TOKEN_TYPE_SESSION\"}"
fi

# 3. RefreshToken (token-family rotation — a WRITE on the hot session path).
#    Each call rotates the family, so a single rt_… is only valid once; bench it
#    with a low TOTAL or expect rotated-away tokens to be rejected after the first
#    success (the point is to measure the rotation write path under load). Supply a
#    fresh rt_… via REFRESH_TOKEN (or session id via REFRESH_SESSION_ID).
if [ -n "$REFRESH_TOKEN" ]; then
  run "RefreshToken" "$AUTHN_PROTO" "udb.core.authn.services.v1.AuthnService.RefreshToken" \
    "{\"refresh_token\":\"$REFRESH_TOKEN\"}"
elif [ -n "${REFRESH_SESSION_ID:-}" ]; then
  run "RefreshToken" "$AUTHN_PROTO" "udb.core.authn.services.v1.AuthnService.RefreshToken" \
    "{\"session_id\":\"$REFRESH_SESSION_ID\"}"
fi

# 4. GetJwks (public, cacheable — sanity throughput).
run "GetJwks" "$AUTHN_PROTO" "udb.core.authn.services.v1.AuthnService.GetJwks" "{}"

# 5. Authorize (single decision — the authz hot path).
run "Authorize" "$AUTHZ_PROTO" "udb.core.authz.services.v1.AuthzService.Authorize" \
  "{\"principal\":{\"subject\":\"$USERNAME\",\"tenant_id\":\"$TENANT\"},\"resource\":{\"resource_type\":\"storage.file\"},\"action\":\"object.read\"}"

# 6. BatchCheckPermissions (batch authz).
run "BatchCheckPermissions" "$AUTHZ_PROTO" "udb.core.authz.services.v1.AuthzService.BatchCheckPermissions" \
  "{\"user_id\":\"$USERNAME\",\"tenant_id\":\"$TENANT\",\"checks\":[{\"object\":\"storage.file\",\"action\":\"object.read\"},{\"object\":\"storage.file\",\"action\":\"object.write\"}]}"

echo; echo "Done. Compare RPS/p99 against prior runs; record notable shifts in bench-history/ per scripts/bench_snapshot.py conventions."
