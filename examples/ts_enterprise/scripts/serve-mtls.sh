#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
set -a; source "$ROOT/secrets/enterprise.env"; set +a
TLS="$ROOT/secrets/tls"
export UDB_TLS_REQUIRED=true UDB_MTLS_REQUIRED=true
export UDB_TLS_CERT_PEM="$(cat "$TLS/server.pem")"
export UDB_TLS_KEY_PEM="$(cat "$TLS/server.key")"
export UDB_MTLS_CLIENT_CA_PEM="$(cat "$TLS/ca.pem")"
export UDB_AUTH_GRPC_ADDR=127.0.0.1:50061 UDB_SESSION_ENABLED=true
export UDB_ALLOW_DEGRADED_BACKENDS=true UDB_RATE_LIMIT_ENABLED=false
export RUST_LOG=info
cd "$ROOT"
exec "${UDB_CLI:-udb}" serve proto/billing "" 127.0.0.1:50051
