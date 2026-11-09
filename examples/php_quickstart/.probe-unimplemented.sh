#!/bin/bash
# Enumerate every RPC the broker exposes (via gRPC reflection) and call each
# with an empty body + the standard UDB metadata headers, recording the gRPC
# status code. Code 12 (Unimplemented) == not wired. Anything else == reached a
# handler (implemented).
set -uo pipefail
ADDR="${1:-127.0.0.1:50051}"
H=(-H "x-tenant-id: quickstart" -H "x-user-id: probe" -H "x-purpose: probe"
   -H "x-correlation-id: probe" -H "x-scopes: udb:read,udb:write,udb:admin"
   -H "x-service-identity: probe" -H "x-udb-project-id: default"
   -H "x-udb-client-catalog-version: 1.0.0")

services=$(grpcurl -plaintext "$ADDR" list | grep -vE "grpc.reflection|grpc.health")
for svc in $services; do
  methods=$(grpcurl -plaintext "$ADDR" list "$svc" 2>/dev/null)
  for m in $methods; do
    out=$(grpcurl -plaintext -max-time 10 "${H[@]}" -d '{}' "$ADDR" "$m" 2>&1)
    if echo "$out" | grep -qiE "Code: Unimplemented|code = Unimplemented|status: Unimplemented"; then
      echo "UNIMPLEMENTED	$m"
    else
      code=$(echo "$out" | grep -oiE "Code: [A-Za-z]+" | head -1)
      echo "ok(${code:-OK})	$m"
    fi
  done
done
