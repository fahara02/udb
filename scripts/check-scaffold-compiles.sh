#!/usr/bin/env bash
# Compile-test the example clients emitted by `udb scaffold` (urgent_fix #21).
#
# The scaffold used to emit a Go client with a fictional import path
# (github.com/udb-project/...) and the deprecated grpc.Dial — neither was ever
# compiled, so the rot went unnoticed. This script generates a fresh scaffold and
# actually builds the Go example against the in-repo Go SDK module, and
# type-checks the TypeScript example, so a broken snippet fails CI.
#
# Usage:  scripts/check-scaffold-compiles.sh
# Env:    UDB_BIN  path to a prebuilt udb binary (else `cargo run -- scaffold`,
#                  which is binary-name-agnostic since the crate ships one bin)
#
# Exit 0 = scaffolds compile; non-zero = a generated example does not build.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> generating scaffold into $WORK"
if [[ -n "${UDB_BIN:-}" ]]; then
  UDB_INIT_DIR="$WORK" "$UDB_BIN" scaffold
else
  # No --bin: the crate has exactly one binary, so this works regardless of its
  # name (the bin was renamed udb → udb).
  ( cd "$REPO" && UDB_INIT_DIR="$WORK" cargo run --quiet -- scaffold )
fi

# ── Go: build the emitted example against the in-repo SDK module ──────────────
echo "==> compiling Go scaffold example"
GO_DIR="$WORK/gocheck"
mkdir -p "$GO_DIR"
cp "$WORK/examples/go/client.go" "$GO_DIR/main.go"
cat > "$GO_DIR/go.mod" <<EOF
module scaffoldcheck

go 1.22

require (
	github.com/fahara02/udb/sdk/go v0.0.0
	google.golang.org/grpc v1.64.0
)

replace github.com/fahara02/udb/sdk/go => $REPO/sdk/go
EOF
( cd "$GO_DIR" && go mod tidy && go build ./... )
echo "    Go scaffold example built OK"

# ── TypeScript: type-check the emitted example ────────────────────────────────
echo "==> type-checking TypeScript scaffold example"
TS_DIR="$WORK/tscheck"
mkdir -p "$TS_DIR/examples/typescript"
cp "$WORK/examples/typescript/client.ts" "$TS_DIR/examples/typescript/client.ts"
# The example loads ../../proto/...; provide the repo proto tree at that relative
# location so proto-loader's path resolves during type-check.
ln -s "$REPO/proto" "$TS_DIR/proto"
( cd "$TS_DIR"
  npm init -y >/dev/null 2>&1
  npm install --no-audit --no-fund --silent \
    typescript @types/node @grpc/grpc-js @grpc/proto-loader >/dev/null 2>&1
  npx --yes tsc --noEmit --esModuleInterop --skipLibCheck --moduleResolution node \
    --target ES2020 --module commonjs examples/typescript/client.ts )
echo "    TypeScript scaffold example type-checked OK"

echo "OK: emitted Go + TypeScript scaffolds compile."
