#!/usr/bin/env bash
set -euo pipefail

CHECK_ONLY=0
if [[ "${1:-}" == "--check-only" ]]; then
  CHECK_ONLY=1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UDB_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${UDB_ROOT}/.." && pwd)"
PROTO_ROOT="${PROTO_ROOT:-${REPO_ROOT}/proto}"
OUT_ROOT="${OUT_ROOT:-${UDB_ROOT}/sdk}"

PROTO_FILES=(
  "udb/entity/v1/types.proto"
  "udb/events/v1/udb_events.proto"
  "udb/services/v1/data_broker.proto"
)

require_cmd() {
  local name="$1"
  local hint="$2"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command '${name}'. ${hint}" >&2
    exit 1
  fi
}

require_cmd protoc "Install Protocol Buffers compiler."
require_cmd protoc-gen-go "Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
require_cmd protoc-gen-go-grpc "Install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
require_cmd python "Install Python 3."
python -c "import grpc_tools.protoc" >/dev/null 2>&1 || {
  echo "Missing Python grpcio-tools. Install with: python -m pip install grpcio grpcio-tools" >&2
  exit 1
}
CSHARP_ENABLED=0
if command -v grpc_csharp_plugin >/dev/null 2>&1; then
  CSHARP_ENABLED=1
else
  echo "[warn] grpc_csharp_plugin not found — C# SDK generation will be skipped. Install Grpc.Tools to enable it." >&2
fi
JAVA_ENABLED=0
if command -v protoc-gen-grpc-java >/dev/null 2>&1; then
  JAVA_ENABLED=1
else
  echo "[warn] protoc-gen-grpc-java not found — Java SDK generation will be skipped." >&2
fi

for proto in "${PROTO_FILES[@]}"; do
  if [[ ! -f "${PROTO_ROOT}/${proto}" ]]; then
    echo "Missing proto file: ${PROTO_ROOT}/${proto}" >&2
    exit 1
  fi
done

if [[ "$CHECK_ONLY" == "1" ]]; then
  echo "SDK generation prerequisites are available."
  [[ "$CSHARP_ENABLED" == "0" ]] && echo "  [warn] C# SDK skipped — grpc_csharp_plugin not found."
  [[ "$JAVA_ENABLED" == "0" ]] && echo "  [warn] Java SDK skipped — protoc-gen-grpc-java not found."
  echo "Proto root: ${PROTO_ROOT}"
  echo "Output root: ${OUT_ROOT}"
  exit 0
fi

GO_OUT="${OUT_ROOT}/go/gen/go"
PYTHON_OUT="${OUT_ROOT}/python"
CSHARP_OUT="${OUT_ROOT}/csharp/Generated"
JAVA_OUT="${OUT_ROOT}/java/gen/java"
mkdir -p "$GO_OUT" "$PYTHON_OUT"

(
  cd "$PROTO_ROOT"
  protoc -I "$PROTO_ROOT" \
    --go_out="$GO_OUT" --go_opt=paths=source_relative \
    --go-grpc_out="$GO_OUT" --go-grpc_opt=paths=source_relative \
    "${PROTO_FILES[@]}"

  python -m grpc_tools.protoc -I "$PROTO_ROOT" \
    --python_out="$PYTHON_OUT" \
    --grpc_python_out="$PYTHON_OUT" \
    --pyi_out="$PYTHON_OUT" \
    "${PROTO_FILES[@]}"

  if [[ "$CSHARP_ENABLED" == "1" ]]; then
    mkdir -p "$CSHARP_OUT"
    protoc -I "$PROTO_ROOT" \
      --csharp_out="$CSHARP_OUT" \
      --grpc_out="$CSHARP_OUT" \
      --plugin="protoc-gen-grpc=$(command -v grpc_csharp_plugin)" \
      "${PROTO_FILES[@]}"
  else
    echo "[warn] C# SDK skipped — grpc_csharp_plugin not available." >&2
  fi

  if [[ "$JAVA_ENABLED" == "1" ]]; then
    mkdir -p "$JAVA_OUT"
    protoc -I "$PROTO_ROOT" \
      --java_out="$JAVA_OUT" \
      --grpc-java_out="$JAVA_OUT" \
      "${PROTO_FILES[@]}"
  else
    echo "[warn] Java SDK skipped — protoc-gen-grpc-java not available." >&2
  fi
)

echo "Generated UDB SDKs under ${OUT_ROOT}"
