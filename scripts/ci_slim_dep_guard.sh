#!/usr/bin/env bash
# NW4 — Slim build dependency-graph guard.
#
# Asserts that `cargo tree --no-default-features --features postgres`
# does NOT pull any of the heavy optional dependency subgraphs. This
# catches the regression mode where a backend plugin or runtime module
# is accidentally added to the always-on code path and starts dragging
# in an HTTP / Kafka / AWS / Mongo / OpenTelemetry dep without the
# operator opting into the feature.
#
# Usage:
#   bash scripts/ci_slim_dep_guard.sh
#
# Exit codes:
#   0  — slim graph is honest.
#   1  — a forbidden dep was detected; the script prints which one and
#        the path that pulls it.

set -u
set -o pipefail

# Run from anywhere — resolve the udb crate dir relative to this
# script so the operator can invoke it from CI without changing CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Forbidden top-level crate names. Each name matches `cargo tree`
# output exactly — these are crate names, not features. Adding a new
# heavy dep means adding it here too (the test below catches any
# leakage).
FORBIDDEN=(
    "reqwest"
    # hyper is intentionally NOT here: it is pulled by tonic
    # (the gRPC server framework), which is an always-on production
    # dep. Backend HTTP clients should use reqwest, so trapping
    # reqwest alone catches every HTTP-backed plugin (Qdrant,
    # MinIO/S3, MongoDB Atlas REST, ClickHouse HTTP, etc.).
    "aws-config"
    "aws-sdk-s3"
    "mongodb-driver"
    "rdkafka"
    "rdkafka-sys"
    "opentelemetry"
    "opentelemetry-otlp"
    # C9: new backend driver deps must stay gated behind their
    # respective features (memcached, mssql, cassandra, azureblob,
    # gcs). All are heavy enough to bloat the slim build.
    "memcache"
    "tiberius"
    "scylla"
    "azure_core"
    "azure_storage"
    "azure_storage_blobs"
    "google-cloud-storage"
)

echo "── NW4 slim-build dep guard ─────────────────────────────────"
echo "Running: cargo tree --no-default-features --features postgres --edges normal"
echo "(--edges normal excludes [dev-dependencies] so we only check the production graph)"
echo

TREE_OUTPUT="$(cd "$CRATE_DIR" && cargo tree --no-default-features --features postgres --edges normal 2>&1)"
TREE_STATUS=$?
if [[ $TREE_STATUS -ne 0 ]]; then
    echo "ERROR: cargo tree failed with exit $TREE_STATUS" >&2
    echo "$TREE_OUTPUT" >&2
    exit $TREE_STATUS
fi

VIOLATIONS=0
for dep in "${FORBIDDEN[@]}"; do
    # Match the dep name at the start of a tree line (after the
    # tree-drawing chars). Use grep -F for literal matching.
    if echo "$TREE_OUTPUT" | grep -q "$dep"; then
        echo "✘ FORBIDDEN dep '$dep' is present in slim build:" >&2
        echo "$TREE_OUTPUT" | grep -- "$dep" | head -5 | sed 's/^/   /' >&2
        VIOLATIONS=$((VIOLATIONS + 1))
    else
        echo "  ✓ $dep absent"
    fi
done

echo
if [[ $VIOLATIONS -eq 0 ]]; then
    echo "✅ Slim build is honest — no forbidden deps in the graph."
    exit 0
else
    echo "❌ $VIOLATIONS forbidden dep(s) found in slim build."
    echo
    echo "The slim build is supposed to compile WITHOUT the heavy HTTP /"
    echo "AWS / Kafka / Mongo / OpenTelemetry subgraphs. Something in"
    echo "the always-on path is now pulling one in. Likely causes:"
    echo
    echo "  • A backend plugin module that should be feature-gated isn't."
    echo "  • A runtime module is unconditionally importing a"
    echo "    feature-gated crate."
    echo "  • A new dep was added to [dependencies] without"
    echo "    'optional = true'."
    echo
    echo "Inspect the tree paths above and gate the offending module"
    echo "behind '#[cfg(feature = \"…\")]'."
    exit 1
fi
