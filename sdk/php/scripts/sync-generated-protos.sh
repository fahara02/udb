#!/usr/bin/env bash
#
# Copy the generated UDB proto + gRPC PHP stubs from the
# monorepo's `gen/php/Udb/` into the SDK package's `gen/` so
# Composer can autoload them via the `Udb\\` PSR-4 prefix
# declared in `composer.json`.
#
# Run this whenever you regenerate protos:
#   buf generate                # writes to gen/php/Udb
#   ./scripts/sync-generated-protos.sh
#   composer dump-autoload      # re-index
#
# When the SDK becomes its own repo, this script is replaced by
# a CI step that pulls the generated stubs from the published
# `fahara02/udb-protos-php` Composer package — but bundling
# works fine for v0.1.0.
#
set -euo pipefail

SDK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MONOREPO_GEN="${SDK_ROOT}/../../../gen/php"

if [[ ! -d "${MONOREPO_GEN}/Udb" ]]; then
  echo "ERROR: ${MONOREPO_GEN}/Udb does not exist." >&2
  echo "Run 'buf generate' in the monorepo root first." >&2
  exit 1
fi

DEST="${SDK_ROOT}/gen"
echo "Syncing generated PHP protos -> ${DEST}/Udb"
mkdir -p "${DEST}/Udb"
rsync -a --delete "${MONOREPO_GEN}/Udb/" "${DEST}/Udb/"

if [[ -d "${MONOREPO_GEN}/GPBMetadata/Udb" ]]; then
  echo "Syncing GPBMetadata -> ${DEST}/GPBMetadata/Udb"
  mkdir -p "${DEST}/GPBMetadata/Udb"
  rsync -a --delete "${MONOREPO_GEN}/GPBMetadata/Udb/" "${DEST}/GPBMetadata/Udb/"
fi

echo "Done. Run 'composer dump-autoload' to refresh PSR-4."
