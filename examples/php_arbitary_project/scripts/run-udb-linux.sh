#!/bin/sh
set -eu

# Downloads the released UDB CLI (a glibc x86_64-unknown-linux-gnu binary) and
# runs it. Must run on a glibc base image (e.g. debian:bookworm-slim) — the
# binary will NOT exec on Alpine/musl. ca-certificates + curl are installed if
# the base image lacks them.
cli=/tmp/udb-proto-parser

if [ ! -x "$cli" ]; then
  if ! command -v curl >/dev/null 2>&1; then
    apt-get update >/dev/null
    apt-get install -y --no-install-recommends ca-certificates curl >/dev/null
    rm -rf /var/lib/apt/lists/*
  fi
  curl -fsSL "${UDB_CLI_URL:?UDB_CLI_URL is required}" -o "$cli"
  chmod +x "$cli"
fi

exec "$cli" "$@"
