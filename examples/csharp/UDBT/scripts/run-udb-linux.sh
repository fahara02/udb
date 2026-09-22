#!/bin/sh
set -eu

cli=/tmp/udb

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
