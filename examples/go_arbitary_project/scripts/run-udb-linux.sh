#!/bin/sh
set -eu

cli=/tmp/udb-proto-parser

if [ ! -x "$cli" ]; then
  curl -fsSL "${UDB_CLI_URL:?UDB_CLI_URL is required}" -o "$cli"
  chmod +x "$cli"
fi

exec "$cli" "$@"
