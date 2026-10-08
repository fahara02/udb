#!/usr/bin/env bash
# Retry idempotent CI setup commands without hiding the last failure. Registry
# 502s must not discard a completed Rust compile before live tests can start.
set -uo pipefail

readonly MAX_CI_ATTEMPTS=3
readonly CI_RETRY_DELAY_SECONDS=5

if [[ "${CI:-}" != "true" ]]; then
  echo "CI setup retries may only run in CI" >&2
  exit 2
fi
if [[ "${1:-}" == "--" ]]; then
  shift
fi
if (( $# == 0 )); then
  echo "usage: ci-retry.sh -- command [args...]" >&2
  exit 2
fi

ci_command_status=1
for ((ci_attempt=1; ci_attempt<=MAX_CI_ATTEMPTS; ci_attempt++)); do
  if "$@"; then
    exit 0
  else
    ci_command_status=$?
  fi
  if (( ci_attempt < MAX_CI_ATTEMPTS )); then
    echo "::warning::CI setup failed with status ${ci_command_status}; retrying (${ci_attempt}/${MAX_CI_ATTEMPTS})"
    sleep "$((ci_attempt * CI_RETRY_DELAY_SECONDS))"
  fi
done
echo "::error::CI setup failed after ${MAX_CI_ATTEMPTS} attempts (status ${ci_command_status})"
exit "$ci_command_status"
