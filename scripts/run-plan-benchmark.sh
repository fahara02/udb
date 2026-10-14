#!/usr/bin/env bash
# Run one plan-driven SDK benchmark in the live suite (Rust, Java, C#).
#
#   scripts/run-plan-benchmark.sh <sdk-id> <display name> <report path> <build cmd> <run cmd>
#
# Order matters: build first (so compile time never ages the seeded fixtures or
# the plan's sessions), then reset to a fresh database and broker, export the
# shared plan with the Python seed, and run the harness with UDB_BENCH_PLAN set.
# Every failure still leaves a Markdown report with a "## Harness error" section
# and a status file, so the collector records it instead of losing the SDK.
# Always exits 0: the collector and the benchmark gate decide pass/fail.
set -uo pipefail

sdk="$1"
name="$2"
report="$3"
build="$4"
run="$5"
plan="${GITHUB_WORKSPACE:-$PWD}/bench-output/${sdk}-plan.json"
mkdir -p bench-output/logs bench-output/status
rm -f "$report"

fail_report() {
  {
    echo "# UDB SDK Live Perf — ${name}"
    echo
    echo "RPCs measured: 0"
    echo
    echo "## Harness error"
    echo
    echo "$1"
    echo
    echo '```text'
    tail -200 "$2" 2>/dev/null
    echo '```'
  } > "$report"
}

reset_code=0
bash -c "$build" 2>&1 | tee "bench-output/logs/${sdk}-build.log"
code=${PIPESTATUS[0]}
if [ "$code" -ne 0 ]; then
  fail_report "The ${name} benchmark failed to build (code ${code})." "bench-output/logs/${sdk}-build.log"
else
  bash bench-output/reset.sh 2>&1 | tee "bench-output/logs/${sdk}-reset.log"
  reset_code=${PIPESTATUS[0]}
  if [ "$reset_code" -ne 0 ]; then
    code=$reset_code
    fail_report "Reset failed with code ${reset_code} before running the ${name} benchmark." "bench-output/logs/${sdk}-reset.log"
  else
    python sdk/python/tests/export_bench_plan.py "$plan" 2>&1 | tee "bench-output/logs/${sdk}-plan.log"
    code=${PIPESTATUS[0]}
    if [ "$code" -ne 0 ]; then
      fail_report "The shared seed could not export the benchmark plan (code ${code})." "bench-output/logs/${sdk}-plan.log"
    else
      UDB_BENCH_PLAN="$plan" bash -c "$run" 2>&1 | tee "bench-output/logs/${sdk}.log"
      code=${PIPESTATUS[0]}
    fi
  fi
fi
if [ ! -f "$report" ]; then
  fail_report "Benchmark command exited with code ${code} before writing $(basename "$report")." "bench-output/logs/${sdk}.log"
fi
note=""
if [ "$reset_code" -ne 0 ]; then
  note="reset failed before ${name} benchmark"
fi
printf '{"exit_code":%d,"note":"%s"}\n' "$code" "$note" > "bench-output/status/${sdk}.json"
exit 0
