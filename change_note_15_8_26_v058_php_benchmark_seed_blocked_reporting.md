# Change note: PHP benchmark seed-blocked reporting

Date: 2026-08-15
Release: 0.5.8

## Changed

- Raw PHP seed calls now raise a status-bearing failure for non-OK gRPC results,
  preserving the numeric code, canonical status name, and server details in the
  seed log.
- `PerfFixturesPhp` can retain failed-seed provenance independently from seeded
  values. A later successful value clears the earlier block.
- `BackupService/StartTenantBackup` marks `backup_id` blocked on its original
  gRPC failure and also fails closed when an OK response omits the response or
  identifier.
- Manifest dependency inspection classifies only RPCs that actually reference a
  known blocked seed. Unknown missing refs continue through the strict resolver
  and remain fatal drift errors.
- The full PHP sweep emits dependent RPCs with `err=SEED_BLOCKED`, zero latency,
  zero iterations, seed source, and original seed status, then continues with
  every unrelated RPC.
- Equal-rank benchmark units use their discovery ordinal as a deterministic sort
  tie-breaker without changing lifecycle phases. Catalog administration also
  explicitly preserves `StageCatalog -> ActivateCatalog -> RollbackCatalog`.
- Report generation asserts exact parity between reflected units and emitted
  samples. The shared collector accepts `SEED_BLOCKED` as a fatal, countable
  harness status, and its self-test covers that schema.
- Benchmark posture pins the dependency classifier, status-bearing exception,
  focused PHP regression, full-row assertion, and collector token.

## Verification

No local Cargo, build, lint, or test command was run, per operator direction and
local hardware limits. GitHub CI must run:

```text
python3 scripts/check-bench-harness-posture.py --selftest
python3 scripts/check-bench-harness-posture.py
python3 scripts/collect_sdk_bench_results.py --selftest
cd sdk/php && vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "retains a failed PHP backup seed status and blocks only dependent RPC bodies"
cd sdk/php && vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "clears a PHP seed block when a later seed attempt succeeds"
cd sdk/php && vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "keeps an unknown missing PHP manifest seed fail closed"
cd sdk/php && UDB_LIVE_PERF=1 vendor/bin/pest tests/Live/GeneratedRpcSurfaceTest.php --filter "measures per-RPC latency"
```

The live benchmark acceptance check is a generated PHP report with exactly the
current reflected RPC count (381 at this revision), no `## Harness error`, and
any blocked Backup dependents listed under `## Seed-blocked RPCs` with the
original `StartTenantBackup` status and details.
