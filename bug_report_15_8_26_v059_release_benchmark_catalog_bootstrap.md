# Bug report: release benchmark never activated its customer project catalog

Date: 2026-08-15
Affected release evidence: 0.5.8
Target correction: 0.5.9
Severity: release-evidence blocker

## Observed

The reusable live SDK suite recreates PostgreSQL and the broker before each SDK,
then bootstraps a user bound to project
`00000000-0000-4000-8000-000000000057`. Identity bootstrap does not stage or
activate a DataBroker catalog. The suite immediately seeded Backup and Vault,
whose exact-project guards correctly refused default-project fallback.

Go, Python, and TypeScript completed reports with the same failures. PHP's
failed `StartTenantBackup` seed left `backup_id` unset, and body interpolation
threw before PHP could emit its report. The result contained 1,143 measurements
(three SDKs times 381 RPCs), 78 failed RPCs, and no PHP report.

The follow-up release-readiness audit found four additional evidence gaps around
the same bootstrap path:

- the new bootstrap script was not tracked by the PR-time workflow linter and
  neither quick CI nor workflow lint compiled it;
- the benchmark accepted a SemVer prefix, downloaded only the executable, and
  did not verify the published binary checksum, manifest checksum, manifest
  asset entry, or `udb --version` identity;
- the benchmark JSON did not record the verified binary digest, while Pages
  accepted successful manual benchmark artifacts and checked neither the
  triggering run/commit nor the tag's published checksum; and
- maintained release examples, the coding skill, Pages copy, README counts, and
  the control-plane diagram still advertised older versions or RPC totals; the
  consumer skill also described the expanded Vault surface as 20 rather than 22
  RPCs.

The first combined PR run also proved that CI reported generated Rust/SDK/native
drift only as truncated log output. On a CI-only development machine that left
no authoritative patch to apply, while the workflow-posture selftest's nominal
benchmark fixture had not been updated with the new bootstrap trigger path.
The same run showed that the TypeScript Vault body regression fixture seeded
tenant and lease values but not the new `<seed:project>` placeholder, so SDK
conformance failed before it could assert exact-project database credentials.
After adding the trigger-path fixture, the next selftest reached a second stale
positive fixture: the reusable-suite sample asserted the bootstrap command was
required but did not contain it. Its non-raw regex literal also emitted a Python
invalid-escape warning.

## Impact

- A release whose main CI was green could not produce valid post-release
  benchmark or Pages evidence.
- Native-service failures were fixture failures masking the intended served RPC
  performance run.
- PHP's missing seed converted the original gRPC status into a harness abort,
  reducing diagnosability.

## Required correction

- After every fresh broker start, authenticate the benchmark principal, export
  the manifest from the exact release binary, stage it for the exact customer
  project, activate its durable catalog id, and verify the returned version and
  checksum against the exact durable binding.
- Preflight an authority-sensitive served call (`ListBackups`) before any SDK
  seed so a durable/in-memory split fails at reset with the original status.
- Send that Backup preflight to the native/auth listener; the public
  DataBroker listener does not serve native Backup RPCs.
- Pin every SDK's destructive catalog lifecycle as StageCatalog,
  ActivateCatalog, then RollbackCatalog, independent of generated method order.
- Resolve Vault database-credential project fields from the authenticated UUID
  project rather than hardcoding the unrelated `default` authority.
- Harden PHP seed dependency handling so future seed failures still produce a
  complete report with explicit seed-blocked RPCs.
- Compile the catalog bootstrap in PR-time quick CI and workflow lint, and track
  its path in both lint trigger blocks with a posture regression.
- Resolve only an anchored released SemVer tag and a compatible Linux asset;
  verify the binary sidecar, manifest sidecar, manifest tag/version/asset
  digest/size, and exact `udb --version` before starting any backend.
- Record the verified binary SHA-256 in the benchmark release object and current
  history point.
- Allow a benchmark-triggered Pages deployment only for a successful benchmark
  that was itself triggered by Release. Bind its artifact to the exact run id,
  workflow, trigger SHA, tag-resolved commit, release URL, and published asset
  checksum before upload.
- Align maintained public counts with the generated descriptor inventory,
  remove the coding skill's drift-prone duplicate count, and align current
  release/publishing examples and the skill baseline to 0.5.9. Put those version
  examples under the canonical version propagator, align the consumer skill's
  Vault inventory with the generated contract, and do not relabel the committed
  v0.4.28 benchmark evidence.
- Do not weaken Backup/Vault exact-project checks.
- On a failed Rust formatting, SDK generation, or native contract/docs gate,
  upload the exact binary Git patch produced by the pinned GitHub runner so the
  repair remains CI-authored and reproducible without a local build.
- Keep the benchmark orchestrator selftest fixture in lockstep with every
  required trigger path, including the catalog bootstrap script.
- Keep the reusable-suite positive fixture in lockstep with the required
  bootstrap command and represent its release regex as a raw Python literal.
- Seed and assert the exact project in TypeScript's Vault manifest-body test so
  the project-authority benchmark body is covered by offline SDK conformance.

## Evidence

GitHub Actions run `31886239424` produced provenance-correct v0.5.8 benchmark
JSON for commit `51286ed93ff989ddacbae16f4d738ac999ebd321`, but reported three
SDKs with Backup/Vault failures and one failed PHP harness. Final 0.5.9 evidence
must contain 1,524 measured RPCs, all four SDK statuses `ok`, and zero failed
RPCs before Pages may deploy it. The benchmark artifact must additionally carry
the SHA-256 of its verified release binary, and the Pages run must prove that
digest against the exact tag's published checksum.

GitHub CI run `31895052655` produced the authoritative Rust formatting patch
and pinned Buf 1.65.0 SDK-generation patch. Both patches applied cleanly and are
part of the follow-up commit; acceptance still requires a later run with no
repair artifact because no drift remains.

That run also showed the first native-repair implementation could wait on
Cargo's target lock after an earlier test failure. Recovery now executes the
already-built `target/debug/udb` directly; if the build did not produce that
binary, the recovery step declines to fabricate native artifacts.

The normal native manifest/docs/diff gates had the same redundant `cargo run`
boundary after `cargo build --all-targets`, causing a later green library run to
spend minutes re-entering Cargo before contract drift could be reported. All
native gates now execute that already-built broker directly.
The workflow and docs-freshness posture fixtures pin this build-once command so
a future edit cannot silently restore the redundant Cargo boundary.

GitHub CI run `31897080052` then produced `ci-native-docs-repair-1`, the
runner-generated native-contract update for first-class Backup project
ownership. The patch was reviewed and applied without invoking a local build.
