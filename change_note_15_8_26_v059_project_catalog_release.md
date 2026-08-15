# Change note: v0.5.9 exact-project catalog and release evidence

Date: 2026-08-15
Release: 0.5.9

## Changed

- The canonical release version advances to 0.5.9 and the governed propagator
  refreshes every maintained SDK manifest, lockfile, install command, stale
  example revision, documentation header, generated wrapper, and Pages version
  label from 0.5.8.
- Served catalog activation and rollback now reload the exact durable
  project/catalog row and publish it with project-specific in-memory authority.
- Catalog response recovery requires an explicitly active project and cannot
  turn the legacy default fallback into a false successful activation.
- Durable rollback now targets prior `ROLLED_BACK` rows; the served RPC requires
  an explicit selector and a transactionally enforced idempotency key so replay
  returns the recorded result without toggling a later active version.
- Durable stage failures in the node-local catalog map are surfaced instead of
  returning a staged response with an empty checksum.
- Catalog manifest reads resolve only the authenticated project's exact active
  manifest. A project without activation fails closed and must explicitly stage
  its release manifest; it cannot inherit the default project's authority.
- Stage/activate/rollback serialize each project with a PostgreSQL advisory lock,
  enforce one ACTIVE row, bind canonical project identity, verify canonical
  manifest integrity and semantic checksum, and persist validation and real
  compatibility-diff evidence with the exact binding/reload transition.
- Startup upgrades quarantine unverifiable legacy staged rows, validate or
  explicitly migrate legacy active provenance, and reject split ACTIVE/binding
  authority in both directions. Replica hydration consumes only matching
  binding, catalog, compatibility-evidence, and reload rows.
- Exact catalog loaders now require the binding compatibility evidence to equal
  the catalog row and require the successful reload proof to match the same
  project, catalog id, version, checksum, and evidence. Load-all hydration,
  transition baselines, provenance-upgrade generation, and the startup
  bidirectional audit enforce the same tuple instead of accepting a stale or
  split proof row.
- The reload listener connects and subscribes before catalog-dependent workers
  start. Reconciliation is generation-gated and all authority-sensitive RPCs
  fail closed while the node cannot prove a fresh exact catalog.
- EnsureProject, project enumeration, catalog reads/mutations, migration RPCs,
  capabilities, and health diagnostics bind body project identity to the
  authenticated claim; cross-project use requires explicit platform authority.
- Native claim enforcement and API-key record authorization now share one
  platform-authority predicate. Explicit platform roles and the exact
  `udb:platform_admin` scope retain cross-boundary authority; `*`, `udb:*`,
  `udb:admin`, and `udb:auth:admin` remain broad action scopes but cannot erase
  a non-empty verified tenant/project claim. Deliberately unbound operator
  identities retain their existing broad-scope compatibility.
- Empty project selectors on capabilities, health, and schema discovery now
  follow their published context/default contract. The shared resolver also
  enforces body-vs-security project equality for trusted in-process calls and
  recognizes only the exact `udb:platform_admin` scope as their cross-project
  escape hatch.
- Catalog payload-integrity evidence now hashes deterministic serialization of
  the decoded typed manifest, so valid authority survives PostgreSQL JSONB
  normalization. Raw request and semantic schema checksums remain distinct.
- Activation and rollback now advance the target catalog row's current
  baseline/evidence in the same transaction as the exact project binding and
  reload log, so reactivation cannot leave a split authority tuple.
- Error-detail posture now pins the centralized catalog project policy detail
  and current typed durable/reconciliation errors, removing stale requirements
  for deleted per-handler authorization and in-memory activation branches.
- Its migration-planning checks now pin the current transaction/authority-lock
  and exact catalog-id error seams rather than removed raw manifest parsing.
- Message-schema discovery, admin summary, and projection-drift control paths
  use the same claim binding and exact active target. Capabilities expose the
  verified semantic manifest checksum, while health rejects raw ACTIVE rows
  whose durable binding/reload proof is missing or split.
- Migration planning loads only the proven exact ACTIVE project catalog rather
  than trusting an unbound `ACTIVE` row.
- Migration control state remains in the canonical system store, while schema
  introspection and PostgreSQL apply/verification use one exact project-routed
  write target. Each run durably pins catalog id/checksum, target instance, and
  credential-free routing/physical provenance; approve refuses legacy unbound
  plans and apply re-resolves plus CAS-verifies the binding while holding the
  same project lock used by catalog transitions.
- Runtime instance discovery now exposes a project-filtered accessor backed by
  the canonical project-router decision, so project capability surfaces can
  omit instances labelled for other projects. Enabled/configured state is
  derived from those exact instances, health probes pin the selected instance,
  the exact PostgreSQL schema/write authority is live-probed, and missing,
  ambiguous, unreachable, or unproven authority makes project health fail.
- Project routing configuration now rejects unknown tokens and an empty
  `strict_with_default:` project during startup validation. If code constructs
  an invalid configuration without validation, routing falls back to strict
  isolation rather than permissive cross-project access.
- Backup inventory listing uses a lightweight exact-project binding rather than
  requiring the full snapshot/movement topology merely to read durable rows.
- The release benchmark reset exports the manifest from the exact release
  binary, explicitly stages and activates it for its customer project, verifies
  the durable row, and preflights a served Backup read before SDK fixture
  creation.
- The Backup preflight uses the native/auth listener, Go and TypeScript pin the
  catalog lifecycle order, and Vault credential fixtures use the authenticated
  benchmark project instead of the default project.
- Quick CI and workflow lint now compile the benchmark catalog bootstrap, both
  workflow-lint trigger blocks track the script, and workflow posture fixtures
  fail if either PR-time guard is removed.
- Failed formatting, SDK code-generation, and native contract/documentation
  gates now retain short-lived CI-generated binary repair patches. This keeps
  generated corrections tied to the pinned GitHub toolchain when local builds
  are intentionally disabled.
- The benchmark orchestrator's positive posture fixture now includes the
  catalog-bootstrap trigger path required by the production workflow.
- The reusable live-suite positive fixture now executes the exact-project
  bootstrap command and uses a raw release-regex literal, so the posture
  selftest represents the production requirement without escape warnings.
- CDC startup now retains the runtime snapshot and passes the borrowed runtime
  expected by the engine constructor, closing the CI compile mismatch without
  changing worker or authority ordering.
- TypeScript's Vault manifest-body regression now supplies and asserts the
  exact project used by dynamic database-credential requests.
- The reusable benchmark accepts only an anchored SemVer release tag and a Linux
  amd64 UDB asset. It downloads and verifies the binary checksum, manifest
  checksum, manifest tag/version/asset digest/size, and exact binary version
  before using the executable.
- Benchmark evidence now records the verified release binary SHA-256 in both the
  release object and current history point.
- Pages consumes benchmark artifacts only from a successful post-Release
  benchmark. It verifies source/workflow/run id, trigger SHA, release URL and
  tag, resolves the tag to the benchmarked commit, and compares the recorded
  digest with the checksum published on that exact release before deployment.
- Maintained README/site/diagram counts now match the generated descriptor
  inventory, while the coding skill points to that inventory instead of
  duplicating a drift-prone total. Current release workflow examples, the skill
  baseline, and Python publishing examples use 0.5.9, and the consumer skill's
  Vault inventory reflects its 22-RPC generated surface; committed v0.4.28
  benchmark evidence remains historically labeled.

## Verification

- No local Cargo/build/test command is run, per operator direction.
- No local Python/workflow test is run either; only static inspection and
  `git diff --check` are used in this constrained worktree.
- Added the ignored live Postgres regression
  `live_postgres_catalog_authority_end_to_end`; GitHub's `Live quick
  (isolation)` workflow can run it with filter
  `live_postgres_catalog_authority_end_to_end`. It includes stale-catalog
  migration-apply refusal before physical execution.
- GitHub CI must compile and test all targets and exercise the focused catalog,
  Backup, Vault, and SDK live lanes.
- GitHub quick CI and workflow lint must observe the bootstrap `py_compile` and
  workflow-posture selftests, followed by a post-release Benchmark -> Pages proof
  of the new checksum/manifest/tag/commit chain.
- Any uploaded CI repair patch must be applied, reviewed, documented, committed
  as `fahara02`, and proven by a subsequent clean run; artifact creation alone
  is not acceptance evidence.
- Applied the `ci-rustfmt-repair-1` and `ci-sdk-codegen-repair-1` artifacts from
  GitHub CI run `31895052655`; the next CI run is the drift-free proof.
- GitHub run `31895052655` compiled all targets and reported 2,700 passing
  library tests before the shared default-project resolver failures; focused
  live run `31895088437` supplied the JSONB provenance-readback failure.
- Focused run `31896157075` proved initial stage/replay, conflicting-key denial,
  concurrent one-ACTIVE enforcement, and rollback before isolating the
  reactivation-evidence split fixed in this revision.
- Focused GitHub run `31897092671` then passed the complete catalog-authority
  regression after the catalog/binding/reload evidence tuple was unified.
- The shared auth live fixture can now stage and activate a served manifest for
  an exact project. CDC bearer/API-key lifetime and data-only API-key CRUD use
  it for `billing`, preserving the production no-default-fallback boundary.
- Unit coverage now denies bound broad-admin/wildcard claims at the common body
  tenant/project guard and across API-key create preflight, get, filtered list,
  update, revoke, rotate, and emergency-revoke paths, while retaining explicit
  platform authority and deliberately unbound operator behavior.
- Refreshed the generated codebase map after adding that shared helper; GitHub
  run `31903131598` had already compiled all targets and passed the full library
  suite before its freshness gate reported the one-line map drift.
- Focused GitHub runs `31903188510` and `31903230512` passed the two corrected
  served-auth live tests independently.
- Applied the three-line `ci-rustfmt-repair-1` artifact from run `31896140645`
  to the shared project resolver before this commit.
- Native repair generation consumes the exact broker already built by the
  Linux Rust job, avoiding a second Cargo invocation and its post-failure target
  lock while preserving runner/toolchain provenance.
- Applied `ci-native-docs-repair-1` from GitHub run `31897080052`; it refreshes
  the native contract for Backup tenant+project isolation using the runner-built
  broker, with no local build or test.
- The independent native contract advances from 5.0.0 to 6.0.0 because Backup
  tenant+project persistence is an intentional database/security contract
  break. CI repair now includes the binary descriptor baseline so the bump and
  exact runner-generated baseline are reviewed together.
- Applied the baseline-only `ci-native-docs-repair-1` artifact from GitHub run
  `31898607698`; its Linux build and full library suite passed before the
  intentional old-baseline comparison produced the artifact.
- Native manifest, lint, docs, and contract-diff gates likewise execute the
  preceding all-target build directly instead of redundantly re-entering Cargo.
- Workflow and docs-freshness posture fixtures now enforce that build-once
  native command path.
- The v0.5.9 tag must point to the exact fully green `main` SHA.
- Completion requires a successful Release workflow, 1,524 measured SDK RPCs,
  four `ok` SDK statuses, zero failed RPCs, and a successful Pages deployment
  consuming that exact benchmark artifact.
