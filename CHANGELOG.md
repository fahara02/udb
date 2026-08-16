# Changelog

All notable user-facing and release-gating changes are tracked here. UDB follows
the package version in `Cargo.toml`; historical v0.3.2 audit material is folded
into the v0.3.x entries because the codebase advanced to v0.3.7 before that
release line was tagged.

## [0.5.14] - 2026-08-16

Patch release restoring the in-place upgrade path, recovering poisoned Redis
connections, and closing two backup restore-key authority gaps. (0.5.13 was
skipped.)

### Fixed

- **In-place upgrade is possible again.** The auto-appended audit block
  (`created_at`, `updated_at`, `created_by`) was numbered `max(explicit) + 1`, so
  appending an explicit field to any message with `audit_fields: true` slid the
  block upward and handed the new fields the numbers a deployed manifest had
  recorded for the audit trio. The drift gate then reported field-number reuse —
  against fields the schema author never numbered — and blocked startup
  unconditionally, so the broker crash-looped and every dependent service lost
  its database, for a table that could be empty. A database created by 0.5.6
  could not start any broker from 0.5.8 onward.

  The block is now allocated from a reserved base (9000), far above hand-written
  fields and below protobuf's reserved range, so appending explicit fields never
  moves it. Existing deployments are unblocked as well: relocating a generated
  audit column is no longer treated as field-number reuse, because audit columns
  are synthesized into the manifest and appear in no `.proto` — their number was
  never a real protobuf field number. The exemption is narrow, so a hand-written
  column that was genuinely renumbered is still blocked, and when a conflict does
  involve an audit column the message now says so instead of telling the operator
  to reserve a number they never wrote. 45 entities declared `audit_fields` and
  each was a latent instance of this.

- **Poisoned shared Redis connections recover.** A broken shared connection was
  reused indefinitely; the client now uses a connection manager that reconnects
  in the background, and rate-limit infra failures keep their typed retryable
  detail so callers still fall back rather than fail hard.

- **Backup restore preserves partition-aware unique keys** and closes the
  restore-key authority gaps found in the post-release audit.

### Notes

- `udb drift` diffs a proto catalog against a prior manifest offline, with no
  database, and surfaces the upgrade block before any broker touches a
  deployment. It is the recommended pre-upgrade gate.

## [0.5.12] - 2026-08-16

Patch release making the opaque-project-id guarantee true for the last path
that still contradicted it.

### Fixed

- **`DeleteFile` with `DELETE_MODE_HARD` accepts an opaque project id.** The
  hard-delete path inserts a durable GC intent, and that insert still bound the
  verified project as `NULLIF($3,'')::uuid`. Both the Storage entity and the
  `udb_storage.gc_intents` ledger are `VARCHAR(120)`, so a real project code
  turned a valid hard delete into `INTERNAL`:

  ```text
  storage GC-intent insert failed: invalid input syntax for type uuid: "ambulife"
  ```

  The bind is `varchar(120)` now. Tenant, file and intent identities stay
  UUID-typed, the empty-project-to-NULL behaviour and the handler's 120-byte
  validation are unchanged, and the tombstone half of the transaction is scoped
  by tenant and file so it was never affected. The served two-project live
  Storage test now performs a HARD delete under a real opaque project and reads
  the exact project back from the durable intent, so reverting the bind fails at
  the reported statement.
- **Cross-tenant Backup restore preserves unique-key authority.** Manifest and
  live PostgreSQL unique keys stay grouped, partition auxiliaries are preserved
  once another member is safely remapped, expression and partial keys are
  evaluated fail closed, numeric identities still require their owned sequence,
  and bounded text remaps retain an alphabetic 128-bit value. Parent maps are
  preallocated; nullable self-references are rebound after insertion so neither
  artifact row order nor a mutual cycle can retain source-tenant identities.
- **Shared Redis connections recover after transport loss without replaying a
  mutation.** Long-lived executor, canonical-store, rate-limit, and CDC handles
  use the reconnecting connection manager. The failed command is returned as an
  uncertain outcome and only a later request uses the replacement connection;
  rate-limit Lua is never replayed in-call. An open circuit is reported as
  retryable `UNAVAILABLE`, not as a false missing-executor configuration error.

### Notes

- 0.5.10 and 0.5.11 both stated that native services accept opaque project ids
  end to end. That was true of every path except this one; deployments that use
  project codes and rely on hard delete should move to 0.5.12.
- The v0.5.11 post-release benchmark proved all 381 canonical RPC identities for
  each measured SDK but correctly withheld Pages publication after four Backup
  restore failures and one Redis transport failure plus its circuit follow-on.
  v0.5.12 must pass the focused Backup/Redis live regressions and the automatic
  1,524-attempt Release → Benchmark → Pages evidence chain before it is called
  complete.

## [0.5.11] - 2026-08-16

Patch release closing the remaining product and benchmark-fixture defects exposed
by the strict v0.5.10 post-release 1,524-RPC evidence gate.

### Fixed

- **Asset registration preserves its effective project authority.** A blank
  request-body project now inherits and persists the verified bearer/header
  project, so the returned asset id is immediately visible to project-scoped
  GetAsset/ListAssets while remaining invisible to other projects.
- **Identifier-only Authz mutations retain tenant scope.** RevokeRole,
  DeleteRole, and DeletePolicyRule compile their typed mutations under the
  verified claim-first tenant context instead of a default context, preserving
  same-tenant success and making foreign-tenant identifiers no-ops.
- **Cross-tenant Backup restore remaps numeric identities safely.** Integral
  serial/identity unique keys receive fresh values from their trusted PostgreSQL
  sequence inside the restore transaction, and typed old-to-new mappings are
  propagated to child foreign keys without overwriting source rows.
- **TypeScript and PHP governance benchmarks use audited authority.** Their
  platform seed actors and reviewers bind to the verified platform user, carry
  explicit short-lived break-glass reason/expiry, and preserve original seed
  failures as complete SEED_BLOCKED evidence rather than sending placeholder ids.

### Verification

- No local Cargo build, test, formatter, SDK generation, or protocol generation
  was run. CI must compile the complete matrix, execute the focused live Asset,
  Authz, and Backup regressions, then prove 381 canonical RPCs across each of Go,
  Python, TypeScript, and PHP (1,524 attempts, zero fatal rows) before Pages can
  publish fresh benchmark evidence.

## [0.5.10] - 2026-08-16

Patch release closing the product, authority, SDK-fixture, and release-evidence
defects exposed by the strict v0.5.9 post-release benchmark.

### Fixed

- **Platform authority can no longer be minted by a tenant.** Reserved platform
  roles require active system/global provenance, tenant role and governance APIs
  cannot create or bind them, and service/API-key grants reject the explicit
  platform scope. A separate direct-Postgres, offline-only bootstrap provisions
  the verified platform principal used by cross-tenant control-plane tests.
- **Tenant revocation has a safe issuance boundary.** After Redis acknowledges
  an inclusive tenant cutoff, the revoke RPC does not complete until a fresh JWT
  can only receive a later `iat`. Logout refresh-family cleanup also carries the
  validated tenant/project authority instead of compiling tenant-scoped IR with
  an empty context.
- **Authn mutation and migration contracts match execution.** MFA challenge
  verification is classified as a mutation, WebAuthn credential lifecycle uses
  exact tenant/project routing, and migration planning no longer emits native
  resource operations for logical-only Redis namespaces.
- **All four live SDK benchmarks use real, authority-correct fixtures.** Actor
  attribution is derived from the authenticated caller, global governance,
  Analytics, cross-tenant restore, and admin purge use a distinct verified
  platform session, Vault credential revocation uses a real disposable lease,
  and WebAuthn mutation fixtures own separate real credentials. PHP reports a
  complete surface even when a prerequisite fails, while TypeScript reports the
  canonical generated wire identity rather than an SDK alias.
- **Failed benchmark artifacts cannot claim complete evidence.** The collector
  stamps `canonical_complete` only after the central gate succeeds; an uploaded
  diagnostic artifact from a failed run remains explicitly incomplete.
- **Release, Benchmark, and Pages evidence is bound end to end.** Pages resolves
  the immutable release tag instead of confusing it with mutable
  `workflow_run.head_sha`. The runner audit requires exact downstream run IDs,
  downloads both artifacts, verifies byte-identical benchmark evidence, and
  binds it to the audited release tag, commit, asset, checksum, and run attempt.
- **CI repair artifacts cover every generated authority.** Native-contract repair
  now regenerates the canonical codebase map and synchronized bundled skill
  reference alongside the manifest, docs, and binary baseline.
- **Native contract governance advances to 7.1.0.** Operation-kind drift is a
  behavioral contract change, and the regenerated Authn contract records
  `VerifyMfaChallenge` as a mutation.
- **Native services accept opaque project ids end to end.** v0.5.9 widened
  `project_id` to `VARCHAR(120)` on Storage, Asset, Scheduler and Workflow (in
  place, through a `using_expression`, so no deployment loses rows) but left four
  call sites casting the bind to `::UUID`: the scheduler and workflow scope
  predicates and the `CreateJob`/`StartWorkflow` inserts. Against the widened
  column that is a type error, and against a human project code the cast itself
  fails — so callers using the documented project-code shape got `INTERNAL`
  instead of the previous `InvalidArgument`. All four now compare and bind the
  project as the opaque string it is; the tenant arm keeps its UUID cast because
  tenants really are UUIDs. An over-length project is refused rather than
  truncated, and a non-empty project authority is never downgraded to
  tenant-wide.
- **`ListAssets` and `GetAsset` no longer return other projects' assets.** Both
  read through a tenant-only native context, and the project predicate had been
  dropped as a workaround for the UUID-typed column — but the asset read carried
  no project filter of its own, leaving the two RPCs with no project confinement
  at all, so a project-scoped caller could list and fetch every project's assets
  inside its tenant. The read now applies the owning project explicitly, and the
  list total carries the same clause so the count cannot disagree with the page.
- **A revoked certificate binding cannot fall through to a weaker selector.** An
  unused request-time lookup filtered inactive rows out in SQL, so an expired or
  revoked binding on a strong selector (SPIFFE URI, fingerprint) would have
  fallen through to a DNS/CN selector on the same certificate had it been wired.
  Removed in favour of the candidate lookup the live path already uses, which
  returns the row together with whether it is usable.
- **Stale cache entries cannot be written back.** An unstamped cache setter
  remained callable beside the LSN-stamped, project-scoped setters; an entry it
  wrote could not be invalidated by the freshness check every reader uses.
- **Vault no longer carries a false capability refusal.** A dead error
  constructor still announced that dynamic database credential issuance is
  disabled. Issuance is bounded instead — every alias is checked against the
  verified tenant, project and instance, and PUBLIC table and SECURITY DEFINER
  grants are audited before a login is minted.
- **SAML signed-element digests compare in constant time.**

## [0.5.9] - 2026-08-15

Patch release correcting the exact-project catalog publication defect exposed
by the v0.5.8 post-release benchmark and restoring trustworthy release evidence.

### Fixed

- **Catalog activation and rollback publish the requested project.** The served
  handlers no longer reload a global last manifest or call the default-project
  activation shim after committing a customer-project row. They reconcile the
  exact durable ACTIVE row into the exact project slot, and recovery cannot
  report default fallback as a successful customer-project activation.
- **Catalog rollback restores a prior version.** Rollback transitions an
  explicitly selected `ROLLED_BACK` row instead of aliasing the STAGED-only
  activation path. Project advisory locking, a one-ACTIVE invariant, durable
  idempotency fingerprints, and recorded replay results prevent concurrent or
  stale retries from toggling a later version.
- **Catalog authority is durable, project-bound, and replica-safe.** Stage and
  activation verify payload integrity, the semantic schema checksum, canonical
  validation, real compatibility-diff evidence, the exact prior binding, and
  project identity. Startup and replica reload publish only matching catalog,
  binding, compatibility-evidence, and transition rows; split or stale
  authority fails closed.
- **Catalog consumers no longer trust raw/default authority.** Capabilities,
  health, schema discovery, admin summary, projection drift, migrations, and
  long-lived workers resolve an exact claim-bound project. Health identifies
  unproven raw ACTIVE rows instead of reporting them as healthy.
- **The release benchmark activates its customer catalog.** Every clean reset
  performs StageCatalog, ActivateCatalog, durable verification, and an
  authority-sensitive served preflight before any SDK seed runs. The Backup and
  Vault fail-closed project checks remain intact.
- **Release benchmark evidence is cryptographically bound to the published
  binary.** The reusable suite accepts only an anchored SemVer tag, verifies the
  downloaded binary and manifest against both published checksum sidecars,
  checks the manifest asset identity and `udb --version`, and records the binary
  SHA-256 in the benchmark JSON. PR-time quick CI and workflow lint compile and
  track the catalog bootstrap script.
- **Pages deploys only exact post-release benchmark proof.** Manual and push-only
  benchmark runs cannot trigger release-evidence publication. Pages binds the
  artifact to the successful post-Release benchmark run and trigger SHA, resolves
  the release tag to the benchmarked commit, and verifies its recorded binary
  digest against the checksum published on that exact tag.
- **Benchmark evidence must cover the complete generated RPC surface.** The
  collector derives the canonical wire-RPC set from the generated benchmark
  manifest and requires Go, Python, TypeScript, and PHP to report every identity
  exactly once with matching aliases, operation ids, dynamic counts, and
  independently normalized zero-failure rows. C# and Java remain explicit skips.
  Pages reruns the same gate against the manifest from the exact benchmarked
  commit before deploying release evidence. Schema v2 separately recomputes
  attempted, successful, capability-skipped, and failed counts, requires real
  finite latency/iteration evidence and exact fatal/history sets, and preserves
  capability skips as nonfatal evidence. Push/manual Pages runs without a fresh
  artifact accept only the pinned v0.4.28 JSON digest and reject committed
  schema-v2 green claims, closing predecessor-diff bypasses. The historical
  surface remains visibly non-green; new evidence must come through Release ->
  Benchmark.
- **Relational wire reads remain type-exact.** PostgreSQL arrays are decoded
  before broad scalar-name matches, preserving supported non-text arrays, NULL
  elements, and SQL NULL while decoder mismatches fail closed. MySQL
  `DATETIME(6)` remains zone-less and microsecond-exact instead of gaining an
  invented UTC offset.
- **Backup inventory remains readable during topology repair.** ListBackups
  requires an exact active project/store binding but no longer demands the full
  backup-execution topology merely to list its durable journal.
- **Backup state is isolated by tenant and project.** Run and policy schemas,
  RLS, indexes, conflicts, CRUD, export/import, scheduling, and retention carry
  first-class project ownership. Blank legacy ownership is quarantined, and a
  same-tenant project cannot list, guess, mutate, restore, or prune another
  project's backup state.
- **Vault secret destruction and its audit evidence commit together.** The
  irreversible multi-version crypto-shred now enqueues its exact-project outbox
  event inside the same PostgreSQL transaction, so an audit failure rolls the
  shred back instead of leaving an unaudited destroyed secret.
- **Notification event contracts match served delivery semantics.** Sent events
  partition by `recipient_ref`, opt-out suppression is a conditional durable
  event, and `RetryNotification` accepts only `FAILED` rows instead of
  resurrecting terminal `SUPPRESSED` decisions. Logs, templates, preferences,
  and delivery attempts now enforce first-class project ownership with
  tenant+project RLS and uniqueness; blank legacy ownership is quarantined
  rather than assigned to `default`.
- **The native contract advances to 7.0.0.** Backup's persisted tenant+project
  security boundary and Notification's corrected partition/emission contract
  are intentional database- and event-contract breaks. The independent native
  contract major and descriptor baseline move together instead of hiding either
  change behind the package patch version.
- **The declared Rust minimum is 1.88.** Repository source already relies on
  Rust 1.88 let-chains across core runtime and generation paths; CI now compiles
  all targets with that exact MSRV instead of advertising an untested 1.85.
- **PHP benchmark seed failures remain observable.** A failed native seed keeps
  its original gRPC code and detail, marks only dependent RPCs `SEED_BLOCKED`,
  and still emits a complete 381-RPC PHP report. Catalog lifecycle ordering is
  pinned as Stage, Activate, then Rollback.
- **Materialized views use the customer project's database.** Creation and TTL
  refresh route through the exact project PostgreSQL write authority, and the
  scheduler observes catalog activation/reload instead of capturing the default
  manifest at startup.
- **Projection repair is project-scoped.** Dead-letter grouping/requeue carries
  `project_id` through every canonical-store adapter, and projection workers do
  not dispatch through stale/default catalog or backend authority.
- **Native body projects are bound to the verified claim.** Config flag,
  Metering quota, and LiveQuery handlers validate tenant and project together
  before constructing their runtime context; a same-tenant request can no
  longer substitute another project's body identifier.
- **Unknown API-key usage remains zero and non-enumerating.** Usage statistics
  for an identifier with no key row return the empty/zero response instead of
  `NotFound`, preserving the endpoint contract without creating a key-existence
  oracle. Existing keys still pass the exact tenant/project authority guard.
- **Project-routing typos fail closed.** Startup rejects unknown routing-mode
  tokens and a blank `strict_with_default:` project; direct runtime parsing
  falls back to strict isolation instead of silently authorizing permissive
  access to unlabeled backend instances.

### Changed

- Exact project catalog state, rather than default fallback, is now the control-
  plane authority for activation responses and strict native-service admission.
- Maintained README/site/diagram inventory text matches the generated descriptor,
  while the coding skill links that inventory instead of duplicating a stale
  count. Current release/Python publishing examples use 0.5.9, and the version
  propagator now governs them; the consumer skill's Vault inventory reflects the
  generated 22-RPC surface. Historical v0.4.28 benchmark evidence keeps its
  original label.

## [0.5.8] - 2026-08-15

Patch release restoring tenant-backup import availability and closing the
release-evidence drift exposed by the v0.5.7 post-release benchmark. No wire
protocol change.

### Fixed

- **`RestoreTenant` no longer rejects every fresh destination because of its own
  journal row.** Restore deliberately records a target-scoped `RUNNING`
  `BackupRun` before opening the freshness transaction, but the guard then
  counted that exact row as pre-existing tenant state. The descriptor-resolved
  backup journal probe now excludes only the current restore id. Older backup or
  restore history and every tenant-authored relation still make the target
  non-fresh.
- **The release benchmark follows canonical project and credential contracts.**
  All SDKs use one UUID project identity, rotate a refresh token only once, run
  tenant-wide session revocation last, and authenticate again before the final
  self-purge. The harness therefore measures served RPC behavior instead of
  stale project codes or deliberately invalidated bearer tokens.
- **Pages cannot publish stale benchmark evidence from a validation-only run.**
  Push validation of the benchmark workflow no longer triggers a deployment.
  A genuine manual or post-release benchmark must succeed and supply its fresh
  `sdk-benchmark-results` artifact; a missing artifact fails the Pages build
  instead of falling back to the committed historical dashboard JSON.
- **Windows release builds no longer depend on a single mutable NASM feed.** CI
  uses the runner-provided Perl and first downloads a checksum-pinned NASM 3.02
  archive from the official distribution. If that host is unavailable, it uses
  the direct versioned Chocolatey package endpoint and verifies both the package
  and embedded official-installer hashes before use.
- **Generated UDB skill wrappers and references now match the 0.5.8 canonical
  guidance.** The OpenAI, Ollama, and plugin-reference copies no longer
  advertise 0.5.6 package commands; copied codebase/API inventories include the
  completed Vault lifecycle surface; and PR quick-gate CI now rejects future
  wrapper or reference drift before merge.

### Changed

- Maintained security, operations, native-service, changelog, generated-map,
  example, and site version references now describe the completed v0.5.7 audit
  wave and identify v0.5.8 as the current product/SDK release.

## [0.5.7] - 2026-08-14

Patch release closing a production-reported migration defect and a cluster of
defects found by the cross-subsystem audit it triggered. Two of them are
isolation boundaries on the served write path, and one returns PII to callers
that the `Select` verb masks it from, so `^0.5.6` consumers should upgrade. No
wire-protocol change. Operators running non-relational stores should read the
migration entry closely: an approved backend change that UDB cannot apply now
refuses to start instead of recording itself as done.

### Fixed
- **An approved `RequiresReview` migration operation is executed.** The startup
  gate accepted an approval plan, then `generate_delta_sql` filtered the very
  set it had just authorized down to `SafeAuto`, discarding every reviewed
  operation before rendering. A replaced foreign key therefore added its new
  constraint, silently kept the old one, and saved the manifest as current — so
  the next boot fast-started over the divergence and deletion stayed blocked by
  a constraint the plan said was gone. The startup lifecycle now selects its
  generator explicitly from the authorization decision, rendering review-required
  work only once the exact canonical change set has passed the approval gate;
  unattended paths still emit only `SafeAuto`, and `Blocked` work remains
  unrenderable on every path.
- **The migration plan shows the SQL that will run.** `udb plan` rendered its
  artifacts with the unattended generator, so an operator approved a plan whose
  SQL omitted the destructive DDL `serve` would then execute.
- **New operator control: `UDB_ACK_MANUAL_BACKEND_RECONCILIATION`.** Set it to
  `true` to assert that a backend change UDB cannot apply itself has been
  reconciled by hand; startup then proceeds and records the new manifest, and
  the assertion is reported as a startup warning rather than applied silently.
  This is the recovery path for the refusal below, and it is required: the diff
  is computed against the STORED manifest, which only advances after a
  successful start, so reconciling the store by hand does NOT clear the refusal
  on its own. It is deliberately independent of `allow_degraded_backend_startup`
  — degraded backend health must never imply "the operator fixed the schema".
- **A migration that cannot be applied refuses to start.** The approval gate
  validates the canonical change set (relational **plus** Qdrant / MongoDB /
  Neo4j / ClickHouse / S3), but the executor recomputed the relational half
  alone and dropped the rest after they had been counted, approved and hashed.
  Creates are covered by desired-state provisioning; the update and drop kinds
  have no executor at all, and now fail startup with the operations named
  instead of recording the manifest as applied over a store that never changed.
- **The delta renderer no longer fails open.** `render_delta_operation` ended in
  `_ => String::new()`, so any change kind without an arm produced empty SQL
  with no error — the mechanism by which approved work could vanish. The match
  is now exhaustive, making an unhandled kind a compile error.
- **`BeginTx` deletes are tenant-scoped.** The transaction apply loop executes
  planner SQL directly: it consults no bridged emitter and installs no request
  GUC, and the planner only checked that the caller *mentioned* the tenant
  column — the value came from the caller. A delete naming another tenant's id
  deleted that tenant's rows and reported success. Delete and update plans now
  carry a verified tenant **and** project predicate derived from the request
  context, so a cross-tenant or cross-project filter matches zero rows.
- **Soft-delete tables are tenant-scoped on `Delete`.** The bridged emitter
  declines every soft-delete table by design, so those deletes always fell back
  to the same unprotected planner SQL. Same fix, same seam.
- **`Update` is project-scoped.** It had a verified tenant predicate but none
  for project, so a caller in one project could update another's rows inside its
  own tenant. `Update` has no bridged emitter, so this was its only boundary.
- **`return_record: true` no longer returns unmasked PII.** Masking was a
  parameter of the row serializer and four of its five callers passed an empty
  set, so `Upsert`/`Update` returned `is_pii` columns in clear text to callers
  whose `Select` masked them — reachable by anyone holding `udb:write` via a
  no-op update. The mask is now derived from the table and the only remaining
  choice is an explicit `ClientVisible` / `InternalUnmasked` intent, the latter
  used solely by compare-and-swap preconditions.
- **Native aggregates fail closed.** The count/sum helpers ended in
  `.unwrap_or(0)`, making a missing row or absent alias indistinguishable from a
  real zero — which is how a quota check reads "nothing used" and admits a write,
  and why a typed-aggregate defect could not be diagnosed from its symptom. A
  shape defect is now an error; SQL `NULL` remains a legitimate zero.
  `GetThroughput` likewise surfaces a decode failure instead of reporting `0`.
- **Open CDC subscriptions no longer outlive their authority.** Long-running
  `PublishCDC` streams periodically re-resolve bearer sessions, API keys, mTLS
  bindings, tenant status, scopes, and the current policy decision. Revocation,
  suspension, policy withdrawal, credential expiry, or a changed scope set now
  terminates the existing stream instead of permitting delivery until disconnect.
  Admission permits, inflight accounting, and the effective deadline also live
  for the lifetime of the lazy response stream rather than ending at RPC return.
- **CDC replay and topic-policy failures now fail closed.** Malformed or pruned
  resume cursors are rejected instead of rewinding to the Unix epoch; journal
  SQL/decode failures terminate replay instead of looking like an idle stream.
  Topic policies load and reload as complete immutable generations, include
  disabled rows, share one ingress/publish/subscriber matcher, and make CDC
  unavailable when the current generation cannot be loaded.
- **Backups are coherent and project-authoritative.** `BackupService` resolves
  one active project catalog and canonical PostgreSQL write instance, then reads
  all tenant tables in one `REPEATABLE READ READ ONLY` transaction. The durable
  run and immutable manifest record the exact project, catalog checksum,
  instance, snapshot/WAL provenance, destination, and object keys. Restore and
  retention use that recorded identity instead of mutable process defaults;
  unsupported multi-instance project topologies refuse completion rather than
  advertising a fuzzy backup as atomic.
- **Vault cannot unseal from plaintext DEK material.** Secret/transit DEKs must
  be protected by an authenticated master-KEK envelope. Every served secret and
  transit operation pins one active-project PostgreSQL write authority before
  its first typed or raw access, and its outbox evidence reuses that same pin so
  weighted routing cannot split key material from its audit event.
- **Dynamic database credentials are tenant- and project-bound.** Issued
  PostgreSQL logins are read-only, have no memberships or RLS-bypass authority,
  receive only explicit relation grants, and enforce restrictive fixed-literal
  tenant/project policies independent of caller-mutable GUCs. Lease issuance is
  idempotent and replay-safe; revoke and emergency project revoke terminate
  sessions, remove policy/grant/role state, prove absence, emit durable evidence,
  and shred the KEK-wrapped recovery envelope at terminal revocation.
- **Native project ownership is enforced consistently.** Storage file
  operations, Scheduler jobs, and Workflow instances bind claim-first project
  authority on creation and apply the same ownership predicate to reads,
  mutations, idempotency replay, and outbox lineage. Non-representable project
  authority fails before database access instead of widening to tenant scope.
- **Storage GC-ledger readiness is scoped to its physical store.** A service or
  database that created `udb_storage.gc_intents` can no longer mark a different
  pool or a later schema lifetime ready through process-global state. Service
  clones share readiness for one binding; a newly bound service rechecks and
  recreates its ledger as required.

### Changed
- **`native_service_context(.., "")` is banned.** The empty project argument fell
  back to the `x-udb-project-id` request header, which the native entity layer
  then applied as a query predicate — a pattern that had already shipped three
  silent read defects. It was guarded by a ratchet holding 77 allowed call sites,
  which only ever stopped the 78th. All 77 were audited against their entity's
  proto and migrated: 58 to `tenant_only_native_service_context` (their entities
  declare no project column), 19 to an explicit
  `project_scoped_native_service_context` (they genuinely need it — `NOT NULL`
  project columns, write stamping, vector-store routing). The allowance list is
  deleted and the pattern is now a hard build failure.
- **Request scope resolves from the validated claim first.** `metadata_tenant_id`
  and `metadata_project_id` consulted the raw header before the bearer claim;
  they now prefer the claim and fall back to the header only where no claim
  context exists (the in-process loopback path).

## [0.5.6] - 2026-08-13

Patch release fixing three PostgreSQL data-plane defects reported from
production — one of which silently violated a declared unique key — plus two
internally-found gaps in the write boundary and the migration manifest. No
wire-protocol change; `^0.5.5` consumers should upgrade. Any consumer upserting
a partitioned entity should read the first entry closely, because the fix
converts a silent data-integrity failure into a refusal.

### Fixed
- **A PostGIS point can be written through `Update`.** The binder classified
  integer columns by substring, and `GEOGRAPHY(POINT,4326)` contains `INT`
  inside `POINT`, so a valid EWKB value was routed through the integer parser
  and refused with `expected integer, got "0101000020…"`. Type dispatch now
  matches the base type token, so `INTERVAL` and `MULTIPOINT` are covered too.
  Fixing the classifier alone was not enough: these columns have no assignment
  cast from a bound `text` parameter, so the statement then failed with SQLSTATE
  42804. `Select`/`Upsert`/`Delete` never hit this because they lower to neutral
  IR and get the compiler's `$n::TYPE` cast — `Update` alone is served from
  planner SQL and had no bridged emitter. It now casts through that same shared
  classifier, so the two write verbs cannot drift apart. `INET`, `CIDR`,
  `MACADDR` and `MACADDR8` shared the identical `Update`-only defect and are
  fixed by the same change.
- **`Upsert` on a partitioned table no longer silently writes a duplicate.**
  PostgreSQL cannot carry a unique index that omits the partition key, so UDB
  widens the generated primary key — and the emitted `ON CONFLICT` arbiter —
  with the partition column. When that column is server-owned
  (`exclude_from_insert`, generated, identity, auto-increment) the caller
  structurally cannot supply it, every insert receives a fresh server value, and
  the arbiter matches no existing row. PostgreSQL reported nothing: the upsert
  degenerated into a plain `INSERT` and wrote a second row for a key the entity
  declares unique. Such an upsert is now refused, naming both ways forward —
  include the partition column in `conflict_fields` and supply it, or use
  `Update`, which targets rows by filter and cannot insert. A caller-supplied
  partition column keeps working.
- **`null` and an empty array are typed by the column, not by the value.** The
  bridged emitter derived each parameter's type from the value it carried: a
  `null` bound as a `text` null, which a `VARCHAR` column accepted and an
  `INTEGER` column refused, and an empty array had no element to infer from so
  it bound as `jsonb`, which a `TEXT[]` column refused. Both surfaced as
  SQLSTATE 42804 (`value type does not match column`), while omitting the field
  instead tripped `NOT NULL` — so a nullable integer and an empty `TEXT[]` were
  unsaveable either way. A write placeholder is now cast to its declared column
  type whenever the bound value carries no type of its own, and an empty array
  takes its element type from that cast.
- **A native integer survives the protobuf wire boundary on every backend.**
  `google.protobuf.Value` carries one numeric kind (double), so a client sending
  a native integer arrived as `25.0` and every backend's binder saw a float
  where an `INTEGER` column was declared. v0.5.5 fixed this at the PostgreSQL
  binder alone; the integer is now recovered at the wire boundary, so MySQL,
  MSSQL, SQLite, MongoDB and Qdrant agree with PostgreSQL and both mutation
  verbs mean the same thing by a native integer. Only exactly-representable
  values inside JSON's interoperable safe-integer range are recovered — anything
  larger stays a float and still fails closed, where the documented
  decimal-string form covers the full `int64` range. Note for `JSONB` columns: a
  payload of `25.0` now stores as `25`, because protobuf cannot distinguish the
  two on the wire.
- **Native services own their migrations again.** The migration manifest
  resolved a schema's owning service through a hardcoded switch that mapped 8 of
  the 20 `udb_*` schemas the native protos declare. The other twelve — backup,
  config, control, embedding, idp, lock, metering, scheduler, search, vault,
  webhook and workflow — resolved to no owner and were filtered out. Their
  tables were still created, because bootstrap DDL is generated from the
  unfiltered schema set, but the diff/apply engine and the ledger manifest never
  saw them: schema evolution went unmanaged and `udb plan` / `drift` /
  `manifest-export` produced a manifest missing those tables. Owners now resolve
  from the descriptor-derived service registry, so adding a native service no
  longer requires extending a switch for its tables to migrate.

## [0.5.5] - 2026-08-11

Patch release fixing a mutation-verb inconsistency reported from production, a
WebAuthn assertion failure, and a restore path that could never succeed. No
wire-protocol change; `^0.5.4` consumers should upgrade.

### Fixed
- **`Update` now accepts a native client integer, exactly as `Upsert` does.**
  protobuf `Struct` carries every number as a double, so a client sending a
  native integer (Go `25`, JS `25`) reached the binder as `25.0`. `Update`
  refused it with `expected integer, got 25.0` while `Upsert` accepted the same
  value on the same column, so the correct encoding depended on which verb you
  called and a mistake surfaced as a server error rather than a constraint at
  the call site. Integral doubles now bind; a fractional or out-of-range double
  is still refused rather than silently truncated.
- **`AuthnService.FinishWebAuthnAuthentication` completes.** Consuming the
  WebAuthn challenge read a tenant-scoped message with an empty tenant, which
  the IR compiler refuses (`tenant_scope_required`), so a valid assertion still
  returned INTERNAL. The authenticated user's tenant is now threaded through.
- **`BackupService.RestoreTenant` can restore onto a fresh tenant again.** The
  fresh-target guard probed every tenant-scoped table, including
  `udb_metering.usage_events` — which metering populates for the target tenant
  when the restore RPC is admitted, before the probe runs. The guard therefore
  detected the broker's own bookkeeping and refused every cross-tenant restore.
  Platform-bookkeeping relations are now excluded from the probe; they are not
  restored from a backup either, so no tenant data can be masked.
- **Restore refusals name the blocking relations**, instead of only reporting a
  row count that gave an operator nothing to act on.

### Added
- A build-time ratchet (`cargo test`) over every
  `native_service_context(.., "")` call site. That helper falls back to the
  `x-udb-project-id` header, which the entity layer then applies as a query
  predicate — the defect behind three broken reads in 0.5.4. New occurrences now
  fail the build.

## [0.5.4] - 2026-08-11

Patch release fixing four native-service reads that failed against real
multi-project traffic, all surfaced by the post-release SDK benchmark. No
wire-protocol change; `^0.5.3` consumers should upgrade.

### Fixed
- **Native reads no longer inherit the request-context project as a filter.**
  `AssetService.GetAsset`/`ListAssets` and `NotificationService.GetNotification`
  built their read context with a helper that falls back to the
  `x-udb-project-id` header, which then reached the query as an extra predicate.
  Against a UUID-typed `project_id` a human project code (for example the
  default project) failed the bind outright with `INVALID_ARGUMENT`
  ("uuid params must be UUID strings"); where the column is textual it silently
  filtered out rows the caller owns, returning `NOT_FOUND` for a real record.
  These reads are now scoped by tenant only, matching `StorageService`. Writes
  are unaffected — they persist the project supplied on the request.
- **`AuthnService.FinishWebAuthnAuthentication` no longer fails INTERNAL.** The
  post-assertion passkey update (sign count, `last_used_at`) went through the
  generic native-store seam with an empty tenant, which that seam refuses
  fail-closed, so a successful assertion still surfaced
  "update WebAuthn passkey failed". The authenticated user's tenant is now
  threaded into the update.

### Changed
- The post-release SDK live benchmark harness seeds a disposable tenant for the
  privileged `AdminPurgeTenant`, a dedicated grantless service account for
  `TransferServiceAccountGrant`, and register-consistent descriptors for
  `FinalizeUpload`; the results collector no longer aborts on the Go harness's
  `NO-BODY` status. These are test-harness and CI changes only.

## [0.5.3] - 2026-08-09

Patch release making insert-via-Upsert reliable for database-generated keys and
making default-deny authorization substantially easier to bootstrap and debug.
No wire-protocol change; `^0.5.2` consumers should upgrade.

### Fixed
- **Upsert identity with generated serial primary keys.** When a record omitted
  its database-assigned primary key and contained two or more `*_id` fields, UDB
  could not name the returned row reliably. The planner now distinguishes the
  generated key from usable identity fields, honors explicit `conflict_fields`
  for disambiguation, and returns a named diagnostic when identity remains
  ambiguous instead of silently choosing the wrong field.
- **Conflict-field diagnostics.** Invalid `conflict_fields` errors now identify
  the accepted fields and the insert-via-Upsert shape needed for a generated
  primary key.

### Added
- **`udb authz seed`.** A straightforward offline Casbin policy seeder writes
  the enforced `udb_authz.policy_rules` rows atomically and idempotently, rejects
  obsolete `data.*` action aliases, and can emit a reproducible policy seed.
- **Actionable authorization denials.** Default-deny and no-matching-policy
  errors now name the three common mismatches—RPC action token, canonical tenant
  UUID, and proto message type—and point operators to `udb authz seed`.
- **UDB agent skills and operator guidance.** The Claude/OpenAI/Ollama skill
  package now covers service authentication, the three authorization surfaces,
  native-service routing, RPC/security inventories, and the UDB contributor
  codebase map, with deterministic wrapper/reference drift checks.

## [0.5.2] - 2026-08-07

Patch release closing a live-reported audit-integrity and write-path correctness
defect, each with a deliberate fail-without-fix regression test. No behavior
changes for correct callers; `^0.5.1` consumers should upgrade.

### Fixed
- **Audit log no longer records mutations that did not happen.** An `Update` (or
  `Delete`) whose filter matched zero rows returned success *and* emitted an
  `update`/`delete` audit event carrying a `resource_uri` and `checksum_sha256`
  that implied a modified record — the audit trail, a regulator-facing artefact,
  claimed a write that never occurred. No audit event is now emitted when a
  mutation affects zero rows, gated at every emission site: unary
  upsert/update/delete, bulk compare-and-set, and the `BeginTx` transaction path
  (which carried the same bug).
- **`column = NULL` filters are rejected on writes, symmetric with reads.** A bare
  `null` filter value — which every SDK produces when a caller passes a language
  `nil`/`None` — compiled to `col = NULL` on the update/delete path. That is always
  UNKNOWN in SQL, so it matched zero rows and reported success while changing
  nothing. The write path now returns a `Malformed` error directing callers to the
  `IsNull` operator (e.g. `{"col": {"$is_null": true}}`), the same guard the read
  path already enforced.

## [0.5.1] - 2026-08-07

Patch release closing seven consumer-reported defects that blocked upgrade,
schema evolution, and service-account deploys — each with a deliberate
fail-without-fix regression test. No behavior changes beyond the fixes; `^0.5.0`
consumers should upgrade.

### Fixed
- **Composite-key writes.** The row-revision store bound a composite primary key's
  NUL separator into the `text` `row_key`; PostgreSQL rejects `0x00` during bind
  decode, so every write to a multi-column-key entity failed fail-closed
  (`row revision store unavailable`). The diagnostic `row_key` now renders the
  separator as U+001F (the unique `revision_key` still hashes the original).
- **Service-account login.** `AuthnService/Login` → `AuthenticateBearer` now returns
  `ACCOUNT_KIND_SERVICE_ACCOUNT` instead of `ACCOUNT_KIND_UNSPECIFIED`: the account
  kind is minted as a JWT claim, decoded on the verified principal, and carried
  through every login/validate path.
- **Migration approval — dual-gate deadlock.** Startup no longer runs two
  independent approval gates that demanded incompatible operation counts (a
  from-empty artifact subset vs the full prior→current diff), which deadlocked any
  upgrade — and every subsequent schema change — carrying both bootstrap artifacts
  and a schema diff. One canonical change set is computed and approved once, before
  any mutation, and both apply phases authorize from it.
- **Migration approval — producer/consumer divergence.** `udb plan` and `serve` now
  share one `canonical_change_set` (`diff_manifests` + `diff_all_backends`), so the
  CLI and the startup gate agree on operation count. `udb plan --emit-approval-plan
  <path>` writes the exact `ExportedPlan` serve accepts (same `operations_hash`,
  `blocked` as an array) — so approving a migration no longer requires a deliberate
  failed startup to read serve's expected hash. On rejection serve now names the
  operations it computed.
- **DropUnique dropped the wrong object.** Removing a column-level `unique: true`
  emitted `DROP INDEX IF EXISTS "<schema>"."<column>"` — the column name, not the
  `uidx_<schema>_<table>_<column>` index UDB created — so it silently no-op'd under
  `IF EXISTS`, recorded itself `applied`, and left the unique index (and its
  409-on-the-second-row) in place forever. It now names the generated index.
- **Concurrent bootstrap DDL race.** Startup applied bootstrap SQL artifacts in
  parallel by default, racing on shared PostgreSQL catalog rows
  (`tuple concurrently updated`) and crash-looping non-deterministically. DDL
  concurrency now defaults to **serial**; opt into parallelism with
  `UDB_DDL_CONCURRENCY=N`, which additionally retries the transient error and names
  the cause on exhaustion.
- **Cross-package enum codegen.** `udb sdk generate` (Go entity layer) now qualifies
  an enum declared in a different proto package by importing that package via its
  own `go_package`, instead of failing closed on a NOT NULL cross-package enum.

## [0.5.0] - 2026-08-05

First "usable" stable release: a broad security/correctness remediation across the
served path plus new native primitives, with a revert-proof, served-path end-to-end
test for every fix. Semver-minor (behavior changes), so `^0.4.x` consumers do not
auto-pull it.

### Fixed
- **Cross-tenant isolation.** Object-store keys and search/vector point-ids are now
  tenant-namespaced (a tenant can no longer read/overwrite/delete another tenant's
  objects or clobber their vectors); the Postgres IR compiler AND's a
  defense-in-depth tenant predicate into reads/updates/deletes, so a body-supplied
  foreign `tenant_id` filter affects zero rows.
- **Wire-codec round-trip.** Postgres `real`/non-text arrays, MySQL/MSSQL temporal
  columns (incl. bare MSSQL `DATE`/`TIME`), Cassandra typed columns, `BYTEA` bytes,
  and JSONB structured values now round-trip exactly instead of collapsing to NULL,
  empty, or double-encoded strings.
- **Idempotency.** Mutation dedup binds a canonical request hash — the same key with
  different inputs conflicts (`FAILED_PRECONDITION`) instead of replaying a bogus
  success; the claim runs before CAS, so a response-loss retry replays the stored
  outcome rather than a spurious precondition failure.
- **Soft-delete.** A Delete on a `soft_delete` entity stamps the tombstone column via
  an UPDATE instead of physically deleting the row, and ordinary reads exclude
  tombstoned rows by default.
- **2PC recovery.** XA commit/rollback are idempotent (an already-terminal xid is no
  longer false-escalated to manual review) + a MySQL orphan-prepare sweep.
- **Projection read fence.** Read-your-writes matches projection work by its natural
  key and counts FAILED/DEAD_LETTER tasks as pending, so a projection-backed read
  genuinely waits (or fails honestly) instead of clearing vacuously.
- **JWKS bearer verification.** The JWKS fetch is timeout-bounded and forced refreshes
  are rate-limited, closing an unauthenticated blocking-DoS on JWKS-mode brokers.
- Resource-op SQL-injection guard; webhook signing secret sealed at rest; OIDC issuer
  locked to the resolved provider; CDC events carry verified actor/correlation.

### Added
- Opaque broker-managed **row revision** with expected-revision CAS on Update/Delete.
- **Lock-fencing at mutation commit** (`lock_name` + `fencing_token` validated against
  LockService in the same transaction).
- Bounded, request-hash-idempotent **bulk CAS** (`DataBroker.BulkCas`).
- Durable, GC-converging **hard delete** for storage objects.
- Privileged cross-tenant **`TenantService.AdminPurgeTenant`**.
- **`BeginTx` expected-CAS** + `cdc_required` fail-closed delivery.
- Atomic **`AuthnService.TransferServiceAccountGrant`** (service-identity cutover under
  revision CAS) + enterprise native-connection SDK facades.

## [0.4.37] - 2026-08-04

Go SDK consumer fixes. Two independent defects — each confirmed against the
checksum-verified official v0.4.36 release — blocked proto-driven Go consumers:
one made generated entity code uncompilable, the other put a stale second
tenant/project identity on every enterprise call. No proto changes; no Rust
runtime data-path changes (the fixes are the Go code generator and the Go SDK).

### Fixed
- **`sdk generate --project-proto --lang go` emitted uncompilable Go.** Two
  generator defects in `src/cli/sdk_gen.rs`:
  - Go field/getter identifiers were derived by a home-grown
    underscore-to-PascalCase pass that mishandled letter/digit boundaries, so a
    legal proto field like `proj4text` produced `Proj4text` / `GetProj4text`
    where `protoc-gen-go` emits `Proj4Text` / `GetProj4Text`. `go_pascal` is now
    a faithful port of `protoc-gen-go`'s `GoCamelCase`, verified against real
    generated getters (`checksum_sha256`→`ChecksumSha256`,
    `int64_values`→`Int64Values`, `p99_execution_ms`→`P99ExecutionMs`, …).
  - Every typed repository `List` declared an `int64` count but returned
    `udbclient.Page.TotalCount` (`int32`) directly, which does not compile. The
    count is now widened with `int64(page.TotalCount)`.
  Guarded by Rust unit tests (`go_pascal` contract + rendered-List widening) and
  a Go compile-contract test (`TestEntityRepoGeneratedContract`) pinning the
  generated repository call shapes against the shipping SDK.
- **`ConnectEnterprise` carried a stale pre-login tenant/project identity.** The
  enterprise connection's metadata interceptor was owned by a `GeneratedClient`
  distinct from `Udb.Generated`, so tenant adoption and bearer refresh updated a
  different object while the connection kept appending the pre-login
  `x-tenant-id` / `x-udb-project-id` hints next to the canonical values — so
  `DataContext` / `NativeContext` calls carried conflicting repeated identity
  headers. Fixed by (a) exposing the single interceptor-owning `GeneratedClient`
  as `Udb.Generated` (`sdk/go/udbclient/project.go`) so adoption/refresh update
  the object the connections actually read, and (b) making the interceptor
  idempotent (`sdk/go/udbclient/generated_client.go`) so it injects a header only
  when the caller has not already set it — restoring the "each identity header
  exactly once" contract. Guarded by a served-path test
  (`TestConnectEnterpriseEmitsCanonicalSingletonMetadata`) asserting singleton
  canonical headers on data + native calls across separate targets and after a
  forced bearer refresh.

## [0.4.36] - 2026-08-02

Capability-lie remediation release. A 10-agent capability-lie sweep found ~24
confirmed cases where a capability was configured, advertised, or security-gated
but its runtime was a stub, no-op, or silent fallback (the same bug class as the
0.4.34/0.4.35 audit-sink saga). This release fixes them, adds two anti-drift
lints so they cannot silently return, and repairs a migration-ledger schema skew.
No proto changes.

### Security
- **CRITICAL — SAML XML Signature Wrapping (XSW) authentication bypass.**
  `validate_response` verified *a* signature over the SAML response but never
  bound the signed reference to the assertion it returned, so an attacker could
  wrap a validly-signed assertion around an injected, unsigned attacker
  assertion and authenticate as anyone. `idp/saml.rs` now rejects any response
  that does not carry exactly one assertion, and requires the signed reference to
  point at either the `Response` (single-assertion-guaranteed) or the exact
  `Assertion` it returns — appended/wrapped assertions are refused fail-closed.
  Covered by `saml_wrapped_response_is_rejected`,
  `saml_appended_second_assertion_is_rejected`, and a positive
  `saml_single_signed_assertion_still_passes`.

### Fixed
- **Cross-tenant read/write leaks in several compilers and executors.** Tenant/
  project scope predicates were dropped on paths that advertised tenant
  isolation: MySQL/SQLite/MSSQL read+delete+search (`scoped_where_clause` now
  ANDs the context predicate into every filter), MongoDB and Neo4j aggregate
  (`$match`/`WHERE` now inject the tenant predicate), Redis (executor is now
  namespaced `udb:{project}:{tenant}:` and scans are scoped), and Qdrant search
  (tenant filter is ANDed into the query). Pinecone by-id fetch/delete now
  **fail closed** under a tenant context instead of silently returning an
  unscoped point. Each is covered by a predicate-injection test.
- **Security annotations that were decorative are now enforced.** The
  `method_security` tower layer projected only 12 of the 16 authored
  `EndpointSecurityContract` fields; assurance-level (AAL/`acr`), owner-field,
  rate-limit policy ref, and audit-event-type are now carried onto
  `MethodSecurity` and enforced (`required_assurance_level` gate, body-owner
  guard, policy-keyed public rate-limit bucket). A new exhaustive
  `every_endpoint_security_field_is_enforced_or_whitelisted` test destructures
  the contract so a newly-added field fails to compile until it is wired.
- **Fail-closed / honest-close corrections.** `UDB_AUDIT_SINK=kafka` is now
  rejected at config-validate time (the Kafka audit transport is unimplemented,
  previously a silent stdout fallback); Weaviate `NOT` filters are rejected
  instead of silently dropped; capability advertisements corrected to match
  runtime (ClickHouse `supports_rls=false`, object stores `supports_ttl=false`,
  Cassandra `supports_schema_migration=false`); the signing-provider and
  compliance-encryption facts are now derived from real configuration at startup
  (fail-closed) rather than advertised unconditionally; the local rate-limiter
  path no longer opens wide when Redis is absent.
- **Two anti-drift lints** (`plugin.rs`, `method_security.rs`) assert every
  advertised backend capability matches a runtime allowlist and every security
  contract field is enforced-or-whitelisted, so this bug class fails the build
  if it returns.
- **Migration ledger: `migration_runtime_state.last_error_id` schema skew.** The
  Go UDB migration service's runtime-state upsert writes `last_error_id`, but the
  canonical DDL (`control::tracker`) never declared the column and — unlike
  `schema_migrations` — carried no `ALTER ... ADD COLUMN IF NOT EXISTS` upgrade
  guard, so a database bootstrapped by an older UDB failed at bring-up with
  `cannot persist initial runtime state: pq: column "last_error_id" ... does not
  exist`. The DDL now declares `last_error_id BIGINT NULL` (soft reference to
  `migration_error_log.id BIGSERIAL`) and adds the idempotent upgrade guard so
  pre-existing tables self-heal on the next bootstrap.

## [0.4.35] - 2026-07-31

### Fixed
- **Durable Postgres audit sink: silent-failure hardening + startup validation.**
  0.4.34 wired the sink correctly (verified: a real-crate test now exercises
  `UdbConfig::from_env()` + `emit_audit` against Postgres and asserts a row
  persists), but its background writer **exited on the first `ensure_table`/insert
  error and never came back** — so a transient or misconfigured sink silently sent
  every subsequent audit to stdout, indistinguishable from a working sink. Three
  changes make it robust + observable:
  - The writer **never exits** — it lazily (re)creates the table until it sticks and,
    on any failure, falls back to stdout *visibly* and retries the next event
    (self-healing instead of permanently dead).
  - New **startup fail-closed validation** (`serve()`): when `UDB_AUDIT_SINK=postgres`
    the audit table is created at boot; success logs `durable Postgres audit sink
    ready`, failure logs the exact reason and, under fail-closed, **refuses to
    start** — a broken sink can no longer masquerade as working.
  - The audit table PK is now `BIGSERIAL` (no `gen_random_uuid`/pgcrypto dependency;
    works on older Postgres).
  Covered by `runtime::core::audit::tests::repro_real_config_and_emit_persists`
  (env-gated live test).

## [0.4.34] - 2026-07-31

### Fixed
- **The Postgres audit sink now actually persists (was a stub → stdout).** With
  `UDB_AUDIT_SINK=postgres` the data-plane audit emitter had never written to
  Postgres — it fell back to stdout with a "transport not yet wired" warning, while
  the fail-closed startup gate accepted `UDB_AUDIT_SINK=postgres` as a *durable*
  sink. That was a capability lie: enabling fail-closed left the audit trail going
  to stdout. `core::audit` now wires a real, self-creating durable sink — a bounded
  background writer creates the configured `UDB_AUDIT_PG_TABLE`
  (`CREATE SCHEMA`/`CREATE TABLE IF NOT EXISTS`, mirroring the auth-plane
  `PostgresAuditLogSink`) and INSERTs each committed mutation off the request path.
  Best-effort: a full queue / unreachable audit DB falls back to stdout with a
  warning (never silently dropped) and never blocks the write. The table name is
  validated as a safe qualified identifier before interpolation. `UDB_AUDIT_SINK`
  (data-plane mutations) and `UDB_AUDIT_EXPORT_POSTGRES=1` (auth-plane
  `udb_system.auth_audit_log`) are two independent sinks — both documented in
  `docs/enterprise-deployment.md` §2.1.
- **Fail-closed durable-audit gate is now honest.** `durable_audit_sink_declared()`
  no longer accepts `UDB_AUDIT_SINK=database`/`durable` (which resolve to
  `AuditSinkKind::None` — *no* sink); it accepts only the values that actually
  persist (`postgres`/`pg`, now real) plus the auth-plane export
  (`UDB_AUDIT_EXPORT_POSTGRES=1`). Kafka remains an honestly-warned stdout fallback.

## [0.4.33] - 2026-07-31

Closes the three items that shipped deferred in 0.4.32 (no-deferral follow-through)
plus a fail-closed enterprise-bring-up ergonomics fix. No proto changes.

### Fixed
- **`UDB_FAIL_CLOSED` no longer forces the broker to terminate its own TLS.** The
  fail-closed AUDIT posture (deny-on-error + durable audit sink) was coupled to the
  full production transport checklist, so enabling it demanded TLS + service identity
  even behind an edge TLS proxy / service mesh. New **`UDB_TRUSTED_TRANSPORT=true`**
  is an explicit, auditable operator acknowledgment that transport is secured
  upstream — it satisfies the TLS + service-identity gates while every other gate
  (auth, durable audit sink, header-scopes) stays enforced. Unset, transport gates
  are unchanged. The startup gate already lists all unmet requirements at once; the
  new escape hatch + a from-scratch fail-closed recipe are documented in
  `docs/enterprise-deployment.md` §2.1. Covered by a `validate_production` unit test.

### Added / Changed (deferred-item completion)
- **Scheduler per-job timezone / DST.** Cron next-fire is now DST-correct: a job
  carries an IANA timezone in its existing opaque `payload` JSON (`{"timezone":
  "America/New_York"}`, no proto change), evaluated via the newly-vendored
  `chrono-tz`. Spring-forward gaps resolve to the post-transition instant and
  fall-back overlaps to the earlier instant; a process default is set with
  `UDB_SCHEDULER_TZ`; an invalid zone name is rejected at `CreateJob`. No timezone =
  UTC (unchanged).
- **Backup retention + scheduled backups now actually run.** A leader-elected
  maintenance worker (`WORKER_BACKUP_RETENTION`, interval `UDB_BACKUP_MAINTENANCE_INTERVAL_SECS`,
  default 300s) enumerates every enabled `BackupPolicy` cross-tenant and enforces
  `retention_days`/`max_retained_backups` (prunes old runs + their objects) and fires
  due scheduled backups (`schedule_cron`) via the internal backup routine — closing
  the previously-inert policy columns / unbounded run accumulation.
- **Vault: full encrypt-vs-HMAC key isolation.** A dedicated `hmac-sha256` transit
  algorithm makes the three purposes mutually exclusive — an `aes256-gcm-siv` key
  encrypts/decrypts only, `ed25519` signs only, `hmac-sha256` MACs only (0.4.32
  separated signing from HMAC but still let one symmetric key both encrypt and MAC).

## [0.4.32] - 2026-07-30

Native-service upgrade — batch 2, plus a full cross-service gap-review hardening
pass. Continues the no-deferrals plan implementation across all five services
(transactional Vault, Embedding erasure + Matryoshka, richer Notification delivery,
Search/vector tenant-filter completeness, LiveQuery knobs) plus a fail-closed
audit-sink ergonomics fix — and then closes a read-only gap review spanning every
native service (see the security-hardening section below). No proto changes.

### Fixed
- **Fail-closed startup now accepts the durable Postgres audit sink.**
  `UDB_FAIL_CLOSED=true` previously refused to start unless an external HTTP sink
  (`UDB_AUDIT_SINK_URL`) was set. It now also accepts the always-on durable
  Postgres audit log via `UDB_AUDIT_SINK=postgres` (the append-only
  `udb_system.auth_audit_log` that `udb compliance evidence` chain-exports), so a
  single-node Postgres deployment can enable fail-closed with no external endpoint.
- **Weaviate/Pinecone vector search now enforce the tenant filter** (same
  cross-tenant leak class fixed for Elasticsearch in 0.4.30): the Weaviate GraphQL
  `where` and the Pinecone metadata `filter` now AND in the tenant scope, over the
  `_tenant_id` the generic upsert already stamps.
- **Vault `PutSecret` CAS is now a true compare-and-swap** — a concurrent
  duplicate on `(tenant_id, secret_path, version)` returns a clean retryable
  `ABORTED` (was an opaque Postgres `23505`). `rotate_transit_key` and multi-version
  `DestroySecret` are now single transactions (no observable "zero ACTIVE key"
  window; accurate destroyed count). `get_secret` no longer returns soft-DELETED
  values for an explicit version.
- **Embedding right-to-erasure completes on non-Qdrant backends.** Teardown /
  row-delete / stale-chunk cleanup now dispatch portable deletes for
  Elasticsearch / Pinecone / Weaviate (previously Qdrant-only, so a delete event
  stayed pending forever). ES also gains a real collection alias at register
  (fixing ES Retrieve).
- **Notification: a missing recipient address no longer loops forever** — an
  EMAIL/SMS/PUSH intent with no address is recorded FAILED with a clear reason
  (IN_APP, legitimately address-less, is unaffected).

### Added / Changed
- **Vault:** per-tenant transit-op quota (`UDB_VAULT_TRANSIT_QUOTA_PER_MINUTE`);
  keyset-paginated `ListSecrets` with a correct `total_count`; dedicated audit
  topics for `GenerateDataKey`/`Rewrap`. DEKs, the AEAD nonce, and dynamic DB
  passwords now draw from the OS CSPRNG (`random_32_bytes`/`random_bytes`) instead
  of concatenated UUIDs.
- **Embedding:** Matryoshka truncated-dim serving is now active end-to-end
  (`UDB_EMBEDDING_MATRYOSHKA_STRATEGY`); `parent_window` neighbor-text gathering is
  one payload-only query instead of N ANN searches; the two duplicate similarity
  implementations are one shared metric-aware `similarity`; registry/chunking/
  backoff defaults are `UDB_EMBEDDING_*` knobs.
- **Notification:** per-provider auth schemes (bearer / api-key / basic /
  hmac-SHA256), a provider idempotency key (crash-retry can't double-send), real
  response message-id extraction (header or JSON path), and a bounded HTTP
  delivery timeout (`UDB_NOTIFICATION_DELIVERY_TIMEOUT_SECS`).
- **Search:** operator-tunable RRF `k` and per-index/modality `fusion_weights`
  (`UDB_SEARCH_RRF_K`, `UDB_SEARCH_FUSION_WEIGHTS`); per-index vector distance and
  text analyzer resolved from the index `metadata_json`.
- **LiveQuery:** `UDB_`-prefixed buffer knob (with a deprecated alias),
  snapshot-limit env overrides, and an idle-stream keepalive
  (`UDB_LIVEQUERY_KEEPALIVE_SECS`) so LB idle timeouts don't reap healthy streams.
- **Shared:** a constant-time byte-slice compare (`constant_time_eq_bytes`).
- **Search full-text execution (`SEARCH_MODE_TEXT`).** A text-only query now
  executes a real Elasticsearch `multi_match` (BM25) over the stored text,
  tenant-scoped, via a new mediated `vector_text_search` seam (no proto change) —
  previously it failed closed. Qdrant text-only stays fail-closed with a typed
  capability error (needs a payload full-text index) pending its mediated path.
- **Notification exponential backoff + DLQ.** The delivery worker now spaces
  retries with capped exponential backoff + equal jitter (keyed off the attempt
  row's `attempt_count`/`updated_at`, no schema change;
  `UDB_NOTIFICATION_BACKOFF_BASE_SECS`/`_CAP_SECS`), and emits a once-per-log
  dead-letter event (`udb.notification.delivery.dead_lettered.v1`) when a log is
  exhausted, so operators can alert on DLQ depth.
- **LiveQuery durable resume + metrics + one tenant-scope boundary.** `Subscribe`
  accepts an `x-udb-livequery-resume` cursor and replays missed changes from the
  durable CDC journal (bounded by `UDB_LIVEQUERY_RESUME_REPLAY_LIMIT`) before
  handing off to the live feed, stamping each change's `event_id`. The delta path
  is now metered (forwarded / dropped-by-scope / dropped-by-filter / backpressure
  / lag counters + a per-tenant active-stream gauge). The tenant-scope check now
  delegates to the canonical engine-tail matcher (one leak boundary, no divergent
  copy).
- **Vault fail-closed sealing.** `UDB_VAULT_REQUIRE_MASTER_KEY=true` seals the
  vault when only a dev plaintext passthrough is available, so a secrets engine
  never silently stores DEKs in the clear regardless of the global dev default.
  (The concrete external `AwsKmsProvider` remains omitted by the project's
  documented offline-vendoring constraint.)

### Security hardening (cross-service gap-review pass)
A read-only gap review spanning every native service surfaced (and this release
fixes) the following. No proto changes; all fixes are fail-closed.

- **Backup: cross-tenant restore is now gated on the caller's verified claim, not a
  request bool.** `RestoreTenant` previously trusted `allow_cross_tenant` from the
  wire; it now requires the caller to be a cross-tenant admin (`is_cross_tenant_admin()`)
  and derives the privileged flag from the claim — closing an inject-A's-rows-into-B
  path (principals/api-keys/credentials). Restore also verifies the backup manifest
  against its recorded SHA-256 and treats an empty per-table checksum as a failure
  (an object-store writer can no longer repoint objects at another tenant's artifact),
  and the fresh-target guard now runs inside the restore transaction.
- **Webhook: SSRF hardening.** Delivery no longer follows redirects
  (`redirect::Policy::none()`, so a `302`→link-local/metadata is refused), pins the
  validated IP for the connection (closes a DNS-rebind TOCTOU), applies a per-delivery
  timeout (`UDB_WEBHOOK_DELIVERY_TIMEOUT_MS`) with bounded per-tenant concurrency
  (`UDB_WEBHOOK_DELIVERY_CONCURRENCY`, no head-of-line blocking), and signs a
  timestamp with the body (`X-Udb-Timestamp`) to bound replay.
- **Auth: break-glass can no longer be self-asserted.** The governance authorizer now
  authorizes `break_glass` against the caller's VERIFIED `break_glass_admin` role
  (over-the-wire path) instead of the request-supplied `actor.break_glass` bool — a
  caller can no longer skip the standing-scope / SoD gate.
- **Tenant: SUSPENDED/INACTIVE now has enforcement teeth.** A fail-closed request-time
  status gate rejects a suspended tenant's live tokens immediately (not at TTL);
  `CreateTenant` now validates/authorizes `parent_tenant_id` and no longer discloses an
  existing tenant's UUID/status (or emits a spurious `tenant.created`) on a duplicate code.
- **Workflow: sagas actually compensate.** A timed-out/failed saga with completed steps
  now transitions to COMPENSATING (reverse-order compensation) instead of settling FAILED
  with steps left applied; COMPENSATING instances are non-signalable; the signal path
  serializes with `SELECT … FOR UPDATE` (no lost signal); and the previously-dead
  `WAITING_SIGNAL`/`pending_signal` path is now a working opt-in signal-gated wait.
- **Vault: transit hardening.** `BatchDecrypt` is bounded
  (`UDB_VAULT_MAX_BATCH_DECRYPT_ITEMS`); the per-tenant transit quota now also charges
  the KEK-unwrapping RPCs (`Decrypt`/`BatchDecrypt`/`GenerateDataKey`/`Rewrap`/`Verify`);
  transit keys are purpose-separated (an Ed25519 signing key is refused for HMAC and vice
  versa); the fixed-window quota counter evicts idle tenants.
- **Embedding/Search (portable vector backends): reachable read bugs fixed.** Weaviate
  vector search was a silent stub returning `[]` — it now parses the response (cosine-
  normalized) and reads/writes the same physical class; Elasticsearch-backed hits are
  unwrapped to the flat point shape, fixing empty provenance AND an own-tenant leak of
  internal keys + the raw vector; ES vector scores are de-offset to true cosine; and
  `parent_window` neighbor scoping is honored on portable backends.
- **LiveQuery durable resume: no lost sends, filter parity.** Replay now accumulates up
  to the limit of the subscriber's OWN in-scope rows via a bounded paginated scan
  (previously the SQL `LIMIT` was applied before the tenant filter, so a busy shared
  entity could dilute a tenant's replay to empty), and replayed changes now pass the
  subscriber's `user_filter` like the live path.
- **Notification: exactly-once dead-letter + opt-out on retry.** `RetryNotification` now
  resets the delivery-attempt budget in the same transaction as the resurrection (no more
  double dead-letter / one-shot retry), and recipient opt-out is enforced by the delivery
  worker and retry path, not only at send.
- **Scheduler:** atomic per-tenant job quota (advisory-lock + guarded insert, no TOCTOU),
  quadrennial/leap-day crons resolve instead of being rejected/dead-lettered, fired events
  carry a stable `(job_id, scheduled_slot)` idempotency key, and CRUD outbox events are
  enqueued inside the mutation transaction.
- **Storage/Asset/Lock:** derived asset writes stamp server-side encryption
  (`UDB_STORAGE_SERVER_SIDE_ENCRYPTION`; native presign-PUT fails closed when SSE is
  required), object keys reject path traversal, `StartPipeline` compensates to FAILED
  instead of stranding RUNNING rows, and lock fencing tokens are per-lock monotonic under
  the lease (independent of the outbox) and bumped on lapsed re-acquire.

Known fast-follow (tracked, not shipped in 0.4.32): the leader-elected trigger for backup
retention pruning + scheduled backups (the tested `prune_tenant_backups` primitive ships,
its cross-tenant scheduler spawn does not); per-job scheduler timezone/DST (needs a proto
field + a vendored IANA-tz crate); and full encrypt-vs-HMAC key isolation on a shared
symmetric key (this release separates signing from HMAC).

## [0.4.31] - 2026-07-30

Native-service upgrade release. Implements the tractable items from the
native-service upgrade review across all five reviewed services (notification,
vault, search, live-query, embedding): correctness/robustness fixes, per-tenant
fairness, and operator-tunable knobs. No proto changes; no feature removed.

### Added / Changed
- **Notification — bounded delivery retries.** A failing provider was re-POSTed
  every interval forever. A delivery attempt now moves the log to `FAILED` once
  `attempt_count` reaches `UDB_NOTIFICATION_DELIVERY_MAX_ATTEMPTS` (default 6),
  removing it from the PENDING work queue; below the ceiling it stays PENDING
  (bounded auto-retry). `RetryNotification` now retries only the `FAILED` state
  (resetting the retry budget) — it no longer resurrects `SUPPRESSED` (opted-out)
  rows, closing an opt-out-bypass compliance gap. Delivery batch size is now
  `UDB_NOTIFICATION_DELIVERY_BATCH`. (Exponential backoff + DLQ need a
  `next_attempt_at` schema column and remain deferred.)
- **Vault — hardening.** `DestroySecret` now requires the `confirmation_token`
  to equal the target `secret_path` (an irreversible crypto-shred must name its
  path). The DB-lease reaper logs-and-continues on a single un-droppable role
  instead of aborting the batch. `BatchEncrypt` rejects oversized input
  (`UDB_VAULT_MAX_BATCH_ENCRYPT`, default 256). New knobs
  `UDB_VAULT_MAX_VERSIONS_SCAN` and `UDB_VAULT_DB_LEASE_REAPER_BATCH`.
- **Search — pagination, robustness, mode.** Deep pagination is fixed: each index
  now fetches `offset + page_size` candidates (a fixed `top_k` made page 2+
  empty) and a `next_page_token` is emitted only when a further page truly
  exists. A single failing index is skipped (logged) so a multi-index search
  still returns the healthy indexes; the whole search fails only if every target
  errors. `SearchRequest.mode` is now enforced (a mode that contradicts the
  supplied `query_text`/`query_vector` is rejected). New knobs
  `UDB_SEARCH_MAX_TOP_K`, `UDB_SEARCH_MAX_INDEXES_PER_TENANT`.
- **LiveQuery — global cap + classification.** A process-wide concurrent-stream
  ceiling (`UDB_LIVEQUERY_MAX_STREAMS_GLOBAL`, default 4096) now bounds total
  streams across tenants (the per-tenant budget alone did not). `change_op` now
  classifies `upsert` by the topic verb (created/inserted → INSERT) instead of
  always UPDATE. Predicate values coerce to numeric only in canonical form, so a
  leading-zero business string (e.g. `"0123"`) binds as text, not `123`.
- **Embedding — unified similarity + fairness.** The two duplicate cosine/
  similarity implementations are consolidated into one metric-aware
  `similarity(metric, a, b)` (higher-is-better for every metric), making the
  fresh-buffer and MMR paths consistent (fixing EUCLID read-your-writes ordering).
  The read-your-writes fresh buffer is now sharded per tenant, so a write-heavy
  tenant can no longer evict another tenant's entries. The rerank URL, candidate
  cap, MMR over-fetch, and rerank timeout are now config knobs
  (`UDB_EMBEDDING_RERANK_URL`/`_MAX_CANDIDATES`/`_MMR_OVERFETCH`/
  `_RERANK_TIMEOUT_SECS`).

### Deferred (tracked in the private upgrade plan)
- Notification exponential backoff + DLQ; shared native-delivery worker.
- Vault transactional `PutSecret` CAS / atomic `rotate_transit_key`; KMS master KEK.
- Search full-text-only execution path (`SEARCH_MODE_TEXT`).
- LiveQuery durable resume from the CDC journal; delta-path metrics.
- Embedding non-Qdrant portable teardown; Matryoshka truncated-dim cutover.

## [0.4.30] - 2026-07-30

Native-service hardening release. A rigorous review of the incomplete native
thin-layer services (vault, search, embedding, live-query, notification) fixed
one CRITICAL cross-tenant data leak, two cross-tenant/isolation bugs, and two
correctness bugs, plus a security-crypto de-duplication. No feature was removed.

### Breaking / wire changes
- None. No proto changes; all fixes are behavioral/internal.

### Fixed
- **CRITICAL — Search (Elasticsearch) cross-tenant read leak.** The Elasticsearch
  vector-search path ran `match_all` and ignored the tenant filter entirely, and
  ES documents carried no tenant tag — so an ES-backed index could return other
  tenants' documents. The generic (non-Qdrant) vector upsert now stamps
  `_tenant_id`/`_project_id` onto every point payload (mirroring the Qdrant
  executor's write-time stamp), and the ES `_search` query now AND's in a `term`
  filter on `payload._tenant_id.keyword`. (Operators with a custom ES mapping must
  ensure `_tenant_id` is keyword-indexed.)
- **Notification cross-tenant denial-of-delivery.** `ReportDelivery` never
  verified the `log_id` belonged to the caller's tenant, and the delivery worker's
  dedup had no tenant predicate — so one tenant could stamp a `DELIVERED` attempt
  on another tenant's notification and suppress its real delivery. `ReportDelivery`
  now checks log ownership (returns `NotFound` otherwise) and the worker dedup is
  tenant-scoped.
- **LiveQuery filtered deltas silently dropped.** The change-row extractor did not
  unwrap the UDB CDC envelope's `payload`, so every *filtered* live subscription
  evaluated predicates against the envelope (never the row) and dropped all
  matching deltas; change frames also shipped the wrong shape. `payload` is now a
  recognised row key.
- **Embedding hybrid `Retrieve` returned nothing under a score threshold.** A
  cosine `score_threshold` was applied to RRF-fused hybrid scores (a different
  scale), wiping every hybrid result. The threshold is now applied only on the
  vector-only path (where the engine already enforces it) and skipped on the
  hybrid path.
- **Search discarded engine relevance scores.** A single-index search had its
  cosine score overwritten by the positional RRF value; single-index results now
  keep the engine's own relevance score (RRF still fuses across multiple indexes).

### Changed
- **Notification delivery status now transitions** `PENDING → SENT → DELIVERED`
  (forward-only, tenant-scoped) when a delivery succeeds or is reported, so
  `GetNotification`/`GetDeliveryStats` reflect real outcomes instead of reporting
  every delivered notification as `PENDING`.
- **Vault HMAC-SHA256 de-duplicated** onto the shared, test-covered
  `runtime::security::hmac_sha256` primitive (verified byte-identical via an
  RFC 4231 known-answer test) instead of a second hand-rolled copy. No behavior
  change.

## [0.4.29] - 2026-07-29

The rate-limit ergonomics + per-key release. Fixes a discoverability trap where
the DataBroker per-tenant limit read as an un-overridable hardcoded 1000/60s, and
adds opt-in per-key budgets so one principal can no longer exhaust a whole
tenant.

### Breaking / wire changes
- None. All changes are additive and config-only; there are no proto changes.
  The per-key limiter is opt-in (`UDB_RATE_LIMIT_PER_KEY`, default off) and the
  default per-tenant behavior is byte-identical to 0.4.28.

### Fixed
- The DataBroker per-tenant rate limit is now discoverable. The governing knob
  `UDB_RATE_LIMIT_MAX_PER_WINDOW` additionally accepts the short alias
  `UDB_RATE_LIMIT_MAX`, and `UDB_RATE_LIMIT_WINDOW_SECS` accepts
  `UDB_RATE_LIMIT_WINDOW` — the obvious names operators reach for now take effect
  instead of silently doing nothing (which made the limit read as hardcoded). The
  `RESOURCE_EXHAUSTED` deny message now names the governing env knob and the
  offending tenant, so operators can attribute pressure and raise the ceiling
  without grepping the binary.

### Added
- Opt-in per-key DataBroker rate limiting (`UDB_RATE_LIMIT_PER_KEY=true`). When
  enabled the limiter buckets per `(tenant, credential, operation)` instead of
  `(tenant, operation)`, so one noisy principal no longer exhausts the tenant's
  shared budget. A key's `api_keys.rate_limit_per_minute` column — settable via
  `CreateApiKey`'s `rate_limit_per_minute` field or a direct `UPDATE`, and
  preserved across key rotation — RAISES that key's budget above the tenant
  default (`per_minute × window ÷ 60`). It is **raise-only**: because the column
  is `NOT NULL DEFAULT 60`, a low/default value never lowers a key below
  `UDB_RATE_LIMIT_MAX_PER_WINDOW`, so enabling the feature can only relax
  pressure, never strangle default keys. Default (flag off) behavior is unchanged.

### Docs
- `docs/enterprise-deployment.md` and the `using-udb` skill guide now document
  every `UDB_RATE_LIMIT_*` knob, the per-tenant vs per-key semantics, and the trap
  that the `UDB_API_KEY_*` / `UDB_PUBLIC_BOOTSTRAP_*` limiters are separate from
  DataBroker. The guidance is embedded in the skill (what a consumer's agent sees
  after installing an SDK), not only in the repo.

## [0.4.28] - 2026-07-26

The transaction-completeness release: `BeginTx` now supports partial `update`
mutations and returns a read-your-writes write-receipt, closing the two deferred
BeginTx-vs-unary parity gaps left after 0.4.27.

### Breaking / wire changes
- None. Additive proto fields only: `Mutation.changes` + `Mutation.increments`
  (the partial-update payload for a transactional update) and
  `TxStatus.write_receipt` (the read-your-writes fence). Existing clients that
  do not set or read these fields are unaffected.

### Added
- `update` is now a supported `BeginTx` operation. A transactional mutation with
  `operation = "update"` SETs the named `changes` columns and/or applies atomic
  `increments` on the rows matched by `filter`, atomically with the rest of the
  transaction. The unary `Update` and the transactional update now share one
  `execute_update_in_tx` core (plan → bind → execute → projection enqueue → CDC
  emit), so the two paths cannot diverge on side-effects. Scope: runs under the
  default saga / best-effort strategy; an explicit `two_phase` strategy with an
  update mutation fails closed, and the unary `expected` compare-and-swap
  precondition is intentionally not carried on the transactional path (rather
  than silently ignored).
- `TxStatus.write_receipt`: a committed `BeginTx` now returns a `WriteReceipt`
  (source LSN + outbox sequence + manifest checksum), so a client can fence a
  following read for read-your-writes exactly as `MutationResponse.write_receipt`
  does on the unary verbs. Previously `BeginTx` returned no receipt at all. The
  LSN is read post-commit so it reflects the transaction's durable position;
  per-projection-task fencing for transactions is left for a follow-up.

## [0.4.27] - 2026-07-26

The transaction-parity release: writes through `BeginTx` now perform the same
post-commit side-effects as the unary DataBroker verbs, and the object-store
`server_side_encryption` annotation is finally enforced on the wire.

### Breaking / wire changes
- None.

### Fixed
- `BeginTx` transactional writes did not invalidate the SELECT read-cache, so a
  cached full-primary-key `Select` kept serving the pre-transaction row after a
  committed transactional upsert/delete — a read-your-writes violation that
  persisted until the cache entry's TTL lapsed. The transaction commit path now
  drops the same select-cache entries the unary upsert/update/delete paths do,
  one per touched table, gated on an actual commit (plain COMMIT and 2PC alike).
- `BeginTx` transactional writes emitted no CDC change event: a CDC-enabled
  entity written inside a transaction never reached the outbox → tailer →
  subscribers/journal, unlike the identical write over the unary RPC. The
  transaction path now emits the transactional-outbox event in the same tx for
  each committed upsert/delete.
- `BeginTx` transactional writes left no audit trail: they were never recorded
  to the configured audit sink — a compliance blind spot for exactly the batched
  writes that belong in a transaction. Each committed transactional
  upsert/delete is now audited like the unary paths.
- The object-store `server_side_encryption` annotation was computed by the
  planner (`ObjectStreamPlan.requires_server_side_encryption`) but never applied:
  S3/MinIO uploads went out with no server-side encryption requested. The S3
  executor and both object PUT paths (streaming and transactional) now request
  SSE-S3 (AES-256) when the annotation is set, so at-rest encryption is enforced
  on the wire instead of being a silent no-op; a backend that cannot honor SSE
  now fails the write loudly rather than storing plaintext. (GCS and Azure Blob
  encrypt at rest by platform default, satisfying the requirement there.)

## [0.4.26] - 2026-07-25

The migration-gate release: the documented four-eyes plan-approval flow now
actually governs schema-diff changes, and `udb plan` produces a diff that
hash-matches what `serve` applies.

### Breaking / wire changes
- None.

### Fixed
- Startup schema-diff migrations aborted on ANY non-`SafeAuto` change *before*
  the `migration.require_approval_plan` gate was consulted, so the documented
  export-plan → approve → apply flow was unreachable for `RequiresReview`
  schema changes (drop-unique / drop-index / drop-column / drop-table): the
  gate only ever governed review-required SQL artifacts. The startup path now
  runs a single gate mirroring the SQL-artifact branch — `Blocked` changes
  always abort, while `RequiresReview` changes apply when (and only when) a
  configured approved plan matches the current diff. Removing a column-level
  `unique: true` (a drop-unique) now also honors an `allow_drop` annotation,
  matching drop-column / drop-index.
- `udb plan`, `udb drift`, and `udb manifest-export` built their "new" manifest
  from the application proto root only, while `serve` merges the embedded
  internal `udb_*` schemas before diffing. A CLI-exported plan therefore listed
  dozens of phantom drop-table operations against the internal schemas and
  could never hash-match the serve-side diff the approval gate verifies
  against. The three CLI commands now merge the embedded native schemas exactly
  as `serve` does.

### CI / release integrity
- Two posture guards still pinned pre-fix strings left by the 0.4.25 release (a
  stale benchmark-listing placeholder token, and a Pages benchmark-fallback URL
  that was intentionally removed to stop republishing a stale board), which had
  held `main` CI red. Both guards now assert the shipped state.

## [0.4.25] - 2026-07-25

The embedding-retrieval fix release: a freshly registered embedding model is
now immediately retrievable.

### Breaking / wire changes
- None.

### Fixed
- `EmbeddingService.RegisterModel` created the physical vector collection but
  not the Qdrant alias that the model advertises as its query target
  (`StoredModel::collection()` prefers `collection_alias`). Every
  `EmbeddingService.Retrieve` against a just-registered model therefore
  returned `FailedPrecondition` (Qdrant HTTP 404) until an unrelated
  activation/cutover happened to create the alias — a divergence that only
  surfaced against a fresh vector store. RegisterModel now points the alias at
  the active collection on registration (Qdrant-only; idempotent).

## [0.4.24] - 2026-07-25

The Update-verb hardening release: the 0.4.23 `DataBroker.Update` verb now
survives projected entities, and the SDK benchmark measures the full 376-RPC
surface with per-language service-account seeding.

### Breaking / wire changes
- None. All changes are fixes to existing surfaces.

### Fixed
- `DataBroker.Update` on an entity with projection targets (vector / cache /
  object) returned INTERNAL: the projection task was enqueued under an
  `operation` the task table's CHECK constraint rejects, with the request
  FILTER as its payload. Update now returns the post-update rows and enqueues
  each one as an `upsert` task with the real row payload, so projections
  re-materialize correctly.
- AuthnService lookups that receive a non-UUID `user_id` (for example a
  service NAME) return typed not-found instead of a raw database INTERNAL.

### Benchmarks / conformance
- The Python and TypeScript live harnesses seed a real ACTIVE service
  account (owner UUID + typed grant + certificate binding) for the
  apikey/grant RPC family, matching Go and PHP — the benchmark's measured
  bodies no longer fall back to unseeded placeholder values.
- The Go perf report's failure table carries the server error message
  (detail column), so a CI-only failure is diagnosable from the artifact.

## [0.4.23] - 2026-07-24

The consumption-seam wave: every improvement from the AmbuLife consumer
deep-dive, in one release.

### Breaking / wire changes
- None. The `Update` verb and all new surfaces are additive.

### Generated-output changes (regenerate with `udb sdk generate --project-proto`)
- The entities file now stamps `udb <version> - project-proto manifest sha256`
  plus a `GeneratedManifestHash` const; verify freshness in CI with the new
  `udb sdk diff --project-proto <dir> --against <file>`.
- Typed repositories are generated per entity (`List`/`Get`/`UpdateGuarded`/
  `DeleteGuarded`) plus a per-entity `UDBColumn` column-policy table.
- Enum reads tolerate BOTH short tokens and full enum names (dual-read window
  for `udb migrate-enum-tokens`).

### New surface
- `DataBroker.Update`: partial update (`changes`) + atomic `increments`, with
  CAS (`expected`) and keyed idempotent replay. SDK: `Entity.Update`,
  `Entity.Increment`.
- Filters on AEAD-encrypted columns now FAIL CLOSED with the blind-index
  column named — and string-equality lookups are transparently rewritten
  through the blind index, which the broker now populates server-side on
  write (consumers delete their hand-rolled HMAC derivation).
- Authorization denials name the evaluated (action, resource, tenant) tuple
  and the miss class.
- `rate_limit_failure_mode = closed|local|open` declares the limiter's
  Redis-outage posture (default stays `closed`), with a degraded metric and
  health-report warning.
- `udb migrate-enum-tokens --entity <FQN> --column <col> [--dry-run]`:
  batched, idempotent rewrite of legacy enum wire forms to short tokens.
- `udb doctor --consumer --key <api-key> --entity <FQN>`: one-command
  reachability + broker-health + authn/authz-chain diagnosis.
- Post-release SDK benchmarks now pin the harness tree to the released tag
  (results always match the release they are labeled with).

## [0.4.22] - 2026-07-24

### New surface
- Drift/blocked-migration errors and the startup log name the migration
  LEDGER database (host/database/relation) and prior checksum.
- Serving a vendored `udb proto export` tree that conflicts with the embedded
  system catalog fails at intake with the actual fix.
- A serve whose input has zero custom schemas while the prior manifest still
  records custom tables aborts BEFORE planning a drop-everything migration
  (`UDB_ALLOW_EMPTY_CUSTOM_INPUT=1` overrides).
- `udb requirements` surfaces the native StorageService bucket contract so a
  green run cannot be followed by a presigned-PUT 404 on a missing bucket.

## [0.4.21] - 2026-07-24

### Breaking / wire changes
- Broker-rendered timestamps now use the canonical `Z` suffix instead of
  `+00:00`. All shipped SDK decoders already parse both forms.

### Generated-output changes
- Enum columns resolve from file-level enum declarations (same package); an
  unresolved enum, message-typed, or repeated column is now skipped with a
  TODO on BOTH write and read — never written raw.
- Marshalling covers only proto-declared fields (no more phantom audit-column
  getters); DATE columns write date-only values.

### New surface
- The rate-limit Redis connection recovers automatically after a Redis
  restart (no more broker-restart-to-heal).

## [0.4.20] - 2026-07-23

### New surface
- `DeleteRequest.expected`: compare-and-swap deletes (`WithDeleteExpected`).
- Keyset pagination on the relational read path + `Entity.SelectPage`.
- Go SDK: `IsCASConflict`, `Error.RetryAfter()`, `Error.Reason()`.
- SDK templates are embedded in the binary — `udb sdk generate` works from an
  installed binary without a source checkout.
- Notification providers accept a `body_template` for non-default APIs.

## [0.4.19] - 2026-07-23

### Generated-output changes
- proto3 `optional` fields marshal presence-aware (pointer reads, omitted
  unset writes).
- Empty nullable strings are omitted so SQL NULL round-trips as NULL.
- Generated `udbAsTime` carries the six-layout timestamp ladder.

### New surface
- Typed `ErrorDetail` on DataBroker and Authn authentication failures;
  api-key store outages surface as retryable UNAVAILABLE, never
  Unauthenticated.
- CDC leader fencing, journal-before-broadcast ordering, idempotency-key
  retention sweeping, and the data-plane audit emitter.
- Cache keys fold in limit/sort; tenant isolation requires the tenant
  predicate OUTSIDE any `$or`.

## Unreleased - hardening (from v0.3.7)

The private masterplan/todo board re-grounded every tracked item in real v0.3.7
source, adversarially verified it against code anchors, and closed the majority of the
0.3.7 follow-up gates. This section records the source/proof work landed on top of
the 0.3.7 baseline; it is not yet a new release tag.

### Fixed

- **Postgres promoted-primary read fence:** `wait_for_token` used a stale non-null
  replay LSN on a promoted primary; it now gates on `pg_is_in_recovery()` so a
  promoted primary reports the current WAL LSN.
- **SQL Server migration-audit backfill:** wrapped the guarded backfill in
  `EXEC(...)` so SQL Server defers compile-time name resolution (code 207).
- **MySQL XA crash-recovery:** `XA RECOVER` ran over the prepared-statement
  protocol (MySQL error 1295); switched to the text protocol, and in-doubt
  transactions now recover.
- **Metering `QueryUsage` under-report:** the generic native aggregate read never
  installed the `app.current_tenant_id` RLS GUC, so the RLS predicate matched zero
  rows; the tenant GUC is now installed in the read transaction before the SUM.
- **WebAuthn attestation:** ErrorDetail metadata was decoded as protobuf without
  `.to_bytes()` (a base64 bug, not a proto mismatch), and empty attestation
  signatures were misclassified; both fixed.
- **Embedding backfill worker:** the leader work-emitter produced zero
  `work.v1` from real rows because of a missing project-isolation filter in the
  served select plus two CDC journal-envelope read bugs (the loader and the
  completion dedup read top-level payload keys that the CDC tailer nests under
  `payload.payload`); all three fixed and proven live.
- **Storage quota bypass:** the storage aggregate quota read is tenant-scoped so
  it can no longer under-count usage.

### Added

- **Live canonical conformance:** all nine canonical stores verified 9/9 green
  backend-by-backend against real infrastructure.
- **Distributed CAS:** Keeper-backed ClickHouse canonical contracts and
  Elasticsearch native compare-and-set observed green; Qdrant fail-closed proof is
  green; Weaviate/Pinecone are terminally fail-closed (no usable CAS primitive).
- **Media plane proofs:** vendored ffmpeg transcode and the LiveKit SFU served
  WebRTC smoke observed green over the broker three-listener topology
  (public / native-bearer / webrtc-peer).
- **IR mediation by default:** raw dispatch gated, compiler classification
  single-sourced, SDK IR builders shipped in templates and committed SDKs, served
  `GenericDispatch` cross-language byte parity and the PG planner/IR merge A-B
  oracle observed green.
- **Typed error contract:** a public `udb.entity.v1.ErrorDetail` wire trailer with
  decoders and convenience accessors across all six SDKs, plus structured
  validation/quota/policy detail across the native services.
- **Durable idempotency dedup:** keyed Upsert/Delete and BatchUpsert claim a
  tenant/project/type/operation-scoped dedup row in the same transaction and replay
  the stored first-writer response with `was_duplicate=true`, fail-closed when the
  dedup store is unavailable.
- **`udb scaffold`:** first-class CLI alias for `init-project` so the
  six-language `scaffold-compiles` gate can emit and compile all six examples.

## 0.3.7 - 2026-06-27

### Added

- Added the post-v0.3.2 native-service wave to the broker source: Vault, Lock,
  Scheduler, Webhook, Search, Cache, LiveQuery, Config, Metering, Backup,
  Embedding, Workflow, and notification-delivery adapter surfaces are now
  represented in source and service wiring. Committed generated SDK stubs and
  descriptor artifacts still depend on the Linux/Docker codegen pass.
- Added SDK ergonomics and conformance work across all six language SDKs:
  replay-safe metadata, DownloadFile helpers, neutral-IR query builder surfaces,
  cross-language naming contracts, bench body manifests, and mock-transport
  facade sequence gates.
- Added the six-language scaffold compile gate: `udb scaffold` emits Go,
  Python, TypeScript, C#, Java, and PHP examples, and CI now has a
  `scaffold-compiles` job that validates all six against real toolchains.
- Added WebRTC media-plane progress: egress contracts, an `SfuBridge` seam,
  embedded-SFU cleanup hooks, and a LiveKit token backend that binds join tokens
  to `{tenant,room,peer}` and exposes them as gRPC metadata.

### Fixed

- Closed the v0.3.2 release-blocker class from the June 10 audit:
  cross-tenant auth-plane reads, join-fusion tenant binding, MongoDB/ClickHouse
  raw-dispatch posture claims, CDC publish-loss paths, StateMachine publishing
  recovery, XA write-ahead decision/recovery handling, and migration approval
  token enforcement.
- Hardened tenant/session revocation, including tenant and principal deny-after
  checks on token validation.
- Hardened secrets posture with redacting `Debug` implementations and descriptor
  coverage checks for storage-only output fields.
- Consolidated CI/workflow duplication around build-once broker artifacts,
  shared SDK toolchain setup, offline SDK conformance, and benchmark ownership.
- Fixed multiple SDK/client alignment gaps: credential threading, AuthzCache TTL
  semantics, native-control-plane target selection, retry/idempotency behavior,
  DownloadFile streaming fallback, and canonical helper naming.

### Changed

- The v0.3.2 tag decision is superseded by the v0.3.7 code reality. The old
  audit/fix-plan inputs remain archived under
  `private/archive/2026-06-10-release-audit/`; this changelog is the public
  release-history fold-up of those sources.
- CI now treats generated scaffold compilation as a hard source gate once the
  `scaffold-compiles` job runs in GitHub Actions.
- Beta route and SDK alias migrations are documented in
  `docs/api-sdk-beta-migration.md`; public docs and Swagger artifacts are guarded
  so retired route shapes stay out of current examples and published API docs.

### Known Follow-Ups

- Run the maintainer-owned Linux/Docker codegen pass: `buf generate`, native
  contract/docs regeneration, `udb sdk generate`, and generated fixture refresh.
- Observe the new scaffold compile job and existing native live-conformance CI
  steps green before marking their plan items fully done.
- Complete host-infra proof items: LiveKit container lifecycle test, vendored
  ffmpeg transcode, multi-process HA/fault-injection rigs, and live
  backend-by-backend conformance.

## 0.3.2 Audit Line - 2026-06-10

### Security and Correctness Blockers Identified

- The v0.3.2 pre-release audit identified release blockers in tenant isolation,
  CDC durability, XA recovery, migration approval enforcement, backend posture
  honesty, and CI/live-verification coverage.
- The remediation plan split fixes into release-blocker, auth/hot-path,
  CDC/outbox, migration/governance, SDK, CI, and hygiene tiers, with each item
  requiring source evidence before being marked done.

### Outcome

- Most v0.3.2 blockers were fixed before the codebase advanced to the v0.3.7
  baseline. Remaining work moved into the private masterplan/todo board as
  explicit gates rather than being treated as closed release hygiene.
