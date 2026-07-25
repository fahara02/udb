# Changelog

All notable user-facing and release-gating changes are tracked here. UDB follows
the package version in `Cargo.toml`; historical v0.3.2 audit material is folded
into the v0.3.x entries because the codebase advanced to v0.3.7 before that
release line was tagged.

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
