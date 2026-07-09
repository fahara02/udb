# Changelog

All notable user-facing and release-gating changes are tracked here. UDB follows
the package version in `Cargo.toml`; historical v0.3.2 audit material is folded
into the v0.3.x entries because the codebase advanced to v0.3.7 before that
release line was tagged.

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
