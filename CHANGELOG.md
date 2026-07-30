# Changelog

All notable user-facing and release-gating changes are tracked here. UDB follows
the package version in `Cargo.toml`; historical v0.3.2 audit material is folded
into the v0.3.x entries because the codebase advanced to v0.3.7 before that
release line was tagged.

## [0.4.32] - 2026-07-30

Native-service upgrade — batch 2. Continues the no-deferrals plan implementation
across all five services (transactional Vault, Embedding erasure + Matryoshka,
richer Notification delivery, Search/vector tenant-filter completeness, LiveQuery
knobs) plus a fail-closed audit-sink ergonomics fix. No proto changes.

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
