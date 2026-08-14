# UDB v0.5.7 Vault raw SQL and entity dispatch can target different stores

Date: 2026-08-14
Status: full served project-store correction implemented; GitHub CI pending
Affected paths: secret list/destroy, transit rotation, dynamic DB credentials

## Summary

Most Vault reads/writes use project-aware native-entity dispatch, but four
critical paths execute raw SQL against a Postgres pool captured at service build
with an empty project. In a project-routed deployment, Put/Get can operate in one
Vault store while List/Destroy/Rotate and database-role issuance operate in the
default store. The lease reaper also watches only that default pool.

## Confirmed served path

- `build_vault_service` calls
  `native_store_pool_for_service("vault", true, "")` once and stores the pool.
- Put/Get/Delete/Undelete and normal transit operations call native entity
  dispatch with `context.project_id`, so their target is selected per request.
- ListSecrets count/page SQL, DestroySecret's transactional shred/outbox, and
  RotateTransitKey's demote/insert transaction use the captured `svc.pg_pool`.
- GenerateDatabaseCredentials creates the physical Postgres login on that pool,
  then writes its lease through project-aware entity dispatch.
- The leader lease reaper is spawned with the empty-project Vault pool and cannot
  discover leases routed elsewhere.

## Consequences

- A project can Put/Get a secret that List cannot see and Destroy cannot shred.
- Rotation can inspect key versions in one store and demote/insert versions in
  another, returning success without changing the key used for encryption.
- A dynamic login and its lease can be created in different database worlds; the
  worker may never revoke the recorded login or may watch the wrong lease table.
- Transactional outbox claims apply only to the default-store mutation, not the
  project store the caller actually uses.

## Required correction

- Resolve one authoritative Vault store/pool from the verified operation context
  and use it for both native dispatch and raw transactional SQL.
- If Vault is intentionally tenant-global, remove project routing from every
  Vault path and reject/normalize project metadata consistently; do not mix the
  two models.
- Persist physical backend/instance identity on DB credential leases and make
  the reaper enumerate/route to that identity.
- Re-resolve on runtime topology changes rather than retaining a startup pool.
- Add a two-project/two-instance live test spanning Put/List/Destroy/Rotate and
  login issuance/reaping.

## 2026-08-14 correction

- Added a single Postgres binding resolver that returns the selected physical
  instance as well as its pool. Typed native dispatch honors that internally
  pinned instance, preventing weighted routing from selecting a second database
  during a compound operation.
- Vault direct-store operations now canonicalize the project, require an
  explicitly active project catalog, and dynamically resolve the Vault binding
  for each request. The build-time empty-project pool was removed.
- `ListSecrets`, `DestroySecret`, and `RotateTransitKey` now use the resolved
  request project; rotation pins the same instance for its typed read and raw
  transaction. Best-effort Vault events also resolve the event's project store.
- The lease reaper now re-enumerates all active projects and resolves their
  current Vault stores on every pass instead of watching only the startup default
  pool. New direct database-role issuance is disabled by the separate authority
  correction, so it can no longer split a role and lease across stores.

## 2026-08-15 full project-store correction

- Every Vault RPC that reads or mutates durable secret/transit state now resolves
  the active project once, selects the project's write-authority Postgres
  instance once, and pins that instance in the shared `RequestContext` before
  the first typed native-entity operation. This closes the remaining typed-only
  read/write drift in Put/Delete/Undelete/CreateTransitKey and all transit crypto
  handlers; project absence fails closed before storage is touched.
- List is intentionally pinned to the write authority as well. Vault key/secret
  reads therefore cannot select a read replica while a later mutation or audit
  step selects a different physical database.
- Vault outbox emission now receives the already pinned request context and
  resolves the exact named instance from that context. It no longer constructs
  a fresh tenant/project-only context that re-enters weighted selection.
- Added an ignored live served-path regression that provisions two real
  PostgreSQL databases, binds one named instance to each of two active projects,
  starts the generated Vault tonic service, and drives the generated client
  through Put/Get/List/Destroy/CreateTransitKey/Rotate/Encrypt. It proves same
  tenant/path/key values remain physically independent and verifies the unique
  rotate/destroy outbox topics exist only in project A's database.

The raw/project-store split itself is now closed in production code and has a
real two-project/two-instance regression. Dynamic database-credential lease
provenance/reconciliation is being corrected in the dedicated Vault credential
lifecycle wave because it changes that entity's durable contract and reaper;
it is not silently treated as proof of this request-routing change.

## Verification log

- Traced service construction and every direct `svc.pg_pool` use against the
  project-aware native dispatch paths.
- `cargo check --lib --no-default-features --features postgres -j 2` passed
  locally after the correction (warnings only).
- Focused Vault unit execution was terminated after the local linker remained
  CPU-bound for more than ten minutes during the prior wave.
- Per operator direction, no local Cargo/build/test command was run for the
  2026-08-15 correction. GitHub CI must run the quick gate, library tests, and
  native-service live tests; no pass is claimed before those jobs are green.

— DONE (2026-08-15): all served Vault secret/transit and audit paths reuse one
active-project physical authority; the two-project/two-instance tonic regression
is in `src/runtime/service/vault_service/project_store_live.rs` (CI pending).
