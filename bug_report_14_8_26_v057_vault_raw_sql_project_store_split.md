# UDB v0.5.7 Vault raw SQL and entity dispatch can target different stores

Date: 2026-08-14
Status: request/worker routing corrected; live topology proof and legacy lease provenance remain
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

The served request split is corrected. A live two-project/two-instance
conformance test is still required, and pre-existing lease rows do not contain a
trustworthy physical-instance identity for forensic/reconciliation use. Those
items remain open rather than being represented as complete.

## Verification log

- Traced service construction and every direct `svc.pg_pool` use against the
  project-aware native dispatch paths.
- `cargo check --lib --no-default-features --features postgres -j 2` passed
  locally after the correction (warnings only).
- Focused Vault unit execution was terminated after the local linker remained
  CPU-bound for more than ten minutes; no test result is claimed. GitHub CI is
  pending for this wave; no production data was mutated.
