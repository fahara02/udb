# UDB v0.5.7 storage project-ownership bypass

Date: 2026-08-14
Status: corrected in source; GitHub CI pending
Affected service: `udb.core.storage.services.v1.StorageService`
Real client impact: Ambulife uses native storage for user and partner media

## Summary

`RegisterUpload` validates and stores a project id, but every later file RPC is
authorized and queried only by `(tenant_id, file_id)` (or tenant alone for
`ListFiles`). The handlers deliberately build a tenant-only native context and
the shared file predicates omit `project_id`.

A caller authenticated to project A can therefore read metadata, mint a download
or re-upload URL, stream bytes, finalize, modify visibility/ownership metadata,
or delete a project-B file in the same tenant when it knows the file UUID.
`ListFiles` is worse: it enumerates every live file in the tenant for a
project-scoped caller. Tenant RLS cannot enforce this missing intra-tenant
boundary.

## Confirmed served paths

- `RegisterUpload` is the only request carrying `project_id`; it calls
  `validate_request_scope` and persists the value on the File row.
- `FinalizeUpload`, `GetDownloadUrl`, `ReissueUploadUrl`, `DownloadFile`,
  `GetFile`, `UpdateFile`, and `DeleteFile` call only
  `validate_request_tenant`, then use `file_read_by_id(tenant_id, file_id)`.
- `ListFiles` uses `file_list_filter`, whose base clauses are tenant plus
  `deleted_at IS NULL`; no project clause is present.
- All of those paths use `tenant_only_native_service_context`, so compiler-added
  project predicates cannot compensate for the explicit filter omission.
- The file id is random, but an opaque identifier is not an authorization
  boundary and can appear in application data, logs, URLs, events, or support
  workflows.

## Required correction

- Resolve the validated caller project from claim-first metadata on every
  file-id and list method.
- When the caller is project-scoped, require the stored File project to match in
  the database predicate; return the normal not-found response on mismatch.
- Preserve tenant-wide behavior only for credentials intentionally carrying no
  project scope.
- Decide and document whether Storage's native entity placement is tenant-wide
  or project-routed. Do not silently change routing for existing File rows while
  adding the ownership predicate.
- Add served two-project tests for every capability class: metadata read/list,
  presign/stream, finalize/update, and soft/hard delete.

## Implemented correction

- `resolved_storage_project_scope` now resolves project authority claim-first,
  validates every non-empty value as a UUID, and preserves an empty value only
  for intentionally tenant-wide credentials.
- `RegisterUpload` persists that effective project even when a project-scoped
  client omits the redundant request-body field.
- The shared live-file read and list predicates now add `project_id = ...` for
  project-scoped callers. The native context remains tenant-only deliberately:
  Storage metadata placement did not change, and project is enforced as an
  authorization predicate rather than silently becoming a routing key.
- Finalize, download/presign/reissue, get, update, soft delete, hard delete, and
  list all pass the verified project into the predicate. Cross-project targets
  retain the ordinary not-found shape.
- HARD-delete idempotency replay checks the stored intent project before
  returning its fingerprint or outcome, including the concurrent-winner path.
  A known key therefore cannot reveal another project's delete result.
- A pure neutral-IR regression test covers project-scoped and intentionally
  tenant-wide read/list filters. An ignored live Postgres served-path test
  creates two projects in one tenant and covers read/list, presign/reissue,
  stream, finalize, update, soft delete, hard delete, and tenant-wide behavior.

## Verification log

- Source trace completed across all nine RPC handlers, the File neutral-IR
  predicates/projection, request protos, native context semantics, and the real
  customer integration inventory.
- `git diff --check` passes for the isolated Storage files.
- Per operator direction, no local Cargo build or test was run. Compilation,
  unit tests, and the ignored live Postgres regression remain pending in GitHub
  CI; this report must not be read as a green-CI claim until that run completes.
- Initial CI run `31824506981` reported one quick-gate-only `rustfmt` import
  wrap. The exact CI diff was applied in a follow-up; authoritative replacement
  CI remains pending.
