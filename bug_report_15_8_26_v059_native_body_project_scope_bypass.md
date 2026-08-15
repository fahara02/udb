# Bug report: native handlers accepted an unchecked body project

Date: 2026-08-15
Target release: 0.5.9
Severity: critical isolation defect

## Observed

`validate_request_tenant` deliberately validates only the request tenant. Ten
project-bearing Config, Metering, and LiveQuery handlers called that tenant-only
helper, then passed `request.project_id` into `native_service_context`. A token
bound to project A could therefore send the same tenant with project B; the
runtime context and durable predicates were constructed for B without comparing
the body project to the verified bearer claim or project metadata.

Affected operations were Config Put/Get/List/Delete/Evaluate, Metering
Put/Get/List/Check quota, and LiveQuery Subscribe.

## Impact

A same-tenant caller could select, mutate, enumerate, evaluate, or subscribe to
another project's native state where the underlying entity and routing contract
accepted that project. Tenant validation alone did not preserve project
isolation.

## Required correction

Keep tenant-only validation for genuinely tenant-only contracts. For every
explicit project-bearing native request, atomically validate tenant and project
against metadata and the installed claim before constructing the runtime
context. Preserve typed `project_claim_mismatch` and
`project_metadata_mismatch` policy denials and add a posture guard for these
handlers.

## Evidence

Unit coverage exercises both a project-A claim/project-B body denial and a
matching tenant/project context. The source posture test requires the ten
affected handler paths to use the atomic scope helper. GitHub CI is the only
compile/test authority for this correction; no local Cargo/build/test is run.
