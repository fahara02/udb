# Bug report: SDK benchmark caller and wire identity drift

Date: 2026-08-16
Affected release: 0.5.9 post-release benchmark evidence
Severity: release-evidence integrity
Target correction: 0.5.10

## Observed

Post-release Benchmark run `31919949691` failed the canonical completeness gate.
The PHP benchmark exited with no freshly generated RPC rows after its authz role
seed was denied and `role_id` body hydration escaped the per-RPC measurement
boundary. The TypeScript report contained all 381 rows, but nine Cache and
Embedding rows used SDK alias-derived method names instead of canonical gRPC
wire method names, producing nine missing and nine unexpected identities.

## Root cause

- PHP kept only the authenticated tenant returned by `AuthenticateBearer`; it
  discarded the verified principal subject and reused a disposable target
  user's UUID for `created_by`, `assigned_by`, and governance actor attribution.
- PHP tracked only selected Backup seed failures. An untracked missing authz
  prerequisite reached the strict manifest resolver outside an exception
  boundary and aborted report generation.
- Known seed-blocked PHP rows used zero iterations and zero timing even though
  the canonical collector requires positive per-row measurement evidence.
- TypeScript correctly resolved SDK aliases through generated metadata for
  operation kind, API alias, and operation ID, but report sample construction
  independently rebuilt the RPC name with `snakeToPascal(methodName)`.

The same run exposed two product authority gaps rather than benchmark-only
drift:

- A tenant administrator could create or bind a role named `platform_admin`.
  Token projection then treated the tenant-defined label as platform authority,
  so a project-bound principal could cross tenant/project boundaries.
- Service-account grants rejected wildcard/admin aliases but did not reject the
  exact `udb:platform_admin` scope. Because API-key issuance and refresh consume
  the canonical service grant, this was another route to a platform-authority
  claim.

The Analytics global reads, Backup cross-tenant restore, Tenant administrative
purge, and Authz governance failures also showed that the benchmark used its
ordinary tenant session for RPCs whose contract requires explicit platform
authority. Finally, tenant-wide session revocation stored an inclusive
second-resolution cutoff (`iat <= cutoff`) but returned soon enough for an
immediate replacement login to receive that same second and be correctly
rejected as revoked.

## Impact

The artifact could not prove the complete four-SDK RPC surface. PHP lost every
row after one authority-sensitive seed failure, while TypeScript mislabeled
valid calls as noncanonical wire operations. The release binary was unchanged;
the failure was in the benchmark harness and its evidence identities.

## Required correction

- Preserve verified `caller_subject` and its production-equivalent stable UUID
  separately from disposable target/principal fixture identities.
- Use caller attribution for served authz actor fields and governance actor
  subjects without converting policy targets into callers.
- Record failed authz prerequisite provenance and convert every known or unknown
  body-hydration failure into one fatal, positive-timing, positive-iteration row
  while continuing the canonical sweep.
- Derive TypeScript report `service` and `rpc` from the generated canonical path
  for every unary and streaming sample path.
- Add focused caller/target, blocked-evidence, and 381-RPC alias-bijection
  regressions.
- Reserve platform role aliases across typed roles, literal role bindings, and
  governance drafts/activation. Accept platform authority from role snapshots
  only when the durable role has exact active system/global provenance.
- Provide an explicit direct-Postgres, offline-only
  `auth bootstrap user --platform-admin` seam. Reject the flag in served mode;
  ordinary tenant RPCs must never be able to mint or assign this authority.
- Reject `udb:platform_admin` in the central service-grant scope validator so
  grant create/replace, certificate attenuation, API-key creation/refresh, and
  request-time grant resolution all fail closed.
- Provision a separate platform benchmark user and route only the exact global
  RPC allowlist through that verified session in Go, Python, TypeScript, and
  PHP. Direct Authz CRUD and self-purge remain ordinary-tenant operations.
- Preserve the inclusive `iat <= cutoff` revocation rule. After the durable and
  Redis cutoff is successfully published, delay the successful revoke response
  until wall-clock issuance is strictly greater than `cutoff`; do not weaken the
  old-token boundary to `<`.

## Acceptance evidence

GitHub CI must run the focused PHP and TypeScript tests, then a post-release
benchmark against a successor immutable tag. The PHP report must contain every
canonical row even when a prerequisite is forced to fail. The TypeScript report
must contain the exact canonical wire set once, including all Cache and
Embedding alias cases, with no missing or unexpected identities.

Security acceptance additionally requires negative live coverage proving that
tenant-created role aliases, literal/governance bindings, service grants, and
API keys cannot acquire `udb:platform_admin`; a trusted offline-provisioned
platform principal can perform the exact global RPC set; ordinary sessions
remain tenant/project-bound; and a fresh login immediately after tenant-wide
revocation succeeds only with `iat > cutoff` while same-second tokens remain
denied.
