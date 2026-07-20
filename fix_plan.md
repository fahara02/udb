# UDB AmbuLife Integration Root Fix Plan

Date: 2026-07-20
Scope: UDB-AUTH-003/004/005/006/007, UDB-CAT-001/002 and UDB-SRV-003
Reference: `latest_udb_bug.md` and `bug_report_17_7_26_ambulife_integration.md`

## Objective

Close the remaining AmbuLife integration defects at their shared roots rather
than adding listener-specific or SDK-specific exceptions. Authentication must
derive one fail-closed principal from verified credential lineage. Catalog
routing must use canonical protobuf identity. Startup failures must retain
machine-readable findings and remediation.

The work is complete only after the public DataBroker listener and native
listener pass positive and negative black-box tests using durable stores.

## Corrected Baseline

The current v0.4.15 source already contains parts of the required behavior, so
these paths must be hardened and tested rather than independently reimplemented:

- AUTH-003: password service-account scope attenuation exists, but grants are
  encoded in mutable generic profile attributes and have no typed API/CLI.
- AUTH-004: direct DataBroker API-key authentication and API-key-to-bearer
  exchange exist. Resolver installation is incorrectly coupled to native-service
  construction, and resolution blocks a runtime worker through a synchronous
  global adapter.
- AUTH-005: DataBroker uses verified JWT `sub` and rejects mismatched
  `x-user-id`. Served-path negative coverage is still required.
- AUTH-006: key records preserve service identity and metadata, but the upstream
  service-account identity remains stored in mutable profile attributes.
- AUTH-007: mTLS still has no server-controlled principal/scope registration and
  is bypassed whenever JWT verification is configured.
- CAT-001/SRV-003: FQN-aware linting and exact lookup exist, but ambiguous short
  names can still resolve to an arbitrary table.
- CAT-002: startup emits individual flattened lines, but the lifecycle report
  does not retain structured `LintItem` values and the rendered line omits the
  remediation suggestion.

## Invariants

1. Subjects, service identities, tenants, projects and scopes come from verified
   credentials and server-controlled records, never caller assertion.
2. Request headers may correlate with or narrow verified identity. A mismatch is
   rejected; a header cannot fill or widen authoritative identity.
3. Unknown, expired, revoked, ambiguous or unavailable credential state fails
   closed.
4. Each credential is validated once per request. DataBroker and native method
   security consume the same verified principal.
5. Service credentials cannot receive admin, owner or wildcard scopes through
   password, API-key or mTLS paths.
6. Fully qualified protobuf message identity is canonical. A short name is valid
   only when unique in the composed catalog.
7. Durable auth and catalog state must not be replaced by process-local stores or
   hardcoded compatibility aliases.

## Phase 1: Shared Verified-Principal Pipeline

### 1.1 Introduce one principal contract

Add a shared runtime type carrying:

- credential type;
- verified subject;
- immutable service identity;
- tenant and project;
- effective scopes and roles;
- token/session/key/certificate identifier;
- authentication method and optional certificate identity.

Install it in tonic request extensions from an asynchronous Tower layer before
DataBroker handlers or native method-security enforcement run. Preserve
`SecurityContext` as the operation context, but construct it only from the
verified principal plus non-authoritative request context such as purpose,
correlation ID and consistency options.

### 1.2 Remove the synchronous API-key bridge

Replace `ApiKeyPrincipalResolverImpl::resolve`, its process-global resolver cell
and `block_in_place` database lookup with an async authenticator that reuses the
existing keyed-HMAC API-key validator and durable `PostgresApiKeyStore`.

Create shared auth runtime dependencies independently of native-listener
enablement. A DataBroker-only deployment must initialize API-key validation even
when `build_auth_services()` is never called. Native Authn and ApiKey services
must reuse those same dependencies rather than creating another store/resolver.

### 1.3 Define credential composition

- Reject API key plus bearer, session plus bearer, or other unrelated multiple
  credentials.
- Treat a verified client certificate as transport identity when a bearer is
  also present. If the certificate has a service binding, require its service,
  tenant and project to agree with the bearer.
- Permit mTLS-only authentication when the method allows mTLS and a valid
  server-side certificate binding resolves to a principal.
- Never read identity or scopes from `x-service-identity`, `x-user-id`,
  `x-tenant-id`, `x-udb-project-id` or `x-scopes` as authority.
- A tenant/project header may echo a credential claim; reject disagreement.
  Do not use a header as fallback for a missing required claim.

### 1.4 Unify consumers

- Change DataBroker `security_from_request` to consume the verified extension.
- Change native `MethodSecurityLayer` to consume the same extension.
- Extend the runtime credential enum with mTLS value `5`, matching
  `udb.core.common.v1.CredentialType`.
- Keep public bootstrap RPC behavior explicit in descriptor method security.
- Ensure authorization audits record credential type and immutable principal
  lineage without recording credential secrets.

## Phase 2: Typed Service-Account Grants

### 2.1 Add the canonical contract

Define a typed proto-owned service-account grant surface instead of encoding
security policy in `profile_attributes`. The durable model must contain:

- service-account user ID and immutable service identity;
- tenant and project binding;
- approved scopes;
- status, revision and timestamps;
- creator/updater principal and audit reason.

Expose authorized native RPCs and CLI commands to create, inspect, replace and
revoke grants. Creation and mutation require explicit authn/authz administrative
scopes and produce durable audit events.

### 2.2 Centralize grant validation

Use one helper for password login, API-key creation/authentication, token refresh
and certificate binding. It must:

- normalize and deduplicate scopes;
- reject `*`, `udb:*`, `udb:admin`, owner roles and equivalent aliases;
- reject requested scopes outside the approved grant;
- return the requested subset, or the approved set when no subset is requested;
- reject inactive accounts, missing grants, tenant/project mismatch and stale
  grant revisions.

Make service identity immutable after creation. Rotation must be an explicit,
audited operation that revokes or invalidates dependent credentials.

### 2.3 Migrate existing records

Provide a deterministic migration from recognized profile keys to typed grants.
Reject malformed, ambiguous or privileged legacy values and report every rejected
record. Keep profile attributes non-authoritative after migration.

## Phase 3: Server-Controlled mTLS Bindings

Add a canonical durable certificate binding owned by the auth plane. Prefer a
SPIFFE URI SAN as identity; support an issuer-and-serial or certificate
fingerprint selector where required. Store:

- certificate selector and service-account owner;
- tenant/project and grant revision;
- status, validity window and revocation metadata;
- audit provenance.

Add protected native RPC and CLI management surfaces. At request time, validate
the TLS peer certificate, resolve the binding, resolve the current account grant
and construct the verified principal. Reject unknown, expired, revoked,
misbound or store-unavailable certificates.

Do not copy free-form scopes into certificate metadata. Effective scopes must be
derived from the current server-controlled account grant, optionally attenuated
by a binding-specific subset.

## Phase 4: Canonical Catalog Identity

### 4.1 Make lookup ambiguity explicit

Replace the `Option<ManifestTable>` lookup contract with a result that
distinguishes `Missing` from `Ambiguous`. Build separate indexes for:

- exact protobuf FQN;
- qualified physical `schema.table` identity;
- short protobuf name only when globally unique;
- bare physical table name only when globally unique.

Never overwrite a prior short-name or bare-table entry. Mark it ambiguous and
return an actionable error listing every candidate FQN and physical table.

### 4.2 Propagate canonical identity

Use the existing manifest FQN helpers across linting, projections, query
planning, runtime routing, native catalog composition, schema checksums and SDK
metadata. Internal requests should carry an FQN. Compatibility short names may
remain only as a unique-name convenience at the boundary.

Remove consumer-specific aliases once exact FQN startup and CRUD acceptance pass.

## Phase 5: Structured Startup Findings

Extend `StartupLifecycleReport` with backward-compatible, serde-defaulted fields:

- `lint_items: Vec<LintItem>`;
- `lint_error_count`;
- `lint_warning_count`.

Populate them for successful lint with warnings, blocking lint and force-sync
bypass. Keep existing string warning/error arrays for compatibility, but derive
them from the structured findings. Include `suggestion` in human rendering and
emit structured tracing fields for severity, kind, schema, table, column, source,
description and remediation.

The failure JSON returned by startup must contain the complete structured list,
not only counts or flattened lines.

## Phase 6: Acceptance Tests

### Auth tests

- Create a service account and typed grant containing only `udb:read` and
  `udb:write`; login returns exactly that identity and scope set.
- Scope widening, wildcard/admin grants, cross-tenant/project use and inactive or
  revoked accounts return `PermissionDenied` or `Unauthenticated` as appropriate.
- A scoped API key performs DataBroker CRUD on a data-only listener with native
  services disabled.
- API-key exchange returns a short-lived bearer with exact attenuated claims.
- API-key metadata, owner and immutable service identity survive create/get/auth.
- JWT subject A plus `x-user-id: B` is rejected on a served listener.
- JWT tenant/project mismatch and missing required authoritative claims fail
  closed.
- Registered mTLS-only authentication works while JWT verification is enabled.
- Unknown/revoked certificates, certificate/bearer mismatch and header scope
  injection fail closed.
- Native method security accepts mTLS only when descriptor metadata declares it.

### Catalog and lifecycle tests

- Start a broker containing both `ambulife.authn.entity.v1.OTP` and
  `udb.core.authn.entity.v1.OTP`, plus the corresponding `User` and `Session`
  pairs, without renaming either catalog.
- Exact-FQN Select and Upsert route to the correct physical tables.
- A colliding short-name request returns an ambiguity error and never reaches a
  backend.
- A composed-catalog failure serializes every `LintItem`, count and remediation
  in lifecycle JSON and emits each finding in startup logs.
- Force-sync retains the complete bypassed finding list for audit.

## Verification Order

1. Run formatting and focused unit tests for each phase.
2. Run auth integration tests against durable Postgres stores.
3. Run composed-catalog startup and DataBroker served-path tests.
4. Run the complete Rust workspace checks and feature matrix once after focused
   failures are resolved.
5. Run SDK conformance for supported current SDKs without duplicating broker
   builds.
6. Build the release broker once, run black-box acceptance against that exact
   artifact, then publish artifacts and documentation only after acceptance.

## Completion Gate

Do not mark an issue fixed from source inspection alone. Update
`latest_udb_bug.md` only when the corresponding served-path acceptance test has
passed against the release candidate artifact. Record the exact commit, binary
digest, SDK version and test evidence so a later release cannot regress to a
source-only or replaced-asset claim.

---

## Implementation Completion Update

Update date: 2026-07-20
Branch reviewed: `fix/auth-catalog-0417`
Source status: **EDIT COMPLETE**
Release status: **EXECUTION PENDING**

The root-fix implementation now covers every phase in this plan. The release
gate is intentionally still closed because Cargo and live acceptance were not
run in this pass, per operator instruction. Source completion is not being used
as a substitute for release-candidate evidence.

| Phase | Current source status | Evidence surface |
|---|---|---|
| 1. Shared verified-principal pipeline | Complete | One async `CredentialResolveLayer`, canonical `VerifiedPrincipal`, no synchronous DB resolver, fail-closed credential composition, audit lineage |
| 2. Typed service-account grants | Complete | Proto/store/RPC/CLI lifecycle, authoritative login/refresh/JWT/API-key checks, explicit audited identity rotation, deterministic profile migration |
| 3. Server-controlled mTLS bindings | Complete | Typed selectors, durable binding/grant/account resolution, validity/revocation/re-review, common native policy gates |
| 4. Canonical catalog identity | Complete | Exact/physical/unique-short indexes, typed missing/ambiguous errors, exact-FQN runtime/planner routing |
| 5. Structured startup findings | Complete | Serde-defaulted findings/counts, remediation rendering, structured tracing on block/warn/force-sync |
| 6. Acceptance test implementation | Complete, not executed | Durable Postgres tests plus served DataBroker-only, native grant-management and mTLS listeners |

### Closed Review Findings

- CRIT-1: removed mutable-profile certificate authority; durable certificate
  binding resolution is the only request-time mTLS authority.
- CRIT-2: every grant/binding RPC declares and enforces claim-bound tenant
  scope; served negative coverage calls every management RPC cross-tenant.
- CRIT-3: grant creation locks and validates an active service-account owner;
  all service credential paths re-check current owner state.
- CRIT-4: API-key create/update/auth/exchange is attenuated against the current
  typed grant and reviewed revision.
- HIGH-1 through HIGH-4: certificate selectors are typed, bearer/certificate
  credentials compose, revoked selectors can be auditedly superseded, and mTLS
  traverses the common native security gates with descriptor opt-in.
- HIGH-5: dotted identities are exact-only and ambiguous short names return all
  candidates without backend dispatch.
- HIGH-6: grant/binding mutation and audit outbox writes share one SQL
  transaction and roll back together.
- HIGH-7: real served-path acceptance tests now cover the data-only DataBroker,
  grant-management native listener and TLS native listener.

### Acceptance Coverage Added

- Typed grant login, forbidden/widened scopes, cross-tenant/project access,
  account deactivation, grant replacement/revocation and identity rotation.
- API-key metadata/identity persistence, current-grant attenuation, short-lived
  bearer exchange and DataBroker CRUD with native services absent.
- Served JWT subject/tenant/project mismatch, missing authoritative claims and
  header scope-injection denials.
- Served mTLS-only positive authentication while JWT verification is enabled,
  unknown/revoked certificate denials and valid-certificate/valid-bearer
  identity-splice denial.
- Composed AmbuLife/UDB `OTP`, `User` and `Session` identities, exact-FQN
  Select/Upsert routing and short-name ambiguity rejection before dispatch.
- Complete structured lifecycle finding/count/remediation serialization and
  force-sync audit retention.
- Authorization compliance envelopes now retain numeric credential type,
  non-secret credential id (`jti`, key prefix or binding id), and immutable
  service identity for allow, deny and mutation-event lineage.

### Validation Performed Without Cargo

- `buf lint`: passed.
- `buf build`: passed.
- Direct `rustfmt --edition 2024 --check` over all fix-plan Rust files: passed.
- `git diff --check`: passed (only the existing generated-doc CRLF warning).
- Static authority audit: no synchronous API-key/certificate resolver remains;
  profile grant parsing is migration-only; production catalog consumers use
  typed resolution.

### Mandatory Release Gate Still Pending

Run the Verification Order above when Cargo execution is permitted. In
particular, compile first, execute the focused and ignored live-Postgres served
tests, run workspace/feature checks once, then test the exact release binary.
Only after those pass should `latest_udb_bug.md` be updated with commit, digest,
SDK and release-candidate evidence.

---

## Superseded Initial Review (Pre-Fix Snapshot)

The review below is retained as the defect baseline that drove this work. Its
percentages and open-finding statements describe the worktree before the fixes
summarized above and are not the current implementation status.

Review date: 2026-07-20
Reviewed branch: `fix/auth-catalog-0417`
Reviewed baseline: `dbb1c8c093697bf57c9e9b155a868da167ccc832` plus the
uncommitted implementation worktree
Verification posture: source and diff review only. Cargo verification was
stopped and was not completed, per operator instruction.

### Executive Status

The implementation is not complete and is not safe to release as `0.4.17`.
Several useful contracts and components have landed in the worktree, but the
new durable grant and certificate-binding model is bypassed by legacy runtime
paths. Critical tenant, credential-lineage and revocation invariants remain
unmet. No served-path acceptance suite proves the implementation.

Estimated completion against this plan: **46%**. This percentage measures
implemented checklist surface, not release confidence. Release readiness stays
blocked until every Critical and High finding below is fixed and Phase 6 passes
against the release candidate artifact.

| Phase | Review status | Estimated completion | Primary blocker |
|---|---|---:|---|
| 1. Shared verified-principal pipeline | Partial | 45% | Legacy synchronous resolvers remain; JWT and mTLS do not compose through one principal |
| 2. Typed service-account grants | Partial | 45% | API keys and refresh are not grant-backed; profile fallback remains authoritative |
| 3. Server-controlled mTLS bindings | Unsafe partial | 30% | Mutable profile resolver bypasses binding rows; selector and re-review paths are defective |
| 4. Canonical catalog identity | Partial | 65% | Unknown dotted FQNs still fall back to a leaf name; old `Option` lookup remains widespread |
| 5. Structured startup findings | Mostly implemented | 80% | Structured tracing is incomplete for warning and force-sync paths |
| 6. Acceptance tests | Mostly missing | 10% | No served DataBroker-only, mTLS, cross-tenant, revocation or real startup/CRUD tests |

### Implemented Surface

- [x] Added proto entities for `ServiceAccountGrant` and
  `CertificateBinding`.
- [x] Added Authn RPC contracts for grant and certificate-binding management.
- [x] Regenerated the corresponding SDK message and service contracts.
- [x] Added an asynchronous Tower credential-resolution layer.
- [x] Mounted credential resolution on TCP and UDS serving paths.
- [x] Added auth dependency installation for the DataBroker-only listener.
- [x] Added typed Postgres grant and certificate-binding stores.
- [x] Added profile-to-grant migration and a migration CLI command.
- [x] Added typed-grant lookup to password service-account login.
- [x] Added explicit ambiguous catalog lookup results and candidate reporting.
- [x] Added FQN composition tests at resolver/lint-helper level.
- [x] Added structured lint findings and counts to
  `StartupLifecycleReport`.
- [x] Added lint remediation to human-readable finding lines.

### Critical Findings

#### CRIT-1: Certificate bindings are bypassable

`src/runtime/service/auth_service/mod.rs` still installs
`ServiceIdentityGrantResolverImpl`, which resolves a certificate identity
directly from mutable `users.profile_attributes_json` or `external_subject`.
`src/runtime/security.rs` falls back to that resolver whenever the asynchronous
binding lookup does not return a principal.

Impact: a certificate whose SAN/CN matches a mutable profile can authenticate
without any `CertificateBinding` row. Revocation, validity, binding-specific
attenuation and grant-revision review are bypassed.

Required correction:

- Delete the profile-based certificate resolver and its process-global fallback.
- Treat the asynchronous `certificate_bindings -> service_account_grants ->
  active service account` lookup as the only mTLS authority.
- Preserve lookup failure as a typed fail-closed outcome in request extensions;
  never reinterpret a binding miss as permission to try a weaker authority.

#### CRIT-2: Grant and binding RPCs are not claim-tenant-bound

The new RPC descriptor security blocks require a tenant-scoped bearer but do
not declare `tenant_field: "tenant_id"`. The handlers in
`src/runtime/service/auth_service/grants.rs` only verify that the body tenant is
non-empty; they do not compare it with `VerifiedClaimContext.tenant_id`.

Impact: a tenant-A principal carrying a grant-management/read scope can submit a
tenant-B request and create, inspect, replace or revoke tenant-B grants and
certificate bindings.

Required correction:

- Add `tenant_field: "tenant_id"` to every new RPC endpoint-security contract.
- Call the shared body-tenant claim binder in every handler after decoding.
- Add served positive and negative cross-tenant tests for every management RPC.

#### CRIT-3: Grant ownership does not verify service-account state

`create_grant` validates identifiers and relies on a user foreign key, but it
does not verify that the owner is an ACTIVE `SERVICE_ACCOUNT` in the same tenant
and project. Request-time certificate resolution checks only the grant row and
does not re-check current user status.

Impact: a grant can target a human or inactive account, and disabling a service
account does not necessarily disable its mTLS authentication.

Required correction:

- Resolve and lock the owner user during grant creation.
- Require active service-account kind and exact tenant/project binding.
- Re-check current account status and kind during all service-credential
  authentication paths.
- Add deactivation and cross-tenant owner tests.

#### CRIT-4: API keys bypass typed grants

`src/runtime/service/auth_service/apikey.rs` remains profile-based. Create stores
the caller-provided scope list directly, update replaces scopes directly, and
authentication validates only the API-key record. No API-key path resolves the
current typed grant or calls the central scope validator.

Impact: keys can retain scopes after grant replacement/revocation and can carry
scopes outside the service account's approved grant.

Required correction:

- Resolve the active typed grant during key create, update, direct
  authentication and key-to-bearer exchange.
- Store only an attenuated subset plus the reviewed grant revision.
- Reject stale revision, revoked grant, inactive owner and scope widening.
- Decide and implement immediate key revocation or mandatory invalidation when
  a grant or service identity changes.

### High Findings

#### HIGH-1: Bearer and mTLS composition is absent

`src/runtime/credential_layer.rs` intentionally skips certificate-binding
resolution when a bearer is present. JWT handling does not compare bearer
subject/service identity/tenant/project with the certificate binding. It also
still falls back to `x-tenant-id` and `x-udb-project-id` when signed claims are
missing.

Required correction: resolve certificate bindings whenever a verified client
certificate is present, compose according to Phase 1.3, reject disagreement and
remove header fallback for required authoritative JWT identity.

#### HIGH-2: Certificate selector kinds are confused

`resolve_certificate_grant` obtains one preferred identity from
`service_identity_from_der` and attempts that same value as SPIFFE URI, DNS SAN
and subject CN. Certificates containing multiple relevant names are not parsed
into independently typed selectors, and a URI value can incorrectly match a
DNS/CN binding.

Required correction: parse the certificate once into a typed selector set
containing its fingerprint, every URI SAN, every DNS SAN and the subject CN.
Match each value only under its real selector kind and validate selector values
when bindings are created.

#### HIGH-3: Certificate bindings cannot be re-reviewed reliably

Grant replacement increments `grant_revision` and intentionally invalidates
bindings, but there is no binding replace/review RPC. The unique selector index
also includes revoked rows, so revoke-then-create with the same selector still
collides.

Required correction: add a transactional replace/review operation with expected
binding/grant revision, or use an active-only uniqueness model that preserves
history while permitting an audited replacement.

#### HIGH-4: mTLS native enforcement returns before common gates

The mTLS branch in `src/runtime/service/method_security.rs` returns before the
shared internal-only, CSRF, request-context, tenant, project and role checks.
No current endpoint declares `CREDENTIAL_TYPE_MTLS`, so the path is also not
served by any descriptor contract.

Required correction: authenticate mTLS into the common claim context, then run
the same post-authentication policy gates as bearer credentials. Add an explicit
descriptor opt-in and served test for at least one intended mTLS RPC.

#### HIGH-5: Unknown dotted FQNs can still resolve by leaf name

`table_for_message` extracts a leaf from every request and falls back to that
leaf when exact lookup misses. An input such as `wrong.package.Invoice` can
therefore route to the catalog's only `Invoice` table.

Required correction: when input is dotted, allow exact protobuf FQN or exact
qualified physical identity only. Use short-name convenience lookup only for an
undotted input, and keep protobuf and physical indexes separate.

#### HIGH-6: Security mutations and audit events are not atomic

Grant/binding mutations commit before calling best-effort `emit_ops_event`, even
though Authn already exposes `emit_event_in_tx`. An outbox failure can leave a
successful security mutation with no durable audit event.

Required correction: perform each mutation and event/outbox insert in one SQL
transaction and propagate event-write failure so both roll back.

#### HIGH-7: Served-path acceptance coverage is absent

The new coverage exercises pure scope validation, lookup helpers and composed
lint data. It does not start listeners or prove durable auth/catalog behavior.

Required correction: implement every Phase 6 test against served listeners and
durable Postgres. Each regression test must fail when its corresponding fix is
reverted.

### Medium Findings

- The CLI exposes only `auth migrate-grants`; create/get/list/replace/revoke
  grant and binding management commands are still missing.
- `table_for_message` remains an `Option` compatibility API at many runtime
  call sites, so some ambiguous requests still surface as generic unknown-type
  errors instead of typed candidate lists.
- Structured lifecycle items are retained, but warning and force-sync paths do
  not emit every finding as structured tracing fields.
- Certificate `not_before` and `not_after` columns exist, but the create RPC
  cannot set a validity window.
- Profile attributes remain an indefinite password-login fallback; there is no
  deployment state that makes the typed grant mandatory after migration.
- The implementation bumped repository and SDK versions to `0.4.17` before
  compile, test and served acceptance evidence exists.

### Required Landing Order

1. Close CRIT-1 and remove every production legacy certificate fallback.
2. Close CRIT-2 and CRIT-3 before exposing grant RPCs.
3. Make typed grants authoritative for all service credentials, closing CRIT-4.
4. Correct typed certificate parsing, binding replacement and bearer+mTLS
   composition.
5. Route mTLS through all common native method-security gates.
6. Make dotted catalog identities exact-only and propagate typed lookup errors.
7. Make security mutation and outbox audit writes transactional.
8. Complete CLI management and structured warning/force-sync output.
9. Add all served-path acceptance tests.
10. Run the Verification Order from this plan without duplicate builds.
11. Only after green acceptance, retain or re-apply the `0.4.17` version bump and
    update `latest_udb_bug.md` with artifact-backed evidence.

### Review Verdict

Status: **BLOCKED FOR RELEASE**.

The current implementation must not be described as completing the fix plan.
The typed proto/store surface is useful groundwork, but credential authority is
still split between durable grants, API-key records and mutable profiles. The
release gate remains closed until the critical lineage and tenant-boundary
findings are removed and served-path acceptance passes.
