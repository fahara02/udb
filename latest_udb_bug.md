# Latest UDB bug status — AmbuLife integration (consolidated)

Date: **2026-07-21 (Asia/Dhaka)**
Reference releases: **official UDB v0.4.17** (release 357268442, Windows binary
SHA-256 `a68635ce07ab82f7059adec8d24a9a1827a29172d85c06bc9fe2600b90e241b6`) and
**v0.4.15** (`69a5e8d9`, the last release whose broker starts — see
UDB-CAT-003).
Client: official Go SDK `github.com/fahara02/udb/sdk/go@v0.4.17`

This file is the one-page consolidated index of every UDB defect found by
AmbuLife integration, with its **latest** status. Full evidence and
reproductions live in `bug_report_17_7_26_ambulife_integration.md`,
`bug_report_21_7_26_ambulife_integration.md` (this directory) and the AmbuLife
root report `E:\Projects\ambulife\bug_report_20_7_26.md`. No UDB source was
modified or compiled by AmbuLife; brokers are run only from checksum-verified
official release binaries.

Status legend:

- **FIXED (verified)** — proven against the published artifact/module.
- **FIXED IN SOURCE (live retest blocked)** — the correcting code is present in
  the `v0.4.17` tag, but the v0.4.17 broker cannot start (UDB-CAT-003), so no
  runtime acceptance is possible.
- **OPEN** — still present in the latest release.

## NEW in v0.4.17

| ID | Title | Severity | Status |
|---|---|---|---|
| UDB-CAT-003 | v0.4.17 broker cannot start: embedded `udb_authn.service_account_grants` and `certificate_bindings` declare `db_table_security tenant_column "tenant_id"` without `tenant_column: true` on the pg_column, failing the broker's own manifest validation at `PROTO_CHECKSUM_LINT` | **Release-blocking regression** | **OPEN** — reproduced with the official Windows binary against any proto root; full report + one-line fix in `bug_report_21_7_26_ambulife_integration.md` |

## Summary table (all prior findings)

| ID | Title | Severity | Latest status |
|---|---|---|---|
| UDB-GEN-001 | Go SDK shipped private `google/api` copies → init-time registry panic | Critical | **FIXED (verified)** since v0.4.15; stays fixed in the v0.4.17 SDK (all AmbuCore/Beacon suites pass on `@v0.4.17`) |
| UDB-REL-001 | Release omitted the module-qualified Go SDK tag | High | **FIXED (verified)** — `sdk/go/v0.4.17` resolves via the Go proxy |
| UDB-REL-002 | v0.4.14 assets replaced in place without provenance | High | **ADDRESSED** — v0.4.15 and v0.4.17 are immutable manifest-verified releases; v0.4.14 stands as a historical incident |
| UDB-GO-005 | No conditional-update (CAS) primitive on Upsert | Critical | **CLIENT API FIXED (verified)**; broker-side live retest blocked by UDB-CAT-003 |
| UDB-GO-006 | Go Entity facade ignored request-scoped `WithMetadata` correlation/purpose | High | **FIXED (verified)** in v0.4.17 — `Client.Context` merges request-scoped audit metadata; identity stays non-overridable; shipped tests prove attacker-controlled identity in context is ignored |
| UDB-AUTH-003 | Password service accounts cannot obtain restricted data scopes | Critical | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 typed `ServiceAccountGrant` + central scope validator (rejects admin/owner/wildcard) drives password login; `udb auth migrate-grants` migrates legacy profiles |
| UDB-AUTH-004 | Scoped native API keys rejected on the DataBroker data plane | Critical | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 async credential layer resolves `x-api-key` against the stored record + owner grant; fail-closed reconciliation (`reconcile_api_key_principal`) |
| UDB-AUTH-005 | Client `x-user-id` replaces the verified bearer subject | Critical | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 binds the principal to the verified JWT `sub`; a disagreeing non-empty `x-user-id` → `PermissionDenied`; same rule mirrored on the API-key and mTLS paths |
| UDB-AUTH-006 | API keys lose service identity | High | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 keys attenuate to the owner's typed grant; key creation requires an ACTIVE grant with immutable `service_identity` |
| UDB-AUTH-007 | mTLS not a scoped alternative when JWT is configured | Critical | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 server-controlled `CertificateBinding` (SPIFFE/DNS/CN/fingerprint) is the only mTLS authority; binding resolves the current grant at request time; headers can never redirect the principal |
| UDB-AUTH-008 | v0.4.15 rejects scoped service API keys on native Storage (blocked partner onboarding live) | Critical | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 method security accepts `CREDENTIAL_TYPE_API_KEY` on declaring native methods; principal faces every post-auth gate |
| UDB-AUTH-009 | ApiKey List missed active owner keys; duplicate ACTIVE names; no CLI revoke | High | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 enforces ACTIVE-name uniqueness per owner and adds offline `udb auth api-key list/revoke` |
| UDB-DAT-001 | JSONB arrays bound as PostgreSQL `text[]` | High | **FIXED IN SOURCE** since v0.4.15 (`bind_one` binds JSON/JSONB before the array branch); live retest pending |
| UDB-MIG-001 | `allow_drop` stale-hint cycle | Critical | **FIXED IN SOURCE** since v0.4.15 (stale hint non-blocking); live retest pending |
| UDB-CAT-001 | Embedded native schemas collide with legal consumer protobuf short names | Critical | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 ambiguity-refusing lookup: exact FQN / `schema.table` resolve deterministically; shared short names refuse with candidate listings instead of first-wins misrouting |
| UDB-CAT-002 | Startup catalog lint hides the actual blocking findings | High | **FIXED IN SOURCE (live retest blocked)** — v0.4.17 keeps structured `LintItem` findings + remediation at startup (and demonstrably prints them — the UDB-CAT-003 failure was fully self-describing) |
| UDB-SRV-003 | FQN composition defect in manifest projection identities | High | **FIXED IN SOURCE (live retest blocked)** — same v0.4.17 canonical `(schema, table)` identity work as CAT-001 |

## What this means for the next UDB release

**One defect now blocks everything: UDB-CAT-003.** The entire v0.4.17 auth and
catalog overhaul is unverifiable because `udb serve` cannot start. The fix is
two one-line proto annotations (`tenant_column: true` on
`service_account_grant.proto` field 4 and `certificate_binding.proto` field 5)
plus artifact regeneration — and a release gate that actually boots `udb serve`
against an empty PostGIS database before publishing.

Once a startable patch release ships, AmbuLife will immediately rerun: typed
grant creation (`udbservicebootstrap` already creates typed grants through the
official RPCs on ≥0.4.17 brokers), key issuance under grant attenuation,
data-plane API-key authentication, Storage register/finalize under scoped keys
(the partner-onboarding blocker), verified-principal negative probes
(`x-user-id` confusion, cross-tenant denial), and the catalog identity model.

## AmbuLife-side status

- AmbuCore and Beacon pin Go SDK `v0.4.17`; complete test suites pass.
- The local runtime runs the official **v0.4.15** broker (last startable
  release) on a durable Docker stack (`infra/udb/docker-compose.local.yml`,
  PostGIS + Redis AOF + MinIO with named volumes) after the 2026-07-21
  ephemeral-container data loss; identities and service keys were
  re-bootstrapped exclusively through official CLI/RPC paths and the smoke
  probe passes (login → upsert → select read-back → delete).
- The fail-closed boundary is unchanged: no owner/admin role for services, no
  locally minted bearer, no header-scope enablement, no default-allow ABAC, no
  raw SQL, no RLS bypass.
