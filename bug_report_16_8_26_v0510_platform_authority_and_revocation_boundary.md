# Bug report: platform authority provenance and revocation boundary gaps

Date: 2026-08-16
Affected release: 0.5.9
Target correction: 0.5.10
Severity: critical authorization boundary

## Observed defects

1. Tenant administrators could create or bind reserved role labels such as
   `platform_admin`. Token projection treated the label as platform authority
   without proving that it came from an active system/global role.
2. The central service-account grant validator rejected wildcard/admin scopes
   but accepted exact `udb:platform_admin`. API keys and certificate-bound
   service sessions consume those grants, so this could mint a platform claim.
3. The post-release SDK benchmark used one tenant-bound administrator for both
   tenant operations and legitimately global/cross-tenant RPCs. Analytics
   global reads, Backup restore, Tenant administrative purge, and Authz
   governance consequently failed or encouraged unsafe scope broadening.
4. Tenant-wide revocation correctly denies `iat <= cutoff`, but the RPC could
   return during the same wall-clock second. An immediate replacement login
   then received an `iat` equal to the cutoff and was correctly rejected,
   making fresh-session recovery nondeterministic.

## Security model required

- Platform authority is a control-plane capability, not a tenant-selected name.
  It must originate from one fixed active role with exact system/global
  provenance and no tenant/project binding.
- Only a direct-Postgres, offline administrative command may create and assign
  that role. Served-mode tenant RPC authority must not reach the seam.
- Reserved role aliases must be rejected by typed role CRUD, literal bindings,
  governance draft/activation, and snapshot hydration unless exact durable
  provenance is present.
- `udb:platform_admin` must be rejected by the shared service-grant validator on
  both requested and approved inputs. That one boundary must cover grant
  create/replace, API-key issue/refresh, certificate attenuation, and
  request-time grant resolution.
- Tenant-wide revoke keeps the inclusive old-token rule. Once the durable/Redis
  cutoff is published successfully, the RPC must not claim fresh-session
  readiness until issuance time is strictly greater than the cutoff.
- Live benchmark evidence must use separate ordinary and trusted platform
  credentials in Go, Python, TypeScript, and PHP, routing only an exact global
  RPC allowlist through the platform session.

## Acceptance criteria

- Negative tests reject reserved role labels through every served binding path
  and reject `udb:platform_admin` on either side of service-grant validation.
- The offline bootstrap fails closed if the fixed role row has any unexpected
  code/status/system/scope/tenant/project provenance, and served mode rejects
  `--platform-admin`.
- Cross-tenant/global live tests succeed only with the separately provisioned
  platform user; tenant Authz CRUD and self-purge remain on the ordinary user.
- Tokens issued in the cutoff second remain denied (`iat <= cutoff`), while a
  replacement login after a successful revoke response has `iat > cutoff`.

## Verification status

No local Cargo, SDK, build, lint, format, or test command was run. Local work was
limited to static source inspection and `git diff --check`; GitHub CI is the
required verification authority.
