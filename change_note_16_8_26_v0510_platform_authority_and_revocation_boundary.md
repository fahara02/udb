# Change note: trusted platform authority and deterministic tenant revoke

Date: 2026-08-16
Release: 0.5.10

## Changed

- Added a fixed, active, system/global `platform_admin` role authority with
  exact durable provenance checks. Role aliases created by tenants, literal
  role bindings, governance documents, and unproven snapshot rows cannot mint
  platform authority.
- Added `udb auth bootstrap user --platform-admin` as an explicit direct-DSN,
  offline-only trust seam. The served-environment CLI route rejects the flag;
  the ordinary gRPC tenant authority cannot create or assign the reserved role.
- Projected `udb:platform_admin` only from the proven system role and reused one
  canonical platform predicate across method/body tenant-project enforcement.
- Rejected `udb:platform_admin` in the central service-account scope validator.
  Grant create/replace, API-key issue/refresh, certificate binding, and
  request-time service-grant resolution therefore fail closed through the same
  validation path.
- Preserved inclusive tenant revocation (`iat <= cutoff`). After successful
  durable/Redis cutoff publication, the revoke RPC waits until a replacement
  token can be issued in a strictly later second. Redis publication failure
  retains the durable denial boundary and does not claim fresh-login readiness.
- The live SDK workflow now provisions separate ordinary and offline-trusted
  platform users. Go, Python, TypeScript, and PHP verify the platform role and
  route only Analytics global reads, Backup restore, Tenant administrative
  purge, and Authz governance through it; tenant Authz CRUD and self-purge stay
  bound to the ordinary session.

## Regression coverage

- Rust unit coverage checks reserved role provenance/aliases, literal binding
  denial, token scope projection, same-second revoke denial, next-second
  issuance, and forbidden service-grant scopes on requested and approved sides.
- Go, Python, TypeScript, and PHP fixture/routing tests distinguish platform
  actor/reviewer metadata from ordinary caller attribution and target IDs.
- The existing live benchmark workflow supplies the two principals and is the
  CI gate for end-to-end global success plus tenant/project denial behavior.

## Verification

No local Cargo, SDK, build, lint, format, or test command was run, per the
CI-only direction. Static inspection and `git diff --check` are the only local
checks. Required GitHub checks are the normal CI Rust unit jobs and the
`Live SDK Quick Test` benchmark filter covering the four generated SDKs.
