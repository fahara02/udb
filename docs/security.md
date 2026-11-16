# Security


```text
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│    ██    ██  ██████   ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│    ██    ██  ██   ██  ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│     ██████   ██████   ██████                                               │
│                                                                            │
│    UNIVERSAL DATA BROKER                                                   │
│    gRPC data plane | native control plane | tenant/project scope guard     │
│                                                                            │
│    crate v0.6.21 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This page explains how UDB keeps data safe: who a request is, what it's allowed
to do, and how secrets stay out of responses. Read it if you're deploying a
broker, wiring up an SDK client, or reviewing a UDB deployment for security.

UDB builds its security on five pieces that work together:

- an **explicit request context** that travels with every call;
- **endpoint security declared in the protobuf descriptors** themselves;
- **native auth and authz services** that decide identity and permission;
- **backend-aware enforcement** that adapts to each database; and
- **redaction metadata**, also generated from the descriptors, that strips
  secrets out of responses.

The rest of this page walks through each piece.

## Request Context

The request context tells the broker who is calling and why. Attach it to every
request except health checks. It should carry:

- `x-tenant-id`
- `x-udb-project-id`
- `x-purpose`
- `x-correlation-id`
- `x-scopes`
- `x-service-identity`
- `x-user-id` when an end user exists
- `x-udb-client-catalog-version`

A read can optionally ask for stronger consistency. Add a read fence
(`RequestContext.read_fence`, header `x-udb-read-fence`) and a consistency mode
(`RequestContext.consistency_mode`, header `x-udb-consistency`) to get
read-your-writes behavior instead of eventual reads. For the details, see
"Consistency, Write Receipts, And Read Fences" in
[native-services.md](native-services.md).

You rarely set these headers by hand: each SDK fills them in from a
language-native metadata object.

Two of these fields anchor everything else. The project id picks the active
application catalog, and the tenant id is the security and isolation boundary
inside that catalog.

## Identity And Authentication

Authentication answers "who is calling?" The native authn service handles a wide
range of credentials: JWT validation, UDB-issued JWTs, refresh tokens,
server-side sessions, API keys, password login, MFA (multi-factor auth), OTP
(one-time passwords), devices, WebAuthn/passkeys, and external identity mapping.

For production, configure identity with these settings:

| Setting | Purpose |
|---|---|
| `UDB_JWT_ISSUER` | Expected token issuer |
| `UDB_JWT_AUDIENCE` | Expected audience |
| `UDB_JWT_PUBLIC_KEY` / `UDB_JWT_JWKS_URL` | JWT validation keys |
| `UDB_JWT_PRIVATE_KEY` | UDB-issued token signing |
| `UDB_DELEGATED_TOKEN_MAX_TTL` | Longest lifetime of a delegated job token (default `24h`) |
| `UDB_AUTH_GRPC_ADDR` | Native auth/control listener |

A few habits keep tokens safe: use short token TTLs (time-to-live) for
privileged workflows, rotate keys through JWKS (JSON Web Key Set) where you can,
and keep private keys in a secret manager rather than in config files.

A backend service authenticates as a service account: a `SERVICE_ACCOUNT` user
plus a typed grant that caps its scopes. Its bearer carries the roles bound to
it, minus every admin-class role a workload may never hold. To ask whether one
of your users may do something, to act in a second tenant, or to read who a
token belongs to, see [service-accounts.md](service-accounts.md).

### Delegated job tokens

A durable job (an export, a report, a nightly sync) often runs long after the
user who started it signed out, and after their 15-minute access token
expired. A delegated job token lets the job act for that user without keeping
the user's own credentials around. It follows the RFC 8693 token-exchange
model: the user's token is the subject, the service is the actor.

The flow:

1. The user starts the job in your app. While their access token is still
   valid, your backend service calls `AuthnService.ExchangeTokenForJob` with its
   OWN service-account bearer and the request fields `subject_token` (the
   user's access token), `purpose` (for example `export`), `job_id` and
   `ttl_seconds`.
2. The broker checks the caller and the user's token, then returns
   `access_token`, `jti`, `expires_at_unix`, `subject` (the user), `actor` (the
   service's identity), `purpose`, `job_id` and `scopes`.
3. The job stores the token with the job and calls UDB with it. Each call
   runs as the user.

Who may mint one: a service account's own bearer whose ACTIVE typed grant holds
the scope `udb:token:delegate`, in the same tenant as the user's token. A
person's bearer, a raw API key (exchange it for a service bearer with
`Authenticate` first), a tenant-membership token or a delegated token is refused
with `PERMISSION_DENIED`, even when it holds the scope. A role cannot
confer `udb:token:delegate`; only a service grant can.

What the user's token must be: a valid, unexpired, unrevoked access token of
an active user. A service token, a tenant-membership token or another delegated
token is refused (a delegated token is never exchanged again), as is a token of
another tenant. These refusals are `PERMISSION_DENIED` and name the cause.

What the minted token carries:

| Claim | Value |
|---|---|
| `sub`, `tenant_id`, `project_id` | The user's, from their token |
| `act` | `{ "sub": <service identity>, "account_id": <service account> }` |
| `purpose` | The requested purpose, signed, so it counts as a verified purpose (it relaxes the PII rules for `export`, `verification` and `audit` as a signed purpose does) |
| `job_id` | The requested job id |
| `jti` | `dlg_` followed by a new UUID |
| `scopes` | The scopes both the user's token and the service's grant allow; never `udb:token:delegate` or `udb:authn:exchange-tenant-token` |
| `delegated` | `true`: no refresh token is issued, and the token cannot be exchanged again |
| `exp` | now + `ttl_seconds` (0 asks for the maximum), at most `UDB_DELEGATED_TOKEN_MAX_TTL` |

`UDB_DELEGATED_TOKEN_MAX_TTL` takes seconds or a number with an `s`, `m`, `h`
or `d` suffix. The default is `24h`.

Every use re-checks the token's live state through the same check (and the same
short verdict cache) as every other UDB token: the user is active and not
revoked, the `jti` and the tenant are not revoked, and the service still has an
ACTIVE grant holding `udb:token:delegate` on an ACTIVE service account. Removing
the scope from the grant, revoking the grant, deactivating the service account,
suspending the user or revoking the `jti` stops the token at its next use.

Every mint publishes `udb.authn.token.delegated.v1`, and every use publishes
`udb.authn.token.delegated.used.v1` when the token's live state is checked (at
most once per verdict-cache window per token). Both carry the actor, the
subject, the `jti`, the job id and the purpose. Each request served with the
token is also logged on the `udb.audit.authn` target.

SDK helpers: TypeScript `UdbAuthClient.exchangeTokenForJob({ subjectToken,
purpose, jobId, ttlSeconds })`, Go `AuthClient.ExchangeTokenForJob(ctx,
ExchangeTokenForJobRequest{...})`, Python `UdbAuthClient.exchange_token_for_job(
subject_token, purpose=..., job_id=..., ttl_seconds=...)`. Each is called with
the service's own bearer.

### Expired and revoked tokens

A refused bearer says why, on every listener, for every token (not only
delegated ones). Both refusals stay `UNAUTHENTICATED`:

| Reason | When | What to do |
|---|---|---|
| `UDB_TOKEN_EXPIRED` | The token's `exp` has passed (beyond the clock-skew leeway) | Refresh the token or sign in again. A delegated token cannot be refreshed: mint a new one from a fresh user token. |
| `UDB_TOKEN_REVOKED` | The token is no longer backed by live state: its `jti`, session or refresh family was revoked, its user or tenant was revoked or deactivated, or a delegated token's service lost its grant or `udb:token:delegate` | Do not retry with the same token; sign in again. |

Other bearer refusals (a bad signature, a wrong issuer or audience) keep
`UDB_AUTHENTICATION_FAILED`. The SDKs keep classifying all three as
`unauthenticated` and add `isTokenExpired` / `isTokenRevoked` (TypeScript),
`IsTokenExpired` / `IsTokenRevoked` (Go) and `is_token_expired` /
`is_token_revoked` (Python).

## Authorization

Authorization answers the next question: "is this caller allowed to do this?"
The native authz service makes that decision using RBAC (role-based),
ABAC (attribute-based), and simple ReBAC (relationship-based) rules, all scoped
to tenant and project domains. Both broker requests and native-service requests
are checked against the request context and the security requirements declared
in the descriptors.

A decision can draw on any of these inputs:

- service identity;
- user id;
- scopes;
- tenant and project ids;
- resource type and id;
- relationship tuples;
- policy bundles;
- endpoint and field annotations.

Once a request passes authorization, `GetNativeAccess` can hand back short-lived,
restricted backend access. Keep this call server-side — never expose it to clients.

### How authorization is enforced (two surfaces)

In practice there are two independent enforcement surfaces, and a `PERMISSION_DENIED`
is cured differently depending on which one you hit:

- **Data-plane CRUD** (`DataBroker` `Select`/`Upsert`/`Delete`/`Update`/`BulkCas`)
  is authorized by the **Casbin** engine over the `udb_authz.policy_rules`
  governance table (default-DENY). To grant access you seed a policy row.
- **Native-service RPCs** are authorized by **token scopes** declared with the
  `endpoint_security` annotation (convention `udb:<service>:<method>`, or an admin
  scope). To grant access you mint a token/API key that carries the scope; a
  service account can only hold scopes present in its `ServiceAccountGrant`.

A person's token gets extra scopes from a role only when that role sets
`token_scopes` (stored as `Role.token_scopes_json`), and only inside the tenant
and project that own the role. A role without `token_scopes` adds none, and
service-account bearers never take scopes from roles: their ceiling is the grant.

Casbin policy rows do nothing for native RPCs, and token scopes do not authorize
data CRUD — do not confuse them. (A legacy ABAC table, `udb_abac_policies`, still
exists but is no longer consulted for decisions; seed `policy_rules`, not it.)

`DataBroker.PutPolicy` (and the `UDB_ALLOW_LEGACY_PUT_POLICY` override) was removed
in 0.6.0. Use `AuthzService.PutAuthzPolicy` or `udb policy apply` to change the live
policy source.

### Seeding data-plane authorization (the standard path)

With no policy rows, even the bootstrap admin gets `PERMISSION_DENIED` on CRUD.
Three tokens must be exact or a policy silently never matches:

1. `action` is the **literal RPC method name** — `Select`, `Upsert`, `Delete`,
   `Update`, `BulkCas` (case-sensitive), or `*`. The intuitive `data.select` /
   `read` / `write` vocabulary does **not** match.
2. `object` is the `message_type` (proto FQN), or a package glob (`shop.v1.*` = `shop.v1` and `shop.v1.<anything>`; dots are literal), or `*`. A path-style object containing `/` uses `keyMatch2`.
3. `tenant_id` is the caller's canonical **tenant UUID** (not the human code).

A policy matches only its exact tenant domain, or every tenant when its tenant is
`*` (or empty). Tenants do not inherit: a parent tenant's policies do not apply inside its
child tenants, so apply a policy file to each tenant that needs it.

Declare the standard set for a project in a version-controlled policy file and
reconcile the tenant to it in one transaction:

```yaml
# policies.yaml — after `udb auth bootstrap user …` printed the tenant UUID $T
tenant: "<T>"
policies:
  - {effect: allow, tenant: "<T>", role: app_rw, resource: "*", action: Select}
  - {effect: allow, tenant: "<T>", role: app_rw, resource: "*", action: Upsert}
  - {effect: allow, tenant: "<T>", role: app_rw, resource: "*", action: Delete}
  - {effect: allow, tenant: "<T>", role: app_rw, resource: "*", action: Update}
  - {effect: allow, tenant: "<T>", role: app_rw, resource: "*", action: BulkCas}
```

```bash
udb policy apply -f policies.yaml --tenant $T     # idempotent; `udb policy diff` previews it
udb auth role bind --principal <user-or-service-id> --role app_rw --tenant $T
```

Role-gated rows authorize every principal bound to the role, human users and
service accounts alike. Narrow `resource` to a message type (or a package glob) and
`action` to specific verbs as needed. Equivalent at runtime:
`AuthzService.PutAuthzPolicy`/`CreatePolicyRule`. (`udb authz seed` and
`udb policy-seed` were removed in 0.6.0.) Dev bypass:
`UDB_ABAC_DEFAULT_ALLOW=true` — but only while `policy_rules` is empty (the first
row flips the gate back to deny-by-default, so apply all rows in one transaction,
as `udb policy apply` does).

Every tenant-scoped data request must also carry a non-empty `purpose` (else it is
denied before authorization runs) and the tenant UUID.

## Identity Providers And Provisioning

To plug UDB into an existing identity system, the native identity-provider
service supports OIDC, SAML, SCIM, JIT (just-in-time) provisioning, external
identity links, and previews of group-to-role mapping.

A typical enterprise setup looks like this:

1. Register the tenant IdP.
2. Configure JWT, OIDC, or SAML validation.
3. Enable SCIM provisioning for users and groups.
4. Map external groups to UDB roles.
5. Validate decisions with `CheckAccess` or an SDK `can()` helper.

## Transport

Encrypt traffic in transit. Use TLS for client traffic, and keep the native
control-plane listener on an internal interface rather than a public one. mTLS
(mutual TLS) is available on every listener (`UDB_TLS_CERT_PATH`,
`UDB_TLS_KEY_PATH`, `UDB_MTLS_CLIENT_CA_PATH`) and is not on by default;
`UDB_ENV=production` forces secure transport and mTLS on and refuses to start
without the certificates.

## Sensitive Data

Mark sensitive and storage-only fields with proto field-security annotations.
From there UDB does the work for you: generated output views and runtime
redaction paths read the descriptor metadata and keep stored secrets out of
public responses.

Reads mask personal data. A column marked `is_pii` (or `mask_in_logs`) comes back
as `***MASKED***` to any caller that does not hold the `udb:pii:read` scope (or
`udb:*`). Grant `udb:pii:read` only to the services and roles that must see the
clear value.

Filtering or sorting on a PII or encrypted column needs the same scope (or a
purpose of `export`, `verification` or `audit`). A filter is a question about
the value: without this rule, a caller that may not see a national ID could
still learn whether a row with a guessed one exists. Blind-index equality
lookups on an encrypted column count too. A service that looks rows up by such
a column holds `udb:pii:read`.

Here's how to handle common classes of sensitive data:

| Data class | Handling |
|---|---|
| Passwords and credentials | Hash or store in a dedicated secret store; never expose in output DTOs |
| API keys and recovery codes | Store only verifier material; return cleartext only at creation time |
| Session and refresh token material | Store verifier state and expiration metadata |
| MFA and WebAuthn state | Treat as account-security material with restricted admin access |
| Audit payloads | Preserve identifiers and redaction context without leaking secrets |

### Vault master-key and database-credential posture

Native Vault remains sealed unless it can authenticate a real master-KEK
envelope. Plaintext or base64-only DEK rows are not migration shortcuts: they
fail closed and require a controlled offline inventory and rewrap. Secret,
transit, raw-SQL, typed-store, and outbox work for one request is pinned to the
same active-project PostgreSQL write authority so routing cannot split key
material from the audit event that records its use.

Dynamic PostgreSQL credentials are deliberately narrower than ordinary database
roles. Each login is bound to the verified tenant, project, active instance,
physical database, policy revision, and explicit relation set. It receives only
direct `SELECT`, no role memberships, no administrative or RLS-bypass flags, and
restrictive fixed-literal tenant/project RLS policies. Changing compatibility
GUCs therefore cannot widen visibility. Callers must provide an idempotency key;
use the single-lease or emergency project revoke RPCs to terminate sessions and
remove the generated role, grants, and policies.

## End-To-End Encrypted Messaging

`MessagingService` is the MLS delivery service and key directory. It never holds
a group secret or sees plaintext, but it does vouch for which signature key
belongs to which device. An honest broker refuses a key change for a registered
device id and checks published KeyPackages and commit leaves against the
directory key, so a swapped key can only come from a compromised directory or
broker. The SDK clients defend against that by pinning keys:

- **Caught:** a different key for a device the client has already seen, whether
  it comes from the directory, a claimed KeyPackage, a Welcome or another
  member's commit. Listing devices and adding members fail closed
  (`KeyChanged`); a changed key inside a group the device already belongs to is
  reported as a key event. The pin never changes until the user compares the
  new fingerprint out of band and accepts it.
- **Reported:** a new device of a user the client already knows, and any new
  device of the user's own account (a broker adding a device to your account).
- **Not caught by pinning alone:** a key that was already swapped the first time
  the client saw that device (trust on first use), or a forged new device of a
  user nobody compares. Users close that gap by comparing safety numbers (12
  groups of 5 digits computed from both users' pinned keys) in person or over a
  call, and by acting on new-device events.

Pins live in the client's device state, so they survive restarts and travel in
an encrypted backup. Downgrading a client to 0.6.0 drops them.

## Audit And Events

Every auth, policy, native-service, and CDC (change-data-capture) event carries
the full context you need for an audit trail: tenant/project, correlation, actor,
operation, resource, and redaction context. Configure your event sinks and
retention policy before you go to production.

### Long-lived CDC authorization

Authorization at stream creation is not permanent authority. `PublishCDC`
periodically revalidates the original bearer session, API key, or certificate
binding together with tenant status, scopes, and the current policy decision.
Revocation, suspension, policy withdrawal, expiry, or a changed scope set closes
the stream; clients reconnect to establish a fresh scope-derived topic filter.
Malformed or no-longer-retained cursors are errors, and an unavailable topic
policy generation or journal read/decode failure terminates delivery rather than
falling back to open access, epoch replay, or a false-idle stream.

## Compliance Profiles

A compliance profile is just a bundle of controls you turn on together. Common
deployments use:

| Profile | Typical settings |
|---|---|
| Baseline SaaS | TLS, JWT validation, audit events, tenant/project metadata, secret manager |
| Regulated auth | MFA, short token TTLs, strict transport, audit retention, policy approvals |
| Enterprise SSO | OIDC or SAML, SCIM, group-to-role mappings, signed policy bundles |
| High isolation | Separate backend credentials, internal native listener, mTLS, per-tenant limits |

These profiles are deployment choices, not switches UDB flips for you. UDB
exposes the controls; you decide which policy your environment requires.

## Supply Chain

To vet your dependencies locally, run:

```bash
cargo deny check advisories bans licenses sources
```

Also keep your generated SDKs and descriptor manifests pinned to the release
version in `versions.json`.
