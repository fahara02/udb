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
│    crate v0.5.15 | protocol v1.0.0                                          │
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
| `UDB_AUTH_GRPC_ADDR` | Native auth/control listener |

A few habits keep tokens safe: use short token TTLs (time-to-live) for
privileged workflows, rotate keys through JWKS (JSON Web Key Set) where you can,
and keep private keys in a secret manager rather than in config files.

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

Casbin policy rows do nothing for native RPCs, and token scopes do not authorize
data CRUD — do not confuse them. (A legacy ABAC table, `udb_abac_policies`, still
exists but is no longer consulted for decisions; seed `policy_rules`, not it.)

### Seeding data-plane authorization (the standard path)

With no policy rows, even the bootstrap admin gets `PERMISSION_DENIED` on CRUD.
Three tokens must be exact or a policy silently never matches:

1. `action` is the **literal RPC method name** — `Select`, `Upsert`, `Delete`,
   `Update`, `BulkCas` (case-sensitive), or `*`. The intuitive `data.select` /
   `read` / `write` vocabulary does **not** match.
2. `object` is the `message_type` (proto FQN), or a `keyMatch2` glob, or `*`.
3. `tenant_id` is the caller's canonical **tenant UUID** (not the human code).

Seed the standard set for a project in one idempotent, atomic command:

```bash
# after `udb auth bootstrap user …` printed the tenant UUID $T
udb authz seed --tenant $T --role app_rw          # ALLOW Select/Upsert/Delete/Update/BulkCas on * for role app_rw
udb auth role bind --principal <user-or-service-id> --role app_rw --tenant $T
```

`udb authz seed` writes role-gated rows with the correct tokens; bind both human
users and service accounts to the role and one policy set authorizes both.
`--entity <fqn>` (repeatable) narrows to specific message types; `--action`
(repeatable) narrows the verbs; `--emit <path>` also writes a version-controllable
policy JSON. Equivalents: `AuthzService.PutAuthzPolicy`/`CreatePolicyRule` at
runtime, or `udb policy-seed` for an offline SQL bundle. Dev bypass:
`UDB_ABAC_DEFAULT_ALLOW=true` — but only while `policy_rules` is empty (the first
row flips the gate back to deny-by-default, so seed all rows atomically).

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
control-plane listener on an internal interface rather than a public one. For
internal service-to-service traffic, use mTLS (mutual TLS) where your deployment
profile calls for it.

## Sensitive Data

Mark sensitive and storage-only fields with proto field-security annotations.
From there UDB does the work for you: generated output views and runtime
redaction paths read the descriptor metadata and keep stored secrets out of
public responses.

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
