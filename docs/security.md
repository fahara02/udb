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
│    crate v0.3.7 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
UDB security is based on explicit request context, descriptor-declared endpoint
security, native auth/authz services, backend-aware enforcement, and redaction
metadata generated from protobuf descriptors.

## Request Context

Every non-health request should include:

- `x-tenant-id`
- `x-udb-project-id`
- `x-purpose`
- `x-correlation-id`
- `x-scopes`
- `x-service-identity`
- `x-user-id` when an end user exists
- `x-udb-client-catalog-version`

SDKs attach these values from language-native metadata objects.

Project id chooses the active application catalog. Tenant id remains the
security and isolation boundary inside that catalog.

## Identity And Authentication

The native authn service supports JWT validation, UDB-issued JWTs, refresh
tokens, server-side sessions, API keys, password login, MFA, OTP, devices,
WebAuthn/passkeys, and external identity mapping.

Production identity configuration should include:

| Setting | Purpose |
|---|---|
| `UDB_JWT_ISSUER` | Expected token issuer |
| `UDB_JWT_AUDIENCE` | Expected audience |
| `UDB_JWT_PUBLIC_KEY` / `UDB_JWT_JWKS_URL` | JWT validation keys |
| `UDB_JWT_PRIVATE_KEY` | UDB-issued token signing |
| `UDB_AUTH_GRPC_ADDR` | Native auth/control listener |

Use short token TTLs for privileged workflows, rotate keys through JWKS where
possible, and store private keys in a secret manager.

## Authorization

The native authz service supports RBAC, ABAC, and simple ReBAC decisions with
tenant/project domains. Broker and native service requests are evaluated against
the request context and descriptor-declared security requirements.

Authorization inputs can include:

- service identity;
- user id;
- scopes;
- tenant and project ids;
- resource type and id;
- relationship tuples;
- policy bundles;
- endpoint and field annotations.

`GetNativeAccess` can issue short-lived, restricted backend access after a
successful authorization decision. Keep this server-side.

## Identity Providers And Provisioning

UDB supports OIDC, SAML, SCIM, JIT provisioning, external identity links, and
group-to-role mapping previews through the native identity provider service.

Typical enterprise flow:

1. Register the tenant IdP.
2. Configure JWT, OIDC, or SAML validation.
3. Enable SCIM provisioning for users and groups.
4. Map external groups to UDB roles.
5. Validate decisions with `CheckAccess` or an SDK `can()` helper.

## Transport

Use TLS for client traffic and keep the native control-plane listener on an
internal interface. Use mTLS for internal service-to-service traffic where
required by the deployment profile.

## Sensitive Data

Use proto field-security annotations for sensitive and storage-only fields.
Generated output views and runtime redaction paths use descriptor metadata to
avoid returning stored secrets in public responses.

Recommended handling:

| Data class | Handling |
|---|---|
| Passwords and credentials | Hash or store in a dedicated secret store; never expose in output DTOs |
| API keys and recovery codes | Store only verifier material; return cleartext only at creation time |
| Session and refresh token material | Store verifier state and expiration metadata |
| MFA and WebAuthn state | Treat as account-security material with restricted admin access |
| Audit payloads | Preserve identifiers and redaction context without leaking secrets |

## Audit And Events

Auth, policy, native-service, and CDC events preserve tenant/project,
correlation, actor, operation, resource, and redaction context. Configure event
sinks and retention before production use.

## Compliance Profiles

Compliance-oriented deployments commonly enable:

| Profile | Typical settings |
|---|---|
| Baseline SaaS | TLS, JWT validation, audit events, tenant/project metadata, secret manager |
| Regulated auth | MFA, short token TTLs, strict transport, audit retention, policy approvals |
| Enterprise SSO | OIDC or SAML, SCIM, group-to-role mappings, signed policy bundles |
| High isolation | Separate backend credentials, internal native listener, mTLS, per-tenant limits |

Profiles are deployment choices. UDB exposes the controls; operators decide the
required policy for their environment.

## Supply Chain

Recommended local check:

```bash
cargo deny check advisories bans licenses sources
```

Also keep generated SDKs and descriptor manifests tied to the release version in
`versions.json`.
