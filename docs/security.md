# Security

UDB security is request-context based. A request is authorized from service
identity, tenant, project, purpose, scopes, catalog version, and backend route.

## Production Defaults

Set:

```env
UDB_ENV=production
UDB_SERVICE_IDENTITY_REQUIRED=true
UDB_MTLS_REQUIRED=true
UDB_TLS_REQUIRED=true
UDB_PG_TLS_REQUIRED=true
UDB_REDIS_TLS_REQUIRED=true
UDB_KAFKA_TLS_REQUIRED=true
UDB_ALLOW_HEADER_SCOPES=false
UDB_PII_SAFE_LOGGING=true
```

`UDB_ENV=production` enables stricter security defaults in
[../src/runtime/security.rs](../src/runtime/security.rs). `APP_ENV` only selects
dotenv files; it is not the production security switch.

## TLS And mTLS

Use file paths or inline PEM env values:

```env
UDB_TLS_CERT_PATH=/etc/udb/tls/server.crt
UDB_TLS_KEY_PATH=/etc/udb/tls/server.key
UDB_MTLS_CLIENT_CA_PATH=/etc/udb/tls/client-ca.crt
```

The server also accepts `UDB_TLS_CERT_PEM`, `UDB_TLS_KEY_PEM`, and
`UDB_MTLS_CLIENT_CA_PEM`.

## ABAC

ABAC policies can be supplied from JSON env, file, or the system table:

```env
UDB_ABAC_POLICY_FILE=docs/abac_seed.json
UDB_ABAC_SCHEMA=udb_system
UDB_ABAC_TABLE=udb_abac_policies
UDB_ABAC_DEFAULT_ALLOW=false
```

Use `policy-lint` before loading policy changes.

## Native Auth Lifetimes

Native authn defaults are intentionally conservative but usable:

```env
UDB_SESSION_TTL_SECONDS=86400
UDB_SESSION_IDLE_TTL_SECONDS=3600
UDB_OTP_TTL_SECONDS=600
UDB_OTP_COOLDOWN_SECONDS=60
UDB_NATIVE_ACCESS_TTL_SECS=900
```

Sessions last up to one day with a one-hour idle window. OTPs last ten minutes
with a sixty-second resend cooldown. Native direct-DB access grants last fifteen
minutes so clients re-authorize regularly while avoiding per-request DSN churn.

## Audit

Admin and security-sensitive operations should emit audit records. Supported
sink settings include:

```env
UDB_AUDIT_SINK=postgres
UDB_AUDIT_PG_TABLE=udb_system.udb_admin_audit_log
UDB_AUDIT_MIN_SEVERITY=info
```

Other sink modes include `none`, `stdout`, `file`, and `kafka`.

## Encryption And Key Management

Static keys are available for local/dev use. Vault transit settings are present
for production integration:

```env
UDB_ENCRYPTION_KEY_V1=
UDB_ENCRYPTION_ACTIVE_VERSION=1
UDB_VAULT_ADDR=https://vault.example.com
UDB_VAULT_TRANSIT_MOUNT=transit
UDB_VAULT_TRANSIT_KEY_NAME=udb
```

Do not send Vault tokens over plain HTTP unless `UDB_DEV_MODE=true` is an
intentional local-only override.

## Supply Chain

Run:

```powershell
cargo deny check advisories bans licenses sources
```

The publishable package should resolve from crates.io only. The repository keeps
vendored logical-replication decoder provenance in source comments and notices,
but does not rely on Cargo git patches for release builds.
