# Enterprise deployment guide

This is the one-page, from-scratch reference for bringing up a hardened UDB
broker for real clients (gRPC broker + app + Postgres + optional Qdrant/Redis,
behind an edge TLS proxy). It exists because an enterprise bring-up otherwise
means discovering ~8 hard requirements one-at-a-time, each behind a ~2-minute
restart cycle. Run **`udb requirements`** (the backend contract your protos
declare — Postgres/Qdrant/object-store/Redis, with the env var for each) and
**`udb doctor --enterprise`** (manifest-aware preflight) FIRST — together they
list every unmet prerequisite and missing required backend at once, before you
pay a single slow startup.

> Source of truth: the broker now runs a **one-shot enterprise preflight** at
> startup (and in `udb doctor --enterprise`) that reports the entire unmet set in
> a single consolidated log block instead of failing serially. See
> `src/runtime/preflight.rs`.

---

## 1. Minimal env set that boots a hardened broker

Every variable below is real and load-bearing. `udb doctor --enterprise` checks
the auth/session/encryption/authz/redis subset.

| Variable | Purpose |
|---|---|
| `UDB_PG_DSN` (or `DATABASE_URL`) | Primary Postgres DSN. `UDB_PG_DSN` wins if both set. **Use a DIRECT/session endpoint, not a transaction pooler** — see §4. |
| `UDB_JWT_PRIVATE_KEY` | PEM (inline or path) used to sign UDB-issued login JWTs. |
| `UDB_JWT_PUBLIC_KEY` | PEM used to verify JWTs. Without it, JWT validation is "not configured". |
| `UDB_PASSWORD_HASH_SECRET` | Keyed secret for native password hashing. Required to bootstrap/verify native users. (Falls back to `UDB_SESSION_HASH_SECRET`.) |
| `UDB_SESSION_ENABLED=true` | Turns on server-side sessions. Off by default → `Authenticate` (login) returns `FAILED_PRECONDITION: sessions disabled`. |
| `UDB_SESSION_HASH_SECRET` | Keyed secret for sessions (and a fallback for the password/bundle secrets). |
| `UDB_SESSION_BACKEND=postgres` | Where sessions live: `postgres` (default) or `redis` (needs the `redis` feature and `REDIS_URL`). **Postgres** keeps sessions durable and re-checks a revocation atomically with the validation that races it. **Redis** keeps them in memory: a Redis restart without AOF signs every user out, and validation reads then stamps in two steps. Prefer `postgres`. Validation stamps `last_active_at` at most once per touch interval (60 s, or a tenth of the idle TTL when shorter); every other call reads without a lock. |
| `UDB_SESSION_TTL_SECONDS=86400` / `UDB_SESSION_IDLE_TTL_SECONDS=3600` | Absolute session lifetime and idle timeout. The idle timeout can fire up to one touch interval early. |
| `UDB_AUTH_VERDICT_CACHE_MS=2000` | How long a broker remembers that a bearer is still valid (user active, not revoked, session and grant live), so a page's many calls, and a gateway that authenticates every request, do not each pay those round trips. Triggers advance one sequence (`udb_authn.udb_auth_state_version`) when that state changes and publish the new value at commit; each node keeps it in memory from a listener, so a revocation, logout, suspension, grant or membership change made on any node, or in SQL, is refused everywhere within milliseconds and a call costs no database round trip. While a node's listener is disconnected its calls read the sequence (one small statement each). `UDB_AUTH_STATE_READ_PER_CALL=true` always reads it, for no delivery window at that price. The broker installs the triggers on first use; a role without DDL rights gets a warning and only this time bound. `0` checks live state on every call. |
| `UDB_ENCRYPTION_KEY` | 32-byte object/native-state encryption key (base64/hex/raw). Required under `UDB_ENV=production`, any fail-closed posture (`UDB_FAIL_CLOSED` / `UDB_ENTERPRISE_AUDIT`), or `UDB_OBJECT_NATIVE_STATE_ENCRYPTION_REQUIRED=true`: without a key that loads, the broker refuses to start and names this variable (see §2.2). A Vault key source (`UDB_VAULT_ADDR` + `UDB_VAULT_TOKEN`) also satisfies it. |
| `UDB_AUTH_GRPC_ADDR=0.0.0.0:50061` | Exposes the auth control plane — see §3. Defaults to loopback-only. |
| `UDB_SERVICE_IDENTITY_REQUIRED=true` | Require a verified service identity (forced on in production). |
| `REDIS_URL` (or `UDB_REDIS_DSN`) | Redis for the distributed rate limiter / sessions. Without it each broker process enforces the budget on its own, unless `UDB_RATE_LIMIT_FAILURE_MODE=open`. |
| `UDB_RATE_LIMIT_MAX_PER_WINDOW=10000` | **The data-plane budget.** Max DataBroker requests per `(tenant, operation)` bucket per window (default `10000`). This is the knob that governs `Select`/`Upsert`/… — raise it for high-throughput tenants. Short alias: `UDB_RATE_LIMIT_MAX`. |
| `UDB_RATE_LIMIT_WINDOW_SECS=60` | Fixed-window length in seconds (default `60`). Short alias: `UDB_RATE_LIMIT_WINDOW`. |
| `UDB_RATE_LIMIT_ENABLED=true` | Master switch for the data-plane limiter (default on). Set `false` to disable entirely. |
| `UDB_RATE_LIMIT_PER_KEY=false` | Opt-in **per-key** limiting (default off). When on, each credential gets its own window bucket and a key's `api_keys.rate_limit_per_minute` column **raises** its budget above the tenant default (raise-only — never lowers below it). Off = one shared `(tenant, operation)` bucket. |
| `UDB_RATE_LIMIT_FAILURE_MODE=closed` | Behaviour when the limiter's Redis is unreachable: `closed` (retryable error, default), `local` (per-process fallback), `open` (allow). |
| `UDB_QDRANT_URL` | Qdrant endpoint (if using vector features). |
| `UDB_ABAC_DEFAULT_ALLOW=true` | Coarse dev/bootstrap escape hatch — see §5. Omit (default deny) once you seed real policies. |

Aliases: `UDB_PG_DSN` > `DATABASE_URL`; `UDB_REDIS_DSN` > `REDIS_URL`;
`UDB_QDRANT_URL` > `QDRANT_URL`;
`UDB_RATE_LIMIT_MAX_PER_WINDOW` > `UDB_RATE_LIMIT_MAX`;
`UDB_RATE_LIMIT_WINDOW_SECS` > `UDB_RATE_LIMIT_WINDOW`. Mint the first admin
offline with `udb auth bootstrap user`.

> **Data-plane rate limiting (the `ResourceExhausted: … requests per Ns window`
> error).** By default the DataBroker limiter is a fixed window keyed by
> `(tenant_id, operation)` — every principal (API key / service session) in a
> tenant shares one budget (`UDB_RATE_LIMIT_MAX_PER_WINDOW`), so aggregate
> traffic, not per-key traffic, is what trips it. The deny message names the
> governing knob and the offending tenant so you can attribute pressure without
> cross-referencing logs. The `UDB_API_KEY_RATE_LIMIT_*` /
> `UDB_PUBLIC_BOOTSTRAP_RATE_LIMIT_*` env vars govern the auth control-plane /
> public-bootstrap paths, **not** DataBroker.
>
> **Size the budget for your read pattern.** The default of 10,000 requests a minute
> is shared by the whole tenant for each operation, and every table read is one
> `Select`. A schema with one table per kind makes many small reads per screen, so a
> few busy screens can spend the budget. SDK reads retry a refused call a few times
> with a short backoff before raising the error, and those retries spend the budget
> too, so the first symptom can be slow screens rather than errors. To diagnose it,
> watch `udb_rate_limit_denied_total{operation}` and the broker's warning, written
> once per bucket per window. Then raise `UDB_RATE_LIMIT_MAX_PER_WINDOW` to your
> measured peak plus headroom.
>
> **Per-key budgets (`UDB_RATE_LIMIT_PER_KEY=true`).** With the shared per-tenant
> bucket, one noisy principal exhausts the whole tenant. Enable per-key limiting
> to give each credential its own window bucket; then a key's
> `api_keys.rate_limit_per_minute` column **raises** its budget above the tenant
> default (`per_minute × window ÷ 60`). Set it via `CreateApiKey`'s
> `rate_limit_per_minute` field or a direct `UPDATE`. It is **raise-only** —
> because the column is `NOT NULL DEFAULT 60`, honoring it as an absolute ceiling
> would strangle every default key, so a low value never lowers a key below
> `UDB_RATE_LIMIT_MAX_PER_WINDOW`. To throttle a noisy key *downward*, lower the
> tenant default and raise the well-behaved keys instead.

> **CRLF gotcha:** a `.env` saved with Windows CRLF line endings leaves a trailing
> `\r` on each value. UDB now trims env values at load, but prefer LF `.env`
> files — a stray `\r` on a URL/host is still a latent footgun outside UDB.

---

## 2. Production mode (`UDB_ENV=production`) and TLS

`UDB_ENV=production` force-enables mandatory TLS **and** mTLS (all three mTLS
flags), so you must provide the full cert set before the broker will boot:

- `UDB_TLS_REQUIRED=true`, `UDB_TLS_CERT_PATH`, `UDB_TLS_KEY_PATH`
- `UDB_MTLS_CLIENT_CA_PEM` / `…_PATH` (mTLS client CA)

The historical rustls "Could not automatically determine the process-level
CryptoProvider" panic that made production mode unbootable is **fixed** (the
broker installs the `aws-lc-rs` provider at startup). If you don't need broker
TLS, a valid hardened alternative is to terminate TLS at an edge proxy (nginx)
and run the broker plaintext on a private network with real JWT login — see §2.1.

### 2.1 Fail-closed audit posture (`UDB_FAIL_CLOSED`) and its checklist

`UDB_FAIL_CLOSED=true` (alias `UDB_ENTERPRISE_AUDIT=true`) is the **runtime
enforcement posture**: security-sensitive lookups (revocation, JWKS/signing-key,
rate-limit store) **deny on a dependency error** instead of failing open, and the
broker refuses to serve unless a hardened deployment checklist is satisfied. It is
implied automatically by `UDB_ENV=production`.

Because it runs the **whole** production checklist at once, the broker aborts
startup listing **every** unmet item together (not one-at-a-time). The checklist:

| Requirement | Satisfy with |
|---|---|
| Secure transport | broker TLS (`UDB_TLS_REQUIRED=true` + `UDB_TLS_CERT_PATH`/`UDB_TLS_KEY_PATH`) **OR** the trusted-transport acknowledgment below |
| Service identity | `UDB_SERVICE_IDENTITY_REQUIRED=true` **OR** the trusted-transport acknowledgment |
| Authentication | JWT (`UDB_JWT_PUBLIC_KEY` or `UDB_JWT_JWKS_URL`) **or** mTLS |
| No header scopes | do **not** set `UDB_ALLOW_HEADER_SCOPES` |
| Durable audit sink | external HTTP/SIEM (`UDB_AUDIT_SINK_URL=…`) **OR** the data-plane Postgres audit sink (`UDB_AUDIT_SINK=postgres` + `UDB_AUDIT_PG_TABLE=<schema.table>`) **OR** the auth-plane durable export (`UDB_AUDIT_EXPORT_POSTGRES=1`). Whichever you pick, a data-plane sink that keeps nothing (`UDB_AUDIT_SINK=stdout`, or the unwired `kafka`) refuses startup: use `postgres` or `file` |
| PII-safe logging | on by default — only fails if you explicitly set `UDB_PII_SAFE_LOGGING=false` |

**Two escape hatches let a single-node / behind-a-proxy deployment run fail-closed
without standing up broker TLS or an external SIEM:**

- **`UDB_AUDIT_SINK=postgres` + `UDB_AUDIT_PG_TABLE=<schema.table>`** — persists
  data-plane mutation audits to that Postgres table (self-created on first write,
  `CREATE SCHEMA`/`CREATE TABLE IF NOT EXISTS`), no external endpoint. Separately,
  **`UDB_AUDIT_EXPORT_POSTGRES=1`** activates the auth-plane durable log
  `udb_system.auth_audit_log` (login/revocation/policy/admin events — the record
  `udb compliance evidence` chain-hashes). Either satisfies the fail-closed durable
  gate; enable both for a full data-plane + auth-plane trail. (These are two
  independent sinks — `UDB_AUDIT_SINK` governs data-plane mutations,
  `UDB_AUDIT_EXPORT_POSTGRES` the auth plane.)

  > **Point `UDB_AUDIT_PG_TABLE` at a dedicated table and let the sink create it.**
  > Naming a table that already exists is checked at startup: the boot check proves
  > the relation is *writable in the sink's shape*, not merely creatable, and refuses
  > to start (under `UDB_FAIL_CLOSED`) naming the columns that are missing.
  > In particular do **not** point it at `udb_system.udb_admin_audit_log` — that is
  > UDB's own hash-chained admin-audit table, with a different shape and its own
  > writer. Adding columns to it by hand would leave two writers with incompatible
  > expectations on one relation.
- **`UDB_TRUSTED_TRANSPORT=true`** — an explicit, auditable acknowledgment that
  transport is secured **outside** the broker (a TLS-terminating reverse proxy,
  a service mesh with mTLS sidecars, or a trusted private network). It satisfies
  the TLS **and** service-identity gates so the broker need not own certs. Unset,
  every transport gate stays fully enforced — this never silently weakens anything.

**Minimal fail-closed bring-up behind a TLS proxy (copy-paste):**

```bash
UDB_FAIL_CLOSED=true            # deny-on-error + audit posture
UDB_TRUSTED_TRANSPORT=true      # TLS terminated upstream (proxy / mesh / trusted net)
UDB_AUDIT_SINK=postgres         # persist data-plane audits to Postgres, no SIEM
UDB_AUDIT_PG_TABLE=udb_system.data_audit_log   # required for sink=postgres
UDB_AUDIT_EXPORT_POSTGRES=1     # also persist the auth-plane audit log (auth_audit_log)
UDB_JWT_PUBLIC_KEY=…            # (or UDB_JWT_JWKS_URL) — auth is still required
# UDB_PG_DSN / UDB_JWT_PRIVATE_KEY / session + encryption secrets per §1
```

**Full broker-terminated TLS instead?** Drop `UDB_TRUSTED_TRANSPORT` and provide
`UDB_TLS_REQUIRED=true` + `UDB_TLS_CERT_PATH`/`UDB_TLS_KEY_PATH` (+ the mTLS client
CA if you enable mTLS), exactly as §2 above. `UDB_ENV=production` selects that path
automatically.

> `udb doctor --enterprise` reports this checklist (including which escape hatch, if
> any, is active) before you pay a slow startup.

### 2.2 Which gates need `UDB_ENV=production` and which apply under fail-closed

`UDB_ENV=production` implies fail-closed, but fail-closed does **not** imply
production. A staging broker with `UDB_ENV=staging` and `UDB_FAIL_CLOSED=true` gets
every gate in the first table and **none** of the second. If staging should mirror
production, set the production-only variables yourself (for example
`UDB_TLS_REQUIRED=true`, `UDB_MTLS_REQUIRED=true`) and leave the dev switches in
the second table unset.

**Applies under any fail-closed posture** (`UDB_FAIL_CLOSED`, `UDB_ENTERPRISE_AUDIT`,
or `UDB_ENV=production`), in any `UDB_ENV`:

| Gate | Effect |
|---|---|
| Deny on dependency error | Revocation lookup, signing-key/JWKS store and rate-limit store errors deny instead of allowing. |
| Hardened startup checklist (§2.1) | Startup aborts unless transport, service identity, authentication, no header scopes, a durable audit sink and PII-safe logging are satisfied. |
| Encryption-at-rest key | Startup aborts unless an encryption key **loads** (`UDB_ENCRYPTION_KEY`, or a Vault key source). A key that fails to decode or a Vault export that fails is named in the error. Before 0.6.15 a fail-closed broker without `UDB_ENV=production` started keyless and failed at the first secret, native-state or vault write. |
| TURN secret | TURN credentials are minted only with `UDB_TURN_SECRET`; the built-in dev fallback secret is refused (STUN only). Before 0.6.15 this applied only in production. |
| Backup tenant scope | With a backup DB configured, startup aborts unless `UDB_BACKUP_TENANT_ID` or `UDB_ALLOW_CROSS_TENANT_BACKUP=true` is set. |
| Audit table shape | An existing `UDB_AUDIT_PG_TABLE` that lacks the sink's columns aborts startup. |
| Auth readiness | A `noop://` audit sink fails auth readiness instead of passing as a dev setting. |

**Applies only under `UDB_ENV=production`** (or `prod`):

| Gate | Effect |
|---|---|
| TLS and mTLS forced on | `UDB_REQUIRE_SECURE_TRANSPORT`, `UDB_MTLS_REQUIRED` and both internal mTLS flags are forced on, overriding an explicit `false`. |
| TLS defaults | `UDB_TLS_REQUIRED`, `UDB_PG_TLS_REQUIRED`, `UDB_REDIS_TLS_REQUIRED`, `UDB_KAFKA_TLS_REQUIRED` and `UDB_SERVICE_IDENTITY_REQUIRED` default to `true` (an explicit value still wins). |
| Encryption required at config load | `UDB_OBJECT_NATIVE_STATE_ENCRYPTION_REQUIRED` is forced on, so config validation fails without a key source. |
| Default-allow refused | `UDB_ABAC_DEFAULT_ALLOW` aborts startup and fails auth readiness. Under fail-closed alone it is reported, not refused, because CI smoke and staging rely on it. |
| Private OAuth token URL refused | `UDB_VAULT_OAUTH_ALLOW_PRIVATE_TOKEN_URL` is ignored. |

**Applies under the production posture** (`UDB_ENV=production`, **or** both
`UDB_TLS_REQUIRED=true` and `UDB_SERVICE_IDENTITY_REQUIRED=true`), not under
fail-closed alone:

| Gate | Effect |
|---|---|
| Raw generic dispatch | Raw dispatch to a mediated backend is refused unless `UDB_DISPATCH_ALLOW_RAW_<BACKEND>` is set. |
| Dev identity providers | `UDB_SAML_TEST_MODE` and `UDB_OIDC_TEST_MODE` are refused. |
| OTP dev echo | `UDB_OTP_DEV_ECHO` is refused. |
| WebAuthn | The relying-party origin must be non-loopback `https://`, and an attestation/resident-key/user-verification policy this build cannot enforce is refused. |

---

## 3. The auth control plane is on a separate (loopback-default) listener

Every native service — Authn, Authz, ApiKey, IdP, ControlPlane, Storage, Vault,
Messaging, Cache and the rest (the full list is
[docs/generated/native-services.md](generated/native-services.md)) — is **not**
served on the public DataBroker port (`:50051`). They bind a separate internal listener at
`control_plane_addr`, which **defaults to loopback `127.0.0.1:(public_port+10)`**
(i.e. `127.0.0.1:50061`).

Symptoms when it's not exposed:
- `Authenticate`/login against `:50051` → `12 UNIMPLEMENTED` (the auth services
  aren't mounted there).
- against `:50061` from another host/container → `ECONNREFUSED` (loopback only).

Fix: `UDB_AUTH_GRPC_ADDR=<internal-ip>:50061` (bind on a trusted interface only),
and set the SDK's `authTarget` / `auth_target` / `AuthTarget` to it. This is a
deliberate security default — keep the auth plane off the public internet.

---

## 4. Postgres: use a direct/session endpoint, not a transaction pooler

At startup the broker takes a Postgres **session-level advisory lock** to
serialize schema modification. Over a transaction pooler (PgBouncer, Neon
`-pooler`, Supabase `pooler.`, port `6432`) a "session" is not pinned to one
backend, so a crashed instance can **strand the lock** → every subsequent start
sees *"another UDB instance holds the startup advisory lock"* and crash-loops.

- The broker now **warns loudly** at startup when the DSN looks pooled.
- Point `UDB_PG_DSN` at the **direct** endpoint.
- Recover a stranded lock with `udb admin release-lock` — run it **against the
  direct DSN** (the pooler may not see the stranded backend).

### 4.1 Read replicas serve default reads

When you configure at least one Postgres read replica, a read that names no
consistency mode goes to a replica. Writes always go to the primary. A read
that asks for `strong` (or `primary_read`) always goes to the primary.

| Env var | Default | What it does |
| --- | --- | --- |
| `UDB_PG_REPLICA_DSNS` / `UDB_PG_REPLICA_DSN` | unset | Comma-separated replica DSNs. Unset means every read uses the primary. |
| `UDB_PG_REPLICA_MAX_LAG_SECS` | 3 (minimum 3) | How far behind a replica can be and still serve a default read. A replica further behind is skipped. If no replica qualifies, the primary serves the read. The read never fails because of lag. |
| `UDB_PG_REPLICA_HEALTH_INTERVAL_SECS` | 10 (minimum 10) | How often each replica's lag is measured. |
| `UDB_PG_REPLICA_STRATEGY` | `round_robin` | `least_lag`, `random_healthy`, or `primary_only`. `primary_only` turns replica reads off. |
| `UDB_PG_REPLICA_DEFAULT_READS` | `true` | Set `false` to keep reads that name no mode on the primary. Replicas then serve only reads that ask for `replica_bounded`, `bounded_staleness`, or `eventual`. |

**Read-after-write.** After a caller writes, the broker remembers the write's
primary position (`WriteReceipt.source_lsn`). The caller is identified by
tenant, project, user, and service identity. The broker keeps this position
for the lag bound plus one health interval (13 s by default). The caller's next
default read on the same broker waits up to 250 ms for the replica to reach that
position. If the replica doesn't get there in time, the primary serves the read.
So the caller never reads data older than its own write. After a `BeginTx`, or
when a write has no Postgres position, the caller's default reads go to the
primary for that period.

This memory belongs to one broker process. Suppose a caller writes through
broker A and then reads through broker B. That read is protected only by the
lag bound. To get exact read-your-writes across brokers, pass the write receipt
back as a read fence (`read_fence` / `x-udb-read-fence`). The SDKs already
provide this. The memory is also cleared when the runtime reloads its
configuration.

**What `UDB_PG_REPLICA_DEFAULT_READS=false` costs.** Every read that names no
mode goes to the primary. Your replicas then carry only reads that opt in, and
read throughput is limited by the primary. Turn the default off only if a
caller needs to see writes made through *another* broker within the lag bound
and cannot pass a read fence. Otherwise, mark just those reads `strong`.

---

## 5. Authorization: data-plane Casbin over `udb_authz.policy_rules`

**Data-plane** authorization is **one** Casbin engine over the durable governance
table `udb_authz.policy_rules` — there is no separate in-memory ABAC snapshot and
no env-JSON policy lane (a rule written here **is** the live decision). It is
**default-deny**. (The legacy `udb_abac_policies` table still exists but is no
longer consulted — seed `policy_rules`.) **Native-service RPCs** are authorized by
a *different* surface — the `endpoint_security` token **scopes** on the
method-security layer (`udb:<service>:<method>`), not a Casbin data policy — so a
native `PERMISSION_DENIED` is a missing token scope, not a missing policy row.

- **Live enforcement** reads a **shared, PG-warmed snapshot** of `policy_rules`
  (revision-fenced — the cache invalidates on any policy mutation and a warmer
  reloads it) and evaluates the **real RPC action** the broker submits —
  `Select` / `Upsert` / `Delete` / `Update` / `BulkCas` (NOT a `data.*` alias) —
  against the rules for the request's tenant. Three tokens must be exact or a
  policy silently never matches: the **action** (method name), the **object**
  (the `message_type` FQN or a glob/`*`), and the **`tenant_id`** (the caller's
  canonical tenant UUID). Every request must also carry a non-empty **purpose**.
  With no matching rule the decision falls back to `UDB_ABAC_DEFAULT_ALLOW`
  (default `false` = deny-all) and the denied client sees a `7 PERMISSION_DENIED`
  whose message names the policy command and the token traps.
- **Apply policy** — declare role-gated ALLOW rules in a policy file and run
  **`udb policy apply -f <policies.yaml> --tenant <uuid>`**: it reconciles the
  tenant's rows in `udb_authz.policy_rules` **idempotently and atomically** (one
  transaction, so an open `UDB_ABAC_DEFAULT_ALLOW` window never half-closes;
  `udb policy diff` previews it). Then bind principals to the role:
  `udb auth role bind --principal <id> --role app_rw --tenant <uuid>` (works for
  users AND service accounts). Equivalent at runtime: `AuthzService.CreatePolicyRule`/
  `PutAuthzPolicy` (effective immediately). `udb authz seed` and `udb policy-seed`
  were removed in 0.6.0.

- **Tenants do not inherit policies.** A rule matches its exact tenant domain, or
  every tenant when its tenant is `*`. A parent tenant's rules do not apply inside
  its child tenants: apply the policy file to each tenant. A service account
  reaches another tenant only through an approved membership and
  `ExchangeServiceTokenForTenant` ([service-accounts.md](service-accounts.md)).
- **Roles add token scopes only when they declare them.** A person bound to a role
  carries that role's `token_scopes` (inside the role's own tenant and project);
  a role without `token_scopes` adds none. Service-account scopes come from the
  grant, never from roles.

- **Snapshot refresh cost.** Every `UDB_AUTHZ_SNAPSHOT_TTL_SECS` (default 5)
  the warmer reads one small revision fingerprint from
  `udb_authz.authz_revisions`. The full policy/role/tuple reload runs only when
  that revision moved, when a loaded role binding or tuple has reached its
  expiry, or when the last full reload is older than
  `UDB_AUTHZ_SNAPSHOT_FULL_RELOAD_SECS` (default 60). Every authz RPC that
  changes policy, roles, assignments or tuples (and `udb policy apply`) appends a
  revision, so its effect is still visible on the next tick on every node.
  Writes that bypass the revision (raw SQL against the `udb_authz` tables, a
  tenant restore) are picked up within `UDB_AUTHZ_SNAPSHOT_FULL_RELOAD_SECS`.
  Set it to `0` to reload in full on every tick.

For dev/bootstrap: `UDB_ABAC_DEFAULT_ALLOW=true` (only while `policy_rules` is
empty). For production: seed real least-privilege `policy_rules` and leave
default-deny on.

---

## 6. Talking to a self-signed / mTLS broker from an SDK

`secure: true` alone uses the system CA roots and presents **no** client cert, so
it cannot reach a private-CA or mTLS broker. Pass explicit TLS material instead.

TypeScript (`@udb_plus/sdk`) — `UdbProjectConfig.tls` threads to both the data and
auth clients:

```ts
const udb = new UdbProject({
  // ...
  tls: {
    rootCerts: fs.readFileSync("ca.pem"),       // private/self-signed CA
    privateKey: fs.readFileSync("client.key"),  // mTLS only
    certChain: fs.readFileSync("client.crt"),   // mTLS only
  },
});
```

- For a private CA with no client cert, set only `tls.rootCerts`.
- For mTLS, add `tls.privateKey` + `tls.certChain`.
- (grpc-js fallback: `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` can override the default
  CA bundle for `secure:true`, but passing `tls.rootCerts` is the supported path.)

---

## 7. Faster restarts (skip re-bootstrap when unchanged)

By default the broker re-runs the full migration/provision/verify suite on every
start (idempotent, but ~2 min against a remote DB). When your proto schema hasn't
changed between restarts, set:

```
UDB_STARTUP_SKIP_IF_UNCHANGED=true
```

On start the broker compares the persisted manifest checksum
(`proto_schema_versions`) against the current one; if they match it skips the
generate/apply/provision/verify phases and goes straight to serve (the cheap
advisory-lock + ledger-DDL + system-catalog bootstrap still run). Logs
`UDB fast start: proto manifest checksum unchanged …`.

Tradeoff: external schema/store drift is not re-verified on a fast start (same as
`UDB_SKIP_UNCHANGED_VERIFY`). Force a full run with `udb admin force-sync` or by
unsetting the flag. `force-sync`, `UDB_FORCE_RESEED`, and dry-run always take the
full path.

> **Note — what actually makes a fresh start slow.** Two different costs:
> 1. The migration/provision/verify suite above — dominant against a **remote DB**
>    (network × many idempotent statements). `UDB_STARTUP_SKIP_IF_UNCHANGED`
>    targets this.
> 2. **Backend connectivity.** Each configured backend is probed at startup. A
>    configured-but-**unreachable** backend (a stale DSN, a down service) used to
>    stall the whole boot for that driver's full timeout (e.g. MongoDB's ~30 s
>    server-selection), serially. The broker now **bounds each backend's startup
>    probe** and degrades a too-slow backend to "unavailable" (it keeps serving
>    every reachable backend). Tune the budget with `UDB_BACKEND_STARTUP_PROBE_SECS`
>    (default 8). A backend that times out is logged: `backend registration
>    exceeded the startup probe budget … backend=<Kind>`. If a healthy backend is
>    merely slow to connect, **raise** this value. The cleanest fix is still to not
>    configure DSNs for backends you don't run — a single-Postgres broker starts in
>    a few seconds.

## 8. Honest startup signaling

- The early `udb DataBroker starting (bootstrapping; …)` line means the process
  started, **not** that gRPC is accepting. The public socket binds at the END of
  startup (minutes against a remote DB). Wait for `UDB DataBroker is ready`
  before pointing clients at it. (The metrics port binds early — don't use it as
  a readiness probe for the data plane.)
- Fatal startup failures (migration / advisory-lock) now log each error on its
  own line (not one opaque JSON blob) and exit non-zero.

## 9. Single-node deployments without Kafka (CDC)

If the deployment has no Kafka but `KAFKA_BROKERS` is set (e.g. inherited from a
shared compose env), the change-data-capture tailer and the storage→asset
consumer keep trying to reach the broker. Set **`UDB_CDC_ENABLED=false`** to make
this a clean full-stop: the tailer and consumer never start, **and** nothing
writes transactional-outbox rows: data mutations, auth events and native-service
events alike (so `udb_system.outbox_events` no longer grows unbounded with no
consumer to drain it). An event a caller enqueues by name
(`EnqueueOutboxEvent`, a transaction's enqueue step, DLQ replay) is still stored
and is delivered once CDC runs. To deliver events without Kafka, keep CDC on and
set `UDB_CDC_JOURNAL_ONLY=true`. Default is enabled.

- When CDC stays enabled but Kafka is briefly unreachable, the broker no longer
  floods the log: the "failed to publish to kafka" and consumer recv-error lines
  are collapsed to one per cooldown (carrying a suppressed count). Tune the CDC
  cooldown with `UDB_CDC_PUBLISH_FAIL_LOG_COOLDOWN_SECS` (default 30).
- CDC publish is fully decoupled from the write path: an unreachable broker
  **never** rolls back or fails a data mutation — the outbox row commits with the
  write, and delivery is retried asynchronously by the tailer.

### 9.1 Background polling cadences

These loops run whether or not anything changed. Each idle tick is one small
indexed read; the heavier work runs only when that read shows a change. The
defaults favour latency; raise them to cut idle database traffic on a quiet
fleet.

| Variable | Default | What one idle tick costs |
|---|---|---|
| `UDB_CDC_POLL_INTERVAL_MS` | `250` | The CDC outbox tailer: one `outbox_events` page read. |
| `UDB_CDC_STREAM_JOURNAL_POLL_MS` | `500` | Per open `StreamCdc` subscriber: one primary-key read of the journal head; the journal range read runs only when the head moved past the subscriber's cursor. |
| `UDB_LIVEQUERY_JOURNAL_POLL_MS` | `500` | Per live-query feed (one per topic, tenant and project, shared by its subscribers): one indexed journal probe. |
| `UDB_CDC_TOPIC_POLICY_RELOAD_INTERVAL_MS` | `1000` | One read of the (small) topic policy table; the generation bumps only when a row changed. |
| `UDB_CATALOG_RECONCILE_INTERVAL_MS` | `2000` | Backstop behind the `udb_catalog_reload` notification: one catalog generation read; the full reconcile runs only when it moved. |
| `UDB_AUTHZ_SNAPSHOT_TTL_SECS` | `5` | One authz revision read; see §5 for when the full reload runs (`UDB_AUTHZ_SNAPSHOT_FULL_RELOAD_SECS`). |

A zero, empty or unparsable value keeps the default. The `LockService` lease
table is created once per broker process (retried after a failure), not on each
`AcquireLock`/`RenewLock`.

## 10. Diagnosing an opaque `INTERNAL` on a write

An unexpected database failure on a write (e.g. `PostgreSQL upsert failed`) now
logs the full detail at ERROR server-side — SQLSTATE, the driver message, and the
violated constraint/relation, plus the failing SQL statement — so the cause
(an RLS `WITH CHECK`, a `BEFORE UPDATE` trigger on audit columns, a CHECK
constraint, …) is visible in the broker log. The client status additionally
carries the SQLSTATE and constraint/relation. To also surface the raw driver
message to the client, set `UDB_VERBOSE_DB_ERRORS=true` (off by default, since a
few driver messages can quote row values).
