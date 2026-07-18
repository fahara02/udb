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
| `UDB_ENCRYPTION_KEY` | 32-byte object/native-state encryption key (base64/hex/raw). Required when encryption-at-rest is mandated, else config validation fails. |
| `UDB_AUTH_GRPC_ADDR=0.0.0.0:50061` | Exposes the auth control plane — see §3. Defaults to loopback-only. |
| `UDB_SERVICE_IDENTITY_REQUIRED=true` | Require a verified service identity (forced on in production). |
| `REDIS_URL` (or `UDB_REDIS_DSN`) | Redis for the distributed rate limiter / sessions. Without it the rate limiter no-ops. |
| `UDB_QDRANT_URL` | Qdrant endpoint (if using vector features). |
| `UDB_ABAC_DEFAULT_ALLOW=true` | Coarse dev/bootstrap escape hatch — see §5. Omit (default deny) once you seed real policies. |

Aliases: `UDB_PG_DSN` > `DATABASE_URL`; `UDB_REDIS_DSN` > `REDIS_URL`;
`UDB_QDRANT_URL` > `QDRANT_URL`. Mint the first admin offline with
`udb auth bootstrap user`.

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
and run the broker plaintext on a private network with real JWT login.

---

## 3. The auth control plane is on a separate (loopback-default) listener

The Authn / Authz / ApiKey / IdP / ControlPlane services are **not** served on the
public DataBroker port (`:50051`). They bind a separate internal listener at
`control_plane_addr`, which **defaults to loopback `127.0.0.1:(public_port+10)`**
(i.e. `127.0.0.1:50061`).

Symptoms when it's not exposed:
- `Authenticate`/login against `:50051` → `12 UNIMPLEMENTED` (the auth services
  aren't mounted there).
- against `:50061` from another host/container → `ECONNREFUSED` (loopback only).

Fix: `UDB_AUTH_GRPC_ADDR=0.0.0.0:50061` (bind on a trusted interface only). This
is a deliberate security default — keep the auth plane off the public internet.

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

---

## 5. Authorization: one Casbin engine over `udb_authz.policy_rules`

Authorization is **one** Casbin engine, not two surfaces. The data plane's
`authorize()` gate and the control-plane `AuthzService.Check` both decide from
the same durable governance table, `udb_authz.policy_rules`. It is
**default-deny**.

- **Live enforcement** reads a **shared, PG-warmed snapshot** of `policy_rules`
  (revision-fenced — the cache invalidates on any policy mutation and a warmer
  reloads it). It evaluates the real RPC action the broker submits —
  `Select` / `Upsert` / `Delete` — against the rules for the request's tenant.
  When no rule matches, the decision falls back to `UDB_ABAC_DEFAULT_ALLOW`
  (default `false` = deny-all), and a denied client sees `7 PERMISSION_DENIED:
  no authz policy (default deny); configure authorization via the AuthzService
  (policy_rules) or set UDB_ABAC_DEFAULT_ALLOW=true for dev`.
- **Seed policy** two ways, both writing `udb_authz.policy_rules`: at runtime
  from app code via `AuthzService.CreatePolicyRule` (effective immediately, the
  snapshot reloads), or offline via **`udb policy-seed`** (emits INSERTs into
  `udb_authz.policy_rules`, piped to psql). There is no separate in-memory ABAC
  snapshot and no env-JSON policy lane — a rule written here **is** the live
  decision.

For dev/bootstrap: `UDB_ABAC_DEFAULT_ALLOW=true`. For production: seed real
`policy_rules` and leave default-deny on.

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
this a clean full-stop: the tailer and consumer never start, **and** mutations
stop writing transactional-outbox rows (so `udb_system.outbox_events` no longer
grows unbounded with no consumer to drain it). Default is enabled.

- When CDC stays enabled but Kafka is briefly unreachable, the broker no longer
  floods the log: the "failed to publish to kafka" and consumer recv-error lines
  are collapsed to one per cooldown (carrying a suppressed count). Tune the CDC
  cooldown with `UDB_CDC_PUBLISH_FAIL_LOG_COOLDOWN_SECS` (default 30).
- CDC publish is fully decoupled from the write path: an unreachable broker
  **never** rolls back or fails a data mutation — the outbox row commits with the
  write, and delivery is retried asynchronously by the tailer.

## 10. Diagnosing an opaque `INTERNAL` on a write

An unexpected database failure on a write (e.g. `PostgreSQL upsert failed`) now
logs the full detail at ERROR server-side — SQLSTATE, the driver message, and the
violated constraint/relation, plus the failing SQL statement — so the cause
(an RLS `WITH CHECK`, a `BEFORE UPDATE` trigger on audit columns, a CHECK
constraint, …) is visible in the broker log. The client status additionally
carries the SQLSTATE and constraint/relation. To also surface the raw driver
message to the client, set `UDB_VERBOSE_DB_ERRORS=true` (off by default, since a
few driver messages can quote row values).
