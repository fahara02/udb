# UDB 0.4.14 — AmbuLife Integration Findings Triage (2026-07-18)

**Triage-only artifact.** No source, generated code, or manifests were changed to
produce this. Each finding was reproduced/root-caused against the live tree on
branch `refactor/modularize-services`. It exists to set the **0.4.14 scope** and to
ground a *planned, surgical, ripple-mapped* fix for each item — these live in core,
older subsystems (SQL/IR compiler, planner tenant/RLS gate, Casbin authz engine,
buf codegen, manifest router), so no quick patches.

Two governing directives from the maintainer:
- **Authz = Casbin only, nothing else.** The string-vocab coarse path is the "second
  surface" to eliminate; all authorization converges on the one Casbin engine.
- **Surgical fixes only.** Map full blast radius → written plan → minimal change →
  verify (build + the specific CI gate + a regression test). No config-only-then-
  defer-regen splits (that is exactly what sank the GEN-001 fix).

## Status of the whole finding set

| ID | Severity | Confirmed | Fix size | 0.4.14 |
|---|---|---|---|---|
| REL-001 (0.4.13 unpublished) | Blocker | ✅ | release run | **FIXING NOW** — `v0.4.13` release building/publishing |
| GO-001 (login hints dropped) | High | already fixed | — | done (enterprise.go:121) |
| GO-002 (static bearer/refresh) | High | already fixed | — | done this session (940ce6fb) |
| GO-003 (Adopt UUID guard) | High | already fixed | — | done (prior) |
| GO-004 (Entity idempotency) | High | already fixed | — | done (prior) |
| STO-001 (upload resume) | High | already fixed | — | `ReissueUploadUrl` shipped |
| **GEN-001** (Go `google/api` panic) | Blocker | ✅ | atomic regen | **P1** |
| **SRV-001** (ABAC vocab mismatch) | Blocker | ✅ | small + CI guard | **P1** |
| SRV-002 (service_identity `unknown`) | High | ✅ | medium | P2 |
| GO-005 / Bug 2 (no CAS / no UPDATE) | High | ✅ | proto field | P2 |
| SRV-003 / Bug 6 (bare-name routing) | High | ✅ | router + lint | P2 |
| Bug 5 (RLS w/o tenant column bricks) | High | ✅ | planner + lint | P2 |

PG data-plane codec bugs from `bug_report_16_7_26.md` (geography 1a/1b, inet 1c,
date 1d, smallint 3) were fixed in the PR chain and ship in 0.4.13.

---

## P1 — GEN-001: Go SDK vendors `google/api` → init-panic for genproto consumers

**Confirmed.** `buf.gen.yaml:11-26` has no `google/api` disable; the managed
`go_package_prefix` override rewrites the vendored protos into a private package.
`sdk/go/gen/google/api/{annotations,field_behavior,http}.pb.go` are committed, and
**24 UDB service stubs blank-import** the private package (e.g.
`sdk/go/gen/udb/core/authn/services/v1/authn_service.pb.go:10`). No stub imports
canonical `google.golang.org/genproto/googleapis/api`; `sdk/go/go.mod` lacks it. Any
Go binary linking both the SDK and canonical genproto (grpc-gateway users, the
Firebase/FCM client) panics at init: `google/api/http.proto is already registered`.

**Why it was reverted (the key lesson).** The fix `36925326` was **config-only**
(added `- path: google/api` to `managed.disable`) but deferred stub regen + dir
deletion + `go.mod` to "the release regen pass." That instantly reddens the CI gate
`Proto (buf) → Verify committed stubs are current` (`.github/workflows/ci.yml:681-710`):
the new config regenerates stubs importing genproto, but the tree still holds the
vendored copies → `git diff --quiet` fails. So it was reverted (`b541c410`), and the
release regen `18c3728d` then ran on the *reverted* config, re-emitting the copies.
The gate's own comment (ci.yml:682-686) even *asserts* the vendored copies are correct.

**Surgical fix (one atomic commit):** (1) `buf.gen.yaml` add `- path: google/api` to
`managed.disable`; (2) `rm -rf sdk/go/gen/google/api`; (3) regen exactly as CI does
with the pinned toolchain (buf `1.65.0` via `bufbuild/buf:1.65.0` Docker):
`buf generate --include-imports` → `openapi-postprocess.mjs` → `check-openapi-api-rules.mjs`
→ `sdk-codegen-postprocess.mjs`; (4) `cd sdk/go && go mod tidy && go build ./...`;
(5) update the ci.yml:682-686 comment to reflect Go-imports-genproto. Commit all
together so the freshness gate is clean.

**Blast radius:** Go SDK only (24 stub imports swap to genproto; 3 private files
deleted). Other 5 SDKs unaffected (the rewrite is a `go_package_prefix` artifact).
New transitive dep `genproto/googleapis/api` enters `sdk/go/go.mod` — the intended
single-registrant cost. Gate to satisfy in the same commit: buf-stub freshness.

---

## P1 — SRV-001: ABAC action vocabulary mismatch denies all CRUD under default-deny

**Confirmed, end-to-end, and the repo contradicts itself.** DataBroker hardcodes the
Casbin **action** as `Select`/`Upsert`/`Delete` (`handlers_data.rs:96,161,304,417`;
batch `:257,367`), fed straight into the one Casbin engine (`service/mod.rs:619-625`
→ `casbin_engine.rs:138-244`). Matching is exact/prefix only (no alias). The shipped
`examples/ts_enterprise/scripts/bootstrap.sh:69` seeds `"operation":"data.select"` /
`data.upsert` / `data.delete` → never matches → **every CRUD `PERMISSION_DENIED`**
under default-deny. Meanwhile `docs/abac_seed.json` uses the *working* `Select`/`*`
vocab, and the `using-udb` skill (`udb-skill/shared/using-udb.md:469-478`,
`udb-skill/openai/instructions.md`) uses the *broken* `data.*` vocab.

**Why CI/live E2E never caught it.** Every green live-SDK run sets
`UDB_ABAC_DEFAULT_ALLOW=true` (`.github/actions/broker-env/action.yml:183`,
`ci.yml:346`, `docker-compose.integration.yml:9`, `.env.local:67`), which bypasses
policy evaluation. The **only** default-deny path is `ts_enterprise/serve.sh:38-39`
("DELIBERATELY NOT SET") — and its own `bootstrap.sh` seeds the broken vocab. So the
sole default-deny surface ships broken and the green tests never touch it.

**Casbin-only framing:** data CRUD already runs through **one** engine — the fix is
to unify the *vocabulary*, not add/remove an engine. Establish ONE canonical action
namespace as **generated constants** shared by handlers, docs, examples, SDK helpers;
`handlers_data.rs` is the source of truth for what is actually submitted. (The genuine
non-Casbin "second surface" to converge separately: control-RPC `"*"` short-circuit
`service/mod.rs:597-599`, `require_portal_permission` `:691`, admin/`udb:dispatch`
scope gates — these bypass Casbin and are the real "something else.")

**Surgical fix:** pick the canonical token (recommend the request-side `Select`/
`Upsert`/`Delete` since that is what the broker submits, matching `docs/abac_seed.json`),
emit it as shared constants, and correct the drifted seeders (`bootstrap.sh:69`, the
`using-udb` skill, native-service examples). Add the CI consistency guard the report
asks for: fail when any example/doc action string is not one DataBroker submits. Add a
default-deny live test that runs the checked-in bootstrap unchanged and proves seeded
CRUD succeeds (no `UDB_ABAC_DEFAULT_ALLOW`).

**Blast radius:** if the canonical token stays `Select/Upsert/Delete`, code is
unchanged; churn is in seeds/docs/skill/examples + a new CI guard + a new default-deny
test. If instead the token becomes `data.*`, the three `.authorize(...)` literals +
`docs/abac_seed.json` + the runtime authz tests flip. **Note:** this directly fixes the
false claim in the (just-humanized) `ts_enterprise` README that its bootstrap "seeds a
real policy so you can watch it work."

---

## P2 — SRV-002: configured service identity becomes `unknown` for password bearers

**Confirmed.** Bearer `service_identity` is sourced ONLY from the JWT claim,
hardcoding `"unknown"` otherwise and never reading `x-service-identity`
(`security.rs:1314-1316`); the header fallback lives only in the non-JWT/mTLS branch
(`:1347-1354`) a bearer never reaches. Password logins carry no `service_identity`
claim (signer inserts only when non-empty `:1023-1025`; person users have empty
service identity, `authn/mod.rs:1813`, `sessions.rs:807-818`). `UDB_SERVICE_IDENTITY_REQUIRED`
is readiness/production-validation only (`security.rs:244,409-411`), never a
request-time deny. Net: a configured `Config.ServiceIdentity` cannot become the ABAC
subject or audit identity via password login.

**Casbin-only fix:** a service subject must come from a **verified credential**, never
a trusted header. Issue a service-account / client-credentials JWT whose
`service_identity`/`sub` claim carries the service principal (the signer already
supports it, `security.rs:937-974`); the SDK's `Config.ServiceIdentity` should drive a
client-credentials token exchange. Make "identity required" a **request-time** deny in
`service/mod.rs::authorize()` (fail closed when the resolved subject is `unknown`/empty
and a policy requires a concrete subject), gated by the existing flag. Keep the header
path dead for bearers (correct zero-trust).

**Blast radius:** `security.rs:1308-1331` (resolution) + `authorize()` request-time
enforcement; a new client-credentials token-exchange path + SDK wiring; docs. No proto
change if the claim already exists on the token.

---

## P2 — GO-005 / Bug 2: no compare-and-swap and no true UPDATE on the generic API

**Confirmed.** `UpsertRequest`/`DeleteRequest` (`proto/udb/entity/v1/relational.proto:47-63`)
have no `expected`/version/precondition field — only `idempotency_key` (dedups replays,
not distinct racers). The **response** already carries `resource_version`
(`mutation.proto:14-29`) — an asymmetry: the server hands back a version but offers no
request field to condition on it. No `Update` RPC (`data_broker.proto:44-62`). Upsert
compiles to `INSERT … ON CONFLICT DO UPDATE` (`ir/compile/postgres.rs:429-486`), so the
full tuple is NOT-NULL-checked before conflict resolution → a partial record fails 23502
"required field missing" even when the row exists (`executor_utils.rs:576,603-609`). A
**real UPDATE compiler already exists** (`postgres.rs:509-579` `compile_update`) but is
reachable only via the privileged, production-gated `GenericDispatch`
(`handlers_data.rs:977-989,458-468,~2151`). `LockService` has fencing tokens but the Go
`Udb` facade exposes no Lock facade, and mutations can't require a token atomically.

**Surgical fix (recommended):** add an `expected` precondition field (field 9) to
`UpsertRequest` (e.g. `google.protobuf.Struct` column→value, or `int64 expected_version`
keyed to a manifest version column), evaluated in the same tx/RLS context → mismatch
`FAILED_PRECONDITION`; pair with an `update_only` path that reuses the existing
`compile_update` (kills the 23502 partial-record failure). **New field, not new RPC** →
smaller blast radius: no GOLDEN service/method row, no bench count-pin bump, no authz
inventory entry; buf regen + typed-client passthrough + native-contract descriptor +
the guard emission across the 5 SQL compilers (`mysql/mssql/sqlite/clickhouse`) + a
documented fail-closed posture for non-SQL/mediated backends. Backwards-compatible
(unset = today).

---

## P2 — SRV-003 / Bug 6: bare-message-name routing + schema-blind collision lint

**Confirmed.** The one resolver `manifest_index.rs::table_for_message` is FQN-preferred
with a **bare-name first-wins fallback bucket** (`:46-61`): FQN wins when present, else
falls back to the leaf name, whose first inserted owner wins. So a consumer's
`ambulife.authn.entity.v1.User` degrades to the `user` bucket owned by the embedded
`udb.core.authn.entity.v1.User` → routes to `udb_authn.users`. Serve-time "collisions"
are the bare-name-derived physical table name counted **schema-blind** at
`lint.rs:96-98` (`duplicate_table_name_across_schemas`), gated by `lifecycle.rs:635-644`
(bypassed by `UDB_STARTUP_FORCE_SYNC`).

**Open question (needs one live check):** that lint is `Warning` severity, and
`passed = error_count == 0` — so warnings alone don't fail the gate. The "20 collisions"
is the consumer's paraphrase; the exact error-vs-warning split (and whether some
collapse to an error-severity `missing_message_projection`/write-owner lint,
`lint.rs:660/682/703`) needs **one `udb lint` run on the full AmbuLife manifest** before
committing a fix. The root keying is bare-name regardless.

**Good news:** embedded + consumer schemas already compose through **one deterministic
FQN-keyed manifest** (`native_catalog.rs:242-289`, dedup on `(proto_package, message_name)`) —
the merge is not the problem; the router fallback + schema-blind lint are.

**Surgical fix:** make the router's bare fallback **ambiguity-refusing** (return
None/ambiguity error when >1 FQN shares a leaf) rather than first-wins, keeping FQN
primary; re-key `duplicate_table_name_across_schemas` by `(schema, table)`.
**Blast radius (the reason for care):** the router is on the hot path — 7 planner sites,
4 `setup_data` sites, `tx_object`, `schema_registry`, `native_catalog::native_relation`,
and all 18 IR `resolve_table` impls currently accept bare names AND physical table names.
Pure-FQN would break native-service CRUD (resolves by model name) and any short-name
caller — hence FQN-primary + ambiguity-refusing fallback, NOT FQN-only. Main SDK regen
cost: `sdk_gen.rs` short-name aliases (`{{ENTITY_SHORT_NAME}}`) stop being globally
unique → FQN-qualify. CDC (uses `table.cdc_topic`), DDL/migration, RLS unaffected.

---

## P2 — Bug 5: `enable_rls: true` without a tenant column bricks the table

**Confirmed.** `table_requires_tenant_column` (`generation/sql/mod.rs:528-532`) returns
true on bare `enable_rls` alone, so the planner's tenant guard fires
`unresolved_tenant_column_error` (`planning/broker/mod.rs:1576-1581`) at six data-plane
sites (`:465,612,726,887,1013,1080`) + the join-fusion path
(`postgres_helpers.rs:187-190`) → every read/write rejected before SQL. The build-time
validator disagrees with the runtime one: `validate_table_security_alignment`
(`manifest/build.rs:1375-1400`) is gated on `table_security_declared` + isolation-mode,
so a bare `enable_rls: true` skips it — nothing flags it at build/lint time; a passing
test even locks the per-request rejection (`broker/mod.rs:2232-2296`). **Worse:**
`enable_rls` is *derived* true whenever project isolation is on (`build.rs:509,121-125`),
so a **project-scoped-only** table with no tenant column is also bricked — the working
independent project block (`broker/mod.rs:472-474` etc.) is never reached because the
tenant gate runs first. Legit victims: Casbin-domain-scoped tables (authz `roles`/
`user_roles`/`policy_rules`) and project-scoped tables.

**Surgical fix (both, discriminated by intent):** (1) in `table_requires_tenant_column`,
stop treating bare `enable_rls` as a *tenant*-column demand — require a tenant column
only when tenant isolation is actually declared, letting the existing project/domain
paths take over; (2) add a build-time/lint gate that fails closed only on the genuinely
unscoped case (isolation declared but neither tenant nor project column resolves) — so a
never-queryable table is caught in CI, not per request.
**Blast radius:** the six planner sites + join-fusion path read the predicate; the
locking test (`broker/mod.rs:2232-2296`) flips; sibling fail-closed registrations
(`embedding_service/errors.rs:146`, `search_service/errors.rs:111`,
`tenant_purge.rs`) reuse the string. Must preserve the read-side tenant-filter
enforcement for genuinely tenant-scoped tables while exempting domain/project ones.
DDL/RLS-policy gen (`default_tenant_rls_policy`) already tolerates a missing tenant
column.

---

## Recommended 0.4.14 sequencing

1. **GEN-001 (P1)** — self-contained (Go SDK + buf gate), unblocks AmbuCore Go binaries.
   Land as one atomic regenerated commit.
2. **SRV-001 (P1)** — unblocks the default-deny enterprise bootstrap; mostly seeds/docs/
   skill + a CI consistency guard + a default-deny test. Pairs naturally with fixing the
   `ts_enterprise` README claim.
3. **Bug 5** and **SRV-003** — both touch the planner/manifest core; do together with a
   single live `udb lint` run on the full AmbuLife manifest to resolve the SRV-003
   error-vs-warning open question first.
4. **GO-005** — proto field addition (`expected` + `update_only`); regen across SDKs.
5. **SRV-002** — service-account/client-credentials token flow + request-time
   identity-required deny.

Each gets its own written fix plan + blast-radius checklist + regression test before any
edit, per the surgical-fix directive. Nothing here is changed yet.

---

## AUTHZ CASBIN-ONLY REARCHITECTURE — implementation plan (2026-07-18, maintainer: "DO THE FIX")

**Decision: HARD CUT.** Data path enforces `policy_rules` (Casbin) ONLY; delete the legacy
`AbacPolicy` env-JSON lane.

### Ground truth (verified)
- Broker `authorize()` (service/mod.rs:617) reads `self.current_abac_snapshot()` = the broker's
  OWN env-JSON `abac_snapshot` (RwLock, from `AbacPolicy` via `from_abac_policies`). Same for
  `handlers_data.rs:244,360`, `handlers_vector.rs:187`, `control_plane/mod.rs:795`.
- The Casbin ENGINE is shared (`casbin_authorize`), but the POLICY SOURCE is not.
- `build_auth_services` (auth_service/mod.rs:92-104) creates a SEPARATE shared `Arc<ArcSwap<AuthzSnapshot>>`,
  PG-warms it from `policy_rules` via `AuthzServiceImpl::shared(...).warm_shared_snapshot()`
  (serve() mod.rs:2301 + interval 2309) — but the broker never reads it.
- `AuthzServiceImpl::load_snapshot_from_postgres` (authz/mod.rs:1230) already builds the full
  AuthzSnapshot (active policy_rules + role bindings + tuples), revision-fenced.

### Surgical fix
1. DataBrokerService: replace `abac_snapshot: Arc<RwLock<Arc<AuthzSnapshot>>>` with a shared
   `authz_snapshot: Arc<ArcSwap<AuthzSnapshot>>`; keep `abac_default_allow`.
2. `build_auth_services`: use `self.authz_snapshot.clone()` as the shared cell (drop the
   `from_abac_policies` env-JSON seed) → broker + authn + authz share ONE PG-warmed cell.
3. Repoint all data-path readers to `self.current_authz_snapshot()` (= `authz_snapshot.load_full()`).
4. Remove: `abac_policies`, `build_abac_snapshot`, `current_abac_snapshot`, `refresh_abac_snapshot`,
   env-JSON loading (`UDB_ABAC_POLICIES_JSON`, config `abac_policies_json`, setup_data), and the
   `AbacPolicy` struct (31 refs) + `from_abac`/`from_abac_policies` — migrate tests to seed via
   AuthzPolicy/policy_rules.
5. Deny hint (casbin_engine.rs:174-179), doctor.rs, preflight.rs, security.rs: point operators at
   AuthzService/policy_rules seeding, not env ABAC. Keep `UDB_ABAC_DEFAULT_ALLOW` (dev escape hatch).
6. `examples/ts_enterprise/scripts/bootstrap.sh` + `using-udb` skill: seed via
   AuthzService.CreatePolicyRule/PutAuthzPolicy with the real `Select`/`Upsert`/`Delete` vocab
   (also fixes SRV-001). Add the CI consistency guard.
7. Verify: cargo build --features webauthn; authz unit tests; a default-deny live proof that a
   policy_rules-seeded Select/Upsert/Delete succeeds and cross-tenant denies.

Fail-closed note: with the env lane gone, an unseeded broker denies all data ops (correct) unless
`UDB_ABAC_DEFAULT_ALLOW=true` (dev). Operators MUST seed via AuthzService — documented breaking change.
