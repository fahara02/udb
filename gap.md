# UDB_MASTERPLAN_2026 — Gap Analysis (plan claims vs. actual source)

**Date:** 2026-07-01
**Method:** 12 file-partitioned verifier agents (one per phase) followed every tracked item's
code anchors from `UDB_MASTERPLAN_2026.md` into the real `E:/Projects/udb` source tree,
classified each as CONFIRMED / OVERSTATED / WRONG / PENDING, and every non-CONFIRMED verdict
was adversarially re-checked by a second agent (default = overturn unless the code clearly
supports the gap).
**Question asked:** *What is pending, and where is the existing implementation wrong?*

---

## Headline verdict

**The plan's self-assessment matches the source. No capability lies, stubs, fail-open holes,
or unreachable "wired" code were found across all 91 items.** Every item the plan marks `DONE`
was located in source and does what the plan claims; every item marked `PARTIAL` is honestly
partial. The adversarial refutation pass produced **zero overturns** — nothing the plan calls
done turned out to be missing or wrong.

So the direct answer to "where is the implementation wrong": **the audit found no wrong
implementations in source.** What remains is almost entirely *proof under live/multi-process/
host conditions* (Gate D) plus a small number of genuine *code tails* (below).

### Confidence caveat — read this before trusting the "all green"

Two reasons not to over-read the 100%-CONFIRMED result:

1. **The verifiers ran on a fast model (Haiku 4.5).** They reliably confirm that named
   symbols/RPCs/workers *exist and are wired*. They are weaker at catching subtle *semantic*
   wrongness (an off-by-one epoch, a tenant filter that's present but scoped wrong, a CAS that
   races). A zero-kill adversarial pass is a weak signal, not a strong one.
2. **"Source-verified" ≠ "proven correct."** Many `DONE` items have **never executed live**
   (Gate D is deferred to the maintainer's CI/host). A source-complete path that has never run
   under a real multi-process/backend load is exactly where a wrong implementation would hide.
   See the **Risk register** below.

### Coverage gap in this audit
- **Item 9.13 (Notification delivery adapters) was not independently verified** — the Phase-9C
  slice returned findings only for 9.9–9.12. The plan marks 9.13 `PARTIAL` (generic delivery
  worker leader-spawned; SMTP/SES/Twilio/FCM sidecar + broker-worker HTTPS proof remaining).
  Treat 9.13 as *unaudited this pass*, matching the plan's own `PARTIAL`.

---

## What is actually pending

### A. Genuine remaining CODE work (not just observation)

These are the only items where source itself is incomplete — a follow-up commit is required,
not merely a CI run:

| Item | What is still un-built in source | Anchor |
|---|---|---|
| **2.4** Merge two Postgres SQL paths | Bridge functions (`build_select_logical_read` / `build_upsert_logical_write` / `build_delete_logical_delete`) + an ignored A/B oracle exist, **but the production SELECT/UPSERT/DELETE emission has NOT been switched to the bridged IR**, and the planner value-adds have not been consolidated into a data-plane wrapper. Two paths still run in parallel. | `src/planning/broker/mod.rs:441,858,1067`; `src/ir/compile/live_tests/postgres_live.rs:107` |
| **3.2** Vector-store multi-process CAS | Elasticsearch CAS is fully wired. **Qdrant / Pinecone / Weaviate have NO native CAS** — they deliberately **fail closed** for canonical `SystemStores` (`cas_unsupported`). Real backend-native CAS for those three is unbuilt. | `src/runtime/canonical_store/vector_system.rs:271,1486`; `qdrant.rs:145` |
| **5.3** ffmpeg transcode | Executor path (`run_ffmpeg_transcode`, `resolve_ffmpeg_binary`, mp4 validation) is wired, but the **vendored ffmpeg binary is not packaged** — resolver searches env/vendored paths and fails closed when absent. Binary packaging + manifest are TODO. | `src/runtime/service/asset_service/mod.rs:1045,1090` |
| **4.1** WebAuthn attestation | All OpenSSL statement-verification functions are source-wired, but they are behind the `webauthn` feature and the **release build must be produced with that feature enabled** and manually proven. | `src/runtime/service/auth_service/authn/mod.rs:346,452,597,663` |

### B. Generated-artifact freshness (Gate C) — required by any FUTURE proto delta
The current native contract / OpenAPI / six SDK stubs are in sync as of this plan. This becomes
a live gap the next time any proto surface changes (4.3 annotation, 5.2 `trigger_topic`,
5.5 egress, 6.2 rollback RPC, 10.2–10.4 ORM entities/relations/tx, or any new native RPC):
re-run `buf generate` → native contract/docs/baseline → SDK generation → fixture refresh before
release. The `check-sdk-service-coverage.py` guard already fails closed in `quick-gate`.

### C. Proof / observation only (Gate D + closeout) — code is done, execution isn't
These are **not code gaps**. They need the maintainer's CI/host to *run and record* results:

- **0.2 / 0.3** — observe the two native-integration CI steps green; coherent closeout
  commit/tag decision (Cargo.toml is already `0.3.7`; `v0.3.2` tag intentionally never cut).
- **1.1 / 1.2 / 1.4 / 1.5** — multi-process HA suite, all-nine-backend live conformance,
  Kafka-kill/network-drop fault rig, six-language scaffold-compile: all source-wired + posture-
  pinned in `.github/workflows/ha-smokes.yml` / `ci.yml`; **observed green runs remain**.
- **2.2** — backend-by-backend live golden conformance (harness + CI job exist).
- **2.5 / 10.1 / 10.2 / 10.3 / 10.4** — served cross-language SDK/ORM conformance
  (typed query builder, repository CRUD, N+1-safe relations, UoW version-conflict) —
  templates + committed clients present; **live served-path proof pending**.
- **3.1 / 3.2 / 3.6** — ClickHouse-Keeper lease, vector CAS boundary, two-participant 2PC:
  live multi-process rigs pending.
- **5.4** — external SFU (LiveKit container) live proof.
- **9.9 / 9.11 / 9.13** — metering rollup / embedding sidecar / notification adapter
  live-provider proof.

---

## Risk register — "DONE" items where wrongness could still surface

These are source-verified `DONE` but have **not been proven live**. If any implementation is
wrong, this is where it would be — ranked by blast radius. Recommend a targeted live/adversarial
pass here before trusting them in production:

1. **3.3 tenant/principal token-kill** (`revocation.rs`, `lifecycle.rs:311`) — security-critical
   revocation ordering (jti→tenant→principal→DB, fail-closed). Confirmed present; never proven
   under a live Redis-outage + concurrent-login race.
2. **6.4 read-replica routing / 6.5 CDC shard fencing** — correctness depends on real replica
   LSN comparison and producer-epoch fencing across processes; N=1 identity is unit-tested but
   N>1 multi-shard ownership is only source-verified.
3. **3.1/3.2 canonical locks** — CAS correctness under genuine multi-process contention is the
   whole point and is exactly what remains unproven (Gate D).
4. **4.1 WebAuthn attestation chains** — x5c/packed/TPM/Android-Key/FIDO-U2F signature paths are
   source-present but the OpenSSL feature has not been exercised with real authenticator vectors.
5. **9.4 Webhook SSRF guard** — private-range rejection is source-present; worth a live
   DNS-rebinding probe since a miss here is an egress hole.

---

## Bottom line (Pass 1)
- **Nothing tracked in the plan is wrong-in-source per the pass-1 (symbol-existence) check.**
- **See the DEEP PASS below** — a stronger, file-read-grounded re-verification of the high-risk
  items *did* find real bugs that pass 1's symbol-existence check missed.

---

# DEEP PASS (Pass 2) — file-read-grounded correctness re-verification

**Date:** 2026-07-01
**Why:** Pass 1 confirmed 100% of items on a fast model — a weak signal. Pass 2 re-checked the
15 highest-risk areas on a stronger model (Sonnet, high effort). Every agent had to **open the
files, quote exact lines, and trace real behavior** (not just confirm a symbol exists), with an
adversarial "find the hole" stance, and every BUG/INCOMPLETE was independently re-read by a second
agent before landing here. This pass reads real code, so it surfaces wrong implementations that
pass 1 could not.

## Confirmed defects (grounded in quoted code, independently re-confirmed)

### 🔴 HIGH severity

**H1 — WebAuthn TPM & Android-Key attestation don't bind the credential key (item 4.1)**
`src/runtime/service/auth_service/authn/mod.rs`. The chain-of-trust and packed/FIDO-U2F signature
verifiers are real. **But `verify_tpm_attestation_signature` (~663) and
`verify_android_key_attestation_signature` (~868) never check that the `credentialPublicKey`
embedded in `authenticatorData` (the key the broker will actually store and authenticate with)
matches the *attested* key (TPM `pubArea` / the x5c leaf cert).** They only bind to the *bytes* of
`authData` via a hash. A rogue/software authenticator can present a genuine hardware attestation
(certInfo/pubArea or x5c leaf that chains to the configured roots) while embedding an
attacker-chosen, non-hardware-backed key as the actual credential — and the broker accepts it as
"hardware-attested," defeating a tenant's `attested` conveyance policy. `webauthn-rs` is built with
**no** `attestation_ca_list`, so this custom path is the *entire* attestation defense. FIDO-U2F and
packed are **not** vulnerable (U2F re-derives X/Y from authData's own COSE key; packed's leaf key
signs directly). Confirmed by grep: `cbor_read_cose_ec2_es256_public_key` is called only from the
U2F path. **Fix:** parse the COSE key from `authData` and assert equality with `pubArea` (TPM) /
leaf `cert.public_key()` (android-key); reject on mismatch.

**H2 — Webhook SSRF guard is bypassable two ways (item 9.4)**
`src/runtime/service/webhook_service/mod.rs` + `src/runtime/service/mod.rs:2523`.
(a) **DNS-rebinding TOCTOU:** `resolve_and_validate_target` (~278) does its own `lookup_host` and
checks the IPs, then hands the bare URL string to `reqwest`, which does an *independent* DNS
resolution at connect time. No IP pinning / custom connector. An attacker's DNS can answer the
validation lookup with a public IP and the connect-time lookup with `169.254.169.254`/RFC1918.
(b) **Redirect following:** the delivery client is `reqwest::Client::new()` — default policy follows
up to 10 redirects, and only `endpoint.url` is ever validated, never the `Location` header. A public
endpoint can 302 to an internal URL and reqwest follows it, delivering the signed body to internal
infra. Confirmed by grep: no `redirect`/`.resolve(` anywhere. **Fix:** pin the validated IP into a
custom connector (or re-validate the connected peer IP) and set
`redirect::Policy::none()` (or re-validate every hop).

**H3 — MySQL / SQLite / MSSQL IR path injects NO tenant predicate (item 2.2/RLS, cross-cutting)**
`src/ir/compile/{mysql,sqlite,mssql}.rs`. The always-served **Postgres** typed Select is safe
(tenant GUC from the authenticated claim + RLS policy + fail-closed planner check — verified
correct). **But `compile_read`/write/delete for MySQL, SQLite, MSSQL build their WHERE clause purely
from the caller-supplied `op.filter` and never reference `ctx.tenant_id`/`ctx.project_id`** — unlike
ClickHouse/Cassandra (`append_context_predicates` in `ir/compile/util.rs:156`) and
Mongo/ES/Neo4j (unconditional system-field AND). Production **forces** callers onto this mediated IR
path (raw dispatch is gated off), so a tenant-isolated entity hosted on MySQL/SQLite/MSSQL can be
read **cross-tenant** by any authorized caller who simply omits the tenant predicate. `supports_rls`
is honestly `false` (no capability-matrix lie), but the enforcement hole is real and reachable.
**Fix:** mirror `append_context_predicates` in the mysql/sqlite/mssql compilers, OR block
`enable_rls`/tenant-isolation on tables whose backend resolves to these three, OR extend the
planner's "tenant filter required" fail-closed check to their IR path.

### 🟠 MEDIUM severity

**M1 — Read-replica bounded reads can serve stale data without warning at startup (item 6.4)**
`src/runtime/replica.rs`. The LSN-*fenced* path is correct (real `pg_last_wal_replay_lsn`
comparison server-side, failover + `StaleReadWarning`). **But the *unfenced* bounded-read path
trusts optimistic defaults:** `PgReplicaPool::new` sets `healthy=true, lag_millis=0`, and
`refresh_health_once` (the boot seed) `tokio::spawn`s probes **without awaiting them** — so the
`.await` at `setup_data.rs:~2873` returns before any real lag reading lands, and the manager starts
serving. During that window (and after a replica reconnect; probe cadence ≥10s) `is_eligible()` sees
`lag=0` and returns a possibly-far-behind replica with **no** `StaleReadWarning`. Secondary:
`SelectV2` discards the stale warning entirely (documented). **Fix:** pessimistic default
(`healthy=false` until first probe) or actually join the boot probes before publishing the manager.

**M2 — Metering double-counts usage on retry → overbilling / premature quota denial (item 9.9)**
`src/runtime/service/native_helpers.rs:200` + `metering_service/mod.rs:239`. The `admit_on`
auto-meter hook calls `record_usage`, a bare `INSERT` with a fresh `gen_random_uuid()` PK, **no
idempotency key, no `ON CONFLICT`**. Any client retry/replay of a native RPC after a successful
server-side admission inserts a second `usage_events` row for the same logical op — inflating both
the `CheckQuota` windowed SUM (denies legitimate traffic early) and the hourly billing rollup
(`TOPIC_USAGE_ROLLUP`). **Note:** the plan's "fail-open quota" label is *wrong* — `CheckQuota`
actually **fails closed** (`Status::unavailable`) on aggregate error, which is safe. **Fix:** thread
a per-admission idempotency key + unique index + `ON CONFLICT DO NOTHING`.

### 🟡 LOW severity / known-incomplete

**L1 — CDC DLQ ack writes the raw (unfenced) producer epoch (item 6.5)**
`src/runtime/cdc/engine_dlq.rs:~108` (`ack_event`) binds `self.config.producer_epoch` instead of
`fenced_producer_epoch()` — the only CDC writer that doesn't fold the shard. Journal-only audit
inconsistency under sharding (N>1); **not** a fencing/ownership bypass (the in-doubt sweep queries
only the outbox table, whose 7 writers all fold correctly, and the bit-math itself is verified
correct). **Fix:** one-line — bind `fenced_producer_epoch()`.

**L2 — Two Postgres SQL paths not merged; production is 100% legacy planner (item 2.4, INCOMPLETE)**
`src/planning/broker/mod.rs`. The IR bridge fns (`build_select_logical_read` etc.) are `#[cfg(test)]`
— compiled out of production; the only callers are test-only + an `#[ignore]`d live oracle. All
served CRUD SQL is still the legacy `format!()` planner. **This matches the plan's own tracked
status** ("2.4 scheduled after 2.1") and is not exploitable (the legacy path enforces the same
tenant/scope/PII checks). It's a real, already-known migration gap, not a hidden bug.

## Confirmed CORRECT under deep read (adversarial stance, no hole found)
- **3.1 ClickHouse Keeper lease** — real CAS via KeeperMap `keeper_map_strict_mode=1` +
  token-gated conditional UPDATE + confirm-SELECT. (An initial BUG flag was **overturned** on
  re-read against ClickHouse's strict-mode atomicity guarantee.)
- **3.2 fail-closed** — Qdrant/Pinecone/Weaviate return hard typed errors on every CAS/lease/
  sequence path; never registered as canonical stores; test-pinned. No fail-open.
- **3.3 token kill** — inclusive cutoff (`iat <= cutoff`), fail-closed on denylist error (forced in
  prod), correct jti→tenant→principal→DB order.
- **4.3 internal_grpc_only** — derived from tonic transport connect-info (loopback socket / verified
  mTLS), never a forgeable header; fails closed on unknown peer. *Latent footgun:* the gate sits in
  the bearer branch, so a future `AUTH_MODE_PUBLIC + internal_grpc_only` RPC would skip it — no such
  RPC exists today (all 4 use bearer).
- **9.1 Vault seal-gate** — all 13 operating RPCs call `check_seal()?` first (fails closed via a real
  KEK probe); only `SealStatus` skips it and it returns no secret material.

## Re-verified (the 3 degenerate pass-2 agents were re-run, grounded)
- **3.2 Elasticsearch CAS atomicity → CORRECT.** `try_acquire_cas_advisory_lease` GETs the doc's
  `_seq_no`/`_primary_term`, then the CAS write is a single `PUT /_doc/{id}?if_seq_no=..&if_primary_term=..`
  (or `/_create/` for insert-if-absent) — Elasticsearch's native optimistic concurrency; the version
  guard rides in the *same* request, so two racers can't both win (loser gets 409). Retry loop up to
  8×. `vector_system.rs:511-576,1504-1539`. No TOCTOU.
- **2.1 raw-dispatch gate → CORRECT (fail-closed in prod).** When a mediated backend has no compiled
  dispatch and the opt-out env is unset, `raw_dispatch_decision` returns
  `Status::failed_precondition` in production and the `?` at `handlers_data.rs:539` blocks the raw
  fallthrough; test-pinned (`raw_dispatch_decision(&pg, true, false, ..)` → FailedPrecondition). Opt-out
  is honored only when explicitly set; dev counts a metric.
- **9.13 notification delivery → REAL worker, SDKs intentionally sidecar-delegated (matches plan
  PARTIAL).** `run_notification_delivery_worker_once` is leader-spawned under
  `WORKER_NOTIFICATION_DELIVERY` (`service/mod.rs:2611`), dedupes terminal deliveries via a
  `NOT EXISTS` on SENT/DELIVERED attempt rows, is tenant-scoped, and fails closed (no-poison on empty
  config; FAILED outcome + terminal event on missing channel provider). SMTP/SES/Twilio/FCM are **not**
  in-broker — the broker does a generic bearer-auth HTTPS POST to a configured `endpoint_url`;
  provider SDKs live in sidecars by design. Not a stub, not a lie; the "provider proof" that remains
  is the sidecar HTTPS round-trip (Gate D). `notification_service/mod.rs:2131-2503`.

**Net:** all three cleared as **CORRECT / honestly-partial** — no new defects. The confirmed-defect
list above (H1–H3, M1–M2, L1–L2) is complete for this pass.

## What the deep pass changes about the answer to "where is it wrong"
Pass 1's "nothing is wrong" was a fast-model artifact. **The code is largely honest and correct,
but there are genuine wrong implementations — concentrated in the never-run-live security surfaces
the risk register predicted:** WebAuthn attestation (H1), webhook SSRF (H2), and MySQL/SQLite/MSSQL
tenant isolation (H3) are the three to fix first; the replica-startup staleness (M1) and metering
double-count (M2) follow.

## Recommended fix order
1. **H3** MySQL/SQLite/MSSQL tenant-predicate injection — silent cross-tenant read is the worst
   blast radius (multi-tenant data leak on the production-forced path).
2. **H2** Webhook SSRF (DNS-pin + disable redirects) — internal-network / cloud-metadata exposure.
3. **H1** WebAuthn TPM/android-key credential-key binding — defeats attestation assurance.
4. **M1** replica pessimistic-default / join boot probes; **M2** metering idempotency key.
5. **L1** one-line CDC DLQ epoch fold; **L2** the 2.4 merge (already scheduled).
