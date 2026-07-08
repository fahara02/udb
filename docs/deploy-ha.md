# High-availability deployment: 3-replica reference architecture

This is the documented, tested reference shape for running UDB as a small
high-availability cluster. It targets master-plan item **6.1 (N-replica
reference architecture)**.

Every guarantee below names the concrete mechanism in the source tree and the
green test that exercises it. Where a guarantee is proven only by the
**shared-pool** convergence rig (multiple service instances over one live
Postgres pool) and not yet by a true multi-process / network-partition rig, it
is explicitly marked **shared-pool-proven, multi-process pending (1.1)**. The
true multi-process HA oracles are source-wired under master-plan item **1.1**
and in `.github/workflows/ha-smokes.yml`, but they are not yet recorded green.
Do not read any guarantee here as partition-tested until that run is observed.

---

## 1. Topology

```
                       ┌──────────────────────────┐
        clients ─────► │   L4/L7 load balancer     │  (round-robin / least-conn)
                       └────────────┬─────────────┘
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
        ┌───────────┐        ┌───────────┐         ┌───────────┐
        │ replica A │        │ replica B │         │ replica C │
        │  udb bin  │        │  udb bin  │         │  udb bin  │
        └─────┬─────┘        └─────┬─────┘         └─────┬─────┘
              └─────────────────────┼─────────────────────┘
                                    ▼
                  ┌──────────────────────────────────────┐
                  │  shared durable control-plane store   │
                  │  Postgres: udb_system.*               │
                  │   (lease ledger, outbox, CDC journal, │
                  │    authz/authn registries, node state)│
                  └──────────────────────────────────────┘
                                    │
                  ┌─────────────────┴───────────────────┐
                  ▼                                      ▼
            Kafka (CDC publish, EOS)              Redis (idempotency claims)
```

All three replicas are **identical** `udb` binaries with identical
configuration. There is no "primary" replica in the configuration — leadership
for singleton work is elected dynamically at runtime through the shared lease
ledger (Section 3). Any replica can serve any client request; coordination
happens entirely through the shared durable store, never through replica-to-
replica RPC.

The three replicas share **one logical control-plane Postgres** (the
`udb_system` schema). That shared store is the single source of truth for: the
singleton lease ledger, the transactional outbox, the CDC publish journal, the
authn/authz registries, and the control-plane node-state ledger.

---

## 2. What each replica runs

Each replica runs the full service surface (data-plane broker + all enabled
native services). Two classes of work behave differently across replicas:

| Work class | Runs on | Coordination |
| --- | --- | --- |
| **Request serving** (data-plane RPCs, auth, native-service RPCs) | every replica, concurrently | none needed — reads/writes are durable on the shared store; per-instance caches converge (Section 4) |
| **Singleton background workers** (CDC source tailers, in-doubt/XA recovery, storage orphan reaper, WebRTC stale-peer reaper, projection materializer/reconciliation, compliance evidence export) | exactly one replica at a time | lease-guarded (Section 3) |

The singleton workers are enumerated as the `WORKER_*` keys in
`src/runtime/singleton.rs` (`WORKER_CDC_POSTGRES_SOURCE`,
`WORKER_CDC_MYSQL_SOURCE`, `WORKER_CDC_MONGODB_SOURCE`,
`WORKER_STORAGE_ORPHAN_REAPER`, `WORKER_WEBRTC_STALE_PEER_REAPER`,
`WORKER_PROJECTION_MATERIALIZER`, `WORKER_PROJECTION_RECONCILIATION`,
`WORKER_XA_RECOVERY`, `WORKER_EVIDENCE_EXPORT`). They are spawned on every
replica via `native_runtime.rs::spawn_while_leader`, which wraps each tick in
`singleton::run_while_leader` — so all three replicas *try* to run the worker
but only the lease holder actually does.

---

## 3. Guarantee: no duplicate singleton work (lease-guarded)

**Mechanism.** `src/runtime/singleton.rs` implements a fencing lease over the
shared `udb_system.cdc_lock_log` relation. `run_while_leader` acquires a
`PostgresSingletonLease` via an atomic `INSERT … ON CONFLICT … DO UPDATE …
WHERE acquired_at < NOW() - ttl RETURNING fencing_token`, heartbeats it on a
TTL/3 interval, and aborts the task if the heartbeat ever fails (the lease was
lost). Takeover is only possible *after* TTL expiry and always **advances the
`fencing_token`**, so a stalled old holder cannot resume as a second writer.
The advertised bound is the typed constant `SINGLETON_HA_TARGET`
(`max_duplicate_winners: 1`, `max_failover_seconds: 5`, `recovery_point: "last
durable SystemStores/outbox/saga/2PC ledger commit"`).

**Tests (unit, deterministic — green in the normal `cargo test` run):**
in `src/runtime/singleton.rs`:
- `worker_lock_keys_are_stable_and_distinct` — every `WORKER_*` key hashes to a
  stable, distinct lock slot (no two workers collide on one lease).
- `lease_ttl_has_lower_bound` — the TTL is floored so failover stays bounded.
- `singleton_ha_target_is_bounded_by_lease_floor` — `SINGLETON_HA_TARGET` is
  pinned to `max_duplicate_winners == 1` and the lease floor.
- `lease_sql_prevents_split_brain_and_fences_stale_owners` — the acquire SQL is
  atomic per lock key, only takes over after expiry, and bumps the fencing
  token; heartbeat/release are owner+token scoped.
- `lease_state_model_has_no_double_winner_before_expiry` — a second broker
  cannot win while the lease is live.
- `lease_state_model_failover_requires_expiry_and_advances_fence` — takeover
  needs TTL expiry and advances the fence to 2.

**Tests (shared-pool convergence, live Postgres, env-gated `#[ignore]`):** the
multi-node suite proves that a mutation on one instance converges on the
instance that did *not* perform it, over the shared durable store —
in `src/runtime/service/auth_service/tests/`:
- `ha_multinode_live.rs` —
  `live_postgres_ha_authz_revision_invalidation`,
  `live_postgres_ha_policy_bundle_revocation`,
  `live_postgres_ha_policy_distribution_ack_nack_rollback` (per-node ACK/NACK
  ledgers: one node's NACK never advances another node's accepted version),
  `live_postgres_ha_apikey_revocation_propagation`,
  `live_postgres_ha_refresh_token_replay_race` (durable token-family reuse
  detection across nodes).
- `ha_convergence_live.rs` —
  `live_postgres_ha_session_revocation_propagation`,
  `live_postgres_ha_logout_all_propagation`.
- `ha_jwks_rotation_live.rs` —
  `live_postgres_ha_signing_key_jwks_rotation` (signing-key rotation +
  compromise propagate via the durable JWKS registry, no per-node cache lag).

**Status:** the lease *logic* (single winner, fencing, bounded failover) is
proven by the deterministic unit tests above. The cross-node *convergence*
property is **shared-pool-proven, multi-process pending (1.1)** — the live
suite runs two/three service instances against one shared pool inside one
process, which exercises the durable-store coordination but not OS-process
isolation, real network partitions, or a genuinely split lease ledger.

---

## 4. Guarantee: per-tenant fairness under load

**Mechanism.** `src/runtime/channels.rs::acquire_fair_with_backpressure`
admits each operation through a chain of scoped semaphores: a base op semaphore,
then a **per-tenant** semaphore (`tenant_sems`, keyed by `scope_key1(op,
tenant)` and bounded by `tenant_limit(op, tenant)`), then per-project,
per-instance, and per-backend-instance semaphores. The per-tenant scoping (the
`tenant:{id}:{op}` granularity, now carried as a precomputed `u64` scope hash
per the PERF note at `channels.rs:191`) caps how much in-flight work any single
tenant can hold on a replica, so one noisy tenant cannot starve the others on
the same replica. `fairness_weight` further weights admission per
(project, tenant, op).

**Status:** this is a **per-replica** fairness control — each replica enforces
its own per-tenant in-flight budget independently. It is in the shipped
admission path. A *cluster-wide* per-tenant budget that coordinates across
replicas is **not** implemented here (that is master-plan 6.3, connection-pool
tiering with per-tenant budgets) — do not claim global per-tenant fairness.

---

## 5. Guarantee: exactly-once side effects

**Mechanism (two layers):**

1. **Transactional outbox + idempotent publish.** Side-effecting events are
   written to the durable `udb_system.outbox_events` table in the same
   transaction as the state change, then drained by the CDC engine. The drain
   path (`src/runtime/cdc/engine_tail.rs::process_outbox_event`) consults
   `was_durably_published(event_id)` (the durable CDC journal) plus a Redis
   `SET NX` idempotency claim before publishing, so a redelivery / replay of the
   same `event_id` is recognized as already-published and acked **without** a
   second publish.

2. **Producer-epoch fencing for Kafka EOS.** The CDC producer uses a stable
   `transactional.id` (`src/runtime/cdc/kafka_tx.rs`, derived from
   `CdcConfig::transactional_id()`) combined with `producer_epoch`
   (`src/runtime/cdc/mod.rs:162`). On takeover, `src/runtime/cdc/indoubt_recovery.rs`
   sweeps rows stuck in `publishing` whose `producer_epoch` is **less than** the
   current epoch (or past the in-doubt timeout) and reconciles them, so a
   zombie/old producer cannot double-emit after a new leader has fenced it.

**Tests:**
- `src/runtime/service/auth_service/tests/ha_multinode_live.rs::live_postgres_ha_cdc_idempotent_double_process`
  (gated on the `kafka` + `redis` features and live Postgres+Kafka+Redis) drives
  `process_outbox_event` twice for the same `event_id` and asserts the second
  pass is a durable-publish-skip + ack, **not** a republish.
- `scripts/ha_cdc_no_duplicate_smoke.sh` source-wires the multi-process Docker
  form of the same oracle: after a first publish is journaled and observed once
  on Kafka, it kills the current CDC row-lock holder, waits for peer takeover,
  reinserts the same `event_id`, and asserts the peer acks the redelivery
  without a second Kafka message. This is still run-pending until the container
  smoke is observed green.
- `src/runtime/cdc/kafka_tx.rs::transactional_id_is_stable_across_restarts`
  (deterministic unit test) — the `transactional.id` is reproducible across
  restarts, which is what makes Kafka's in-doubt recovery actually fence the
  prior producer.
- `src/runtime/cdc/indoubt_recovery.rs` / `src/runtime/cdc/live_tests.rs` carry
  the in-doubt epoch-sweep coverage.

**Status:** outbox dedup + the idempotent-double-process property are proven by
the live test above on the shared pool, and the multi-process Docker restart
oracle is now source-wired. The epoch-fencing logic is proven by the
deterministic `transactional_id` test and the in-doubt recovery module. The
**leader-handoff** exactly-once property under a real split (old leader still
alive on a partitioned process while a new leader fences it) remains
**source-wired but run-pending (1.1)** until the container smoke is recorded
green.

---

## 6. Failure behavior

| Failure | What happens | Backed by |
| --- | --- | --- |
| One replica crashes / is drained | LB stops routing to it; in-flight requests on that replica fail and are retried by the client against another replica. Durable state is unaffected (it lives in the shared store). | request serving is stateless over the shared store — convergence suite (Section 3) |
| Singleton-worker leader crashes | Its lease stops heartbeating; after TTL expiry (`SINGLETON_HA_TARGET.max_failover_seconds`, floored at 5s) another replica wins the lease with an advanced `fencing_token` and resumes from the last durable ledger commit. No duplicate worker runs in the gap. | `lease_state_model_failover_requires_expiry_and_advances_fence`, `singleton_ha_target_is_bounded_by_lease_floor` |
| Stale leader resumes after a pause | Its heartbeat fails (token advanced) → `run_while_leader` returns the "lease lost" error and the task aborts; its CDC publishes are fenced by the lower `producer_epoch`. | `lease_sql_prevents_split_brain_and_fences_stale_owners`; in-doubt epoch sweep (`indoubt_recovery.rs`) |
| Event redelivery after a publish | Recognized via the durable CDC journal + Redis idempotency claim and acked without republishing. | `live_postgres_ha_cdc_idempotent_double_process` |
| In-doubt XA participant after broker loss | The surviving broker's lease-gated XA recovery worker reads the durable `udb_xa_ledger`, resolves registered MySQL/Postgres in-doubt participants, commits commit-intent rows, and parks repeated failures for manual review. | `tests/ha/xa_two_participant.rs`; `scripts/ha_xa_recovery_smoke.sh` (source-wired, run pending) |
| One tenant floods a replica | Its per-tenant semaphore saturates and it receives backpressure (`RESOURCE_EXHAUSTED` / queue timeout); other tenants on that replica keep their own budgets. | `acquire_fair_with_backpressure` (Section 4) |
| Network partition between replicas | **Not yet tested.** The design relies on the shared store remaining the single arbiter, but no partition rig exists. | **multi-process pending (1.1)** |

---

## 7. Operator configuration (env)

Set these identically on all three replicas. Names are taken verbatim from the
config readers.

| Concern | Env var | Notes |
| --- | --- | --- |
| Singleton lease TTL | (constant) | `WORKER_SINGLETON_LEASE_TTL` = 30s in `singleton.rs`; failover floor is 5s (`MIN_LEASE_TTL_SECS`). Not env-tunable today. |
| Lease owner identity | `HOSTNAME` / `COMPUTERNAME` | `singleton.rs::worker_owner_id` builds `"{host}:{pid}:{worker}"`; give each replica a distinct hostname so lease ownership is attributable. |
| CDC producer epoch | `UDB_CDC_PRODUCER_EPOCH` | bump on a coordinated producer-fencing event; the in-doubt sweep fences rows below the current epoch. |
| CDC transactional-id prefix | `UDB_CDC_TRANSACTIONAL_ID_PREFIX` | must be stable across restarts for Kafka EOS recovery to fence the prior producer. |
| CDC idempotency window | `UDB_CDC_*` (see `cdc/mod.rs::from_env`) | Redis URL is required for the `SET NX` idempotency fast-path; without it the durable journal still dedups but the fast-path is skipped. |
| Authz snapshot TTL | `UDB_AUTHZ_SNAPSHOT_TTL_SECS` | bounds how long a non-mutating replica's authz snapshot can lag the durable revision (the convergence window the HA tests rely on). |

All three replicas MUST point at the **same** control-plane Postgres
(`udb_system`), the same Kafka cluster, and the same Redis — coordination is
entirely through these shared durable stores, not replica-to-replica.

---

## 8. What is proven vs. pending

**Proven (green tests today):**
- Single-winner lease + fencing + bounded failover — deterministic unit tests in
  `singleton.rs`.
- Cross-instance convergence of auth/authz/api-key/token/JWKS/control-plane state
  over a shared pool — the `ha_multinode_live.rs` / `ha_convergence_live.rs` /
  `ha_jwks_rotation_live.rs` suites (live Postgres, env-gated).
- Idempotent CDC double-process (no republish) — `live_postgres_ha_cdc_idempotent_double_process`.
- Stable `transactional.id` for EOS recovery — `transactional_id_is_stable_across_restarts`.
- Per-replica per-tenant fairness — shipped in `acquire_fair_with_backpressure`.

**Shared-pool-proven, multi-process pending (1.1):**
- True OS-process isolation of the three replicas (the live suite runs instances
  in one process over one shared pool).
- Network-partition behavior and split-lease-ledger arbitration.
- Leader-handoff exactly-once under a partitioned-but-alive old leader, pending
  observed execution of the Docker HA smokes.
- CDC peer-takeover no-duplicate Docker oracle — `scripts/ha_cdc_no_duplicate_smoke.sh`
  (source-wired; observed container run pending).
- XA recovery peer-survivor Docker oracle — `scripts/ha_xa_recovery_smoke.sh`
  with `docker-compose.xa-ha.yml` (source-wired; observed container run pending).
- Scheduled/manual HA diagnostics workflow — `.github/workflows/ha-smokes.yml`
  runs the three process-level smokes and uploads compose logs before teardown
  (source-wired; first green run pending).

**Not in scope of 6.1 (tracked elsewhere):**
- Cluster-wide per-tenant connection budgets → master-plan 6.3.
- Read-replica routing → 6.4. CDC shard-id epoch fencing for scale-out → 6.5.

Until item 1.1 records a green multi-process / partition run, treat the
convergence guarantees as *durable-store-coordination correct* but not yet
*partition-hardened*.
