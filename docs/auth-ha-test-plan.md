# Auth HA Test Plan

This plan is for the Phase 1.1 multi-process HA gate. The live Rust tests under
`src/runtime/service/auth_service/tests/ha_*_live.rs` still prove cross-instance
convergence inside one process. They do not prove a killed broker process hands
singleton work to another broker. The multi-process proof uses Docker Compose.

## Topology

Run two real broker containers against one durable integration stack:

- `udb-ha-a` on host ports `50061` / `50062`
- `udb-ha-b` on host ports `50071` / `50072`
- shared `postgres`, `redis`, `kafka`, `qdrant`, and `minio`

The services are defined under the `broker-ha` profile in
`docker-compose.integration.yml`. Their deterministic hostnames are part of the
oracle because singleton owner ids are recorded as
`<hostname>:<pid>:<worker_name>`.

## Lease-Failover Oracle

Run:

```bash
scripts/ha_multinode_smoke.sh
```

The script starts the HA profile and watches `udb_system.udb_cdc_lock_log` for
`udb:projection:materializer`:

1. Wait until exactly one HA broker owns the projection-materializer singleton.
2. Kill the owning broker container.
3. Wait for the peer broker to acquire the same lock key.
4. Assert the new row has a higher `fencing_token`.

This proves the holder-dies/backup-wins path with real broker processes and the
same durable lease table used by production singleton workers.

## CDC No-Duplicate-On-Restart Oracle

Run:

```bash
scripts/ha_cdc_no_duplicate_smoke.sh
```

The script starts the same HA profile, waits for the CDC row-level lock holder,
publishes a unique outbox event, and proves it is journaled and visible on Kafka
exactly once. It then kills the CDC holder container, waits for the peer broker
to acquire the CDC lock, reinserts the same `event_id`, and asserts the peer
acks the duplicate from the durable CDC journal without producing a second Kafka
message.

Useful overrides:

- `UDB_HA_PROJECT` changes the Docker Compose project name.
- `UDB_HA_KEEP_STACK=1` leaves containers running after the check.
- `UDB_HA_FAILOVER_TIMEOUT_SECS` changes the takeover wait budget.
- `UDB_HA_WORKER` can point the oracle at another long-lived singleton worker.
- `UDB_HA_CDC_PROJECT` / `UDB_HA_CDC_KEEP_STACK` apply the same project-scope
  and cleanup controls to the CDC no-duplicate smoke.

## XA Recovery Oracle

Run:

```bash
scripts/ha_xa_recovery_smoke.sh
```

The script composes the integration stack, the canonical MySQL service, and the
XA-specific broker overlay in `docker-compose.xa-ha.yml`. It starts two
XA-enabled broker containers, kills one, seeds a real prepared MySQL XA
transaction plus a UDB XA commit-intent ledger row, and waits for the surviving
broker's actual `WORKER_XA_RECOVERY` loop to commit the participant and mark the
ledger `committed`.

This is the process-level counterpart to the ignored Rust live tests in
`tests/ha/xa_two_participant.rs`.

## Remaining Multi-Process Proofs

The Phase 1.1 item is not fully closed until:

- the three Docker smokes above are run and recorded green, and
- the scheduled/manual HA smoke workflow (`.github/workflows/ha-smokes.yml`) is
  observed green. The workflow runs all three smokes with stacks retained long
  enough to upload broker/Postgres/Kafka/MySQL diagnostics, then tears them down.
