# UDB native-auth event analytics (Kafka → Spark)

This directory holds the **downstream consumer** of UDB's event-driven native
auth services. It closes the loop:

```
authn / authz / apikey service  (Rust gRPC handlers)
        │  emit domain event
        ▼
udb_system.outbox_events        (transactional outbox, Postgres)
        │  CDC engine tails + relays  (src/runtime/cdc)
        ▼
Apache Kafka                    (per-domain topics, e.g. udb.authn.user.login.v1)
        │  subscribe
        ▼
Apache Spark Structured Streaming   (auth_events_streaming.py)
        │  decode EventEnvelope, window per tenant
        ▼
metrics sink (parquet / Delta / dashboard)
```

## Producer side

The native auth services publish events through an outbox sink
(`src/runtime/service/auth_service/events.rs`). Each mutation enqueues a row into
`udb_system.outbox_events`; the existing CDC engine relays it to Kafka. No second
publish path — auth events ride the same relay as broker CDC events.

Topics produced today (see `events.rs::topics`):

| Domain | Topic | Emitted by |
| --- | --- | --- |
| authn | `udb.authn.user.registered.v1` | `create_user` |
| authn | `udb.authn.user.login.v1` | `login` |
| authn | `udb.authn.session.revoked.v1` | `revoke_session` |
| authz | `udb.authz.role.created.v1` | `create_role` |
| authz | `udb.authz.role.assigned.v1` | `assign_role` |
| authz | `udb.authz.access.denied.v1` | `authorize` deny |
| apikey | `udb.apikey.created.v1` | `create_api_key` |
| apikey | `udb.apikey.revoked.v1` | `revoke_api_key` |

## Wire contract: `EventEnvelope`

Every Kafka record value is a JSON `udb.events.v1.EventEnvelope`
(`proto/udb/events/v1/udb_events.proto`):

```json
{
  "event_id":       "11111111-1111-4111-8111-111111111111",
  "event_type":     "udb.authn.user.login.v1",
  "timestamp":      "2026-06-01T12:00:00Z",
  "correlation_id": "login:<user_id>",
  "document_id":    "<user_id>",
  "payload":        { "user_id": "...", "session_id": "...", "tenant_id": "acme", "...": "..." }
}
```

`document_id` is the Kafka partition key (per-aggregate ordering). `payload`
carries the concrete domain-event fields (proto messages in
`proto/udb/core/{authn,authz,apikey}/events/v1`).

## Run

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  analytics/spark/auth_events_streaming.py \
  --bootstrap "$UDB_KAFKA_BOOTSTRAP" \
  --output ./_spark_out/auth_metrics \
  --checkpoint ./_spark_out/_chk \
  --window "1 minute" \
  --console
```

The job produces per-tenant, per-event-type counts over a tumbling event-time
window — the raw feed behind the analytics events
(`udb.analytics.daily.summary.v1`, `udb.analytics.sla.breach.v1`) defined in
`proto/udb/core/analytics/events/v1`.
