# UDB Embedding Sidecar

The broker owns durable work, model identity, tenant scope, vector routing, and
ACK/NACK state. This sidecar owns inference and document parsing. Provider
credentials never enter broker events: work carries a `vault://` reference and
the sidecar resolves it through `UDB_VAULT_RESOLVER_URL` with an authenticated,
tenant-and-project-scoped short cache. Set `UDB_VAULT_RESOLVER_TOKEN`; the
resolver receives JSON `{reference, tenant_id, project_id}` and a bearer token.
The sidecar refuses production Vault resolution without all three scope/auth
inputs.

Endpoints:

- `POST /embed` and `/v1/embed`: one durable work item.
- `POST /embed-batch` and `/v1/embed-batch`: up to 256 work items and one
  `ReportEmbeddingBatch` request.
- `POST /rerank` and `/v1/rerank`: deterministic local reranking or a configured
  cross-encoder provider.
- `POST /parse` and `/v1/parse`: built-in text/HTML parsing or a configured
  layout-aware parser.
- `GET /healthz`: provider and dimension readiness.

Work includes `work_item_id`, `chunk_hash`, model dimensions/dtype/task,
`tenant_id`, `project_id`, `provider_endpoint_ref`, and optional parent text plus character/token boundaries
for contextual retrieval or late chunking. Reports echo the durable identity so
the broker can validate the model, dimensions, hash, source, and point before it
stores and ACKs the vector.

The image runs in one of two modes, chosen by `UDB_EMBEDDING_SIDECAR_MODE`:

- `http` (default): the HTTP inference contract above only. Something else
  must read `udb.embedding.work.v1`, call these endpoints, and submit the
  returned report to the broker.
- `consumer`: the sidecar is that consumer. It consumes `udb.embedding.work.v1`,
  batches provider calls, retries transient provider errors, and reports every
  result back to the broker over gRPC. The HTTP server keeps running, so
  `/healthz` and the endpoints above still work. Projects do not need to write
  their own embedding consumer.

## Consumer mode

```
Kafka udb.embedding.work.v1 -> group by provider key -> one provider call per batch
    -> ReportEmbeddingBatch (successes) / ReportEmbeddingFailure (failures)
    -> commit offsets
```

- **Batching.** A poll window holds up to `UDB_EMBEDDING_BATCH_MAX_SIZE`
  records, or whatever has arrived `UDB_EMBEDDING_BATCH_MAX_WAIT_MS` after its
  first record. The window's records are grouped by `(tenant, project, provider,
  model_name, dimensions, provider_endpoint_ref, task_type, output_dtype)`, and
  each group becomes one `{endpoint}/embeddings` call with an `input` array.
  Results are split back out by `data[].index`. Tenant and project are part of
  the key because the endpoint and API key come from a tenant-and-project-scoped
  Vault lookup, so tenants never share a provider request. Late-chunking items
  are still embedded one at a time.
- **Retries.** The consumer retries 408, 425, 429, 5xx, connection errors and
  timeouts, using exponential backoff with full jitter. It stops after
  `UDB_EMBEDDING_PROVIDER_RETRY_MAX_ATTEMPTS` attempts and reports the item with
  `retryable=true`. The broker then reschedules the item within its own attempt
  budget, and dead-letters it once that budget is spent. Any other 4xx, or a
  malformed provider response, is reported right away with `retryable=false`.
  When a multi-item call fails permanently, the consumer retries each item on
  its own, so one bad input does not fail the rest of the batch.
- **Reporting.** Successes go out as one `ReportEmbeddingBatch` per
  (tenant, project), with at most 256 items in each. If the broker rejects an
  item in a batch, the consumer reports that item through
  `ReportEmbeddingFailure`.
- **At-least-once delivery.** Auto-commit is off. Offsets are committed only
  after every record in the window has been reported, whether it succeeded or
  failed. Only a **transient** report failure rewinds a window: the broker is
  `UNAVAILABLE`, the call hits `DEADLINE_EXCEEDED`, or the broker answers
  `RESOURCE_EXHAUSTED` or `ABORTED` even after the report retry policy. In that
  case nothing is committed: the consumer seeks back to the window's first
  offset and processes the window again after a backoff. Duplicate reports are
  safe because the broker matches a report to its work item by `work_item_id`
  and `chunk_hash` and upserts the vector by point id.
- **Records that cannot succeed are skipped.** The consumer logs them, counts
  them, and commits past them so that a single tenant never stalls a
  partition. This covers:
  - a record that is not JSON, or that carries a credential-shaped key;
  - an item whose tenant has no bearer token configured;
  - an item the broker refuses for a non-transient reason, such as
    `UNAUTHENTICATED`, `PERMISSION_DENIED` or `INTERNAL`.

  The error log names the tenant, the project and the work item ids, never the
  token. The window log line carries an `unreportable` count, and a running
  total. The broker's visibility-timeout sweep then re-sends or dead-letters
  each skipped work item. Fixing a tenant's token therefore heals its work on
  the next re-send, without replaying Kafka. A tokens file that cannot be read
  affects every tenant, so it counts as transient and rewinds the window.
- **Graceful shutdown.** On SIGTERM or SIGINT, the consumer either finishes or
  rewinds the window in flight, then closes the Kafka consumer.

### Environment

| Variable | Default | Purpose |
|---|---|---|
| `UDB_EMBEDDING_SIDECAR_MODE` | `http` | `http` or `consumer` |
| `UDB_EMBEDDING_KAFKA_BOOTSTRAP` | `$UDB_KAFKA_BROKERS`, else `127.0.0.1:9092` | Kafka bootstrap servers (comma-separated) |
| `UDB_EMBEDDING_KAFKA_TOPIC` | `udb.embedding.work.v1` | Work topic |
| `UDB_EMBEDDING_KAFKA_GROUP` | `udb-embedding-sidecar` | Consumer group. To scale out, add replicas to the same group |
| `UDB_EMBEDDING_KAFKA_AUTO_OFFSET_RESET` | `earliest` | Where a new group starts reading |
| `UDB_EMBEDDING_KAFKA_SECURITY_PROTOCOL`, `..._SSL_CAFILE`, `..._SSL_CERTFILE`, `..._SSL_KEYFILE`, `..._SASL_MECHANISM`, `..._SASL_USERNAME`, `..._SASL_PASSWORD` | unset | Kafka TLS and SASL settings, passed to kafka-python |
| `UDB_EMBEDDING_KAFKA_MAX_POLL_INTERVAL_MS` | `600000` | Must be longer than the worst-case time to embed and report one window |
| `UDB_EMBEDDING_BATCH_MAX_SIZE` | `64` (capped at 256) | Maximum records per window and per provider call |
| `UDB_EMBEDDING_BATCH_MAX_WAIT_MS` | `500` | How long a window waits to fill once its first record arrives |
| `UDB_EMBEDDING_PROVIDER_RETRY_MAX_ATTEMPTS`, `..._BACKOFF_BASE_SECONDS`, `..._BACKOFF_CAP_SECONDS` | `5`, `0.5`, `30` | Provider retry policy |
| `UDB_EMBEDDING_REPORT_RETRY_MAX_ATTEMPTS`, `..._BACKOFF_BASE_SECONDS`, `..._BACKOFF_CAP_SECONDS` | `5`, `0.5`, `30` | Retry policy for report calls that fail with UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED or ABORTED |
| `UDB_EMBED_PROVIDER_TIMEOUT_SECONDS` | `30` | Timeout for each provider HTTP call |
| `UDB_EMBEDDING_BROKER_TARGET` | `127.0.0.1:50061` | The broker's **native** listener |
| `UDB_EMBEDDING_BROKER_TLS_CA`, `..._CERT`, `..._KEY`, `..._SERVER_NAME` | unset | mTLS to the broker. Required for any non-loopback target |
| `UDB_EMBEDDING_BEARER_TOKEN` or `UDB_EMBEDDING_BEARER_TOKEN_FILE` | unset | Service bearer for a single tenant |
| `UDB_EMBEDDING_BEARER_TOKENS_FILE` | unset | JSON `{"<tenant uuid>": "<jwt>", "*": "<fallback>"}`. Reloaded when the file changes |
| `UDB_EMBEDDING_REPORT_DEADLINE_SECONDS` | `30` | Deadline for each report gRPC call |
| `UDB_EMBEDDING_LOG_LEVEL` | `INFO` | Log level |

Provider settings work the same way as in HTTP mode: `UDB_VAULT_RESOLVER_URL`
and `UDB_VAULT_RESOLVER_TOKEN`. Consumer mode needs the packages in
`requirements.txt`. The image installs them; outside the image, run
`pip install -r requirements.txt`. HTTP mode needs only the standard library.
The `udb.core.embedding.services.v1` stubs come from the `udb-client` wheel. In
a checkout of this repository, the consumer falls back to `sdk/python/gen`.

### What the operator must provision

1. **Kafka publishing on the broker.** The broker must publish its outbox to
   Kafka, which requires `UDB_KAFKA_BROKERS` on the broker. Without it,
   `udb.embedding.work.v1` never reaches the topic. The native embedding
   service must also run on Postgres.
2. **An internal transport path to the broker.** `ReportEmbedding`,
   `ReportEmbeddingBatch` and `ReportEmbeddingFailure` are
   `internal_grpc_only`. The broker accepts them only from a loopback peer or
   from a caller with a verified mTLS client identity. That leaves two options:
   - Run the sidecar on the broker's host or in its pod, and dial
     `127.0.0.1:50061` over plaintext.
   - Give the sidecar a client certificate signed by a CA the broker trusts.

   The sidecar refuses to start with plaintext against a non-loopback target.
3. **A service bearer for each tenant.** Endpoint security on these RPCs sets:
   - `AUTH_MODE_BEARER`, with scope `udb:embedding:report-embedding`;
   - credential types `BEARER_JWT`, `SESSION` or `SERVICE_ACCOUNT`;
   - `tenant_required` and `request_context_required`.

   The tenant claim in the token must equal the work item's tenant, because a
   header cannot select the tenant. A deployment that serves several tenants
   therefore needs one token per tenant, supplied through
   `UDB_EMBEDDING_BEARER_TOKENS_FILE`. If a bearer source is configured but
   holds no token for a tenant, that tenant's items are skipped and logged
   while the other tenants carry on. If no bearer source is configured at all,
   calls go out without `authorization`; use that only against an auth-disabled
   development broker. The authz policy must also allow that
   principal the decision resources `embedding.ReportEmbedding`,
   `embedding.ReportEmbeddingBatch` and `embedding.ReportEmbeddingFailure`.
   Service accounts need an explicit grant.

   On every call the sidecar sends these headers:
   - `authorization`;
   - `x-tenant-id` and `x-project-id`, both taken from the work item;
   - a fresh `x-request-id`;
   - `x-correlation-id`.
4. **Provider credentials in Vault.** These are set up the same way as in HTTP
   mode.

To run the sidecar in consumer mode:

```bash
pip install -r sidecars/embedding/requirements.txt
UDB_EMBEDDING_SIDECAR_MODE=consumer \
UDB_EMBEDDING_KAFKA_BOOTSTRAP=kafka:9092 \
UDB_EMBEDDING_BROKER_TARGET=127.0.0.1:50061 \
UDB_EMBEDDING_BEARER_TOKENS_FILE=/run/secrets/udb-embedding-tokens.json \
UDB_VAULT_RESOLVER_URL=https://vault-resolver.internal/resolve \
UDB_VAULT_RESOLVER_TOKEN=... \
python sidecars/embedding/embedding_sidecar.py
```

The consumer-mode unit tests use fakes: a fake Kafka consumer, a fake reporter,
and an in-process gRPC servicer for the transport.

```bash
python -m pytest sidecars/embedding/tests
```

## HTTP mode notes

`UDB_EMBED_PROVIDER=deterministic` is only for local smoke and fixtures.
Production OpenAI-compatible providers require a Vault secret with `endpoint`
and `api_key`; contextual and late-chunking models additionally require
`contextualizer_endpoint` and `late_chunking_endpoint`. Reranking uses
`UDB_RERANK_PROVIDER` plus `UDB_RERANK_VAULT_REF`. Layout-aware parsing uses
`UDB_DOCUMENT_PARSER_VAULT_REF`.

Run the local contract gates:

```bash
python scripts/embedding_sidecar_smoke.py --selftest
python scripts/embedding_sidecar_smoke.py
python scripts/embedding_sidecar_roundtrip_smoke.py --selftest
python scripts/embedding_retrieval_eval.py
```

The live round-trip harness consumes a complete `udb.embedding.work.v1`
envelope, preserves its durable fields, calls the sidecar, then invokes the
internal `ReportEmbedding` RPC. It requires Postgres, `grpcurl`, a running broker,
and a sidecar:

```bash
python scripts/embedding_sidecar_roundtrip_smoke.py \
  --pg-dsn "$UDB_INTEGRATION_PG_DSN" \
  --sidecar-url http://127.0.0.1:58090 \
  --broker 127.0.0.1:50061 \
  --bearer-token "$UDB_BEARER_TOKEN"
```
