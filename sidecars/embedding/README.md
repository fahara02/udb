# UDB Embedding Sidecar

The broker owns durable work, model identity, tenant scope, vector routing, and
ACK/NACK state. This sidecar owns inference and document parsing. Provider
credentials never enter broker events: work carries a `vault://` reference and
the sidecar resolves it through `UDB_VAULT_RESOLVER_URL` with a short cache.

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
`provider_endpoint_ref`, and optional parent text plus character/token boundaries
for contextual retrieval or late chunking. Reports echo the durable identity so
the broker can validate the model, dimensions, hash, source, and point before it
stores and ACKs the vector.

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
