# UDB Embedding Sidecar

Provider adapter for master-plan 9.11. The broker remains model-free: it emits
`udb.embedding.work.v1` payloads with row identity, text, model id, and vector
routing only. This sidecar accepts that work payload and returns the JSON body a
trusted sidecar can submit to the internal-only `EmbeddingService.ReportEmbedding`
callback.

Work request contract:

```http
POST /embed
Content-Type: application/json
```

```json
{
  "tenant_id": "tenant-a",
  "source": "contacts",
  "row_pk": "contact-1",
  "text": "Ada Lovelace wrote the first algorithm.",
  "model_id": "deterministic-v1",
  "target_collection": "contacts_vec"
}
```

Response contract:

```json
{
  "status": "embedded",
  "provider": "deterministic",
  "target_collection": "contacts_vec",
  "report_embedding_request": {
    "tenant_id": "tenant-a",
    "source_name": "contacts",
    "row_pk": "contact-1",
    "vector": [0.1, 0.2],
    "model": "deterministic-v1",
    "dims": 16
  }
}
```

Local smoke:

```bash
python scripts/embedding_sidecar_smoke.py
docker compose -f docker-compose.integration.yml --profile embedding up --build -d embedding-sidecar
python scripts/embedding_sidecar_smoke.py --url http://127.0.0.1:58090
```

The built-in `deterministic` provider is for smoke and local fixtures. Production
sidecars should replace `embed_text` with a model provider that keeps credentials
inside the sidecar process. Broker work payloads must never contain credentials;
this sidecar rejects credential-shaped keys recursively to preserve that contract.

This smoke is sidecar-scoped. Full 9.11 proof still requires a live sidecar
consumer to read `udb.embedding.work.v1`, call the broker's internal gRPC
`ReportEmbedding` callback, and observe backfill rows becoming reported vectors.

Gate-D round trip harness:

```bash
python scripts/embedding_sidecar_roundtrip_smoke.py --selftest
python scripts/embedding_sidecar_roundtrip_smoke.py \
  --pg-dsn "$UDB_INTEGRATION_PG_DSN" \
  --sidecar-url http://127.0.0.1:58090 \
  --broker 127.0.0.1:50061 \
  --bearer-token "$UDB_BEARER_TOKEN"
```

The live command consumes one durable `udb.embedding.work.v1` payload from the
outbox/journal, posts it to the sidecar, then calls the internal
`ReportEmbedding` RPC through `grpcurl` using the checked-in proto/import paths
by default. Use `--use-reflection` only against a listener that actually exposes
reflection. It is not a replacement for the observed green Gate-D run; it is the
runnable proof command for that run.
