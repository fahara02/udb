## EmbeddingService
_proto: core/embedding/services/v1/embedding_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | Backfill | MUTATION | BackfillRequest | `{ "tenant_id": "<seed:tenant_id>", "source_name": "sdk_live_records" }` | schedules a backfill for the seeded source. |
| [ ] | DeleteSource | DESTRUCTIVE | DeleteSourceRequest | `{ "tenant_id": "<seed:tenant_id>", "source_name": "sdk_live_records" }` | deletes the seeded source registration. |
| [ ] | ListSources | READ_ONLY | ListSourcesRequest | `{ "tenant_id": "<seed:tenant_id>", "page_size": 50 }` | lists embedding sources for the tenant. |
| [ ] | RegisterSource | MUTATION | RegisterSourceRequest | `{ "tenant_id": "<seed:tenant_id>", "source_name": "sdk_live_records", "source_message_type": "<seed:message_type>", "text_fields": ["payload"], "target_collection": "sdk_live_records", "model_id": "text-embedding-3-small", "metadata_json": "{}" }` | registers the seeded message payload as an embedding source. |
| [ ] | ReportEmbedding | MUTATION | ReportEmbeddingRequest | `{ "tenant_id": "<seed:tenant_id>", "source_name": "sdk_live_records", "row_pk": "<seed:record_id>", "vector": [0.1, 0.2, 0.3], "model": "text-embedding-3-small", "dims": 3 }` | Sidecar-facing RPC still appears in full surface. |
| [ ] | Retrieve | READ_ONLY | RetrieveRequest | `{ "tenant_id": "<seed:tenant_id>", "source_name": "sdk_live_records", "query_text": "perf", "query_vector": [0.1, 0.2, 0.3], "top_k": 5 }` | retrieves seeded embedding/search records. |
