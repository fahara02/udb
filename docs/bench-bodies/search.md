## SearchService
_proto: core/search/services/v1/search_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateIndex | MUTATION | CreateIndexRequest | `{ "tenant_id": "<seed:tenant_id>", "index_name": "sdk_live_records", "source_message_type": "<seed:message_type>", "backend": "qdrant", "resource_name": "sdk_live_records", "vector_dims": 3, "metadata_json": "{}" }` | creates the seeded search index. |
| [ ] | DeleteIndex | DESTRUCTIVE | DeleteIndexRequest | `{ "tenant_id": "<seed:tenant_id>", "index_name": "sdk_live_records" }` | deletes the seeded search index. |
| [ ] | ListIndexes | READ_ONLY | ListIndexesRequest | `{ "tenant_id": "<seed:tenant_id>", "page_size": 50 }` | lists search indexes for the tenant. |
| [ ] | Reindex | MUTATION | ReindexRequest | `{ "tenant_id": "<seed:tenant_id>", "index_name": "sdk_live_records" }` | requests a rebuild of the seeded search index. |
| [ ] | Search | READ_ONLY | SearchRequest | `{ "tenant_id": "<seed:tenant_id>", "index_name": "sdk_live_records", "query_text": "perf", "query_vector": [0.1, 0.2, 0.3], "top_k": 5, "mode": "SEARCH_MODE_HYBRID" }` | runs a seeded hybrid search query. |
