## CacheService
_proto: core/cache/services/v1/cache_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateNamespace | MUTATION | CreateNamespaceRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "max_bytes": 1048576, "default_ttl_seconds": 300 }` | creates the benchmark namespace with bounded size and TTL. |
| [ ] | CacheService.Delete | MUTATION | DeleteRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "key": "<seed:object_key>" }` | Service-qualified because DataBroker also has Delete. |
| [ ] | DeleteNamespace | DESTRUCTIVE | DeleteNamespaceRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "confirmation_token": "sdk-perf-cache" }` | destructive namespace flush requires an explicit confirmation token. |
| [ ] | Get | READ_ONLY | GetRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "key": "<seed:object_key>" }` | reads a seeded cache key inside the benchmark namespace. |
| [ ] | GetNamespaceStats | READ_ONLY | GetNamespaceStatsRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache" }` | reads namespace usage counters. |
| [ ] | Increment | MUTATION | IncrementRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "key": "sdk-perf-counter", "delta": 1, "ttl_seconds_if_created": 300, "max": 0 }` | its own integer counter key (the seeded object key holds bytes, which Increment refuses); max 0 = unbounded, so every iteration applies. |
| [ ] | Scan | READ_ONLY | ScanRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "key_prefix": "", "limit": 50, "page_token": "0" }` | scans from cursor 0 inside the benchmark namespace. |
| [ ] | Set | MUTATION | SetRequest | `{ "tenant_id": "<seed:tenant_id>", "namespace": "sdk-perf-cache", "key": "<seed:object_key>", "value": "cGVyZg==", "ttl_seconds": 300 }` | value is bytes, encoded as protobuf JSON base64 for "perf". |
