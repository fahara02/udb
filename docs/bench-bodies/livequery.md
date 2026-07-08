## LiveQueryService
_proto: core/livequery/services/v1/livequery_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | Subscribe | READ_ONLY | SubscribeRequest | `{ "tenant_id": "<seed:tenant_id>", "message_type": "udb.core.lock.entity.v1.Lock", "filters": [{ "field": "lock_name", "op": "LIVE_QUERY_COMPARISON_EQ", "value": "sdk-perf-renew-lock" }], "snapshot_limit": 10 }` | server-streaming; first response path uses the seeded LockService renew lock row. |
