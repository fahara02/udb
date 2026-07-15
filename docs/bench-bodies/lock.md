## LockService
_proto: core/lock/services/v1/lock_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | AcquireLock | MUTATION | AcquireLockRequest | `{ "tenant_id": "<seed:tenant_id>", "lock_name": "sdk-perf-acquire-lock", "owner_id": "<seed:user_id>", "lease_ttl_seconds": 60, "metadata_json": "{}" }` | acquires a disposable lock name for the seeded user. |
| [ ] | ReleaseLock | MUTATION | ReleaseLockRequest | `{ "tenant_id": "<seed:tenant_id>", "lock_name": "sdk-perf-release-lock", "owner_id": "<seed:user_id>", "fencing_token": <seed:release_fencing_token> }` | releases a separately seeded lock using the captured fencing token. |
| [ ] | RenewLock | MUTATION | RenewLockRequest | `{ "tenant_id": "<seed:tenant_id>", "lock_name": "sdk-perf-renew-lock", "owner_id": "<seed:user_id>", "fencing_token": <seed:renew_fencing_token>, "lease_ttl_seconds": 60 }` | renews a separately seeded lock using the captured fencing token. |
| [ ] | GetLock | READ_ONLY | GetLockRequest | `{ "tenant_id": "<seed:tenant_id>", "lock_name": "sdk-perf-acquire-lock" }` | reads back a single lock by name within the seeded tenant (found=false when absent). |
| [ ] | ListLocks | READ_ONLY | ListLocksRequest | `{ "tenant_id": "<seed:tenant_id>", "status_filter": "HELD", "page_size": 50 }` | lists the seeded tenant's HELD locks, first page. |
