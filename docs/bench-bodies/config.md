## ConfigService
_proto: core/config/services/v1/config_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [x] | DeleteFlag | DESTRUCTIVE | DeleteFlagRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "environment": "prod", "flag_key": "sdk.perf.enabled" }` | deletes the seeded project flag. |
| [x] | EvaluateFlags | READ_ONLY | EvaluateFlagsRequest | `{ "tenant_id": "<seed:tenant_id>", "keys": ["sdk.perf.enabled"], "context": { "project_id": "<seed:project>", "environment": "prod" } }` | evaluates the seeded project flag. |
| [x] | GetFlag | READ_ONLY | GetFlagRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "environment": "prod", "flag_key": "sdk.perf.enabled" }` | reads the seeded project flag. |
| [x] | ListFlags | READ_ONLY | ListFlagsRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "environment": "prod", "limit": 50 }` | lists flags for the seeded project environment. |
| [x] | PutFlag | MUTATION | PutFlagRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "environment": "prod", "flag_key": "sdk.perf.enabled", "value": { "bool_value": true }, "enabled": true, "rollout_percentage": 100, "rollout_context_key": "user_id", "metadata_json": "{}" }` | upserts the seeded project flag. |
