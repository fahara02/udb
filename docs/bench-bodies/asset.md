## AssetService

_proto: core/asset/services/v1/asset_service.proto · 8 RPCs_

All request messages are defined inline in `asset_service.proto`; every request field is a flat scalar (`string` / `int32`) — no enums, nested messages, `oneof`, or `repeated` appear in any request type (entity messages `PipelineDefinition` / `PipelineInstance` / `PipelineStep` / `Asset` are used in responses only). `tenant_required: true` on every RPC, so `tenant_id` is always needed.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CompleteStep | MUTATION | CompleteStepRequest | `{ "tenant_id": "<seed:tenant_id>", "step_id": "<seed:step_id>", "status": "COMPLETED", "result": "{}", "error_message": "" }` | `status` is a STRING field (not a proto enum); valid values per proto comment: `COMPLETED` \|`SKIPPED` \|`FAILED`. `step_id` must be a real step from a started pipeline (GetPipeline response `steps[].id`) or INVALID_ARGUMENT. `error_message` only meaningful when status=FAILED. |
| [ ] | CreatePipelineDefinition | MUTATION | CreatePipelineDefinitionRequest | `{ "tenant_id": "<seed:tenant_id>", "name": "thumbnail-pipeline", "description": "Generate thumbnails", "media_type": "image/png", "steps": "[{\"name\":\"resize\",\"type\":\"TRANSFORM\"}]", "version": 1 }` | `steps` is a JSON string (field 5), not a list. Returns `definition_id` — capture as `<seed:definition_id>`. |
| [ ] | GetAsset | READ_ONLY | GetAssetRequest | `{ "tenant_id": "<seed:tenant_id>", "asset_id": "<seed:asset_id>" }` | asset_id from RegisterAsset or NOT_FOUND. |
| [ ] | GetPipeline | READ_ONLY | GetPipelineRequest | `{ "tenant_id": "<seed:tenant_id>", "instance_id": "<seed:instance_id>" }` | instance_id from StartPipeline. Response carries instance + repeated steps. |
| [ ] | GetPipelineDefinition | READ_ONLY | GetPipelineDefinitionRequest | `{ "tenant_id": "<seed:tenant_id>", "definition_id": "<seed:definition_id>" }` | definition_id must exist (from CreatePipelineDefinition) or NOT_FOUND. |
| [ ] | ListAssets | READ_ONLY | ListAssetsRequest | `{ "tenant_id": "<seed:tenant_id>", "media_type": "image/png", "status": "", "page": 1, "page_size": 20 }` | `media_type`/`status` are optional string filters; omit (empty) to list all. |
| [ ] | RegisterAsset | MUTATION | RegisterAssetRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "file_id": "<seed:file_id>", "name": "logo.png", "media_type": "image/png", "metadata": "{\"source\":\"upload\"}" }` | `file_id` references an existing StorageService file (`<seed:file_id>`). `project_id` carries the live project (an opaque code, not a UUID) so the project-scoped write and the project-scoped reads that follow agree. Returns `asset_id` — capture as `<seed:asset_id>`. |
| [ ] | StartPipeline | MUTATION | StartPipelineRequest | `{ "tenant_id": "<seed:tenant_id>", "definition_id": "<seed:definition_id>", "asset_id": "<seed:asset_id>", "context": "{}", "correlation_id": "run-001" }` | Needs a real `definition_id` AND `asset_id` or INVALID_ARGUMENT/NOT_FOUND. Returns `instance_id` — capture as `<seed:instance_id>`. |

Seed legend: `tenant_id`, `project` (→ `project_id`), `definition_id`, `asset_id`, `instance_id`, `file_id`, `object_key`, `bucket`. Note: `object_key` and `bucket` are not used by any AssetService request (they belong to StorageService); AssetService references storage only via `file_id`.
