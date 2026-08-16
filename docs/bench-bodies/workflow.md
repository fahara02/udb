## WorkflowService
_proto: core/workflow/services/v1/workflow_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CancelWorkflow | DESTRUCTIVE | CancelWorkflowRequest | `{ "tenant_id": "<seed:tenant_id>", "workflow_id": "<seed:cancel_workflow_id>", "reason": "sdk perf cancel" }` | cancels a disposable seeded workflow. |
| [ ] | GetWorkflow | READ_ONLY | GetWorkflowRequest | `{ "tenant_id": "<seed:tenant_id>", "workflow_id": "<seed:workflow_id>" }` | reads the seeded workflow instance. |
| [ ] | ListWorkflows | READ_ONLY | ListWorkflowsRequest | `{ "tenant_id": "<seed:tenant_id>", "status": "RUNNING", "page": 1, "page_size": 20 }` | lists running workflows for the tenant. |
| [ ] | SignalWorkflow | MUTATION | SignalWorkflowRequest | `{ "tenant_id": "<seed:tenant_id>", "workflow_id": "<seed:workflow_id>", "signal_name": "continue", "signal_payload": "{\"ok\":true}" }` | signals the seeded workflow instance. |
| [ ] | StartWorkflow | MUTATION | StartWorkflowRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "workflow_type": "sdk.perf.workflow", "total_steps": 20, "payload": "{}", "compensations": "[]", "correlation_id": "<seed:record_id>" }` | starts a bounded multi-step workflow tied to the seeded record. `project_id` carries the live project (an opaque code, not a UUID) so the project-scoped write and the project-scoped reads that follow agree. |
