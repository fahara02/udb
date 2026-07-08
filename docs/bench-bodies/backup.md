## BackupService
_proto: core/backup/services/v1/backup_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | DeleteBackupPolicy | MUTATION | DeleteBackupPolicyRequest | `{ "tenant_id": "<seed:tenant_id>", "policy_name": "sdk-perf-default" }` | deletes the seeded backup policy by name. |
| [ ] | GetBackup | READ_ONLY | GetBackupRequest | `{ "tenant_id": "<seed:tenant_id>", "backup_id": "<seed:backup_id>" }` | reads the seeded tenant backup. |
| [ ] | GetBackupPolicy | READ_ONLY | GetBackupPolicyRequest | `{ "tenant_id": "<seed:tenant_id>", "policy_name": "sdk-perf-default" }` | reads the seeded backup policy. |
| [ ] | ListBackupPolicies | READ_ONLY | ListBackupPoliciesRequest | `{ "tenant_id": "<seed:tenant_id>", "page_size": 20 }` | lists backup policies for the tenant. |
| [ ] | ListBackups | READ_ONLY | ListBackupsRequest | `{ "tenant_id": "<seed:tenant_id>", "page_size": 20 }` | lists backups for the tenant. |
| [ ] | PutBackupPolicy | MUTATION | PutBackupPolicyRequest | `{ "tenant_id": "<seed:tenant_id>", "policy_name": "sdk-perf-default", "schedule_cron": "0 3 * * *", "retention_days": 7, "max_retained_backups": 3, "enabled": true, "metadata_json": "{}" }` | upserts the seeded backup policy. |
| [ ] | RestoreTenant | DESTRUCTIVE | RestoreTenantRequest | `{ "source_tenant_id": "<seed:tenant_id>", "target_tenant_id": "<seed:restore_tenant_id>", "backup_id": "<seed:backup_id>", "confirmation_token": "yes", "allow_cross_tenant": true }` | Destructive restore target must be fresh; source-tenant auth plus explicit cross-tenant approval authorizes restore into the new target. |
| [ ] | StartTenantBackup | MUTATION | StartTenantBackupRequest | `{ "tenant_id": "<seed:tenant_id>" }` | Starts a tenant backup. |
