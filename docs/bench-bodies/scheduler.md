## SchedulerService
_proto: core/scheduler/services/v1/scheduler_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateJob | MUTATION | CreateJobRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "", "name": "sdk-perf-job", "schedule_type": "CRON", "cron_expression": "*/5 * * * *", "payload": "{}", "target_topic": "sdk.perf.scheduler", "max_attempts": 3, "backoff_seconds": 30 }` | creates a one-topic scheduler job. `project_id` is empty because the scheduler schema treats non-empty project ids as UUIDs; the default live project code is not a UUID. |
| [ ] | DeleteJob | DESTRUCTIVE | DeleteJobRequest | `{ "tenant_id": "<seed:tenant_id>", "job_id": "<seed:job_id>" }` | deletes the seeded scheduler job. |
| [ ] | GetJob | READ_ONLY | GetJobRequest | `{ "tenant_id": "<seed:tenant_id>", "job_id": "<seed:job_id>" }` | reads the seeded scheduler job. |
| [ ] | ListJobs | READ_ONLY | ListJobsRequest | `{ "tenant_id": "<seed:tenant_id>", "page": 1, "page_size": 20 }` | lists scheduler jobs for the tenant. |
| [ ] | PauseJob | MUTATION | PauseJobRequest | `{ "tenant_id": "<seed:tenant_id>", "job_id": "<seed:job_id>" }` | pauses the seeded scheduler job. |
| [ ] | ResumeJob | MUTATION | ResumeJobRequest | `{ "tenant_id": "<seed:tenant_id>", "job_id": "<seed:job_id>" }` | resumes the seeded scheduler job. |
