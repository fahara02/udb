## NotificationService

_proto: core/notification/services/v1/notification_service.proto · 11 RPCs_

Enums (entity/v1/enums.proto):
- `NotificationChannel`: `NOTIFICATION_CHANNEL_UNSPECIFIED|EMAIL|SMS|PUSH|IN_APP|WEBHOOK`
- `NotificationStatus`: `NOTIFICATION_STATUS_UNSPECIFIED|PENDING|SENT|DELIVERED|FAILED|SUPPRESSED`

Shared: `RequestContext` (common.v1) carries tenant/credential context; `PageRequest` = `{page:int32, page_size:int32, page_token:string}`. All RPCs are `tenant_required` + bearer JWT/session.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | SendNotification | MUTATION | SendNotificationRequest | `event_type:"<seed:event_type>"`, `recipient_id:"<seed:user_id>"`, `recipient_address:"user@example.com"`, `tenant_id:"<seed:tenant_id>"`, `project_id:"<seed:project>"`, `locale:"en"`, `variables:{}`, `channels:["NOTIFICATION_CHANNEL_EMAIL"]` | event_type must match a template. channels empty = template-default. Optional: resource_type/resource_id/resource_name/correlation_id. Field 13 = RequestContext context. |
| [ ] | GetNotification | READ_ONLY | GetNotificationRequest | `log_id:"<seed:log_id>"` | only field is log_id (1). |
| [ ] | ListNotifications | READ_ONLY | ListNotificationsRequest | `tenant_id:"<seed:tenant_id>"`, `page:{page:1,page_size:20}` | all filters optional: recipient_id, project_id, resource_type, resource_id, event_type, channel(enum), status(NotificationStatus enum). |
| [ ] | RetryNotification | MUTATION | RetryNotificationRequest | `log_id:"<seed:log_id>"` | log_id (1) must reference a FAILED log; field 2 = RequestContext context. |
| [ ] | UpsertTemplate | MUTATION | UpsertTemplateRequest | `event_type:"<seed:event_type>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `locale:"en"`, `subject_template:"Hello {name}"`, `body_template:"Body {name}"`, `is_active:true` | field 7 = RequestContext context. |
| [ ] | GetTemplate | READ_ONLY | GetTemplateRequest | `event_type:"<seed:event_type>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `locale:"en"` | keyed by event_type+channel+locale. |
| [ ] | ListTemplates | READ_ONLY | ListTemplatesRequest | `page:{page:1,page_size:20}` | all optional: event_type, channel(enum), active_only(bool). |
| [ ] | GetDeliveryStats | READ_ONLY | GetDeliveryStatsRequest | `tenant_id:"<seed:tenant_id>"`, `event_type:"<seed:event_type>"`, `date_from:"2026-01-01"`, `date_to:"2026-12-31"` | date_from/date_to format YYYY-MM-DD; event_type optional. |
| [ ] | SetPreference | MUTATION | SetPreferenceRequest | `user_id:"<seed:user_id>"`, `tenant_id:"<seed:tenant_id>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `event_type:""`, `is_opted_out:true` | event_type empty = channel-wide opt-out; field 6 = RequestContext context. |
| [ ] | GetPreference | READ_ONLY | GetPreferenceRequest | `user_id:"<seed:user_id>"`, `tenant_id:"<seed:tenant_id>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `event_type:""` | keyed by user_id+tenant_id+channel+event_type. |
| [ ] | ListPreferences | READ_ONLY | ListPreferencesRequest | `user_id:"<seed:user_id>"`, `tenant_id:"<seed:tenant_id>"`, `page:{page:1,page_size:20}` | lists all preferences for a user. |
