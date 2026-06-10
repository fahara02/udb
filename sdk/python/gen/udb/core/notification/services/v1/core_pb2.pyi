from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.notification.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.notification.entity.v1 import notification_log_pb2 as _notification_log_pb2
from udb.core.notification.entity.v1 import notification_preference_pb2 as _notification_preference_pb2
from udb.core.notification.entity.v1 import notification_template_pb2 as _notification_template_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SendNotificationRequest(_message.Message):
    __slots__ = ("event_type", "recipient_id", "recipient_address", "tenant_id", "project_id", "resource_type", "resource_id", "resource_name", "correlation_id", "locale", "variables", "channels", "context")
    class VariablesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    VARIABLES_FIELD_NUMBER: _ClassVar[int]
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    recipient_id: str
    recipient_address: str
    tenant_id: str
    project_id: str
    resource_type: str
    resource_id: str
    resource_name: str
    correlation_id: str
    locale: str
    variables: _containers.ScalarMap[str, str]
    channels: _containers.RepeatedScalarFieldContainer[_enums_pb2.NotificationChannel]
    context: _types_pb2.RequestContext
    def __init__(self, event_type: _Optional[str] = ..., recipient_id: _Optional[str] = ..., recipient_address: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., resource_name: _Optional[str] = ..., correlation_id: _Optional[str] = ..., locale: _Optional[str] = ..., variables: _Optional[_Mapping[str, str]] = ..., channels: _Optional[_Iterable[_Union[_enums_pb2.NotificationChannel, str]]] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class SendNotificationResponse(_message.Message):
    __slots__ = ("logs",)
    LOGS_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedCompositeFieldContainer[_notification_log_pb2.NotificationLog]
    def __init__(self, logs: _Optional[_Iterable[_Union[_notification_log_pb2.NotificationLog, _Mapping]]] = ...) -> None: ...

class GetNotificationRequest(_message.Message):
    __slots__ = ("log_id",)
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    log_id: str
    def __init__(self, log_id: _Optional[str] = ...) -> None: ...

class GetNotificationResponse(_message.Message):
    __slots__ = ("log",)
    LOG_FIELD_NUMBER: _ClassVar[int]
    log: _notification_log_pb2.NotificationLog
    def __init__(self, log: _Optional[_Union[_notification_log_pb2.NotificationLog, _Mapping]] = ...) -> None: ...

class ListNotificationsRequest(_message.Message):
    __slots__ = ("recipient_id", "tenant_id", "project_id", "resource_type", "resource_id", "event_type", "channel", "status", "page")
    RECIPIENT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    recipient_id: str
    tenant_id: str
    project_id: str
    resource_type: str
    resource_id: str
    event_type: str
    channel: _enums_pb2.NotificationChannel
    status: _enums_pb2.NotificationStatus
    page: _dto_pb2.PageRequest
    def __init__(self, recipient_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., event_type: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., status: _Optional[_Union[_enums_pb2.NotificationStatus, str]] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListNotificationsResponse(_message.Message):
    __slots__ = ("logs", "page")
    LOGS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedCompositeFieldContainer[_notification_log_pb2.NotificationLog]
    page: _dto_pb2.PageResponse
    def __init__(self, logs: _Optional[_Iterable[_Union[_notification_log_pb2.NotificationLog, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class RetryNotificationRequest(_message.Message):
    __slots__ = ("log_id", "context")
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    log_id: str
    context: _types_pb2.RequestContext
    def __init__(self, log_id: _Optional[str] = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class RetryNotificationResponse(_message.Message):
    __slots__ = ("log",)
    LOG_FIELD_NUMBER: _ClassVar[int]
    log: _notification_log_pb2.NotificationLog
    def __init__(self, log: _Optional[_Union[_notification_log_pb2.NotificationLog, _Mapping]] = ...) -> None: ...

class UpsertTemplateRequest(_message.Message):
    __slots__ = ("event_type", "channel", "locale", "subject_template", "body_template", "is_active", "context")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    BODY_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    channel: _enums_pb2.NotificationChannel
    locale: str
    subject_template: str
    body_template: str
    is_active: bool
    context: _types_pb2.RequestContext
    def __init__(self, event_type: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., locale: _Optional[str] = ..., subject_template: _Optional[str] = ..., body_template: _Optional[str] = ..., is_active: bool = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class UpsertTemplateResponse(_message.Message):
    __slots__ = ("template",)
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    template: _notification_template_pb2.NotificationTemplate
    def __init__(self, template: _Optional[_Union[_notification_template_pb2.NotificationTemplate, _Mapping]] = ...) -> None: ...

class GetTemplateRequest(_message.Message):
    __slots__ = ("event_type", "channel", "locale")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    channel: _enums_pb2.NotificationChannel
    locale: str
    def __init__(self, event_type: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., locale: _Optional[str] = ...) -> None: ...

class GetTemplateResponse(_message.Message):
    __slots__ = ("template",)
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    template: _notification_template_pb2.NotificationTemplate
    def __init__(self, template: _Optional[_Union[_notification_template_pb2.NotificationTemplate, _Mapping]] = ...) -> None: ...

class ListTemplatesRequest(_message.Message):
    __slots__ = ("event_type", "channel", "active_only", "page")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    channel: _enums_pb2.NotificationChannel
    active_only: bool
    page: _dto_pb2.PageRequest
    def __init__(self, event_type: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., active_only: bool = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListTemplatesResponse(_message.Message):
    __slots__ = ("templates", "page")
    TEMPLATES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    templates: _containers.RepeatedCompositeFieldContainer[_notification_template_pb2.NotificationTemplate]
    page: _dto_pb2.PageResponse
    def __init__(self, templates: _Optional[_Iterable[_Union[_notification_template_pb2.NotificationTemplate, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class GetDeliveryStatsRequest(_message.Message):
    __slots__ = ("tenant_id", "event_type", "date_from", "date_to")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATE_FROM_FIELD_NUMBER: _ClassVar[int]
    DATE_TO_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    event_type: str
    date_from: str
    date_to: str
    def __init__(self, tenant_id: _Optional[str] = ..., event_type: _Optional[str] = ..., date_from: _Optional[str] = ..., date_to: _Optional[str] = ...) -> None: ...

class ChannelStats(_message.Message):
    __slots__ = ("channel", "sent", "delivered", "failed", "suppressed", "delivery_rate")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    SENT_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    SUPPRESSED_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_RATE_FIELD_NUMBER: _ClassVar[int]
    channel: _enums_pb2.NotificationChannel
    sent: int
    delivered: int
    failed: int
    suppressed: int
    delivery_rate: float
    def __init__(self, channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., sent: _Optional[int] = ..., delivered: _Optional[int] = ..., failed: _Optional[int] = ..., suppressed: _Optional[int] = ..., delivery_rate: _Optional[float] = ...) -> None: ...

class GetDeliveryStatsResponse(_message.Message):
    __slots__ = ("total_sent", "total_delivered", "total_failed", "overall_delivery_rate", "by_channel")
    TOTAL_SENT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DELIVERED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FAILED_FIELD_NUMBER: _ClassVar[int]
    OVERALL_DELIVERY_RATE_FIELD_NUMBER: _ClassVar[int]
    BY_CHANNEL_FIELD_NUMBER: _ClassVar[int]
    total_sent: int
    total_delivered: int
    total_failed: int
    overall_delivery_rate: float
    by_channel: _containers.RepeatedCompositeFieldContainer[ChannelStats]
    def __init__(self, total_sent: _Optional[int] = ..., total_delivered: _Optional[int] = ..., total_failed: _Optional[int] = ..., overall_delivery_rate: _Optional[float] = ..., by_channel: _Optional[_Iterable[_Union[ChannelStats, _Mapping]]] = ...) -> None: ...

class SetPreferenceRequest(_message.Message):
    __slots__ = ("user_id", "tenant_id", "channel", "event_type", "is_opted_out", "context")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_OPTED_OUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    tenant_id: str
    channel: _enums_pb2.NotificationChannel
    event_type: str
    is_opted_out: bool
    context: _types_pb2.RequestContext
    def __init__(self, user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., event_type: _Optional[str] = ..., is_opted_out: bool = ..., context: _Optional[_Union[_types_pb2.RequestContext, _Mapping]] = ...) -> None: ...

class SetPreferenceResponse(_message.Message):
    __slots__ = ("preference",)
    PREFERENCE_FIELD_NUMBER: _ClassVar[int]
    preference: _notification_preference_pb2.NotificationPreference
    def __init__(self, preference: _Optional[_Union[_notification_preference_pb2.NotificationPreference, _Mapping]] = ...) -> None: ...

class GetPreferenceRequest(_message.Message):
    __slots__ = ("user_id", "tenant_id", "channel", "event_type")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    tenant_id: str
    channel: _enums_pb2.NotificationChannel
    event_type: str
    def __init__(self, user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., event_type: _Optional[str] = ...) -> None: ...

class GetPreferenceResponse(_message.Message):
    __slots__ = ("preference",)
    PREFERENCE_FIELD_NUMBER: _ClassVar[int]
    preference: _notification_preference_pb2.NotificationPreference
    def __init__(self, preference: _Optional[_Union[_notification_preference_pb2.NotificationPreference, _Mapping]] = ...) -> None: ...

class ListPreferencesRequest(_message.Message):
    __slots__ = ("user_id", "tenant_id", "page")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    tenant_id: str
    page: _dto_pb2.PageRequest
    def __init__(self, user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., page: _Optional[_Union[_dto_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListPreferencesResponse(_message.Message):
    __slots__ = ("preferences", "page")
    PREFERENCES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    preferences: _containers.RepeatedCompositeFieldContainer[_notification_preference_pb2.NotificationPreference]
    page: _dto_pb2.PageResponse
    def __init__(self, preferences: _Optional[_Iterable[_Union[_notification_preference_pb2.NotificationPreference, _Mapping]]] = ..., page: _Optional[_Union[_dto_pb2.PageResponse, _Mapping]] = ...) -> None: ...
