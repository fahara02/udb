import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.notification.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationLog(_message.Message):
    __slots__ = ("log_id", "template_id", "event_type", "channel", "recipient_id", "recipient_address", "tenant_id", "project_id", "resource_type", "resource_id", "resource_name", "correlation_id", "status", "error_message", "provider_message_id", "retry_count", "sent_at", "delivered_at", "created_at", "rendered_subject", "rendered_body")
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    RETRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    SENT_AT_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    RENDERED_SUBJECT_FIELD_NUMBER: _ClassVar[int]
    RENDERED_BODY_FIELD_NUMBER: _ClassVar[int]
    log_id: str
    template_id: str
    event_type: str
    channel: _enums_pb2.NotificationChannel
    recipient_id: str
    recipient_address: str
    tenant_id: str
    project_id: str
    resource_type: str
    resource_id: str
    resource_name: str
    correlation_id: str
    status: _enums_pb2.NotificationStatus
    error_message: str
    provider_message_id: str
    retry_count: int
    sent_at: _timestamp_pb2.Timestamp
    delivered_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    rendered_subject: str
    rendered_body: str
    def __init__(self, log_id: _Optional[str] = ..., template_id: _Optional[str] = ..., event_type: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., recipient_id: _Optional[str] = ..., recipient_address: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., resource_id: _Optional[str] = ..., resource_name: _Optional[str] = ..., correlation_id: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.NotificationStatus, str]] = ..., error_message: _Optional[str] = ..., provider_message_id: _Optional[str] = ..., retry_count: _Optional[int] = ..., sent_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., delivered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., rendered_subject: _Optional[str] = ..., rendered_body: _Optional[str] = ...) -> None: ...

class NotificationDeliveryAttempt(_message.Message):
    __slots__ = ("attempt_id", "notification_id", "tenant_id", "channel", "provider", "status", "attempt_count", "last_error", "provider_message_id", "created_at", "updated_at")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    notification_id: str
    tenant_id: str
    channel: _enums_pb2.NotificationChannel
    provider: str
    status: _enums_pb2.NotificationStatus
    attempt_count: int
    last_error: str
    provider_message_id: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, attempt_id: _Optional[str] = ..., notification_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., provider: _Optional[str] = ..., status: _Optional[_Union[_enums_pb2.NotificationStatus, str]] = ..., attempt_count: _Optional[int] = ..., last_error: _Optional[str] = ..., provider_message_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
