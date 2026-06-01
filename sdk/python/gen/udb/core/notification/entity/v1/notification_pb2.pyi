import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.notification.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Notification(_message.Message):
    __slots__ = ("notification_id", "recipient_id", "type", "channel", "subject", "message", "template_data", "priority", "status", "scheduled_at", "sent_at", "delivered_at", "read_at", "created_at", "retry_count", "error_message", "tenant_id")
    class TemplateDataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NOTIFICATION_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_DATA_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_AT_FIELD_NUMBER: _ClassVar[int]
    SENT_AT_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_AT_FIELD_NUMBER: _ClassVar[int]
    READ_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    RETRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    notification_id: str
    recipient_id: str
    type: _enums_pb2.NotificationType
    channel: _enums_pb2.NotificationChannel
    subject: str
    message: str
    template_data: _containers.ScalarMap[str, str]
    priority: _enums_pb2.NotificationPriority
    status: _enums_pb2.NotificationStatus
    scheduled_at: _timestamp_pb2.Timestamp
    sent_at: _timestamp_pb2.Timestamp
    delivered_at: _timestamp_pb2.Timestamp
    read_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    retry_count: int
    error_message: str
    tenant_id: str
    def __init__(self, notification_id: _Optional[str] = ..., recipient_id: _Optional[str] = ..., type: _Optional[_Union[_enums_pb2.NotificationType, str]] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., subject: _Optional[str] = ..., message: _Optional[str] = ..., template_data: _Optional[_Mapping[str, str]] = ..., priority: _Optional[_Union[_enums_pb2.NotificationPriority, str]] = ..., status: _Optional[_Union[_enums_pb2.NotificationStatus, str]] = ..., scheduled_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., sent_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., delivered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., read_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., retry_count: _Optional[int] = ..., error_message: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...
