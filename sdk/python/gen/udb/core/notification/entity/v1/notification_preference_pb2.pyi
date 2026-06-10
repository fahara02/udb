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

class NotificationPreference(_message.Message):
    __slots__ = ("preference_id", "user_id", "tenant_id", "channel", "event_type", "is_opted_out", "created_at", "updated_at", "created_by")
    PREFERENCE_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_OPTED_OUT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    preference_id: str
    user_id: str
    tenant_id: str
    channel: _enums_pb2.NotificationChannel
    event_type: str
    is_opted_out: bool
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    created_by: str
    def __init__(self, preference_id: _Optional[str] = ..., user_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., channel: _Optional[_Union[_enums_pb2.NotificationChannel, str]] = ..., event_type: _Optional[str] = ..., is_opted_out: bool = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_by: _Optional[str] = ...) -> None: ...
