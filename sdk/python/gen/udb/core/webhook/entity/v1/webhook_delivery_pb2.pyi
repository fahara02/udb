import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WebhookDelivery(_message.Message):
    __slots__ = ("delivery_id", "tenant_id", "endpoint_id", "event_id", "topic", "status", "attempt_count", "response_status", "signature", "last_error", "payload_json", "delivered_at", "audit_info")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_STATUS_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_AT_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    tenant_id: str
    endpoint_id: str
    event_id: str
    topic: str
    status: str
    attempt_count: int
    response_status: int
    signature: str
    last_error: str
    payload_json: str
    delivered_at: _timestamp_pb2.Timestamp
    audit_info: _types_pb2.AuditInfo
    def __init__(self, delivery_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., endpoint_id: _Optional[str] = ..., event_id: _Optional[str] = ..., topic: _Optional[str] = ..., status: _Optional[str] = ..., attempt_count: _Optional[int] = ..., response_status: _Optional[int] = ..., signature: _Optional[str] = ..., last_error: _Optional[str] = ..., payload_json: _Optional[str] = ..., delivered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ...) -> None: ...
