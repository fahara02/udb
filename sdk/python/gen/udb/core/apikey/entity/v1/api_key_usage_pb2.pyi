import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiKeyUsage(_message.Message):
    __slots__ = ("usage_id", "key_id", "endpoint", "ip_address", "http_status", "latency_ms", "rate_limited", "requested_at", "tenant_id")
    USAGE_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    HTTP_STATUS_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    RATE_LIMITED_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    usage_id: str
    key_id: str
    endpoint: str
    ip_address: str
    http_status: int
    latency_ms: int
    rate_limited: bool
    requested_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, usage_id: _Optional[str] = ..., key_id: _Optional[str] = ..., endpoint: _Optional[str] = ..., ip_address: _Optional[str] = ..., http_status: _Optional[int] = ..., latency_ms: _Optional[int] = ..., rate_limited: bool = ..., requested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...
