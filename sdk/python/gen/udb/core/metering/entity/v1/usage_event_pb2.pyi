import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UsageEvent(_message.Message):
    __slots__ = ("usage_id", "tenant_id", "principal_id", "method", "unit", "quantity", "occurred_at", "occurred_at_unix", "metadata_json")
    USAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    usage_id: str
    tenant_id: str
    principal_id: str
    method: str
    unit: str
    quantity: int
    occurred_at: _timestamp_pb2.Timestamp
    occurred_at_unix: int
    metadata_json: str
    def __init__(self, usage_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., principal_id: _Optional[str] = ..., method: _Optional[str] = ..., unit: _Optional[str] = ..., quantity: _Optional[int] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., occurred_at_unix: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...
