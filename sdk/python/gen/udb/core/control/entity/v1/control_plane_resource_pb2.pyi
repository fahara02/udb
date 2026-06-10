import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.control.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ControlPlaneResource(_message.Message):
    __slots__ = ("resource_id", "resource_type", "name", "tenant_id", "project_id", "version", "content_hash", "payload_json", "updated_by", "created_at", "updated_at")
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    resource_id: str
    resource_type: _enums_pb2.ResourceType
    name: str
    tenant_id: str
    project_id: str
    version: str
    content_hash: str
    payload_json: str
    updated_by: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, resource_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., name: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., version: _Optional[str] = ..., content_hash: _Optional[str] = ..., payload_json: _Optional[str] = ..., updated_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
