from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PipelineDefinition(_message.Message):
    __slots__ = ("definition_id", "tenant_id", "name", "description", "media_type", "steps", "version", "status", "audit_info")
    DEFINITION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    definition_id: str
    tenant_id: str
    name: str
    description: str
    media_type: str
    steps: str
    version: int
    status: str
    audit_info: _types_pb2.AuditInfo
    def __init__(self, definition_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., media_type: _Optional[str] = ..., steps: _Optional[str] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ...) -> None: ...
