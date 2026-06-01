from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from udb.core.tenant.entity.v1 import enums_pb2 as _enums_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TenantConfig(_message.Message):
    __slots__ = ("id", "tenant_id", "config_key", "config_value", "type", "description", "audit_info")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_KEY_FIELD_NUMBER: _ClassVar[int]
    CONFIG_VALUE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    config_key: str
    config_value: str
    type: _enums_pb2.ConfigType
    description: str
    audit_info: _types_pb2.AuditInfo
    def __init__(self, id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., config_key: _Optional[str] = ..., config_value: _Optional[str] = ..., type: _Optional[_Union[_enums_pb2.ConfigType, str]] = ..., description: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ...) -> None: ...
