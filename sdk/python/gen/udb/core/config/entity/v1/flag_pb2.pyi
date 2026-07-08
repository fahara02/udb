from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Flag(_message.Message):
    __slots__ = ("flag_id", "tenant_id", "project_id", "environment", "flag_key", "value_type", "value_json", "enabled", "rollout_percentage", "rollout_context_key", "revision", "metadata_json")
    FLAG_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    FLAG_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_JSON_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_PERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_CONTEXT_KEY_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    flag_id: str
    tenant_id: str
    project_id: str
    environment: str
    flag_key: str
    value_type: str
    value_json: str
    enabled: bool
    rollout_percentage: int
    rollout_context_key: str
    revision: int
    metadata_json: str
    def __init__(self, flag_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., flag_key: _Optional[str] = ..., value_type: _Optional[str] = ..., value_json: _Optional[str] = ..., enabled: bool = ..., rollout_percentage: _Optional[int] = ..., rollout_context_key: _Optional[str] = ..., revision: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...
