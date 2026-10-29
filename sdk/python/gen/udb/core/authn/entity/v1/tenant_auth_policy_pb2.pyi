import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TenantAuthPolicy(_message.Message):
    __slots__ = ("tenant_id", "password_required", "passwordless_channels_json", "magic_link_base_url", "step_up_rules_json", "revision", "updated_by", "created_at", "updated_at")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    PASSWORDLESS_CHANNELS_JSON_FIELD_NUMBER: _ClassVar[int]
    MAGIC_LINK_BASE_URL_FIELD_NUMBER: _ClassVar[int]
    STEP_UP_RULES_JSON_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    password_required: bool
    passwordless_channels_json: str
    magic_link_base_url: str
    step_up_rules_json: str
    revision: int
    updated_by: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, tenant_id: _Optional[str] = ..., password_required: bool = ..., passwordless_channels_json: _Optional[str] = ..., magic_link_base_url: _Optional[str] = ..., step_up_rules_json: _Optional[str] = ..., revision: _Optional[int] = ..., updated_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
