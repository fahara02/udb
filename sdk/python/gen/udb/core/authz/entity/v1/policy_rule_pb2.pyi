import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authz.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PolicyRule(_message.Message):
    __slots__ = ("policy_id", "subject", "domain", "object", "action", "effect", "condition", "description", "is_active", "created_by", "created_at", "updated_at", "deleted_at", "tenant_id", "deleted_by", "project_id", "resource_type", "attributes_json")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_JSON_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    subject: str
    domain: str
    object: str
    action: str
    effect: _enums_pb2.PolicyEffect
    condition: str
    description: str
    is_active: bool
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    deleted_at: _timestamp_pb2.Timestamp
    tenant_id: str
    deleted_by: str
    project_id: str
    resource_type: str
    attributes_json: str
    def __init__(self, policy_id: _Optional[str] = ..., subject: _Optional[str] = ..., domain: _Optional[str] = ..., object: _Optional[str] = ..., action: _Optional[str] = ..., effect: _Optional[_Union[_enums_pb2.PolicyEffect, str]] = ..., condition: _Optional[str] = ..., description: _Optional[str] = ..., is_active: bool = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ..., deleted_by: _Optional[str] = ..., project_id: _Optional[str] = ..., resource_type: _Optional[str] = ..., attributes_json: _Optional[str] = ...) -> None: ...
