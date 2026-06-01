from udb.core.common.v1 import db_pb2 as _db_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PolicyTuple(_message.Message):
    __slots__ = ("policy_tuple_id", "tuple_kind", "subject", "domain", "object", "action", "effect", "condition", "tenant_id", "project_id")
    POLICY_TUPLE_ID_FIELD_NUMBER: _ClassVar[int]
    TUPLE_KIND_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    policy_tuple_id: int
    tuple_kind: str
    subject: str
    domain: str
    object: str
    action: str
    effect: str
    condition: str
    tenant_id: str
    project_id: str
    def __init__(self, policy_tuple_id: _Optional[int] = ..., tuple_kind: _Optional[str] = ..., subject: _Optional[str] = ..., domain: _Optional[str] = ..., object: _Optional[str] = ..., action: _Optional[str] = ..., effect: _Optional[str] = ..., condition: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ...) -> None: ...
