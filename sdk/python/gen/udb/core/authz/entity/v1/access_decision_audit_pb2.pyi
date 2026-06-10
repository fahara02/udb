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

class AccessDecisionAudit(_message.Message):
    __slots__ = ("decision_audit_id", "user_id", "domain", "object", "action", "effect", "decision_source", "matched_rule", "reason", "ip_address", "correlation_id", "decided_at", "tenant_id")
    DECISION_AUDIT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    DECISION_SOURCE_FIELD_NUMBER: _ClassVar[int]
    MATCHED_RULE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    decision_audit_id: str
    user_id: str
    domain: str
    object: str
    action: str
    effect: _enums_pb2.PolicyEffect
    decision_source: _enums_pb2.DecisionSource
    matched_rule: str
    reason: str
    ip_address: str
    correlation_id: str
    decided_at: _timestamp_pb2.Timestamp
    tenant_id: str
    def __init__(self, decision_audit_id: _Optional[str] = ..., user_id: _Optional[str] = ..., domain: _Optional[str] = ..., object: _Optional[str] = ..., action: _Optional[str] = ..., effect: _Optional[_Union[_enums_pb2.PolicyEffect, str]] = ..., decision_source: _Optional[_Union[_enums_pb2.DecisionSource, str]] = ..., matched_rule: _Optional[str] = ..., reason: _Optional[str] = ..., ip_address: _Optional[str] = ..., correlation_id: _Optional[str] = ..., decided_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ...) -> None: ...
