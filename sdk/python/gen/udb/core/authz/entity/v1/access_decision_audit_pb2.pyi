import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authz.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AccessDecisionAudit(_message.Message):
    __slots__ = ("decision_audit_id", "user_id", "domain", "object", "action", "effect", "decision_source", "matched_rule", "reason", "ip_address", "correlation_id", "decided_at", "tenant_id", "decision_id", "policy_version", "relationship_version", "purpose", "scopes", "matched_policy_ids", "project_id", "actor_kind", "resource_type", "trace_id", "span_id", "user_agent_hash", "decision_input")
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
    DECISION_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELATIONSHIP_VERSION_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    MATCHED_POLICY_IDS_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_KIND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_HASH_FIELD_NUMBER: _ClassVar[int]
    DECISION_INPUT_FIELD_NUMBER: _ClassVar[int]
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
    decision_id: str
    policy_version: str
    relationship_version: str
    purpose: str
    scopes: str
    matched_policy_ids: _containers.RepeatedScalarFieldContainer[str]
    project_id: str
    actor_kind: str
    resource_type: str
    trace_id: str
    span_id: str
    user_agent_hash: str
    decision_input: str
    def __init__(self, decision_audit_id: _Optional[str] = ..., user_id: _Optional[str] = ..., domain: _Optional[str] = ..., object: _Optional[str] = ..., action: _Optional[str] = ..., effect: _Optional[_Union[_enums_pb2.PolicyEffect, str]] = ..., decision_source: _Optional[_Union[_enums_pb2.DecisionSource, str]] = ..., matched_rule: _Optional[str] = ..., reason: _Optional[str] = ..., ip_address: _Optional[str] = ..., correlation_id: _Optional[str] = ..., decided_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tenant_id: _Optional[str] = ..., decision_id: _Optional[str] = ..., policy_version: _Optional[str] = ..., relationship_version: _Optional[str] = ..., purpose: _Optional[str] = ..., scopes: _Optional[str] = ..., matched_policy_ids: _Optional[_Iterable[str]] = ..., project_id: _Optional[str] = ..., actor_kind: _Optional[str] = ..., resource_type: _Optional[str] = ..., trace_id: _Optional[str] = ..., span_id: _Optional[str] = ..., user_agent_hash: _Optional[str] = ..., decision_input: _Optional[str] = ...) -> None: ...
