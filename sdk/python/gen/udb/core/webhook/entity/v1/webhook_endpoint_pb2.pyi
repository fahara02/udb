import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from udb.core.common.v1 import types_pb2 as _types_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WebhookEndpoint(_message.Message):
    __slots__ = ("endpoint_id", "tenant_id", "url", "topic_pattern", "signing_secret", "active", "description", "max_attempts", "metadata_json", "audit_info", "deleted_at", "deleted_by")
    ENDPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    TOPIC_PATTERN_FIELD_NUMBER: _ClassVar[int]
    SIGNING_SECRET_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    AUDIT_INFO_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    endpoint_id: str
    tenant_id: str
    url: str
    topic_pattern: str
    signing_secret: str
    active: bool
    description: str
    max_attempts: int
    metadata_json: str
    audit_info: _types_pb2.AuditInfo
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, endpoint_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., url: _Optional[str] = ..., topic_pattern: _Optional[str] = ..., signing_secret: _Optional[str] = ..., active: bool = ..., description: _Optional[str] = ..., max_attempts: _Optional[int] = ..., metadata_json: _Optional[str] = ..., audit_info: _Optional[_Union[_types_pb2.AuditInfo, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...
