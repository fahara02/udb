import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SamlReplayEntry(_message.Message):
    __slots__ = ("saml_replay_entry_id", "tenant_id", "provider_id", "assertion_id", "not_on_or_after", "consumed_at")
    SAML_REPLAY_ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_ID_FIELD_NUMBER: _ClassVar[int]
    NOT_ON_OR_AFTER_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_AT_FIELD_NUMBER: _ClassVar[int]
    saml_replay_entry_id: str
    tenant_id: str
    provider_id: str
    assertion_id: str
    not_on_or_after: _timestamp_pb2.Timestamp
    consumed_at: _timestamp_pb2.Timestamp
    def __init__(self, saml_replay_entry_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., assertion_id: _Optional[str] = ..., not_on_or_after: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., consumed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
