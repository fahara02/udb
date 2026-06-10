import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.control.entity.v1 import enums_pb2 as _enums_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ControlPlaneNodeState(_message.Message):
    __slots__ = ("node_state_id", "node_id", "resource_type", "subscribed_names", "accepted_version", "last_good_version", "last_response_nonce", "nack_error_detail", "nonce_counter", "created_at", "updated_at")
    NODE_STATE_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIBED_NAMES_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_GOOD_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_RESPONSE_NONCE_FIELD_NUMBER: _ClassVar[int]
    NACK_ERROR_DETAIL_FIELD_NUMBER: _ClassVar[int]
    NONCE_COUNTER_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    node_state_id: str
    node_id: str
    resource_type: _enums_pb2.ResourceType
    subscribed_names: str
    accepted_version: str
    last_good_version: str
    last_response_nonce: str
    nack_error_detail: str
    nonce_counter: int
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, node_state_id: _Optional[str] = ..., node_id: _Optional[str] = ..., resource_type: _Optional[_Union[_enums_pb2.ResourceType, str]] = ..., subscribed_names: _Optional[str] = ..., accepted_version: _Optional[str] = ..., last_good_version: _Optional[str] = ..., last_response_nonce: _Optional[str] = ..., nack_error_detail: _Optional[str] = ..., nonce_counter: _Optional[int] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
