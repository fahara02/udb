import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessagingGroup(_message.Message):
    __slots__ = ("id", "tenant_id", "group_id", "epoch", "public_group_state", "group_info", "group_info_epoch", "created_by_device_id", "created_at", "updated_at", "external_join_policy")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_GROUP_STATE_FIELD_NUMBER: _ClassVar[int]
    GROUP_INFO_FIELD_NUMBER: _ClassVar[int]
    GROUP_INFO_EPOCH_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_JOIN_POLICY_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    group_id: str
    epoch: int
    public_group_state: bytes
    group_info: bytes
    group_info_epoch: int
    created_by_device_id: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    external_join_policy: str
    def __init__(self, id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., group_id: _Optional[str] = ..., epoch: _Optional[int] = ..., public_group_state: _Optional[bytes] = ..., group_info: _Optional[bytes] = ..., group_info_epoch: _Optional[int] = ..., created_by_device_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., external_join_policy: _Optional[str] = ...) -> None: ...
