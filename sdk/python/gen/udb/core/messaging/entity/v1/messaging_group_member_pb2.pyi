from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MessagingGroupMember(_message.Message):
    __slots__ = ("id", "tenant_id", "group_id", "device_id", "subject_id", "leaf_index", "joined_epoch")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    LEAF_INDEX_FIELD_NUMBER: _ClassVar[int]
    JOINED_EPOCH_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    group_id: str
    device_id: str
    subject_id: str
    leaf_index: int
    joined_epoch: int
    def __init__(self, id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., group_id: _Optional[str] = ..., device_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., leaf_index: _Optional[int] = ..., joined_epoch: _Optional[int] = ...) -> None: ...
