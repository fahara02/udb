import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessagingKeyPackage(_message.Message):
    __slots__ = ("id", "tenant_id", "device_id", "key_package", "key_package_ref", "last_resort", "published_at", "claimed_at", "claimed_by_device_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PACKAGE_FIELD_NUMBER: _ClassVar[int]
    KEY_PACKAGE_REF_FIELD_NUMBER: _ClassVar[int]
    LAST_RESORT_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_AT_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_AT_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_BY_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    device_id: str
    key_package: bytes
    key_package_ref: str
    last_resort: bool
    published_at: _timestamp_pb2.Timestamp
    claimed_at: _timestamp_pb2.Timestamp
    claimed_by_device_id: str
    def __init__(self, id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., device_id: _Optional[str] = ..., key_package: _Optional[bytes] = ..., key_package_ref: _Optional[str] = ..., last_resort: bool = ..., published_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., claimed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., claimed_by_device_id: _Optional[str] = ...) -> None: ...
