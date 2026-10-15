import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessagingDevice(_message.Message):
    __slots__ = ("id", "tenant_id", "subject_id", "device_id", "signature_public_key", "credential", "key_fingerprint", "status", "inbox_head_seq", "acked_seq", "registered_at", "revoked_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    KEY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    INBOX_HEAD_SEQ_FIELD_NUMBER: _ClassVar[int]
    ACKED_SEQ_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    subject_id: str
    device_id: str
    signature_public_key: bytes
    credential: bytes
    key_fingerprint: str
    status: str
    inbox_head_seq: int
    acked_seq: int
    registered_at: _timestamp_pb2.Timestamp
    revoked_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., device_id: _Optional[str] = ..., signature_public_key: _Optional[bytes] = ..., credential: _Optional[bytes] = ..., key_fingerprint: _Optional[str] = ..., status: _Optional[str] = ..., inbox_head_seq: _Optional[int] = ..., acked_seq: _Optional[int] = ..., registered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
