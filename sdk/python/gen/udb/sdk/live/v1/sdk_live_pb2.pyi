from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SdkLiveRecord(_message.Message):
    __slots__ = ("record_id", "tenant_id", "project_id", "lookup_key", "payload", "revision", "blob_ref")
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    LOOKUP_KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    BLOB_REF_FIELD_NUMBER: _ClassVar[int]
    record_id: str
    tenant_id: str
    project_id: str
    lookup_key: str
    payload: str
    revision: int
    blob_ref: str
    def __init__(self, record_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., project_id: _Optional[str] = ..., lookup_key: _Optional[str] = ..., payload: _Optional[str] = ..., revision: _Optional[int] = ..., blob_ref: _Optional[str] = ...) -> None: ...
