from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class VaultSecret(_message.Message):
    __slots__ = ("secret_id", "tenant_id", "secret_path", "version", "ciphertext", "data_key_wrapped", "state", "metadata_json")
    SECRET_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    DATA_KEY_WRAPPED_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    secret_id: str
    tenant_id: str
    secret_path: str
    version: int
    ciphertext: str
    data_key_wrapped: str
    state: str
    metadata_json: str
    def __init__(self, secret_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., secret_path: _Optional[str] = ..., version: _Optional[int] = ..., ciphertext: _Optional[str] = ..., data_key_wrapped: _Optional[str] = ..., state: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...
