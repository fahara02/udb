from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class VaultTransitKey(_message.Message):
    __slots__ = ("key_id", "tenant_id", "key_name", "version", "algorithm", "wrapped_key_material", "state", "metadata_json")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_KEY_MATERIAL_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    tenant_id: str
    key_name: str
    version: int
    algorithm: str
    wrapped_key_material: str
    state: str
    metadata_json: str
    def __init__(self, key_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., version: _Optional[int] = ..., algorithm: _Optional[str] = ..., wrapped_key_material: _Optional[str] = ..., state: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...
