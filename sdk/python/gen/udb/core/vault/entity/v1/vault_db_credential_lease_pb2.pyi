import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VaultDbCredentialLease(_message.Message):
    __slots__ = ("lease_id", "tenant_id", "role_name", "username", "parent_role", "backend", "issued_at", "expires_at", "revoked_at", "state", "metadata_json", "project_id", "idempotency_key", "request_hash", "credential_ciphertext", "target_instance", "last_error", "revoke_reason", "revocation_operation_id", "revocation_requested_at")
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_NAME_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_ROLE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    REQUEST_HASH_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    TARGET_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    REVOCATION_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    REVOCATION_REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    lease_id: str
    tenant_id: str
    role_name: str
    username: str
    parent_role: str
    backend: str
    issued_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    revoked_at: _timestamp_pb2.Timestamp
    state: str
    metadata_json: str
    project_id: str
    idempotency_key: str
    request_hash: str
    credential_ciphertext: str
    target_instance: str
    last_error: str
    revoke_reason: str
    revocation_operation_id: str
    revocation_requested_at: _timestamp_pb2.Timestamp
    def __init__(self, lease_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., role_name: _Optional[str] = ..., username: _Optional[str] = ..., parent_role: _Optional[str] = ..., backend: _Optional[str] = ..., issued_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., state: _Optional[str] = ..., metadata_json: _Optional[str] = ..., project_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., request_hash: _Optional[str] = ..., credential_ciphertext: _Optional[str] = ..., target_instance: _Optional[str] = ..., last_error: _Optional[str] = ..., revoke_reason: _Optional[str] = ..., revocation_operation_id: _Optional[str] = ..., revocation_requested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
