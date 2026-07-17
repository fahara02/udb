from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PutSecretRequest(_message.Message):
    __slots__ = ("tenant_id", "secret_path", "secret_value", "expected_version", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    SECRET_VALUE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    secret_path: str
    secret_value: str
    expected_version: int
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., secret_path: _Optional[str] = ..., secret_value: _Optional[str] = ..., expected_version: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class PutSecretResponse(_message.Message):
    __slots__ = ("secret_path", "version", "message", "error")
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    secret_path: str
    version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, secret_path: _Optional[str] = ..., version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetSecretRequest(_message.Message):
    __slots__ = ("tenant_id", "secret_path", "version")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    secret_path: str
    version: int
    def __init__(self, tenant_id: _Optional[str] = ..., secret_path: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class GetSecretResponse(_message.Message):
    __slots__ = ("secret_path", "version", "secret_value", "metadata_json", "message", "error")
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SECRET_VALUE_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    secret_path: str
    version: int
    secret_value: str
    metadata_json: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, secret_path: _Optional[str] = ..., version: _Optional[int] = ..., secret_value: _Optional[str] = ..., metadata_json: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListSecretsRequest(_message.Message):
    __slots__ = ("tenant_id", "path_prefix", "page", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_PREFIX_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    path_prefix: str
    page: int
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., path_prefix: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class SecretSummary(_message.Message):
    __slots__ = ("secret_path", "latest_version", "state")
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    LATEST_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    secret_path: str
    latest_version: int
    state: str
    def __init__(self, secret_path: _Optional[str] = ..., latest_version: _Optional[int] = ..., state: _Optional[str] = ...) -> None: ...

class ListSecretsResponse(_message.Message):
    __slots__ = ("secrets", "total_count", "error", "next_page_token")
    SECRETS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    secrets: _containers.RepeatedCompositeFieldContainer[SecretSummary]
    total_count: int
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, secrets: _Optional[_Iterable[_Union[SecretSummary, _Mapping]]] = ..., total_count: _Optional[int] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class DeleteSecretRequest(_message.Message):
    __slots__ = ("tenant_id", "secret_path")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    secret_path: str
    def __init__(self, tenant_id: _Optional[str] = ..., secret_path: _Optional[str] = ...) -> None: ...

class DeleteSecretResponse(_message.Message):
    __slots__ = ("message", "error")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class UndeleteSecretRequest(_message.Message):
    __slots__ = ("tenant_id", "secret_path")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    secret_path: str
    def __init__(self, tenant_id: _Optional[str] = ..., secret_path: _Optional[str] = ...) -> None: ...

class UndeleteSecretResponse(_message.Message):
    __slots__ = ("secret_path", "version", "message", "error")
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    secret_path: str
    version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, secret_path: _Optional[str] = ..., version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DestroySecretRequest(_message.Message):
    __slots__ = ("tenant_id", "secret_path", "confirmation_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SECRET_PATH_FIELD_NUMBER: _ClassVar[int]
    CONFIRMATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    secret_path: str
    confirmation_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., secret_path: _Optional[str] = ..., confirmation_token: _Optional[str] = ...) -> None: ...

class DestroySecretResponse(_message.Message):
    __slots__ = ("destroyed_versions", "message", "error")
    DESTROYED_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    destroyed_versions: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, destroyed_versions: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class CreateTransitKeyRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "algorithm")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    algorithm: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., algorithm: _Optional[str] = ...) -> None: ...

class CreateTransitKeyResponse(_message.Message):
    __slots__ = ("key_name", "version", "message", "error")
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    key_name: str
    version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, key_name: _Optional[str] = ..., version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RotateTransitKeyRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ...) -> None: ...

class RotateTransitKeyResponse(_message.Message):
    __slots__ = ("key_name", "version", "message", "error")
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    key_name: str
    version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, key_name: _Optional[str] = ..., version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class EncryptRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "plaintext")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    PLAINTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    plaintext: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., plaintext: _Optional[str] = ...) -> None: ...

class EncryptResponse(_message.Message):
    __slots__ = ("ciphertext", "key_version", "message", "error")
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ciphertext: str
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, ciphertext: _Optional[str] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DecryptRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "ciphertext")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    ciphertext: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., ciphertext: _Optional[str] = ...) -> None: ...

class DecryptResponse(_message.Message):
    __slots__ = ("plaintext", "key_version", "message", "error")
    PLAINTEXT_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    plaintext: str
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, plaintext: _Optional[str] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GenerateDataKeyRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ...) -> None: ...

class GenerateDataKeyResponse(_message.Message):
    __slots__ = ("plaintext", "ciphertext", "key_version", "message", "error")
    PLAINTEXT_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    plaintext: str
    ciphertext: str
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, plaintext: _Optional[str] = ..., ciphertext: _Optional[str] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RewrapRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "ciphertext")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    ciphertext: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., ciphertext: _Optional[str] = ...) -> None: ...

class RewrapResponse(_message.Message):
    __slots__ = ("ciphertext", "key_version", "message", "error")
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ciphertext: str
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, ciphertext: _Optional[str] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetTransitPublicKeyRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ...) -> None: ...

class TransitPublicKey(_message.Message):
    __slots__ = ("version", "public_key", "state")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    version: int
    public_key: str
    state: str
    def __init__(self, version: _Optional[int] = ..., public_key: _Optional[str] = ..., state: _Optional[str] = ...) -> None: ...

class GetTransitPublicKeyResponse(_message.Message):
    __slots__ = ("key_name", "algorithm", "public_keys", "message", "error")
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEYS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    key_name: str
    algorithm: str
    public_keys: _containers.RepeatedCompositeFieldContainer[TransitPublicKey]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, key_name: _Optional[str] = ..., algorithm: _Optional[str] = ..., public_keys: _Optional[_Iterable[_Union[TransitPublicKey, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class BatchEncryptRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "plaintexts")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    PLAINTEXTS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    plaintexts: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., plaintexts: _Optional[_Iterable[str]] = ...) -> None: ...

class BatchEncryptResponse(_message.Message):
    __slots__ = ("ciphertexts", "key_version", "message", "error")
    CIPHERTEXTS_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ciphertexts: _containers.RepeatedScalarFieldContainer[str]
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, ciphertexts: _Optional[_Iterable[str]] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class BatchDecryptRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "ciphertexts")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    CIPHERTEXTS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    ciphertexts: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., ciphertexts: _Optional[_Iterable[str]] = ...) -> None: ...

class BatchDecryptResponse(_message.Message):
    __slots__ = ("plaintexts", "message", "error")
    PLAINTEXTS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    plaintexts: _containers.RepeatedScalarFieldContainer[str]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, plaintexts: _Optional[_Iterable[str]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SignRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "input")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    input: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., input: _Optional[str] = ...) -> None: ...

class SignResponse(_message.Message):
    __slots__ = ("signature", "key_version", "message", "error")
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    signature: str
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, signature: _Optional[str] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class VerifyRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "input", "signature")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    input: str
    signature: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., input: _Optional[str] = ..., signature: _Optional[str] = ...) -> None: ...

class VerifyResponse(_message.Message):
    __slots__ = ("valid", "message", "error")
    VALID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, valid: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class HmacRequest(_message.Message):
    __slots__ = ("tenant_id", "key_name", "input")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    key_name: str
    input: str
    def __init__(self, tenant_id: _Optional[str] = ..., key_name: _Optional[str] = ..., input: _Optional[str] = ...) -> None: ...

class HmacResponse(_message.Message):
    __slots__ = ("hmac", "key_version", "message", "error")
    HMAC_FIELD_NUMBER: _ClassVar[int]
    KEY_VERSION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    hmac: str
    key_version: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, hmac: _Optional[str] = ..., key_version: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SealStatusRequest(_message.Message):
    __slots__ = ("tenant_id",)
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    def __init__(self, tenant_id: _Optional[str] = ...) -> None: ...

class SealStatusResponse(_message.Message):
    __slots__ = ("sealed", "kek_configured", "message", "error")
    SEALED_FIELD_NUMBER: _ClassVar[int]
    KEK_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    sealed: bool
    kek_configured: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, sealed: bool = ..., kek_configured: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GenerateDatabaseCredentialsRequest(_message.Message):
    __slots__ = ("tenant_id", "role_name", "ttl_seconds")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    role_name: str
    ttl_seconds: int
    def __init__(self, tenant_id: _Optional[str] = ..., role_name: _Optional[str] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class GenerateDatabaseCredentialsResponse(_message.Message):
    __slots__ = ("username", "password", "lease_id", "lease_ttl_seconds", "message", "error")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    username: str
    password: str
    lease_id: str
    lease_ttl_seconds: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, username: _Optional[str] = ..., password: _Optional[str] = ..., lease_id: _Optional[str] = ..., lease_ttl_seconds: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
