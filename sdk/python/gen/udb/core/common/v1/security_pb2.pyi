from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AuthMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUTH_MODE_UNSPECIFIED: _ClassVar[AuthMode]
    AUTH_MODE_PUBLIC: _ClassVar[AuthMode]
    AUTH_MODE_BEARER: _ClassVar[AuthMode]
    AUTH_MODE_API_KEY: _ClassVar[AuthMode]
    AUTH_MODE_SERVICE_ACCOUNT: _ClassVar[AuthMode]

class SecurityClassification(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SECURITY_CLASSIFICATION_UNSPECIFIED: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_PUBLIC: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_INTERNAL: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_CONFIDENTIAL: _ClassVar[SecurityClassification]
    SECURITY_CLASSIFICATION_RESTRICTED: _ClassVar[SecurityClassification]

class DataCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATA_CATEGORY_UNSPECIFIED: _ClassVar[DataCategory]
    DATA_CATEGORY_PERSONAL: _ClassVar[DataCategory]
    DATA_CATEGORY_FINANCIAL: _ClassVar[DataCategory]
    DATA_CATEGORY_BIOMETRIC: _ClassVar[DataCategory]
    DATA_CATEGORY_IDENTITY: _ClassVar[DataCategory]
    DATA_CATEGORY_OPERATIONAL: _ClassVar[DataCategory]
    DATA_CATEGORY_SYSTEM: _ClassVar[DataCategory]
AUTH_MODE_UNSPECIFIED: AuthMode
AUTH_MODE_PUBLIC: AuthMode
AUTH_MODE_BEARER: AuthMode
AUTH_MODE_API_KEY: AuthMode
AUTH_MODE_SERVICE_ACCOUNT: AuthMode
SECURITY_CLASSIFICATION_UNSPECIFIED: SecurityClassification
SECURITY_CLASSIFICATION_PUBLIC: SecurityClassification
SECURITY_CLASSIFICATION_INTERNAL: SecurityClassification
SECURITY_CLASSIFICATION_CONFIDENTIAL: SecurityClassification
SECURITY_CLASSIFICATION_RESTRICTED: SecurityClassification
DATA_CATEGORY_UNSPECIFIED: DataCategory
DATA_CATEGORY_PERSONAL: DataCategory
DATA_CATEGORY_FINANCIAL: DataCategory
DATA_CATEGORY_BIOMETRIC: DataCategory
DATA_CATEGORY_IDENTITY: DataCategory
DATA_CATEGORY_OPERATIONAL: DataCategory
DATA_CATEGORY_SYSTEM: DataCategory
PII_FIELD_NUMBER: _ClassVar[int]
pii: _descriptor.FieldDescriptor
ENCRYPTED_SECURITY_FIELD_NUMBER: _ClassVar[int]
encrypted_security: _descriptor.FieldDescriptor
LOG_MASKED_FIELD_NUMBER: _ClassVar[int]
log_masked: _descriptor.FieldDescriptor
LOG_REDACTED_FIELD_NUMBER: _ClassVar[int]
log_redacted: _descriptor.FieldDescriptor
SENSITIVE_FIELD_NUMBER: _ClassVar[int]
sensitive: _descriptor.FieldDescriptor
REQUIRES_CONSENT_FIELD_NUMBER: _ClassVar[int]
requires_consent: _descriptor.FieldDescriptor
DATA_PURPOSE_FIELD_NUMBER: _ClassVar[int]
data_purpose: _descriptor.FieldDescriptor
RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
retention_days: _descriptor.FieldDescriptor
TOKENIZED_FIELD_NUMBER: _ClassVar[int]
tokenized: _descriptor.FieldDescriptor
SECURITY_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
security_classification: _descriptor.FieldDescriptor
DATA_CATEGORY_FIELD_NUMBER: _ClassVar[int]
data_category: _descriptor.FieldDescriptor
ENDPOINT_SECURITY_FIELD_NUMBER: _ClassVar[int]
endpoint_security: _descriptor.FieldDescriptor
REST_CONTRACT_FIELD_NUMBER: _ClassVar[int]
rest_contract: _descriptor.FieldDescriptor

class EndpointSecurity(_message.Message):
    __slots__ = ("mode", "roles", "scopes", "tenant_required", "csrf_required", "policy_ref", "internal_grpc_only")
    MODE_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    TENANT_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    CSRF_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    POLICY_REF_FIELD_NUMBER: _ClassVar[int]
    INTERNAL_GRPC_ONLY_FIELD_NUMBER: _ClassVar[int]
    mode: AuthMode
    roles: _containers.RepeatedScalarFieldContainer[str]
    scopes: _containers.RepeatedScalarFieldContainer[str]
    tenant_required: bool
    csrf_required: bool
    policy_ref: str
    internal_grpc_only: bool
    def __init__(self, mode: _Optional[_Union[AuthMode, str]] = ..., roles: _Optional[_Iterable[str]] = ..., scopes: _Optional[_Iterable[str]] = ..., tenant_required: bool = ..., csrf_required: bool = ..., policy_ref: _Optional[str] = ..., internal_grpc_only: bool = ...) -> None: ...

class RestContract(_message.Message):
    __slots__ = ("response_envelope", "api_error", "pagination_meta", "explicit_nulls")
    RESPONSE_ENVELOPE_FIELD_NUMBER: _ClassVar[int]
    API_ERROR_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_META_FIELD_NUMBER: _ClassVar[int]
    EXPLICIT_NULLS_FIELD_NUMBER: _ClassVar[int]
    response_envelope: bool
    api_error: bool
    pagination_meta: bool
    explicit_nulls: bool
    def __init__(self, response_envelope: bool = ..., api_error: bool = ..., pagination_meta: bool = ..., explicit_nulls: bool = ...) -> None: ...
