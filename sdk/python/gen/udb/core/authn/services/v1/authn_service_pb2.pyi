import datetime

from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.authn.entity.v1 import certificate_binding_pb2 as _certificate_binding_pb2
from udb.core.authn.entity.v1 import service_account_grant_pb2 as _service_account_grant_pb2
from udb.core.authn.services.v1 import core_pb2 as _core_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateServiceAccountGrantRequest(_message.Message):
    __slots__ = ("tenant_id", "user_id", "service_identity", "project_id", "approved_scopes", "reason")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    service_identity: str
    project_id: str
    approved_scopes: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ..., service_identity: _Optional[str] = ..., project_id: _Optional[str] = ..., approved_scopes: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ...) -> None: ...

class CreateServiceAccountGrantResponse(_message.Message):
    __slots__ = ("grant", "message", "error")
    GRANT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    grant: _service_account_grant_pb2.ServiceAccountGrant
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, grant: _Optional[_Union[_service_account_grant_pb2.ServiceAccountGrant, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetServiceAccountGrantRequest(_message.Message):
    __slots__ = ("tenant_id", "user_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ...) -> None: ...

class GetServiceAccountGrantResponse(_message.Message):
    __slots__ = ("grant", "message", "error")
    GRANT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    grant: _service_account_grant_pb2.ServiceAccountGrant
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, grant: _Optional[_Union[_service_account_grant_pb2.ServiceAccountGrant, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListServiceAccountGrantsRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListServiceAccountGrantsResponse(_message.Message):
    __slots__ = ("grants", "next_page_token", "message", "error")
    GRANTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    grants: _containers.RepeatedCompositeFieldContainer[_service_account_grant_pb2.ServiceAccountGrant]
    next_page_token: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, grants: _Optional[_Iterable[_Union[_service_account_grant_pb2.ServiceAccountGrant, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReplaceServiceAccountGrantRequest(_message.Message):
    __slots__ = ("tenant_id", "user_id", "approved_scopes", "project_id", "reason", "expected_revision")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_SCOPES_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    approved_scopes: _containers.RepeatedScalarFieldContainer[str]
    project_id: str
    reason: str
    expected_revision: int
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ..., approved_scopes: _Optional[_Iterable[str]] = ..., project_id: _Optional[str] = ..., reason: _Optional[str] = ..., expected_revision: _Optional[int] = ...) -> None: ...

class ReplaceServiceAccountGrantResponse(_message.Message):
    __slots__ = ("grant", "message", "error")
    GRANT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    grant: _service_account_grant_pb2.ServiceAccountGrant
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, grant: _Optional[_Union[_service_account_grant_pb2.ServiceAccountGrant, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RotateServiceAccountIdentityRequest(_message.Message):
    __slots__ = ("tenant_id", "user_id", "new_service_identity", "expected_revision", "reason")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    new_service_identity: str
    expected_revision: int
    reason: str
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ..., new_service_identity: _Optional[str] = ..., expected_revision: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class RotateServiceAccountIdentityResponse(_message.Message):
    __slots__ = ("grant", "previous_service_identity", "message", "error")
    GRANT_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    grant: _service_account_grant_pb2.ServiceAccountGrant
    previous_service_identity: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, grant: _Optional[_Union[_service_account_grant_pb2.ServiceAccountGrant, _Mapping]] = ..., previous_service_identity: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RevokeServiceAccountGrantRequest(_message.Message):
    __slots__ = ("tenant_id", "user_id", "reason")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    reason: str
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RevokeServiceAccountGrantResponse(_message.Message):
    __slots__ = ("revoked", "message", "error")
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    revoked: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, revoked: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class CreateCertificateBindingRequest(_message.Message):
    __slots__ = ("tenant_id", "user_id", "selector_kind", "selector_value", "scope_subset", "reason", "not_before", "not_after")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_KIND_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_VALUE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_SUBSET_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    NOT_BEFORE_FIELD_NUMBER: _ClassVar[int]
    NOT_AFTER_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    user_id: str
    selector_kind: str
    selector_value: str
    scope_subset: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    not_before: _timestamp_pb2.Timestamp
    not_after: _timestamp_pb2.Timestamp
    def __init__(self, tenant_id: _Optional[str] = ..., user_id: _Optional[str] = ..., selector_kind: _Optional[str] = ..., selector_value: _Optional[str] = ..., scope_subset: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ..., not_before: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., not_after: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CreateCertificateBindingResponse(_message.Message):
    __slots__ = ("binding", "message", "error")
    BINDING_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    binding: _certificate_binding_pb2.CertificateBinding
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, binding: _Optional[_Union[_certificate_binding_pb2.CertificateBinding, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListCertificateBindingsRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListCertificateBindingsResponse(_message.Message):
    __slots__ = ("bindings", "next_page_token", "message", "error")
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    bindings: _containers.RepeatedCompositeFieldContainer[_certificate_binding_pb2.CertificateBinding]
    next_page_token: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, bindings: _Optional[_Iterable[_Union[_certificate_binding_pb2.CertificateBinding, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RevokeCertificateBindingRequest(_message.Message):
    __slots__ = ("tenant_id", "binding_id", "reason")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BINDING_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    binding_id: str
    reason: str
    def __init__(self, tenant_id: _Optional[str] = ..., binding_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RevokeCertificateBindingResponse(_message.Message):
    __slots__ = ("revoked", "message", "error")
    REVOKED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    revoked: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, revoked: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
