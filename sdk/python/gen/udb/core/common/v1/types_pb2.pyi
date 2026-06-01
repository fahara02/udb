import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import domain_types_pb2 as _domain_types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union
from udb.core.common.v1.domain_types_pb2 import Money as Money
from udb.core.common.v1.domain_types_pb2 import Address as Address
from udb.core.common.v1.domain_types_pb2 import ContactInfo as ContactInfo
from udb.core.common.v1.domain_types_pb2 import GeoPoint as GeoPoint
from udb.core.common.v1.domain_types_pb2 import DateRange as DateRange
from udb.core.common.v1.domain_types_pb2 import ExternalReference as ExternalReference
from udb.core.common.v1.domain_types_pb2 import ActorReference as ActorReference
from udb.core.common.v1.domain_types_pb2 import ResourceReference as ResourceReference
from udb.core.common.v1.domain_types_pb2 import TagSet as TagSet
from udb.core.common.v1.domain_types_pb2 import PersonGender as PersonGender
from udb.core.common.v1.domain_types_pb2 import ClientSurface as ClientSurface
from udb.core.common.v1.domain_types_pb2 import PaymentMethod as PaymentMethod

DESCRIPTOR: _descriptor.FileDescriptor
PERSON_GENDER_UNSPECIFIED: _domain_types_pb2.PersonGender
PERSON_GENDER_MALE: _domain_types_pb2.PersonGender
PERSON_GENDER_FEMALE: _domain_types_pb2.PersonGender
PERSON_GENDER_OTHER: _domain_types_pb2.PersonGender
PERSON_GENDER_NOT_PROVIDED: _domain_types_pb2.PersonGender
CLIENT_SURFACE_UNSPECIFIED: _domain_types_pb2.ClientSurface
CLIENT_SURFACE_WEB: _domain_types_pb2.ClientSurface
CLIENT_SURFACE_IOS: _domain_types_pb2.ClientSurface
CLIENT_SURFACE_ANDROID: _domain_types_pb2.ClientSurface
CLIENT_SURFACE_API: _domain_types_pb2.ClientSurface
CLIENT_SURFACE_ADMIN: _domain_types_pb2.ClientSurface
CLIENT_SURFACE_WORKER: _domain_types_pb2.ClientSurface
PAYMENT_METHOD_UNSPECIFIED: _domain_types_pb2.PaymentMethod
PAYMENT_METHOD_CARD: _domain_types_pb2.PaymentMethod
PAYMENT_METHOD_CASH: _domain_types_pb2.PaymentMethod
PAYMENT_METHOD_BANK_TRANSFER: _domain_types_pb2.PaymentMethod
PAYMENT_METHOD_MOBILE_WALLET: _domain_types_pb2.PaymentMethod
PAYMENT_METHOD_ACCOUNT_CREDIT: _domain_types_pb2.PaymentMethod
PAYMENT_METHOD_OTHER: _domain_types_pb2.PaymentMethod

class ConfidenceLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONFIDENCE_LEVEL_UNSPECIFIED: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_HIGH: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_MEDIUM: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_LOW: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_REVIEW_REQUIRED: _ClassVar[ConfidenceLevel]

class IngestionSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INGESTION_SOURCE_UNSPECIFIED: _ClassVar[IngestionSource]
    INGESTION_SOURCE_WEB_UPLOAD: _ClassVar[IngestionSource]
    INGESTION_SOURCE_EMAIL: _ClassVar[IngestionSource]
    INGESTION_SOURCE_MOBILE_APP: _ClassVar[IngestionSource]
    INGESTION_SOURCE_API: _ClassVar[IngestionSource]
    INGESTION_SOURCE_BATCH_IMPORT: _ClassVar[IngestionSource]
    INGESTION_SOURCE_WORKER: _ClassVar[IngestionSource]

class ContentType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTENT_TYPE_UNSPECIFIED: _ClassVar[ContentType]
    CONTENT_TYPE_JSON: _ClassVar[ContentType]
    CONTENT_TYPE_TEXT: _ClassVar[ContentType]
    CONTENT_TYPE_PDF: _ClassVar[ContentType]
    CONTENT_TYPE_JPEG: _ClassVar[ContentType]
    CONTENT_TYPE_PNG: _ClassVar[ContentType]
    CONTENT_TYPE_BINARY: _ClassVar[ContentType]

class ExportFormat(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXPORT_FORMAT_UNSPECIFIED: _ClassVar[ExportFormat]
    EXPORT_FORMAT_JSON: _ClassVar[ExportFormat]
    EXPORT_FORMAT_CSV: _ClassVar[ExportFormat]
    EXPORT_FORMAT_XLSX: _ClassVar[ExportFormat]
    EXPORT_FORMAT_DOCX: _ClassVar[ExportFormat]
    EXPORT_FORMAT_PDF: _ClassVar[ExportFormat]

class SortDirection(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SORT_DIRECTION_UNSPECIFIED: _ClassVar[SortDirection]
    SORT_DIRECTION_ASC: _ClassVar[SortDirection]
    SORT_DIRECTION_DESC: _ClassVar[SortDirection]
CONFIDENCE_LEVEL_UNSPECIFIED: ConfidenceLevel
CONFIDENCE_LEVEL_HIGH: ConfidenceLevel
CONFIDENCE_LEVEL_MEDIUM: ConfidenceLevel
CONFIDENCE_LEVEL_LOW: ConfidenceLevel
CONFIDENCE_LEVEL_REVIEW_REQUIRED: ConfidenceLevel
INGESTION_SOURCE_UNSPECIFIED: IngestionSource
INGESTION_SOURCE_WEB_UPLOAD: IngestionSource
INGESTION_SOURCE_EMAIL: IngestionSource
INGESTION_SOURCE_MOBILE_APP: IngestionSource
INGESTION_SOURCE_API: IngestionSource
INGESTION_SOURCE_BATCH_IMPORT: IngestionSource
INGESTION_SOURCE_WORKER: IngestionSource
CONTENT_TYPE_UNSPECIFIED: ContentType
CONTENT_TYPE_JSON: ContentType
CONTENT_TYPE_TEXT: ContentType
CONTENT_TYPE_PDF: ContentType
CONTENT_TYPE_JPEG: ContentType
CONTENT_TYPE_PNG: ContentType
CONTENT_TYPE_BINARY: ContentType
EXPORT_FORMAT_UNSPECIFIED: ExportFormat
EXPORT_FORMAT_JSON: ExportFormat
EXPORT_FORMAT_CSV: ExportFormat
EXPORT_FORMAT_XLSX: ExportFormat
EXPORT_FORMAT_DOCX: ExportFormat
EXPORT_FORMAT_PDF: ExportFormat
SORT_DIRECTION_UNSPECIFIED: SortDirection
SORT_DIRECTION_ASC: SortDirection
SORT_DIRECTION_DESC: SortDirection

class TenantContext(_message.Message):
    __slots__ = ("tenant_id", "organization_id", "project_id", "environment", "region", "partition_id", "access_surface", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_SURFACE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    organization_id: str
    project_id: str
    environment: str
    region: str
    partition_id: str
    access_surface: str
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, tenant_id: _Optional[str] = ..., organization_id: _Optional[str] = ..., project_id: _Optional[str] = ..., environment: _Optional[str] = ..., region: _Optional[str] = ..., partition_id: _Optional[str] = ..., access_surface: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class RequestContext(_message.Message):
    __slots__ = ("tenant", "request_id", "correlation_id", "user_id", "headers", "trace_id", "span_id", "ip_address", "user_agent", "timestamp", "principal_id", "service_identity", "scopes", "roles", "purpose", "idempotency_key", "client_catalog_version", "consistency", "attributes", "traceparent")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TENANT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_ID_FIELD_NUMBER: _ClassVar[int]
    SERVICE_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_CATALOG_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    tenant: TenantContext
    request_id: str
    correlation_id: str
    user_id: str
    headers: _containers.ScalarMap[str, str]
    trace_id: str
    span_id: str
    ip_address: str
    user_agent: str
    timestamp: _timestamp_pb2.Timestamp
    principal_id: str
    service_identity: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    roles: _containers.RepeatedScalarFieldContainer[str]
    purpose: str
    idempotency_key: str
    client_catalog_version: str
    consistency: str
    attributes: _containers.ScalarMap[str, str]
    traceparent: str
    def __init__(self, tenant: _Optional[_Union[TenantContext, _Mapping]] = ..., request_id: _Optional[str] = ..., correlation_id: _Optional[str] = ..., user_id: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ..., trace_id: _Optional[str] = ..., span_id: _Optional[str] = ..., ip_address: _Optional[str] = ..., user_agent: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., principal_id: _Optional[str] = ..., service_identity: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., roles: _Optional[_Iterable[str]] = ..., purpose: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., client_catalog_version: _Optional[str] = ..., consistency: _Optional[str] = ..., attributes: _Optional[_Mapping[str, str]] = ..., traceparent: _Optional[str] = ...) -> None: ...

class AuditInfo(_message.Message):
    __slots__ = ("created_at", "updated_at", "created_by", "updated_by", "deleted_at", "deleted_by")
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    UPDATED_BY_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_BY_FIELD_NUMBER: _ClassVar[int]
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    created_by: str
    updated_by: str
    deleted_at: _timestamp_pb2.Timestamp
    deleted_by: str
    def __init__(self, created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_by: _Optional[str] = ..., updated_by: _Optional[str] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_by: _Optional[str] = ...) -> None: ...

class FileReference(_message.Message):
    __slots__ = ("file_id", "file_name", "storage_uri", "mime_type", "file_size", "checksum")
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_NAME_FIELD_NUMBER: _ClassVar[int]
    STORAGE_URI_FIELD_NUMBER: _ClassVar[int]
    MIME_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILE_SIZE_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    file_id: str
    file_name: str
    storage_uri: str
    mime_type: str
    file_size: int
    checksum: str
    def __init__(self, file_id: _Optional[str] = ..., file_name: _Optional[str] = ..., storage_uri: _Optional[str] = ..., mime_type: _Optional[str] = ..., file_size: _Optional[int] = ..., checksum: _Optional[str] = ...) -> None: ...

class BoundingBox(_message.Message):
    __slots__ = ("x", "y", "width", "height")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    WIDTH_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    width: float
    height: float
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ..., width: _Optional[float] = ..., height: _Optional[float] = ...) -> None: ...

class ConfidenceScore(_message.Message):
    __slots__ = ("value", "level")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    value: float
    level: ConfidenceLevel
    def __init__(self, value: _Optional[float] = ..., level: _Optional[_Union[ConfidenceLevel, str]] = ...) -> None: ...

class DateRangeFilter(_message.Message):
    __slots__ = ("to",)
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    to: str
    def __init__(self, to: _Optional[str] = ..., **kwargs) -> None: ...
