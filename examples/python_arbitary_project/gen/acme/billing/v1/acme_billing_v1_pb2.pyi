from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CacheBackend(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CACHE_BACKEND_UNSPECIFIED: _ClassVar[CacheBackend]
    CACHE_BACKEND_REDIS: _ClassVar[CacheBackend]

class VectorBackend(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VECTOR_BACKEND_UNSPECIFIED: _ClassVar[VectorBackend]
    VECTOR_BACKEND_QDRANT: _ClassVar[VectorBackend]

class VectorDistance(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VECTOR_DISTANCE_UNSPECIFIED: _ClassVar[VectorDistance]
    VECTOR_DISTANCE_COSINE: _ClassVar[VectorDistance]

class ObjectBackend(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OBJECT_BACKEND_UNSPECIFIED: _ClassVar[ObjectBackend]
    OBJECT_BACKEND_S3: _ClassVar[ObjectBackend]

class PiiKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PII_KIND_UNSPECIFIED: _ClassVar[PiiKind]
    PII_KIND_NAME: _ClassVar[PiiKind]
    PII_KIND_EMAIL: _ClassVar[PiiKind]
CACHE_BACKEND_UNSPECIFIED: CacheBackend
CACHE_BACKEND_REDIS: CacheBackend
VECTOR_BACKEND_UNSPECIFIED: VectorBackend
VECTOR_BACKEND_QDRANT: VectorBackend
VECTOR_DISTANCE_UNSPECIFIED: VectorDistance
VECTOR_DISTANCE_COSINE: VectorDistance
OBJECT_BACKEND_UNSPECIFIED: ObjectBackend
OBJECT_BACKEND_S3: ObjectBackend
PII_KIND_UNSPECIFIED: PiiKind
PII_KIND_NAME: PiiKind
PII_KIND_EMAIL: PiiKind
TABLE_FIELD_NUMBER: _ClassVar[int]
table: _descriptor.FieldDescriptor
CACHE_FIELD_NUMBER: _ClassVar[int]
cache: _descriptor.FieldDescriptor
VECTOR_STORE_FIELD_NUMBER: _ClassVar[int]
vector_store: _descriptor.FieldDescriptor
OBJECT_STORE_FIELD_NUMBER: _ClassVar[int]
object_store: _descriptor.FieldDescriptor
COLUMN_FIELD_NUMBER: _ClassVar[int]
column: _descriptor.FieldDescriptor

class TableOptions(_message.Message):
    __slots__ = ("table_name", "schema_name", "migration_order", "is_table", "enable_rls", "tenant_column", "comment")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    MIGRATION_ORDER_FIELD_NUMBER: _ClassVar[int]
    IS_TABLE_FIELD_NUMBER: _ClassVar[int]
    ENABLE_RLS_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    schema_name: str
    migration_order: int
    is_table: bool
    enable_rls: bool
    tenant_column: str
    comment: str
    def __init__(self, table_name: _Optional[str] = ..., schema_name: _Optional[str] = ..., migration_order: _Optional[int] = ..., is_table: bool = ..., enable_rls: bool = ..., tenant_column: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class CacheOptions(_message.Message):
    __slots__ = ("backend", "key_pattern", "ttl_seconds", "write_through", "read_through")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    KEY_PATTERN_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    WRITE_THROUGH_FIELD_NUMBER: _ClassVar[int]
    READ_THROUGH_FIELD_NUMBER: _ClassVar[int]
    backend: CacheBackend
    key_pattern: str
    ttl_seconds: int
    write_through: bool
    read_through: bool
    def __init__(self, backend: _Optional[_Union[CacheBackend, str]] = ..., key_pattern: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., write_through: bool = ..., read_through: bool = ...) -> None: ...

class VectorStoreOptions(_message.Message):
    __slots__ = ("backend", "collection_name", "dimension", "distance")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    backend: VectorBackend
    collection_name: str
    dimension: int
    distance: VectorDistance
    def __init__(self, backend: _Optional[_Union[VectorBackend, str]] = ..., collection_name: _Optional[str] = ..., dimension: _Optional[int] = ..., distance: _Optional[_Union[VectorDistance, str]] = ...) -> None: ...

class ObjectStoreOptions(_message.Message):
    __slots__ = ("backend", "bucket", "path_prefix", "server_side_encryption", "presigned_read", "presigned_write", "presigned_ttl_seconds")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PATH_PREFIX_FIELD_NUMBER: _ClassVar[int]
    SERVER_SIDE_ENCRYPTION_FIELD_NUMBER: _ClassVar[int]
    PRESIGNED_READ_FIELD_NUMBER: _ClassVar[int]
    PRESIGNED_WRITE_FIELD_NUMBER: _ClassVar[int]
    PRESIGNED_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    backend: ObjectBackend
    bucket: str
    path_prefix: str
    server_side_encryption: bool
    presigned_read: bool
    presigned_write: bool
    presigned_ttl_seconds: int
    def __init__(self, backend: _Optional[_Union[ObjectBackend, str]] = ..., bucket: _Optional[str] = ..., path_prefix: _Optional[str] = ..., server_side_encryption: bool = ..., presigned_read: bool = ..., presigned_write: bool = ..., presigned_ttl_seconds: _Optional[int] = ...) -> None: ...

class ColumnOptions(_message.Message):
    __slots__ = ("column_name", "is_primary_key", "tenant_column", "pii_kind", "encrypt", "foreign_key", "is_created_at", "is_updated_at")
    COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_KEY_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    PII_KIND_FIELD_NUMBER: _ClassVar[int]
    ENCRYPT_FIELD_NUMBER: _ClassVar[int]
    FOREIGN_KEY_FIELD_NUMBER: _ClassVar[int]
    IS_CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    IS_UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    column_name: str
    is_primary_key: bool
    tenant_column: bool
    pii_kind: PiiKind
    encrypt: bool
    foreign_key: str
    is_created_at: bool
    is_updated_at: bool
    def __init__(self, column_name: _Optional[str] = ..., is_primary_key: bool = ..., tenant_column: bool = ..., pii_kind: _Optional[_Union[PiiKind, str]] = ..., encrypt: bool = ..., foreign_key: _Optional[str] = ..., is_created_at: bool = ..., is_updated_at: bool = ...) -> None: ...

class Invoice(_message.Message):
    __slots__ = ("invoice_id", "org_id", "customer_name", "customer_email", "amount_cents", "currency", "status", "created_at", "updated_at")
    INVOICE_ID_FIELD_NUMBER: _ClassVar[int]
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_NAME_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_CENTS_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    invoice_id: str
    org_id: str
    customer_name: str
    customer_email: str
    amount_cents: int
    currency: str
    status: str
    created_at: str
    updated_at: str
    def __init__(self, invoice_id: _Optional[str] = ..., org_id: _Optional[str] = ..., customer_name: _Optional[str] = ..., customer_email: _Optional[str] = ..., amount_cents: _Optional[int] = ..., currency: _Optional[str] = ..., status: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class InvoiceLineItem(_message.Message):
    __slots__ = ("line_item_id", "org_id", "invoice_id", "description", "unit_price", "quantity")
    LINE_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    INVOICE_ID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    line_item_id: str
    org_id: str
    invoice_id: str
    description: str
    unit_price: int
    quantity: int
    def __init__(self, line_item_id: _Optional[str] = ..., org_id: _Optional[str] = ..., invoice_id: _Optional[str] = ..., description: _Optional[str] = ..., unit_price: _Optional[int] = ..., quantity: _Optional[int] = ...) -> None: ...

class Product(_message.Message):
    __slots__ = ("product_id", "name", "description", "price_cents", "sku")
    PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PRICE_CENTS_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    product_id: str
    name: str
    description: str
    price_cents: int
    sku: str
    def __init__(self, product_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., price_cents: _Optional[int] = ..., sku: _Optional[str] = ...) -> None: ...

class BillingDocument(_message.Message):
    __slots__ = ("document_id", "invoice_id", "object_key", "content_type", "size_bytes")
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    INVOICE_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    document_id: str
    invoice_id: str
    object_key: str
    content_type: str
    size_bytes: int
    def __init__(self, document_id: _Optional[str] = ..., invoice_id: _Optional[str] = ..., object_key: _Optional[str] = ..., content_type: _Optional[str] = ..., size_bytes: _Optional[int] = ...) -> None: ...
