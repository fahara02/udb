from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BackupTableEntry(_message.Message):
    __slots__ = ("schema", "table", "tenant_column", "object_key", "row_count", "checksum_sha256")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    OBJECT_KEY_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    schema: str
    table: str
    tenant_column: str
    object_key: str
    row_count: int
    checksum_sha256: str
    def __init__(self, schema: _Optional[str] = ..., table: _Optional[str] = ..., tenant_column: _Optional[str] = ..., object_key: _Optional[str] = ..., row_count: _Optional[int] = ..., checksum_sha256: _Optional[str] = ...) -> None: ...

class BackupExcludedTable(_message.Message):
    __slots__ = ("schema", "table", "reason")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    schema: str
    table: str
    reason: str
    def __init__(self, schema: _Optional[str] = ..., table: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class BackupRunSummary(_message.Message):
    __slots__ = ("backup_id", "tenant_id", "kind", "status", "object_prefix", "manifest_checksum", "table_count", "total_rows", "excluded_count", "source_tenant_id", "target_tenant_id", "created_at_unix", "completed_at_unix")
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    OBJECT_PREFIX_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    TABLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_COUNT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    backup_id: str
    tenant_id: str
    kind: str
    status: str
    object_prefix: str
    manifest_checksum: str
    table_count: int
    total_rows: int
    excluded_count: int
    source_tenant_id: str
    target_tenant_id: str
    created_at_unix: int
    completed_at_unix: int
    def __init__(self, backup_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., kind: _Optional[str] = ..., status: _Optional[str] = ..., object_prefix: _Optional[str] = ..., manifest_checksum: _Optional[str] = ..., table_count: _Optional[int] = ..., total_rows: _Optional[int] = ..., excluded_count: _Optional[int] = ..., source_tenant_id: _Optional[str] = ..., target_tenant_id: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., completed_at_unix: _Optional[int] = ...) -> None: ...

class BackupPolicyView(_message.Message):
    __slots__ = ("policy_id", "tenant_id", "policy_name", "schedule_cron", "retention_days", "max_retained_backups", "enabled", "object_backend", "object_bucket", "created_at_unix", "updated_at_unix")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_CRON_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETAINED_BACKUPS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BACKEND_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BUCKET_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    tenant_id: str
    policy_name: str
    schedule_cron: str
    retention_days: int
    max_retained_backups: int
    enabled: bool
    object_backend: str
    object_bucket: str
    created_at_unix: int
    updated_at_unix: int
    def __init__(self, policy_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., policy_name: _Optional[str] = ..., schedule_cron: _Optional[str] = ..., retention_days: _Optional[int] = ..., max_retained_backups: _Optional[int] = ..., enabled: bool = ..., object_backend: _Optional[str] = ..., object_bucket: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., updated_at_unix: _Optional[int] = ...) -> None: ...

class StartTenantBackupRequest(_message.Message):
    __slots__ = ("tenant_id", "policy_name", "object_backend", "object_bucket", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BACKEND_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BUCKET_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    policy_name: str
    object_backend: str
    object_bucket: str
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., policy_name: _Optional[str] = ..., object_backend: _Optional[str] = ..., object_bucket: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class StartTenantBackupResponse(_message.Message):
    __slots__ = ("backup_id", "object_prefix", "manifest_checksum", "table_count", "total_rows", "excluded_count", "tables", "excluded", "message", "error")
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_PREFIX_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    TABLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_COUNT_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backup_id: str
    object_prefix: str
    manifest_checksum: str
    table_count: int
    total_rows: int
    excluded_count: int
    tables: _containers.RepeatedCompositeFieldContainer[BackupTableEntry]
    excluded: _containers.RepeatedCompositeFieldContainer[BackupExcludedTable]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, backup_id: _Optional[str] = ..., object_prefix: _Optional[str] = ..., manifest_checksum: _Optional[str] = ..., table_count: _Optional[int] = ..., total_rows: _Optional[int] = ..., excluded_count: _Optional[int] = ..., tables: _Optional[_Iterable[_Union[BackupTableEntry, _Mapping]]] = ..., excluded: _Optional[_Iterable[_Union[BackupExcludedTable, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RestoreTenantRequest(_message.Message):
    __slots__ = ("source_tenant_id", "target_tenant_id", "backup_id", "confirmation_token", "allow_cross_tenant", "metadata_json")
    SOURCE_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIRMATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ALLOW_CROSS_TENANT_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    source_tenant_id: str
    target_tenant_id: str
    backup_id: str
    confirmation_token: str
    allow_cross_tenant: bool
    metadata_json: str
    def __init__(self, source_tenant_id: _Optional[str] = ..., target_tenant_id: _Optional[str] = ..., backup_id: _Optional[str] = ..., confirmation_token: _Optional[str] = ..., allow_cross_tenant: bool = ..., metadata_json: _Optional[str] = ...) -> None: ...

class RestoreTenantResponse(_message.Message):
    __slots__ = ("backup_id", "source_object_prefix", "restored_table_count", "restored_rows", "message", "error")
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_OBJECT_PREFIX_FIELD_NUMBER: _ClassVar[int]
    RESTORED_TABLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESTORED_ROWS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backup_id: str
    source_object_prefix: str
    restored_table_count: int
    restored_rows: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, backup_id: _Optional[str] = ..., source_object_prefix: _Optional[str] = ..., restored_table_count: _Optional[int] = ..., restored_rows: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListBackupsRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token", "kind")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    kind: str
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., kind: _Optional[str] = ...) -> None: ...

class ListBackupsResponse(_message.Message):
    __slots__ = ("backups", "next_page_token", "error")
    BACKUPS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backups: _containers.RepeatedCompositeFieldContainer[BackupRunSummary]
    next_page_token: str
    error: _dto_pb2.ApiError
    def __init__(self, backups: _Optional[_Iterable[_Union[BackupRunSummary, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetBackupRequest(_message.Message):
    __slots__ = ("tenant_id", "backup_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    BACKUP_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    backup_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., backup_id: _Optional[str] = ...) -> None: ...

class GetBackupResponse(_message.Message):
    __slots__ = ("backup", "tables", "excluded", "error")
    BACKUP_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backup: BackupRunSummary
    tables: _containers.RepeatedCompositeFieldContainer[BackupTableEntry]
    excluded: _containers.RepeatedCompositeFieldContainer[BackupExcludedTable]
    error: _dto_pb2.ApiError
    def __init__(self, backup: _Optional[_Union[BackupRunSummary, _Mapping]] = ..., tables: _Optional[_Iterable[_Union[BackupTableEntry, _Mapping]]] = ..., excluded: _Optional[_Iterable[_Union[BackupExcludedTable, _Mapping]]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class PutBackupPolicyRequest(_message.Message):
    __slots__ = ("tenant_id", "policy_name", "schedule_cron", "retention_days", "max_retained_backups", "enabled", "object_backend", "object_bucket", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_CRON_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETAINED_BACKUPS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BACKEND_FIELD_NUMBER: _ClassVar[int]
    OBJECT_BUCKET_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    policy_name: str
    schedule_cron: str
    retention_days: int
    max_retained_backups: int
    enabled: bool
    object_backend: str
    object_bucket: str
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., policy_name: _Optional[str] = ..., schedule_cron: _Optional[str] = ..., retention_days: _Optional[int] = ..., max_retained_backups: _Optional[int] = ..., enabled: bool = ..., object_backend: _Optional[str] = ..., object_bucket: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class PutBackupPolicyResponse(_message.Message):
    __slots__ = ("policy_id", "message", "error")
    POLICY_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    policy_id: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, policy_id: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetBackupPolicyRequest(_message.Message):
    __slots__ = ("tenant_id", "policy_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    policy_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., policy_name: _Optional[str] = ...) -> None: ...

class GetBackupPolicyResponse(_message.Message):
    __slots__ = ("policy", "error")
    POLICY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    policy: BackupPolicyView
    error: _dto_pb2.ApiError
    def __init__(self, policy: _Optional[_Union[BackupPolicyView, _Mapping]] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListBackupPoliciesRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListBackupPoliciesResponse(_message.Message):
    __slots__ = ("policies", "next_page_token", "error")
    POLICIES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    policies: _containers.RepeatedCompositeFieldContainer[BackupPolicyView]
    next_page_token: str
    error: _dto_pb2.ApiError
    def __init__(self, policies: _Optional[_Iterable[_Union[BackupPolicyView, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteBackupPolicyRequest(_message.Message):
    __slots__ = ("tenant_id", "policy_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    policy_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., policy_name: _Optional[str] = ...) -> None: ...

class DeleteBackupPolicyResponse(_message.Message):
    __slots__ = ("deleted", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
