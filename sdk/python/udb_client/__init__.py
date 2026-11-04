from ._channel import default_channel_options, merge_channel_options
from .client import (
    Page,
    UdbAsyncClient,
    UdbClient,
    UpsertResult,
    decode_records,
    receipt_from_response,
    to_record_json,
    to_struct,
    was_duplicate,
)
from .consume import ConsumedEvent, ConsumeOptions, consume, consume_async
from .error_code import (
    ErrorCode,
    ErrorInfo,
    code_of,
    error_reason,
    inspect,
    is_cas_conflict,
    is_conflict,
    is_not_found,
    is_unique,
    missing_scope,
)
from .exceptions import (
    UdbAuthzDenied,
    UdbConfigurationError,
    UdbConflict,
    UdbError,
    UdbNotFound,
    UdbPolicyBundleError,
    UdbRpcError,
)
from .metadata import (
    DEFAULT_READ_FENCE_MAX_WAIT_MS,
    ConsistencyMode,
    Metadata,
    ReadFence,
    UDB_PROTOCOL_VERSION,
    WriteReceipt,
    read_fence_from_receipt,
)
from .record_codec import (
    WRITE_INSERT,
    WRITE_UPDATE,
    WriteMode,
    decode_field,
    decode_record,
    encode_event,
    encode_field,
    encode_record,
    format_timestamp,
    parse_timestamp,
)
from .schema import (
    event_type,
    message_type,
    partition_key_field,
    primary_key,
    primary_keys,
    project_column,
    tenant_column,
    topic,
)
from .table import (
    AsyncRepo,
    AsyncTable,
    Column,
    Repo,
    Table,
    TablePage,
    TableSpec,
    UDBColumn,
    table_spec_of,
)
from .tx import EVENT_ENVELOPE_VERSION, TxScope
from .negotiation import (
    ENCODING_RECORD_BATCH_V2,
    ENCODING_RECORD_SET_V1,
    Negotiator,
    client_supported_encodings,
)

# Auth + authz ergonomics and token lifecycle. Imported defensively because the
# generated authn/authz stubs must have been emitted (buf generate
# --include-imports) for these to import.
try:  # pragma: no cover - presence depends on codegen having run
    from .auth import (
        POLICY_BUNDLE_ALGORITHM,
        AuthzCache,
        InMemoryTokenStore,
        StoredToken,
        TokenSession,
        TokenStore,
        UdbAuthClient,
        as_resource,
        native_transaction,
        verify_policy_bundle,
    )
    from .project import (
        UdbAsyncProject,
        UdbConfig,
        UdbProject,
        create_udb,
        create_udb_async,
    )
    # Facade sub-client classes for type annotations (accessed in practice via
    # ``project.storage`` / ``project.asset`` / ``project.webrtc``).
    from .project import _AssetFacade as AssetFacade
    from .project import _StorageFacade as StorageFacade
    from .project import _WebRtcFacade as WebRtcFacade

    _HAS_AUTH = True
except ImportError:  # pragma: no cover
    _HAS_AUTH = False

# Generated robustness layer (rendered by `udb sdk generate` into
# generated_client.py). Imported defensively so a checkout without the generated
# file still works with the hand-written client.
try:  # pragma: no cover - presence depends on codegen having run
    from .generated_client import (
        GeneratedClient,
        RetryPolicy,
        UdbDetailedRpcError,
    )

    _HAS_GENERATED = True
except ImportError:  # pragma: no cover
    _HAS_GENERATED = False

__all__ = [
    "AsyncRepo",
    "AsyncTable",
    "Column",
    "ConsumedEvent",
    "ConsumeOptions",
    "ErrorCode",
    "ErrorInfo",
    "EVENT_ENVELOPE_VERSION",
    "Page",
    "Repo",
    "Table",
    "TablePage",
    "TableSpec",
    "TxScope",
    "UDBColumn",
    "UdbConflict",
    "UdbNotFound",
    "UpsertResult",
    "WRITE_INSERT",
    "WRITE_UPDATE",
    "WriteMode",
    "code_of",
    "consume",
    "consume_async",
    "decode_field",
    "decode_record",
    "encode_event",
    "encode_field",
    "encode_record",
    "error_reason",
    "event_type",
    "format_timestamp",
    "inspect",
    "is_cas_conflict",
    "is_conflict",
    "is_not_found",
    "is_unique",
    "message_type",
    "missing_scope",
    "parse_timestamp",
    "partition_key_field",
    "primary_key",
    "primary_keys",
    "project_column",
    "table_spec_of",
    "tenant_column",
    "topic",
    "DEFAULT_READ_FENCE_MAX_WAIT_MS",
    "ENCODING_RECORD_BATCH_V2",
    "ENCODING_RECORD_SET_V1",
    "ConsistencyMode",
    "Metadata",
    "Negotiator",
    "ReadFence",
    "UDB_PROTOCOL_VERSION",
    "WriteReceipt",
    "UdbAsyncClient",
    "UdbAuthzDenied",
    "UdbClient",
    "UdbConfigurationError",
    "UdbError",
    "UdbPolicyBundleError",
    "UdbRpcError",
    "client_supported_encodings",
    "default_channel_options",
    "merge_channel_options",
    "decode_records",
    "read_fence_from_receipt",
    "receipt_from_response",
    "to_record_json",
    "to_struct",
    "was_duplicate",
]

if _HAS_GENERATED:
    __all__ += ["GeneratedClient", "RetryPolicy", "UdbDetailedRpcError"]

if _HAS_AUTH:
    __all__ += [
        "POLICY_BUNDLE_ALGORITHM",
        "AssetFacade",
        "AuthzCache",
        "InMemoryTokenStore",
        "StorageFacade",
        "WebRtcFacade",
        "StoredToken",
        "TokenSession",
        "TokenStore",
        "UdbAsyncProject",
        "UdbAuthClient",
        "UdbConfig",
        "UdbProject",
        "as_resource",
        "create_udb",
        "create_udb_async",
        "native_transaction",
        "verify_policy_bundle",
    ]

# Session, pool, environment and builder surface. Needs the same generated stubs
# as the project facade above.
if _HAS_AUTH:
    from ._credentials import UdbCredentialError
    from .connect_env import Expect, config_from_env, connect_from_env, connect_from_env_async
    from .enterprise import (
        EnterpriseConfig,
        EnterpriseLockService,
        EnterpriseSession,
        TenantState,
        UdbCredentialBindingError,
        UdbTenantError,
        connect_enterprise,
        connect_enterprise_async,
    )
    from .facades import AdminFacade, AsyncAdminFacade, AsyncFlagsFacade, FlagsFacade, UserSession
    from .graph_vector import (
        AsyncGraphTraversalBuilder,
        AsyncVectorSearchBuilder,
        GraphTraversalBuilder,
        VectorHit,
        VectorSearchBuilder,
    )
    from .livequery import (
        AsyncLiveQuery,
        LiveQuery,
        LiveQueryEvent,
        UdbNoDeltaFeed,
        lq_eq,
        lq_ge,
        lq_gt,
        lq_in,
        lq_is_not_null,
        lq_is_null,
        lq_le,
        lq_lt,
        lq_ne,
        lq_not_in,
    )
    from .schema_options import (
        apply_column_defaults,
        export_eligible,
        graph_label,
        max_len,
        owner_column,
        owner_columns,
        retention_class,
        soft_delete,
        topic,
        unique_keys,
    )
    from .tenant_pool import TenantSessionPool, api_key_tenant_pool

    __all__ += [
        "AdminFacade",
        "AsyncAdminFacade",
        "AsyncFlagsFacade",
        "AsyncGraphTraversalBuilder",
        "AsyncLiveQuery",
        "AsyncVectorSearchBuilder",
        "EnterpriseConfig",
        "EnterpriseLockService",
        "EnterpriseSession",
        "Expect",
        "FlagsFacade",
        "GraphTraversalBuilder",
        "LiveQuery",
        "LiveQueryEvent",
        "TenantSessionPool",
        "TenantState",
        "UdbCredentialBindingError",
        "UdbCredentialError",
        "UdbNoDeltaFeed",
        "UdbTenantError",
        "UserSession",
        "VectorHit",
        "VectorSearchBuilder",
        "api_key_tenant_pool",
        "apply_column_defaults",
        "config_from_env",
        "connect_enterprise",
        "connect_enterprise_async",
        "connect_from_env",
        "connect_from_env_async",
        "export_eligible",
        "graph_label",
        "lq_eq",
        "lq_ge",
        "lq_gt",
        "lq_in",
        "lq_is_not_null",
        "lq_is_null",
        "lq_le",
        "lq_lt",
        "lq_ne",
        "lq_not_in",
        "max_len",
        "owner_column",
        "owner_columns",
        "retention_class",
        "soft_delete",
        "topic",
        "unique_keys",
    ]
