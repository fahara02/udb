from ._channel import default_channel_options, merge_channel_options
from .client import UdbAsyncClient, UdbClient, decode_records, to_record_json, to_struct
from .exceptions import (
    UdbAuthzDenied,
    UdbConfigurationError,
    UdbError,
    UdbPolicyBundleError,
    UdbRpcError,
)
from .metadata import Metadata, UDB_PROTOCOL_VERSION
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
    "ENCODING_RECORD_BATCH_V2",
    "ENCODING_RECORD_SET_V1",
    "Metadata",
    "Negotiator",
    "UDB_PROTOCOL_VERSION",
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
    "to_record_json",
    "to_struct",
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
