from .client import UdbAsyncClient, UdbClient, decode_records, to_record_json, to_struct
from .exceptions import UdbConfigurationError, UdbError, UdbRpcError
from .metadata import Metadata, UDB_PROTOCOL_VERSION
from .negotiation import (
    ENCODING_RECORD_BATCH_V2,
    ENCODING_RECORD_SET_V1,
    Negotiator,
    client_supported_encodings,
)

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
    "UdbClient",
    "UdbConfigurationError",
    "UdbError",
    "UdbRpcError",
    "client_supported_encodings",
    "decode_records",
    "to_record_json",
    "to_struct",
]

if _HAS_GENERATED:
    __all__ += ["GeneratedClient", "RetryPolicy", "UdbDetailedRpcError"]
