"""Shared default gRPC channel options for all UDB Python clients.

Every UDB client (sync ``UdbClient``, async ``UdbAsyncClient``, the generated
robustness layer, and the high-level ``UdbProject`` facade) holds **one
long-lived channel** and reuses it across RPCs. These defaults make that warm
channel behave well:

* **Keepalive** so an otherwise idle HTTP/2 connection does not drop to IDLE
  (gRPC default ~300s) and pay a fresh TCP+TLS+HTTP/2 handshake on the next RPC.
* A native gRPC **service-config retry policy** (jittered exponential backoff,
  ``UNAVAILABLE`` only, bounded by the call deadline) so transient broker blips
  do not surface as errors. This is the transport-level retry; the generated
  layer additionally applies an app-level retry for read-safe RPCs.

User-supplied ``channel_options`` always win: :func:`merge_channel_options`
appends them after the defaults so a caller can override any single key.
"""

from __future__ import annotations

import json
from typing import Any, Sequence

# Keepalive (milliseconds). Ping an idle connection every 30s, give the ack 10s,
# and allow pings even with no active stream so a fully idle channel stays warm.
KEEPALIVE_TIME_MS = 30_000
KEEPALIVE_TIMEOUT_MS = 10_000

# Native gRPC service-config retry: 4 attempts, 0.1s base, 2s ceiling, x2 growth,
# only on UNAVAILABLE. gRPC applies ~20% jitter to each backoff internally and
# bounds the whole sequence by the call deadline.
_RETRY_SERVICE_CONFIG = {
    "methodConfig": [
        {
            "name": [{}],  # empty name = applies to every method on the channel
            "retryPolicy": {
                "maxAttempts": 4,
                "initialBackoff": "0.1s",
                "maxBackoff": "2s",
                "backoffMultiplier": 2.0,
                "retryableStatusCodes": ["UNAVAILABLE"],
            },
        }
    ]
}


def default_channel_options() -> list[tuple[str, Any]]:
    """Return UDB's default gRPC channel options (keepalive + retry).

    Suitable to pass directly as ``options=`` to ``grpc.insecure_channel`` /
    ``grpc.secure_channel`` (and their ``grpc.aio`` equivalents).
    """

    return [
        ("grpc.keepalive_time_ms", KEEPALIVE_TIME_MS),
        ("grpc.keepalive_timeout_ms", KEEPALIVE_TIMEOUT_MS),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.enable_retries", 1),
        ("grpc.service_config", json.dumps(_RETRY_SERVICE_CONFIG)),
    ]


def merge_channel_options(
    user_options: Sequence[tuple[str, Any]] | None,
) -> list[tuple[str, Any]]:
    """Merge UDB defaults with caller-supplied options; caller options win.

    Caller options are appended last so that for any duplicate key the caller's
    value takes precedence in gRPC's last-wins channel-arg resolution.
    """

    return [*default_channel_options(), *list(user_options or ())]
