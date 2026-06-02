"""Protocol-negotiation helpers for the UDB wire protocol.

The server advertises protocol support on ``CapabilitiesResponse.protocol_support``
(field 9). Generated ``_pb2`` stubs may not yet carry that field, so these helpers
read it DEFENSIVELY: a missing field, ``None``, or an empty ``encodings`` list falls
back to V1 (``record_set_v1`` / the ``Select``/``RecordSet`` path). V2 is never
hard-required.
"""

from __future__ import annotations

from typing import Any, Iterable

from .metadata import UDB_PROTOCOL_VERSION

# Wire encoding identifiers.
ENCODING_RECORD_SET_V1 = "record_set_v1"
ENCODING_RECORD_BATCH_V2 = "record_batch_v2"

# Row encodings this SDK build can decode, most preferred first. record_set_v1 is
# always supported; record_batch_v2 becomes usable once SelectV2 wrappers land
# (task A.4).
CLIENT_SUPPORTED_ENCODINGS: tuple[str, ...] = (ENCODING_RECORD_SET_V1,)


def _extract_protocol_support(source: Any) -> Any:
    """Return the protocol_support view from a CapabilitiesResponse, a raw
    protocol_support object/dict, or ``None`` — without requiring the generated
    field to exist."""

    if source is None:
        return None
    # Already a protocol_support-shaped object/dict?
    if _get(source, "encodings", None) is not None or _get(
        source, "min_protocol_version", None
    ) not in (None, ""):
        # Could be the response itself if it also carried a nested field; prefer the
        # nested one when present.
        nested = _get(source, "protocol_support", None)
        return nested if nested is not None else source
    # A CapabilitiesResponse: pull the nested field defensively.
    return _get(source, "protocol_support", None)


def _get(obj: Any, name: str, default: Any) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


class Negotiator:
    """Picks the best encoding shared by this client and a server's protocol support.

    Accepts a generated ``CapabilitiesResponse``, a bare ``protocol_support``
    object, a plain dict, or ``None`` (server advertised nothing). ``None`` and
    empty inputs negotiate V1.
    """

    def __init__(self, source: Any = None) -> None:
        self._support = _extract_protocol_support(source)

    def _encodings(self) -> list[str]:
        raw = _get(self._support, "encodings", None)
        if not raw:
            return []
        return [str(item) for item in raw]

    def supports_encoding(self, name: str) -> bool:
        """Whether the server advertises ``name``. V1 is implicit when the server
        advertised no encodings."""

        encodings = self._encodings()
        if not encodings:
            return name == ENCODING_RECORD_SET_V1
        return name in encodings

    def negotiated_encoding(self) -> str:
        """Return ``record_batch_v2`` only if the server advertises it AND this
        client supports it; otherwise fall back to ``record_set_v1``."""

        if (
            ENCODING_RECORD_BATCH_V2 in CLIENT_SUPPORTED_ENCODINGS
            and self.supports_encoding(ENCODING_RECORD_BATCH_V2)
        ):
            return ENCODING_RECORD_BATCH_V2
        return ENCODING_RECORD_SET_V1

    def protocol_range(self) -> tuple[str, str]:
        """Server's ``(min, max)`` protocol version, falling back to the client's
        compiled-in version when unknown."""

        minimum = _get(self._support, "min_protocol_version", "") or UDB_PROTOCOL_VERSION
        maximum = _get(self._support, "max_protocol_version", "") or UDB_PROTOCOL_VERSION
        return (str(minimum), str(maximum))

    def server_supports_streaming_reads(self) -> bool:
        """Whether the server advertises streaming reads. Absent support is False
        (V1 unary behavior)."""

        return bool(_get(self._support, "supports_streaming_reads", False))


def client_supported_encodings() -> Iterable[str]:
    """Encodings this SDK build can decode."""

    return CLIENT_SUPPORTED_ENCODINGS
