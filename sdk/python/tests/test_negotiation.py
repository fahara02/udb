from __future__ import annotations

from udb_client import (
    ENCODING_RECORD_BATCH_V2,
    ENCODING_RECORD_SET_V1,
    Negotiator,
)
from udb_client.metadata import UDB_PROTOCOL_VERSION
from udb_client.negotiation import CLIENT_SUPPORTED_ENCODINGS


class _FakeProtocolSupport:
    """Stand-in for the generated protocol_support field (not yet in _pb2 stubs)."""

    def __init__(
        self,
        encodings,
        *,
        min_version: str = "",
        max_version: str = "",
        streaming: bool = False,
    ) -> None:
        self.encodings = list(encodings)
        self.min_protocol_version = min_version
        self.max_protocol_version = max_version
        self.supports_streaming_reads = streaming


class _FakeCapabilities:
    def __init__(self, protocol_support) -> None:
        self.protocol_support = protocol_support


def test_negotiates_v1_when_only_record_set_advertised() -> None:
    caps = _FakeCapabilities(_FakeProtocolSupport([ENCODING_RECORD_SET_V1]))
    neg = Negotiator(caps)
    assert neg.negotiated_encoding() == ENCODING_RECORD_SET_V1
    assert neg.supports_encoding(ENCODING_RECORD_SET_V1)
    assert not neg.supports_encoding(ENCODING_RECORD_BATCH_V2)


def test_selects_v2_when_advertised_and_client_supports() -> None:
    caps = _FakeCapabilities(
        _FakeProtocolSupport([ENCODING_RECORD_SET_V1, ENCODING_RECORD_BATCH_V2])
    )
    neg = Negotiator(caps)
    assert neg.supports_encoding(ENCODING_RECORD_BATCH_V2)
    # Client only compiles in V1 today, so it must still fall back until SelectV2
    # wrappers land. Once ENCODING_RECORD_BATCH_V2 is in CLIENT_SUPPORTED_ENCODINGS
    # this asserts V2 selection.
    expected = (
        ENCODING_RECORD_BATCH_V2
        if ENCODING_RECORD_BATCH_V2 in CLIENT_SUPPORTED_ENCODINGS
        else ENCODING_RECORD_SET_V1
    )
    assert neg.negotiated_encoding() == expected


def test_falls_back_to_v1_when_protocol_support_absent() -> None:
    # None source (old stub / nothing advertised).
    neg_none = Negotiator(None)
    assert neg_none.negotiated_encoding() == ENCODING_RECORD_SET_V1
    assert neg_none.supports_encoding(ENCODING_RECORD_SET_V1)
    assert not neg_none.server_supports_streaming_reads()
    assert neg_none.protocol_range() == (UDB_PROTOCOL_VERSION, UDB_PROTOCOL_VERSION)

    # CapabilitiesResponse whose protocol_support is None.
    neg_empty_caps = Negotiator(_FakeCapabilities(None))
    assert neg_empty_caps.negotiated_encoding() == ENCODING_RECORD_SET_V1

    # protocol_support present but with an empty encodings list.
    neg_empty = Negotiator(_FakeCapabilities(_FakeProtocolSupport([])))
    assert neg_empty.negotiated_encoding() == ENCODING_RECORD_SET_V1


def test_accepts_plain_dict_protocol_support() -> None:
    caps = {
        "protocol_support": {
            "encodings": [ENCODING_RECORD_SET_V1, ENCODING_RECORD_BATCH_V2],
            "min_protocol_version": "1.0.0",
            "max_protocol_version": "2.0.0",
            "supports_streaming_reads": True,
        }
    }
    neg = Negotiator(caps)
    assert neg.supports_encoding(ENCODING_RECORD_BATCH_V2)
    assert neg.server_supports_streaming_reads()
    assert neg.protocol_range() == ("1.0.0", "2.0.0")
