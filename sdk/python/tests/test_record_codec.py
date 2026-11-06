"""The record codec produces what the Go SDK's produces on the same inputs.

The expected values are the ones ``sdk/go/udbclient/record_codec_test.go`` asserts for
the same messages, in the same order: this file is that test ported, so the Go
assertions are the shared contract.
"""

from __future__ import annotations

from datetime import datetime, timezone
import itertools
import json

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.timestamp_pb2 import Timestamp
import pytest

from udb.core.apikey.events.v1 import apikey_events_pb2
from udb.core.common.v1 import db_pb2
from udb.core.authn.entity.v1 import device_pb2
from udb.core.storage.entity.v1 import file_pb2
from udb_client import (
    WRITE_INSERT,
    WRITE_UPDATE,
    decode_field,
    decode_record,
    encode_event,
    encode_field,
    encode_record,
    format_timestamp,
    message_type,
    parse_timestamp,
    primary_key,
    project_column,
    tenant_column,
)
from udb_client.record_codec import _array_literal

CODEC_TENANT = "0190f1b2-0000-7000-8000-0000000000aa"
File = file_pb2.File


def _timestamp(text: str) -> Timestamp:
    stamp = Timestamp()
    stamp.FromJsonString(text)
    return stamp


def test_encode_record_follows_column_annotations() -> None:
    file = File(
        tenant_id=CODEC_TENANT,
        filename="notes.pdf",
        status="FILE_STATUS_PENDING",
        file_type="FILE_TYPE_PDF",
        expires_at=_timestamp("2026-10-07T09:30:00Z"),
    )
    record = encode_record(file, WRITE_INSERT)
    assert "file_id" not in record, "an empty NOT NULL UUID key is dropped so its default applies"
    assert "project_id" in record and record["project_id"] is None, "an empty nullable column is NULL"
    assert record["status"] == "PENDING" and record["file_type"] == "PDF", "enums in VARCHAR are short tokens"
    assert record["expires_at"] == "2026-10-07T09:30:00Z"
    assert record["tenant_id"] == CODEC_TENANT
    assert "audit_info" not in record, "an unset message in a NOT NULL column is dropped"
    assert record["size_bytes"] == 0 and isinstance(record["size_bytes"], int), "64-bit integers are exact numbers"

    file.project_id = "owned-project"
    update = encode_record(file, WRITE_UPDATE)
    for owned in ("file_id", "tenant_id", "project_id"):
        assert owned not in update, f"an update must not carry {owned}"


def test_decode_record_accepts_every_stored_shape() -> None:
    row = {
        "file_id": "0190f1b2-0000-7000-8000-000000000001",
        "tenant_id": CODEC_TENANT,
        "filename": "notes.pdf",
        "status": "PENDING",
        "file_type": "FILE_TYPE_PDF",
        "scan_verdict": "1",
        "is_public": "true",
        "expires_at": "2026-10-07T09:30:00Z",
        "not_a_column": "ignored",
    }
    file = decode_record(row, File())
    assert file.status == File.DESCRIPTOR.fields_by_name["status"].enum_type.values_by_name["FILE_STATUS_PENDING"].number
    assert file.file_type == File.DESCRIPTOR.fields_by_name["file_type"].enum_type.values_by_name["FILE_TYPE_PDF"].number
    assert file.is_public is True
    assert file.expires_at.ToJsonString() == "2026-10-07T09:30:00Z"


def test_decode_record_refuses_an_unknown_enum_instead_of_decoding_zero() -> None:
    with pytest.raises(ValueError, match="udb: decode"):
        decode_record({"status": "NOT_A_STATUS"}, File())


def test_decode_record_replaces_what_the_message_held() -> None:
    file = File(filename="old.pdf", checksum="keep-me-not")
    decode_record({"filename": "new.pdf"}, file)
    assert file.filename == "new.pdf" and file.checksum == ""


def test_record_codec_round_trips() -> None:
    original = File(
        file_id="0190f1b2-0000-7000-8000-000000000002",
        tenant_id=CODEC_TENANT,
        filename="a.txt",
        status="FILE_STATUS_PENDING",
        is_public=True,
    )
    back = json.loads(json.dumps(encode_record(original, WRITE_INSERT)))
    out = decode_record(back, File())
    assert (out.file_id, out.filename, out.status, out.is_public) == (
        original.file_id, original.filename, original.status, original.is_public,
    )


def test_schema_accessors_read_the_annotations() -> None:
    assert message_type(File()) == "udb.core.storage.entity.v1.File"
    assert primary_key(File()) == "file_id"
    assert tenant_column(File) == "tenant_id"
    assert project_column(File) == "project_id"


def test_encode_and_decode_field_match_the_record_codec() -> None:
    file = File(tenant_id=CODEC_TENANT, status="FILE_STATUS_PENDING")
    assert encode_field(file, "status") == ("PENDING", True)
    assert encode_field(file, "project_id") == (None, True), "an empty nullable column encodes as NULL"
    with pytest.raises(ValueError):
        encode_field(file, "nope")
    out = File()
    decode_field(out, "status", "FILE_STATUS_ACTIVE")
    decode_field(out, "file_type", "PDF")
    status = out.status
    assert status == File.DESCRIPTOR.fields_by_name["status"].enum_type.values_by_name["FILE_STATUS_ACTIVE"].number
    assert out.file_type == File.DESCRIPTOR.fields_by_name["file_type"].enum_type.values_by_name["FILE_TYPE_PDF"].number
    decode_field(out, "status", None)
    assert out.status == status, "NULL leaves the field unchanged"


_PRESENCE_SEQUENCE = itertools.count()


def _presence_message(text_type: str):
    """The Go test's dynamic message: optional and oneof text columns and a plain one."""

    def column(sql_type: str, target: descriptor_pb2.FieldDescriptorProto) -> None:
        target.options.Extensions[db_pb2.pg_column].sql_type = sql_type

    proto = descriptor_pb2.FileDescriptorProto(
        name=f"udb_codec_presence_test_{next(_PRESENCE_SEQUENCE)}.proto",
        package=f"udb.test.codec.p{next(_PRESENCE_SEQUENCE)}",
        syntax="proto3",
        dependency=[db_pb2.DESCRIPTOR.name],
    )
    message = proto.message_type.add(name="Row")
    message.oneof_decl.add(name="choice")
    message.oneof_decl.add(name="_note")
    string, int64, optional = (
        descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    field = message.field.add(name="note", number=1, label=optional, type=string, oneof_index=1, proto3_optional=True)
    column(text_type, field)
    field = message.field.add(name="text_choice", number=2, label=optional, type=string, oneof_index=0)
    column(text_type, field)
    field = message.field.add(name="count_choice", number=3, label=optional, type=int64, oneof_index=0)
    column("BIGINT", field)
    field = message.field.add(name="plain", number=4, label=optional, type=string)
    column("TEXT", field)
    pool = descriptor_pool.Default()
    pool.Add(proto)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{proto.package}.Row"))


@pytest.mark.parametrize("mode", [WRITE_INSERT, WRITE_UPDATE])
@pytest.mark.parametrize("sql_type", ["TEXT", "VARCHAR(64)", "CHAR(8)", "UUID", "DATE"])
@pytest.mark.parametrize("name", ["note", "text_choice"])
def test_record_codec_preserves_present_empty_strings(mode, sql_type: str, name: str) -> None:
    Row = _presence_message(sql_type)
    message = Row()
    absent = encode_record(message, mode)
    assert name in absent and absent[name] is None, "unset encodes as NULL"
    setattr(message, name, "")
    record = encode_record(message, mode)
    assert record[name] == "", "present empty stays empty"
    assert encode_field(message, name) == ("", True), "the field encoder keeps presence"
    assert "plain" in record and record["plain"] is None, "ordinary empty nullable text keeps its NULL shape"
    decoded = decode_record(record, Row())
    assert decoded.HasField(name) and getattr(decoded, name) == "", "round trip keeps presence"


def test_record_codec_refuses_present_empty_json() -> None:
    message = _presence_message("JSONB")()
    message.note = ""
    for mode in (WRITE_INSERT, WRITE_UPDATE):
        with pytest.raises(ValueError, match="not JSON"):
            encode_record(message, mode)
    with pytest.raises(ValueError, match="not JSON"):
        encode_field(message, "note")


@pytest.mark.parametrize("first", ["text_choice", "count_choice"])
def test_decode_field_refuses_conflicting_oneof_without_changing_message(first: str) -> None:
    second = "text_choice" if first == "count_choice" else "count_choice"
    values = {"text_choice": "", "count_choice": 7}
    message = _presence_message("TEXT")()
    decode_field(message, first, values[first])
    before = message.SerializeToString()
    with pytest.raises(ValueError, match="oneof choice"):
        decode_field(message, second, values[second])
    assert message.SerializeToString() == before, "the refused column replaced the oneof member"
    decode_field(message, second, None)
    assert message.SerializeToString() == before, "a NULL alternative keeps the existing member"
    decode_field(message, first, values[first])
    assert message.SerializeToString() == before, "the same member stays writable"


def test_decode_coerces_text_shapes_like_go() -> None:
    # A string field whose column came back as JSON gets that value's JSON text, with
    # sorted keys and Go's escapes; CHAR padding is trimmed; DATE keeps its date.
    Row = _presence_message("CHAR(8)")
    assert decode_record({"plain": {"b": "<", "a": 1}}, Row()).plain == '{"a":1,"b":"\\u003c"}'
    assert decode_record({"plain": True}, Row()).plain == "true"
    assert decode_record({"note": "ab   "}, Row()).note == "ab"
    assert decode_record({"note": "2026-10-07T00:00:00Z"}, _presence_message("DATE")()).note == "2026-10-07"


def test_array_literals_like_go() -> None:
    string = File.DESCRIPTOR.fields_by_name["filename"]
    integer = File.DESCRIPTOR.fields_by_name["size_bytes"]
    assert _array_literal('{a,"b, c",d}', string) == ["a", "b, c", "d"]
    assert _array_literal("{1,2}", integer) == [1, 2]
    assert _array_literal("{}", string) == []
    with pytest.raises(ValueError, match="holds NULL"):
        _array_literal("{a,NULL}", string)
    assert _array_literal('{a,"NULL"}', string) == ["a", "NULL"], "a quoted NULL is text"
    with pytest.raises(ValueError, match="nested arrays"):
        _array_literal("{{1}}", integer)
    with pytest.raises(ValueError, match="not an array literal"):
        _array_literal("1,2", integer)


def test_encode_event_uses_names_and_needs_a_topic() -> None:
    event = apikey_events_pb2.ApiKeyCreatedEvent(key_id="k1", tenant_id="t1")
    assert encode_event(event) == {"key_id": "k1", "tenant_id": "t1"}, "proto names, no unpopulated fields"
    with pytest.raises(ValueError, match="declares no outbox topic"):
        encode_event(File())


def test_timestamps_have_one_text_form() -> None:
    instant = datetime(2026, 10, 15, 8, 20, 0, 800000, tzinfo=timezone.utc)
    assert format_timestamp(instant) == "2026-10-15T08:20:00.800000Z"
    assert parse_timestamp("2026-10-15T08:20:00.8Z") == instant
    assert parse_timestamp("2026-10-15T10:20:00.800000+02:00") == instant
    assert parse_timestamp("") is None
    with pytest.raises(ValueError):
        parse_timestamp("yesterday")


def test_a_masked_column_is_no_value_and_stays_unset_on_decode() -> None:
    file = decode_record(
        {"file_id": "f", "size_bytes": "***MASKED***", "filename": "***MASKED***"}, File()
    )
    assert file.file_id == "f"
    assert file.size_bytes == 0
    # An unprotected text column may really hold the text.
    assert file.filename == "***MASKED***"
    device = decode_record({"last_ip_masked": "***MASKED***"}, device_pb2.Device())
    assert device.last_ip_masked == ""
