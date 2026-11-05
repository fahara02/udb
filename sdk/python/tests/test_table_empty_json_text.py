"""Which columns treat "" as unset in a row, matching the generated Go mapper."""

from types import SimpleNamespace

import pytest
from google.protobuf.descriptor import FieldDescriptor

from udb_client.table import _empty_json_text_is_unset


def _field(kind=FieldDescriptor.TYPE_STRING, presence=False):
    return SimpleNamespace(
        type=kind,
        has_presence=presence,
        label=FieldDescriptor.LABEL_OPTIONAL,
        is_repeated=False,
    )


def _column(not_null=False, jsonb=True):
    return SimpleNamespace(not_null=not_null, is_json=False, is_jsonb=jsonb, sql_type="")


def test_a_nullable_json_text_column_treats_empty_as_unset():
    assert _empty_json_text_is_unset(_field(), _column())


@pytest.mark.parametrize(
    "field, column",
    [
        (_field(), _column(not_null=True)),
        (_field(presence=True), _column()),
        (_field(), _column(jsonb=False)),
        (_field(kind=FieldDescriptor.TYPE_MESSAGE), _column()),
    ],
)
def test_every_other_column_keeps_empty_as_sent(field, column):
    assert not _empty_json_text_is_unset(field, column)
