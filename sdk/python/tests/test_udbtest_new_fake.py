"""``udbtest.new_fake``: tables from a descriptor set, judged by a policy file."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any
import uuid

from google.protobuf import descriptor_pb2
import pytest

from udb.core.storage.entity.v1 import file_pb2
from udb_client import ErrorCode, UdbRpcError, inspect
from udb_client import udbtest

FILE = "udb.core.storage.entity.v1.File"


def descriptor_set(*messages: Any) -> descriptor_pb2.FileDescriptorSet:
    """The set ``--include-imports`` emits, with importers before their imports, so
    the order cannot be what makes it load."""
    files: dict[str, descriptor_pb2.FileDescriptorProto] = {}

    def visit(file: Any) -> None:
        if file.name in files:
            return
        proto = descriptor_pb2.FileDescriptorProto()
        file.CopyToProto(proto)
        files[file.name] = proto
        for dependency in file.dependencies:
            visit(dependency)

    for message in messages:
        visit(message.DESCRIPTOR.file)
    return descriptor_pb2.FileDescriptorSet(file=list(files.values()))


def policy_file(tmp_path: Path, text: str) -> str:
    path = tmp_path / "policy.yaml"
    path.write_text(text, encoding="utf-8")
    return str(path)


def row() -> dict[str, Any]:
    file_id = str(uuid.uuid4())
    return {"file_id": file_id, "filename": "a.pdf", "object_key": f"k/{file_id}"}


def test_no_policy_file_denies_data_requests() -> None:
    with udbtest.new_fake(descriptor_set(file_pb2.File)) as fake:
        client = fake.client(str(uuid.uuid4()))
        for _ in range(2):
            with pytest.raises(UdbRpcError) as refused:
                client.entity(FILE).select()
            info = inspect(refused.value)
            assert info.code is ErrorCode.POLICY_DENIED and info.missing["rule"] == f"Select {FILE}"


def test_policy_file_allows_and_a_deny_wins(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    path = policy_file(
        tmp_path,
        """
tenant: "*"
policies:
  - {id: read, effect: allow, action: "*", resource: "udb.core.storage.*"}
  - {id: no-delete, effect: deny, action: Delete, resource: "*", required_scopes: [not-held]}
  - {id: off, enabled: false, effect: deny, action: Select, resource: "*"}
  - {id: scoped, effect: allow, action: Upsert, resource: "other.*", required_scopes: [not-held]}
""",
    )
    with udbtest.new_fake(descriptor_set(file_pb2.File), path) as fake:
        tenant = str(uuid.uuid4())
        entity = fake.client(tenant).entity(FILE, key=["file_id"])
        stored = row()
        for _ in range(2):
            entity.upsert(stored)
        rows = entity.select(where={"file_id": stored["file_id"]})
        assert rows[0]["status"] == "PENDING", "a generated message class still applies column defaults"
        assert fake.rows(FILE, tenant)[0]["tenant_id"] == tenant
        with pytest.raises(UdbRpcError) as denied:
            entity.delete(where={"file_id": stored["file_id"]})
        assert inspect(denied.value).code is ErrorCode.POLICY_DENIED


def test_document_tenant_applies_only_to_rules_that_name_none(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    path = policy_file(
        tmp_path,
        "tenant: acme\npolicies:\n  - {id: a, effect: allow}\n  - {id: b, effect: allow, tenant: other}\n",
    )
    assert [policy.tenant for policy in udbtest.load_policies(path)] == ["acme", "other"]


def test_a_bare_list_is_a_policy_document(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    path = policy_file(tmp_path, '[{"id": "a", "effect": "allow", "priority": 3}]')
    policy = udbtest.load_policies(path)[0]
    assert (policy.id, policy.effect, policy.priority) == ("a", "allow", 3)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("policies: [unterminated", "parse policy file"),
        ("", "empty policy document"),
        ("tenant: acme\n", "requires policies"),
        ("just text\n", "expected a policy list"),
        ("- {id: a}\n---\n- {id: b}\n", "one document"),
        ("- {id: a, effect: maybe}\n", "invalid effect"),
        ("- not-a-mapping\n", "must be a mapping"),
        ("- {id: a, priority: high}\n", "priority must be an integer"),
        ("- {id: a, enabled: perhaps}\n", "enabled must be a boolean"),
        ("- {id: a, required_scopes: one}\n", "required_scopes must be a list"),
        ("- {id: a, conditions: [x]}\n", "conditions must be a mapping"),
    ],
)
def test_malformed_policy_is_refused_and_nothing_serves(tmp_path: Path, text: str, message: str) -> None:
    pytest.importorskip("yaml")
    with pytest.raises(ValueError, match=message):
        udbtest.new_fake(descriptor_set(file_pb2.File), policy_file(tmp_path, text))


def test_missing_policy_file_is_refused(tmp_path: Path) -> None:
    pytest.importorskip("yaml")
    with pytest.raises(ValueError, match="policy file"):
        udbtest.load_policies(str(tmp_path / "absent.yaml"))


def test_absent_yaml_parser_names_the_extra(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "yaml", None)
    with pytest.raises(RuntimeError, match=r"udb-client\[test\]"):
        udbtest.new_fake(descriptor_set(file_pb2.File), policy_file(tmp_path, "[]\n"))


def test_descriptor_set_must_be_complete_and_declare_tables() -> None:
    with pytest.raises(ValueError, match="descriptor_set is required"):
        udbtest.new_fake(None)  # type: ignore[arg-type]
    complete = descriptor_set(file_pb2.File)
    incomplete = descriptor_pb2.FileDescriptorSet(
        file=[file for file in complete.file if file.name == file_pb2.DESCRIPTOR.name]
    )
    with pytest.raises(ValueError, match="which the set lacks"):
        udbtest.new_fake(incomplete)
    no_tables = descriptor_set(descriptor_pb2.FileDescriptorProto)
    with pytest.raises(ValueError, match="declares no tables"):
        udbtest.new_fake(no_tables)


def _predicate_columns(column: str) -> str | None:
    return {"retired_at": "retired_at", "deleted_at": "deleted_at", "kind": "kind", "label": "label"}.get(column)


def test_unique_predicate_conjunction_of_null_tests() -> None:
    from udb_client.udbtest.fake import _unique_predicate

    test = _unique_predicate('retired_at IS NULL and "Deleted_At" is null', _predicate_columns)
    assert test({"retired_at": None, "deleted_at": None}), "a live row takes part in the unique key"
    assert not test({"retired_at": "2026-01-01T00:00:00Z", "deleted_at": None}), "a retired row does not"
    assert not test({"retired_at": None, "deleted_at": "2026-01-01T00:00:00Z"}), "a deleted row does not"


def test_unique_predicate_literal_comparison() -> None:
    from udb_client.udbtest.fake import _unique_predicate

    test = _unique_predicate("kind = 'primary' AND deleted_at IS NULL", _predicate_columns)
    assert test({"kind": "primary"})
    assert not test({"kind": "secondary"}) and not test({"kind": None}), "another kind, or NULL, does not take part"
    assert not test({"kind": "primary", "deleted_at": "2026-01-01T00:00:00Z"})
    not_equal = _unique_predicate("label <> 'it''s and more'", _predicate_columns)
    assert not not_equal({"label": "it's and more"}) and not_equal({"label": "x"}) and not not_equal({"label": None})


@pytest.mark.parametrize(
    "where",
    [
        "retired_at IS NULL OR deleted_at IS NULL",
        "(deleted_at IS NULL)",
        "lower(kind) = 'primary'",
        "unknown_column IS NULL",
        "kind > 'a'",
    ],
)
def test_unique_predicate_refuses_what_it_cannot_read(where: str) -> None:
    from udb_client.udbtest.fake import _unique_predicate

    with pytest.raises(ValueError, match="unsupported unique-index predicate"):
        _unique_predicate(where, _predicate_columns)


def test_embedded_environment_carries_only_the_approval_plan_it_is_given() -> None:
    from udb_client.udbtest.embedded import _approval_plan_args, _embedded_environment

    source = {
        "PATH": "/bin", "UDB_PG_DSN": "postgres://other", "udb_grpc_addr": "x", "DATABASE_URL": "postgres://x",
        "UDB_HOME": "/h", "UDB_MIGRATION_APPROVAL_PLAN": "/stale.json",
    }
    assert _embedded_environment(source, "/proto") == {"PATH": "/bin", "UDB_HOME": "/h", "UDB_PROTO_ROOT": "/proto"}
    approved = _embedded_environment(source, "/proto", "/project/approval.json")
    assert approved["UDB_MIGRATION_APPROVAL_PLAN"] == "/project/approval.json"
    assert "UDB_PG_DSN" not in approved
    assert _approval_plan_args("/project/approval.json") == ["plan", "--emit-approval-plan", "/project/approval.json"]


_CHECKED = iter(range(1, 1000))


def _checked_file(enum_values: list[str]) -> Any:
    """A copy of File whose ``status`` column declares ``enum_values``, so its CHECK lists them."""
    from google.protobuf import descriptor_pool, message_factory

    from udb.core.common.v1 import db_pb2

    model = descriptor_pb2.FileDescriptorProto()
    file_pb2.File.DESCRIPTOR.file.CopyToProto(model)
    number = next(_CHECKED)
    model.name = f"checked/file_{number}.proto"
    model.package = f"checked.file{number}"
    entity = next(m for m in model.message_type if m.name == "File")
    del model.message_type[:]
    model.message_type.add().CopyFrom(entity)
    status = next(f for f in model.message_type[0].field if f.name == "status")
    status.options.Extensions[db_pb2.pg_column].enum_values.extend(enum_values)
    pool = descriptor_pool.Default()
    pool.Add(model)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(f"{model.package}.File"))


@pytest.mark.parametrize(
    ("declared", "stored"),
    [
        (["FILE_STATUS_PENDING", "FILE_STATUS_ACTIVE"], "FILE_STATUS_PENDING"),
        (["PENDING", "ACTIVE"], "PENDING"),
    ],
)
def test_text_enum_stores_the_declared_form_and_the_fake_enforces_the_check(declared, stored) -> None:
    from udb_client.record_codec import WRITE_INSERT, decode_record, encode_record
    from udb_client.table import udb_enum_to_text

    message = _checked_file(declared)
    full = message.DESCRIPTOR.fields_by_name["status"].enum_type.values_by_name
    # The writer: the declared form, whichever form the caller used; a value outside raises.
    assert encode_record(message(status=full["FILE_STATUS_PENDING"].number), WRITE_INSERT)["status"] == stored
    with pytest.raises(ValueError, match="breaks constraint chk_files_status_enum"):
        encode_record(message(status=full["FILE_STATUS_DELETED"].number), WRITE_INSERT)
    assert udb_enum_to_text("PENDING", "FILE_STATUS_", declared) == stored
    assert udb_enum_to_text("FILE_STATUS_PENDING", "FILE_STATUS_", declared) == stored
    with pytest.raises(ValueError, match="CHECK"):
        udb_enum_to_text("FILE_STATUS_DELETED", "FILE_STATUS_", declared)
    # The reader returns the full name from either stored form.
    for text in ("PENDING", "FILE_STATUS_PENDING"):
        decoded = message()
        decode_record({"status": text}, decoded)
        assert decoded.status == full["FILE_STATUS_PENDING"].number
    name = message.DESCRIPTOR.full_name
    with udbtest.new(message) as fake:
        tenant = str(uuid.uuid4())
        entity = fake.client(tenant).entity(name, key=["file_id"])
        for written in ("FILE_STATUS_PENDING", "PENDING"):
            record = row()
            entity.upsert({**record, "status": written})
            stored_row = next(r for r in fake.rows(name, tenant) if r["file_id"] == record["file_id"])
            assert stored_row["status"] == stored, written
        # Negative: a declared value outside the list, its short token, and junk break the CHECK.
        for bad in ("FILE_STATUS_DELETED", "DELETED", "NOT_A_STATUS"):
            with pytest.raises(UdbRpcError) as refused:
                entity.upsert({**row(), "status": bad})
            assert "breaks constraint chk_files_status_enum" in str(refused.value), bad


def test_a_text_enum_without_declared_values_keeps_the_short_token() -> None:
    from udb_client.record_codec import WRITE_INSERT, encode_record
    from udb_client.table import udb_enum_to_text

    assert encode_record(file_pb2.File(status="FILE_STATUS_PENDING"), WRITE_INSERT)["status"] == "PENDING"
    assert udb_enum_to_text("FILE_STATUS_PENDING", "FILE_STATUS_") == "PENDING"
