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
