#!/usr/bin/env python3
"""Fail CI if generated ORM template invariants drift."""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class TemplatePosture:
    label: str
    path: str
    required: tuple[str, ...]


TEMPLATES: tuple[TemplatePosture, ...] = (
    TemplatePosture(
        "go",
        "sdk-templates/go/udbclient/generated_client.go.tmpl",
        (
            "PrimaryKeys: []string{{{ENTITY_PRIMARY_KEYS}}}",
            "UpdateOnConflict(updateFields, r.Descriptor.PrimaryKeys...)",
            "if len(d.PrimaryKeys) == 0",
            "descriptor.TenantField, descriptor.ProjectField",
            "for _, field := range descriptor.PrimaryKeys",
            "RequireTransactionalBackend",
            "beginTxFullMethod",
            "ToRequest builds the GenericDispatchRequest",
            "no tenant/project/context is set",
        ),
    ),
    TemplatePosture(
        "typescript",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            "primaryKeys: [{{ENTITY_PRIMARY_KEYS}}]",
            "if (!binding.primaryKeys || binding.primaryKeys.length === 0)",
            ".updateOnConflict(updateFields, this.binding.primaryKeys)",
            "[binding.tenantField, binding.projectField]",
            "binding.primaryKeys.map",
            "requireTransactionalBackend",
            "begin_tx",
            "GenericDispatchRequest body",
            "no tenant/project/context is set",
        ),
    ),
    TemplatePosture(
        "python",
        "sdk-templates/python/udb_client/generated_client.py.tmpl",
        (
            "primary_keys=({{ENTITY_PRIMARY_KEYS}},)",
            "if not binding.primary_keys:",
            ".update_on_conflict(update_fields, self.binding.primary_keys)",
            "(binding.tenant_field, binding.project_field)",
            "for field_name in binding.primary_keys",
            "require_transactional_backend",
            "begin_tx",
            "no tenant/project/context set",
        ),
    ),
    TemplatePosture(
        "php",
        "sdk-templates/php/src/Generated/GeneratedClient.php.tmpl",
        (
            "'primary_keys' => [{{ENTITY_PRIMARY_KEYS}}]",
            "($this->binding['primary_keys'] ?? []) === []",
            "->updateOnConflict($updateFields, $this->binding['primary_keys'])",
            "$binding['tenant_field']",
            "$binding['project_field']",
            "foreach ($binding['primary_keys'] as $field)",
            "requireTransactionalBackend",
            "BeginTx",
        ),
    ),
    TemplatePosture(
        "java",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl",
        (
            "List<String> primaryKeys",
            "if (binding.primaryKeys().isEmpty())",
            ".updateOnConflict(updateFields, binding.primaryKeys())",
            "binding.tenantField()",
            "binding.projectField()",
            "for (String field : binding.primaryKeys())",
            "requireTransactionalBackend",
            "BeginTx",
            "no tenant/project/context set",
        ),
    ),
    TemplatePosture(
        "csharp",
        "sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl",
        (
            "IReadOnlyList<string> PrimaryKeys",
            "if (Binding.PrimaryKeys.Count == 0)",
            ".UpdateOnConflict(updateFields, Binding.PrimaryKeys)",
            "binding.TenantField, binding.ProjectField",
            "foreach (var field in binding.PrimaryKeys)",
            "RequireTransactionalBackend",
            "BeginTx",
        ),
    ),
)


FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)update_?on_?conflict[^\n]{0,100}['\"]id['\"]"),
    re.compile(r"(?i)conflict_(?:on|fields)[^\n]{0,100}['\"]id['\"]"),
    re.compile(r"\bbinding\.key\b"),
    re.compile(r"\bthis\.binding\.key\b"),
)


def read_template(template: TemplatePosture, root: Path = ROOT) -> str:
    path = root / template.path
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise AssertionError(f"{template.label}: missing template {template.path}") from exc


def assert_template(template: TemplatePosture, root: Path = ROOT) -> list[str]:
    text = read_template(template, root)
    failures: list[str] = []
    for needle in template.required:
        if needle not in text:
            failures.append(f"{template.label}: missing required token: {needle}")
    for pattern in FORBIDDEN_PATTERNS:
        if pattern.search(text):
            failures.append(f"{template.label}: forbidden hardcoded/legacy conflict token: {pattern.pattern}")
    return failures


def assert_ci_mentions_guard(root: Path = ROOT) -> list[str]:
    ci = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    failures: list[str] = []
    if "check-orm-template-posture.py" not in ci:
        failures.append("ci: quick-gate does not run check-orm-template-posture.py")
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        template = TemplatePosture("demo", "demo.tmpl", ("required token",))
        (root / "demo.tmpl").write_text("required token\n", encoding="utf-8")
        failures = assert_template(template, root)
        if failures:
            raise AssertionError(f"expected clean template, got {failures}")

        (root / "demo.tmpl").write_text("missing\n", encoding="utf-8")
        failures = assert_template(template, root)
        if not failures or "missing required token" not in failures[0]:
            raise AssertionError(f"expected missing-token failure, got {failures}")

        (root / "demo.tmpl").write_text(
            "required token\nupdateOnConflict(fields, ['id'])\n",
            encoding="utf-8",
        )
        failures = assert_template(template, root)
        if not any("forbidden" in failure for failure in failures):
            raise AssertionError(f"expected forbidden-token failure, got {failures}")

    print("orm template posture selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo parser assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures: list[str] = []
    for template in TEMPLATES:
        failures.extend(assert_template(template))
    failures.extend(assert_ci_mentions_guard())

    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        return 1

    print(f"orm template posture guard passed ({len(TEMPLATES)} templates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
