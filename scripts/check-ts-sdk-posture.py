#!/usr/bin/env python3
"""Fail CI if the TypeScript simple-client SDK posture drifts."""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SourceCheck:
    label: str
    path: str
    required: tuple[str, ...]
    forbidden: tuple[str, ...] = ()


CHECKS: tuple[SourceCheck, ...] = (
    SourceCheck(
        "typescript generated-client template error details",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            'const ERROR_DETAIL_TRAILER = "udb-error-detail-bin";',
            "function decodeErrorDetailBytes(bytes: Buffer): UdbErrorDetail",
            "retryable?: boolean;",
            "kind?: number;",
            "kindName?: string;",
            "get retryable(): boolean",
            "get kind(): number | undefined",
            "get kindName(): string | undefined",
            "detail.kindName = ERROR_KIND_NAMES[value];",
            "async unary<TRes = any>(",
            "request: any,",
            'import type * as Msg from "./messages";',
            "type KnownRequestMessages",
            "SelectRequest: Msg.SelectRequest;",
            "type RpcInput<Name extends string>",
            'RpcInput<"{{RPC_INPUT}}">',
            '<TRes = RpcOutput<"{{RPC_OUTPUT}}">',
        ),
        forbidden=("udb-error-reason", "udb-error-code"),
    ),
    SourceCheck(
        "typescript generated-client emitted error details",
        "sdk/typescript/generatedClient.ts",
        (
            'const ERROR_DETAIL_TRAILER = "udb-error-detail-bin";',
            "function decodeErrorDetailBytes(bytes: Buffer): UdbErrorDetail",
            "retryable?: boolean;",
            "kind?: number;",
            "kindName?: string;",
            "get retryable(): boolean",
            "get kind(): number | undefined",
            "get kindName(): string | undefined",
            'import type * as Msg from "./messages";',
            "type KnownRequestMessages",
            "SelectRequest: Msg.SelectRequest;",
            "type RpcInput<Name extends string>",
            'select<TRes = RpcOutput<"RecordSet">>(request: RpcInput<"SelectRequest">',
            'register_upload<TRes = RpcOutput<"RegisterUploadResponse">>(request: RpcInput<"RegisterUploadRequest">',
        ),
        forbidden=("udb-error-reason", "udb-error-code"),
    ),
    SourceCheck(
        "typescript generated entity registry template",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            "export interface EntityBinding",
            "primaryKeys: string[];",
            "tenantField?: string;",
            "projectField?: string;",
            "export const ENTITY_REGISTRY",
            "primaryKeys: [{{ENTITY_PRIMARY_KEYS}}]",
            'tenantField: "{{ENTITY_TENANT_FIELD}}"',
            'projectField: "{{ENTITY_PROJECT_FIELD}}"',
        ),
    ),
    SourceCheck(
        "typescript messages surface",
        "sdk/typescript/messages.ts",
        (
            "export interface SelectRequest",
            "message_type?: string;",
            "limit?: number;",
            "export interface UpsertRequest",
            "record_json?: Bytes;",
            "conflict_fields?: string[];",
            "write_receipt_json?: string;",
            "export interface RegisterUploadRequest",
            "export interface LoginRequest",
            "export interface AuthenticateResponse",
            "export interface MigrationStatusResponse",
            "approval_token?: string;",
            "[k: string]: unknown;",
        ),
    ),
    SourceCheck(
        "typescript consistency helpers",
        "sdk/typescript/consistency.ts",
        (
            "export interface WriteReceipt",
            "source_lsn: string;",
            "projection_task_ids: string[];",
            "export interface ReadFence",
            "min_outbox_lsn?: string;",
            "export function readFenceFromReceipt",
            "min_outbox_lsn: r.source_lsn ? r.source_lsn : undefined",
            "export function parseWriteReceipt",
            "export function receiptFromResponse",
            "const json = resp.write_receipt_json;",
            'return { headers: { "x-udb-read-fence": JSON.stringify(fence) } };',
            "export function afterWrite",
        ),
    ),
    SourceCheck(
        "typescript workflow facade helpers",
        "sdk/typescript/project.ts",
        (
            "async uploadFile(",
            "RegisterUpload",
            "FinalizeUpload",
            "invalid upload_url",
            "admin = new AdminFacade",
            "async applyCurrent(",
            "approveResp?.approval_token",
            "entity<T = any>(",
            "table<T = any>(",
            "async loginAndAdoptTenant(",
            "AuthenticateBearer",
            "if (tenantId) this.setTenant(tenantId);",
        ),
    ),
    SourceCheck(
        "typescript bound entity helper",
        "sdk/typescript/entity.ts",
        (
            "export class EntityHandle",
            "async upsert(",
            '"Upsert"',
            "conflict_fields: this.opts.key",
            "async select(",
            '"Select"',
            "async delete(",
            '"Delete"',
            "record_json: jsonBytes(record)",
        ),
    ),
    SourceCheck(
        "typescript stream helpers",
        "sdk/typescript/stream.ts",
        (
            "export async function sendOneClientStream",
            "stream.write(msg);",
            "stream.end();",
            "return response;",
            "export function sendOneBidiAwaitFirst",
            'duplex.on("data"',
            'duplex.on("error"',
        ),
    ),
    SourceCheck(
        "typescript package exports and compile surface",
        "sdk/typescript/index.ts",
        (
            'export * from "./messages";',
            'export * from "./consistency";',
            'export * from "./stream";',
            'export * from "./entity";',
        ),
    ),
    SourceCheck(
        "typescript tsconfig compile surface",
        "sdk/typescript/tsconfig.build.json",
        (
            '"messages.ts"',
            '"consistency.ts"',
            '"stream.ts"',
            '"entity.ts"',
            '"project.ts"',
        ),
    ),
    SourceCheck(
        "typescript sdk helper tests",
        "sdk/typescript/sdkhelpers.test.ts",
        (
            "udb-error-detail-bin",
            "the real decoder reads retryable/kind/capability_required",
            "UdbError exposes kind / kindName / retryable",
            "parseWriteReceipt parses the golden write_receipt",
            "readFenceFromReceipt maps source_lsn->min_outbox_lsn",
            "receiptFromResponse reads write_receipt_json",
            "withReadFence omits empty fields and sets the x-udb-read-fence header",
            "sendOneClientStream writes exactly one message",
            "sendOneBidiAwaitFirst resolves on the first data",
        ),
    ),
    SourceCheck(
        "typescript facade sequence tests",
        "sdk/typescript/facade.test.ts",
        (
            "StorageFacade.uploadFile = RegisterUpload + PUT + FinalizeUpload",
            "workflow-sequences.md",
            "EntityHandle.upsert = one Upsert; select = one Select",
            "loginAndAdoptTenant = [Login, AuthenticateBearer]",
            'assert.deepEqual(calls, ["Login", "AuthenticateBearer"]);',
            "EntityHandle.select accepts the contract { where } form",
        ),
    ),
)


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for check in CHECKS:
        path = root / check.path
        if not path.is_file():
            failures.append(f"{check.label}: missing file {check.path}")
            continue
        text = path.read_text(encoding="utf-8")
        for token in check.required:
            if token not in text:
                failures.append(f"{check.label}: missing token {token!r} in {check.path}")
        for token in check.forbidden:
            if token in text:
                failures.append(f"{check.label}: forbidden stale token {token!r} in {check.path}")
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        contents_by_path: dict[str, list[str]] = {}
        for check in CHECKS:
            contents_by_path.setdefault(check.path, []).extend(check.required)

        for rel_path, tokens in contents_by_path.items():
            path = root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(dict.fromkeys(tokens)) + "\n", encoding="utf-8")

        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        generated = root / "sdk-templates/typescript/generatedClient.ts.tmpl"
        generated.write_text(generated.read_text(encoding="utf-8") + "\nudb-error-reason\n", encoding="utf-8")
        failures = check_source(root)
        if not any("forbidden stale token 'udb-error-reason'" in failure for failure in failures):
            raise AssertionError(f"expected stale string-trailer failure, got {failures}")

        generated.write_text(generated.read_text(encoding="utf-8").replace("type RpcInput<Name extends string>", ""), encoding="utf-8")
        failures = check_source(root)
        if not any("type RpcInput" in failure for failure in failures):
            raise AssertionError(f"expected generated typed signature failure, got {failures}")

    print("TypeScript SDK posture selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = check_source()
    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        return 1
    print("TypeScript SDK posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
