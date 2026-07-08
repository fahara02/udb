#!/usr/bin/env python3
"""Fail CI if the Python/PHP simple-client SDK posture drifts."""

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
        "python generated invoker and typed errors",
        "sdk/python/udb_client/generated_client.py",
        (
            "class UdbDetailedRpcError(UdbRpcError):",
            "self.error_detail = _decode_error_detail(detail)",
            "def is_retryable(self) -> bool:",
            "def kind(self) -> str:",
            "def invoke_unary(",
            "read_only: bool,",
            "rpc_path: str = \"\"",
        ),
    ),
    SourceCheck(
        "python project facade routes through generated invoker",
        "sdk/python/udb_client/project.py",
        (
            "from .generated_client import RPC_OPERATION_KIND, invoke_unary",
            "return invoke_unary(",
            "def upload_file(",
            "is_public: bool | None = None",
            "if is_public is not None:",
            "raise UdbConfigurationError(f\"upload_file:",
            "def login_and_adopt_tenant(",
            "authenticate_bearer(token",
            "self.bind_metadata(",
            "op_kind = RPC_OPERATION_KIND.get(",
        ),
    ),
    SourceCheck(
        "python bound entity and receipt helpers",
        "sdk/python/udb_client/client.py",
        (
            "def receipt_from_response",
            "write_receipt_json",
            "def entity(self, message_type: str",
            "def table(self, name: str",
            "class _BoundEntity:",
            "def upsert(",
            "conflict_fields=self._key",
        ),
    ),
    SourceCheck(
        "python typed consistency helpers",
        "sdk/python/udb_client/metadata.py",
        (
            "class WriteReceipt:",
            "class ReadFence:",
            "def from_receipt",
            "source_lsn",
            "min_outbox_lsn",
            "def after_write",
            "\"x-udb-read-fence\"",
        ),
    ),
    SourceCheck(
        "python package exports consistency and typed errors",
        "sdk/python/udb_client/__init__.py",
        (
            "ReadFence",
            "WriteReceipt",
            "UdbDetailedRpcError",
        ),
    ),
    SourceCheck(
        "python simple-client sequence tests",
        "sdk/python/tests/test_simple_client.py",
        (
            "test_upload_file_sequence_no_proof_reads",
            "test_entity_upsert_one_upsert_no_readback",
            "ReadFence.from_receipt(receipt",
            "test_register_upload_is_public_unset_by_default",
            "test_finalize_and_update_is_public_unset",
            "test_login_and_adopt_tenant_two_rpcs",
        ),
    ),
    SourceCheck(
        "php project facade retry and login/adopt",
        "sdk/php/src/UdbProject.php",
        (
            "public function loginAndAdoptTenant(",
            "authenticateBearer($token",
            "public function invoke(",
            "GeneratedClient::OPERATION_KIND",
            "isRetryableStatus",
            "UdbRpcException::fromGrpcStatus",
        ),
    ),
    SourceCheck(
        "php typed RPC exception",
        "sdk/php/src/Exceptions/UdbRpcException.php",
        (
            "public ?object $errorDetail = null;",
            "public function isRetryable(): bool",
            "public function kind(): string",
            "public static function fromGrpcStatus",
            "udb-error-detail-bin",
        ),
    ),
    SourceCheck(
        "php storage upload and optional isPublic",
        "sdk/php/src/Services/StorageService.php",
        (
            "public function uploadFile(",
            "?callable $httpPut = null",
            "?bool $isPublic = null",
            "if ($isPublic !== null)",
            "RegisterUpload returned no upload_url",
            "finalizeUpload(",
        ),
    ),
    SourceCheck(
        "php bound entity helpers",
        "sdk/php/src/UdbClient.php",
        (
            "public function entity(string $messageType",
            "public function table(string $name",
            "public static function toStruct(",
        ),
    ),
    SourceCheck(
        "php bound entity implementation",
        "sdk/php/src/BoundEntity.php",
        (
            "final class BoundEntity",
            "public function select(",
            "public function upsert(",
            "public function delete(",
            "setConflictFields",
            "UdbClient::toStruct",
        ),
    ),
    SourceCheck(
        "php typed consistency metadata",
        "sdk/php/src/UdbMetadata.php",
        (
            "final class UdbMetadata",
            "'x-udb-read-fence'",
            "public function withReadFence(",
            "public function afterWrite(",
        ),
    ),
    SourceCheck(
        "php write receipt value object",
        "sdk/php/src/WriteReceipt.php",
        (
            "final class WriteReceipt",
            "public static function fromJson",
            "source_lsn",
        ),
    ),
    SourceCheck(
        "php read fence value object",
        "sdk/php/src/ReadFence.php",
        (
            "final class ReadFence",
            "public static function fromReceipt",
            "min_outbox_lsn",
            "projection_task_ids",
        ),
    ),
    SourceCheck(
        "php consistency helper functions",
        "sdk/php/src/functions.php",
        (
            "function readFenceFromReceipt(",
            "readFenceFromReceipt(",
        ),
    ),
    SourceCheck(
        "php simple-client tests",
        "sdk/php/tests/Unit/SimpleClientTest.php",
        (
            "maps a WriteReceipt into a ReadFence",
            "carries a settable errorDetail",
            "isRetryable()",
            "afterWrite installs",
        ),
    ),
    SourceCheck(
        "php bench manifest consumer",
        "sdk/php/tests/Live/GeneratedRpcSurfaceTest.php",
        (
            "function phpBenchBodyRows(): array",
            "docs/generated/bench-bodies.json",
            "function phpBenchBodyMarkdownRows(): array",
            "basename($path) === 'workflow-sequences.md'",
            "expect(count(phpBenchBodyRows()))->toBe(count(phpLiveRpcCatalog()))",
            "expect(count($fromJson))->toBe($expected)",
            "expect(count($fromMd))->toBe($expected)",
            "manifest carries representative service-qualified RPC keys",
            "DataBroker.Delete",
            "CacheService.Delete",
            "PeerService.JoinSession",
            "perfBodyPhp returns the manifest-documented typed request body",
            "$mkBody = fn () => perfBodyPhp(",
        ),
        forbidden=(
            "asserts 265 manifest rows",
            "count == 262",
            "toBe(262)",
            "toBe(265)",
            "exactly 262",
            "exactly 265",
            "?? requestFor($method)",
        ),
    ),
    SourceCheck(
        "php upload sequence tests",
        "sdk/php/tests/Unit/MediaServiceWiringTest.php",
        (
            "StorageFacade.uploadFile",
            "uploadFile() missing injectable $httpPut seam",
            "RegisterUpload",
            "FinalizeUpload",
        ),
    ),
    SourceCheck(
        "php naming aliases tests",
        "sdk/php/tests/Unit/NamingContractAliasesTest.php",
        (
            "readFenceFromReceipt maps source_lsn",
            "metadata->afterWrite",
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

        project = root / "sdk/python/udb_client/project.py"
        project.write_text(
            project.read_text(encoding="utf-8").replace("def upload_file(", "def upload_file_removed("),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("def upload_file(" in failure for failure in failures):
            raise AssertionError(f"expected Python upload_file drift failure, got {failures}")

        storage = root / "sdk/php/src/Services/StorageService.php"
        storage.write_text(
            storage.read_text(encoding="utf-8").replace("?bool $isPublic = null", "bool $isPublic = false"),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("?bool $isPublic = null" in failure for failure in failures):
            raise AssertionError(f"expected PHP optional isPublic drift failure, got {failures}")

    print("Python/PHP SDK posture selftest passed")
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
    print("Python/PHP SDK posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
