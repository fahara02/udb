#!/usr/bin/env python3
"""Fail CI if the six-language scaffold compile gate drifts."""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ScaffoldLanguage:
    label: str
    emitted_path: str
    compile_tokens: tuple[str, ...]
    setup_token: str


LANGUAGES: tuple[ScaffoldLanguage, ...] = (
    ScaffoldLanguage("go", "examples/go/client.go", ("go mod tidy", "go build ./..."), 'go: "true"'),
    ScaffoldLanguage("typescript", "examples/typescript/client.ts", ("npx --yes tsc", "examples/typescript/client.ts"), 'node: "true"'),
    ScaffoldLanguage("python", "examples/python/client.py", ("python -m py_compile client.py", "data_broker_pb2_grpc.DataBrokerStub"), 'python: "true"'),
    ScaffoldLanguage("csharp", "examples/csharp/Client.cs", ("dotnet build", "Udb.Client.csproj"), 'dotnet: "true"'),
    ScaffoldLanguage("java", "examples/java/Client.java", ("mvn -B -ntp compile", "sdk/java/gen"), 'java: "true"'),
    ScaffoldLanguage("php", "examples/php/client.php", ("composer install", "php -l client.php"), 'php: "true"'),
)


def _read(root: Path, path: str) -> str:
    return (root / path).read_text(encoding="utf-8")


def _require(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle not in text:
        failures.append(f"missing {label}: {needle}")


def _reject(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle in text:
        failures.append(f"forbidden {label}: {needle}")


def _job_block(ci: str, job_name: str) -> str:
    matches = list(re.finditer(r"^  [A-Za-z0-9_-]+:\s*$", ci, flags=re.MULTILINE))
    for index, match in enumerate(matches):
        if match.group(0).strip() == f"{job_name}:":
            end = matches[index + 1].start() if index + 1 < len(matches) else len(ci)
            return ci[match.start() : end]
    return ""


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    ci = _read(root, ".github/workflows/ci.yml")
    script = _read(root, "scripts/check-scaffold-compiles.sh")
    scaffold = _read(root, "src/cli/scaffold.rs")
    plan = _read(root, "UDB_MASTERPLAN_2026.md")
    job = _job_block(ci, "scaffold-compiles")

    _require(ci, "python3 scripts/check-scaffold-posture.py", "CI quick-gate scaffold posture guard", failures)
    _require(job, "needs: build-broker", "build-once broker dependency", failures)
    _require(job, "actions/download-artifact@v4", "broker artifact download", failures)
    _require(job, "name: udb-broker-debug", "broker artifact name", failures)
    _require(job, "chmod +x target/debug/udb", "broker executable bit", failures)
    _require(job, "UDB_BIN=target/debug/udb bash scripts/check-scaffold-compiles.sh", "artifact-backed scaffold compile command", failures)
    _reject(job, "cargo run", "scaffold CI cargo fallback", failures)

    _require(script, 'if [[ -n "${UDB_BIN:-}" ]]; then', "UDB_BIN fast path", failures)
    _require(script, 'UDB_INIT_DIR="$WORK" "$UDB_BIN" scaffold', "prebuilt scaffold invocation", failures)
    _require(script, "cargo run --quiet -- scaffold", "local fallback scaffold invocation", failures)
    _require(script, "OK: emitted Go, TypeScript, Python, C#, Java, and PHP scaffolds compile.", "all-language success line", failures)

    _require(scaffold, "pub(crate) fn scaffold_files()", "scaffold file source", failures)
    _require(scaffold, "scaffold_emits_examples_for_all_six_sdks", "six-language Rust source test", failures)
    for language in LANGUAGES:
        _require(scaffold, f'("{language.emitted_path}",', f"{language.label} emitted scaffold path", failures)
        _require(script, language.emitted_path, f"{language.label} compile script file check", failures)
        _require(job, language.setup_token, f"{language.label} toolchain setup", failures)
        for token in language.compile_tokens:
            _require(script, token, f"{language.label} compile command", failures)

    _require(plan, "scaffold posture guard", "masterplan scaffold guard note", failures)
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / ".github/workflows").mkdir(parents=True)
        (root / "scripts").mkdir()
        (root / "src/cli").mkdir(parents=True)
        ci = """
name: demo
jobs:
  quick-gate:
    steps:
      - run: python3 scripts/check-scaffold-posture.py
  scaffold-compiles:
    needs: build-broker
    steps:
      - uses: ./.github/actions/setup-sdk-toolchains
        with:
          node: "true"
          python: "true"
          go: "true"
          dotnet: "true"
          java: "true"
          php: "true"
      - uses: actions/download-artifact@v4
        with:
          name: udb-broker-debug
      - run: chmod +x target/debug/udb
      - run: UDB_BIN=target/debug/udb bash scripts/check-scaffold-compiles.sh
  versions:
    steps: []
"""
        script = """
if [[ -n "${UDB_BIN:-}" ]]; then
  UDB_INIT_DIR="$WORK" "$UDB_BIN" scaffold
else
  cargo run --quiet -- scaffold
fi
examples/go/client.go
examples/python/client.py
examples/typescript/client.ts
examples/csharp/Client.cs
examples/java/Client.java
examples/php/client.php
go mod tidy
go build ./...
npx --yes tsc examples/typescript/client.ts
python -m py_compile client.py
data_broker_pb2_grpc.DataBrokerStub
dotnet build Udb.Client.csproj
mvn -B -ntp compile
sdk/java/gen
composer install
php -l client.php
OK: emitted Go, TypeScript, Python, C#, Java, and PHP scaffolds compile.
"""
        scaffold = """
pub(crate) fn scaffold_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("examples/go/client.go", ""),
        ("examples/python/client.py", ""),
        ("examples/typescript/client.ts", ""),
        ("examples/csharp/Client.cs", ""),
        ("examples/java/Client.java", ""),
        ("examples/php/client.php", ""),
    ]
}
fn scaffold_emits_examples_for_all_six_sdks() {}
"""
        (root / ".github/workflows/ci.yml").write_text(ci, encoding="utf-8")
        (root / "scripts/check-scaffold-compiles.sh").write_text(script, encoding="utf-8")
        (root / "src/cli/scaffold.rs").write_text(scaffold, encoding="utf-8")
        (root / "UDB_MASTERPLAN_2026.md").write_text("scaffold posture guard\n", encoding="utf-8")
        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        (root / ".github/workflows/ci.yml").write_text(
            ci.replace("needs: build-broker", "runs-on: ubuntu-latest"),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("build-once broker dependency" in failure for failure in failures):
            raise AssertionError(f"expected missing-build-broker failure, got {failures}")

    print("scaffold posture selftest passed")
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
    print(f"scaffold posture guard passed ({len(LANGUAGES)} languages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
