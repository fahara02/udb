#!/usr/bin/env python3
"""urgent_fix #19: fail CI when a descriptor-known native service has NO generated
robustness client in a shipped SDK language.

The native contract (`docs/generated/udb-native-contract.json`) is the single
source of truth for which services exist. Each language's generated robustness
client must reference every one of them by its short service name; the audit found
Storage/Asset/IdP/WebRTC missing from all six languages even though the manifest
listed them. After `udb sdk generate`, this guard keeps them from drifting back out.

All six shipped SDK languages are required. A missing generated client is a Gate C
failure, not a skip, because source templates can advance while committed SDK
outputs silently remain stale.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Per-language generated-client source (the robustness layer, not raw buf stubs).
# Glob patterns - layouts differ: TS/Python/C#/Java ship a single generated
# client file; PHP/Go ship per-service generated client files.
CLIENTS = {
    "typescript": ["sdk/typescript/generatedClient.ts"],
    "python": ["sdk/python/udb_client/generated_client.py"],
    "csharp": ["sdk/csharp/Udb.Client/GeneratedClient.cs"],
    "java": ["sdk/java/src/main/java/dev/udb/client/generated/*.java"],
    "php": [
        "sdk/php/src/Generated/GeneratedClient.php",
        "sdk/php/gen/**/*Client.php",
    ],
    "go": ["sdk/go/**/*client*.go", "sdk/go/gen/**/*.go"],
}

@dataclass(frozen=True)
class ContractService:
    package: str
    name: str

    @property
    def full_name(self) -> str:
        return f"{self.package}.{self.name}"


def contract_services(root: Path = ROOT) -> list[ContractService]:
    contract = root / "docs" / "generated" / "udb-native-contract.json"
    data = json.loads(contract.read_text(encoding="utf-8"))
    services = []
    for svc in data.get("services", []):
        full = svc.get("service", "")
        if "." in full:
            package, name = full.rsplit(".", 1)
            services.append(ContractService(package=package, name=name))
    return sorted(set(services), key=lambda service: service.full_name)


def rel_package_path(service: ContractService) -> Path:
    return Path(*service.package.split("."))


def pascal_package_path(service: ContractService) -> Path:
    return Path(*(part[:1].upper() + part[1:] for part in service.package.split(".")))


def cached_text(file: Path, cache: dict[Path, str]) -> str:
    if file not in cache:
        cache[file] = file.read_text(encoding="utf-8", errors="ignore")
    return cache[file]


def files_containing(files: list[Path], needle: str, cache: dict[Path, str]) -> bool:
    for file in files:
        if needle in cached_text(file, cache):
            return True
    return False


def stub_artifacts(
    lang: str, service: ContractService, root: Path = ROOT
) -> tuple[list[Path], str]:
    rel = rel_package_path(service)
    if lang == "typescript":
        directory = root / "sdk" / "typescript" / "gen" / rel
        return sorted(directory.glob("*_pb.ts")), f"export const {service.name}: GenService"
    if lang == "python":
        directory = root / "sdk" / "python" / "gen" / rel
        return sorted(directory.glob("*_pb2_grpc.py")), f"class {service.name}Stub"
    if lang == "csharp":
        directory = root / "sdk" / "csharp" / "gen"
        return sorted(directory.glob("**/*Grpc.cs")), f'__ServiceName = "{service.full_name}"'
    if lang == "java":
        directory = root / "sdk" / "java" / "gen" / "com" / rel
        return sorted(directory.glob("*.java")), f'SERVICE_NAME = "{service.full_name}"'
    if lang == "php":
        path = root / "sdk" / "php" / "gen" / pascal_package_path(service) / f"{service.name}Client.php"
        return [path] if path.exists() else [], f"class {service.name}Client"
    if lang == "go":
        directory = root / "sdk" / "go" / "gen" / rel
        return sorted(directory.glob("*.go")), f"New{service.name}Client"
    return [], service.name


def check_service_coverage(root: Path = ROOT) -> int:
    contract = root / "docs" / "generated" / "udb-native-contract.json"
    if not contract.exists():
        print(f"::error::{contract} missing — run `udb native manifest > {contract}`")
        return 1
    services = contract_services(root)
    if not services:
        print("::error::native contract lists zero services")
        return 1

    failed = False
    checked = 0
    text_cache: dict[Path, str] = {}
    for lang, patterns in CLIENTS.items():
        files = []
        for pat in patterns:
            files.extend(sorted(root.glob(pat)))
        if not files:
            failed = True
            print(
                f"::error::{lang} generated client is missing; run `udb sdk generate --lang {lang}` "
                "or Gate C all-language generation and commit the output."
            )
            continue
        checked += 1
        text = "\n".join(cached_text(f, text_cache) for f in files)
        # The robustness wrapper must mention each service by short name, but that
        # alone is not enough: wrappers can drift ahead of the raw buf/protoc
        # artifacts. Also require a concrete generated stub symbol in the expected
        # per-language artifact location.
        missing = [s.name for s in services if s.name not in text]
        if missing:
            failed = True
            print(
                f"::error::{lang} generated client is missing {len(missing)} "
                f"contract service(s): {', '.join(missing)}"
            )
        missing_stubs = []
        for service in services:
            stub_files, needle = stub_artifacts(lang, service, root)
            if not stub_files or not files_containing(stub_files, needle, text_cache):
                missing_stubs.append(service.full_name)
        if missing_stubs:
            failed = True
            print(
                f"::error::{lang} generated stubs are missing {len(missing_stubs)} "
                f"contract service artifact(s): {', '.join(missing_stubs)}"
            )
        else:
            print(f"ok {lang}: all {len(services)} service stubs present")

    if checked == 0:
        print("::error::no generated SDK clients found to check")
        return 1
    if failed:
        print("SDK service coverage drift: regenerate all SDK outputs and commit.")
        return 1
    print(f"SDK service coverage OK — {len(services)} services across {checked} language(s).")
    return 0


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_minimal_sdk(root: Path, service: ContractService, *, omit: str | None = None) -> None:
    package_rel = rel_package_path(service)
    package_pascal = pascal_package_path(service)
    service_text = f"{service.name}\n{service.full_name}\n"
    if omit != "typescript":
        write(root / "sdk/typescript/generatedClient.ts", service_text)
        write(
            root / "sdk/typescript/gen" / package_rel / "foo_pb.ts",
            f"export const {service.name}: GenService = null as any;\n",
        )
    if omit != "python":
        write(root / "sdk/python/udb_client/generated_client.py", service_text)
        write(
            root / "sdk/python/gen" / package_rel / "foo_pb2_grpc.py",
            f"class {service.name}Stub: pass\n",
        )
    if omit != "csharp":
        write(root / "sdk/csharp/Udb.Client/GeneratedClient.cs", service_text)
        write(
            root / "sdk/csharp/gen/FooGrpc.cs",
            f'const string __ServiceName = "{service.full_name}";\n',
        )
    if omit != "java":
        write(root / "sdk/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java", service_text)
        write(
            root / "sdk/java/gen/com" / package_rel / "FooGrpc.java",
            f'public static final String SERVICE_NAME = "{service.full_name}";\n',
        )
    if omit != "php":
        write(root / "sdk/php/src/Generated/GeneratedClient.php", service_text)
        write(
            root / "sdk/php/gen" / package_pascal / f"{service.name}Client.php",
            f"<?php class {service.name}Client {{}}\n",
        )
    if omit != "go":
        write(root / "sdk/go/udbclient/generated_client.go", service_text)
        write(
            root / "sdk/go/gen" / package_rel / "foo.pb.go",
            f"func New{service.name}Client() {{}}\n",
        )


def run_selftest() -> int:
    def checked(root: Path) -> int:
        # Negative fixtures intentionally emit ::error:: markers. Keep those out
        # of GitHub Actions logs so expected selftest failures are not surfaced as
        # real check annotations.
        with contextlib.redirect_stdout(io.StringIO()):
            return check_service_coverage(root)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        service = ContractService("udb.core.foo.services.v1", "FooService")
        write(
            root / "docs/generated/udb-native-contract.json",
            json.dumps({"services": [{"service": service.full_name}]}) + "\n",
        )
        write_minimal_sdk(root, service)
        assert checked(root) == 0, "complete six-language fixture failed"

        missing = root / "missing"
        write(
            missing / "docs/generated/udb-native-contract.json",
            json.dumps({"services": [{"service": service.full_name}]}) + "\n",
        )
        write_minimal_sdk(missing, service, omit="python")
        assert checked(missing) == 1, "missing language should fail"

        stale = root / "stale"
        write(
            stale / "docs/generated/udb-native-contract.json",
            json.dumps({"services": [{"service": service.full_name}]}) + "\n",
        )
        write_minimal_sdk(stale, service)
        stale_stub = stale / "sdk/go/gen" / rel_package_path(service) / "foo.pb.go"
        stale_stub.write_text("func NewOtherServiceClient() {}\n", encoding="utf-8")
        assert checked(stale) == 1, "stale generated stub should fail"

    print("sdk service coverage selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()
    return check_service_coverage()


if __name__ == "__main__":
    raise SystemExit(main())
