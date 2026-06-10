#!/usr/bin/env python3
"""urgent_fix #19: fail CI when a descriptor-known native service has NO generated
robustness client in a shipped SDK language.

The native contract (`docs/generated/udb-native-contract.json`) is the single
source of truth for which services exist. Each language's generated robustness
client must reference every one of them by its short service name; the audit found
Storage/Asset/IdP/WebRTC missing from all six languages even though the manifest
listed them. After `udb sdk generate`, this guard keeps them from drifting back out.

A language whose generated-client file is absent is SKIPPED (not failed) so the
guard does not block a language that hasn't been generated yet; a language whose
client EXISTS but omits a contract service FAILS.
"""
import json
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONTRACT = ROOT / "docs" / "generated" / "udb-native-contract.json"

# Per-language generated-client source (the robustness layer, not raw buf stubs).
# Glob patterns - layouts differ: TS/Python/C#/Java ship a single generated client
# file; PHP/Go ship per-service generated client files. A language whose glob
# matches NOTHING is skipped; a language with matches must cover every service
# and must also have the corresponding generated protobuf/gRPC stub artifacts.
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


def contract_services() -> list[ContractService]:
    data = json.loads(CONTRACT.read_text(encoding="utf-8"))
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


def files_containing(files: list[Path], needle: str) -> bool:
    for file in files:
        if needle in file.read_text(encoding="utf-8", errors="ignore"):
            return True
    return False


def stub_artifacts(lang: str, service: ContractService) -> tuple[list[Path], str]:
    rel = rel_package_path(service)
    if lang == "typescript":
        directory = ROOT / "sdk" / "typescript" / "gen" / rel
        return sorted(directory.glob("*_pb.ts")), f"export const {service.name}: GenService"
    if lang == "python":
        directory = ROOT / "sdk" / "python" / "gen" / rel
        return sorted(directory.glob("*_pb2_grpc.py")), f"class {service.name}Stub"
    if lang == "csharp":
        directory = ROOT / "sdk" / "csharp" / "gen"
        return sorted(directory.glob("**/*Grpc.cs")), f'__ServiceName = "{service.full_name}"'
    if lang == "java":
        directory = ROOT / "sdk" / "java" / "gen" / "com" / rel
        return sorted(directory.glob("*.java")), f'SERVICE_NAME = "{service.full_name}"'
    if lang == "php":
        path = ROOT / "sdk" / "php" / "gen" / pascal_package_path(service) / f"{service.name}Client.php"
        return [path] if path.exists() else [], f"class {service.name}Client"
    if lang == "go":
        directory = ROOT / "sdk" / "go" / "gen" / rel
        return sorted(directory.glob("*.go")), f"New{service.name}Client"
    return [], service.name


def main() -> int:
    if not CONTRACT.exists():
        print(f"::error::{CONTRACT} missing — run `udb native manifest > {CONTRACT}`")
        return 1
    services = contract_services()
    if not services:
        print("::error::native contract lists zero services")
        return 1

    failed = False
    checked = 0
    for lang, patterns in CLIENTS.items():
        files = []
        for pat in patterns:
            files.extend(sorted(ROOT.glob(pat)))
        if not files:
            print(f"skip {lang}: no generated client files match {patterns}")
            continue
        checked += 1
        text = "\n".join(f.read_text(encoding="utf-8", errors="ignore") for f in files)
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
            stub_files, needle = stub_artifacts(lang, service)
            if not stub_files or not files_containing(stub_files, needle):
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
        print(
            "SDK service coverage drift: regenerate with `udb sdk generate` and commit."
        )
        return 1
    print(f"SDK service coverage OK — {len(services)} services across {checked} language(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
