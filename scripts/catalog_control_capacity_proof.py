#!/usr/bin/env python3
"""CI-only actual served catalog capacity comparison; selftest is log validation only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

BASELINE = "c6664a2e752338f36ef99b687d2eeb643de25c9d"
FIXTURE = "src/runtime/service/live_tests/catalog_reviewed_transition_live.rs"
MODULE = "runtime::service::live_tests::catalog_reviewed_transition_live::"
CASES = (
    "live_reviewed_catalog_transition_requires_native_approval_and_verified_application",
    "live_reviewed_catalog_transition_single_connection_distinct_target",
    "live_reviewed_catalog_transition_single_connection_primary_target",
)
ORIGINAL_CASE = MODULE + CASES[2]
GREEN_FILTER = MODULE + "live_reviewed_catalog_transition"
ASSERTION = (
    "CATALOG_CONTROL_CAPACITY: served reviewed candidate planning must succeed "
    "within one configured control connection"
)
POOL_REFUSAL = "pool timed out while waiting for an open connection"
PRODUCTION = (
    "src/runtime/core/catalog_admin.rs",
    "src/runtime/core/catalog_transition.rs",
    "src/runtime/core/catalog_sql.rs",
    "src/runtime/core/accessors.rs",
    "src/migration/phase_runner.rs",
)
RECEIPT_INPUTS = (
    *PRODUCTION,
    FIXTURE,
    "src/runtime/service/live_tests/mod.rs",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
)
SCRIPT = "scripts/catalog_control_capacity_proof.py"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], stderr=subprocess.STDOUT)


def head() -> str:
    return git("rev-parse", "HEAD").decode().strip()


def clean() -> None:
    require(not git("status", "--porcelain", "--untracked-files=no").strip(),
            "comparison requires clean tracked checkout")


def root() -> Path:
    return Path(os.environ["GITHUB_WORKSPACE"]).resolve()


def output() -> Path:
    return Path(os.environ["RUNNER_TEMP"], "catalog-control-capacity-proof").resolve()


def evidence() -> dict:
    return json.loads((output() / "source-comparison.json").read_text(encoding="utf-8"))


def write_json(name: str, value: object) -> None:
    (output() / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def validate_fixture(data: bytes) -> None:
    text = data.decode("utf-8")
    require(ASSERTION in text, "corrected fixture lacks the named real capacity assertion")
    names = re.findall(r"async fn (live_reviewed_catalog_transition_[A-Za-z0-9_]+)\(\)", text)
    require(sorted(names) == sorted(CASES), "corrected fixture must retain exactly the three reviewed served cases")
    require(
        "let provisioner_subject = provisioner.user_id;" in text
        and "bootstrap owned native PERSON provisioner without attribution" in text
        and "assert!(provisioner.created_by.is_empty());" in text
        and "bootstrap provisioner created_by must be SQL NULL" in text
        and "activate durable native PERSON provisioner" in text,
        "both phases require a persisted native PERSON provisioner with null bootstrap attribution",
    )
    require("config.primary.max_open_conns = 1;" in text and
            "config.primary.min_connections = 1;" in text and
            "config.primary.acquire_timeout_secs = 2;" in text,
            "actual serving max1 budget must be explicit")
    require("config.channels.migration_max_concurrent = 2;" in text,
            "concurrent fixture migration RPCs need explicit admission without enlarging max1 database pools")
    require('"standalone-producer-unique"' in text and '"owned-promoted-unique"' in text
            and "assert_native_unique_base(" in text and "UNIQUE USING INDEX" in text,
            "both phases must retain actual native standalone producer verification and deliberate owned promotion")
    require("get_max_connections()" in text and
            "run_reviewed_catalog_fixture(true, true)" in text and
            "run_reviewed_catalog_fixture(true, false)" in text,
            "both shared/distinct serving pool assertions must execute")
    for newer_api in (
        "_on_connection(", "CatalogAuthorityConnection", "postgres_pools_share_connections",
        "release_catalog_apply_authority(", "apply_reviewed_sql_artifact_on_connection(",
    ):
        require(newer_api not in text, "identical fixture must use only c666-existing production APIs")
    require("pg_try_advisory_xact_lock" in text and "Code::Cancelled" in text,
            "corrected proof must retain actual cancellation/project-lock controls")


def validate_original(log: str, exit_code: int) -> None:
    require(exit_code == 101, "baseline must return Cargo's actual executed test failure")
    require(re.search(r"(?m)^running 1 test$", log) is not None,
            "baseline must execute exactly one serving test")
    results = re.findall(
        r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", log
    )
    require(results == [("FAILED", "0", "1", "0")],
            "build/infra/ignored/multiple-test outcomes cannot count as baseline RED")
    require(ORIGINAL_CASE in log and re.search(
        r"(?m)^\s*" + re.escape(ORIGINAL_CASE) + r"\s*$", log
    ) is not None, "baseline failure list must identify the exact actual named serving test")
    require(ASSERTION in log and POOL_REFUSAL in log,
            "baseline must fail at the named genuine pool acquisition capacity refusal")


def validate_corrected(log: str, exit_code: int) -> None:
    require(exit_code == 0, "corrected served matrix must succeed")
    require(re.search(r"(?m)^running 3 tests$", log) is not None,
            "corrected must execute original matrix plus both max1 target layouts")
    results = re.findall(
        r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", log
    )
    require(results == [("ok", "3", "0", "0")],
            "corrected must pass exactly three real, nonignored serving matrices")
    for case in CASES:
        require("test " + MODULE + case + " ..." in log,
                "corrected output must include each exact named serving case")


def snapshot(label: str) -> dict:
    receipt = {}
    for name in RECEIPT_INPUTS:
        data = Path(name).read_bytes()
        dest = output() / (label + "-source") / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        receipt[name] = {"sha256": digest(data), "bytes": len(data)}
    tracked = {}
    for raw in git("ls-files", "-z").split(b"\0"):
        if not raw:
            continue
        name = raw.decode("utf-8")
        path = Path(name)
        if path.is_file():
            tracked[name] = digest(path.read_bytes())
    write_json(label + "-tracked-source-hashes.json", tracked)
    return receipt


def verify_corrected(state: dict) -> None:
    require(head() == state["corrected_commit"], "corrected commit identity changed")
    clean()
    for name, expected in state["corrected_sources"].items():
        require(digest(Path(name).read_bytes()) == expected["sha256"],
                "corrected source differs from saved immutable Git checkout: " + name)
    require(digest(Path(FIXTURE).read_bytes()) == state["fixture_sha256"],
            "corrected serving fixture differs from identical baseline overlay")


def prepare() -> None:
    os.chdir(root())
    output().mkdir(parents=True, exist_ok=True)
    clean()
    requested = os.environ.get("BASELINE_COMMIT", BASELINE)
    require(requested == BASELINE, "baseline must be exact audited immutable c666")
    corrected = head()
    require(corrected == os.environ["GITHUB_SHA"], "corrected checkout differs from dispatched GitHub SHA")
    require(corrected != BASELINE, "proof needs a committed corrected checkout")
    subprocess.run(["git", "merge-base", "--is-ancestor", BASELINE, corrected], check=True)
    fixture = Path(FIXTURE).read_bytes()
    require(fixture == git("show", corrected + ":" + FIXTURE),
            "fixture must be exact committed corrected bytes")
    validate_fixture(fixture)
    for name in RECEIPT_INPUTS:
        git("cat-file", "-e", BASELINE + ":" + name)
    baseline_module = git("show", BASELINE + ":src/runtime/service/live_tests/mod.rs")
    require(b"mod catalog_reviewed_transition_live;" in baseline_module,
            "baseline must already register the exact native served module")
    require(b"release_catalog_apply_authority" not in git("show", BASELINE + ":" + PRODUCTION[0]),
            "baseline unexpectedly contains the capacity correction")
    runner = Path(__file__).read_bytes()
    require(runner == git("show", corrected + ":" + SCRIPT),
            "CI runner copy must match the exact committed proof script")
    (output() / "identical-fixture.rs").write_bytes(fixture)
    state = {
        "baseline_commit": BASELINE,
        "corrected_commit": corrected,
        "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "proof_script_sha256": digest(runner),
        "fixture_sha256": digest(fixture),
        "original_test": ORIGINAL_CASE,
        "corrected_tests": [MODULE + name for name in CASES],
        "exact_red_assertion": ASSERTION,
        "actual_pool_refusal": POOL_REFUSAL,
        "corrected_sources": snapshot("corrected"),
        "comparison_scope": "immutable baseline checkout with only identical corrected serving fixture overlaid",
        "pending": ["original runtime RED", "corrected runtime GREEN"],
    }
    write_json("source-comparison.json", state)
    print("Pinned immutable c666 and corrected checkout with identical served fixture.")


def cargo(log_name: str, selector: str, exact: bool) -> dict:
    command = ["cargo", "test", "--locked", "--lib", selector, "--"]
    if exact:
        command.append("--exact")
    command += ["--ignored", "--nocapture", "--test-threads=1"]
    log_path = output() / log_name
    with log_path.open("w", encoding="utf-8") as stream:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace")
        assert proc.stdout is not None
        for line in proc.stdout:
            stream.write(line)
            stream.flush()
            print(line, end="", flush=True)
        exit_code = proc.wait()
    log = log_path.read_text(encoding="utf-8")
    # Cargo's actual --lib executable must have been launched. A compiler
    # diagnostic, guessed pathname or merely prepared binary cannot count.
    write_json(log_name.removesuffix(".log") + "-execution.json", {
        "command": command, "exit_code": exit_code, "log_sha256": digest(log_path.read_bytes()),
    })
    binaries = re.findall(r"Running unittests [^\n]* \(([^\n]+)\)", log)
    require(len(binaries) == 1, "Cargo must launch exactly one actual library test executable")
    binary = Path(binaries[0]).resolve()
    target = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    require(binary.is_relative_to(target) and binary.is_file() and os.access(binary, os.X_OK),
            "actual executed library binary must reside under the owned target directory")
    with binary.open("rb") as stream:
        require(stream.read(4) == b"\x7fELF", "actual Linux test executable must be ELF")
        stream.seek(0)
        binary_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "command": command,
        "exit_code": exit_code,
        "log_sha256": digest(log_path.read_bytes()),
        "test_binary": str(binary),
        "test_binary_sha256": binary_sha256,
        "test_binary_bytes": binary.stat().st_size,
    }


def original() -> None:
    os.chdir(root())
    state = evidence()
    verify_corrected(state)
    receipt = {"baseline_commit": BASELINE, "accepted_runtime_red": False}
    try:
        subprocess.run(["git", "checkout", "--detach", BASELINE], check=True)
        require(head() == BASELINE, "baseline checkout identity mismatch")
        clean()
        Path(FIXTURE).write_bytes((output() / "identical-fixture.rs").read_bytes())
        changed = git("diff", "--name-only").decode().splitlines()
        require(changed == [FIXTURE], "baseline may differ only by the identical serving fixture")
        require(digest(Path(FIXTURE).read_bytes()) == state["fixture_sha256"],
                "baseline fixture overlay changed")
        baseline_sources = snapshot("original")
        for name in PRODUCTION:
            require(digest(Path(name).read_bytes()) == digest(git("show", BASELINE + ":" + name)),
                    "original production path differs from immutable baseline: " + name)
        receipt["source_hashes"] = baseline_sources
        receipt.update(cargo("original.log", ORIGINAL_CASE, True))
        validate_original((output() / "original.log").read_text(encoding="utf-8"), receipt["exit_code"])
        receipt["accepted_runtime_red"] = True
        print("Actual original max1 serving request reproduced the exact capacity refusal.")
    except Exception as error:
        receipt["error"] = str(error)
        raise
    finally:
        write_json("original-outcome.json", receipt)
        # Keep the runner/evidence outside checkout so baseline cannot remove it.
        # Restore even after compile/infra/unrelated failure; refuse dirty or
        # mismatched corrected bytes before any GREEN execution.
        restore()


def restore() -> None:
    os.chdir(root())
    state = evidence()
    require(head() in (BASELINE, state["corrected_commit"]), "restore refuses an unrelated checkout")
    changed = git("diff", "--name-only").decode().splitlines()
    require(set(changed).issubset({FIXTURE}), "restore refuses unrelated tracked modifications")
    if head() == BASELINE:
        subprocess.run(["git", "checkout", "--", FIXTURE], check=True)
        subprocess.run(["git", "checkout", "--detach", state["corrected_commit"]], check=True)
    verify_corrected(state)
    write_json("restore-outcome.json", {
        "corrected_commit": head(), "fixture_sha256": digest(Path(FIXTURE).read_bytes()),
        "tracked_checkout_clean": True,
    })


def corrected() -> None:
    os.chdir(root())
    state = evidence()
    verify_corrected(state)
    receipt = {"corrected_commit": head(), "accepted_runtime_green": False}
    try:
        receipt["source_hashes"] = snapshot("corrected")
        receipt.update(cargo("corrected.log", GREEN_FILTER, False))
        validate_corrected((output() / "corrected.log").read_text(encoding="utf-8"), receipt["exit_code"])
        receipt["accepted_runtime_green"] = True
        print("Actual corrected original/max1 shared/max1 distinct served matrices passed.")
    except Exception as error:
        receipt["error"] = str(error)
        raise
    finally:
        write_json("corrected-outcome.json", receipt)


def finish() -> None:
    os.chdir(root())
    state = evidence()
    verify_corrected(state)
    red = json.loads((output() / "original-outcome.json").read_text(encoding="utf-8"))
    green = json.loads((output() / "corrected-outcome.json").read_text(encoding="utf-8"))
    require(red.get("accepted_runtime_red") is True and green.get("accepted_runtime_green") is True,
            "proof requires both accepted actual runtime outcomes")
    require(red["test_binary_sha256"] != green["test_binary_sha256"],
            "baseline and corrected actual executables must differ")
    require(red["source_hashes"][FIXTURE]["sha256"] == green["source_hashes"][FIXTURE]["sha256"]
            == state["fixture_sha256"], "both phases must execute identical serving fixture bytes")
    write_json("proof-outcome.json", {
        "success": True, "baseline_commit": BASELINE, "corrected_commit": state["corrected_commit"],
        "fixture_sha256": state["fixture_sha256"], "original": red, "corrected": green,
    })
    print("Accepted exact actual capacity RED/GREEN; no compile/infra credit.")


def selftest() -> None:
    # Pure source/log-validator controls; no Git, Cargo, database or app.
    red = ("running 1 test\nthread 'x' panicked:\n" + ASSERTION + ": " + POOL_REFUSAL
           + "\nfailures:\n    " + ORIGINAL_CASE
           + "\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 2 filtered out;\n")
    green = ("running 3 tests\n" + "".join("test " + MODULE + name + " ... ok\n" for name in CASES)
             + "test result: ok. 3 passed; 0 failed; 0 ignored; 2 filtered out;\n")
    validate_original(red, 101)
    validate_corrected(green, 0)
    bad = [
        (validate_original, red, 0),
        (validate_original, "error: could not compile\n", 101),
        (validate_original, red.replace(ASSERTION, "fixture failed provisioning"), 101),
        (validate_original, red.replace(POOL_REFUSAL, "database not found"), 101),
        (validate_original, red.replace("running 1 test", "running 2 tests"), 101),
        (validate_original, red.replace("0 ignored", "1 ignored"), 101),
        (validate_original, red.replace(ORIGINAL_CASE, MODULE + "wrong_case"), 101),
        (validate_corrected, green, 101),
        (validate_corrected, green.replace("running 3 tests", "running 1 test"), 0),
        (validate_corrected, green.replace(CASES[1], "missing_distinct_case"), 0),
        (validate_corrected, green.replace("0 failed", "1 failed"), 0),
        (validate_corrected, green.replace("0 ignored", "1 ignored"), 0),
    ]
    for fn, log, code in bad:
        try:
            fn(log, code)
        except RuntimeError:
            continue
        raise AssertionError("negative log admission unexpectedly passed")
    fixture = (Path(__file__).resolve().parent.parent / FIXTURE).read_bytes()
    validate_fixture(fixture)
    for token in (b"config.channels.migration_max_concurrent = 2;", b'"standalone-producer-unique"', b"UNIQUE USING INDEX"):
        try:
            validate_fixture(fixture.replace(token, b"removed_fixture_control"))
        except RuntimeError:
            continue
        raise AssertionError("negative fixture admission unexpectedly passed")
    print("capacity proof admission: 2 positive + 12 negative log controls; 1 positive + 3 negative fixture controls passed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "original", "restore", "corrected", "finish", "selftest"])
    args = parser.parse_args()
    globals()[args.phase]()


if __name__ == "__main__":
    main()
