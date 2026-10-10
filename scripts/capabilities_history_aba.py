#!/usr/bin/env python3
"""Same-host optimized, actual PostgreSQL/served RPC A1-B-A2 comparison.

This script executes applications only in dedicated CI. Local selftest is an
admission/receipt parser check and provides no performance or serving evidence.
"""
from __future__ import annotations
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

BASELINE = "678fec4160236f7f392f47b89d03f87012591d77"
PRODUCTION = {
    "src/runtime/core/catalog_admin.rs": {
        "A": "cdefe77d8f2a1f0f85700d9ca6a434c8e111121225944c621a42ae0b46189341",
        "B": "f7265f5b51761e34b1591a67eaf8050fc8b55419db1b8803375d1be61d6d2198",
    },
    "src/runtime/service/handlers_meta.rs": {
        "A": "b3f4f391f4318f84671aa03b8e3a619f2eb4b859c475f9d8bb31d4270f3ee8ba",
        "B": "342029f337a078a10ec26a0db42a410d546d371bd7ac46e3a6f3f0c220b7885f",
    },
}
FIXTURE = "src/runtime/service/live_tests/catalog_reviewed_transition_live.rs"
SCRIPT = "scripts/capabilities_history_aba.py"
TEST = "runtime::service::live_tests::catalog_reviewed_transition_live::live_capabilities_catalog_history_profile"
START = b"// BEGIN CAPABILITIES_HISTORY_CI_PROFILE"
END = b"// END CAPABILITIES_HISTORY_CI_PROFILE"
# Filled only from the final private measurement-block bytes at freeze.
BLOCK_SHA256 = "ff3af7c6cbe4f3462472c992b303fa66a38bceb44cadf79e385f733a4db5777d"
POLICY_ENV = {
    "UDB_RATE_LIMIT_POLICY_AUTHN_LOGIN_PUBLIC": "20000",
    "UDB_ABUSE_POLICY_AUTHN_LOGIN_ABUSE": "20000",
    "UDB_GRPC_MAX_RECV_BYTES": "33554432",
}
ATTEMPTS = 50
LOGIN_ATTEMPTS = 1024
HISTORIES = (1, 8, 32)
TABLES = (2, 257)


def require(ok: bool, message: str) -> None:
    if not ok:
        raise RuntimeError(message)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], stderr=subprocess.STDOUT)


def output() -> Path:
    return Path(os.environ["RUNNER_TEMP"], "capabilities-history-aba").resolve()


def write(name: str, value: object) -> None:
    path = output() / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def head() -> str:
    return git("rev-parse", "HEAD").decode().strip()


def clean() -> None:
    require(not git("status", "--porcelain", "--untracked-files=no").strip(),
            "comparison requires clean tracked checkout")


def block(data: bytes) -> bytes:
    require(data.count(START) == data.count(END) == 1,
            "identical fixture must have one delimited measurement block")
    return data[data.index(START):data.index(END) + len(END)]


def source_snapshot(label: str) -> dict:
    values = {}
    for raw in git("ls-files", "-z").split(b"\0"):
        if raw:
            path = Path(raw.decode())
            if path.is_file():
                values[path.as_posix()] = sha(path.read_bytes())
    write(label + "-tracked-source-hashes.json", values)
    for name in (*PRODUCTION, FIXTURE, "Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
        target = output() / (label + "-source") / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(Path(name).read_bytes())
    return values


def host_receipt() -> dict:
    files = {}
    for name in ("/proc/stat", "/proc/loadavg", "/proc/meminfo", "/proc/self/status",
                 "/proc/self/cgroup", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu.stat",
                 "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max"):
        try:
            files[name] = Path(name).read_text()
        except OSError:
            files[name] = None
    return {
        "time_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
        "hostname": os.uname().nodename, "kernel": list(os.uname()),
        "cpu_count": os.cpu_count(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "files": files,
        "runner_name": os.environ.get("RUNNER_NAME"), "runner_os": os.environ.get("RUNNER_OS"),
    }


def database_service_receipt() -> dict:
    # Inspect only identity/resource fields; never collect container environment.
    container = os.environ["PG_SERVICE_CONTAINER_ID"]
    require(re.fullmatch(r"[0-9a-f]{12,64}", container) is not None,
            "actual owned PostgreSQL service container identity is required")
    fields = ("Id", "Image", "State.Pid", "State.StartedAt", "State.Running",
              "HostConfig.NanoCpus", "HostConfig.CpuQuota", "HostConfig.CpuPeriod",
              "HostConfig.CpusetCpus", "HostConfig.Memory")
    template = "\n".join("{{json ." + field + "}}" for field in fields)
    result = subprocess.run(["docker", "inspect", "--format", template, container],
        text=True, capture_output=True, timeout=15)
    require(result.returncode == 0, "owned PostgreSQL service metadata unavailable")
    values = result.stdout.splitlines()
    require(len(values) == len(fields), "PostgreSQL service identity fields incomplete")
    receipt = {field: json.loads(raw) for field, raw in zip(fields, values)}
    require(receipt["State.Running"] is True, "actual PostgreSQL service must remain running")
    try:
        receipt["host_cpu_affinity"] = sorted(os.sched_getaffinity(receipt["State.Pid"]))
    except OSError:
        receipt["host_cpu_affinity"] = None
    require(bool(receipt["host_cpu_affinity"]),
            "actual PostgreSQL service CPU affinity must be observed for comparison")
    return receipt


def psql(sql: str, dsn: str | None = None) -> str:
    # The CI fixture DSN is never echoed or copied into a receipt/error.
    result = subprocess.run(["psql", dsn or os.environ["UDB_LIVE_NATIVE_PG_DSN"],
        "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At"], input=sql, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    require(result.returncode == 0, "owned PostgreSQL setup/restore query failed; no performance admission")
    return result.stdout.strip()


def quiet_receipt_preflight() -> dict:
    # Execute in CI before compiling: DO's status tag must not contaminate the
    # following JSON row. Do not discard output lines to conceal a bad command.
    sql = """DO $quiet_receipt$ BEGIN PERFORM 1; END $quiet_receipt$;
SELECT json_build_object('schema_version', 1, 'quiet_command_tags', TRUE);
"""
    value = json.loads(psql(sql))
    require(value == {"schema_version": 1, "quiet_command_tags": True},
            "actual DO+SELECT JSON receipt preflight failed; no performance admission")
    return value


def owned_dsn(database: str) -> str:
    require(re.fullmatch(r"udb_cap_hist_[0-9a-f]{32}", database) is not None,
            "database must be the exact UUID-derived owned fixture name")
    dsn = os.environ["UDB_LIVE_NATIVE_PG_DSN"]
    base, sep, query = dsn.partition("?")
    return base.rsplit("/", 1)[0] + "/" + database + (sep + query if sep else "")


def database_receipt(database: str) -> dict:
    # All row data stays inside this owned database. Persist only identity,
    # table cardinality and a SHA of sorted actual row JSON (no credential values).
    identity = psql("SELECT json_build_object('database',current_database(),'oid',"
        "(SELECT oid FROM pg_database WHERE datname=current_database()),"
        "'server_version_num',current_setting('server_version_num'),"
        "'server_address',inet_server_addr(),'server_port',inet_server_port())", owned_dsn(database))
    query = """DO $receipt$ DECLARE r RECORD; BEGIN
      CREATE TEMP TABLE udb_profile_receipt(name TEXT, rows BIGINT, data_sha256 TEXT);
      FOR r IN SELECT n.nspname AS s,c.relname AS t FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind='r' AND n.nspname NOT IN('pg_catalog','information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%' AND n.nspname NOT LIKE 'pg_temp%%'
        ORDER BY n.nspname,c.relname LOOP
        EXECUTE format($scan$INSERT INTO udb_profile_receipt SELECT %L,count(*),
          encode(sha256(convert_to(COALESCE(string_agg(row_json,'' ORDER BY row_json),''),'UTF8')),'hex')
          FROM(SELECT row_to_json(x)::TEXT AS row_json FROM %I.%I x) q$scan$,
          r.s||'.'||r.t,r.s,r.t);
      END LOOP;
    END $receipt$;
    SELECT COALESCE(json_agg(udb_profile_receipt ORDER BY name),'[]'::JSON) FROM udb_profile_receipt;"""
    rows = json.loads(psql(query, owned_dsn(database)))
    return {"identity": json.loads(identity), "tables": rows,
            "all_owned_row_digest": sha(json.dumps(rows, sort_keys=True).encode())}


def activate_variant(state: dict, variant: str) -> dict:
    require(head() == BASELINE, "measurement tree must retain the audited baseline Git identity")
    changed = set(git("diff", "--name-only").decode().splitlines())
    require(changed.issubset({FIXTURE, *PRODUCTION}), "refuse unrelated source mutation")
    Path(FIXTURE).write_bytes((output() / "identical-fixture.rs").read_bytes())
    for name, expected in PRODUCTION.items():
        data = git("show", BASELINE + ":" + name) if variant == "A" else (output() / "candidate-source" / name).read_bytes()
        require(sha(data) == expected[variant], "pinned production source hash mismatch: " + name)
        Path(name).write_bytes(data)
    require(sha(block(Path(FIXTURE).read_bytes())) == BLOCK_SHA256,
            "identical actual measurement block differs from freeze")
    return source_snapshot(variant)


def build(variant: str) -> dict:
    command = ["cargo", "test", "--locked", "--profile", "dist", "--lib", "--no-run", "--message-format=json"]
    log = output() / (variant + "-build.log")
    binaries = []
    with log.open("w", encoding="utf-8") as stream:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace")
        assert proc.stdout is not None
        for line in proc.stdout:
            stream.write(line); stream.flush()
            try:
                item = json.loads(line)
            except ValueError:
                print(line, end="", flush=True)
                continue
            if item.get("reason") == "compiler-message":
                print(item.get("message", {}).get("rendered", ""), end="", flush=True)
            if item.get("reason") == "compiler-artifact" and item.get("executable") and "lib" in item.get("target", {}).get("kind", []):
                require(item.get("profile", {}).get("test") is True,
                        "actual built library artifact must be a test executable")
                require(str(item.get("profile", {}).get("opt_level")) == "3",
                        "actual measurement artifact must use optimized dist profile")
                binaries.append(Path(item["executable"]).resolve())
        code = proc.wait()
    require(code == 0 and len(binaries) == 1, "compile failure/missing actual executable is not performance evidence")
    binary = binaries[0]
    require(binary.is_relative_to(Path(os.environ["CARGO_TARGET_DIR"]).resolve()) and binary.is_file(),
            "built executable must remain under owned Cargo target")
    with binary.open("rb") as stream:
        require(stream.read(4) == bytes((127, 69, 76, 70)) and os.access(binary, os.X_OK), "actual Linux executable must be ELF")
    saved = output() / "bin" / variant
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(binary, saved)
    return {"command": command, "exit_code": code, "binary": str(saved),
            "binary_sha256": file_sha(saved), "binary_bytes": saved.stat().st_size,
            "build_log_sha256": file_sha(log)}


def validate_test(log: str, code: int) -> None:
    require(code == 0, "actual named serving test must succeed")
    require(re.findall(r"(?m)^running (\d+) test[s]?$", log) == ["1"],
            "exactly one actual serving test must execute")
    require(re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", log) == [("ok", "1", "0", "0")],
            "compile/infra/ignored/missing/multiple tests cannot count as measurement")
    require(TEST in log, "actual named profile test absent")


def validate_receipt_files(log: str, directory: Path, mode: str) -> None:
    reported = [json.loads(line) for line in re.findall(
        r"(?m)(?:^|[ \t])CAPABILITIES_HISTORY_PROFILE (\{.*\})$", log)]
    expected = set(HISTORIES) if mode in ("prepare", "sample") else set()
    require(len(reported) == len(expected) and
            {row["history_count"] for row in reported} == expected,
            "actual executed test must emit each expected history receipt exactly once")
    require({path.name for path in directory.iterdir()} ==
            {f"history-{history}.json" for history in expected},
            "receipt directory has unexpected or missing actual output files")
    for row in reported:
        name = f"history-{row['history_count']}.json"
        require(row["receipt_file"] == name and
                file_sha(directory / name) == row["receipt_sha256"],
                "executed test receipt SHA must match actual saved JSON bytes")
        actual = json.loads((directory / name).read_text(encoding="utf-8"))
        require(actual["mode"] == mode and actual["history_count"] == row["history_count"],
                "actual receipt mode/history must match executed phase marker")


def stable_host(value: dict) -> dict:
    identity = {key: value[key] for key in
                ("hostname", "kernel", "cpu_count", "cpu_affinity", "clock_ticks_per_second")}
    identity["cgroup_limits"] = {key: value["files"][key] for key in
        ("/proc/self/cgroup", "/sys/fs/cgroup/cpu.max",
         "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max")}
    return identity


def validate_samples(rows: list, rpc: str, count: int, workers: int = 1) -> None:
    selected = [row for row in rows if row["rpc"] == rpc]
    require(len(selected) == count * workers, "fixed actual request count mismatch: " + rpc)
    for worker in range(workers):
        actual = [row["attempt"] for row in selected if row["worker"] == worker] if rpc == "Login" else [row["attempt"] for row in selected]
        require(sorted(actual) == list(range(count)), "duplicate/missing actual request attempts: " + rpc)
    require(all(row["ok"] is True and row["code"] is None and
        row["end_us"] >= row["start_us"] and row["latency_us"] == row["end_us"] - row["start_us"] for row in selected),
        "all actual timed calls must succeed with exact monotonic intervals")
    if rpc != "Login":
        require(all(type(row["response_protobuf_bytes"]) is int and
                    row["response_protobuf_bytes"] > 0 for row in selected),
                "actual successful read response message sizes must be retained")


def overlap_count(rows: list, rpc: str) -> int:
    logins = [row for row in rows if row["rpc"] == "Login"]
    count = 0
    for row in (row for row in rows if row["rpc"] == rpc):
        at_start = sum(login["start_us"] <= row["start_us"] < login["end_us"] for login in logins)
        at_end = sum(login["start_us"] < row["end_us"] <= login["end_us"] for login in logins)
        count += int(at_start > 0 and at_end > 0)
    return count


def validate_receipt(value: dict, prepared: dict, tables: int) -> None:
    require(value["dataset"] == prepared["dataset"], "actual stored history rows/bytes/hash changed")
    require(value["history_count"] in HISTORIES and value["customer_table_count"] == tables,
            "actual dataset shape mismatch")
    require(value["pool_max"] == value["budgets"]["pg_pool_max"] == 4 and
            value["budgets"]["tokio_worker_threads"] == 2 and
            value["budgets"]["grpc_max_message_bytes"] == 33554432,
            "actual shared serving and message budgets mismatch")
    for key in ("login_rate_limit_per_minute", "login_abuse_limit_per_minute"):
        require(value["budgets"][key] == "20000", "benchmark-only declared policy override mismatch")
    require(value["attempts_per_rpc"] == ATTEMPTS, "must sample fifty actual calls per RPC per lane")
    idle, burst = value["idle"]["samples"], value["burst"]
    require(len(idle) == 2 * ATTEMPTS, "idle extra/missing RPCs")
    for rpc in ("GetCapabilities", "Select"):
        validate_samples(idle, rpc, ATTEMPTS)
        validate_samples(burst["samples"], rpc, ATTEMPTS)
        actual = overlap_count(burst["samples"], rpc)
        key = "overlapping_capabilities_successes" if rpc == "GetCapabilities" else "overlapping_select_successes"
        require(actual == burst[key] == ATTEMPTS, "insufficient genuine Login/read overlap; no burst admission")
    validate_samples(burst["samples"], "Login", LOGIN_ATTEMPTS, 2)
    require(len(burst["samples"]) == 2 * ATTEMPTS + 2 * LOGIN_ATTEMPTS,
            "burst extra/missing RPCs")
    require(burst["closed_loop_workers"] == 4 and burst["login_workers"] == 2 and
            burst["maximum_client_inflight"] <= 4 and burst["maximum_login_inflight"] == 2,
            "actual capped client concurrency mismatch")
    require(burst["login_attempts_per_worker"] == LOGIN_ATTEMPTS and
            burst["read_attempts_per_rpc"] == ATTEMPTS, "declared workload count mismatch")
    require(burst["pool_snapshots"] and all(0 <= row["idle"] <= row["size"] <= 4 for row in burst["pool_snapshots"]),
            "actual shared pool observations violate configured capacity")
    # Reconstruct all actual client intervals; do not trust only a reported peak.
    edges = sorted([(row["start_us"], 1) for row in burst["samples"]] +
                   [(row["end_us"], -1) for row in burst["samples"]], key=lambda pair: (pair[0], pair[1]))
    active = peak = 0
    for _, delta in edges:
        active += delta
        require(active >= 0, "invalid monotonic workload intervals")
        peak = max(peak, active)
    require(active == 0 and peak <= 4, "reconstructed actual concurrency exceeds four")


def execute(state: dict, variant: str, tables: int, namespace: str, mode: str, label: str) -> dict:
    binary = Path(state["binaries"][variant]["binary"])
    require(file_sha(binary) == state["binaries"][variant]["binary_sha256"],
            "saved actual binary differs from build receipt")
    receipts = output() / label
    receipts.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update(POLICY_ENV)
    env.update({"UDB_CAPABILITIES_PROFILE_NAMESPACE": namespace,
        "UDB_CAPABILITIES_PROFILE_MODE": mode, "UDB_CAPABILITIES_PROFILE_TABLES": str(tables),
        "UDB_CAPABILITIES_PROFILE_ATTEMPTS": str(ATTEMPTS),
        "UDB_CAPABILITIES_PROFILE_LOGIN_ATTEMPTS": str(LOGIN_ATTEMPTS),
        "UDB_CAPABILITIES_PROFILE_RECEIPTS_DIR": str(receipts)})
    command = [str(binary), TEST, "--exact", "--ignored", "--nocapture", "--test-threads=1"]
    before_host = host_receipt()
    require(stable_host(before_host) == state["stable_host_identity"],
            "same-host affinity/kernel/CPU budget changed between phases")
    write(label + "-host-before.json", before_host)
    require(database_service_receipt() == state["postgres_service_identity"],
            "actual PostgreSQL service identity/resource budget changed between phases")
    log = output() / (label + ".log")
    with log.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace")
        assert process.stdout is not None
        for line in process.stdout:
            stream.write(line); stream.flush(); print(line, end="", flush=True)
        code = process.wait()
    after_host = host_receipt()
    require(stable_host(after_host) == state["stable_host_identity"],
            "same-host affinity/kernel/CPU budget changed during phase")
    write(label + "-host-after.json", after_host)
    require(database_service_receipt() == state["postgres_service_identity"],
            "actual PostgreSQL service identity/resource budget changed during phase")
    execution = {"variant": variant, "mode": mode, "command": command,
        "exit_code": code, "binary_sha256": file_sha(binary),
        "log_sha256": file_sha(log), "phase_environment": {**POLICY_ENV,
            "namespace": namespace, "tables": tables, "attempts": ATTEMPTS,
            "login_attempts_per_worker": LOGIN_ATTEMPTS}}
    write(label + "-execution.json", execution)
    actual_log = log.read_text(encoding="utf-8")
    validate_test(actual_log, code)
    validate_receipt_files(actual_log, receipts, mode)
    return execution


def summaries(rows: list) -> dict:
    result = {}
    for rpc in {row["rpc"] for row in rows}:
        values = sorted(row["latency_us"] for row in rows if row["rpc"] == rpc)
        percentile = lambda p: values[min(len(values)-1, math.ceil(len(values)*p)-1)]
        result[rpc] = {"attempts": len(values), "mean_us": statistics.mean(values),
            "p50_us": percentile(.5), "p95_us": percentile(.95), "p99_us": percentile(.99)}
        if rpc != "Login":
            sizes = [row["response_protobuf_bytes"] for row in rows if row["rpc"] == rpc]
            result[rpc]["response_protobuf_bytes"] = {
                "total": sum(sizes), "min": min(sizes), "max": max(sizes),
                "mean": statistics.mean(sizes), "excludes": "gRPC framing/TCP/TLS"}
    return result


def restore(state: dict) -> None:
    require(head() in (BASELINE, state["proof_commit"]), "restore refuses unrelated Git identity")
    changed = set(git("diff", "--name-only").decode().splitlines())
    require(changed.issubset({FIXTURE, *PRODUCTION}), "restore refuses unrelated tracked modifications")
    if head() == BASELINE:
        subprocess.run(["git", "checkout", "--", FIXTURE, *PRODUCTION], check=True)
        subprocess.run(["git", "checkout", "--detach", state["proof_commit"]], check=True)
    clean()
    require(head() == state["proof_commit"], "restored proof commit mismatch")


def run() -> None:
    os.chdir(Path(os.environ["GITHUB_WORKSPACE"]).resolve())
    output().mkdir(parents=True, exist_ok=True)
    clean()
    require(os.environ.get("BASELINE_COMMIT", BASELINE) == BASELINE,
            "only exact immutable audited baseline is admitted")
    proof = head()
    require(proof == os.environ["GITHUB_SHA"], "dispatch and checkout identities differ")
    subprocess.run(["git", "merge-base", "--is-ancestor", BASELINE, proof], check=True)
    fixture = Path(FIXTURE).read_bytes()
    require(fixture == git("show", proof + ":" + FIXTURE) and sha(block(fixture)) == BLOCK_SHA256,
            "fixture must be the identical exact committed frozen measurement bytes")
    require(Path(__file__).read_bytes() == git("show", proof + ":" + SCRIPT),
            "copied runner must match exact committed proof source")
    require(b"mod catalog_reviewed_transition_live;" in git("show", BASELINE + ":src/runtime/service/live_tests/mod.rs"),
            "audited baseline must register existing measurement fixture module")
    (output() / "identical-fixture.rs").write_bytes(fixture)
    for name, pins in PRODUCTION.items():
        data = Path(name).read_bytes()
        require(sha(data) == pins["B"] and data == git("show", proof + ":" + name),
                "candidate must be exact frozen committed two-source projection")
        require(sha(git("show", BASELINE + ":" + name)) == pins["A"], "audited baseline bytes changed")
        saved = output() / "candidate-source" / name
        saved.parent.mkdir(parents=True, exist_ok=True); saved.write_bytes(data)
    state = {"proof_commit": proof, "baseline_commit": BASELINE,
        "run_id": os.environ.get("GITHUB_RUN_ID"), "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "fixture_sha256": sha(fixture), "fixture_block_sha256": BLOCK_SHA256,
        "runner_sha256": sha(Path(__file__).read_bytes()), "production_pins": PRODUCTION,
        "policy_environment": POLICY_ENV, "binaries": {}, "datasets": [],
        "comparison": "Same host, exact A binary in A1/A2, only two production source files differ in B.",
        "database_restore": "Same named owned database is recreated from a closed immutable prepared template before every phase.",
        "benchmark_accounting": "Separate native artifact; canonical386/1544 untouched.",
        "performance_claim": False}
    owned = []
    try:
        require(psql("SELECT CASE WHEN current_setting('server_version_num')::INTEGER BETWEEN 160000 AND 169999 AND rolsuper AND rolcreatedb THEN 1 ELSE 0 END FROM pg_roles WHERE rolname=current_user") == "1",
                "actual PostgreSQL16 fixture must permit owned database creation")
        write("psql-quiet-receipt-preflight.json", quiet_receipt_preflight())
        initial_host = host_receipt()
        state["stable_host_identity"] = stable_host(initial_host)
        state["postgres_service_identity"] = database_service_receipt()
        write("host-initial.json", initial_host)
        write("postgres-service-initial.json", state["postgres_service_identity"])
        write("toolchain.json", {cmd: subprocess.check_output(cmd.split(), text=True) for cmd in ("rustc -Vv", "cargo -V", "psql -V")})
        subprocess.run(["git", "checkout", "--detach", BASELINE], check=True)
        a = activate_variant(state, "A"); state["binaries"]["A"] = build("A")
        b = activate_variant(state, "B"); state["binaries"]["B"] = build("B")
        require(set(a) == set(b) and {name for name in a if a[name] != b[name]} == set(PRODUCTION),
                "A/B source snapshots may differ only by the two frozen production paths")
        require(state["binaries"]["A"]["binary_sha256"] != state["binaries"]["B"]["binary_sha256"],
                "candidate must execute a distinct actual compiled binary")
        write("source-comparison.json", state)
        for tables in TABLES:
            namespace = str(uuid.uuid4()); database = "udb_cap_hist_" + uuid.UUID(namespace).hex
            template = "udb_cap_template_" + uuid.UUID(namespace).hex
            owned.append((database, template))
            activate_variant(state, "A")
            execute(state, "A", tables, namespace, "prepare", f"tables-{tables}-prepare")
            prepared = {history: json.loads((output() / f"tables-{tables}-prepare" / f"history-{history}.json").read_text()) for history in HISTORIES}
            prepared_db = database_receipt(database)
            require(not psql(f"SELECT pid FROM pg_stat_activity WHERE datname='{database}'"),
                    "prepared source DB must have no live serving connections before snapshot")
            psql(f'CREATE DATABASE "{template}" TEMPLATE "{database}"; ALTER DATABASE "{template}" ALLOW_CONNECTIONS false;')
            phases = []
            for phase, variant in (("A1", "A"), ("B", "B"), ("A2", "A")):
                activate_variant(state, variant)
                psql(f'DROP DATABASE "{database}" WITH(FORCE); CREATE DATABASE "{database}" TEMPLATE "{template}";')
                before = database_receipt(database)
                require(before["tables"] == prepared_db["tables"], "full prepared row snapshot must match before each measured phase")
                label = f"tables-{tables}-{phase}"
                execution = execute(state, variant, tables, namespace, "sample", label)
                values = {}
                for history in HISTORIES:
                    path = output() / label / f"history-{history}.json"
                    value = json.loads(path.read_text())
                    validate_receipt(value, prepared[history], tables)
                    require(value["budgets"] == prepared[history]["budgets"], "all declared budgets must match preparation")
                    values[history] = {"receipt_sha256": sha(path.read_bytes()), "dataset": value["dataset"],
                        "idle": summaries(value["idle"]["samples"]), "burst": summaries(value["burst"]["samples"])}
                phases.append({"phase": phase, "variant": variant, "execution": execution,
                    "database_before": before, "database_after": database_receipt(database), "histories": values})
            require(phases[0]["execution"]["binary_sha256"] == phases[2]["execution"]["binary_sha256"],
                    "A1 and A2 must execute the same exact preserved compiled binary")
            state["datasets"].append({"customer_tables": tables, "namespace": namespace,
                "prepared_database": prepared_db, "phases": phases})
            write("source-comparison.json", state)
            psql(f'DROP DATABASE "{database}" WITH(FORCE); DROP DATABASE "{template}" WITH(FORCE);')
            owned.remove((database, template))
        activate_variant(state, "B")
        execute(state, "B", 2, str(uuid.uuid4()), "correctness", "candidate-authority-controls")
        state["accepted_same_host_comparison"] = True
        state["performance_claim"] = False
        state["analysis_note"] = "Raw matched distributions and Select control retained. A1/A2 drift and synthetic fixture limits must be assessed before any latency/CPU cause or production improvement claim."
        write("final-outcome.json", state)
    except Exception as error:
        state["accepted_same_host_comparison"] = False; state["error"] = str(error)
        write("final-outcome.json", state)
        raise
    finally:
        cleanup_failures = []
        for database, template in owned:
            try:
                psql(f'DROP DATABASE IF EXISTS "{database}" WITH(FORCE); DROP DATABASE IF EXISTS "{template}" WITH(FORCE);')
            except Exception:
                cleanup_failures.append({"database": database, "template": template,
                                         "status": "owned database cleanup failed"})
        write("cleanup.json", {"failures": cleanup_failures})
        try:
            restore(state)
        except Exception:
            state["accepted_same_host_comparison"] = False
            state["restore_status"] = "corrected tracked checkout restoration failed"
            write("final-outcome.json", state)
            raise
        if cleanup_failures:
            state["accepted_same_host_comparison"] = False
            state["cleanup_status"] = "owned database cleanup failed"
            write("final-outcome.json", state)
            raise RuntimeError("owned fixture cleanup failed; retained receipt; no comparison admission")


def selftest() -> None:
    # Pure receipt/log controls only. These synthetic objects cannot be serving,
    # binary, database, baseline RED or performance evidence.
    log = f"running 1 test\ntest {TEST} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;\n"
    validate_test(log, 0)
    refused = 0
    for altered, code in ((log, 101), (log.replace("1 passed", "0 passed"), 0),
                          (log.replace("0 ignored", "1 ignored"), 0),
                          (log.replace(TEST, "wrong_case"), 0), ("compiler error", 101)):
        try:
            validate_test(altered, code)
        except RuntimeError:
            refused += 1
    rows = [{"rpc": "Login", "worker": 0, "attempt": 0, "start_us": 0,
             "end_us": 20, "latency_us": 20, "ok": True, "code": None},
            {"rpc": "GetCapabilities", "worker": 0, "attempt": 0, "start_us": 5,
             "end_us": 10, "latency_us": 5, "ok": True, "code": None,
             "login_inflight_at_start": 1, "login_inflight_at_end": 1}]
    validate_samples(rows, "Login", 1)
    require(overlap_count(rows, "GetCapabilities") == 1, "actual interval parser control")
    for field, value in (("ok", False), ("attempt", 1), ("latency_us", 0)):
        bad = copy.deepcopy(rows); bad[0][field] = value
        try:
            validate_samples(bad, "Login", 1)
        except RuntimeError:
            refused += 1
    bad = copy.deepcopy(rows); bad[0]["end_us"] = 4
    require(overlap_count(bad, "GetCapabilities") == 0,
            "claimed atomic overlap cannot replace actual completed Login intervals")
    refused += 1
    with tempfile.TemporaryDirectory(prefix="aba-source-control-", dir=Path(__file__).resolve().parent) as temporary:
        directory = Path(temporary)
        receipts = []
        for history in HISTORIES:
            name = f"history-{history}.json"
            path = directory / name
            path.write_text(json.dumps({"history_count": history, "mode": "sample"}))
            receipts.append({"history_count": history, "receipt_file": name,
                             "receipt_sha256": file_sha(path)})
        receipt_log = "".join("CAPABILITIES_HISTORY_PROFILE " + json.dumps(row) + "\n"
                              for row in receipts)
        validate_receipt_files(receipt_log, directory, "sample")
        validate_receipt_files("test " + TEST + " ... " + receipt_log, directory, "sample")
        for altered in (receipt_log.replace(receipts[0]["receipt_sha256"], "0" * 64),
                        receipt_log + receipt_log.splitlines()[0] + "\n",
                        receipt_log.replace('"history_count": 1,', '"history_count": 2,')):
            try:
                validate_receipt_files(altered, directory, "sample")
            except RuntimeError:
                refused += 1
        extra = directory / "unreported.json"
        extra.write_text("{}")
        try:
            validate_receipt_files(receipt_log, directory, "sample")
        except RuntimeError:
            refused += 1
        extra.unlink()
        (directory / "history-1.json").unlink()
        try:
            validate_receipt_files(receipt_log, directory, "sample")
        except RuntimeError:
            refused += 1
    require(refused == 14, "all source-only negative controls must refuse")
    print("capabilities-history-aba: 5 positive and 14 negative source-only controls passed; no runtime evidence")
    quiet_receipt_selftest()


def quiet_receipt_selftest() -> None:
    # Exercise the real psql entrypoint with a standard-library subprocess mock;
    # neither psql nor a database is started by these source-only controls.
    from unittest.mock import patch
    dsn = "postgresql://source-only.invalid/owned"
    good = '{"schema_version":1,"quiet_command_tags":true}\n'
    calls = []

    def capture(command, **kwargs):
        require(command == ["psql", dsn, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At"],
                "psql must use the exact quiet, unaligned, tuples-only, fail-closed argument contract")
        require(kwargs == {"input": sql, "text": True, "stdout": subprocess.PIPE,
                           "stderr": subprocess.PIPE, "timeout": 120},
                "psql must preserve bounded subprocess IO and private SQL input")
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, good, "")

    sql = "DO $quiet_receipt$ BEGIN PERFORM 1; END $quiet_receipt$;\nSELECT json_build_object('schema_version', 1, 'quiet_command_tags', TRUE);\n"
    with patch.dict(os.environ, {"UDB_LIVE_NATIVE_PG_DSN": dsn}), \
            patch.object(subprocess, "run", side_effect=capture):
        require(quiet_receipt_preflight() == json.loads(good) and len(calls) == 1,
                "real psql argument/DO+SELECT receipt positive control failed")
    refused = 0
    for code, stdout in ((1, good), (0, "DO\n" + good), (0, ""),
                         (0, '{"schema_version":1,"quiet_command_tags":false}'),
                         (0, '{"schema_version":2,"quiet_command_tags":true}'),
                         (0, good + good)):
        with patch.dict(os.environ, {"UDB_LIVE_NATIVE_PG_DSN": dsn}), \
                patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], code, stdout, "private-error")):
            try:
                quiet_receipt_preflight()
            except (RuntimeError, json.JSONDecodeError):
                refused += 1
    require(refused == 6, "all quiet receipt/error negative controls must refuse")
    print("capabilities-history-aba: 1 exact-argument positive and 6 quiet-receipt negative source-only controls passed; no database evidence")


if __name__ == "__main__":
    require(len(sys.argv) == 2 and sys.argv[1] in ("run", "selftest"), "expected run or selftest")
    {"run": run, "selftest": selftest}[sys.argv[1]]()
