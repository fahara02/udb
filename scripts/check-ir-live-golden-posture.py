#!/usr/bin/env python3
"""Fail CI if live IR-golden backend coverage can silently drift."""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ProvisionedBackend:
    label: str
    module: str
    source_file: str
    services: tuple[str, ...]
    ci_env_tokens: tuple[str, ...]
    consumed_env_tokens: tuple[str, ...]
    extra_consumer_files: tuple[str, ...] = ()


PROVISIONED_BACKENDS: tuple[ProvisionedBackend, ...] = (
    ProvisionedBackend("postgres", "postgres_live", "postgres_live.rs", ("postgres",), ("UDB_PG_DSN",), ("UDB_PG_DSN",)),
    ProvisionedBackend("sqlite", "sqlite_live", "sqlite_live.rs", (), (), ()),
    ProvisionedBackend("mysql", "mysql_live", "mysql_live.rs", ("mysql",), ("UDB_MYSQL_DSN",), ("UDB_MYSQL_DSN",)),
    ProvisionedBackend("mssql", "mssql_live", "mssql_live.rs", ("mssql",), ("UDB_MSSQL_DSN",), ("UDB_MSSQL_DSN",)),
    ProvisionedBackend("mongodb", "mongodb_live", "mongodb_live.rs", ("mongodb",), ("UDB_NOSQL_DSN", "UDB_MONGODB_DSN"), ("UDB_NOSQL_DSN",)),
    ProvisionedBackend("cassandra", "cassandra_live", "cassandra_live.rs", ("cassandra",), ("UDB_CASSANDRA_DSN",), ("UDB_CASSANDRA_DSN",)),
    ProvisionedBackend("neo4j", "neo4j_live", "neo4j_live.rs", ("neo4j",), ("UDB_GRAPH_HTTP_URL", "UDB_GRAPH_USER", "UDB_GRAPH_PASSWORD"), ("UDB_GRAPH_HTTP_URL", "UDB_GRAPH_USER", "UDB_GRAPH_PASSWORD"), ("src/runtime/executors/neo4j.rs",)),
    ProvisionedBackend("clickhouse", "clickhouse_live", "clickhouse_live.rs", ("clickhouse",), ("UDB_CLICKHOUSE_DSN", "UDB_COLUMN_USER", "UDB_COLUMN_PASSWORD", "UDB_COLUMN_DATABASE"), ("UDB_CLICKHOUSE_DSN", "UDB_COLUMN_USER", "UDB_COLUMN_PASSWORD", "UDB_COLUMN_DATABASE")),
    ProvisionedBackend("elasticsearch", "elasticsearch_live", "elasticsearch_live.rs", ("elasticsearch",), ("UDB_ELASTIC_DSN",), ("UDB_ELASTIC_DSN",)),
    ProvisionedBackend("weaviate", "weaviate_live", "weaviate_live.rs", ("weaviate",), ("UDB_WEAVIATE_DSN",), ("UDB_WEAVIATE_DSN",)),
    ProvisionedBackend("qdrant", "qdrant_live", "qdrant_live.rs", ("qdrant",), ("UDB_QDRANT_URL",), ("UDB_QDRANT_URL",)),
    ProvisionedBackend("redis", "redis_live", "redis_live.rs", ("redis",), ("UDB_REDIS_DSN",), ("UDB_REDIS_DSN",)),
    ProvisionedBackend("memcached", "memcached_live", "memcached_live.rs", ("memcached",), ("UDB_MEMCACHED_DSN",), ("UDB_MEMCACHED_DSN",)),
    ProvisionedBackend("s3/minio", "s3_live", "s3_live.rs", ("minio",), ("UDB_INTEGRATION_MINIO_ENDPOINT", "UDB_INTEGRATION_MINIO_ACCESS_KEY", "UDB_INTEGRATION_MINIO_SECRET_KEY"), (), ("src/runtime/executors/object_stream_live_tests.rs",)),
)

EXTERNAL_BACKENDS: tuple[str, ...] = (
    "azureblob_live",
    "gcs_live",
    "pinecone_live",
)


def _read(root: Path, path: str) -> str:
    return (root / path).read_text(encoding="utf-8")


def _require(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle not in text:
        failures.append(f"missing {label}: {needle}")


def _reject(text: str, needle: str, label: str, failures: list[str]) -> None:
    if needle in text:
        failures.append(f"forbidden {label}: {needle}")


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    ci = _read(root, ".github/workflows/ci.yml")
    mod_rs = _read(root, "src/ir/compile/live_tests/mod.rs")
    support = _read(root, "src/ir/compile/live_tests/support.rs")
    plan = _read(root, "UDB_MASTERPLAN_2026.md")

    _require(
        ci,
        "docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis memcached qdrant minio",
        "integration provisioned backend stack",
        failures,
    )
    _require(
        ci,
        "docker compose -f docker-compose.canonical.yml up -d --wait mysql mssql mongodb cassandra neo4j clickhouse elasticsearch weaviate",
        "canonical provisioned backend stack",
        failures,
    )
    _require(
        ci,
        "curl -fsS http://127.0.0.1:58080/v1/.well-known/ready",
        "Weaviate readiness gate",
        failures,
    )
    _require(ci, 'UDB_IR_LIVE_GOLDEN_TESTS: "1"', "IR live-golden enable flag", failures)
    _require(
        ci,
        "cargo test --locked --lib ir::compile::live_tests -- --ignored --nocapture --test-threads=1",
        "ignored live IR-golden test invocation",
        failures,
    )
    _require(
        ci,
        "python3 scripts/check-ir-live-golden-posture.py",
        "CI quick-gate IR posture guard",
        failures,
    )
    _require(
        support,
        'std::env::var("UDB_IR_LIVE_GOLDEN_TESTS")',
        "shared live IR enable helper",
        failures,
    )

    for backend in PROVISIONED_BACKENDS:
        _require(mod_rs, f"mod {backend.module};", f"{backend.label} live-test module", failures)
        live_file = _read(root, f"src/ir/compile/live_tests/{backend.source_file}")
        consumer_source = live_file + "\n" + "\n".join(
            _read(root, path) for path in backend.extra_consumer_files
        )
        _require(live_file, "#[ignore", f"{backend.label} ignored live-test marker", failures)
        _require(live_file, "live_ir_enabled()", f"{backend.label} live enable guard", failures)
        for service in backend.services:
            _require(ci, service, f"{backend.label} provisioned service", failures)
        for env_token in backend.ci_env_tokens:
            _require(ci, env_token, f"{backend.label} CI env", failures)
        for env_token in backend.consumed_env_tokens:
            _require(consumer_source, env_token, f"{backend.label} env consumer", failures)

    for module in EXTERNAL_BACKENDS:
        _require(mod_rs, f"mod {module};", f"{module} external module", failures)
    _require(ci, "External-only backends without CI", "external backend honest-skip comment", failures)
    _reject(ci, "UDB_AZUREBLOB_DSN:", "Azure Blob CI credential injection", failures)
    _reject(ci, "UDB_GCS_DSN:", "GCS CI credential injection", failures)
    _reject(ci, "UDB_PINECONE_DSN:", "Pinecone CI credential injection", failures)
    _require(plan, "IR live-golden posture guard", "masterplan IR guard note", failures)
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / ".github/workflows").mkdir(parents=True)
        live = root / "src/ir/compile/live_tests"
        live.mkdir(parents=True)
        (live / "support.rs").write_text(
            'std::env::var("UDB_IR_LIVE_GOLDEN_TESTS");\n',
            encoding="utf-8",
        )
        mod_text = "\n".join(f"mod {backend.module};" for backend in PROVISIONED_BACKENDS)
        mod_text += "\nmod azureblob_live;\nmod gcs_live;\nmod pinecone_live;\n"
        (live / "mod.rs").write_text(mod_text, encoding="utf-8")
        for backend in PROVISIONED_BACKENDS:
            tokens = "\n".join(
                token for token in backend.consumed_env_tokens
            )
            (live / backend.source_file).write_text(
                f"#[ignore]\nfn t() {{ live_ir_enabled(); {tokens}; }}\n",
                encoding="utf-8",
            )
        (root / "src/runtime/executors").mkdir(parents=True)
        (root / "src/runtime/executors/neo4j.rs").write_text(
            "UDB_GRAPH_HTTP_URL\nUDB_GRAPH_USER\nUDB_GRAPH_PASSWORD\n",
            encoding="utf-8",
        )
        (root / "src/runtime/executors/object_stream_live_tests.rs").write_text(
            "UDB_INTEGRATION_MINIO_ENDPOINT\n",
            encoding="utf-8",
        )
        ci = """
run: docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis memcached qdrant minio
run: docker compose -f docker-compose.canonical.yml up -d --wait mysql mssql mongodb cassandra neo4j clickhouse elasticsearch weaviate
run: curl -fsS http://127.0.0.1:58080/v1/.well-known/ready
run: python3 scripts/check-ir-live-golden-posture.py
# External-only backends without CI credentials skip honestly.
env:
  UDB_IR_LIVE_GOLDEN_TESTS: "1"
  UDB_PG_DSN: x
  UDB_MYSQL_DSN: x
  UDB_MSSQL_DSN: x
  UDB_NOSQL_DSN: x
  UDB_MONGODB_DSN: x
  UDB_CASSANDRA_DSN: x
  UDB_GRAPH_HTTP_URL: x
  UDB_GRAPH_USER: x
  UDB_GRAPH_PASSWORD: x
  UDB_CLICKHOUSE_DSN: x
  UDB_COLUMN_USER: x
  UDB_COLUMN_PASSWORD: x
  UDB_COLUMN_DATABASE: x
  UDB_ELASTIC_DSN: x
  UDB_WEAVIATE_DSN: x
  UDB_QDRANT_URL: x
  UDB_REDIS_DSN: x
  UDB_MEMCACHED_DSN: x
  UDB_INTEGRATION_MINIO_ENDPOINT: x
  UDB_INTEGRATION_MINIO_ACCESS_KEY: x
  UDB_INTEGRATION_MINIO_SECRET_KEY: x
run: cargo test --locked --lib ir::compile::live_tests -- --ignored --nocapture --test-threads=1
"""
        (root / ".github/workflows/ci.yml").write_text(ci, encoding="utf-8")
        (root / "UDB_MASTERPLAN_2026.md").write_text(
            "IR live-golden posture guard\n",
            encoding="utf-8",
        )
        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        (root / ".github/workflows/ci.yml").write_text(
            ci.replace("UDB_WEAVIATE_DSN: x\n", ""),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("weaviate CI env" in failure for failure in failures):
            raise AssertionError(f"expected missing-Weaviate-env failure, got {failures}")

    print("IR live-golden posture selftest passed")
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
    print(f"IR live-golden posture guard passed ({len(PROVISIONED_BACKENDS)} provisioned backends)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
