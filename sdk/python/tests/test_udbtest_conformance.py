"""The typed-store contract against the in-memory fake: the Go contract ported case by
case, plus the shared ``sdk/conformance/table-cases.json`` that every SDK runner reads."""

from __future__ import annotations

from pathlib import Path

import pytest

from udb.core.storage.entity.v1 import file_pb2
from udb_client import udbtest
from udb_client.udbtest import conformance

CASES_FILE = Path(__file__).resolve().parents[2] / "conformance" / "table-cases.json"


@pytest.fixture
def connect():
    with udbtest.new(file_pb2.File) as fake:
        yield fake.client


@pytest.mark.parametrize("index", range(len(conformance._CONTRACT)), ids=[name for name, _ in conformance._CONTRACT])
def test_table_contract(connect, index: int) -> None:
    list(conformance.table_contract(connect))[index][1]()


@pytest.mark.parametrize("case", conformance.load_table_cases(CASES_FILE), ids=lambda case: case["name"])
def test_shared_table_cases(connect, case) -> None:
    conformance.run_table_case(connect, case)


def test_run_table_runs_the_whole_contract(connect) -> None:
    conformance.run_table(connect)
    conformance.run_table_cases(connect, CASES_FILE)


def test_run_table_reports_a_store_that_breaks_the_contract() -> None:
    class Broken:
        """Connects to nothing, so every case must fail rather than pass quietly."""

        def __call__(self, tenant: str):
            raise RuntimeError(f"no broker for {tenant}")

    with pytest.raises(AssertionError, match="conformance case"):
        conformance.run_table(Broken())


def test_conformance_refuses_tenants_that_are_not_two_distinct() -> None:
    with pytest.raises(ValueError):
        list(conformance.table_contract(lambda tenant: None, ("same", "same")))
