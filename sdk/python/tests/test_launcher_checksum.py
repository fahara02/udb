"""Launcher checksum-verification unit tests (M6).

Exercises the pure helpers in :mod:`udb_client._cli` (manifest parsing, file
hashing, and the verify orchestration) without touching the network — the
HTTP fetch is monkeypatched.
"""

from __future__ import annotations

import hashlib

import pytest

from udb_client import _cli


def _write(tmp_path, data: bytes):
    path = tmp_path / "udb.bin"
    path.write_bytes(data)
    return str(path), hashlib.sha256(data).hexdigest()


def test_sha256_file(tmp_path) -> None:
    path, expected = _write(tmp_path, b"hello udb")
    assert _cli._sha256_file(path) == expected


def test_manifest_two_column_sha256sum_format() -> None:
    text = "abc123  udb-v0.3.2-linux-x86_64\nffffff  other-asset\n"
    assert (
        _cli._expected_sha_from_manifest(text, "udb-v0.3.2-linux-x86_64") == "abc123"
    )


def test_manifest_binary_mode_and_path_prefix() -> None:
    text = "deadbeef *dist/udb-v0.3.2-windows-x86_64.exe\n"
    got = _cli._expected_sha_from_manifest(text, "udb-v0.3.2-windows-x86_64.exe")
    assert got == "deadbeef"


def test_manifest_single_column_fallback() -> None:
    # A per-asset `.sha256` file may contain only the digest.
    assert _cli._expected_sha_from_manifest("ABCDEF\n", "anything") == "abcdef"


def test_verify_checksum_match(tmp_path, monkeypatch) -> None:
    path, digest = _write(tmp_path, b"binary-bytes")
    asset = _cli._asset_name()
    monkeypatch.setattr(
        _cli, "_fetch_text", lambda url: f"{digest}  {asset}\n" if url.endswith(f"{asset}.sha256") else None
    )
    monkeypatch.delenv("UDB_SKIP_CHECKSUM", raising=False)
    # No exception → verified.
    _cli._verify_checksum(path, asset)


def test_verify_checksum_mismatch_aborts(tmp_path, monkeypatch) -> None:
    path, _digest = _write(tmp_path, b"binary-bytes")
    asset = _cli._asset_name()
    monkeypatch.setattr(_cli, "_fetch_text", lambda url: f"{'0' * 64}  {asset}\n")
    monkeypatch.delenv("UDB_SKIP_CHECKSUM", raising=False)
    with pytest.raises(SystemExit) as exc:
        _cli._verify_checksum(path, asset)
    assert "checksum mismatch" in str(exc.value)


def test_verify_checksum_absent_manifest_is_best_effort(tmp_path, monkeypatch) -> None:
    path, _digest = _write(tmp_path, b"binary-bytes")
    asset = _cli._asset_name()
    monkeypatch.setattr(_cli, "_fetch_text", lambda url: None)
    monkeypatch.delenv("UDB_SKIP_CHECKSUM", raising=False)
    # Missing manifest must not raise (warn + continue).
    _cli._verify_checksum(path, asset)


def test_verify_checksum_skipped_by_env(tmp_path, monkeypatch) -> None:
    path, _digest = _write(tmp_path, b"binary-bytes")
    asset = _cli._asset_name()

    def _boom(url):  # pragma: no cover - must not be called
        raise AssertionError("network fetch attempted despite UDB_SKIP_CHECKSUM")

    monkeypatch.setattr(_cli, "_fetch_text", _boom)
    monkeypatch.setenv("UDB_SKIP_CHECKSUM", "1")
    _cli._verify_checksum(path, asset)
