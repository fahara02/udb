"""Recursion guard for the ``udb`` console-script launcher (:mod:`udb_client._cli`).

Inside a venv the ``udb`` console script on PATH is this launcher. Probing a
launcher with ``--version`` used to start the launcher again, which probed PATH
again: unbounded recursion. These tests put launcher copies and wrappers on
PATH and count every process the fake binary and the wrappers see.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from udb_client import _cli

SDK_ROOT = Path(__file__).resolve().parents[1]
POSIX = os.name != "nt"


def _write_exec(path: Path, body: str) -> Path:
    path.write_text(body)
    path.chmod(0o755)
    return path


def _lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text().splitlines() if line]


def test_nested_launcher_never_probes_path(monkeypatch, tmp_path) -> None:
    _write_exec(tmp_path / "udb", "not a launcher")
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv(_cli._LAUNCHER_ENV, "1")

    def _boom(binary: str) -> bool:  # pragma: no cover - must not be called
        raise AssertionError(f"nested launcher probed {binary}")

    monkeypatch.setattr(_cli, "_binary_version_matches", _boom)
    assert _cli._path_binary() is None


def test_launcher_copies_and_console_scripts_are_not_candidates(monkeypatch, tmp_path) -> None:
    console = _write_exec(
        tmp_path / "console-udb",
        "#!/usr/bin/env python\nimport sys\nfrom udb_client._cli import main\nsys.exit(main())\n",
    )
    copy = _write_exec(tmp_path / "copy-udb", Path(_cli.__file__).read_text())
    real = _write_exec(tmp_path / "real-udb", "#!/bin/sh\necho udb\n")
    monkeypatch.setattr(sys, "argv", [str(tmp_path / "self-udb")])
    selfish = _write_exec(tmp_path / "self-udb", "#!/bin/sh\n")

    assert _cli._is_launcher_candidate(str(console))
    assert _cli._is_launcher_candidate(str(copy))
    assert _cli._is_launcher_candidate(str(selfish))
    assert _cli._is_launcher_candidate(str(tmp_path / "missing"))
    assert not _cli._is_launcher_candidate(str(real))


def test_path_candidates_lists_every_match_in_order(monkeypatch, tmp_path) -> None:
    exe = "udb.exe" if os.name == "nt" else "udb"
    d1, d2, empty = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    for d in (d1, d2, empty):
        d.mkdir()
    _write_exec(d1 / exe, "one")
    _write_exec(d2 / exe, "two")
    monkeypatch.setenv("PATH", os.pathsep.join(map(str, (d1, empty, d2, d1))))
    got = [Path(p).parent for p in _cli._path_candidates()]
    assert got == [d1, d2]


def _rig(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    record = tmp_path / "record.log"
    xdg = tmp_path / "xdg"
    cache = xdg / "udb" / "bin" / _cli.UDB_VERSION
    cache.mkdir(parents=True)
    _write_exec(
        cache / "udb",
        f"#!/bin/sh\necho \"fake guard=$UDB_LAUNCHER_ACTIVE $*\" >> '{record}'\n"
        f"if [ \"$1\" = \"--version\" ]; then echo \"udb {_cli.UDB_VERSION}\"; exit 0; fi\nexit 3\n",
    )
    env = {k: v for k, v in os.environ.items() if k not in ("UDB_BIN", _cli._LAUNCHER_ENV)}
    env["XDG_CACHE_HOME"] = str(xdg)
    env["UDB_SKIP_DOWNLOAD"] = "1"
    # Same import roots as pytest's `pythonpath = [".", "gen"]`.
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_ROOT), str(SDK_ROOT / "gen")])
    return record, cache, env


def _console_script(path: Path, record: Path) -> Path:
    return _write_exec(
        path,
        f"#!{sys.executable}\nimport sys\n"
        f"open({str(record)!r}, 'a').write('console\\n')\n"
        "from udb_client._cli import main\nsys.exit(main())\n",
    )


def _run(script: Path, path_dirs: list[Path], env: dict[str, str], *args: str):
    env = dict(env)
    env["PATH"] = os.pathsep.join([*map(str, path_dirs), str(Path(sys.executable).parent), "/usr/bin", "/bin"])
    started = time.monotonic()
    res = subprocess.run([str(script), *args], env=env, capture_output=True, text=True, timeout=30)
    return res, time.monotonic() - started


@pytest.mark.skipif(not POSIX, reason="fake binary and console script are shell/shebang scripts")
def test_console_script_on_path_is_skipped(tmp_path) -> None:
    record, _cache, env = _rig(tmp_path)
    venv_bin = tmp_path / "venv" / "bin"
    venv_bin.mkdir(parents=True)
    other_bin = tmp_path / "other"
    other_bin.mkdir()
    me = _console_script(venv_bin / "udb", record)
    _console_script(other_bin / "udb", record)

    res, elapsed = _run(me, [venv_bin, other_bin], env, "status", "--x")
    assert elapsed < 10, f"launcher took {elapsed:.1f}s"
    assert res.returncode == 3, res.stderr
    # The launcher ran once (itself); no other launcher was started; the cached
    # binary was exec'd once, carrying the guard variable.
    assert _lines(record) == ["console", "fake guard=1 status --x"]


@pytest.mark.skipif(not POSIX, reason="fake binary and wrapper are shell scripts")
def test_unrecognised_wrapper_is_probed_a_bounded_number_of_times(tmp_path) -> None:
    record, _cache, env = _rig(tmp_path)
    hidden = tmp_path / "hidden"
    hidden.mkdir()
    launcher = _console_script(hidden / "udb-launcher", record)
    wrap = tmp_path / "wrap"
    wrap.mkdir()
    # A wrapper that names neither this package nor the guard variable.
    _write_exec(wrap / "udb", f"#!/bin/sh\necho wrapper >> '{record}'\nexec '{launcher}' \"$@\"\n")
    entry = _console_script(tmp_path / "entry-udb", record)

    res, elapsed = _run(entry, [wrap], env, "ping")
    assert res.returncode == 3, res.stderr
    lines = _lines(record)
    # Probe: wrapper -> nested launcher (skips PATH) -> cached binary --version.
    # Exec: the same chain once more with the real args. A slow interpreter
    # start can hit the 5 s probe timeout, in which case the launcher falls
    # back to the cached binary directly; either way the count is bounded.
    assert lines.count("wrapper") <= 2, lines
    assert lines.count("console") <= 3, lines
    assert len(lines) <= 7, lines
    assert lines[-1] == "fake guard=1 ping", lines
    assert elapsed < 30, f"launcher took {elapsed:.1f}s"
