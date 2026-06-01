"""Regenerate the Python SDK protobuf bindings.

The canonical generator is the repo-root ``buf.gen.yaml``. ``--include-imports``
vendors the ``google/api`` annotation stubs (which the native control-plane
gRPC stubs import) into ``sdk/python/gen`` so the published wheel is
self-contained and needs no external ``googleapis`` dependency.

The committed ``sdk/python/gen`` tree is the source of truth — CI hard-fails on
drift against ``buf generate --include-imports`` — so this script just refreshes
it locally. It regenerates every SDK (buf emits all languages); only
``sdk/python/gen`` is consumed by the Python build.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def main() -> int:
    if shutil.which("buf") is None:
        print(
            "buf was not found on PATH. Install it from https://buf.build and "
            "re-run, or run `buf generate --include-imports` from the repo root.",
            file=sys.stderr,
        )
        return 1
    return subprocess.run(
        ["buf", "generate", "--include-imports"], cwd=ROOT
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
