from __future__ import annotations

from pathlib import Path
import shutil
import sys

import grpc_tools
from grpc_tools import protoc


PROJECT = Path(__file__).resolve().parents[1]
PROTO = PROJECT / "proto"
GEN = PROJECT / "gen"
WELL_KNOWN_TYPES = Path(grpc_tools.__file__).resolve().parent / "_proto"


def main() -> int:
    if GEN.exists():
        shutil.rmtree(GEN)
    GEN.mkdir(parents=True, exist_ok=True)

    proto_file = PROTO / "acme" / "billing" / "v1" / "acme_billing_v1.proto"
    code = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{PROTO}",
            f"-I{WELL_KNOWN_TYPES}",
            f"--python_out={GEN}",
            f"--pyi_out={GEN}",
            str(proto_file),
        ]
    )
    if code != 0:
        return code

    for directory in [
        GEN,
        GEN / "acme",
        GEN / "acme" / "billing",
        GEN / "acme" / "billing" / "v1",
    ]:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "__init__.py").touch()
    return 0


if __name__ == "__main__":
    sys.exit(main())
