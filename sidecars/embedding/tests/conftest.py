import sys
from pathlib import Path

SIDECAR_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SIDECAR_DIR.parents[1]

for path in (SIDECAR_DIR, REPO_ROOT / "sdk" / "python" / "gen"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
