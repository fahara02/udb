#!/usr/bin/env python3
"""Deterministic benchmark-dataset generator for UDB perf work (D.2).

Writes a realistic, varied dataset under `data/` for the Criterion hot-path
benches (`benches/hotpath_bench.rs`). Everything is seeded, so the same
`--target-mb` reproduces byte-identical files. Total stays <= 1 GiB (hard cap).

Files produced (NDJSON unless noted), each line one record:
  records.ndjson        varied structured rows (str/int/float/bool/null/nested/
                        array/base64) — exercises struct<->json conversion.
  dispatch_sql.ndjson   {"sql":..,"params":[..]}            (parse_sql_dispatch)
  dispatch_object.ndjson{"op":..,"bucket":..,"key":..,"data_base64":..}
                                                            (parse_object_dispatch
                                                             + object_bytes_from_json)
  dispatch_rest.ndjson  {"path":..,"method":..,"body":{..}} (parse_rest_dispatch)
  blobs_b64.ndjson      {"size":n,"b64":"…"} varied-size base64 (codec benches)
  MANIFEST.json         counts + byte sizes (benches read this; asserts <= cap)

Usage:  python data/gen_bench_data.py [--target-mb 512]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import sys

HARD_CAP_BYTES = 1024 * 1024 * 1024  # 1 GiB — never exceed.
SEED = 20260602  # fixed seed for reproducibility

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

WORDS = (
    "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima "
    "mike november oscar papa quebec romeo sierra tango uniform victor whiskey"
).split()


def rnd_words(rng: random.Random, n: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(n))


def make_record(rng: random.Random, i: int) -> dict:
    """A varied row hitting every prost Value/Kind variant."""
    return {
        "id": "%032x" % rng.getrandbits(128),
        "tenant_id": "tenant-%d" % (i % 64),
        "project_id": "proj-%d" % (i % 16),
        "seq": i,
        "score": round(rng.uniform(-1e6, 1e6), 4),
        "ratio": rng.random(),
        "active": bool(rng.getrandbits(1)),
        "deleted": None if rng.getrandbits(2) else False,
        "tags": [rng.choice(WORDS) for _ in range(rng.randint(0, 5))],
        "counts": [rng.randint(0, 1_000_000) for _ in range(rng.randint(0, 4))],
        "metadata": {
            "kind": rng.choice(WORDS),
            "n": rng.randint(0, 1 << 30),
            "flag": bool(rng.getrandbits(1)),
            "nested": {"depth": 2, "label": rnd_words(rng, 2)},
        },
        "note": rnd_words(rng, rng.randint(3, 24)),
        "payload_b64": base64.b64encode(rng.randbytes(rng.randint(8, 96))).decode(),
        "created_at": "2026-%02d-%02dT%02d:%02d:%02dZ"
        % (1 + i % 12, 1 + i % 28, i % 24, i % 60, i % 60),
    }


def make_sql(rng: random.Random, i: int) -> dict:
    return {
        "sql": "SELECT id, seq, score, note FROM app.events "
        "WHERE tenant_id = $1 AND seq > $2 ORDER BY seq DESC LIMIT $3",
        "params": ["tenant-%d" % (i % 64), i, rng.randint(10, 500)],
    }


def make_object(rng: random.Random, i: int) -> dict:
    blob = rng.randbytes(rng.randint(64, 4096))
    return {
        "op": rng.choice(["put", "get", "delete"]),
        "bucket": "bucket-%d" % (i % 32),
        "key": "t/tenant-%d/obj/%08d.bin" % (i % 64, i),
        "content_type": "application/octet-stream",
        "data_base64": base64.b64encode(blob).decode(),
    }


def make_rest(rng: random.Random, i: int) -> dict:
    return {
        "method": rng.choice(["POST", "GET", "PUT", "DELETE"]),
        "path": "/v1/indexes/idx-%d/query" % (i % 8),
        "body": {
            "topK": rng.randint(1, 100),
            "vector": [round(rng.random(), 5) for _ in range(rng.choice([8, 16, 32]))],
            "filter": {"tenant_id": "tenant-%d" % (i % 64)},
        },
    }


def make_blob(rng: random.Random, i: int) -> dict:
    # Sizes spanning the codec regimes: tiny, small, page, large.
    size = rng.choice([16, 256, 4096, 65536])
    return {"size": size, "b64": base64.b64encode(rng.randbytes(size)).decode()}


def write_ndjson(path: str, budget: int, make, rng: random.Random) -> tuple[int, int]:
    """Write NDJSON lines from `make(rng, i)` until ~budget bytes. Returns
    (line_count, bytes_written)."""
    count = 0
    written = 0
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        while written < budget:
            line = json.dumps(make(rng, count), separators=(",", ":")) + "\n"
            fh.write(line)
            written += len(line.encode("utf-8"))
            count += 1
    return count, written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-mb", type=int, default=512,
                    help="approx total dataset size in MiB (clamped to [16, 1000])")
    args = ap.parse_args()
    target = max(16, min(args.target_mb, 1000)) * 1024 * 1024
    if target > HARD_CAP_BYTES:
        target = HARD_CAP_BYTES

    # Proportions across the file kinds (records dominate; object dispatch
    # carries base64 blobs).
    plan = [
        ("records.ndjson", 0.60, make_record),
        ("dispatch_object.ndjson", 0.18, make_object),
        ("blobs_b64.ndjson", 0.10, make_blob),
        ("dispatch_sql.ndjson", 0.06, make_sql),
        ("dispatch_rest.ndjson", 0.06, make_rest),
    ]
    rng = random.Random(SEED)
    manifest = {"seed": SEED, "target_bytes": target, "files": {}}
    total = 0
    for name, frac, make in plan:
        path = os.path.join(DATA_DIR, name)
        count, written = write_ndjson(path, int(target * frac), make, rng)
        manifest["files"][name] = {"records": count, "bytes": written}
        total += written
        print(f"  {name}: {count:,} records, {written/1024/1024:.1f} MiB")
    manifest["total_bytes"] = total
    if total > HARD_CAP_BYTES:
        print(f"ERROR: total {total} exceeds 1 GiB cap", file=sys.stderr)
        return 1
    with open(os.path.join(DATA_DIR, "MANIFEST.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    print(f"Total: {total/1024/1024:.1f} MiB across {len(plan)} files (cap 1024 MiB).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
