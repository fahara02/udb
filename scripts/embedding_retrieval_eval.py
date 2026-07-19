#!/usr/bin/env python3
"""Evaluate an embedding retrieval golden set and fail on quality regression."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = ROOT / "tests" / "fixtures" / "embedding-retrieval-golden.json"


class EvaluationError(RuntimeError):
    pass


def relevance_map(value: Any) -> dict[str, float]:
    if isinstance(value, list):
        return {str(item): 1.0 for item in value}
    if isinstance(value, dict):
        return {str(key): float(score) for key, score in value.items() if float(score) > 0}
    raise EvaluationError("query relevant must be an array or id-to-grade object")


def evaluate_query(query: dict[str, Any]) -> tuple[float, float, float]:
    relevant = relevance_map(query.get("relevant"))
    ranked = [str(item) for item in query.get("ranked", [])]
    k = max(1, int(query.get("k") or 10))
    top = ranked[:k]
    recall = len(set(top) & set(relevant)) / max(1, len(relevant))
    reciprocal_rank = next((1.0 / rank for rank, item in enumerate(top, 1) if item in relevant), 0.0)
    dcg = sum((relevant.get(item, 0.0) / math.log2(rank + 1)) for rank, item in enumerate(top, 1))
    ideal = sorted(relevant.values(), reverse=True)[:k]
    ideal_dcg = sum(score / math.log2(rank + 1) for rank, score in enumerate(ideal, 1))
    ndcg = dcg / ideal_dcg if ideal_dcg else 0.0
    return recall, ndcg, reciprocal_rank


def evaluate(fixture: dict[str, Any]) -> dict[str, float]:
    queries = fixture.get("queries")
    if not isinstance(queries, list) or not queries:
        raise EvaluationError("fixture queries must be a non-empty array")
    metrics = [evaluate_query(query) for query in queries if isinstance(query, dict)]
    if len(metrics) != len(queries):
        raise EvaluationError("every query must be an object")
    count = float(len(metrics))
    return {
        "recall_at_k": sum(metric[0] for metric in metrics) / count,
        "ndcg_at_k": sum(metric[1] for metric in metrics) / count,
        "mrr_at_k": sum(metric[2] for metric in metrics) / count,
        "queries": count,
    }


def enforce(metrics: dict[str, float], thresholds: dict[str, Any]) -> None:
    failures = []
    for name in ("recall_at_k", "ndcg_at_k", "mrr_at_k"):
        minimum = float(thresholds.get(name, 0.0))
        if metrics[name] + 1e-12 < minimum:
            failures.append(f"{name}={metrics[name]:.6f} < {minimum:.6f}")
    if failures:
        raise EvaluationError("retrieval quality regression: " + ", ".join(failures))


def selftest() -> None:
    fixture = {
        "queries": [
            {"relevant": {"a": 2, "b": 1}, "ranked": ["a", "x", "b"], "k": 3},
            {"relevant": ["c"], "ranked": ["x", "c"], "k": 2},
        ]
    }
    metrics = evaluate(fixture)
    if metrics["recall_at_k"] != 1.0 or not (0.0 < metrics["mrr_at_k"] < 1.0):
        raise EvaluationError(f"metric selftest failed: {metrics}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", nargs="?", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()
    if args.selftest:
        selftest()
        print("embedding retrieval evaluation selftest passed")
        return 0
    fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    metrics = evaluate(fixture)
    enforce(metrics, fixture.get("thresholds", {}))
    print(json.dumps(metrics, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (EvaluationError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")))
        raise SystemExit(1)
