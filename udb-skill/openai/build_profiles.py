"""Publish the canonical UDB instructions as runnable Responses API payloads."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.request


DEFAULT_MODEL = "gpt-4o-mini"
RESPONSES_URL = "https://api.openai.com/v1/responses"
SMOKE_MAX_OUTPUT_TOKENS = 128
PROFILES = (
    ("udb-assistant", "instructions.md", "Name the UDB TypeScript SDK package."),
    (
        "udb-coding",
        "instructions-udb-coding.md",
        "Name the source of truth for UDB's native service contracts.",
    ),
)


def verify_response(payload: dict, api_key: str) -> None:
    request = urllib.request.Request(
        RESPONSES_URL,
        data=json.dumps({**payload, "max_output_tokens": SMOKE_MAX_OUTPUT_TOKENS}).encode(),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                result = json.load(response)
            break
        except urllib.error.HTTPError as exc:
            # Report only the machine-readable code, never the free-form body.
            try:
                code = json.load(exc).get("error", {}).get("code") or "unknown"
            except (ValueError, AttributeError):
                code = "unknown"
            if not isinstance(code, str) or not re.fullmatch(r"[a-zA-Z0-9_]{1,64}", code):
                code = "unknown"
            if exc.code == 429 and code in ("rate_limit_exceeded", "unknown") and attempt < 2:
                time.sleep(5 * (attempt + 1))
                continue
            raise RuntimeError(
                f"Responses API verification failed: HTTP {exc.code} ({code})"
            ) from None
    if result.get("status") != "completed" or not any(
        content.get("type") == "output_text" and content.get("text", "").strip()
        for item in result.get("output", [])
        for content in item.get("content", [])
    ):
        raise RuntimeError("Responses API did not complete with text output")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--model", default=os.environ.get("OPENAI_MODEL") or DEFAULT_MODEL)
    parser.add_argument("--verify-api", action="store_true")
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    versions = json.loads((source.parents[1] / "versions.json").read_text(encoding="utf-8"))
    args.output.mkdir(parents=True, exist_ok=True)
    assets = []
    for slug, filename, question in PROFILES:
        instructions = (source / filename).read_text(encoding="utf-8")
        if not instructions.strip():
            raise RuntimeError(f"Empty OpenAI instructions: {filename}")
        payload = {"model": args.model, "instructions": instructions, "input": question, "store": False}
        if args.verify_api:
            api_key = os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                raise RuntimeError("OPENAI_API_KEY is required for API verification")
            verify_response(payload, api_key)
        asset = args.output / f"{slug}-responses.json"
        asset.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        assets.append({"file": asset.name, "sha256": hashlib.sha256(asset.read_bytes()).hexdigest()})
        print(f"Built {asset.name}" + ("; Responses API verified" if args.verify_api else ""))
    manifest = {"version": versions["components"]["udb"]["version"], "model": args.model, "assets": assets}
    (args.output / "udb-openai-responses-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
