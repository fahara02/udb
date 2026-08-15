# Bug report: generated UDB skill wrappers retained 0.5.6 guidance

Date: 2026-08-15
Affected release process: 0.5.8 preparation
Severity: published documentation and release availability

## Observed

The 0.5.8 version propagation updated the canonical
`udb-skill/shared/using-udb.md`, but did not regenerate three derived copies:

- the plugin-local `using-udb` reference;
- the OpenAI instructions;
- the Ollama Modelfile.

All three still described UDB 0.5.6 and supplied 0.5.6 install commands. PR CI
did not run the skill synchronization checks; they ran only in the main-push
publisher, which rejected main run `31877566060` after merge.

## Impact

Publishing the skill would either fail after every release merge or distribute
stale SDK install guidance. The release PR could appear green even though a
governed, public-facing artifact was out of sync.

## Required correction

- Regenerate all `using-udb` wrappers from the canonical 0.5.8 source.
- Run `sync_skills.py --check`, `sync_udb_coding.py --check`, and
  `sync_references.py --check` in the required PR/main quick gate.
- Pin those commands in workflow posture so the pre-merge check cannot silently
  disappear.

## Evidence

Main publisher run `31877566060` reported these exact out-of-sync files:

- `plugins/udb/skills/using-udb/references/using-udb.md`;
- `openai/instructions.md`;
- `ollama/Modelfile`.
