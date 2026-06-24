# UDB Agent Skills

Portable agent skills for [UDB](https://github.com/fahara02/udb), shipped in
**three equivalent packagings from one source of truth each** so any agent
runtime can use them:

| Skill | What it teaches | Audience |
|---|---|---|
| **`using-udb`** | Use a running UDB broker: connect a language SDK (TS/Python/Go/Java/C#/PHP), authenticate with scopes/credentials, CRUD proto-defined entities over the gRPC DataBroker API | App developers (and agents helping them) |
| **`udb-coding`** | Contribute code TO the UDB repository: the ten house directives (proto-first, reuse shared helpers, no code islands, no capability lies, no hardcodes, no stubs, fail closed, no in-memory stores, tests call the served path, cargo discipline), the shared-machinery map, the new-native-service recipe, and the pre-DONE flaw catalog | Coding agents / contributors working in the UDB repo |

| Runtime | Artifacts | How it's used |
|---|---|---|
| **Claude Code** | `plugins/udb/skills/<skill>/SKILL.md` (+ `.claude-plugin/`) | Installable plugin (both skills in one plugin) via a marketplace |
| **OpenAI** | `openai/instructions.md` · `openai/instructions-udb-coding.md` | Custom GPT instructions / Assistants `instructions` / `system` message |
| **Ollama** | `ollama/Modelfile` · `ollama/Modelfile.udb-coding` | `SYSTEM` prompt baked into a local model |

The **canonical knowledge** lives in `shared/<skill>.md`
([`shared/using-udb.md`](shared/using-udb.md),
[`shared/udb-coding.md`](shared/udb-coding.md)); the wrappers embed/reference it
verbatim. Publishing is **automated** by `.github/workflows/publish-skill.yml`
on every push that touches the skill — it validates structure + wrapper sync,
pushes both Ollama models, and syncs both OpenAI Assistants. The Claude plugin
metadata lives under `udb-skill/.claude-plugin/` so the repo root stays clean.

## Quick install

**Claude Code**
```
/plugin marketplace add .                     # from udb-skill/
/plugin install udb@udb-skills
```
Both skills then auto-activate on matching questions — `using-udb` for "how do I
call UDB from <language>?", `udb-coding` for "implement this UDB plan item /
how should UDB code be structured?". Local test without publishing:
```
/plugin marketplace add ./udb-skill           # from the repo root
# or: claude --plugin-dir ./udb-skill/plugins/udb
```

**OpenAI** — copy the body of [`openai/instructions.md`](openai/instructions.md)
(usage assistant) or
[`openai/instructions-udb-coding.md`](openai/instructions-udb-coding.md)
(coding agent) into a Custom GPT's *Instructions*, or pass it as the Assistants
API `instructions` / a Chat Completions `system` message.

**Ollama**
```bash
cd ollama
ollama create udb-assistant -f ./Modelfile             # usage assistant
ollama create udb-coding    -f ./Modelfile.udb-coding  # coding agent
ollama run udb-coding
```
(Edit `FROM` to any local base model you have pulled.)

## What `using-udb` covers
- Per-language SDK install + client construction (TS, Python, Go, Java, C#, PHP)
- The metadata contract (tenant / project / scopes / identity headers)
- CRUD over the DataBroker (`Select` / `Upsert` / `Delete` by `message_type`)
- Auth: credentials, scopes, the offline **`udb auth bootstrap user`** flow
- Defining entities as annotated protos and the proto→manifest→DDL pipeline
- The `udb` CLI (serve, sdk generate, proto export, doctor, auth)

## What `udb-coding` covers
- A real **codebase guide**: the proto→descriptor→runtime spine, both request
  lifecycles file-by-file, and the CDC/canonical-store/XA/IR/migration mechanics
  with actual function names — plus a pointer to the CI-gated, generated
  `docs/generated/codebase-map.md` (module graphs + per-file symbol index)
- The **ten directives**, each tied to the audit finding that motivated it
  (code islands → wire-in, duplicate helpers → the cross-tenant leak they caused,
  fail-open guards, mirror tests, per-request env reads, …)
- The **shared-machinery map**: which existing helper to use for admission,
  outbox events, pagination, tenant claim-binding, leader-elected workers,
  crypto, leases, metrics label bounding — with file paths
- Two **companion references** carrying the UDB-specific *delta* of each
  technology (NOT generic tutorials — frontier models already know generic Rust
  and SQL):
  - **`rust-stack`** — tokio/tonic/tower/prost/sqlx/rdkafka as used here, with
    the traps (no blocking in async, headers-only tower layer, proto3 presence,
    transactional outbox, gRPC status conventions, redacting Debug)
  - **`backends`** — per-engine quirks for all 18 backends (RLS/tenant posture,
    canonical-store tier, the live-DB + audit traps: MSSQL `@read_only` pooled-
    connection break, Neo4j MERGE lease race, Redis Lua-only lease, ClickHouse/
    Mongo "Enforced"-but-raw cross-tenant lies, MySQL 8.4 syntax, …)
- The **new-native-service recipe**, after-proto regen protocol, the
  **10-question flaw catalog**, and the honest-`[~]` reporting rules

### Why companions instead of vendoring generic Rust/Postgres skills
Frontier models already know generic Rust, SQL, and each engine better than any
vendored 300-line skill — and external skills rot and carry license/maintenance
baggage. The value is the **UDB-specific delta** (which traps THIS repo hit,
which idiom THIS repo uses), single-sourced in `shared/` and drift-gated like
the rest of the skill. Claude loads a companion on demand (progressive
disclosure, zero cost until needed); the OpenAI/Ollama wrappers bake them inline
since those runtimes have no file-load step.

## Layout
```
udb-skill/
├── .claude-plugin/marketplace.json            # Claude marketplace (lists the plugin)
├── plugins/udb/
│   ├── .claude-plugin/plugin.json             # Claude plugin manifest (both skills)
│   └── skills/
│       ├── using-udb/
│       │   ├── SKILL.md                       # skill entry (trigger + quick ref)
│       │   └── references/using-udb.md        # full guide (progressive disclosure)
│       └── udb-coding/
│           ├── SKILL.md
│           └── references/udb-coding.md
├── openai/instructions.md                     # OpenAI wrapper: using-udb
├── openai/instructions-udb-coding.md          # OpenAI wrapper: udb-coding
├── ollama/Modelfile                           # Ollama wrapper: using-udb
├── ollama/Modelfile.udb-coding                # Ollama wrapper: udb-coding
├── shared/using-udb.md                        # CANONICAL source (edit here)
├── shared/udb-coding.md                       # CANONICAL source (edit here)
├── LICENSE                                    # MIT
└── README.md
```
Plus, at the **repo root**: `.github/workflows/publish-skill.yml` (automated
publish). Root-level `.claude-plugin/` is intentionally ignored; keep Claude
metadata inside `udb-skill/`.

## Keeping wrappers in sync
`shared/<skill>.md` is canonical. After editing it, **regenerate the wrappers
with the sync script** (each wrapper = its fixed preamble + the canonical body;
the Claude reference is a verbatim copy; Ollama re-closes its `SYSTEM """` block):
```bash
python sync_skills.py            # rewrite the using-udb wrappers from shared/using-udb.md
python sync_skills.py --check    # CI gate: exit 1 if any wrapper drifted
```
The script is scoped to **`using-udb`**. The `udb-coding` OpenAI/Ollama wrappers
inline the companion files (`backends`, `rust-stack`), so regenerate those with
their own flow:
```powershell
$body = Get-Content "shared/udb-coding.md" -Raw
Set-Content "plugins/udb/skills/udb-coding/references/udb-coding.md" $body -Encoding utf8 -NoNewline
# then re-embed body (+ companions) under the headers in openai/ and ollama/ udb-coding wrappers
```

## License
MIT — see [`LICENSE`](LICENSE).
