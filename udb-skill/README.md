# UDB Agent Skill — "Using UDB"

A portable agent skill that teaches an AI assistant how to help developers **use a
running [UDB](https://github.com/fahara02/udb) broker** — connect a language SDK,
authenticate with scopes/credentials, and CRUD proto-defined entities over the
gRPC DataBroker API.

It ships in **three equivalent packagings from one source of truth** so any agent
runtime can use it:

| Runtime | Artifact | How it's used |
|---|---|---|
| **Claude Code** | `plugins/udb/skills/using-udb/SKILL.md` (+ `.claude-plugin/`) | Installable plugin/skill via a marketplace |
| **OpenAI** | `openai/instructions.md` | Custom GPT instructions / Assistants `instructions` / `system` message |
| **Ollama** | `ollama/Modelfile` | `SYSTEM` prompt baked into a local model |

The **canonical knowledge** lives in [`shared/using-udb.md`](shared/using-udb.md);
the three wrappers embed/reference it verbatim. Publishing is **automated** by
`.github/workflows/publish-skill.yml` on every push that touches the skill — it
validates the skill, pushes the Ollama model, and syncs the OpenAI Assistant. The
Claude skill needs no publish step: the repo-root `.claude-plugin/marketplace.json`
makes it installable directly from the repo.

## Quick install

**Claude Code**
```
/plugin marketplace add fahara02/udb
/plugin install udb@udb-skills
```
Then the `using-udb` skill auto-activates when you ask about using UDB (or invoke
it explicitly). Local test without publishing:
```
/plugin marketplace add .                     # from the repo root
# or: claude --plugin-dir ./udb-skill/plugins/udb
```

**OpenAI** — copy the body of [`openai/instructions.md`](openai/instructions.md)
into a Custom GPT's *Instructions*, or pass it as the Assistants API `instructions`
/ a Chat Completions `system` message.

**Ollama**
```bash
cd ollama
ollama create udb-assistant -f ./Modelfile   # edit FROM to your local base model
ollama run udb-assistant
```

## What the skill covers
- Per-language SDK install + client construction (TS, Python, Go, Java, C#, PHP)
- The metadata contract (tenant / project / scopes / identity headers)
- CRUD over the DataBroker (`Select` / `Upsert` / `Delete` by `message_type`)
- Auth: credentials, scopes, the offline **`udb auth bootstrap user`** first-credential flow
- Defining entities as annotated protos (`table` / `column`) and the proto→manifest→DDL pipeline
- The `udb` CLI (serve, sdk generate, proto export, doctor, auth)

## Layout
```
udb-skill/
├── .claude-plugin/marketplace.json          # Claude marketplace (lists the plugin)
├── plugins/udb/
│   ├── .claude-plugin/plugin.json            # Claude plugin manifest
│   └── skills/using-udb/
│       ├── SKILL.md                          # skill entry (trigger + quick ref)
│       └── references/using-udb.md           # full guide (progressive disclosure)
├── openai/instructions.md                    # OpenAI system instructions
├── ollama/Modelfile                          # Ollama SYSTEM prompt
├── shared/using-udb.md                        # CANONICAL source (edit here)
├── LICENSE                                     # MIT
└── README.md
```
Plus, at the **repo root**: `.claude-plugin/marketplace.json` (Claude install
source) and `.github/workflows/publish-skill.yml` (automated publish).

## Keeping wrappers in sync
`shared/using-udb.md` is canonical. After editing it, regenerate the Claude
reference / OpenAI / Ollama wrappers from it (the publish workflow's validate job
**fails** if the Claude reference drifts from the shared file):
```powershell
$s = Get-Content shared/using-udb.md -Raw
Set-Content plugins/udb/skills/using-udb/references/using-udb.md $s -Encoding utf8
# (re-embed $s under the headers in openai/instructions.md and ollama/Modelfile)
```

## License
MIT — see [`LICENSE`](LICENSE).
</content>
