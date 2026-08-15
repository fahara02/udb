# UDB site (`docs/site`)

A self-contained, multi-page marketing/docs site for UDB — light theme derived
from the brand logo (`docs/assets/udb_logo.svg`): `#f8fafc` base, warm orange
actions, darker cyan/blue accents, Inter. The authoring surface is static
HTML/CSS plus vanilla JS; the GitHub Pages workflow performs the publish-time
contract work: rebuilds `udb.wasm`, syncs shared assets and Swagger JSON, pulls
the latest benchmark artifact when available, and validates the complete site
artifact before deploy.

## Pages

| File | Page |
|---|---|
| `index.html` | Landing — product positioning, descriptor-to-SDK flow, stats, request pipeline, tabbed code, backend marquee |
| `architecture.html` | Proto → manifest → runtime pipeline; descriptor-as-contract |
| `data-plane.html` | DataBroker: backends, 2PC/XA, sagas, CDC, migrations |
| `control-plane.html` | Descriptor-rendered native service catalog: auth, identity, tenancy, policy distribution |
| `security.html` | RLS, encryption, mTLS, fail-closed posture, compliance profiles |
| `enterprise.html` | HA/leader election, recovery, backpressure, observability, runbooks |
| `sdks.html` | Six SDKs, per-language quickstarts, conformance |
| `api.html` | Swagger UI over `api/udb-broker.swagger.json`, copied into the Pages artifact |
| `benchmarks.html` | Benchmark dashboard shell; renders release-binary SDK results when `bench-results.json` is populated |
| **`playground.html`** | **Interactive** — runs UDB's **real** proto parser, compiled to WebAssembly (`udb.wasm`), in the browser |

Shared: `styles.css` (theme + components), `app.js` (scroll-reveal, count-up,
mobile nav), `playground.js` (the WASM playground logic), `benchmarks.js` (the
benchmark dashboard renderer), `udb.wasm` (UDB's real parser/AST/checksum
compiled to `wasm32-unknown-unknown`).

## The live playground (real UDB, not a mock)

`playground.html` loads `udb.wasm` — the **`crates/udb-wasm`** cdylib bridge over
**`crates/udb-portable`**, which `#[path]`-includes the *same* parser / AST /
deterministic-checksum source the UDB server compiles. So when you paste a
UDB-annotated `.proto`, **UDB itself** (in WebAssembly) parses it client-side and
produces the exact catalog `ProtoSchema` (tables, columns, RLS, per-column data
classes) and the same sha-256 manifest checksum the broker computes — no server,
no re-implementation.

**Scope (honest):** the demo shows real UDB **parsing, catalog modeling,
bootstrap-DDL generation, and query-compiler output**. The served gRPC broker,
DB drivers, async execution, and real database I/O (`tokio` / `sqlx` / `tonic`)
remain server-only; this is not an in-browser database.

### Rebuilding `udb.wasm`

```bash
cargo build -p udb-wasm --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/udb_wasm.wasm docs/site/udb.wasm
```

The deploy workflow rebuilds it fresh on every publish, so the in-browser parser
never drifts from the server's.

## Benchmark JSON

`bench-results.json` is produced by `scripts/collect_sdk_bench_results.py` from
SDK Markdown reports. The post-release benchmark workflow uploads it as the
`sdk-benchmark-results` artifact; `pages.yml` consumes that artifact on benchmark
completion and fails closed if a real benchmark has no fresh artifact.
Validation-only benchmark runs do not deploy Pages. Direct pushes and manual
deploys may publish only the exact pinned SHA-256 of the committed v0.4.28
historical JSON; committed schema-v2/canonical evidence is rejected, and new
evidence is published only by the Release -> Benchmark artifact chain. The
historical JSON is visibly marked legacy/incomplete and never rendered as green
proof. Per-RPC
rows prefer
descriptor-derived `operation_id`, then `api_alias`, then the legacy
`service/rpc` wire identity. The dashboard keeps the wire RPC as row detail so
users can filter by canonical API identity without losing transport diagnostics.

## Assets

The page references images at `./assets/…`. The canonical source images live in
`docs/assets/` (shared with the READMEs and docs); the deploy workflow copies
them into `docs/site/assets/` at publish time, so they are **not** duplicated in
git (`docs/site/assets/` is `.gitignore`d).

## Deploy (GitHub Pages → GitHub Actions)

This repo ships `.github/workflows/pages.yml`, which publishes `docs/site/` as
the Pages root. The workflow syncs the brand assets, copies `api/*.json` into
`docs/site/api/`, pulls benchmark JSON, rebuilds `udb.wasm`, runs the
current-editor WASM smoke, verifies every first-class page/script/data artifact,
parses the Swagger and benchmark JSON, and crawls local HTML `href`/`src`
references before upload.

1. Repo **Settings → Pages → Source: GitHub Actions**.
2. Push to `main` (or run the workflow manually) — the site deploys to
   `https://<owner>.github.io/<repo>/`.

## Preview locally

```bash
# the workflow does this copy for you in CI:
mkdir -p docs/site/assets && cp docs/assets/*.svg docs/site/assets/
python -m http.server -d docs/site 8000   # open http://localhost:8000/
```
