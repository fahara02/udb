# One broker, two projects — multi-project scoping

Most teams start with one set of protos and one database. Then a second product
shows up with its *own* entities, its *own* canonical store, and its *own*
projection targets — and you do **not** want to stand up a whole second broker
for it. This example shows the shape UDB uses instead: **one broker serving
several independent projects side by side**, each with its own proto namespace,
catalog version, and backend routing.

The one idea to take away: a request carries a **`project_id`**, and that field
decides *which* catalog and *which* backends the request touches. `tenant_id` is
a different axis — it isolates rows *within* a project (row-level security).
Project = which schema + which databases; tenant = whose data inside it.

Two projects are wired here:

| Project (`project_id`) | Namespace | Canonical store | Projection / extra backends |
|---|---|---|---|
| `acme-billing` | `acme.billing.v1` | Postgres (`pg-primary`) | Redis cache, Qdrant vector, S3/MinIO object |
| `zen-clinic` | `zen.clinic.v1` | Postgres (`pg-primary`) | Neo4j graph, ClickHouse analytics |

## What's in here

- `configs/projects.yaml` — the per-project routing map. This is the file you
  copy and edit for a new project: `namespace`, `catalog_version`,
  `cdc_topic_prefix`, and the `backends` block (canonical + the projection
  targets, each `backend` + `instance`).
- `proto/acme/...` and `proto/zen/...` — the entities each project serves
  (`Invoice` / `ProductSearchDocument` for ACME, `Appointment` /
  `CareGraphEdge` for Zen). Notice both keep a `tenant_id` field — that's the
  value row-level security matches on.
- `traffic/` — two ready-to-send request bodies: an `Upsert` of an ACME invoice
  and a `Select` of Zen appointments. Each one sets its own `context.project_id`.

Everything is deliberately minimal so you can lift it into a real project and
grow it.

## The 30-second version

You need a running broker plus [`ghz`](https://ghz.sh) (for the load profile).
The local playground gives you a broker on `:50051` with Postgres, Redis,
Qdrant, MinIO, and Kafka behind it. Run from the repo root:

```bash
# Terminal 1 — bring up the broker + backends (foreground-free, runs detached):
./scripts/playground.sh up          # UDB gRPC ends up on localhost:50051

# Terminal 2 — drive both projects through the one broker:
PROFILE=multi-project-smoke PROTO_ROOT=proto UDB_HOST=localhost:50051 \
  ./scripts/load_test.sh
```

The `multi-project-smoke` profile fires two flows at the same broker:

1. an `Upsert` into `acme-billing` (`acme.billing.v1.Invoice`, scope
   `udb:write`, tenant `tenant-acme`), and
2. a `Select` from `zen-clinic` (`zen.clinic.v1.Appointment`, scope `udb:read`,
   tenant `tenant-zen`).

Same broker, same wire, two different `project_id`s → two different catalogs and
backend sets.

### Send a single fixture with grpcurl

Once the broker is up you can replay either checked-in request body by hand. The
JSON in `traffic/` *is* the request message (context + payload), and the
metadata headers carry auth the same way the load profile does:

```bash
grpcurl -plaintext \
  -import-path proto -proto proto/udb/services/v1/data_broker.proto \
  -H 'x-tenant-id: tenant-acme' -H 'x-scopes: udb:write' \
  -H 'x-purpose: example' -H 'x-service-identity: example.multi_project' \
  -d @ localhost:50051 udb.services.v1.DataBroker.Upsert \
  < examples/multi_project/traffic/acme_invoice_upsert.json
```

Swap in `traffic/zen_appointment_select.json` with
`...DataBroker.Select` and the `zen` headers to exercise the other project.

## Configuration

`load_test.sh` reads these (defaults in parens); the values above override the
two that matter for the playground:

| Env var | What it is | Default |
|---|---|---|
| `PROFILE` | which scenario to run — use `multi-project-smoke` | `read-heavy` |
| `UDB_HOST` | broker gRPC address | `localhost:50000` |
| `PROTO_ROOT` | import root for the DataBroker proto | `../proto` |
| `CONCURRENCY` | parallel in-flight requests | `50` |
| `TOTAL_REQUESTS` | requests per case | `10000` |

The playground serves gRPC on `50051` and the proto lives at `proto/` from the
repo root, so both need overriding when you run from the repo root.

## Common mistakes this example is designed to prevent

- **Confusing project with tenant.** `project_id` picks the schema and the
  databases; `tenant_id` isolates rows inside a project. Set the wrong
  `project_id` and you hit the wrong catalog entirely; set the wrong `tenant_id`
  and row-level security quietly returns nothing.
- **Sending a human string where a canonical tenant UUID belongs.** The fixtures
  use readable placeholders (`tenant-acme`, `tenant-zen`) so the template is easy
  to read, but a real RLS-enforced broker compares the **canonical tenant UUID**,
  not the human code. In production, resolve your tenant to its UUID first and
  put *that* in `tenant_id` — otherwise writes stamp a value the database can't
  match and reads come back empty.
- **Assuming a new project just works.** A project only routes once its entry
  exists in the routing config (`configs/projects.yaml` is the template) and its
  proto namespace is served by the broker. Copy the block, set the `namespace`,
  `catalog_version`, and `backends`, then point requests at the new `project_id`.
