# ACME Billing — Arbitrary Project Namespace Example

This directory demonstrates UDB operating against a completely arbitrary proto
namespace (`acme.billing.v1`) with no project-specific dependency anywhere.

It proves the portability claim: **any proto annotated with `db.proto` extension
options can be a UDB schema source, regardless of package name or project.**

---

## Files

| Path | Purpose |
|------|---------|
| `proto/acme_billing_v1.proto` | Sample schema: Invoice, LineItem, Product, BillingDocument |
| `configs/database.yaml` | Connection config template |
| `scripts/bootstrap.sh` | One-command local bootstrap |

---

## Quick Start

### 1 — Parse and lint the schema

```sh
# From the repo root (udb/ directory)
cargo run --bin udb-proto-parser -- parse \
  --root examples/arbitrary_project/proto \
  acme_billing_v1.proto

cargo run --bin udb-proto-parser -- lint \
  --root examples/arbitrary_project/proto \
  acme_billing_v1.proto
```

### 2 — Generate bootstrap SQL

```sh
cargo run --bin udb-proto-parser -- sql \
  --root examples/arbitrary_project/proto \
  acme_billing_v1.proto
```

Expected output (abbreviated):
```sql
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.invoices (
    invoice_id TEXT PRIMARY KEY,
    org_id     TEXT NOT NULL,
    ...
);
```

### 3 — Preview UDB system catalog DDL

```sh
cargo run --bin udb-proto-parser -- system-ddl
```

### 4 — Generate DSN catalog

```sh
cargo run --bin udb-proto-parser -- dsn \
  --root examples/arbitrary_project/proto \
  acme_billing_v1.proto
```

### 5 — Run the UDB gRPC server against this schema

```sh
export UDB_PG_DSN=postgres://udb:udb@localhost:5432/acme_billing_dev
export UDB_CACHE_DSN=redis://localhost:6379
export UDB_VECTOR_DSN=http://localhost:6333
export UDB_OBJECT_DSN=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export UDB_S3_BUCKET=acme-billing-documents
export UDB_ABAC_DEFAULT_ALLOW=true

cargo run --bin udb-proto-parser -- serve \
  --root examples/arbitrary_project/proto \
  acme_billing_v1.proto
```

The broker will start on `0.0.0.0:50051` serving the `acme.billing.v1` schema.

---

## What the schema demonstrates

| Feature | Where |
|---------|-------|
| Relational table with RLS + tenant column | `Invoice.org_id` |
| PII field masking (`pii_kind: PII_KIND_EMAIL`) | `Invoice.customer_email` |
| Field-level encryption | `Invoice.customer_email` |
| Redis cache with `write_through`/`read_through` | `Invoice` |
| Foreign key reference | `InvoiceLineItem.invoice_id` |
| Vector store (Qdrant) | `Product` |
| S3 object store with SSE | `BillingDocument` |

---

## Supported client calls (once the server is running)

```sh
# Health check
grpcurl -plaintext localhost:50051 project-specific.udb.services.v1.DataBroker/GetHealthReport

# Upsert an invoice (replace tenant_id with your org)
grpcurl -plaintext -d '{
  "context": {"tenant_id": "acme-org-1", "scopes": ["udb:write"]},
  "message_type": "acme.billing.v1.Invoice",
  "payload": {
    "invoice_id": "inv-001",
    "org_id": "acme-org-1",
    "customer_name": "Alice",
    "amount_cents": 9900,
    "currency": "USD",
    "status": "DRAFT"
  }
}' localhost:50051 project-specific.udb.services.v1.DataBroker/Upsert

# Select invoices for a tenant
grpcurl -plaintext -d '{
  "context": {"tenant_id": "acme-org-1", "scopes": ["udb:read"]},
  "message_type": "acme.billing.v1.Invoice",
  "filter": {"org_id": {"string_value": "acme-org-1"}},
  "limit": 10
}' localhost:50051 project-specific.udb.services.v1.DataBroker/Select

# Vector search over products
grpcurl -plaintext -d '{
  "context": {"tenant_id": "acme-org-1", "scopes": ["udb:read"]},
  "collection": "acme_products",
  "vector": [0.1, 0.2, ...],
  "limit": 5,
  "with_payload": true
}' localhost:50051 project-specific.udb.services.v1.DataBroker/VectorSearch

# Hybrid search with text re-ranking
grpcurl -plaintext -d '{
  "context": {"tenant_id": "acme-org-1", "scopes": ["udb:read"]},
  "collection": "acme_products",
  "vector": [0.1, 0.2, ...],
  "text_query": "premium billing subscription",
  "fusion_weights": [0.7, 0.3],
  "limit": 5,
  "with_payload": true
}' localhost:50051 project-specific.udb.services.v1.DataBroker/VectorHybridSearch
```

---

## Compatibility notes

- The proto extension options use the package prefix `acme.billing.v1.*` — UDB
  resolves extensions by suffix pattern, not by exact package name.
- No project-specific proto file is imported or required anywhere.
- The same UDB binary serves ACME Billing and any other project schemas; just
  point `--root` at the correct proto directory.
