# UDB Architecture

UDB is a Rust gRPC broker that parses project protos into a catalog manifest and
routes typed or generic data operations to feature-gated backend plugins.

## Runtime Shape

1. The CLI/build layer loads dotenv files, YAML config, and OS environment.
2. Project protos are parsed into a catalog manifest.
3. Backend plugins register pools or clients at startup.
4. Requests enter the gRPC service with tenant/project/security metadata.
5. The runtime authorizes the request, resolves catalog/backend routing, applies
   channel limits, and dispatches either a typed operation or neutral IR.
6. CDC, saga, projection, audit, and admin paths write to UDB system tables.

## Backend Inventory

Default builds compile every backend feature. Slim builds can use:

```powershell
cargo check --no-default-features --features postgres
```

| Backend | Feature | Primary env | Role |
|---|---|---|---|
| Postgres | always available | `UDB_PG_DSN` / `DATABASE_URL` | canonical SQL store, system catalog, migrations |
| MySQL | `mysql` | `UDB_MYSQL_DSN` | canonical SQL store and generic dispatch |
| SQLite | `sqlite` | `UDB_SQLITE_DSN` | embedded/dev canonical store and generic dispatch |
| SQL Server | `mssql` | `UDB_MSSQL_DSN` | SQL backend via TDS |
| ClickHouse | `clickhouse` | `UDB_COLUMN_DSN` / `UDB_COLUMN_HTTP_URL` | column/analytics backend |
| Redis | `redis` | `UDB_REDIS_DSN` / `REDIS_URL` | cache, idempotency, CDC helper |
| Memcached | `memcached` | `UDB_MEMCACHED_DSN` | cache backend |
| Qdrant | `qdrant` | `UDB_QDRANT_URL` / `QDRANT_URL` | vector backend |
| Weaviate | `weaviate` | `UDB_WEAVIATE_DSN` | vector/search backend |
| Pinecone | `pinecone` | `UDB_PINECONE_DSN` | vector backend |
| MinIO/S3 | `s3` | `UDB_MINIO_ENDPOINT` / AWS env | object storage |
| Azure Blob | `azureblob` | `UDB_AZUREBLOB_DSN` | object storage |
| Google Cloud Storage | `gcs` | `UDB_GCS_DSN` | object storage |
| MongoDB | `mongodb`, `mongodb-native` | `UDB_NOSQL_DSN`, `UDB_NOSQL_API_URL` | document backend and optional native driver |
| Elasticsearch | `elasticsearch` | `UDB_ELASTIC_DSN` | text/search backend |
| Neo4j | `neo4j` | `UDB_GRAPH_DSN`, `UDB_GRAPH_HTTP_URL` | graph backend |
| Cassandra/Scylla | `cassandra` | `UDB_CASSANDRA_DSN` | wide-column backend |
| Kafka | `kafka` | `UDB_KAFKA_BROKERS` | CDC publication |

The complete env surface is documented in [../.env.example](../.env.example).

## Canonical Stores

Postgres remains the default canonical store. MySQL and SQLite also implement
the canonical-store contract for peer-to-peer operation and embedded/dev
profiles. Secondary stores are not forced to pretend they have SQL semantics;
capabilities are exposed through plugin contracts and operation-specific
support.

## Control Planes

- Catalog staging, activation, rollback, and validation live under
  [../src/runtime/service/handlers_catalog.rs](../src/runtime/service/handlers_catalog.rs).
- System store abstractions live under
  [../src/runtime/canonical_store](../src/runtime/canonical_store).
- Migration planning and phased apply live under
  [../src/migration](../src/migration).
- CDC lives under [../src/runtime/cdc](../src/runtime/cdc).
- Saga compensation and recovery live under [../src/runtime](../src/runtime).

## Known Architecture Edges

- Some late-added backend plugins own their clients directly rather than being
  fully represented in `ConnectionManager`. That is acceptable for static
  startup, but reload/drain/rotation semantics should be made consistent before
  claiming a universal connection lifecycle.
- CDC uses the generic source abstraction for publishable builds. Postgres WAL
  tailing is disabled until the required copy-both APIs are available from
  published `tokio-postgres`; use a CDC sidecar/source adapter for that path.
- Kubernetes operator packaging and a production JSON/HTTP gateway are not
  in-tree runtime features today.
