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

UDB recognizes 18 backend kinds in `src/backend/mod.rs`. The default build
compiles the full broker surface except the optional native MongoDB driver
profile; slim builds can use:

```powershell
cargo check --no-default-features --features postgres
```

| Backend | Feature | Primary env | Role in matching build |
|---|---|---|---|
| Postgres | always available | `UDB_PG_DSN` / `DATABASE_URL` | canonical SQL store, system catalog, migrations |
| MySQL | `mysql` | `UDB_MYSQL_DSN` | canonical SQL store and generic dispatch |
| SQLite | `sqlite` | `UDB_SQLITE_DSN` | embedded/dev canonical store and generic dispatch |
| SQL Server | `mssql` | `UDB_MSSQL_DSN` | canonical SQL store via TDS, relational CRUD, transactions, session context |
| ClickHouse | `clickhouse` | `UDB_COLUMN_DSN` / `UDB_COLUMN_HTTP_URL` | canonical append/analytics profile, column query/mutate, eventual consistency |
| Redis | `redis` | `UDB_REDIS_DSN` / `REDIS_URL` | cache plus native canonical store when the durable AOF profile passes startup checks |
| Memcached | `memcached` | `UDB_MEMCACHED_DSN` | cache projection only; no durable canonical-state contract |
| Qdrant | `qdrant` | `UDB_QDRANT_URL` / `QDRANT_URL` | vector search/upsert plus native canonical system collection |
| Weaviate | `weaviate` | `UDB_WEAVIATE_DSN` | vector/hybrid search plus shared vector-system canonical adapter |
| Pinecone | `pinecone` | `UDB_PINECONE_DSN` | managed vector search plus shared vector-system canonical adapter |
| MinIO | `s3` | `UDB_MINIO_ENDPOINT` / S3-compatible env | object projection; canonical candidate gated on conditional writes and fencing |
| S3 | `s3` | AWS env / `UDB_S3_DSN` | object projection; streaming put/get, presign, multipart |
| Azure Blob | `azureblob` | `UDB_AZUREBLOB_DSN` | object projection; staged-block object path, canonical candidate |
| Google Cloud Storage | `gcs` | `UDB_GCS_DSN` | object projection; generation-precondition canonical candidate |
| MongoDB | `mongodb`, `mongodb-native` | `UDB_NOSQL_DSN`, `UDB_NOSQL_API_URL`, `UDB_MONGODB_DSN` | Data API/document projection by default; canonical with `mongodb-native` against a majority-write topology |
| Elasticsearch | `elasticsearch` | `UDB_ELASTIC_DSN` | search/hybrid/knn plus shared search-system canonical adapter |
| Neo4j | `neo4j` | `UDB_GRAPH_DSN`, `UDB_GRAPH_HTTP_URL` | graph query/mutate plus graph-native canonical store |
| Cassandra/Scylla | `cassandra` | `UDB_CASSANDRA_DSN` | wide-column query/mutate plus LWT-backed canonical store |

Kafka is not a `BackendKind`; the `kafka` feature and `UDB_KAFKA_BROKERS`
enable CDC publication, transactional outbox delivery, and DLQ paths.

The complete env surface is documented in [../.env.example](../.env.example).

## Canonical Stores

Postgres remains the default canonical store. The code also registers
feature-gated `SystemStores` implementations for MySQL, SQLite, SQL Server,
Redis, Cassandra/Scylla, Neo4j, Qdrant, ClickHouse, Weaviate, Pinecone, and
Elasticsearch. Native MongoDB joins that set only in `mongodb-native` builds,
where the runtime can verify replica-set or sharded majority semantics.

Object stores are not promoted as canonical state backends yet. S3, MinIO,
Azure Blob, and GCS expose streaming object operations and a documented
feasibility profile, but their canonical role remains gated on conditional
writes, etag/generation fencing, listing recovery, and multipart safety.
Memcached is explicitly projection-only. Capabilities are exposed through
plugin contracts, `GetCapabilities`, `udb doctor`, and `compat-matrix`.

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
