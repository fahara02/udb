//! B.7 — shared SQL canonical-core schema (DDL) rendering. Pure string builders
//! extracted from the per-backend canonical stores so MSSQL (B.8) and future
//! SQL backends reuse one source of truth instead of copying Postgres. Each
//! function returns the EXACT statements the hand-written store used; the
//! byte-identical tests below pin that equivalence.

/// Postgres `udb_projection_tasks` DDL: CREATE TABLE + the five index
/// creations, the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS next_retry_at`
/// upgrade step, and the final next-retry partial index. Returned in the
/// exact order the store executes them. `table` is the (already-quoted /
/// schema-qualified) relation reference the store resolves at call time.
pub(crate) fn postgres_projection_tasks_ddl(table: &str) -> Vec<String> {
    let rel = table;
    vec![
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    task_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    idempotency_key    TEXT NOT NULL UNIQUE,
                    project_id         TEXT NOT NULL DEFAULT '',
                    manifest_checksum  TEXT NOT NULL DEFAULT '',
                    message_type       TEXT NOT NULL DEFAULT '',
                    source_schema      TEXT NOT NULL DEFAULT '',
                    source_table       TEXT NOT NULL DEFAULT '',
                    source_row_key     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    operation          TEXT NOT NULL DEFAULT 'upsert'
                                       CHECK (operation IN ('upsert','delete')),
                    target_backend     TEXT NOT NULL DEFAULT '',
                    target_instance    TEXT NOT NULL DEFAULT '',
                    projection_kind    TEXT NOT NULL DEFAULT '',
                    resource_name      TEXT NOT NULL DEFAULT '',
                    target_options     JSONB NOT NULL DEFAULT '[]'::JSONB,
                    source_payload     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    source_checksum    TEXT NOT NULL DEFAULT '',
                    status             TEXT NOT NULL DEFAULT 'PENDING'
                                       CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')),
                    retry_count        INTEGER NOT NULL DEFAULT 0,
                    last_error         TEXT NOT NULL DEFAULT '',
                    next_retry_at      TIMESTAMPTZ,
                    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at       TIMESTAMPTZ
                )
                "#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_status_created_at"
                     ON {rel} (status, created_at)"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_project_status_created_at"
                     ON {rel} (project_id, status, created_at)"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_backend_status"
                     ON {rel} (target_backend, target_instance, status)"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_claim_pending"
                     ON {rel} (project_id, created_at, task_id)
                     WHERE status = 'PENDING'"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_claim_failed"
                     ON {rel} (project_id, next_retry_at, created_at, task_id)
                     WHERE status = 'FAILED'"#
        ),
        format!(r#"ALTER TABLE {rel} ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ"#),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_next_retry"
                     ON {rel} (next_retry_at) WHERE status = 'FAILED' AND next_retry_at IS NOT NULL"#
        ),
    ]
}

/// MySQL `udb_projection_tasks` DDL. Unlike the PG/SQLite stores, the MySQL
/// store executes these with bespoke per-statement error tolerance (CREATE
/// INDEX has no IF NOT EXISTS on older MySQL; ADD COLUMN can collide), so this
/// returns the statements as a struct of named parts in the exact order /
/// grouping the store needs rather than one flat list. `table` is the table
/// name the store interpolates (`udb_projection_tasks` by default).
pub(crate) struct MysqlProjectionTasksDdl {
    pub create_table: String,
    pub create_idx_status: String,
    pub create_idx_project_status: String,
    pub create_idx_backend: String,
    pub add_next_retry: String,
    pub create_idx_retry: String,
}

pub(crate) fn mysql_projection_tasks_ddl(table: &str) -> MysqlProjectionTasksDdl {
    let table_name = table;
    MysqlProjectionTasksDdl {
        create_table: format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                task_id            CHAR(36) NOT NULL PRIMARY KEY,
                idempotency_key    VARCHAR(255) NOT NULL UNIQUE,
                project_id         VARCHAR(255) NOT NULL DEFAULT '',
                manifest_checksum  VARCHAR(255) NOT NULL DEFAULT '',
                message_type       VARCHAR(255) NOT NULL DEFAULT '',
                source_schema      VARCHAR(255) NOT NULL DEFAULT '',
                source_table       VARCHAR(255) NOT NULL DEFAULT '',
                source_row_key     JSON NOT NULL,
                operation          VARCHAR(16) NOT NULL DEFAULT 'upsert',
                target_backend     VARCHAR(64) NOT NULL DEFAULT '',
                target_instance    VARCHAR(255) NOT NULL DEFAULT '',
                projection_kind    VARCHAR(64) NOT NULL DEFAULT '',
                resource_name      VARCHAR(255) NOT NULL DEFAULT '',
                target_options     JSON NOT NULL,
                source_payload     JSON NOT NULL,
                source_checksum    VARCHAR(255) NOT NULL DEFAULT '',
                status             VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                retry_count        INT NOT NULL DEFAULT 0,
                last_error         TEXT NOT NULL,
                next_retry_at      TIMESTAMP(6) NULL,
                created_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                completed_at       TIMESTAMP(6) NULL,
                CONSTRAINT chk_{table_name}_op CHECK (operation IN ('upsert','delete')),
                CONSTRAINT chk_{table_name}_status CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        ),
        create_idx_status: format!(
            "CREATE INDEX idx_{table_name}_status_created_at \
             ON {table_name} (status, created_at)"
        ),
        create_idx_project_status: format!(
            "CREATE INDEX idx_{table_name}_project_status_created_at \
             ON {table_name} (project_id, status, created_at)"
        ),
        create_idx_backend: format!(
            "CREATE INDEX idx_{table_name}_backend_status \
             ON {table_name} (target_backend, target_instance, status)"
        ),
        add_next_retry: format!(
            "ALTER TABLE {table_name} ADD COLUMN next_retry_at TIMESTAMP(6) NULL"
        ),
        create_idx_retry: format!(
            "CREATE INDEX idx_{table_name}_next_retry \
             ON {table_name} (status, next_retry_at)"
        ),
    }
}

/// SQLite `udb_projection_tasks` DDL: CREATE TABLE + four index creations and
/// the `ALTER TABLE ... ADD COLUMN next_retry_at` upgrade step, in the exact
/// order the store executes them. `table` is the table name the store
/// interpolates (`udb_projection_tasks` by default).
pub(crate) fn sqlite_projection_tasks_ddl(table: &str) -> Vec<String> {
    let table_name = table;
    vec![
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    task_id          TEXT PRIMARY KEY,
                    idempotency_key  TEXT NOT NULL UNIQUE,
                    project_id       TEXT NOT NULL DEFAULT '',
                    manifest_checksum TEXT NOT NULL DEFAULT '',
                    message_type     TEXT NOT NULL DEFAULT '',
                    source_schema    TEXT NOT NULL DEFAULT '',
                    source_table     TEXT NOT NULL DEFAULT '',
                    source_row_key   TEXT NOT NULL DEFAULT '{{}}',
                    operation        TEXT NOT NULL DEFAULT 'upsert'
                                     CHECK (operation IN ('upsert','delete')),
                    target_backend   TEXT NOT NULL DEFAULT '',
                    target_instance  TEXT NOT NULL DEFAULT '',
                    projection_kind  TEXT NOT NULL DEFAULT '',
                    resource_name    TEXT NOT NULL DEFAULT '',
                    target_options   TEXT NOT NULL DEFAULT '[]',
                    source_payload   TEXT NOT NULL DEFAULT '{{}}',
                    source_checksum  TEXT NOT NULL DEFAULT '',
                    status           TEXT NOT NULL DEFAULT 'PENDING'
                                     CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')),
                    retry_count      INTEGER NOT NULL DEFAULT 0,
                    last_error       TEXT NOT NULL DEFAULT '',
                    next_retry_at    TEXT,
                    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    completed_at     TEXT
                )
                "#
        ),
        // Index: claim queue scan by (status, created_at).
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_status_created_at \
                 ON {table_name} (status, created_at)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_project_status_created_at \
                 ON {table_name} (project_id, status, created_at)"
        ),
        // Index: per-backend filter for worker pools.
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_backend_status \
                 ON {table_name} (target_backend, target_instance, status)"
        ),
        format!("ALTER TABLE {table_name} ADD COLUMN next_retry_at TEXT"),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_next_retry \
                 ON {table_name} (status, next_retry_at)"
        ),
    ]
}

// ---------------------------------------------------------------------------
// Group A — sagas (`udb_sagas`)
// ---------------------------------------------------------------------------

/// Postgres `udb_sagas` DDL: CREATE TABLE + the `(tenant_id, status,
/// updated_at DESC)` index, in the exact order the store executes them.
/// `rel` is the (already-quoted / schema-qualified) relation reference the
/// store resolves at call time (`"udb_system"."udb_sagas"` by default).
pub(crate) fn postgres_sagas_ddl(rel: &str) -> Vec<String> {
    vec![
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    saga_id              UUID PRIMARY KEY,
                    tx_id                TEXT NOT NULL DEFAULT '',
                    tenant_id            TEXT NOT NULL DEFAULT '',
                    correlation_id       TEXT NOT NULL DEFAULT '',
                    status               TEXT NOT NULL DEFAULT 'pending'
                                         CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')),
                    backend_instance     TEXT NOT NULL DEFAULT '',
                    operation            TEXT NOT NULL DEFAULT '',
                    current_step         INTEGER NOT NULL DEFAULT 0,
                    retry_count          INTEGER NOT NULL DEFAULT 0,
                    recovery_attempts    INTEGER NOT NULL DEFAULT 0,
                    compensation_status  TEXT NOT NULL DEFAULT 'none'
                                         CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')),
                    steps                JSONB NOT NULL DEFAULT '[]'::JSONB,
                    compensations        JSONB NOT NULL DEFAULT '[]'::JSONB,
                    last_error           TEXT NOT NULL DEFAULT '',
                    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_sagas_tenant_status"
                     ON {rel} (tenant_id, status, updated_at DESC)"#
        ),
    ]
}

/// MySQL `udb_sagas` DDL. The store creates the table then creates the
/// index with bespoke duplicate-key tolerance, so this returns the two
/// statements as named parts rather than a flat list. `table` is the table
/// name the store interpolates (`udb_sagas` by default).
pub(crate) struct MysqlSagasDdl {
    pub create_table: String,
    pub create_idx: String,
}

pub(crate) fn mysql_sagas_ddl(table: &str) -> MysqlSagasDdl {
    let table_name = table;
    MysqlSagasDdl {
        create_table: format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                saga_id              CHAR(36) NOT NULL PRIMARY KEY,
                tx_id                VARCHAR(255) NOT NULL DEFAULT '',
                tenant_id            VARCHAR(255) NOT NULL DEFAULT '',
                correlation_id       VARCHAR(255) NOT NULL DEFAULT '',
                status               VARCHAR(32) NOT NULL DEFAULT 'pending',
                backend_instance     VARCHAR(255) NOT NULL DEFAULT '',
                operation            VARCHAR(255) NOT NULL DEFAULT '',
                current_step         INT NOT NULL DEFAULT 0,
                retry_count          INT NOT NULL DEFAULT 0,
                recovery_attempts    INT NOT NULL DEFAULT 0,
                compensation_status  VARCHAR(32) NOT NULL DEFAULT 'none',
                steps                JSON NOT NULL,
                compensations        JSON NOT NULL,
                last_error           TEXT NOT NULL,
                created_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                CONSTRAINT chk_{table_name}_status CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')),
                CONSTRAINT chk_{table_name}_comp_status CHECK (compensation_status IN ('none','completed','manual_review','retry_requested'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        ),
        create_idx: format!(
            "CREATE INDEX idx_{table_name}_tenant_status \
             ON {table_name} (tenant_id, status, updated_at)"
        ),
    }
}

/// SQLite `udb_sagas` DDL: CREATE TABLE + the `(tenant_id, status,
/// updated_at DESC)` index, in the exact order the store executes them.
/// `table` is the table name the store interpolates (`udb_sagas` by default).
pub(crate) fn sqlite_sagas_ddl(table: &str) -> Vec<String> {
    let table_name = table;
    vec![
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                saga_id              TEXT PRIMARY KEY,
                tx_id                TEXT NOT NULL DEFAULT '',
                tenant_id            TEXT NOT NULL DEFAULT '',
                correlation_id       TEXT NOT NULL DEFAULT '',
                status               TEXT NOT NULL DEFAULT 'pending'
                                     CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')),
                backend_instance     TEXT NOT NULL DEFAULT '',
                operation            TEXT NOT NULL DEFAULT '',
                current_step         INTEGER NOT NULL DEFAULT 0,
                retry_count          INTEGER NOT NULL DEFAULT 0,
                recovery_attempts    INTEGER NOT NULL DEFAULT 0,
                compensation_status  TEXT NOT NULL DEFAULT 'none'
                                     CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')),
                steps                TEXT NOT NULL DEFAULT '[]',
                compensations        TEXT NOT NULL DEFAULT '[]',
                last_error           TEXT NOT NULL DEFAULT '',
                created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_tenant_status \
             ON {table_name} (tenant_id, status, updated_at DESC)"
        ),
    ]
}

// ---------------------------------------------------------------------------
// Group B — outbox (`udb_outbox_events`), from the main store
// `ensure_system_tables`. The advisory-lease table is a SEPARATE method and is
// intentionally NOT extracted here.
// ---------------------------------------------------------------------------

/// Postgres outbox `CREATE TABLE IF NOT EXISTS`. `relation` is the
/// (already-quoted / schema-qualified) relation the store resolves via
/// `safe_relation()`.
pub(crate) fn postgres_outbox_ddl(relation: &str) -> String {
    let rel = relation;
    format!(
        "CREATE TABLE IF NOT EXISTS {rel} ( \
                event_seq      BIGSERIAL PRIMARY KEY, \
                event_id       UUID NOT NULL UNIQUE, \
                topic          TEXT NOT NULL, \
                partition_key  TEXT NOT NULL DEFAULT '', \
                payload        JSONB NOT NULL, \
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW() \
            )"
    )
}

/// MySQL outbox `CREATE TABLE IF NOT EXISTS` — the full outbox schema (parity
/// with the Postgres system catalog) including the inline delivery-state index.
/// `relation` is the relation the store resolves via `safe_relation()`.
pub(crate) fn mysql_outbox_ddl(relation: &str) -> String {
    let rel = relation;
    format!(
        // Full outbox schema (parity with the Postgres system catalog): the
        // production tailer UPDATEs delivery_state + the Kafka state columns,
        // so a minimal table breaks at-least-once delivery on MySQL (#132).
        "CREATE TABLE IF NOT EXISTS {rel} ( \
                event_seq      BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                event_id       CHAR(36) NOT NULL UNIQUE, \
                topic          VARCHAR(255) NOT NULL, \
                partition_key  VARCHAR(255) NOT NULL DEFAULT '', \
                payload        JSON NOT NULL, \
                headers        JSON NULL, \
                delivery_state VARCHAR(20) NOT NULL DEFAULT 'pending', \
                publishing_started_at TIMESTAMP(6) NULL, \
                published_at   TIMESTAMP(6) NULL, \
                acked_at       TIMESTAMP(6) NULL, \
                dlq_at         TIMESTAMP(6) NULL, \
                producer_epoch BIGINT NOT NULL DEFAULT 0, \
                transactional_id VARCHAR(255) NOT NULL DEFAULT '', \
                kafka_partition INT NULL, \
                kafka_offset   BIGINT NULL, \
                last_error     TEXT NULL, \
                created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
                INDEX idx_outbox_delivery_state (delivery_state, event_seq) \
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )
}

/// SQLite outbox `CREATE TABLE IF NOT EXISTS` — the full outbox schema
/// (parity with the Postgres system catalog). `table` is the table the store
/// resolves via `safe_table()`.
pub(crate) fn sqlite_outbox_ddl(table: &str) -> String {
    format!(
        // Full outbox schema (parity with the Postgres system catalog): the
        // production tailer UPDATEs delivery_state + the Kafka state columns,
        // so a minimal table breaks at-least-once delivery on SQLite (#132).
        "CREATE TABLE IF NOT EXISTS {table} ( \
                event_seq      INTEGER PRIMARY KEY AUTOINCREMENT, \
                event_id       TEXT NOT NULL UNIQUE, \
                topic          TEXT NOT NULL, \
                partition_key  TEXT NOT NULL DEFAULT '', \
                payload        TEXT NOT NULL, \
                headers        TEXT, \
                delivery_state TEXT NOT NULL DEFAULT 'pending', \
                publishing_started_at TEXT, \
                published_at   TEXT, \
                acked_at       TEXT, \
                dlq_at         TEXT, \
                producer_epoch INTEGER NOT NULL DEFAULT 0, \
                transactional_id TEXT NOT NULL DEFAULT '', \
                kafka_partition INTEGER, \
                kafka_offset   INTEGER, \
                last_error     TEXT NOT NULL DEFAULT '', \
                created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
            )"
    )
}

/// B.8 — MSSQL outbox `CREATE TABLE` (T-SQL) — the full outbox schema (parity
/// with the Postgres/MySQL/SQLite system catalog). `relation` is the relation
/// the store resolves via its `safe_relation()` guard. Wrapped in an
/// `IF NOT EXISTS (SELECT * FROM sys.objects …)` guard so it is idempotent
/// (T-SQL has no `CREATE TABLE IF NOT EXISTS`). Type mapping mirrors the other
/// SQL backends: `BIGINT IDENTITY(1,1)` for the auto-increment `event_seq`
/// (BIGSERIAL/AUTO_INCREMENT analogue), `UNIQUEIDENTIFIER` for the UUID
/// `event_id`, `NVARCHAR(MAX)` validated with `ISJSON(...) = 1` for the JSON
/// columns (JSONB/JSON analogue), `DATETIME2(7)` defaulting to
/// `SYSUTCDATETIME()` for timestamps. The `OBJECT_ID(@rel, 'U')` guard treats
/// the relation as a single (already-safe) object name.
#[cfg(feature = "mssql")]
pub(crate) fn mssql_outbox_ddl(relation: &str) -> String {
    let rel = relation;
    format!(
        "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                event_seq      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, \
                event_id       UNIQUEIDENTIFIER NOT NULL UNIQUE, \
                topic          NVARCHAR(255) NOT NULL, \
                partition_key  NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_partition_key DEFAULT '', \
                payload        NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_payload_json CHECK (ISJSON(payload) = 1), \
                headers        NVARCHAR(MAX) NULL, \
                delivery_state NVARCHAR(20) NOT NULL CONSTRAINT df_{rel}_delivery_state DEFAULT 'pending', \
                publishing_started_at DATETIME2(7) NULL, \
                published_at   DATETIME2(7) NULL, \
                acked_at       DATETIME2(7) NULL, \
                dlq_at         DATETIME2(7) NULL, \
                producer_epoch BIGINT NOT NULL CONSTRAINT df_{rel}_producer_epoch DEFAULT 0, \
                transactional_id NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_transactional_id DEFAULT '', \
                kafka_partition INT NULL, \
                kafka_offset   BIGINT NULL, \
                last_error     NVARCHAR(MAX) NULL, \
                created_at     DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME() \
            ); \
            CREATE INDEX idx_outbox_delivery_state ON {rel} (delivery_state, event_seq); \
         END"
    )
}

/// B.8 — MSSQL advisory-lease `CREATE TABLE` (T-SQL). Mirrors the Postgres /
/// MySQL `udb_advisory_leases` shape (`lease_name` PK, `owner_id`,
/// `expires_at`). `DATETIME2(7)` is the timestamp type. Idempotent via the
/// `OBJECT_ID(... 'U') IS NULL` guard.
#[cfg(feature = "mssql")]
pub(crate) fn mssql_advisory_lease_ddl() -> String {
    "IF OBJECT_ID(N'udb_advisory_leases', N'U') IS NULL \
     BEGIN \
        CREATE TABLE udb_advisory_leases ( \
            lease_name NVARCHAR(255) NOT NULL PRIMARY KEY, \
            owner_id   NVARCHAR(255) NOT NULL, \
            expires_at DATETIME2(7) NOT NULL \
        ); \
     END"
    .to_string()
}

/// B.8 — MSSQL `udb_projection_tasks` DDL (T-SQL). Mirrors the PG/MySQL/SQLite
/// column set with SQL Server types: `UNIQUEIDENTIFIER` for the uuid PK
/// (defaulted by the caller's INSERT, not a column default), `NVARCHAR(MAX)`
/// validated with `ISJSON(...) = 1` for the JSON columns, `DATETIME2(7)`
/// defaulting to `SYSUTCDATETIME()` for timestamps, named CHECK / DEFAULT
/// constraints, and `IF OBJECT_ID(... 'U') IS NULL` idempotency guards on the
/// table and each filtered index. Returned in the exact order the store
/// executes them. `rel` is the (already-safe) relation reference.
#[cfg(feature = "mssql")]
pub(crate) fn mssql_projection_tasks_ddl(rel: &str) -> String {
    format!(
        "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                task_id            UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                idempotency_key    NVARCHAR(450) NOT NULL UNIQUE, \
                project_id         NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_project_id DEFAULT '', \
                manifest_checksum  NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_manifest_checksum DEFAULT '', \
                message_type       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_message_type DEFAULT '', \
                source_schema      NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_source_schema DEFAULT '', \
                source_table       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_source_table DEFAULT '', \
                source_row_key     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_row_key_json CHECK (ISJSON(source_row_key) = 1), \
                operation          NVARCHAR(16) NOT NULL CONSTRAINT df_{rel}_operation DEFAULT 'upsert' \
                                   CONSTRAINT chk_{rel}_operation CHECK (operation IN ('upsert','delete')), \
                target_backend     NVARCHAR(64) NOT NULL CONSTRAINT df_{rel}_target_backend DEFAULT '', \
                target_instance    NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_target_instance DEFAULT '', \
                projection_kind    NVARCHAR(64) NOT NULL CONSTRAINT df_{rel}_projection_kind DEFAULT '', \
                resource_name      NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_resource_name DEFAULT '', \
                target_options     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_options_json CHECK (ISJSON(target_options) = 1), \
                source_payload     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_payload_json CHECK (ISJSON(source_payload) = 1), \
                source_checksum    NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_source_checksum DEFAULT '', \
                status             NVARCHAR(16) NOT NULL CONSTRAINT df_{rel}_status DEFAULT 'PENDING' \
                                   CONSTRAINT chk_{rel}_status CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')), \
                retry_count        INT NOT NULL CONSTRAINT df_{rel}_retry_count DEFAULT 0, \
                last_error         NVARCHAR(MAX) NOT NULL CONSTRAINT df_{rel}_last_error DEFAULT '', \
                next_retry_at      DATETIME2(7) NULL, \
                created_at         DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME(), \
                updated_at         DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_updated_at DEFAULT SYSUTCDATETIME(), \
                completed_at       DATETIME2(7) NULL \
            ); \
            CREATE INDEX idx_{rel}_status_created_at ON {rel} (status, created_at); \
            CREATE INDEX idx_{rel}_project_status_created_at ON {rel} (project_id, status, created_at); \
            CREATE INDEX idx_{rel}_backend_status ON {rel} (target_backend, target_instance, status); \
            CREATE INDEX idx_{rel}_claim_pending ON {rel} (project_id, created_at, task_id) WHERE status = 'PENDING'; \
            CREATE INDEX idx_{rel}_claim_failed ON {rel} (project_id, next_retry_at, created_at, task_id) WHERE status = 'FAILED'; \
         END"
    )
}

/// B.8 — MSSQL `udb_sagas` DDL (T-SQL). Mirrors the PG/MySQL/SQLite column set
/// with SQL Server types and named CHECK constraints; idempotent via the
/// `OBJECT_ID(... 'U') IS NULL` guard. `rel` is the (already-safe) relation.
#[cfg(feature = "mssql")]
pub(crate) fn mssql_sagas_ddl(rel: &str) -> String {
    format!(
        "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                saga_id              UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                tx_id                NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_tx_id DEFAULT '', \
                tenant_id            NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_tenant_id DEFAULT '', \
                correlation_id       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_correlation_id DEFAULT '', \
                status               NVARCHAR(32) NOT NULL CONSTRAINT df_{rel}_status DEFAULT 'pending' \
                                     CONSTRAINT chk_{rel}_status CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')), \
                backend_instance     NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_backend_instance DEFAULT '', \
                operation            NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_operation DEFAULT '', \
                current_step         INT NOT NULL CONSTRAINT df_{rel}_current_step DEFAULT 0, \
                retry_count          INT NOT NULL CONSTRAINT df_{rel}_retry_count DEFAULT 0, \
                recovery_attempts    INT NOT NULL CONSTRAINT df_{rel}_recovery_attempts DEFAULT 0, \
                compensation_status  NVARCHAR(32) NOT NULL CONSTRAINT df_{rel}_comp_status DEFAULT 'none' \
                                     CONSTRAINT chk_{rel}_comp_status CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')), \
                steps                NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_steps_json CHECK (ISJSON(steps) = 1), \
                compensations        NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_comps_json CHECK (ISJSON(compensations) = 1), \
                last_error           NVARCHAR(MAX) NOT NULL CONSTRAINT df_{rel}_last_error DEFAULT '', \
                created_at           DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME(), \
                updated_at           DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_updated_at DEFAULT SYSUTCDATETIME() \
            ); \
            CREATE INDEX idx_{rel}_tenant_status ON {rel} (tenant_id, status, updated_at DESC); \
         END"
    )
}

/// B.8 — MSSQL `udb_admin_audit_log` DDL (T-SQL). Mirrors the PG/MySQL/SQLite
/// column set; idempotent via the `OBJECT_ID(... 'U') IS NULL` guard. `rel` is
/// the (already-safe) relation. The hash columns are plain `NVARCHAR` — the
/// chain hash is computed in Rust (`compute_admin_audit_hash`), never in SQL.
#[cfg(feature = "mssql")]
pub(crate) fn mssql_admin_audit_ddl(rel: &str) -> String {
    format!(
        "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                audit_id         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                actor            NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_actor DEFAULT '', \
                operation        NVARCHAR(255) NOT NULL, \
                target           NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_target DEFAULT '', \
                request_json     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_request_json CHECK (ISJSON(request_json) = 1), \
                result           NVARCHAR(32) NOT NULL CONSTRAINT df_{rel}_result DEFAULT 'ok', \
                tenant_id        NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_tenant_id DEFAULT '', \
                project_id       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_project_id DEFAULT '', \
                correlation_id   NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_correlation_id DEFAULT '', \
                previous_hash    NVARCHAR(128) NOT NULL CONSTRAINT df_{rel}_previous_hash DEFAULT '', \
                current_hash     NVARCHAR(128) NOT NULL CONSTRAINT df_{rel}_current_hash DEFAULT '', \
                signer_key_id    NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_signer_key_id DEFAULT '', \
                external_anchor  NVARCHAR(MAX) NOT NULL CONSTRAINT df_{rel}_external_anchor DEFAULT '', \
                created_at       DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME() \
            ); \
            CREATE INDEX idx_{rel}_op ON {rel} (operation, created_at DESC); \
            CREATE INDEX idx_{rel}_hash ON {rel} (current_hash); \
         END"
    )
}

/// B.8 — MSSQL migration-audit DDL (T-SQL): the runs table + its index, then
/// the op-ledger table (FK → runs) + its index. Returned as TWO separate guarded
/// batches (runs first so its FK target exists before the ledger is created).
/// `runs` / `ledger` are the (already-safe) relations. Idempotent via the
/// `OBJECT_ID(... 'U') IS NULL` guards.
#[cfg(feature = "mssql")]
pub(crate) fn mssql_migration_audit_ddl(runs: &str, ledger: &str) -> (String, String) {
    let runs_ddl = format!(
        "IF OBJECT_ID(N'{runs}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {runs} ( \
                run_id            UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                project_id        NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_project_id DEFAULT '', \
                catalog_version   NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_catalog_version DEFAULT '', \
                state             NVARCHAR(32) NOT NULL CONSTRAINT df_{runs}_state DEFAULT 'DRY_RUN' \
                                  CONSTRAINT chk_{runs}_state CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')), \
                operations_hash   NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_operations_hash DEFAULT '', \
                approval_token    NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_approval_token DEFAULT '', \
                started_at        DATETIME2(7) NOT NULL CONSTRAINT df_{runs}_started_at DEFAULT SYSUTCDATETIME(), \
                finished_at       DATETIME2(7) NULL, \
                error             NVARCHAR(MAX) NOT NULL CONSTRAINT df_{runs}_error DEFAULT '' \
            ); \
            CREATE INDEX idx_{runs}_project_state ON {runs} (project_id, state, started_at DESC); \
         END"
    );
    let ledger_ddl = format!(
        "IF OBJECT_ID(N'{ledger}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {ledger} ( \
                id                BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, \
                run_id            UNIQUEIDENTIFIER NOT NULL CONSTRAINT fk_{ledger}_run REFERENCES {runs}(run_id) ON DELETE CASCADE, \
                operation_index   INT NOT NULL, \
                backend           NVARCHAR(64) NOT NULL CONSTRAINT df_{ledger}_backend DEFAULT 'postgres', \
                resource_uri      NVARCHAR(MAX) NOT NULL CONSTRAINT df_{ledger}_resource_uri DEFAULT '', \
                operation_kind    NVARCHAR(64) NOT NULL CONSTRAINT df_{ledger}_operation_kind DEFAULT '', \
                status            NVARCHAR(32) NOT NULL CONSTRAINT df_{ledger}_status DEFAULT 'PENDING' \
                                  CONSTRAINT chk_{ledger}_status CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')), \
                rollback_json     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{ledger}_rollback_json CHECK (ISJSON(rollback_json) = 1), \
                error             NVARCHAR(MAX) NOT NULL CONSTRAINT df_{ledger}_error DEFAULT '', \
                applied_at        DATETIME2(7) NULL \
            ); \
            CREATE INDEX idx_{ledger}_run_idx ON {ledger} (run_id, operation_index); \
         END"
    );
    (runs_ddl, ledger_ddl)
}

// ---------------------------------------------------------------------------
// Group C — admin audit (`udb_admin_audit_log`)
// ---------------------------------------------------------------------------

/// Postgres `udb_admin_audit_log` DDL: CREATE TABLE + the `(operation,
/// created_at DESC)` and `(current_hash)` indexes, in the exact order the
/// store executes them. `rel` is the (already-quoted / schema-qualified)
/// relation reference the store resolves at call time.
pub(crate) fn postgres_admin_audit_ddl(rel: &str) -> Vec<String> {
    vec![
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    audit_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    actor            TEXT NOT NULL DEFAULT '',
                    operation        TEXT NOT NULL,
                    target           TEXT NOT NULL DEFAULT '',
                    request_json     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    result           TEXT NOT NULL DEFAULT 'ok',
                    tenant_id        TEXT NOT NULL DEFAULT '',
                    project_id       TEXT NOT NULL DEFAULT '',
                    correlation_id   TEXT NOT NULL DEFAULT '',
                    previous_hash    TEXT NOT NULL DEFAULT '',
                    current_hash     TEXT NOT NULL DEFAULT '',
                    signer_key_id    TEXT NOT NULL DEFAULT '',
                    external_anchor  TEXT NOT NULL DEFAULT '',
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_admin_audit_log_op"
                     ON {rel} (operation, created_at DESC)"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_admin_audit_log_hash"
                     ON {rel} (current_hash)"#
        ),
    ]
}

/// MySQL `udb_admin_audit_log` DDL. The store creates the table then creates
/// the two indexes with bespoke duplicate-key tolerance, so this returns the
/// statements as named parts. `table` is the table name the store
/// interpolates (`udb_admin_audit_log` by default).
pub(crate) struct MysqlAdminAuditDdl {
    pub create_table: String,
    pub idx_op: String,
    pub idx_hash: String,
}

pub(crate) fn mysql_admin_audit_ddl(table: &str) -> MysqlAdminAuditDdl {
    let table_name = table;
    MysqlAdminAuditDdl {
        create_table: format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                audit_id         CHAR(36) NOT NULL PRIMARY KEY,
                actor            VARCHAR(255) NOT NULL DEFAULT '',
                operation        VARCHAR(255) NOT NULL,
                target           VARCHAR(255) NOT NULL DEFAULT '',
                request_json     JSON NOT NULL,
                result           VARCHAR(32) NOT NULL DEFAULT 'ok',
                tenant_id        VARCHAR(255) NOT NULL DEFAULT '',
                project_id       VARCHAR(255) NOT NULL DEFAULT '',
                correlation_id   VARCHAR(255) NOT NULL DEFAULT '',
                previous_hash    VARCHAR(128) NOT NULL DEFAULT '',
                current_hash     VARCHAR(128) NOT NULL DEFAULT '',
                signer_key_id    VARCHAR(255) NOT NULL DEFAULT '',
                external_anchor  TEXT NOT NULL,
                created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        ),
        idx_op: format!("CREATE INDEX idx_{table_name}_op ON {table_name} (operation, created_at)"),
        idx_hash: format!("CREATE INDEX idx_{table_name}_hash ON {table_name} (current_hash)"),
    }
}

/// SQLite `udb_admin_audit_log` DDL: CREATE TABLE + the `(operation,
/// created_at DESC)` and `(current_hash)` indexes, in the exact order the
/// store executes them. `table` is the table name the store interpolates.
pub(crate) fn sqlite_admin_audit_ddl(table: &str) -> Vec<String> {
    let table_name = table;
    vec![
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                audit_id         TEXT PRIMARY KEY,
                actor            TEXT NOT NULL DEFAULT '',
                operation        TEXT NOT NULL,
                target           TEXT NOT NULL DEFAULT '',
                request_json     TEXT NOT NULL DEFAULT '{{}}',
                result           TEXT NOT NULL DEFAULT 'ok',
                tenant_id        TEXT NOT NULL DEFAULT '',
                project_id       TEXT NOT NULL DEFAULT '',
                correlation_id   TEXT NOT NULL DEFAULT '',
                previous_hash    TEXT NOT NULL DEFAULT '',
                current_hash     TEXT NOT NULL DEFAULT '',
                signer_key_id    TEXT NOT NULL DEFAULT '',
                external_anchor  TEXT NOT NULL DEFAULT '',
                created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{table_name}_op \
             ON {table_name} (operation, created_at DESC)"
        ),
        format!("CREATE INDEX IF NOT EXISTS idx_{table_name}_hash ON {table_name} (current_hash)"),
    ]
}

// ---------------------------------------------------------------------------
// Group D — migration audit (`udb_migration_runs` + the op-ledger table)
// ---------------------------------------------------------------------------

/// Postgres migration-audit DDL: CREATE TABLE runs + its index, CREATE TABLE
/// ledger (FK to runs) + its index, in the exact order the store executes them.
/// `runs_rel` / `ledger_rel` are the (already-quoted / schema-qualified)
/// relation references the store resolves at call time.
pub(crate) fn postgres_migration_audit_ddl(runs_rel: &str, ledger_rel: &str) -> Vec<String> {
    vec![
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {runs_rel} (
                    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    project_id        TEXT NOT NULL DEFAULT '',
                    catalog_version   TEXT NOT NULL DEFAULT '',
                    state             TEXT NOT NULL DEFAULT 'DRY_RUN'
                                      CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                    operations_hash   TEXT NOT NULL DEFAULT '',
                    approval_token    TEXT NOT NULL DEFAULT '',
                    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at       TIMESTAMPTZ,
                    error             TEXT NOT NULL DEFAULT ''
                )
                "#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_migration_runs_project_state"
                     ON {runs_rel} (project_id, state, started_at DESC)"#
        ),
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {ledger_rel} (
                    id                BIGSERIAL PRIMARY KEY,
                    run_id            UUID NOT NULL REFERENCES {runs_rel}(run_id) ON DELETE CASCADE,
                    operation_index   INTEGER NOT NULL,
                    backend           TEXT NOT NULL DEFAULT 'postgres',
                    resource_uri      TEXT NOT NULL DEFAULT '',
                    operation_kind    TEXT NOT NULL DEFAULT '',
                    status            TEXT NOT NULL DEFAULT 'PENDING'
                                      CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                    rollback_json     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    error             TEXT NOT NULL DEFAULT '',
                    applied_at        TIMESTAMPTZ
                )
                "#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_udb_migration_op_ledger_run_idx"
                     ON {ledger_rel} (run_id, operation_index)"#
        ),
    ]
}

/// MySQL migration-audit DDL. The store creates each table then its index with
/// bespoke duplicate-key tolerance, interleaved (runs table, runs index,
/// ledger table, ledger index), so this returns the four statements as named
/// parts. `runs` / `ledger` are the table names the store interpolates.
pub(crate) struct MysqlMigrationAuditDdl {
    pub runs_ddl: String,
    pub runs_idx: String,
    pub ledger_ddl: String,
    pub ledger_idx: String,
}

pub(crate) fn mysql_migration_audit_ddl(runs: &str, ledger: &str) -> MysqlMigrationAuditDdl {
    let runs_table = runs;
    let ledger_table = ledger;
    MysqlMigrationAuditDdl {
        runs_ddl: format!(
            r#"
            CREATE TABLE IF NOT EXISTS {runs_table} (
                run_id            CHAR(36) NOT NULL PRIMARY KEY,
                project_id        VARCHAR(255) NOT NULL DEFAULT '',
                catalog_version   VARCHAR(255) NOT NULL DEFAULT '',
                state             VARCHAR(32) NOT NULL DEFAULT 'DRY_RUN',
                operations_hash   VARCHAR(255) NOT NULL DEFAULT '',
                approval_token    VARCHAR(255) NOT NULL DEFAULT '',
                started_at        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                finished_at       TIMESTAMP(6) NULL,
                error             TEXT NOT NULL,
                CONSTRAINT chk_{runs_table}_state CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        ),
        runs_idx: format!(
            "CREATE INDEX idx_{runs_table}_project_state ON {runs_table} (project_id, state, started_at)"
        ),
        ledger_ddl: format!(
            r#"
            CREATE TABLE IF NOT EXISTS {ledger_table} (
                id                BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_id            CHAR(36) NOT NULL,
                operation_index   INT NOT NULL,
                backend           VARCHAR(64) NOT NULL DEFAULT 'postgres',
                resource_uri      TEXT NOT NULL,
                operation_kind    VARCHAR(64) NOT NULL DEFAULT '',
                status            VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                rollback_json     JSON NOT NULL,
                error             TEXT NOT NULL,
                applied_at        TIMESTAMP(6) NULL,
                CONSTRAINT chk_{ledger_table}_status CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                CONSTRAINT fk_{ledger_table}_run FOREIGN KEY (run_id) REFERENCES {runs_table}(run_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        ),
        ledger_idx: format!(
            "CREATE INDEX idx_{ledger_table}_run_idx ON {ledger_table} (run_id, operation_index)"
        ),
    }
}

/// SQLite migration-audit DDL: runs table + index, ledger table (FK to runs) +
/// index, in the exact order the store executes them after the `PRAGMA
/// foreign_keys = ON` (which the store issues separately and is NOT part of
/// this builder). `runs` / `ledger` are the table names the store interpolates.
pub(crate) fn sqlite_migration_audit_ddl(runs: &str, ledger: &str) -> Vec<String> {
    let runs_table = runs;
    let ledger_table = ledger;
    vec![
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {runs_table} (
                run_id            TEXT PRIMARY KEY,
                project_id        TEXT NOT NULL DEFAULT '',
                catalog_version   TEXT NOT NULL DEFAULT '',
                state             TEXT NOT NULL DEFAULT 'DRY_RUN'
                                  CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                operations_hash   TEXT NOT NULL DEFAULT '',
                approval_token    TEXT NOT NULL DEFAULT '',
                started_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                finished_at       TEXT,
                error             TEXT NOT NULL DEFAULT ''
            )
            "#
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{runs_table}_project_state \
             ON {runs_table} (project_id, state, started_at DESC)"
        ),
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {ledger_table} (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            TEXT NOT NULL,
                operation_index   INTEGER NOT NULL,
                backend           TEXT NOT NULL DEFAULT 'postgres',
                resource_uri      TEXT NOT NULL DEFAULT '',
                operation_kind    TEXT NOT NULL DEFAULT '',
                status            TEXT NOT NULL DEFAULT 'PENDING'
                                  CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                rollback_json     TEXT NOT NULL DEFAULT '{{}}',
                error             TEXT NOT NULL DEFAULT '',
                applied_at        TEXT,
                FOREIGN KEY (run_id) REFERENCES {runs_table}(run_id) ON DELETE CASCADE
            )
            "#
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_{ledger_table}_run_idx \
             ON {ledger_table} (run_id, operation_index)"
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = "udb_projection_tasks";

    /// Byte-identical gate: the extracted Postgres builder must produce the
    /// exact statements the hand-written `PostgresCanonicalStore::ensure_
    /// projection_tables` used. These literals are the pre-refactor source.
    #[test]
    fn postgres_projection_tasks_ddl_is_byte_identical() {
        let rel = TABLE;
        let expected = vec![
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    task_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    idempotency_key    TEXT NOT NULL UNIQUE,
                    project_id         TEXT NOT NULL DEFAULT '',
                    manifest_checksum  TEXT NOT NULL DEFAULT '',
                    message_type       TEXT NOT NULL DEFAULT '',
                    source_schema      TEXT NOT NULL DEFAULT '',
                    source_table       TEXT NOT NULL DEFAULT '',
                    source_row_key     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    operation          TEXT NOT NULL DEFAULT 'upsert'
                                       CHECK (operation IN ('upsert','delete')),
                    target_backend     TEXT NOT NULL DEFAULT '',
                    target_instance    TEXT NOT NULL DEFAULT '',
                    projection_kind    TEXT NOT NULL DEFAULT '',
                    resource_name      TEXT NOT NULL DEFAULT '',
                    target_options     JSONB NOT NULL DEFAULT '[]'::JSONB,
                    source_payload     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    source_checksum    TEXT NOT NULL DEFAULT '',
                    status             TEXT NOT NULL DEFAULT 'PENDING'
                                       CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')),
                    retry_count        INTEGER NOT NULL DEFAULT 0,
                    last_error         TEXT NOT NULL DEFAULT '',
                    next_retry_at      TIMESTAMPTZ,
                    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at       TIMESTAMPTZ
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_status_created_at"
                     ON {rel} (status, created_at)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_project_status_created_at"
                     ON {rel} (project_id, status, created_at)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_backend_status"
                     ON {rel} (target_backend, target_instance, status)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_claim_pending"
                     ON {rel} (project_id, created_at, task_id)
                     WHERE status = 'PENDING'"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_claim_failed"
                     ON {rel} (project_id, next_retry_at, created_at, task_id)
                     WHERE status = 'FAILED'"#
            ),
            format!(r#"ALTER TABLE {rel} ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ"#),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_next_retry"
                     ON {rel} (next_retry_at) WHERE status = 'FAILED' AND next_retry_at IS NOT NULL"#
            ),
        ];
        assert_eq!(postgres_projection_tasks_ddl(TABLE), expected);
    }

    /// Byte-identical gate for the MySQL store.
    #[test]
    fn mysql_projection_tasks_ddl_is_byte_identical() {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                task_id            CHAR(36) NOT NULL PRIMARY KEY,
                idempotency_key    VARCHAR(255) NOT NULL UNIQUE,
                project_id         VARCHAR(255) NOT NULL DEFAULT '',
                manifest_checksum  VARCHAR(255) NOT NULL DEFAULT '',
                message_type       VARCHAR(255) NOT NULL DEFAULT '',
                source_schema      VARCHAR(255) NOT NULL DEFAULT '',
                source_table       VARCHAR(255) NOT NULL DEFAULT '',
                source_row_key     JSON NOT NULL,
                operation          VARCHAR(16) NOT NULL DEFAULT 'upsert',
                target_backend     VARCHAR(64) NOT NULL DEFAULT '',
                target_instance    VARCHAR(255) NOT NULL DEFAULT '',
                projection_kind    VARCHAR(64) NOT NULL DEFAULT '',
                resource_name      VARCHAR(255) NOT NULL DEFAULT '',
                target_options     JSON NOT NULL,
                source_payload     JSON NOT NULL,
                source_checksum    VARCHAR(255) NOT NULL DEFAULT '',
                status             VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                retry_count        INT NOT NULL DEFAULT 0,
                last_error         TEXT NOT NULL,
                next_retry_at      TIMESTAMP(6) NULL,
                created_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                completed_at       TIMESTAMP(6) NULL,
                CONSTRAINT chk_{TABLE}_op CHECK (operation IN ('upsert','delete')),
                CONSTRAINT chk_{TABLE}_status CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let create_idx_status = format!(
            "CREATE INDEX idx_{TABLE}_status_created_at \
             ON {TABLE} (status, created_at)"
        );
        let create_idx_project_status = format!(
            "CREATE INDEX idx_{TABLE}_project_status_created_at \
             ON {TABLE} (project_id, status, created_at)"
        );
        let create_idx_backend = format!(
            "CREATE INDEX idx_{TABLE}_backend_status \
             ON {TABLE} (target_backend, target_instance, status)"
        );
        let add_next_retry =
            format!("ALTER TABLE {TABLE} ADD COLUMN next_retry_at TIMESTAMP(6) NULL");
        let create_idx_retry = format!(
            "CREATE INDEX idx_{TABLE}_next_retry \
             ON {TABLE} (status, next_retry_at)"
        );

        let got = mysql_projection_tasks_ddl(TABLE);
        assert_eq!(got.create_table, create_table);
        assert_eq!(got.create_idx_status, create_idx_status);
        assert_eq!(got.create_idx_project_status, create_idx_project_status);
        assert_eq!(got.create_idx_backend, create_idx_backend);
        assert_eq!(got.add_next_retry, add_next_retry);
        assert_eq!(got.create_idx_retry, create_idx_retry);
    }

    /// Byte-identical gate for the SQLite store.
    #[test]
    fn sqlite_projection_tasks_ddl_is_byte_identical() {
        let expected = vec![
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    task_id          TEXT PRIMARY KEY,
                    idempotency_key  TEXT NOT NULL UNIQUE,
                    project_id       TEXT NOT NULL DEFAULT '',
                    manifest_checksum TEXT NOT NULL DEFAULT '',
                    message_type     TEXT NOT NULL DEFAULT '',
                    source_schema    TEXT NOT NULL DEFAULT '',
                    source_table     TEXT NOT NULL DEFAULT '',
                    source_row_key   TEXT NOT NULL DEFAULT '{{}}',
                    operation        TEXT NOT NULL DEFAULT 'upsert'
                                     CHECK (operation IN ('upsert','delete')),
                    target_backend   TEXT NOT NULL DEFAULT '',
                    target_instance  TEXT NOT NULL DEFAULT '',
                    projection_kind  TEXT NOT NULL DEFAULT '',
                    resource_name    TEXT NOT NULL DEFAULT '',
                    target_options   TEXT NOT NULL DEFAULT '[]',
                    source_payload   TEXT NOT NULL DEFAULT '{{}}',
                    source_checksum  TEXT NOT NULL DEFAULT '',
                    status           TEXT NOT NULL DEFAULT 'PENDING'
                                     CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')),
                    retry_count      INTEGER NOT NULL DEFAULT 0,
                    last_error       TEXT NOT NULL DEFAULT '',
                    next_retry_at    TEXT,
                    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    updated_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    completed_at     TEXT
                )
                "#
            ),
            // Index: claim queue scan by (status, created_at).
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{TABLE}_status_created_at \
                 ON {TABLE} (status, created_at)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{TABLE}_project_status_created_at \
                 ON {TABLE} (project_id, status, created_at)"
            ),
            // Index: per-backend filter for worker pools.
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{TABLE}_backend_status \
                 ON {TABLE} (target_backend, target_instance, status)"
            ),
            format!("ALTER TABLE {TABLE} ADD COLUMN next_retry_at TEXT"),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{TABLE}_next_retry \
                 ON {TABLE} (status, next_retry_at)"
            ),
        ];
        assert_eq!(sqlite_projection_tasks_ddl(TABLE), expected);
    }

    // -----------------------------------------------------------------------
    // Group A — sagas
    // -----------------------------------------------------------------------

    /// Byte-identical gate: the extracted Postgres sagas builder must produce
    /// the exact statements `PostgresCanonicalStore::ensure_saga_tables` used.
    #[test]
    fn postgres_sagas_ddl_is_byte_identical() {
        let rel = r#""udb_system"."udb_sagas""#;
        let expected = vec![
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    saga_id              UUID PRIMARY KEY,
                    tx_id                TEXT NOT NULL DEFAULT '',
                    tenant_id            TEXT NOT NULL DEFAULT '',
                    correlation_id       TEXT NOT NULL DEFAULT '',
                    status               TEXT NOT NULL DEFAULT 'pending'
                                         CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')),
                    backend_instance     TEXT NOT NULL DEFAULT '',
                    operation            TEXT NOT NULL DEFAULT '',
                    current_step         INTEGER NOT NULL DEFAULT 0,
                    retry_count          INTEGER NOT NULL DEFAULT 0,
                    recovery_attempts    INTEGER NOT NULL DEFAULT 0,
                    compensation_status  TEXT NOT NULL DEFAULT 'none'
                                         CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')),
                    steps                JSONB NOT NULL DEFAULT '[]'::JSONB,
                    compensations        JSONB NOT NULL DEFAULT '[]'::JSONB,
                    last_error           TEXT NOT NULL DEFAULT '',
                    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_sagas_tenant_status"
                     ON {rel} (tenant_id, status, updated_at DESC)"#
            ),
        ];
        assert_eq!(postgres_sagas_ddl(rel), expected);
    }

    /// Byte-identical gate for the MySQL sagas store.
    #[test]
    fn mysql_sagas_ddl_is_byte_identical() {
        const TABLE: &str = "udb_sagas";
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                saga_id              CHAR(36) NOT NULL PRIMARY KEY,
                tx_id                VARCHAR(255) NOT NULL DEFAULT '',
                tenant_id            VARCHAR(255) NOT NULL DEFAULT '',
                correlation_id       VARCHAR(255) NOT NULL DEFAULT '',
                status               VARCHAR(32) NOT NULL DEFAULT 'pending',
                backend_instance     VARCHAR(255) NOT NULL DEFAULT '',
                operation            VARCHAR(255) NOT NULL DEFAULT '',
                current_step         INT NOT NULL DEFAULT 0,
                retry_count          INT NOT NULL DEFAULT 0,
                recovery_attempts    INT NOT NULL DEFAULT 0,
                compensation_status  VARCHAR(32) NOT NULL DEFAULT 'none',
                steps                JSON NOT NULL,
                compensations        JSON NOT NULL,
                last_error           TEXT NOT NULL,
                created_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                CONSTRAINT chk_{TABLE}_status CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')),
                CONSTRAINT chk_{TABLE}_comp_status CHECK (compensation_status IN ('none','completed','manual_review','retry_requested'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let create_idx = format!(
            "CREATE INDEX idx_{TABLE}_tenant_status \
             ON {TABLE} (tenant_id, status, updated_at)"
        );
        let got = mysql_sagas_ddl(TABLE);
        assert_eq!(got.create_table, create_table);
        assert_eq!(got.create_idx, create_idx);
    }

    /// Byte-identical gate for the SQLite sagas store.
    #[test]
    fn sqlite_sagas_ddl_is_byte_identical() {
        const TABLE: &str = "udb_sagas";
        let expected = vec![
            format!(
                r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                saga_id              TEXT PRIMARY KEY,
                tx_id                TEXT NOT NULL DEFAULT '',
                tenant_id            TEXT NOT NULL DEFAULT '',
                correlation_id       TEXT NOT NULL DEFAULT '',
                status               TEXT NOT NULL DEFAULT 'pending'
                                     CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')),
                backend_instance     TEXT NOT NULL DEFAULT '',
                operation            TEXT NOT NULL DEFAULT '',
                current_step         INTEGER NOT NULL DEFAULT 0,
                retry_count          INTEGER NOT NULL DEFAULT 0,
                recovery_attempts    INTEGER NOT NULL DEFAULT 0,
                compensation_status  TEXT NOT NULL DEFAULT 'none'
                                     CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')),
                steps                TEXT NOT NULL DEFAULT '[]',
                compensations        TEXT NOT NULL DEFAULT '[]',
                last_error           TEXT NOT NULL DEFAULT '',
                created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{TABLE}_tenant_status \
             ON {TABLE} (tenant_id, status, updated_at DESC)"
            ),
        ];
        assert_eq!(sqlite_sagas_ddl(TABLE), expected);
    }

    // -----------------------------------------------------------------------
    // Group B — outbox
    // -----------------------------------------------------------------------

    /// Byte-identical gate for the Postgres outbox `ensure_system_tables`.
    #[test]
    fn postgres_outbox_ddl_is_byte_identical() {
        let rel = r#""udb_system"."udb_outbox_events""#;
        let expected = format!(
            "CREATE TABLE IF NOT EXISTS {rel} ( \
                event_seq      BIGSERIAL PRIMARY KEY, \
                event_id       UUID NOT NULL UNIQUE, \
                topic          TEXT NOT NULL, \
                partition_key  TEXT NOT NULL DEFAULT '', \
                payload        JSONB NOT NULL, \
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW() \
            )"
        );
        assert_eq!(postgres_outbox_ddl(rel), expected);
    }

    /// Byte-identical gate for the MySQL outbox `ensure_system_tables`.
    #[test]
    fn mysql_outbox_ddl_is_byte_identical() {
        let rel = "udb_outbox_events";
        let expected = format!(
            "CREATE TABLE IF NOT EXISTS {rel} ( \
                event_seq      BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                event_id       CHAR(36) NOT NULL UNIQUE, \
                topic          VARCHAR(255) NOT NULL, \
                partition_key  VARCHAR(255) NOT NULL DEFAULT '', \
                payload        JSON NOT NULL, \
                headers        JSON NULL, \
                delivery_state VARCHAR(20) NOT NULL DEFAULT 'pending', \
                publishing_started_at TIMESTAMP(6) NULL, \
                published_at   TIMESTAMP(6) NULL, \
                acked_at       TIMESTAMP(6) NULL, \
                dlq_at         TIMESTAMP(6) NULL, \
                producer_epoch BIGINT NOT NULL DEFAULT 0, \
                transactional_id VARCHAR(255) NOT NULL DEFAULT '', \
                kafka_partition INT NULL, \
                kafka_offset   BIGINT NULL, \
                last_error     TEXT NULL, \
                created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
                INDEX idx_outbox_delivery_state (delivery_state, event_seq) \
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        assert_eq!(mysql_outbox_ddl(rel), expected);
    }

    /// Byte-identical gate for the SQLite outbox `ensure_system_tables`.
    #[test]
    fn sqlite_outbox_ddl_is_byte_identical() {
        let table = "udb_outbox_events";
        let expected = format!(
            "CREATE TABLE IF NOT EXISTS {table} ( \
                event_seq      INTEGER PRIMARY KEY AUTOINCREMENT, \
                event_id       TEXT NOT NULL UNIQUE, \
                topic          TEXT NOT NULL, \
                partition_key  TEXT NOT NULL DEFAULT '', \
                payload        TEXT NOT NULL, \
                headers        TEXT, \
                delivery_state TEXT NOT NULL DEFAULT 'pending', \
                publishing_started_at TEXT, \
                published_at   TEXT, \
                acked_at       TEXT, \
                dlq_at         TEXT, \
                producer_epoch INTEGER NOT NULL DEFAULT 0, \
                transactional_id TEXT NOT NULL DEFAULT '', \
                kafka_partition INTEGER, \
                kafka_offset   INTEGER, \
                last_error     TEXT NOT NULL DEFAULT '', \
                created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
            )"
        );
        assert_eq!(sqlite_outbox_ddl(table), expected);
    }

    /// B.8 byte-identical gate for the MSSQL outbox builder.
    #[cfg(feature = "mssql")]
    #[test]
    fn mssql_outbox_ddl_is_byte_identical() {
        let rel = "udb_outbox_events";
        let expected = format!(
            "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
             BEGIN \
                CREATE TABLE {rel} ( \
                    event_seq      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, \
                    event_id       UNIQUEIDENTIFIER NOT NULL UNIQUE, \
                    topic          NVARCHAR(255) NOT NULL, \
                    partition_key  NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_partition_key DEFAULT '', \
                    payload        NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_payload_json CHECK (ISJSON(payload) = 1), \
                    headers        NVARCHAR(MAX) NULL, \
                    delivery_state NVARCHAR(20) NOT NULL CONSTRAINT df_{rel}_delivery_state DEFAULT 'pending', \
                    publishing_started_at DATETIME2(7) NULL, \
                    published_at   DATETIME2(7) NULL, \
                    acked_at       DATETIME2(7) NULL, \
                    dlq_at         DATETIME2(7) NULL, \
                    producer_epoch BIGINT NOT NULL CONSTRAINT df_{rel}_producer_epoch DEFAULT 0, \
                    transactional_id NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_transactional_id DEFAULT '', \
                    kafka_partition INT NULL, \
                    kafka_offset   BIGINT NULL, \
                    last_error     NVARCHAR(MAX) NULL, \
                    created_at     DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME() \
                ); \
                CREATE INDEX idx_outbox_delivery_state ON {rel} (delivery_state, event_seq); \
             END"
        );
        assert_eq!(mssql_outbox_ddl(rel), expected);
    }

    /// B.8 byte-identical gate for the MSSQL advisory-lease builder.
    #[cfg(feature = "mssql")]
    #[test]
    fn mssql_advisory_lease_ddl_is_byte_identical() {
        let expected = "IF OBJECT_ID(N'udb_advisory_leases', N'U') IS NULL \
             BEGIN \
                CREATE TABLE udb_advisory_leases ( \
                    lease_name NVARCHAR(255) NOT NULL PRIMARY KEY, \
                    owner_id   NVARCHAR(255) NOT NULL, \
                    expires_at DATETIME2(7) NOT NULL \
                ); \
             END";
        assert_eq!(mssql_advisory_lease_ddl(), expected);
    }

    /// B.8 byte-identical gate for the MSSQL projection-tasks builder.
    #[cfg(feature = "mssql")]
    #[test]
    fn mssql_projection_tasks_ddl_is_byte_identical() {
        let rel = "udb_projection_tasks";
        let expected = format!(
            "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                task_id            UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                idempotency_key    NVARCHAR(450) NOT NULL UNIQUE, \
                project_id         NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_project_id DEFAULT '', \
                manifest_checksum  NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_manifest_checksum DEFAULT '', \
                message_type       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_message_type DEFAULT '', \
                source_schema      NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_source_schema DEFAULT '', \
                source_table       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_source_table DEFAULT '', \
                source_row_key     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_row_key_json CHECK (ISJSON(source_row_key) = 1), \
                operation          NVARCHAR(16) NOT NULL CONSTRAINT df_{rel}_operation DEFAULT 'upsert' \
                                   CONSTRAINT chk_{rel}_operation CHECK (operation IN ('upsert','delete')), \
                target_backend     NVARCHAR(64) NOT NULL CONSTRAINT df_{rel}_target_backend DEFAULT '', \
                target_instance    NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_target_instance DEFAULT '', \
                projection_kind    NVARCHAR(64) NOT NULL CONSTRAINT df_{rel}_projection_kind DEFAULT '', \
                resource_name      NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_resource_name DEFAULT '', \
                target_options     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_options_json CHECK (ISJSON(target_options) = 1), \
                source_payload     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_payload_json CHECK (ISJSON(source_payload) = 1), \
                source_checksum    NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_source_checksum DEFAULT '', \
                status             NVARCHAR(16) NOT NULL CONSTRAINT df_{rel}_status DEFAULT 'PENDING' \
                                   CONSTRAINT chk_{rel}_status CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')), \
                retry_count        INT NOT NULL CONSTRAINT df_{rel}_retry_count DEFAULT 0, \
                last_error         NVARCHAR(MAX) NOT NULL CONSTRAINT df_{rel}_last_error DEFAULT '', \
                next_retry_at      DATETIME2(7) NULL, \
                created_at         DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME(), \
                updated_at         DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_updated_at DEFAULT SYSUTCDATETIME(), \
                completed_at       DATETIME2(7) NULL \
            ); \
            CREATE INDEX idx_{rel}_status_created_at ON {rel} (status, created_at); \
            CREATE INDEX idx_{rel}_project_status_created_at ON {rel} (project_id, status, created_at); \
            CREATE INDEX idx_{rel}_backend_status ON {rel} (target_backend, target_instance, status); \
            CREATE INDEX idx_{rel}_claim_pending ON {rel} (project_id, created_at, task_id) WHERE status = 'PENDING'; \
            CREATE INDEX idx_{rel}_claim_failed ON {rel} (project_id, next_retry_at, created_at, task_id) WHERE status = 'FAILED'; \
         END"
        );
        assert_eq!(mssql_projection_tasks_ddl(rel), expected);
    }

    /// B.8 byte-identical gate for the MSSQL sagas builder.
    #[cfg(feature = "mssql")]
    #[test]
    fn mssql_sagas_ddl_is_byte_identical() {
        let rel = "udb_sagas";
        let expected = format!(
            "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                saga_id              UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                tx_id                NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_tx_id DEFAULT '', \
                tenant_id            NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_tenant_id DEFAULT '', \
                correlation_id       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_correlation_id DEFAULT '', \
                status               NVARCHAR(32) NOT NULL CONSTRAINT df_{rel}_status DEFAULT 'pending' \
                                     CONSTRAINT chk_{rel}_status CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','in_doubt','failed_compensation','manual_review')), \
                backend_instance     NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_backend_instance DEFAULT '', \
                operation            NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_operation DEFAULT '', \
                current_step         INT NOT NULL CONSTRAINT df_{rel}_current_step DEFAULT 0, \
                retry_count          INT NOT NULL CONSTRAINT df_{rel}_retry_count DEFAULT 0, \
                recovery_attempts    INT NOT NULL CONSTRAINT df_{rel}_recovery_attempts DEFAULT 0, \
                compensation_status  NVARCHAR(32) NOT NULL CONSTRAINT df_{rel}_comp_status DEFAULT 'none' \
                                     CONSTRAINT chk_{rel}_comp_status CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')), \
                steps                NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_steps_json CHECK (ISJSON(steps) = 1), \
                compensations        NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_comps_json CHECK (ISJSON(compensations) = 1), \
                last_error           NVARCHAR(MAX) NOT NULL CONSTRAINT df_{rel}_last_error DEFAULT '', \
                created_at           DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME(), \
                updated_at           DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_updated_at DEFAULT SYSUTCDATETIME() \
            ); \
            CREATE INDEX idx_{rel}_tenant_status ON {rel} (tenant_id, status, updated_at DESC); \
         END"
        );
        assert_eq!(mssql_sagas_ddl(rel), expected);
    }

    /// B.8 byte-identical gate for the MSSQL admin-audit builder.
    #[cfg(feature = "mssql")]
    #[test]
    fn mssql_admin_audit_ddl_is_byte_identical() {
        let rel = "udb_admin_audit_log";
        let expected = format!(
            "IF OBJECT_ID(N'{rel}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {rel} ( \
                audit_id         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                actor            NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_actor DEFAULT '', \
                operation        NVARCHAR(255) NOT NULL, \
                target           NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_target DEFAULT '', \
                request_json     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{rel}_request_json CHECK (ISJSON(request_json) = 1), \
                result           NVARCHAR(32) NOT NULL CONSTRAINT df_{rel}_result DEFAULT 'ok', \
                tenant_id        NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_tenant_id DEFAULT '', \
                project_id       NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_project_id DEFAULT '', \
                correlation_id   NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_correlation_id DEFAULT '', \
                previous_hash    NVARCHAR(128) NOT NULL CONSTRAINT df_{rel}_previous_hash DEFAULT '', \
                current_hash     NVARCHAR(128) NOT NULL CONSTRAINT df_{rel}_current_hash DEFAULT '', \
                signer_key_id    NVARCHAR(255) NOT NULL CONSTRAINT df_{rel}_signer_key_id DEFAULT '', \
                external_anchor  NVARCHAR(MAX) NOT NULL CONSTRAINT df_{rel}_external_anchor DEFAULT '', \
                created_at       DATETIME2(7) NOT NULL CONSTRAINT df_{rel}_created_at DEFAULT SYSUTCDATETIME() \
            ); \
            CREATE INDEX idx_{rel}_op ON {rel} (operation, created_at DESC); \
            CREATE INDEX idx_{rel}_hash ON {rel} (current_hash); \
         END"
        );
        assert_eq!(mssql_admin_audit_ddl(rel), expected);
    }

    /// B.8 byte-identical gate for the MSSQL migration-audit builder.
    #[cfg(feature = "mssql")]
    #[test]
    fn mssql_migration_audit_ddl_is_byte_identical() {
        let runs = "udb_migration_runs";
        let ledger = "udb_migration_op_ledger";
        let expected_runs = format!(
            "IF OBJECT_ID(N'{runs}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {runs} ( \
                run_id            UNIQUEIDENTIFIER NOT NULL PRIMARY KEY, \
                project_id        NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_project_id DEFAULT '', \
                catalog_version   NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_catalog_version DEFAULT '', \
                state             NVARCHAR(32) NOT NULL CONSTRAINT df_{runs}_state DEFAULT 'DRY_RUN' \
                                  CONSTRAINT chk_{runs}_state CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')), \
                operations_hash   NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_operations_hash DEFAULT '', \
                approval_token    NVARCHAR(255) NOT NULL CONSTRAINT df_{runs}_approval_token DEFAULT '', \
                started_at        DATETIME2(7) NOT NULL CONSTRAINT df_{runs}_started_at DEFAULT SYSUTCDATETIME(), \
                finished_at       DATETIME2(7) NULL, \
                error             NVARCHAR(MAX) NOT NULL CONSTRAINT df_{runs}_error DEFAULT '' \
            ); \
            CREATE INDEX idx_{runs}_project_state ON {runs} (project_id, state, started_at DESC); \
         END"
        );
        let expected_ledger = format!(
            "IF OBJECT_ID(N'{ledger}', N'U') IS NULL \
         BEGIN \
            CREATE TABLE {ledger} ( \
                id                BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, \
                run_id            UNIQUEIDENTIFIER NOT NULL CONSTRAINT fk_{ledger}_run REFERENCES {runs}(run_id) ON DELETE CASCADE, \
                operation_index   INT NOT NULL, \
                backend           NVARCHAR(64) NOT NULL CONSTRAINT df_{ledger}_backend DEFAULT 'postgres', \
                resource_uri      NVARCHAR(MAX) NOT NULL CONSTRAINT df_{ledger}_resource_uri DEFAULT '', \
                operation_kind    NVARCHAR(64) NOT NULL CONSTRAINT df_{ledger}_operation_kind DEFAULT '', \
                status            NVARCHAR(32) NOT NULL CONSTRAINT df_{ledger}_status DEFAULT 'PENDING' \
                                  CONSTRAINT chk_{ledger}_status CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')), \
                rollback_json     NVARCHAR(MAX) NOT NULL CONSTRAINT chk_{ledger}_rollback_json CHECK (ISJSON(rollback_json) = 1), \
                error             NVARCHAR(MAX) NOT NULL CONSTRAINT df_{ledger}_error DEFAULT '', \
                applied_at        DATETIME2(7) NULL \
            ); \
            CREATE INDEX idx_{ledger}_run_idx ON {ledger} (run_id, operation_index); \
         END"
        );
        let (got_runs, got_ledger) = mssql_migration_audit_ddl(runs, ledger);
        assert_eq!(got_runs, expected_runs);
        assert_eq!(got_ledger, expected_ledger);
    }

    // -----------------------------------------------------------------------
    // Group C — admin audit
    // -----------------------------------------------------------------------

    /// Byte-identical gate for the Postgres admin-audit store.
    #[test]
    fn postgres_admin_audit_ddl_is_byte_identical() {
        let rel = r#""udb_system"."udb_admin_audit_log""#;
        let expected = vec![
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    audit_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    actor            TEXT NOT NULL DEFAULT '',
                    operation        TEXT NOT NULL,
                    target           TEXT NOT NULL DEFAULT '',
                    request_json     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    result           TEXT NOT NULL DEFAULT 'ok',
                    tenant_id        TEXT NOT NULL DEFAULT '',
                    project_id       TEXT NOT NULL DEFAULT '',
                    correlation_id   TEXT NOT NULL DEFAULT '',
                    previous_hash    TEXT NOT NULL DEFAULT '',
                    current_hash     TEXT NOT NULL DEFAULT '',
                    signer_key_id    TEXT NOT NULL DEFAULT '',
                    external_anchor  TEXT NOT NULL DEFAULT '',
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_admin_audit_log_op"
                     ON {rel} (operation, created_at DESC)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_admin_audit_log_hash"
                     ON {rel} (current_hash)"#
            ),
        ];
        assert_eq!(postgres_admin_audit_ddl(rel), expected);
    }

    /// Byte-identical gate for the MySQL admin-audit store.
    #[test]
    fn mysql_admin_audit_ddl_is_byte_identical() {
        const TABLE: &str = "udb_admin_audit_log";
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                audit_id         CHAR(36) NOT NULL PRIMARY KEY,
                actor            VARCHAR(255) NOT NULL DEFAULT '',
                operation        VARCHAR(255) NOT NULL,
                target           VARCHAR(255) NOT NULL DEFAULT '',
                request_json     JSON NOT NULL,
                result           VARCHAR(32) NOT NULL DEFAULT 'ok',
                tenant_id        VARCHAR(255) NOT NULL DEFAULT '',
                project_id       VARCHAR(255) NOT NULL DEFAULT '',
                correlation_id   VARCHAR(255) NOT NULL DEFAULT '',
                previous_hash    VARCHAR(128) NOT NULL DEFAULT '',
                current_hash     VARCHAR(128) NOT NULL DEFAULT '',
                signer_key_id    VARCHAR(255) NOT NULL DEFAULT '',
                external_anchor  TEXT NOT NULL,
                created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let idx_op = format!("CREATE INDEX idx_{TABLE}_op ON {TABLE} (operation, created_at)");
        let idx_hash = format!("CREATE INDEX idx_{TABLE}_hash ON {TABLE} (current_hash)");
        let got = mysql_admin_audit_ddl(TABLE);
        assert_eq!(got.create_table, create_table);
        assert_eq!(got.idx_op, idx_op);
        assert_eq!(got.idx_hash, idx_hash);
    }

    /// Byte-identical gate for the SQLite admin-audit store.
    #[test]
    fn sqlite_admin_audit_ddl_is_byte_identical() {
        const TABLE: &str = "udb_admin_audit_log";
        let expected = vec![
            format!(
                r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                audit_id         TEXT PRIMARY KEY,
                actor            TEXT NOT NULL DEFAULT '',
                operation        TEXT NOT NULL,
                target           TEXT NOT NULL DEFAULT '',
                request_json     TEXT NOT NULL DEFAULT '{{}}',
                result           TEXT NOT NULL DEFAULT 'ok',
                tenant_id        TEXT NOT NULL DEFAULT '',
                project_id       TEXT NOT NULL DEFAULT '',
                correlation_id   TEXT NOT NULL DEFAULT '',
                previous_hash    TEXT NOT NULL DEFAULT '',
                current_hash     TEXT NOT NULL DEFAULT '',
                signer_key_id    TEXT NOT NULL DEFAULT '',
                external_anchor  TEXT NOT NULL DEFAULT '',
                created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{TABLE}_op \
             ON {TABLE} (operation, created_at DESC)"
            ),
            format!("CREATE INDEX IF NOT EXISTS idx_{TABLE}_hash ON {TABLE} (current_hash)"),
        ];
        assert_eq!(sqlite_admin_audit_ddl(TABLE), expected);
    }

    // -----------------------------------------------------------------------
    // Group D — migration audit
    // -----------------------------------------------------------------------

    /// Byte-identical gate for the Postgres migration-audit store.
    #[test]
    fn postgres_migration_audit_ddl_is_byte_identical() {
        let runs_rel = r#""udb_system"."udb_migration_runs""#;
        let ledger_rel = r#""udb_system"."udb_migration_op_ledger""#;
        let expected = vec![
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {runs_rel} (
                    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    project_id        TEXT NOT NULL DEFAULT '',
                    catalog_version   TEXT NOT NULL DEFAULT '',
                    state             TEXT NOT NULL DEFAULT 'DRY_RUN'
                                      CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                    operations_hash   TEXT NOT NULL DEFAULT '',
                    approval_token    TEXT NOT NULL DEFAULT '',
                    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at       TIMESTAMPTZ,
                    error             TEXT NOT NULL DEFAULT ''
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_migration_runs_project_state"
                     ON {runs_rel} (project_id, state, started_at DESC)"#
            ),
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {ledger_rel} (
                    id                BIGSERIAL PRIMARY KEY,
                    run_id            UUID NOT NULL REFERENCES {runs_rel}(run_id) ON DELETE CASCADE,
                    operation_index   INTEGER NOT NULL,
                    backend           TEXT NOT NULL DEFAULT 'postgres',
                    resource_uri      TEXT NOT NULL DEFAULT '',
                    operation_kind    TEXT NOT NULL DEFAULT '',
                    status            TEXT NOT NULL DEFAULT 'PENDING'
                                      CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                    rollback_json     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    error             TEXT NOT NULL DEFAULT '',
                    applied_at        TIMESTAMPTZ
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_migration_op_ledger_run_idx"
                     ON {ledger_rel} (run_id, operation_index)"#
            ),
        ];
        assert_eq!(postgres_migration_audit_ddl(runs_rel, ledger_rel), expected);
    }

    /// Byte-identical gate for the MySQL migration-audit store.
    #[test]
    fn mysql_migration_audit_ddl_is_byte_identical() {
        const RUNS_TABLE: &str = "udb_migration_runs";
        const LEDGER_TABLE: &str = "udb_migration_op_ledger";
        let runs_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_id            CHAR(36) NOT NULL PRIMARY KEY,
                project_id        VARCHAR(255) NOT NULL DEFAULT '',
                catalog_version   VARCHAR(255) NOT NULL DEFAULT '',
                state             VARCHAR(32) NOT NULL DEFAULT 'DRY_RUN',
                operations_hash   VARCHAR(255) NOT NULL DEFAULT '',
                approval_token    VARCHAR(255) NOT NULL DEFAULT '',
                started_at        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                finished_at       TIMESTAMP(6) NULL,
                error             TEXT NOT NULL,
                CONSTRAINT chk_{RUNS_TABLE}_state CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let runs_idx = format!(
            "CREATE INDEX idx_{RUNS_TABLE}_project_state ON {RUNS_TABLE} (project_id, state, started_at)"
        );
        let ledger_ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                id                BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_id            CHAR(36) NOT NULL,
                operation_index   INT NOT NULL,
                backend           VARCHAR(64) NOT NULL DEFAULT 'postgres',
                resource_uri      TEXT NOT NULL,
                operation_kind    VARCHAR(64) NOT NULL DEFAULT '',
                status            VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                rollback_json     JSON NOT NULL,
                error             TEXT NOT NULL,
                applied_at        TIMESTAMP(6) NULL,
                CONSTRAINT chk_{LEDGER_TABLE}_status CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                CONSTRAINT fk_{LEDGER_TABLE}_run FOREIGN KEY (run_id) REFERENCES {RUNS_TABLE}(run_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let ledger_idx = format!(
            "CREATE INDEX idx_{LEDGER_TABLE}_run_idx ON {LEDGER_TABLE} (run_id, operation_index)"
        );
        let got = mysql_migration_audit_ddl(RUNS_TABLE, LEDGER_TABLE);
        assert_eq!(got.runs_ddl, runs_ddl);
        assert_eq!(got.runs_idx, runs_idx);
        assert_eq!(got.ledger_ddl, ledger_ddl);
        assert_eq!(got.ledger_idx, ledger_idx);
    }

    /// Byte-identical gate for the SQLite migration-audit store.
    #[test]
    fn sqlite_migration_audit_ddl_is_byte_identical() {
        const RUNS_TABLE: &str = "udb_migration_runs";
        const LEDGER_TABLE: &str = "udb_migration_op_ledger";
        let expected = vec![
            format!(
                r#"
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_id            TEXT PRIMARY KEY,
                project_id        TEXT NOT NULL DEFAULT '',
                catalog_version   TEXT NOT NULL DEFAULT '',
                state             TEXT NOT NULL DEFAULT 'DRY_RUN'
                                  CHECK (state IN ('DRY_RUN','PREFLIGHT','APPLYING','VERIFYING','COMPLETED','ERROR','DEAD_LETTER')),
                operations_hash   TEXT NOT NULL DEFAULT '',
                approval_token    TEXT NOT NULL DEFAULT '',
                started_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                finished_at       TEXT,
                error             TEXT NOT NULL DEFAULT ''
            )
            "#
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{RUNS_TABLE}_project_state \
             ON {RUNS_TABLE} (project_id, state, started_at DESC)"
            ),
            format!(
                r#"
            CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id            TEXT NOT NULL,
                operation_index   INTEGER NOT NULL,
                backend           TEXT NOT NULL DEFAULT 'postgres',
                resource_uri      TEXT NOT NULL DEFAULT '',
                operation_kind    TEXT NOT NULL DEFAULT '',
                status            TEXT NOT NULL DEFAULT 'PENDING'
                                  CHECK (status IN ('PENDING','APPLIED','VERIFIED','SKIPPED','FAILED','ROLLED_BACK')),
                rollback_json     TEXT NOT NULL DEFAULT '{{}}',
                error             TEXT NOT NULL DEFAULT '',
                applied_at        TEXT,
                FOREIGN KEY (run_id) REFERENCES {RUNS_TABLE}(run_id) ON DELETE CASCADE
            )
            "#
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS idx_{LEDGER_TABLE}_run_idx \
             ON {LEDGER_TABLE} (run_id, operation_index)"
            ),
        ];
        assert_eq!(
            sqlite_migration_audit_ddl(RUNS_TABLE, LEDGER_TABLE),
            expected
        );
    }
}
