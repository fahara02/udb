//! The durable auth audit log (`udb_system.auth_audit_log`) as a table
//! range-partitioned by month on `occurred_at`, so retention drops a whole
//! month at once instead of deleting rows one by one and leaving the space
//! behind.
//!
//! A new log is created partitioned by the audit sink: a parent whose primary
//! key is `(audit_id, occurred_at)` (a key of a partitioned table must hold
//! the partition column), a default partition (a write never fails for want
//! of a partition) and the current and next month. The cleanup pass keeps
//! months ahead, named `<table>_p_YYYYMM` like every UDB monthly partition.
//!
//! An existing unpartitioned log is converted only when an operator asks for
//! it (`udb cleanup partition-audit-log`), online:
//!
//! 1. it builds, without blocking writes (`CONCURRENTLY`), a unique index on
//!    `(audit_id, occurred_at)` and an index on `occurred_at`;
//! 2. it adds and validates (validation lets writes through) a CHECK
//!    constraint `occurred_at < B`, where B is the first day of the month
//!    after next: every row already in the table is older, and so is every
//!    row written until B, so the constraint never refuses a write before
//!    the conversion is done;
//! 3. in one short transaction under a 3 s `lock_timeout` (retried with
//!    backoff, then given up with nothing changed) it renames the table to
//!    `<table>_legacy`, makes the new index its primary key, creates the
//!    partitioned parent under the original name with the same columns and
//!    defaults, owner, grants, row-level security and policies, attaches the
//!    old table as the DEFAULT partition and creates the month starting at B.
//!
//! No row is moved or rewritten. The old table becomes the default partition
//! as it is, every row stays readable through the parent, and the cleanup pass
//! deletes its rows as they pass retention (a default partition is never
//! dropped). Rows written until B land there too; from B on each month has its
//! own partition. The CHECK constraint is what lets the parent take a new
//! month from B on without scanning the old table under a lock: PostgreSQL
//! skips the scan of a default partition whose validated constraints already
//! rule out the new month's range.

use std::time::{Duration, Instant};

use sqlx::{Connection, PgConnection, PgPool};

use super::{
    PolicyDef, PolicyRow, RlsState, policy_from_row, qi, ql, relation_policies,
    rls_sync_statements, split_relation, statement_timeout,
};

/// The CHECK constraint `occurred_at < B` on the default partition of a
/// converted log: no month partition is ever created below B.
pub(crate) const FLOOR_CONSTRAINT: &str = "udb_partition_floor";
/// How long one locking statement waits for its lock before giving up.
const LOCK_TIMEOUT: &str = "3s";
const LOCK_TRIES: u32 = 5;
/// The swap only changes the catalog; anything slower means PostgreSQL
/// started scanning a table under the lock, and the swap is rolled back.
const SWAP_STATEMENT_TIMEOUT: &str = "30s";
const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";
/// PostgreSQL's identifier length.
const MAX_IDENT: usize = 63;

/// `(schema, table)` of a relation as the sink is configured with it
/// (`udb_system.auth_audit_log`); an unqualified name is in `public`.
pub(crate) fn split(relation: &str) -> (String, String) {
    split_relation(relation).unwrap_or_else(|| {
        (
            "public".to_string(),
            relation.trim().trim_matches('"').to_string(),
        )
    })
}

fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", qi(schema), qi(table))
}

/// The month partitions from the current month to `ahead` months on, as
/// `(start, end, YYYYMM)`, in the database's clock and time zone.
pub(crate) async fn month_bounds(
    pool: &PgPool,
    ahead: i32,
) -> Result<Vec<(String, String, String)>, String> {
    sqlx::query_as(
        "SELECT g::text, (g + interval '1 month')::text, to_char(g, 'YYYYMM') \
         FROM generate_series(date_trunc('month', now()), \
                              date_trunc('month', now()) + ($1::int * interval '1 month'), \
                              interval '1 month') AS g",
    )
    .bind(ahead)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("plan the audit log's month partitions: {err}"))
}

/// The statements that create the log partitioned on a database that has no
/// such table yet: the parent (`columns` is its column list, without a key),
/// its `occurred_at` index, the default partition and the given months. All
/// `IF NOT EXISTS`, so a second creator racing the first changes nothing.
pub(crate) fn create_statements(
    schema: &str,
    table: &str,
    columns: &str,
    months: &[(String, String, String)],
) -> Vec<String> {
    let parent = qualified(schema, table);
    let mut statements = vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {parent} ({columns}, CONSTRAINT {} PRIMARY KEY \
             (audit_id, occurred_at)) PARTITION BY RANGE (occurred_at)",
            qi(&format!("{table}_pkey"))
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {parent} (occurred_at)",
            qi(&format!("{table}_occurred_at_idx"))
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {parent} DEFAULT",
            qualified(schema, &format!("{table}_default"))
        ),
    ];
    for (start, end, suffix) in months {
        statements.push(format!(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {parent} FOR VALUES FROM ({}) TO ({})",
            qualified(schema, &format!("{table}_p_{suffix}")),
            ql(start),
            ql(end)
        ));
    }
    statements
}

/// Whether `relation` is a partitioned table.
pub(crate) async fn is_partitioned(pool: &PgPool, relation: &str) -> Result<bool, String> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass($1))",
    )
    .bind(relation)
    .fetch_one(pool)
    .await
    .map_err(|err| format!("read the partitioning of {relation}: {err}"))
}

/// Create the month partitions of the partitioned log from the current month
/// (or from its default partition's floor, when the log was converted and the
/// months before it still belong to the old table) up to `ahead` months on,
/// named `<table>_p_YYYYMM`, each carrying the parent's row-level security
/// from the start. A month that cannot be created now (the table is busy, or
/// rows of that month already sit in the default partition) is skipped with a
/// warning: its rows land in the default partition, so no write fails, and the
/// next pass tries again. Returns the partitions created.
pub(crate) async fn create_months_ahead(
    pool: &PgPool,
    schema: &str,
    table: &str,
    ahead: i32,
) -> Result<i64, String> {
    let parent = qualified(schema, table);
    let months: Vec<(String, String, String)> = sqlx::query_as(
        "WITH floor AS ( \
             SELECT max((regexp_match(pg_get_constraintdef(con.oid), '< ''([^'']+)'''))[1]::timestamptz) AS floor_at \
             FROM pg_inherits i \
             JOIN pg_class c ON c.oid = i.inhrelid \
             JOIN pg_constraint con ON con.conrelid = c.oid \
             WHERE i.inhparent = to_regclass($1) \
               AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT' \
               AND con.contype = 'c' AND con.conname = $2 AND con.convalidated \
         ) \
         SELECT g::text, (g + interval '1 month')::text, to_char(g, 'YYYYMM') \
         FROM floor, generate_series( \
             GREATEST(date_trunc('month', now()), COALESCE(floor.floor_at, '-infinity'::timestamptz)), \
             date_trunc('month', now()) + ($3::int * interval '1 month'), \
             interval '1 month') AS g",
    )
    .bind(&parent)
    .bind(FLOOR_CONSTRAINT)
    .bind(ahead)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("plan the month partitions of {parent}: {err}"))?;
    let mut created = 0i64;
    let mut parent_rls: Option<RlsState> = None;
    for (start, end, suffix) in months {
        let child = qualified(schema, &format!("{table}_p_{suffix}"));
        let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(&child)
            .fetch_one(pool)
            .await
            .map_err(|err| format!("look up partition {child}: {err}"))?;
        if present {
            continue;
        }
        if parent_rls.is_none() {
            parent_rls = Some(rls_of(pool, schema, table).await?);
        }
        let secure = rls_sync_statements(
            &child,
            parent_rls.as_ref().expect("read above"),
            &RlsState::default(),
        );
        if create_month(pool, &parent, &child, &start, &end, &secure).await? {
            created += 1;
            tracing::info!(table = %parent, partition = %child, rls_statements = secure.len(), "created audit log month partition ahead of writes");
        }
    }
    Ok(created)
}

/// Create one month partition and its row-level security in one
/// transaction, serialized with every other creator of the log's months.
/// `Ok(false)` when the month was not created now (see [`skip_or_fail`]).
async fn create_month(
    pool: &PgPool,
    parent: &str,
    child: &str,
    start: &str,
    end: &str,
    secure: &[String],
) -> Result<bool, String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| format!("create partition {child}: {err}"))?;
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(LOCK_TIMEOUT)
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("create partition {child}: {err}"))?;
    if let Err(err) = sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("udb.audit_partition:{parent}"))
        .execute(&mut *tx)
        .await
    {
        return skip_or_fail(child, err).map(|()| false);
    }
    // Under the lock: a creator that went first already made it.
    let present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(child)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| format!("look up partition {child}: {err}"))?;
    if present {
        return Ok(false);
    }
    let ddl = format!(
        "CREATE TABLE {child} PARTITION OF {parent} FOR VALUES FROM ({}) TO ({})",
        ql(start),
        ql(end)
    );
    if let Err(err) = sqlx::query(&ddl).execute(&mut *tx).await {
        return skip_or_fail(child, err).map(|()| false);
    }
    for statement in secure {
        sqlx::query(statement)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("secure partition {child}: {statement}: {err}"))?;
    }
    tx.commit()
        .await
        .map_err(|err| format!("create partition {child}: {err}"))?;
    Ok(true)
}

/// A failed month creation: `Ok(())` when the month is simply not created
/// now (a racing creator won, the table is busy, rows of the month are
/// already in the default partition), the error otherwise.
fn skip_or_fail(child: &str, err: sqlx::Error) -> Result<(), String> {
    match sqlstate(&err).as_deref() {
        // Another creator won, or another partition covers the range.
        Some("42P07" | "42P17" | "23505") => Ok(()),
        Some(SQLSTATE_LOCK_NOT_AVAILABLE) => {
            tracing::warn!(partition = %child, "audit log busy: month partition not created this time, retried on the next pass");
            Ok(())
        }
        Some("23514") => {
            tracing::warn!(
                partition = %child,
                "rows of this month are already in the audit log's default partition, so the month \
                 stays there; they are deleted from it as they pass retention"
            );
            Ok(())
        }
        _ => Err(format!("create partition {child}: {err}")),
    }
}

fn sqlstate(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db) => db.code().map(|code| code.into_owned()),
        _ => None,
    }
}

async fn rls_of(pool: &PgPool, schema: &str, table: &str) -> Result<RlsState, String> {
    let (enabled, forced): (bool, bool) = sqlx::query_as(
        "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass",
    )
    .bind(qualified(schema, table))
    .fetch_one(pool)
    .await
    .map_err(|err| format!("read the row-level security of {schema}.{table}: {err}"))?;
    Ok(RlsState {
        enabled,
        forced,
        policies: relation_policies(pool, schema, table).await?,
    })
}

/// What [`convert_online`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Converted {
    pub relation: String,
    /// The log was already partitioned: nothing was done.
    pub already_partitioned: bool,
    /// The old table, now the default partition.
    pub legacy_partition: Option<String>,
    /// Where the month partitions start (B); older rows are in the legacy
    /// partition.
    pub months_from: Option<String>,
    pub partitions_created: Vec<String>,
    /// The planner's row estimate of the legacy partition.
    pub legacy_rows_estimate: i64,
    /// How long the swap held its lock.
    pub locked: Duration,
    /// Swap attempts (a busy table is retried).
    pub attempts: u32,
}

impl Converted {
    fn already(relation: String) -> Self {
        Self {
            relation,
            already_partitioned: true,
            legacy_partition: None,
            months_from: None,
            partitions_created: Vec::new(),
            legacy_rows_estimate: 0,
            locked: Duration::ZERO,
            attempts: 0,
        }
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "relation": self.relation,
            "already_partitioned": self.already_partitioned,
            "legacy_partition": self.legacy_partition,
            "months_from": self.months_from,
            "partitions_created": self.partitions_created,
            "legacy_rows_estimate": self.legacy_rows_estimate,
            "locked_ms": self.locked.as_millis() as u64,
            "attempts": self.attempts,
        })
    }
}

/// Convert the unpartitioned log `schema.table` into a monthly partitioned
/// table online, keeping every row where it is (see the module doc). A log
/// that is already partitioned is left as it is. Every refusal names why and
/// leaves the table as it was; the two indexes built for the conversion are
/// kept, so the next attempt does not build them again.
pub(crate) async fn convert_online(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<Converted, String> {
    let relation = qualified(schema, table);
    let legacy = format!("{table}_legacy");
    // The longest name the conversion gives: `<table>_legacy_occurred_at_idx`.
    if legacy.len() + "_occurred_at_idx".len() > MAX_IDENT {
        return Err(format!(
            "{relation}: the table name is too long for the partition and index names \
             the conversion gives; nothing changed"
        ));
    }
    let mut pooled = pool
        .acquire()
        .await
        .map_err(|err| format!("connect: {err}"))?;
    // The conversion lifts the pool's statement timeout for its index builds
    // and validation; the connection is closed afterwards instead of going
    // back to the pool with that setting.
    pooled.close_on_drop();
    let conn: &mut PgConnection = &mut pooled;
    let kind: Option<String> =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
            .bind(&relation)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| format!("look up {relation}: {err}"))?;
    match kind.as_deref() {
        None => {
            return Err(format!(
                "{relation} does not exist: nothing to convert (the audit sink creates it \
                 partitioned on its first write)"
            ));
        }
        Some("p") => return Ok(Converted::already(format!("{schema}.{table}"))),
        Some("r") => {}
        Some(other) => {
            return Err(format!(
                "{relation} is not a plain table (relkind {other}); nothing changed"
            ));
        }
    }
    let legacy_taken: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(qualified(schema, &legacy))
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| format!("look up {schema}.{legacy}: {err}"))?;
    if legacy_taken {
        return Err(format!(
            "{schema}.{legacy} already exists, and the old table would take that name; nothing \
             changed. Rename or drop it, then run the conversion again"
        ));
    }
    sqlx::query("SELECT set_config('statement_timeout', $1, false)")
        .bind(statement_timeout())
        .execute(&mut *conn)
        .await
        .map_err(|err| format!("set statement_timeout: {err}"))?;
    refuse_unconvertible(conn, &relation).await?;
    let unique_index = ensure_index(
        conn,
        schema,
        &relation,
        &format!("{table}_audit_id_occurred_at_key"),
        &["audit_id", "occurred_at"],
        true,
    )
    .await?;
    ensure_index(
        conn,
        schema,
        &relation,
        &format!("{table}_occurred_at_idx"),
        &["occurred_at"],
        false,
    )
    .await?;
    let floor = add_floor(conn, &relation).await?;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match swap(conn, schema, table, &legacy, &floor, &unique_index).await {
            Ok(mut done) => {
                done.attempts = attempt;
                tracing::info!(
                    table = %relation,
                    legacy = ?done.legacy_partition,
                    months_from = %floor,
                    locked_ms = done.locked.as_millis() as u64,
                    attempts = attempt,
                    "auth audit log converted to monthly partitions"
                );
                return Ok(done);
            }
            Err(SwapError::Busy) if attempt < LOCK_TRIES => {
                tracing::warn!(table = %relation, attempt, "auth audit log busy; retrying the partition swap");
                tokio::time::sleep(Duration::from_millis(500u64 << attempt.min(6))).await;
            }
            Err(SwapError::Busy) => {
                let undo = drop_floor(conn, &relation).await;
                return Err(format!(
                    "{relation} is busy, retry later: the swap could not take its lock within \
                     {LOCK_TIMEOUT} in {LOCK_TRIES} tries; nothing changed{}",
                    floor_note(&relation, &floor, undo)
                ));
            }
            Err(SwapError::Failed(why)) => {
                let undo = drop_floor(conn, &relation).await;
                return Err(format!(
                    "partitioning {relation} failed, nothing changed: {why}{}",
                    floor_note(&relation, &floor, undo)
                ));
            }
        }
    }
}

/// What to tell the operator when the floor constraint of a failed attempt
/// could not be removed: from B on it would refuse every write.
fn floor_note(relation: &str, floor: &str, undo: Result<(), String>) -> String {
    match undo {
        Ok(()) => String::new(),
        Err(why) => format!(
            "; but the CHECK constraint {FLOOR_CONSTRAINT} (occurred_at < {floor}) it added could \
             not be removed ({why}): run the conversion again, or remove it with ALTER TABLE \
             {relation} DROP CONSTRAINT {FLOOR_CONSTRAINT} before {floor}, when it would start \
             refusing writes"
        ),
    }
}

/// Everything the conversion cannot carry over to the partitioned table, as
/// one refusal.
async fn refuse_unconvertible(conn: &mut PgConnection, relation: &str) -> Result<(), String> {
    let checks: [(&str, String); 11] = [
        (
            "triggers",
            "SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal"
                .to_string(),
        ),
        (
            "foreign keys",
            "SELECT count(*) FROM pg_constraint WHERE contype = 'f' \
             AND (conrelid = $1::regclass OR confrelid = $1::regclass)"
                .to_string(),
        ),
        (
            "exclusion or deferrable constraints",
            "SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass \
             AND (contype = 'x' OR (contype IN ('p', 'u') AND condeferrable))"
                .to_string(),
        ),
        (
            "CHECK constraints",
            format!(
                "SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass \
                 AND contype = 'c' AND conname <> {}",
                ql(FLOOR_CONSTRAINT)
            ),
        ),
        (
            "identity columns",
            "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 \
             AND NOT attisdropped AND attidentity <> ''"
                .to_string(),
        ),
        (
            "column-level grants",
            "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 \
             AND NOT attisdropped AND attacl IS NOT NULL"
                .to_string(),
        ),
        (
            "views or rules over it",
            "SELECT count(*) FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid \
             WHERE d.refobjid = $1::regclass AND r.ev_class <> $1::regclass"
                .to_string(),
        ),
        (
            "inheritance",
            "SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass OR inhrelid = $1::regclass"
                .to_string(),
        ),
        (
            "explicit publication membership",
            "SELECT count(*) FROM pg_publication_rel WHERE prrelid = $1::regclass".to_string(),
        ),
        (
            "an unlogged or temporary table",
            "SELECT count(*) FROM pg_class WHERE oid = $1::regclass AND relpersistence <> 'p'"
                .to_string(),
        ),
        (
            "a replica identity index",
            "SELECT count(*) FROM pg_class WHERE oid = $1::regclass AND relreplident = 'i'"
                .to_string(),
        ),
    ];
    let mut reasons: Vec<&str> = Vec::new();
    for (what, sql) in &checks {
        let count: i64 = sqlx::query_scalar(sql.as_str())
            .bind(relation)
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| format!("inspect {relation} ({what}): {err}"))?;
        if count > 0 {
            reasons.push(*what);
        }
    }
    let columns: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT attname::text, format_type(atttypid, atttypmod), attnotnull FROM pg_attribute \
         WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped \
           AND attname IN ('audit_id', 'occurred_at')",
    )
    .bind(relation)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| format!("inspect the columns of {relation}: {err}"))?;
    let column = |name: &str| columns.iter().find(|(column, _, _)| column == name);
    if !column("audit_id").is_some_and(|(_, _, not_null)| *not_null) {
        reasons.push("no NOT NULL audit_id column");
    }
    if !column("occurred_at")
        .is_some_and(|(_, kind, not_null)| *not_null && kind == "timestamp with time zone")
    {
        reasons.push("no NOT NULL timestamptz occurred_at column");
    }
    if reasons.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "partitioning {relation} refused: it has {}; nothing changed",
            reasons.join(", ")
        ))
    }
}

/// A valid plain index on exactly `columns` (unique when `unique`) of
/// `relation`, built `CONCURRENTLY` under `name` when there is none. Returns
/// its name.
async fn ensure_index(
    conn: &mut PgConnection,
    schema: &str,
    relation: &str,
    name: &str,
    columns: &[&str],
    unique: bool,
) -> Result<String, String> {
    let wanted: Vec<String> = columns.iter().map(|column| column.to_string()).collect();
    let find = "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE i.indrelid = $1::regclass AND i.indisvalid AND i.indisready \
           AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = i.indnatts \
           AND i.indisunique = $3 \
           AND ARRAY(SELECT a.attname::text \
                     FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
                     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
                     ORDER BY k.ord) = $2::text[] \
         ORDER BY i.indisprimary DESC, c.relname LIMIT 1";
    let found: Option<String> = sqlx::query_scalar(find)
        .bind(relation)
        .bind(&wanted)
        .bind(unique)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| format!("read the indexes of {relation}: {err}"))?;
    if let Some(found) = found {
        return Ok(found);
    }
    let index = qualified(schema, name);
    let invalid: Option<bool> = sqlx::query_scalar(
        "SELECT NOT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass($1)",
    )
    .bind(&index)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| format!("look up index {index}: {err}"))?;
    match invalid {
        // A build an earlier attempt did not finish: built again.
        Some(true) => {
            let sql = format!("DROP INDEX CONCURRENTLY IF EXISTS {index}");
            sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&sql))
                .await
                .map_err(|err| format!("{sql}: {err}"))?;
        }
        Some(false) => {
            return Err(format!(
                "index {index} exists with another definition than ({}); nothing changed. \
                 Rename or drop it, then run the conversion again",
                columns.join(", ")
            ));
        }
        None => {}
    }
    let sql = format!(
        "CREATE {}INDEX CONCURRENTLY {} ON {relation} ({})",
        if unique { "UNIQUE " } else { "" },
        qi(name),
        columns
            .iter()
            .map(|column| qi(column))
            .collect::<Vec<_>>()
            .join(", ")
    );
    tracing::info!(table = %relation, index = %index, "building index for the audit log partitioning (no write lock)");
    sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&sql))
        .await
        .map_err(|err| format!("{sql}: {err}"))?;
    let found: Option<String> = sqlx::query_scalar(find)
        .bind(relation)
        .bind(&wanted)
        .bind(unique)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| format!("read the indexes of {relation}: {err}"))?;
    found.ok_or_else(|| format!("{sql} left no valid index; nothing changed"))
}

/// Add and validate the floor constraint `occurred_at < B`, B the first day
/// of the month after next. Validation reads the whole table but lets writes
/// through. A constraint an earlier attempt left with the same B is kept.
/// Returns B.
async fn add_floor(conn: &mut PgConnection, relation: &str) -> Result<String, String> {
    let floor: String =
        sqlx::query_scalar("SELECT (date_trunc('month', now()) + interval '2 months')::text")
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| format!("plan the partition floor: {err}"))?;
    let existing: Option<(bool, bool)> = sqlx::query_as(
        "SELECT con.convalidated, \
                COALESCE((regexp_match(pg_get_constraintdef(con.oid), '< ''([^'']+)'''))[1]::timestamptz \
                         = $3::timestamptz, false) \
         FROM pg_constraint con \
         WHERE con.conrelid = $1::regclass AND con.conname = $2 AND con.contype = 'c'",
    )
    .bind(relation)
    .bind(FLOOR_CONSTRAINT)
    .bind(&floor)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| format!("read the constraints of {relation}: {err}"))?;
    match existing {
        Some((true, true)) => return Ok(floor),
        Some(_) => {
            locked_alter(
                conn,
                relation,
                &format!(
                    "ALTER TABLE {relation} DROP CONSTRAINT IF EXISTS {}",
                    qi(FLOOR_CONSTRAINT)
                ),
            )
            .await?;
        }
        None => {}
    }
    locked_alter(
        conn,
        relation,
        &format!(
            "ALTER TABLE {relation} ADD CONSTRAINT {} CHECK (occurred_at < {}) NOT VALID",
            qi(FLOOR_CONSTRAINT),
            ql(&floor)
        ),
    )
    .await?;
    let validate = format!(
        "ALTER TABLE {relation} VALIDATE CONSTRAINT {}",
        qi(FLOOR_CONSTRAINT)
    );
    if let Err(err) = sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&validate)).await {
        let undo = drop_floor(conn, relation).await;
        return Err(format!(
            "{relation} could not take the constraint occurred_at < {floor} (a row dated \
             {floor} or later cannot stay in the old table as its default partition): {err}; \
             nothing changed{}",
            floor_note(relation, &floor, undo)
        ));
    }
    Ok(floor)
}

async fn drop_floor(conn: &mut PgConnection, relation: &str) -> Result<(), String> {
    locked_alter(
        conn,
        relation,
        &format!(
            "ALTER TABLE {relation} DROP CONSTRAINT IF EXISTS {}",
            qi(FLOOR_CONSTRAINT)
        ),
    )
    .await
}

/// Run one `ALTER TABLE` that takes a short exclusive lock, in its own
/// transaction under the lock timeout, retried with backoff while the table is
/// busy.
async fn locked_alter(conn: &mut PgConnection, relation: &str, sql: &str) -> Result<(), String> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let mut tx = conn.begin().await.map_err(|err| format!("{sql}: {err}"))?;
        sqlx::query("SELECT set_config('lock_timeout', $1, true)")
            .bind(LOCK_TIMEOUT)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("{sql}: {err}"))?;
        match sqlx::Executor::execute(&mut *tx, sqlx::raw_sql(sql)).await {
            Ok(_) => {
                tx.commit().await.map_err(|err| format!("{sql}: {err}"))?;
                return Ok(());
            }
            Err(err) if sqlstate(&err).as_deref() == Some(SQLSTATE_LOCK_NOT_AVAILABLE) => {
                drop(tx);
                if attempt >= LOCK_TRIES {
                    return Err(format!(
                        "{relation} is busy, retry later: `{sql}` could not take its lock \
                         within {LOCK_TIMEOUT} in {LOCK_TRIES} tries"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(500u64 << attempt.min(6))).await;
            }
            Err(err) => return Err(format!("{sql}: {err}")),
        }
    }
}

enum SwapError {
    Busy,
    Failed(String),
}

impl From<sqlx::Error> for SwapError {
    fn from(err: sqlx::Error) -> Self {
        if sqlstate(&err).as_deref() == Some(SQLSTATE_LOCK_NOT_AVAILABLE) {
            SwapError::Busy
        } else {
            SwapError::Failed(err.to_string())
        }
    }
}

/// The locked part, one transaction: rename the old table to `legacy`, make
/// `unique_index` its primary key, create the partitioned parent under the
/// original name with the old table's columns, defaults, row-level security,
/// policies, grants, comment and owner, attach the old table as the default
/// partition and create the month starting at `floor`.
async fn swap(
    conn: &mut PgConnection,
    schema: &str,
    table: &str,
    legacy: &str,
    floor: &str,
    unique_index: &str,
) -> Result<Converted, SwapError> {
    let relation = qualified(schema, table);
    let legacy_relation = qualified(schema, legacy);
    let mut tx = conn.begin().await?;
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(LOCK_TIMEOUT)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(SWAP_STATEMENT_TIMEOUT)
        .execute(&mut *tx)
        .await?;
    let started = Instant::now();
    sqlx::query(&format!("LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *tx)
        .await?;
    // Under the lock, again: another session may have converted or changed
    // the table since the preparation.
    let kind: String =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = $1::regclass")
            .bind(&relation)
            .fetch_one(&mut *tx)
            .await?;
    if kind != "r" {
        return Err(SwapError::Failed(format!(
            "{relation} is no longer a plain table (relkind {kind}): another session changed it"
        )));
    }
    let floor_held: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = $1::regclass \
         AND conname = $2 AND contype = 'c' AND convalidated)",
    )
    .bind(&relation)
    .bind(FLOOR_CONSTRAINT)
    .fetch_one(&mut *tx)
    .await?;
    if !floor_held {
        return Err(SwapError::Failed(format!(
            "the constraint {FLOOR_CONSTRAINT} was removed from {relation} during the preparation"
        )));
    }
    refuse_unconvertible(&mut tx, &relation)
        .await
        .map_err(SwapError::Failed)?;
    let (owner, rls, forced, comment): (String, bool, bool, Option<String>) = sqlx::query_as(
        "SELECT pg_get_userbyid(relowner)::text, relrowsecurity, relforcerowsecurity, \
                obj_description(oid, 'pg_class') \
         FROM pg_class WHERE oid = $1::regclass",
    )
    .bind(&relation)
    .fetch_one(&mut *tx)
    .await?;
    let grants: Vec<(Option<String>, String, bool)> = sqlx::query_as(
        "SELECT CASE WHEN a.grantee = 0 THEN NULL ELSE pg_get_userbyid(a.grantee)::text END, \
                a.privilege_type::text, a.is_grantable \
         FROM pg_class c, aclexplode(c.relacl) a \
         WHERE c.oid = $1::regclass AND a.grantee <> c.relowner",
    )
    .bind(&relation)
    .fetch_all(&mut *tx)
    .await?;
    let policy_rows: Vec<PolicyRow> = sqlx::query_as(
        "SELECT policyname::text, permissive, roles::text[], cmd, qual, with_check \
         FROM pg_policies WHERE schemaname = $1 AND tablename = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(&mut *tx)
    .await?;
    let policies: Vec<PolicyDef> = policy_rows.into_iter().map(policy_from_row).collect();
    let primary_key: Option<(String, Vec<String>)> = sqlx::query_as(
        "SELECT con.conname::text, \
                ARRAY(SELECT a.attname::text \
                      FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) \
                      JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum \
                      ORDER BY k.ord) \
         FROM pg_constraint con WHERE con.conrelid = $1::regclass AND con.contype = 'p'",
    )
    .bind(&relation)
    .fetch_optional(&mut *tx)
    .await?;
    let primary_index: Option<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE i.indrelid = $1::regclass AND i.indisprimary",
    )
    .bind(&relation)
    .fetch_optional(&mut *tx)
    .await?;
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE i.indrelid = $1::regclass ORDER BY c.relname",
    )
    .bind(&relation)
    .fetch_all(&mut *tx)
    .await?;
    let (month_end, month_suffix): (String, String) = sqlx::query_as(
        "SELECT ($1::timestamptz + interval '1 month')::text, to_char($1::timestamptz, 'YYYYMM')",
    )
    .bind(floor)
    .fetch_one(&mut *tx)
    .await?;
    let month = qualified(schema, &format!("{table}_p_{month_suffix}"));

    let key_is_partition_ready = primary_key.as_ref().is_some_and(|(_, columns)| {
        columns
            .iter()
            .map(String::as_str)
            .eq(["audit_id", "occurred_at"])
    });
    let mut statements = vec![format!("ALTER TABLE {relation} RENAME TO {}", qi(legacy))];
    if !key_is_partition_ready {
        // A key of a partitioned table must hold the partition column: the
        // old key on audit_id alone gives way to the unique index on
        // (audit_id, occurred_at) built beforehand (no scan: both columns
        // are already NOT NULL).
        if let Some((name, _)) = &primary_key {
            // audit_id is already NOT NULL (it was the key): this only makes
            // that survive the key's removal, without a scan.
            statements.push(format!(
                "ALTER TABLE {legacy_relation} ALTER COLUMN audit_id SET NOT NULL"
            ));
            statements.push(format!(
                "ALTER TABLE {legacy_relation} DROP CONSTRAINT {}",
                qi(name)
            ));
        }
        statements.push(format!(
            "ALTER TABLE {legacy_relation} ADD CONSTRAINT {} PRIMARY KEY USING INDEX {}",
            qi(&format!("{legacy}_pkey")),
            qi(unique_index)
        ));
    }
    // Free the original's index names for the parent.
    for name in &indexes {
        let replaced = !key_is_partition_ready
            && (primary_index.as_ref() == Some(name) || name == unique_index);
        if replaced {
            continue;
        }
        if let Some(rest) = name.strip_prefix(table) {
            let renamed = format!("{legacy}{rest}");
            if renamed.len() > MAX_IDENT {
                return Err(SwapError::Failed(format!(
                    "index {name} cannot be renamed to {renamed}: longer than {MAX_IDENT} characters"
                )));
            }
            statements.push(format!(
                "ALTER INDEX {} RENAME TO {}",
                qualified(schema, name),
                qi(&renamed)
            ));
        }
    }
    statements.push(format!(
        "CREATE TABLE {relation} (LIKE {legacy_relation} INCLUDING DEFAULTS INCLUDING GENERATED \
         INCLUDING STORAGE INCLUDING COMMENTS, CONSTRAINT {} PRIMARY KEY (audit_id, occurred_at)) \
         PARTITION BY RANGE (occurred_at)",
        qi(&format!("{table}_pkey"))
    ));
    statements.push(format!(
        "CREATE INDEX {} ON {relation} (occurred_at)",
        qi(&format!("{table}_occurred_at_idx"))
    ));
    if rls {
        statements.push(format!("ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY"));
    }
    if forced {
        statements.push(format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"));
    }
    if let Some(comment) = &comment {
        statements.push(format!("COMMENT ON TABLE {relation} IS {}", ql(comment)));
    }
    for policy in &policies {
        statements.push(policy.create_on(&relation));
    }
    // The old table as it is: its indexes on the parent's key and on
    // occurred_at are adopted, nothing is rebuilt or scanned (it has no
    // sibling partition yet).
    statements.push(format!(
        "ALTER TABLE {relation} ATTACH PARTITION {legacy_relation} DEFAULT"
    ));
    // The first month: the default partition's floor constraint proves it
    // holds no row of it, so it is not scanned.
    statements.push(format!(
        "CREATE TABLE {month} PARTITION OF {relation} FOR VALUES FROM ({}) TO ({})",
        ql(floor),
        ql(&month_end)
    ));
    let parent_rls = RlsState {
        enabled: rls,
        forced,
        policies: policies.clone(),
    };
    statements.extend(rls_sync_statements(
        &month,
        &parent_rls,
        &RlsState::default(),
    ));
    for (grantee, privilege, grantable) in grants {
        statements.push(format!(
            "GRANT {privilege} ON {relation} TO {}{}",
            grantee
                .map(|role| qi(&role))
                .unwrap_or_else(|| "PUBLIC".to_string()),
            if grantable { " WITH GRANT OPTION" } else { "" }
        ));
    }
    // Last: once the owner changes, this role may no longer alter them.
    statements.push(format!("ALTER TABLE {month} OWNER TO {}", qi(&owner)));
    statements.push(format!("ALTER TABLE {relation} OWNER TO {}", qi(&owner)));
    for sql in &statements {
        if let Err(err) = sqlx::Executor::execute(&mut *tx, sqlx::raw_sql(sql)).await {
            return Err(match SwapError::from(err) {
                SwapError::Busy => SwapError::Busy,
                SwapError::Failed(why) => SwapError::Failed(format!("{sql}: {why}")),
            });
        }
    }
    let legacy_rows_estimate: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = $1::regclass",
    )
    .bind(&legacy_relation)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Converted {
        relation: format!("{schema}.{table}"),
        already_partitioned: false,
        legacy_partition: Some(format!("{schema}.{legacy}")),
        months_from: Some(floor.to_string()),
        partitions_created: vec![format!("{schema}.{table}_p_{month_suffix}")],
        legacy_rows_estimate,
        locked: started.elapsed(),
        attempts: 0,
    })
}

#[cfg(test)]
mod tests;
