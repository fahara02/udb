//! Online compaction: give a bloated table's space back to the operating
//! system without locking it while its rows are copied.
//!
//! `VACUUM FULL` rewrites a table under an ACCESS EXCLUSIVE lock held for the
//! whole rewrite: on a multi-gigabyte audit log every read and write of it
//! waits minutes. Compaction instead (the pg_repack approach, without the
//! extension):
//!
//! 1. creates an empty copy of the table (columns, defaults, constraints,
//!    every index under a temporary name), marked as UDB's;
//! 2. copies the rows into it page range by page range, each range its own
//!    short transaction, while the table stays fully in use;
//! 3. takes the lock only for the swap, with a short `lock_timeout` (a busy
//!    table makes it give up and retry rather than queue every other session
//!    behind it): applies the rows that changed during the copy, checks the
//!    row counts match, moves grants, row-level security, owned sequences and
//!    the table comment over, renames the copy into place and drops the old
//!    table in the same transaction.
//!
//! A table whose shape it cannot move faithfully (triggers, foreign keys,
//! identity columns, views over it, inheritance, publications, column grants,
//! exclusion or deferrable constraints, no primary key) is refused with the
//! reason, never compacted partially. A partitioned table is compacted
//! partition by partition: the bloated partition is detached and its copy
//! attached under the same bound, within the same short locked swap (the
//! parent is locked too, as DETACH requires).

use std::time::{Duration, Instant};

use sqlx::{Connection, PgConnection, PgPool};

use super::{PolicyDef, qi, ql, statement_timeout, sweep_pages};

/// Marker comment on the copy, so a leftover from an interrupted run is
/// recognised as UDB's and is the only table compaction ever drops unasked.
const MARKER: &str = "udb online compaction in progress";
/// Below this a table is not worth copying.
const MIN_BYTES: i64 = 8 * 1024 * 1024;
/// How long the swap waits for its lock before giving up and retrying.
const LOCK_TIMEOUT: &str = "3s";
const LOCK_TRIES: u32 = 5;
const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";

/// What one compaction did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Compacted {
    pub rows: i64,
    pub bytes_before: i64,
    pub bytes_after: i64,
    /// How long the swap held the lock.
    pub locked: Duration,
}

/// The table as the catalog describes it.
struct Shape {
    oid: i64,
    schema: String,
    table: String,
    bytes: i64,
    /// Insertable columns (generated ones are recomputed by the copy).
    columns: Vec<String>,
    /// For a partition: its parent and its bound (`FOR VALUES ...` or
    /// `DEFAULT`), so the copy is attached in its place.
    partition: Option<(String, String)>,
}

impl Shape {
    fn qualified(&self) -> String {
        format!("{}.{}", qi(&self.schema), qi(&self.table))
    }
    fn copy_name(&self) -> String {
        format!("{}_udb_compact", self.table)
    }
    fn copy(&self) -> String {
        format!("{}.{}", qi(&self.schema), qi(&self.copy_name()))
    }
    fn column_list(&self, alias: &str) -> String {
        self.columns
            .iter()
            .map(|column| format!("{alias}{}", qi(column)))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Compact `relation` when it is worth compacting: more than twice its live
/// size, or (`force`) any table over 8 MiB. A partitioned table is compacted
/// leaf by leaf, each partition swapped for its compact copy under the same
/// bound. `Ok(None)` when nothing was worth it. Every refusal is an `Err`
/// naming why; for a partitioned table the other leaves are still compacted.
pub(crate) async fn compact_online(
    pool: &PgPool,
    relation: &str,
    force: bool,
) -> Result<Option<Compacted>, String> {
    let kind: Option<String> =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
            .bind(relation)
            .fetch_optional(pool)
            .await
            .map_err(|err| format!("look up the table: {err}"))?;
    if kind.as_deref() != Some("p") {
        return compact_one(pool, relation, force).await;
    }
    let mut conn = pool.acquire().await.map_err(|err| err.to_string())?;
    let leaves = leaves(&mut conn, relation).await?;
    drop(conn);
    let mut total: Option<Compacted> = None;
    let mut failures = Vec::new();
    for leaf in leaves {
        match compact_one(pool, &leaf, force).await {
            Ok(Some(done)) => {
                let sum = total.get_or_insert(Compacted {
                    rows: 0,
                    bytes_before: 0,
                    bytes_after: 0,
                    locked: Duration::ZERO,
                });
                sum.rows += done.rows;
                sum.bytes_before += done.bytes_before;
                sum.bytes_after += done.bytes_after;
                sum.locked = sum.locked.max(done.locked);
            }
            Ok(None) => {}
            Err(why) => failures.push(why),
        }
    }
    if failures.is_empty() {
        Ok(total)
    } else {
        Err(failures.join("; "))
    }
}

async fn compact_one(
    pool: &PgPool,
    relation: &str,
    force: bool,
) -> Result<Option<Compacted>, String> {
    let mut conn = pool.acquire().await.map_err(|err| err.to_string())?;
    // One compaction of a table at a time, across every broker and CLI run:
    // a session lock on the dedicated connection all the work runs on.
    let Some(shape) = describe(&mut conn, relation).await? else {
        return Ok(None);
    };
    if !worth_compacting(&mut conn, &shape, force).await? {
        return Ok(None);
    }
    let locked: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext('udb_compact'), $1::int)")
            .bind(shape.oid as i32)
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
    if !locked {
        return Err(format!(
            "online compaction of {}: another compaction of it is running",
            shape.qualified()
        ));
    }
    let result = compact_locked(&mut conn, &shape).await;
    if result.is_err() {
        // Leave nothing behind but the original table.
        let _ = drop_copy(&mut conn, &shape).await;
    }
    let unlocked = sqlx::query("SELECT pg_advisory_unlock(hashtext('udb_compact'), $1::int)")
        .bind(shape.oid as i32)
        .execute(&mut *conn)
        .await;
    if unlocked.is_err() {
        conn.close_on_drop();
    }
    result.map(Some)
}

async fn compact_locked(conn: &mut PgConnection, shape: &Shape) -> Result<Compacted, String> {
    refuse_unmovable(conn, shape).await?;
    drop_copy(conn, shape).await?;
    let indexes = create_copy(conn, shape).await?;
    copy_rows(conn, shape).await?;
    // The rows changed during the copy, applied without the lock first, so
    // the locked pass has little left to do.
    {
        let mut tx = admin_tx(conn).await?;
        apply_changes(&mut tx, shape).await?;
        tx.commit().await.map_err(|err| err.to_string())?;
    }
    let mut tries = 0;
    let (rows, locked) = loop {
        tries += 1;
        match swap(conn, shape, &indexes).await {
            Ok(done) => break done,
            Err(SwapError::Busy) if tries < LOCK_TRIES => {
                tracing::info!(table = %shape.qualified(), tries, "online compaction: table busy, retrying the swap");
                tokio::time::sleep(Duration::from_secs(2 * u64::from(tries))).await;
            }
            Err(SwapError::Busy) => {
                return Err(format!(
                    "online compaction of {}: the table stayed busy, its lock was not free within {LOCK_TIMEOUT} in {LOCK_TRIES} tries; nothing changed, run it again later",
                    shape.qualified()
                ));
            }
            Err(SwapError::Failed(why)) => {
                return Err(format!("online compaction of {}: {why}", shape.qualified()));
            }
        }
    };
    let analyze = format!("ANALYZE {}", shape.qualified());
    sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&analyze))
        .await
        .map_err(|err| format!("ANALYZE after compaction: {err}"))?;
    let bytes_after: i64 = sqlx::query_scalar("SELECT pg_total_relation_size($1::regclass)")
        .bind(shape.qualified())
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| err.to_string())?;
    tracing::info!(
        table = %shape.qualified(),
        rows,
        bytes_before = shape.bytes,
        bytes_after,
        locked_ms = locked.as_millis() as u64,
        "online compaction gave the table's free space back"
    );
    Ok(Compacted {
        rows,
        bytes_before: shape.bytes,
        bytes_after,
        locked,
    })
}

async fn describe(conn: &mut PgConnection, relation: &str) -> Result<Option<Shape>, String> {
    let row: Option<(i64, String, String, String, bool, i64)> = sqlx::query_as(
        "SELECT c.oid::bigint, n.nspname::text, c.relname::text, c.relkind::text, \
                c.relispartition, pg_total_relation_size(c.oid) \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = to_regclass($1)",
    )
    .bind(relation)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| format!("look up the table: {err}"))?;
    let Some((oid, schema, table, kind, is_partition, bytes)) = row else {
        return Ok(None);
    };
    if kind != "r" {
        return Ok(None);
    }
    let partition = if is_partition {
        let (parent, bound): (String, String) = sqlx::query_as(
            "SELECT i.inhparent::regclass::text, pg_get_expr(c.relpartbound, c.oid) \
             FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid WHERE c.oid = $1::oid",
        )
        .bind(oid)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| format!("read the partition bound: {err}"))?;
        Some((parent, bound))
    } else {
        None
    };
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT attname::text FROM pg_attribute \
         WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped AND attgenerated = '' \
         ORDER BY attnum",
    )
    .bind(oid)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| format!("read the columns: {err}"))?;
    Ok(Some(Shape {
        oid,
        schema,
        table,
        bytes,
        columns,
        partition,
    }))
}

/// The leaf partitions of a partitioned table, as qualified names.
async fn leaves(conn: &mut PgConnection, relation: &str) -> Result<Vec<String>, String> {
    sqlx::query_scalar(
        "SELECT relid::regclass::text FROM pg_partition_tree($1::regclass) \
         WHERE isleaf ORDER BY relid::regclass::text",
    )
    .bind(relation)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| format!("list the partitions: {err}"))
}

/// More than twice the live estimate (rows × average row width from the
/// planner statistics), or any table over 8 MiB when forced.
async fn worth_compacting(
    conn: &mut PgConnection,
    shape: &Shape,
    force: bool,
) -> Result<bool, String> {
    if shape.bytes < MIN_BYTES {
        return Ok(false);
    }
    if force {
        return Ok(true);
    }
    let (tuples, width): (f64, i64) = sqlx::query_as(
        "SELECT GREATEST(c.reltuples, 0)::float8, \
                COALESCE((SELECT SUM(avg_width)::bigint FROM pg_stats \
                          WHERE schemaname = $2 AND tablename = $3), 0) \
         FROM pg_class c WHERE c.oid = $1::oid",
    )
    .bind(shape.oid)
    .bind(&shape.schema)
    .bind(&shape.table)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| format!("estimate the live size: {err}"))?;
    let live = tuples * (width as f64 + 32.0);
    Ok(shape.bytes as f64 > 2.0 * live.max(MIN_BYTES as f64 / 2.0))
}

/// Everything compaction cannot carry over faithfully, as one refusal.
async fn refuse_unmovable(conn: &mut PgConnection, shape: &Shape) -> Result<(), String> {
    let checks: [(&str, &str); 11] = [
        (
            "triggers",
            // A partition's triggers cloned from its parent are cloned onto
            // the copy again when it is attached.
            "SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::oid AND NOT tgisinternal \
             AND tgparentid = 0",
        ),
        (
            "foreign keys",
            // Likewise a partition's foreign keys inherited from its parent.
            "SELECT count(*) FROM pg_constraint WHERE contype = 'f' \
             AND ((conrelid = $1::oid AND conparentid = 0) OR confrelid = $1::oid)",
        ),
        (
            "exclusion or deferrable constraints",
            "SELECT count(*) FROM pg_constraint WHERE conrelid = $1::oid \
             AND (contype = 'x' OR (contype IN ('p', 'u') AND condeferrable))",
        ),
        (
            "identity columns",
            "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::oid AND attnum > 0 \
             AND NOT attisdropped AND attidentity <> ''",
        ),
        (
            "column-level grants",
            "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::oid AND attnum > 0 \
             AND NOT attisdropped AND attacl IS NOT NULL",
        ),
        (
            "views or rules over it",
            "SELECT count(*) FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid \
             WHERE d.refobjid = $1::oid AND r.ev_class <> $1::oid",
        ),
        (
            "inheritance",
            // A partition's own link to its parent is moved by the swap;
            // classic inheritance, or partitions below it, are not.
            "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE i.inhparent = $1::oid OR (i.inhrelid = $1::oid AND NOT c.relispartition)",
        ),
        (
            "explicit publication membership",
            "SELECT count(*) FROM pg_publication_rel WHERE prrelid = $1::oid",
        ),
        (
            "an unlogged or temporary table",
            "SELECT count(*) FROM pg_class WHERE oid = $1::oid AND relpersistence <> 'p'",
        ),
        (
            "a replica identity index",
            "SELECT count(*) FROM pg_class WHERE oid = $1::oid AND relreplident = 'i'",
        ),
        (
            "extended statistics",
            "SELECT count(*) FROM pg_statistic_ext WHERE stxrelid = $1::oid",
        ),
    ];
    let mut reasons = Vec::new();
    for (what, sql) in checks {
        let count: i64 = sqlx::query_scalar(sql)
            .bind(shape.oid)
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| format!("inspect the table ({what}): {err}"))?;
        if count > 0 {
            reasons.push(what);
        }
    }
    let has_key: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_index WHERE indrelid = $1::oid AND indisprimary)",
    )
    .bind(shape.oid)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| format!("inspect the table (primary key): {err}"))?;
    if !has_key {
        reasons.push("no primary key");
    }
    if reasons.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "online compaction of {} refused: it has {}; use --reclaim full in a maintenance window",
            shape.qualified(),
            reasons.join(", ")
        ))
    }
}

/// Drop the copy left by an interrupted run: only a table carrying the
/// marker. Anything else under that name is refused, never dropped.
async fn drop_copy(conn: &mut PgConnection, shape: &Shape) -> Result<(), String> {
    let comment: Option<Option<String>> =
        sqlx::query_scalar("SELECT obj_description(to_regclass($1), 'pg_class')")
            .bind(shape.copy())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(shape.copy())
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| err.to_string())?;
    if !exists {
        return Ok(());
    }
    if comment.flatten().as_deref() != Some(MARKER) {
        return Err(format!(
            "online compaction of {}: {} exists and is not a compaction copy; rename or drop it",
            shape.qualified(),
            shape.copy()
        ));
    }
    let sql = format!("DROP TABLE {}", shape.copy());
    sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&sql))
        .await
        .map_err(|err| format!("drop the leftover copy: {err}"))?;
    Ok(())
}

/// One index of the original: its name, the name of its twin on the copy,
/// and the constraint it backs (name and kind), if any.
struct IndexPlan {
    name: String,
    temp: String,
    constraint: Option<(String, String)>,
}

/// An index of the original as the catalog lists it: oid, name, definition
/// tail (from ` USING`), and the name and kind of the constraint it backs.
type IndexRow = (i64, String, String, Option<String>, Option<String>);

async fn create_copy(conn: &mut PgConnection, shape: &Shape) -> Result<Vec<IndexPlan>, String> {
    let rows: Vec<IndexRow> = sqlx::query_as(
        "SELECT i.indexrelid::bigint, c.relname::text, \
                substring(pg_get_indexdef(i.indexrelid) from ' USING .*$'), \
                con.conname::text, con.contype::text \
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.conrelid = i.indrelid \
         WHERE i.indrelid = $1::oid ORDER BY c.relname",
    )
    .bind(shape.oid)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| format!("read the indexes: {err}"))?;
    let unique: Vec<i64> = sqlx::query_scalar(
        "SELECT indexrelid::bigint FROM pg_index WHERE indrelid = $1::oid AND indisunique",
    )
    .bind(shape.oid)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    let reloptions: Option<Vec<String>> =
        sqlx::query_scalar("SELECT reloptions FROM pg_class WHERE oid = $1::oid")
            .bind(shape.oid)
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| err.to_string())?;
    let mut statements = vec![format!(
        "CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED \
         INCLUDING STORAGE INCLUDING COMMENTS)",
        shape.copy(),
        shape.qualified()
    )];
    statements.push(format!(
        "COMMENT ON TABLE {} IS {}",
        shape.copy(),
        ql(MARKER)
    ));
    if let Some(options) = reloptions.filter(|options| !options.is_empty()) {
        statements.push(format!(
            "ALTER TABLE {} SET ({})",
            shape.copy(),
            options.join(", ")
        ));
    }
    let mut plans = Vec::new();
    for (oid, name, tail, constraint, kind) in rows {
        let temp = format!("udbc_{oid}");
        let unique_kw = if unique.contains(&oid) { "UNIQUE " } else { "" };
        statements.push(format!(
            "CREATE {unique_kw}INDEX {} ON {}{tail}",
            qi(&temp),
            shape.copy()
        ));
        plans.push(IndexPlan {
            name,
            temp,
            constraint: constraint.zip(kind),
        });
    }
    for sql in statements {
        sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(&sql))
            .await
            .map_err(|err| format!("create the copy: {err}"))?;
    }
    Ok(plans)
}

/// A transaction with the platform-admin escape (these tables FORCE
/// row-level security; a copy that saw one tenant would lose the rest) and
/// cleanup's statement timeout, on the compaction's own connection.
async fn admin_tx(
    conn: &mut PgConnection,
) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, String> {
    let mut tx = conn.begin().await.map_err(|err| err.to_string())?;
    sqlx::query("SELECT set_config('app.platform_admin', 'true', true)")
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(statement_timeout())
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    Ok(tx)
}

/// Copy every row, page range by page range, each range its own
/// transaction; a row already copied (it moved pages during the copy) is
/// skipped and settled by [`apply_changes`].
async fn copy_rows(conn: &mut PgConnection, shape: &Shape) -> Result<(), String> {
    let pages: i64 = sqlx::query_scalar(
        "SELECT (pg_relation_size($1::oid) / current_setting('block_size')::bigint)",
    )
    .bind(shape.oid)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    let step = sweep_pages().max(1);
    let columns = shape.column_list("");
    let mut start = 0i64;
    while start < pages {
        let end = start + step;
        let sql = format!(
            "INSERT INTO {copy} ({columns}) SELECT {columns} FROM {table} \
             WHERE ctid >= '({start},0)'::tid AND ctid < '({end},0)'::tid \
             ON CONFLICT DO NOTHING",
            copy = shape.copy(),
            table = shape.qualified(),
        );
        let mut tx = admin_tx(conn).await?;
        sqlx::query(&sql)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("copy pages {start}..{end}: {err}"))?;
        tx.commit().await.map_err(|err| err.to_string())?;
        start = end;
    }
    Ok(())
}

/// Make the copy hold exactly the original's rows: drop copied rows that no
/// longer exist as they are (deleted or updated since), then add the
/// original's rows the copy lacks. Rows compare by their whole text form, so
/// every column type works.
async fn apply_changes(tx: &mut PgConnection, shape: &Shape) -> Result<(), String> {
    let o = shape.column_list("o.");
    let n = shape.column_list("n.");
    let stale = format!(
        "DELETE FROM {copy} n WHERE NOT EXISTS (SELECT 1 FROM {table} o \
         WHERE ROW({o})::text = ROW({n})::text)",
        copy = shape.copy(),
        table = shape.qualified(),
    );
    sqlx::query(&stale)
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("drop changed rows from the copy: {err}"))?;
    let missing = format!(
        "INSERT INTO {copy} ({columns}) SELECT {o} FROM {table} o \
         WHERE NOT EXISTS (SELECT 1 FROM {copy} n WHERE ROW({n})::text = ROW({o})::text)",
        copy = shape.copy(),
        table = shape.qualified(),
        columns = shape.column_list(""),
    );
    sqlx::query(&missing)
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("add new rows to the copy: {err}"))?;
    Ok(())
}

enum SwapError {
    Busy,
    Failed(String),
}

impl From<sqlx::Error> for SwapError {
    fn from(err: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &err
            && db.code().as_deref() == Some(SQLSTATE_LOCK_NOT_AVAILABLE)
        {
            return SwapError::Busy;
        }
        SwapError::Failed(err.to_string())
    }
}

/// The locked part: settle the last changes, prove the copy is whole, move
/// everything that belongs to the table, rename, drop the original.
async fn swap(
    conn: &mut PgConnection,
    shape: &Shape,
    indexes: &[IndexPlan],
) -> Result<(i64, Duration), SwapError> {
    let mut tx = admin_tx(conn).await.map_err(SwapError::Failed)?;
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(LOCK_TIMEOUT)
        .execute(&mut *tx)
        .await?;
    let started = Instant::now();
    // A partition is detached from its parent, which needs the parent's lock
    // too: taken first, parent before partition, the order DDL on it uses.
    let lock = match &shape.partition {
        Some((parent, _)) => format!(
            "LOCK TABLE ONLY {parent}, ONLY {} IN ACCESS EXCLUSIVE MODE",
            shape.qualified()
        ),
        None => format!("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE", shape.qualified()),
    };
    sqlx::query(&lock).execute(&mut *tx).await?;
    // Once held, nothing else in the swap should wait on a lock.
    sqlx::query("SELECT set_config('lock_timeout', '0', true)")
        .execute(&mut *tx)
        .await?;
    apply_changes(&mut tx, shape)
        .await
        .map_err(SwapError::Failed)?;
    let (original, copied): (i64, i64) = sqlx::query_as(&format!(
        "SELECT (SELECT count(*) FROM {}), (SELECT count(*) FROM {})",
        shape.qualified(),
        shape.copy()
    ))
    .fetch_one(&mut *tx)
    .await?;
    if original != copied {
        return Err(SwapError::Failed(format!(
            "the copy holds {copied} rows and the table {original}; nothing changed"
        )));
    }
    let policies = table_policies(&mut tx, shape)
        .await
        .map_err(SwapError::Failed)?;
    let mut statements = Vec::new();
    // Owner, grants, row-level security, the table comment.
    let (owner, rls, forced, replident, comment): (String, bool, bool, String, Option<String>) =
        sqlx::query_as(
            "SELECT pg_get_userbyid(relowner)::text, relrowsecurity, relforcerowsecurity, \
                    relreplident::text, obj_description(oid, 'pg_class') \
             FROM pg_class WHERE oid = $1::oid",
        )
        .bind(shape.oid)
        .fetch_one(&mut *tx)
        .await?;
    statements.push(format!(
        "ALTER TABLE {} OWNER TO {}",
        shape.copy(),
        qi(&owner)
    ));
    let grants: Vec<(Option<String>, String, bool)> = sqlx::query_as(
        "SELECT CASE WHEN a.grantee = 0 THEN NULL ELSE pg_get_userbyid(a.grantee)::text END, \
                a.privilege_type::text, a.is_grantable \
         FROM pg_class c, aclexplode(c.relacl) a \
         WHERE c.oid = $1::oid AND a.grantee <> c.relowner",
    )
    .bind(shape.oid)
    .fetch_all(&mut *tx)
    .await?;
    for (grantee, privilege, grantable) in grants {
        statements.push(format!(
            "GRANT {privilege} ON {} TO {}{}",
            shape.copy(),
            grantee
                .map(|role| qi(&role))
                .unwrap_or_else(|| "PUBLIC".to_string()),
            if grantable { " WITH GRANT OPTION" } else { "" }
        ));
    }
    if rls {
        statements.push(format!(
            "ALTER TABLE {} ENABLE ROW LEVEL SECURITY",
            shape.copy()
        ));
    }
    if forced {
        statements.push(format!(
            "ALTER TABLE {} FORCE ROW LEVEL SECURITY",
            shape.copy()
        ));
    }
    match replident.as_str() {
        "f" => statements.push(format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            shape.copy()
        )),
        "n" => statements.push(format!(
            "ALTER TABLE {} REPLICA IDENTITY NOTHING",
            shape.copy()
        )),
        _ => {}
    }
    statements.push(format!(
        "COMMENT ON TABLE {} IS {}",
        shape.copy(),
        comment
            .as_deref()
            .map(ql)
            .unwrap_or_else(|| "NULL".to_string())
    ));
    // Sequences the original owns (serial columns) follow their column.
    let owned: Vec<(String, String)> = sqlx::query_as(
        "SELECT d.objid::regclass::text, a.attname::text FROM pg_depend d \
         JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
         JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid \
         WHERE d.refobjid = $1::oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'a'",
    )
    .bind(shape.oid)
    .fetch_all(&mut *tx)
    .await?;
    for (sequence, column) in owned {
        statements.push(format!(
            "ALTER SEQUENCE {sequence} OWNED BY {}.{}",
            shape.copy(),
            qi(&column)
        ));
    }
    if let Some((parent, _)) = &shape.partition {
        statements.push(format!(
            "ALTER TABLE {parent} DETACH PARTITION {}",
            shape.qualified()
        ));
    }
    // Free the original's index names, then give them to the copy.
    for index in indexes {
        statements.push(format!(
            "ALTER INDEX {}.{} RENAME TO {}",
            qi(&shape.schema),
            qi(&index.name),
            qi(&format!("udbo_{}", index.temp.trim_start_matches("udbc_")))
        ));
    }
    let retired = format!("{}_udb_retired", shape.table);
    statements.push(format!(
        "ALTER TABLE {} RENAME TO {}",
        shape.qualified(),
        qi(&retired)
    ));
    statements.push(format!(
        "ALTER TABLE {} RENAME TO {}",
        shape.copy(),
        qi(&shape.table)
    ));
    for index in indexes {
        match &index.constraint {
            Some((name, kind)) if kind == "p" || kind == "u" => statements.push(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} {} USING INDEX {}",
                shape.qualified(),
                qi(name),
                if kind == "p" { "PRIMARY KEY" } else { "UNIQUE" },
                qi(&index.temp)
            )),
            _ => statements.push(format!(
                "ALTER INDEX {}.{} RENAME TO {}",
                qi(&shape.schema),
                qi(&index.temp),
                qi(&index.name)
            )),
        }
    }
    if let Some((parent, bound)) = &shape.partition {
        // Under its original bound; the parent's indexes and constraints
        // adopt the copy's matching ones, its triggers are cloned onto it.
        statements.push(format!(
            "ALTER TABLE {parent} ATTACH PARTITION {} {bound}",
            shape.qualified()
        ));
    }
    statements.push(format!("DROP TABLE {}.{}", qi(&shape.schema), qi(&retired)));
    for sql in statements {
        sqlx::Executor::execute(&mut *tx, sqlx::raw_sql(&sql))
            .await
            .map_err(|err| SwapError::Failed(format!("{sql}: {err}")))?;
    }
    // Row-level security policies, recreated on the table under its name.
    for policy in policies {
        let sql = policy.create_on(&shape.qualified());
        sqlx::Executor::execute(&mut *tx, sqlx::raw_sql(&sql))
            .await
            .map_err(|err| SwapError::Failed(format!("{sql}: {err}")))?;
    }
    tx.commit().await?;
    Ok((original, started.elapsed()))
}

/// The original's row-level security policies, read on the swap's own
/// transaction before the original is renamed and dropped.
async fn table_policies(tx: &mut PgConnection, shape: &Shape) -> Result<Vec<PolicyDef>, String> {
    let rows: Vec<super::PolicyRow> = sqlx::query_as(
        "SELECT policyname::text, permissive, roles::text[], cmd, qual, with_check \
         FROM pg_policies WHERE schemaname = $1 AND tablename = $2",
    )
    .bind(&shape.schema)
    .bind(&shape.table)
    .fetch_all(&mut *tx)
    .await
    .map_err(|err| format!("read the policies: {err}"))?;
    Ok(rows.into_iter().map(super::policy_from_row).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn live_pool() -> PgPool {
        let dsn = std::env::var("UDB_LIVE_AUTH_PG_DSN")
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&dsn)
            .await
            .expect("connect live postgres")
    }

    async fn run(pool: &PgPool, sql: &str) {
        sqlx::Executor::execute(pool, sqlx::raw_sql(sql))
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }

    async fn names(pool: &PgPool, sql: &str, relation: &str) -> Vec<String> {
        let mut names: Vec<String> = sqlx::query_scalar(sql)
            .bind(relation)
            .fetch_all(pool)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
        names.sort();
        names
    }

    /// Against a real Postgres: a bloated table (90% of its rows deleted)
    /// written to throughout the compaction comes back far smaller with
    /// every row, including those written during the copy, and with its
    /// index and constraint names, grants, row-level security and policy,
    /// comment and owned sequence intact; a table with a trigger is refused.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_online_compaction -- --ignored --nocapture"]
    async fn live_postgres_online_compaction_keeps_rows_and_shape_while_written() {
        let pool = live_pool().await;
        let schema = format!("udb_compact_{}", uuid::Uuid::new_v4().simple());
        let relation = format!("{schema}.audit");
        run(&pool, &format!("CREATE SCHEMA {schema}")).await;
        run(
            &pool,
            &format!(
                "CREATE TABLE {relation} ( \
                     id UUID PRIMARY KEY DEFAULT gen_random_uuid(), \
                     seq BIGSERIAL CONSTRAINT audit_seq_key UNIQUE, \
                     body TEXT NOT NULL CHECK (body <> ''), \
                     payload JSONB NOT NULL DEFAULT '{{}}'::jsonb, \
                     body_len INT GENERATED ALWAYS AS (length(body)) STORED, \
                     occurred_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            ),
        )
        .await;
        for sql in [
            format!("CREATE INDEX audit_occurred_at_idx ON {relation} (occurred_at)"),
            format!("COMMENT ON TABLE {relation} IS 'the audit fixture'"),
            format!("GRANT SELECT ON {relation} TO PUBLIC"),
            format!("ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY"),
            format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"),
            format!(
                "CREATE POLICY audit_admin ON {relation} USING \
                 (current_setting('app.platform_admin', true) = 'true')"
            ),
            format!(
                "INSERT INTO {relation} (body, payload) \
                 SELECT repeat('x', 400), jsonb_build_object('n', g) FROM generate_series(1, 60000) g"
            ),
            format!("DELETE FROM {relation} WHERE seq % 10 <> 0"),
        ] {
            run(&pool, &sql).await;
        }
        let index_sql = "SELECT indexname::text FROM pg_indexes \
                         WHERE schemaname || '.' || tablename = $1";
        let constraint_sql =
            "SELECT conname::text FROM pg_constraint WHERE conrelid = $1::regclass";
        let indexes_before = names(&pool, index_sql, &relation).await;
        let constraints_before = names(&pool, constraint_sql, &relation).await;

        // A writer that keeps inserting while the compaction runs.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let pool = pool.clone();
            let relation = relation.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut written = 0i64;
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    let mut tx = pool.begin().await.expect("writer tx");
                    sqlx::query("SELECT set_config('app.platform_admin', 'true', true)")
                        .execute(&mut *tx)
                        .await
                        .expect("writer escape");
                    sqlx::query(&format!("INSERT INTO {relation} (body) VALUES ('during')"))
                        .execute(&mut *tx)
                        .await
                        .expect("a write during the compaction succeeds");
                    tx.commit().await.expect("writer commit");
                    written += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                written
            })
        };
        let done = compact_online(&pool, &relation, true)
            .await
            .expect("online compaction")
            .expect("the bloated table is compacted");
        stop.store(true, std::sync::atomic::Ordering::Release);
        let written = writer.await.expect("writer");
        assert!(written > 0, "rows were written during the compaction");
        assert!(
            done.bytes_after * 3 < done.bytes_before,
            "the table shrank: {done:?}"
        );

        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.platform_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let (rows, during): (i64, i64) = sqlx::query_as(&format!(
            "SELECT count(*), count(*) FILTER (WHERE body = 'during') FROM {relation}"
        ))
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            during, written,
            "every row written during the compaction is kept"
        );
        assert_eq!(rows, 6000 + written, "no row lost or duplicated");

        assert_eq!(names(&pool, index_sql, &relation).await, indexes_before);
        assert_eq!(
            names(&pool, constraint_sql, &relation).await,
            constraints_before
        );
        let (rls, forced, comment, public_select, policies, sequence): (
            bool,
            bool,
            Option<String>,
            bool,
            i64,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT c.relrowsecurity, c.relforcerowsecurity, obj_description(c.oid, 'pg_class'), \
                    has_table_privilege('public', c.oid, 'SELECT'), \
                    (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid), \
                    pg_get_serial_sequence($1, 'seq') \
             FROM pg_class c WHERE c.oid = $1::regclass",
        )
        .bind(&relation)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            rls && forced,
            "row-level security is still enabled and forced"
        );
        assert_eq!(comment.as_deref(), Some("the audit fixture"));
        assert!(public_select, "the grant carried over");
        assert_eq!(policies, 1, "the policy carried over");
        assert!(
            sequence.is_some(),
            "the serial sequence still belongs to the column"
        );
        let leftovers: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname <> 'audit' AND c.relkind = 'r'",
        )
        .bind(&schema)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(leftovers, 0, "no copy or retired table is left behind");

        // A table compaction cannot move faithfully is refused, unchanged
        // (bloated again first: a table under 8 MiB is never compacted).
        run(
            &pool,
            &format!(
                "CREATE FUNCTION {schema}.noop() RETURNS trigger LANGUAGE plpgsql \
                 AS 'BEGIN RETURN NEW; END'; \
                 CREATE TRIGGER audit_noop BEFORE INSERT ON {relation} \
                 FOR EACH ROW EXECUTE FUNCTION {schema}.noop(); \
                 INSERT INTO {relation} (body) \
                 SELECT repeat('z', 400) FROM generate_series(1, 30000)"
            ),
        )
        .await;
        let refused = compact_online(&pool, &relation, true).await;
        assert!(
            refused.as_ref().is_err_and(|why| why.contains("triggers")),
            "a table with a trigger is refused: {refused:?}"
        );
        run(&pool, &format!("DROP SCHEMA {schema} CASCADE")).await;
    }

    /// Against a real Postgres: compacting a partitioned table swaps its
    /// bloated partition for a compact copy attached under the same bound,
    /// with every row, the partition's primary key adopted by the parent's,
    /// and rows still routed to it afterwards.
    #[tokio::test]
    #[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_online_compaction -- --ignored --nocapture"]
    async fn live_postgres_online_compaction_swaps_a_bloated_partition() {
        let pool = live_pool().await;
        let schema = format!("udb_compact_{}", uuid::Uuid::new_v4().simple());
        let parent = format!("{schema}.decisions");
        let leaf = format!("{schema}.decisions_p_now");
        for sql in [
            format!("CREATE SCHEMA {schema}"),
            format!(
                "CREATE TABLE {parent} (id BIGINT NOT NULL, at TIMESTAMPTZ NOT NULL, body TEXT, \
                 PRIMARY KEY (id, at)) PARTITION BY RANGE (at)"
            ),
            format!(
                "CREATE TABLE {leaf} PARTITION OF {parent} \
                 FOR VALUES FROM ('2000-01-01') TO ('3000-01-01')"
            ),
            format!("CREATE INDEX decisions_at_idx ON {parent} (at)"),
            format!(
                "INSERT INTO {parent} SELECT g, now(), repeat('y', 500) \
                 FROM generate_series(1, 40000) g"
            ),
            format!("DELETE FROM {parent} WHERE id % 20 <> 0"),
        ] {
            run(&pool, &sql).await;
        }
        let bound_sql =
            "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = $1::regclass";
        let bound_before: String = sqlx::query_scalar(bound_sql)
            .bind(&leaf)
            .fetch_one(&pool)
            .await
            .unwrap();
        let leaf_indexes_before = names(
            &pool,
            "SELECT indexname::text FROM pg_indexes WHERE schemaname || '.' || tablename = $1",
            &leaf,
        )
        .await;

        let done = compact_online(&pool, &parent, true)
            .await
            .expect("online compaction of the partitioned table")
            .expect("the bloated partition is compacted");
        assert!(
            done.bytes_after * 3 < done.bytes_before,
            "the partition shrank: {done:?}"
        );

        let bound_after: String = sqlx::query_scalar(bound_sql)
            .bind(&leaf)
            .fetch_one(&pool)
            .await
            .expect("the partition is attached under its name");
        assert_eq!(bound_after, bound_before, "the same bound");
        assert_eq!(
            names(
                &pool,
                "SELECT indexname::text FROM pg_indexes WHERE schemaname || '.' || tablename = $1",
                &leaf,
            )
            .await,
            leaf_indexes_before,
            "the partition's index names are kept"
        );
        let attached_indexes: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE c.relkind = 'i' AND c.relnamespace = $1::regnamespace",
        )
        .bind(&schema)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            attached_indexes, 2,
            "both partition indexes belong to the parent's again"
        );
        run(
            &pool,
            &format!("INSERT INTO {parent} VALUES (999999, now(), 'after')"),
        )
        .await;
        let (rows, routed): (i64, i64) = sqlx::query_as(&format!(
            "SELECT (SELECT count(*) FROM {parent}), (SELECT count(*) FROM {leaf} WHERE id = 999999)"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows, 2001,
            "every row kept, and a new one written through the parent"
        );
        assert_eq!(routed, 1, "a new row is routed to the compacted partition");
        run(&pool, &format!("DROP SCHEMA {schema} CASCADE")).await;
    }
}
