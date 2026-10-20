//! Durable CDC publication order. PostgreSQL owns allocation: the insert trigger
//! locks one persistent head row until commit, so every visible higher position
//! has a committed prefix. Kafka and application work occur before this lock.
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::{CdcEnvelope, fnv1a_64};
use crate::generation::sql::ql;
use crate::runtime::executor_utils::qi_runtime as qi;
use crate::runtime::system::SystemCatalogConfig;

pub(crate) type JournalPosition = i64;
/// The journal column the insert trigger assigns. It is NOT NULL with no
/// default, so shape checks must count it as written by UDB.
pub(crate) const POSITION_COLUMN: &str = "journal_position";
const HEAD_TABLE: &str = "udb_cdc_journal_heads";
const ALLOCATOR: &str = "udb_cdc_assign_journal_position";
const IMMUTABLE: &str = "udb_cdc_keep_journal_position";
const READER_TABLE: &str = "udb_journal_reader_cursors";
/// Reader row recording that never-acknowledged named cursors were moved to
/// the head once, at the upgrade that made new consumers start there.
const UNACKED_ORIGIN_MIGRATION: &str = "cdc.unacked_consumer_origin.v1";

#[derive(Debug, Clone)]
pub(crate) struct JournalEntry {
    pub(crate) position: JournalPosition,
    pub(crate) envelope: CdcEnvelope,
}

impl JournalEntry {
    pub(crate) fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let position: i64 = row.try_get("journal_position")?;
        if position <= 0 {
            return Err(sqlx::Error::Decode(Box::new(std::io::Error::other(
                "CDC journal position must be positive",
            ))));
        }
        let id: Uuid = row.try_get("event_id")?;
        let payload: serde_json::Value = row.try_get("payload")?;
        Ok(Self {
            position,
            envelope: CdcEnvelope {
                event_id: id.to_string(),
                topic: row.try_get("topic")?,
                partition_key: row.try_get("partition_key")?,
                payload_json: payload.to_string(),
                published_at: row.try_get("published_at")?,
                journal_position: position,
                progress_only: false,
            },
        })
    }
}

pub(crate) fn head_relation(config: &SystemCatalogConfig) -> String {
    format!("{}.{}", qi(&config.cdc.system_schema), qi(HEAD_TABLE))
}

/// Durable positions of the broker's own journal readers (the webhook
/// dispatcher, the cache invalidator). Each scans forward from its position
/// instead of re-reading the whole retained journal on every tick.
pub(crate) fn reader_cursors_relation(config: &SystemCatalogConfig) -> String {
    format!("{}.{}", qi(&config.cdc.system_schema), qi(READER_TABLE))
}

/// The topic of every progress marker. Reserved: no producer may publish it.
pub(crate) const PROGRESS_TOPIC: &str = "udb.cdc.progress.v1";

/// Whether `topic` is reserved for the broker's own stream markers.
pub(crate) fn is_reserved_topic(topic: &str) -> bool {
    topic.trim() == PROGRESS_TOPIC
}

/// A progress token is an RFC 9562 version-8 UUID: bytes 0..6 are `udbpro`,
/// byte 6 carries the version, byte 7 is zero, and bytes 8..16 hold the
/// journal position big-endian under the `10` variant bits. Positions below
/// 2^62 fit. It never names a journal row, so it stays valid after retention.
const PROGRESS_TOKEN_PREFIX: [u8; 8] = [0x75, 0x64, 0x62, 0x70, 0x72, 0x6f, 0x80, 0x00];
const PROGRESS_VARIANT_BITS: u64 = 0x8000_0000_0000_0000;
const PROGRESS_VARIANT_MASK: u64 = 0xC000_0000_0000_0000;

pub(crate) fn progress_token(position: JournalPosition) -> Uuid {
    let position = u64::try_from(position).unwrap_or(0) & !PROGRESS_VARIANT_MASK;
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&PROGRESS_TOKEN_PREFIX);
    bytes[8..].copy_from_slice(&(position | PROGRESS_VARIANT_BITS).to_be_bytes());
    Uuid::from_bytes(bytes)
}

/// The position a progress token carries, or `None` for any other UUID.
pub(crate) fn decode_progress_token(id: &Uuid) -> Option<JournalPosition> {
    let bytes = id.as_bytes();
    if bytes[..8] != PROGRESS_TOKEN_PREFIX {
        return None;
    }
    let raw = u64::from_be_bytes(bytes[8..].try_into().ok()?);
    if raw & PROGRESS_VARIANT_MASK != PROGRESS_VARIANT_BITS {
        return None;
    }
    i64::try_from(raw & !PROGRESS_VARIANT_MASK).ok()
}

/// A stream marker saying the subscriber has read every committed row up to
/// `position`. Its id is a progress token, never a journal row's id, so an
/// acknowledgment of it moves a named cursor without naming a foreign event.
pub(crate) fn progress_envelope(position: JournalPosition, caught_up: bool) -> CdcEnvelope {
    CdcEnvelope {
        event_id: progress_token(position).to_string(),
        topic: PROGRESS_TOPIC.to_string(),
        partition_key: String::new(),
        payload_json: serde_json::json!({
            "kind": "progress",
            "journal_position": position.to_string(),
            "caught_up": caught_up,
        })
        .to_string(),
        published_at: chrono::Utc::now(),
        journal_position: position,
        progress_only: true,
    }
}

/// Called within the system catalog's existing single bootstrap transaction.
/// The journal lock excludes old writers throughout backfill and trigger setup.
/// Existing positions and the persistent head are never renumbered or reset.
pub(crate) fn schema_statements(config: &SystemCatalogConfig) -> Vec<String> {
    let journal = config.cdc_journal_relation();
    let cursors = config.cdc_consumer_cursors_relation();
    let heads = head_relation(config);
    let schema = qi(&config.cdc.system_schema);
    let schema_value = ql(&config.cdc.system_schema);
    let table_value = ql(&config.cdc_journal_table);
    let cursors_table_value = ql(crate::runtime::system::CDC_CONSUMER_CURSORS_TABLE);
    let heads_table_value = ql(HEAD_TABLE);
    let allocator = format!("{schema}.{}", qi(ALLOCATOR));
    let immutable = format!("{schema}.{}", qi(IMMUTABLE));
    let journal_literal = ql(&journal);
    let heads_literal = ql(&heads);
    let cursors_literal = ql(&cursors);
    let allocate_literal = ql(ALLOCATOR);
    let immutable_literal = ql(IMMUTABLE);
    let suffix = format!("{:016x}", fnv1a_64(journal.as_bytes()));
    let readers = reader_cursors_relation(config);
    let origin_marker = ql(UNACKED_ORIGIN_MIGRATION);
    let move_unacked = if config.cdc.keep_unacked_consumer_origin {
        String::new()
    } else {
        format!(
            "UPDATE {cursors} c SET last_journal_position = h.last_position,
              last_published_at = COALESCE((SELECT j.published_at FROM {journal} j WHERE j.journal_position <= h.last_position ORDER BY j.journal_position DESC LIMIT 1), c.last_published_at)
              FROM {heads} h
              WHERE h.journal_schema = {schema_value} AND h.journal_table = {table_value}
                AND c.last_journal_position = 0
                AND c.last_event_id = '00000000-0000-0000-0000-000000000000'::UUID;"
        )
    };
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {heads} (journal_schema TEXT NOT NULL, journal_table TEXT NOT NULL, last_position BIGINT NOT NULL CHECK (last_position >= 0), PRIMARY KEY (journal_schema, journal_table))"
        ),
        format!("ALTER TABLE {journal} ADD COLUMN IF NOT EXISTS journal_position BIGINT"),
        format!("ALTER TABLE {cursors} ADD COLUMN IF NOT EXISTS last_journal_position BIGINT"),
        format!(
            r#"DO {}"#,
            ql(&format!(
                r#"
        DECLARE floor_position BIGINT;
        BEGIN
          LOCK TABLE {journal} IN ACCESS EXCLUSIVE MODE;
          LOCK TABLE {cursors} IN SHARE ROW EXCLUSIVE MODE;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid={journal_literal}::regclass AND n.nspname::TEXT={schema_value} AND c.relname::TEXT={table_value}) THEN
            RAISE EXCEPTION 'CDC journal configured identifier exceeds PostgreSQL name authority';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid={cursors_literal}::regclass AND n.nspname::TEXT={schema_value} AND c.relname::TEXT={cursors_table_value}) OR
             NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid={heads_literal}::regclass AND n.nspname::TEXT={schema_value} AND c.relname::TEXT={heads_table_value}) THEN
            RAISE EXCEPTION 'CDC durable cursor/head configured identifier exceeds PostgreSQL name authority';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid={journal_literal}::regclass AND attname='journal_position' AND atttypid='pg_catalog.int8'::regtype AND NOT attisdropped) OR
             NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid={cursors_literal}::regclass AND attname='last_journal_position' AND atttypid='pg_catalog.int8'::regtype AND NOT attisdropped) OR
             (SELECT COUNT(*) FROM pg_catalog.pg_attribute WHERE attrelid={heads_literal}::regclass AND NOT attisdropped AND attnotnull AND
               ((attname IN ('journal_schema','journal_table') AND atttypid='pg_catalog.text'::regtype) OR (attname='last_position' AND atttypid='pg_catalog.int8'::regtype))) <> 3 OR
             NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attname='journal_schema'
               JOIN pg_catalog.pg_attribute b ON b.attrelid=i.indrelid AND b.attname='journal_table'
               WHERE i.indrelid={heads_literal}::regclass AND i.indisprimary AND i.indisvalid AND i.indisready AND i.indnkeyatts=2 AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indkey[0]=a.attnum AND i.indkey[1]=b.attnum) THEN
            RAISE EXCEPTION 'CDC journal durable position/head storage shape is invalid';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM {heads} WHERE journal_schema={schema_value} AND journal_table={table_value}) AND
             (EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid={journal_literal}::regclass AND attname='journal_position' AND attnotnull AND NOT attisdropped) OR
              EXISTS (SELECT 1 FROM {journal} WHERE journal_position IS NOT NULL) OR
              EXISTS (SELECT 1 FROM {cursors} WHERE last_journal_position > 0)) THEN
            RAISE EXCEPTION 'CDC journal durable head is missing; refusing to reset publication order';
          END IF;
          IF EXISTS (SELECT 1 FROM {heads} WHERE journal_schema={schema_value} AND journal_table={table_value}
              AND (last_position < 0 OR last_position < COALESCE((SELECT MAX(journal_position) FROM {journal}), 0))) THEN
            RAISE EXCEPTION 'CDC journal durable head is invalid or behind retained publication order';
          END IF;
          INSERT INTO {heads} (journal_schema, journal_table, last_position)
            VALUES ({schema_value}, {table_value}, 0) ON CONFLICT DO NOTHING;
          SELECT GREATEST(h.last_position, COALESCE((SELECT MAX(journal_position) FROM {journal}), 0))
            INTO floor_position FROM {heads} h
            WHERE h.journal_schema = {schema_value} AND h.journal_table = {table_value} FOR UPDATE;
          WITH unpositioned AS (
            SELECT event_id, floor_position + ROW_NUMBER() OVER (ORDER BY published_at, event_id) AS position
            FROM {journal} WHERE journal_position IS NULL
          ) UPDATE {journal} j SET journal_position = u.position FROM unpositioned u WHERE j.event_id = u.event_id;
          UPDATE {heads} SET last_position = GREATEST(floor_position, COALESCE((SELECT MAX(journal_position) FROM {journal}), 0))
            WHERE journal_schema = {schema_value} AND journal_table = {table_value};
          UPDATE {cursors} c SET last_journal_position = COALESCE(
            (SELECT journal_position FROM {journal} WHERE event_id = c.last_event_id),
            (SELECT MAX(journal_position) FROM {journal} WHERE (published_at, event_id) <= (c.last_published_at, c.last_event_id)), 0)
            WHERE c.last_journal_position IS NULL;
          IF EXISTS (SELECT 1 FROM {journal} WHERE journal_position <= 0) OR
             EXISTS (SELECT 1 FROM {journal} GROUP BY journal_position HAVING COUNT(*) > 1) OR
             EXISTS (SELECT 1 FROM {cursors} WHERE last_journal_position < 0 OR last_journal_position > (SELECT last_position FROM {heads} WHERE journal_schema={schema_value} AND journal_table={table_value})) THEN
            RAISE EXCEPTION 'CDC journal/cursor has invalid durable positions';
          END IF;
        END;"#
            ))
        ),
        format!(
            "ALTER TABLE {journal} ALTER COLUMN journal_position SET NOT NULL, ALTER COLUMN journal_position DROP DEFAULT"
        ),
        format!(
            "ALTER TABLE {cursors} ALTER COLUMN last_journal_position SET DEFAULT 0, ALTER COLUMN last_journal_position SET NOT NULL"
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {journal} (journal_position)",
            qi(&format!("udb_cdc_position_{suffix}"))
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {journal} (topic, journal_position)",
            qi(&format!("udb_cdc_topic_position_{suffix}"))
        ),
        // Wildcard topic patterns read a byte-order range of topics (see
        // `topic_filter`); a database collation other than C cannot serve it.
        format!(
            r#"CREATE INDEX IF NOT EXISTS {} ON {journal} ((topic COLLATE "C"), journal_position)"#,
            qi(&format!("udb_cdc_topic_c_position_{suffix}"))
        ),
        // Retention deletes in published_at order across every topic.
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {journal} (published_at)",
            qi(&format!("udb_cdc_published_{suffix}"))
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {readers} (reader TEXT PRIMARY KEY, last_position BIGINT NOT NULL CHECK (last_position >= 0), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        ),
        // New consumer names now start at the head. A name claimed before that
        // and never acknowledged sits at the origin and would replay the whole
        // retained journal on its first subscription; move those to the head
        // once, recorded so a later EARLIEST claim is never moved.
        format!(
            "DO {}",
            ql(&format!(
                r#"
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM {readers} WHERE reader = {origin_marker}) THEN
            {move_unacked}
            INSERT INTO {readers} (reader, last_position)
              SELECT {origin_marker}, last_position FROM {heads}
              WHERE journal_schema = {schema_value} AND journal_table = {table_value}
              ON CONFLICT (reader) DO NOTHING;
          END IF;
        END;"#
            ))
        ),
        format!(
            r#"CREATE OR REPLACE FUNCTION {allocator}() RETURNS TRIGGER LANGUAGE plpgsql SET search_path = pg_catalog AS {}"#,
            ql(&format!(
                r#"
        BEGIN
          UPDATE {heads} SET last_position = last_position + 1
            WHERE journal_schema = TG_TABLE_SCHEMA AND journal_table = TG_TABLE_NAME
            RETURNING last_position INTO NEW.journal_position;
          IF NOT FOUND THEN RAISE EXCEPTION 'CDC journal durable head is missing'; END IF;
          RETURN NEW;
        END;"#
            ))
        ),
        format!(
            r#"CREATE OR REPLACE FUNCTION {immutable}() RETURNS TRIGGER LANGUAGE plpgsql SET search_path = pg_catalog AS {}"#,
            ql(&format!(
                r#"
        BEGIN
          IF NEW.journal_position IS DISTINCT FROM OLD.journal_position OR NEW.event_id IS DISTINCT FROM OLD.event_id THEN
            RAISE EXCEPTION 'CDC journal event identity and position are immutable';
          END IF;
          NEW.published_at := OLD.published_at;
          RETURN NEW;
        END;"#
            ))
        ),
        format!("DROP TRIGGER IF EXISTS {} ON {journal}", qi(ALLOCATOR)),
        format!(
            "CREATE TRIGGER {} BEFORE INSERT ON {journal} FOR EACH ROW EXECUTE FUNCTION {allocator}()",
            qi(ALLOCATOR)
        ),
        format!(
            "ALTER TABLE {journal} ENABLE ALWAYS TRIGGER {}",
            qi(ALLOCATOR)
        ),
        format!("DROP TRIGGER IF EXISTS {} ON {journal}", qi(IMMUTABLE)),
        format!(
            "CREATE TRIGGER {} BEFORE UPDATE ON {journal} FOR EACH ROW EXECUTE FUNCTION {immutable}()",
            qi(IMMUTABLE)
        ),
        format!(
            "ALTER TABLE {journal} ENABLE ALWAYS TRIGGER {}",
            qi(IMMUTABLE)
        ),
        format!(
            "DO {}",
            ql(&format!(
                r#"
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attname='journal_position'
              WHERE i.indrelid={journal_literal}::regclass AND i.indisunique AND i.indisvalid AND i.indisready AND i.indnkeyatts=1 AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indkey[0]=a.attnum) THEN
            RAISE EXCEPTION 'CDC journal immutable position uniqueness is unavailable';
          END IF;
          IF (SELECT COUNT(*) FROM pg_catalog.pg_trigger WHERE tgrelid={journal_literal}::regclass
              AND NOT tgisinternal AND tgenabled='A' AND tgname IN ({allocate_literal},{immutable_literal})) <> 2 THEN
            RAISE EXCEPTION 'CDC journal commit-prefix trigger authority is unavailable';
          END IF;
        END;"#
            ))
        ),
    ]
}

/// The durable head persists even when retention has removed every journal row.
pub(crate) async fn head(pool: &PgPool, config: &SystemCatalogConfig) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT last_position FROM {} WHERE journal_schema=$1 AND journal_table=$2",
        head_relation(config)
    ))
    .bind(&config.cdc.system_schema)
    .bind(&config.cdc_journal_table)
    .fetch_one(pool)
    .await
}

/// One canonical journal write, shared by outbox publication and external
/// sources. Caller commits before broadcasting; optional source offset shares
/// this SAME PostgreSQL transaction, never a second pool or remote authority.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert(
    conn: &mut PgConnection,
    config: &SystemCatalogConfig,
    event_id: Uuid,
    topic: &str,
    partition_key: &str,
    payload: &str,
    partition: Option<i32>,
    offset: Option<i64>,
    producer_epoch: i64,
    transactional_id: &str,
) -> Result<JournalEntry, sqlx::Error> {
    if is_reserved_topic(topic) {
        return Err(sqlx::Error::Protocol(format!(
            "CDC topic {PROGRESS_TOPIC} is reserved for stream progress markers"
        )));
    }
    let row = sqlx::query(&format!(
        "INSERT INTO {} (event_id, topic, partition_key, payload, published_at, kafka_partition, kafka_offset, delivery_state, producer_epoch, transactional_id) \
         VALUES ($1,$2,$3,$4::JSONB,NOW(),$5,$6,'published',$7,$8) \
         ON CONFLICT (event_id) DO UPDATE SET delivery_state='published', \
         kafka_partition=EXCLUDED.kafka_partition,kafka_offset=EXCLUDED.kafka_offset, \
         producer_epoch=EXCLUDED.producer_epoch,transactional_id=EXCLUDED.transactional_id \
         RETURNING event_id,topic,partition_key,payload,published_at,journal_position", config.cdc_journal_relation()))
        .bind(event_id).bind(topic).bind(partition_key).bind(payload).bind(partition).bind(offset)
        .bind(producer_epoch).bind(transactional_id).fetch_one(conn).await?;
    JournalEntry::from_row(&row)
}

pub(crate) async fn resolve_event_position(
    pool: &PgPool,
    event_id: &str,
) -> Result<i64, tonic::Status> {
    let id = Uuid::parse_str(event_id.trim()).map_err(|_| {
        crate::runtime::executor_utils::invalid_argument_fields(
            "since_event_id must be a valid UUID",
            [("since_event_id", "must be a valid UUID")],
        )
    })?;
    let config = SystemCatalogConfig::current();
    let not_found = || {
        crate::runtime::executor_utils::schema_status(
            tonic::Code::NotFound,
            "cdc",
            "resolve_resume_cursor",
            "cdc_resume_cursor_not_found",
            "CDC resume cursor is unknown or no longer retained",
        )
    };
    let read_failed = |err: sqlx::Error| {
        crate::runtime::executor_utils::sqlx_error_to_status("CDC resume cursor read failed", &err)
    };
    // A progress token resumes at its position without a row lookup, so it
    // remains valid after retention removed the rows around it.
    if let Some(position) = decode_progress_token(&id) {
        let committed = head(pool, &config).await.map_err(read_failed)?;
        return if position <= committed {
            Ok(position)
        } else {
            Err(not_found())
        };
    }
    let journal = config.cdc_journal_relation();
    let position: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT journal_position FROM {journal} WHERE event_id=$1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(read_failed)?;
    position.ok_or_else(not_found)
}

/// The lowest retained position, or `None` when retention emptied the journal.
pub(crate) async fn min_position(
    pool: &PgPool,
    config: &SystemCatalogConfig,
) -> Result<Option<JournalPosition>, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT MIN(journal_position) FROM {}",
        config.cdc_journal_relation()
    ))
    .fetch_one(pool)
    .await
}

/// A broker journal reader's stored position, `None` before its first tick.
pub(crate) async fn reader_position(
    pool: &PgPool,
    config: &SystemCatalogConfig,
    reader: &str,
) -> Result<Option<JournalPosition>, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT last_position FROM {} WHERE reader = $1",
        reader_cursors_relation(config)
    ))
    .bind(reader)
    .fetch_optional(pool)
    .await
}

/// Move a broker journal reader forward. Never moves it backward.
pub(crate) async fn advance_reader(
    pool: &PgPool,
    config: &SystemCatalogConfig,
    reader: &str,
    position: JournalPosition,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "INSERT INTO {readers} AS current_reader (reader, last_position, updated_at) VALUES ($1, $2, NOW()) \
         ON CONFLICT (reader) DO UPDATE SET \
           last_position = GREATEST(current_reader.last_position, EXCLUDED.last_position), updated_at = NOW()",
        readers = reader_cursors_relation(config)
    ))
    .bind(reader)
    .bind(position.max(0))
    .execute(pool)
    .await
    .map(|_| ())
}

/// The window `(from, to]` a broker journal reader scans next, at most `span`
/// positions wide. A reader with no stored position, or one behind retention,
/// starts just before the lowest retained row.
pub(crate) async fn reader_window(
    pool: &PgPool,
    config: &SystemCatalogConfig,
    reader: &str,
    head: JournalPosition,
    span: i64,
) -> Result<(JournalPosition, JournalPosition), sqlx::Error> {
    let stored = reader_position(pool, config, reader).await?.unwrap_or(0);
    let retained_floor = min_position(pool, config)
        .await?
        .map_or(head, |lowest| lowest - 1);
    let from = stored.max(retained_floor).min(head);
    Ok((from, head.min(from.saturating_add(span.max(1)))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_token_round_trips_positions() {
        for position in [0, 1, 42, 1 << 40, (1i64 << 62) - 1] {
            let token = progress_token(position);
            assert_eq!(token.get_version_num(), 8, "{token}");
            assert_eq!(token.get_variant(), uuid::Variant::RFC4122, "{token}");
            assert_eq!(&token.as_bytes()[..6], b"udbpro");
            assert_eq!(decode_progress_token(&token), Some(position), "{token}");
        }
    }

    #[test]
    fn ordinary_ids_are_not_progress_tokens() {
        for _ in 0..256 {
            assert_eq!(decode_progress_token(&Uuid::new_v4()), None);
        }
        assert_eq!(decode_progress_token(&Uuid::nil()), None);
        assert_eq!(decode_progress_token(&Uuid::from_bytes([0xff; 16])), None);
        // The prefix alone is not enough: the variant bits must be `10`.
        let mut bytes = *progress_token(7).as_bytes();
        bytes[8] = 0x40;
        assert_eq!(decode_progress_token(&Uuid::from_bytes(bytes)), None);
    }

    #[test]
    fn progress_envelope_carries_position_and_reserved_topic() {
        let marker = progress_envelope(1234, true);
        assert!(marker.progress_only);
        assert_eq!(marker.journal_position, 1234);
        assert_eq!(marker.topic, PROGRESS_TOPIC);
        assert!(marker.partition_key.is_empty());
        let payload: serde_json::Value = serde_json::from_str(&marker.payload_json).unwrap();
        assert_eq!(
            payload,
            serde_json::json!({"kind": "progress", "journal_position": "1234", "caught_up": true})
        );
        let id = Uuid::parse_str(&marker.event_id).unwrap();
        assert_eq!(decode_progress_token(&id), Some(1234));
        assert!(is_reserved_topic(PROGRESS_TOPIC));
        assert!(is_reserved_topic(" udb.cdc.progress.v1 "));
        assert!(!is_reserved_topic("udb.cdc.progress.v2"));
    }
}
