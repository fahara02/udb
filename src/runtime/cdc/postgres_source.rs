//! Transactional PostgreSQL INSERT capture and per-consumer receipts. A capture
//! sequence orders finite scan cycles; only receipts establish completion. The
//! source pool never participates in the destination journal transaction.
use std::{
    pin::Pin,
    str::FromStr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use futures::Stream;
use sha2::{Digest, Sha256};
use sqlx::{
    PgConnection, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::source::{CdcEvent, CdcSource, PostgresCdcSource};
use crate::generation::sql::ql;
use crate::runtime::{executor_utils::qi_runtime as qi, system::SystemCatalogConfig};

pub(crate) const REGISTRY: &str = "udb_cdc_source_registry";
pub(crate) const CONSUMERS: &str = "udb_cdc_source_consumers";
pub(crate) const CAPTURE: &str = "udb_cdc_source_capture";
pub(crate) const RECEIPTS: &str = "udb_cdc_source_receipts";
pub(crate) const DESTINATIONS: &str = "udb_cdc_source_destinations";
const SEQUENCE: &str = "udb_cdc_source_capture_order";
const IMMUTABLE: &str = "udb_cdc_source_keep_authority";
const MARKER: &str = "udb_cdc_source_generation";
const TRUNCATE_GUARD: &str = "udb_cdc_source_refuse_truncate";
const POSITIVE_ORDER: &str = "udb_cdc_source_capture_positive";
const POOL_CAPACITY: u32 = 2;
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const PREPARE_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_DELAY: Duration = Duration::from_millis(500);
const PAGE_SIZE: i64 = 100;
const APPLICATION_NAME: &str = "udb-cdc-source-capture";

pub(crate) fn identity_digest(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

#[derive(Clone)]
struct Relations {
    schema: String,
    registry: String,
    consumers: String,
    capture: String,
    receipts: String,
    destinations: String,
    sequence: String,
    immutable: String,
}

impl Relations {
    fn new(schema: &str) -> Self {
        let relation = |name: &str| format!("{}.{}", qi(schema), qi(name));
        Self {
            schema: schema.into(),
            registry: relation(REGISTRY),
            consumers: relation(CONSUMERS),
            capture: relation(CAPTURE),
            receipts: relation(RECEIPTS),
            destinations: relation(DESTINATIONS),
            sequence: relation(SEQUENCE),
            immutable: relation(IMMUTABLE),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PreparedPostgresSource {
    pool: PgPool,
    relations: Relations,
    generation: Uuid,
    slot: String,
    slot_hash: String,
    namespace: String,
    binding: String,
    generation_text: String,
    source_relation: String,
    source_oid: i64,
    function: String,
    function_body: String,
    trigger: String,
    destination: Arc<OnceLock<String>>,
}

async fn execute(conn: &mut PgConnection, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::query(sql).execute(conn).await.map(|_| ())
}

fn immutable_body() -> &'static str {
    "BEGIN RAISE EXCEPTION 'CDC source durable authority is immutable'; END;"
}

fn capture_body(r: &Relations, generation: Uuid, source_oid: i64) -> String {
    format!(
        "BEGIN IF TG_OP <> 'INSERT' OR TG_WHEN <> 'AFTER' OR TG_LEVEL <> 'ROW' OR TG_RELID::BIGINT <> {source_oid} OR NOT EXISTS (SELECT 1 FROM {} WHERE generation={}::UUID AND relation_oid={source_oid}::OID) THEN RAISE EXCEPTION 'CDC source capture generation or relation mismatch'; END IF; INSERT INTO {} (generation,capture_id,capture_order,source_event_seq,topic,payload,created_at) VALUES ({}::UUID,pg_catalog.gen_random_uuid(),pg_catalog.nextval({}::regclass),NEW.event_seq,NEW.topic,NEW.payload,NEW.created_at); RETURN NEW; END;",
        r.registry,
        ql(&generation.to_string()),
        r.capture,
        ql(&generation.to_string()),
        ql(&r.sequence)
    )
}

/// Resolve factory configuration once, enroll under a source write lock, and own
/// one bounded pool for polling, health and receipts. Neither DSN nor credentials
/// enter the durable namespace or logs.
pub(crate) async fn prepare(factory: &PostgresCdcSource) -> Result<Arc<dyn CdcSource>, String> {
    tokio::time::timeout(PREPARE_TIMEOUT, prepare_inner(factory))
        .await
        .map_err(|_| "postgres source prepare deadline exceeded".to_string())?
        .map(|source| Arc::new(source) as Arc<dyn CdcSource>)
}

async fn prepare_inner(factory: &PostgresCdcSource) -> Result<PreparedPostgresSource, String> {
    let slot = factory.slot.trim();
    if slot.is_empty() {
        return Err("postgres source requires a nonempty durable consumer slot".into());
    }
    let options = PgConnectOptions::from_str(&factory.dsn)
        .map_err(|_| "postgres source connection configuration is invalid".to_string())?;
    let options = if options.get_application_name().is_none() {
        options.application_name(APPLICATION_NAME)
    } else {
        options
    };
    let endpoint = format!(
        "{}:{}:{}",
        options.get_host(),
        options.get_port(),
        options
            .get_socket()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    );
    let pool = PgPoolOptions::new()
        .max_connections(POOL_CAPACITY)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .after_connect(|conn, _| {
            Box::pin(async move {
                // Shared SQL-literal rendering assumes standard string syntax;
                // own that setting before using quoted identifiers in bodies.
                sqlx::query("SET standard_conforming_strings=on")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET statement_timeout='10s'")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET lock_timeout='5s'").execute(conn).await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|_| "postgres source connection failed".to_string())?;
    let config = SystemCatalogConfig::current();
    let configured = if factory.publication.trim().is_empty() {
        std::env::var("UDB_CDC_POSTGRES_SOURCE_TABLE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| config.cdc.outbox_relation())
    } else {
        factory.publication.trim().to_string()
    };
    let r = Relations::new(&config.cdc.system_schema);
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("postgres source enrollment: {e}"))?;
    // Serialize only enrollment/catalog work. Business capture has no head lock.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(super::fnv1a_64(r.registry.as_bytes()) as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("postgres source enrollment lock: {e}"))?;
    let row = sqlx::query("SELECT c.oid::BIGINT AS oid,n.nspname::TEXT AS schema,c.relname::TEXT AS name,c.relkind::TEXT AS kind,c.relpersistence::TEXT AS persistence,current_database()::TEXT AS database FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=pg_catalog.to_regclass($1)")
        .bind(&configured).fetch_optional(&mut *tx).await
        .map_err(|e|format!("postgres source relation resolution: {e}"))?
        .ok_or_else(||"postgres source relation is missing".to_string())?;
    let schema: String = row.try_get("schema").map_err(|e| e.to_string())?;
    let name: String = row.try_get("name").map_err(|e| e.to_string())?;
    let oid: i64 = row.try_get("oid").map_err(|e| e.to_string())?;
    let kind: String = row.try_get("kind").map_err(|e| e.to_string())?;
    let database: String = row.try_get("database").map_err(|e| e.to_string())?;
    let persistence: String = row.try_get("persistence").map_err(|e| e.to_string())?;
    if kind != "r" || persistence != "p" {
        return Err("postgres source capture requires a permanent ordinary table".into());
    }
    let relation = format!("{}.{}", qi(&schema), qi(&name));
    let enrolled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=$1::BIGINT::OID AND conname=$2)")
        .bind(oid).bind(MARKER).fetch_one(&mut *tx).await.map_err(|e|e.to_string())?;
    // First enrollment excludes original INSERT/DELETE until trigger+backfill
    // commit. Existing authority needs only a DDL lock, so reconnect cannot
    // stall behind an intentionally held normal producer transaction.
    let lock_mode = if enrolled {
        "ACCESS SHARE"
    } else {
        "SHARE ROW EXCLUSIVE"
    };
    execute(
        &mut tx,
        &format!("LOCK TABLE {relation} IN {lock_mode} MODE"),
    )
    .await
    .map_err(|e| format!("postgres source enrollment table lock: {e}"))?;
    let resolved: i64 = sqlx::query_scalar("SELECT pg_catalog.to_regclass($1)::OID::BIGINT")
        .bind(&configured)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    if resolved != oid {
        return Err("postgres source relation changed during enrollment".into());
    }
    let source_columns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_catalog.pg_attribute WHERE attrelid=$1::BIGINT::OID AND NOT attisdropped AND attnotnull AND ((attname='event_seq' AND atttypid='pg_catalog.int8'::regtype) OR (attname='topic' AND atttypid='pg_catalog.text'::regtype) OR (attname='payload' AND atttypid='pg_catalog.jsonb'::regtype) OR (attname='created_at' AND atttypid='pg_catalog.timestamptz'::regtype))")
        .bind(oid).fetch_one(&mut *tx).await.map_err(|e|e.to_string())?;
    if source_columns != 4 {
        return Err("postgres source event columns require exact non-null BIGINT/TEXT/JSONB/TIMESTAMPTZ authority".into());
    }
    let marker: Option<String> = sqlx::query_scalar("SELECT pg_catalog.obj_description(oid,'pg_constraint') FROM pg_catalog.pg_constraint WHERE conrelid=$1::BIGINT::OID AND conname=$2")
        .bind(oid).bind(MARKER).fetch_optional(&mut *tx).await.map_err(|e|e.to_string())?.flatten();
    let names = [
        &r.registry,
        &r.consumers,
        &r.capture,
        &r.receipts,
        &r.destinations,
        &r.sequence,
    ];
    let mut present = 0;
    for target in names {
        let exists: bool = sqlx::query_scalar("SELECT pg_catalog.to_regclass($1) IS NOT NULL")
            .bind(target)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        present += usize::from(exists);
    }
    if present != 0 && present != names.len() {
        return Err("postgres source capture catalog is incomplete".into());
    }
    if marker.is_some() && present == 0 {
        return Err("postgres source enrolled capture catalog was removed".into());
    }
    if present == 0 {
        create_catalog(&mut tx, &r)
            .await
            .map_err(|e| format!("postgres source catalog creation: {e}"))?;
    }
    verify_catalog(&mut tx, &r)
        .await
        .map_err(|e| format!("postgres source catalog authority: {e}"))?;
    let existing: Option<(Uuid, i64)> = sqlx::query_as(&format!(
        "SELECT generation,relation_oid::BIGINT FROM {} WHERE source_schema=$1 AND source_table=$2",
        r.registry
    ))
    .bind(&schema)
    .bind(&name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    let fresh = existing.is_none();
    let generation = match existing {
        Some((generation, prior_oid)) => {
            if prior_oid != oid || marker.as_deref() != Some(generation.to_string().as_str()) {
                return Err(
                    "postgres source registered generation or relation was replaced".into(),
                );
            }
            generation
        }
        None if marker.is_some() => {
            return Err("postgres source enrollment marker lost its registration".into());
        }
        None => Uuid::new_v4(),
    };
    let suffix = &identity_digest(&[&schema, &name])[..24];
    let function = format!(
        "{}.{}",
        qi(&r.schema),
        qi(&format!("udb_source_capture_{suffix}"))
    );
    let trigger = format!("udb_source_capture_{suffix}");
    let function_body = capture_body(&r, generation, oid);
    if fresh {
        sqlx::query(&format!("INSERT INTO {} (generation,source_schema,source_table,relation_oid) VALUES ($1,$2,$3,$4::BIGINT::OID)",r.registry))
            .bind(generation).bind(&schema).bind(&name).bind(oid).execute(&mut *tx).await.map_err(|e|e.to_string())?;
        execute(
            &mut tx,
            &format!(
                "ALTER TABLE {relation} ADD CONSTRAINT {} CHECK (TRUE) NOT VALID",
                qi(MARKER)
            ),
        )
        .await
        .map_err(|e| e.to_string())?;
        execute(
            &mut tx,
            &format!(
                "COMMENT ON CONSTRAINT {} ON {relation} IS {}",
                qi(MARKER),
                ql(&generation.to_string())
            ),
        )
        .await
        .map_err(|e| e.to_string())?;
        execute(&mut tx,&format!("CREATE FUNCTION {function}() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS {}",ql(&function_body))).await.map_err(|e|e.to_string())?;
        execute(
            &mut tx,
            &format!("REVOKE ALL ON FUNCTION {function}() FROM PUBLIC"),
        )
        .await
        .map_err(|e| e.to_string())?;
        execute(&mut tx,&format!("CREATE TRIGGER {} AFTER INSERT ON {relation} FOR EACH ROW EXECUTE FUNCTION {function}()",qi(&trigger))).await.map_err(|e|e.to_string())?;
        execute(
            &mut tx,
            &format!(
                "ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {}",
                qi(&trigger)
            ),
        )
        .await
        .map_err(|e| e.to_string())?;
        execute(&mut tx,&format!("INSERT INTO {} (generation,capture_id,capture_order,source_event_seq,topic,payload,created_at) SELECT {}::UUID,pg_catalog.gen_random_uuid(),pg_catalog.nextval({}::regclass),event_seq,topic,payload,created_at FROM {relation}",r.capture,ql(&generation.to_string()),ql(&r.sequence))).await.map_err(|e|e.to_string())?;
    }
    sqlx::query(&format!(
        "INSERT INTO {} (generation,slot) VALUES ($1,$2) ON CONFLICT (generation,slot) DO NOTHING",
        r.consumers
    ))
    .bind(generation)
    .bind(slot)
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    let slot_hash = identity_digest(&[slot]);
    // A configured logical identity can survive endpoint changes. The fallback
    // excludes user/password and binds endpoint/database/relation/consumer.
    let logical = std::env::var("UDB_CDC_POSTGRES_SOURCE_ID")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or(endpoint);
    let binding = identity_digest(&[
        "udb.source-binding.v1",
        &logical,
        &database,
        &schema,
        &name,
        slot,
    ]);
    // A cloned source database can retain generation/capture UUIDs. Its stable
    // configured identity still separates destination cursor and event authority.
    let namespace = format!("postgres:{binding}:{generation}:{slot_hash}");
    let prepared = PreparedPostgresSource {
        pool: pool.clone(),
        relations: r,
        generation,
        slot: slot.into(),
        slot_hash,
        namespace,
        binding,
        generation_text: generation.to_string(),
        source_relation: relation,
        source_oid: oid,
        function,
        function_body,
        trigger,
        destination: Arc::new(OnceLock::new()),
    };
    prepared.verify(&mut tx).await?;
    tx.commit()
        .await
        .map_err(|e| format!("postgres source enrollment commit: {e}"))?;
    Ok(prepared)
}

async fn create_catalog(conn: &mut PgConnection, r: &Relations) -> Result<(), sqlx::Error> {
    let statements = [
        format!("CREATE SCHEMA IF NOT EXISTS {}", qi(&r.schema)),
        format!(
            "CREATE TABLE {} (generation UUID PRIMARY KEY,source_schema TEXT NOT NULL,source_table TEXT NOT NULL,relation_oid OID NOT NULL,UNIQUE(source_schema,source_table))",
            r.registry
        ),
        format!(
            "CREATE TABLE {} (generation UUID NOT NULL REFERENCES {}(generation),slot TEXT NOT NULL CHECK(slot<>''),PRIMARY KEY(generation,slot))",
            r.consumers, r.registry
        ),
        format!(
            "CREATE SEQUENCE {} AS BIGINT MINVALUE 1 NO CYCLE",
            r.sequence
        ),
        format!(
            "CREATE TABLE {} (generation UUID NOT NULL REFERENCES {}(generation),capture_id UUID NOT NULL,capture_order BIGINT NOT NULL UNIQUE CONSTRAINT {POSITIVE_ORDER} CHECK(capture_order>0),source_event_seq BIGINT NOT NULL,topic TEXT NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL,PRIMARY KEY(generation,capture_id))",
            r.capture, r.registry
        ),
        format!(
            "CREATE INDEX {} ON {} (generation,capture_order)",
            qi("udb_cdc_source_pending_order"),
            r.capture
        ),
        format!(
            "CREATE TABLE {} (generation UUID NOT NULL,slot TEXT NOT NULL,capture_id UUID NOT NULL,completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),PRIMARY KEY(generation,slot,capture_id),FOREIGN KEY(generation,slot) REFERENCES {}(generation,slot),FOREIGN KEY(generation,capture_id) REFERENCES {}(generation,capture_id))",
            r.receipts, r.consumers, r.capture
        ),
        format!(
            "CREATE TABLE {} (generation UUID NOT NULL,slot TEXT NOT NULL,source_binding TEXT NOT NULL,destination_binding TEXT NOT NULL,PRIMARY KEY(generation,slot),FOREIGN KEY(generation,slot) REFERENCES {}(generation,slot))",
            r.destinations, r.consumers
        ),
        format!(
            "CREATE FUNCTION {}() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS {}",
            r.immutable,
            ql(immutable_body())
        ),
        format!("REVOKE ALL ON FUNCTION {}() FROM PUBLIC", r.immutable),
    ];
    for statement in statements {
        execute(conn, &statement).await?;
    }
    for target in [
        &r.registry,
        &r.consumers,
        &r.capture,
        &r.receipts,
        &r.destinations,
    ] {
        execute(conn, &format!("REVOKE ALL ON {target} FROM PUBLIC")).await?;
        execute(conn,&format!("CREATE TRIGGER {} BEFORE UPDATE OR DELETE ON {target} FOR EACH ROW EXECUTE FUNCTION {}()",qi(IMMUTABLE),r.immutable)).await?;
        execute(
            conn,
            &format!(
                "ALTER TABLE {target} ENABLE ALWAYS TRIGGER {}",
                qi(IMMUTABLE)
            ),
        )
        .await?;
        execute(conn, &format!("CREATE TRIGGER {} BEFORE TRUNCATE ON {target} FOR EACH STATEMENT EXECUTE FUNCTION {}()", qi(TRUNCATE_GUARD), r.immutable)).await?;
        execute(
            conn,
            &format!(
                "ALTER TABLE {target} ENABLE ALWAYS TRIGGER {}",
                qi(TRUNCATE_GUARD)
            ),
        )
        .await?;
    }
    execute(
        conn,
        &format!("REVOKE ALL ON SEQUENCE {} FROM PUBLIC", r.sequence),
    )
    .await?;
    Ok(())
}

async fn verify_catalog(conn: &mut PgConnection, r: &Relations) -> Result<(), sqlx::Error> {
    // Catalog names and generated bodies are safely SQL-literal rendered.
    let mut body = String::from(
        "DECLARE owner OID; BEGIN SELECT oid INTO owner FROM pg_catalog.pg_roles WHERE rolname=current_user; ",
    );
    for (relation, columns, key) in [
        (
            &r.registry,
            vec![
                ("generation", "uuid"),
                ("source_schema", "text"),
                ("source_table", "text"),
                ("relation_oid", "oid"),
            ],
            vec!["generation"],
        ),
        (
            &r.consumers,
            vec![("generation", "uuid"), ("slot", "text")],
            vec!["generation", "slot"],
        ),
        (
            &r.capture,
            vec![
                ("generation", "uuid"),
                ("capture_id", "uuid"),
                ("capture_order", "int8"),
                ("source_event_seq", "int8"),
                ("topic", "text"),
                ("payload", "jsonb"),
                ("created_at", "timestamptz"),
            ],
            vec!["generation", "capture_id"],
        ),
        (
            &r.receipts,
            vec![
                ("generation", "uuid"),
                ("slot", "text"),
                ("capture_id", "uuid"),
                ("completed_at", "timestamptz"),
            ],
            vec!["generation", "slot", "capture_id"],
        ),
        (
            &r.destinations,
            vec![
                ("generation", "uuid"),
                ("slot", "text"),
                ("source_binding", "text"),
                ("destination_binding", "text"),
            ],
            vec!["generation", "slot"],
        ),
    ] {
        let rel = ql(relation);
        body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_class WHERE oid={rel}::regclass AND relkind='r' AND relpersistence='p' AND relowner=owner AND NOT relrowsecurity) THEN RAISE EXCEPTION 'CDC source table owner or shape mismatch'; END IF; "));
        for (column, ty) in columns {
            body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid={rel}::regclass AND attname={} AND atttypid={}::regtype AND attnotnull AND NOT attisdropped) THEN RAISE EXCEPTION 'CDC source storage type mismatch'; END IF; ",ql(column),ql(&format!("pg_catalog.{ty}"))));
        }
        let key_array = key.iter().map(|v| ql(v)).collect::<Vec<_>>().join(",");
        body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid={rel}::regclass AND i.indisprimary AND i.indisvalid AND i.indisready AND i.indimmediate AND (SELECT array_agg(a.attname::TEXT ORDER BY k.ordinality) FROM unnest(i.indkey::SMALLINT[]) WITH ORDINALITY k(num,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num)=ARRAY[{key_array}]::TEXT[]) THEN RAISE EXCEPTION 'CDC source storage primary key mismatch'; END IF; "));
        body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid={rel}::regclass AND t.tgname={} AND t.tgenabled='A' AND t.tgtype=27 AND t.tgqual IS NULL AND t.tgnargs=0 AND NOT t.tgisinternal AND p.oid={}::regprocedure AND p.prosrc={} AND p.proowner=owner AND NOT p.prosecdef AND p.prorettype='pg_catalog.trigger'::regtype AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='plpgsql') AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']::TEXT[]) THEN RAISE EXCEPTION 'CDC source immutable trigger authority mismatch'; END IF; ",ql(IMMUTABLE),ql(&format!("{}()",r.immutable)),ql(immutable_body())));
        body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid={rel}::regclass AND t.tgname={} AND t.tgenabled='A' AND t.tgtype=34 AND t.tgqual IS NULL AND t.tgnargs=0 AND NOT t.tgisinternal AND t.tgfoid={}::regprocedure) THEN RAISE EXCEPTION 'CDC source truncate guard authority mismatch'; END IF; ",ql(TRUNCATE_GUARD),ql(&format!("{}()",r.immutable))));
        body.push_str(&format!("IF EXISTS(SELECT 1 FROM pg_catalog.pg_attribute att CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) acl WHERE att.attrelid={rel}::regclass AND NOT att.attisdropped AND acl.grantee<>owner AND acl.privilege_type IN ('INSERT','UPDATE','REFERENCES')) THEN RAISE EXCEPTION 'CDC source untrusted column privilege is forbidden'; END IF; "));
        // PUBLIC may have no direct table privileges, even on a reused object.
        body.push_str(&format!("IF EXISTS(SELECT 1 FROM pg_catalog.pg_class c CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a WHERE c.oid={rel}::regclass AND a.grantee<>owner AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','TRIGGER','REFERENCES')) THEN RAISE EXCEPTION 'CDC source untrusted storage privilege is forbidden'; END IF; "));
    }
    body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class index_class ON index_class.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=index_class.relam WHERE i.indrelid={}::regclass AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname='btree' AND (SELECT array_agg(a.attname::TEXT ORDER BY k.ordinality) FROM unnest(i.indkey::SMALLINT[]) WITH ORDINALITY k(num,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num)=ARRAY['generation','capture_order']::TEXT[]) THEN RAISE EXCEPTION 'CDC source pending scan index authority mismatch'; END IF; ",ql(&r.capture)));
    body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid={}::regclass AND conname={} AND contype='c' AND convalidated AND pg_catalog.pg_get_expr(conbin,conrelid)='(capture_order > 0)') OR EXISTS(SELECT 1 FROM {} WHERE capture_order<=0) THEN RAISE EXCEPTION 'CDC source positive capture order authority mismatch'; END IF; ",ql(&r.capture),ql(POSITIVE_ORDER),r.capture));
    body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_sequence s ON s.seqrelid=c.oid WHERE c.oid={}::regclass AND c.relowner=owner AND c.relpersistence='p' AND s.seqtypid='pg_catalog.int8'::regtype AND s.seqmin=1 AND NOT s.seqcycle) THEN RAISE EXCEPTION 'CDC source capture sequence authority mismatch'; END IF; ",ql(&r.sequence)));
    for (relation, columns) in [
        (&r.registry, vec!["source_schema", "source_table"]),
        (&r.capture, vec!["capture_order"]),
    ] {
        let keys = columns.iter().map(|v| ql(v)).collect::<Vec<_>>().join(",");
        body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid={}::regclass AND i.indisunique AND i.indisvalid AND i.indisready AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL AND (SELECT array_agg(a.attname::TEXT ORDER BY k.ordinality) FROM unnest(i.indkey::SMALLINT[]) WITH ORDINALITY k(num,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num)=ARRAY[{keys}]::TEXT[]) THEN RAISE EXCEPTION 'CDC source unique identity authority mismatch'; END IF; ",ql(relation)));
    }
    for (relation, columns, target, target_columns) in [
        (
            &r.consumers,
            vec!["generation"],
            &r.registry,
            vec!["generation"],
        ),
        (
            &r.capture,
            vec!["generation"],
            &r.registry,
            vec!["generation"],
        ),
        (
            &r.receipts,
            vec!["generation", "slot"],
            &r.consumers,
            vec!["generation", "slot"],
        ),
        (
            &r.receipts,
            vec!["generation", "capture_id"],
            &r.capture,
            vec!["generation", "capture_id"],
        ),
        (
            &r.destinations,
            vec!["generation", "slot"],
            &r.consumers,
            vec!["generation", "slot"],
        ),
    ] {
        let keys = columns.iter().map(|v| ql(v)).collect::<Vec<_>>().join(",");
        let targets = target_columns
            .iter()
            .map(|v| ql(v))
            .collect::<Vec<_>>()
            .join(",");
        body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conrelid={}::regclass AND c.confrelid={}::regclass AND c.contype='f' AND c.convalidated AND NOT c.condeferrable AND c.confdeltype='a' AND c.confupdtype='a' AND (SELECT array_agg(a.attname::TEXT ORDER BY k.ordinality) FROM unnest(c.conkey) WITH ORDINALITY k(num,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num)=ARRAY[{keys}]::TEXT[] AND (SELECT array_agg(a.attname::TEXT ORDER BY k.ordinality) FROM unnest(c.confkey) WITH ORDINALITY k(num,ordinality) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.num)=ARRAY[{targets}]::TEXT[]) THEN RAISE EXCEPTION 'CDC source foreign identity authority mismatch'; END IF; ",ql(relation),ql(target)));
    }
    body.push_str(&format!("IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspname::TEXT={} AND n.nspowner=owner) THEN RAISE EXCEPTION 'CDC source schema owner or identifier mismatch'; END IF; ",ql(&r.schema)));
    body.push_str(&format!("IF EXISTS(SELECT 1 FROM pg_catalog.pg_proc p CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) a WHERE p.oid={}::regprocedure AND a.grantee<>owner) THEN RAISE EXCEPTION 'CDC source untrusted helper execution is forbidden'; END IF; ",ql(&format!("{}()",r.immutable))));
    body.push_str(&format!("IF EXISTS(SELECT 1 FROM pg_catalog.pg_class c CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('S',c.relowner))) a WHERE c.oid={}::regclass AND a.grantee<>owner) THEN RAISE EXCEPTION 'CDC source untrusted sequence privilege is forbidden'; END IF; ",ql(&r.sequence)));
    body.push_str(&format!("IF EXISTS(SELECT 1 FROM pg_catalog.pg_namespace n CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a WHERE n.nspname::TEXT={} AND a.privilege_type='CREATE' AND a.grantee<>owner) THEN RAISE EXCEPTION 'CDC source schema permits untrusted CREATE'; END IF; END;",ql(&r.schema)));
    execute(conn, &format!("DO {}", ql(&body))).await
}

impl PreparedPostgresSource {
    async fn verify(&self, conn: &mut PgConnection) -> Result<(), String> {
        verify_catalog(conn, &self.relations)
            .await
            .map_err(|e| format!("postgres source catalog authority: {e}"))?;
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid WHERE t.tgrelid=$1::BIGINT::OID AND c.oid=pg_catalog.to_regclass($2) AND t.tgname=$3 AND t.tgenabled='A' AND t.tgtype=5 AND NOT t.tgisinternal AND t.tgnargs=0 AND t.tgqual IS NULL AND p.oid=$4::regprocedure AND p.prosrc=$5 AND p.prosecdef AND p.prorettype='pg_catalog.trigger'::regtype AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='plpgsql') AND p.proowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']::TEXT[] AND NOT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) a WHERE a.grantee<>p.proowner))")
            .bind(self.source_oid).bind(&self.source_relation).bind(&self.trigger).bind(format!("{}()",self.function)).bind(&self.function_body)
            .fetch_one(&mut *conn).await.map_err(|e|format!("postgres source trigger authority: {e}"))?;
        if !valid {
            return Err("postgres source capture trigger authority mismatch".into());
        }
        let generation: Option<Uuid> = sqlx::query_scalar(&format!(
            "SELECT generation FROM {} WHERE relation_oid=$1::BIGINT::OID",
            self.relations.registry
        ))
        .bind(self.source_oid)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| e.to_string())?;
        if generation != Some(self.generation) {
            return Err("postgres source capture generation authority mismatch".into());
        }
        let marker: Option<String> = sqlx::query_scalar("SELECT pg_catalog.obj_description(oid,'pg_constraint') FROM pg_catalog.pg_constraint WHERE conrelid=$1::BIGINT::OID AND conname=$2 AND contype='c'")
            .bind(self.source_oid).bind(MARKER).fetch_optional(&mut *conn).await.map_err(|e|e.to_string())?.flatten();
        if marker.as_deref() != Some(self.generation_text.as_str()) {
            return Err("postgres source enrollment marker authority mismatch".into());
        }
        Ok(())
    }

    fn offset(&self, id: Uuid) -> String {
        format!(
            "capture-v1:{}:{}:{}:{id}",
            self.binding, self.generation, self.slot_hash
        )
    }
    fn parse_offset(&self, offset: &str) -> Result<Uuid, String> {
        let prefix = format!(
            "capture-v1:{}:{}:{}:",
            self.binding, self.generation, self.slot_hash
        );
        offset
            .strip_prefix(&prefix)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| {
                "postgres source offset generation/consumer/capture identity mismatch".to_string()
            })
    }
    fn event(&self, row: &sqlx::postgres::PgRow) -> Result<CdcEvent, String> {
        let id: Uuid = row.try_get("capture_id").map_err(|e| e.to_string())?;
        let topic: String = row.try_get("topic").map_err(|e| e.to_string())?;
        let payload: serde_json::Value = row.try_get("payload").map_err(|e| e.to_string())?;
        let created: chrono::DateTime<chrono::Utc> =
            row.try_get("created_at").map_err(|e| e.to_string())?;
        let mut event = CdcEvent::insert(topic, payload, self.offset(id));
        event.source_ts_unix_ms = created.timestamp_millis();
        Ok(event)
    }
}

#[async_trait]
impl CdcSource for PreparedPostgresSource {
    fn backend_label(&self) -> &str {
        "postgres"
    }
    fn offset_namespace(&self) -> &str {
        &self.namespace
    }
    fn enrollment_binding(&self) -> Option<(&str, &str)> {
        Some((&self.binding, &self.generation_text))
    }

    async fn bind_destination(&self, destination: &str) -> Result<(), String> {
        if destination.is_empty() {
            return Err("CDC_SOURCE_DESTINATION_REFUSED: destination identity is empty".into());
        }
        tokio::time::timeout(OPERATION_TIMEOUT,async {
            let mut tx=self.pool.begin().await.map_err(|e|format!("postgres source destination claim I/O: {e}"))?;
            execute(&mut tx,&format!("LOCK TABLE {} IN ACCESS SHARE MODE",self.source_relation)).await.map_err(|e|format!("postgres source destination claim I/O: {e}"))?;
            self.verify(&mut tx).await?;
            // This consumer-row lock also conflicts with standalone receipt FK
            // checks, so first destination claim and direct ACK cannot race.
            sqlx::query(&format!("SELECT generation FROM {} WHERE generation=$1 AND slot=$2 FOR UPDATE",self.relations.consumers))
                .bind(self.generation).bind(&self.slot).fetch_one(&mut *tx).await.map_err(|e|format!("postgres source destination claim I/O: {e}"))?;
            let existing:Option<(String,String)>=sqlx::query_as(&format!("SELECT source_binding,destination_binding FROM {} WHERE generation=$1 AND slot=$2",self.relations.destinations))
                .bind(self.generation).bind(&self.slot).fetch_optional(&mut *tx).await.map_err(|e|format!("postgres source destination claim I/O: {e}"))?;
            match existing {
                Some((source,target)) if source==self.binding && target==destination => {}
                Some(_) => return Err("CDC_SOURCE_DESTINATION_REFUSED: consumer belongs to a different configured source or destination".into()),
                None => {
                    let completed:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {} WHERE generation=$1 AND slot=$2)",self.relations.receipts))
                        .bind(self.generation).bind(&self.slot).fetch_one(&mut *tx).await.map_err(|e|format!("postgres source destination claim I/O: {e}"))?;
                    if completed {
                        return Err("CDC_SOURCE_DESTINATION_REFUSED: consumer has standalone receipts; configure a new source slot".into());
                    }
                    sqlx::query(&format!("INSERT INTO {} (generation,slot,source_binding,destination_binding) VALUES ($1,$2,$3,$4)",self.relations.destinations))
                        .bind(self.generation).bind(&self.slot).bind(&self.binding).bind(destination).execute(&mut *tx).await.map_err(|e|format!("postgres source destination claim I/O: {e}"))?;
                }
            }
            tx.commit().await.map_err(|e|format!("postgres source destination claim commit I/O: {e}"))?;
            if self.destination.get().is_some_and(|bound| bound != destination) {
                return Err("CDC_SOURCE_DESTINATION_REFUSED: prepared consumer is already bound to another destination".into());
            }
            let _ = self.destination.set(destination.to_string());
            Ok(())
        }).await.map_err(|_|"postgres source destination claim deadline exceeded".to_string())?
    }

    async fn open(
        &self,
        from_offset: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<CdcEvent, String>> + Send>>, String> {
        // Legacy numbers are diagnostics, never proof every lower row completed.
        if !from_offset.is_empty() && from_offset.parse::<i64>().is_err() {
            let id = self.parse_offset(from_offset)?;
            let present: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE generation=$1 AND capture_id=$2)",
                self.relations.capture
            ))
            .bind(self.generation)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
            if !present {
                return Err("postgres source offset references a missing captured event".into());
            }
        }
        self.health().await?;
        let source = self.clone();
        Ok(Box::pin(async_stream::try_stream! {
            loop {
                source.health().await?;
                let ceiling:i64=sqlx::query_scalar(&format!("SELECT COALESCE(MAX(capture_order),0) FROM {} WHERE generation=$1",source.relations.capture))
                    .bind(source.generation).fetch_one(&source.pool).await.map_err(|e|format!("postgres source scan ceiling: {e}"))?;
                let mut page=0_i64;
                loop {
                    let rows=sqlx::query(&format!("SELECT q.capture_id,q.capture_order,q.topic,q.payload,q.created_at FROM {} q WHERE q.generation=$1 AND q.capture_order>$2 AND q.capture_order<=$3 AND NOT EXISTS(SELECT 1 FROM {} r WHERE r.generation=q.generation AND r.slot=$4 AND r.capture_id=q.capture_id) ORDER BY q.capture_order LIMIT {PAGE_SIZE}",source.relations.capture,source.relations.receipts))
                        .bind(source.generation).bind(page).bind(ceiling).bind(&source.slot).fetch_all(&source.pool).await
                        .map_err(|e|format!("postgres source pending scan: {e}"))?;
                    if rows.is_empty() { break; }
                    for row in rows {
                        let position:i64=row.try_get("capture_order").map_err(|e|e.to_string())?;
                        let event=source.event(&row)?;
                        page=position;
                        yield event;
                    }
                }
                tokio::time::sleep(POLL_DELAY).await;
            }
        }))
    }
    async fn acknowledge(&self, event: &CdcEvent) -> Result<(), String> {
        let id = self.parse_offset(&event.source_offset)?;
        tokio::time::timeout(OPERATION_TIMEOUT,async {
            let mut tx=self.pool.begin().await.map_err(|e|e.to_string())?;
            // Serialize against privileged authority-changing DDL, not producers.
            execute(&mut tx,&format!("LOCK TABLE {} IN ACCESS SHARE MODE",self.source_relation)).await.map_err(|e|e.to_string())?;
            self.verify(&mut tx).await?;
            sqlx::query(&format!("SELECT generation FROM {} WHERE generation=$1 AND slot=$2 FOR UPDATE",self.relations.consumers))
                .bind(self.generation).bind(&self.slot).fetch_one(&mut *tx).await.map_err(|e|e.to_string())?;
            let claim:Option<(String,String)>=sqlx::query_as(&format!("SELECT source_binding,destination_binding FROM {} WHERE generation=$1 AND slot=$2",self.relations.destinations))
                .bind(self.generation).bind(&self.slot).fetch_optional(&mut *tx).await.map_err(|e|e.to_string())?;
            match (self.destination.get(),claim) {
                (None,None) => {}
                (Some(bound),Some((source,target))) if source==self.binding && target==*bound => {}
                _ => return Err("CDC_SOURCE_DESTINATION_REFUSED: receipt requires the prepared consumer's verified destination claim".into()),
            }
            let row=sqlx::query(&format!("SELECT capture_id,topic,payload,created_at FROM {} WHERE generation=$1 AND capture_id=$2",self.relations.capture))
                .bind(self.generation).bind(id).fetch_optional(&mut *tx).await.map_err(|e|e.to_string())?
                .ok_or_else(||"postgres source receipt references an unknown captured event".to_string())?;
            if self.event(&row)? != *event { return Err("postgres source receipt event differs from immutable captured image".into()); }
            sqlx::query(&format!("INSERT INTO {} (generation,slot,capture_id) VALUES ($1,$2,$3) ON CONFLICT(generation,slot,capture_id) DO NOTHING",self.relations.receipts))
                .bind(self.generation).bind(&self.slot).bind(id).execute(&mut *tx).await.map_err(|e|e.to_string())?;
            tx.commit().await.map_err(|e|e.to_string())
        }).await.map_err(|_|"postgres source receipt deadline exceeded".to_string())?
    }
    async fn health(&self) -> Result<(), String> {
        tokio::time::timeout(OPERATION_TIMEOUT, async {
            let mut conn = self.pool.acquire().await.map_err(|e| e.to_string())?;
            self.verify(&mut conn).await
        })
        .await
        .map_err(|_| "postgres source health deadline exceeded".to_string())?
    }
}
