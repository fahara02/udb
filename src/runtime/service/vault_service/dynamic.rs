//! Dynamic database-credential engine helpers for the native `VaultService`: the
//! operator allow-list config (`UDB_VAULT_DB_ROLES_JSON`), the Postgres-identifier
//! validators, the identifier/literal quoters, the generated username/password
//! sources, the TTL resolver, and the tenant/project-bound short-lived login
//! authority. The generated login receives no parent-role membership. Instead it
//! gets direct read-only grants to an explicit relation allow-list plus one
//! restrictive RLS policy per relation whose tenant/project literals are fixed by
//! operator configuration. Caller-changeable GUCs are compatibility hints only.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use super::config::{
    DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS, DEFAULT_DB_CREDENTIAL_TTL_SECONDS,
    MIN_DB_CREDENTIAL_TTL_SECONDS,
};
use super::errors::{
    vault_db_credentials_config_status, vault_db_role_creation_status, vault_field_violation,
    vault_internal_status,
};

const DIRECT_DATABASE_PRIVILEGES: &[&str] = &["SELECT"];
const MAX_BOUND_RELATIONS: usize = 64;
const POLICY_NAME_SUFFIX_LEN: usize = 5;
pub(crate) type DbCredentialRoleConfigs = HashMap<String, DbCredentialRoleConfig>;

/// One exact relation protected by the database-native credential authority.
/// Both scope columns are mandatory: a tenant-only table cannot honestly back a
/// credential advertised as tenant *and* project bound.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DbCredentialRelationConfig {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) tenant_column: String,
    pub(crate) project_column: String,
    pub(crate) privileges: Vec<String>,
}

/// Operator allow-list entry for dynamic database credentials. `role_name` is
/// the RPC-facing alias, and every remaining selector is an immutable binding:
/// an alias cannot float between tenants, projects, instances, databases, or
/// policy revisions. The generated login password is returned once and never
/// stored.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DbCredentialRoleConfig {
    pub(crate) role_name: String,
    pub(crate) tenant_id: String,
    pub(crate) project_id: String,
    pub(crate) target_instance: String,
    pub(crate) database_name: String,
    pub(crate) policy_revision: String,
    pub(crate) relations: Vec<DbCredentialRelationConfig>,
    #[serde(default)]
    pub(crate) ttl_seconds_max: Option<i32>,
}

/// Non-secret physical and policy provenance persisted with each lease.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DbCredentialAuthorityProvenance {
    pub(crate) target_instance: String,
    pub(crate) database_name: String,
    pub(crate) server_address: String,
    pub(crate) server_port: i32,
    pub(crate) policy_revision: String,
    pub(crate) policy_sha256: String,
    pub(crate) policy_names: Vec<String>,
    pub(crate) relations: Vec<String>,
}

pub(crate) fn vault_db_role_configs() -> Result<&'static DbCredentialRoleConfigs, Status> {
    static CONFIGS: OnceLock<Result<DbCredentialRoleConfigs, String>> =
        OnceLock::new();
    match CONFIGS.get_or_init(|| {
        let raw = std::env::var("UDB_VAULT_DB_ROLES_JSON")
            .map_err(|_| "UDB_VAULT_DB_ROLES_JSON is not configured".to_string())?;
        parse_vault_db_role_configs(&raw)
    }) {
        Ok(configs) => Ok(configs),
        Err(err) => Err(vault_db_credentials_config_status(format!(
            "vault dynamic database credentials are not configured: {err}"
        ))),
    }
}

pub(crate) fn parse_vault_db_role_configs(
    raw: &str,
) -> Result<HashMap<String, DbCredentialRoleConfig>, String> {
    let entries: Vec<DbCredentialRoleConfig> =
        serde_json::from_str(raw).map_err(|err| format!("invalid JSON: {err}"))?;
    if entries.is_empty() {
        return Err("at least one role entry is required".to_string());
    }
    let mut configs = HashMap::with_capacity(entries.len());
    for mut entry in entries {
        entry.role_name = entry.role_name.trim().to_string();
        entry.tenant_id = entry.tenant_id.trim().to_string();
        entry.project_id = entry.project_id.trim().to_string();
        entry.target_instance = entry.target_instance.trim().to_string();
        entry.database_name = entry.database_name.trim().to_string();
        entry.policy_revision = entry.policy_revision.trim().to_string();
        validate_db_role_alias(&entry.role_name).map_err(|err| err.message().to_string())?;
        validate_scope_selector(&entry.tenant_id, "tenant_id")?;
        validate_scope_selector(&entry.project_id, "project_id")?;
        validate_runtime_instance(&entry.target_instance)?;
        validate_pg_identifier_value(&entry.database_name, "database_name")?;
        validate_policy_revision(&entry.policy_revision)?;
        if entry.relations.is_empty() || entry.relations.len() > MAX_BOUND_RELATIONS {
            return Err(format!(
                "role '{}' must bind 1..={MAX_BOUND_RELATIONS} relations",
                entry.role_name,
            ));
        }
        let mut relations = HashSet::with_capacity(entry.relations.len());
        for relation in &mut entry.relations {
            relation.schema = relation.schema.trim().to_string();
            relation.table = relation.table.trim().to_string();
            relation.tenant_column = relation.tenant_column.trim().to_string();
            relation.project_column = relation.project_column.trim().to_string();
            validate_pg_identifier_value(&relation.schema, "relation schema")?;
            validate_pg_identifier_value(&relation.table, "relation table")?;
            validate_pg_identifier_value(&relation.tenant_column, "tenant_column")?;
            validate_pg_identifier_value(&relation.project_column, "project_column")?;
            if matches!(relation.schema.as_str(), "pg_catalog" | "information_schema") {
                return Err(format!(
                    "role '{}' cannot delegate a system relation",
                    entry.role_name
                ));
            }
            if !relations.insert((relation.schema.clone(), relation.table.clone())) {
                return Err(format!(
                    "role '{}' contains duplicate relation '{}.{}'",
                    entry.role_name, relation.schema, relation.table
                ));
            }
            if relation.privileges.is_empty() {
                return Err(format!(
                    "role '{}' relation '{}.{}' must declare SELECT",
                    entry.role_name, relation.schema, relation.table
                ));
            }
            for privilege in &mut relation.privileges {
                *privilege = privilege.trim().to_ascii_uppercase();
                if !DIRECT_DATABASE_PRIVILEGES.contains(&privilege.as_str()) {
                    return Err(format!(
                        "role '{}' relation '{}.{}' privilege '{}' is unsafe; only SELECT is supported",
                        entry.role_name, relation.schema, relation.table, privilege
                    ));
                }
            }
            relation.privileges.sort();
            relation.privileges.dedup();
        }
        let max_ttl = entry
            .ttl_seconds_max
            .unwrap_or(DEFAULT_DB_CREDENTIAL_MAX_TTL_SECONDS);
        if max_ttl < MIN_DB_CREDENTIAL_TTL_SECONDS {
            return Err(format!(
                "role '{}' ttl_seconds_max must be at least {MIN_DB_CREDENTIAL_TTL_SECONDS}",
                entry.role_name
            ));
        }
        if configs.insert(entry.role_name.clone(), entry).is_some() {
            return Err("duplicate role_name in UDB_VAULT_DB_ROLES_JSON".to_string());
        }
    }
    Ok(configs)
}

fn validate_scope_selector(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
    {
        return Err(format!(
            "{label} must be 1..128 ASCII chars using letters, digits, _, -, :, or ."
        ));
    }
    Ok(())
}

fn validate_runtime_instance(value: &str) -> Result<(), String> {
    validate_scope_selector(value, "target_instance")
}

fn validate_policy_revision(value: &str) -> Result<(), String> {
    validate_scope_selector(value, "policy_revision")
}

pub(crate) fn validate_db_role_alias(role_name: &str) -> Result<(), Status> {
    if role_name.is_empty()
        || role_name.len() > 128
        || !role_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
    {
        return Err(vault_field_violation(
            "role_name",
            "must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
            "role_name must be 1..128 ASCII chars using letters, digits, _, -, :, or .",
        ));
    }
    Ok(())
}

pub(crate) fn validate_pg_identifier_value(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 63
        || value.starts_with(|ch: char| ch.is_ascii_digit())
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(format!(
            "{label} '{value}' is not a valid unquoted Postgres identifier"
        ));
    }
    Ok(())
}

pub(crate) fn pg_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn generate_db_password() -> String {
    // A clean 256-bit OS-CSPRNG draw (no fixed UUID version/variant nibbles).
    BASE64_STANDARD.encode(crate::runtime::executor_utils::random_32_bytes())
}

pub(crate) fn generate_db_username() -> String {
    format!("udb_vault_{}", Uuid::new_v4().simple())
}

pub(crate) fn requested_db_credential_ttl(
    req_ttl_seconds: i32,
    max_ttl_seconds: i32,
) -> Result<i32, Status> {
    let ttl = if req_ttl_seconds <= 0 {
        DEFAULT_DB_CREDENTIAL_TTL_SECONDS.min(max_ttl_seconds)
    } else {
        req_ttl_seconds
    };
    if ttl < MIN_DB_CREDENTIAL_TTL_SECONDS {
        return Err(vault_field_violation(
            "ttl_seconds",
            format!("must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"),
            format!("ttl_seconds must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"),
        ));
    }
    if ttl > max_ttl_seconds {
        return Err(vault_field_violation(
            "ttl_seconds",
            format!("must not exceed configured maximum {max_ttl_seconds}"),
            format!("ttl_seconds exceeds configured maximum {max_ttl_seconds}"),
        ));
    }
    Ok(ttl)
}

/// Reject an alias unless it is bound to the verified tenant/project and the
/// already-resolved canonical runtime instance. This check happens before any
/// database role or policy mutation.
pub(crate) fn validate_db_credential_binding(
    config: &DbCredentialRoleConfig,
    tenant_id: &str,
    project_id: &str,
    target_instance: &str,
) -> Result<(), Status> {
    let target_instance = if target_instance.trim().is_empty() {
        "primary"
    } else {
        target_instance.trim()
    };
    if config.tenant_id != tenant_id
        || config.project_id != project_id
        || config.target_instance != target_instance
    {
        return Err(vault_db_credentials_config_status(format!(
            "vault dynamic database role '{}' is not bound to tenant '{}', project '{}', instance '{}'",
            config.role_name, tenant_id, project_id, target_instance
        )));
    }
    Ok(())
}

/// Create a database-native tenant/project-bound login on the caller's
/// transaction connection. The helper never commits: issuance/lifecycle code
/// owns the transaction containing the durable lease FSM and strict outbox.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_postgres_login_role(
    conn: &mut PgConnection,
    username: &str,
    password: &str,
    expires_at: DateTime<Utc>,
    tenant_id: &str,
    project_id: &str,
    target_instance: &str,
    config: &DbCredentialRoleConfig,
) -> Result<DbCredentialAuthorityProvenance, Status> {
    validate_pg_identifier_value(username, "generated username")
        .map_err(|err| vault_internal_status("create_postgres_login_role", err))?;
    validate_db_credential_binding(config, tenant_id, project_id, target_instance)?;

    let target = sqlx::query(
        "SELECT current_database() AS database_name, \
                COALESCE(inet_server_addr()::text, 'local-socket') AS server_address, \
                COALESCE(inet_server_port(), 0) AS server_port",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        vault_db_role_creation_status(format!(
            "vault database credential target inspection failed: {err}"
        ))
    })?;
    let database_name: String = target.get("database_name");
    let server_address: String = target.get("server_address");
    let server_port: i32 = target.get("server_port");
    if database_name != config.database_name {
        return Err(vault_db_credentials_config_status(format!(
            "vault dynamic database role '{}' expected database '{}' but canonical instance '{}' resolved database '{}'",
            config.role_name, config.database_name, config.target_instance, database_name
        )));
    }

    audit_public_database_authority(conn, config).await?;
    for relation in &config.relations {
        audit_bound_relation(conn, relation).await?;
    }

    let create_sql = format!(
        "CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE \
         NOREPLICATION NOBYPASSRLS PASSWORD {} VALID UNTIL {}",
        pg_ident(username),
        pg_literal(password),
        pg_literal(&expires_at.to_rfc3339()),
    );
    sqlx::query(&create_sql)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            vault_db_role_creation_status(format!(
                "vault database credential role creation failed: {err}"
            ))
        })?;

    // These defaults preserve compatibility with existing UDB policies, but are
    // never the authority: callers may change custom GUCs, while the restrictive
    // policies installed below retain fixed literal predicates.
    for (setting, value) in [
        ("app.current_tenant_id", tenant_id),
        ("app.current_project_id", project_id),
    ] {
        let sql = format!(
            "ALTER ROLE {} SET {} = {}",
            pg_ident(username),
            setting,
            pg_literal(value)
        );
        sqlx::query(&sql)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                vault_db_role_creation_status(format!(
                    "vault database credential scope-default installation failed: {err}"
                ))
            })?;
    }
    let read_only_sql = format!(
        "ALTER ROLE {} SET default_transaction_read_only = on",
        pg_ident(username)
    );
    sqlx::query(&read_only_sql)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            vault_db_role_creation_status(format!(
                "vault database credential read-only default installation failed: {err}"
            ))
        })?;

    let mut policy_names = Vec::with_capacity(config.relations.len());
    let mut relations = Vec::with_capacity(config.relations.len());
    for (index, relation) in config.relations.iter().enumerate() {
        let relation_name = format!("{}.{}", relation.schema, relation.table);
        let policy_name = credential_policy_name(username, index)?;
        let relation_sql = format!(
            "{}.{}",
            pg_ident(&relation.schema),
            pg_ident(&relation.table)
        );
        for sql in [
            format!(
                "ALTER TABLE {relation_sql} ENABLE ROW LEVEL SECURITY"
            ),
            format!(
                "ALTER TABLE {relation_sql} FORCE ROW LEVEL SECURITY"
            ),
            format!(
                "GRANT USAGE ON SCHEMA {} TO {}",
                pg_ident(&relation.schema),
                pg_ident(username)
            ),
            format!("GRANT SELECT ON TABLE {relation_sql} TO {}", pg_ident(username)),
        ] {
            sqlx::query(&sql)
                .execute(&mut *conn)
                .await
                .map_err(|err| {
                    vault_db_role_creation_status(format!(
                        "vault database credential grant/RLS installation failed for {relation_name}: {err}"
                    ))
                })?;
        }
        let predicate = format!(
            "({})::text = {} AND ({})::text = {}",
            pg_ident(&relation.tenant_column),
            pg_literal(tenant_id),
            pg_ident(&relation.project_column),
            pg_literal(project_id)
        );
        let policy_sql = format!(
            "CREATE POLICY {} ON {relation_sql} AS RESTRICTIVE FOR SELECT TO {} USING ({predicate})",
            pg_ident(&policy_name),
            pg_ident(username)
        );
        sqlx::query(&policy_sql)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                vault_db_role_creation_status(format!(
                    "vault database credential restrictive policy installation failed for {relation_name}: {err}"
                ))
            })?;
        policy_names.push(policy_name);
        relations.push(relation_name);
    }

    audit_created_login(conn, username).await?;

    let policy_json = serde_json::to_vec(config).map_err(|err| {
        vault_internal_status(
            "create_postgres_login_role",
            format!("serialize effective database credential policy failed: {err}"),
        )
    })?;
    let policy_sha256 = sha256_hex(&policy_json);

    Ok(DbCredentialAuthorityProvenance {
        target_instance: if target_instance.trim().is_empty() {
            "primary".to_string()
        } else {
            target_instance.trim().to_string()
        },
        database_name,
        server_address,
        server_port,
        policy_revision: config.policy_revision.clone(),
        policy_sha256,
        policy_names,
        relations,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn credential_policy_name(username: &str, index: usize) -> Result<String, Status> {
    let suffix = format!("_r{index:03}");
    if suffix.len() != POLICY_NAME_SUFFIX_LEN || username.len() + suffix.len() > 63 {
        return Err(vault_internal_status(
            "create_postgres_login_role",
            "generated restrictive policy name exceeds the Postgres identifier limit",
        ));
    }
    Ok(format!("{username}{suffix}"))
}

async fn audit_public_database_authority(
    conn: &mut PgConnection,
    config: &DbCredentialRoleConfig,
) -> Result<(), Status> {
    let public_table_acl: Option<String> = sqlx::query_scalar(
        "SELECT format('%I.%I:%s', n.nspname, c.relname, acl.privilege_type) \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) acl \
         WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND acl.grantee = 0 \
           AND acl.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE') \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass \
                           AND d.objid = c.oid AND d.deptype = 'e') \
         LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        vault_db_role_creation_status(format!(
            "vault database credential PUBLIC table-privilege audit failed: {err}"
        ))
    })?;
    if let Some(grant) = public_table_acl {
        return Err(vault_db_credentials_config_status(format!(
            "database '{}' grants PUBLIC data privilege '{grant}'; bounded credential issuance is refused",
            config.database_name
        )));
    }

    let public_security_definer: Option<String> = sqlx::query_scalar(
        "SELECT format('%I.%I', n.nspname, p.proname) \
         FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) acl \
         WHERE p.prosecdef AND acl.grantee = 0 AND acl.privilege_type = 'EXECUTE' \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass \
                           AND d.objid = p.oid AND d.deptype = 'e') \
         LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        vault_db_role_creation_status(format!(
            "vault database credential SECURITY DEFINER audit failed: {err}"
        ))
    })?;
    if let Some(function) = public_security_definer {
        return Err(vault_db_credentials_config_status(format!(
            "database '{}' exposes PUBLIC SECURITY DEFINER function '{function}'; bounded credential issuance is refused",
            config.database_name
        )));
    }
    Ok(())
}

async fn audit_bound_relation(
    conn: &mut PgConnection,
    relation: &DbCredentialRelationConfig,
) -> Result<(), Status> {
    let row = sqlx::query(
        "SELECT c.relkind::text AS relkind, \
                EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = $3 AND NOT a.attisdropped) AS has_tenant, \
                EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = $4 AND NOT a.attisdropped) AS has_project, \
                EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid AND p.polpermissive \
                        AND p.polcmd IN ('*', 'r') AND 0::oid = ANY(p.polroles)) AS has_public_permissive_select \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(&relation.schema)
    .bind(&relation.table)
    .bind(&relation.tenant_column)
    .bind(&relation.project_column)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        vault_db_role_creation_status(format!(
            "vault database credential relation audit failed for '{}.{}': {err}",
            relation.schema, relation.table
        ))
    })?
    .ok_or_else(|| {
        vault_db_credentials_config_status(format!(
            "vault database credential relation '{}.{}' does not exist on the canonical target",
            relation.schema, relation.table
        ))
    })?;
    let relkind: String = row.get("relkind");
    let has_tenant: bool = row.get("has_tenant");
    let has_project: bool = row.get("has_project");
    let has_public_permissive_select: bool = row.get("has_public_permissive_select");
    if !matches!(relkind.as_str(), "r" | "p") || !has_tenant || !has_project {
        return Err(vault_db_credentials_config_status(format!(
            "vault database credential relation '{}.{}' is not a table with configured tenant/project columns",
            relation.schema, relation.table
        )));
    }
    if !has_public_permissive_select {
        return Err(vault_db_credentials_config_status(format!(
            "vault database credential relation '{}.{}' has no PUBLIC permissive SELECT policy for the restrictive binding to narrow",
            relation.schema, relation.table
        )));
    }
    Ok(())
}

async fn audit_created_login(conn: &mut PgConnection, username: &str) -> Result<(), Status> {
    let safe: bool = sqlx::query_scalar(
        "SELECT r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolinherit \
                AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication \
                AND NOT r.rolbypassrls \
                AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid) \
         FROM pg_roles r WHERE r.rolname = $1",
    )
    .bind(username)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        vault_db_role_creation_status(format!(
            "vault database credential generated-role audit failed: {err}"
        ))
    })?
    .unwrap_or(false);
    if !safe {
        return Err(vault_db_credentials_config_status(
            "generated database login did not retain the required NOINHERIT/NOBYPASSRLS/no-membership posture",
        ));
    }
    Ok(())
}

pub(crate) async fn drop_postgres_login_role(pool: &PgPool, username: &str) -> Result<(), String> {
    validate_pg_identifier_value(username, "generated username")?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| format!("begin generated database role cleanup failed: {err}"))?;
    let role_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
            .bind(username)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| format!("resolve generated database role {username} failed: {err}"))?;
    if role_exists {
        let policies: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT n.nspname, c.relname, p.polname \
             FROM pg_policy p \
             JOIN pg_class c ON c.oid = p.polrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE (SELECT oid FROM pg_roles WHERE rolname = $1) = ANY(p.polroles)",
        )
        .bind(username)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| format!("list generated database role policies failed: {err}"))?;
        for (schema, table, policy) in policies {
            for (value, label) in [
                (&schema, "policy schema"),
                (&table, "policy table"),
                (&policy, "policy name"),
            ] {
                validate_pg_identifier_value(value, label)?;
            }
            let sql = format!(
                "DROP POLICY IF EXISTS {} ON {}.{}",
                pg_ident(&policy),
                pg_ident(&schema),
                pg_ident(&table)
            );
            sqlx::query(&sql)
                .execute(&mut *tx)
                .await
                .map_err(|err| format!("drop generated database role policy failed: {err}"))?;
        }
        let drop_owned = format!("DROP OWNED BY {}", pg_ident(username));
        sqlx::query(&drop_owned)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("drop generated database role grants failed: {err}"))?;
        let drop_role = format!("DROP ROLE {}", pg_ident(username));
        sqlx::query(&drop_role)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("drop generated database role {username} failed: {err}"))?;
    }
    tx.commit()
        .await
        .map_err(|err| format!("commit generated database role cleanup failed: {err}"))
}

/// Fence an expiring/revoked login before dropping it. Password `VALID UNTIL`
/// prevents new authentication but does not end sessions that connected before
/// expiry, so the lifecycle worker must terminate those backends explicitly.
pub(crate) async fn terminate_postgres_login_sessions(
    pool: &PgPool,
    username: &str,
) -> Result<u64, String> {
    validate_pg_identifier_value(username, "generated username")?;
    let terminated = sqlx::query_scalar::<_, bool>(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE usename = $1 AND pid <> pg_backend_pid()",
    )
    .bind(username)
    .fetch_all(pool)
    .await
    .map_err(|err| {
        format!("terminate sessions for generated database role {username} failed: {err}")
    })?;
    if terminated.iter().any(|did_terminate| !did_terminate) {
        return Err(format!(
            "one or more sessions for generated database role {username} could not be terminated"
        ));
    }
    Ok(terminated.len() as u64)
}

pub(crate) async fn postgres_role_exists(pool: &PgPool, username: &str) -> Result<bool, String> {
    validate_pg_identifier_value(username, "generated username")?;
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
        .bind(username)
        .fetch_one(pool)
        .await
        .map_err(|err| format!("verify generated database role {username} absence failed: {err}"))
}
