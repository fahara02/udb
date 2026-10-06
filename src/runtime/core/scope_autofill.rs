//! Tenant and project scope filled in from the caller's verified context.
//!
//! A tenant-scoped table needs its tenant (and project) column in every filter
//! and record. The broker already knows both from the verified request context,
//! so making every caller repeat them only produced mistakes: a forgotten
//! tenant filter failed the read, a record without its tenant failed the write,
//! and a mistyped tenant (a code where the UUID is stored) silently matched
//! nothing. Here the broker supplies the values the caller left out, and
//! refuses a value that names a different tenant.
//!
//! Defense in depth stays in place: the planner still requires the columns and
//! still ANDs the verified scope predicate into every statement.

use serde_json::Value as JsonValue;

use crate::broker::RequestContext;
use crate::generation::ManifestTable;
use crate::generation::sql::{resolve_project_column_ref, resolve_tenant_column_ref};
use crate::runtime::error_reasons::{Refusal, TABLE_NOT_TENANT_SCOPED, TENANT_MISMATCH};

/// Whether a write filter may be widened with the scope keys. A Delete/Update
/// filter that names nothing but scope would, once filled, hit every row of
/// the tenant; it stays as the caller sent it (and the planner refuses it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterUse {
    Read,
    Write,
}

fn scope_columns<'t>(table: &'t ManifestTable, context: &RequestContext) -> Vec<(&'t str, String)> {
    let mut out = Vec::new();
    if let Some(tenant) = resolve_tenant_column_ref(table)
        && !context.tenant_id.trim().is_empty()
    {
        out.push((tenant.column_name.as_str(), context.tenant_id.clone()));
    }
    if let Some(project) = resolve_project_column_ref(table)
        && !context.project_id.trim().is_empty()
    {
        out.push((project.column_name.as_str(), context.project_id.clone()));
    }
    out
}

/// Every column a filter references, resolved to physical names, at any depth
/// (`$and` / `$or` / `$not` groups included).
fn referenced_columns(
    filter: &JsonValue,
    resolver: &std::collections::HashMap<String, String>,
    out: &mut Vec<String>,
) {
    match filter {
        JsonValue::Object(map) => {
            for (key, value) in map {
                if key.starts_with('$') {
                    referenced_columns(value, resolver, out);
                } else {
                    out.push(crate::planning::broker::resolve_column(resolver, key));
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                referenced_columns(item, resolver, out);
            }
        }
        _ => {}
    }
}

/// The scalar a top-level filter entry pins by equality: `"x"` or `{"$eq": "x"}`.
fn pinned_string(value: &JsonValue) -> Option<&str> {
    match value {
        JsonValue::String(text) => Some(text),
        JsonValue::Object(map) if map.len() == 1 => map
            .get("$eq")
            .or_else(|| map.get("eq"))
            .and_then(JsonValue::as_str),
        _ => None,
    }
}

fn tenant_mismatch(column: &str) -> tonic::Status {
    Refusal::new(
        TENANT_MISMATCH,
        format!(
            "the request names a different tenant in {column} than the caller's verified tenant"
        ),
    )
    .column(column)
    .into_status()
}

/// Fills the tenant/project equality into a filter that leaves them out, and
/// refuses one that pins a different tenant. Also refuses a tenant filter on a
/// table that has no tenant column (`UDB_TABLE_NOT_TENANT_SCOPED`) instead of
/// the generic unknown-field error.
pub(crate) fn autofill_filter(
    table: &ManifestTable,
    filter: &mut JsonValue,
    context: &RequestContext,
    usage: FilterUse,
) -> Result<(), tonic::Status> {
    let resolver = crate::planning::broker::column_resolver(table);
    if resolve_tenant_column_ref(table).is_none()
        && let JsonValue::Object(map) = &*filter
        && map.keys().any(|key| {
            key.eq_ignore_ascii_case("tenant_id")
                && !resolver.contains_key(&key.to_ascii_lowercase())
        })
    {
        return Err(Refusal::new(
            TABLE_NOT_TENANT_SCOPED,
            format!(
                "{}.{} has no tenant column, so it cannot be filtered by tenant_id",
                table.schema, table.table
            ),
        )
        .into_status());
    }
    let scope = scope_columns(table, context);
    if scope.is_empty() {
        return Ok(());
    }
    if filter.is_null() {
        *filter = JsonValue::Object(serde_json::Map::new());
    }
    let JsonValue::Object(map) = filter else {
        return Ok(());
    };
    // A pinned tenant that differs from the verified one used to match nothing
    // in silence (the verified predicate is ANDed in); name it instead.
    if let Some(tenant) = resolve_tenant_column_ref(table) {
        for (key, value) in map.iter() {
            if crate::planning::broker::resolve_column(&resolver, key) == tenant.column_name
                && let Some(pinned) = pinned_string(value)
                && pinned != context.tenant_id
            {
                return Err(tenant_mismatch(&tenant.column_name));
            }
        }
    }
    let mut referenced = Vec::new();
    referenced_columns(&JsonValue::Object(map.clone()), &resolver, &mut referenced);
    if usage == FilterUse::Write
        && referenced
            .iter()
            .all(|column| scope.iter().any(|(scope_column, _)| scope_column == column))
    {
        return Ok(());
    }
    for (column, value) in scope {
        if !referenced.iter().any(|seen| seen == column) {
            map.insert(column.to_string(), JsonValue::String(value));
        }
    }
    Ok(())
}

/// Fills the tenant/project into a record that leaves them out, and refuses a
/// record whose tenant differs from the verified one.
pub(crate) fn autofill_record(
    table: &ManifestTable,
    record: &mut JsonValue,
    context: &RequestContext,
) -> Result<(), tonic::Status> {
    let JsonValue::Object(map) = record else {
        return Ok(());
    };
    let resolver = crate::planning::broker::column_resolver(table);
    let tenant_column = resolve_tenant_column_ref(table).map(|column| column.column_name.clone());
    for (column, value) in scope_columns(table, context) {
        let present: Option<(String, JsonValue)> = map
            .iter()
            .find(|(key, _)| crate::planning::broker::resolve_column(&resolver, key) == column)
            .map(|(key, value)| (key.clone(), value.clone()));
        match present {
            None => {
                map.insert(column.to_string(), JsonValue::String(value));
            }
            Some((key, JsonValue::Null)) => {
                map.insert(key, JsonValue::String(value));
            }
            Some((_, JsonValue::String(given)))
                if tenant_column.as_deref() == Some(column) && given != value =>
            {
                return Err(tenant_mismatch(column));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{ManifestColumn, ManifestTableSecurity};
    use serde_json::json;

    fn table(with_tenant: bool, with_project: bool) -> ManifestTable {
        let column = |name: &str| ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            sql_type: "TEXT".to_string(),
            ..ManifestColumn::default()
        };
        let mut columns = vec![column("id"), column("status")];
        if with_tenant {
            columns.push(column("tenant_id"));
        }
        if with_project {
            columns.push(column("project_id"));
        }
        ManifestTable {
            schema: "app".to_string(),
            table: "notes".to_string(),
            primary_key: vec!["id".to_string()],
            columns,
            table_security: ManifestTableSecurity {
                tenant_column: if with_tenant {
                    "tenant_id".to_string()
                } else {
                    String::new()
                },
                project_column: if with_project {
                    "project_id".to_string()
                } else {
                    String::new()
                },
                ..ManifestTableSecurity::default()
            },
            ..ManifestTable::default()
        }
    }

    fn ctx() -> RequestContext {
        RequestContext {
            tenant_id: "t-1".to_string(),
            project_id: "p-1".to_string(),
            ..RequestContext::default()
        }
    }

    #[test]
    fn read_filters_gain_the_verified_scope() {
        let mut filter = json!({"status": "OPEN"});
        autofill_filter(&table(true, true), &mut filter, &ctx(), FilterUse::Read).unwrap();
        assert_eq!(
            filter,
            json!({"status": "OPEN", "tenant_id": "t-1", "project_id": "p-1"})
        );

        let mut empty = JsonValue::Null;
        autofill_filter(&table(true, false), &mut empty, &ctx(), FilterUse::Read).unwrap();
        assert_eq!(empty, json!({"tenant_id": "t-1"}));

        // Already scoped (even inside a group): left alone.
        let mut nested = json!({"$and": [{"tenant_id": {"$eq": "t-1"}}, {"id": "a"}]});
        autofill_filter(&table(true, false), &mut nested, &ctx(), FilterUse::Read).unwrap();
        assert_eq!(
            nested,
            json!({"$and": [{"tenant_id": {"$eq": "t-1"}}, {"id": "a"}]})
        );
    }

    #[test]
    fn a_scope_only_write_filter_is_never_widened_to_the_whole_tenant() {
        let mut empty = json!({});
        autofill_filter(&table(true, false), &mut empty, &ctx(), FilterUse::Write).unwrap();
        assert_eq!(empty, json!({}));
        let mut keyed = json!({"id": "a"});
        autofill_filter(&table(true, false), &mut keyed, &ctx(), FilterUse::Write).unwrap();
        assert_eq!(keyed, json!({"id": "a", "tenant_id": "t-1"}));
    }

    #[test]
    fn a_different_tenant_is_refused_by_name() {
        let mut filter = json!({"tenant_id": {"$eq": "acme"}, "id": "a"});
        let err =
            autofill_filter(&table(true, false), &mut filter, &ctx(), FilterUse::Read).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            crate::runtime::error_reasons::reason_of(&err).as_deref(),
            Some("UDB_TENANT_MISMATCH")
        );
        let mut record = json!({"id": "a", "tenant_id": "acme"});
        assert!(autofill_record(&table(true, false), &mut record, &ctx()).is_err());
    }

    #[test]
    fn records_gain_the_verified_scope() {
        let mut record = json!({"id": "a", "status": "OPEN"});
        autofill_record(&table(true, true), &mut record, &ctx()).unwrap();
        assert_eq!(
            record,
            json!({"id": "a", "status": "OPEN", "tenant_id": "t-1", "project_id": "p-1"})
        );
        let mut explicit_null = json!({"id": "a", "tenant_id": null});
        autofill_record(&table(true, false), &mut explicit_null, &ctx()).unwrap();
        assert_eq!(explicit_null, json!({"id": "a", "tenant_id": "t-1"}));
    }

    #[test]
    fn a_tenant_filter_on_an_unscoped_table_is_named() {
        let mut filter = json!({"tenant_id": "t-1", "id": "a"});
        let err = autofill_filter(&table(false, false), &mut filter, &ctx(), FilterUse::Read)
            .unwrap_err();
        assert_eq!(
            crate::runtime::error_reasons::reason_of(&err).as_deref(),
            Some("UDB_TABLE_NOT_TENANT_SCOPED")
        );
        let mut plain = json!({"id": "a"});
        autofill_filter(&table(false, false), &mut plain, &ctx(), FilterUse::Read).unwrap();
        assert_eq!(plain, json!({"id": "a"}));
    }
}
