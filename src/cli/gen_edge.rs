//! `udb gen edge <source table> -[REL{prop:type,…}]-> <target table>
//! [--package <proto package>] [--message <Name>]`: print the proto for an edge
//! table that UDB projects as a graph relationship. The edge row's foreign keys
//! name both endpoints (so the endpoint labels resolve from the catalog), the
//! graph store's label is the relationship type, and the row's other columns
//! become relationship properties.

/// A parsed edge spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeSpec {
    pub(crate) source: String,
    pub(crate) relationship: String,
    pub(crate) properties: Vec<(String, String)>,
    pub(crate) target: String,
}

fn identifier(text: &str, what: &str) -> Result<String, String> {
    let text = text.trim();
    if text.is_empty()
        || !text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || text.starts_with(|c: char| c.is_ascii_digit())
    {
        return Err(format!(
            "{what} '{text}' must be a plain identifier (letters, digits, _)"
        ));
    }
    Ok(text.to_string())
}

/// Parse `a -[REL{weight:double,kind:string}]-> b` (spaces optional).
pub(crate) fn parse_edge_spec(spec: &str) -> Result<EdgeSpec, String> {
    let spec: String = spec.split_whitespace().collect::<Vec<_>>().join("");
    let (source, rest) = spec
        .split_once("-[")
        .ok_or_else(|| "expected <source> -[REL]-> <target>".to_string())?;
    let (inner, target) = rest
        .split_once("]->")
        .ok_or_else(|| "expected ]-> before the target table".to_string())?;
    let (relationship, properties) = match inner.split_once('{') {
        Some((relationship, props)) => {
            let props = props
                .strip_suffix('}')
                .ok_or_else(|| "relationship properties must end with }".to_string())?;
            let mut parsed = Vec::new();
            for prop in props.split(',').filter(|p| !p.is_empty()) {
                let (name, ty) = prop.split_once(':').unwrap_or((prop, "string"));
                parsed.push((
                    identifier(name, "property")?,
                    ty.trim().to_ascii_lowercase(),
                ));
            }
            (relationship, parsed)
        }
        None => (inner, Vec::new()),
    };
    Ok(EdgeSpec {
        source: identifier(source, "source table")?,
        relationship: identifier(relationship, "relationship type")?,
        properties,
        target: identifier(target, "target table")?,
    })
}

fn pascal(text: &str) -> String {
    text.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => {
                    format!(
                        "{}{}",
                        first.to_ascii_uppercase(),
                        chars.as_str().to_ascii_lowercase()
                    )
                }
                None => String::new(),
            }
        })
        .collect()
}

fn property_type(ty: &str) -> Result<(&'static str, &'static str), String> {
    Ok(match ty {
        "string" | "text" => ("string", "TEXT"),
        "int" | "int64" | "bigint" => ("int64", "BIGINT"),
        "int32" | "integer" => ("int32", "INTEGER"),
        "double" | "float" | "float64" => ("double", "DOUBLE PRECISION"),
        "bool" | "boolean" => ("bool", "BOOLEAN"),
        "timestamp" | "time" => ("google.protobuf.Timestamp", "TIMESTAMPTZ"),
        other => {
            return Err(format!(
                "property type '{other}' is not supported; use string, int64, int32, double, bool or timestamp"
            ));
        }
    })
}

/// The proto message for the edge table.
pub(crate) fn render_edge_proto(
    spec: &EdgeSpec,
    package: &str,
    message: &str,
) -> Result<String, String> {
    let message = if message.trim().is_empty() {
        format!(
            "{}{}{}",
            pascal(&spec.source),
            pascal(&spec.relationship),
            pascal(&spec.target)
        )
    } else {
        identifier(message, "message name")?
    };
    let table = format!(
        "{}_{}_{}",
        spec.source.to_ascii_lowercase(),
        spec.relationship.to_ascii_lowercase(),
        spec.target.to_ascii_lowercase()
    );
    let (source_field, target_field) = if spec.source == spec.target {
        ("source_id".to_string(), "target_id".to_string())
    } else {
        (
            format!("{}_id", spec.source.to_ascii_lowercase()),
            format!("{}_id", spec.target.to_ascii_lowercase()),
        )
    };
    let column = |name: &str, number: usize, ty: &str, sql: &str, extra: &str| {
        format!(
            "  {ty} {name} = {number} [(udb.core.common.v1.pg_column) = {{ column_name: \"{name}\" sql_type: \"{sql}\"{extra} }}];\n"
        )
    };
    let endpoint = |name: &str, number: usize, references: &str| {
        column(
            name,
            number,
            "string",
            "UUID",
            &format!(
                " not_null: true foreign_key: {{ references_table: \"{references}\" references_column: \"id\" on_delete: REFERENTIAL_ACTION_CASCADE }}"
            ),
        )
    };
    let mut fields = String::new();
    fields.push_str(&column(
        "id",
        1,
        "string",
        "UUID",
        " primary_key: true not_null: true default_value: \"gen_random_uuid()\"",
    ));
    fields.push_str(&column(
        "tenant_id",
        2,
        "string",
        "UUID",
        " tenant_column: true not_null: true",
    ));
    fields.push_str(&endpoint(&source_field, 3, &spec.source));
    fields.push_str(&endpoint(&target_field, 4, &spec.target));
    let mut needs_timestamp = false;
    for (index, (name, ty)) in spec.properties.iter().enumerate() {
        let (proto_type, sql) = property_type(ty)?;
        needs_timestamp |= proto_type.starts_with("google.");
        fields.push_str(&column(name, 5 + index, proto_type, sql, ""));
    }
    let package_line = if package.trim().is_empty() {
        String::new()
    } else {
        format!("package {};\n\n", package.trim())
    };
    let timestamp_import = if needs_timestamp {
        "import \"google/protobuf/timestamp.proto\";\n"
    } else {
        ""
    };
    Ok(format!(
        "syntax = \"proto3\";\n\n{package_line}{timestamp_import}import \"udb/core/common/v1/db.proto\";\n\n\
         // {source} -[{rel}]-> {target}. Each row is one relationship; its other\n\
         // columns are the relationship's properties. Both endpoints must already\n\
         // be projected as nodes in the same tenant.\n\
         message {message} {{\n\
         \x20 option (udb.core.common.v1.pg_table) = {{ table_name: \"{table}\" }};\n\
         \x20 option (udb.core.common.v1.graph_store) = {{\n\
         \x20   backend: GRAPH_BACKEND_NEO4J\n\
         \x20   node_label: \"{rel}\"\n\
         \x20   tenant_field: \"tenant_id\"\n\
         \x20   edge_source_field: \"{source_field}\"\n\
         \x20   edge_target_field: \"{target_field}\"\n\
         \x20 }};\n\n{fields}}}\n",
        source = spec.source,
        rel = spec.relationship,
        target = spec.target,
    ))
}

#[cfg(test)]
mod gen_edge_tests {
    use super::{parse_edge_spec, render_edge_proto};

    #[test]
    fn parses_specs_with_and_without_properties() {
        let spec = parse_edge_spec("notes -[RELATED{weight:double, kind}]-> notes").unwrap();
        assert_eq!(spec.source, "notes");
        assert_eq!(spec.relationship, "RELATED");
        assert_eq!(spec.target, "notes");
        assert_eq!(
            spec.properties,
            vec![
                ("weight".into(), "double".into()),
                ("kind".into(), "string".into())
            ]
        );
        let bare = parse_edge_spec("users-[FOLLOWS]->users").unwrap();
        assert!(bare.properties.is_empty());
        assert!(parse_edge_spec("users FOLLOWS users").is_err());
        assert!(parse_edge_spec("users -[FOL LOWS]-> us-ers").is_err());
    }

    #[test]
    fn renders_an_edge_table_with_endpoint_keys_and_graph_store() {
        let spec = parse_edge_spec("authors -[WROTE{at:timestamp}]-> books").unwrap();
        let proto = render_edge_proto(&spec, "acme.library.v1", "").unwrap();
        assert!(proto.contains("message AuthorsWroteBooks {"), "{proto}");
        assert!(proto.contains("table_name: \"authors_wrote_books\""));
        assert!(proto.contains("edge_source_field: \"authors_id\""));
        assert!(proto.contains("edge_target_field: \"books_id\""));
        assert!(proto.contains("references_table: \"books\""));
        assert!(proto.contains("node_label: \"WROTE\""));
        assert!(proto.contains("google.protobuf.Timestamp at = 5"));
        assert!(proto.contains("import \"google/protobuf/timestamp.proto\";"));
        let self_edge = render_edge_proto(
            &parse_edge_spec("notes-[RELATED]->notes").unwrap(),
            "",
            "Link",
        )
        .unwrap();
        assert!(
            self_edge.contains("message Link {")
                && self_edge.contains("source_id")
                && self_edge.contains("target_id")
        );
        assert!(render_edge_proto(&parse_edge_spec("a-[R{x:blob}]->b").unwrap(), "", "").is_err());
    }
}
