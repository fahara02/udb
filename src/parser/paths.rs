pub(super) fn resolve_schema_from_path(declared: &str, file_path: &str) -> String {
    let declared = declared.trim();
    if !declared.is_empty() {
        declared.to_lowercase()
    } else {
        let domain = extract_proto_domain(file_path);
        if domain.is_empty() {
            "public".to_string()
        } else {
            domain
        }
    }
}

pub(super) fn resolve_migration_dir(file_path: &str, schema_name: &str) -> String {
    if !schema_name.trim().is_empty() {
        schema_name.to_string()
    } else {
        let domain = extract_proto_domain(file_path);
        if domain.is_empty() {
            "public".to_string()
        } else {
            domain
        }
    }
}

pub(super) fn extract_proto_domain(file_path: &str) -> String {
    let normalized = file_path.replace('\\', "/");
    let parts: Vec<&str> = normalized.split('/').collect();
    for (idx, part) in parts.iter().enumerate() {
        if matches!(*part, "entity" | "services" | "events") && idx >= 2 {
            return parts[idx - 2].to_lowercase();
        }
    }
    String::new()
}
