//! `udb proto fmt` — UDB-friendly proto source formatting.
//!
//! This intentionally is not a full replacement for `buf format`. Its job is
//! narrower: keep field declarations with long UDB annotation option lists on a
//! single physical line so exported protos stay readable like SQL DDL.

use std::path::{Path, PathBuf};

#[derive(Debug, Default)]
pub(crate) struct ProtoFmtReport {
    pub(crate) scanned: usize,
    pub(crate) changed: usize,
}

/// Format every `.proto` file under `root`.
///
/// When `check` is true, files are scanned but not written; the returned exit
/// code is 2 if any file would change.
pub(crate) fn run(root: &str, check: bool) -> i32 {
    let root = normalize_root(root);
    match format_tree(&root, check) {
        Ok(report) => {
            let root_display = root.to_string_lossy().replace('\\', "/");
            if check && report.changed > 0 {
                eprintln!(
                    "proto fmt: {} file(s) under `{root_display}` need formatting",
                    report.changed
                );
                2
            } else {
                println!(
                    "proto fmt: {} file(s) scanned, {} file(s) {}",
                    report.scanned,
                    report.changed,
                    if check { "would change" } else { "formatted" }
                );
                0
            }
        }
        Err(err) => {
            eprintln!("proto fmt: {err}");
            1
        }
    }
}

pub(crate) fn format_tree(root: &Path, check: bool) -> Result<ProtoFmtReport, String> {
    if !root.exists() {
        return Err(format!("root `{}` does not exist", root.display()));
    }
    let mut files = Vec::new();
    collect_proto_files(root, &mut files)?;
    files.sort();

    let mut report = ProtoFmtReport::default();
    for path in files {
        report.scanned += 1;
        let input = std::fs::read_to_string(&path)
            .map_err(|err| format!("read {}: {err}", path.display()))?;
        let formatted = format_proto_source(&input);
        if formatted != input {
            report.changed += 1;
            let display = path.to_string_lossy().replace('\\', "/");
            if check {
                println!("  [needs-format] {display}");
            } else {
                std::fs::write(&path, formatted)
                    .map_err(|err| format!("write {}: {err}", path.display()))?;
                println!("  [formatted] {display}");
            }
        }
    }
    Ok(report)
}

fn normalize_root(root: &str) -> PathBuf {
    let trimmed = root.trim();
    if trimmed.is_empty() {
        PathBuf::from("proto")
    } else {
        PathBuf::from(trimmed)
    }
}

fn collect_proto_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    if root.is_file() {
        if root.extension().and_then(|ext| ext.to_str()) == Some("proto") {
            out.push(root.to_path_buf());
        }
        return Ok(());
    }

    let entries =
        std::fs::read_dir(root).map_err(|err| format!("read dir {}: {err}", root.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("read dir {}: {err}", root.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|err| format!("stat {}: {err}", path.display()))?;
        if file_type.is_dir() {
            collect_proto_files(&path, out)?;
        } else if file_type.is_file()
            && path.extension().and_then(|ext| ext.to_str()) == Some("proto")
        {
            out.push(path);
        }
    }
    Ok(())
}

pub(crate) fn format_proto_source(input: &str) -> String {
    let line_ending = if input.contains("\r\n") { "\r\n" } else { "\n" };
    let had_trailing_newline = input.ends_with('\n');
    let normalized = input.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.lines().collect();
    let mut out = Vec::with_capacity(lines.len());
    let mut i = 0usize;

    while i < lines.len() {
        if let Some((line, next)) = collapse_field_declaration(&lines, i) {
            out.push(line);
            i = next;
        } else {
            out.push(lines[i].to_string());
            i += 1;
        }
    }

    let mut formatted = out.join(line_ending);
    if had_trailing_newline {
        formatted.push_str(line_ending);
    }
    formatted
}

fn collapse_field_declaration(lines: &[&str], start: usize) -> Option<(String, usize)> {
    let first = lines.get(start)?;
    if !starts_multiline_field(first) {
        return None;
    }

    let mut parts = vec![first.trim().to_string()];
    let mut i = start + 1;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        parts.push(trimmed.to_string());
        i += 1;
        if trimmed.ends_with("];") {
            let indent = leading_ws(first);
            return Some((format!("{indent}{}", normalize_joined_parts(&parts)), i));
        }
        if trimmed.ends_with(';') && !trimmed.ends_with("];") {
            return None;
        }
    }
    None
}

fn starts_multiline_field(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//")
        || trimmed.starts_with("option ")
        || trimmed.starts_with("rpc ")
        || trimmed.starts_with("reserved ")
        || trimmed.starts_with("extensions ")
    {
        return false;
    }
    trimmed.contains(" = ") && trimmed.ends_with('[') && !trimmed.contains("];")
}

fn leading_ws(value: &str) -> &str {
    let end = value.len() - value.trim_start().len();
    &value[..end]
}

fn normalize_joined_parts(parts: &[String]) -> String {
    let joined = parts.join(" ");
    joined
        .replace("[ ", "[")
        .replace(" ];", "];")
        .replace(" ,", ",")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::format_proto_source;

    #[test]
    fn collapses_multiline_field_options() {
        let input = r#"message User {
  string password_hash = 4 [
    (udb.core.common.v1.sensitive) = true,
    (udb.core.common.v1.db_column_security) = {
      secret_classification: SECRET_CLASSIFICATION_CREDENTIAL
      output_view: OUTPUT_VIEW_STORAGE_ONLY
    }
  ];
}
"#;
        let output = format_proto_source(input);
        assert!(output.contains("  string password_hash = 4 [(udb.core.common.v1.sensitive) = true, (udb.core.common.v1.db_column_security) = { secret_classification: SECRET_CLASSIFICATION_CREDENTIAL output_view: OUTPUT_VIEW_STORAGE_ONLY }];"));
    }

    #[test]
    fn leaves_message_options_multiline() {
        let input = r#"message User {
  option (udb.core.common.v1.pg_table) = {
    table_name: "users"
  };
}
"#;
        assert_eq!(format_proto_source(input), input);
    }
}
