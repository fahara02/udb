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

    if has_line_comment(first) {
        return None;
    }
    let mut parts = vec![first.trim().to_string()];
    let mut i = start + 1;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        // Collapsing a declaration that carries a `//` comment would pull every
        // following token - the annotation body and the closing `];` - onto the
        // comment's line, where the compiler treats them as commentary. The
        // result is a field with an unterminated `[`: not a reformatted proto, a
        // broken one. Layout is not worth that, so such a field is left alone.
        if has_line_comment(trimmed) {
            return None;
        }
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

/// True when `line` carries a `//` line comment OUTSIDE a string literal.
///
/// A literal may legitimately contain `//` - a URL default, a regex - and that
/// is not a comment, so scanning naively would refuse to format perfectly
/// collapsible fields. Block comments are not checked: `/* ... */` keeps its
/// terminator when lines are joined, so it survives the collapse intact.
fn has_line_comment(line: &str) -> bool {
    let bytes = line.as_bytes();
    let mut in_string = false;
    let mut escaped = false;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\"' {
                in_string = false;
            }
        } else if ch == '\"' {
            in_string = true;
        } else if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            return true;
        }
        i += 1;
    }
    false
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

/// Collapse a joined field declaration onto one line WITHOUT rewriting the
/// contents of string literals.
///
/// The previous form ran `split_whitespace()` and `replace(" ,", ",")` across
/// the whole joined line, so it rewrote annotation VALUES as well as layout:
/// `default_value: "\"'draft  review , pending'\""` came back as
/// `"\"'draft review, pending'\""` - a different SQL DEFAULT than the proto
/// declared. The same collapse silently altered CHECK expressions, regex
/// patterns and backfill SQL. `udb proto export --fmt` runs this over a
/// consumer's own tree, so the damage landed in their schema, not ours.
///
/// Formatting must never change what a declaration means, so whitespace
/// collapsing and punctuation tightening now apply only OUTSIDE quoted
/// strings. Layout behaviour outside literals is unchanged.
fn normalize_joined_parts(parts: &[String]) -> String {
    let joined = parts.join(" ");
    let mut out = String::with_capacity(joined.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut pending_space = false;
    for ch in joined.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\"' {
                in_string = false;
            }
            continue;
        }
        if ch.is_whitespace() {
            pending_space = !out.is_empty();
            continue;
        }
        if pending_space {
            // Mirrors the old `[ ` -> `[`, ` ];` -> `];`, ` ,` -> `,` tightening.
            let tighten = matches!(ch, ',' | ']') || out.ends_with('[');
            if !tighten {
                out.push(' ');
            }
            pending_space = false;
        }
        if ch == '\"' {
            in_string = true;
        }
        out.push(ch);
    }
    out
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

    // A formatter must not change what a declaration MEANS. Whitespace and
    // commas inside an annotation string are data: a SQL DEFAULT, a CHECK
    // expression, a regex. Collapsing them rewrote the schema silently, and
    // `udb proto export --fmt` did it to a consumer's own tree.
    #[test]
    fn preserves_whitespace_and_commas_inside_string_literals() {
        let input = r#"message FormatterRepro {
  string state = 1 [
    (udb.core.common.v1.pg_column) = {
      column_name: \"state\"
      sql_type: \"TEXT\"
      default_value: \"'draft  review , pending'\"
    }
  ];
}
"#;
        let output = format_proto_source(input);
        assert!(
            output.contains(r#"default_value: \"'draft  review , pending'\""#),
            "the literal must survive verbatim, got: {output}"
        );
        // The layout collapse still happened around it.
        assert!(
            output.contains("  string state = 1 [(udb.core.common.v1.pg_column) = {"),
            "the field must still collapse onto one line, got: {output}"
        );
    }

    // A literal containing the very tokens the tightening looks for must not
    // be edited either.
    #[test]
    fn punctuation_inside_a_literal_is_not_tightened() {
        let input = r#"message T {
  string s = 1 [
    (udb.core.common.v1.pg_column) = {
      check_expression: \"s IN ( 'a' , 'b' )\"
    }
  ];
}
"#;
        let output = format_proto_source(input);
        assert!(
            output.contains(r#"check_expression: \"s IN ( 'a' , 'b' )\""#),
            "punctuation inside a literal must be untouched, got: {output}"
        );
    }
    // Collapsing a field whose option list carries a `//` comment would put the
    // annotation and the closing `];` INSIDE that comment, leaving an
    // unterminated `[`. The file would no longer parse, so the field is left
    // multiline instead.
    #[test]
    fn a_field_with_a_line_comment_is_left_untouched() {
        let input = r#"message T {
  string s = 1 [
    // the broker fills this when absent
    (udb.core.common.v1.pg_column) = {
      column_name: \"s\"
    }
  ];
}
"#;
        assert_eq!(
            format_proto_source(input),
            input,
            "a commented field must be left alone, never collapsed into the comment"
        );
    }

    // `//` inside a string literal is data, not a comment, so the field still
    // collapses - refusing here would leave ordinary declarations unformatted.
    #[test]
    fn double_slash_inside_a_literal_still_collapses() {
        let input = r#"message T {
  string s = 1 [
    (udb.core.common.v1.pg_column) = {
      default_value: \"'https://example.test/a'\"
    }
  ];
}
"#;
        let output = format_proto_source(input);
        assert!(
            output.contains("  string s = 1 [(udb.core.common.v1.pg_column) = {"),
            "a URL literal must not block the collapse, got: {output}"
        );
        assert!(
            output.contains(r#"'https://example.test/a'"#),
            "the URL must survive verbatim, got: {output}"
        );
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
