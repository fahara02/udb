use super::{
    ParserConfig, db_parser::ProtoParser, infer_sql_type, lexer::Lexer, to_plural, to_snake_case,
};
use crate::ast::ProtoSchema;

/// Test helper: parse a proto source string into the same `Vec<ProtoSchema>`
/// the public `parse_file_report` returns, without needing a temp file.
fn parse_str(source: &str) -> Vec<ProtoSchema> {
    let cfg = ParserConfig::default();
    let tokens = Lexer::new(source.as_bytes(), "<test>".to_string())
        .tokenize()
        .expect("lex test source");
    ProtoParser::new(tokens, "<test>".to_string(), &cfg)
        .parse_report()
        .expect("parse test source")
        .schemas
}

#[test]
fn to_snake_case_converts_pascal_and_camel() {
    assert_eq!(to_snake_case("UserId"), "user_id");
    assert_eq!(to_snake_case("TOTPEnabled"), "totp_enabled");
    assert_eq!(to_snake_case("fullName"), "full_name");
}

#[test]
fn to_plural_inflects_common_endings() {
    assert_eq!(to_plural("user"), "users");
    assert_eq!(to_plural("company"), "companies");
    assert_eq!(to_plural("status"), "status");
}

#[test]
fn infer_sql_type_maps_proto_primitives() {
    assert_eq!(infer_sql_type("string"), "TEXT");
    assert_eq!(infer_sql_type("google.protobuf.Timestamp"), "TIMESTAMPTZ");
    assert_eq!(infer_sql_type("map"), "JSONB");
}

// ── NW-universal: language file-option parsing ─────────────────────────────
//
// Pre-NW-universal the parser only recognised `php_namespace`,
// `php_class_prefix`, `php_metadata_namespace`. Every other protoc
// language option (Java, C#, Go, Ruby, etc.) was silently dropped.
// These tests pin the new universal-options contract.

const MULTI_LANG_PROTO: &str = r#"
package acme.billing.v1;

option php_namespace = "Acme\\Billing\\V1";
option java_package = "com.acme.billing.v1";
option csharp_namespace = "Acme.Billing.V1";
option go_package = "github.com/acme/billing/v1;billingv1";
option ruby_package = "Acme::Billing::V1";
option swift_prefix = "AB";
option objc_class_prefix = "ACME";
option scala_package = "com.acme.billing.v1";
option rust_namespace = "acme::billing::v1";
option ts_namespace = "acme.billing.v1";

message Customer {
  option (table) = {
    table_name: "customers"
    schema_name: "billing"
    migration_order: 1
  };
  string id = 1;
}
"#;

#[test]
fn parser_captures_all_known_language_options() {
    let schemas = parse_str(MULTI_LANG_PROTO);
    assert_eq!(schemas.len(), 1);
    let s = &schemas[0];
    // Legacy PHP fields stay populated (back-compat for build.rs);
    // the lexer unescapes `\\` → `\` and strips surrounding quotes.
    assert_eq!(s.php_namespace, "Acme\\Billing\\V1");
    // Universal map carries every recognised language option, also
    // with quotes stripped and escapes resolved.
    assert_eq!(
        s.language_option("java_package").unwrap(),
        "com.acme.billing.v1"
    );
    assert_eq!(
        s.language_option("csharp_namespace").unwrap(),
        "Acme.Billing.V1"
    );
    assert_eq!(
        s.language_option("go_package").unwrap(),
        "github.com/acme/billing/v1;billingv1"
    );
    assert_eq!(
        s.language_option("ruby_package").unwrap(),
        "Acme::Billing::V1"
    );
    assert_eq!(s.language_option("swift_prefix").unwrap(), "AB");
    assert_eq!(s.language_option("objc_class_prefix").unwrap(), "ACME");
    assert_eq!(
        s.language_option("scala_package").unwrap(),
        "com.acme.billing.v1"
    );
    assert_eq!(
        s.language_option("rust_namespace").unwrap(),
        "acme::billing::v1"
    );
    assert_eq!(
        s.language_option("ts_namespace").unwrap(),
        "acme.billing.v1"
    );
}

#[test]
fn parser_ignores_unknown_options_silently() {
    // An option that isn't in KNOWN_LANGUAGE_FILE_OPTIONS shouldn't
    // land in the universal map (lint warning is the orthogonal path).
    let src = r#"
package x;
option some_random_option = "value";
message M {
  option (table) = { table_name: "m" schema_name: "x" migration_order: 1 };
  string id = 1;
}
"#;
    let schemas = parse_str(src);
    assert!(
        !schemas[0]
            .language_options
            .contains_key("some_random_option")
    );
}

#[test]
fn namespace_for_handles_canonical_language_aliases() {
    let schemas = parse_str(MULTI_LANG_PROTO);
    let s = &schemas[0];
    // "kotlin" reuses java_package (Kotlin shares Java's namespace
    // option per the protoc spec).
    assert!(s.namespace_for("kotlin").is_some());
    assert_eq!(s.namespace_for("java"), s.namespace_for("kotlin"));
    // ".net" / "dotnet" / "cs" all alias csharp.
    assert_eq!(s.namespace_for("csharp"), s.namespace_for("dotnet"));
    assert_eq!(s.namespace_for("csharp"), s.namespace_for(".net"));
    // python falls back to the proto package.
    assert_eq!(s.namespace_for("python"), Some("acme.billing.v1"));
}

#[test]
fn fully_qualified_name_applies_per_language_separator() {
    let schemas = parse_str(MULTI_LANG_PROTO);
    let s = &schemas[0];
    // Java uses `.` joining package + message.
    assert_eq!(
        s.fully_qualified_name("java", "Customer"),
        "com.acme.billing.v1.Customer"
    );
    // C# uses `.` joining.
    assert_eq!(
        s.fully_qualified_name("csharp", "Customer"),
        "Acme.Billing.V1.Customer"
    );
    // Ruby uses `::` joining.
    assert_eq!(
        s.fully_qualified_name("ruby", "Customer"),
        "Acme::Billing::V1::Customer"
    );
    // Rust uses `::` joining.
    assert_eq!(
        s.fully_qualified_name("rust", "Customer"),
        "acme::billing::v1::Customer"
    );
    // Objective-C uses no separator (prefix-as-class-prefix).
    assert_eq!(s.fully_qualified_name("objc", "Customer"), "ACMECustomer");
    // Swift uses no separator (prefix-as-class-prefix).
    assert_eq!(s.fully_qualified_name("swift", "Customer"), "ABCustomer");
}

#[test]
fn declared_languages_lists_every_recognised_language() {
    let schemas = parse_str(MULTI_LANG_PROTO);
    let s = &schemas[0];
    let langs = s.declared_languages();
    // Order is determined by BTreeMap key iteration; assert presence,
    // not order.
    for expected in [
        "csharp", "go", "java", "objc", "php", "ruby", "rust", "scala", "swift", "ts",
    ] {
        assert!(langs.contains(&expected), "missing language: {expected}");
    }
}

// ── NW-universal: `reserved` field number / name parsing ──────────────────
//
// Pre-NW-universal the db_parser skipped `reserved` blocks entirely,
// so the manifest had no way to refuse a NEW field that happened to
// reuse a number or name marked reserved in the old proto. protoc's
// compile-time `reserved` check only fires within the SAME .proto
// file at compile time — UDB's diff path needs it across manifest
// versions.

#[test]
fn parser_captures_reserved_field_numbers_and_names() {
    let src = r#"
package acme.billing.v1;
message Customer {
  option (table) = { table_name: "customers" schema_name: "billing" migration_order: 1 };
  reserved 3, 5 to 10, 99 to max;
  reserved "old_email", "deprecated_phone";
  string id = 1;
}
"#;
    let schemas = parse_str(src);
    let s = &schemas[0];

    // Single numbers + ranges + to-max all land as ranges.
    assert_eq!(s.reserved_numbers.len(), 3);
    assert_eq!(s.reserved_numbers[0].start, 3);
    assert_eq!(s.reserved_numbers[0].end, 3);
    assert_eq!(s.reserved_numbers[1].start, 5);
    assert_eq!(s.reserved_numbers[1].end, 10);
    assert_eq!(s.reserved_numbers[2].start, 99);
    assert_eq!(s.reserved_numbers[2].end, i32::MAX);

    // Quoted reserved names with quotes stripped.
    assert_eq!(s.reserved_names, vec!["old_email", "deprecated_phone"]);
}

#[test]
fn parser_handles_proto_without_reserved() {
    // Regression guard: schemas without `reserved` blocks still
    // parse cleanly.
    let src = r#"
package x;
message M {
  option (table) = { table_name: "m" schema_name: "x" migration_order: 1 };
  string id = 1;
}
"#;
    let schemas = parse_str(src);
    assert!(schemas[0].reserved_numbers.is_empty());
    assert!(schemas[0].reserved_names.is_empty());
}

#[test]
fn schema_without_language_options_returns_none_namespace() {
    let src = r#"
package x;
message M {
  option (table) = { table_name: "m" schema_name: "x" migration_order: 1 };
  string id = 1;
}
"#;
    let schemas = parse_str(src);
    let s = &schemas[0];
    assert_eq!(s.namespace_for("java"), None);
    assert_eq!(s.namespace_for("csharp"), None);
    // python still falls back to proto_package.
    assert_eq!(s.namespace_for("python"), Some("x"));
    // FQN without namespace == bare message name.
    assert_eq!(s.fully_qualified_name("java", "M"), "M");
    assert_eq!(s.fully_qualified_name("objc", "M"), "M");
}
