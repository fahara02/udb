use std::collections::{BTreeMap, HashMap};

use super::lexer::{Token, TokenKind};
use crate::ast::{
    KNOWN_LANGUAGE_FILE_OPTIONS, ProtoColumn, ProtoNestedEnum, ProtoNestedEnumValue, ProtoSchema,
};

use super::naming::{infer_sql_type, to_plural, to_snake_case};
use super::options::{
    OptionKind, OptionValue, apply_column_security_scalar_option, apply_column_security_values,
    apply_column_value, apply_column_values, apply_db_column_security_values,
    apply_schema_security_values, apply_table_security_values, apply_table_values,
    cache_from_values, column_store_from_values, document_store_from_values,
    foreign_key_from_reference, generic_store_from_values, graph_from_values,
    is_numeric_annotation_key, model_registry_from_values, option_kind, storage_from_values,
    timeseries_from_values, vector_from_values,
};
use super::paths::{resolve_migration_dir, resolve_schema_from_path};
use super::structure::{matches_nested_block, matches_top_level_block};
use super::{
    AnnotationParserMode, ParseError, ParseReport, ParserConfig, ParserDiagnostic,
    UDB_ANNOTATION_VERSION,
};

pub(super) struct ProtoParser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    file: String,
    config: &'a ParserConfig,
    diagnostics: Vec<ParserDiagnostic>,
    annotation_version_seen: bool,
    proto_package: String,
    php_namespace: String,
    php_class_prefix: String,
    php_metadata_namespace: String,
    /// NW-universal: every recognised protoc file-level language
    /// option (PHP / Java / C# / Go / Ruby / Swift / Objective-C /
    /// Scala / Rust / TS / Dart) lands here. Pre-fix the parser
    /// silently dropped everything except the three PHP options
    /// above, which made non-PHP codegen impossible. The legacy
    /// `php_*` fields stay populated for back-compat.
    language_options: BTreeMap<String, String>,
}

impl<'a> ProtoParser<'a> {
    pub(super) fn new(tokens: Vec<Token>, file: String, config: &'a ParserConfig) -> Self {
        Self {
            tokens,
            pos: 0,
            file,
            config,
            diagnostics: Vec::new(),
            annotation_version_seen: false,
            proto_package: String::new(),
            php_namespace: String::new(),
            php_class_prefix: String::new(),
            php_metadata_namespace: String::new(),
            language_options: BTreeMap::new(),
        }
    }

    pub(super) fn parse_report(&mut self) -> Result<ParseReport, ParseError> {
        let schemas = self.parse_schemas()?;
        if self.config.annotation_mode == AnnotationParserMode::Strict
            && let Some(diagnostic) = self
                .diagnostics
                .iter()
                .find(|diagnostic| is_strict_parse_failure_diagnostic(diagnostic))
        {
            return Err(ParseError::Syntax {
                file: diagnostic.file.clone(),
                line: diagnostic.line,
                column: diagnostic.column,
                message: format!("[{}]: {}", diagnostic.code, diagnostic.message),
            });
        }
        Ok(ParseReport {
            schemas,
            diagnostics: std::mem::take(&mut self.diagnostics),
        })
    }

    fn parse_schemas(&mut self) -> Result<Vec<ProtoSchema>, ParseError> {
        let mut schemas = Vec::new();
        while self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("message") {
                self.consume();
                let Some(message_name) = self.consume_ident() else {
                    self.skip_to_statement_end();
                    continue;
                };
                if let Some(mut schema) = self.parse_message(&message_name)? {
                    self.lint_schema_conventions(&schema);
                    schema.schema_name =
                        resolve_schema_from_path(&schema.declared_schema_name, &self.file);
                    schema.migration_dir = resolve_migration_dir(&self.file, &schema.schema_name);
                    schema.file = self.file.clone();
                    if schema.table_name.trim().is_empty() {
                        schema.table_name = to_plural(&to_snake_case(&schema.message_name));
                    } else {
                        schema.table_name = to_snake_case(&schema.table_name);
                    }
                    schemas.push(schema);
                }
                continue;
            }

            if self.cur().is_ident("option") {
                self.consume();
                self.parse_file_option();
                continue;
            }

            if self.cur().is_ident("package") {
                self.consume();
                self.proto_package = self.read_type_name();
                if self.cur().kind == TokenKind::Semicolon {
                    self.consume();
                }
                continue;
            }

            if self.cur().is_ident("extend") {
                self.consume();
                self.read_type_name();
                self.skip_block();
            } else if matches_top_level_block(self.cur().value.as_str()) {
                self.consume();
                self.consume_ident();
                self.skip_block();
            } else {
                self.skip_to_statement_end();
            }
        }
        self.lint_missing_annotation_version();
        Ok(schemas)
    }

    fn parse_file_option(&mut self) {
        let span = self.cur().clone();
        let option_name = self.read_option_name();
        let has_equal = self.cur().kind == TokenKind::Equal;
        if has_equal {
            self.consume();
        }
        let value = self.read_option_value();
        if is_annotation_version_option(&option_name) {
            self.annotation_version_seen = true;
            let expected = self.config.expected_annotation_version.trim();
            if !expected.is_empty() && value.trim_matches('"') != expected {
                self.diagnostic_at(
                    &span,
                    "udb_annotation_version_unsupported",
                    &format!(
                        "UDB annotation version `{}` is not supported by this parser; expected `{}`",
                        value.trim_matches('"'),
                        expected
                    ),
                );
            }
        } else if self.should_lint_contract() && is_udb_option_name(&option_name) {
            self.diagnostic_at(
                &span,
                "unknown_udb_annotation",
                &format!("unknown UDB file option `{option_name}`"),
            );
        }
        // Trimmed once so each branch consumes the same representation
        // — protoc strings come in with surrounding double quotes which
        // `read_option_value` already strips, but defensively trim
        // whitespace before storing.
        let trimmed_value = value.trim().to_string();
        match option_name.as_str() {
            // Legacy PHP-specific fields stay populated for back-compat
            // with downstream readers (build.rs etc.) that haven't been
            // migrated to consume `language_options` yet.
            "php_namespace" => self.php_namespace = trimmed_value.clone(),
            "php_class_prefix" => self.php_class_prefix = trimmed_value.clone(),
            "php_metadata_namespace" => self.php_metadata_namespace = trimmed_value.clone(),
            _ => {}
        }
        // NW-universal: route ANY recognised language option into the
        // catalog map so codegen for Java / C# / Go / Ruby / Swift /
        // Objective-C / Scala / Rust / TS / Dart can read its
        // namespace through `ProtoSchema::namespace_for(language)`.
        if KNOWN_LANGUAGE_FILE_OPTIONS.contains(&option_name.as_str()) {
            self.language_options
                .insert(option_name.clone(), trimmed_value);
        }
        if !has_equal && self.should_lint_contract() {
            self.diagnostic_at(
                &span,
                "udb_annotation_missing_equal",
                &format!("file option `{option_name}` is missing `=` before its value"),
            );
        }
        if self.cur().kind == TokenKind::Semicolon {
            self.consume();
        }
    }

    fn parse_message(&mut self, message_name: &str) -> Result<Option<ProtoSchema>, ParseError> {
        if self.cur().kind != TokenKind::LBrace {
            return Err(self.syntax("expected '{' after message name"));
        }
        self.consume();

        let mut schema = ProtoSchema::new(message_name);
        schema.proto_package = self.proto_package.clone();
        schema.php_namespace = self.php_namespace.clone();
        schema.php_class_prefix = self.php_class_prefix.clone();
        schema.php_metadata_namespace = self.php_metadata_namespace.clone();
        // NW-universal: hand the file's full language-options catalog
        // to every message inside the file. A message can override
        // these via per-message options, but the file-level set is the
        // baseline.
        schema.language_options = self.language_options.clone();
        while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("option") {
                self.consume();
                self.parse_message_option(&mut schema);
                continue;
            }
            if self.cur().is_ident("oneof") {
                self.consume();
                let oneof_group = self.consume_ident().unwrap_or_default();
                if self.cur().kind == TokenKind::LBrace {
                    self.consume();
                    while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof
                    {
                        if self.cur().kind == TokenKind::Ident
                            && let Some(column) = self.parse_field_in_oneof(&oneof_group)
                            && !column.column_name.is_empty()
                        {
                            schema.columns.push(column);
                        } else {
                            self.skip_to_statement_end();
                        }
                    }
                    if self.cur().kind == TokenKind::RBrace {
                        self.consume();
                    }
                } else {
                    self.skip_to_statement_end();
                }
                continue;
            }
            if self.cur().is_ident("enum") {
                self.consume();
                let enum_name = self.consume_ident().unwrap_or_default();
                if let Some(parsed) = self.parse_nested_enum_body(enum_name) {
                    schema.nested_enums.push(parsed);
                }
                continue;
            }
            if matches_nested_block(self.cur().value.as_str()) {
                // Nested message (and any other nested block) — skipped; UDB
                // models tables, not nested message types.
                self.consume();
                self.consume_ident();
                self.skip_block();
                continue;
            }
            if self.cur().value == "reserved" {
                // NW-universal: capture the reserved field numbers /
                // names into the schema so manifest diffs can refuse
                // a NEW field whose number/name collides with one
                // marked reserved by an earlier proto version.
                self.consume();
                self.parse_reserved_into(&mut schema);
                continue;
            }
            if self.cur().value == "extensions" {
                let token = self.cur().clone();
                self.diagnostic_at(
                    &token,
                    "unsupported_extensions",
                    "proto extensions are not represented by UDB's schema model and were ignored",
                );
                self.skip_to_statement_end();
                continue;
            }
            if self.cur().kind == TokenKind::Ident {
                if let Some(column) = self.parse_field()
                    && !column.column_name.is_empty()
                {
                    schema.columns.push(column);
                }
                continue;
            }
            self.consume();
        }

        if self.cur().kind == TokenKind::RBrace {
            self.consume();
        }
        if !schema.is_table && !schema_has_store_projection(&schema) {
            return Ok(None);
        }

        schema.columns.sort_by_key(|col| col.field_number);
        for column in &schema.columns {
            if let Some(fk) = &column.foreign_key {
                schema.foreign_keys.push(fk.clone());
            }
            for index in &column.indexes {
                schema.indexes.push(index.clone());
            }
        }
        Ok(Some(schema))
    }

    fn parse_message_option(&mut self, schema: &mut ProtoSchema) {
        let span = self.cur().clone();
        let option_name = self.read_option_name();
        if self.cur().kind == TokenKind::Equal {
            self.consume();
        }

        let namespace = &self.config.proto_namespace;
        let Some(kind) = option_kind(&option_name, namespace) else {
            if self.should_lint_contract() && is_udb_option_name(&option_name) {
                self.diagnostic_at(
                    &span,
                    "unknown_udb_annotation",
                    &format!("unknown UDB message option `{option_name}`"),
                );
            }
            self.skip_option_value();
            return;
        };
        self.lint_annotation_name(&span, &option_name);

        if option_name.trim().is_empty() {
            self.diagnostic_at(
                &span,
                "empty_db_option_name",
                "DB option is missing an option name",
            );
        }
        if self.prev().kind != TokenKind::Equal {
            self.diagnostic_at(
                &span,
                "db_option_missing_equal",
                &format!("DB option `{option_name}` is missing `=` before its value"),
            );
        }
        if self.cur().kind != TokenKind::LBrace {
            let token = self.cur().clone();
            self.diagnostic_at(
                &token,
                "db_option_expected_block",
                &format!("DB option `{option_name}` must use a `{{}}` block"),
            );
            self.skip_to_statement_end();
            return;
        }
        let values = self.parse_option_block(&option_name);
        match kind {
            OptionKind::Table => {
                schema.is_table = true;
                apply_table_values(schema, &values)
            }
            OptionKind::VectorStore => schema.vector_store = Some(vector_from_values(&values)),
            OptionKind::GraphStore => schema.graph_store = Some(graph_from_values(&values)),
            OptionKind::DocumentStore => {
                schema.document_store = Some(document_store_from_values(&values))
            }
            OptionKind::TimeSeriesStore => {
                schema.timeseries_store = Some(timeseries_from_values(&values))
            }
            OptionKind::Cache => schema.cache = Some(cache_from_values(&values)),
            OptionKind::Security => apply_schema_security_values(&mut schema.security, &values),
            OptionKind::TableSecurity => apply_table_security_values(schema, &values),
            OptionKind::ModelRegistry => {
                schema.model_registry = Some(model_registry_from_values(&values))
            }
            OptionKind::ColumnStore => {
                schema.column_store = Some(column_store_from_values(&values))
            }
            OptionKind::SqlStore => schema
                .generic_stores
                .push(generic_store_from_values("sql", &values)),
            OptionKind::NoSqlStore => schema
                .generic_stores
                .push(generic_store_from_values("nosql", &values)),
            OptionKind::GenericStore => schema
                .generic_stores
                .push(generic_store_from_values("generic", &values)),
            OptionKind::ObjectStore => schema
                .generic_stores
                .push(generic_store_from_values("object", &values)),
            _ => {}
        }
        if self.cur().kind == TokenKind::Semicolon {
            self.consume();
        }
    }

    fn parse_field(&mut self) -> Option<ProtoColumn> {
        self.parse_field_with_oneof("")
    }

    fn parse_field_in_oneof(&mut self, oneof_group: &str) -> Option<ProtoColumn> {
        self.parse_field_with_oneof(oneof_group)
    }

    fn parse_field_with_oneof(&mut self, oneof_group: &str) -> Option<ProtoColumn> {
        let mut is_array = false;
        if matches!(
            self.cur().value.as_str(),
            "optional" | "required" | "repeated"
        ) {
            is_array = self.cur().is_ident("repeated");
            self.consume();
        }

        let proto_type = self.read_type_name();
        if proto_type.is_empty() {
            self.skip_to_statement_end();
            return None;
        }

        let Some(field_name) = self.consume_ident() else {
            self.skip_to_statement_end();
            return None;
        };
        if self.cur().kind != TokenKind::Equal {
            self.skip_to_statement_end();
            return None;
        }
        self.consume();

        // proto3 requires every field to carry a positive field number. A
        // missing or unparseable number means malformed input — previously this
        // silently produced a column with field_number = 0 (an invalid slot that
        // also collides in the checksum sort). Emit a diagnostic and skip the
        // field, matching the other malformed-field branches above.
        let field_number = if self.cur().kind == TokenKind::Number {
            match self.cur().value.parse::<i32>() {
                Ok(value) => {
                    self.consume();
                    value
                }
                Err(_) => {
                    let tok = self.cur().clone();
                    self.diagnostic_at(
                        &tok,
                        "db_field_number_invalid",
                        &format!(
                            "field '{field_name}' has an unparseable field number '{}'; skipping",
                            tok.value
                        ),
                    );
                    self.skip_to_statement_end();
                    return None;
                }
            }
        } else {
            let tok = self.cur().clone();
            self.diagnostic_at(
                &tok,
                "db_field_number_missing",
                &format!("field '{field_name}' is missing its proto field number; skipping"),
            );
            self.skip_to_statement_end();
            return None;
        };
        if let Err(reason) = validate_proto_field_number(field_number) {
            let tok = self.cur().clone();
            self.diagnostic_at(
                &tok,
                "db_field_number_out_of_range",
                &format!("field '{field_name}' uses invalid proto field number {field_number}: {reason}; skipping"),
            );
            self.skip_to_statement_end();
            return None;
        }

        let mut column = ProtoColumn {
            field_name: field_name.clone(),
            column_name: to_snake_case(&field_name),
            proto_type: proto_type.clone(),
            sql_type: infer_sql_type(&proto_type),
            is_array,
            field_number,
            oneof_group: oneof_group.to_string(),
            ..ProtoColumn::default()
        };
        if let Some((key, value)) = parse_map_kv(&proto_type) {
            if !is_valid_proto_map_key_type(&key) {
                let tok = self.cur().clone();
                self.diagnostic_at(
                    &tok,
                    "db_map_key_type_invalid",
                    &format!(
                        "field '{field_name}' uses invalid proto map key type '{key}'; map keys must be bool, string, or integral scalar types"
                    ),
                );
                self.skip_to_statement_end();
                return None;
            }
            column.map_key_type = key;
            column.map_value_type = value;
        }

        if self.cur().kind == TokenKind::LBracket {
            self.consume();
            self.parse_field_options(&mut column);
        }
        if column.foreign_key.is_none() && !column.references.trim().is_empty() {
            column.foreign_key = foreign_key_from_reference(&column);
        }
        if self.cur().kind == TokenKind::Semicolon {
            self.consume();
        }
        Some(column)
    }

    /// Parse a nested enum body `{ NAME = N [opts]; ... }` into its values.
    /// Enum-level `option`/`reserved` lines and per-value `[ ... ]` options are
    /// skipped. The enum name has already been consumed; `cur()` is at the `{`.
    fn parse_nested_enum_body(&mut self, name: String) -> Option<ProtoNestedEnum> {
        if self.cur().kind != TokenKind::LBrace {
            self.skip_to_statement_end();
            return None;
        }
        self.consume(); // {
        let mut values = Vec::new();
        while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("option") || self.cur().value == "reserved" {
                self.skip_to_statement_end();
                continue;
            }
            if self.cur().kind != TokenKind::Ident {
                self.skip_to_statement_end();
                continue;
            }
            let value_name = self.consume_ident().unwrap_or_default();
            if self.cur().kind != TokenKind::Equal {
                self.skip_to_statement_end();
                continue;
            }
            self.consume(); // =
            let number = if self.cur().kind == TokenKind::Number {
                let tok = self.cur().clone();
                let n = match tok.value.parse::<i32>() {
                    Ok(v) => v,
                    Err(_) => {
                        self.diagnostic_at(
                            &tok,
                            "db_enum_value_number_invalid",
                            &format!(
                                "enum value '{value_name}' has an unparseable number '{}'; defaulting to 0",
                                tok.value
                            ),
                        );
                        0
                    }
                };
                self.consume();
                n
            } else {
                0
            };
            // Skip optional per-value options `[ ... ]`.
            if self.cur().kind == TokenKind::LBracket {
                while self.cur().kind != TokenKind::RBracket && self.cur().kind != TokenKind::Eof {
                    self.consume();
                }
                if self.cur().kind == TokenKind::RBracket {
                    self.consume();
                }
            }
            if self.cur().kind == TokenKind::Semicolon {
                self.consume();
            }
            if !value_name.is_empty() {
                values.push(ProtoNestedEnumValue {
                    name: value_name,
                    number,
                });
            }
        }
        if self.cur().kind == TokenKind::RBrace {
            self.consume();
        }
        Some(ProtoNestedEnum { name, values })
    }

    fn parse_field_options(&mut self, column: &mut ProtoColumn) {
        while self.cur().kind != TokenKind::RBracket && self.cur().kind != TokenKind::Eof {
            if self.cur().kind == TokenKind::Comma {
                self.consume();
                continue;
            }
            let span = self.cur().clone();
            let option_name = self.read_option_name();
            let sub_field = if self.cur().kind == TokenKind::Dot {
                self.consume();
                self.consume_ident().unwrap_or_default()
            } else {
                String::new()
            };
            let has_equal = self.cur().kind == TokenKind::Equal;
            if has_equal {
                self.consume();
            }

            let namespace = &self.config.proto_namespace;
            let option_kind = option_kind(&option_name, namespace);
            if option_kind.is_some() {
                self.lint_annotation_name(&span, &option_name);
            } else if self.should_lint_contract() && is_udb_option_name(&option_name) {
                self.diagnostic_at(
                    &span,
                    "unknown_udb_annotation",
                    &format!("unknown UDB field option `{option_name}`"),
                );
            }
            if option_kind.is_some() && !has_equal {
                self.diagnostic_at(
                    &span,
                    "db_option_missing_equal",
                    &format!("DB field option `{option_name}` is missing `=` before its value"),
                );
            }
            if self.cur().kind == TokenKind::LBrace {
                let values = self.parse_option_block(&option_name);
                match option_kind {
                    Some(OptionKind::Column) => apply_column_values(column, &values),
                    Some(OptionKind::Storage) => {
                        column.storage = Some(storage_from_values(&values));
                    }
                    Some(OptionKind::Security) => {
                        apply_column_security_values(&mut column.security, &values);
                    }
                    Some(OptionKind::ColumnSecurity) => {
                        apply_db_column_security_values(&mut column.security, &values);
                    }
                    _ => {}
                }
            } else {
                let value_token = self.cur().clone();
                let value = self.read_option_value();
                if !sub_field.is_empty() {
                    self.validate_numeric_annotation_value(&sub_field, &value, &value_token);
                } else if matches!(option_kind, Some(OptionKind::FieldSecurityScalar)) {
                    let key = annotation_leaf_name(&option_name);
                    self.validate_numeric_annotation_value(&key, &value, &value_token);
                }
                match option_kind {
                    Some(OptionKind::Column) if !sub_field.is_empty() => {
                        apply_column_value(column, &sub_field, &value);
                    }
                    Some(OptionKind::FieldSecurityScalar) => {
                        apply_column_security_scalar_option(
                            &mut column.security,
                            &option_name,
                            &value,
                        );
                    }
                    Some(_) if sub_field.is_empty() => {
                        self.diagnostic_at(
                            &span,
                            "db_option_expected_block",
                            &format!("DB field option `{option_name}` must use a `{{}}` block"),
                        );
                    }
                    _ => {}
                }
            }

            if self.cur().kind == TokenKind::Comma {
                self.consume();
            }
        }
        if self.cur().kind == TokenKind::RBracket {
            self.consume();
        } else {
            let token = self.cur().clone();
            self.diagnostic_at(
                &token,
                "db_field_options_unclosed",
                &format!(
                    "field `{}` options are missing a closing `]`",
                    column.field_name
                ),
            );
        }
    }

    fn parse_option_block(&mut self, option_name: &str) -> HashMap<String, Vec<OptionValue>> {
        let mut values: HashMap<String, Vec<OptionValue>> = HashMap::new();
        if self.cur().kind != TokenKind::LBrace {
            return values;
        }
        let start = self.cur().clone();
        self.consume();

        while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
            if self.cur().kind == TokenKind::Comma || self.cur().kind == TokenKind::Semicolon {
                self.consume();
                continue;
            }
            let Some(key) = self.consume_ident() else {
                let token = self.cur().clone();
                self.diagnostic_at(
                    &token,
                    "db_option_key_expected",
                    &format!("DB option `{option_name}` has a malformed block entry"),
                );
                self.consume();
                continue;
            };
            if self.cur().kind == TokenKind::Colon {
                self.consume();
            } else {
                let token = self.cur().clone();
                self.diagnostic_at(
                    &token,
                    "db_option_colon_expected",
                    &format!("DB option `{option_name}.{key}` is missing `:` before its value"),
                );
            }
            let parsed_values = if self.cur().kind == TokenKind::LBrace {
                vec![OptionValue::Block(self.parse_option_block(option_name))]
            } else if self.cur().kind == TokenKind::LBracket {
                self.parse_option_list(option_name, &key)
            } else {
                let value_token = self.cur().clone();
                let value = self.read_option_value();
                self.validate_numeric_annotation_value(&key, &value, &value_token);
                vec![OptionValue::Scalar(value)]
            };
            values.entry(key).or_default().extend(parsed_values);
            if self.cur().kind == TokenKind::Comma {
                self.consume();
            }
        }

        if self.cur().kind == TokenKind::RBrace {
            self.consume();
        } else {
            self.diagnostic_at(
                &start,
                "db_option_block_unclosed",
                &format!("DB option `{option_name}` block is missing a closing `}}`"),
            );
        }
        values
    }

    fn parse_option_list(&mut self, option_name: &str, key: &str) -> Vec<OptionValue> {
        let mut values = Vec::new();
        let start = self.cur().clone();
        self.consume();

        while self.cur().kind != TokenKind::RBracket && self.cur().kind != TokenKind::Eof {
            if self.cur().kind == TokenKind::Comma {
                self.consume();
                continue;
            }

            let value = if self.cur().kind == TokenKind::LBrace {
                OptionValue::Block(self.parse_option_block(option_name))
            } else {
                let value_token = self.cur().clone();
                let value = self.read_option_value();
                self.validate_numeric_annotation_value(key, &value, &value_token);
                OptionValue::Scalar(value)
            };
            values.push(value);

            if self.cur().kind == TokenKind::Comma {
                self.consume();
            }
        }

        if self.cur().kind == TokenKind::RBracket {
            self.consume();
        } else {
            self.diagnostic_at(
                &start,
                "db_option_list_unclosed",
                &format!("DB option `{option_name}.{key}` list is missing a closing `]`"),
            );
        }

        values
    }

    fn validate_numeric_annotation_value(&mut self, key: &str, value: &str, token: &Token) {
        if !self.should_lint_contract() || !is_numeric_annotation_key(key) {
            return;
        }
        if value.trim().parse::<i32>().is_err() {
            self.diagnostic_at(
                token,
                "malformed_numeric_annotation_value",
                &format!("numeric annotation `{key}=\"{value}\"` must be a valid i32"),
            );
        }
    }

    fn read_option_name(&mut self) -> String {
        let mut out = String::new();
        let paren = self.cur().kind == TokenKind::LParen;
        if paren {
            out.push('(');
            self.consume();
        }
        while matches!(self.cur().kind, TokenKind::Ident | TokenKind::Dot) {
            out.push_str(&self.cur().value);
            self.consume();
        }
        if paren && self.cur().kind == TokenKind::RParen {
            self.consume();
            out.push(')');
        }
        out
    }

    fn read_type_name(&mut self) -> String {
        let Some(first) = self.consume_ident() else {
            return String::new();
        };
        let mut parts = vec![first];
        while self.cur().kind == TokenKind::Dot {
            self.consume();
            let Some(part) = self.consume_ident() else {
                break;
            };
            parts.push(part);
        }
        parts.join(".")
    }

    fn read_option_value(&mut self) -> String {
        match self.cur().kind {
            TokenKind::String => {
                let mut parts = Vec::new();
                while self.cur().kind == TokenKind::String {
                    parts.push(self.cur().value.clone());
                    self.consume();
                }
                parts.join("")
            }
            TokenKind::Number | TokenKind::Ident => {
                let value = self.cur().value.clone();
                self.consume();
                value
            }
            TokenKind::Minus => {
                self.consume();
                if self.cur().kind == TokenKind::Number {
                    let value = format!("-{}", self.cur().value);
                    self.consume();
                    value
                } else {
                    "-".to_string()
                }
            }
            TokenKind::LBrace => {
                self.skip_block();
                String::new()
            }
            _ => {
                self.consume();
                String::new()
            }
        }
    }

    fn skip_option_value(&mut self) {
        if self.cur().kind == TokenKind::LBrace {
            self.skip_block();
            if self.cur().kind == TokenKind::Semicolon {
                self.consume();
            }
        } else {
            self.skip_to_statement_end();
        }
    }

    fn skip_block(&mut self) {
        if self.cur().kind != TokenKind::LBrace {
            return;
        }
        self.consume();
        let mut depth = 1usize;
        while self.cur().kind != TokenKind::Eof && depth > 0 {
            match self.cur().kind {
                TokenKind::LBrace => depth += 1,
                TokenKind::RBrace => depth -= 1,
                _ => {}
            }
            self.consume();
        }
    }

    /// NW-universal: parse a `reserved` statement directly into the
    /// schema. Supports both forms:
    ///   `reserved 1, 2, 5 to 10, 12 to max;`  → field numbers
    ///   `reserved "old_name", "deprecated";`  → field names
    /// Single-line statements can mix both forms separated by commas.
    /// The trailing semicolon is consumed; the caller has already
    /// consumed the `reserved` keyword itself.
    fn parse_reserved_into(&mut self, schema: &mut ProtoSchema) {
        use crate::ast::ProtoReservedRange;
        while self.cur().kind != TokenKind::Semicolon && self.cur().kind != TokenKind::Eof {
            match self.cur().kind {
                TokenKind::Comma => {
                    self.consume();
                }
                TokenKind::String => {
                    // String literal → reserved field NAME. Strip the
                    // surrounding quotes; the lexer keeps them.
                    let raw = self.consume().value;
                    let name = raw.trim_matches('"').trim_matches('\'').to_string();
                    if !name.is_empty() {
                        schema.reserved_names.push(name);
                    }
                }
                TokenKind::Number => {
                    // Number literal → reserved field NUMBER. May be
                    // followed by `to N` (range) or `to max`.
                    let start_tok = self.consume();
                    // A reserved number that silently became 0 (i32 overflow /
                    // malformed literal) would leave the slot it was meant to
                    // protect unreserved, defeating the add_column reserved-reuse
                    // guard. Diagnose and skip rather than push a bogus range.
                    let start: i32 = match start_tok.value.parse() {
                        Ok(v) => v,
                        Err(_) => {
                            self.diagnostic_at(
                                &start_tok,
                                "db_reserved_number_invalid",
                                &format!(
                                    "reserved field number '{}' is not a valid i32; ignoring",
                                    start_tok.value
                                ),
                            );
                            continue;
                        }
                    };
                    let mut end = start;
                    if self.cur().is_ident("to") {
                        self.consume();
                        if self.cur().is_ident("max") {
                            self.consume();
                            end = i32::MAX;
                        } else if self.cur().kind == TokenKind::Number {
                            end = self.consume().value.parse().unwrap_or(start);
                        }
                    }
                    schema.reserved_numbers.push(ProtoReservedRange {
                        start,
                        end,
                        span: Default::default(),
                    });
                }
                _ => {
                    // Anything unexpected — consume to make forward
                    // progress so we don't loop infinitely on a
                    // malformed reserved statement.
                    self.consume();
                }
            }
        }
        if self.cur().kind == TokenKind::Semicolon {
            self.consume();
        }
    }

    fn skip_to_statement_end(&mut self) {
        while self.cur().kind != TokenKind::Eof {
            match self.cur().kind {
                TokenKind::Semicolon => {
                    self.consume();
                    return;
                }
                TokenKind::LBrace => {
                    self.skip_block();
                    return;
                }
                _ => {
                    self.consume();
                }
            }
        }
    }

    fn consume_ident(&mut self) -> Option<String> {
        if self.cur().kind == TokenKind::Ident {
            let value = self.cur().value.clone();
            self.consume();
            Some(value)
        } else {
            None
        }
    }

    fn consume(&mut self) -> Token {
        let token = self.cur().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        token
    }

    fn cur(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .unwrap_or_else(|| self.tokens.last().expect("lexer always emits an EOF token"))
    }

    fn prev(&self) -> &Token {
        self.tokens
            .get(self.pos.saturating_sub(1))
            .unwrap_or_else(|| {
                self.tokens
                    .first()
                    .expect("lexer always emits an EOF token")
            })
    }

    fn syntax(&self, message: &str) -> ParseError {
        ParseError::Syntax {
            file: self.file.clone(),
            line: self.cur().line,
            column: self.cur().column,
            message: message.to_string(),
        }
    }

    fn diagnostic_at(&mut self, token: &Token, code: &str, message: &str) {
        self.diagnostics.push(ParserDiagnostic {
            file: self.file.clone(),
            line: token.line,
            column: token.column,
            code: code.to_string(),
            message: message.to_string(),
        });
    }

    fn should_lint_contract(&self) -> bool {
        matches!(
            self.config.annotation_mode,
            AnnotationParserMode::Warn | AnnotationParserMode::Strict
        )
    }

    fn lint_annotation_name(&mut self, token: &Token, option_name: &str) {
        if !self.should_lint_contract() || is_canonical_udb_option_name(option_name) {
            return;
        }
        self.diagnostic_at(
            token,
            "legacy_udb_annotation_alias",
            &format!(
                "annotation `{option_name}` is a compatibility alias; use canonical `(udb.<name>)` annotations"
            ),
        );
    }

    fn lint_schema_conventions(&mut self, schema: &ProtoSchema) {
        if !self.should_lint_contract() || !schema.is_table {
            return;
        }
        let token = self.cur().clone();
        if schema.declared_schema_name.trim().is_empty() {
            self.diagnostic_at(
                &token,
                "project_schema_path_convention",
                &format!(
                    "message `{}` derives schema_name from its file path; set udb.table.schema_name for project-agnostic protos",
                    schema.message_name
                ),
            );
        }
        if schema.table_name.trim().is_empty() {
            self.diagnostic_at(
                &token,
                "project_table_naming_convention",
                &format!(
                    "message `{}` derives table_name from its message name; set udb.table.table_name for project-agnostic protos",
                    schema.message_name
                ),
            );
        }
    }

    fn lint_missing_annotation_version(&mut self) {
        if !self.should_lint_contract() || self.annotation_version_seen {
            return;
        }
        self.diagnostics.push(ParserDiagnostic {
            file: self.file.clone(),
            line: 1,
            column: 1,
            code: "udb_annotation_version_missing".to_string(),
            message: format!(
                "missing `option (udb.annotation_version) = \"{}\";`",
                UDB_ANNOTATION_VERSION
            ),
        });
    }
}

fn normalize_annotation_name(option_name: &str) -> String {
    option_name
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim_start_matches('.')
        .to_ascii_lowercase()
}

fn is_annotation_version_option(option_name: &str) -> bool {
    normalize_annotation_name(option_name) == "udb.annotation_version"
}

fn annotation_leaf_name(option_name: &str) -> String {
    let normalized = normalize_annotation_name(option_name);
    normalized
        .rsplit('.')
        .next()
        .unwrap_or(normalized.as_str())
        .to_string()
}

fn is_strict_parse_failure_diagnostic(diagnostic: &ParserDiagnostic) -> bool {
    diagnostic.code == "malformed_numeric_annotation_value"
}

/// Parse the key/value type names out of a `map<K, V>` proto type token (the
/// lexer captures it as a single normalized token, e.g. "map<string,int32>").
/// Returns `None` for non-map types. The value type may be dotted (e.g.
/// `foo.Bar`); proto disallows nested maps, so the first top-level comma
/// separates key from value.
fn parse_map_kv(proto_type: &str) -> Option<(String, String)> {
    let inner = proto_type.strip_prefix("map<")?.strip_suffix('>')?;
    let (key, value) = inner.split_once(',')?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return None;
    }
    Some((key.to_string(), value.to_string()))
}

fn validate_proto_field_number(value: i32) -> Result<(), &'static str> {
    if value <= 0 {
        return Err("field numbers must be positive");
    }
    if value > 536_870_911 {
        return Err("field numbers must not exceed 536870911");
    }
    if (19_000..=19_999).contains(&value) {
        return Err("field numbers 19000 through 19999 are reserved by protobuf");
    }
    Ok(())
}

fn is_valid_proto_map_key_type(value: &str) -> bool {
    matches!(
        value.trim(),
        "int32"
            | "int64"
            | "uint32"
            | "uint64"
            | "sint32"
            | "sint64"
            | "fixed32"
            | "fixed64"
            | "sfixed32"
            | "sfixed64"
            | "bool"
            | "string"
    )
}

fn is_canonical_udb_option_name(option_name: &str) -> bool {
    normalize_annotation_name(option_name).starts_with("udb.")
}

fn is_udb_option_name(option_name: &str) -> bool {
    let normalized = normalize_annotation_name(option_name);
    normalized.starts_with("udb.") || normalized.contains(".udb.")
}

fn schema_has_store_projection(schema: &ProtoSchema) -> bool {
    schema.vector_store.is_some()
        || schema.graph_store.is_some()
        || schema.document_store.is_some()
        || schema.timeseries_store.is_some()
        || schema.cache.is_some()
        || schema.model_registry.is_some()
        || schema.column_store.is_some()
        || !schema.generic_stores.is_empty()
        || schema.columns.iter().any(|column| column.storage.is_some())
}
