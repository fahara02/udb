//! `udb-portable` — the WASM/edge-safe subset of the UDB stack (U28).
//!
//! ## What's in here
//!
//! - [`ast`] — `ProtoSchema`, `ProtoColumn`, and every other AST type
//!   that the catalog manifest is built from. Pure serde structs, no
//!   IO. Re-uses `udb/src/schema/ast.rs` via `#[path]` include so the
//!   server crate and the WASM crate emit byte-identical types.
//! - [`checksum`] — `schema_checksum(&[ProtoSchema])`. The exact
//!   function the server uses to compute manifest checksums; a browser
//!   client can pre-compute the hash and short-circuit a round-trip.
//! - [`parser`] — proto file lexer + parser. The public API takes
//!   **bytes** instead of paths so it works in environments without a
//!   filesystem. A WASM consumer fetches the `.proto` source via
//!   `window.fetch` / edge KV and passes the bytes to
//!   [`parser::parse_proto_source`] or
//!   [`parser::parse_ast_source`].
//!
//! ## What's deliberately NOT in here
//!
//! Anything that pulls a native dependency:
//!
//! - The runtime (`tokio`, `sqlx`, `rdkafka`, `mongodb-driver`,
//!   `aws-sdk-s3`, `redis`) — server-only.
//! - The gRPC server (`tonic`, `prost`) — server-only, plus the
//!   generated code from `tonic-build` pulls tokio.
//! - File-based parser APIs (`parse_directory`, `parse_file`,
//!   `parse_ast_file`) — they call `std::fs::read_dir` /
//!   `std::fs::read`, which work on `wasm32-wasi` but not on
//!   `wasm32-unknown-unknown` (browser). The source-based API is the
//!   portable surface.
//! - The catalog manager, projection engine, CDC engine, migration
//!   sync — all server orchestration.
//!
//! ## Build targets
//!
//! - `cargo build -p udb-portable` — native (works on linux/mac/win).
//! - `cargo build -p udb-portable --target wasm32-unknown-unknown` —
//!   browser-compatible. This is the U28 acceptance gate.
//! - `cargo build -p udb-portable --target wasm32-wasi` — edge runtime
//!   (Cloudflare Workers, Wasmtime, etc.).
//!
//! ## Why `#[path]` instead of duplication
//!
//! A copy would drift. A path-include keeps a single source of truth:
//! when the main crate updates `ProtoSchema` to add a column option,
//! the WASM crate picks it up on the next build with no manual sync.

#![warn(unused)]
#![warn(dead_code)]

/// Re-exposed AST types. The submodule's `crate::ast` paths are
/// satisfied by the `pub use schema::ast as ast` line below — every
/// source file the parser includes uses `crate::ast::…` and resolves
/// to the same set of types the main `udb` crate exposes.
pub mod schema {
    #[path = "../../../../src/schema/ast.rs"]
    pub mod ast;

    #[path = "../../../../src/schema/checksum.rs"]
    pub mod checksum;
}

// Mirror the main crate's `pub use ast::*` re-export so submodules
// importing `crate::ast::ProtoSchema` find the right type.
pub use schema::ast;
pub use schema::checksum;
pub use schema::checksum::schema_checksum;

/// U13 step 2 — SDK-side schema cache. Tracks
/// `(catalog_version, manifest_checksum)` headers across responses,
/// stores descriptors by qualified message name, and reports
/// negotiation outcomes so SDK clients know when to invalidate.
pub mod schema_cache;
pub use schema_cache::{CatalogCompatibility, Negotiation, SchemaCache};

pub mod parser {
    //! Proto file lexer + source-based parser. See crate docs for why
    //! we don't expose the path-based variants.

    // Submodules are pure: lexer is std::fmt only; the parser
    // submodules (ast_parser, db_parser, naming, options, paths,
    // selection, structure) reference each other via `super::` and
    // reference the AST via `crate::ast::…`, both of which work after
    // the `pub use schema::ast` re-export at the crate root.
    #[path = "../../../../src/parser/lexer.rs"]
    pub mod lexer;

    #[path = "../../../../src/parser/naming.rs"]
    mod naming;

    #[path = "../../../../src/parser/paths.rs"]
    mod paths;

    #[path = "../../../../src/parser/structure.rs"]
    mod structure;

    #[path = "../../../../src/parser/selection.rs"]
    mod selection;

    #[path = "../../../../src/parser/options.rs"]
    mod options;

    #[path = "../../../../src/parser/ast_parser.rs"]
    mod ast_parser;

    #[path = "../../../../src/parser/db_parser.rs"]
    mod db_parser;

    use std::fmt;
    use std::path::PathBuf;

    use crate::ast::{ProtoFileAst, ProtoSchema};
    use ast_parser::ProtoAstParser;
    use db_parser::ProtoParser;
    use lexer::{LexError, Lexer};
    use selection::dedupe_canonical_table_schemas;

    /// Pinned at the same value as the main `udb` crate so a WASM
    /// client and the server agree on the annotation contract version.
    pub const UDB_ANNOTATION_VERSION: &str = "1";

    /// Mirror of the main crate's `AnnotationParserMode`. The shared
    /// parser submodules read it via `super::AnnotationParserMode`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AnnotationParserMode {
        Compat,
        Warn,
        Strict,
    }

    #[derive(Debug, Clone)]
    pub struct ParserConfig {
        pub proto_namespace: String,
        pub annotation_mode: AnnotationParserMode,
        pub expected_annotation_version: String,
    }

    impl ParserConfig {
        pub fn new(proto_namespace: impl Into<String>) -> Self {
            Self {
                proto_namespace: proto_namespace.into(),
                annotation_mode: AnnotationParserMode::Compat,
                expected_annotation_version: UDB_ANNOTATION_VERSION.to_string(),
            }
        }

        pub fn with_annotation_mode(mut self, mode: AnnotationParserMode) -> Self {
            self.annotation_mode = mode;
            self
        }

        pub fn with_expected_annotation_version(mut self, version: impl Into<String>) -> Self {
            self.expected_annotation_version = version.into();
            self
        }
    }

    impl Default for ParserConfig {
        fn default() -> Self {
            Self::new("")
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ParserDiagnostic {
        pub file: String,
        pub line: usize,
        pub column: usize,
        pub code: String,
        pub message: String,
    }

    impl fmt::Display for ParserDiagnostic {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}:{}:{} [{}]: {}",
                self.file, self.line, self.column, self.code, self.message
            )
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub struct ParseReport {
        pub schemas: Vec<ProtoSchema>,
        pub diagnostics: Vec<ParserDiagnostic>,
    }

    impl ParseReport {
        pub fn passed(&self) -> bool {
            self.diagnostics.is_empty()
        }
    }

    /// Source-only variant of `ParseError`. Notably no `Directory` or
    /// `Io` variants — those exist on the main crate's `ParseError` to
    /// surface `std::fs::read_dir` failures, which the portable crate
    /// can't produce because it doesn't read the filesystem.
    #[derive(Debug)]
    pub enum ParseError {
        Lex(LexError),
        Syntax {
            file: String,
            line: usize,
            column: usize,
            message: String,
        },
        /// Retained so the source-based functions can still produce a
        /// path-shaped error if a caller passes a logical path string
        /// (e.g. a URL or virtual FS path).
        Io {
            path: PathBuf,
            source: std::io::Error,
        },
    }

    impl fmt::Display for ParseError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Lex(source) => write!(f, "{source}"),
                Self::Syntax {
                    file,
                    line,
                    column,
                    message,
                } => write!(f, "{file}:{line}:{column}: {message}"),
                Self::Io { path, source } => {
                    write!(f, "cannot read {}: {source}", path.display())
                }
            }
        }
    }

    impl std::error::Error for ParseError {}

    /// Parse a proto-DB schema from in-memory bytes. The portable
    /// equivalent of `udb::parser::parse_file`, minus the filesystem.
    pub fn parse_proto_source(
        source: &[u8],
        file: impl Into<String>,
        config: &ParserConfig,
    ) -> Result<ParseReport, ParseError> {
        let file = file.into();
        let tokens = Lexer::new(source, file.clone())
            .tokenize()
            .map_err(ParseError::Lex)?;
        ProtoParser::new(tokens, file, config).parse_report()
    }

    /// Same as [`parse_proto_source`] but drops the diagnostics for
    /// callers that only need the schemas. Mirrors
    /// `udb::parser::parse_file` (which also discards diagnostics).
    pub fn parse_proto_schemas(
        source: &[u8],
        file: impl Into<String>,
        config: &ParserConfig,
    ) -> Result<Vec<ProtoSchema>, ParseError> {
        parse_proto_source(source, file, config).map(|report| report.schemas)
    }

    /// Parse a proto file into the lossless `ProtoFileAst` (the AST
    /// the codegen pipeline consumes). Source-only — no fs.
    pub fn parse_ast_source(
        source: &[u8],
        file: impl Into<String>,
    ) -> Result<ProtoFileAst, ParseError> {
        let file = file.into();
        let tokens = Lexer::new(source, file.clone())
            .tokenize()
            .map_err(ParseError::Lex)?;
        ProtoAstParser::new(tokens, file).parse()
    }

    /// Deduplicate a `Vec<ProtoSchema>` the same way the main crate's
    /// directory-aware parser does. WASM clients that assemble schemas
    /// from multiple proto files can apply this to merge canonical
    /// duplicates before checksumming.
    pub fn dedupe_schemas(schemas: Vec<ProtoSchema>) -> Vec<ProtoSchema> {
        dedupe_canonical_table_schemas(schemas)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity: the AST round-trips through serde_json. Pins that the
    /// `#[path]`-included module compiles in the WASM-target dep
    /// graph (serde + serde_json only) and that no transitive include
    /// drags in a server-only type.
    #[test]
    fn proto_schema_round_trips_through_serde() {
        let schema = ast::ProtoSchema {
            message_name: "User".to_string(),
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            ..Default::default()
        };
        let json = serde_json::to_string(&schema).unwrap();
        let back: ast::ProtoSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back.message_name, "User");
        assert_eq!(back.table_name, "users");
    }

    /// Pin: deterministic checksum. The WASM client computes the same
    /// hash the server computes, so a browser can short-circuit an
    /// upload when its local manifest already matches.
    #[test]
    fn checksum_is_deterministic_across_field_order() {
        let mut schema_a = ast::ProtoSchema {
            message_name: "Order".to_string(),
            schema_name: "shop".to_string(),
            table_name: "orders".to_string(),
            ..Default::default()
        };
        schema_a.columns = vec![
            ast::ProtoColumn {
                field_name: "id".to_string(),
                column_name: "id".to_string(),
                field_number: 1,
                ..Default::default()
            },
            ast::ProtoColumn {
                field_name: "created_at".to_string(),
                column_name: "created_at".to_string(),
                field_number: 2,
                ..Default::default()
            },
        ];
        // Same schema with columns declared in reverse order — the
        // checksum impl sorts by field_number so the hash must match.
        let mut schema_b = schema_a.clone();
        schema_b.columns.reverse();

        let h_a = schema_checksum(&[schema_a]).unwrap();
        let h_b = schema_checksum(&[schema_b]).unwrap();
        assert_eq!(h_a, h_b, "checksum must be order-independent");
        assert_eq!(h_a.len(), 64, "sha256 hex is 64 chars");
    }

    /// Pin: source-based parser accepts a minimal proto and produces
    /// no diagnostics. This is the only public entry-point a browser
    /// client calls, so its smoke test is the WASM acceptance proxy.
    #[test]
    fn parse_proto_source_accepts_minimal_proto() {
        let source = br#"
            syntax = "proto3";
            package udb.test.v1;
            message Empty {}
        "#;
        let cfg = parser::ParserConfig::new("udb.test.v1");
        let report =
            parser::parse_proto_source(source, "test.proto", &cfg).expect("parse should succeed");
        // No schemas — the message has no DB annotation. Diagnostics
        // empty because it's a syntactically valid proto file.
        assert!(report.diagnostics.is_empty());
    }

    /// Pin: AST-only parse returns the lossless `ProtoFileAst`. This
    /// is what a generator-on-the-edge would consume.
    #[test]
    fn parse_ast_source_returns_file_ast() {
        let source = br#"
            syntax = "proto3";
            package udb.test.v1;
            message Foo { string name = 1; }
        "#;
        let ast_file =
            parser::parse_ast_source(source, "test.proto").expect("AST parse should succeed");
        let has_foo = ast_file
            .definitions
            .iter()
            .any(|def| matches!(def, ast::ProtoDefinition::Message(m) if m.name == "Foo"));
        assert!(has_foo, "AST must contain message Foo");
    }
}
