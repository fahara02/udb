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
//! - [`generation`] — build a `CatalogManifest` from parsed schemas and
//!   render bootstrap + delta **DDL** for any SQL backend
//!   ([`generation::sql::generate_bootstrap_sql`] /
//!   [`generation::sql::generate_delta_sql`]).
//! - [`migration`] — diff two manifests into `ChangeOperation`s with
//!   SafeAuto/RequiresReview/Blocked classification ([`migration::diff`],
//!   [`migration::diff_backends`]). (The full `build_migration_plan` —
//!   SQL/DSN/provisioning artifacts — stays server-side.)
//! - [`ir`] — **the IR → backend query compiler.** Lowers neutral
//!   `LogicalRead`/`Write`/`Delete`/`Search`/`Aggregate` operations into a
//!   `CompiledRendering` (SQL / JSON / key-value / object) string for all 18
//!   backends ([`ir::compile`]) — browser/edge query compilation with zero
//!   server round-trip, reusing the exact lowering the server runs.
//! - [`backend`] — the [`backend::BackendKind`] enum the compilers match on.
//! - [`descriptor`] — hand-decode the embedded `FileDescriptorSet` bytes into
//!   the full UDB contract (services/RPCs/messages, endpoint+field security,
//!   native-service surface, emitted events) via
//!   [`descriptor::descriptor_contract_manifest_from_bytes`].
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

/// Manifest generation, DDL rendering, and manifest diff — the portable
/// half of `udb`'s `generation` + `migration` subsystems. A WASM/edge
/// client can build a [`generation::manifest::CatalogManifest`] from
/// parsed `ProtoSchema`s, emit bootstrap + delta DDL for any SQL backend
/// (`generation::sql::generate_bootstrap_sql` / `generate_delta_sql`),
/// and diff two manifests into `ChangeOperation`s with
/// SafeAuto/RequiresReview/Blocked classification — entirely client-side.
///
/// Deliberately NOT included (server-only): `build_migration_plan`, DSN
/// catalogs, provisioning, `apply`/`sync`/`phase_runner`, and the
/// per-backend driver DDL in `generation::backends`. The included
/// modules' `crate::ast::*` paths resolve via the `pub use schema::ast`
/// re-export above; `generation::sql` and `migration::diff` have a mutual
/// dependency (the DDL renderer reads `ChangeOperation`; the differ reads
/// `generation::sql::validate_enum_value`) and are both included here.
pub mod generation {
    #[path = "../../../../src/generation/manifest/mod.rs"]
    pub mod manifest;

    #[path = "../../../../src/generation/manifest_index.rs"]
    pub mod manifest_index;

    #[path = "../../../../src/generation/sql/mod.rs"]
    pub mod sql;

    // Re-export at the `generation` level exactly like the server's
    // `generation/mod.rs`, so the IR compilers' `crate::generation::CatalogManifest`
    // / `ManifestTable` paths (and the manifest-build/diff consumers) resolve
    // identically in both crates.
    pub use manifest::{
        CatalogManifest, GENERATOR_VERSION, ManifestCheck, ManifestColumn, ManifestColumnSecurity,
        ManifestExtension, ManifestForeignKey, ManifestIndex, ManifestMaterializedView,
        ManifestMigrationReport, ManifestSchemaChecksum, ManifestStore, ManifestStoreOption,
        ManifestTable, ManifestTableSecurity, ManifestTrigger, migrate_manifest_to_current,
    };
    pub use sql::{
        GeneratedArtifact, SqlGenerationConfig, generate_bootstrap_sql, generate_delta_sql,
    };
}

pub mod migration {
    //! Portable schema-diff surface only (no apply/sync/ledger).
    #[path = "../../../../src/migration/diff.rs"]
    pub mod diff;

    #[path = "../../../../src/migration/diff_backends.rs"]
    pub mod diff_backends;
}

/// The backend-identity enum the IR→SQL compilers match on. Just the enum
/// (split into `src/backend/kind.rs`); the native `backend::plugin`/`plugins`
/// driver inventory is NOT included.
pub mod backend {
    #[path = "../../../../src/backend/kind.rs"]
    mod kind;
    pub use kind::BackendKind;
}

/// The single broker function the IR→SQL compilers call
/// (`SqlCompiler::resolve_table`), re-exported from the portable
/// `generation::manifest_index` leaf so `crate::broker::table_for_message`
/// resolves identically to the server.
pub mod broker {
    pub use crate::generation::manifest_index::table_for_message;
}

/// **The IR → backend query compiler — the big prize.** Lowers neutral
/// `LogicalRead`/`Write`/`Delete`/`Search`/`Aggregate` operations into a
/// `CompiledRendering` (SQL / JSON / key-value / object) string for any of the
/// 18 supported backends (every backend module enabled by the `all-compilers`
/// default feature). It is pure CPU work — a browser or edge worker can compile
/// a query to backend-native SQL/DSL with ZERO server round-trip, reusing the
/// exact lowering the server runs.
///
/// Excluded (not on the compile path): `ir::plan` (multi-backend async
/// execution — pulls tokio) and `ir::raw_dispatch` (runtime policy / env).
pub mod ir {
    #[path = "../../../../src/ir/cache_key.rs"]
    pub mod cache_key;
    #[path = "../../../../src/ir/compile/mod.rs"]
    pub mod compile;
    #[path = "../../../../src/ir/filter.rs"]
    pub mod filter;
    #[path = "../../../../src/ir/operations.rs"]
    pub mod operations;
    #[path = "../../../../src/ir/projection.rs"]
    pub mod projection;
    #[path = "../../../../src/ir/value.rs"]
    pub mod value;
}

/// Minimal shim so the `#[path]`-shared `descriptor_manifest.rs` resolves its one
/// `crate::runtime::native_catalog::embedded_file_descriptor_set()` reference on a
/// target that does not bundle the server's embedded descriptor. On wasm/edge the
/// caller supplies the descriptor bytes itself (browser fetch / embedded asset)
/// and uses [`descriptor::descriptor_contract_manifest_from_bytes`]; the no-arg
/// server-style accessor decodes empty here.
pub mod runtime {
    pub mod native_catalog {
        /// Stub: no embedded descriptor on the portable target — returns empty.
        pub fn embedded_file_descriptor_set() -> &'static [u8] {
            &[]
        }
    }
}

/// Descriptor / RPC-contract decode — hand-decodes the embedded
/// `FileDescriptorSet` bytes into the UDB contract manifest (services, RPCs,
/// messages, endpoint/field security, native-service surface, emitted events),
/// preserving the custom UDB option extensions that `prost_types` would drop. A
/// browser/edge client fetches `udb_descriptor.bin` and calls
/// [`descriptor::descriptor_contract_manifest_from_bytes`] to read the full
/// contract entirely client-side.
pub mod descriptor {
    #[path = "../../../../src/runtime/descriptor_manifest.rs"]
    mod inner;
    pub use inner::*;
}

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

    #[path = "../../../../src/parser/facade.rs"]
    mod facade;

    use crate::ast::{ProtoFileAst, ProtoSchema};
    use ast_parser::ProtoAstParser;
    use db_parser::ProtoParser;
    use lexer::Lexer;
    use selection::dedupe_canonical_table_schemas;

    pub use facade::{
        AnnotationParserMode, ParseError, ParseReport, ParserConfig, ParserDiagnostic,
        UDB_ANNOTATION_VERSION,
    };
    pub use options::{ParserOptionMetadata, documented_option_metadata, parser_option_kind_name};

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
