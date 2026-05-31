# UDB Annotation Contract

UDB accepts project-owned proto packages. Projects keep their own package names,
message names, and language options; UDB reads annotations and turns them into a
catalog manifest.

## Contract

- UDB annotations are versioned separately from the broker runtime.
- The parser accepts annotation packages by suffix, so projects do not have to
  put their business protos under `udb.*`.
- `UDB_ANNOTATION_VERSION` is the parser-side compatibility marker.
- Annotation handling supports compatibility, warning, and strict modes.
- Proto3 `reserved` names and ranges are preserved for drift and migration
  checks.

## What Annotations Describe

- SQL tables, columns, indexes, primary keys, and RLS columns.
- Cache, vector, object, document, graph, column, and time-series store hints.
- Projection materialization targets.
- Security classification, PII handling, and tenant/project routing metadata.
- Language options that SDK generation should carry forward.

## Relevant Code

- Parser entrypoint: [../src/parser/mod.rs](../src/parser/mod.rs)
- Option extraction: [../src/parser/options.rs](../src/parser/options.rs)
- DB annotation parser: [../src/parser/db_parser.rs](../src/parser/db_parser.rs)
- Manifest AST: [../src/schema/ast.rs](../src/schema/ast.rs)
- Portable parser subset: [../crates/udb-portable](../crates/udb-portable)

## Operator Notes

Use strict annotation mode in CI for production projects. Use warning mode while
migrating legacy protos so teams can see all incompatibilities before blocking
the build.
