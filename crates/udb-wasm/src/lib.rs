//! Browser bridge for `udb-portable` — exposes UDB's **real** proto-DB parser,
//! lossless AST, and deterministic manifest checksum to JavaScript through a
//! tiny C ABI over wasm linear memory. No wasm-bindgen, no extra logic: the
//! parsing/checksum is the exact same UDB code the server runs.
//!
//! ## Memory protocol (JS side)
//! 1. `p = udb_alloc(bytes.length)`; copy the UTF-8 proto source into memory at `p`.
//! 2. `packed = udb_parse(p, len, nsPtr, nsLen)` (free the input via `udb_free`).
//! 3. `packed` is an i64: high 32 bits = result pointer, low 32 bits = result
//!    byte length. Read that many UTF-8 bytes (a JSON document) from memory.
//! 4. `udb_free(resultPtr, resultLen)` to release the result.
//!
//! The same buffer protocol is used by the schema-cache bridge below:
//! `udb_schema_cache_new()` returns an opaque cache pointer, and
//! `udb_schema_cache_observe` / `udb_schema_cache_descriptor` /
//! `udb_schema_cache_insert_descriptor` expose the portable SDK descriptor cache
//! to browser clients.

use std::alloc::{Layout, alloc, dealloc};

// New portable surfaces wired into the playground (all run in the browser):
use udb_portable::generation::manifest::CatalogManifest;
use udb_portable::generation::sql::{SqlGenerationConfig, generate_bootstrap_sql};
use udb_portable::ir::compile::postgres::PostgresCompiler;
use udb_portable::ir::compile::{CompileContext, CompiledRendering, Compiler};
use udb_portable::ir::operations::LogicalRead;
use udb_portable::parser::{ParserConfig, UDB_ANNOTATION_VERSION, parse_proto_source};
use udb_portable::schema::ast::ProtoSchema;
use udb_portable::{CatalogCompatibility, Negotiation, SchemaCache, schema_checksum};

/// Allocate `len` bytes of wasm linear memory; returns a pointer JS writes into.
#[unsafe(no_mangle)]
pub extern "C" fn udb_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return std::ptr::null_mut();
    }
    // SAFETY: align 1 is valid for any size; we hand the pointer to JS which
    // returns it verbatim to `udb_free` with the same length.
    unsafe { alloc(Layout::from_size_align(len, 1).unwrap()) }
}

/// Free a buffer previously returned by `udb_alloc` or the result of `udb_parse`.
///
/// # Safety
/// `ptr`/`len` must be a pair returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe { dealloc(ptr, Layout::from_size_align(len, 1).unwrap()) }
}

/// Run UDB's real parser + checksum over the given proto source.
///
/// Returns an i64 packing `(result_ptr << 32) | result_len`; the result is a
/// leaked UTF-8 JSON buffer the caller must release with `udb_free`.
///
/// # Safety
/// `src`/`src_len` and `ns`/`ns_len` must point to valid UTF-8 buffers of the
/// given lengths (typically just-allocated via `udb_alloc`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_parse(
    src: *const u8,
    src_len: usize,
    ns: *const u8,
    ns_len: usize,
) -> u64 {
    let source = if src.is_null() {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(src, src_len) }
    };
    let namespace = if ns.is_null() {
        ""
    } else {
        let ns_bytes = unsafe { std::slice::from_raw_parts(ns, ns_len) };
        std::str::from_utf8(ns_bytes).unwrap_or("")
    };

    let json = run(source, namespace);
    // into_boxed_slice() shrinks capacity to exactly len, so the buffer the
    // caller later releases via `udb_free(ptr, len)` matches the allocation
    // size dlmalloc recorded (a Vec keeps capacity ≥ len, which would mismatch).
    let boxed = json.into_bytes().into_boxed_slice();
    let len = boxed.len();
    let ptr = boxed.as_ptr() as u64;
    Box::leak(boxed); // ownership transfers to JS; freed via `udb_free`.
    (ptr << 32) | (len as u64)
}

/// Create an opaque portable [`SchemaCache`] handle for JS.
#[unsafe(no_mangle)]
pub extern "C" fn udb_schema_cache_new() -> *mut SchemaCache {
    Box::into_raw(Box::new(SchemaCache::new()))
}

/// Free a cache handle returned by [`udb_schema_cache_new`].
///
/// # Safety
/// `cache` must be a pointer returned by `udb_schema_cache_new` and must not be
/// used again after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_free(cache: *mut SchemaCache) {
    if cache.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(cache));
    }
}

/// Record server catalog headers and return the typed negotiation outcome as
/// JSON. The descriptor cache is flushed by `udb-portable` on catalog roll.
///
/// # Safety
/// `cache` must be a valid handle. `catalog_version` and `manifest_checksum`
/// must point to valid UTF-8 buffers for the provided lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_observe(
    cache: *mut SchemaCache,
    catalog_version: *const u8,
    catalog_version_len: usize,
    manifest_checksum: *const u8,
    manifest_checksum_len: usize,
) -> u64 {
    let Some(cache) = (unsafe { cache.as_mut() }) else {
        return json_result(serde_json::json!({ "ok": false, "error": "null schema cache" }));
    };
    let catalog_version = unsafe { read_utf8(catalog_version, catalog_version_len) };
    let manifest_checksum = unsafe { read_utf8(manifest_checksum, manifest_checksum_len) };
    let outcome = cache.observe(catalog_version, manifest_checksum);
    json_result(serde_json::json!({
        "ok": true,
        "negotiation": negotiation_json(outcome),
        "descriptor_count": cache.descriptor_count(),
    }))
}

/// Return the most recent observed `(catalog_version, manifest_checksum)` pair.
///
/// # Safety
/// `cache` must be a valid handle returned by `udb_schema_cache_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_last_observation(cache: *const SchemaCache) -> u64 {
    let Some(cache) = (unsafe { cache.as_ref() }) else {
        return json_result(serde_json::json!({ "ok": false, "error": "null schema cache" }));
    };
    let observation = cache
        .last_observation()
        .map(|(catalog_version, manifest_checksum)| {
            serde_json::json!({
                "catalog_version": catalog_version,
                "manifest_checksum": manifest_checksum,
            })
        });
    json_result(serde_json::json!({ "ok": true, "observation": observation }))
}

/// Number of message descriptors currently cached.
///
/// # Safety
/// `cache` must be a valid handle returned by `udb_schema_cache_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_descriptor_count(cache: *const SchemaCache) -> usize {
    unsafe { cache.as_ref() }
        .map(SchemaCache::descriptor_count)
        .unwrap_or(0)
}

/// Store a descriptor JSON document under a fully-qualified message name.
///
/// # Safety
/// `cache` must be valid. `qualified_name` and `schema_json` must point to valid
/// UTF-8 buffers for the provided lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_insert_descriptor(
    cache: *mut SchemaCache,
    qualified_name: *const u8,
    qualified_name_len: usize,
    schema_json: *const u8,
    schema_json_len: usize,
) -> u64 {
    let Some(cache) = (unsafe { cache.as_mut() }) else {
        return json_result(serde_json::json!({ "ok": false, "error": "null schema cache" }));
    };
    let qualified_name = unsafe { read_utf8(qualified_name, qualified_name_len) };
    let schema_json = unsafe { read_utf8(schema_json, schema_json_len) };
    match serde_json::from_str::<ProtoSchema>(&schema_json) {
        Ok(schema) => {
            cache.insert_descriptor(qualified_name, schema);
            json_result(serde_json::json!({
                "ok": true,
                "descriptor_count": cache.descriptor_count(),
            }))
        }
        Err(err) => json_result(serde_json::json!({
            "ok": false,
            "error": format!("invalid descriptor JSON: {err}"),
        })),
    }
}

/// Look up a cached descriptor by fully-qualified message name and return it as
/// JSON, or `"descriptor": null` on a cache miss.
///
/// # Safety
/// `cache` must be valid. `qualified_name` must point to a valid UTF-8 buffer
/// for the provided length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_descriptor(
    cache: *const SchemaCache,
    qualified_name: *const u8,
    qualified_name_len: usize,
) -> u64 {
    let Some(cache) = (unsafe { cache.as_ref() }) else {
        return json_result(serde_json::json!({ "ok": false, "error": "null schema cache" }));
    };
    let qualified_name = unsafe { read_utf8(qualified_name, qualified_name_len) };
    let descriptor = cache
        .descriptor(&qualified_name)
        .and_then(|schema| serde_json::to_value(schema).ok());
    json_result(serde_json::json!({ "ok": true, "descriptor": descriptor }))
}

/// Compare a local SDK manifest checksum with the most recently observed server
/// checksum, returning the portable compatibility verdict as JSON.
///
/// # Safety
/// `cache` must be valid. `sdk_checksum` must point to a valid UTF-8 buffer for
/// the provided length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn udb_schema_cache_compatibility(
    cache: *const SchemaCache,
    sdk_checksum: *const u8,
    sdk_checksum_len: usize,
) -> u64 {
    let Some(cache) = (unsafe { cache.as_ref() }) else {
        return json_result(serde_json::json!({ "ok": false, "error": "null schema cache" }));
    };
    let sdk_checksum = unsafe { read_utf8(sdk_checksum, sdk_checksum_len) };
    let compatibility = cache.compatibility(&sdk_checksum).map(compatibility_json);
    json_result(serde_json::json!({ "ok": true, "compatibility": compatibility }))
}

/// The UDB annotation contract version this build pins (for display).
#[unsafe(no_mangle)]
pub extern "C" fn udb_annotation_version_ptr() -> *const u8 {
    UDB_ANNOTATION_VERSION.as_ptr()
}
#[unsafe(no_mangle)]
pub extern "C" fn udb_annotation_version_len() -> usize {
    UDB_ANNOTATION_VERSION.len()
}

/// Parse with UDB's real parser and serialize the real `ProtoSchema`s + the
/// real deterministic checksum to JSON. All output comes straight from
/// `udb-portable`; this function only shapes it for the browser.
fn run(source: &[u8], namespace: &str) -> String {
    let cfg = ParserConfig::new(namespace);
    match parse_proto_source(source, "playground.proto", &cfg) {
        Ok(report) => {
            let checksum = schema_checksum(&report.schemas).unwrap_or_default();
            let diagnostics: Vec<serde_json::Value> = report
                .diagnostics
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "line": d.line,
                        "column": d.column,
                        "code": d.code,
                        "message": d.message,
                    })
                })
                .collect();
            // `report.schemas` is the real `Vec<ProtoSchema>` (serde Serialize),
            // so the browser sees exactly what the catalog manifest is built from.
            let schemas_json =
                serde_json::to_value(&report.schemas).unwrap_or(serde_json::Value::Null);
            // Build the CatalogManifest + bootstrap DDL from the SAME schemas —
            // the exact server generation pipeline, running in the browser.
            let manifest_json = CatalogManifest::from_schemas(&report.schemas)
                .ok()
                .and_then(|m| serde_json::to_value(&m).ok())
                .unwrap_or(serde_json::Value::Null);
            let ddl = generate_bootstrap_sql(&report.schemas, &SqlGenerationConfig::default())
                .map(|arts| {
                    arts.iter()
                        .map(|a| a.content.clone())
                        .collect::<Vec<_>>()
                        .join("\n\n")
                })
                .unwrap_or_default();
            // Demo the IR→SQL compiler: compile `SELECT *` for the first table.
            let compiled_query = compile_sample_query(&report.schemas);
            serde_json::json!({
                "ok": true,
                "table_count": report.schemas.len(),
                "schemas": schemas_json,
                "diagnostics": diagnostics,
                "checksum": checksum,
                "annotation_version": UDB_ANNOTATION_VERSION,
                "manifest": manifest_json,
                "ddl": ddl,
                "compiled_query": compiled_query,
            })
            .to_string()
        }
        Err(err) => err_json(&err.to_string()),
    }
}

/// Compile a sample `SELECT *` read for the first annotated table through the
/// REAL Postgres IR compiler — proving end-to-end browser query compilation.
fn compile_sample_query(schemas: &[ProtoSchema]) -> serde_json::Value {
    let Some(first) = schemas.first() else {
        return serde_json::Value::Null;
    };
    let manifest = match CatalogManifest::from_schemas(schemas) {
        Ok(m) => m,
        Err(_) => return serde_json::Value::Null,
    };
    let ctx = CompileContext::new(&manifest);
    let read = LogicalRead {
        message_type: first.message_name.clone(),
        filter: None,
        projection: None,
        sort: Vec::new(),
        pagination: None,
    };
    match PostgresCompiler.compile_read(&read, &ctx) {
        Ok(CompiledRendering::Sql { statement, .. }) => serde_json::json!({
            "message_type": first.message_name,
            "operation": "Read (SELECT *)",
            "backend": "postgres",
            "sql": statement,
        }),
        Ok(_) => serde_json::Value::Null,
        Err(err) => serde_json::json!({ "error": err.to_string() }),
    }
}

fn err_json(message: &str) -> String {
    serde_json::json!({ "ok": false, "error": message }).to_string()
}

fn json_result(value: serde_json::Value) -> u64 {
    let boxed = value.to_string().into_bytes().into_boxed_slice();
    let len = boxed.len();
    let ptr = boxed.as_ptr() as u64;
    Box::leak(boxed);
    (ptr << 32) | (len as u64)
}

unsafe fn read_utf8(ptr: *const u8, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf8_lossy(bytes).into_owned()
}

fn negotiation_json(outcome: Negotiation) -> serde_json::Value {
    match outcome {
        Negotiation::Initial {
            catalog_version,
            manifest_checksum,
        } => serde_json::json!({
            "kind": "initial",
            "catalog_version": catalog_version,
            "manifest_checksum": manifest_checksum,
        }),
        Negotiation::Stable {
            catalog_version,
            manifest_checksum,
        } => serde_json::json!({
            "kind": "stable",
            "catalog_version": catalog_version,
            "manifest_checksum": manifest_checksum,
        }),
        Negotiation::CatalogRolled {
            previous_version,
            previous_checksum,
            new_version,
            new_checksum,
        } => serde_json::json!({
            "kind": "catalog_rolled",
            "previous_version": previous_version,
            "previous_checksum": previous_checksum,
            "new_version": new_version,
            "new_checksum": new_checksum,
        }),
    }
}

fn compatibility_json(compatibility: CatalogCompatibility) -> serde_json::Value {
    match compatibility {
        CatalogCompatibility::Same => serde_json::json!({ "kind": "same" }),
        CatalogCompatibility::BreakingMaybe {
            sdk_checksum,
            server_checksum,
        } => serde_json::json!({
            "kind": "breaking_maybe",
            "sdk_checksum": sdk_checksum,
            "server_checksum": server_checksum,
        }),
    }
}
