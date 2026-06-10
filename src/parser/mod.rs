use std::fs;
use std::path::{Path, PathBuf};

use crate::ast::{ProtoFileAst, ProtoSchema};

mod ast_parser;
mod db_parser;
mod facade;
pub mod lexer;
mod naming;
mod options;
mod paths;
mod selection;
mod structure;

use ast_parser::ProtoAstParser;
use db_parser::ProtoParser;
use lexer::Lexer;
pub use options::{ParserOptionMetadata, documented_option_metadata, parser_option_kind_name};
use selection::dedupe_canonical_table_schemas;

#[cfg(test)]
use naming::{infer_sql_type, to_plural, to_snake_case};

pub use facade::{
    AnnotationParserMode, ParseError, ParseReport, ParserConfig, ParserDiagnostic,
    UDB_ANNOTATION_VERSION,
};

pub fn parse_directory(
    dir_path: impl AsRef<Path>,
    config: &ParserConfig,
) -> Result<Vec<ProtoSchema>, ParseError> {
    parse_directory_report(dir_path, config).map(|report| report.schemas)
}

pub fn parse_directory_report(
    dir_path: impl AsRef<Path>,
    config: &ParserConfig,
) -> Result<ParseReport, ParseError> {
    let dir_path = dir_path.as_ref();
    let mut paths = Vec::new();
    collect_proto_files(dir_path, &mut paths).map_err(|source| ParseError::Io {
        path: dir_path.to_path_buf(),
        source,
    })?;
    paths.sort();

    let mut schemas = Vec::new();
    let mut diagnostics = Vec::new();
    let mut errors = Vec::new();
    for path in paths {
        match parse_file_report(&path, config) {
            Ok(mut report) => {
                schemas.append(&mut report.schemas);
                diagnostics.append(&mut report.diagnostics);
            }
            Err(err) => errors.push(format!("{}: {err}", path.display())),
        }
    }

    let schemas = dedupe_canonical_table_schemas(schemas);
    if errors.is_empty() {
        Ok(ParseReport {
            schemas,
            diagnostics,
        })
    } else {
        Err(ParseError::Directory {
            path: dir_path.to_path_buf(),
            errors,
        })
    }
}

pub fn parse_file(
    file_path: impl AsRef<Path>,
    config: &ParserConfig,
) -> Result<Vec<ProtoSchema>, ParseError> {
    parse_file_report(file_path, config).map(|report| report.schemas)
}

/// Parse proto source held in memory (e.g. embedded native-service protos),
/// rather than reading from disk. `name` is the logical file name used in
/// diagnostics. Shares the same lex/parse path as [`parse_file_report`].
pub fn parse_proto_source(
    source: &[u8],
    name: impl Into<String>,
    config: &ParserConfig,
) -> Result<ParseReport, ParseError> {
    let file = name.into();
    let tokens = Lexer::new(source, file.clone())
        .tokenize()
        .map_err(ParseError::Lex)?;
    ProtoParser::new(tokens, file, config).parse_report()
}

pub fn parse_file_report(
    file_path: impl AsRef<Path>,
    config: &ParserConfig,
) -> Result<ParseReport, ParseError> {
    let file_path = file_path.as_ref();
    let source = fs::read(file_path).map_err(|source| ParseError::Io {
        path: file_path.to_path_buf(),
        source,
    })?;
    let file = file_path.to_string_lossy().to_string();
    let tokens = Lexer::new(&source, file.clone())
        .tokenize()
        .map_err(ParseError::Lex)?;
    ProtoParser::new(tokens, file, config).parse_report()
}

pub fn parse_ast_file(file_path: impl AsRef<Path>) -> Result<ProtoFileAst, ParseError> {
    let file_path = file_path.as_ref();
    let source = fs::read(file_path).map_err(|source| ParseError::Io {
        path: file_path.to_path_buf(),
        source,
    })?;
    let file = file_path.to_string_lossy().to_string();
    parse_ast_source(&source, file)
}

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

fn collect_proto_files(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_proto_files(&path, paths)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("proto") {
            paths.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod parser_tests;
