use std::fmt;
use std::path::PathBuf;

use crate::ast::ProtoSchema;

use super::lexer::LexError;

pub const UDB_ANNOTATION_VERSION: &str = "1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationParserMode {
    /// Preserve legacy aliases and path/default naming conventions silently.
    Compat,
    /// Preserve compatibility, but emit diagnostics for non-canonical contract use.
    Warn,
    /// Emit diagnostics for every non-canonical contract use.
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

#[derive(Debug)]
pub enum ParseError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Lex(LexError),
    Syntax {
        file: String,
        line: usize,
        column: usize,
        message: String,
    },
    Directory {
        path: PathBuf,
        errors: Vec<String>,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "cannot read {}: {source}", path.display()),
            Self::Lex(source) => write!(f, "{source}"),
            Self::Syntax {
                file,
                line,
                column,
                message,
            } => write!(f, "{file}:{line}:{column}: {message}"),
            Self::Directory { path, errors } => write!(
                f,
                "parse_directory {}: {} file(s) failed:\n  {}",
                path.display(),
                errors.len(),
                errors.join("\n  ")
            ),
        }
    }
}

impl std::error::Error for ParseError {}
