//! Projection row filter: which source rows a secondary-store projection holds.
//!
//! A projection (vector / document / graph / search / cache / object store fed
//! from the canonical table) may declare `projection_filter`, a small boolean
//! expression over the entity's own columns:
//!
//! ```text
//! state = 'PUBLISHED' AND superseded_at IS NULL
//! ```
//!
//! Grammar (keywords are case-insensitive):
//!
//! ```text
//! expr       := or
//! or         := and ( OR and )*
//! and        := unary ( AND unary )*
//! unary      := NOT unary | '(' expr ')' | predicate
//! predicate  := column ( '=' | '<>' | '!=' | '<' | '<=' | '>' | '>=' ) literal
//!             | column IS [ NOT ] NULL
//!             | column [ NOT ] IN '(' literal ( ',' literal )* ')'
//! literal    := 'single-quoted string' | number | TRUE | FALSE
//! ```
//!
//! Nothing else is accepted: no functions, no arithmetic, no column-to-column
//! comparison, no `NULL` literal (`col = NULL` never matches in SQL; write
//! `col IS NULL`). The parsed form is the IR's [`LogicalFilter`].
//!
//! The manifest build parses the expression, resolves every column against the
//! entity (proto field or column name) and checks each literal against the
//! column's type; any problem is a manifest validation error. A valid filter is
//! stored in the projection's options in canonical form (physical column names),
//! which is what the projection worker, the drift scanner and the hybrid-search
//! text leg read back.
//!
//! Evaluation follows SQL's three-valued logic: a comparison against a NULL
//! column is UNKNOWN, and a row is projected only when the whole filter is TRUE.

use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::value::LogicalValue;

use super::{ManifestColumn, ManifestTable, is_array_sql_type};

/// The projection option key carrying the row filter.
pub const PROJECTION_FILTER_OPTION: &str = "projection_filter";

// ── Parsing ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(String),
    Op(&'static str),
    LParen,
    RParen,
    Comma,
}

fn tokenize(text: &str) -> Result<Vec<(usize, Tok)>, String> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let (offset, ch) = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '(' => {
                tokens.push((offset, Tok::LParen));
                i += 1;
            }
            ')' => {
                tokens.push((offset, Tok::RParen));
                i += 1;
            }
            ',' => {
                tokens.push((offset, Tok::Comma));
                i += 1;
            }
            '=' => {
                tokens.push((offset, Tok::Op("=")));
                i += 1;
            }
            '!' => {
                if chars.get(i + 1).map(|(_, c)| *c) == Some('=') {
                    tokens.push((offset, Tok::Op("<>")));
                    i += 2;
                } else {
                    return Err(format!("unexpected '!' at offset {offset} (use <> or !=)"));
                }
            }
            '<' => match chars.get(i + 1).map(|(_, c)| *c) {
                Some('=') => {
                    tokens.push((offset, Tok::Op("<=")));
                    i += 2;
                }
                Some('>') => {
                    tokens.push((offset, Tok::Op("<>")));
                    i += 2;
                }
                _ => {
                    tokens.push((offset, Tok::Op("<")));
                    i += 1;
                }
            },
            '>' => {
                if chars.get(i + 1).map(|(_, c)| *c) == Some('=') {
                    tokens.push((offset, Tok::Op(">=")));
                    i += 2;
                } else {
                    tokens.push((offset, Tok::Op(">")));
                    i += 1;
                }
            }
            '\'' => {
                let mut value = String::new();
                let mut j = i + 1;
                let mut closed = false;
                while j < chars.len() {
                    let c = chars[j].1;
                    if c == '\'' {
                        if chars.get(j + 1).map(|(_, c)| *c) == Some('\'') {
                            value.push('\'');
                            j += 2;
                            continue;
                        }
                        closed = true;
                        j += 1;
                        break;
                    }
                    value.push(c);
                    j += 1;
                }
                if !closed {
                    return Err(format!(
                        "string literal starting at offset {offset} is not closed"
                    ));
                }
                tokens.push((offset, Tok::Str(value)));
                i = j;
            }
            c if c.is_ascii_digit() || c == '-' || c == '.' => {
                let mut j = i;
                let mut number = String::new();
                if c == '-' {
                    number.push('-');
                    j += 1;
                }
                let mut digits = 0;
                let mut seen_dot = false;
                let mut seen_exp = false;
                while j < chars.len() {
                    let c = chars[j].1;
                    if c.is_ascii_digit() {
                        digits += 1;
                        number.push(c);
                        j += 1;
                    } else if c == '.' && !seen_dot && !seen_exp {
                        seen_dot = true;
                        number.push(c);
                        j += 1;
                    } else if (c == 'e' || c == 'E') && digits > 0 && !seen_exp {
                        seen_exp = true;
                        number.push(c);
                        j += 1;
                        if let Some((_, sign)) = chars.get(j)
                            && (*sign == '+' || *sign == '-')
                        {
                            number.push(*sign);
                            j += 1;
                        }
                    } else {
                        break;
                    }
                }
                if digits == 0 || number.ends_with(['e', 'E', '+', '-']) {
                    return Err(format!("malformed number at offset {offset}"));
                }
                if let Some((_, next)) = chars.get(j)
                    && (next.is_alphanumeric() || *next == '_')
                {
                    return Err(format!("malformed number at offset {offset}"));
                }
                tokens.push((offset, Tok::Num(number)));
                i = j;
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut j = i;
                let mut ident = String::new();
                while j < chars.len() && (chars[j].1.is_ascii_alphanumeric() || chars[j].1 == '_') {
                    ident.push(chars[j].1);
                    j += 1;
                }
                tokens.push((offset, Tok::Ident(ident)));
                i = j;
            }
            other => {
                return Err(format!(
                    "unexpected character '{other}' at offset {offset}; only columns, comparisons, IS [NOT] NULL, [NOT] IN (...), AND/OR/NOT, parentheses and literals are allowed"
                ));
            }
        }
    }
    Ok(tokens)
}

const KEYWORDS: &[&str] = &["and", "or", "not", "is", "null", "in", "true", "false"];

struct Parser {
    tokens: Vec<(usize, Tok)>,
    pos: usize,
    end: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|(_, tok)| tok)
    }

    fn offset(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map(|(offset, _)| *offset)
            .unwrap_or(self.end)
    }

    fn keyword(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(ident)) if ident.eq_ignore_ascii_case(word))
    }

    fn eat_keyword(&mut self, word: &str) -> bool {
        if self.keyword(word) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, word: &str) -> Result<(), String> {
        if self.eat_keyword(word) {
            Ok(())
        } else {
            Err(format!(
                "expected {} at offset {}",
                word.to_ascii_uppercase(),
                self.offset()
            ))
        }
    }

    fn expect(&mut self, tok: Tok, what: &str) -> Result<(), String> {
        if self.peek() == Some(&tok) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected {what} at offset {}", self.offset()))
        }
    }

    fn or(&mut self) -> Result<LogicalFilter, String> {
        let mut branches = vec![self.and()?];
        while self.eat_keyword("or") {
            branches.push(self.and()?);
        }
        Ok(if branches.len() == 1 {
            branches.remove(0)
        } else {
            LogicalFilter::Or(branches)
        })
    }

    fn and(&mut self) -> Result<LogicalFilter, String> {
        let mut branches = vec![self.unary()?];
        while self.eat_keyword("and") {
            branches.push(self.unary()?);
        }
        Ok(if branches.len() == 1 {
            branches.remove(0)
        } else {
            LogicalFilter::And(branches)
        })
    }

    fn unary(&mut self) -> Result<LogicalFilter, String> {
        if self.eat_keyword("not") {
            return Ok(LogicalFilter::Not(Box::new(self.unary()?)));
        }
        if self.peek() == Some(&Tok::LParen) {
            self.pos += 1;
            let inner = self.or()?;
            self.expect(Tok::RParen, "')'")?;
            return Ok(inner);
        }
        self.predicate()
    }

    fn column(&mut self) -> Result<String, String> {
        let offset = self.offset();
        match self.peek().cloned() {
            Some(Tok::Ident(ident))
                if !KEYWORDS.iter().any(|kw| ident.eq_ignore_ascii_case(kw)) =>
            {
                self.pos += 1;
                Ok(ident)
            }
            Some(Tok::Ident(ident)) => Err(format!(
                "expected a column at offset {offset}, found keyword {}",
                ident.to_ascii_uppercase()
            )),
            Some(_) => Err(format!(
                "expected a column at offset {offset}; a filter compares a column with a literal"
            )),
            None => Err(format!(
                "expected a column at offset {offset}, found the end of the filter"
            )),
        }
    }

    fn literal(&mut self) -> Result<LogicalValue, String> {
        let offset = self.offset();
        let value = match self.peek().cloned() {
            Some(Tok::Str(value)) => LogicalValue::String(value),
            Some(Tok::Num(number)) => parse_number(&number)
                .ok_or_else(|| format!("number {number} at offset {offset} is out of range"))?,
            Some(Tok::Ident(word)) if word.eq_ignore_ascii_case("true") => LogicalValue::Bool(true),
            Some(Tok::Ident(word)) if word.eq_ignore_ascii_case("false") => {
                LogicalValue::Bool(false)
            }
            Some(Tok::Ident(word)) if word.eq_ignore_ascii_case("null") => {
                return Err(format!(
                    "NULL at offset {offset} cannot be compared (a comparison with NULL never matches); write `column IS NULL` or `column IS NOT NULL`"
                ));
            }
            Some(Tok::Ident(word)) => {
                return Err(format!(
                    "expected a literal at offset {offset}, found '{word}'; a filter compares a column with a literal ('text', a number, TRUE or FALSE), never with another column"
                ));
            }
            _ => {
                return Err(format!(
                    "expected a literal ('text', a number, TRUE or FALSE) at offset {offset}"
                ));
            }
        };
        self.pos += 1;
        Ok(value)
    }

    fn predicate(&mut self) -> Result<LogicalFilter, String> {
        let field = self.column()?;
        if self.eat_keyword("is") {
            let negated = self.eat_keyword("not");
            self.expect_keyword("null")?;
            let is_null = LogicalFilter::IsNull(field);
            return Ok(if negated {
                LogicalFilter::Not(Box::new(is_null))
            } else {
                is_null
            });
        }
        let negated_in = self.eat_keyword("not");
        if negated_in || self.keyword("in") {
            self.expect_keyword("in")?;
            self.expect(Tok::LParen, "'(' after IN")?;
            let mut values = vec![self.literal()?];
            while self.peek() == Some(&Tok::Comma) {
                self.pos += 1;
                values.push(self.literal()?);
            }
            self.expect(Tok::RParen, "')' closing the IN list")?;
            let in_list = LogicalFilter::InList { field, values };
            return Ok(if negated_in {
                LogicalFilter::Not(Box::new(in_list))
            } else {
                in_list
            });
        }
        let offset = self.offset();
        let op = match self.peek() {
            Some(Tok::Op(op)) => match *op {
                "=" => ComparisonOp::Eq,
                "<>" => ComparisonOp::Ne,
                "<" => ComparisonOp::Lt,
                "<=" => ComparisonOp::Le,
                ">" => ComparisonOp::Gt,
                ">=" => ComparisonOp::Ge,
                other => return Err(format!("unsupported operator '{other}' at offset {offset}")),
            },
            _ => {
                return Err(format!(
                    "expected a comparison (=, <>, <, <=, >, >=), IS [NOT] NULL or [NOT] IN after column '{field}' at offset {offset}"
                ));
            }
        };
        self.pos += 1;
        let value = self.literal()?;
        Ok(LogicalFilter::Comparison { field, op, value })
    }
}

fn parse_number(number: &str) -> Option<LogicalValue> {
    if number.contains(['.', 'e', 'E']) {
        number
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(LogicalValue::Float)
    } else {
        number.parse::<i64>().ok().map(LogicalValue::Int)
    }
}

/// Parse a projection filter expression. Column names are kept as written;
/// [`resolve_projection_filter`] maps them onto the entity.
pub fn parse_projection_filter(text: &str) -> Result<LogicalFilter, String> {
    if text.trim().is_empty() {
        return Err("the filter is empty".to_string());
    }
    let mut parser = Parser {
        tokens: tokenize(text)?,
        pos: 0,
        end: text.len(),
    };
    let filter = parser.or()?;
    if parser.pos < parser.tokens.len() {
        return Err(format!(
            "unexpected input at offset {} (did you miss AND / OR?)",
            parser.offset()
        ));
    }
    Ok(filter)
}

// ── Validation against the entity ────────────────────────────────────────────

/// What a column's values can be compared with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnClass {
    Text,
    Uuid,
    Integer,
    Number,
    Bool,
    /// Dates and times: their JSON text form depends on the writer, so only
    /// IS [NOT] NULL is accepted.
    Temporal,
    /// JSON, bytes, arrays, vectors and other non-scalar types: IS [NOT] NULL only.
    Other,
}

fn column_class(column: &ManifestColumn) -> ColumnClass {
    if column.is_array || is_array_sql_type(&column.sql_type) {
        return ColumnClass::Other;
    }
    if !column.enum_values.is_empty() {
        return ColumnClass::Text;
    }
    let sql_type = column.sql_type.trim().to_ascii_lowercase();
    let base = sql_type.split('(').next().unwrap_or_default().trim();
    match base {
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "citext"
        | "name" | "string" => ColumnClass::Text,
        "uuid" => ColumnClass::Uuid,
        "smallint" | "int2" | "integer" | "int" | "int4" | "bigint" | "int8" | "serial"
        | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2" => ColumnClass::Integer,
        "numeric" | "decimal" | "real" | "float4" | "float8" | "double precision" | "float" => {
            ColumnClass::Number
        }
        "boolean" | "bool" => ColumnClass::Bool,
        _ if base.starts_with("timestamp")
            || base.starts_with("time")
            || base == "date"
            || base.starts_with("interval") =>
        {
            ColumnClass::Temporal
        }
        _ => ColumnClass::Other,
    }
}

fn check_literal(column: &ManifestColumn, value: &LogicalValue) -> Result<(), String> {
    let name = &column.column_name;
    let sql_type = &column.sql_type;
    match (column_class(column), value) {
        (ColumnClass::Text, LogicalValue::String(text)) => {
            if !column.enum_values.is_empty()
                && !column.enum_values.iter().any(|allowed| allowed == text)
            {
                return Err(format!(
                    "'{text}' is not one of column '{name}''s values ({})",
                    column.enum_values.join(", ")
                ));
            }
            Ok(())
        }
        (ColumnClass::Uuid, LogicalValue::String(text)) => uuid::Uuid::parse_str(text)
            .map(|_| ())
            .map_err(|_| format!("'{text}' is not a UUID, and column '{name}' is {sql_type}")),
        (ColumnClass::Integer, LogicalValue::Int(_)) => Ok(()),
        (ColumnClass::Number, LogicalValue::Int(_) | LogicalValue::Float(_)) => Ok(()),
        (ColumnClass::Bool, LogicalValue::Bool(_)) => Ok(()),
        (ColumnClass::Temporal, _) => Err(format!(
            "column '{name}' is {sql_type}; a date/time column supports only IS [NOT] NULL in a projection filter"
        )),
        (ColumnClass::Other, _) => Err(format!(
            "column '{name}' is {sql_type}; only scalar text, uuid, number and boolean columns can be compared (IS [NOT] NULL works on any column)"
        )),
        (_, value) => Err(format!(
            "column '{name}' is {sql_type} but is compared with {} literal",
            literal_kind(value)
        )),
    }
}

fn literal_kind(value: &LogicalValue) -> &'static str {
    match value {
        LogicalValue::String(_) => "a text",
        LogicalValue::Int(_) => "an integer",
        LogicalValue::Float(_) => "a decimal",
        LogicalValue::Bool(_) => "a boolean",
        _ => "an unsupported",
    }
}

/// The entity column a filter names, by proto field or column name
/// (case-insensitive).
fn find_column<'t>(table: &'t ManifestTable, name: &str) -> Option<&'t ManifestColumn> {
    table.columns.iter().find(|column| {
        column.column_name.eq_ignore_ascii_case(name)
            || column.field_name.eq_ignore_ascii_case(name)
    })
}

fn resolve_column<'t>(table: &'t ManifestTable, name: &str) -> Result<&'t ManifestColumn, String> {
    let column = find_column(table, name).ok_or_else(|| {
        format!(
            "'{name}' is not a column of {}.{}",
            table.schema, table.table
        )
    })?;
    if column.encrypted || column.security.is_encrypted {
        return Err(format!(
            "column '{}' is encrypted; the projection worker sees only its ciphertext and cannot filter on it",
            column.column_name
        ));
    }
    Ok(column)
}

/// Check `filter` against `table` and return it with every column resolved to
/// its physical column name.
pub fn resolve_projection_filter(
    filter: &LogicalFilter,
    table: &ManifestTable,
) -> Result<LogicalFilter, String> {
    Ok(match filter {
        LogicalFilter::And(branches) => LogicalFilter::And(
            branches
                .iter()
                .map(|branch| resolve_projection_filter(branch, table))
                .collect::<Result<Vec<_>, String>>()?,
        ),
        LogicalFilter::Or(branches) => LogicalFilter::Or(
            branches
                .iter()
                .map(|branch| resolve_projection_filter(branch, table))
                .collect::<Result<Vec<_>, String>>()?,
        ),
        LogicalFilter::Not(inner) => {
            LogicalFilter::Not(Box::new(resolve_projection_filter(inner, table)?))
        }
        LogicalFilter::IsNull(field) => {
            LogicalFilter::IsNull(resolve_column(table, field)?.column_name.clone())
        }
        LogicalFilter::Comparison { field, op, value } => {
            let column = resolve_column(table, field)?;
            check_literal(column, value)?;
            let class = column_class(column);
            if matches!(class, ColumnClass::Bool | ColumnClass::Uuid)
                && !matches!(op, ComparisonOp::Eq | ComparisonOp::Ne)
            {
                return Err(format!(
                    "column '{}' is {}; it supports only =, <> and IN",
                    column.column_name, column.sql_type
                ));
            }
            if !matches!(
                op,
                ComparisonOp::Eq
                    | ComparisonOp::Ne
                    | ComparisonOp::Lt
                    | ComparisonOp::Le
                    | ComparisonOp::Gt
                    | ComparisonOp::Ge
            ) {
                return Err(format!(
                    "operator '{}' is not supported in a projection filter",
                    op.token()
                ));
            }
            LogicalFilter::Comparison {
                field: column.column_name.clone(),
                op: *op,
                value: value.clone(),
            }
        }
        LogicalFilter::InList { field, values } => {
            let column = resolve_column(table, field)?;
            if values.is_empty() {
                return Err(format!(
                    "the IN list for column '{}' is empty",
                    column.column_name
                ));
            }
            for value in values {
                check_literal(column, value)?;
            }
            LogicalFilter::InList {
                field: column.column_name.clone(),
                values: values.clone(),
            }
        }
    })
}

/// Parse `text` and resolve it against `table`, returning the canonical form.
pub fn canonical_projection_filter(text: &str, table: &ManifestTable) -> Result<String, String> {
    let filter = parse_projection_filter(text)?;
    let resolved = resolve_projection_filter(&filter, table)?;
    Ok(render_projection_filter(&resolved))
}

// ── Rendering ────────────────────────────────────────────────────────────────

fn render_literal(value: &LogicalValue) -> String {
    match value {
        LogicalValue::String(text) => format!("'{}'", text.replace('\'', "''")),
        LogicalValue::Int(number) => number.to_string(),
        // `{:?}` keeps a fractional part or exponent, so the literal re-parses
        // as a decimal.
        LogicalValue::Float(number) => format!("{number:?}"),
        LogicalValue::Bool(true) => "TRUE".to_string(),
        LogicalValue::Bool(false) => "FALSE".to_string(),
        LogicalValue::Null => "NULL".to_string(),
        other => format!("'{}'", other.type_token()),
    }
}

fn op_symbol(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Ne => "<>",
        ComparisonOp::Lt => "<",
        ComparisonOp::Le => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Ge => ">=",
        // Never produced by the parser; rendered so a malformed filter fails
        // to re-parse instead of rendering a different predicate.
        other => other.token(),
    }
}

fn render_with(filter: &LogicalFilter, column: &dyn Fn(&str) -> String) -> String {
    let child = |branch: &LogicalFilter| match branch {
        LogicalFilter::And(_) | LogicalFilter::Or(_) => {
            format!("({})", render_with(branch, column))
        }
        _ => render_with(branch, column),
    };
    match filter {
        LogicalFilter::And(branches) if branches.is_empty() => "TRUE".to_string(),
        LogicalFilter::Or(branches) if branches.is_empty() => "FALSE".to_string(),
        LogicalFilter::And(branches) => {
            branches.iter().map(child).collect::<Vec<_>>().join(" AND ")
        }
        LogicalFilter::Or(branches) => branches.iter().map(child).collect::<Vec<_>>().join(" OR "),
        LogicalFilter::Not(inner) => match inner.as_ref() {
            LogicalFilter::IsNull(field) => format!("{} IS NOT NULL", column(field)),
            LogicalFilter::InList { field, values } => format!(
                "{} NOT IN ({})",
                column(field),
                values
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            other => format!("NOT ({})", render_with(other, column)),
        },
        LogicalFilter::IsNull(field) => format!("{} IS NULL", column(field)),
        LogicalFilter::Comparison { field, op, value } => format!(
            "{} {} {}",
            column(field),
            op_symbol(*op),
            render_literal(value)
        ),
        LogicalFilter::InList { field, values } => format!(
            "{} IN ({})",
            column(field),
            values
                .iter()
                .map(render_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The canonical text of a filter: the form the manifest stores and operators see.
pub fn render_projection_filter(filter: &LogicalFilter) -> String {
    render_with(filter, &|name: &str| name.to_string())
}

/// The filter as a SQL predicate over `alias`'s columns (quoted identifiers,
/// literals inlined: they are parsed literals, never caller text).
pub fn render_projection_filter_sql(filter: &LogicalFilter, alias: &str) -> String {
    render_with(filter, &|name: &str| {
        format!("{alias}.\"{}\"", name.replace('"', "\"\""))
    })
}

// ── Evaluation ───────────────────────────────────────────────────────────────

fn compare_strings(row: &str, literal: &str) -> std::cmp::Ordering {
    // A UUID compares by value: the stored row and a request record may spell
    // the same id in different cases.
    if let (Ok(left), Ok(right)) = (uuid::Uuid::parse_str(row), uuid::Uuid::parse_str(literal)) {
        return left.cmp(&right);
    }
    row.cmp(literal)
}

fn json_number(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(number) => number.as_f64(),
        // int64 / numeric values often travel as JSON strings.
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn json_integer(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(text) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// `row_value <op> literal`: `Ok(None)` when the row value is NULL (UNKNOWN).
fn compare(
    field: &str,
    row_value: &serde_json::Value,
    op: ComparisonOp,
    literal: &LogicalValue,
) -> Result<Option<bool>, String> {
    use std::cmp::Ordering;
    if row_value.is_null() {
        return Ok(None);
    }
    let mismatch = || {
        format!(
            "column '{field}' holds {row_value} in the projected row, which cannot be compared with {}",
            render_literal(literal)
        )
    };
    let ordering: Ordering = match literal {
        LogicalValue::String(text) => match row_value {
            serde_json::Value::String(row) => compare_strings(row, text),
            _ => return Err(mismatch()),
        },
        LogicalValue::Int(number) => match json_integer(row_value) {
            Some(row) => row.cmp(number),
            None => {
                let row = json_number(row_value).ok_or_else(mismatch)?;
                row.partial_cmp(&(*number as f64)).ok_or_else(mismatch)?
            }
        },
        LogicalValue::Float(number) => {
            let row = json_number(row_value).ok_or_else(mismatch)?;
            row.partial_cmp(number).ok_or_else(mismatch)?
        }
        LogicalValue::Bool(expected) => {
            let row = match row_value {
                serde_json::Value::Bool(row) => *row,
                serde_json::Value::String(text) if text.eq_ignore_ascii_case("true") => true,
                serde_json::Value::String(text) if text.eq_ignore_ascii_case("false") => false,
                _ => return Err(mismatch()),
            };
            row.cmp(expected)
        }
        _ => return Err(mismatch()),
    };
    Ok(Some(match op {
        ComparisonOp::Eq => ordering == Ordering::Equal,
        ComparisonOp::Ne => ordering != Ordering::Equal,
        ComparisonOp::Lt => ordering == Ordering::Less,
        ComparisonOp::Le => ordering != Ordering::Greater,
        ComparisonOp::Gt => ordering == Ordering::Greater,
        ComparisonOp::Ge => ordering != Ordering::Less,
        other => {
            return Err(format!(
                "operator '{}' is not supported in a projection filter",
                other.token()
            ));
        }
    }))
}

fn row_value<'r>(row: &'r serde_json::Value, field: &str) -> Result<&'r serde_json::Value, String> {
    let map = row
        .as_object()
        .ok_or_else(|| "the projected row is not a JSON object".to_string())?;
    map.get(field)
        .or_else(|| {
            map.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(field))
                .map(|(_, value)| value)
        })
        .ok_or_else(|| {
            format!(
                "the projected row has no column '{field}', so the projection filter cannot be evaluated; the write must project the whole stored row"
            )
        })
}

/// Three-valued evaluation: `Some(true)` TRUE, `Some(false)` FALSE, `None` UNKNOWN.
fn evaluate(filter: &LogicalFilter, row: &serde_json::Value) -> Result<Option<bool>, String> {
    Ok(match filter {
        LogicalFilter::And(branches) => {
            let mut unknown = false;
            for branch in branches {
                match evaluate(branch, row)? {
                    Some(false) => return Ok(Some(false)),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            if unknown { None } else { Some(true) }
        }
        LogicalFilter::Or(branches) => {
            let mut unknown = false;
            for branch in branches {
                match evaluate(branch, row)? {
                    Some(true) => return Ok(Some(true)),
                    None => unknown = true,
                    Some(false) => {}
                }
            }
            if unknown { None } else { Some(false) }
        }
        LogicalFilter::Not(inner) => evaluate(inner, row)?.map(|value| !value),
        LogicalFilter::IsNull(field) => Some(row_value(row, field)?.is_null()),
        LogicalFilter::Comparison { field, op, value } => {
            compare(field, row_value(row, field)?, *op, value)?
        }
        LogicalFilter::InList { field, values } => {
            let current = row_value(row, field)?;
            if current.is_null() {
                None
            } else {
                let mut matched = false;
                for value in values {
                    if compare(field, current, ComparisonOp::Eq, value)? == Some(true) {
                        matched = true;
                        break;
                    }
                }
                Some(matched)
            }
        }
    })
}

/// Whether `row` (a column-keyed source row) belongs in the projection: the
/// filter must be TRUE (UNKNOWN, from a NULL column, does not match). An error
/// means the row cannot be judged — a filter column is absent or holds a value
/// of another type — and the caller must refuse rather than guess.
pub fn projection_filter_matches(
    filter: &LogicalFilter,
    row: &serde_json::Value,
) -> Result<bool, String> {
    Ok(evaluate(filter, row)? == Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn column(name: &str, sql_type: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            sql_type: sql_type.to_string(),
            ..ManifestColumn::default()
        }
    }

    fn concepts() -> ManifestTable {
        let mut state = column("state", "TEXT");
        state.enum_values = vec![
            "DRAFT".to_string(),
            "PUBLISHED".to_string(),
            "RETIRED".to_string(),
        ];
        let mut secret = column("secret", "TEXT");
        secret.encrypted = true;
        let mut to_seq = column("to_seq", "BIGINT");
        to_seq.field_name = "toSeq".to_string();
        ManifestTable {
            schema: "terms".to_string(),
            table: "concepts".to_string(),
            columns: vec![
                column("id", "UUID"),
                state,
                to_seq,
                column("score", "NUMERIC(10,2)"),
                column("active", "BOOLEAN"),
                column("title", "VARCHAR(200)"),
                column("published_at", "TIMESTAMPTZ"),
                column("payload", "JSONB"),
                secret,
            ],
            ..ManifestTable::default()
        }
    }

    fn canonical(text: &str) -> Result<String, String> {
        canonical_projection_filter(text, &concepts())
    }

    #[test]
    fn parses_and_canonicalizes_the_published_current_filter() {
        assert_eq!(
            canonical("state = 'PUBLISHED' and TOSEQ is null").unwrap(),
            "state = 'PUBLISHED' AND to_seq IS NULL"
        );
        assert_eq!(
            canonical("not (active = false or score >= 2.5) AND title != 'it''s'").unwrap(),
            "NOT (active = FALSE OR score >= 2.5) AND title <> 'it''s'"
        );
        assert_eq!(
            canonical("state NOT IN ('DRAFT', 'RETIRED') OR (to_seq > -3 AND to_seq IS NOT NULL)")
                .unwrap(),
            "state NOT IN ('DRAFT', 'RETIRED') OR (to_seq > -3 AND to_seq IS NOT NULL)"
        );
        // The canonical form is a fixed point.
        let once = canonical("toSeq<>1 or (state='DRAFT' and active=true)").unwrap();
        assert_eq!(once, "to_seq <> 1 OR (state = 'DRAFT' AND active = TRUE)");
        assert_eq!(canonical(&once).unwrap(), once);
    }

    #[test]
    fn refuses_what_the_grammar_does_not_allow() {
        for (text, needle) in [
            ("", "empty"),
            ("state = 'PUBLISHED' AND", "end of the filter"),
            ("state = PUBLISHED", "never with another column"),
            ("state = NULL", "IS NULL"),
            ("state IN ()", "literal"),
            ("lower(state) = 'x'", "expected a comparison"),
            ("state = 'x' ; DROP TABLE t", "unexpected character ';'"),
            ("state = 'unclosed", "not closed"),
            ("state = 'x' state = 'y'", "AND / OR"),
            ("to_seq = 1 + 2", "unexpected character '+'"),
            ("to_seq = 99999999999999999999", "out of range"),
        ] {
            let error = canonical(text).expect_err(text);
            assert!(error.contains(needle), "{text}: {error}");
        }
    }

    #[test]
    fn refuses_unknown_columns_wrong_literals_and_unfilterable_columns() {
        for (text, needle) in [
            (
                "status = 'PUBLISHED'",
                "'status' is not a column of terms.concepts",
            ),
            ("state = 'ARCHIVED'", "not one of column 'state''s values"),
            ("state = 1", "compared with an integer literal"),
            ("to_seq = 'one'", "compared with a text literal"),
            ("to_seq = 1.5", "compared with a decimal literal"),
            ("active = 'yes'", "compared with a text literal"),
            ("active > TRUE", "only =, <> and IN"),
            ("id = 'not-a-uuid'", "is not a UUID"),
            ("published_at > '2026-01-01'", "only IS [NOT] NULL"),
            ("payload = 'x'", "only scalar"),
            ("secret = 'x'", "encrypted"),
            ("secret IS NULL", "encrypted"),
        ] {
            let error = canonical(text).expect_err(text);
            assert!(error.contains(needle), "{text}: {error}");
        }
        // IS NULL is fine on any non-encrypted column.
        assert_eq!(
            canonical("published_at IS NOT NULL AND payload IS NULL").unwrap(),
            "published_at IS NOT NULL AND payload IS NULL"
        );
        assert!(canonical("score > 1 AND score < 2.75").is_ok());
        assert!(canonical("id = '6F9619FF-8B86-D011-B42D-00C04FC964FF'").is_ok());
    }

    fn matches(text: &str, row: serde_json::Value) -> Result<bool, String> {
        projection_filter_matches(&parse_projection_filter(text).unwrap(), &row)
    }

    #[test]
    fn evaluates_published_current_rows() {
        let filter = "state = 'PUBLISHED' AND to_seq IS NULL";
        assert!(matches(filter, json!({"state": "PUBLISHED", "to_seq": null})).unwrap());
        assert!(!matches(filter, json!({"state": "RETIRED", "to_seq": null})).unwrap());
        assert!(!matches(filter, json!({"state": "PUBLISHED", "to_seq": 7})).unwrap());
        assert!(!matches(filter, json!({"state": null, "to_seq": null})).unwrap());
    }

    #[test]
    fn evaluation_follows_three_valued_logic() {
        // NULL <> 'x' is UNKNOWN, and NOT UNKNOWN is still UNKNOWN: no match.
        assert!(!matches("state <> 'DRAFT'", json!({"state": null})).unwrap());
        assert!(!matches("NOT (state = 'DRAFT')", json!({"state": null})).unwrap());
        assert!(!matches("state NOT IN ('DRAFT')", json!({"state": null})).unwrap());
        // UNKNOWN OR TRUE is TRUE; UNKNOWN AND FALSE is FALSE.
        assert!(
            matches(
                "state = 'A' OR active = TRUE",
                json!({"state": null, "active": true})
            )
            .unwrap()
        );
        assert!(
            !matches(
                "state = 'A' AND active = TRUE",
                json!({"state": null, "active": false})
            )
            .unwrap()
        );
    }

    #[test]
    fn evaluation_compares_by_type() {
        assert!(matches("to_seq >= 10", json!({"to_seq": "12"})).unwrap());
        assert!(matches("to_seq < 10", json!({"to_seq": 9})).unwrap());
        assert!(matches("score > 2.5", json!({"score": 2.75})).unwrap());
        assert!(matches("score = 3", json!({"score": 3.0})).unwrap());
        assert!(matches("active = TRUE", json!({"active": "true"})).unwrap());
        assert!(matches("state IN ('A', 'B')", json!({"state": "B"})).unwrap());
        assert!(
            matches(
                "id = '6f9619ff-8b86-d011-b42d-00c04fc964ff'",
                json!({"id": "6F9619FF-8B86-D011-B42D-00C04FC964FF"})
            )
            .unwrap()
        );
    }

    #[test]
    fn evaluation_refuses_a_row_it_cannot_judge() {
        let missing = matches("state = 'PUBLISHED'", json!({"id": "x"})).unwrap_err();
        assert!(missing.contains("has no column 'state'"), "{missing}");
        let wrong = matches("to_seq = 1", json!({"to_seq": "one"})).unwrap_err();
        assert!(wrong.contains("cannot be compared"), "{wrong}");
        let not_object = matches("to_seq = 1", json!([1])).unwrap_err();
        assert!(not_object.contains("not a JSON object"), "{not_object}");
    }

    #[test]
    fn sql_rendering_quotes_columns_and_inlines_parsed_literals() {
        let filter = parse_projection_filter("state = 'it''s' AND to_seq IS NULL").unwrap();
        assert_eq!(
            render_projection_filter_sql(&filter, "t"),
            "t.\"state\" = 'it''s' AND t.\"to_seq\" IS NULL"
        );
    }
}
