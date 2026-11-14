//! A comparison key for a CHECK expression that is stable across PostgreSQL's
//! canonical rewriting.
//!
//! PostgreSQL stores a CHECK as a parse tree and renders it back with
//! `pg_get_expr`, so the text it returns is not the text that was written, and a
//! rule restored from a dump is re-parsed from that rendering and can render
//! differently again. The same rule can therefore read as:
//!
//! * `kind IN ('test','medicine')` (as declared),
//! * `((kind)::text = ANY ((ARRAY['test'::character varying, 'medicine'::character varying])::text[]))`
//!   (first parse), or
//! * `((kind)::text = ANY (ARRAY[('test'::character varying)::text, ('medicine'::character varying)::text]))`
//!   (after a dump and restore).
//!
//! [`canonical_check_expression`] maps all of those to one key by undoing exactly
//! the rewrites PostgreSQL applies: implicit casts on literals, columns, arrays
//! and parenthesised operands; `= ANY (ARRAY[..])` / `<> ALL (ARRAY[..])` back to
//! `IN` / `NOT IN`; a single-element `IN` to `=`; `BETWEEN` to its two
//! comparisons; `~~` to `LIKE`; quoted negative numbers; and the parentheses
//! it adds around every operand. Case, identifier quotes and whitespace outside
//! string literals are ignored. A cast on a function result or other computed
//! value is kept, and a string literal's content is never changed: two keys are
//! equal only when the rules are equal up to PostgreSQL's own coercions.

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// An identifier or keyword, lowercased, identifier quotes removed.
    Word(String),
    /// A string literal including its quotes, verbatim.
    Str(String),
    Num(String),
    Op(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Cast,
}

/// Words that may precede a parenthesised group without making it a call's
/// argument list.
const NON_CALLEE_WORDS: &[&str] = &[
    "and",
    "or",
    "not",
    "when",
    "then",
    "else",
    "case",
    "between",
    "like",
    "ilike",
    "is",
    "similar",
    "escape",
    "symmetric",
];

/// Type names that continue with further words (`character varying`,
/// `timestamp with time zone`, `double precision`).
const MULTIWORD_TYPE_HEADS: &[&str] = &[
    "character",
    "double",
    "timestamp",
    "time",
    "bit",
    "national",
];
const MULTIWORD_TYPE_TAILS: &[&str] = &[
    "varying",
    "precision",
    "with",
    "without",
    "time",
    "zone",
    "character",
];
const NUMERIC_TYPE_NAMES: &[&str] = &[
    "smallint", "integer", "int", "int2", "int4", "int8", "bigint", "numeric", "decimal", "real",
    "float4", "float8", "double",
];

/// The comparison key of a CHECK expression; see the module documentation.
pub fn canonical_check_expression(expression: &str) -> String {
    let mut toks = tokenize(expression);
    // Every rewrite only removes tokens or replaces a span with a shorter or
    // equal canonical form, so the loop reaches a fixed point; the bound guards
    // against a rewrite pair that would ever undo each other.
    for _ in 0..64 {
        let before = toks.clone();
        strip_casts(&mut toks);
        drop_redundant_parens(&mut toks);
        rewrite_any_all(&mut toks);
        rewrite_between(&mut toks);
        rewrite_single_in(&mut toks);
        if toks == before {
            break;
        }
    }
    toks.iter().map(render).collect::<Vec<_>>().join(" ")
}

fn render(tok: &Tok) -> String {
    match tok {
        Tok::Word(value) | Tok::Str(value) | Tok::Num(value) | Tok::Op(value) => value.clone(),
        Tok::LParen => "(".to_string(),
        Tok::RParen => ")".to_string(),
        Tok::LBracket => "[".to_string(),
        Tok::RBracket => "]".to_string(),
        Tok::Comma => ",".to_string(),
        Tok::Cast => "::".to_string(),
    }
}

const OPERATOR_CHARS: &str = "+-*/<>=~!@#%^&|`?";
/// PostgreSQL's lexer lets a multi-character operator end in `+` or `-` only
/// when it contains one of these.
const OPERATOR_TRAILING_SIGN_CHARS: &str = "~!@#%^&|`?";

fn tokenize(input: &str) -> Vec<Tok> {
    let chars: Vec<char> = input.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '\'' => {
                let mut literal = String::from('\'');
                i += 1;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if chars.get(i + 1) == Some(&'\'') {
                            literal.push_str("''");
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    literal.push(chars[i]);
                    i += 1;
                }
                literal.push('\'');
                toks.push(Tok::Str(literal));
            }
            '"' => {
                let mut ident = String::new();
                i += 1;
                while i < chars.len() {
                    if chars[i] == '"' {
                        if chars.get(i + 1) == Some(&'"') {
                            ident.push('"');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    ident.push(chars[i]);
                    i += 1;
                }
                toks.push(Tok::Word(ident.to_lowercase()));
            }
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            '[' => {
                toks.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                toks.push(Tok::RBracket);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            ':' if chars.get(i + 1) == Some(&':') => {
                toks.push(Tok::Cast);
                i += 2;
            }
            c if c.is_ascii_digit()
                || (c == '.' && chars.get(i + 1).is_some_and(|next| next.is_ascii_digit())) =>
            {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                if i < chars.len() && matches!(chars[i], 'e' | 'E') {
                    let mut j = i + 1;
                    if j < chars.len() && matches!(chars[j], '+' | '-') {
                        j += 1;
                    }
                    if j < chars.len() && chars[j].is_ascii_digit() {
                        i = j;
                        while i < chars.len() && chars[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
                let number: String = chars[start..i].iter().collect();
                toks.push(Tok::Num(number.to_ascii_lowercase()));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
                {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                toks.push(Tok::Word(word.to_lowercase()));
            }
            c if OPERATOR_CHARS.contains(c) => {
                let start = i;
                while i < chars.len() && OPERATOR_CHARS.contains(chars[i]) {
                    i += 1;
                }
                let mut op: String = chars[start..i].iter().collect();
                while op.len() > 1
                    && (op.ends_with('+') || op.ends_with('-'))
                    && !op
                        .chars()
                        .any(|ch| OPERATOR_TRAILING_SIGN_CHARS.contains(ch))
                {
                    op.pop();
                    i -= 1;
                }
                push_operator(&mut toks, &op);
            }
            other => {
                toks.push(Tok::Op(other.to_string()));
                i += 1;
            }
        }
    }
    merge_negative_numbers(&mut toks);
    toks
}

fn push_operator(toks: &mut Vec<Tok>, op: &str) {
    let word = |value: &str| Tok::Word(value.to_string());
    match op {
        "!=" => toks.push(Tok::Op("<>".to_string())),
        "~~" => toks.push(word("like")),
        "~~*" => toks.push(word("ilike")),
        "!~~" => toks.extend([word("not"), word("like")]),
        "!~~*" => toks.extend([word("not"), word("ilike")]),
        other => toks.push(Tok::Op(other.to_string())),
    }
}

/// A `-` before a number where no left operand can stand is the number's sign,
/// matching PostgreSQL's quoted `'-1'::integer` rendering once its cast is gone.
fn merge_negative_numbers(toks: &mut Vec<Tok>) {
    let mut i = 0;
    while i + 1 < toks.len() {
        let unary_position = match i.checked_sub(1).map(|k| &toks[k]) {
            None => true,
            Some(Tok::LParen | Tok::LBracket | Tok::Comma | Tok::Op(_) | Tok::Cast) => true,
            Some(Tok::Word(word)) => is_keyword_operand_boundary(word),
            _ => false,
        };
        if unary_position
            && toks[i] == Tok::Op("-".to_string())
            && let Tok::Num(number) = &toks[i + 1]
        {
            let merged = Tok::Num(format!("-{number}"));
            toks.splice(i..i + 2, [merged]);
        }
        i += 1;
    }
}

/// Keywords after which the next token starts a new operand.
fn is_keyword_operand_boundary(word: &str) -> bool {
    NON_CALLEE_WORDS.contains(&word)
        || matches!(word, "in" | "any" | "all" | "select" | "return" | "end")
}

fn matching_close(toks: &[Tok], open: usize) -> Option<usize> {
    let (open_tok, close_tok) = match toks.get(open)? {
        Tok::LParen => (Tok::LParen, Tok::RParen),
        Tok::LBracket => (Tok::LBracket, Tok::RBracket),
        _ => return None,
    };
    let mut depth = 0usize;
    for (offset, tok) in toks[open..].iter().enumerate() {
        if *tok == open_tok {
            depth += 1;
        } else if *tok == close_tok {
            depth -= 1;
            if depth == 0 {
                return Some(open + offset);
            }
        }
    }
    None
}

fn matching_open(toks: &[Tok], close: usize) -> Option<usize> {
    let (open_tok, close_tok) = match toks.get(close)? {
        Tok::RParen => (Tok::LParen, Tok::RParen),
        Tok::RBracket => (Tok::LBracket, Tok::RBracket),
        _ => return None,
    };
    let mut depth = 0usize;
    for (index, tok) in toks[..=close].iter().enumerate().rev() {
        if *tok == close_tok {
            depth += 1;
        } else if *tok == open_tok {
            depth -= 1;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

/// Whether the `(` at `open` is a call's argument list (`lower(x)`, `ANY (..)`,
/// `IN (..)`) rather than a grouping.
fn is_call_paren(toks: &[Tok], open: usize) -> bool {
    match open.checked_sub(1).map(|k| &toks[k]) {
        Some(Tok::Word(word)) => !NON_CALLEE_WORDS.contains(&word.as_str()),
        _ => false,
    }
}

/// The end (exclusive) of the type name starting at `start`, if one is there.
fn type_name_end(toks: &[Tok], start: usize) -> Option<(usize, String)> {
    let Some(Tok::Word(head)) = toks.get(start) else {
        return None;
    };
    let head = head.clone();
    let mut end = start + 1;
    // Schema-qualified type: `public.status`.
    while matches!(toks.get(end), Some(Tok::Op(dot)) if dot == ".")
        && matches!(toks.get(end + 1), Some(Tok::Word(_)))
    {
        end += 2;
    }
    if MULTIWORD_TYPE_HEADS.contains(&head.as_str()) {
        while matches!(toks.get(end), Some(Tok::Word(word)) if MULTIWORD_TYPE_TAILS.contains(&word.as_str()))
        {
            end += 1;
        }
    }
    if toks.get(end) == Some(&Tok::LParen) {
        let close = matching_close(toks, end)?;
        if toks[end + 1..close]
            .iter()
            .all(|tok| matches!(tok, Tok::Num(_) | Tok::Comma))
        {
            end = close + 1;
        }
    }
    while toks.get(end) == Some(&Tok::LBracket) && toks.get(end + 1) == Some(&Tok::RBracket) {
        end += 2;
    }
    Some((end, head))
}

/// Whether the operand ending at `end` is one PostgreSQL coerces implicitly: a
/// literal, a column, an array constructor or a parenthesised group. A call's
/// result keeps its cast.
fn is_coercible_operand(toks: &[Tok], end: usize) -> bool {
    match &toks[end] {
        Tok::Str(_) | Tok::Num(_) => true,
        Tok::Word(word) => !is_keyword_operand_boundary(word),
        Tok::RParen => matching_open(toks, end).is_some_and(|open| !is_call_paren(toks, open)),
        Tok::RBracket => matching_open(toks, end).is_some_and(|open| {
            matches!(open.checked_sub(1).map(|k| &toks[k]), Some(Tok::Word(word)) if word == "array")
        }),
        _ => false,
    }
}

fn strip_casts(toks: &mut Vec<Tok>) {
    let mut i = 1;
    while i < toks.len() {
        if toks[i] != Tok::Cast || !is_coercible_operand(toks, i - 1) {
            i += 1;
            continue;
        }
        let Some((end, type_head)) = type_name_end(toks, i + 1) else {
            i += 1;
            continue;
        };
        // A quoted number cast to a numeric type is that number: PostgreSQL
        // renders `-1` as `'-1'::integer`.
        if NUMERIC_TYPE_NAMES.contains(&type_head.as_str()) {
            let number = match &toks[i - 1] {
                Tok::Str(literal) => {
                    let inner = &literal[1..literal.len() - 1];
                    is_plain_number(inner).then(|| inner.to_ascii_lowercase())
                }
                _ => None,
            };
            if let Some(number) = number {
                toks[i - 1] = Tok::Num(number);
            }
        }
        toks.drain(i..end);
    }
}

fn is_plain_number(value: &str) -> bool {
    let digits = value.strip_prefix('-').unwrap_or(value);
    !digits.is_empty()
        && digits.chars().any(|c| c.is_ascii_digit())
        && digits
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E' | '+' | '-'))
        && digits.parse::<f64>().is_ok()
}

/// Whether `content` is exactly one operand: an atom, a call, an array
/// constructor or a nested group.
fn is_single_primary(content: &[Tok]) -> bool {
    match content {
        [] => false,
        [Tok::Word(word)] => !is_keyword_operand_boundary(word),
        [Tok::Str(_) | Tok::Num(_)] => true,
        [Tok::LParen, ..] => matching_close(content, 0) == Some(content.len() - 1),
        [Tok::Word(word), Tok::LBracket, ..] if word == "array" => {
            matching_close(content, 1) == Some(content.len() - 1)
        }
        [Tok::Word(word), Tok::LParen, ..] if !NON_CALLEE_WORDS.contains(&word.as_str()) => {
            matching_close(content, 1) == Some(content.len() - 1)
        }
        _ => false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Neighbor {
    Boundary,
    Or,
    And,
    Not,
    Other,
}

fn left_neighbor(tok: Option<&Tok>) -> Neighbor {
    match tok {
        None | Some(Tok::LParen | Tok::LBracket | Tok::Comma) => Neighbor::Boundary,
        Some(Tok::Word(word)) => match word.as_str() {
            "or" => Neighbor::Or,
            "and" => Neighbor::And,
            "not" => Neighbor::Not,
            "when" | "then" | "else" => Neighbor::Boundary,
            _ => Neighbor::Other,
        },
        _ => Neighbor::Other,
    }
}

fn right_neighbor(tok: Option<&Tok>) -> Neighbor {
    match tok {
        None | Some(Tok::RParen | Tok::RBracket | Tok::Comma) => Neighbor::Boundary,
        Some(Tok::Word(word)) => match word.as_str() {
            "or" => Neighbor::Or,
            "and" => Neighbor::And,
            "when" | "then" | "else" | "end" => Neighbor::Boundary,
            _ => Neighbor::Other,
        },
        _ => Neighbor::Other,
    }
}

/// The boolean connectives at the top level of `content` (outside any group).
fn top_level_connectives(content: &[Tok]) -> (bool, bool) {
    let (mut has_and, mut has_or) = (false, false);
    let mut depth = 0i32;
    for tok in content {
        match tok {
            Tok::LParen | Tok::LBracket => depth += 1,
            Tok::RParen | Tok::RBracket => depth -= 1,
            Tok::Word(word) if depth == 0 => match word.as_str() {
                "and" => has_and = true,
                "or" => has_or = true,
                // A CASE hides its connectives from us; keep its group.
                "case" => {
                    has_and = true;
                    has_or = true;
                }
                _ => {}
            },
            _ => {}
        }
    }
    (has_and, has_or)
}

/// Removes one redundant grouping per call site scan, repeatedly: a group around
/// a single operand, or a group whose content binds at least as tightly as the
/// boolean connectives around it.
fn drop_redundant_parens(toks: &mut Vec<Tok>) {
    let mut i = 0;
    while i < toks.len() {
        if toks[i] != Tok::LParen || is_call_paren(toks, i) {
            i += 1;
            continue;
        }
        let Some(close) = matching_close(toks, i) else {
            i += 1;
            continue;
        };
        let content = &toks[i + 1..close];
        let next = toks.get(close + 1);
        // `(expr)::type` and `(expr)[n]` bind the group as an operand.
        let postfix = matches!(next, Some(Tok::Cast | Tok::LBracket));
        let droppable = if is_single_primary(content) {
            !postfix || content.len() == 1
        } else if postfix || content.is_empty() {
            false
        } else {
            let left = left_neighbor(i.checked_sub(1).map(|k| &toks[k]));
            let right = right_neighbor(next);
            let (has_and, has_or) = top_level_connectives(content);
            if left == Neighbor::Other || right == Neighbor::Other {
                false
            } else if left == Neighbor::Not {
                !has_and && !has_or
            } else if left == Neighbor::And || right == Neighbor::And {
                !has_or
            } else {
                true
            }
        };
        if droppable {
            toks.remove(close);
            toks.remove(i);
        } else {
            i += 1;
        }
    }
}

/// `x = ANY (ARRAY[a, b])` is `x IN (a, b)`; `x <> ALL (ARRAY[a, b])` is
/// `x NOT IN (a, b)`.
fn rewrite_any_all(toks: &mut Vec<Tok>) {
    let mut i = 0;
    while i + 4 < toks.len() {
        let replacement: Option<&[&str]> = match (&toks[i], &toks[i + 1]) {
            (Tok::Op(op), Tok::Word(quantifier)) if op == "=" && quantifier == "any" => {
                Some(&["in"][..])
            }
            (Tok::Op(op), Tok::Word(quantifier)) if op == "<>" && quantifier == "all" => {
                Some(&["not", "in"][..])
            }
            _ => None,
        };
        let rewritten = replacement.and_then(|words| {
            if toks[i + 2] != Tok::LParen
                || toks[i + 3] != Tok::Word("array".to_string())
                || toks[i + 4] != Tok::LBracket
            {
                return None;
            }
            let close_paren = matching_close(toks, i + 2)?;
            let close_bracket = matching_close(toks, i + 4)?;
            if close_bracket + 1 != close_paren {
                return None;
            }
            let mut out: Vec<Tok> = words.iter().map(|w| Tok::Word((*w).to_string())).collect();
            out.push(Tok::LParen);
            out.extend_from_slice(&toks[i + 5..close_bracket]);
            out.push(Tok::RParen);
            Some((close_paren, out))
        });
        if let Some((close_paren, out)) = rewritten {
            toks.splice(i..=close_paren, out);
        }
        i += 1;
    }
}

/// `x BETWEEN lo AND hi` is `(x >= lo AND x <= hi)`, and `NOT BETWEEN` is
/// `(x < lo OR x > hi)`, which is how PostgreSQL renders them. Only single-token
/// operands are rewritten, so the bounds are unambiguous.
fn rewrite_between(toks: &mut Vec<Tok>) {
    let atom = |tok: &Tok| match tok {
        Tok::Str(_) | Tok::Num(_) => true,
        Tok::Word(word) => !is_keyword_operand_boundary(word),
        _ => false,
    };
    let word = |value: &str| Tok::Word(value.to_string());
    let op = |value: &str| Tok::Op(value.to_string());
    let mut i = 0;
    while i + 4 < toks.len() {
        let negated = toks[i + 1] == word("not");
        let between = i + 1 + usize::from(negated);
        if between + 3 < toks.len()
            && atom(&toks[i])
            && toks[between] == word("between")
            && atom(&toks[between + 1])
            && toks[between + 2] == word("and")
            && atom(&toks[between + 3])
            // `a BETWEEN 1 AND 5 + 1` has a compound upper bound: leave it.
            && !matches!(toks.get(between + 4), Some(Tok::Op(_) | Tok::Cast | Tok::LBracket | Tok::LParen))
            // `a + b BETWEEN ..` has a compound subject: leave it.
            && !matches!(i.checked_sub(1).map(|k| &toks[k]), Some(Tok::Op(_) | Tok::Cast))
        {
            let (subject, low, high) = (
                toks[i].clone(),
                toks[between + 1].clone(),
                toks[between + 3].clone(),
            );
            let (low_op, joiner, high_op) = if negated {
                ("<", "or", ">")
            } else {
                (">=", "and", "<=")
            };
            let out = vec![
                Tok::LParen,
                subject.clone(),
                op(low_op),
                low,
                word(joiner),
                subject,
                op(high_op),
                high,
                Tok::RParen,
            ];
            toks.splice(i..=between + 3, out);
        }
        i += 1;
    }
}

/// `x IN (a)` is `x = a` and `x NOT IN (a)` is `x <> a`: PostgreSQL renders a
/// one-element list as the plain comparison.
fn rewrite_single_in(toks: &mut Vec<Tok>) {
    let mut i = 0;
    while i + 1 < toks.len() {
        if toks[i] != Tok::Word("in".to_string()) || toks[i + 1] != Tok::LParen {
            i += 1;
            continue;
        }
        let Some(close) = matching_close(toks, i + 1) else {
            i += 1;
            continue;
        };
        let content = &toks[i + 2..close];
        let single = is_single_primary(content)
            && !matches!(content.first(), Some(Tok::Word(word)) if word == "select");
        if !single {
            i += 1;
            continue;
        }
        let negated = i > 0 && toks[i - 1] == Tok::Word("not".to_string());
        let start = if negated { i - 1 } else { i };
        let mut out = vec![Tok::Op(if negated { "<>" } else { "=" }.to_string())];
        out.extend_from_slice(content);
        toks.splice(start..=close, out);
        i = start + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::canonical_check_expression as key;

    fn assert_same(declared: &str, rendered: &str) {
        assert_eq!(
            key(declared),
            key(rendered),
            "declared `{declared}` and rendered `{rendered}` must compare equal"
        );
    }

    /// Each pair is a rule as written and as `pg_get_expr` returns it (first
    /// parse, then after a dump and restore re-parsed the rendering).
    #[test]
    fn postgres_renderings_compare_equal_to_the_declared_rule() {
        for (declared, rendered) in [
            (
                "kind IN ('test','medicine')",
                "((kind)::text = ANY ((ARRAY['test'::character varying, 'medicine'::character varying])::text[]))",
            ),
            (
                "kind IN ('test','medicine')",
                "((kind)::text = ANY (ARRAY[('test'::character varying)::text, ('medicine'::character varying)::text]))",
            ),
            (
                "kind IN ('test', 'medicine')",
                "(kind = ANY (ARRAY['test'::text, 'medicine'::text]))",
            ),
            (
                "status NOT IN ('deleted')",
                "((status)::text <> 'deleted'::text)",
            ),
            (
                "status NOT IN ('a', 'b')",
                "((status)::text <> ALL ((ARRAY['a'::character varying, 'b'::character varying])::text[]))",
            ),
            ("kind IN ('only')", "((kind)::text = 'only'::text)"),
            ("price >= 0", "(price >= (0)::numeric)"),
            ("price > -1", "(price > ('-1'::integer)::numeric)"),
            ("qty > -1", "(qty > '-1'::integer)"),
            ("char_length(name) > 0", "(char_length((name)::text) > 0)"),
            (
                "a > 0 AND b > 0 AND c <> ''",
                "((a > 0) AND (b > 0) AND (c <> ''::text))",
            ),
            (
                "a > 0 OR b > 0 AND c > 0",
                "((a > 0) OR ((b > 0) AND (c > 0)))",
            ),
            (
                "score BETWEEN 0 AND 100",
                "((score >= 0) AND (score <= 100))",
            ),
            (
                "score NOT BETWEEN 0 AND 100",
                "((score < 0) OR (score > 100))",
            ),
            ("email LIKE '%@%'", "((email)::text ~~ '%@%'::text)"),
            ("email NOT ILIKE '%x%'", "((email)::text !~~* '%x%'::text)"),
            ("\"order\" > 0", "(\"order\" > 0)"),
            (
                "ends_at IS NULL OR ends_at > starts_at",
                "((ends_at IS NULL) OR (ends_at > starts_at))",
            ),
            ("NOT (a = b)", "(NOT (a = b))"),
            ("a != b", "(a <> b)"),
            ("Status   =  'Active'", "((status)::text = 'Active'::text)"),
        ] {
            assert_same(declared, rendered);
        }
    }

    /// The key only absorbs PostgreSQL's own rewriting: a different rule stays
    /// different.
    #[test]
    fn genuinely_different_rules_stay_different() {
        for (left, right) in [
            ("kind IN ('test','medicine')", "kind IN ('test')"),
            ("kind IN ('test','medicine')", "kind IN ('test','Medicine')"),
            ("status = 'Active'", "status = 'ACTIVE'"),
            ("(a OR b) AND c", "a OR b AND c"),
            ("(a + b) * c > 0", "a + b * c > 0"),
            ("NOT (a AND b)", "NOT a AND b"),
            ("price > 0", "price >= 0"),
            ("price > -1", "price > 1"),
            ("lower(name)::text <> ''", "lower(name) <> ''"),
            ("kind IN ('a','b')", "kind NOT IN ('a','b')"),
            ("score BETWEEN 0 AND 100", "score BETWEEN 0 AND 10"),
        ] {
            assert_ne!(key(left), key(right), "`{left}` vs `{right}`");
        }
    }

    #[test]
    fn literals_keep_their_case_and_spacing() {
        assert_eq!(key("note = 'A  b'"), "note = 'A  b'");
        assert_eq!(key("note = 'it''s'"), "note = 'it''s'");
    }

    #[test]
    fn rendering_is_idempotent() {
        let rendered = "((kind)::text = ANY ((ARRAY['test'::character varying, 'medicine'::character varying])::text[]))";
        let once = key(rendered);
        assert_eq!(key(&once), once);
    }
}
