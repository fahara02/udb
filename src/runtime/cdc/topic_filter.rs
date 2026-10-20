//! SQL form of a CDC topic pattern. Topic patterns are matched with
//! `WildMatch` everywhere in memory: `*` spans any run of characters (dots
//! included), `?` is exactly one character, and matching is case-sensitive.
//! A journal read that filters only in memory has to fetch every row of every
//! topic to find the few it wants; this compiles the same language into a
//! predicate PostgreSQL can answer from an index.
//!
//! A pattern with a literal prefix becomes a byte-order range over
//! `topic COLLATE "C"` (served by the journal's `(topic COLLATE "C",
//! journal_position)` index), narrowed by `LIKE` when the pattern has more
//! wildcards than one trailing `*`. A pattern that starts with a wildcard has no
//! range to scan and stays an in-memory filter.

/// A compiled topic pattern. Callers keep the in-memory `WildMatch` check as a
/// backstop; this only decides which rows the database returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TopicFilter {
    /// Every topic, or a pattern with no literal prefix to scan by.
    All,
    /// A pattern without wildcards names exactly one topic.
    Exact(String),
    /// Topics starting with `lo`: `lo <= topic < hi` in byte order. `like` is
    /// the remaining pattern when it is more than the prefix plus one `*`.
    Prefix {
        lo: String,
        hi: String,
        like: Option<String>,
    },
}

impl TopicFilter {
    pub(crate) fn compile(pattern: &str) -> Self {
        if pattern.is_empty() || pattern == "*" {
            return Self::All;
        }
        let Some(wildcard) = pattern.find(['*', '?']) else {
            return Self::Exact(pattern.to_string());
        };
        let prefix = &pattern[..wildcard];
        let Some(hi) = prefix_successor(prefix) else {
            return Self::All;
        };
        let like = if pattern.len() == prefix.len() + 1 && pattern.ends_with('*') {
            None
        } else {
            Some(like_pattern(pattern))
        };
        Self::Prefix {
            lo: prefix.to_string(),
            hi,
            like,
        }
    }

    /// The predicate over `column`, numbering its parameters from
    /// `first_bind`, and the values to bind in that order.
    pub(crate) fn sql(&self, column: &str, first_bind: usize) -> (String, Vec<String>) {
        match self {
            Self::All => ("TRUE".to_string(), Vec::new()),
            Self::Exact(topic) => (format!("{column} = ${first_bind}"), vec![topic.clone()]),
            Self::Prefix { lo, hi, like } => {
                let mut predicate = format!(
                    "{column} COLLATE \"C\" >= ${first_bind} AND {column} COLLATE \"C\" < ${}",
                    first_bind + 1
                );
                let mut binds = vec![lo.clone(), hi.clone()];
                if let Some(like) = like {
                    predicate.push_str(&format!(
                        " AND {column} LIKE ${} ESCAPE '\\'",
                        first_bind + 2
                    ));
                    binds.push(like.clone());
                }
                (predicate, binds)
            }
        }
    }
}

/// The least string greater than every string that starts with `prefix`:
/// the prefix with its last character incremented. UTF-8 byte order is code
/// point order, so this bound holds under `COLLATE "C"`. A character with no
/// successor (the end of the surrogate gap aside, only `char::MAX`) is dropped
/// and the one before it incremented instead.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = match last as u32 {
            0xD7FF => Some('\u{E000}'),
            code => char::from_u32(code + 1),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// `*` → `%`, `?` → `_`, with LIKE's own metacharacters escaped by `\`.
fn like_pattern(pattern: &str) -> String {
    let mut like = String::with_capacity(pattern.len() + 4);
    for ch in pattern.chars() {
        match ch {
            '\\' | '%' | '_' => {
                like.push('\\');
                like.push(ch);
            }
            '*' => like.push('%'),
            '?' => like.push('_'),
            _ => like.push(ch),
        }
    }
    like
}

#[cfg(test)]
mod tests {
    use super::*;
    use wildmatch::WildMatch;

    /// The SQL predicate evaluated the way PostgreSQL would: byte-order range
    /// then `LIKE ... ESCAPE '\'`.
    fn sql_matches(filter: &TopicFilter, topic: &str) -> bool {
        match filter {
            TopicFilter::All => true,
            TopicFilter::Exact(exact) => topic == exact,
            TopicFilter::Prefix { lo, hi, like } => {
                topic.as_bytes() >= lo.as_bytes()
                    && topic.as_bytes() < hi.as_bytes()
                    && like
                        .as_deref()
                        .is_none_or(|like| like_matches(&like_tokens(like), topic))
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum LikeToken {
        Any,
        One,
        Literal(char),
    }

    fn like_tokens(like: &str) -> Vec<LikeToken> {
        let mut tokens = Vec::new();
        let mut chars = like.chars();
        while let Some(ch) = chars.next() {
            tokens.push(match ch {
                '\\' => LikeToken::Literal(chars.next().expect("escape is followed by a char")),
                '%' => LikeToken::Any,
                '_' => LikeToken::One,
                other => LikeToken::Literal(other),
            });
        }
        tokens
    }

    fn like_matches(tokens: &[LikeToken], topic: &str) -> bool {
        let text: Vec<char> = topic.chars().collect();
        fn go(tokens: &[LikeToken], text: &[char]) -> bool {
            match tokens.split_first() {
                None => text.is_empty(),
                Some((LikeToken::Any, rest)) => {
                    (0..=text.len()).any(|skip| go(rest, &text[skip..]))
                }
                Some((LikeToken::One, rest)) => !text.is_empty() && go(rest, &text[1..]),
                Some((LikeToken::Literal(ch), rest)) => {
                    text.first() == Some(ch) && go(rest, &text[1..])
                }
            }
        }
        go(tokens, &text)
    }

    #[test]
    fn literal_pattern_is_exact() {
        let filter = TopicFilter::compile("virtued.admin_action.recorded.v1");
        assert_eq!(
            filter,
            TopicFilter::Exact("virtued.admin_action.recorded.v1".into())
        );
        let (sql, binds) = filter.sql("topic", 3);
        assert_eq!(sql, "topic = $3");
        assert_eq!(binds, vec!["virtued.admin_action.recorded.v1".to_string()]);
    }

    #[test]
    fn trailing_star_is_a_range_without_like() {
        let filter = TopicFilter::compile("virtued.*");
        assert_eq!(
            filter,
            TopicFilter::Prefix {
                lo: "virtued.".into(),
                hi: "virtued/".into(),
                like: None
            }
        );
        let (sql, binds) = filter.sql("j.topic", 4);
        assert_eq!(
            sql,
            "j.topic COLLATE \"C\" >= $4 AND j.topic COLLATE \"C\" < $5"
        );
        assert_eq!(binds, vec!["virtued.".to_string(), "virtued/".to_string()]);
    }

    #[test]
    fn inner_wildcard_adds_escaped_like() {
        let filter = TopicFilter::compile("virtued.*.admin_action.recorded.v1");
        assert_eq!(
            filter,
            TopicFilter::Prefix {
                lo: "virtued.".into(),
                hi: "virtued/".into(),
                like: Some("virtued.%.admin\\_action.recorded.v1".into())
            }
        );
        let (sql, binds) = filter.sql("topic", 3);
        assert_eq!(
            sql,
            "topic COLLATE \"C\" >= $3 AND topic COLLATE \"C\" < $4 AND topic LIKE $5 ESCAPE '\\'"
        );
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn patterns_without_a_literal_prefix_stay_in_memory() {
        for pattern in ["*", "", "?x", "*.v1"] {
            assert_eq!(TopicFilter::compile(pattern), TopicFilter::All, "{pattern}");
            assert_eq!(TopicFilter::compile(pattern).sql("topic", 1).0, "TRUE");
        }
    }

    #[test]
    fn like_metacharacters_in_topics_are_literal() {
        let filter = TopicFilter::compile("a%b_c\\d.*.x");
        let TopicFilter::Prefix { lo, like, .. } = &filter else {
            panic!("expected a prefix filter: {filter:?}");
        };
        assert_eq!(lo, "a%b_c\\d.");
        assert_eq!(like.as_deref(), Some("a\\%b\\_c\\\\d.%.x"));
        assert!(sql_matches(&filter, "a%b_c\\d.any.x"));
        assert!(!sql_matches(&filter, "aXb_c\\d.any.x"));
        assert!(!sql_matches(&filter, "a%bXc\\d.any.x"));
    }

    #[test]
    fn successor_skips_values_without_one() {
        assert_eq!(prefix_successor("ab"), Some("ac".into()));
        assert_eq!(prefix_successor("a\u{D7FF}"), Some("a\u{E000}".into()));
        assert_eq!(prefix_successor("a\u{10FFFF}"), Some("b".into()));
        assert_eq!(prefix_successor("\u{10FFFF}"), None);
        assert_eq!(prefix_successor(""), None);
    }

    /// A range filter must select exactly the topics WildMatch selects; `All`
    /// selects every topic and leaves the decision to the in-memory backstop.
    #[test]
    fn sql_form_agrees_with_wildmatch() {
        let topics = [
            "",
            "a",
            "ab",
            "abc",
            "udb.lock.acquired.v1",
            "udb.lock.released.v1",
            "udb.lockx.acquired.v1",
            "udb.authn.user.registered.v1",
            "udb/authn",
            "udb.",
            "udb",
            "virtued.admin_action.recorded.v1",
            "virtued.tenant.admin_action.recorded.v1",
            "virtued.tenant.adminXaction.recorded.v1",
            "virtued.a.b.admin_action.recorded.v1",
            "virtued.admin_action.recorded.v2",
            "Virtued.admin_action.recorded.v1",
            "a%b",
            "a_b",
            "a\\b",
            "aXb",
            "x.y.z",
            "x..z",
            "xyz",
            "é.topic",
            "éx.topic",
            "日本.v1",
        ];
        let patterns = [
            "*",
            "a",
            "a*",
            "a?",
            "a?c",
            "ab*",
            "udb.*",
            "udb.lock.*",
            "udb.lock*",
            "udb.*.v1",
            "udb.lock.?cquired.v1",
            "udb.*.*.v1",
            "udb*",
            "udb.?",
            "virtued.*",
            "virtued.*.admin_action.recorded.v1",
            "virtued.*admin_action*",
            "virtued.admin_action.recorded.v?",
            "a%*",
            "a_*",
            "a\\*",
            "a%b",
            "x.*.z",
            "x?*z",
            "x*",
            "é.*",
            "é*",
            "日*.v1",
            "*.v1",
            "?",
            "??",
        ];
        for pattern in patterns {
            let filter = TopicFilter::compile(pattern);
            let matcher = WildMatch::new(pattern);
            for topic in topics {
                if filter == TopicFilter::All {
                    assert!(sql_matches(&filter, topic));
                    continue;
                }
                assert_eq!(
                    sql_matches(&filter, topic),
                    matcher.matches(topic),
                    "pattern {pattern:?} topic {topic:?} filter {filter:?}"
                );
            }
        }
    }
}
