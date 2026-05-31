//! Sort, projection, and pagination (U2 step 1).
//!
//! These three travel together because compilers emit them as one trailing
//! clause group (`ORDER BY … LIMIT … OFFSET …`, `find(…, {sort, projection,
//! skip, limit})`, Qdrant `with_payload + with_vectors + limit + offset`).

use serde::{Deserialize, Serialize};

/// Selected fields to return. Empty `fields` = "everything the backend has",
/// non-empty = explicit projection list. `include_metadata` surfaces
/// backend-specific bookkeeping (`_id` in Mongo, `ctid` in Postgres, score
/// in Qdrant) — opt-in because most callers don't want it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalProjection {
    pub fields: Vec<String>,
    #[serde(default)]
    pub include_metadata: bool,
}

impl LogicalProjection {
    pub fn fields(fields: impl IntoIterator<Item = String>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
            include_metadata: false,
        }
    }

    pub fn is_select_all(&self) -> bool {
        self.fields.is_empty()
    }
}

/// One ORDER BY clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSort {
    pub field: String,
    pub direction: SortDirection,
    #[serde(default)]
    pub nulls: NullOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Where NULLs go in a sorted result. `Default` defers to the backend
/// (Postgres: NULLs sort last by default for ASC, first for DESC; Mongo:
/// NULLs sort first; ClickHouse: NULLs sort last).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullOrder {
    First,
    Last,
    #[default]
    Default,
}

impl SortDirection {
    pub fn token(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

/// Pagination control. Compilers pick the cheapest form their backend
/// supports — keyset (cursor) is preferred when available; offset-based is
/// the fallback for backends without a stable cursor.
///
/// Mixing `offset` and `cursor` is an error (caught at compile time, not
/// here, because the right error message depends on which backend).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalPagination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// Opaque cursor (Mongo `_id`-after, Qdrant `offset_id`, S3
    /// `continuation_token`). The compiler treats this as backend-specific
    /// and only validates non-emptiness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl LogicalPagination {
    pub fn limit(n: u32) -> Self {
        Self {
            limit: Some(n),
            ..Self::default()
        }
    }

    pub fn page(offset: u64, limit: u32) -> Self {
        Self {
            limit: Some(limit),
            offset: Some(offset),
            cursor: None,
        }
    }

    pub fn uses_cursor(&self) -> bool {
        self.cursor.as_ref().is_some_and(|c| !c.is_empty())
    }

    pub fn uses_offset(&self) -> bool {
        self.offset.is_some_and(|o| o > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_default_is_select_all() {
        let p = LogicalProjection::default();
        assert!(p.is_select_all());
        assert!(p.fields.is_empty());
        assert!(!p.include_metadata);
    }

    #[test]
    fn pagination_mode_helpers() {
        let p = LogicalPagination::page(20, 10);
        assert!(p.uses_offset());
        assert!(!p.uses_cursor());

        let c = LogicalPagination {
            cursor: Some("abc".into()),
            limit: Some(10),
            ..Default::default()
        };
        assert!(c.uses_cursor());
        assert!(!c.uses_offset());

        let empty = LogicalPagination::default();
        assert!(!empty.uses_cursor());
        assert!(!empty.uses_offset());
    }

    #[test]
    fn sort_direction_token() {
        assert_eq!(SortDirection::Asc.token(), "asc");
        assert_eq!(SortDirection::Desc.token(), "desc");
    }
}
