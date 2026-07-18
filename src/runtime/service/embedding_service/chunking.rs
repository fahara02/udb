//! Pure source-text chunking for the native `EmbeddingService`, plus the
//! composite vector-point-id scheme that carries a chunk's provenance.
//!
//! Long documents cannot be embedded as one vector — a model caps its input and
//! a single vector over thousands of characters is retrieval-poor. This splits a
//! row's text into overlapping, word-boundary-aware windows; the leader-owned
//! work emitter fans out ONE `udb.embedding.work.v1` event per chunk, and each
//! embedded chunk is stored as its own point.
//!
//! Point-id scheme (backward-compatible): a row that fits in a single chunk
//! keeps its bare `row_pk` — existing one-vector-per-row ids are unchanged — and
//! a multi-chunk row uses `{row_pk}{SEP}{seq}`. Every stored point is tagged with
//! its parent `row_pk` (`_parent_pk`) so a row change/delete erases all of that
//! row's chunks by payload filter (reusing the B.1.5 filter-delete seam), never
//! by guessing chunk ids. All functions here are pure — unit-tested without an
//! engine.

/// Separator between a parent `row_pk` and its chunk sequence in a composite
/// point id. Multi-character + punctuated so it is vanishingly unlikely to occur
/// at the tail of a real primary key, letting the parent be recovered
/// unambiguously.
pub(crate) const CHUNK_ID_SEP: &str = "#chunk:";

/// One chunk of a source row's text: its sequence within the row, the text, and
/// the character offset where it starts (provenance metadata carried in the work
/// event and the point payload).
pub(crate) struct Chunk {
    pub(crate) seq: u32,
    pub(crate) text: String,
    pub(crate) char_start: usize,
}

/// The vector point id for chunk `seq` of `parent_pk`. A single-chunk row
/// (`chunk_count <= 1`) keeps the bare parent pk so pre-chunking ids are stable;
/// a multi-chunk row gets `{parent}{SEP}{seq}`.
pub(crate) fn chunk_point_id(parent_pk: &str, seq: u32, chunk_count: usize) -> String {
    if chunk_count <= 1 {
        parent_pk.to_string()
    } else {
        format!("{parent_pk}{CHUNK_ID_SEP}{seq}")
    }
}

/// Recover `(parent_pk, chunk_seq)` from a point id. A bare id (no sentinel, or a
/// non-numeric suffix — e.g. a real pk that happens to contain the separator
/// followed by non-digits) is treated as a single-chunk / legacy point at seq 0.
pub(crate) fn parse_chunk_point_id(point_id: &str) -> (&str, u32) {
    if let Some(idx) = point_id.rfind(CHUNK_ID_SEP) {
        let seq_str = &point_id[idx + CHUNK_ID_SEP.len()..];
        if !seq_str.is_empty() && seq_str.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(seq) = seq_str.parse::<u32>() {
                return (&point_id[..idx], seq);
            }
        }
    }
    (point_id, 0)
}

/// Split `text` into at most `max_chunks` overlapping windows of at most `size`
/// characters each, preferring to cut at a whitespace boundary so a word is not
/// split. Each window after the first re-includes `overlap` characters from the
/// tail of the previous one (so context spanning a boundary is not lost);
/// `overlap` is clamped below `size` to guarantee forward progress. Char-based
/// throughout, so a multi-byte boundary never panics. Text within `size` (after
/// trimming) yields a single chunk; empty/whitespace-only text yields none.
pub(crate) fn chunk_text(text: &str, size: usize, overlap: usize, max_chunks: usize) -> Vec<Chunk> {
    let size = size.max(1);
    let overlap = overlap.min(size - 1);
    let max_chunks = max_chunks.max(1);

    // Char index -> byte offset, computed once; `boundaries[i]` is the byte start
    // of char i, and `boundaries[len]` is the string length.
    let mut boundaries: Vec<usize> = text.char_indices().map(|(b, _)| b).collect();
    boundaries.push(text.len());
    let whitespace: Vec<bool> = text.chars().map(char::is_whitespace).collect();
    let total = whitespace.len();

    if total == 0 {
        return Vec::new();
    }
    if total <= size {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        return vec![Chunk {
            seq: 0,
            text: trimmed.to_string(),
            char_start: 0,
        }];
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut seq = 0u32;
    while start < total && (chunks.len() as usize) < max_chunks {
        let hard_end = (start + size).min(total);
        // Prefer to end at a whitespace boundary within the window (never for the
        // final window, and never so early that no progress is made).
        let mut end = hard_end;
        if hard_end < total {
            if let Some(ws) = (start + 1..hard_end).rev().find(|&i| whitespace[i]) {
                end = ws;
            }
        }
        let slice = text[boundaries[start]..boundaries[end]].trim();
        if !slice.is_empty() {
            chunks.push(Chunk {
                seq,
                text: slice.to_string(),
                char_start: start,
            });
            seq += 1;
        }
        if end >= total {
            break;
        }
        // Advance with overlap, always making forward progress.
        let next = end.saturating_sub(overlap).max(start + 1);
        start = next;
    }
    chunks
}

/// Chunk a source row's text using the operator-tuned config
/// ([`super::config::embedding_chunk_size`] / overlap / max-chunks-per-row).
pub(crate) fn chunk_source_text(text: &str) -> Vec<Chunk> {
    chunk_text(
        text,
        super::config::embedding_chunk_size(),
        super::config::embedding_chunk_overlap(),
        super::config::embedding_max_chunks_per_row(),
    )
}
