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
    pub(crate) char_end: usize,
    pub(crate) token_start: usize,
    pub(crate) token_count: usize,
}

fn digest_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(&mut out, "{byte:02x}");
            out
        })
}

pub(crate) fn chunk_content_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};

    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    digest_hex(&Sha256::digest(normalized.as_bytes()))
}

pub(crate) fn deterministic_work_item_id(
    tenant_id: &str,
    source_name: &str,
    parent_pk: &str,
    chunk_seq: u32,
    doc_version: &str,
    chunk_hash: &str,
) -> String {
    use sha2::{Digest, Sha256};

    let identity = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        tenant_id.trim(),
        source_name.trim(),
        parent_pk.trim(),
        chunk_seq,
        doc_version.trim(),
        chunk_hash
    );
    digest_hex(&Sha256::digest(identity.as_bytes()))
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
        let leading = text
            .chars()
            .take_while(|value| value.is_whitespace())
            .count();
        return vec![Chunk {
            seq: 0,
            text: trimmed.to_string(),
            char_start: leading,
            char_end: leading + trimmed.chars().count(),
            token_start: 0,
            token_count: trimmed.split_whitespace().count(),
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
        let raw_slice = &text[boundaries[start]..boundaries[end]];
        let slice = raw_slice.trim();
        if !slice.is_empty() {
            let leading = raw_slice
                .chars()
                .take_while(|value| value.is_whitespace())
                .count();
            let char_start = start + leading;
            chunks.push(Chunk {
                seq,
                text: slice.to_string(),
                char_start,
                char_end: char_start + slice.chars().count(),
                token_start: text[..boundaries[char_start]].split_whitespace().count(),
                token_count: slice.split_whitespace().count(),
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

/// Token-window chunking used once a registered model supplies its tokenizer
/// envelope. The broker does not implement provider-specific BPE; it counts
/// Unicode whitespace-delimited tokens and applies a 15% safety margin against
/// the provider maximum. Provider-exact validation remains in the sidecar.
/// Paragraph and line endings are preferred near a window boundary so markdown,
/// prose, and code blocks remain useful units before the fixed fallback.
pub(crate) fn chunk_text_tokens(
    text: &str,
    requested_tokens: usize,
    overlap_tokens: usize,
    max_input_tokens: usize,
    max_chunks: usize,
) -> Vec<Chunk> {
    let provider_safe_max = max_input_tokens
        .saturating_mul(85)
        .checked_div(100)
        .unwrap_or(0);
    let window = requested_tokens.max(1).min(provider_safe_max.max(1));
    let overlap = overlap_tokens.min(window.saturating_sub(1));
    let max_chunks = max_chunks.max(1);
    let tokens = text
        .split_whitespace()
        .filter_map(|token| {
            let start = token.as_ptr() as usize - text.as_ptr() as usize;
            let end = start + token.len();
            (end <= text.len()).then_some((start, end))
        })
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start_token = 0usize;
    while start_token < tokens.len() && chunks.len() < max_chunks {
        let hard_end = (start_token + window).min(tokens.len());
        let mut end_token = hard_end;
        if hard_end < tokens.len() {
            let lower = start_token + (window.saturating_mul(4) / 5).max(1);
            if let Some(boundary) = (lower..hard_end).rev().find(|index| {
                let gap_start = tokens[*index - 1].1;
                let gap_end = tokens[*index].0;
                let gap_is_block = text
                    .get(gap_start..gap_end)
                    .is_some_and(|gap| gap.contains("\n\n") || gap.contains('\n'));
                let previous = text
                    .get(tokens[*index - 1].0..tokens[*index - 1].1)
                    .unwrap_or_default()
                    .trim_end_matches(|value: char| {
                        matches!(value, '\"' | '\'' | ')' | ']' | '*' | '_')
                    });
                let next = text
                    .get(tokens[*index].0..tokens[*index].1)
                    .unwrap_or_default();
                gap_is_block
                    || previous
                        .chars()
                        .next_back()
                        .is_some_and(|value| matches!(value, '.' | '?' | '!' | ';' | '}'))
                    || next.starts_with('#')
                    || next.starts_with("```")
            }) {
                end_token = boundary;
            }
        }
        let byte_start = tokens[start_token].0;
        let byte_end = tokens[end_token - 1].1;
        let value = text[byte_start..byte_end].trim();
        if !value.is_empty() {
            chunks.push(Chunk {
                seq: chunks.len() as u32,
                text: value.to_string(),
                char_start: text[..byte_start].chars().count(),
                char_end: text[..byte_end].chars().count(),
                token_start: start_token,
                token_count: end_token - start_token,
            });
        }
        if end_token >= tokens.len() {
            break;
        }
        start_token = end_token.saturating_sub(overlap).max(start_token + 1);
    }
    chunks
}

pub(crate) fn chunk_source_text_for_model(
    text: &str,
    model: &super::model::StoredModel,
) -> Vec<Chunk> {
    if model.chunking_strategy.eq_ignore_ascii_case("CHARACTER") {
        return chunk_source_text(text);
    }
    chunk_text_tokens(
        text,
        model.chunk_tokens.max(1) as usize,
        model.chunk_overlap_tokens.max(0) as usize,
        model.max_input_tokens.max(1) as usize,
        super::config::embedding_max_chunks_per_row(),
    )
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
