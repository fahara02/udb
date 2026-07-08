//! Descriptor-driven outbound DTO redaction (final_task.md §7).
//!
//! `build.rs` (`render_storage_only_redaction`) parses the proto source for every
//! field annotated `output_view: OUTPUT_VIEW_STORAGE_ONLY` and emits a
//! [`RedactStorageOnly`] impl per affected entity message that blanks those
//! fields. Outbound DTO mappers call `.redact_storage_only()` on the built proto
//! before returning it, so a newly-annotated storage-only column is blanked
//! STRUCTURALLY — generated from the contract, not hand-maintained in each mapper
//! and not merely test-enforced.
//!
//! The generated coverage map is checked against the descriptor-derived
//! `OUTPUT_VIEW_STORAGE_ONLY` set, so codegen and the no-leak gate cannot
//! disagree.

include!(concat!(env!("OUT_DIR"), "/dto_redaction.rs"));
