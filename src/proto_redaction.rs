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
//! The generated set is the same `OUTPUT_VIEW_STORAGE_ONLY` set the descriptor
//! no-leak gate (`descriptor_manifest::tests::storage_only_fields_match_no_leak_coverage_set`)
//! verifies, so codegen and gate cannot disagree.

include!(concat!(env!("OUT_DIR"), "/dto_redaction.rs"));
