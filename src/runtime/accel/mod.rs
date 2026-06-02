//! `runtime/accel` — scalar-first acceleration layer (D.5).
//!
//! Safe public wrappers over the hot codecs/checksums. The DEFAULT path is
//! [`scalar`] — no `unsafe`, works on every target. Optional SIMD impls (D.6,
//! behind `simd-codecs` / `simd-checksum`) and assembly (D.7, `asm-kernels`) may
//! override a wrapper, but ONLY after passing the scalar equivalence tests, and
//! they always keep the scalar fallback. CPU feature detection is cached once via
//! [`detect::cpu_features`].
//!
//! Wrapping the codecs here (rather than calling `base64`/checksum crates inline)
//! gives a single swap-in point: D.6 changes happen in this module + its features,
//! not scattered across executors.

pub mod detect;
pub mod scalar;

pub use detect::cpu_features;

// D.6: maintained-SIMD-crate evaluation. The optional impls live behind
// off-by-default features (`simd-codecs` → `base64-simd`, `simd-checksum` →
// `crc32fast`) and are proven byte-identical to scalar by the equivalence tests
// below. ADOPTING them (turning the feature on by default) is gated on benchmark
// results per D.6 — that decision waits for the perf-bench phase; until then the
// scalar path ships, and the SIMD path is opt-in + correctness-verified.

/// base64 STANDARD encode.
#[cfg(feature = "simd-codecs")]
pub fn base64_encode(input: &[u8]) -> String {
    base64_simd::STANDARD.encode_to_string(input)
}
#[cfg(not(feature = "simd-codecs"))]
pub fn base64_encode(input: &[u8]) -> String {
    scalar::base64_encode(input)
}

/// base64 STANDARD decode. Error flattened to `String` so the scalar and SIMD
/// backends present one signature to callers.
#[cfg(feature = "simd-codecs")]
pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    base64_simd::STANDARD
        .decode_to_vec(input.as_bytes())
        .map_err(|e| e.to_string())
}
#[cfg(not(feature = "simd-codecs"))]
pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    scalar::base64_decode(input).map_err(|e| e.to_string())
}

/// CRC-32 object-chunk integrity checksum (non-cryptographic; security/audit
/// hashes stay on `sha2`).
#[cfg(feature = "simd-checksum")]
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}
#[cfg(not(feature = "simd-checksum"))]
pub fn crc32(data: &[u8]) -> u32 {
    scalar::crc32(data)
}

// ── SIMD↔scalar equivalence (run with the respective feature enabled) ─────────

#[cfg(all(test, feature = "simd-codecs"))]
mod simd_codecs_equivalence {
    use super::scalar;

    #[test]
    fn base64_simd_matches_scalar() {
        for len in [0usize, 1, 2, 3, 7, 16, 64, 255, 4096] {
            let data: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
            let simd = super::base64_encode(&data);
            assert_eq!(
                simd,
                scalar::base64_encode(&data),
                "encode mismatch @ {len}"
            );
            assert_eq!(
                super::base64_decode(&simd).unwrap(),
                data,
                "decode mismatch @ {len}"
            );
        }
    }
}

#[cfg(all(test, feature = "simd-checksum"))]
mod simd_checksum_equivalence {
    use super::scalar;

    #[test]
    fn crc32fast_matches_scalar() {
        for s in [b"".as_slice(), b"123456789", b"The quick brown fox"] {
            assert_eq!(
                super::crc32(s),
                scalar::crc32(s),
                "crc32 mismatch for {s:?}"
            );
        }
    }
}
