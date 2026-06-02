//! Scalar-first codec/checksum implementations (D.5).
//!
//! These are safe (no `unsafe`), always-available, and work on every platform.
//! They are BOTH the default path AND the correctness oracle: any SIMD (D.6) or
//! assembly (D.7) variant added behind a feature flag must produce byte-for-byte
//! identical output, proven against these in equivalence tests.

use base64::Engine as _;

/// base64 STANDARD encode (the alphabet UDB's SQL/object cells use).
pub fn base64_encode(input: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(input)
}

/// base64 STANDARD decode.
pub fn base64_decode(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    base64::engine::general_purpose::STANDARD.decode(input)
}

/// CRC-32/ISO-HDLC (reflected, poly `0xEDB88320`) — a NON-cryptographic checksum
/// for object-chunk integrity only. Security/audit hashes stay on `sha2`; never
/// substitute this for those (D.6 rule).
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_including_binary() {
        let data = b"\x00\x01\x02 hello world \xff\xfe\x80 binary";
        let encoded = base64_encode(data);
        assert_eq!(base64_decode(&encoded).expect("decode"), data);
    }

    #[test]
    fn crc32_matches_standard_check_values() {
        // CRC-32/ISO-HDLC well-known vectors.
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }
}
