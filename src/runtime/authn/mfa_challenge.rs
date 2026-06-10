//! MFA challenge domain rules that do not depend on tonic/proto adapters.

use uuid::Uuid;

/// Pending OTPs expire after this many failed submissions.
pub const MAX_OTP_ATTEMPTS: i32 = 5;

/// Draw a 6-digit OTP from the CSPRNG-backed UUID generator.
///
/// The `u128 % 1_000_000` modulo bias is negligible for a human-entered OTP.
/// Codes are hashed before storage and are not intended to be recomputable.
pub fn generate_otp_code() -> String {
    let n = (Uuid::new_v4().as_u128() % 1_000_000) as u32;
    format!("{n:06}")
}

/// A CSPRNG-backed, human-transcribable single-use recovery code.
///
/// Shape: three groups of four lowercase hex chars, about 48 bits of entropy.
pub fn generate_recovery_code() -> String {
    let raw = Uuid::new_v4().simple().to_string();
    format!("{}-{}-{}", &raw[0..4], &raw[4..8], &raw[8..12])
}

pub fn attempts_remaining(attempt_count: i32) -> i32 {
    (MAX_OTP_ATTEMPTS - attempt_count).max(0)
}

pub fn should_expire_after_failed_attempt(attempt_count: i32) -> bool {
    attempt_count >= MAX_OTP_ATTEMPTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otp_codes_are_six_digits() {
        let code = generate_otp_code();

        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|ch| ch.is_ascii_digit()));
    }

    #[test]
    fn recovery_codes_are_grouped_hex() {
        let code = generate_recovery_code();
        let parts: Vec<_> = code.split('-').collect();

        assert_eq!(parts.len(), 3);
        assert!(parts.iter().all(|part| part.len() == 4));
        assert!(
            parts
                .iter()
                .flat_map(|part| part.chars())
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
        );
    }

    #[test]
    fn attempt_limit_rules_enforce_max_otp_attempts() {
        // `attempts_remaining` counts down from the max and floors at zero.
        assert_eq!(attempts_remaining(0), MAX_OTP_ATTEMPTS);
        assert_eq!(attempts_remaining(MAX_OTP_ATTEMPTS - 1), 1);
        assert_eq!(attempts_remaining(MAX_OTP_ATTEMPTS), 0);
        assert_eq!(
            attempts_remaining(MAX_OTP_ATTEMPTS + 3),
            0,
            "remaining attempts must never go negative"
        );

        // A challenge expires the moment it reaches the max failed attempts.
        assert!(!should_expire_after_failed_attempt(MAX_OTP_ATTEMPTS - 1));
        assert!(should_expire_after_failed_attempt(MAX_OTP_ATTEMPTS));
        assert!(should_expire_after_failed_attempt(MAX_OTP_ATTEMPTS + 1));
    }
}
