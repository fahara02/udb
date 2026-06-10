//! Signing-key registry domain constants and state predicates.

pub const DEFAULT_SIGNING_ALGORITHM: &str = "RS256";

pub const STATE_ACTIVE: &str = "ACTIVE";
pub const STATE_VERIFYING: &str = "VERIFYING";
pub const STATE_RETIRED: &str = "RETIRED";
pub const STATE_COMPROMISED: &str = "COMPROMISED";

pub const EMERGENCY_COMPROMISE_REASON: &str = "emergency_compromise";

/// States whose public material can be included in JWKS.
pub fn jwks_publishable_state(state: &str) -> bool {
    matches!(state, STATE_ACTIVE | STATE_VERIFYING)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwks_excludes_retired_and_compromised_keys() {
        assert!(jwks_publishable_state(STATE_ACTIVE));
        assert!(jwks_publishable_state(STATE_VERIFYING));
        assert!(!jwks_publishable_state(STATE_RETIRED));
        assert!(!jwks_publishable_state(STATE_COMPROMISED));
    }
}
