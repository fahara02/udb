//! Refresh-token family domain primitives.
//!
//! Persistence and rotation are handled by the auth service adapter; this module
//! owns the credential format so parsing/formatting can be tested without tonic
//! request/response types or a database.

/// Opaque prefix for a token-family refresh credential.
///
/// Deliberately not `sess_` or `udbk_`: raw session IDs and API keys are separate
/// credential surfaces and must never be accepted as token-family credentials.
pub const REFRESH_TOKEN_PREFIX: &str = "rt_";

/// Parsed `rt_<family_id>.<jti>` refresh credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshTokenParts {
    pub family_id: String,
    pub jti: String,
}

/// Format an opaque token-family refresh credential.
pub fn format_refresh_token(family_id: &str, jti: &str) -> String {
    format!("{REFRESH_TOKEN_PREFIX}{family_id}.{jti}")
}

/// Parse `rt_<family_id>.<jti>` into its domain parts.
///
/// Returns `None` for values from other credential surfaces (legacy session IDs,
/// API keys, empty input, malformed token-family values).
pub fn parse_refresh_token(token: &str) -> Option<RefreshTokenParts> {
    let body = token.strip_prefix(REFRESH_TOKEN_PREFIX)?;
    let (family_id, jti) = body.split_once('.')?;
    if family_id.is_empty() || jti.is_empty() {
        return None;
    }
    Some(RefreshTokenParts {
        family_id: family_id.to_string(),
        jti: jti.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_token_round_trips_family_and_jti() {
        let token = format_refresh_token("fam-123", "jti-abc");
        let parsed = parse_refresh_token(&token).expect("valid refresh token");

        assert_eq!(parsed.family_id, "fam-123");
        assert_eq!(parsed.jti, "jti-abc");
    }

    #[test]
    fn parse_refresh_token_rejects_other_credential_surfaces() {
        assert!(parse_refresh_token("sess_deadbeef").is_none());
        assert!(parse_refresh_token("udbk_prefix.secret").is_none());
        assert!(parse_refresh_token("rt_").is_none());
        assert!(parse_refresh_token("rt_fam-only").is_none());
        assert!(parse_refresh_token("rt_.jti").is_none());
        assert!(parse_refresh_token("rt_fam.").is_none());
        assert!(parse_refresh_token("").is_none());
    }
}
