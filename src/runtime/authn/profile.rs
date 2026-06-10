//! Storage-independent user-profile helpers for native authn.

/// Redact sensitive keys from a user profile JSON object before it leaves the
/// authn domain. Invalid/non-object input degrades to `{}`.
pub fn redact_profile_attributes_json(raw: &str) -> String {
    fn scrub(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map.iter_mut() {
                    let lower = key.to_ascii_lowercase();
                    let sensitive = lower.starts_with('_')
                        || lower.contains("internal")
                        || lower.contains("sensitive")
                        || lower.contains("secret")
                        || lower.contains("token")
                        || lower.contains("password")
                        || lower.contains("credential")
                        || lower.contains("recovery_code");
                    if sensitive {
                        *child = serde_json::Value::String("[REDACTED]".to_string());
                    } else {
                        scrub(child);
                    }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    scrub(item);
                }
            }
            _ => {}
        }
    }

    let mut value = serde_json::from_str(raw).unwrap_or_else(|_| serde_json::json!({}));
    if !value.is_object() {
        return "{}".to_string();
    }
    scrub(&mut value);
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_redaction_scrubs_sensitive_nested_keys() {
        let redacted = redact_profile_attributes_json(
            r#"{"tier":"gold","nested":{"api_token":"abc","items":[{"secret_note":"x"}]}}"#,
        );
        let value: serde_json::Value = serde_json::from_str(&redacted).unwrap();
        assert_eq!(value["tier"], "gold");
        assert_eq!(value["nested"]["api_token"], "[REDACTED]");
        assert_eq!(value["nested"]["items"][0]["secret_note"], "[REDACTED]");
    }

    #[test]
    fn profile_redaction_rejects_non_object_input() {
        assert_eq!(redact_profile_attributes_json(r#"["secret"]"#), "{}");
        assert_eq!(redact_profile_attributes_json("not json"), "{}");
    }
}
