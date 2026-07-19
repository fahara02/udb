//! Storage-independent user-profile helpers for native authn.

fn value_string<'a>(value: &'a serde_json::Value, keys: &[&str]) -> Option<&'a str> {
    let object = value.as_object()?;
    for key in keys {
        if let Some(raw) = object.get(*key).and_then(|v| v.as_str()) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed);
            }
        }
    }
    None
}

fn parse_scopes(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    let mut scopes = Vec::new();
    if trimmed.starts_with('[') {
        if let Ok(serde_json::Value::Array(items)) =
            serde_json::from_str::<serde_json::Value>(trimmed)
        {
            for item in items {
                if let Some(scope) = item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    let scope = scope.to_string();
                    if !scopes.contains(&scope) {
                        scopes.push(scope);
                    }
                }
            }
            return scopes;
        }
    }
    for scope in trimmed
        .split([',', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
    {
        let scope = scope.to_string();
        if !scopes.contains(&scope) {
            scopes.push(scope);
        }
    }
    scopes
}

/// Resolve the immutable service identity stored in a SERVICE_ACCOUNT profile.
pub fn service_identity_from_profile_attributes_json(raw: &str) -> String {
    let value = serde_json::from_str(raw).unwrap_or_else(|_| serde_json::json!({}));
    value_string(
        &value,
        &[
            "service_identity",
            "serviceIdentity",
            "managed_service_identity",
            "managedServiceIdentity",
        ],
    )
    .unwrap_or_default()
    .to_string()
}

/// Resolve the reviewed service-account scope grant carried by a native user
/// profile. The values may be comma/space separated or a JSON string array.
pub fn service_scopes_from_profile_attributes_json(raw: &str) -> Vec<String> {
    let value = serde_json::from_str(raw).unwrap_or_else(|_| serde_json::json!({}));
    value_string(
        &value,
        &[
            "approved_scopes",
            "approvedScopes",
            "allowed_scopes",
            "allowedScopes",
            "service_scopes",
            "serviceScopes",
            "udb_scopes",
            "udbScopes",
            "scopes",
            "scope",
        ],
    )
    .map(parse_scopes)
    .unwrap_or_default()
}

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

    #[test]
    fn service_profile_extracts_identity_and_scopes() {
        let raw = r#"{
            "serviceIdentity": "ambulife.authn",
            "approvedScopes": "[\"udb:read\", \"udb:write\", \"udb:read\"]"
        }"#;
        assert_eq!(
            service_identity_from_profile_attributes_json(raw),
            "ambulife.authn"
        );
        assert_eq!(
            service_scopes_from_profile_attributes_json(raw),
            vec!["udb:read".to_string(), "udb:write".to_string()]
        );
    }
}
