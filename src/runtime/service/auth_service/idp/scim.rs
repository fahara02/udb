//! SCIM 2.0 (RFC 7643/7644) parsing helpers — pure, DB-free logic.
//!
//! The handlers persist into the native external_identities / user tables; these
//! helpers translate the SCIM JSON envelope into the fields the store needs and
//! apply PATCH operations to a SCIM resource document. Group→role binding is
//! NOT done here — it goes exclusively through the configured group mapping in
//! [`super::mapping::map_groups_to_roles`].

use serde_json::{Value, json};

/// Normalized view of a SCIM User resource.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScimUserView {
    pub user_name: String,
    pub display_name: String,
    pub email: String,
    pub active: bool,
    pub groups: Vec<String>,
}

/// Parse a SCIM 2.0 User JSON body into a normalized view. Tolerates the common
/// shapes: `userName`, `displayName`/`name.formatted`, `emails[].value`
/// (primary first), `active`, `groups[].value`.
pub fn parse_scim_user(body: &str) -> Result<ScimUserView, String> {
    let v: Value =
        serde_json::from_str(body.trim()).map_err(|e| format!("invalid SCIM user JSON: {e}"))?;
    if !v.is_object() {
        return Err("SCIM user must be a JSON object".to_string());
    }
    let user_name = v
        .get("userName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let display_name = v
        .get("displayName")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            v.get("name")
                .and_then(|n| n.get("formatted"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_default();
    let email = primary_email(&v);
    // SCIM `active` defaults to true when absent (RFC 7643 §4.1.1).
    let active = v.get("active").and_then(Value::as_bool).unwrap_or(true);
    let groups = string_values(v.get("groups"));
    if user_name.trim().is_empty() {
        return Err("SCIM user is missing required userName".to_string());
    }
    Ok(ScimUserView {
        user_name,
        display_name,
        email,
        active,
        groups,
    })
}

/// Normalized view of a SCIM Group resource.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScimGroupView {
    pub display_name: String,
    pub members: Vec<String>,
}

pub fn parse_scim_group(body: &str) -> Result<ScimGroupView, String> {
    let v: Value =
        serde_json::from_str(body.trim()).map_err(|e| format!("invalid SCIM group JSON: {e}"))?;
    let display_name = v
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if display_name.trim().is_empty() {
        return Err("SCIM group is missing required displayName".to_string());
    }
    let members = string_values(v.get("members"));
    Ok(ScimGroupView {
        display_name,
        members,
    })
}

fn primary_email(v: &Value) -> String {
    let Some(arr) = v.get("emails").and_then(Value::as_array) else {
        return v
            .get("email")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
    };
    // Prefer primary == true; else first with a value.
    if let Some(p) = arr.iter().find(|e| {
        e.get("primary").and_then(Value::as_bool).unwrap_or(false)
            && e.get("value").and_then(Value::as_str).is_some()
    }) {
        return p
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
    }
    arr.iter()
        .find_map(|e| e.get("value").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string()
}

/// Extract string values from a SCIM multi-valued attribute: an array of
/// `{value: "..."}` objects, an array of bare strings, or a single string.
fn string_values(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|i| match i {
                Value::String(s) => Some(s.clone()),
                Value::Object(_) => i
                    .get("value")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                _ => None,
            })
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// One parsed SCIM PATCH operation.
#[derive(Debug, Clone)]
pub struct PatchOp {
    pub op: String,
    pub path: String,
    pub value: Value,
}

/// Apply SCIM PATCH operations (RFC 7644 §3.5.2) to a normalized user view.
/// Supports `replace`/`add`/`remove` on `active`, `displayName`, `emails`,
/// `userName`, and `groups`. Returns the mutated view.
pub fn apply_user_patch(mut user: ScimUserView, ops: &[PatchOp]) -> Result<ScimUserView, String> {
    for op in ops {
        let path = op.path.trim().to_ascii_lowercase();
        let action = op.op.trim().to_ascii_lowercase();
        match path.as_str() {
            "active" => {
                user.active = match &op.value {
                    Value::Bool(b) => *b,
                    Value::String(s) => matches!(s.to_ascii_lowercase().as_str(), "true" | "1"),
                    _ => user.active,
                };
            }
            "displayname" => {
                user.display_name = op.value.as_str().unwrap_or_default().to_string();
            }
            "username" => {
                user.user_name = op.value.as_str().unwrap_or(&user.user_name).to_string();
            }
            "emails" | "emails[type eq \"work\"].value" => {
                if let Some(s) = op.value.as_str() {
                    user.email = s.to_string();
                } else {
                    user.email = primary_email(&json!({ "emails": op.value }));
                }
            }
            "groups" => {
                let incoming = string_values(Some(&op.value));
                match action.as_str() {
                    "remove" => user.groups.retain(|g| !incoming.contains(g)),
                    _ => {
                        for g in incoming {
                            if !user.groups.contains(&g) {
                                user.groups.push(g);
                            }
                        }
                    }
                }
            }
            // An empty path with an object value is a bulk replace.
            "" => {
                if let Value::Object(_) = &op.value {
                    if let Ok(merged) = parse_scim_user(&op.value.to_string()) {
                        user = merged;
                    }
                }
            }
            _ => { /* unknown path: ignore (SCIM is permissive) */ }
        }
    }
    Ok(user)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scim_user_emails_and_active_default() {
        let body = r#"{
            "schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],
            "userName":"jdoe",
            "name":{"formatted":"John Doe"},
            "emails":[{"value":"alt@x.com"},{"value":"jdoe@corp.com","primary":true}],
            "groups":[{"value":"eng"},{"value":"admins"}]
        }"#;
        let u = parse_scim_user(body).expect("parse");
        assert_eq!(u.user_name, "jdoe");
        assert_eq!(u.display_name, "John Doe");
        assert_eq!(u.email, "jdoe@corp.com"); // primary wins
        assert!(u.active); // default true
        assert_eq!(u.groups, vec!["eng", "admins"]);
    }

    #[test]
    fn patch_deactivates_user() {
        let user = ScimUserView {
            user_name: "x".into(),
            active: true,
            ..Default::default()
        };
        let ops = vec![PatchOp {
            op: "replace".into(),
            path: "active".into(),
            value: Value::Bool(false),
        }];
        let out = apply_user_patch(user, &ops).unwrap();
        assert!(!out.active);
    }

    #[test]
    fn patch_adds_and_removes_groups() {
        let user = ScimUserView {
            user_name: "x".into(),
            groups: vec!["eng".into()],
            ..Default::default()
        };
        let add = vec![PatchOp {
            op: "add".into(),
            path: "groups".into(),
            value: json!([{"value":"admins"}]),
        }];
        let out = apply_user_patch(user, &add).unwrap();
        assert_eq!(out.groups, vec!["eng", "admins"]);
        let remove = vec![PatchOp {
            op: "remove".into(),
            path: "groups".into(),
            value: json!(["eng"]),
        }];
        let out2 = apply_user_patch(out, &remove).unwrap();
        assert_eq!(out2.groups, vec!["admins"]);
    }

    #[test]
    fn rejects_user_without_username() {
        assert!(parse_scim_user(r#"{"displayName":"x"}"#).is_err());
    }
}
