pub(super) fn matches_top_level_block(value: &str) -> bool {
    matches!(value, "enum" | "service")
}

pub(super) fn matches_nested_block(value: &str) -> bool {
    matches!(value, "message" | "enum" | "oneof")
}
