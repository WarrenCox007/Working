use std::path::Path;

/// Returns true if path is allowed given allow/deny lists.
/// If allow list is non-empty, path must match at least one allowed prefix.
/// Deny list always overrides.
pub fn is_allowed(path: &Path, allow: &[String], deny: &[String]) -> bool {
    let path_str = path.to_string_lossy();
    for prefix in deny {
        if path_str.starts_with(prefix) {
            return false;
        }
    }
    if allow.is_empty() {
        return true;
    }
    allow.iter().any(|p| path_str.starts_with(p))
}
