use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn generic_udb_surface_has_no_forbidden_project_names() {
    let forbidden_project_names = [
        concat!("life", "plus"),
        concat!("med", "pac"),
        concat!("lu", "na"),
        concat!("ambu", "life"),
    ];
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_files(root, &mut files);

    let mut violations = Vec::new();
    for file in files {
        let relative = file.strip_prefix(root).unwrap_or(&file);
        if should_skip(relative) {
            continue;
        }
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };
        let lower = content.to_ascii_lowercase();
        for forbidden in forbidden_project_names {
            if lower.contains(forbidden) {
                violations.push(format!("{} contains {}", relative.display(), forbidden));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "generic UDB files contain forbidden project names:\n{}",
        violations.join("\n")
    );
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn should_skip(relative: &Path) -> bool {
    let text = relative.to_string_lossy().replace('\\', "/");
    text.starts_with("target/")
        || text.starts_with("target_")
        || text.contains("/obj/")
        || text.contains("/bin/")
        || text.starts_with("tests/golden/")
        || text == "tests/neutrality_tests.rs"
        || text.ends_with("Cargo.lock")
}
