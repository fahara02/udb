use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn generic_udb_surface_has_no_forbidden_project_names() {
    let forbidden_project_names = [
        concat!("life", "plus"),
        concat!("med", "pac"),
        concat!("lu", "na"),
        concat!("ambu", "life"),
    ];
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = collect_tracked_files(root).unwrap_or_else(|| {
        let mut files = Vec::new();
        collect_files(root, &mut files);
        files
    });

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

fn collect_tracked_files(root: &Path) -> Option<Vec<PathBuf>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("ls-files")
        .arg("-z")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(
        output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
            .filter_map(|entry| std::str::from_utf8(entry).ok())
            .map(|entry| root.join(entry))
            .collect(),
    )
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if should_skip_dir(&path) {
                continue;
            }
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn should_skip_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    matches!(
        name,
        ".git"
            | ".idea"
            | ".vscode"
            | "target"
            | "target_debug"
            | "node_modules"
            | "dist"
            | "dist-test"
            | "bin"
            | "obj"
    )
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
