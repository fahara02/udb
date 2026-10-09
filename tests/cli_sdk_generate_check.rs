//! Exercise the real CLI generation path without requiring a language compiler.
//! The scaffold CI producer separately checks the canonical Go entity output.

use std::collections::BTreeMap;
use std::fs::{self, File, FileTimes};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, SystemTime};

struct Fixture {
    root: PathBuf,
    templates: PathBuf,
    output: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("udb-sdk-check-{}", uuid::Uuid::new_v4()));
        let templates = root.join("custom templates");
        let output = root.join("custom output");
        fs::create_dir_all(templates.join("python/assets")).unwrap();
        fs::create_dir_all(templates.join("php")).unwrap();
        fs::write(
            templates.join("python/client.py.tmpl"),
            "version={{UDB_VERSION}}\nlanguage={{LANG}}\nrpcs={{RPC_COUNT}}\n\
             # @@UDB_RPC_BEGIN\n{{SERVICE_FULL}}/{{RPC_NAME}}\n# @@UDB_RPC_END\n",
        )
        .unwrap();
        fs::write(
            templates.join("python/assets/copied.bin"),
            [0, 1, 255, 128, 13, 10],
        )
        .unwrap();
        // Ordinary generation overwrites the copied path with the later
        // template emission. Check must compare the same final bytes.
        fs::write(templates.join("python/shared.txt"), "intermediate").unwrap();
        fs::write(
            templates.join("python/shared.txt.tmpl"),
            "final {{UDB_VERSION}}\n",
        )
        .unwrap();
        fs::write(templates.join("python/README.md"), "not emitted").unwrap();
        fs::write(
            templates.join("php/other.php.tmpl"),
            "version={{UDB_VERSION}}\nlanguage={{LANG}}\n",
        )
        .unwrap();
        Self {
            root,
            templates,
            output,
        }
    }

    fn run(&self, verb: Option<&str>, lang: &str, output: &Path, flags: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_udb"));
        command.current_dir(&self.root).arg("sdk");
        if let Some(verb) = verb {
            command.arg(verb);
        }
        command
            .arg("--lang")
            .arg(lang)
            .arg("--templates")
            .arg(&self.templates)
            .arg("--out")
            .arg(output)
            .args(flags);
        command.output().expect("run the actual udb CLI")
    }

    fn check(
        &self,
        verb: Option<&str>,
        lang: &str,
        output: &Path,
        flags: &[&str],
        expected_code: i32,
    ) -> Output {
        stamp_files_in_the_past(&self.root);
        let before = snapshot(&self.root);
        let mut flags = flags.to_vec();
        flags.push("--check");
        let result = self.run(verb, lang, output, &flags);
        assert_exit(&result, expected_code);
        assert_eq!(snapshot(&self.root), before, "--check changed the tree");
        result
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

type Snapshot = BTreeMap<PathBuf, (Option<Vec<u8>>, SystemTime)>;

fn snapshot(root: &Path) -> Snapshot {
    fn visit(root: &Path, path: &Path, entries: &mut Snapshot) {
        let metadata = fs::metadata(path).unwrap();
        let contents = if metadata.is_dir() {
            None
        } else {
            Some(fs::read(path).unwrap())
        };
        entries.insert(
            path.strip_prefix(root).unwrap().to_path_buf(),
            (contents, metadata.modified().unwrap()),
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), entries);
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

fn stamp_files_in_the_past(root: &Path) {
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            stamp_files_in_the_past(&path);
        } else {
            File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(
                    FileTimes::new()
                        .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_234_567_890)),
                )
                .unwrap();
        }
    }
}

fn assert_exit(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn generate_check_honors_templates_output_language_selectors_and_aliases() {
    let fixture = Fixture::new();
    let selectors = ["--surface", "public"];
    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &selectors),
        0,
    );
    let client = fs::read_to_string(fixture.output.join("python/client.py")).unwrap();
    assert!(client.contains(&format!("version={}", env!("CARGO_PKG_VERSION"))));
    assert!(client.contains("language=python"));
    assert!(!client.contains("{{"));
    assert_eq!(
        fs::read(fixture.output.join("python/assets/copied.bin")).unwrap(),
        [0, 1, 255, 128, 13, 10]
    );
    assert!(!fixture.output.join("python/README.md").exists());
    assert!(!fixture.root.join("sdk").exists());

    // Unselected language files and consumer-owned files are outside this
    // generation request's ownership boundary.
    fs::create_dir_all(fixture.output.join("php")).unwrap();
    fs::write(
        fixture.output.join("php/other.php"),
        "unselected stale output",
    )
    .unwrap();
    fs::write(fixture.output.join("consumer-notes.txt"), "keep this").unwrap();
    fixture.check(Some("generate"), "python", &fixture.output, &selectors, 0);
    fixture.check(Some("gen"), "python", &fixture.output, &selectors, 0);
    fixture.check(None, "python", &fixture.output, &selectors, 0);

    // Removing the surface selector changes the actual rendered RPC list.
    let drift = fixture.check(Some("generate"), "python", &fixture.output, &[], 1);
    assert!(String::from_utf8_lossy(&drift.stderr).contains("client.py"));
    fixture.check(Some("generate"), "all", &fixture.output, &selectors, 1);
    assert_exit(
        &fixture.run(Some("generate"), "all", &fixture.output, &selectors),
        0,
    );
    fixture.check(Some("generate"), "all", &fixture.output, &selectors, 0);
}

#[test]
fn generate_check_detects_rendered_copied_and_missing_output_without_repair() {
    let fixture = Fixture::new();
    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &[]),
        0,
    );
    fs::write(fixture.output.join("python/client.py"), "hand edited").unwrap();
    let drift = fixture.check(Some("generate"), "python", &fixture.output, &[], 1);
    assert!(String::from_utf8_lossy(&drift.stderr).contains("client.py"));
    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &[]),
        0,
    );

    fs::write(fixture.output.join("python/assets/copied.bin"), [0, 1, 2]).unwrap();
    let drift = fixture.check(Some("generate"), "python", &fixture.output, &[], 1);
    assert!(String::from_utf8_lossy(&drift.stderr).contains("copied.bin"));
    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &[]),
        0,
    );

    fs::remove_file(fixture.output.join("python/client.py")).unwrap();
    fixture.check(Some("generate"), "python", &fixture.output, &[], 1);
    assert!(!fixture.output.join("python/client.py").exists());
    let missing = fixture.root.join("missing output/never created");
    fixture.check(Some("generate"), "python", &missing, &[], 1);
    assert!(!fixture.root.join("missing output").exists());

    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &[]),
        0,
    );
    fixture.check(Some("generate"), "python", &fixture.output, &[], 0);

    fs::write(
        fixture.templates.join("python/client.py.tmpl"),
        "changed template {{UDB_VERSION}}\n",
    )
    .unwrap();
    fixture.check(Some("generate"), "python", &fixture.output, &[], 1);
    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &[]),
        0,
    );
    fixture.check(Some("generate"), "python", &fixture.output, &[], 0);
}

#[test]
fn generate_check_render_and_selector_errors_leave_all_output_unchanged() {
    let fixture = Fixture::new();
    assert_exit(
        &fixture.run(Some("generate"), "python", &fixture.output, &[]),
        0,
    );
    fixture.check(
        Some("generate"),
        "python",
        &fixture.output,
        &["--service", "not_a_udb_service"],
        1,
    );
    fs::write(
        fixture.templates.join("python/unresolved.py.tmpl"),
        "{{UNRESOLVED_CHECK_TOKEN}}\n",
    )
    .unwrap();
    fixture.check(Some("generate"), "python", &fixture.output, &[], 1);
    assert!(!fixture.output.join("python/unresolved.py").exists());
}
