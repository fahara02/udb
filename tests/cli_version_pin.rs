//! An explicit deployment pin must stop the real CLI before reading protos or
//! opening backend connections. The CI all-integration-target lane runs this.

use std::process::Command;

#[test]
fn serve_refuses_a_mismatched_build_before_proto_or_backend_startup() {
    let scratch = std::env::temp_dir().join(format!("udb-version-pin-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&scratch).expect("isolated CLI working directory");
    struct RemoveScratch(std::path::PathBuf);
    impl Drop for RemoveScratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = RemoveScratch(scratch.clone());
    let missing_proto_root = scratch.join("does-not-exist");
    let output = Command::new(env!("CARGO_BIN_EXE_udb"))
        .current_dir(&scratch)
        .env("UDB_EXPECTED_VERSION", "v0.0.0")
        .arg("serve")
        .arg(missing_proto_root)
        .output()
        .expect("run actual udb binary");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("UDB_EXPECTED_VERSION is 0.0.0"), "{stderr}");
    assert!(stderr.contains(env!("CARGO_PKG_VERSION")), "{stderr}");
    assert!(!stderr.contains("DataBroker starting"), "{stderr}");
}
