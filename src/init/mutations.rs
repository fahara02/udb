use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationKind {
    CreateDir,
    CreateFile,
    ManagedBlock,
    OverlayFile,
    VerifyOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationAction {
    Create,
    Merge,
    Skip,
    Warn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningLevel {
    None,
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanMutation {
    pub path: PathBuf,
    pub kind: MutationKind,
    pub before_hash: Option<String>,
    pub action: MutationAction,
    pub idempotency_key: String,
    pub warning_level: WarningLevel,
    pub summary: String,
}

impl PlanMutation {
    pub fn new(
        root: &Path,
        path: impl Into<PathBuf>,
        kind: MutationKind,
        action: MutationAction,
        idempotency_key: impl Into<String>,
        warning_level: WarningLevel,
        summary: impl Into<String>,
    ) -> Self {
        let path = path.into();
        let before_hash = fs::read(root.join(&path)).ok().map(|bytes| {
            let digest = Sha256::digest(bytes);
            format!("{digest:x}")
        });
        Self {
            path,
            kind,
            before_hash,
            action,
            idempotency_key: idempotency_key.into(),
            warning_level,
            summary: summary.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn existing_file_mutation_captures_hash() {
        let root = temp_root("hash");
        let mut file = File::create(root.join(".env.example")).unwrap();
        writeln!(file, "UDB_ENDPOINT=http://localhost:9090").unwrap();

        let mutation = PlanMutation::new(
            &root,
            ".env.example",
            MutationKind::ManagedBlock,
            MutationAction::Merge,
            "env-example",
            WarningLevel::Info,
            "merge env example block",
        );
        assert!(mutation.before_hash.is_some());

        fs::remove_dir_all(root).unwrap();
    }

    fn temp_root(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("udb-init-mutation-{name}-{suffix}"));
        fs::create_dir_all(&root).unwrap();
        root
    }
}
