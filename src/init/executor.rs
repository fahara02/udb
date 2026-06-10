use crate::init::mutations::{MutationKind, PlanMutation};
use crate::init::plan::InitPlanResult;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const MANAGED_ENV_KEY: &str = "udb-init-env";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyReceipt {
    pub applied: bool,
    pub dry_run: bool,
    pub writes: Vec<ApplyWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevertReceipt {
    pub reverted: bool,
    pub removed: Vec<ApplyWrite>,
    pub skipped: Vec<ApplyWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyWrite {
    pub path: PathBuf,
    pub action: String,
}

pub fn apply_init_plan(
    root: impl AsRef<Path>,
    result: &InitPlanResult,
    dry_run: bool,
) -> io::Result<ApplyReceipt> {
    let root = root.as_ref();
    let mut writes = Vec::new();

    for mutation in &result.plan.mutations {
        writes.push(apply_mutation(root, result, mutation, dry_run)?);
    }

    if !dry_run {
        fs::create_dir_all(root.join(".udb"))?;
        let receipt = ApplyReceipt {
            applied: true,
            dry_run: false,
            writes: writes.clone(),
        };
        write_json(root, Path::new(".udb/init-receipt.json"), &receipt)?;
    }

    Ok(ApplyReceipt {
        applied: !dry_run,
        dry_run,
        writes,
    })
}

fn apply_mutation(
    root: &Path,
    result: &InitPlanResult,
    mutation: &PlanMutation,
    dry_run: bool,
) -> io::Result<ApplyWrite> {
    match mutation.kind {
        MutationKind::CreateDir => {
            if !dry_run {
                fs::create_dir_all(root.join(&mutation.path))?;
            }
            Ok(write(&mutation.path, "ensure_dir"))
        }
        MutationKind::CreateFile if mutation.path == PathBuf::from(".udb/init-plan.json") => {
            if !dry_run {
                write_json(root, &mutation.path, result)?;
            }
            Ok(write(&mutation.path, "write_json"))
        }
        MutationKind::CreateFile if mutation.path == PathBuf::from(".udb/init-state.json") => {
            if !dry_run {
                write_json(root, &mutation.path, &result.plan)?;
            }
            Ok(write(&mutation.path, "write_json"))
        }
        MutationKind::ManagedBlock if mutation.path == PathBuf::from(".env.example") => {
            if !dry_run {
                upsert_env_example(root, result)?;
            }
            Ok(write(&mutation.path, "upsert_managed_block"))
        }
        MutationKind::OverlayFile
            if mutation.path == PathBuf::from("configs/udb.init.backends.yaml") =>
        {
            if !dry_run {
                write_text(root, &mutation.path, &backends_overlay(result))?;
            }
            Ok(write(&mutation.path, "write_overlay"))
        }
        MutationKind::OverlayFile
            if mutation.path == PathBuf::from("configs/udb.init.services.yaml") =>
        {
            if !dry_run {
                write_text(root, &mutation.path, &services_overlay(result))?;
            }
            Ok(write(&mutation.path, "write_overlay"))
        }
        MutationKind::OverlayFile
            if mutation.path == PathBuf::from("configs/udb.init.database.yaml") =>
        {
            if !dry_run {
                write_text(root, &mutation.path, &database_overlay(result))?;
            }
            Ok(write(&mutation.path, "write_overlay"))
        }
        MutationKind::OverlayFile if mutation.path == PathBuf::from("docker-compose.udb.yml") => {
            if !dry_run {
                write_text(root, &mutation.path, &compose_overlay(result))?;
            }
            Ok(write(&mutation.path, "write_overlay"))
        }
        _ => Ok(write(&mutation.path, "planned")),
    }
}

pub fn revert_last_init(root: impl AsRef<Path>) -> io::Result<RevertReceipt> {
    let root = root.as_ref();
    let receipt_path = root.join(".udb/init-receipt.json");
    let receipt_raw = fs::read_to_string(&receipt_path)?;
    let receipt: ApplyReceipt = serde_json::from_str(&receipt_raw)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    let mut removed = Vec::new();
    let mut skipped = Vec::new();

    for write in receipt.writes {
        match write.action.as_str() {
            "ensure_dir" => skipped.push(ApplyWrite {
                path: write.path,
                action: "skip_directory".to_string(),
            }),
            "upsert_managed_block" if write.path == PathBuf::from(".env.example") => {
                if remove_env_block(root)? {
                    removed.push(ApplyWrite {
                        path: write.path,
                        action: "remove_managed_block".to_string(),
                    });
                } else {
                    skipped.push(ApplyWrite {
                        path: write.path,
                        action: "managed_block_absent".to_string(),
                    });
                }
            }
            "write_json" | "write_overlay" => {
                let full_path = root.join(&write.path);
                if full_path.is_file() {
                    fs::remove_file(&full_path)?;
                    removed.push(ApplyWrite {
                        path: write.path,
                        action: "remove_file".to_string(),
                    });
                } else {
                    skipped.push(ApplyWrite {
                        path: write.path,
                        action: "file_absent".to_string(),
                    });
                }
            }
            _ => skipped.push(ApplyWrite {
                path: write.path,
                action: "not_generated_by_init".to_string(),
            }),
        }
    }

    if receipt_path.is_file() {
        fs::remove_file(&receipt_path)?;
        removed.push(ApplyWrite {
            path: PathBuf::from(".udb/init-receipt.json"),
            action: "remove_receipt".to_string(),
        });
    }

    let revert = RevertReceipt {
        reverted: true,
        removed,
        skipped,
    };
    write_json(root, Path::new(".udb/init-revert-receipt.json"), &revert)?;
    Ok(revert)
}

fn upsert_env_example(root: &Path, result: &InitPlanResult) -> io::Result<()> {
    let path = Path::new(".env.example");
    let full_path = root.join(path);
    let existing = fs::read_to_string(&full_path).unwrap_or_default();
    let block = env_block(result);
    let updated = upsert_managed_block(&existing, MANAGED_ENV_KEY, &block);
    write_text(root, path, &updated)
}

fn remove_env_block(root: &Path) -> io::Result<bool> {
    let path = Path::new(".env.example");
    let full_path = root.join(path);
    let existing = match fs::read_to_string(&full_path) {
        Ok(existing) => existing,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(source),
    };
    let Some(updated) = remove_managed_block(&existing, MANAGED_ENV_KEY) else {
        return Ok(false);
    };
    write_text(root, path, &updated)?;
    Ok(true)
}

fn env_block(result: &InitPlanResult) -> String {
    let mut out = String::new();
    out.push_str("UDB_GRPC_ADDR=http://127.0.0.1:50051\n");
    out.push_str("UDB_PROTO_ROOT=proto\n");
    for backend in &result.plan.selections.backends {
        out.push_str(&format!("UDB_{}_DSN=\n", backend.to_ascii_uppercase()));
    }
    out
}

fn database_overlay(result: &InitPlanResult) -> String {
    format!(
        "app_name: udb-init\nprofile: {:?}\nmigration:\n  migrations_path: db/migration\n  seeders_path: db/seeders\n  proto_dir: proto\n  bootstrap_dir: db/bootstrap\n",
        result.plan.profile
    )
}

fn backends_overlay(result: &InitPlanResult) -> String {
    let mut out = String::from("app_name: udb-init-backends\nbackend_instances:\n  instances:\n");
    for backend in &result.plan.selections.backends {
        out.push_str(&format!(
            "    - name: {backend}\n      backend: {backend}\n      role: projection\n      dsn: ${{UDB_{}_DSN}}\n",
            backend.to_ascii_uppercase()
        ));
    }
    out
}

fn services_overlay(result: &InitPlanResult) -> String {
    let mut out = String::from(
        "app_name: udb-init-services\nnative_services:\n  enabled: true\n  services:\n",
    );
    for service in &result.plan.selections.native_services {
        out.push_str(&format!("    {service}:\n      enabled: true\n"));
    }
    out
}

fn compose_overlay(result: &InitPlanResult) -> String {
    let has_backend = |id: &str| {
        result
            .plan
            .selections
            .backends
            .iter()
            .any(|backend| backend == id)
    };

    let mut out = String::from("services:\n");
    if has_backend("postgres") {
        out.push_str(
            "  postgres:\n    image: postgres:16\n    environment:\n      POSTGRES_USER: udb\n      POSTGRES_PASSWORD: udb\n      POSTGRES_DB: udb\n    ports:\n      - \"5432:5432\"\n",
        );
    }
    if has_backend("redis") {
        out.push_str("  redis:\n    image: redis:7\n    ports:\n      - \"6379:6379\"\n");
    }
    if has_backend("qdrant") {
        out.push_str("  qdrant:\n    image: qdrant/qdrant:latest\n    ports:\n      - \"6333:6333\"\n      - \"6334:6334\"\n");
    }
    if has_backend("minio") {
        out.push_str(
            "  minio:\n    image: minio/minio:latest\n    command: server /data --console-address :9001\n    environment:\n      MINIO_ROOT_USER: minioadmin\n      MINIO_ROOT_PASSWORD: minioadmin\n    ports:\n      - \"9000:9000\"\n      - \"9001:9001\"\n",
        );
    }
    out
}

fn upsert_managed_block(existing: &str, key: &str, block: &str) -> String {
    let start = format!("# BEGIN UDB MANAGED: {key}");
    let end = format!("# END UDB MANAGED: {key}");
    let rendered = format!("{start}\n{}{end}\n", block.trim_end());

    if let Some(start_index) = existing.find(&start) {
        if let Some(relative_end) = existing[start_index..].find(&end) {
            let end_index = start_index + relative_end + end.len();
            let mut output = String::new();
            output.push_str(existing[..start_index].trim_end());
            output.push_str("\n\n");
            output.push_str(&rendered);
            output.push_str(existing[end_index..].trim_start_matches(['\r', '\n']));
            return output;
        }
    }

    let mut output = existing.trim_end().to_string();
    if !output.is_empty() {
        output.push_str("\n\n");
    }
    output.push_str(&rendered);
    output
}

fn remove_managed_block(existing: &str, key: &str) -> Option<String> {
    let start = format!("# BEGIN UDB MANAGED: {key}");
    let end = format!("# END UDB MANAGED: {key}");
    let start_index = existing.find(&start)?;
    let relative_end = existing[start_index..].find(&end)?;
    let end_index = start_index + relative_end + end.len();
    let mut output = String::new();
    output.push_str(existing[..start_index].trim_end());
    if !output.is_empty()
        && !existing[end_index..]
            .trim_start_matches(['\r', '\n'])
            .is_empty()
    {
        output.push_str("\n\n");
    }
    output.push_str(existing[end_index..].trim_start_matches(['\r', '\n']));
    Some(output)
}

fn write_json<T: Serialize>(root: &Path, path: &Path, value: &T) -> io::Result<()> {
    let json = serde_json::to_string_pretty(value)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    write_text(root, path, &(json + "\n"))
}

fn write_text(root: &Path, path: &Path, content: &str) -> io::Result<()> {
    let full_path = root.join(path);
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(full_path, content)
}

fn write(path: &Path, action: &str) -> ApplyWrite {
    ApplyWrite {
        path: path.to_path_buf(),
        action: action.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_block_replaces_existing_block() {
        let existing =
            "# BEGIN UDB MANAGED: udb-init-env\nOLD=1\n# END UDB MANAGED: udb-init-env\n";
        let updated = upsert_managed_block(existing, MANAGED_ENV_KEY, "NEW=1\n");
        assert!(updated.contains("NEW=1"));
        assert!(!updated.contains("OLD=1"));
    }

    #[test]
    fn managed_block_can_be_removed() {
        let existing = "KEEP=1\n\n# BEGIN UDB MANAGED: udb-init-env\nOLD=1\n# END UDB MANAGED: udb-init-env\n\nAFTER=1\n";
        let updated = remove_managed_block(existing, MANAGED_ENV_KEY).unwrap();
        assert!(updated.contains("KEEP=1"));
        assert!(updated.contains("AFTER=1"));
        assert!(!updated.contains("OLD=1"));
    }
}
