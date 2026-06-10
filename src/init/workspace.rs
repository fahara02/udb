use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootKind {
    Empty,
    Application,
    UdbRepo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectedLanguage {
    Php,
    Python,
    TypeScript,
    JavaScript,
    Go,
    Java,
    CSharp,
    Rust,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectedFramework {
    Laravel,
    Symfony,
    FastApi,
    Starlette,
    ReactVite,
    Next,
    Express,
    Fastify,
    GoNetHttp,
    GoGrpc,
    SpringBoot,
    AspNetCore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageManager {
    Composer,
    Uv,
    Npm,
    Pnpm,
    Yarn,
    Go,
    Maven,
    Gradle,
    Dotnet,
    Cargo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceScan {
    pub root: PathBuf,
    pub root_kind: RootKind,
    pub languages: Vec<DetectedLanguage>,
    pub frameworks: Vec<DetectedFramework>,
    pub package_managers: Vec<PackageManager>,
    pub existing_udb_files: Vec<PathBuf>,
    pub proto_roots: Vec<PathBuf>,
    pub config_files: Vec<PathBuf>,
    pub docker_files: Vec<PathBuf>,
    pub sdk_dirs: Vec<PathBuf>,
    pub dbops_dirs: Vec<PathBuf>,
}

pub fn scan_workspace(cwd: impl AsRef<Path>) -> std::io::Result<WorkspaceScan> {
    let root = cwd.as_ref().to_path_buf();
    let mut scan = WorkspaceScan {
        root: root.clone(),
        root_kind: RootKind::Empty,
        languages: Vec::new(),
        frameworks: Vec::new(),
        package_managers: Vec::new(),
        existing_udb_files: Vec::new(),
        proto_roots: Vec::new(),
        config_files: Vec::new(),
        docker_files: Vec::new(),
        sdk_dirs: Vec::new(),
        dbops_dirs: Vec::new(),
    };

    let entries = fs::read_dir(&root)?.collect::<Result<Vec<_>, _>>()?;
    if !entries.is_empty() {
        scan.root_kind = RootKind::Application;
    }

    detect_file_signals(&root, &mut scan);
    detect_directory_signals(&root, &mut scan);
    dedupe_scan(&mut scan);

    if has_file(&root, "Cargo.toml")
        && has_dir(&root, "src")
        && has_file(&root, "buf.yaml")
        && has_file(&root, "VERSIONING.md")
    {
        scan.root_kind = RootKind::UdbRepo;
    }

    Ok(scan)
}

impl WorkspaceScan {
    pub fn primary_framework_id(&self) -> Option<&'static str> {
        self.frameworks.first().map(|framework| match framework {
            DetectedFramework::Laravel => "laravel",
            DetectedFramework::Symfony => "symfony",
            DetectedFramework::FastApi => "fastapi",
            DetectedFramework::Starlette => "starlette",
            DetectedFramework::ReactVite => "react-vite",
            DetectedFramework::Next => "next",
            DetectedFramework::Express => "express",
            DetectedFramework::Fastify => "fastify",
            DetectedFramework::GoNetHttp => "go-net-http",
            DetectedFramework::GoGrpc => "go-grpc",
            DetectedFramework::SpringBoot => "spring-boot",
            DetectedFramework::AspNetCore => "aspnet-core",
        })
    }
}

fn detect_file_signals(root: &Path, scan: &mut WorkspaceScan) {
    if has_file(root, "composer.json") {
        push_unique(&mut scan.languages, DetectedLanguage::Php);
        push_unique(&mut scan.package_managers, PackageManager::Composer);
        if has_file(root, "artisan") {
            push_unique(&mut scan.frameworks, DetectedFramework::Laravel);
        } else {
            push_unique(&mut scan.frameworks, DetectedFramework::Symfony);
        }
    }

    if has_file(root, "pyproject.toml") {
        push_unique(&mut scan.languages, DetectedLanguage::Python);
        if has_file(root, "uv.lock") {
            push_unique(&mut scan.package_managers, PackageManager::Uv);
        }
        push_unique(&mut scan.frameworks, DetectedFramework::FastApi);
    }

    if has_file(root, "package.json") {
        push_unique(&mut scan.languages, DetectedLanguage::TypeScript);
        push_unique(&mut scan.package_managers, PackageManager::Npm);
        if has_file(root, "pnpm-lock.yaml") {
            push_unique(&mut scan.package_managers, PackageManager::Pnpm);
        }
        if has_file(root, "yarn.lock") {
            push_unique(&mut scan.package_managers, PackageManager::Yarn);
        }
        if has_glob_prefix(root, "next.config.") {
            push_unique(&mut scan.frameworks, DetectedFramework::Next);
        } else if has_glob_prefix(root, "vite.config.") || has_file(root, "src/App.tsx") {
            push_unique(&mut scan.frameworks, DetectedFramework::ReactVite);
        } else {
            push_unique(&mut scan.frameworks, DetectedFramework::Express);
        }
    }

    if has_file(root, "go.mod") {
        push_unique(&mut scan.languages, DetectedLanguage::Go);
        push_unique(&mut scan.package_managers, PackageManager::Go);
        push_unique(&mut scan.frameworks, DetectedFramework::GoNetHttp);
    }

    if has_file(root, "pom.xml") {
        push_unique(&mut scan.languages, DetectedLanguage::Java);
        push_unique(&mut scan.package_managers, PackageManager::Maven);
        push_unique(&mut scan.frameworks, DetectedFramework::SpringBoot);
    }

    if has_glob_prefix(root, "build.gradle") {
        push_unique(&mut scan.languages, DetectedLanguage::Java);
        push_unique(&mut scan.package_managers, PackageManager::Gradle);
        push_unique(&mut scan.frameworks, DetectedFramework::SpringBoot);
    }

    if has_glob_suffix(root, ".csproj") || has_file(root, "global.json") {
        push_unique(&mut scan.languages, DetectedLanguage::CSharp);
        push_unique(&mut scan.package_managers, PackageManager::Dotnet);
        push_unique(&mut scan.frameworks, DetectedFramework::AspNetCore);
    }

    if has_file(root, "Cargo.toml") {
        push_unique(&mut scan.languages, DetectedLanguage::Rust);
        push_unique(&mut scan.package_managers, PackageManager::Cargo);
    }

    for file in ["buf.yaml", "buf.gen.yaml"] {
        if has_file(root, file) {
            scan.proto_roots.push(PathBuf::from(file));
        }
    }
    for file in [
        "udb.config.json",
        ".udb/native-services.json",
        "configs/database.yaml",
        "configs/backends.yaml",
        "configs/services.yaml",
    ] {
        if has_file(root, file) {
            scan.config_files.push(PathBuf::from(file));
        }
    }
    for file in ["docker-compose.yml", "docker-compose.yaml"] {
        if has_file(root, file) {
            scan.docker_files.push(PathBuf::from(file));
        }
    }
}

fn detect_directory_signals(root: &Path, scan: &mut WorkspaceScan) {
    for dir in [".udb", "proto", "sdk", "sdk-templates"] {
        if has_dir(root, dir) {
            match dir {
                ".udb" => scan.existing_udb_files.push(PathBuf::from(dir)),
                "proto" => scan.proto_roots.push(PathBuf::from(dir)),
                "sdk" | "sdk-templates" => scan.sdk_dirs.push(PathBuf::from(dir)),
                _ => {}
            }
        }
    }
    for dir in ["db/migration", "db/seeders", "db/bootstrap"] {
        if has_dir(root, dir) {
            scan.dbops_dirs.push(PathBuf::from(dir));
        }
    }
}

fn dedupe_scan(scan: &mut WorkspaceScan) {
    scan.existing_udb_files.sort();
    scan.existing_udb_files.dedup();
    scan.proto_roots.sort();
    scan.proto_roots.dedup();
    scan.config_files.sort();
    scan.config_files.dedup();
    scan.docker_files.sort();
    scan.docker_files.dedup();
    scan.sdk_dirs.sort();
    scan.sdk_dirs.dedup();
    scan.dbops_dirs.sort();
    scan.dbops_dirs.dedup();
}

fn has_file(root: &Path, relative: &str) -> bool {
    root.join(relative).is_file()
}

fn has_dir(root: &Path, relative: &str) -> bool {
    root.join(relative).is_dir()
}

fn has_glob_prefix(root: &Path, prefix: &str) -> bool {
    fs::read_dir(root)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .any(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
}

fn has_glob_suffix(root: &Path, suffix: &str) -> bool {
    fs::read_dir(root)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .any(|entry| entry.file_name().to_string_lossy().ends_with(suffix))
}

fn push_unique<T: PartialEq>(items: &mut Vec<T>, item: T) {
    if !items.contains(&item) {
        items.push(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn detects_laravel_workspace() {
        let root = temp_root("laravel");
        File::create(root.join("composer.json")).unwrap();
        File::create(root.join("artisan")).unwrap();

        let scan = scan_workspace(&root).unwrap();
        assert_eq!(scan.root_kind, RootKind::Application);
        assert!(scan.languages.contains(&DetectedLanguage::Php));
        assert!(scan.frameworks.contains(&DetectedFramework::Laravel));
        assert!(scan.package_managers.contains(&PackageManager::Composer));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_udb_repo_shape() {
        let root = temp_root("udb-repo");
        fs::create_dir(root.join("src")).unwrap();
        File::create(root.join("Cargo.toml")).unwrap();
        File::create(root.join("buf.yaml")).unwrap();
        File::create(root.join("VERSIONING.md")).unwrap();

        let scan = scan_workspace(&root).unwrap();
        assert_eq!(scan.root_kind, RootKind::UdbRepo);

        fs::remove_dir_all(root).unwrap();
    }

    fn temp_root(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("udb-init-{name}-{suffix}"));
        fs::create_dir_all(&root).unwrap();
        root
    }
}
