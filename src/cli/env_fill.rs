//! `udb env fill` — complete an existing `.env` instead of hand-writing the rest.
//!
//! `udb env --profile` writes a fresh file. An operator who already has one (from
//! a template, a colleague, an older release) usually lacks a handful of values a
//! broker needs to boot and log anyone in: the session and API-key hash secrets,
//! a 32-byte encryption key, the RS256 keypair the broker signs its own bearers
//! with. Most of those are random and can be generated; a few (the database, the
//! TLS material) only the operator can answer.
//!
//! This reads the file, works out what the profile still needs, generates what can
//! be generated, asks only for what cannot, shows the plan with secrets masked, and
//! writes nothing until the operator approves. It never changes a value that is
//! already set, keeps a timestamped backup, and keeps the file owner-only.
//!
//! Only the file counts: a value exported in the current shell is not a value the
//! broker will see when it is started from this file somewhere else.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

use super::env_template::EnvProfile;
use super::keygen::{
    ensure_rs256_keypair, generated_encryption_key, generated_secret, private_write,
};

/// What the operator asked `udb env fill` to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillMode {
    /// Show the plan and write after approval (`--yes` approves up front).
    Apply { yes: bool },
    /// Exit non-zero listing what is missing; write nothing.
    Check,
    /// Show the plan; write nothing.
    DryRun,
}

#[derive(Debug, Clone)]
pub(crate) struct FillOptions {
    pub(crate) file: PathBuf,
    pub(crate) keys_dir: PathBuf,
    /// `None` infers it from the file's `UDB_ENV` (production → enterprise).
    pub(crate) profile: Option<EnvProfile>,
    pub(crate) dsn: Option<String>,
    pub(crate) mode: FillMode,
}

/// How a missing value is produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Make {
    /// A 48-character random secret.
    Secret,
    /// A 32-byte random key, base64.
    EncryptionKey,
    /// A fixed value the profile implies.
    Fixed(&'static str),
    /// Path to the generated RS256 private / public key.
    JwtPrivate,
    JwtPublic,
    /// Only the operator knows; `default` is offered when there is a sane one.
    Ask {
        prompt: &'static str,
        default: Option<&'static str>,
    },
}

#[derive(Debug, Clone, Copy)]
struct Need {
    key: &'static str,
    why: &'static str,
    secret: bool,
    make: Make,
}

const LOCAL_DSN: &str = "postgres://udb:udb@127.0.0.1:5432/udb?sslmode=disable";

fn needs(profile: EnvProfile) -> Vec<Need> {
    let dev = profile == EnvProfile::Dev;
    let mut out = vec![
        Need {
            key: "UDB_ENV",
            why: "Deployment posture; `production` enforces TLS and fail-closed checks.",
            secret: false,
            make: Make::Fixed(if dev { "development" } else { "production" }),
        },
        Need {
            key: "UDB_PG_DSN",
            why: "Postgres, the one mandatory backend. Use a direct/session endpoint, not a transaction pooler.",
            secret: true,
            make: Make::Ask {
                prompt: "PostgreSQL DSN",
                default: if dev { Some(LOCAL_DSN) } else { None },
            },
        },
        Need {
            key: "UDB_GRPC_ADDR",
            why: "Data-plane listener applications connect to.",
            secret: false,
            make: Make::Fixed(if dev {
                "127.0.0.1:50051"
            } else {
                "0.0.0.0:50051"
            }),
        },
        Need {
            key: "UDB_ENCRYPTION_KEY",
            why: "At-rest encryption for secret columns and vault material. Losing it loses that data.",
            secret: true,
            make: Make::EncryptionKey,
        },
        Need {
            key: "UDB_SESSION_ENABLED",
            why: "Server-side sessions; without them login returns FAILED_PRECONDITION.",
            secret: false,
            make: Make::Fixed("true"),
        },
        Need {
            key: "UDB_SESSION_HASH_SECRET",
            why: "Hashes session tokens and, by default, native passwords. Keep it stable.",
            secret: true,
            make: Make::Secret,
        },
        Need {
            key: "UDB_API_KEY_HASH_SECRET",
            why: "Hashes API keys; separate from the session secret so rotating one keeps the other.",
            secret: true,
            make: Make::Secret,
        },
        Need {
            key: "UDB_APPROVAL_SIGNING_KEY",
            why: "Signs migration approval plans so a plan cannot change between review and apply.",
            secret: true,
            make: Make::Secret,
        },
        Need {
            key: "UDB_JWT_PRIVATE_KEY",
            why: "RS256 key the broker signs its own bearer tokens with; without it login issues no token.",
            secret: false,
            make: Make::JwtPrivate,
        },
        Need {
            key: "UDB_JWT_PUBLIC_KEY",
            why: "Matching RS256 public key the broker verifies its bearers with.",
            secret: false,
            make: Make::JwtPublic,
        },
    ];
    if !dev {
        out.extend([
            Need {
                key: "UDB_MTLS_REQUIRED",
                why: "Reject clients without a certificate signed by the client CA.",
                secret: false,
                make: Make::Fixed("true"),
            },
            Need {
                key: "UDB_MTLS_CLIENT_CA_PATH",
                why: "CA bundle client certificates are verified against.",
                secret: false,
                make: Make::Ask {
                    prompt: "Client CA bundle path",
                    default: None,
                },
            },
            Need {
                key: "UDB_TLS_CERT_PATH",
                why: "Server certificate for the gRPC listeners.",
                secret: false,
                make: Make::Ask {
                    prompt: "Server TLS certificate path",
                    default: None,
                },
            },
            Need {
                key: "UDB_TLS_KEY_PATH",
                why: "Private key for the server certificate.",
                secret: false,
                make: Make::Ask {
                    prompt: "Server TLS private key path",
                    default: None,
                },
            },
            Need {
                key: "UDB_MIGRATE_ENABLED",
                why: "Off: apply schema changes deliberately with `udb plan`, not on a restart.",
                secret: false,
                make: Make::Fixed("false"),
            },
        ]);
    }
    out
}

/// `KEY=VALUE` pairs of a dotenv file: `export ` prefixes, surrounding quotes and
/// comment lines are handled; an empty value counts as unset.
pub(crate) fn parse_env(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let mut value = value.trim();
        if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            value = &value[1..value.len() - 1];
        }
        if !value.is_empty() {
            out.insert(key.trim().to_string(), value.to_string());
        }
    }
    out
}

/// One value the plan will add.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Planned {
    key: &'static str,
    value: String,
    origin: &'static str,
    secret: bool,
    why: &'static str,
}

#[derive(Debug, Default)]
struct Plan {
    add: Vec<Planned>,
    already_set: usize,
    /// Values only the operator can supply, not answered yet.
    unanswered: Vec<&'static str>,
    keypair: bool,
}

/// Work out what to add. `answer` supplies operator input for `Make::Ask`
/// (returning `None` leaves it unanswered); generation that needs the disk (the
/// JWT keypair) is only described here and happens on apply.
fn plan(
    profile: EnvProfile,
    existing: &BTreeMap<String, String>,
    keys_dir: &Path,
    dsn: Option<&str>,
    answer: &mut dyn FnMut(&'static str, &'static str, Option<&'static str>) -> Option<String>,
) -> Result<Plan, String> {
    let mut out = Plan::default();
    let has = |key: &str| existing.contains_key(key);
    match (has("UDB_JWT_PRIVATE_KEY"), has("UDB_JWT_PUBLIC_KEY")) {
        (true, false) | (false, true) => {
            return Err(
                "the file sets only one of UDB_JWT_PRIVATE_KEY / UDB_JWT_PUBLIC_KEY; set both or \
                 remove both and let `udb env fill` generate a matching pair"
                    .into(),
            );
        }
        (false, false) => out.keypair = true,
        (true, true) => {}
    }
    let private = keys_dir.join("jwt-private.pem").display().to_string();
    let public = keys_dir.join("jwt-public.pem").display().to_string();
    for need in needs(profile) {
        if has(need.key) {
            out.already_set += 1;
            continue;
        }
        let (value, origin) = match need.make {
            Make::Secret => (generated_secret(), "generated"),
            Make::EncryptionKey => (generated_encryption_key(), "generated"),
            Make::Fixed(value) => (value.to_string(), "default"),
            Make::JwtPrivate => (private.clone(), "generated"),
            Make::JwtPublic => (public.clone(), "generated"),
            Make::Ask { prompt, default } => {
                let provided = if need.key == "UDB_PG_DSN" {
                    dsn.map(str::to_string)
                } else {
                    None
                };
                match provided.or_else(|| answer(need.key, prompt, default)) {
                    Some(value) if !value.trim().is_empty() => (value.trim().to_string(), "you"),
                    _ => {
                        out.unanswered.push(need.key);
                        continue;
                    }
                }
            }
        };
        out.add.push(Planned {
            key: need.key,
            value,
            origin,
            secret: need.secret,
            why: need.why,
        });
    }
    Ok(out)
}

fn masked(planned: &Planned) -> String {
    if !planned.secret {
        return planned.value.clone();
    }
    let shown: String = planned.value.chars().take(4).collect();
    format!("{shown}… ({} chars)", planned.value.chars().count())
}

fn render_plan(file: &Path, profile: EnvProfile, plan: &Plan) -> String {
    let mut out = format!(
        "udb env fill: {} ({} profile) — {} to add, {} already set\n",
        file.display(),
        profile_label(profile),
        plan.add.len(),
        plan.already_set
    );
    let width = plan.add.iter().map(|p| p.key.len()).max().unwrap_or(0);
    for planned in &plan.add {
        out.push_str(&format!(
            "  + {:width$}  {:9}  {}\n      {}\n",
            planned.key,
            planned.origin,
            masked(planned),
            planned.why,
        ));
    }
    if plan.keypair {
        out.push_str("  (the RS256 keypair is written owner-only next to the paths above)\n");
    }
    for key in &plan.unanswered {
        out.push_str(&format!("  ? {key}  needs a value only you can give\n"));
    }
    out
}

fn profile_label(profile: EnvProfile) -> &'static str {
    match profile {
        EnvProfile::Dev => "dev",
        EnvProfile::Enterprise => "enterprise",
    }
}

/// Write the planned values: generate the keypair if the plan includes it, back
/// the file up, then append one marked block. Returns the backup path, if any.
fn apply(
    file: &Path,
    keys_dir: &Path,
    profile: EnvProfile,
    plan: &Plan,
) -> Result<Option<PathBuf>, String> {
    if plan.keypair {
        ensure_rs256_keypair(keys_dir)?;
    }
    let mut block = format!(
        "\n# ── Added by `udb env fill --profile {}` ─────────────────────────────\n",
        profile_label(profile)
    );
    for planned in &plan.add {
        block.push_str(&format!(
            "# {}\n{}={}\n",
            planned.why, planned.key, planned.value
        ));
    }
    if !file.exists() {
        private_write(file, block.trim_start().as_bytes())?;
        return Ok(None);
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let backup = file.with_file_name(format!(
        "{}.bak-{stamp}",
        file.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(".env")
    ));
    let original = fs::read(file).map_err(|err| format!("read {}: {err}", file.display()))?;
    private_write(&backup, &original)?;
    let mut handle = fs::OpenOptions::new()
        .append(true)
        .open(file)
        .map_err(|err| format!("open {}: {err}", file.display()))?;
    handle
        .write_all(block.as_bytes())
        .map_err(|err| format!("write {}: {err}", file.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(file, fs::Permissions::from_mode(0o600))
            .map_err(|err| format!("restrict {}: {err}", file.display()))?;
    }
    Ok(Some(backup))
}

/// `udb env fill`. Exit codes: 0 done (or nothing to do), 1 incomplete or
/// declined, 2 invalid input.
pub(crate) fn run(options: &FillOptions) -> i32 {
    let text = match fs::read_to_string(&options.file) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(err) => {
            eprintln!(
                "udb env fill: cannot read {}: {err}",
                options.file.display()
            );
            return 2;
        }
    };
    let existing = parse_env(&text);
    let profile = options.profile.unwrap_or_else(|| {
        match existing
            .get("UDB_ENV")
            .map(|value| value.to_ascii_lowercase())
        {
            Some(value) if value == "production" || value == "prod" => EnvProfile::Enterprise,
            _ => EnvProfile::Dev,
        }
    });
    let interactive =
        matches!(options.mode, FillMode::Apply { yes: false }) && std::io::stdin().is_terminal();
    let stdin = std::io::stdin();
    let mut answer = |key: &'static str, prompt: &'static str, default: Option<&'static str>| {
        if !interactive {
            // Non-interactive: take a safe default when there is one, else leave
            // the value for the operator.
            return match options.mode {
                FillMode::Apply { yes: true } => default.map(str::to_string),
                _ => None,
            };
        }
        match default {
            Some(default) => eprint!("{prompt} for {key} [{default}]: "),
            None => eprint!("{prompt} for {key}: "),
        }
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).is_err() {
            return None;
        }
        let line = line.trim();
        if line.is_empty() {
            default.map(str::to_string)
        } else {
            Some(line.to_string())
        }
    };
    let plan = match plan(
        profile,
        &existing,
        &options.keys_dir,
        options.dsn.as_deref(),
        &mut answer,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            eprintln!("udb env fill: {err}");
            return 2;
        }
    };

    if options.mode == FillMode::Check {
        let mut missing: Vec<&str> = plan.add.iter().map(|planned| planned.key).collect();
        missing.extend(plan.unanswered.iter().copied());
        if missing.is_empty() {
            println!(
                "udb env fill: {} is complete for the {} profile",
                options.file.display(),
                profile_label(profile)
            );
            return 0;
        }
        println!(
            "udb env fill: {} is missing {} value(s) for the {} profile: {}\n\
             Run `udb env fill --file {}` to generate and add them.",
            options.file.display(),
            missing.len(),
            profile_label(profile),
            missing.join(", "),
            options.file.display()
        );
        return 1;
    }

    eprint!("{}", render_plan(&options.file, profile, &plan));
    if plan.add.is_empty() && plan.unanswered.is_empty() {
        eprintln!("Nothing to add.");
        return 0;
    }
    if options.mode == FillMode::DryRun {
        eprintln!("Dry run: nothing written.");
        return 0;
    }
    if !plan.unanswered.is_empty() {
        eprintln!(
            "Not written: {} still need a value. Answer them interactively, or set them in the file and re-run.",
            plan.unanswered.join(", ")
        );
        return 1;
    }
    let approved = match options.mode {
        FillMode::Apply { yes: true } => true,
        _ if !interactive => {
            eprintln!("Not written: approve with --yes, or run in a terminal to confirm.");
            return 1;
        }
        _ => {
            eprint!(
                "Write these {} value(s) to {}? [y/N]: ",
                plan.add.len(),
                options.file.display()
            );
            let mut line = String::new();
            let _ = stdin.lock().read_line(&mut line);
            matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes")
        }
    };
    if !approved {
        eprintln!("Not written.");
        return 1;
    }
    match apply(&options.file, &options.keys_dir, profile, &plan) {
        Ok(backup) => {
            eprintln!(
                "Wrote {} value(s) to {}{}.\n\
                 The file now holds generated secrets: keep it out of version control.\n\
                 Next: udb doctor",
                plan.add.len(),
                options.file.display(),
                backup
                    .map(|path| format!(" (previous version: {})", path.display()))
                    .unwrap_or_default()
            );
            0
        }
        Err(err) => {
            eprintln!("udb env fill: {err}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_answers(_: &'static str, _: &'static str, _: Option<&'static str>) -> Option<String> {
        None
    }

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("udb-env-fill-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn dotenv_parsing_handles_export_quotes_comments_and_empty_values() {
        let parsed = parse_env(
            "# comment\nexport A=1\nB=\"two words\"\nC='x'\nD=\nE = spaced \nnot a pair\n",
        );
        assert_eq!(parsed.get("A").map(String::as_str), Some("1"));
        assert_eq!(parsed.get("B").map(String::as_str), Some("two words"));
        assert_eq!(parsed.get("C").map(String::as_str), Some("x"));
        assert!(!parsed.contains_key("D"), "an empty value counts as unset");
        assert_eq!(parsed.get("E").map(String::as_str), Some("spaced"));
    }

    #[test]
    fn an_empty_dev_file_gets_every_boot_and_login_value() {
        let plan = plan(
            EnvProfile::Dev,
            &BTreeMap::new(),
            Path::new("/keys"),
            None,
            &mut |_, _, default| default.map(str::to_string),
        )
        .unwrap();
        let keys: Vec<&str> = plan.add.iter().map(|planned| planned.key).collect();
        for key in [
            "UDB_PG_DSN",
            "UDB_ENCRYPTION_KEY",
            "UDB_SESSION_ENABLED",
            "UDB_SESSION_HASH_SECRET",
            "UDB_API_KEY_HASH_SECRET",
            "UDB_JWT_PRIVATE_KEY",
            "UDB_JWT_PUBLIC_KEY",
        ] {
            assert!(keys.contains(&key), "{key} missing from {keys:?}");
        }
        assert!(plan.keypair && plan.unanswered.is_empty());
        let encryption = plan
            .add
            .iter()
            .find(|p| p.key == "UDB_ENCRYPTION_KEY")
            .unwrap();
        udb::runtime::validate_encryption_key(&encryption.value).expect("broker-valid key");
    }

    #[test]
    fn values_already_set_are_never_changed() {
        let existing = parse_env("UDB_SESSION_HASH_SECRET=keep-me\nUDB_PG_DSN=postgres://mine\n");
        let plan = plan(
            EnvProfile::Dev,
            &existing,
            Path::new("/keys"),
            Some("postgres://other"),
            &mut no_answers,
        )
        .unwrap();
        assert!(
            plan.add
                .iter()
                .all(|p| p.key != "UDB_SESSION_HASH_SECRET" && p.key != "UDB_PG_DSN")
        );
        assert_eq!(plan.already_set, 2);
    }

    #[test]
    fn half_a_keypair_is_refused() {
        let existing = parse_env("UDB_JWT_PRIVATE_KEY=/k/private.pem\n");
        assert!(
            plan(
                EnvProfile::Dev,
                &existing,
                Path::new("/keys"),
                None,
                &mut no_answers
            )
            .is_err()
        );
    }

    #[test]
    fn enterprise_asks_for_tls_material_it_cannot_invent() {
        let plan = plan(
            EnvProfile::Enterprise,
            &BTreeMap::new(),
            Path::new("/keys"),
            None,
            &mut no_answers,
        )
        .unwrap();
        for key in [
            "UDB_PG_DSN",
            "UDB_MTLS_CLIENT_CA_PATH",
            "UDB_TLS_CERT_PATH",
            "UDB_TLS_KEY_PATH",
        ] {
            assert!(
                plan.unanswered.contains(&key),
                "{key} must be asked, not invented"
            );
        }
    }

    #[test]
    fn secrets_are_masked_in_the_plan() {
        let planned = Planned {
            key: "UDB_SESSION_HASH_SECRET",
            value: "abcdefghijklmnop".into(),
            origin: "generated",
            secret: true,
            why: "",
        };
        let shown = masked(&planned);
        assert!(
            shown.starts_with("abcd") && !shown.contains("efgh"),
            "{shown}"
        );
    }

    #[test]
    fn apply_backs_up_appends_and_writes_the_keypair() {
        let dir = temp_dir();
        let file = dir.join(".env");
        fs::write(&file, "UDB_ENV=development\n").unwrap();
        let keys = dir.join("keys");
        let options = FillOptions {
            file: file.clone(),
            keys_dir: keys.clone(),
            profile: None,
            dsn: Some("postgres://u:p@db:5432/udb".into()),
            mode: FillMode::Apply { yes: true },
        };
        assert_eq!(run(&options), 0);
        let filled = parse_env(&fs::read_to_string(&file).unwrap());
        assert_eq!(
            filled.get("UDB_ENV").map(String::as_str),
            Some("development")
        );
        assert_eq!(
            filled.get("UDB_PG_DSN").map(String::as_str),
            Some("postgres://u:p@db:5432/udb")
        );
        assert!(keys.join("jwt-private.pem").exists() && keys.join("jwt-public.pem").exists());
        let backups = fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".env.bak-"))
            .count();
        assert_eq!(backups, 1, "the previous file is kept");
        let check = FillOptions {
            mode: FillMode::Check,
            ..options
        };
        assert_eq!(run(&check), 0, "a filled file passes --check");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn check_reports_missing_values_without_writing() {
        let dir = temp_dir();
        let file = dir.join(".env");
        fs::write(&file, "UDB_ENV=development\n").unwrap();
        let options = FillOptions {
            file: file.clone(),
            keys_dir: dir.join("keys"),
            profile: None,
            dsn: None,
            mode: FillMode::Check,
        };
        assert_eq!(run(&options), 1);
        assert_eq!(fs::read_to_string(&file).unwrap(), "UDB_ENV=development\n");
        assert!(!dir.join("keys").exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
