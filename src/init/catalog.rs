use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendRole {
    Canonical,
    Cache,
    Vector,
    Object,
    Document,
    Search,
    Graph,
    Column,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCatalogEntry {
    pub id: String,
    pub label: String,
    pub role: BackendRole,
    pub default_for_local: bool,
    pub requires_secret_prompt: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeServiceCatalogEntry {
    pub id: String,
    pub label: String,
    pub aliases: Vec<String>,
    pub default_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameworkCatalogEntry {
    pub id: String,
    pub language: String,
    pub label: String,
    pub primary: bool,
}

pub fn backend_catalog() -> Vec<BackendCatalogEntry> {
    vec![
        backend(
            "postgres",
            "PostgreSQL",
            BackendRole::Canonical,
            true,
            false,
            &[],
        ),
        backend(
            "mysql",
            "MySQL / MariaDB",
            BackendRole::Canonical,
            false,
            false,
            &[],
        ),
        backend(
            "sqlite",
            "SQLite",
            BackendRole::Canonical,
            false,
            false,
            &[],
        ),
        backend(
            "sqlserver",
            "SQL Server",
            BackendRole::Canonical,
            false,
            false,
            &[],
        ),
        backend(
            "clickhouse",
            "ClickHouse",
            BackendRole::Search,
            false,
            false,
            &[],
        ),
        backend("redis", "Redis", BackendRole::Cache, true, false, &[]),
        backend(
            "memcached",
            "Memcached",
            BackendRole::Cache,
            false,
            false,
            &[],
        ),
        backend("qdrant", "Qdrant", BackendRole::Vector, false, false, &[]),
        backend(
            "weaviate",
            "Weaviate",
            BackendRole::Vector,
            false,
            false,
            &[],
        ),
        backend(
            "pinecone",
            "Pinecone",
            BackendRole::Vector,
            false,
            true,
            &[],
        ),
        backend("minio", "MinIO", BackendRole::Object, false, false, &[]),
        backend("s3", "S3", BackendRole::Object, false, true, &[]),
        backend(
            "azureblob",
            "Azure Blob",
            BackendRole::Object,
            false,
            true,
            &[],
        ),
        backend(
            "gcs",
            "Google Cloud Storage",
            BackendRole::Object,
            false,
            true,
            &[],
        ),
        backend(
            "mongodb",
            "MongoDB",
            BackendRole::Document,
            false,
            false,
            &[],
        ),
        backend(
            "elasticsearch",
            "Elasticsearch",
            BackendRole::Search,
            false,
            false,
            &[],
        ),
        backend("neo4j", "Neo4j", BackendRole::Graph, false, false, &[]),
        backend(
            "cassandra",
            "Cassandra / ScyllaDB",
            BackendRole::Column,
            false,
            false,
            &["Schema generation and migration support may be partial for some workflows."],
        ),
    ]
}

pub fn native_service_catalog() -> Vec<NativeServiceCatalogEntry> {
    [
        ("analytics", "Analytics", &[][..], false),
        ("apikey", "API keys", &[][..], false),
        ("asset", "Asset pipeline", &[][..], false),
        ("authn", "Authentication", &["auth"], false),
        ("authz", "Authorization", &["auth"], false),
        ("control", "Control plane", &[][..], true),
        ("idp", "Identity providers", &[][..], false),
        ("notification", "Notification", &["notify"], false),
        ("storage", "Storage", &[][..], false),
        ("tenant", "Tenant", &["tenant/org"], true),
        ("webrtc_room", "WebRTC rooms", &["webrtc"], false),
        ("webrtc_peer", "WebRTC peers", &["webrtc"], false),
        ("webrtc_track", "WebRTC tracks", &["webrtc"], false),
        ("webrtc_turn", "WebRTC TURN", &["webrtc"], false),
        ("webrtc_signaling", "WebRTC signaling", &["webrtc"], false),
    ]
    .into_iter()
    .map(
        |(id, label, aliases, default_enabled)| NativeServiceCatalogEntry {
            id: id.to_string(),
            label: label.to_string(),
            aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
            default_enabled,
        },
    )
    .collect()
}

pub fn framework_catalog() -> Vec<FrameworkCatalogEntry> {
    [
        ("laravel", "php", "Laravel", true),
        ("symfony", "php", "Symfony", false),
        ("fastapi", "python", "FastAPI", true),
        ("starlette", "python", "Starlette", false),
        ("react-vite", "typescript", "React / Vite", true),
        ("next", "typescript", "Next.js", true),
        ("express", "typescript", "Express", false),
        ("fastify", "typescript", "Fastify", false),
        ("go-net-http", "go", "net/http", true),
        ("go-grpc", "go", "gRPC", false),
        ("spring-boot", "java", "Spring Boot", true),
        ("aspnet-core", "csharp", "ASP.NET Core", true),
    ]
    .into_iter()
    .map(|(id, language, label, primary)| FrameworkCatalogEntry {
        id: id.to_string(),
        language: language.to_string(),
        label: label.to_string(),
        primary,
    })
    .collect()
}

pub fn default_backend_ids_for_features(features: &[String]) -> Vec<String> {
    let mut ids = vec!["postgres".to_string(), "redis".to_string()];
    if contains_feature(features, &["vector", "search", "embeddings"]) {
        ids.push("qdrant".to_string());
    }
    if contains_feature(features, &["storage", "asset", "assets", "object"]) {
        ids.push("minio".to_string());
    }
    ids
}

pub fn normalize_backend_id(value: &str) -> String {
    let normalized = normalize_token(value);
    match normalized.as_str() {
        "pg" | "postgresql" => "postgres".to_string(),
        "mariadb" => "mysql".to_string(),
        "mssql" | "sql-server" | "sql_server" => "sqlserver".to_string(),
        "mongo" => "mongodb".to_string(),
        "elastic" | "opensearch" => "elasticsearch".to_string(),
        "azure-blob" | "azure_blob" | "blob" => "azureblob".to_string(),
        "google-cloud-storage" | "google_cloud_storage" => "gcs".to_string(),
        "scylla" | "scylladb" => "cassandra".to_string(),
        "object" | "object-store" | "object_storage" => "minio".to_string(),
        _ => normalized,
    }
}

pub fn normalize_framework_id(value: &str) -> String {
    let normalized = normalize_token(value);
    match normalized.as_str() {
        "vite" | "react" | "react-vite-ts" => "react-vite".to_string(),
        "nextjs" | "next-js" => "next".to_string(),
        "aspnet" | "aspnetcore" | "asp-net-core" => "aspnet-core".to_string(),
        "spring" => "spring-boot".to_string(),
        "go" | "net-http" | "net_http" => "go-net-http".to_string(),
        "grpc-go" | "go-grpc" => "go-grpc".to_string(),
        _ => normalized,
    }
}

pub fn expand_native_service_selection(value: &str) -> Vec<String> {
    let normalized = normalize_token(value);
    match normalized.as_str() {
        "auth" => vec![
            "authn".to_string(),
            "authz".to_string(),
            "apikey".to_string(),
        ],
        "identity" => vec!["authn".to_string(), "authz".to_string(), "idp".to_string()],
        "tenant-org" | "tenant_org" | "org" | "organization" => vec!["tenant".to_string()],
        "notify" | "notifications" => vec!["notification".to_string()],
        "assets" | "media" => vec!["asset".to_string(), "storage".to_string()],
        "webrtc" => vec![
            "webrtc_room".to_string(),
            "webrtc_peer".to_string(),
            "webrtc_track".to_string(),
            "webrtc_turn".to_string(),
            "webrtc_signaling".to_string(),
        ],
        _ => {
            let aliases = native_service_catalog()
                .into_iter()
                .find(|entry| {
                    entry.id == normalized || entry.aliases.iter().any(|alias| alias == &normalized)
                })
                .map(|entry| vec![entry.id]);
            aliases.unwrap_or_else(|| vec![normalized])
        }
    }
}

fn normalize_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(' ', "-")
}

fn backend(
    id: &str,
    label: &str,
    role: BackendRole,
    default_for_local: bool,
    requires_secret_prompt: bool,
    warnings: &[&str],
) -> BackendCatalogEntry {
    BackendCatalogEntry {
        id: id.to_string(),
        label: label.to_string(),
        role,
        default_for_local,
        requires_secret_prompt,
        warnings: warnings.iter().map(|warning| warning.to_string()).collect(),
    }
}

fn contains_feature(features: &[String], needles: &[&str]) -> bool {
    features.iter().any(|feature| {
        let normalized = feature.trim().to_ascii_lowercase();
        needles.iter().any(|needle| normalized == *needle)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_catalog_matches_plan_ids() {
        let ids: Vec<_> = backend_catalog()
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(
            ids,
            vec![
                "postgres",
                "mysql",
                "sqlite",
                "sqlserver",
                "clickhouse",
                "redis",
                "memcached",
                "qdrant",
                "weaviate",
                "pinecone",
                "minio",
                "s3",
                "azureblob",
                "gcs",
                "mongodb",
                "elasticsearch",
                "neo4j",
                "cassandra",
            ]
        );
    }

    #[test]
    fn feature_defaults_add_specialized_backends() {
        let defaults =
            default_backend_ids_for_features(&["vector".to_string(), "storage".to_string()]);
        assert!(defaults.contains(&"postgres".to_string()));
        assert!(defaults.contains(&"redis".to_string()));
        assert!(defaults.contains(&"qdrant".to_string()));
        assert!(defaults.contains(&"minio".to_string()));
    }
}
