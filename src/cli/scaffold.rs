//! main.rs split — scaffold (Phase H).
use super::*;

/// Emit a minimal project scaffold to the current directory (or UDB_INIT_DIR).
pub(crate) fn emit_init_project_scaffold() {
    let dir = env::var("UDB_INIT_DIR").unwrap_or_else(|_| ".".to_string());
    for (rel_path, content) in scaffold_files() {
        let path = format!("{dir}/{rel_path}");
        let parent = std::path::Path::new(&path).parent().unwrap();
        if let Err(e) = fs::create_dir_all(parent) {
            eprintln!("could not create directory {}: {e}", parent.display());
            continue;
        }
        if std::path::Path::new(&path).exists() {
            eprintln!("skipping {path} (already exists)");
            continue;
        }
        match fs::write(&path, content) {
            Ok(()) => eprintln!("created {path}"),
            Err(e) => eprintln!("failed to write {path}: {e}"),
        }
    }
    eprintln!(
        "\nProject scaffold created. Run `udb system-ddl | psql $DATABASE_URL` to bootstrap."
    );
}

/// The scaffold's `(relative_path, file_contents)` pairs. Extracted from the
/// emitter so the generated example clients can be string-/compile-checked in
/// tests and by the CI scaffold-compile gate (see `scripts/check-scaffold-compiles.sh`).
pub(crate) fn scaffold_files() -> Vec<(&'static str, &'static str)> {
    let proto_sample = r#"syntax = "proto3";
package myapp.v1;

import "udb/core/common/v1/db.proto";
import "udb/core/common/v1/security.proto";

message User {
  option (udb.core.common.v1.pg_table) = {
    table_name: "users"
    schema_name: "app"
    is_table: true
    enable_rls: true
  };

  option (udb.core.common.v1.db_table_security) = {
    tenant_column: "tenant_id"
    tenant_isolation_mode: "tenant"
  };

  string id = 1 [(udb.core.common.v1.pg_column) = {
    column_name: "id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
  }];
  string tenant_id = 2 [(udb.core.common.v1.pg_column) = {
    column_name: "tenant_id"
    sql_type: "UUID"
    not_null: true
  }];
  string email = 3 [
    (udb.core.common.v1.pg_column) = {
      column_name: "email"
      sql_type: "TEXT"
      encrypted: true
    },
    (udb.core.common.v1.pii) = true,
    (udb.core.common.v1.log_masked) = true,
    (udb.core.common.v1.data_purpose) = "login"
  ];
  string created_at = 4 [(udb.core.common.v1.pg_column) = {
    column_name: "created_at"
    sql_type: "TIMESTAMPTZ"
    not_null: true
    default_value: "now()"
  }];
}
"#;
    let config_template = r#"# configs/database.yaml — UDB runtime configuration template
# Copy and customize this file. Expand env-vars with ${VAR} syntax.

tier1_postgres:
  primary:
    dsn: "${DATABASE_URL}"
    max_connections: 50

tier2_redis:
  session:
    dsn: "${REDIS_URL}"

tier3_qdrant:
  embeddings:
    url: "${QDRANT_URL}"

tier4_minio:
  artifacts:
    endpoint: "${S3_ENDPOINT}"
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
    region: "us-east-1"
"#;
    let docker_compose = r#"# docker-compose.udb.yml — Local UDB development environment
version: "3.8"
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: udb
      POSTGRES_PASSWORD: udb
      POSTGRES_DB: udb
    ports: ["5432:5432"]

  redis:
    image: redis:7
    ports: ["6379:6379"]

  qdrant:
    image: qdrant/qdrant:latest
    ports: ["6333:6333"]

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    ports: ["9092:9092"]
    depends_on: [zookeeper]

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
"#;
    let go_client = r#"// examples/go/client.go — minimal UDB gRPC client (Go)
// go get google.golang.org/grpc github.com/fahara02/udb/sdk/go/gen/udb/entity/v1 github.com/fahara02/udb/sdk/go/gen/udb/services/v1
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := servicesv1.NewDataBrokerClient(conn)
	resp, err := c.GetHealthReport(context.Background(), &entityv1.HealthReportRequest{
		Context: &entityv1.RequestContext{Purpose: "health", ServiceIdentity: "example"},
		WithProbes: false,
	})
	if err != nil {
		log.Fatalf("GetHealthReport: %v", err)
	}
	b, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(b))
}
"#;
    let python_client = r#"# examples/python/client.py — minimal UDB gRPC client (Python)
# pip install grpcio grpcio-tools
import grpc, json, sys
sys.path.insert(0, "gen/python")
from google.protobuf.json_format import MessageToDict
from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc

def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = data_broker_pb2_grpc.DataBrokerStub(channel)
    resp = stub.GetHealthReport(types_pb2.HealthReportRequest(
        context=types_pb2.RequestContext(purpose="health", service_identity="example"),
        with_probes=False,
    ))
    print(json.dumps(MessageToDict(resp), indent=2))

if __name__ == "__main__":
    main()
"#;
    let typescript_client = r#"// examples/typescript/client.ts — minimal UDB gRPC client (TypeScript)
// npm install @grpc/grpc-js @grpc/proto-loader
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

const PROTO_PATH = path.resolve(__dirname, "../../proto/udb/services/v1/data_broker.proto");
const def = protoLoader.loadSync(PROTO_PATH, { keepCase: true, longs: String });
const udbProto = grpc.loadPackageDefinition(def) as any;
const client = new udbProto.udb.services.v1.DataBroker(
  "localhost:50051",
  grpc.credentials.createInsecure()
);

client.GetHealthReport(
  { context: { purpose: "health", service_identity: "example" }, with_probes: false },
  (err: Error | null, resp: unknown) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(JSON.stringify(resp, null, 2));
  }
);
"#;
    let csharp_client = r#"// examples/csharp/Client.cs — minimal UDB gRPC client (C#)
// dotnet add package Grpc.Net.Client Google.Protobuf Grpc.Tools
using Grpc.Net.Client;
using Udb.Entity.V1;
using Udb.Services.V1;

using var channel = GrpcChannel.ForAddress("http://localhost:50051");
var client = new DataBroker.DataBrokerClient(channel);
var resp = await client.GetHealthReportAsync(new HealthReportRequest {
    Context = new RequestContext { Purpose = "health", ServiceIdentity = "example" },
    WithProbes = false
});
Console.WriteLine(resp);
"#;
    vec![
        ("proto/app/v1/user.proto", proto_sample),
        ("configs/database.yaml", config_template),
        ("docker-compose.udb.yml", docker_compose),
        ("examples/go/client.go", go_client),
        ("examples/python/client.py", python_client),
        ("examples/typescript/client.ts", typescript_client),
        ("examples/csharp/Client.cs", csharp_client),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(rel: &str) -> &'static str {
        scaffold_files()
            .into_iter()
            .find(|(p, _)| *p == rel)
            .unwrap_or_else(|| panic!("scaffold missing {rel}"))
            .1
    }

    #[test]
    fn go_example_uses_the_real_module_path_and_grpc_newclient() {
        let go = file("examples/go/client.go");
        // Real published module path (was the fictional github.com/udb-project/...).
        assert!(
            go.contains("github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"),
            "Go example must import the real entity gen path"
        );
        assert!(
            go.contains("github.com/fahara02/udb/sdk/go/gen/udb/services/v1"),
            "Go example must import the real services gen path"
        );
        // The fictional org must not reappear.
        assert!(
            !go.contains("github.com/udb-project/"),
            "Go example still references the fictional udb-project org"
        );
        // grpc.Dial is deprecated; the example must use grpc.NewClient.
        assert!(
            go.contains("grpc.NewClient("),
            "Go example must use grpc.NewClient"
        );
        assert!(
            !go.contains("grpc.Dial("),
            "Go example must not use the deprecated grpc.Dial"
        );
    }

    #[test]
    fn typescript_example_loads_the_data_broker_proto() {
        let ts = file("examples/typescript/client.ts");
        assert!(
            ts.contains("@grpc/grpc-js"),
            "TS example must import @grpc/grpc-js"
        );
        assert!(
            ts.contains("data_broker.proto"),
            "TS example must load the DataBroker proto"
        );
    }

    #[test]
    fn scaffold_proto_catalogs_with_default_parser_options() {
        let root = std::env::temp_dir().join(format!(
            "udb_scaffold_quickstart_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        ));
        for (rel, content) in scaffold_files()
            .into_iter()
            .filter(|(rel, _)| rel.ends_with(".proto"))
        {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().expect("scaffold proto has parent"))
                .expect("create scaffold proto parent");
            std::fs::write(&path, content).expect("write scaffold proto");
        }

        let schemas = udb::parse_directory(root.join("proto"), &udb::ParserConfig::default())
            .expect("default parser should read scaffold annotations");
        let _ = std::fs::remove_dir_all(&root);

        let user = schemas
            .iter()
            .find(|schema| schema.message_name == "User")
            .expect("scaffold should yield a User schema");
        assert_eq!(user.table_name, "users");
        assert_eq!(user.schema_name, "app");
        assert!(user.is_table);
        assert_eq!(user.columns.len(), 4);
        assert!(
            user.columns
                .iter()
                .any(|column| column.column_name == "email" && column.security.is_pii),
            "scaffold email field should retain scalar security metadata"
        );
    }
}
