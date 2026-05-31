//! main.rs split — scaffold (Phase H).
use super::*;

/// Emit a minimal project scaffold to the current directory (or UDB_INIT_DIR).
pub(crate) fn emit_init_project_scaffold() {
    let dir = env::var("UDB_INIT_DIR").unwrap_or_else(|_| ".".to_string());
    let proto_sample = r#"syntax = "proto3";
package myapp.v1;

message User {
  option (myapp.v1.table) = {
    table_name: "users"
    schema_name: "app"
    is_table: true
    enable_rls: true
    tenant_column: "tenant_id"
  };

  string id = 1 [(myapp.v1.column) = {
    column_name: "id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
  }];
  string tenant_id = 2 [(myapp.v1.column) = {
    column_name: "tenant_id"
    tenant_column: true
    not_null: true
  }];
  string email = 3 [(myapp.v1.column) = {
    column_name: "email"
    sql_type: "TEXT"
    pii_kind: PII_KIND_EMAIL
    encrypt: true
  }];
  string created_at = 4 [(myapp.v1.column) = {
    column_name: "created_at"
    sql_type: "TIMESTAMPTZ"
    is_created_at: true
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
// go get google.golang.org/grpc github.com/udb-project/udb/gen/go/udb/entity/v1 github.com/udb-project/udb/gen/go/udb/services/v1
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	entityv1 "github.com/udb-project/udb/gen/go/udb/entity/v1"
	servicesv1 "github.com/udb-project/udb/gen/go/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
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
    let files = [
        ("proto/app/v1/user.proto", proto_sample),
        ("configs/database.yaml", config_template),
        ("docker-compose.udb.yml", docker_compose),
        ("examples/go/client.go", go_client),
        ("examples/python/client.py", python_client),
        ("examples/typescript/client.ts", typescript_client),
        ("examples/csharp/Client.cs", csharp_client),
    ];
    for (rel_path, content) in &files {
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
