# syntax=docker/dockerfile:1.7

FROM rust:1.91-bookworm AS builder

WORKDIR /workspace

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Retries=5 \
    && apt-get install -y --no-install-recommends cmake clang curl libcurl4-openssl-dev ca-certificates protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

COPY src/udb/Cargo.toml src/udb/Cargo.lock src/udb/build.rs ./src/udb/
COPY src/udb/src ./src/udb/src
COPY src/udb/tests ./src/udb/tests
COPY src/udb/benches ./src/udb/benches
COPY proto ./proto
RUN mkdir -p /workspace/proto/google/api \
    && cat > /workspace/proto/google/api/http.proto <<'EOF'
syntax = "proto3";
package google.api;

message Http {
  repeated HttpRule rules = 1;
  bool fully_decode_reserved_expansion = 2;
}

message HttpRule {
  string selector = 1;
  oneof pattern {
    string get = 2;
    string put = 3;
    string post = 4;
    string delete = 5;
    string patch = 6;
    CustomHttpPattern custom = 8;
  }
  string body = 7;
  string response_body = 12;
  repeated HttpRule additional_bindings = 11;
}

message CustomHttpPattern {
  string kind = 1;
  string path = 2;
}
EOF
RUN cat > /workspace/proto/google/api/annotations.proto <<'EOF'
syntax = "proto3";
package google.api;

import "google/api/http.proto";
import "google/protobuf/descriptor.proto";

extend google.protobuf.MethodOptions {
  HttpRule http = 72295728;
}
EOF

WORKDIR /workspace/src/udb
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/src/udb/target \
    cargo build --release --bin udb-proto-parser \
    && cp target/release/udb-proto-parser /tmp/udb-proto-parser

ARG GRPC_HEALTH_PROBE_VERSION=v0.4.37
RUN curl -fsSL \
    "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64" \
    -o /usr/local/bin/grpc_health_probe \
    && chmod +x /usr/local/bin/grpc_health_probe

FROM debian:bookworm-slim AS runtime

RUN groupadd --system udb \
    && useradd --system --gid udb --home-dir /app --shell /usr/sbin/nologin udb

WORKDIR /app
COPY --from=builder /tmp/udb-proto-parser /usr/local/bin/udb-proto-parser
COPY --from=builder /usr/local/bin/grpc_health_probe /usr/local/bin/grpc_health_probe
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY proto ./proto
COPY src/udb/configs ./configs

ENV RUST_LOG=info \
    UDB_METRICS_ADDR=0.0.0.0:50052

EXPOSE 50051 50052
USER udb:udb

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/grpc_health_probe", "-addr=127.0.0.1:50051"]

ENTRYPOINT ["/usr/local/bin/udb-proto-parser"]
CMD ["serve", "/app/proto", "", "0.0.0.0:50051"]
