# syntax=docker/dockerfile:1.7
#
# UDB broker image. Builds the `udb-proto-parser` binary from the ROOT crate
# (Cargo.toml + src/ live at the repo root; there is no src/udb/ crate) and runs
# it as the gRPC broker. `build.rs` vendors protoc via `protoc-bin-vendored`, and
# the google/api annotation protos are checked in under third_party/googleapis.

FROM rust:1.91-bookworm AS builder

WORKDIR /workspace

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Retries=5 \
    && apt-get install -y --no-install-recommends build-essential cmake clang curl perl nasm ninja-build pkg-config libcurl4-openssl-dev ca-certificates protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Manifests and workspace members first (better layer caching). The workspace
# declares crates/udb-portable as a member, so its manifest must be present.
COPY Cargo.toml Cargo.lock build.rs ./
COPY crates ./crates
COPY src ./src
COPY proto ./proto
COPY third_party ./third_party
COPY configs ./configs
# `[[bench]] core_bench` resolves to benches/core_bench.rs; cargo needs the file
# to exist when it parses the manifest, even for a --bin build.
COPY benches ./benches

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin udb-proto-parser --features oidc,webauthn \
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
# UDB-owned proto + vendored imports + default configs. Application schemas
# (e.g. the acme_billing example) are mounted in at runtime and served via an
# overridden CMD.
COPY proto ./proto
COPY third_party ./third_party
COPY configs ./configs

ENV RUST_LOG=info \
    UDB_METRICS_ADDR=0.0.0.0:50052

EXPOSE 50051 50052
USER udb:udb

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/grpc_health_probe", "-addr=127.0.0.1:50051"]

ENTRYPOINT ["/usr/local/bin/udb-proto-parser"]
CMD ["serve", "/app/proto", "", "0.0.0.0:50051"]
