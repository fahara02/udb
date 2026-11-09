# PostgreSQL 16 + PostGIS + pg_partman, for the UDB broker.
#
# Same recipe as docker/postgres-pg-partman/Dockerfile (the image the integration
# stack runs), copied here so deploy/ builds on its own:
#   * PostGIS — consumers hit the geography type on their first geo column.
#   * pg_partman — UDB's analytics tables are time-partitioned. Without the
#     extension the broker still boots, but it warns that the premade partitions
#     run out and later writes to those tables fail.
ARG PG_BASE_IMAGE=postgres:16-alpine
ARG POSTGIS_BASE_IMAGE=postgis/postgis:16-3.5-alpine

FROM ${PG_BASE_IMAGE} AS pg-partman-builder

ARG PG_PARTMAN_VERSION=5.2.4

RUN apk add --no-cache build-base clang git llvm
RUN git clone --depth 1 --branch "v${PG_PARTMAN_VERSION}" https://github.com/pgpartman/pg_partman.git /tmp/pg_partman
RUN cd /tmp/pg_partman && make NO_BGW=1 && make NO_BGW=1 install

FROM ${POSTGIS_BASE_IMAGE}

COPY --from=pg-partman-builder /usr/local/share/postgresql/extension/pg_partman* /usr/local/share/postgresql/extension/
