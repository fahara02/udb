FROM postgres:16-alpine AS pg-partman-builder

ARG PG_PARTMAN_VERSION=5.2.4

RUN apk add --no-cache build-base clang git llvm
RUN git clone --depth 1 --branch "v${PG_PARTMAN_VERSION}" https://github.com/pgpartman/pg_partman.git /tmp/pg_partman
RUN cd /tmp/pg_partman && make NO_BGW=1 && make NO_BGW=1 install

FROM postgres:16-alpine

COPY --from=pg-partman-builder /usr/local/share/postgresql/extension/pg_partman* /usr/local/share/postgresql/extension/
