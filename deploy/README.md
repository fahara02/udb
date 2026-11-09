# Deploy UDB with docker compose

A single-host deployment of the UDB broker: the pinned release image, Postgres
(PostGIS + pg_partman), generated secrets, a first admin and default-deny data
policies. Redis, MinIO, Qdrant, Kafka and LiveKit are in the same compose file,
commented out; turn each one on when a native service you use needs it.

For a TLS proxy in front of the broker, see
[`examples/docker_nginx`](../examples/docker_nginx/README.md). It uses the scripts
in this directory.

| File | What it is |
|---|---|
| `docker-compose.yml` | broker + Postgres on; optional backends as commented blocks |
| `.env.example` | every setting, one line each; copied to `.env` by `init.sh` |
| `scripts/init.sh` | `.env` + generated secrets + RS256 key pair (runs `udb env fill` in the image) |
| `scripts/bootstrap.sh` | first admin (`udb auth bootstrap user`) + data policies (`udb policy apply`) |
| `postgres/Dockerfile` | Postgres 16 + PostGIS + pg_partman |
| `proto/` | the schema the broker serves; a placeholder `app.v1.Note`, replace it with yours |

## What you need

- Linux with Docker Engine and the compose plugin 2.24 or newer (`docker compose version`).
- An x86-64 host. The broker image is published for `linux/amd64` only.
- Outbound access to `ghcr.io`, Docker Hub and GitHub (the Postgres image
  builds pg_partman from source the first time).

## 1. Put your protos in place

The broker serves the protos mounted at `/srv/proto` from `./proto`. The
placeholder `proto/app/v1/note.proto` lets the stack boot as shipped. Replace it
with your own package tree, for example `proto/shop/v1/order.proto`. You don't
need to copy UDB's annotation protos: the broker embeds them, so
`import "udb/core/common/v1/db.proto";` resolves on its own.

## 2. Generate `.env` and the secrets

```bash
cd deploy
./scripts/init.sh
```

`init.sh` copies `.env.example` to `.env` and generates the Postgres password
and `UDB_PASSWORD_HASH_SECRET`. It then runs `udb env fill --profile dev` inside
the pinned broker image. That adds the session, API-key and approval secrets,
the encryption key, and an RS256 key pair under `./secrets`. Re-running it is
safe: a value already set is never changed.

`./secrets` ends up owned by the image's `udb` user, because the broker runs as
that user and must read the private key. Your own user can't list it any more.
That is expected.

> **Back up `.env` and `./secrets` together, now.** `UDB_ENCRYPTION_KEY`
> encrypts secret columns and vault material. If you lose it, you lose that data.

## 3. Create the first admin and the data policies

```bash
./scripts/bootstrap.sh
# or choose the values:
UDB_ADMIN_USER=ops UDB_ADMIN_PASSWORD='S0me-long-passphrase!' UDB_TENANT=acme ./scripts/bootstrap.sh
```

Both steps run offline against Postgres, through one-off containers of the
broker image:

```bash
docker compose run --rm udb auth bootstrap user \
  --username admin --email admin@example.com --password '…' --tenant acme --project default
docker compose run --rm -v "$PWD/policies:/etc/udb/policies:ro" udb \
  policy apply -f /etc/udb/policies/<tenant-uuid>.json --tenant <tenant-uuid>
```

`auth bootstrap user` creates the tenant and the first user. It binds the user
to `organization_owner`, so a login carries `udb:*`, and it prints the
canonical tenant UUID. Data-plane authorization is **default-deny**: until a
policy allows an action, every data RPC returns `PERMISSION_DENIED`, even for
the organization owner. `bootstrap.sh` writes
`policies/<tenant-uuid>.json` with allow rules for `Select`, `Upsert` and
`Delete` in that tenant, gated on `udb:read` and `udb:write`, and applies it.
For least privilege, narrow `subject` and `resource` in that file and re-run
`./scripts/bootstrap.sh`. On a second run it skips user creation and only
re-applies the policies.

The admin credentials and tenant UUID go to `admin.env` (owner-only, git-ignored).

## 4. Start

```bash
docker compose up -d --wait
```

`--wait` returns once the broker's health check (`grpc_health_probe` on the data
port) passes. The first start migrates UDB's own schemas and your protos' tables,
so give it up to a minute.

## 5. Verify

```bash
docker compose ps                                               # both services "healthy"
docker compose exec udb grpc_health_probe -addr=127.0.0.1:50051  # data plane: status: SERVING
docker compose exec udb grpc_health_probe -addr=127.0.0.1:50061  # native control plane
docker compose exec udb udb requirements                         # backends your protos need, and whether they are set
docker compose exec udb udb doctor --enterprise --human          # configuration + enterprise preflight
docker compose logs -f udb                                       # look for "UDB DataBroker is ready"
curl -s http://127.0.0.1:50052/readyz                           # readiness, from the metrics listener
curl -s http://127.0.0.1:50052/metrics | head                    # Prometheus metrics
```

Then connect a client. The data plane is on `127.0.0.1:50051` and the native
control plane (login, authz, storage, ...) on `127.0.0.1:50061`. Every SDK takes
both addresses, for example Python
`create_udb(target="127.0.0.1:50051", auth_target="127.0.0.1:50061", ...)`. The
enterprise examples (`examples/{go,python,ts}_enterprise`) log in with the
values in `admin.env`.

## Ports

| Port | Listener | Default binding |
|---|---|---|
| 50051 | data plane (`udb.services.v1.DataBroker`) | `127.0.0.1` |
| 50061 | native control plane (`udb.core.*`: authn, authz, storage, ...) | `127.0.0.1` |
| 50052 | Prometheus metrics | `127.0.0.1` |
| 50071 | WebRTC peer listener (commented out) | `127.0.0.1` |
| 5432 | Postgres | `127.0.0.1` |

Every binding uses `UDB_BIND` from `.env`. Keep it on loopback and put a TLS
proxy in front, as in [`examples/docker_nginx`](../examples/docker_nginx/README.md).
Setting `UDB_BIND=0.0.0.0` exposes plaintext gRPC on every interface.

## Turn on a backend

Each backend has three parts in `docker-compose.yml`, all commented out:

- the service block;
- its lines in the broker's `environment:`;
- its `depends_on` entry and named volume.

Uncomment all three, then run `docker compose up -d --wait`. After that,
`docker compose exec udb udb requirements` shows the backend as `OK`.

Which native service needs which backend (from
[`docs/generated/native-services.md`](../docs/generated/native-services.md)):

| Backend | Native services | Broker settings |
|---|---|---|
| Postgres (always on) | authn, authz, apikey, tenant, idp, vault, config, control, analytics, metering, scheduler, workflow, webhook, lock, livequery, search, embedding, messaging, webrtc room/track/peer/turn | `UDB_PG_DSN` |
| Redis | cache; shared rate limiting across replicas | `UDB_REDIS_DSN` |
| MinIO / S3 | storage, asset, backup artifacts, proto-declared buckets | `UDB_MINIO_ENDPOINT`, `UDB_MINIO_ACCESS_KEY`, `UDB_MINIO_SECRET_KEY`, `UDB_MINIO_REGION`, `UDB_STORAGE_BUCKET` |
| Qdrant | proto-declared vector collections | `UDB_QDRANT_URL` |
| Kafka | notification, CDC to Kafka topics, the embedding sidecar | `UDB_KAFKA_BROKERS`, `UDB_CDC_JOURNAL_ONLY=false` |
| LiveKit | WebRTC media through an SFU | `UDB_LIVEKIT_URL`, `UDB_LIVEKIT_API_KEY`, `UDB_LIVEKIT_API_SECRET`, `UDB_WEBRTC_GRPC_ADDR` |

### Turn on Redis (cache service, shared rate limiting)

1. Uncomment the `redis` service, `udb-redis` volume, `UDB_REDIS_DSN` line and
   `redis` `depends_on` entry.
2. `docker compose up -d --wait`.

AOF persistence stays on (`--appendonly yes`), because UDB's Redis store
refuses to run without it.

### Turn on storage (MinIO: storage, asset, backup)

1. In `.env`, uncomment `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`, and set a
   long random password (`od -An -N24 -tx1 /dev/urandom | tr -d ' \n'`).
2. Uncomment the `minio` and `minio-bootstrap` services, the `udb-minio`
   volume, the five `UDB_MINIO_*` / `UDB_STORAGE_BUCKET` lines and the
   `minio-bootstrap` `depends_on` entry.
3. `docker compose up -d --wait`. `minio-bootstrap` creates the `udb-storage`
   bucket that StorageService presigns against. Without the bucket, presigned
   uploads return 404.

Presigned URLs carry the endpoint host the broker signs with (`minio:9000`).
Clients outside the compose network need that name to resolve to MinIO, for
example through a DNS name for MinIO that both the broker and the clients use
as `UDB_MINIO_ENDPOINT`. For a managed S3 bucket, drop the MinIO blocks. Point
`UDB_MINIO_ENDPOINT`, the keys and `UDB_MINIO_REGION` at it, and set
`UDB_STORAGE_BUCKET` to a bucket that already exists.

### Turn on Qdrant (vector collections)

Uncomment the `qdrant` service, the `udb-qdrant` volume, `UDB_QDRANT_URL` and
the `depends_on` entry. Only protos that declare a vector store need it.
`udb requirements` lists them.

### Turn on Kafka (notification, CDC to Kafka, embedding sidecar)

1. Uncomment the `kafka` service, the `udb-kafka` volume, the `depends_on`
   entry, and both `UDB_KAFKA_BROKERS` and `UDB_CDC_JOURNAL_ONLY: "false"`.
   The value in `environment:` overrides `UDB_CDC_JOURNAL_ONLY=true` from `.env`.
2. `docker compose up -d --wait`.

The block runs the official `apache/kafka` image as one KRaft node, reachable as
`kafka:9092` inside the compose network. Topic auto-creation is off. Create the
topics your deployment uses (`docker compose exec kafka
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic
<name>`), or set `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` for a trial. Without
Kafka, `UDB_CDC_JOURNAL_ONLY=true` delivers UDB's events through the durable
Postgres journal and local streams.

### Turn on LiveKit (WebRTC SFU)

1. Uncomment the `livekit` service, the five `UDB_LIVEKIT_*` /
   `UDB_WEBRTC_GRPC_ADDR` lines, the `50071` port and the `depends_on` entry.
2. `docker compose up -d --wait`.

`--dev` LiveKit uses the `devkey`/`secret` pair and plaintext `ws://`, which the
broker accepts only with `UDB_LIVEKIT_ALLOW_INSECURE=1`. For real use, give
LiveKit its own keys and a TLS (`wss://`) endpoint, and remove that line.
Browsers connect to LiveKit directly, so its ports must be reachable from them.

### Turn a native service off

Every native service is on by default. To switch one off, add
`UDB_NATIVE_<ID>_ENABLED=false` to `.env`, for example
`UDB_NATIVE_NOTIFICATION_ENABLED=false`. The IDs are in the first column of
`docs/generated/native-services.md`, upper-cased.

Once every backend your services use is configured, set
`UDB_ALLOW_DEGRADED_BACKENDS=false`. A missing backend then stops the boot,
instead of the affected services answering `FAILED_PRECONDITION`.

## Switching to production

The shipped `.env` keeps `UDB_ENV=development`: the broker speaks plaintext gRPC
on the compose network, and TLS belongs to a proxy in front of it. Login is
still real (RS256 JWT), and authorization is still default-deny. Pick one of
these two hardened postures. `udb doctor --enterprise --human` reports what is
still missing for either one.

**A. Behind a TLS proxy, fail-closed.** Keep `UDB_ENV=development`, keep the
broker ports off the public network, and add to `.env`:

```bash
UDB_FAIL_CLOSED=true                          # deny on dependency errors; refuse to start until the checklist passes
UDB_TRUSTED_TRANSPORT=true                    # acknowledges that TLS is terminated by the proxy in front
UDB_AUDIT_SINK=postgres                       # data-plane mutation audit into Postgres
UDB_AUDIT_PG_TABLE=udb_system.data_audit_log  # a dedicated table; the sink creates it
UDB_AUDIT_EXPORT_POSTGRES=1                   # auth-plane audit (udb_system.auth_audit_log)
```

**B. Broker-terminated TLS with mandatory mTLS.** Set `UDB_ENV=production`. That
forces broker TLS and client-certificate checks on every listener. Mount the
certificates (for example `./tls:/run/udb-tls:ro`) and set:

```bash
UDB_ENV=production
UDB_TLS_REQUIRED=true
UDB_TLS_CERT_PATH=/run/udb-tls/server.crt
UDB_TLS_KEY_PATH=/run/udb-tls/server.key
UDB_MTLS_REQUIRED=true
UDB_MTLS_CLIENT_CA_PATH=/run/udb-tls/client-ca.pem
```

Production mode also turns on `UDB_PG_TLS_REQUIRED`. Either enable TLS on
Postgres and use `sslmode=require` (or stricter) in `UDB_PG_DSN`, or set
`UDB_PG_TLS_REQUIRED=false` explicitly when Postgres is only on the private
compose network. `udb env --profile enterprise` prints the full production
template to compare against.

In both postures:

- **Migrations.** Set `UDB_MIGRATE_ENABLED=false`. Then a restart applies no
  schema change to your protos unless an approved plan covers it, so schema
  changes go through `udb plan`.
- **Ports.** Publish only the proxy. Remove the broker and Postgres `ports:`
  entries, or keep them on loopback.
- **Image.** Pin `UDB_VERSION`, and upgrade one release at a time
  (`docs/upgrading.md`).
- **Backups.** Run `docker compose exec -T postgres pg_dump -U udb -Fc udb >
  udb-$(date +%F).dump` on a schedule. Keep `.env` and `./secrets` with the
  dumps, but stored separately from them. Rehearse a restore.

## Everyday operations

```bash
docker compose logs -f udb                     # broker logs
docker compose pull udb && docker compose up -d --wait   # after changing UDB_VERSION
docker compose restart udb                     # pick up .env changes
docker compose down                            # stop; volumes (data) are kept
docker compose down -v                         # stop AND DELETE all data
```

## Troubleshooting

- **`run ./scripts/init.sh first`** comes from compose. `.env` is missing or
  still has no `POSTGRES_PASSWORD`.
- **Login returns `UNIMPLEMENTED`**: the client sent it to the data port.
  Login is on the native control plane (`:50061`).
- **Every data call returns `PERMISSION_DENIED`**: no policy allows it. Check
  `policies/<tenant-uuid>.json` and re-run `./scripts/bootstrap.sh`.
- **Reads return no rows**: the client used the tenant code (`acme`) instead
  of the canonical tenant UUID in `admin.env`.
- **`permission denied` reading `/run/udb-secrets/jwt-private.pem`**: re-run
  `./scripts/init.sh`, which hands `./secrets` to the image's `udb` user. With
  rootless Docker or SELinux, see the notes below.
- **SELinux hosts (Fedora, RHEL)**: append `:z` to the `./proto` and
  `./secrets` volume entries.
- **Rootless Docker**: the `chown` in `init.sh` maps `./secrets` to one of
  your subordinate UIDs on the host. Inside the container it is still owned by
  `udb`, so the broker can read it. To inspect the files from the host, use
  `docker run --rm -v "$PWD/secrets:/s" --entrypoint ls <image> -l /s`.
