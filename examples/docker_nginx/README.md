# UDB behind nginx, in docker compose

A small project, `shop`, with one table (`shop.v1.Order`). It runs the UDB
broker, Postgres and nginx in docker compose. nginx terminates TLS on `:443`
and routes each gRPC call by its path:

| gRPC path | Goes to | What lives there |
|---|---|---|
| `/udb.services.v1.DataBroker/*` | `udb:50051` | data plane: select, upsert, delete, transactions |
| `/grpc.health.v1.Health/*` | `udb:50051` | health of the data plane |
| `/udb.core.*` | `udb:50061` | native control plane: login, authz, storage, tenants, ... |
| anything else | not forwarded | answered with gRPC `UNIMPLEMENTED` |

Clients use one address and one TLS channel for both planes. The broker and
Postgres publish no ports. Only nginx is reachable from outside the compose
network.

```
client ──TLS :443──> nginx ──h2c──> udb:50051 (DataBroker)
                           └──h2c──> udb:50061 (udb.core.*) ──> postgres
```

The setup reuses [`deploy/scripts`](../../deploy/README.md) for secrets, the
first admin and the policies, and builds Postgres from
[`deploy/postgres`](../../deploy/postgres/Dockerfile).

## What you need

- Linux x86-64 with Docker Engine and the compose plugin 2.24 or newer.
- `openssl` (for the self-signed certificate).
- Port 443 free on the host, or set `UDB_EDGE_PORT` in `.env`.

## Run it

```bash
cd examples/docker_nginx

# 1. Certificates, .env with generated secrets, first admin, data policies.
./scripts/setup.sh

# 2. Start Postgres, the broker and nginx; returns once all three are healthy.
docker compose up -d --wait

# 3. Run the client in a container; it talks only to nginx:443.
docker compose --profile client run --rm client
```

Expected output from step 3 (UUIDs differ):

```
connect: nginx:443 (TLS, verified against the example CA)
login:   admin -> tenant "acme" = 00000000-0000-0000-0000-0000000d0001
         scopes ['udb:admin', 'udb:*']
native:  Authorize(shop.v1.Order, Select) -> allowed=True
write:   shop.v1.Order 9b2d4c1e-…
read:    status=placed total_cents=4200

OK
```

### What each step did

`scripts/setup.sh` runs three scripts. Each one keeps what already exists, so
re-running is safe.

1. `scripts/certs.sh` creates a throwaway CA and a server certificate for
   `localhost`, `127.0.0.1` and `nginx`, in `certs/`. Pass extra DNS names as
   arguments.
2. `deploy/scripts/init.sh .` copies `.env.example` to `.env` and generates
   the Postgres password and `UDB_PASSWORD_HASH_SECRET`. It then runs
   `udb env fill --profile dev` inside the broker image, which adds the
   session, API-key and approval secrets, the encryption key, and an RS256 key
   pair in `secrets/`.
3. `deploy/scripts/bootstrap.sh .` runs `udb auth bootstrap user` in a one-off
   broker container. That creates tenant `acme` and user `admin`, bound to
   `organization_owner`. The script writes the credentials to `admin.env`. It
   then runs `udb policy apply` with allow rules for `Select`, `Upsert` and
   `Delete` in that tenant. Data-plane authorization is default-deny, so without
   these rules every data call returns `PERMISSION_DENIED`.

`docker compose up` serves `proto/shop/v1/order.proto`. On first start the
broker creates the `shop.orders` table.

The client ([`client/main.py`](client/main.py), Python SDK `udb-client`) does
three things:

- Logs in with `login_and_adopt_tenant`. That is AuthnService on the native
  plane, and it swaps the tenant code `acme` for the canonical tenant UUID.
- Makes one more native call, `AuthzService.Authorize`.
- Writes one order and reads it back on the data plane.

All of it goes through nginx: `UDB_TARGET=nginx:443`, with `certs/ca.crt` as the
trusted root.

## Run the client from the host instead

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r client/requirements.txt      # or the checkout's SDK: pip install ../../sdk/python
set -a; . ./admin.env; set +a
UDB_TARGET=localhost:443 UDB_CA_CERT=certs/ca.crt python client/main.py
```

The first line of output then reads `connect: localhost:443 ...`.

## Check the proxy by hand

```bash
# Health through nginx, with the probe the broker image ships:
docker run --rm --platform linux/amd64 --network udb-docker-nginx_default \
  -v "$PWD/certs/ca.crt:/ca.crt:ro" --entrypoint grpc_health_probe \
  ghcr.io/fahara02/udb:0.6.15 -addr=nginx:443 -tls -tls-ca-cert=/ca.crt
# status: SERVING

docker compose logs nginx      # one line per call: path, HTTP status, grpc_status, upstream
docker compose exec udb udb doctor --enterprise --human
```

## Files

| Path | What it is |
|---|---|
| `proto/shop/v1/order.proto` | the project schema the broker serves |
| `docker-compose.yml` | postgres + udb + nginx, and the `client` profile |
| `nginx/nginx.conf` | TLS, path routing, gRPC timeouts, body size, gRPC-shaped errors |
| `scripts/certs.sh` | self-signed CA + server certificate |
| `scripts/setup.sh` | certs + `deploy/scripts/init.sh` + `deploy/scripts/bootstrap.sh` |
| `client/` | Python client and its Dockerfile |
| `.env.example` | broker settings; `setup.sh` copies it to `.env` |

Generated and git-ignored: `.env`, `admin.env`, `secrets/`, `policies/`, `certs/`.

## nginx settings worth knowing

- `http2 on` on the TLS listener and `grpc_pass grpc://` to the broker. gRPC
  needs HTTP/2 on both legs.
- `client_max_body_size 32m`. The broker caps one message at 10 MiB
  (`UDB_GRPC_MAX_MESSAGE_BYTES`). nginx counts a client stream as one body, so
  raise this if you stream large uploads through the broker instead of
  presigned URLs.
- `grpc_read_timeout 300s` / `grpc_send_timeout 300s`. A server stream (live
  queries, event subscriptions, `BeginTx`) that stays silent longer is closed
  by nginx. Raise both for long-idle subscriptions.
- `grpc_set_header X-Real-IP / X-Forwarded-For $remote_addr`. The real client
  address replaces anything the client sent. The broker uses it for rate
  limiting only because `.env` sets `UDB_TRUST_PROXY_IP_HEADERS=true`. Set that
  only when nothing but the proxy can reach the broker.
- Upstream errors are answered in gRPC terms: `UNAVAILABLE` (14) for
  502/503 and `DEADLINE_EXCEEDED` (4) for 504. SDK retries then behave as they
  do against the broker directly.
- `resolver 127.0.0.11` with variables in `grpc_pass`. nginx re-resolves `udb`,
  so a recreated broker container is picked up without restarting nginx.

## Moving to production

- **Real certificates.** Replace `certs/server.crt` / `server.key` with a
  certificate from a public or corporate CA, using the full chain. Clients
  then trust the system roots: drop `root_certificates`. Renew on a schedule,
  then run `docker compose exec nginx nginx -s reload`.
- **Publish only 443.** Set `UDB_EDGE_BIND=0.0.0.0` (or a specific interface)
  and keep the broker and Postgres without published ports, as they are here.
- **Fail-closed broker.** Add `UDB_FAIL_CLOSED=true`, `UDB_TRUSTED_TRANSPORT=true`,
  `UDB_AUDIT_SINK=postgres`, `UDB_AUDIT_PG_TABLE=udb_system.data_audit_log` and
  `UDB_AUDIT_EXPORT_POSTGRES=1` to `.env`. See
  [`deploy/README.md`](../../deploy/README.md#switching-to-production) and
  `docs/enterprise-deployment.md` §2.1.
- **mTLS between nginx and the broker.** Set `UDB_ENV=production` plus
  `UDB_TLS_CERT_PATH`, `UDB_TLS_KEY_PATH` and `UDB_MTLS_CLIENT_CA_PATH` on the
  broker, so every listener requires a client certificate. Then switch nginx to
  `grpc_pass grpcs://...` with `grpc_ssl_certificate` /
  `grpc_ssl_certificate_key` (nginx's client certificate) and
  `grpc_ssl_trusted_certificate` + `grpc_ssl_verify on` (the broker's CA).
  Production mode also requires Postgres TLS unless you set
  `UDB_PG_TLS_REQUIRED=false` for a private network.
- **mTLS from clients to nginx.** Add `ssl_client_certificate` +
  `ssl_verify_client on` to the `server` block. Your client must then present
  a certificate. Before 0.6.14 the Python SDK's `create_udb` accepted only
  `root_certificates`, with no client key or certificate, so use an SDK or
  channel that supports client certificates.
- **Least privilege.** Narrow `policies/<tenant-uuid>.json` (subjects,
  resources, actions) and re-run `../../deploy/scripts/bootstrap.sh .`. Give
  applications service accounts or API keys rather than the admin login
  (`docs/service-accounts.md`).
- **Migrations.** Set `UDB_MIGRATE_ENABLED=false`, and roll schema changes out
  with `udb plan`, so a restart never applies an unreviewed change.
- **Backups.** Run `docker compose exec -T postgres pg_dump -U udb -Fc udb >
  shop-$(date +%F).dump` on a schedule. Keep `.env` and `secrets/` with the
  backups, but stored separately from them: `UDB_ENCRYPTION_KEY` cannot be
  recovered, and without it the encrypted columns are lost. Rehearse a restore.

## Clean up

```bash
docker compose --profile client down -v    # stops everything and DELETES the database volume
```
