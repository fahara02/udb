#!/usr/bin/env bash
# Prepare a UDB compose deployment directory: .env with generated secrets and
# the broker's RS256 key pair. Safe to re-run: a value already in .env is never
# changed, and an existing key pair is kept.
#
#   ./scripts/init.sh [deployment-dir]
#
# deployment-dir defaults to the directory above this script (deploy/). It must
# hold .env.example and docker-compose.yml; examples/docker_nginx calls this
# script with its own directory.
#
# What it does:
#   1. .env.example -> .env (only when .env does not exist yet)
#   2. replaces the Postgres password placeholder with a random value
#   3. adds UDB_PASSWORD_HASH_SECRET when missing
#   4. runs `udb env fill --profile dev` INSIDE the pinned broker image, which
#      appends UDB_SESSION_HASH_SECRET, UDB_API_KEY_HASH_SECRET,
#      UDB_ENCRYPTION_KEY, UDB_APPROVAL_SIGNING_KEY and the UDB_JWT_* paths, and
#      writes secrets/jwt-private.pem + jwt-public.pem
#   5. hands secrets/ to the image's `udb` user (the broker runs as it) and .env
#      back to you
#
# Needs: docker. Run as a user that can run docker.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR="$(cd "${1:-$SCRIPT_DIR/..}" && pwd)"
cd "$DIR"

die() { echo "init: $*" >&2; exit 1; }
rand_hex() { od -An -N32 -tx1 /dev/urandom | tr -d ' \n'; }
env_get() { grep -E "^$1=" .env | tail -n 1 | cut -d= -f2- || true; }

command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH"
[[ -f .env.example ]] || die "$DIR/.env.example not found"

# 1. .env
if [[ ! -f .env ]]; then
  cp .env.example .env
  chmod 600 .env
  echo "init: created .env from .env.example"
fi
if grep -q $'\r' .env; then
  die ".env has Windows (CRLF) line endings; convert it to LF (e.g. dos2unix .env) and re-run"
fi

# 2. Postgres password (same value in POSTGRES_PASSWORD and UDB_PG_DSN).
if grep -q 'change-me-pg-password' .env; then
  pg_password="$(rand_hex)"
  sed -i.tmp "s/change-me-pg-password/${pg_password}/g" .env
  rm -f .env.tmp
  echo "init: generated the Postgres password"
fi

# 3. Dedicated password-hash secret. `udb env fill` does not generate it; the
#    broker would fall back to UDB_SESSION_HASH_SECRET, and rotating that would
#    then invalidate every stored password.
if [[ -z "$(env_get UDB_PASSWORD_HASH_SECRET)" ]]; then
  {
    echo
    echo "# Hashes native user passwords. Keep it stable: changing it invalidates every password."
    echo "UDB_PASSWORD_HASH_SECRET=$(rand_hex)"
  } >> .env
  echo "init: generated UDB_PASSWORD_HASH_SECRET"
fi

# 4 + 5. `udb env fill` inside the pinned image. It runs as root only so it can
#    hand the key files to the image's udb user afterwards.
image_name="$(env_get UDB_IMAGE)"
image_version="$(env_get UDB_VERSION)"
[[ -n "$image_name" && -n "$image_version" ]] || die "set UDB_IMAGE and UDB_VERSION in .env"
image="${image_name}:${image_version}"
mkdir -p secrets

echo "init: running 'udb env fill' in ${image}"
docker run --rm --platform linux/amd64 \
  --user 0:0 \
  --entrypoint /bin/sh \
  -e HOST_UID="$(id -u)" \
  -e HOST_GID="$(id -g)" \
  -v "$DIR:/deploy" \
  -v "$DIR/secrets:/run/udb-secrets" \
  "$image" -c '
    set -e
    udb env fill --file /deploy/.env --keys-dir /run/udb-secrets --profile dev --yes
    chown -R udb:udb /run/udb-secrets
    chmod 0700 /run/udb-secrets
    chmod 0400 /run/udb-secrets/jwt-private.pem
    chmod 0444 /run/udb-secrets/jwt-public.pem
    chown "$HOST_UID:$HOST_GID" /deploy/.env
    chmod 0600 /deploy/.env
    for backup in /deploy/.env.bak-*; do
      if [ -e "$backup" ]; then chown "$HOST_UID:$HOST_GID" "$backup"; fi
    done
  '

for key in UDB_PG_DSN UDB_SESSION_HASH_SECRET UDB_API_KEY_HASH_SECRET UDB_PASSWORD_HASH_SECRET \
           UDB_ENCRYPTION_KEY UDB_JWT_PRIVATE_KEY UDB_JWT_PUBLIC_KEY; do
  [[ -n "$(env_get "$key")" ]] || die "$key is still missing from .env after 'udb env fill'"
done

echo
echo "init: .env is complete and secrets/ holds the RS256 key pair."
echo "init: back up .env and secrets/ together: UDB_ENCRYPTION_KEY cannot be recovered."
echo "next: ./scripts/bootstrap.sh   (first admin user + data policies)"
