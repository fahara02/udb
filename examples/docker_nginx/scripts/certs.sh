#!/usr/bin/env bash
# Self-signed TLS for the example: a throwaway CA and a server certificate
# nginx presents, valid for localhost, 127.0.0.1 and the compose name `nginx`.
#
#   ./scripts/certs.sh [extra-dns-name ...]
#
# Writes certs/ca.crt (give it to clients), certs/ca.key, certs/server.crt and
# certs/server.key. Existing files are kept; delete certs/ to re-issue.
# For anything beyond a local trial, use a certificate from a real CA instead
# (README "Moving to production").
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CERTS="$ROOT/certs"

command -v openssl >/dev/null 2>&1 || { echo "certs: openssl is required" >&2; exit 1; }

mkdir -p "$CERTS"
cd "$CERTS"

if [[ -f server.crt && -f server.key && -f ca.crt ]]; then
  echo "certs: $CERTS already has a certificate; delete it to re-issue"
  exit 0
fi

san="DNS:localhost,DNS:nginx,IP:127.0.0.1"
for name in "$@"; do
  san="${san},DNS:${name}"
done

umask 077
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 825 \
  -subj "/CN=UDB example local CA" \
  -keyout ca.key -out ca.crt \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
  -subj "/CN=localhost" \
  -keyout server.key -out server.csr 2>/dev/null

cat > server.ext <<EXT
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=${san}
EXT

openssl x509 -req -sha256 -days 825 \
  -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile server.ext -out server.crt 2>/dev/null

rm -f server.csr server.ext ca.srl
# The CA certificate is public; clients and the client container read it.
chmod 644 ca.crt server.crt
# nginx's master process reads the key as root inside the container.
chmod 600 ca.key server.key

echo "certs: issued server certificate for ${san}"
echo "certs: clients trust $CERTS/ca.crt"
