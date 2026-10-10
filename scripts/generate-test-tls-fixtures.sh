#!/usr/bin/env bash
set -euo pipefail

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate ephemeral TLS test fixtures" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/tests/fixtures/tls}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

rm -rf "$OUT"
mkdir -p "$OUT"

make_ca() {
  local name="$1"
  local cert="$2"
  local key="$3"
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
    -subj "/CN=$name" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$key" -out "$cert" >/dev/null 2>&1
}

sign_cert() {
  local common_name="$1"
  local cert="$2"
  local key="$3"
  local ca_cert="$4"
  local ca_key="$5"
  local usage="$6"
  local san="${7:-}"
  local stem
  stem="$(basename "$cert" .pem)"

  openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=$common_name" \
    -keyout "$key" -out "$TMP/$stem.csr" >/dev/null 2>&1

  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=$usage"
    if [[ -n "$san" ]]; then
      echo "subjectAltName=$san"
    fi
  } > "$TMP/$stem.ext"

  openssl x509 -req -in "$TMP/$stem.csr" \
    -CA "$ca_cert" -CAkey "$ca_key" -CAcreateserial \
    -days 2 -sha256 -extfile "$TMP/$stem.ext" \
    -out "$cert" >/dev/null 2>&1
}

make_ca "JustyBase Db2 Rust Test CA" "$OUT/ca.pem" "$TMP/ca-key.pem"
sign_cert "localhost" "$OUT/server.pem" "$OUT/server-key.pem" \
  "$OUT/ca.pem" "$TMP/ca-key.pem" "serverAuth" "DNS:localhost,IP:127.0.0.1"
sign_cert "justybase-test-client" "$OUT/client.pem" "$OUT/client-key.pem" \
  "$OUT/ca.pem" "$TMP/ca-key.pem" "clientAuth"

make_ca "JustyBase Db2 Rust Other Test CA" "$OUT/other-ca.pem" "$TMP/other-ca-key.pem"
sign_cert "justybase-untrusted-client" "$OUT/other-client.pem" "$OUT/other-client-key.pem" \
  "$OUT/other-ca.pem" "$TMP/other-ca-key.pem" "clientAuth"

chmod 600 "$OUT"/*-key.pem
echo "Generated ephemeral TLS fixtures in $OUT"
