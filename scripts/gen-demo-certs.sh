#!/usr/bin/env bash
# Generates a throw-away CA plus a server and a client certificate for trying
# TLS / mutual TLS between the bridge and an upstream service. DEMO ONLY: the
# keys are unencrypted and valid for a year; never use them in production.
#
# Usage: scripts/gen-demo-certs.sh [output-dir]      (default: certs/demo)
# The server certificate is valid for: localhost, 127.0.0.1, example-grpc-model, app-svc
set -euo pipefail

OUT="${1:-certs/demo}"
DAYS=365
mkdir -p "$OUT"
cd "$OUT"

# CA
openssl genrsa -out ca.key 2048 2>/dev/null
openssl req -x509 -new -key ca.key -sha256 -days "$DAYS" -subj "/CN=demo-ca" -out ca.pem

# Server (with SANs so hostname verification passes)
openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -subj "/CN=example-grpc-model" -out server.csr
cat > server.ext <<EXT
subjectAltName = DNS:localhost, DNS:example-grpc-model, DNS:app-svc, IP:127.0.0.1
extendedKeyUsage = serverAuth
EXT
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -sha256 -days "$DAYS" -extfile server.ext -out server.pem 2>/dev/null

# Client (for mTLS)
openssl genrsa -out client.key 2048 2>/dev/null
openssl req -new -key client.key -subj "/CN=bridge-kafka-service" -out client.csr
echo "extendedKeyUsage = clientAuth" > client.ext
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -sha256 -days "$DAYS" -extfile client.ext -out client.pem 2>/dev/null

rm -f ./*.csr ./*.ext ./*.srl
chmod 600 ./*.key

echo "Demo certificates written to $OUT:"
ls -1 ./*.pem ./*.key
