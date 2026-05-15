#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/certs/dev"

mkdir -p "${OUT_DIR}"

openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout "${OUT_DIR}/server-key.pem" \
  -out "${OUT_DIR}/server-cert.pem" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:example.com,IP:127.0.0.1"

cp "${OUT_DIR}/server-cert.pem" "${OUT_DIR}/ca-cert.pem"

echo "Generated:"
echo "  ${OUT_DIR}/server-cert.pem"
echo "  ${OUT_DIR}/server-key.pem"
echo "  ${OUT_DIR}/ca-cert.pem"
