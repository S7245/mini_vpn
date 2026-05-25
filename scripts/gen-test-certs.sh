#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/certs/dev"

mkdir -p "${OUT_DIR}"

# 1. 生成 CA 私钥和自签证书
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout "${OUT_DIR}/ca-key.pem" \
  -out "${OUT_DIR}/ca-cert.pem" \
  -subj "/CN=Dev VPN CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

# 2. 生成服务端私钥和 CSR（证书签名请求）
openssl req -new -newkey rsa:2048 -nodes \
  -keyout "${OUT_DIR}/server-key.pem" \
  -out "${OUT_DIR}/server.csr" \
  -subj "/CN=localhost"

# 3. 准备扩展配置，确保是合法 leaf cert
cat > "${OUT_DIR}/server-ext.cnf" <<EOF
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,DNS:example.com,IP:127.0.0.1
EOF

# 4. 用 CA 签发服务端证书
openssl x509 -req -sha256 -days 365 \
  -in "${OUT_DIR}/server.csr" \
  -CA "${OUT_DIR}/ca-cert.pem" \
  -CAkey "${OUT_DIR}/ca-key.pem" \
  -CAcreateserial \
  -extfile "${OUT_DIR}/server-ext.cnf" \
  -out "${OUT_DIR}/server-cert.pem"

# 5. 清理中间文件
rm -f "${OUT_DIR}/server.csr" "${OUT_DIR}/server-ext.cnf"

echo "Generated:"
echo "  ${OUT_DIR}/server-cert.pem"
echo "  ${OUT_DIR}/server-key.pem"
echo "  ${OUT_DIR}/ca-cert.pem"
