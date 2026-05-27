#!/usr/bin/env bash
# ── server-init.sh ────────────────────────────────────────────────────────────
# 新中继服务器一次性初始化脚本（方案 A：Docker + Watchtower）
#
# 使用方式：
#   bash scripts/server-init.sh <GITHUB_PAT> [IMAGE]
#
# 参数：
#   GITHUB_PAT  GitHub Personal Access Token（需要 read:packages 权限）
#   IMAGE       （可选）完整镜像地址，默认 ghcr.io/org/mini_vpn:latest
#
# 前置条件：
#   1. 已将 cert.pem 和 key.pem 放入 /etc/mini_vpn/
#   2. 以 root 或 sudo 权限运行
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── 参数解析 ──────────────────────────────────────────────
GITHUB_PAT=${1:?"Usage: $0 <GITHUB_PAT> [IMAGE]"}
IMAGE=${2:-"ghcr.io/org/mini_vpn:latest"}

GHCR_USER=${GHCR_USER:-"deploy"}                         # GHCR 登录用户名
CERT_DIR="/etc/mini_vpn"
BIND_ADDR=${MINI_VPN_SERVER_BIND_ADDR:-"0.0.0.0:443"}
WATCHTOWER_INTERVAL=${WATCHTOWER_INTERVAL:-300}          # 检查间隔（秒）

echo "==> 初始化中继服务器"
echo "    镜像: $IMAGE"
echo "    监听: $BIND_ADDR"

# ── 1. 安装 Docker ────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo "==> 安装 Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable --now docker
    echo "    Docker 安装完成: $(docker --version)"
else
    echo "==> Docker 已安装: $(docker --version)"
fi

# ── 2. 检查证书文件 ───────────────────────────────────────
if [[ ! -f "$CERT_DIR/cert.pem" || ! -f "$CERT_DIR/key.pem" ]]; then
    echo ""
    echo "⚠️  未在 $CERT_DIR 中找到证书文件。"
    echo "    请执行以下步骤后重新运行此脚本："
    echo "      mkdir -p $CERT_DIR"
    echo "      scp cert.pem key.pem root@$(hostname):$CERT_DIR/"
    exit 1
fi
echo "==> 证书文件检查通过"

# ── 3. 登录 GHCR ──────────────────────────────────────────
echo "==> 登录 GHCR..."
echo "$GITHUB_PAT" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
echo "    登录成功"

# ── 4. 拉取镜像 ───────────────────────────────────────────
echo "==> 拉取镜像: $IMAGE"
docker pull "$IMAGE"

# ── 5. 停止并移除旧容器（如存在）────────────────────────
if docker ps -a --format '{{.Names}}' | grep -q '^mini_vpn$'; then
    echo "==> 停止并移除旧 mini_vpn 容器..."
    docker stop mini_vpn || true
    docker rm mini_vpn || true
fi

# ── 6. 启动 mini_vpn 容器 ─────────────────────────────────
echo "==> 启动 mini_vpn 容器..."
docker run -d \
    --name mini_vpn \
    --restart unless-stopped \
    -p 443:443 \
    -v "$CERT_DIR/cert.pem":/cert.pem:ro \
    -v "$CERT_DIR/key.pem":/key.pem:ro \
    -e MINI_VPN_SERVER_BIND_ADDR="$BIND_ADDR" \
    -e MINI_VPN_SERVER_CERT_PATH=/cert.pem \
    -e MINI_VPN_SERVER_KEY_PATH=/key.pem \
    "$IMAGE"

# ── 7. 验证容器启动成功 ───────────────────────────────────
sleep 2
if docker ps --filter "name=mini_vpn" --filter "status=running" | grep -q mini_vpn; then
    echo "    mini_vpn 容器运行正常 ✅"
else
    echo "    mini_vpn 容器启动失败 ❌，日志如下："
    docker logs mini_vpn
    exit 1
fi

# ── 8. 安装/更新 Watchtower ───────────────────────────────
if docker ps -a --format '{{.Names}}' | grep -q '^watchtower$'; then
    echo "==> 更新 Watchtower..."
    docker stop watchtower || true
    docker rm watchtower || true
fi

echo "==> 启动 Watchtower（每 ${WATCHTOWER_INTERVAL}s 检查镜像更新）..."
docker run -d \
    --name watchtower \
    --restart unless-stopped \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /root/.docker/config.json:/config.json:ro \
    containrrr/watchtower \
    --interval "$WATCHTOWER_INTERVAL" \
    --cleanup \
    --label-enable \
    mini_vpn

echo ""
echo "✅ 初始化完成！"
echo ""
echo "   当前运行容器："
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "   后续无需任何操作：GitHub Actions 推送新镜像后，"
echo "   Watchtower 将在 ${WATCHTOWER_INTERVAL}s 内自动更新此服务器。"
