#!/usr/bin/env bash
# ── rollback.sh ───────────────────────────────────────────────────────────────
# 紧急回滚脚本：将指定服务器（或所有服务器）切换到特定镜像版本。
#
# 使用方式：
#   # 回滚到指定 git SHA（推荐）
#   bash scripts/rollback.sh abc1234
#
#   # 回滚到指定语义化版本
#   bash scripts/rollback.sh v1.2.3
#
#   # 只回滚特定服务器
#   SERVERS="relay-us-1.example.com" bash scripts/rollback.sh abc1234
#
# 环境变量：
#   SERVERS     空格分隔的服务器列表（默认：SERVERS 变量或下方硬编码列表）
#   DEPLOY_USER SSH 用户名（默认：deploy）
#   IMAGE_BASE  镜像基础路径（默认：ghcr.io/org/mini_vpn）
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

TAG=${1:?"Usage: $0 <git-sha|version-tag>  例: $0 abc1234 或 $0 v1.2.3"}

# ── 配置（按实际情况修改）────────────────────────────────
DEPLOY_USER=${DEPLOY_USER:-"deploy"}
IMAGE_BASE=${IMAGE_BASE:-"ghcr.io/org/mini_vpn"}
DEFAULT_SERVERS=(
    "relay-us-1.example.com"
    "relay-us-2.example.com"
    "relay-uk-1.example.com"
)

# 支持通过环境变量覆盖服务器列表
if [[ -n "${SERVERS:-}" ]]; then
    read -ra TARGETS <<< "$SERVERS"
else
    TARGETS=("${DEFAULT_SERVERS[@]}")
fi

IMAGE="$IMAGE_BASE:$TAG"

echo "==> 回滚目标镜像: $IMAGE"
echo "==> 目标服务器 (${#TARGETS[@]} 台):"
printf "    %s\n" "${TARGETS[@]}"
echo ""
read -p "确认回滚？[y/N] " -n 1 -r
echo ""
[[ $REPLY =~ ^[Yy]$ ]] || { echo "已取消"; exit 0; }

# ── 并行回滚所有服务器 ────────────────────────────────────
rollback_server() {
    local host=$1
    echo "[$host] 开始回滚..."

    ssh -o StrictHostKeyChecking=no "$DEPLOY_USER@$host" bash <<EOF
set -e

echo "[$host] 拉取镜像: $IMAGE"
docker pull "$IMAGE"

echo "[$host] 停止当前容器..."
docker stop mini_vpn || true
docker rm mini_vpn || true

echo "[$host] 以旧版本镜像重新启动..."
docker run -d \\
    --name mini_vpn \\
    --restart unless-stopped \\
    -p 443:443 \\
    -v /etc/mini_vpn/cert.pem:/cert.pem:ro \\
    -v /etc/mini_vpn/key.pem:/key.pem:ro \\
    -e MINI_VPN_SERVER_BIND_ADDR="0.0.0.0:443" \\
    -e MINI_VPN_SERVER_CERT_PATH=/cert.pem \\
    -e MINI_VPN_SERVER_KEY_PATH=/key.pem \\
    "$IMAGE"

sleep 2
if docker ps --filter "name=mini_vpn" --filter "status=running" | grep -q mini_vpn; then
    echo "[$host] ✅ 回滚成功，当前版本: $TAG"
else
    echo "[$host] ❌ 回滚后容器未正常启动"
    docker logs mini_vpn --tail 20
    exit 1
fi
EOF
}

export -f rollback_server
export IMAGE DEPLOY_USER

# 并行执行，汇总结果
PIDS=()
RESULTS=()

for server in "${TARGETS[@]}"; do
    rollback_server "$server" &
    PIDS+=($!)
    RESULTS+=("$server")
done

# 等待所有并行任务完成并报告结果
FAILED=0
for i in "${!PIDS[@]}"; do
    if wait "${PIDS[$i]}"; then
        echo "  ✅ ${RESULTS[$i]}: 成功"
    else
        echo "  ❌ ${RESULTS[$i]}: 失败"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
if [[ $FAILED -eq 0 ]]; then
    echo "✅ 全部 ${#TARGETS[@]} 台服务器回滚至 $TAG 完成"
else
    echo "⚠️  ${FAILED} 台服务器回滚失败，请手动检查"
    exit 1
fi
