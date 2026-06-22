#!/usr/bin/env bash
# 刀3.5 真出口 acceptance helper (macOS). NOT a product file — dev/test only.
# 读 env 的 MINI_VPN_TUIC_*（含 UUID/PASSWORD）——脚本本身不含任何凭据。
#
# 用法（凭据先 export，见下）：
#   构建：      cargo build --release            # (在 repo 根，无需 sudo)
#   iperf3 配置：sudo -E bash scripts/knife35-acceptance.sh start <cc> <mode>   # cc=bbr|cubic mode=native|quic
#   全局 soak： sudo -E bash scripts/knife35-acceptance.sh soak [cc]            # 默认 cubic；DNS=198.18.0.1+/15 路由
#   刀5 soak： sudo -E bash scripts/knife35-acceptance.sh soak-knife5 [cc]      # DNS=8.8.8.8(非我方 resolver)+路由进 TUN
#   停 iperf3： sudo -E bash scripts/knife35-acceptance.sh stop
#   停 soak：   sudo -E bash scripts/knife35-acceptance.sh soak-stop            # 自动还原 DNS（soak / soak-knife5 通用）
#
# 凭据 export（向项目负责人要，勿入库）：
#   export MINI_VPN_TUIC_SERVER=47.251.188.205:8443
#   export MINI_VPN_TUIC_UUID=<uuid>
#   export MINI_VPN_TUIC_PASSWORD=<pass>
#   （SNI/ALPN/CA 有默认：example.com / h3 / certs/dev/ca-cert.pem）
#
# 可覆盖 env：IPERF_TARGET（默认 43.110.37.170）、NETSVC（默认自动探测默认路由所在网络服务）。
set -u

# --- repo 根：脚本在 scripts/ 下，取其父目录 ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO/target/release/mini_vpn"
LOG=/tmp/mvpn_accept.log
TARGET="${IPERF_TARGET:-43.110.37.170}"
FAKE_DNS=198.18.0.1
FAKE_NET=198.18.0.0/15
# 刀5 acceptance：把系统 DNS 设成"非我方 resolver"，验证任意 :53 仍被本地劫持伪造。可 K5_RES 覆盖。
K5_RES="${K5_RES:-8.8.8.8}"
DNS_SAVE=/tmp/mvpn_prior_dns.txt
ACTION="${1:-}"

# 自动探测「默认路由所在的网络服务名」(Wi-Fi / Ethernet / ...). 可用 NETSVC 覆盖。
detect_netsvc() {
  if [ -n "${NETSVC:-}" ]; then echo "$NETSVC"; return; fi
  local dev; dev="$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')"
  [ -z "$dev" ] && { echo "Wi-Fi"; return; }
  networksetup -listnetworkserviceorder 2>/dev/null | awk -v d="$dev" '
    /^\([0-9*]+\)/ { name=$0; sub(/^\([0-9*]+\) /,"",name) }
    $0 ~ ("Device: " d ")") { print name; exit }'
}

stop() {
  pkill -f "mini_vpn client-tun" 2>/dev/null
  route -n delete -host "$TARGET" >/dev/null 2>&1
  sleep 1
  echo "stopped + route to $TARGET removed"
}

soak_stop() {
  local svc; svc="$(detect_netsvc)"
  local prior; prior="$(cat "$DNS_SAVE" 2>/dev/null)"
  # 保存值可能是 "There aren't any DNS Servers set on X." → 还原为 empty(回 DHCP)
  if [ -z "$prior" ] || printf '%s' "$prior" | grep -qi "aren't any"; then prior="empty"; fi
  networksetup -setdnsservers "$svc" $prior && echo "DNS($svc) restored: $prior"
  pkill -f "mini_vpn client-tun" 2>/dev/null
  route -n delete -net "$FAKE_NET" >/dev/null 2>&1
  route -n delete -host "$TARGET" >/dev/null 2>&1
  route -n delete -host "$K5_RES" >/dev/null 2>&1   # 刀5：清掉 alt-resolver host 路由（soak-knife5 用）
  sleep 1
  echo "soak stopped + /15 route removed + DNS restored"
}

case "$ACTION" in
  stop)      stop; exit 0 ;;
  soak-stop) soak_stop; exit 0 ;;
  start|soak|soak-knife5) ;;
  *) echo "usage: $0 {start <cc> <mode> | soak [cc] | soak-knife5 [cc] | stop | soak-stop}"; exit 2 ;;
esac

if [ "$ACTION" = "soak" ] || [ "$ACTION" = "soak-knife5" ]; then
  CC="${2:-cubic}"; MODE="native"
else
  CC="${2:-bbr}"; MODE="${3:-native}"
fi

[ -x "$BIN" ] || { echo "!! 未找到 release binary: $BIN（先 'cargo build --release'）"; exit 1; }

# 1. 停旧实例 + 清旧路由
pkill -f "mini_vpn client-tun" 2>/dev/null
route -n delete -host "$TARGET" >/dev/null 2>&1
sleep 1

# 2. 记录起前 utun 集合（差集找新建的）
BEFORE="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"

# 3. 起 client-tun（后台），CC/mode 经 env 覆盖；CA 绝对路径（不依赖 cwd）
: > "$LOG"
cd "$REPO" || { echo "cd $REPO failed"; exit 1; }
MINI_VPN_TUIC_SERVER="${MINI_VPN_TUIC_SERVER:-47.251.188.205:8443}" \
MINI_VPN_TUIC_SNI="${MINI_VPN_TUIC_SNI:-example.com}" \
MINI_VPN_TUIC_CA_PATH="${MINI_VPN_TUIC_CA_PATH:-$REPO/certs/dev/ca-cert.pem}" \
MINI_VPN_TUIC_ALPN="${MINI_VPN_TUIC_ALPN:-h3}" \
MINI_VPN_TUIC_CC="$CC" MINI_VPN_TUIC_UDP_MODE="$MODE" \
nohup "$BIN" client-tun >>"$LOG" 2>&1 &
echo "client-tun started: cc=$CC mode=$MODE pid=$!"

# 4. 等连上（📏 行，最多 30s）
for _ in $(seq 1 30); do grep -q "TUIC datagram" "$LOG" && break; sleep 1; done
if ! grep -q "TUIC datagram" "$LOG"; then
  echo "!! not connected in 30s; last 15 log lines:"; tail -15 "$LOG"; exit 1
fi

# 5. 差集找新建 utun
AFTER="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"
UTUN="$(comm -13 <(echo "$BEFORE") <(echo "$AFTER") | head -1)"
[ -z "$UTUN" ] && { echo "!! no new utun detected (BEFORE=[$BEFORE] AFTER=[$AFTER])"; exit 1; }

# 6. 路由
if [ "$ACTION" = "soak" ] || [ "$ACTION" = "soak-knife5" ]; then
  SVC="$(detect_netsvc)"
  route -n add -net "$FAKE_NET" -interface "$UTUN" >/dev/null 2>&1 \
    && echo "route ${FAKE_NET} -> ${UTUN} OK" || echo "!! /15 route add failed"
  networksetup -getdnsservers "$SVC" 2>/dev/null > "$DNS_SAVE"
  if [ "$ACTION" = "soak-knife5" ]; then
    # 刀5：系统 DNS 设成 alt-resolver（非 198.18.0.1），并把该 resolver 路由进 TUN，
    # 验证任意 :53 仍被劫持伪造 fake-IP（不依赖系统 DNS 指向我方 resolver，见 ADR-0007）。
    route -n add -host "$K5_RES" -interface "$UTUN" >/dev/null 2>&1 \
      && echo "route ${K5_RES} -> ${UTUN} OK (alt-resolver into TUN)" \
      || echo "!! alt-resolver route add failed"
    networksetup -setdnsservers "$SVC" "$K5_RES" \
      && echo "DNS(${SVC}) -> ${K5_RES} (NOT 198.18.0.1; saved=${DNS_SAVE}; soak-stop auto-reverts)"
    echo "---- 刀5 验证（另开终端跑；判据见 plan T-DNS）----"
    echo "  dig @${K5_RES} example.com +short       # 期望 198.18.x.x（fake-IP，非真实 IP）"
    echo "  dig +tcp @${K5_RES} example.com +short  # 期望 超时/拒绝（TCP :53 被 RST）"
    echo "  curl -sS -o /dev/null -w '%{http_code}\\n' https://example.com  # 期望 200/301（经隧道）"
    echo "  grep '🪪 DNS' ${LOG}                     # 期望 见 example.com → fake-IP"
  else
    networksetup -setdnsservers "$SVC" "$FAKE_DNS" \
      && echo "DNS(${SVC}) -> ${FAKE_DNS} (saved=${DNS_SAVE}; soak-stop auto-reverts)"
  fi
else
  route -n add -host "$TARGET" -interface "$UTUN" >/dev/null 2>&1 \
    && echo "route ${TARGET} -> ${UTUN} OK" || echo "!! route add failed"
fi

# 7. 就绪
echo "---- startup lines ----"; grep -E "TUIC datagram|UDP relay mode" "$LOG" | tail -3
echo "READY  utun=${UTUN}  cc=${CC} mode=${MODE}  log=${LOG}"
