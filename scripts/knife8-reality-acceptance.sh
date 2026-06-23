#!/usr/bin/env bash
# 刀8 真出口 acceptance helper (macOS). NOT a product file — dev/test only.
# 读 env 的 MINI_VPN_REALITY_*（含 UUID/PBK）——脚本本身不含任何凭据。
#
# VLESS over REALITY over TCP 第二传输（抗封锁 fallback）。REALITY 是 **TCP-only**：UDP no-op（force-reality）。
#
# 用法（凭据先 export，见下）：
#   构建：     cargo build --release
#   预检：     bash scripts/knife8-reality-acceptance.sh preflight   # openssl 探借用站是否协商 0x1301（无需 sudo）
#   全局 soak：sudo -E bash scripts/knife8-reality-acceptance.sh soak  # 建 TUN + 路由 fake-net + DNS 进 TUN
#   烟测：     bash scripts/knife8-reality-acceptance.sh smoke        # curl HTTPS 经 REALITY 隧道（soak 起好后）
#   停 soak：  sudo -E bash scripts/knife8-reality-acceptance.sh soak-stop   # 还原 DNS + 删路由
#
# 凭据 export（向项目负责人要，勿入库）：
#   export MINI_VPN_REALITY_SERVER=<VPS_IP>:443      # REALITY 服务端端点（实连地址）
#   export MINI_VPN_REALITY_UUID=<uuid>
#   export MINI_VPN_REALITY_PBK=<public_key>          # sing-box generate reality-keypair 的 public_key（base64url 43 字符）
#   export MINI_VPN_REALITY_SHORT_ID=<short_id_hex>   # 与服务端 short_id 一致（可空）
#   export MINI_VPN_REALITY_SNI=<borrowed_site>       # 借用站域名（须 ∈ 服务端 serverNames，推荐 == handshake.server）
#
# 可覆盖 env：DECOY（预检探的借用站，默认 = $MINI_VPN_REALITY_SNI）、K8_RES（DNS resolver，默认 8.8.8.8）、
#            SMOKE_URL（烟测 URL，默认 https://www.cloudflare.com/cdn-cgi/trace）、NETSVC（默认自动探测）。
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO/target/release/mini_vpn"
LOG=/tmp/mvpn_reality_accept.log
FAKE_NET=198.18.0.0/15
K8_RES="${K8_RES:-8.8.8.8}"
DNS_SAVE=/tmp/mvpn_reality_prior_dns.txt
SMOKE_URL="${SMOKE_URL:-https://www.cloudflare.com/cdn-cgi/trace}"
ACTION="${1:-}"

detect_netsvc() {
  if [ -n "${NETSVC:-}" ]; then echo "$NETSVC"; return; fi
  local dev; dev="$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')"
  [ -z "$dev" ] && { echo "Wi-Fi"; return; }
  networksetup -listnetworkserviceorder 2>/dev/null | awk -v d="$dev" '
    /^\([0-9*]+\)/ { name=$0; sub(/^\([0-9*]+\) /,"",name) }
    $0 ~ ("Device: " d ")") { print name; exit }'
}

# openssl 出口预检：探借用站是否能在「仅 offer 0x1301」下协商出 TLS_AES_128_GCM_SHA256。
# 我方 ClientHello 已收紧为仅 0x1301（ADR-0009 修订）；借用站若不接受 → REALITY 握手必败 → 此处 loud。
# ⚠️ 此探针从**本机**出口跑；真实路径是 VPS→借用站，cipher 偏好可能因地域不同（brief §6）——最终以三端 acceptance 为准。
preflight() {
  local decoy="${DECOY:-${MINI_VPN_REALITY_SNI:-}}"
  [ -z "$decoy" ] && { echo "!! 需 MINI_VPN_REALITY_SNI 或 DECOY 指定借用站"; exit 2; }
  echo "==> openssl 预检借用站 ${decoy}:443（仅 offer TLS_AES_128_GCM_SHA256）..."
  # 合并 stdout+stderr：cipher 摘要行（"New, TLSv1.3, Cipher is ..."）在 stderr。
  local out
  out="$(echo | openssl s_client -connect "${decoy}:443" -servername "$decoy" \
        -tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256 2>&1)"
  # 兼容两种格式："Cipher is TLS_AES_128_GCM_SHA256" 与 "Cipher    : TLS_AES_128_GCM_SHA256"。
  local cipher; cipher="$(printf '%s' "$out" | grep -oE 'TLS_(AES|CHACHA)[A-Z0-9_]+' | head -1)"
  if [ "$cipher" = "TLS_AES_128_GCM_SHA256" ]; then
    echo "✅ 借用站 ${decoy} 协商出 ${cipher} → 0x1301 OK，可用作 REALITY handshake server"
    exit 0
  else
    echo "❌ 借用站 ${decoy} 未协商 0x1301（实得 cipher='${cipher:-<握手失败>}'）"
    echo "   → 换一个会协商 0x1301 的借用站（如 gateway.icloud.com / dl.google.com / www.cloudflare.com）"
    echo "   （0x1302/0x1303 是 ADR-0009 gap，本刀不支持，会 loud-fail）"
    exit 1
  fi
}

smoke() {
  echo "==> 烟测：curl ${SMOKE_URL}（应经 REALITY 隧道）"
  local code; code="$(curl -sS -o /tmp/mvpn_reality_smoke.out -w '%{http_code}' --max-time 20 "$SMOKE_URL")"
  echo "HTTP ${code}; body 前几行："; head -5 /tmp/mvpn_reality_smoke.out 2>/dev/null
  echo "---- client 日志（REALITY 握手 / VLESS）----"
  grep -E "REALITY 握手成功|REALITY 出口|REALITY|🔐" "$LOG" | tail -8
  [ "$code" = "200" ] || [ "$code" = "301" ] || [ "$code" = "302" ] \
    && echo "✅ 经 REALITY 隧道可达（HTTP ${code}）" \
    || echo "⚠️ HTTP ${code}（检查三端日志：client 🔐 / sing-box vless inbound / 目标站）"
}

soak_stop() {
  local svc; svc="$(detect_netsvc)"
  local prior; prior="$(cat "$DNS_SAVE" 2>/dev/null)"
  if [ -z "$prior" ] || printf '%s' "$prior" | grep -qi "aren't any"; then prior="empty"; fi
  networksetup -setdnsservers "$svc" $prior && echo "DNS($svc) restored: $prior"
  pkill -f "mini_vpn client-tun" 2>/dev/null
  route -n delete -net "$FAKE_NET" >/dev/null 2>&1
  route -n delete -host "$K8_RES" >/dev/null 2>&1
  sleep 1
  echo "soak stopped + /15 route removed + DNS restored"
}

case "$ACTION" in
  preflight) preflight ;;
  smoke)     smoke; exit 0 ;;
  soak-stop) soak_stop; exit 0 ;;
  soak)      ;;
  *) echo "usage: $0 {preflight | soak | smoke | soak-stop}"; exit 2 ;;
esac

# ---- soak ----
[ -x "$BIN" ] || { echo "!! 未找到 release binary: $BIN（先 'cargo build --release'）"; exit 1; }
: "${MINI_VPN_REALITY_SERVER:?需 export MINI_VPN_REALITY_SERVER}"
: "${MINI_VPN_REALITY_UUID:?需 export MINI_VPN_REALITY_UUID}"
: "${MINI_VPN_REALITY_PBK:?需 export MINI_VPN_REALITY_PBK}"
: "${MINI_VPN_REALITY_SNI:?需 export MINI_VPN_REALITY_SNI（借用站）}"

pkill -f "mini_vpn client-tun" 2>/dev/null
sleep 1
BEFORE="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"

: > "$LOG"
cd "$REPO" || { echo "cd $REPO failed"; exit 1; }
MINI_VPN_UPSTREAM=reality nohup "$BIN" client-tun >>"$LOG" 2>&1 &
echo "client-tun started (REALITY): pid=$!"

# 等配置就绪（REALITY 出口配置行，最多 30s）。REALITY 无启动期常连——首个 TCP 才握手。
for _ in $(seq 1 30); do grep -q "REALITY 出口" "$LOG" && break; sleep 1; done
if ! grep -q "REALITY 出口" "$LOG"; then
  echo "!! REALITY 未就绪（30s）；最后 15 行日志："; tail -15 "$LOG"; exit 1
fi

AFTER="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"
UTUN="$(comm -13 <(echo "$BEFORE") <(echo "$AFTER") | head -1)"
[ -z "$UTUN" ] && { echo "!! no new utun detected"; exit 1; }

SVC="$(detect_netsvc)"
route -n add -net "$FAKE_NET" -interface "$UTUN" >/dev/null 2>&1 \
  && echo "route ${FAKE_NET} -> ${UTUN} OK" || echo "!! /15 route add failed"
route -n add -host "$K8_RES" -interface "$UTUN" >/dev/null 2>&1 \
  && echo "route ${K8_RES} -> ${UTUN} OK (resolver into TUN)" || echo "!! resolver route add failed"
networksetup -getdnsservers "$SVC" 2>/dev/null > "$DNS_SAVE"
networksetup -setdnsservers "$SVC" "$K8_RES" \
  && echo "DNS(${SVC}) -> ${K8_RES} (saved=${DNS_SAVE}; soak-stop auto-reverts)"

echo "READY  utun=${UTUN}  upstream=reality  log=${LOG}"
echo "---- 验证（另开终端）----"
echo "  bash scripts/knife8-reality-acceptance.sh smoke   # curl HTTPS 经 REALITY"
echo "  期望 client 日志见：🔐 REALITY 握手成功（证书 HMAC 校验通过）  ← 非 echo 充数"
echo "  期望 sing-box 见 vless inbound accept（非 decoy 转发）；目标站收到请求"
echo "  注：REALITY=TCP-only，HTTP/3(QUIC over UDP) 会被 no-op 丢 → curl 自动回落 TCP/HTTP2（符合预期）"
