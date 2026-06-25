#!/usr/bin/env bash
# 刀9 真出口 acceptance helper (macOS). NOT a product file — dev/test only.
# 读 env 的 MINI_VPN_TUIC_* + MINI_VPN_REALITY_*（含凭据）——脚本本身不含任何凭据。
#
# 验证 auto-failover：TUIC 主腿 ↔ REALITY 备腿（TCP relay 健康感知切换）；UDP 永绑 TUIC。
#
# 用法（两腿凭据先 export，见下）：
#   构建：       cargo build --release
#   全局 soak：  sudo -E bash scripts/knife9-failover-acceptance.sh soak        # 建 TUN + 全隧道路由 + DNS 进 TUN（failover 模式）
#   烟测：       bash scripts/knife9-failover-acceptance.sh smoke               # curl HTTPS（应 200，无论哪条腿）+ 看当前腿
#   UDP 探针：   bash scripts/knife9-failover-acceptance.sh udp-check           # HTTP/3(QUIC,UDP) 探：TUIC 当班通 / REALITY 当班丢
#   打断 TUIC：  sudo -E bash scripts/knife9-failover-acceptance.sh cut-tuic    # pf 按端口阻断 QUIC/UDP（REALITY :TCP 不受影响）→ 验切 REALITY
#   恢复 TUIC：  sudo -E bash scripts/knife9-failover-acceptance.sh restore-tuic# 清 pf → 验 60s+ 冷却后切回 TUIC
#   看切换：     bash scripts/knife9-failover-acceptance.sh status              # grep 切换/握手日志 + 推断当前腿
#   停 soak：    sudo -E bash scripts/knife9-failover-acceptance.sh soak-stop   # 还原 DNS + 删路由 + 清 pf + kill
#
# 凭据 export（向项目负责人要，勿入库）——**两腿都要配齐**：
#   # TUIC 主腿（同刀3.5/刀8）：
#   export MINI_VPN_TUIC_SERVER=<VPS_IP>:8443
#   export MINI_VPN_TUIC_UUID=<uuid>
#   export MINI_VPN_TUIC_PASSWORD=<pass>
#   #（SNI/ALPN/CA 有默认：example.com / h3 / certs/dev/ca-cert.pem）
#   # REALITY 备腿（同刀8）：
#   export MINI_VPN_REALITY_SERVER=<VPS_IP>:443       # 通常与 TUIC 同 VPS，不同端口/协议
#   export MINI_VPN_REALITY_UUID=<uuid>
#   export MINI_VPN_REALITY_PBK=<public_key>          # base64url 43 字符
#   export MINI_VPN_REALITY_SHORT_ID=<short_id_hex>   # 可空
#   export MINI_VPN_REALITY_SNI=<borrowed_site>       # 借用站域名（须 ∈ 服务端 serverNames）
#
# 可覆盖 env：FAKE_DNS（默认 198.18.0.1）、SMOKE_URL（默认 cloudflare trace）、NETSVC（默认自动探测）。
#
# ⚠️ cut-tuic 会临时改 pf（备份 /etc/pf.conf，restore-tuic/soak-stop 还原）。假定 stock macOS pf。在测试机跑。
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO/target/release/mini_vpn"
LOG=/tmp/mvpn_failover_accept.log
FAKE_NET=198.18.0.0/15
FAKE_DNS="${FAKE_DNS:-198.18.0.1}"
DNS_SAVE=/tmp/mvpn_failover_prior_dns.txt
SMOKE_URL="${SMOKE_URL:-https://www.cloudflare.com/cdn-cgi/trace}"
PF_BAK=/tmp/mvpn_failover_pf.conf.bak
PF_ANCHOR=mvpn_knife9
ACTION="${1:-}"

detect_netsvc() {
  if [ -n "${NETSVC:-}" ]; then echo "$NETSVC"; return; fi
  local dev; dev="$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')"
  [ -z "$dev" ] && { echo "Wi-Fi"; return; }
  networksetup -listnetworkserviceorder 2>/dev/null | awk -v d="$dev" '
    /^\([0-9*]+\)/ { name=$0; sub(/^\([0-9*]+\) /,"",name) }
    $0 ~ ("Device: " d ")") { print name; exit }'
}

# 从 MINI_VPN_TUIC_SERVER（host:port）拆出 host/port，供 pf 按端口阻断 QUIC/UDP。
tuic_host() { printf '%s' "${MINI_VPN_TUIC_SERVER:-}" | sed -E 's/:[0-9]+$//'; }
tuic_port() { printf '%s' "${MINI_VPN_TUIC_SERVER:-}" | sed -E 's/^.*://'; }

cut_tuic() {
  : "${MINI_VPN_TUIC_SERVER:?需 export MINI_VPN_TUIC_SERVER（host:port）}"
  local h p; h="$(tuic_host)"; p="$(tuic_port)"
  echo "==> 阻断 TUIC：pf block outbound UDP → ${h}:${p}（REALITY TCP 不受影响）"
  # 备份系统 pf.conf；把 block 灌进独立 anchor，并在 pf.conf 副本末尾引用该 anchor（保留系统规则）。
  cp /etc/pf.conf "$PF_BAK" 2>/dev/null || : > "$PF_BAK"
  printf 'block drop out quick proto udp from any to %s port %s\n' "$h" "$p" \
    | pfctl -a "$PF_ANCHOR" -f - 2>/dev/null
  { cat /etc/pf.conf 2>/dev/null; echo "anchor \"$PF_ANCHOR\""; } | pfctl -f - 2>/dev/null
  pfctl -e 2>/dev/null || true
  echo "   ✅ 已阻断 QUIC/UDP。≤(idle30s + 1 次重连失败) 内应见日志：🔀 failover：TUIC 连接死(黑洞快路) → 切到 REALITY"
  echo "   验证：bash scripts/knife9-failover-acceptance.sh smoke  （应仍 200，经 REALITY）"
}

restore_tuic() {
  echo "==> 恢复 TUIC：清 pf anchor + 还原系统 pf.conf"
  pfctl -a "$PF_ANCHOR" -F rules 2>/dev/null
  pfctl -f /etc/pf.conf 2>/dev/null
  echo "   ✅ 已恢复。后台探针 30s 节奏 + 连续 3 成功 + 60s 冷却 → 应见日志：🔀 failover：... → 切回 TUIC 主腿"
}

smoke() {
  echo "==> 烟测：curl ${SMOKE_URL}（应 200，无论当前在 TUIC 还是 REALITY 腿）"
  local code; code="$(curl -sS -o /tmp/mvpn_failover_smoke.out -w '%{http_code}' --max-time 20 "$SMOKE_URL")"
  echo "HTTP ${code}; cloudflare trace（看 ip= 是否 VPS 出口）："; grep -E '^(ip|loc|warp)=' /tmp/mvpn_failover_smoke.out 2>/dev/null
  status
  { [ "$code" = "200" ] || [ "$code" = "301" ] || [ "$code" = "302" ]; } \
    && echo "✅ 隧道可达（HTTP ${code}）" \
    || echo "⚠️ HTTP ${code}（检查三端日志 + status）"
}

# HTTP/3(QUIC over UDP) 探针：TUIC 当班 → UDP 经 datagram 面通；REALITY 当班 → UDP no-op 丢 → h3 失败。
udp_check() {
  if ! curl --http3-only --help >/dev/null 2>&1; then
    echo "⚠️ 本机 curl 不支持 --http3-only（macOS 系统 curl 常无 HTTP/3）。"
    echo "   替代观测：cut-tuic 后看 ${LOG} 是否出现 'TUIC UDP↑ 无可用连接，丢弃'（REALITY 当班 UDP 丢的实锤）。"
    return 0
  fi
  echo "==> UDP-over-tunnel 探针：curl --http3-only ${SMOKE_URL}"
  local code; code="$(curl --http3-only -sS -o /dev/null -w '%{http_code}' --max-time 12 "$SMOKE_URL" 2>/dev/null || echo 000)"
  if [ "$code" = "200" ]; then
    echo "✅ HTTP/3 成功（UDP 经 TUIC datagram 面通）→ 当前 UDP 出口=TUIC（应在 TUIC 当班时）"
  else
    echo "❌ HTTP/3 失败（rc/code=${code}）→ UDP 被丢（应在 REALITY 当班时：UDP no-op，符合预期）"
  fi
}

# 轮询等待切换日志（纠正「restore/检查太早」——down 检测需 idle(>=15s) + 5s 重连，别在窗口内就 restore）。
# 用法：wait-switch [reality|tuic]（默认 reality）。
wait_switch() {
  local want="${2:-reality}" pat secs=0 max=80
  if [ "$want" = "tuic" ]; then pat="切回 TUIC"; else pat="切到 REALITY"; fi
  echo "==> 等 failover 日志『${pat}』（最多 ${max}s；别提前 restore）..."
  while [ "$secs" -lt "$max" ]; do
    if grep -q "$pat" "$LOG" 2>/dev/null; then
      echo ""; echo "✅ 切换发生（~${secs}s）：$(grep "$pat" "$LOG" | tail -1)"
      return 0
    fi
    sleep 2; secs=$((secs+2)); printf '\r    %ss...' "$secs"
  done
  echo ""; echo "❌ ${max}s 内未见『${pat}』。当前 status："; status; return 1
}

status() {
  echo "---- failover 切换/握手日志（tail）----"
  grep -E "🔀 failover|REALITY 握手成功|切到 REALITY|切回 TUIC|无可用连接，丢弃|spawn 握手" "$LOG" 2>/dev/null | tail -10
  # 推断当前腿：最后一条切换日志。
  local last; last="$(grep -E "切到 REALITY|切回 TUIC" "$LOG" 2>/dev/null | tail -1)"
  if printf '%s' "$last" | grep -q "切到 REALITY"; then echo "▶ 推断当前 TCP 腿 = REALITY（备）"
  elif printf '%s' "$last" | grep -q "切回 TUIC"; then echo "▶ 推断当前 TCP 腿 = TUIC（主）"
  else echo "▶ 推断当前 TCP 腿 = TUIC（主，未发生切换）"; fi
}

soak_stop() {
  local svc; svc="$(detect_netsvc)"
  local prior; prior="$(cat "$DNS_SAVE" 2>/dev/null)"
  if [ -z "$prior" ] || printf '%s' "$prior" | grep -qi "aren't any"; then prior="empty"; fi
  networksetup -setdnsservers "$svc" $prior && echo "DNS($svc) restored: $prior"
  pkill -f "mini_vpn client-tun" 2>/dev/null
  route -n delete -net "$FAKE_NET" >/dev/null 2>&1
  # 清 pf（若 cut-tuic 留下）。
  pfctl -a "$PF_ANCHOR" -F rules 2>/dev/null
  pfctl -f /etc/pf.conf 2>/dev/null
  sleep 1
  echo "soak stopped + /15 route removed + DNS restored + pf cleared"
}

case "$ACTION" in
  smoke)        smoke; exit 0 ;;
  udp-check)    udp_check; exit 0 ;;
  wait-switch)  wait_switch "$@"; exit $? ;;
  status)       status; exit 0 ;;
  cut-tuic)     cut_tuic; exit 0 ;;
  restore-tuic) restore_tuic; exit 0 ;;
  soak-stop)    soak_stop; exit 0 ;;
  soak)         ;;
  *) echo "usage: $0 {soak | smoke | udp-check | cut-tuic | wait-switch [reality|tuic] | restore-tuic | status | soak-stop}"; exit 2 ;;
esac

# ---- soak（failover 模式：两腿都配齐）----
[ -x "$BIN" ] || { echo "!! 未找到 release binary: $BIN（先 'cargo build --release'）"; exit 1; }
: "${MINI_VPN_TUIC_SERVER:?需 export MINI_VPN_TUIC_SERVER}"
: "${MINI_VPN_TUIC_UUID:?需 export MINI_VPN_TUIC_UUID}"
: "${MINI_VPN_TUIC_PASSWORD:?需 export MINI_VPN_TUIC_PASSWORD}"
: "${MINI_VPN_REALITY_SERVER:?需 export MINI_VPN_REALITY_SERVER（failover 需 REALITY 备腿）}"
: "${MINI_VPN_REALITY_UUID:?需 export MINI_VPN_REALITY_UUID}"
: "${MINI_VPN_REALITY_PBK:?需 export MINI_VPN_REALITY_PBK}"
: "${MINI_VPN_REALITY_SNI:?需 export MINI_VPN_REALITY_SNI（借用站）}"

pkill -f "mini_vpn client-tun" 2>/dev/null
sleep 1
BEFORE="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"

: > "$LOG"
cd "$REPO" || { echo "cd $REPO failed"; exit 1; }
MINI_VPN_UPSTREAM=failover \
MINI_VPN_TUIC_SNI="${MINI_VPN_TUIC_SNI:-example.com}" \
MINI_VPN_TUIC_CA_PATH="${MINI_VPN_TUIC_CA_PATH:-$REPO/certs/dev/ca-cert.pem}" \
MINI_VPN_TUIC_ALPN="${MINI_VPN_TUIC_ALPN:-h3}" \
nohup "$BIN" client-tun >>"$LOG" 2>&1 &
echo "client-tun started (failover): pid=$!"

# 等 failover 就绪（🔀 行，最多 30s）。
for _ in $(seq 1 30); do grep -q "failover 就绪" "$LOG" && break; sleep 1; done
if ! grep -q "failover 就绪" "$LOG"; then
  echo "!! failover 未就绪（30s）；最后 15 行日志（检查两腿凭据）："; tail -15 "$LOG"; exit 1
fi

AFTER="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"
UTUN="$(comm -13 <(echo "$BEFORE") <(echo "$AFTER") | head -1)"
[ -z "$UTUN" ] && { echo "!! no new utun detected"; exit 1; }

SVC="$(detect_netsvc)"
route -n add -net "$FAKE_NET" -interface "$UTUN" >/dev/null 2>&1 \
  && echo "route ${FAKE_NET} -> ${UTUN} OK" || echo "!! /15 route add failed"
networksetup -getdnsservers "$SVC" 2>/dev/null > "$DNS_SAVE"
networksetup -setdnsservers "$SVC" "$FAKE_DNS" \
  && echo "DNS(${SVC}) -> ${FAKE_DNS} (saved=${DNS_SAVE}; soak-stop auto-reverts)"

echo "---- startup lines ----"; grep -E "TUIC 出口|REALITY 出口|failover 就绪|UDP relay" "$LOG" | tail -4
echo "READY  utun=${UTUN}  upstream=failover  log=${LOG}"
echo "---- acceptance 流程（另开终端；⚠️ 关键：down 检测需 idle(>=15s)+5s 重连，cut 后**先 wait-switch 再 restore**）----"
echo "  1) smoke                                      # TUIC 当班 → 200"
echo "  2) sudo -E bash $0 cut-tuic                   # 打断 TUIC（pf 封 UDP:${MINI_VPN_TUIC_SERVER}）"
echo "  3) bash $0 wait-switch reality                # 轮询等『切到 REALITY』（最多 80s；**别提前 restore**）"
echo "  4) smoke                                      # 切换后应仍 200（经 REALITY；status 见 ▶ REALITY）"
echo "  5) sudo -E bash $0 restore-tuic               # 恢复 TUIC（仅在已观察到切到 REALITY 后）"
echo "  6) bash $0 wait-switch tuic                   # 轮询等『切回 TUIC』（连续 3 探针 + 60s 冷却 → ~90s+）"
echo "  停：sudo -E bash $0 soak-stop"
