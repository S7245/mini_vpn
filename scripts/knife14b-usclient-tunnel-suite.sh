#!/usr/bin/env bash
# 刀14c US-client tunnel suite.
#
# Run on the Ubuntu Client VPS. It validates the environment, starts mini_vpn
# client-tun, routes only the iperf target into the TUN, runs the low-RTT probe,
# and writes a self-contained report bundle under /tmp/conn.

set -uo pipefail

usage() {
  cat <<'USAGE'
usage: scripts/knife14b-usclient-tunnel-suite.sh

Required env:
  MINI_VPN_TUIC_SERVER      e.g. 43.153.32.33:8443
  MINI_VPN_TUIC_UUID        TUIC UUID
  MINI_VPN_TUIC_PASSWORD    TUIC password
  MINI_VPN_TUIC_SNI         e.g. example.com
  MINI_VPN_TUIC_CA_PATH     e.g. certs/dev/ca-cert.pem
  MINI_VPN_TUIC_ALPN        e.g. h3

Optional env:
  TARGET=43.130.32.77       iperf target routed into TUN
  EXIT_HOST=<server host>    defaults to MINI_VPN_TUIC_SERVER host
  IPERF_PORT=5201
  OUT_DIR=/tmp/conn
  DURATION=30
  PARALLEL_SET="1 2 4 8"
  SUITE_TAG=knife14c        report/bundle filename tag
  MTU=1200                  TUN MTU passed to mini_vpn before client-tun starts
  MINI_VPN_TCP_DIAG=1       emit knife14c per-handle TCP diagnostics
  RUN_BASE_MTU_P1=0         14c keeps one aligned MTU per process; use a separate MTU=1500 run for baseline
  BUILD_RELEASE=1           build target/release/mini_vpn if missing
  KILL_OLD=1                stop old mini_vpn client-tun before starting
  KEEP_TUNNEL=0             keep mini_vpn running after the suite
  STARTUP_TIMEOUT=25
  METRICS_SECS=5
  CHECK_VPS_SERVICES=1      preflight-check Exit host reachability and Target iperf3 before tunnel starts
  EXIT_PING_REQUIRED=0      set 1 to fail when Exit host ping fails; default warns because ICMP may be blocked
  EXIT_PING_COUNT=3
  EXIT_PING_TIMEOUT=2
  DIRECT_IPERF_DURATION=1   direct Target iperf3 service check duration before routing Target into TUN
  DIRECT_IPERF_TIMEOUT=8s
  WAIT_QUIET_BEFORE_FULL=1  after standalone P1, wait for active relays to drop before full sweep
  QUIET_TIMEOUT_SECS=20
  QUIET_POLL_SECS=1

Output:
  /tmp/conn/mvpn_knife14c_usclient_suite_<timestamp>.md
  /tmp/conn/mvpn_knife14c_usclient_suite_<timestamp>.tar.gz
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_DIR:-/tmp/conn}"
SUITE_TAG="${SUITE_TAG:-knife14c}"
TARGET="${TARGET:-43.130.32.77}"
IPERF_PORT="${IPERF_PORT:-5201}"
DURATION="${DURATION:-30}"
PARALLEL_SET="${PARALLEL_SET:-1 2 4 8}"
MTU="${MTU:-1200}"
RUN_BASE_MTU_P1="${RUN_BASE_MTU_P1:-0}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
KILL_OLD="${KILL_OLD:-1}"
KEEP_TUNNEL="${KEEP_TUNNEL:-0}"
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-25}"
METRICS_SECS="${METRICS_SECS:-5}"
CHECK_VPS_SERVICES="${CHECK_VPS_SERVICES:-1}"
EXIT_PING_REQUIRED="${EXIT_PING_REQUIRED:-0}"
EXIT_PING_COUNT="${EXIT_PING_COUNT:-3}"
EXIT_PING_TIMEOUT="${EXIT_PING_TIMEOUT:-2}"
DIRECT_IPERF_DURATION="${DIRECT_IPERF_DURATION:-1}"
DIRECT_IPERF_TIMEOUT="${DIRECT_IPERF_TIMEOUT:-8s}"
WAIT_QUIET_BEFORE_FULL="${WAIT_QUIET_BEFORE_FULL:-1}"
QUIET_TIMEOUT_SECS="${QUIET_TIMEOUT_SECS:-20}"
QUIET_POLL_SECS="${QUIET_POLL_SECS:-1}"

mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_suite_${TS}.md"
CLIENT_LOG="$OUT_DIR/mvpn_accept_${TS}.log"
BUNDLE="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_suite_${TS}.tar.gz"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$REPO_ROOT/target/release/mini_vpn}"
LOWRTT_SCRIPT="$REPO_ROOT/scripts/knife14b-lowrtt-probe.sh"

VPN_PID=""
TUN_IF=""
RESULT_STATUS="FAILED"
declare -a ARTIFACTS=()
ARTIFACTS+=("$REPORT" "$CLIENT_LOG")

append() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

append_block() {
  local lang="$1"
  shift
  {
    printf '```%s\n' "$lang"
    local line
    for line in "$@"; do
      printf '%s\n' "$line"
    done
    printf '```\n'
  } | tee -a "$REPORT"
}

redacted_env_value() {
  local name="$1"
  local value="${!name:-}"
  case "$name" in
    MINI_VPN_TUIC_PASSWORD)
      if [[ -n "$value" ]]; then
        printf '<redacted:%d chars>' "${#value}"
      else
        printf '<missing>'
      fi
      ;;
    MINI_VPN_TUIC_UUID)
      if [[ -n "$value" ]]; then
        printf '<redacted:%d chars>' "${#value}"
      else
        printf '<missing>'
      fi
      ;;
    *)
      if [[ -n "$value" ]]; then
        printf '%s' "$value"
      else
        printf '<missing>'
      fi
      ;;
  esac
}

run_cmd() {
  append ""
  append '```bash'
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
  } | tee -a "$REPORT"

  set +e
  "$@" 2>&1 | tee -a "$REPORT"
  local status=${PIPESTATUS[0]}
  set -u

  {
    printf 'exit=%s\n' "$status"
    printf '```\n'
  } | tee -a "$REPORT"
  return "$status"
}

fail() {
  append ""
  append "## Failure"
  append "$*"
  RESULT_STATUS="FAILED"
  exit 1
}

warn() {
  append "- WARN: $*"
}

require_cmd() {
  local cmd="$1"
  local hint="$2"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    append "- MISSING: $cmd"
    append "  fix: $hint"
    return 1
  fi
  append "- OK: $cmd ($(command -v "$cmd"))"
  return 0
}

command_status() {
  command -v "$1" >/dev/null 2>&1
}

client_still_running() {
  [[ -n "$VPN_PID" ]] && kill -0 "$VPN_PID" 2>/dev/null
}

client_log_line_count() {
  if [[ -f "$CLIENT_LOG" ]]; then
    wc -l < "$CLIENT_LOG" | tr -d ' '
  else
    echo 0
  fi
}

latest_data_plane_line_since() {
  local start_line="$1"
  if [[ -f "$CLIENT_LOG" ]]; then
    tail -n +"$((start_line + 1))" "$CLIENT_LOG" | grep -E '📊 数据面' | tail -1 || true
  fi
}

wait_for_quiet_tunnel() {
  local label="$1"
  local start_line="$2"
  local deadline=$((SECONDS + QUIET_TIMEOUT_SECS))
  local line=""

  append ""
  append "## Wait For Quiet Tunnel: $label"
  append "- start_log_line: $start_line"
  append "- timeout_secs: $QUIET_TIMEOUT_SECS"

  while ((SECONDS <= deadline)); do
    line="$(latest_data_plane_line_since "$start_line")"
    if [[ "$line" =~ TCP\ relay\ 活跃=([0-9]+)/累计=([0-9]+).*fake-IP\ 活跃=([0-9]+)/在册=([0-9]+) ]]; then
      local tcp_active="${BASH_REMATCH[1]}"
      local fake_active="${BASH_REMATCH[3]}"
      if ((tcp_active == 0 && fake_active == 0)); then
        append '```text'
        append "$line"
        append '```'
        append "quiet: TCP relay and fake-IP active gauges are zero."
        return 0
      fi
    fi
    sleep "$QUIET_POLL_SECS"
  done

  if [[ -n "$line" ]]; then
    append '```text'
    append "$line"
    append '```'
    warn "tunnel still has active relay/fake-IP gauges before $label; skipping full sweep to avoid polluted acceptance data."
  else
    warn "no new data-plane metric line appeared before $label; skipping full sweep to avoid guessing about residual tunnel state."
  fi
  return 1
}

cleanup_stale_target_tun_route() {
  local stale_tun=""
  stale_tun="$(ip -o -4 addr show 2>/dev/null | awk '$4 ~ /^10[.]0[.]0[.]1\// {print $2; exit}')"
  if [[ -z "$stale_tun" ]]; then
    return 0
  fi

  append ""
  append "## Cleanup Stale Target Route"
  append "- stale_tun: $stale_tun"
  run_cmd sudo ip route del "${TARGET}/32" dev "$stale_tun" || true
  run_cmd ip route get "$TARGET" || true
}

preflight_vps_services() {
  append ""
  append "## VPS Service Preflight"
  append "- exit_vps: ${EXIT_HOST}:${EXIT_PORT}（TUIC/sing-box；UDP 服务由后续 mini_vpn handshake 做强校验）"
  append "- target_vps: ${TARGET}:${IPERF_PORT}（iperf3）"

  append ""
  append "### Exit VPS Reachability (${EXIT_HOST})"
  run_cmd ip route get "$EXIT_HOST" || \
    fail "Exit VPS ${EXIT_HOST} 无路由。请检查 Client VPS 网络/安全组。"
  if ! run_cmd ping -c "$EXIT_PING_COUNT" -W "$EXIT_PING_TIMEOUT" "$EXIT_HOST"; then
    if [[ "$EXIT_PING_REQUIRED" == "1" ]]; then
      fail "Exit VPS ${EXIT_HOST} ping 不通。请检查 .33 是否在线、防火墙/安全组是否放行，必要时重启 VPS 或 sing-box。"
    fi
    warn "Exit VPS ${EXIT_HOST} ping 不通；ICMP 可能被禁。后续 mini_vpn TUIC handshake 会继续强校验 sing-box 服务。"
  fi

  append ""
  append "### Target VPS iperf3 Service (${TARGET})"
  run_cmd ip route get "$TARGET" || \
    fail "Target VPS ${TARGET} 无路由。请检查 Client VPS 网络/安全组。"
  run_cmd timeout "$DIRECT_IPERF_TIMEOUT" \
    iperf3 -c "$TARGET" -p "$IPERF_PORT" -t "$DIRECT_IPERF_DURATION" -P 1 || \
    fail "Target VPS ${TARGET}:${IPERF_PORT} direct iperf3 检查失败。请登录 .77 检查或重启 iperf3 服务，例如：sudo systemctl status iperf3 --no-pager；sudo systemctl restart iperf3（或确认手动 iperf3 -s -p ${IPERF_PORT} 正在运行）。"
}

route_target_into_tun() {
  TUN_IF="$(ip -o -4 addr show | awk '$4 ~ /^10[.]0[.]0[.]1\\// {print $2; exit}')"
  if [[ -z "$TUN_IF" ]]; then
    TUN_IF="$(ip -brief addr | awk '/10[.]0[.]0[.]1/ {print $1; exit}')"
  fi
  if [[ -z "$TUN_IF" ]]; then
    append ""
    append "## TUN Discovery Failed"
    run_cmd ip -brief addr || true
    fail "未找到带 10.0.0.1 的 TUN 设备。client-tun 可能没有真正启动；请看 $CLIENT_LOG。"
  fi

  append ""
  append "## TUN Route Setup"
  run_cmd ip -brief addr || true
  run_cmd sudo ip route replace "${TARGET}/32" dev "$TUN_IF" || \
    fail "无法把 ${TARGET}/32 路由到 $TUN_IF。确认脚本以可 sudo 用户运行。"
  run_cmd ip route get "$TARGET" || true
  run_cmd ip route get "$EXIT_HOST" || true

  local target_route exit_route
  target_route="$(ip route get "$TARGET" 2>/dev/null || true)"
  exit_route="$(ip route get "$EXIT_HOST" 2>/dev/null || true)"
  if [[ "$target_route" != *"dev $TUN_IF"* ]]; then
    fail "Target 没有进 TUN。当前 route: $target_route"
  fi
  if [[ "$exit_route" == *"dev $TUN_IF"* ]]; then
    fail "Exit 被错误路由进 TUN，会造成递归。当前 route: $exit_route"
  fi
}

probe_has_receiver_result() {
  local file="$1"
  grep -Eq 'receiver$' "$file"
}

run_lowrtt_probe() {
  local label="$1"
  local parallel="$2"
  local duration="$3"
  local probe_out="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_tunnel_${label}_${TS}.md"
  ARTIFACTS+=("$probe_out")

  append ""
  append "## Probe: $label"
  append "- out: $probe_out"
  append "- parallel_set: $parallel"
  append "- duration: ${duration}s"
  append "- client_log: $CLIENT_LOG"

  run_cmd env \
    LOG="$CLIENT_LOG" \
    OUT="$probe_out" \
    PARALLEL_SET="$parallel" \
    DURATION="$duration" \
    bash "$LOWRTT_SCRIPT" "$TARGET" "$IPERF_PORT"
  local status=$?

  append ""
  append "### Probe $label Summary"
  if [[ -f "$probe_out" ]]; then
    append '```text'
    grep -E 'local 10[.]0[.]0[.]1|receiver$|sender$|error -|Connection reset|log not found|📊|🔬|TUIC datagram|UDP relay mode|tcp-(relay-close|handle-close|global-rx-pressure|loop-flush-tx|tun-flush-fail|send-slice-error)|exit=' "$probe_out" | tail -160 | tee -a "$REPORT" || true
    append '```'
  else
    append "probe report missing: $probe_out"
  fi

  return "$status"
}

diagnose_auth_failure() {
  append ""
  append "## TUIC Startup Diagnosis"
  append "mini_vpn 没有成功连上 TUIC exit。Exit VPS ${EXIT_HOST:-unknown}:${EXIT_PORT:-unknown} 的 sing-box/TUIC 服务可能异常，或凭据/网络不匹配。优先检查这些点："
  append ""
  append "1. 在 ${EXIT_HOST:-43.153.32.33} 上确认 sing-box 已加载新配置："
  append_block bash "sudo systemctl status sing-box --no-pager" \
    "sudo journalctl -u sing-box -n 120 --no-pager" \
    "sudo tail -n 120 /var/log/sing-box.log"
  append "2. 确认 UDP ${EXIT_PORT:-8443} 入站安全组/防火墙允许 Client VPS。"
  append "3. 确认 UUID/password/SNI/ALPN 和 /etc/sing-box/config.json 完全一致；密码建议用单引号 export，避免 shell 特殊字符误处理。"
  append_block bash "export MINI_VPN_TUIC_PASSWORD='你的真实密码'"
  append "4. 确认 MINI_VPN_TUIC_CA_PATH 指向能验证 /etc/sing-box/server-cert.pem 的 CA/证书文件。"
  append "5. 若刚改过 config/cert，重启 sing-box 后再跑："
  append_block bash "sudo systemctl restart sing-box"
}

on_exit() {
  local status=$?

  {
    echo
    echo "## Final Status"
    echo "- status: $RESULT_STATUS"
    echo "- exit_code: $status"
    echo "- report: $REPORT"
    echo "- client_log: $CLIENT_LOG"
    echo "- bundle: $BUNDLE"
  } >> "$REPORT" 2>/dev/null || true

  if [[ "$KEEP_TUNNEL" != "1" ]]; then
    if [[ -n "$TUN_IF" ]]; then
      sudo ip route del "${TARGET}/32" dev "$TUN_IF" >/dev/null 2>&1 || true
    fi
    if [[ -n "$VPN_PID" ]] && kill -0 "$VPN_PID" 2>/dev/null; then
      sudo kill "$VPN_PID" >/dev/null 2>&1 || true
      sleep 1
      sudo kill -9 "$VPN_PID" >/dev/null 2>&1 || true
      sudo pkill -f '[m]ini_vpn.*client-tun' >/dev/null 2>&1 || true
    fi
  fi

  local existing=()
  local f
  for f in "${ARTIFACTS[@]}"; do
    [[ -f "$f" ]] && existing+=("$(basename "$f")")
  done
  if ((${#existing[@]} > 0)); then
    tar -czf "$BUNDLE" -C "$OUT_DIR" "${existing[@]}" >/dev/null 2>&1 || true
  fi

  echo
  echo "report: $REPORT"
  echo "bundle: $BUNDLE"
  exit "$status"
}
trap on_exit EXIT

{
  echo "# 刀14c US Client Tunnel Suite"
  echo
  echo "- date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "- repo: $REPO_ROOT"
  echo "- host: $(hostname 2>/dev/null || echo unknown)"
  echo "- target: ${TARGET}:${IPERF_PORT}"
  echo "- out_dir: $OUT_DIR"
  echo "- report: $REPORT"
  echo "- client_log: $CLIENT_LOG"
} > "$REPORT"

append ""
append "## Environment Checks"
if [[ "$(uname -s)" != "Linux" ]]; then
  fail "此脚本面向 Ubuntu/Linux Client VPS。当前内核: $(uname -s)"
fi

missing=0
require_cmd sudo "sudo apt update && sudo apt install -y sudo" || missing=1
require_cmd ip "sudo apt update && sudo apt install -y iproute2" || missing=1
require_cmd curl "sudo apt update && sudo apt install -y curl" || missing=1
require_cmd iperf3 "sudo apt update && sudo apt install -y iperf3" || missing=1
require_cmd ping "sudo apt update && sudo apt install -y iputils-ping" || missing=1
require_cmd awk "sudo apt update && sudo apt install -y gawk" || missing=1
require_cmd grep "sudo apt update && sudo apt install -y grep" || missing=1
require_cmd sed "sudo apt update && sudo apt install -y sed" || missing=1
require_cmd tar "sudo apt update && sudo apt install -y tar" || missing=1
if [[ "$CHECK_VPS_SERVICES" == "1" ]]; then
  require_cmd timeout "sudo apt update && sudo apt install -y coreutils" || missing=1
fi
if ! command_status dig; then
  warn "dig not found; fake-IP DNS gold check will be skipped by the probe. fix: sudo apt install -y dnsutils"
fi
if ! command_status mtr; then
  warn "mtr not found; direct baseline path trace is unavailable. fix: sudo apt install -y mtr-tiny"
fi
if ((missing)); then
  fail "基础命令缺失。请按上面的 fix 安装后重跑。"
fi

run_cmd uname -a || true
run_cmd date -u '+%Y-%m-%dT%H:%M:%SZ' || true
run_cmd id || true
run_cmd curl -fsS ipinfo.io || true

append ""
append "## TUIC Env Checks"
required_env=(
  MINI_VPN_TUIC_SERVER
  MINI_VPN_TUIC_UUID
  MINI_VPN_TUIC_PASSWORD
  MINI_VPN_TUIC_SNI
  MINI_VPN_TUIC_CA_PATH
  MINI_VPN_TUIC_ALPN
)
env_missing=0
for name in "${required_env[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    append "- MISSING: $name"
    env_missing=1
  else
    append "- $name=$(redacted_env_value "$name")"
  fi
done
if ((env_missing)); then
  append ""
  append "示例（把 UUID/password 换成真实值，password 建议保留单引号）："
  append_block bash \
    "export MINI_VPN_TUIC_SERVER=43.153.32.33:8443" \
    "export MINI_VPN_TUIC_UUID='<uuid>'" \
    "export MINI_VPN_TUIC_PASSWORD='<password>'" \
    "export MINI_VPN_TUIC_SNI=example.com" \
    "export MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem" \
    "export MINI_VPN_TUIC_ALPN=h3"
  fail "TUIC 环境变量不完整。"
fi

if [[ "${MINI_VPN_UPSTREAM:-tuic}" != "tuic" ]]; then
  fail "本测试要求 MINI_VPN_UPSTREAM=tuic。当前 MINI_VPN_UPSTREAM=${MINI_VPN_UPSTREAM}"
fi
export MINI_VPN_UPSTREAM=tuic
export MINI_VPN_TUN_MTU="$MTU"
export MINI_VPN_TCP_DIAG="${MINI_VPN_TCP_DIAG:-1}"
export MINI_VPN_TUIC_CC="${MINI_VPN_TUIC_CC:-cubic}"
export MINI_VPN_TUIC_UDP_MODE="${MINI_VPN_TUIC_UDP_MODE:-native}"
export MINI_VPN_TUIC_ZERO_RTT="${MINI_VPN_TUIC_ZERO_RTT:-false}"

case "$MINI_VPN_TUIC_SERVER" in
  *:*)
    EXIT_HOST="${EXIT_HOST:-${MINI_VPN_TUIC_SERVER%:*}}"
    EXIT_PORT="${MINI_VPN_TUIC_SERVER##*:}"
    ;;
  *) fail "MINI_VPN_TUIC_SERVER 必须包含 host:port，例如 43.153.32.33:8443" ;;
esac

append "- EXIT_HOST=$EXIT_HOST"
append "- EXIT_PORT=$EXIT_PORT"
append "- MINI_VPN_UPSTREAM=$MINI_VPN_UPSTREAM"
append "- MINI_VPN_TUN_MTU=$MINI_VPN_TUN_MTU"
append "- MINI_VPN_TCP_DIAG=$MINI_VPN_TCP_DIAG"
append "- MINI_VPN_TUIC_CC=$MINI_VPN_TUIC_CC"
append "- MINI_VPN_TUIC_UDP_MODE=$MINI_VPN_TUIC_UDP_MODE"
append "- MINI_VPN_TUIC_ZERO_RTT=$MINI_VPN_TUIC_ZERO_RTT"

if [[ "$MINI_VPN_TUIC_ALPN" != "h3" ]]; then
  warn "MINI_VPN_TUIC_ALPN=$MINI_VPN_TUIC_ALPN, but current sing-box config says h3."
fi
if [[ "$MINI_VPN_TUIC_SNI" != "example.com" ]]; then
  warn "MINI_VPN_TUIC_SNI=$MINI_VPN_TUIC_SNI, but current sing-box config says example.com."
fi

if [[ "$MINI_VPN_TUIC_CA_PATH" != /* ]]; then
  MINI_VPN_TUIC_CA_PATH="$REPO_ROOT/$MINI_VPN_TUIC_CA_PATH"
  export MINI_VPN_TUIC_CA_PATH
  append "- normalized MINI_VPN_TUIC_CA_PATH=$MINI_VPN_TUIC_CA_PATH"
fi
if [[ ! -r "$MINI_VPN_TUIC_CA_PATH" ]]; then
  fail "CA 文件不可读: $MINI_VPN_TUIC_CA_PATH。请复制正确 CA/证书，或 export MINI_VPN_TUIC_CA_PATH=/absolute/path。"
fi
if command_status openssl; then
  run_cmd openssl x509 -in "$MINI_VPN_TUIC_CA_PATH" -noout -subject -issuer -dates || true
else
  warn "openssl not found; skipping CA certificate snapshot. fix: sudo apt install -y openssl"
fi

append ""
append "## Sudo / Build Checks"
run_cmd sudo -v || fail "sudo 校验失败。请确认当前用户有 sudo 权限。"
if ! sudo -E env sh -c 'test -n "${MINI_VPN_TUIC_PASSWORD:-}"' >/dev/null 2>&1; then
  fail "sudo -E 没有保留 MINI_VPN_TUIC_* 环境变量。请在 root shell 中 export 这些变量后运行脚本，或调整 sudoers env_keep。"
fi

if [[ ! -x "$BIN" ]]; then
  if [[ "$BUILD_RELEASE" == "1" ]]; then
    require_cmd cargo "curl https://sh.rustup.rs -sSf | sh; source ~/.cargo/env" || \
      fail "target/release/mini_vpn 不存在，且 cargo 不可用。"
    run_cmd cargo build --release || fail "cargo build --release 失败。请把 report 发回来。"
  else
    fail "binary missing: $BIN。设置 BUILD_RELEASE=1 或先运行 cargo build --release。"
  fi
fi
run_cmd "$BIN" --help || true

append ""
append "## Preflight Routes"
run_cmd ip route get "$EXIT_HOST" || true
run_cmd ip route get "$TARGET" || true
run_cmd ip -brief addr || true

append ""
append "## Stop Old Tunnel"
old_pids="$(pgrep -f '[m]ini_vpn.*client-tun' || true)"
if [[ -n "$old_pids" ]]; then
  append "old mini_vpn client-tun pids:"
  append_block text "$old_pids"
  if [[ "$KILL_OLD" == "1" ]]; then
    run_cmd sudo pkill -f '[m]ini_vpn.*client-tun' || true
    sleep 2
  else
    fail "已有 mini_vpn client-tun 在运行。设置 KILL_OLD=1 或手动停止后重跑。"
  fi
else
  append "no old mini_vpn client-tun process found."
fi

cleanup_stale_target_tun_route

if [[ "$CHECK_VPS_SERVICES" == "1" ]]; then
  preflight_vps_services
else
  append ""
  append "## VPS Service Preflight"
  append "skipped because CHECK_VPS_SERVICES=$CHECK_VPS_SERVICES"
fi

append ""
append "## Start mini_vpn client-tun"
: > "$CLIENT_LOG"
append "- client_log: $CLIENT_LOG"
append "- command: sudo -E env MINI_VPN_TUN_MTU=$MTU MINI_VPN_TCP_DIAG=$MINI_VPN_TCP_DIAG MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=$METRICS_SECS $BIN client-tun"
sudo -E env MINI_VPN_TUN_MTU="$MTU" MINI_VPN_TCP_DIAG="$MINI_VPN_TCP_DIAG" MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS="$METRICS_SECS" \
  "$BIN" client-tun > "$CLIENT_LOG" 2>&1 &
VPN_PID=$!
append "- launcher_pid: $VPN_PID"

ready=0
for ((i = 0; i < STARTUP_TIMEOUT; i++)); do
  if ! client_still_running; then
    append ""
    append "mini_vpn exited during startup."
    append_block text "$(tail -n 160 "$CLIENT_LOG" 2>/dev/null || true)"
    if grep -q '连接 TUIC 出口失败' "$CLIENT_LOG" 2>/dev/null; then
      diagnose_auth_failure
    fi
    fail "client-tun 启动失败。"
  fi
  if grep -q '连接 TUIC 出口失败' "$CLIENT_LOG" 2>/dev/null; then
    append_block text "$(tail -n 160 "$CLIENT_LOG" 2>/dev/null || true)"
    diagnose_auth_failure
    fail "TUIC 认证/握手失败。"
  fi
  if grep -q '✅ 已连接 TUIC 出口' "$CLIENT_LOG" 2>/dev/null && \
     grep -q '🌊 UDP relay 数据面就绪' "$CLIENT_LOG" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done

append ""
append "### Startup Log Tail"
append_block text "$(tail -n 160 "$CLIENT_LOG" 2>/dev/null || true)"

if [[ "$ready" != "1" ]]; then
  diagnose_auth_failure
  fail "等待 ${STARTUP_TIMEOUT}s 后仍未看到 TUIC ready 日志。"
fi

route_target_into_tun

append ""
append "## Wait For First Metrics Tick"
sleep "$((METRICS_SECS + 2))"
append_block text "$(tail -n 200 "$CLIENT_LOG" 2>/dev/null || true)"

BASE_MTU="$(ip link show "$TUN_IF" 2>/dev/null | sed -n 's/.* mtu \([0-9][0-9]*\) .*/\1/p' | head -1)"
append ""
append "## MTU / Probe Plan"
append "- tun_if: $TUN_IF"
append "- base_mtu: ${BASE_MTU:-unknown}"
append "- test_mtu: $MTU"
append "- run_base_mtu_p1: $RUN_BASE_MTU_P1"
if [[ "${BASE_MTU:-}" != "$MTU" ]]; then
  fail "TUN MTU mismatch: expected MINI_VPN_TUN_MTU=$MTU but $TUN_IF reports ${BASE_MTU:-unknown}. 请看 $CLIENT_LOG。"
fi

if [[ "$RUN_BASE_MTU_P1" == "1" ]]; then
  append ""
  append "## Base MTU Probe Skipped"
  append "14c 要求 mini_vpn 进程启动前就对齐 OS TUN MTU 与 smoltcp MTU；同一进程内不再先跑 1500 再 ip link set。若要 baseline，请另跑一次 MTU=1500。"
fi

append ""
append "## Verified Test MTU"
run_cmd ip link show "$TUN_IF" || true
route_target_into_tun

run_lowrtt_probe "mtu${MTU}_p1" "1" "$DURATION" || true
MTU_P1_OUT="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_tunnel_mtu${MTU}_p1_${TS}.md"
P1_END_LINE="$(client_log_line_count)"

if [[ -f "$MTU_P1_OUT" ]] && probe_has_receiver_result "$MTU_P1_OUT"; then
  if [[ "$WAIT_QUIET_BEFORE_FULL" == "1" ]]; then
    if wait_for_quiet_tunnel "full sweep" "$P1_END_LINE"; then
      run_lowrtt_probe "mtu${MTU}_full" "$PARALLEL_SET" "$DURATION" || true
    else
      append ""
      append "## Full Sweep Skipped"
      append "P1 后 tunnel 没有在 ${QUIET_TIMEOUT_SECS}s 内确认归零；跳过 full sweep，避免把旧连接残留误判成 P2/P4 问题。"
    fi
  else
    run_lowrtt_probe "mtu${MTU}_full" "$PARALLEL_SET" "$DURATION" || true
  fi
else
  append ""
  append "## Full Sweep Skipped"
  append "MTU=$MTU P1 没有 receiver 结果，说明 tunnel 基础连通/iperf 控制连接已经失败；跳过 full sweep，避免浪费时间。"
fi

append ""
append "## Final Snapshots"
run_cmd ip route get "$TARGET" || true
run_cmd ip route get "$EXIT_HOST" || true
run_cmd ip -s link show "$TUN_IF" || true
append ""
append "### Final mini_vpn Log Tail"
append_block text "$(tail -n 240 "$CLIENT_LOG" 2>/dev/null || true)"

RESULT_STATUS="COMPLETED"
append ""
append "## Send Back"
append "请把这个 bundle 发回来："
append_block text "$BUNDLE"
append "如果附件不方便，也可以发整个目录下这些文件："
append_block text "$(printf '%s\n' "${ARTIFACTS[@]}")"

exit 0
