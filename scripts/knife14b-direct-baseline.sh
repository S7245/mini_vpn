#!/usr/bin/env bash
# 刀14b direct path baseline.
# Run this on the sing-box / TUIC exit host, not on the mini_vpn client.
# It measures whether the exit -> iperf target path is itself a clean 150M+ path.

set -euo pipefail

usage() {
  cat <<'USAGE'
usage: scripts/knife14b-direct-baseline.sh <iperf-target> [port]

env:
  PARALLEL_SET="1 2 4 8"     iperf parallel sweep
  DURATION=30                seconds per iperf run
  OUT=/tmp/mvpn_knife14b_direct_<timestamp>.md
  PING_COUNT=20              ping samples before iperf
  MTR_COUNT=20               mtr samples when mtr is installed

Run on the exit/sing-box server. The output .md is the file to send back for analysis.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TARGET="${1:-}"
PORT="${2:-5201}"
if [[ -z "$TARGET" ]]; then
  usage >&2
  exit 2
fi

command -v iperf3 >/dev/null 2>&1 || { echo "iperf3 not found" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl not found" >&2; exit 1; }

PARALLEL_SET="${PARALLEL_SET:-1 2 4 8}"
DURATION="${DURATION:-30}"
PING_COUNT="${PING_COUNT:-20}"
MTR_COUNT="${MTR_COUNT:-20}"
OUT="${OUT:-/tmp/mvpn_knife14b_direct_$(date +%Y%m%d_%H%M%S).md}"

append_cmd() {
  {
    echo
    echo '```bash'
    printf '$'
    printf ' %q' "$@"
    echo
  } | tee -a "$OUT"

  set +e
  "$@" 2>&1 | tee -a "$OUT"
  local status=${PIPESTATUS[0]}
  set -e

  {
    echo "exit=$status"
    echo '```'
  } | tee -a "$OUT"
}

append_skip() {
  {
    echo
    echo "```text"
    echo "$1"
    echo "```"
  } | tee -a "$OUT"
}

append_section() {
  {
    echo
    echo "## $1"
  } | tee -a "$OUT"
}

{
  echo "# 刀14b direct baseline result"
  echo
  echo "- date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "- target: ${TARGET}:${PORT}"
  echo "- parallel_set: ${PARALLEL_SET}"
  echo "- duration: ${DURATION}s"
  echo "- host: $(hostname 2>/dev/null || echo unknown)"
  echo
  echo "> Run location: sing-box / TUIC exit host. Purpose: prove whether the direct exit-to-target path can stably reach 150M+ before blaming mini_vpn single-connection behavior."
} > "$OUT"

append_section "Host Context"
append_cmd hostname
append_cmd uname -a
append_cmd date -u '+%Y-%m-%dT%H:%M:%SZ'
append_cmd curl -fsS ipinfo.io
append_cmd iperf3 --version

if command -v ip >/dev/null 2>&1; then
  append_cmd ip route get "$TARGET"
  append_cmd ip -brief addr
else
  append_skip "ip command not found; skipping route/address snapshot"
fi

if command -v sysctl >/dev/null 2>&1; then
  append_cmd sysctl net.ipv4.tcp_congestion_control net.ipv4.tcp_available_congestion_control
else
  append_skip "sysctl not found; skipping TCP congestion-control snapshot"
fi

append_section "Path RTT"
if command -v ping >/dev/null 2>&1; then
  append_cmd ping -c "$PING_COUNT" "$TARGET"
else
  append_skip "ping not found; skipping RTT check"
fi

if command -v mtr >/dev/null 2>&1; then
  append_cmd mtr -rwzc "$MTR_COUNT" "$TARGET"
elif command -v tracepath >/dev/null 2>&1; then
  append_cmd tracepath -n "$TARGET"
elif command -v traceroute >/dev/null 2>&1; then
  append_cmd traceroute "$TARGET"
else
  append_skip "mtr/tracepath/traceroute not found; skipping path trace"
fi

append_section "TCP Forward Sweep"
for p in $PARALLEL_SET; do
  append_cmd iperf3 -c "$TARGET" -p "$PORT" -t "$DURATION" -P "$p"
done

append_section "TCP Reverse Sweep"
for p in $PARALLEL_SET; do
  append_cmd iperf3 -c "$TARGET" -p "$PORT" -t "$DURATION" -P "$p" -R
done

append_section "Result Summary Lines"
{
  echo '```text'
  grep -E '^\[SUM\].*(sender|receiver)$|^\[[[:space:]]*[0-9]+\].*(sender|receiver)$|^exit=' "$OUT" || true
  echo '```'
} | tee -a "$OUT"

echo
echo "report written: $OUT"
