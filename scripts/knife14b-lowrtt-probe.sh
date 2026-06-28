#!/usr/bin/env bash
# 刀14b low-RTT fat-path #3 quantify probe.
# Requires an already-running tunnel started with:
#   sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 bash scripts/knife35-acceptance.sh soak
#
# This script does not implement or test a connection pool. It records the single-connection baseline
# needed to decide whether a pool is worth building.

set -euo pipefail

usage() {
  cat <<'USAGE'
usage: scripts/knife14b-lowrtt-probe.sh <iperf-target> [port]

env:
  PARALLEL_SET="1 2 4 8"   iperf parallel sweep
  DURATION=30              seconds per iperf run
  LOG=/tmp/mvpn_accept.log mini_vpn soak log
  OUT=/tmp/mvpn_knife14b_lowrtt_<timestamp>.md
  RUN_UDP=0                set 1 to run UDP probes too
  UDP_BW=90M               UDP offered bandwidth
  UDP_LEN=1200             UDP datagram payload length
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
LOG="${LOG:-/tmp/mvpn_accept.log}"
RUN_UDP="${RUN_UDP:-0}"
UDP_BW="${UDP_BW:-90M}"
UDP_LEN="${UDP_LEN:-1200}"
OUT="${OUT:-/tmp/mvpn_knife14b_lowrtt_$(date +%Y%m%d_%H%M%S).md}"
METRIC_RE='📊 数据面|🔬 主循环|TUIC datagram|UDP relay mode'

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

append_section() {
  {
    echo
    echo "## $1"
  } | tee -a "$OUT"
}

append_subsection() {
  {
    echo
    echo "### $1"
  } | tee -a "$OUT"
}

log_line_count() {
  if [[ -f "$LOG" ]]; then
    wc -l < "$LOG" | tr -d ' '
  else
    echo 0
  fi
}

append_metrics_since() {
  local start_line="$1"
  local title="$2"

  append_subsection "$title"
  if [[ -f "$LOG" ]]; then
    {
      echo '```text'
      tail -n +"$((start_line + 1))" "$LOG" | grep -E "$METRIC_RE" || \
        echo "(no matching mini_vpn metrics emitted during this run)"
      echo '```'
    } | tee -a "$OUT"
  else
    echo "log not found: $LOG" | tee -a "$OUT"
  fi
}

append_iperf_cmd() {
  local metrics_title="$1"
  shift

  local start_line
  start_line="$(log_line_count)"
  append_cmd "$@"
  append_metrics_since "$start_line" "$metrics_title"
}

{
  echo "# 刀14b low-RTT fat-path probe result"
  echo
  echo "- date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "- target: ${TARGET}:${PORT}"
  echo "- parallel_set: ${PARALLEL_SET}"
  echo "- duration: ${DURATION}s"
  echo "- log: ${LOG}"
  echo
  echo "> 判读前先确认：curl ipinfo.io 必须是 exit IP；dig example.com +short 应是 198.18.x.x；📊 TCP relay 累计应增长。"
} > "$OUT"

append_section "Tunnel Gold Checks"
append_cmd curl -fsS ipinfo.io
if command -v dig >/dev/null 2>&1; then
  append_cmd dig example.com +short
else
  echo "dig not found; skipping fake-IP DNS check" | tee -a "$OUT"
fi

append_section "Recent mini_vpn Metrics"
if [[ -f "$LOG" ]]; then
  {
    echo '```text'
    grep -E "$METRIC_RE" "$LOG" | tail -20 || true
    echo '```'
  } | tee -a "$OUT"
else
  echo "log not found: $LOG" | tee -a "$OUT"
fi

append_section "TCP Forward Sweep"
for p in $PARALLEL_SET; do
  append_iperf_cmd "mini_vpn Metrics during TCP Forward P=$p" \
    iperf3 -c "$TARGET" -p "$PORT" -t "$DURATION" -P "$p"
done

append_section "TCP Reverse Sweep"
for p in $PARALLEL_SET; do
  append_iperf_cmd "mini_vpn Metrics during TCP Reverse P=$p" \
    iperf3 -c "$TARGET" -p "$PORT" -t "$DURATION" -P "$p" -R
done

if [[ "$RUN_UDP" == "1" || "$RUN_UDP" == "true" ]]; then
  append_section "UDP Forward Sweep"
  for p in $PARALLEL_SET; do
    append_iperf_cmd "mini_vpn Metrics during UDP Forward P=$p" \
      iperf3 -c "$TARGET" -p "$PORT" -u -b "$UDP_BW" -l "$UDP_LEN" -t "$DURATION" -P "$p"
  done

  append_section "UDP Reverse Sweep"
  for p in $PARALLEL_SET; do
    append_iperf_cmd "mini_vpn Metrics during UDP Reverse P=$p" \
      iperf3 -c "$TARGET" -p "$PORT" -u -b "$UDP_BW" -l "$UDP_LEN" -t "$DURATION" -P "$p" -R
  done
fi

append_section "Post-run mini_vpn Metrics"
if [[ -f "$LOG" ]]; then
  {
    echo '```text'
    grep -E "$METRIC_RE" "$LOG" | tail -30 || true
    echo '```'
  } | tee -a "$OUT"
fi

echo
echo "report written: $OUT"
