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
  IPERF_TIMEOUT_SECS=DURATION+20 external timeout per iperf command
  RUN_UDP=0                set 1 to run UDP probes too
  UDP_BW=90M               UDP offered bandwidth
  UDP_LEN=1200             UDP datagram payload length
  IPERF_BUSY_RETRIES=3     retry an iperf command when the target server is busy
  IPERF_BUSY_WAIT_SECS=5   seconds to wait between busy retries
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
command -v timeout >/dev/null 2>&1 || { echo "timeout not found" >&2; exit 1; }

PARALLEL_SET="${PARALLEL_SET:-1 2 4 8}"
DURATION="${DURATION:-30}"
IPERF_TIMEOUT_SECS="${IPERF_TIMEOUT_SECS:-$((DURATION + 20))}"
LOG="${LOG:-/tmp/mvpn_accept.log}"
RUN_UDP="${RUN_UDP:-0}"
UDP_BW="${UDP_BW:-90M}"
UDP_LEN="${UDP_LEN:-1200}"
IPERF_BUSY_RETRIES="${IPERF_BUSY_RETRIES:-3}"
IPERF_BUSY_WAIT_SECS="${IPERF_BUSY_WAIT_SECS:-5}"
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

append_cleanliness_check() {
  append_section "Pre-run Cleanliness Check"
  if [[ ! -f "$LOG" ]]; then
    echo "log not found: $LOG" | tee -a "$OUT"
    return
  fi

  local line
  line="$(grep -E '📊 数据面' "$LOG" | tail -1 || true)"
  if [[ -z "$line" ]]; then
    echo "no mini_vpn data-plane metric line found before sweep" | tee -a "$OUT"
    return
  fi

  echo '```text' | tee -a "$OUT"
  echo "$line" | tee -a "$OUT"
  echo '```' | tee -a "$OUT"

  if [[ "$line" =~ TCP\ relay\ 活跃=([0-9]+)/累计=([0-9]+).*fake-IP\ 活跃=([0-9]+)/在册=([0-9]+) ]]; then
    local tcp_active="${BASH_REMATCH[1]}"
    local fake_active="${BASH_REMATCH[3]}"
    if (( tcp_active > 0 || fake_active > 0 )); then
      echo "⚠️ background tunnel activity detected; close noisy apps or restart the tunnel before a decisive 14b run." | tee -a "$OUT"
    else
      echo "quiet baseline: no active TCP relay or fake-IP flow in the last metric tick." | tee -a "$OUT"
    fi
  else
    echo "could not parse active-flow gauges from the last metric tick." | tee -a "$OUT"
  fi
}

append_iperf_cmd() {
  local metrics_title="$1"
  shift

  local start_line
  start_line="$(log_line_count)"
  local attempt=1
  local max_attempts=$((IPERF_BUSY_RETRIES + 1))
  while ((attempt <= max_attempts)); do
    local tmp
    tmp="$(mktemp)"
    {
      echo
      echo "- iperf_attempt: ${attempt}/${max_attempts}"
      echo '```bash'
      printf '$'
      printf ' %q' timeout "${IPERF_TIMEOUT_SECS}s" "$@"
      echo
    } | tee -a "$OUT"

    set +e
    timeout "${IPERF_TIMEOUT_SECS}s" "$@" 2>&1 | tee -a "$OUT" "$tmp"
    local status=${PIPESTATUS[0]}
    set -e

    {
      echo "exit=$status"
      echo '```'
    } | tee -a "$OUT"

    if grep -q 'server is busy running a test' "$tmp" && ((attempt < max_attempts)); then
      {
        echo
        echo "> iperf3 target is busy; waiting ${IPERF_BUSY_WAIT_SECS}s before retry ${attempt}/${IPERF_BUSY_RETRIES}."
      } | tee -a "$OUT"
      rm -f "$tmp"
      sleep "$IPERF_BUSY_WAIT_SECS"
      attempt=$((attempt + 1))
      continue
    fi

    rm -f "$tmp"
    break
  done
  append_metrics_since "$start_line" "$metrics_title"
}

{
  echo "# 刀14b low-RTT fat-path probe result"
  echo
  echo "- date: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "- target: ${TARGET}:${PORT}"
  echo "- parallel_set: ${PARALLEL_SET}"
  echo "- duration: ${DURATION}s"
  echo "- iperf_timeout: ${IPERF_TIMEOUT_SECS}s"
  echo "- iperf_busy_retries: ${IPERF_BUSY_RETRIES}"
  echo "- iperf_busy_wait_secs: ${IPERF_BUSY_WAIT_SECS}"
  echo "- log: ${LOG}"
  echo
  echo "> 判读前先确认：curl ipinfo.io 必须是 exit IP；dig example.com +short 应是 198.18.x.x；📊 TCP relay 累计应增长。"
} > "$OUT"

append_cleanliness_check

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
