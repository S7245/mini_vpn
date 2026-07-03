#!/usr/bin/env bash
# 刀14b low-RTT fat-path #3 quantify probe.
# Requires an already-running tunnel started with:
#   sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 bash scripts/knife35-acceptance.sh soak
#
# This script drives iperf through an already-running tunnel. Connection-pool behavior is configured
# by the parent mini_vpn process; this probe only controls traffic direction/order.

set -euo pipefail

usage() {
  cat <<'USAGE'
usage: scripts/knife14b-lowrtt-probe.sh <iperf-target> [port]
       scripts/knife14b-lowrtt-probe.sh --self-test

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
  PROBE_ORDER=forward-first
                            forward-first | reverse-first | forward-only | reverse-only
USAGE
}

iperf_receiver_mbps() {
  local iperf_file="$1"
  awk '
    function to_mbps(value, unit) {
      if (unit == "Kbits/sec") {
        return value / 1000
      }
      if (unit == "Mbits/sec") {
        return value
      }
      if (unit == "Gbits/sec") {
        return value * 1000
      }
      return value
    }

    $NF == "receiver" {
      for (i = 1; i < NF; i++) {
        if ($(i + 1) ~ /bits\/sec$/) {
          mbps = to_mbps($i + 0, $(i + 1))
          found = 1
        }
      }
    }

    END {
      if (found) {
        printf "%.3f\n", mbps
      } else {
        print "unknown"
      }
    }
  ' "$iperf_file"
}

summarize_metrics_window() {
  local start_line="$1"
  local title="$2"
  local iperf_file="$3"
  local log_file="$4"
  local receiver_mbps
  receiver_mbps="$(iperf_receiver_mbps "$iperf_file")"

  if [[ ! -f "$log_file" ]]; then
    {
      echo "- iperf_receiver_mbps: $receiver_mbps"
      echo "- metrics_window: log_missing"
      echo "- attribution: no_metrics"
    }
    return
  fi

  tail -n +"$((start_line + 1))" "$log_file" | awk -v title="$title" -v receiver="$receiver_mbps" '
    function numeric_token(token, key, value) {
      value = token
      sub("^" key "=", "", value)
      gsub(/[^0-9]/, "", value)
      if (value == "") {
        return 0
      }
      return value + 0
    }

    function metric_pair_value(text, key, parts, i, kv) {
      gsub(/^[^(]*[(]/, "", text)
      gsub(/[)]$/, "", text)
      split(text, parts, ",")
      for (i in parts) {
        split(parts[i], kv, "=")
        if (kv[1] == key) {
          return kv[2] + 0
        }
      }
      return 0
    }

    function add_unique(list, item) {
      if (item == "") {
        return list
      }
      unique_count = split(list, unique_parts, ",")
      for (unique_idx = 1; unique_idx <= unique_count; unique_idx++) {
        if (unique_parts[unique_idx] == item) {
          return list
        }
      }
      if (list == "") {
        return item
      }
      return list "," item
    }

    function add_label(label) {
      count = split(labels, parts, "[+]")
      for (idx = 1; idx <= count; idx++) {
        if (parts[idx] == label) {
          return
        }
      }
      if (labels == "") {
        labels = label
      } else {
        labels = labels "+" label
      }
    }

    /tuic-open-tcp/ {
      conn = ""
      id = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = numeric_token($i, "conn")
        } else if ($i ~ /^id=/) {
          id = numeric_token($i, "id")
        }
      }
      open_count++
      open_conns = add_unique(open_conns, conn)
    }

    /tuic-tcp-pool-reconnect/ {
      reconnect_count++
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^reason=/) {
          reason = $i
          sub(/^reason=/, "", reason)
          reconnect_reasons = add_unique(reconnect_reasons, reason)
        }
      }
    }

    /tcp-local-write-pressure/ {
      local_write_count++
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^wait_us=/) {
          wait_us = numeric_token($i, "wait_us")
          if (wait_us > max_local_wait_us) {
            max_local_wait_us = wait_us
          }
        } else if ($i ~ /^payload_bytes=/) {
          payload = numeric_token($i, "payload_bytes")
          if (payload > max_local_payload) {
            max_local_payload = payload
          }
        }
      }
    }

    /tcp-global-rx-pressure/ {
      global_rx_count++
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^wait_us=/) {
          wait_us = numeric_token($i, "wait_us")
          if (wait_us > max_global_wait_us) {
            max_global_wait_us = wait_us
          }
        }
      }
    }

    /tcp-downlink-backpressure/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "paused=true") {
          down_pause_count++
        } else if ($i == "paused=false") {
          down_resume_count++
        } else if ($i ~ /^max_pending=/) {
          pending = numeric_token($i, "max_pending")
          if (pending > max_down_pending) {
            max_down_pending = pending
          }
        } else if ($i ~ /^total_pending=/) {
          total_pending = numeric_token($i, "total_pending")
          if (total_pending > max_down_total) {
            max_down_total = total_pending
          }
        }
      }
    }

    /TUIC QUIC stats/ && /cwnd=/ {
      conn = ""
      id = ""
      cwnd = 0
      lost_bytes = 0
      congestion_events = 0
      tx_data = 0
      tx_stream = 0
      rx_data = 0
      rx_stream = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = numeric_token($i, "conn")
        } else if ($i ~ /^id=/) {
          id = numeric_token($i, "id")
        } else if ($i ~ /^cwnd=/) {
          cwnd = numeric_token($i, "cwnd")
        } else if ($i ~ /^lost_bytes=/) {
          lost_bytes = numeric_token($i, "lost_bytes")
        } else if ($i ~ /^congestion_events=/) {
          congestion_events = numeric_token($i, "congestion_events")
        } else if ($i ~ /^tx_blocked[(]/) {
          tx_data = metric_pair_value($i, "data")
          tx_stream = metric_pair_value($i, "stream")
        } else if ($i ~ /^rx_blocked[(]/) {
          rx_data = metric_pair_value($i, "data")
          rx_stream = metric_pair_value($i, "stream")
        }
      }
      if (conn == "" || id == "") {
        next
      }
      key = conn ":" id
      if (!(key in seen_quic)) {
        seen_quic[key] = 1
        quic_keys[++quic_key_count] = key
        first_lost_bytes[key] = lost_bytes
        first_congestion_events[key] = congestion_events
        first_tx_data[key] = tx_data
        first_tx_stream[key] = tx_stream
        first_rx_data[key] = rx_data
        first_rx_stream[key] = rx_stream
      }
      last_lost_bytes[key] = lost_bytes
      last_congestion_events[key] = congestion_events
      last_tx_data[key] = tx_data
      last_tx_stream[key] = tx_stream
      last_rx_data[key] = rx_data
      last_rx_stream[key] = rx_stream
      if (cwnd > 0 && (!(key in min_cwnd) || cwnd < min_cwnd[key])) {
        min_cwnd[key] = cwnd
      }
      quic_samples++
    }

    END {
      min_cwnd_all = ""
      for (i = 1; i <= quic_key_count; i++) {
        key = quic_keys[i]
        baseline_lost = first_lost_bytes[key]
        baseline_cong = first_congestion_events[key]
        baseline_tx_data = first_tx_data[key]
        baseline_tx_stream = first_tx_stream[key]
        baseline_rx_data = first_rx_data[key]
        baseline_rx_stream = first_rx_stream[key]
        lost_delta = last_lost_bytes[key] - baseline_lost
        cong_delta = last_congestion_events[key] - baseline_cong
        tx_data_delta = last_tx_data[key] - baseline_tx_data
        tx_stream_delta = last_tx_stream[key] - baseline_tx_stream
        rx_data_delta = last_rx_data[key] - baseline_rx_data
        rx_stream_delta = last_rx_stream[key] - baseline_rx_stream

        if (lost_delta > max_lost_bytes_delta) {
          max_lost_bytes_delta = lost_delta
          worst_conn = key
        }
        if (cong_delta > max_congestion_delta) {
          max_congestion_delta = cong_delta
          if (worst_conn == "") {
            worst_conn = key
          }
        }
        if (tx_data_delta > max_tx_data_delta) {
          max_tx_data_delta = tx_data_delta
        }
        if (tx_stream_delta > max_tx_stream_delta) {
          max_tx_stream_delta = tx_stream_delta
        }
        if (rx_data_delta > max_rx_data_delta) {
          max_rx_data_delta = rx_data_delta
        }
        if (rx_stream_delta > max_rx_stream_delta) {
          max_rx_stream_delta = rx_stream_delta
        }
        if ((key in min_cwnd) && (min_cwnd_all == "" || min_cwnd[key] < min_cwnd_all)) {
          min_cwnd_all = min_cwnd[key]
        }
      }

      if (max_tx_data_delta > 0 || max_tx_stream_delta > 0) {
        add_label("quic_flow_control")
      }
      if (max_lost_bytes_delta > 0 || max_congestion_delta > 0) {
        add_label("quic_loss_congestion")
      }
      if (local_write_count > 0) {
        add_label("local_write_pressure")
      }
      if (down_pause_count > 0) {
        add_label("local_downlink_backpressure")
      }
      if (global_rx_count > 0) {
        add_label("global_rx_backpressure")
      }
      if (labels == "") {
        labels = "no_pressure_signal"
      }
      if (open_conns == "") {
        open_conns = "none"
      }
      if (reconnect_reasons == "") {
        reconnect_reasons = "none"
      }
      if (worst_conn == "") {
        worst_conn = "none"
      }
      if (min_cwnd_all == "") {
        min_cwnd_all = "n/a"
      }

      print "- metrics_title: " title
      print "- iperf_receiver_mbps: " receiver
      printf "- tcp_pool: opens=%d conns=%s reconnects=%d reasons=%s\n", open_count, open_conns, reconnect_count, reconnect_reasons
      printf "- local_write_pressure: events=%d max_wait_ms=%.3f max_payload_bytes=%d\n", local_write_count, max_local_wait_us / 1000, max_local_payload
      printf "- global_rx_pressure: events=%d max_wait_ms=%.3f\n", global_rx_count, max_global_wait_us / 1000
      printf "- downlink_backpressure: pause_edges=%d resume_edges=%d max_pending_bytes=%d max_total_pending_bytes=%d\n", down_pause_count, down_resume_count, max_down_pending, max_down_total
      printf "- quic: samples=%d worst_conn=%s max_lost_bytes_delta=%d max_congestion_events_delta=%d min_cwnd=%s max_tx_blocked_data_delta=%d max_tx_blocked_stream_delta=%d max_rx_blocked_data_delta=%d max_rx_blocked_stream_delta=%d\n", quic_samples, worst_conn, max_lost_bytes_delta, max_congestion_delta, min_cwnd_all, max_tx_data_delta, max_tx_stream_delta, max_rx_data_delta, max_rx_stream_delta
      print "- attribution: " labels
    }
  '
}

run_self_test() {
  local tmpdir iperf_sample log_sample summary
  tmpdir="$(mktemp -d)"
  iperf_sample="$tmpdir/iperf.txt"
  log_sample="$tmpdir/mvpn.log"
  trap "rm -rf '$tmpdir'" EXIT

  cat > "$iperf_sample" <<'EOF_IPERF'
[  5]   0.00-30.00  sec   712 MBytes   199 Mbits/sec    7             sender
[  5]   0.00-30.04  sec   682 MBytes   191 Mbits/sec                  receiver
EOF_IPERF

  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=90000 lost=0/12 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-local-write-pressure handle=SocketHandle(1) wait_us=3607684 payload_bytes=64960 pressure_events=1
🔎 tcp-downlink-backpressure paused=true max_pending=2151649 total_pending=2151649 high=2097120 low=524280
🔎 tcp-downlink-backpressure paused=false max_pending=524233 total_pending=524233 high=2097120 low=524280
📊 TUIC QUIC stats conn=3 id=99 rtt=0ms cwnd=13068 lost=303/1374 lost_bytes=439956 congestion_events=38 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=1,max_stream_data=2) udp_tx=1372/1977208B udp_rx=332/20762B dg_max=Some(1418) dg_space=1048576B
🔁 tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot
EOF_LOG

  assert_contains() {
    local haystack="$1"
    local needle="$2"
    if [[ "$haystack" != *"$needle"* ]]; then
      echo "lowrtt probe self-test failed: missing '$needle'" >&2
      echo "$haystack" >&2
      exit 1
    fi
  }

  summary="$(summarize_metrics_window 0 "self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_receiver_mbps: 191.000"
  assert_contains "$summary" "tcp_pool: opens=1 conns=3 reconnects=1 reasons=stale_tcp_pool_slot"
  assert_contains "$summary" "local_write_pressure: events=1 max_wait_ms=3607.684"
  assert_contains "$summary" "downlink_backpressure: pause_edges=1 resume_edges=1 max_pending_bytes=2151649"
  assert_contains "$summary" "max_lost_bytes_delta=439956"
  assert_contains "$summary" "attribution: quic_loss_congestion+local_write_pressure+local_downlink_backpressure"

  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=90000 lost=10/120 lost_bytes=1000 congestion_events=5 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=70000 lost=15/180 lost_bytes=1500 congestion_events=8 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "existing-conn-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "max_lost_bytes_delta=500"
  assert_contains "$summary" "max_congestion_events_delta=3"
  echo "lowrtt probe self-test passed"
  rm -rf "$tmpdir"
  trap - EXIT
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${LOWRTT_PROBE_SOURCE_ONLY:-0}" == "1" ]]; then
  return 0 2>/dev/null || exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
  run_self_test
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
PROBE_ORDER="${PROBE_ORDER:-forward-first}"
OUT="${OUT:-/tmp/mvpn_knife14b_lowrtt_$(date +%Y%m%d_%H%M%S).md}"
METRIC_RE='📊 数据面|🔬 主循环|TUIC datagram|UDP relay mode|TCP socket buffers|TUIC QUIC stats|tuic-open-tcp|tuic-tcp-pool-reconnect|tcp-relay-live|tcp-local-write-pressure|tcp-global-rx-pressure|tcp-downlink-backpressure'

case "$PROBE_ORDER" in
  forward-first|reverse-first|forward-only|reverse-only) ;;
  *)
    echo "invalid PROBE_ORDER=$PROBE_ORDER (expected forward-first|reverse-first|forward-only|reverse-only)" >&2
    exit 2
    ;;
esac

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

append_attribution_summary() {
  local start_line="$1"
  local title="$2"
  local iperf_file="$3"

  append_subsection "Attribution Summary: $title"
  summarize_metrics_window "$start_line" "$title" "$iperf_file" "$LOG" | tee -a "$OUT"
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
  local tmp=""
  while ((attempt <= max_attempts)); do
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
      tmp=""
      sleep "$IPERF_BUSY_WAIT_SECS"
      attempt=$((attempt + 1))
      continue
    fi

    break
  done
  append_metrics_since "$start_line" "$metrics_title"
  if [[ -n "$tmp" && -f "$tmp" ]]; then
    append_attribution_summary "$start_line" "$metrics_title" "$tmp"
    rm -f "$tmp"
  fi
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
  echo "- probe_order: ${PROBE_ORDER}"
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

run_tcp_forward_sweep() {
  append_section "TCP Forward Sweep"
  for p in $PARALLEL_SET; do
    append_iperf_cmd "mini_vpn Metrics during TCP Forward P=$p" \
      iperf3 -c "$TARGET" -p "$PORT" -t "$DURATION" -P "$p"
  done
}

run_tcp_reverse_sweep() {
  append_section "TCP Reverse Sweep"
  for p in $PARALLEL_SET; do
    append_iperf_cmd "mini_vpn Metrics during TCP Reverse P=$p" \
      iperf3 -c "$TARGET" -p "$PORT" -t "$DURATION" -P "$p" -R
  done
}

case "$PROBE_ORDER" in
  forward-first)
    run_tcp_forward_sweep
    run_tcp_reverse_sweep
    ;;
  reverse-first)
    run_tcp_reverse_sweep
    run_tcp_forward_sweep
    ;;
  forward-only)
    run_tcp_forward_sweep
    ;;
  reverse-only)
    run_tcp_reverse_sweep
    ;;
esac

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
