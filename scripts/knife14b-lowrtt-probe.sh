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
  POST_IPERF_METRICS_SETTLE_SECS=2
                            seconds to wait for close-tail metrics after iperf exits
  PROBE_ORDER=forward-first
                            forward-first | reverse-first | forward-only | reverse-only
  TUN_IF=tun0              TUN interface to sample for RX/TX dropped deltas
USAGE
}

iperf_role_mbps() {
  local iperf_file="$1"
  local role="$2"
  awk -v role="$role" '
    function to_mbps(value, unit) {
      if (unit == "bits/sec") {
        return value / 1000000
      }
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

    $NF == role {
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

iperf_receiver_mbps() {
  iperf_role_mbps "$1" "receiver"
}

iperf_sender_mbps() {
  iperf_role_mbps "$1" "sender"
}

iperf_is_reverse_tcp() {
  local iperf_file="$1"
  if grep -q '^Reverse mode,' "$iperf_file"; then
    echo 1
  else
    echo 0
  fi
}

is_uint() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

counter_delta() {
  local before="${1:-unknown}"
  local after="${2:-unknown}"
  if is_uint "$before" && is_uint "$after" && ((after >= before)); then
    echo "$((after - before))"
  else
    echo "unknown"
  fi
}

parse_tun_drop_sample() {
  local file="$1"
  local preferred_if="${2:-}"
  awk -v preferred_if="$preferred_if" '
    function clean_iface(raw) {
      sub(/:.*/, "", raw)
      sub(/@.*/, "", raw)
      return raw
    }

    /^[0-9]+:[[:space:]]/ {
      iface = clean_iface($2)
      selected = preferred_if == "" || iface == preferred_if
      have_rx = 0
      have_tx = 0
      next
    }

    selected && /^[[:space:]]*RX:/ {
      if (getline > 0) {
        rx_dropped = $4 + 0
        have_rx = 1
      }
      next
    }

    selected && /^[[:space:]]*TX:/ {
      if (getline > 0) {
        tx_dropped = $4 + 0
        have_tx = 1
      }
      if (have_rx && have_tx) {
        print iface, rx_dropped, tx_dropped
        exit
      }
    }
  ' "$file"
}

discover_tun_if() {
  if [[ -n "${TUN_IF:-}" ]]; then
    echo "$TUN_IF"
    return
  fi
  if ! command -v ip >/dev/null 2>&1; then
    return
  fi
  ip -o -4 addr show 2>/dev/null | awk '$4 ~ /^10[.]0[.]0[.]1\// {print $2; exit}'
}

sample_tun_drops() {
  local tun_if="${1:-}"
  if [[ -z "$tun_if" ]]; then
    tun_if="$(discover_tun_if || true)"
  fi
  if [[ -z "$tun_if" ]]; then
    echo "unknown unknown unknown"
    return
  fi
  if ! command -v ip >/dev/null 2>&1; then
    echo "$tun_if unknown unknown"
    return
  fi

  local tmp parsed
  tmp="$(mktemp)"
  if ip -s link show dev "$tun_if" > "$tmp" 2>/dev/null; then
    parsed="$(parse_tun_drop_sample "$tmp" "$tun_if" || true)"
    rm -f "$tmp"
    if [[ -n "$parsed" ]]; then
      echo "$parsed"
    else
      echo "$tun_if unknown unknown"
    fi
  else
    rm -f "$tmp"
    echo "$tun_if unknown unknown"
  fi
}

summarize_metrics_window() {
  local start_line="$1"
  local title="$2"
  local iperf_file="$3"
  local log_file="$4"
  local probe_kind="${5:-tcp}"
  local tun_if="${6:-unknown}"
  local tun_rx_before="${7:-unknown}"
  local tun_tx_before="${8:-unknown}"
  local tun_rx_after="${9:-unknown}"
  local tun_tx_after="${10:-unknown}"
  local receiver_mbps sender_mbps reverse_tcp tun_rx_delta tun_tx_delta
  receiver_mbps="$(iperf_receiver_mbps "$iperf_file")"
  sender_mbps="$(iperf_sender_mbps "$iperf_file")"
  reverse_tcp="$(iperf_is_reverse_tcp "$iperf_file")"
  tun_rx_delta="$(counter_delta "$tun_rx_before" "$tun_rx_after")"
  tun_tx_delta="$(counter_delta "$tun_tx_before" "$tun_tx_after")"

  if [[ ! -f "$log_file" ]]; then
    {
      echo "- iperf_sender_mbps: $sender_mbps"
      echo "- iperf_receiver_mbps: $receiver_mbps"
      echo "- tun_drops: if=$tun_if tun_rx_dropped_delta=$tun_rx_delta tun_tx_dropped_delta=$tun_tx_delta"
      echo "- metrics_window: log_missing"
      echo "- attribution: no_metrics"
    }
    return
  fi

  tail -n +"$((start_line + 1))" "$log_file" | awk \
    -v title="$title" \
    -v probe_kind="$probe_kind" \
    -v sender="$sender_mbps" \
    -v receiver="$receiver_mbps" \
    -v reverse_tcp="$reverse_tcp" \
    -v tun_if="$tun_if" \
    -v tun_rx_delta="$tun_rx_delta" \
    -v tun_tx_delta="$tun_tx_delta" \
    -v inherited_low_cwnd_limit=65536 \
    -v inherited_lost_bytes_floor=1048576 \
    -v inherited_cong_floor=100 \
    -v slow_rx_floor_ms=5000 \
    -v data_stream_min_rx_bytes=65536 '
    function numeric_token(token, key, value) {
      value = token
      sub("^" key "=", "", value)
      gsub(/[^0-9]/, "", value)
      if (value == "") {
        return 0
      }
      return value + 0
    }

    function digits_token(token, key, value) {
      value = token
      sub("^" key "=", "", value)
      gsub(/[^0-9]/, "", value)
      return value
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

    function remember_relay_handle(handle) {
      if (handle == "") {
        return
      }
      if (!(handle in seen_relay_handle)) {
        seen_relay_handle[handle] = 1
        relay_handles[++relay_handle_count] = handle
      }
    }

    function remember_tuic_stream(key) {
      if (key == "") {
        return
      }
      if (!(key in seen_tuic_stream)) {
        seen_tuic_stream[key] = 1
        tuic_stream_keys[++tuic_stream_key_count] = key
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
        } else if ($i ~ /^max_tx_queue=/) {
          value = numeric_token($i, "max_tx_queue")
          if (value > max_down_tx_queue) {
            max_down_tx_queue = value
          }
        } else if ($i ~ /^total_tx_queue=/) {
          value = numeric_token($i, "total_tx_queue")
          if (value > max_down_total_tx_queue) {
            max_down_total_tx_queue = value
          }
        } else if ($i ~ /^max_pressure=/) {
          value = numeric_token($i, "max_pressure")
          if (value > max_down_pressure) {
            max_down_pressure = value
          }
        } else if ($i ~ /^total_pressure=/) {
          value = numeric_token($i, "total_pressure")
          if (value > max_down_total_pressure) {
            max_down_total_pressure = value
          }
        }
      }
    }

    /tcp-lifecycle-transition/ {
      lifecycle_transitions++
      line_source = ""
      line_state = ""
      line_pending = 0
      line_remote_bytes = 0
      line_terminal = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^source=/) {
          line_source = $i
          sub(/^source=/, "", line_source)
        } else if ($i ~ /^state=/) {
          line_state = $i
          sub(/^state=/, "", line_state)
        } else if ($i ~ /^pending=/) {
          line_pending = numeric_token($i, "pending")
          if (line_pending > max_lifecycle_pending) {
            max_lifecycle_pending = line_pending
          }
        } else if ($i ~ /^remote_to_global_rx_bytes=/) {
          line_remote_bytes = numeric_token($i, "remote_to_global_rx_bytes")
          if (line_remote_bytes > max_lifecycle_remote_bytes) {
            max_lifecycle_remote_bytes = line_remote_bytes
          }
        } else if ($i == "terminal_candidate=true") {
          line_terminal = 1
        }
      }
      if (line_state == "Closed") {
        lifecycle_closed_edges++
      }
      if (line_terminal == 1) {
        lifecycle_terminal_candidates++
      }
      lifecycle_sources = add_unique(lifecycle_sources, line_source)
      lifecycle_states = add_unique(lifecycle_states, line_state)
    }

    /tcp-downlink-flush/ {
      line_send_window_samples = 0
      line_send_capacity_min = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^pending_total=/) {
          value = numeric_token($i, "pending_total")
          if (value > max_flush_pending_total) {
            max_flush_pending_total = value
          }
        } else if ($i ~ /^pending_max=/) {
          value = numeric_token($i, "pending_max")
          if (value > max_flush_pending_max) {
            max_flush_pending_max = value
          }
        } else if ($i ~ /^pending_high=/) {
          value = numeric_token($i, "pending_high")
          if (value > max_flush_pending_high) {
            max_flush_pending_high = value
          }
        } else if ($i ~ /^remote_to_global_rx_bytes=/) {
          value = numeric_token($i, "remote_to_global_rx_bytes")
          if (value > max_flush_remote_bytes) {
            max_flush_remote_bytes = value
          }
        } else if ($i ~ /^flush_attempts=/) {
          value = numeric_token($i, "flush_attempts")
          if (value > max_flush_attempts) {
            max_flush_attempts = value
          }
        } else if ($i ~ /^no_send_capacity=/) {
          value = numeric_token($i, "no_send_capacity")
          if (value > max_no_send_capacity) {
            max_no_send_capacity = value
          }
        } else if ($i ~ /^send_window_samples=/) {
          line_send_window_samples = numeric_token($i, "send_window_samples")
          if (line_send_window_samples > max_send_window_samples) {
            max_send_window_samples = line_send_window_samples
          }
        } else if ($i ~ /^send_capacity_min=/) {
          line_send_capacity_min = numeric_token($i, "send_capacity_min")
        } else if ($i ~ /^send_capacity_max=/) {
          value = numeric_token($i, "send_capacity_max")
          if (value > max_send_capacity_max) {
            max_send_capacity_max = value
          }
        } else if ($i ~ /^send_queue_max=/) {
          value = numeric_token($i, "send_queue_max")
          if (value > max_send_queue_max) {
            max_send_queue_max = value
          }
        } else if ($i ~ /^recv_queue_max=/) {
          value = numeric_token($i, "recv_queue_max")
          if (value > max_recv_queue_max) {
            max_recv_queue_max = value
          }
        } else if ($i ~ /^may_send_false=/) {
          value = numeric_token($i, "may_send_false")
          if (value > max_may_send_false) {
            max_may_send_false = value
          }
        } else if ($i ~ /^may_recv_false=/) {
          value = numeric_token($i, "may_recv_false")
          if (value > max_may_recv_false) {
            max_may_recv_false = value
          }
        } else if ($i ~ /^no_send_capacity_streak_max=/) {
          value = numeric_token($i, "no_send_capacity_streak_max")
          if (value > max_no_send_capacity_streak) {
            max_no_send_capacity_streak = value
          }
        } else if ($i ~ /^no_send_capacity_pending_max=/) {
          value = numeric_token($i, "no_send_capacity_pending_max")
          if (value > max_no_send_capacity_pending) {
            max_no_send_capacity_pending = value
          }
        } else if ($i ~ /^send_slice_calls=/) {
          value = numeric_token($i, "send_slice_calls")
          if (value > max_flush_send_calls) {
            max_flush_send_calls = value
          }
        } else if ($i ~ /^send_slice_accepted=/) {
          value = numeric_token($i, "send_slice_accepted")
          if (value > max_flush_accepted) {
            max_flush_accepted = value
          }
        } else if ($i ~ /^send_slice_zero=/) {
          value = numeric_token($i, "send_slice_zero")
          if (value > max_flush_zero) {
            max_flush_zero = value
          }
        } else if ($i ~ /^send_slice_errors=/) {
          value = numeric_token($i, "send_slice_errors")
          if (value > max_flush_errors) {
            max_flush_errors = value
          }
        } else if ($i ~ /^budget_limited_calls=/) {
          value = numeric_token($i, "budget_limited_calls")
          if (value > max_budget_limited) {
            max_budget_limited = value
          }
        } else if ($i ~ /^send_slice_max_accepted=/) {
          value = numeric_token($i, "send_slice_max_accepted")
          if (value > max_send_slice_max_accepted) {
            max_send_slice_max_accepted = value
          }
        } else if ($i ~ /^tun_flush_tx_calls=/) {
          value = numeric_token($i, "tun_flush_tx_calls")
          if (value > max_tun_flush_calls) {
            max_tun_flush_calls = value
          }
        } else if ($i ~ /^tun_flush_tx_failures=/) {
          value = numeric_token($i, "tun_flush_tx_failures")
          if (value > max_tun_flush_failures) {
            max_tun_flush_failures = value
          }
        } else if ($i ~ /^tun_flush_deferred=/) {
          value = numeric_token($i, "tun_flush_deferred")
          if (value > max_tun_flush_deferred) {
            max_tun_flush_deferred = value
          }
        } else if ($i ~ /^dirty_handles=/) {
          value = numeric_token($i, "dirty_handles")
          if (value > max_dirty_handles) {
            max_dirty_handles = value
          }
        }
      }
      if (line_send_window_samples > 0 && line_send_capacity_min != "") {
        if (min_send_capacity_min == "" || line_send_capacity_min < min_send_capacity_min) {
          min_send_capacity_min = line_send_capacity_min
        }
      }
    }

    /tcp-tun-egress-feedback/ {
      feedback_delta_token = ""
      for (i = 1; i <= NF; i++) {
        if ($i == "paused=true") {
          tun_feedback_pause_count++
        } else if ($i == "paused=false") {
          tun_feedback_resume_count++
        } else if ($i ~ /^tx_dropped_delta=/) {
          feedback_delta_token = $i
          sub(/^tx_dropped_delta=/, "", feedback_delta_token)
        } else if ($i ~ /^max_pressure=/) {
          value = numeric_token($i, "max_pressure")
          if (value > max_tun_feedback_pressure) {
            max_tun_feedback_pressure = value
          }
        }
      }
      if (feedback_delta_token != "" && feedback_delta_token ~ /^[0-9]+$/) {
        feedback_delta = feedback_delta_token + 0
        tun_feedback_drop_delta_total += feedback_delta
        if (feedback_delta > 0) {
          tun_feedback_drop_events++
        }
        if (feedback_delta > max_tun_feedback_delta) {
          max_tun_feedback_delta = feedback_delta
        }
      }
    }

    /tcp-tun-egress[[:space:]]/ {
      runtime_tun_samples++
      runtime_tun_status = ""
      runtime_tun_delta_token = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^status=/) {
          runtime_tun_status = $i
          sub(/^status=/, "", runtime_tun_status)
        } else if ($i ~ /^tx_dropped_delta=/) {
          runtime_tun_delta_token = $i
          sub(/^tx_dropped_delta=/, "", runtime_tun_delta_token)
        }
      }
      if (runtime_tun_status == "reset") {
        runtime_tun_resets++
      } else if (runtime_tun_status ~ /^(no_interface|invalid_interface|read_error|parse_error)$/) {
        runtime_tun_unavailable++
      }
      if (runtime_tun_delta_token != "" && runtime_tun_delta_token ~ /^[0-9]+$/) {
        runtime_tun_delta = runtime_tun_delta_token + 0
        runtime_tun_drop_delta_total += runtime_tun_delta
        if (runtime_tun_delta > 0) {
          runtime_tun_drop_events++
        }
        if (runtime_tun_delta > max_runtime_tun_delta) {
          max_runtime_tun_delta = runtime_tun_delta
        }
      }
    }

    /tcp-tun-rx-drain/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^attempts=/) {
          value = numeric_token($i, "attempts")
          if (value > max_tun_rx_drain_attempts) {
            max_tun_rx_drain_attempts = value
          }
        } else if ($i ~ /^packets=/) {
          value = numeric_token($i, "packets")
          if (value > max_tun_rx_drain_packets) {
            max_tun_rx_drain_packets = value
          }
        } else if ($i ~ /^tcp=/) {
          value = numeric_token($i, "tcp")
          if (value > max_tun_rx_drain_tcp) {
            max_tun_rx_drain_tcp = value
          }
        } else if ($i ~ /^dns=/) {
          value = numeric_token($i, "dns")
          if (value > max_tun_rx_drain_dns) {
            max_tun_rx_drain_dns = value
          }
        } else if ($i ~ /^udp=/) {
          value = numeric_token($i, "udp")
          if (value > max_tun_rx_drain_udp) {
            max_tun_rx_drain_udp = value
          }
        } else if ($i ~ /^budget_exhausted=/) {
          value = numeric_token($i, "budget_exhausted")
          if (value > max_tun_rx_drain_budget_exhausted) {
            max_tun_rx_drain_budget_exhausted = value
          }
        } else if ($i ~ /^would_block=/) {
          value = numeric_token($i, "would_block")
          if (value > max_tun_rx_drain_would_block) {
            max_tun_rx_drain_would_block = value
          }
        } else if ($i ~ /^errors=/) {
          value = numeric_token($i, "errors")
          if (value > max_tun_rx_drain_errors) {
            max_tun_rx_drain_errors = value
          }
        }
      }
    }

    /tcp-handle-close/ {
      pending = 0
      close_pending_bytes_token = ""
      close_pending_class = ""
      terminal_pending = 0
      has_terminal_pending = 0
      closed_state = 0
      active_socket = 0
      inactive_socket = 0
      send_capacity_socket = 0
      no_send_capacity_socket = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^pending=/) {
          pending = numeric_token($i, "pending")
        } else if ($i ~ /^close_pending_bytes=/) {
          close_pending_bytes_token = numeric_token($i, "close_pending_bytes")
        } else if ($i ~ /^close_pending_class=/) {
          close_pending_class = $i
          sub(/^close_pending_class=/, "", close_pending_class)
        } else if ($i ~ /^terminal_pending_reap_bytes=/) {
          terminal_pending = numeric_token($i, "terminal_pending_reap_bytes")
          has_terminal_pending = 1
        } else if ($i == "tcp_state=Closed") {
          closed_state = 1
        } else if ($i == "active=true") {
          active_socket = 1
        } else if ($i == "active=false") {
          inactive_socket = 1
        } else if ($i == "can_send=true") {
          send_capacity_socket = 1
        } else if ($i == "can_send=false") {
          no_send_capacity_socket = 1
        }
      }
      if (pending == 0 && close_pending_bytes_token != "") {
        pending = close_pending_bytes_token
      }
      if (close_pending_class == "") {
        if (pending == 0) {
          close_pending_class = "none"
        } else if (closed_state && inactive_socket && no_send_capacity_socket) {
          close_pending_class = "terminal_closed_no_send"
        } else if (active_socket && no_send_capacity_socket) {
          close_pending_class = "active_no_send"
        } else if (active_socket && send_capacity_socket) {
          close_pending_class = "active_send_capable"
        } else if (inactive_socket && send_capacity_socket) {
          close_pending_class = "inactive_send_capable"
        } else if (inactive_socket && no_send_capacity_socket) {
          close_pending_class = "inactive_no_send"
        } else {
          close_pending_class = "unknown"
        }
      }
      if (!has_terminal_pending && pending > 0 && close_pending_class == "terminal_closed_no_send") {
        terminal_pending = pending
      }
      if (pending > 0) {
        pending_at_close_events++
        pending_at_close_bytes += pending
        if (pending > max_pending_at_close_bytes) {
          max_pending_at_close_bytes = pending
        }
        if (close_pending_class == "active_no_send") {
          pending_close_active_no_send_events++
          pending_close_active_no_send_bytes += pending
        } else if (close_pending_class == "active_send_capable" || close_pending_class == "inactive_send_capable") {
          pending_close_send_capable_events++
          pending_close_send_capable_bytes += pending
        } else if (close_pending_class == "inactive_no_send") {
          pending_close_inactive_no_send_events++
          pending_close_inactive_no_send_bytes += pending
        } else if (close_pending_class == "unknown") {
          pending_close_unknown_events++
          pending_close_unknown_bytes += pending
        }
      }
      if (terminal_pending > 0) {
        terminal_pending_events++
        terminal_pending_bytes += terminal_pending
        if (terminal_pending > max_terminal_pending_bytes) {
          max_terminal_pending_bytes = terminal_pending
        }
      }
    }

    /tcp-relay-write-half-closed/ && /reason=local_finish/ {
      handle = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^handle=/) {
          handle = numeric_token($i, "handle")
        }
      }
      if (handle != "") {
        local_finish_count++
        local_finish_seen[handle] = 1
        before_finish_bytes[handle] = last_remote_bytes[handle] + 0
        before_finish_reads[handle] = last_remote_reads[handle] + 0
      }
    }

    /tcp-relay-live/ || /tcp-relay-close/ {
      handle = ""
      remote_bytes = 0
      remote_reads = 0
      explicit_after_bytes = 0
      explicit_after_reads = 0
      has_explicit_after_bytes = 0
      has_explicit_after_reads = 0
      first_remote_read_ms = 0
      max_remote_read_gap_ms = 0
      current_remote_read_gap_ms = 0
      has_first_remote_read_ms = 0
      has_max_remote_read_gap_ms = 0
      has_current_remote_read_gap_ms = 0
      read_only_after_finish = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^handle=/) {
          handle = numeric_token($i, "handle")
        } else if ($i == "read_only_after_local_finish=true") {
          read_only_after_finish = 1
        } else if ($i ~ /^remote_to_global_rx_bytes=/) {
          remote_bytes = numeric_token($i, "remote_to_global_rx_bytes")
        } else if ($i ~ /^remote_reads=/) {
          remote_reads = numeric_token($i, "remote_reads")
        } else if ($i ~ /^remote_after_local_finish_bytes=/) {
          explicit_after_bytes = numeric_token($i, "remote_after_local_finish_bytes")
          has_explicit_after_bytes = 1
        } else if ($i ~ /^remote_after_local_finish_reads=/) {
          explicit_after_reads = numeric_token($i, "remote_after_local_finish_reads")
          has_explicit_after_reads = 1
        } else if ($i ~ /^first_remote_read_ms=/) {
          first_remote_read_ms = numeric_token($i, "first_remote_read_ms")
          has_first_remote_read_ms = 1
        } else if ($i ~ /^max_remote_read_gap_ms=/) {
          max_remote_read_gap_ms = numeric_token($i, "max_remote_read_gap_ms")
          has_max_remote_read_gap_ms = 1
        } else if ($i ~ /^current_remote_read_gap_ms=/) {
          current_remote_read_gap_ms = numeric_token($i, "current_remote_read_gap_ms")
          has_current_remote_read_gap_ms = 1
        } else if ($i ~ /^global_rx_queue_used_max=/) {
          value = numeric_token($i, "global_rx_queue_used_max")
          if (value > max_global_rx_queue_used) {
            max_global_rx_queue_used = value
          }
        } else if ($i ~ /^global_rx_queue_capacity=/) {
          value = numeric_token($i, "global_rx_queue_capacity")
          if (value > max_global_rx_queue_capacity) {
            max_global_rx_queue_capacity = value
          }
        }
      }
      if (handle == "") {
        next
      }
      remember_relay_handle(handle)
      if (has_first_remote_read_ms && first_remote_read_ms > max_relay_first_remote_read_ms) {
        max_relay_first_remote_read_ms = first_remote_read_ms
      }
      if (has_max_remote_read_gap_ms && max_remote_read_gap_ms > max_relay_remote_read_gap_ms) {
        max_relay_remote_read_gap_ms = max_remote_read_gap_ms
      }
      if (has_current_remote_read_gap_ms && current_remote_read_gap_ms > max_relay_current_remote_gap_ms) {
        max_relay_current_remote_gap_ms = current_remote_read_gap_ms
      }
      if (has_current_remote_read_gap_ms && remote_reads == 0 && current_remote_read_gap_ms > max_relay_no_first_remote_gap_ms) {
        max_relay_no_first_remote_gap_ms = current_remote_read_gap_ms
      }
      if (remote_bytes > relay_remote_bytes[handle]) {
        relay_remote_bytes[handle] = remote_bytes
      }
      if (has_first_remote_read_ms && first_remote_read_ms > relay_first_remote_read_ms[handle]) {
        relay_first_remote_read_ms[handle] = first_remote_read_ms
      }
      if (has_max_remote_read_gap_ms && max_remote_read_gap_ms > relay_remote_read_gap_ms[handle]) {
        relay_remote_read_gap_ms[handle] = max_remote_read_gap_ms
      }

      if (has_explicit_after_bytes) {
        late_bytes = explicit_after_bytes
      } else if (local_finish_seen[handle] || read_only_after_finish) {
        late_bytes = remote_bytes - before_finish_bytes[handle]
      } else {
        late_bytes = 0
      }
      if (has_explicit_after_reads) {
        late_reads = explicit_after_reads
      } else if (local_finish_seen[handle] || read_only_after_finish) {
        late_reads = remote_reads - before_finish_reads[handle]
      } else {
        late_reads = 0
      }
      if (late_bytes < 0) {
        late_bytes = 0
      }
      if (late_reads < 0) {
        late_reads = 0
      }
      if (late_bytes > max_late_remote_bytes) {
        max_late_remote_bytes = late_bytes
      }
      if (late_reads > max_late_remote_reads) {
        max_late_remote_reads = late_reads
      }
      if (!(handle in last_remote_bytes) || remote_bytes >= last_remote_bytes[handle]) {
        last_remote_bytes[handle] = remote_bytes
        last_remote_reads[handle] = remote_reads
      }
    }

    /tuic-tcp-stream-first-rx/ {
      tuic_first_rx_events++
      has_conn = 0
      has_id = 0
      has_stream = 0
      conn = ""
      id = ""
      stream = ""
      first_rx_ms = 0
      has_first_rx_ms = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = digits_token($i, "conn")
          has_conn = 1
        } else if ($i ~ /^id=/) {
          id = digits_token($i, "id")
          has_id = 1
        } else if ($i ~ /^stream=/) {
          stream = digits_token($i, "stream")
          has_stream = 1
        } else if ($i ~ /^first_rx_ms=/) {
          first_rx_ms = numeric_token($i, "first_rx_ms")
          has_first_rx_ms = 1
          value = first_rx_ms
          if (value > max_tuic_first_rx_ms) {
            max_tuic_first_rx_ms = value
          }
        }
      }
      if (has_conn && has_id && has_stream && has_first_rx_ms) {
        key = conn ":" id ":" stream
        remember_tuic_stream(key)
        if (first_rx_ms > tuic_stream_first_rx_ms[key]) {
          tuic_stream_first_rx_ms[key] = first_rx_ms
        }
      }
    }

    /tuic-tcp-stream-read-gap/ {
      tuic_read_gap_events++
      has_conn = 0
      has_id = 0
      has_stream = 0
      conn = ""
      id = ""
      stream = ""
      read_gap_ms = 0
      read_gap_rx_bytes = 0
      has_read_gap_ms = 0
      has_read_gap_rx_bytes = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = digits_token($i, "conn")
          has_conn = 1
        } else if ($i ~ /^id=/) {
          id = digits_token($i, "id")
          has_id = 1
        } else if ($i ~ /^stream=/) {
          stream = digits_token($i, "stream")
          has_stream = 1
        } else if ($i ~ /^gap_ms=/) {
          read_gap_ms = numeric_token($i, "gap_ms")
          has_read_gap_ms = 1
          value = read_gap_ms
          if (value > max_tuic_read_gap_ms) {
            max_tuic_read_gap_ms = value
          }
        } else if ($i ~ /^rx_bytes=/) {
          read_gap_rx_bytes = numeric_token($i, "rx_bytes")
          has_read_gap_rx_bytes = 1
        }
      }
      if (has_conn && has_id && has_stream) {
        key = conn ":" id ":" stream
        remember_tuic_stream(key)
        if (has_read_gap_ms && read_gap_ms > tuic_stream_read_gap_ms[key]) {
          tuic_stream_read_gap_ms[key] = read_gap_ms
        }
        if (has_read_gap_rx_bytes && read_gap_rx_bytes > tuic_stream_observed_rx_bytes[key]) {
          tuic_stream_observed_rx_bytes[key] = read_gap_rx_bytes
        }
      }
    }

    /tuic-tcp-stream-pending/ {
      tuic_pending_events++
      has_conn = 0
      has_id = 0
      has_stream = 0
      conn = ""
      id = ""
      stream = ""
      pending_gap_ms = 0
      pending_polls = 0
      poll_count = 0
      poll_gap_ms = 0
      pending_rx_bytes = 0
      has_pending_gap_ms = 0
      has_pending_polls = 0
      has_poll_count = 0
      has_poll_gap_ms = 0
      has_pending_rx_bytes = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = digits_token($i, "conn")
          has_conn = 1
        } else if ($i ~ /^id=/) {
          id = digits_token($i, "id")
          has_id = 1
        } else if ($i ~ /^stream=/) {
          stream = digits_token($i, "stream")
          has_stream = 1
        } else if ($i ~ /^pending_gap_ms=/) {
          pending_gap_ms = numeric_token($i, "pending_gap_ms")
          has_pending_gap_ms = 1
          if (pending_gap_ms > max_tuic_pending_gap_ms) {
            max_tuic_pending_gap_ms = pending_gap_ms
          }
        } else if ($i ~ /^pending_polls=/) {
          pending_polls = numeric_token($i, "pending_polls")
          has_pending_polls = 1
          if (pending_polls > max_tuic_pending_polls) {
            max_tuic_pending_polls = pending_polls
          }
        } else if ($i ~ /^polls=/) {
          poll_count = numeric_token($i, "polls")
          has_poll_count = 1
          if (poll_count > max_tuic_polls) {
            max_tuic_polls = poll_count
          }
        } else if ($i ~ /^max_poll_gap_ms=/) {
          poll_gap_ms = numeric_token($i, "max_poll_gap_ms")
          has_poll_gap_ms = 1
          if (poll_gap_ms > max_tuic_poll_gap_ms) {
            max_tuic_poll_gap_ms = poll_gap_ms
          }
        } else if ($i ~ /^rx_bytes=/) {
          pending_rx_bytes = numeric_token($i, "rx_bytes")
          has_pending_rx_bytes = 1
        }
      }
      if (has_conn && has_id && has_stream) {
        key = conn ":" id ":" stream
        remember_tuic_stream(key)
        if (has_pending_gap_ms && pending_gap_ms > tuic_stream_pending_gap_ms[key]) {
          tuic_stream_pending_gap_ms[key] = pending_gap_ms
        }
        if (has_pending_polls && pending_polls > tuic_stream_pending_polls[key]) {
          tuic_stream_pending_polls[key] = pending_polls
        }
        if (has_poll_count && poll_count > tuic_stream_polls[key]) {
          tuic_stream_polls[key] = poll_count
        }
        if (has_poll_gap_ms && poll_gap_ms > tuic_stream_poll_gap_ms[key]) {
          tuic_stream_poll_gap_ms[key] = poll_gap_ms
        }
        if (has_pending_rx_bytes && pending_rx_bytes > tuic_stream_observed_rx_bytes[key]) {
          tuic_stream_observed_rx_bytes[key] = pending_rx_bytes
        }
      }
    }

    /tuic-tcp-stream-close/ {
      tuic_stream_close_events++
      close_rx_bytes = 0
      has_conn = 0
      has_id = 0
      has_stream = 0
      conn = ""
      id = ""
      stream = ""
      close_first_rx_ms = 0
      close_gap_ms = 0
      close_pending_polls = 0
      close_pending_gap_ms = 0
      close_poll_count = 0
      close_poll_gap_ms = 0
      has_close_first_rx_ms = 0
      has_close_gap_ms = 0
      has_close_pending_polls = 0
      has_close_pending_gap_ms = 0
      has_close_poll_count = 0
      has_close_poll_gap_ms = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = digits_token($i, "conn")
          has_conn = 1
        } else if ($i ~ /^id=/) {
          id = digits_token($i, "id")
          has_id = 1
        } else if ($i ~ /^stream=/) {
          stream = digits_token($i, "stream")
          has_stream = 1
        } else if ($i ~ /^first_rx_ms=/) {
          close_first_rx_ms = numeric_token($i, "first_rx_ms")
          has_close_first_rx_ms = 1
          value = close_first_rx_ms
          if (value > max_tuic_close_first_rx_ms) {
            max_tuic_close_first_rx_ms = value
          }
        } else if ($i ~ /^max_read_gap_ms=/) {
          close_gap_ms = numeric_token($i, "max_read_gap_ms")
          has_close_gap_ms = 1
          if (close_gap_ms > max_tuic_close_gap_ms) {
            max_tuic_close_gap_ms = close_gap_ms
          }
        } else if ($i ~ /^rx_bytes=/) {
          close_rx_bytes = numeric_token($i, "rx_bytes")
          if (close_rx_bytes > max_tuic_close_rx_bytes) {
            max_tuic_close_rx_bytes = close_rx_bytes
          }
        } else if ($i ~ /^reads=/) {
          value = numeric_token($i, "reads")
          if (value > max_tuic_close_reads) {
            max_tuic_close_reads = value
          }
        } else if ($i ~ /^pending_polls=/) {
          close_pending_polls = numeric_token($i, "pending_polls")
          has_close_pending_polls = 1
          if (close_pending_polls > max_tuic_pending_polls) {
            max_tuic_pending_polls = close_pending_polls
          }
        } else if ($i ~ /^max_pending_gap_ms=/) {
          close_pending_gap_ms = numeric_token($i, "max_pending_gap_ms")
          has_close_pending_gap_ms = 1
          if (close_pending_gap_ms > max_tuic_pending_gap_ms) {
            max_tuic_pending_gap_ms = close_pending_gap_ms
          }
        } else if ($i ~ /^polls=/) {
          close_poll_count = numeric_token($i, "polls")
          has_close_poll_count = 1
          if (close_poll_count > max_tuic_polls) {
            max_tuic_polls = close_poll_count
          }
        } else if ($i ~ /^max_poll_gap_ms=/) {
          close_poll_gap_ms = numeric_token($i, "max_poll_gap_ms")
          has_close_poll_gap_ms = 1
          if (close_poll_gap_ms > max_tuic_poll_gap_ms) {
            max_tuic_poll_gap_ms = close_poll_gap_ms
          }
        }
      }
      if (close_rx_bytes == 0) {
        tuic_zero_rx_closes++
      }
      if (has_conn && has_id && has_stream) {
        key = conn ":" id ":" stream
        remember_tuic_stream(key)
        if (close_rx_bytes > tuic_stream_close_rx_bytes[key]) {
          tuic_stream_close_rx_bytes[key] = close_rx_bytes
        }
        if (close_rx_bytes > tuic_stream_observed_rx_bytes[key]) {
          tuic_stream_observed_rx_bytes[key] = close_rx_bytes
        }
        if (has_close_first_rx_ms && close_first_rx_ms > tuic_stream_first_rx_ms[key]) {
          tuic_stream_first_rx_ms[key] = close_first_rx_ms
        }
        if (has_close_gap_ms && close_gap_ms > tuic_stream_close_gap_ms[key]) {
          tuic_stream_close_gap_ms[key] = close_gap_ms
        }
        if (has_close_pending_polls && close_pending_polls > tuic_stream_pending_polls[key]) {
          tuic_stream_pending_polls[key] = close_pending_polls
        }
        if (has_close_pending_gap_ms && close_pending_gap_ms > tuic_stream_pending_gap_ms[key]) {
          tuic_stream_pending_gap_ms[key] = close_pending_gap_ms
        }
        if (has_close_poll_count && close_poll_count > tuic_stream_polls[key]) {
          tuic_stream_polls[key] = close_poll_count
        }
        if (has_close_poll_gap_ms && close_poll_gap_ms > tuic_stream_poll_gap_ms[key]) {
          tuic_stream_poll_gap_ms[key] = close_poll_gap_ms
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
        first_cwnd[key] = cwnd
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
      min_start_cwnd_all = ""
      for (i = 1; i <= quic_key_count; i++) {
        key = quic_keys[i]
        baseline_cwnd = first_cwnd[key]
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

        if (baseline_lost > max_start_lost_bytes) {
          max_start_lost_bytes = baseline_lost
        }
        if (baseline_cong > max_start_congestion_events) {
          max_start_congestion_events = baseline_cong
        }
        if (baseline_cwnd > 0 && (min_start_cwnd_all == "" || baseline_cwnd < min_start_cwnd_all)) {
          min_start_cwnd_all = baseline_cwnd
        }
        if (baseline_cwnd > 0 &&
            baseline_cwnd <= inherited_low_cwnd_limit &&
            (baseline_lost >= inherited_lost_bytes_floor || baseline_cong >= inherited_cong_floor)) {
          inherited_quic_congestion = 1
          inherited_quic_conns = add_unique(inherited_quic_conns, key)
        }

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
      if (inherited_quic_congestion > 0) {
        add_label("inherited_quic_congestion")
      }
      if (local_write_count > 0) {
        add_label("local_write_pressure")
      }
      if (tun_tx_delta != "unknown" && (tun_tx_delta + 0) > 0) {
        add_label("local_tun_egress_drop")
      }
      if (runtime_tun_drop_delta_total > 0) {
        add_label("local_tun_egress_drop")
      }
      if (tun_feedback_pause_count > 0) {
        add_label("local_tun_egress_feedback")
      }
      if (down_pause_count > 0) {
        add_label("local_downlink_backpressure")
      }
      if (global_rx_count > 0) {
        add_label("global_rx_backpressure")
      }
      if (max_late_remote_bytes > 0 || max_late_remote_reads > 0) {
        add_label("late_remote_after_local_finish")
      }
      if (terminal_pending_events > 0) {
        add_label("terminal_pending_reap")
      }
      if (pending_at_close_events > 0) {
        add_label("pending_at_close")
      }
      if (pending_close_active_no_send_events > 0) {
        add_label("pending_close_active_no_send")
      }
      if (pending_close_send_capable_events > 0) {
        add_label("pending_close_send_capable")
      }
      if (pending_close_inactive_no_send_events > 0) {
        add_label("pending_close_inactive_no_send")
      }
      if (pending_close_unknown_events > 0) {
        add_label("pending_close_unknown")
      }
      if (lifecycle_terminal_candidates > 0) {
        add_label("local_tcp_terminal_transition")
      }
      for (i = 1; i <= relay_handle_count; i++) {
        handle = relay_handles[i]
        if (relay_remote_bytes[handle] >= data_stream_min_rx_bytes) {
          relay_data_streams++
          if (relay_remote_bytes[handle] > max_relay_data_remote_bytes) {
            max_relay_data_remote_bytes = relay_remote_bytes[handle]
          }
          if (relay_first_remote_read_ms[handle] > max_relay_data_first_remote_read_ms) {
            max_relay_data_first_remote_read_ms = relay_first_remote_read_ms[handle]
          }
          if (relay_remote_read_gap_ms[handle] > max_relay_data_remote_read_gap_ms) {
            max_relay_data_remote_read_gap_ms = relay_remote_read_gap_ms[handle]
          }
        }
      }
      for (i = 1; i <= tuic_stream_key_count; i++) {
        key = tuic_stream_keys[i]
        stream_rx_bytes = tuic_stream_observed_rx_bytes[key]
        if (stream_rx_bytes >= data_stream_min_rx_bytes) {
          tuic_data_streams++
          if (stream_rx_bytes > max_tuic_data_rx_bytes) {
            max_tuic_data_rx_bytes = stream_rx_bytes
          }
          if (tuic_stream_first_rx_ms[key] > max_tuic_data_first_rx_ms) {
            max_tuic_data_first_rx_ms = tuic_stream_first_rx_ms[key]
          }
          if (tuic_stream_read_gap_ms[key] > max_tuic_data_read_gap_ms) {
            max_tuic_data_read_gap_ms = tuic_stream_read_gap_ms[key]
          }
          if (tuic_stream_close_gap_ms[key] > max_tuic_data_close_gap_ms) {
            max_tuic_data_close_gap_ms = tuic_stream_close_gap_ms[key]
          }
          if (tuic_stream_pending_gap_ms[key] > max_tuic_data_pending_gap_ms) {
            max_tuic_data_pending_gap_ms = tuic_stream_pending_gap_ms[key]
          }
          if (tuic_stream_polls[key] > max_tuic_data_polls) {
            max_tuic_data_polls = tuic_stream_polls[key]
          }
          if (tuic_stream_poll_gap_ms[key] > max_tuic_data_poll_gap_ms) {
            max_tuic_data_poll_gap_ms = tuic_stream_poll_gap_ms[key]
          }
        }
      }
      remote_timing_slow = 0
      if (probe_kind == "tcp" && reverse_tcp == "1") {
        if (max_tuic_data_first_rx_ms >= slow_rx_floor_ms) {
          add_label("tuic_stream_first_byte_slow")
          remote_timing_slow = 1
        }
        if (max_tuic_data_read_gap_ms >= slow_rx_floor_ms ||
            max_tuic_data_close_gap_ms >= slow_rx_floor_ms) {
          add_label("tuic_stream_read_gap")
          remote_timing_slow = 1
        }
        if (max_tuic_data_pending_gap_ms >= slow_rx_floor_ms) {
          add_label("tuic_stream_read_pending")
          remote_timing_slow = 1
        }
        if (max_relay_data_first_remote_read_ms >= slow_rx_floor_ms ||
            max_relay_no_first_remote_gap_ms >= slow_rx_floor_ms) {
          add_label("relay_remote_first_byte_slow")
          remote_timing_slow = 1
        }
        if (max_relay_data_remote_read_gap_ms >= slow_rx_floor_ms) {
          add_label("relay_remote_read_gap")
          remote_timing_slow = 1
        }
      }
      if (probe_kind == "tcp" && reverse_tcp == "1" &&
          sender != "unknown" && receiver != "unknown" &&
          (sender + 0) < 5 && (receiver + 0) < 5 &&
          local_write_count == 0 && down_pause_count == 0 && global_rx_count == 0 &&
          max_lost_bytes_delta == 0 && max_congestion_delta == 0 &&
          max_tx_data_delta == 0 && max_tx_stream_delta == 0 &&
          inherited_quic_congestion == 0 &&
          reconnect_count == 0 &&
          remote_timing_slow == 0) {
        add_label("reverse_sender_backpressured")
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
      if (min_start_cwnd_all == "") {
        min_start_cwnd_all = "n/a"
      }
      if (inherited_quic_conns == "") {
        inherited_quic_conns = "none"
      }
      if (lifecycle_sources == "") {
        lifecycle_sources = "none"
      }
      if (lifecycle_states == "") {
        lifecycle_states = "none"
      }
      if (min_send_capacity_min == "") {
        min_send_capacity_min = 0
      }

      print "- metrics_title: " title
      print "- iperf_sender_mbps: " sender
      print "- iperf_receiver_mbps: " receiver
      printf "- tcp_pool: opens=%d conns=%s reconnects=%d reasons=%s\n", open_count, open_conns, reconnect_count, reconnect_reasons
      printf "- local_write_pressure: events=%d max_wait_ms=%.3f max_payload_bytes=%d\n", local_write_count, max_local_wait_us / 1000, max_local_payload
      printf "- global_rx_pressure: events=%d max_wait_ms=%.3f queue_used_max=%d queue_capacity=%d\n", global_rx_count, max_global_wait_us / 1000, max_global_rx_queue_used, max_global_rx_queue_capacity
      printf "- downlink_backpressure: pause_edges=%d resume_edges=%d max_pending_bytes=%d max_total_pending_bytes=%d max_tx_queue_bytes=%d max_total_tx_queue_bytes=%d max_pressure_bytes=%d max_total_pressure_bytes=%d\n", down_pause_count, down_resume_count, max_down_pending, max_down_total, max_down_tx_queue, max_down_total_tx_queue, max_down_pressure, max_down_total_pressure
      printf "- downlink_flush: attempts=%d no_send_capacity=%d send_window_samples=%d send_capacity_min=%d send_capacity_max=%d send_queue_max=%d recv_queue_max=%d may_send_false=%d may_recv_false=%d no_send_streak_max=%d no_send_pending_max=%d send_slice_calls=%d accepted_bytes=%d zero=%d errors=%d budget_limited=%d max_accepted_bytes=%d tun_flush_calls=%d tun_flush_failures=%d tun_flush_deferred=%d pending_total_max=%d pending_max=%d pending_high=%d remote_to_global_rx_bytes=%d dirty_handles_max=%d\n", max_flush_attempts, max_no_send_capacity, max_send_window_samples, min_send_capacity_min, max_send_capacity_max, max_send_queue_max, max_recv_queue_max, max_may_send_false, max_may_recv_false, max_no_send_capacity_streak, max_no_send_capacity_pending, max_flush_send_calls, max_flush_accepted, max_flush_zero, max_flush_errors, max_budget_limited, max_send_slice_max_accepted, max_tun_flush_calls, max_tun_flush_failures, max_tun_flush_deferred, max_flush_pending_total, max_flush_pending_max, max_flush_pending_high, max_flush_remote_bytes, max_dirty_handles
      printf "- tcp_lifecycle: transitions=%d closed_edges=%d terminal_candidates=%d max_pending_bytes=%d max_remote_to_global_rx_bytes=%d sources=%s states=%s\n", lifecycle_transitions, lifecycle_closed_edges, lifecycle_terminal_candidates, max_lifecycle_pending, max_lifecycle_remote_bytes, lifecycle_sources, lifecycle_states
      printf "- terminal_pending_reap: events=%d bytes=%d max_bytes=%d\n", terminal_pending_events, terminal_pending_bytes, max_terminal_pending_bytes
      printf "- pending_at_close: events=%d bytes=%d max_bytes=%d terminal_events=%d terminal_bytes=%d active_no_send_events=%d active_no_send_bytes=%d send_capable_events=%d send_capable_bytes=%d inactive_no_send_events=%d inactive_no_send_bytes=%d unknown_events=%d unknown_bytes=%d\n", pending_at_close_events, pending_at_close_bytes, max_pending_at_close_bytes, terminal_pending_events, terminal_pending_bytes, pending_close_active_no_send_events, pending_close_active_no_send_bytes, pending_close_send_capable_events, pending_close_send_capable_bytes, pending_close_inactive_no_send_events, pending_close_inactive_no_send_bytes, pending_close_unknown_events, pending_close_unknown_bytes
      printf "- relay_late_remote: post_finish_bytes=%d post_finish_reads=%d local_finish_events=%d\n", max_late_remote_bytes, max_late_remote_reads, local_finish_count
      printf "- relay_remote_timing: first_read_max_ms=%d max_read_gap_ms=%d current_gap_max_ms=%d no_first_read_gap_max_ms=%d data_streams=%d data_first_read_max_ms=%d data_max_read_gap_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", max_relay_first_remote_read_ms, max_relay_remote_read_gap_ms, max_relay_current_remote_gap_ms, max_relay_no_first_remote_gap_ms, relay_data_streams, max_relay_data_first_remote_read_ms, max_relay_data_remote_read_gap_ms, max_relay_data_remote_bytes, data_stream_min_rx_bytes
      printf "- tuic_tcp_stream: first_rx_events=%d first_rx_max_ms=%d read_gap_events=%d read_gap_max_ms=%d close_events=%d close_first_rx_max_ms=%d close_gap_max_ms=%d rx_bytes_max=%d reads_max=%d zero_rx_closes=%d data_streams=%d data_first_rx_max_ms=%d data_read_gap_max_ms=%d data_close_gap_max_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", tuic_first_rx_events, max_tuic_first_rx_ms, tuic_read_gap_events, max_tuic_read_gap_ms, tuic_stream_close_events, max_tuic_close_first_rx_ms, max_tuic_close_gap_ms, max_tuic_close_rx_bytes, max_tuic_close_reads, tuic_zero_rx_closes, tuic_data_streams, max_tuic_data_first_rx_ms, max_tuic_data_read_gap_ms, max_tuic_data_close_gap_ms, max_tuic_data_rx_bytes, data_stream_min_rx_bytes
      printf "- tuic_stream_pending: events=%d max_pending_gap_ms=%d pending_polls_max=%d data_streams=%d data_pending_gap_max_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", tuic_pending_events, max_tuic_pending_gap_ms, max_tuic_pending_polls, tuic_data_streams, max_tuic_data_pending_gap_ms, max_tuic_data_rx_bytes, data_stream_min_rx_bytes
      printf "- tuic_stream_polling: polls_max=%d max_poll_gap_ms=%d data_streams=%d data_polls_max=%d data_poll_gap_max_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", max_tuic_polls, max_tuic_poll_gap_ms, tuic_data_streams, max_tuic_data_polls, max_tuic_data_poll_gap_ms, max_tuic_data_rx_bytes, data_stream_min_rx_bytes
      printf "- tun_drops: if=%s tun_rx_dropped_delta=%s tun_tx_dropped_delta=%s\n", tun_if, tun_rx_delta, tun_tx_delta
      printf "- runtime_tun_egress: samples=%d drop_events=%d drop_delta_total=%d max_delta=%d unavailable=%d resets=%d\n", runtime_tun_samples, runtime_tun_drop_events, runtime_tun_drop_delta_total, max_runtime_tun_delta, runtime_tun_unavailable, runtime_tun_resets
      printf "- tun_egress_feedback: pause_edges=%d resume_edges=%d drop_events=%d drop_delta_total=%d max_delta=%d max_pressure_bytes=%d\n", tun_feedback_pause_count, tun_feedback_resume_count, tun_feedback_drop_events, tun_feedback_drop_delta_total, max_tun_feedback_delta, max_tun_feedback_pressure
      printf "- tun_rx_drain: attempts=%d packets=%d tcp=%d dns=%d udp=%d budget_exhausted=%d would_block=%d errors=%d\n", max_tun_rx_drain_attempts, max_tun_rx_drain_packets, max_tun_rx_drain_tcp, max_tun_rx_drain_dns, max_tun_rx_drain_udp, max_tun_rx_drain_budget_exhausted, max_tun_rx_drain_would_block, max_tun_rx_drain_errors
      printf "- quic: samples=%d worst_conn=%s max_lost_bytes_delta=%d max_congestion_events_delta=%d max_start_lost_bytes=%d max_start_congestion_events=%d min_start_cwnd=%s min_cwnd=%s inherited_conns=%s inherited_low_cwnd_threshold=%d max_tx_blocked_data_delta=%d max_tx_blocked_stream_delta=%d max_rx_blocked_data_delta=%d max_rx_blocked_stream_delta=%d\n", quic_samples, worst_conn, max_lost_bytes_delta, max_congestion_delta, max_start_lost_bytes, max_start_congestion_events, min_start_cwnd_all, min_cwnd_all, inherited_quic_conns, inherited_low_cwnd_limit, max_tx_data_delta, max_tx_stream_delta, max_rx_data_delta, max_rx_stream_delta
      print "- attribution: " labels
    }
  '
}

wait_for_post_iperf_metrics_settle() {
  local title="$1"
  local settle_secs="${POST_IPERF_METRICS_SETTLE_SECS:-2}"

  if ! is_uint "$settle_secs"; then
    echo "invalid POST_IPERF_METRICS_SETTLE_SECS=$settle_secs (expected non-negative integer seconds)" >&2
    exit 2
  fi
  if (( settle_secs == 0 )); then
    return
  fi

  if [[ -n "${OUT:-}" ]]; then
    echo "> waiting ${settle_secs}s for post-iperf close-tail metrics before summarizing: ${title}" | tee -a "$OUT"
  fi
  sleep "$settle_secs"
}

run_self_test() {
  local tmpdir iperf_sample log_sample tun_before tun_after summary
  local tun_if_before tun_rx_before tun_tx_before tun_if_after tun_rx_after tun_tx_after
  tmpdir="$(mktemp -d)"
  iperf_sample="$tmpdir/iperf.txt"
  log_sample="$tmpdir/mvpn.log"
  tun_before="$tmpdir/tun-before.txt"
  tun_after="$tmpdir/tun-after.txt"
  trap "rm -rf '$tmpdir'" EXIT

  cat > "$iperf_sample" <<'EOF_IPERF'
[  5]   0.00-30.00  sec   712 MBytes   199 Mbits/sec    7             sender
[  5]   0.00-30.04  sec   682 MBytes   191 Mbits/sec                  receiver
EOF_IPERF

  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=90000 lost=0/12 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-local-write-pressure handle=SocketHandle(1) wait_us=3607684 payload_bytes=64960 pressure_events=1
🔎 tcp-downlink-backpressure paused=true max_pending=2151649 total_pending=2151649 max_tx_queue=1048576 total_tx_queue=1048576 max_pressure=2151649 total_pressure=3200225 high=2097120 low=524280
🔎 tcp-downlink-backpressure paused=false max_pending=524233 total_pending=524233 max_tx_queue=524280 total_tx_queue=524280 max_pressure=524280 total_pressure=1048513 high=2097120 low=524280
🔎 tcp-downlink-flush pending_total=524233 pending_max=524233 pending_high=2151649 remote_to_global_rx_bytes=3145728 flush_attempts=23 no_send_capacity=7 send_window_samples=23 send_capacity_min=0 send_capacity_max=1048576 send_queue_max=1048576 recv_queue_max=4096 may_send_false=1 may_recv_false=1 no_send_capacity_streak_max=7 no_send_capacity_pending_max=524233 send_slice_calls=16 send_slice_accepted=2621440 send_slice_zero=2 send_slice_errors=0 budget_limited_calls=9 send_slice_max_accepted=262144 tun_flush_tx_calls=14 tun_flush_tx_failures=1 tun_flush_deferred=3 dirty_handles=1
🔎 tcp-tun-rx-drain attempts=4 packets=11 tcp=9 dns=1 udp=1 budget_exhausted=1 would_block=3 errors=0
🔎 tcp-tun-egress if=tun0 status=delta tx_dropped_total=1523 tx_dropped_delta=1423 global_rx_paused=true pending_total=524233 pending_max=524233 pending_high=2151649 remote_to_global_rx_bytes=3145728 tun_flush_tx_calls=14 dirty_handles=1
🔎 tcp-lifecycle-transition handle=SocketHandle(1) source=dead_slot_reap prev_source=remote_payload prev_observed_secs=10 observed_secs=15 prev_state=Established state=Closed prev_active=true active=false prev_can_send=true can_send=false prev_can_recv=true can_recv=false prev_may_send=true may_send=false prev_may_recv=true may_recv=false prev_send_queue=65536 send_queue=0 prev_recv_queue=0 recv_queue=0 ctx_state=Relaying uplink_tx=true local_fin_sent=false local_fin_pending_since=none local_fin_last_remote_progress=none pending=4096 pending_high=2151649 remote_to_global_rx_bytes=3145728 send_slice_accepted=2621440 flush_attempts=23 terminal_candidate=true
🔎 tcp-handle-close handle=SocketHandle(1) direction=local reason=dead_slot_reap state=Relaying pending=4096 pending_high=2151649 remote_to_global_rx_bytes=3145728 flush_attempts=23 no_send_capacity=7 send_slice_calls=16 send_slice_accepted=2621440 send_slice_zero=2 send_slice_errors=0 budget_limited_calls=9 send_slice_max_accepted=262144 tun_flush_tx_calls=14 tun_flush_tx_failures=1 tun_flush_deferred=3 close_pending_class=terminal_closed_no_send close_pending_bytes=4096 terminal_pending_reap_bytes=4096 tcp_state=Closed active=false can_send=false can_recv=false
🔎 tcp-handle-close handle=SocketHandle(2) direction=local_to_remote reason=uplink_channel_closed state=Relaying pending=8192 pending_high=589159 remote_to_global_rx_bytes=692415971 flush_attempts=42 no_send_capacity=11 send_slice_calls=31 send_slice_accepted=691826812 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=17 send_slice_max_accepted=262144 tun_flush_tx_calls=29 tun_flush_tx_failures=0 tun_flush_deferred=0 close_pending_class=active_no_send close_pending_bytes=8192 terminal_pending_reap_bytes=0 tcp_state=Established active=true can_send=false can_recv=false
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

  assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    if [[ "$haystack" == *"$needle"* ]]; then
      echo "lowrtt probe self-test failed: unexpectedly found '$needle'" >&2
      echo "$haystack" >&2
      exit 1
    fi
  }

  summary="$(summarize_metrics_window 0 "self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_receiver_mbps: 191.000"
  assert_contains "$summary" "tcp_pool: opens=1 conns=3 reconnects=1 reasons=stale_tcp_pool_slot"
  assert_contains "$summary" "local_write_pressure: events=1 max_wait_ms=3607.684"
  assert_contains "$summary" "global_rx_pressure: events=0 max_wait_ms=0.000 queue_used_max=0 queue_capacity=0"
  assert_contains "$summary" "downlink_backpressure: pause_edges=1 resume_edges=1 max_pending_bytes=2151649 max_total_pending_bytes=2151649 max_tx_queue_bytes=1048576 max_total_tx_queue_bytes=1048576 max_pressure_bytes=2151649 max_total_pressure_bytes=3200225"
  assert_contains "$summary" "downlink_flush: attempts=23 no_send_capacity=7 send_window_samples=23 send_capacity_min=0 send_capacity_max=1048576 send_queue_max=1048576 recv_queue_max=4096 may_send_false=1 may_recv_false=1 no_send_streak_max=7 no_send_pending_max=524233 send_slice_calls=16 accepted_bytes=2621440 zero=2 errors=0 budget_limited=9 max_accepted_bytes=262144 tun_flush_calls=14 tun_flush_failures=1 tun_flush_deferred=3"
  assert_contains "$summary" "tcp_lifecycle: transitions=1 closed_edges=1 terminal_candidates=1 max_pending_bytes=4096 max_remote_to_global_rx_bytes=3145728 sources=dead_slot_reap states=Closed"
  assert_contains "$summary" "terminal_pending_reap: events=1 bytes=4096 max_bytes=4096"
  assert_contains "$summary" "pending_at_close: events=2 bytes=12288 max_bytes=8192 terminal_events=1 terminal_bytes=4096 active_no_send_events=1 active_no_send_bytes=8192 send_capable_events=0 send_capable_bytes=0 inactive_no_send_events=0 inactive_no_send_bytes=0 unknown_events=0 unknown_bytes=0"
  assert_contains "$summary" "relay_remote_timing: first_read_max_ms=0 max_read_gap_ms=0 current_gap_max_ms=0 no_first_read_gap_max_ms=0"
  assert_contains "$summary" "tuic_tcp_stream: first_rx_events=0 first_rx_max_ms=0 read_gap_events=0 read_gap_max_ms=0 close_events=0 close_first_rx_max_ms=0 close_gap_max_ms=0 rx_bytes_max=0 reads_max=0 zero_rx_closes=0"
  assert_contains "$summary" "tuic_stream_pending: events=0 max_pending_gap_ms=0 pending_polls_max=0 data_streams=0 data_pending_gap_max_ms=0 data_rx_bytes_max=0"
  assert_contains "$summary" "tuic_stream_polling: polls_max=0 max_poll_gap_ms=0 data_streams=0 data_polls_max=0 data_poll_gap_max_ms=0 data_rx_bytes_max=0"
  assert_contains "$summary" "runtime_tun_egress: samples=1 drop_events=1 drop_delta_total=1423 max_delta=1423 unavailable=0 resets=0"
  assert_contains "$summary" "tun_rx_drain: attempts=4 packets=11 tcp=9 dns=1 udp=1 budget_exhausted=1 would_block=3 errors=0"
  assert_contains "$summary" "max_lost_bytes_delta=439956"
  assert_contains "$summary" "attribution: quic_loss_congestion+local_write_pressure+local_tun_egress_drop+local_downlink_backpressure+terminal_pending_reap"
  assert_contains "$summary" "pending_at_close+pending_close_active_no_send"

  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=90000 lost=10/120 lost_bytes=1000 congestion_events=5 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=70000 lost=15/180 lost_bytes=1500 congestion_events=8 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "existing-conn-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "max_lost_bytes_delta=500"
  assert_contains "$summary" "max_congestion_events_delta=3"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec  41.2 MBytes  11.5 Mbits/sec   67             sender
[  5]   0.00-30.00  sec  38.4 MBytes  10.7 Mbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=0 id=103708502287008
📊 TUIC QUIC stats conn=0 id=103708502287008 rtt=0ms cwnd=5808 lost=391390/973313 lost_bytes=500976100 congestion_events=93571 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=14,max_stream_data=54) rx_window(max_data=878,max_stream_data=1289) udp_tx=973311/1235276813B udp_rx=198656/95566719B dg_max=Some(1246) dg_space=1048576B
📊 TUIC QUIC stats conn=0 id=103708502287008 rtt=0ms cwnd=23540 lost=391390/976428 lost_bytes=500976100 congestion_events=93571 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=18,max_stream_data=74) rx_window(max_data=878,max_stream_data=1289) udp_tx=976426/1235704935B udp_rx=217657/122723118B dg_max=Some(1246) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "inherited-quic-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "max_lost_bytes_delta=0"
  assert_contains "$summary" "max_congestion_events_delta=0"
  assert_contains "$summary" "max_start_lost_bytes=500976100"
  assert_contains "$summary" "max_start_congestion_events=93571"
  assert_contains "$summary" "attribution: inherited_quic_congestion"
  assert_not_contains "$summary" "attribution: no_pressure_signal"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec  3.00 MBytes   838 Kbits/sec    2             sender
[  5]   0.00-30.00  sec  88.2 KBytes  24.1 Kbits/sec                  receiver
EOF_IPERF
  summary="$(summarize_metrics_window 0 "inherited-suppresses-reverse-sender-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "attribution: inherited_quic_congestion"
  assert_not_contains "$summary" "reverse_sender_backpressured"

  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=99
📊 TUIC QUIC stats conn=1 id=99 rtt=1ms cwnd=247092 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=1 id=99 rtt=1ms cwnd=247092 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  cat > "$tun_before" <<'EOF_TUN'
7: tun0: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1200 qdisc fq_codel state UNKNOWN mode DEFAULT group default qlen 500
    link/none
    RX:  bytes packets errors dropped  missed   mcast
      1024       8      0       0       0       0
    TX:  bytes packets errors dropped carrier collsns
      2048      16      0     100       0       0
EOF_TUN
  cat > "$tun_after" <<'EOF_TUN'
7: tun0: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1200 qdisc fq_codel state UNKNOWN mode DEFAULT group default qlen 500
    link/none
    RX:  bytes packets errors dropped  missed   mcast
      4096      32      0       0       0       0
    TX:  bytes packets errors dropped carrier collsns
      8192      64      0    1523       0       0
EOF_TUN
  read -r tun_if_before tun_rx_before tun_tx_before <<< "$(parse_tun_drop_sample "$tun_before" "tun0")"
  read -r tun_if_after tun_rx_after tun_tx_after <<< "$(parse_tun_drop_sample "$tun_after" "$tun_if_before")"
  summary="$(summarize_metrics_window 0 "tun-drop-self-test" "$iperf_sample" "$log_sample" "tcp" "$tun_if_after" "$tun_rx_before" "$tun_tx_before" "$tun_rx_after" "$tun_tx_after")"
  assert_contains "$summary" "tun_drops: if=tun0 tun_rx_dropped_delta=0 tun_tx_dropped_delta=1423"
  assert_contains "$summary" "attribution: local_tun_egress_drop"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec  72.7 MBytes  20.3 Mbits/sec    0             sender
[  5]   0.00-30.00  sec  67.6 MBytes  18.9 Mbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=99
📊 TUIC QUIC stats conn=1 id=99 rtt=1ms cwnd=247092 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-downlink-flush pending_total=0 pending_max=0 pending_high=0 remote_to_global_rx_bytes=70988513 flush_attempts=201 no_send_capacity=0 send_window_samples=201 send_capacity_min=4194304 send_capacity_max=4194304 send_queue_max=3786786 recv_queue_max=4096 may_send_false=0 may_recv_false=0 no_send_capacity_streak_max=0 no_send_capacity_pending_max=0 send_slice_calls=201 send_slice_accepted=70988513 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=0 send_slice_max_accepted=65535 tun_flush_tx_calls=201 tun_flush_tx_failures=0 tun_flush_deferred=0 dirty_handles=1
🔎 tcp-tun-egress if=tun0 status=delta tx_dropped_total=28123 tx_dropped_delta=14167 global_rx_paused=false pending_total=0 pending_max=0 pending_high=0 remote_to_global_rx_bytes=70988513 tun_flush_tx_calls=201 dirty_handles=1
🔎 tcp-tun-egress-feedback paused=true reason=drop_delta tx_dropped_delta=14167 max_pressure=3786786 total_pressure=3786786 high=4194304 low=1048576 drop_events=1 drop_delta_total=14167 pause_edges=1 resume_edges=0
📊 TUIC QUIC stats conn=1 id=99 rtt=1ms cwnd=247092 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "tun-feedback-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "downlink_backpressure: pause_edges=0 resume_edges=0"
  assert_contains "$summary" "downlink_flush: attempts=201 no_send_capacity=0 send_window_samples=201 send_capacity_min=4194304 send_capacity_max=4194304 send_queue_max=3786786"
  assert_contains "$summary" "runtime_tun_egress: samples=1 drop_events=1 drop_delta_total=14167 max_delta=14167"
  assert_contains "$summary" "tun_egress_feedback: pause_edges=1 resume_edges=0 drop_events=1 drop_delta_total=14167 max_delta=14167 max_pressure_bytes=3786786"
  assert_contains "$summary" "attribution: local_tun_egress_drop+local_tun_egress_feedback"
  assert_not_contains "$summary" "local_downlink_backpressure"
  assert_contains "$summary" "terminal_pending_reap: events=0 bytes=0 max_bytes=0"

  cat > "$iperf_sample" <<'EOF_IPERF'
[  5]   0.00-30.04  sec  2.62 MBytes   733 Kbits/sec    3             sender
[  5]   0.00-30.00  sec  0.00 Bytes  0.00 bits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=99
📊 TUIC QUIC stats conn=1 id=99 rtt=0ms cwnd=247211 lost=0/34 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=32/9214B udp_rx=219/296179B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=0 remote_reads=0 global_rx_wait_max_us=0 global_rx_pressure_events=0 global_rx_queue_used_max=2 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tcp-relay-write-half-closed handle=SocketHandle(1) reason=local_finish
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=true read_only_after_local_finish=true uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=43772 remote_reads=1 remote_after_local_finish_bytes=43772 remote_after_local_finish_reads=1 global_rx_wait_max_us=4 global_rx_pressure_events=0 global_rx_queue_used_max=5 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tcp-relay-close handle=SocketHandle(1) direction=timer reason=half_closed_idle_timeout uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=43772 remote_reads=1 remote_after_local_finish_bytes=43772 remote_after_local_finish_reads=1 global_rx_wait_max_us=4 global_rx_pressure_events=0 global_rx_queue_used_max=5 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
EOF_LOG
  summary="$(summarize_metrics_window 0 "late-remote-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_receiver_mbps: 0.000"
  assert_contains "$summary" "global_rx_pressure: events=0 max_wait_ms=0.000 queue_used_max=5 queue_capacity=1024"
  assert_contains "$summary" "relay_late_remote: post_finish_bytes=43772 post_finish_reads=1"
  assert_contains "$summary" "attribution: late_remote_after_local_finish"

  cat > "$log_sample" <<'EOF_LOG'
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=100 remote_reads=1 global_rx_wait_max_us=0 global_rx_pressure_events=0 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tcp-relay-write-half-closed handle=SocketHandle(1) reason=local_finish
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=true read_only_after_local_finish=true uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=100 remote_reads=1 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 global_rx_wait_max_us=0 global_rx_pressure_events=0 local_write_wait_max_us=0 local_write_pressure_events=0
EOF_LOG
  summary="$(summarize_metrics_window 0 "late-remote-zero-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "relay_late_remote: post_finish_bytes=0 post_finish_reads=0"
  assert_contains "$summary" "attribution: no_pressure_signal"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec  2.62 MBytes   733 Kbits/sec    3             sender
[  5]   0.00-30.00  sec  0.00 Bytes  0.00 bits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99
📊 TUIC QUIC stats conn=3 id=99 rtt=8ms cwnd=247211 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=0 remote_reads=0 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=0 max_remote_read_gap_ms=0 current_remote_read_gap_ms=12000 global_rx_wait_max_us=0 global_rx_pressure_events=0 global_rx_queue_used_max=0 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tuic-tcp-stream-first-rx target=43.130.32.77:5201 conn=3 id=99 stream=8 first_rx_ms=20500 read_bytes=35244 reads=1
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=75128 remote_reads=2 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=20500 max_remote_read_gap_ms=15000 current_remote_read_gap_ms=100 global_rx_wait_max_us=4 global_rx_pressure_events=0 global_rx_queue_used_max=1 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tuic-tcp-stream-read-gap target=43.130.32.77:5201 conn=3 id=99 stream=8 gap_ms=15000 read_bytes=39884 reads=2 rx_bytes=75128
🔎 tuic-tcp-stream-pending target=43.130.32.77:5201 conn=3 id=99 stream=8 pending_gap_ms=12000 pending_polls=97 polls=112 max_poll_gap_ms=5000 rx_bytes=75128 reads=2
🔎 tuic-tcp-stream-close target=43.130.32.77:5201 conn=3 id=99 stream=8 first_rx_ms=20500 max_read_gap_ms=15000 rx_bytes=109304 reads=3 pending_polls=97 max_pending_gap_ms=12000 polls=126 max_poll_gap_ms=5000
📊 TUIC QUIC stats conn=3 id=99 rtt=5ms cwnd=247289 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "stream-timing-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "relay_remote_timing: first_read_max_ms=20500 max_read_gap_ms=15000 current_gap_max_ms=12000 no_first_read_gap_max_ms=12000"
  assert_contains "$summary" "tuic_tcp_stream: first_rx_events=1 first_rx_max_ms=20500 read_gap_events=1 read_gap_max_ms=15000 close_events=1 close_first_rx_max_ms=20500 close_gap_max_ms=15000 rx_bytes_max=109304 reads_max=3 zero_rx_closes=0"
  assert_contains "$summary" "tuic_stream_pending: events=1 max_pending_gap_ms=12000 pending_polls_max=97 data_streams=1 data_pending_gap_max_ms=12000 data_rx_bytes_max=109304"
  assert_contains "$summary" "tuic_stream_polling: polls_max=126 max_poll_gap_ms=5000 data_streams=1 data_polls_max=126 data_poll_gap_max_ms=5000 data_rx_bytes_max=109304"
  assert_contains "$summary" "attribution: tuic_stream_first_byte_slow+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_first_byte_slow+relay_remote_read_gap"
  assert_not_contains "$summary" "reverse_sender_backpressured"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec   111 MBytes  31.0 Mbits/sec    0             sender
[  5]   0.00-30.00  sec   103 MBytes  28.7 Mbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99
📊 TUIC QUIC stats conn=3 id=99 rtt=8ms cwnd=247211 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-relay-close handle=SocketHandle(1) direction=timer reason=half_closed_idle_timeout uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=343 remote_reads=6 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=3 max_remote_read_gap_ms=30147 current_remote_read_gap_ms=0 global_rx_wait_max_us=0 global_rx_pressure_events=0 global_rx_queue_used_max=1 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tcp-relay-close handle=SocketHandle(2) direction=timer reason=remote_eof uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=110790062 remote_reads=9637 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=3 max_remote_read_gap_ms=3763 current_remote_read_gap_ms=0 global_rx_wait_max_us=4 global_rx_pressure_events=0 global_rx_queue_used_max=1 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tuic-tcp-stream-read-gap target=43.130.32.77:5201 conn=3 id=99 stream=0 gap_ms=30147 read_bytes=32 reads=6 rx_bytes=343
🔎 tuic-tcp-stream-close target=43.130.32.77:5201 conn=3 id=99 stream=0 first_rx_ms=3 max_read_gap_ms=30147 rx_bytes=343 reads=6
🔎 tuic-tcp-stream-read-gap target=43.130.32.77:5201 conn=3 id=99 stream=4 gap_ms=3763 read_bytes=12220 reads=101 rx_bytes=1275382
🔎 tuic-tcp-stream-close target=43.130.32.77:5201 conn=3 id=99 stream=4 first_rx_ms=3 max_read_gap_ms=3763 rx_bytes=110790062 reads=9637
📊 TUIC QUIC stats conn=3 id=99 rtt=5ms cwnd=247289 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "control-stream-gap-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "relay_remote_timing: first_read_max_ms=3 max_read_gap_ms=30147 current_gap_max_ms=0 no_first_read_gap_max_ms=0 data_streams=1 data_first_read_max_ms=3 data_max_read_gap_ms=3763"
  assert_contains "$summary" "tuic_tcp_stream: first_rx_events=0 first_rx_max_ms=0 read_gap_events=2 read_gap_max_ms=30147 close_events=2 close_first_rx_max_ms=3 close_gap_max_ms=30147 rx_bytes_max=110790062 reads_max=9637 zero_rx_closes=0 data_streams=1 data_first_rx_max_ms=3 data_read_gap_max_ms=3763 data_close_gap_max_ms=3763 data_rx_bytes_max=110790062"
  assert_not_contains "$summary" "tuic_stream_read_gap"
  assert_not_contains "$summary" "relay_remote_read_gap"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec   642 MBytes   179 Mbits/sec    0             sender
[  5]   0.00-30.00  sec   641 MBytes   179 Mbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=99
EOF_LOG
  local late_start_line
  late_start_line="$(wc -l < "$log_sample" | tr -d ' ')"
  (
    sleep 0.2
    cat >> "$log_sample" <<'EOF_LATE_LOG'
🔎 tcp-handle-close handle=SocketHandle(3) direction=local reason=dead_slot_reap state=Relaying pending=16384 pending_high=524635 remote_to_global_rx_bytes=671088640 flush_attempts=17 no_send_capacity=3 send_slice_calls=14 send_slice_accepted=670564005 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=2 send_slice_max_accepted=262144 tun_flush_tx_calls=13 tun_flush_tx_failures=0 tun_flush_deferred=0 close_pending_class=terminal_closed_no_send close_pending_bytes=16384 terminal_pending_reap_bytes=16384 tcp_state=Closed active=false can_send=false can_recv=false
EOF_LATE_LOG
  ) &
  local late_writer_pid=$!
  POST_IPERF_METRICS_SETTLE_SECS=1 wait_for_post_iperf_metrics_settle "late-close-tail-self-test"
  wait "$late_writer_pid"
  summary="$(summarize_metrics_window "$late_start_line" "late-close-tail-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "terminal_pending_reap: events=1 bytes=16384 max_bytes=16384"
  assert_contains "$summary" "pending_at_close: events=1 bytes=16384 max_bytes=16384 terminal_events=1 terminal_bytes=16384"
  assert_contains "$summary" "attribution: terminal_pending_reap+pending_at_close"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec  3.00 MBytes   838 Kbits/sec    2             sender
[  5]   0.00-30.00  sec  88.2 KBytes  24.1 Kbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=99
📊 TUIC QUIC stats conn=1 id=99 rtt=8ms cwnd=247211 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=90312 remote_reads=6 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 global_rx_wait_max_us=5 global_rx_pressure_events=0 local_write_wait_max_us=0 local_write_pressure_events=0
📊 TUIC QUIC stats conn=1 id=99 rtt=5ms cwnd=247289 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "reverse-sender-backpressure-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_sender_mbps: 0.838"
  assert_contains "$summary" "attribution: reverse_sender_backpressured"
  summary="$(summarize_metrics_window 0 "udp-reverse-self-test" "$iperf_sample" "$log_sample" "udp")"
  assert_not_contains "$summary" "reverse_sender_backpressured"
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
POST_IPERF_METRICS_SETTLE_SECS="${POST_IPERF_METRICS_SETTLE_SECS:-2}"
PROBE_ORDER="${PROBE_ORDER:-forward-first}"
OUT="${OUT:-/tmp/mvpn_knife14b_lowrtt_$(date +%Y%m%d_%H%M%S).md}"
METRIC_RE='📊 数据面|🔬 主循环|TUIC datagram|UDP relay mode|TCP socket buffers|TUIC QUIC stats|tuic-open-tcp|tuic-tcp-stream-first-rx|tuic-tcp-stream-read-gap|tuic-tcp-stream-pending|tuic-tcp-stream-close|tuic-tcp-pool-reconnect|tcp-relay-live|tcp-relay-write-half-closed|tcp-relay-close|tcp-handle-close|tcp-lifecycle-transition|tcp-local-write-pressure|tcp-global-rx-pressure|tcp-downlink-backpressure|tcp-downlink-flush|tcp-tun-rx-drain|tcp-tun-egress'

case "$PROBE_ORDER" in
  forward-first|reverse-first|forward-only|reverse-only) ;;
  *)
    echo "invalid PROBE_ORDER=$PROBE_ORDER (expected forward-first|reverse-first|forward-only|reverse-only)" >&2
    exit 2
    ;;
esac
if ! is_uint "$POST_IPERF_METRICS_SETTLE_SECS"; then
  echo "invalid POST_IPERF_METRICS_SETTLE_SECS=$POST_IPERF_METRICS_SETTLE_SECS (expected non-negative integer seconds)" >&2
  exit 2
fi

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
  local probe_kind="$4"
  local tun_if="${5:-unknown}"
  local tun_rx_before="${6:-unknown}"
  local tun_tx_before="${7:-unknown}"
  local tun_rx_after="${8:-unknown}"
  local tun_tx_after="${9:-unknown}"

  append_subsection "Attribution Summary: $title"
  summarize_metrics_window \
    "$start_line" \
    "$title" \
    "$iperf_file" \
    "$LOG" \
    "$probe_kind" \
    "$tun_if" \
    "$tun_rx_before" \
    "$tun_tx_before" \
    "$tun_rx_after" \
    "$tun_tx_after" | tee -a "$OUT"
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
  local probe_kind="tcp"
  if [[ "$metrics_title" == *"UDP "* ]]; then
    probe_kind="udp"
  fi

  local start_line
  start_line="$(log_line_count)"
  local tun_before tun_after
  local tun_if_before tun_rx_before tun_tx_before
  local tun_if_after tun_rx_after tun_tx_after
  tun_before="$(sample_tun_drops "${TUN_IF:-}")"
  read -r tun_if_before tun_rx_before tun_tx_before <<< "$tun_before"
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
  wait_for_post_iperf_metrics_settle "$metrics_title"
  tun_after="$(sample_tun_drops "$tun_if_before")"
  read -r tun_if_after tun_rx_after tun_tx_after <<< "$tun_after"
  append_metrics_since "$start_line" "$metrics_title"
  if [[ -n "$tmp" && -f "$tmp" ]]; then
    append_attribution_summary \
      "$start_line" \
      "$metrics_title" \
      "$tmp" \
      "$probe_kind" \
      "$tun_if_after" \
      "$tun_rx_before" \
      "$tun_tx_before" \
      "$tun_rx_after" \
      "$tun_tx_after"
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
  echo "- post_iperf_metrics_settle_secs: ${POST_IPERF_METRICS_SETTLE_SECS}"
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
