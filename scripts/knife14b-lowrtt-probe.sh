#!/usr/bin/env bash
# 刀14b low-RTT fat-path #3 quantify probe.
# Requires an already-running tunnel started with:
#   sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 bash scripts/knife35-acceptance.sh soak
#
# This script drives iperf through an already-running tunnel. Connection-pool behavior is configured
# by the parent mini_vpn process; this probe only controls traffic direction/order.

set -euo pipefail

readonly FORWARD_DISCRIMINATOR_MAX_QUIC_LOST_BYTES=16777216

usage() {
  cat <<'USAGE'
usage: scripts/knife14b-lowrtt-probe.sh <iperf-target> [port]
       scripts/knife14b-lowrtt-probe.sh --self-test

env:
  PARALLEL_SET="1 2 4 8"   iperf parallel sweep
  DURATION=30              seconds per iperf run
  IPERF_BYTES=""           optional fixed TCP byte goal (for example 64M); replaces -t and permits an EOF-close proof
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
  MINI_VPN_PROBE_INCLUDE_STREAM_SERVICE_WINDOW=0
                            set 1 to include per-chunk tcp-stream-service-window lines
  TUN_IF=tun0              TUN interface to sample for RX/TX dropped deltas
  ROUTING_MODE=full-tunnel full-tunnel | target-only; target-only skips public-exit/fake-IP gold checks
  FORWARD_DISCRIMINATOR=0  set 1 only for the bounded forward-only Gate; makes receiver/drop/loss failures fatal
USAGE
}

routing_expectation_note() {
  case "${1:-full-tunnel}" in
    target-only)
      echo "target-only routing: only the iperf Target is routed through TUN; public exit-IP and fake-IP DNS gold checks are not applicable."
      ;;
    full-tunnel)
      echo "full-tunnel routing: curl ipinfo.io must be the exit IP; dig example.com +short must be a 198.18.x.x fake-IP."
      ;;
    *) return 2 ;;
  esac
}

forward_discriminator_report_passes() {
  local report="$1"
  [[ -f "$report" ]] || return 1
  grep -Eq '[[:space:]]receiver$' "$report" || return 1
  awk -v loss_limit="$FORWARD_DISCRIMINATOR_MAX_QUIC_LOST_BYTES" '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^tun_tx_dropped_delta=/) {
          split($i, value, "=")
          tun_samples++
          if (value[2] !~ /^[0-9]+$/ || (value[2] + 0) != 0) {
            tun_bad = 1
          }
        }
        if ($i ~ /^aggregate_lost_bytes_delta=/) {
          split($i, value, "=")
          quic_samples++
          if (value[2] !~ /^[0-9]+$/ || (value[2] + 0) > loss_limit) {
            quic_bad = 1
          }
        }
      }
    }
    END {
      exit !(tun_samples > 0 && tun_bad == 0 && quic_samples > 0 && quic_bad == 0)
    }
  ' "$report"
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

iperf_interval_profile() {
  local iperf_file="$1"
  awk '
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

    /^\[[[:space:]]*[0-9]+]/ {
      if ($NF == "sender" || $NF == "receiver") {
        next
      }
      interval_found = 0
      rate_found = 0
      duration = 0
      rate_mbps = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^[0-9]+([.][0-9]+)?-[0-9]+([.][0-9]+)?$/) {
          split($i, bounds, "-")
          start_sec = bounds[1] + 0
          end_sec = bounds[2] + 0
          duration = end_sec - start_sec
          interval_found = 1
        }
        if (i < NF && $(i + 1) ~ /^([KMG]?bits\/sec|bits\/sec)$/) {
          rate_mbps = to_mbps($i + 0, $(i + 1))
          rate_found = 1
        }
      }
      if (interval_found && rate_found && duration > 0 && duration <= 2.0) {
        samples++
        rates[samples] = rate_mbps
        total += rate_mbps
      }
    }

    END {
      if (samples == 0) {
        printf "0 0.000 0.000 0 0.000 0.000 0\n"
        exit
      }

      tail_samples = samples < 6 ? samples : 6
      tail_start = samples - tail_samples + 1
      tail_min = ""
      for (i = 1; i <= samples; i++) {
        if (i >= tail_start) {
          tail_total += rates[i]
          if (tail_min == "" || rates[i] < tail_min) {
            tail_min = rates[i]
          }
        } else {
          prefix_total += rates[i]
          prefix_samples++
        }
      }
      overall_avg = total / samples
      tail_avg = tail_total / tail_samples
      prefix_avg = prefix_samples > 0 ? prefix_total / prefix_samples : overall_avg
      tail_collapse = 0
      if (samples >= 4 && prefix_avg >= 100 && tail_avg < 50 && tail_avg <= prefix_avg * 0.35) {
        tail_collapse = 1
      }
      printf "%d %.3f %.3f %d %.3f %.3f %d\n", samples, overall_avg, prefix_avg, tail_samples, tail_avg, tail_min, tail_collapse
    }
  ' "$iperf_file"
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

tcp_transfer_spec() {
  if [[ -n "${IPERF_BYTES:-}" ]]; then
    printf '%s %s\n' -n "$IPERF_BYTES"
  else
    printf '%s %s\n' -t "$DURATION"
  fi
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
  local interval_samples interval_avg interval_prefix_avg interval_tail_samples interval_tail_avg
  local interval_tail_min interval_tail_collapse
  receiver_mbps="$(iperf_receiver_mbps "$iperf_file")"
  sender_mbps="$(iperf_sender_mbps "$iperf_file")"
  reverse_tcp="$(iperf_is_reverse_tcp "$iperf_file")"
  tun_rx_delta="$(counter_delta "$tun_rx_before" "$tun_rx_after")"
  tun_tx_delta="$(counter_delta "$tun_tx_before" "$tun_tx_after")"
  read -r \
    interval_samples \
    interval_avg \
    interval_prefix_avg \
    interval_tail_samples \
    interval_tail_avg \
    interval_tail_min \
    interval_tail_collapse <<< "$(iperf_interval_profile "$iperf_file")"

  if [[ ! -f "$log_file" ]]; then
    {
      echo "- iperf_sender_mbps: $sender_mbps"
      echo "- iperf_receiver_mbps: $receiver_mbps"
      echo "- iperf_interval_profile: samples=$interval_samples overall_avg_mbps=$interval_avg prefix_avg_mbps=$interval_prefix_avg tail_samples=$interval_tail_samples tail_avg_mbps=$interval_tail_avg tail_min_mbps=$interval_tail_min tail_collapse=$interval_tail_collapse"
      echo "- throughput_shape: shape=unknown tail_collapse=$interval_tail_collapse local_pressure=unknown no_data=0 stable_high=0"
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
    -v interval_samples="$interval_samples" \
    -v interval_avg="$interval_avg" \
    -v interval_prefix_avg="$interval_prefix_avg" \
    -v interval_tail_samples="$interval_tail_samples" \
    -v interval_tail_avg="$interval_tail_avg" \
    -v interval_tail_min="$interval_tail_min" \
    -v interval_tail_collapse="$interval_tail_collapse" \
    -v tun_if="$tun_if" \
    -v tun_rx_delta="$tun_rx_delta" \
    -v tun_tx_delta="$tun_tx_delta" \
    -v inherited_low_cwnd_limit=65536 \
    -v inherited_lost_bytes_floor=1048576 \
    -v inherited_cong_floor=100 \
    -v slow_rx_floor_ms=5000 \
    -v tuic_stream_starved_rx_ceiling=5242880 \
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

    function string_token(token, key, value) {
      value = token
      sub("^" key "=", "", value)
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

    /tuic-tcp-pool-selection/ {
      selection_count++
      generation = ""
      probe_result = ""
      selection_reconnect_reason = ""
      last_success_age = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^generation=/) {
          generation = digits_token($i, "generation")
        } else if ($i ~ /^probe_result=/) {
          probe_result = string_token($i, "probe_result")
        } else if ($i ~ /^reconnect_reason=/) {
          selection_reconnect_reason = string_token($i, "reconnect_reason")
        } else if ($i ~ /^last_success_age_secs=/) {
          last_success_age = string_token($i, "last_success_age_secs")
        }
      }
      selection_generations = add_unique(selection_generations, generation)
      selection_probe_results = add_unique(selection_probe_results, probe_result)
      selection_reconnect_reasons = add_unique(selection_reconnect_reasons, selection_reconnect_reason)
      if (last_success_age ~ /^[0-9]+$/ && last_success_age + 0 > max_last_success_age_secs) {
        max_last_success_age_secs = last_success_age + 0
      }
    }

    /tuic-open-tcp/ {
      conn = ""
      id = ""
      stream = ""
      handle = ""
      epoch = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          conn = numeric_token($i, "conn")
        } else if ($i ~ /^id=/) {
          id = numeric_token($i, "id")
        } else if ($i ~ /^stream=/) {
          stream = digits_token($i, "stream")
        } else if ($i ~ /^handle=/) {
          handle = string_token($i, "handle")
        } else if ($i ~ /^epoch=/) {
          epoch = digits_token($i, "epoch")
        }
      }
      open_count++
      open_conns = add_unique(open_conns, conn)
      if (conn != "" && id != "" && stream != "" && handle != "" && epoch != "") {
        stream_service_tuic_bridge_events++
      }
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

    /tcp-global-rx-backpressure/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "paused=true") {
          global_receive_pause_count++
        } else if ($i == "paused=false") {
          global_receive_resume_count++
        } else if ($i ~ /^max_pending=/) {
          value = numeric_token($i, "max_pending")
          if (value > max_global_receive_pending) {
            max_global_receive_pending = value
          }
        } else if ($i ~ /^total_pending=/) {
          value = numeric_token($i, "total_pending")
          if (value > max_global_receive_total_pending) {
            max_global_receive_total_pending = value
          }
        } else if ($i ~ /^max_tx_queue=/) {
          value = numeric_token($i, "max_tx_queue")
          if (value > max_global_receive_tx_queue) {
            max_global_receive_tx_queue = value
          }
        } else if ($i ~ /^receive_high=/) {
          value = numeric_token($i, "receive_high")
          if (value > max_global_receive_high) {
            max_global_receive_high = value
          }
        } else if ($i ~ /^receive_low=/) {
          value = numeric_token($i, "receive_low")
          if (value > max_global_receive_low) {
            max_global_receive_low = value
          }
        } else if ($i ~ /^receive_total_high=/) {
          value = numeric_token($i, "receive_total_high")
          if (value > max_global_receive_total_high) {
            max_global_receive_total_high = value
          }
        } else if ($i ~ /^receive_total_low=/) {
          value = numeric_token($i, "receive_total_low")
          if (value > max_global_receive_total_low) {
            max_global_receive_total_low = value
          }
        } else if ($i == "local_egress_paused=true") {
          global_receive_local_egress_paused = 1
        } else if ($i == "tun_feedback_paused=true") {
          global_receive_tun_feedback_paused = 1
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

    /tcp-stream-service-window/ {
      stream_service_windows++
      line_conn = ""
      line_id = ""
      line_stream = ""
      line_last_blocked_reason = ""
      line_useful_progress = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^conn=/) {
          line_conn = digits_token($i, "conn")
        } else if ($i ~ /^id=/) {
          line_id = digits_token($i, "id")
        } else if ($i ~ /^stream=/) {
          line_stream = digits_token($i, "stream")
        } else if ($i ~ /^remote_poll_ticks=/) {
          value = numeric_token($i, "remote_poll_ticks")
          if (value > max_stream_service_remote_poll_ticks) {
            max_stream_service_remote_poll_ticks = value
          }
        } else if ($i ~ /^remote_poll_len_min=/) {
          value = numeric_token($i, "remote_poll_len_min")
          if (value > 0 &&
              (min_stream_service_remote_poll_len_min == "" ||
               value < min_stream_service_remote_poll_len_min)) {
            min_stream_service_remote_poll_len_min = value
          }
        } else if ($i ~ /^remote_poll_len_max=/) {
          value = numeric_token($i, "remote_poll_len_max")
          if (value > max_stream_service_remote_poll_len_max) {
            max_stream_service_remote_poll_len_max = value
          }
        } else if ($i ~ /^pending_fresh_stream_frames=/) {
          value = numeric_token($i, "pending_fresh_stream_frames")
          if (value > max_stream_service_pending_fresh_stream_frames) {
            max_stream_service_pending_fresh_stream_frames = value
          }
        } else if ($i ~ /^pending_stale_stream_frames=/) {
          value = numeric_token($i, "pending_stale_stream_frames")
          if (value > max_stream_service_pending_stale_stream_frames) {
            max_stream_service_pending_stale_stream_frames = value
          }
        } else if ($i ~ /^remote_read_chunks=/) {
          value = numeric_token($i, "remote_read_chunks")
          if (value > max_stream_service_remote_read_chunks) {
            max_stream_service_remote_read_chunks = value
          }
        } else if ($i ~ /^remote_read_bytes=/) {
          value = numeric_token($i, "remote_read_bytes")
          if (value > max_stream_service_remote_read_bytes) {
            max_stream_service_remote_read_bytes = value
          }
        } else if ($i ~ /^global_rx_pressure_events=/) {
          value = numeric_token($i, "global_rx_pressure_events")
          if (value > max_stream_service_global_rx_pressure_events) {
            max_stream_service_global_rx_pressure_events = value
          }
        } else if ($i ~ /^global_rx_queue_used_max=/) {
          value = numeric_token($i, "global_rx_queue_used_max")
          if (value > max_stream_service_global_rx_queue_used) {
            max_stream_service_global_rx_queue_used = value
          }
        } else if ($i ~ /^global_rx_queue_capacity=/) {
          value = numeric_token($i, "global_rx_queue_capacity")
          if (value > max_stream_service_global_rx_queue_capacity) {
            max_stream_service_global_rx_queue_capacity = value
          }
        } else if ($i ~ /^local_accepted_bytes=/) {
          value = numeric_token($i, "local_accepted_bytes")
          if (value > max_stream_service_local_accepted_bytes) {
            max_stream_service_local_accepted_bytes = value
          }
        } else if ($i ~ /^local_egress_drain_bytes=/) {
          value = numeric_token($i, "local_egress_drain_bytes")
          if (value > max_stream_service_local_egress_drain_bytes) {
            max_stream_service_local_egress_drain_bytes = value
          }
        } else if ($i ~ /^local_flush_tx_calls=/) {
          value = numeric_token($i, "local_flush_tx_calls")
          if (value > max_stream_service_local_flush_tx_calls) {
            max_stream_service_local_flush_tx_calls = value
          }
        } else if ($i ~ /^local_flush_tx_failures=/) {
          value = numeric_token($i, "local_flush_tx_failures")
          if (value > max_stream_service_local_flush_tx_failures) {
            max_stream_service_local_flush_tx_failures = value
          }
        } else if ($i ~ /^local_dirty_passes=/) {
          value = numeric_token($i, "local_dirty_passes")
          if (value > max_stream_service_local_dirty_passes) {
            max_stream_service_local_dirty_passes = value
          }
        } else if ($i ~ /^last_blocked_reason=/) {
          line_last_blocked_reason = string_token($i, "last_blocked_reason")
        } else if ($i == "useful_progress=true") {
          line_useful_progress = 1
        }
      }
      if (line_useful_progress > 0) {
        stream_service_useful_progress_windows++
      }
      if (line_last_blocked_reason != "") {
        stream_service_last_blocked_reason = line_last_blocked_reason
      }
      if (line_conn != "" && line_id != "" && line_stream != "") {
        stream_service_tuic_bridge_events++
      }
    }

    /tcp-local-egress-service/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^windows=/) {
          value = numeric_token($i, "windows")
          if (value > max_local_egress_service_windows) {
            max_local_egress_service_windows = value
          }
        } else if ($i ~ /^cycles=/) {
          value = numeric_token($i, "cycles")
          if (value > max_local_egress_service_cycles) {
            max_local_egress_service_cycles = value
          }
        } else if ($i ~ /^accepted_bytes=/) {
          value = numeric_token($i, "accepted_bytes")
          if (value > max_local_egress_service_accepted_bytes) {
            max_local_egress_service_accepted_bytes = value
          }
        } else if ($i ~ /^egress_drain_bytes=/) {
          value = numeric_token($i, "egress_drain_bytes")
          if (value > max_local_egress_service_egress_drain_bytes) {
            max_local_egress_service_egress_drain_bytes = value
          }
        } else if ($i ~ /^tun_rx_packets=/) {
          value = numeric_token($i, "tun_rx_packets")
          if (value > max_local_egress_service_tun_rx_packets) {
            max_local_egress_service_tun_rx_packets = value
          }
        } else if ($i ~ /^flush_tx_calls=/) {
          value = numeric_token($i, "flush_tx_calls")
          if (value > max_local_egress_service_flush_tx_calls) {
            max_local_egress_service_flush_tx_calls = value
          }
        } else if ($i ~ /^flush_tx_failures=/) {
          value = numeric_token($i, "flush_tx_failures")
          if (value > max_local_egress_service_flush_tx_failures) {
            max_local_egress_service_flush_tx_failures = value
          }
        } else if ($i ~ /^dirty_passes=/) {
          value = numeric_token($i, "dirty_passes")
          if (value > max_local_egress_service_dirty_passes) {
            max_local_egress_service_dirty_passes = value
          }
        } else if ($i ~ /^target_reached=/) {
          value = numeric_token($i, "target_reached")
          if (value > max_local_egress_service_target_reached) {
            max_local_egress_service_target_reached = value
          }
        } else if ($i ~ /^no_progress=/) {
          value = numeric_token($i, "no_progress")
          if (value > max_local_egress_service_no_progress) {
            max_local_egress_service_no_progress = value
          }
        } else if ($i ~ /^cycle_budget=/) {
          value = numeric_token($i, "cycle_budget")
          if (value > max_local_egress_service_cycle_budget) {
            max_local_egress_service_cycle_budget = value
          }
        } else if ($i ~ /^hard_pause=/) {
          value = numeric_token($i, "hard_pause")
          if (value > max_local_egress_service_hard_pause) {
            max_local_egress_service_hard_pause = value
          }
        } else if ($i ~ /^no_work=/) {
          value = numeric_token($i, "no_work")
          if (value > max_local_egress_service_no_work) {
            max_local_egress_service_no_work = value
          }
        } else if ($i ~ /^egress_actor_windows=/) {
          value = numeric_token($i, "egress_actor_windows")
          if (value > max_local_egress_actor_windows) {
            max_local_egress_actor_windows = value
          }
        } else if ($i ~ /^egress_actor_cycles=/) {
          value = numeric_token($i, "egress_actor_cycles")
          if (value > max_local_egress_actor_cycles) {
            max_local_egress_actor_cycles = value
          }
        } else if ($i ~ /^egress_actor_admitted_bytes=/) {
          value = numeric_token($i, "egress_actor_admitted_bytes")
          if (value > max_local_egress_actor_admitted_bytes) {
            max_local_egress_actor_admitted_bytes = value
          }
        } else if ($i ~ /^egress_actor_drain_bytes=/) {
          value = numeric_token($i, "egress_actor_drain_bytes")
          if (value > max_local_egress_actor_drain_bytes) {
            max_local_egress_actor_drain_bytes = value
          }
        } else if ($i ~ /^egress_actor_no_progress=/) {
          value = numeric_token($i, "egress_actor_no_progress")
          if (value > max_local_egress_actor_no_progress) {
            max_local_egress_actor_no_progress = value
          }
        } else if ($i ~ /^egress_actor_immediate_wake=/) {
          value = numeric_token($i, "egress_actor_immediate_wake")
          if (value > max_local_egress_actor_immediate_wake) {
            max_local_egress_actor_immediate_wake = value
          }
        } else if ($i ~ /^egress_actor_self_wake=/) {
          value = numeric_token($i, "egress_actor_self_wake")
          if (value > max_local_egress_actor_self_wake) {
            max_local_egress_actor_self_wake = value
          }
        } else if ($i ~ /^egress_actor_external_wait=/) {
          value = numeric_token($i, "egress_actor_external_wait")
          if (value > max_local_egress_actor_external_wait) {
            max_local_egress_actor_external_wait = value
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
        } else if ($i ~ /^downstream_permit_bytes=/) {
          value = numeric_token($i, "downstream_permit_bytes")
          if (value > max_downstream_permit_bytes) {
            max_downstream_permit_bytes = value
          }
        } else if ($i ~ /^downstream_permit_released_bytes=/) {
          value = numeric_token($i, "downstream_permit_released_bytes")
          if (value > max_downstream_permit_released_bytes) {
            max_downstream_permit_released_bytes = value
          }
        } else if ($i ~ /^downstream_permit_pending_high=/) {
          value = numeric_token($i, "downstream_permit_pending_high")
          if (value > max_downstream_permit_pending_high) {
            max_downstream_permit_pending_high = value
          }
        } else if ($i ~ /^terminal_late_remote_payload_bytes=/) {
          value = numeric_token($i, "terminal_late_remote_payload_bytes")
          if (value > max_flush_terminal_late_remote_bytes) {
            max_flush_terminal_late_remote_bytes = value
          }
        } else if ($i ~ /^terminal_late_remote_payload_events=/) {
          value = numeric_token($i, "terminal_late_remote_payload_events")
          if (value > max_flush_terminal_late_remote_events) {
            max_flush_terminal_late_remote_events = value
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
        } else if ($i ~ /^headroom_limited_calls=/) {
          value = numeric_token($i, "headroom_limited_calls")
          if (value > max_headroom_limited) {
            max_headroom_limited = value
          }
        } else if ($i ~ /^headroom_deferred_bytes=/) {
          value = numeric_token($i, "headroom_deferred_bytes")
          if (value > max_headroom_deferred_bytes) {
            max_headroom_deferred_bytes = value
          }
        } else if ($i ~ /^drain_credit_granted_bytes=/) {
          value = numeric_token($i, "drain_credit_granted_bytes")
          if (value > max_drain_credit_granted_bytes) {
            max_drain_credit_granted_bytes = value
          }
        } else if ($i ~ /^drain_credit_planned_bytes=/) {
          value = numeric_token($i, "drain_credit_planned_bytes")
          if (value > max_drain_credit_planned_bytes) {
            max_drain_credit_planned_bytes = value
          }
        } else if ($i ~ /^drain_credit_used_bytes=/) {
          value = numeric_token($i, "drain_credit_used_bytes")
          if (value > max_drain_credit_used_bytes) {
            max_drain_credit_used_bytes = value
          }
        } else if ($i ~ /^egress_payload_credit_bytes=/) {
          value = numeric_token($i, "egress_payload_credit_bytes")
          if (value > max_egress_payload_credit_bytes) {
            max_egress_payload_credit_bytes = value
          }
        } else if ($i ~ /^drop_credit_debt_bytes=/) {
          value = numeric_token($i, "drop_credit_debt_bytes")
          if (value > max_drop_credit_debt_bytes) {
            max_drop_credit_debt_bytes = value
          }
        } else if ($i ~ /^drop_credit_debt_paid_bytes=/) {
          value = numeric_token($i, "drop_credit_debt_paid_bytes")
          if (value > max_drop_credit_debt_paid_bytes) {
            max_drop_credit_debt_paid_bytes = value
          }
        } else if ($i ~ /^drop_credit_blocked_bytes=/) {
          value = numeric_token($i, "drop_credit_blocked_bytes")
          if (value > max_drop_credit_blocked_bytes) {
            max_drop_credit_blocked_bytes = value
          }
        } else if ($i ~ /^pressure_credit_debt_bytes=/) {
          value = numeric_token($i, "pressure_credit_debt_bytes")
          if (value > max_pressure_credit_debt_bytes) {
            max_pressure_credit_debt_bytes = value
          }
        } else if ($i ~ /^pressure_credit_debt_paid_bytes=/) {
          value = numeric_token($i, "pressure_credit_debt_paid_bytes")
          if (value > max_pressure_credit_debt_paid_bytes) {
            max_pressure_credit_debt_paid_bytes = value
          }
        } else if ($i ~ /^pressure_credit_blocked_bytes=/) {
          value = numeric_token($i, "pressure_credit_blocked_bytes")
          if (value > max_pressure_credit_blocked_bytes) {
            max_pressure_credit_blocked_bytes = value
          }
        } else if ($i ~ /^hard_edge_guard_bytes=/) {
          value = numeric_token($i, "hard_edge_guard_bytes")
          if (value > max_hard_edge_guard_bytes) {
            max_hard_edge_guard_bytes = value
          }
        } else if ($i ~ /^hard_edge_guard_limited_calls=/) {
          value = numeric_token($i, "hard_edge_guard_limited_calls")
          if (value > max_hard_edge_guard_limited) {
            max_hard_edge_guard_limited = value
          }
        } else if ($i ~ /^hard_edge_guard_deferred_bytes=/) {
          value = numeric_token($i, "hard_edge_guard_deferred_bytes")
          if (value > max_hard_edge_guard_deferred_bytes) {
            max_hard_edge_guard_deferred_bytes = value
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

    /tcp-terminal-remote-payload/ {
      line_bytes = 0
      line_total_bytes = 0
      line_events = 0
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^bytes=/) {
          line_bytes = numeric_token($i, "bytes")
        } else if ($i ~ /^total_bytes=/) {
          line_total_bytes = numeric_token($i, "total_bytes")
        } else if ($i ~ /^events=/) {
          line_events = numeric_token($i, "events")
        }
      }
      terminal_late_remote_payload_events++
      terminal_late_remote_payload_bytes += line_bytes
      if (line_total_bytes > max_terminal_late_remote_payload_bytes) {
        max_terminal_late_remote_payload_bytes = line_total_bytes
      }
      if (line_events > max_terminal_late_remote_payload_events) {
        max_terminal_late_remote_payload_events = line_events
      }
    }

    /tcp-reverse-window/ {
      reverse_window_events++
      line_send_capacity = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^payload_bytes=/) {
          reverse_window_payload_bytes += numeric_token($i, "payload_bytes")
        } else if ($i ~ /^accepted_bytes=/) {
          reverse_window_accepted_bytes += numeric_token($i, "accepted_bytes")
        } else if ($i ~ /^pending=/) {
          value = numeric_token($i, "pending")
          if (value > max_reverse_window_pending) {
            max_reverse_window_pending = value
          }
        } else if ($i ~ /^send_capacity=/) {
          line_send_capacity = numeric_token($i, "send_capacity")
          if (line_send_capacity > max_reverse_send_capacity) {
            max_reverse_send_capacity = line_send_capacity
          }
        } else if ($i ~ /^send_queue=/) {
          value = numeric_token($i, "send_queue")
          if (value > max_reverse_send_queue) {
            max_reverse_send_queue = value
          }
        } else if ($i ~ /^recv_queue=/) {
          value = numeric_token($i, "recv_queue")
          if (value > max_reverse_recv_queue) {
            max_reverse_recv_queue = value
          }
        } else if ($i == "may_recv=false") {
          reverse_window_may_recv_false++
        } else if ($i == "active=false") {
          reverse_window_active_false++
        } else if ($i == "can_send=false") {
          reverse_window_can_send_false++
        }
      }
      if (line_send_capacity != "" &&
          (min_reverse_send_capacity == "" || line_send_capacity < min_reverse_send_capacity)) {
        min_reverse_send_capacity = line_send_capacity
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
        } else if ($i ~ /^timer_active_flow_attempts=/) {
          value = numeric_token($i, "timer_active_flow_attempts")
          if (value > max_tun_rx_drain_timer_active_flow) {
            max_tun_rx_drain_timer_active_flow = value
          }
        } else if ($i ~ /^timer_stalled_read_attempts=/) {
          value = numeric_token($i, "timer_stalled_read_attempts")
          if (value > max_tun_rx_drain_timer_stalled_read) {
            max_tun_rx_drain_timer_stalled_read = value
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

    /tcp-relay-ack-drain-hint/ {
      relay_gap_hint_events++
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^gap_ms=/) {
          value = numeric_token($i, "gap_ms")
          if (value > max_relay_gap_hint_gap_ms) {
            max_relay_gap_hint_gap_ms = value
          }
        } else if ($i ~ /^budget=/) {
          value = numeric_token($i, "budget")
          if (value > max_relay_gap_hint_budget) {
            max_relay_gap_hint_budget = value
          }
        } else if ($i ~ /^cadence_floor=/) {
          value = numeric_token($i, "cadence_floor")
          if (value > max_relay_gap_hint_cadence_floor) {
            max_relay_gap_hint_cadence_floor = value
          }
          if (value > 0) {
            relay_gap_hint_cadence_events++
          }
        }
      }
    }

    /tcp-handle-close/ {
      pending = 0
      close_pending_bytes_token = ""
      close_pending_class = ""
      close_egress_bytes = 0
      close_egress_bytes_token = ""
      close_egress_class = ""
      close_egress_drain_candidate = 0
      has_close_egress_drain_candidate = 0
      send_queue = 0
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
        } else if ($i ~ /^close_egress_bytes=/) {
          close_egress_bytes_token = numeric_token($i, "close_egress_bytes")
        } else if ($i ~ /^close_egress_class=/) {
          close_egress_class = $i
          sub(/^close_egress_class=/, "", close_egress_class)
        } else if ($i == "close_egress_drain_candidate=true") {
          close_egress_drain_candidate = 1
          has_close_egress_drain_candidate = 1
        } else if ($i == "close_egress_drain_candidate=false") {
          close_egress_drain_candidate = 0
          has_close_egress_drain_candidate = 1
        } else if ($i ~ /^terminal_pending_reap_bytes=/) {
          terminal_pending = numeric_token($i, "terminal_pending_reap_bytes")
          has_terminal_pending = 1
        } else if ($i ~ /^terminal_late_remote_payload_bytes=/) {
          value = numeric_token($i, "terminal_late_remote_payload_bytes")
          if (value > max_terminal_late_remote_payload_close_bytes) {
            max_terminal_late_remote_payload_close_bytes = value
          }
          if (value > max_terminal_late_remote_payload_bytes) {
            max_terminal_late_remote_payload_bytes = value
          }
        } else if ($i ~ /^terminal_late_remote_payload_events=/) {
          value = numeric_token($i, "terminal_late_remote_payload_events")
          if (value > max_terminal_late_remote_payload_close_events) {
            max_terminal_late_remote_payload_close_events = value
          }
          if (value > max_terminal_late_remote_payload_events) {
            max_terminal_late_remote_payload_events = value
          }
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
        } else if ($i ~ /^send_queue=/) {
          send_queue = numeric_token($i, "send_queue")
        }
      }
      if (pending == 0 && close_pending_bytes_token != "") {
        pending = close_pending_bytes_token
      }
      if (close_egress_bytes_token != "") {
        close_egress_bytes = close_egress_bytes_token
      } else {
        close_egress_bytes = send_queue
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
      if (close_egress_class == "") {
        if (close_egress_bytes == 0) {
          close_egress_class = "none"
        } else if (closed_state && inactive_socket && no_send_capacity_socket) {
          close_egress_class = "terminal_closed_no_send"
        } else if (active_socket && no_send_capacity_socket) {
          close_egress_class = "active_no_send"
        } else if (active_socket && send_capacity_socket) {
          close_egress_class = "active_send_capable"
        } else if (inactive_socket && send_capacity_socket) {
          close_egress_class = "inactive_send_capable"
        } else if (inactive_socket && no_send_capacity_socket) {
          close_egress_class = "inactive_no_send"
        } else {
          close_egress_class = "unknown"
        }
      }
      if (!has_close_egress_drain_candidate &&
          close_egress_class == "active_send_capable") {
        close_egress_drain_candidate = 1
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
      if (close_egress_bytes > 0) {
        egress_at_close_events++
        egress_at_close_bytes += close_egress_bytes
        if (close_egress_bytes > max_egress_at_close_bytes) {
          max_egress_at_close_bytes = close_egress_bytes
        }
        if (close_egress_class == "terminal_closed_no_send") {
          egress_close_terminal_events++
          egress_close_terminal_bytes += close_egress_bytes
        } else if (close_egress_class == "active_no_send") {
          egress_close_active_no_send_events++
          egress_close_active_no_send_bytes += close_egress_bytes
        } else if (close_egress_class == "active_send_capable" || close_egress_class == "inactive_send_capable") {
          egress_close_send_capable_events++
          egress_close_send_capable_bytes += close_egress_bytes
        } else if (close_egress_class == "inactive_no_send") {
          egress_close_inactive_no_send_events++
          egress_close_inactive_no_send_bytes += close_egress_bytes
        } else if (close_egress_class == "unknown") {
          egress_close_unknown_events++
          egress_close_unknown_bytes += close_egress_bytes
        }
        if (close_egress_drain_candidate == 1) {
          egress_close_drain_candidate_events++
          egress_close_drain_candidate_bytes += close_egress_bytes
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
      max_remote_read_gap_before_finish_ms = 0
      max_remote_read_gap_after_finish_ms = 0
      first_local_finish_after_first_remote_read_ms = 0
      current_remote_read_gap_ms = 0
      has_first_remote_read_ms = 0
      has_max_remote_read_gap_ms = 0
      has_max_remote_read_gap_before_finish_ms = 0
      has_max_remote_read_gap_after_finish_ms = 0
      has_first_local_finish_after_first_remote_read_ms = 0
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
        } else if ($i ~ /^max_remote_read_gap_before_local_finish_ms=/) {
          max_remote_read_gap_before_finish_ms = numeric_token($i, "max_remote_read_gap_before_local_finish_ms")
          has_max_remote_read_gap_before_finish_ms = 1
        } else if ($i ~ /^max_remote_read_gap_after_local_finish_ms=/) {
          max_remote_read_gap_after_finish_ms = numeric_token($i, "max_remote_read_gap_after_local_finish_ms")
          has_max_remote_read_gap_after_finish_ms = 1
        } else if ($i ~ /^first_local_finish_after_first_remote_read_ms=/) {
          first_local_finish_after_first_remote_read_ms = numeric_token($i, "first_local_finish_after_first_remote_read_ms")
          has_first_local_finish_after_first_remote_read_ms = 1
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
      if (has_max_remote_read_gap_before_finish_ms && max_remote_read_gap_before_finish_ms > max_relay_remote_read_gap_before_finish_ms) {
        max_relay_remote_read_gap_before_finish_ms = max_remote_read_gap_before_finish_ms
      }
      if (has_max_remote_read_gap_after_finish_ms && max_remote_read_gap_after_finish_ms > max_relay_remote_read_gap_after_finish_ms) {
        max_relay_remote_read_gap_after_finish_ms = max_remote_read_gap_after_finish_ms
      }
      if (has_first_local_finish_after_first_remote_read_ms && first_local_finish_after_first_remote_read_ms > max_first_local_finish_after_first_remote_read_ms) {
        max_first_local_finish_after_first_remote_read_ms = first_local_finish_after_first_remote_read_ms
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
      if (has_max_remote_read_gap_before_finish_ms && max_remote_read_gap_before_finish_ms > relay_remote_read_gap_before_finish_ms[handle]) {
        relay_remote_read_gap_before_finish_ms[handle] = max_remote_read_gap_before_finish_ms
      }
      if (has_max_remote_read_gap_after_finish_ms && max_remote_read_gap_after_finish_ms > relay_remote_read_gap_after_finish_ms[handle]) {
        relay_remote_read_gap_after_finish_ms[handle] = max_remote_read_gap_after_finish_ms
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
      conn_rx_stream_frames_since_read = 0
      conn_rx_stream_frames_since_pending = 0
      pending_cause = ""
      has_pending_gap_ms = 0
      has_pending_polls = 0
      has_poll_count = 0
      has_poll_gap_ms = 0
      has_pending_rx_bytes = 0
      has_conn_rx_stream_frames_since_read = 0
      has_conn_rx_stream_frames_since_pending = 0
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
        } else if ($i ~ /^conn_rx_stream_frames_since_read=/) {
          conn_rx_stream_frames_since_read = numeric_token($i, "conn_rx_stream_frames_since_read")
          has_conn_rx_stream_frames_since_read = 1
          if (conn_rx_stream_frames_since_read > max_tuic_pending_conn_rx_stream_frames_since_read) {
            max_tuic_pending_conn_rx_stream_frames_since_read = conn_rx_stream_frames_since_read
          }
        } else if ($i ~ /^conn_rx_stream_frames_since_pending=/) {
          conn_rx_stream_frames_since_pending = numeric_token($i, "conn_rx_stream_frames_since_pending")
          has_conn_rx_stream_frames_since_pending = 1
          if (conn_rx_stream_frames_since_pending > max_tuic_pending_conn_rx_stream_frames_since_pending) {
            max_tuic_pending_conn_rx_stream_frames_since_pending = conn_rx_stream_frames_since_pending
          }
        } else if ($i ~ /^pending_cause=/) {
          pending_cause = string_token($i, "pending_cause")
        }
      }
      if (pending_cause == "connection_stream_frames_pending") {
        tuic_pending_cause_connection_stream_frames_pending++
        if (has_conn_rx_stream_frames_since_pending &&
            conn_rx_stream_frames_since_pending > max_tuic_connection_stream_conn_rx_stream_frames_since_pending) {
          max_tuic_connection_stream_conn_rx_stream_frames_since_pending = conn_rx_stream_frames_since_pending
        }
      } else if (pending_cause == "connection_fresh_stream_frames_pending") {
        tuic_pending_cause_connection_fresh_stream_frames_pending++
        if (has_conn_rx_stream_frames_since_pending &&
            conn_rx_stream_frames_since_pending > max_tuic_fresh_conn_rx_stream_frames_since_pending) {
          max_tuic_fresh_conn_rx_stream_frames_since_pending = conn_rx_stream_frames_since_pending
        }
        if (has_conn_rx_stream_frames_since_pending &&
            conn_rx_stream_frames_since_pending > 0) {
          tuic_fresh_nonzero_since_pending++
        }
      } else if (pending_cause == "connection_stale_stream_frames_pending") {
        tuic_pending_cause_connection_stale_stream_frames_pending++
        if (has_conn_rx_stream_frames_since_pending &&
            conn_rx_stream_frames_since_pending > max_tuic_stale_conn_rx_stream_frames_since_pending) {
          max_tuic_stale_conn_rx_stream_frames_since_pending = conn_rx_stream_frames_since_pending
        }
        if (has_conn_rx_stream_frames_since_pending &&
            conn_rx_stream_frames_since_pending == 0) {
          tuic_stale_zero_since_pending++
        }
      } else if (pending_cause == "connection_rx_no_stream_frames") {
        tuic_pending_cause_connection_rx_no_stream_frames++
      } else if (pending_cause == "no_connection_rx") {
        tuic_pending_cause_no_connection_rx++
      } else if (pending_cause == "no_transport_sample") {
        tuic_pending_cause_no_transport_sample++
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
        if (lost_delta > 0) {
          aggregate_lost_bytes_delta += lost_delta
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
      if (max_drop_credit_debt_bytes > 0 || max_drop_credit_debt_paid_bytes > 0 || max_drop_credit_blocked_bytes > 0) {
        add_label("local_drop_credit")
      }
      if (max_pressure_credit_debt_bytes > 0 || max_pressure_credit_debt_paid_bytes > 0 || max_pressure_credit_blocked_bytes > 0) {
        add_label("local_pressure_credit")
      }
      if (down_pause_count > 0) {
        add_label("local_downlink_backpressure")
      }
      if (global_receive_pause_count > 0) {
        add_label("local_global_rx_receive_window")
      }
      local_pressure = 0
      if (tun_tx_delta != "unknown" && (tun_tx_delta + 0) > 0) {
        local_pressure = 1
      }
      if (runtime_tun_drop_delta_total > 0 ||
          tun_feedback_pause_count > 0 ||
          down_pause_count > 0 ||
          global_receive_pause_count > 0 ||
          max_tun_flush_deferred > 0) {
        local_pressure = 1
      }
      tail_collapse_active = 0
      if (probe_kind == "tcp" && reverse_tcp == "1" && interval_tail_collapse == "1") {
        tail_collapse_active = 1
        add_label("iperf_tail_collapse")
        if (local_pressure > 0) {
          add_label("tail_collapse_local_pressure")
        }
      }
      if (global_rx_count > 0) {
        add_label("global_rx_backpressure")
      }
      if (max_late_remote_bytes > 0 || max_late_remote_reads > 0) {
        add_label("late_remote_after_local_finish")
      }
      if (terminal_late_remote_payload_events > 0 ||
          terminal_late_remote_payload_bytes > 0 ||
          max_terminal_late_remote_payload_bytes > 0) {
        add_label("terminal_late_remote_payload")
      }
      if (terminal_pending_events > 0) {
        add_label("terminal_pending_reap")
      }
      if (max_terminal_late_remote_payload_close_events > 0 ||
          max_terminal_late_remote_payload_close_bytes > 0) {
        add_label("terminal_closed_late_payload")
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
      if (egress_at_close_events > 0) {
        add_label("egress_at_close")
      }
      if (egress_close_active_no_send_events > 0) {
        add_label("egress_close_active_no_send")
      }
      if (egress_close_send_capable_events > 0) {
        add_label("egress_close_send_capable")
      }
      if (egress_close_inactive_no_send_events > 0) {
        add_label("egress_close_inactive_no_send")
      }
      if (egress_close_drain_candidate_events > 0) {
        add_label("egress_close_drain_candidate")
      }
      if (egress_close_unknown_events > 0) {
        add_label("egress_close_unknown")
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
          if (relay_remote_read_gap_before_finish_ms[handle] > max_relay_data_remote_read_gap_before_finish_ms) {
            max_relay_data_remote_read_gap_before_finish_ms = relay_remote_read_gap_before_finish_ms[handle]
          }
          if (relay_remote_read_gap_after_finish_ms[handle] > max_relay_data_remote_read_gap_after_finish_ms) {
            max_relay_data_remote_read_gap_after_finish_ms = relay_remote_read_gap_after_finish_ms[handle]
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
      clean_reverse_no_data = 0
      if (probe_kind == "tcp" && reverse_tcp == "1" &&
          sender != "unknown" && receiver != "unknown" &&
          (sender + 0) < 5 && (receiver + 0) < 5 &&
          local_write_count == 0 && down_pause_count == 0 && global_rx_count == 0 &&
          max_lost_bytes_delta == 0 && max_congestion_delta == 0 &&
          max_tx_data_delta == 0 && max_tx_stream_delta == 0 &&
          inherited_quic_congestion == 0 &&
          reconnect_count == 0) {
        clean_reverse_no_data = 1
      }
      if (probe_kind == "tcp" && reverse_tcp == "1" &&
          clean_reverse_no_data == 1 &&
          remote_timing_slow == 0) {
        add_label("reverse_sender_backpressured")
      }
      if (clean_reverse_no_data == 1) {
        add_label("target_sender_stalled")
        if (max_tuic_data_pending_gap_ms >= slow_rx_floor_ms &&
            max_tuic_data_rx_bytes > 0 &&
            max_tuic_data_rx_bytes < tuic_stream_starved_rx_ceiling) {
          add_label("tuic_stream_starved")
        }
      }
      throughput_shape = "unknown"
      no_data_shape = 0
      stable_high_shape = 0
      if (probe_kind == "tcp" && reverse_tcp == "1" && sender != "unknown" && receiver != "unknown") {
        if ((sender + 0) < 5 && (receiver + 0) < 5) {
          throughput_shape = "no_data"
          no_data_shape = 1
        } else if (tail_collapse_active > 0 && local_pressure > 0) {
          throughput_shape = "tail_collapse_local_pressure"
        } else if (tail_collapse_active > 0) {
          throughput_shape = "tail_collapse"
        } else if ((receiver + 0) >= 100 && interval_tail_samples > 0 && (interval_tail_avg + 0) >= 100) {
          throughput_shape = "stable_high"
          stable_high_shape = 1
        } else if ((receiver + 0) >= 100) {
          throughput_shape = "high_average_unclassified"
        } else {
          throughput_shape = "low_average"
        }
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
      if (selection_generations == "") {
        selection_generations = "none"
      }
      if (selection_probe_results == "") {
        selection_probe_results = "none"
      }
      if (selection_reconnect_reasons == "") {
        selection_reconnect_reasons = "none"
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
      if (min_reverse_send_capacity == "") {
        min_reverse_send_capacity = 0
      }
      if (min_stream_service_remote_poll_len_min == "") {
        min_stream_service_remote_poll_len_min = 0
      }
      if (stream_service_last_blocked_reason == "") {
        stream_service_last_blocked_reason = "none"
      }
      d2_flow_joinable = 0
      d2_flow_reason = "no_tuic_data_stream"
      if (tuic_data_streams > 0 && stream_service_windows == 0) {
        d2_flow_reason = "no_stream_service_window"
      } else if (tuic_data_streams > 0 && stream_service_windows > 0 &&
                 stream_service_tuic_bridge_events == 0) {
        d2_flow_reason = "missing_tuic_handle_epoch_bridge"
      } else if (tuic_data_streams > 0 && stream_service_windows > 0) {
        d2_flow_joinable = 1
        d2_flow_reason = "joined_by_bridge"
      }

      print "- metrics_title: " title
      print "- iperf_sender_mbps: " sender
      print "- iperf_receiver_mbps: " receiver
      printf "- iperf_interval_profile: samples=%d overall_avg_mbps=%.3f prefix_avg_mbps=%.3f tail_samples=%d tail_avg_mbps=%.3f tail_min_mbps=%.3f tail_collapse=%d\n", interval_samples, interval_avg, interval_prefix_avg, interval_tail_samples, interval_tail_avg, interval_tail_min, interval_tail_collapse
      printf "- throughput_shape: shape=%s tail_collapse=%d local_pressure=%d no_data=%d stable_high=%d\n", throughput_shape, tail_collapse_active, local_pressure, no_data_shape, stable_high_shape
      printf "- tcp_pool: opens=%d conns=%s reconnects=%d reasons=%s\n", open_count, open_conns, reconnect_count, reconnect_reasons
      printf "- tcp_pool_health: selections=%d generations=%s probe_results=%s reconnect_reasons=%s max_last_success_age_secs=%d\n", selection_count, selection_generations, selection_probe_results, selection_reconnect_reasons, max_last_success_age_secs
      printf "- local_write_pressure: events=%d max_wait_ms=%.3f max_payload_bytes=%d\n", local_write_count, max_local_wait_us / 1000, max_local_payload
      printf "- global_rx_pressure: events=%d max_wait_ms=%.3f queue_used_max=%d queue_capacity=%d\n", global_rx_count, max_global_wait_us / 1000, max_global_rx_queue_used, max_global_rx_queue_capacity
      printf "- global_rx_receive: pause_edges=%d resume_edges=%d max_pending_bytes=%d max_total_pending_bytes=%d max_tx_queue_bytes=%d receive_high=%d receive_low=%d receive_total_high=%d receive_total_low=%d local_egress_paused=%d tun_feedback_paused=%d\n", global_receive_pause_count, global_receive_resume_count, max_global_receive_pending, max_global_receive_total_pending, max_global_receive_tx_queue, max_global_receive_high, max_global_receive_low, max_global_receive_total_high, max_global_receive_total_low, global_receive_local_egress_paused, global_receive_tun_feedback_paused
      printf "- downlink_backpressure: pause_edges=%d resume_edges=%d max_pending_bytes=%d max_total_pending_bytes=%d max_tx_queue_bytes=%d max_total_tx_queue_bytes=%d max_pressure_bytes=%d max_total_pressure_bytes=%d\n", down_pause_count, down_resume_count, max_down_pending, max_down_total, max_down_tx_queue, max_down_total_tx_queue, max_down_pressure, max_down_total_pressure
      printf "- downlink_flush: attempts=%d no_send_capacity=%d send_window_samples=%d send_capacity_min=%d send_capacity_max=%d send_queue_max=%d recv_queue_max=%d may_send_false=%d may_recv_false=%d no_send_streak_max=%d no_send_pending_max=%d send_slice_calls=%d accepted_bytes=%d zero=%d errors=%d budget_limited=%d headroom_limited=%d headroom_deferred_bytes=%d drain_credit_granted_bytes=%d drain_credit_planned_bytes=%d drain_credit_used_bytes=%d egress_payload_credit_bytes=%d drop_credit_debt_bytes=%d drop_credit_debt_paid_bytes=%d drop_credit_blocked_bytes=%d pressure_credit_debt_bytes=%d pressure_credit_debt_paid_bytes=%d pressure_credit_blocked_bytes=%d hard_edge_guard_bytes=%d hard_edge_guard_limited=%d hard_edge_guard_deferred_bytes=%d max_accepted_bytes=%d tun_flush_calls=%d tun_flush_failures=%d tun_flush_deferred=%d pending_total_max=%d pending_max=%d pending_high=%d remote_to_global_rx_bytes=%d downstream_permit_bytes=%d downstream_permit_released_bytes=%d downstream_permit_pending_high=%d terminal_late_remote_payload_bytes=%d terminal_late_remote_payload_events=%d dirty_handles_max=%d\n", max_flush_attempts, max_no_send_capacity, max_send_window_samples, min_send_capacity_min, max_send_capacity_max, max_send_queue_max, max_recv_queue_max, max_may_send_false, max_may_recv_false, max_no_send_capacity_streak, max_no_send_capacity_pending, max_flush_send_calls, max_flush_accepted, max_flush_zero, max_flush_errors, max_budget_limited, max_headroom_limited, max_headroom_deferred_bytes, max_drain_credit_granted_bytes, max_drain_credit_planned_bytes, max_drain_credit_used_bytes, max_egress_payload_credit_bytes, max_drop_credit_debt_bytes, max_drop_credit_debt_paid_bytes, max_drop_credit_blocked_bytes, max_pressure_credit_debt_bytes, max_pressure_credit_debt_paid_bytes, max_pressure_credit_blocked_bytes, max_hard_edge_guard_bytes, max_hard_edge_guard_limited, max_hard_edge_guard_deferred_bytes, max_send_slice_max_accepted, max_tun_flush_calls, max_tun_flush_failures, max_tun_flush_deferred, max_flush_pending_total, max_flush_pending_max, max_flush_pending_high, max_flush_remote_bytes, max_downstream_permit_bytes, max_downstream_permit_released_bytes, max_downstream_permit_pending_high, max_flush_terminal_late_remote_bytes, max_flush_terminal_late_remote_events, max_dirty_handles
      printf "- stream_service_window: windows=%d remote_poll_ticks_max=%d remote_poll_len_min_min=%d remote_poll_len_max_max=%d remote_read_chunks_max=%d remote_read_bytes_max=%d global_rx_pressure_events_max=%d global_rx_queue_used_max=%d global_rx_queue_capacity_max=%d local_accepted_bytes_max=%d local_egress_drain_bytes_max=%d local_flush_tx_calls_max=%d local_flush_tx_failures_max=%d local_dirty_passes_max=%d pending_fresh_stream_frames_max=%d pending_stale_stream_frames_max=%d useful_progress_windows=%d last_blocked_reason=%s\n", stream_service_windows, max_stream_service_remote_poll_ticks, min_stream_service_remote_poll_len_min, max_stream_service_remote_poll_len_max, max_stream_service_remote_read_chunks, max_stream_service_remote_read_bytes, max_stream_service_global_rx_pressure_events, max_stream_service_global_rx_queue_used, max_stream_service_global_rx_queue_capacity, max_stream_service_local_accepted_bytes, max_stream_service_local_egress_drain_bytes, max_stream_service_local_flush_tx_calls, max_stream_service_local_flush_tx_failures, max_stream_service_local_dirty_passes, max_stream_service_pending_fresh_stream_frames, max_stream_service_pending_stale_stream_frames, stream_service_useful_progress_windows, stream_service_last_blocked_reason
      printf "- local_egress_service: windows_max=%d cycles_max=%d accepted_bytes_max=%d egress_drain_bytes_max=%d tun_rx_packets_max=%d flush_tx_calls_max=%d flush_tx_failures_max=%d dirty_passes_max=%d target_reached_max=%d no_progress_max=%d cycle_budget_max=%d hard_pause_max=%d no_work_max=%d egress_actor_windows_max=%d egress_actor_cycles_max=%d egress_actor_admitted_bytes_max=%d egress_actor_drain_bytes_max=%d egress_actor_no_progress_max=%d egress_actor_immediate_wake_max=%d egress_actor_self_wake_max=%d egress_actor_external_wait_max=%d\n", max_local_egress_service_windows, max_local_egress_service_cycles, max_local_egress_service_accepted_bytes, max_local_egress_service_egress_drain_bytes, max_local_egress_service_tun_rx_packets, max_local_egress_service_flush_tx_calls, max_local_egress_service_flush_tx_failures, max_local_egress_service_dirty_passes, max_local_egress_service_target_reached, max_local_egress_service_no_progress, max_local_egress_service_cycle_budget, max_local_egress_service_hard_pause, max_local_egress_service_no_work, max_local_egress_actor_windows, max_local_egress_actor_cycles, max_local_egress_actor_admitted_bytes, max_local_egress_actor_drain_bytes, max_local_egress_actor_no_progress, max_local_egress_actor_immediate_wake, max_local_egress_actor_self_wake, max_local_egress_actor_external_wait
      printf "- d2_flow_timeline: joinable=%d reason=%s tuic_data_streams=%d stream_service_windows=%d data_poll_gap_max_ms=%d data_read_gap_max_ms=%d data_pending_gap_max_ms=%d local_accepted_bytes_max=%d local_egress_drain_bytes_max=%d pressure_credit_debt_bytes=%d headroom_limited=%d tun_flush_deferred=%d\n", d2_flow_joinable, d2_flow_reason, tuic_data_streams, stream_service_windows, max_tuic_data_poll_gap_ms, max_tuic_data_read_gap_ms, max_tuic_data_pending_gap_ms, max_stream_service_local_accepted_bytes, max_stream_service_local_egress_drain_bytes, max_pressure_credit_debt_bytes, max_headroom_limited, max_tun_flush_deferred
      printf "- tcp_lifecycle: transitions=%d closed_edges=%d terminal_candidates=%d max_pending_bytes=%d max_remote_to_global_rx_bytes=%d sources=%s states=%s\n", lifecycle_transitions, lifecycle_closed_edges, lifecycle_terminal_candidates, max_lifecycle_pending, max_lifecycle_remote_bytes, lifecycle_sources, lifecycle_states
      printf "- tcp_reverse_window: events=%d payload_bytes=%d accepted_bytes=%d pending_max=%d send_capacity_min=%d send_capacity_max=%d send_queue_max=%d recv_queue_max=%d may_recv_false=%d active_false=%d can_send_false=%d\n", reverse_window_events, reverse_window_payload_bytes, reverse_window_accepted_bytes, max_reverse_window_pending, min_reverse_send_capacity, max_reverse_send_capacity, max_reverse_send_queue, max_reverse_recv_queue, reverse_window_may_recv_false, reverse_window_active_false, reverse_window_can_send_false
      printf "- terminal_pending_reap: events=%d bytes=%d max_bytes=%d\n", terminal_pending_events, terminal_pending_bytes, max_terminal_pending_bytes
      printf "- terminal_late_remote_payload: events=%d bytes=%d max_bytes=%d close_max_bytes=%d close_max_events=%d\n", terminal_late_remote_payload_events, terminal_late_remote_payload_bytes, max_terminal_late_remote_payload_bytes, max_terminal_late_remote_payload_close_bytes, max_terminal_late_remote_payload_close_events
      printf "- pending_at_close: events=%d bytes=%d max_bytes=%d terminal_events=%d terminal_bytes=%d active_no_send_events=%d active_no_send_bytes=%d send_capable_events=%d send_capable_bytes=%d inactive_no_send_events=%d inactive_no_send_bytes=%d unknown_events=%d unknown_bytes=%d\n", pending_at_close_events, pending_at_close_bytes, max_pending_at_close_bytes, terminal_pending_events, terminal_pending_bytes, pending_close_active_no_send_events, pending_close_active_no_send_bytes, pending_close_send_capable_events, pending_close_send_capable_bytes, pending_close_inactive_no_send_events, pending_close_inactive_no_send_bytes, pending_close_unknown_events, pending_close_unknown_bytes
      printf "- egress_at_close: events=%d bytes=%d max_bytes=%d terminal_events=%d terminal_bytes=%d active_no_send_events=%d active_no_send_bytes=%d send_capable_events=%d send_capable_bytes=%d inactive_no_send_events=%d inactive_no_send_bytes=%d drain_candidate_events=%d drain_candidate_bytes=%d unknown_events=%d unknown_bytes=%d\n", egress_at_close_events, egress_at_close_bytes, max_egress_at_close_bytes, egress_close_terminal_events, egress_close_terminal_bytes, egress_close_active_no_send_events, egress_close_active_no_send_bytes, egress_close_send_capable_events, egress_close_send_capable_bytes, egress_close_inactive_no_send_events, egress_close_inactive_no_send_bytes, egress_close_drain_candidate_events, egress_close_drain_candidate_bytes, egress_close_unknown_events, egress_close_unknown_bytes
      printf "- relay_late_remote: post_finish_bytes=%d post_finish_reads=%d local_finish_events=%d\n", max_late_remote_bytes, max_late_remote_reads, local_finish_count
      printf "- relay_remote_timing: first_read_max_ms=%d max_read_gap_ms=%d current_gap_max_ms=%d no_first_read_gap_max_ms=%d data_streams=%d data_first_read_max_ms=%d data_max_read_gap_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d max_read_gap_before_finish_ms=%d max_read_gap_after_finish_ms=%d data_max_read_gap_before_finish_ms=%d data_max_read_gap_after_finish_ms=%d first_local_finish_after_first_remote_read_max_ms=%d\n", max_relay_first_remote_read_ms, max_relay_remote_read_gap_ms, max_relay_current_remote_gap_ms, max_relay_no_first_remote_gap_ms, relay_data_streams, max_relay_data_first_remote_read_ms, max_relay_data_remote_read_gap_ms, max_relay_data_remote_bytes, data_stream_min_rx_bytes, max_relay_remote_read_gap_before_finish_ms, max_relay_remote_read_gap_after_finish_ms, max_relay_data_remote_read_gap_before_finish_ms, max_relay_data_remote_read_gap_after_finish_ms, max_first_local_finish_after_first_remote_read_ms
      printf "- relay_gap_hints: events=%d max_gap_ms=%d max_budget=%d cadence_events=%d max_cadence_floor=%d\n", relay_gap_hint_events, max_relay_gap_hint_gap_ms, max_relay_gap_hint_budget, relay_gap_hint_cadence_events, max_relay_gap_hint_cadence_floor
      printf "- tuic_tcp_stream: first_rx_events=%d first_rx_max_ms=%d read_gap_events=%d read_gap_max_ms=%d close_events=%d close_first_rx_max_ms=%d close_gap_max_ms=%d rx_bytes_max=%d reads_max=%d zero_rx_closes=%d data_streams=%d data_first_rx_max_ms=%d data_read_gap_max_ms=%d data_close_gap_max_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", tuic_first_rx_events, max_tuic_first_rx_ms, tuic_read_gap_events, max_tuic_read_gap_ms, tuic_stream_close_events, max_tuic_close_first_rx_ms, max_tuic_close_gap_ms, max_tuic_close_rx_bytes, max_tuic_close_reads, tuic_zero_rx_closes, tuic_data_streams, max_tuic_data_first_rx_ms, max_tuic_data_read_gap_ms, max_tuic_data_close_gap_ms, max_tuic_data_rx_bytes, data_stream_min_rx_bytes
      printf "- tuic_stream_pending: events=%d max_pending_gap_ms=%d pending_polls_max=%d data_streams=%d data_pending_gap_max_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", tuic_pending_events, max_tuic_pending_gap_ms, max_tuic_pending_polls, tuic_data_streams, max_tuic_data_pending_gap_ms, max_tuic_data_rx_bytes, data_stream_min_rx_bytes
      printf "- tuic_stream_pending_causes: connection_stream_frames_pending=%d connection_fresh_stream_frames_pending=%d connection_stale_stream_frames_pending=%d connection_rx_no_stream_frames=%d no_connection_rx=%d no_transport_sample=%d\n", tuic_pending_cause_connection_stream_frames_pending, tuic_pending_cause_connection_fresh_stream_frames_pending, tuic_pending_cause_connection_stale_stream_frames_pending, tuic_pending_cause_connection_rx_no_stream_frames, tuic_pending_cause_no_connection_rx, tuic_pending_cause_no_transport_sample
      printf "- tuic_stream_frame_delta: pending_events=%d conn_rx_stream_frames_since_read_max=%d conn_rx_stream_frames_since_pending_max=%d fresh_since_pending_max=%d stale_since_pending_max=%d connection_stream_since_pending_max=%d fresh_nonzero_since_pending=%d stale_zero_since_pending=%d\n", tuic_pending_events, max_tuic_pending_conn_rx_stream_frames_since_read, max_tuic_pending_conn_rx_stream_frames_since_pending, max_tuic_fresh_conn_rx_stream_frames_since_pending, max_tuic_stale_conn_rx_stream_frames_since_pending, max_tuic_connection_stream_conn_rx_stream_frames_since_pending, tuic_fresh_nonzero_since_pending, tuic_stale_zero_since_pending
      printf "- tuic_stream_polling: polls_max=%d max_poll_gap_ms=%d data_streams=%d data_polls_max=%d data_poll_gap_max_ms=%d data_rx_bytes_max=%d data_rx_min_bytes=%d\n", max_tuic_polls, max_tuic_poll_gap_ms, tuic_data_streams, max_tuic_data_polls, max_tuic_data_poll_gap_ms, max_tuic_data_rx_bytes, data_stream_min_rx_bytes
      printf "- tun_drops: if=%s tun_rx_dropped_delta=%s tun_tx_dropped_delta=%s\n", tun_if, tun_rx_delta, tun_tx_delta
      printf "- runtime_tun_egress: samples=%d drop_events=%d drop_delta_total=%d max_delta=%d unavailable=%d resets=%d\n", runtime_tun_samples, runtime_tun_drop_events, runtime_tun_drop_delta_total, max_runtime_tun_delta, runtime_tun_unavailable, runtime_tun_resets
      printf "- tun_egress_feedback: pause_edges=%d resume_edges=%d drop_events=%d drop_delta_total=%d max_delta=%d max_pressure_bytes=%d\n", tun_feedback_pause_count, tun_feedback_resume_count, tun_feedback_drop_events, tun_feedback_drop_delta_total, max_tun_feedback_delta, max_tun_feedback_pressure
      printf "- tun_rx_drain: attempts=%d packets=%d timer_active_flow=%d timer_stalled_read=%d tcp=%d dns=%d udp=%d budget_exhausted=%d would_block=%d errors=%d\n", max_tun_rx_drain_attempts, max_tun_rx_drain_packets, max_tun_rx_drain_timer_active_flow, max_tun_rx_drain_timer_stalled_read, max_tun_rx_drain_tcp, max_tun_rx_drain_dns, max_tun_rx_drain_udp, max_tun_rx_drain_budget_exhausted, max_tun_rx_drain_would_block, max_tun_rx_drain_errors
      printf "- quic: samples=%d worst_conn=%s max_lost_bytes_delta=%d aggregate_lost_bytes_delta=%d max_congestion_events_delta=%d max_start_lost_bytes=%d max_start_congestion_events=%d min_start_cwnd=%s min_cwnd=%s inherited_conns=%s inherited_low_cwnd_threshold=%d max_tx_blocked_data_delta=%d max_tx_blocked_stream_delta=%d max_rx_blocked_data_delta=%d max_rx_blocked_stream_delta=%d\n", quic_samples, worst_conn, max_lost_bytes_delta, aggregate_lost_bytes_delta, max_congestion_delta, max_start_lost_bytes, max_start_congestion_events, min_start_cwnd_all, min_cwnd_all, inherited_quic_conns, inherited_low_cwnd_limit, max_tx_data_delta, max_tx_stream_delta, max_rx_data_delta, max_rx_stream_delta
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
🔎 tuic-tcp-pool-probe conn=3 id=99 generation=2 idle_age_secs=12 result=alive
🔎 tuic-tcp-pool-selection conn=3 id=99 generation=2 last_success_age_secs=12 probe_result=alive reconnect_reason=none
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99 stream=8 relay_mode=ordered_join startup_auth_attempts=0 handle=SocketHandle(1) epoch=1
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=90000 lost=0/12 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-local-write-pressure handle=SocketHandle(1) wait_us=3607684 payload_bytes=64960 pressure_events=1
🔎 tcp-downlink-backpressure paused=true max_pending=2151649 total_pending=2151649 max_tx_queue=1048576 total_tx_queue=1048576 max_pressure=2151649 total_pressure=3200225 high=2097120 low=524280
🔎 tcp-downlink-backpressure paused=false max_pending=524233 total_pending=524233 max_tx_queue=524280 total_tx_queue=524280 max_pressure=524280 total_pressure=1048513 high=2097120 low=524280
🔎 tcp-global-rx-backpressure paused=true max_pending=8388480 total_pending=8388480 max_tx_queue=1048576 total_tx_queue=1048576 max_pressure=8388480 total_pressure=9437056 receive_high=8388480 receive_low=2097120 receive_total_high=16776960 receive_total_low=4194240 local_egress_paused=true tun_feedback_paused=false
🔎 tcp-global-rx-backpressure paused=false max_pending=2097120 total_pending=2097120 max_tx_queue=524280 total_tx_queue=524280 max_pressure=2097120 total_pressure=2611400 receive_high=8388480 receive_low=2097120 receive_total_high=16776960 receive_total_low=4194240 local_egress_paused=false tun_feedback_paused=false
🔎 tcp-downlink-flush pending_total=524233 pending_max=524233 pending_high=2151649 remote_to_global_rx_bytes=3145728 downstream_permit_bytes=3145728 downstream_permit_released_bytes=2621440 downstream_permit_pending_high=524233 terminal_late_remote_payload_bytes=128 terminal_late_remote_payload_events=1 flush_attempts=23 no_send_capacity=7 send_window_samples=23 send_capacity_min=0 send_capacity_max=1048576 send_queue_max=1048576 recv_queue_max=4096 may_send_false=1 may_recv_false=1 no_send_capacity_streak_max=7 no_send_capacity_pending_max=524233 send_slice_calls=16 send_slice_accepted=2621440 send_slice_zero=2 send_slice_errors=0 budget_limited_calls=9 headroom_limited_calls=4 headroom_deferred_bytes=4096 drain_credit_granted_bytes=8192 drain_credit_planned_bytes=4096 drain_credit_used_bytes=2048 egress_payload_credit_bytes=12345 drop_credit_debt_bytes=1024 drop_credit_debt_paid_bytes=512 drop_credit_blocked_bytes=1536 pressure_credit_debt_bytes=2048 pressure_credit_debt_paid_bytes=256 pressure_credit_blocked_bytes=768 hard_edge_guard_bytes=24576 hard_edge_guard_limited_calls=2 hard_edge_guard_deferred_bytes=8192 send_slice_max_accepted=262144 tun_flush_tx_calls=14 tun_flush_tx_failures=1 tun_flush_deferred=3 dirty_handles=1
🔎 tcp-relay-ack-drain-hint handle=SocketHandle(1) epoch=7 gap_ms=7002 remote_to_global_rx_bytes=3145728 budget=240 cadence_floor=15340
🔎 tcp-tun-rx-drain attempts=4 pre_payload_attempts=0 remote_payload_attempts=1 remote_payload_deferred_attempts=1 remote_payload_deferred_delayed_attempts=1 remote_payload_deferred_pressure_attempts=0 relay_gap_hint_attempts=1 relay_gap_hint_followup_attempts=0 maintenance_attempts=0 timer_active_flow_attempts=0 timer_stalled_read_attempts=2 other_attempts=0 packets=11 tcp=9 dns=1 udp=1 budget_exhausted=1 would_block=3 errors=0
🔎 tcp-tun-egress if=tun0 status=delta tx_dropped_total=1523 tx_dropped_delta=1423 global_rx_paused=true pending_total=524233 pending_max=524233 pending_high=2151649 remote_to_global_rx_bytes=3145728 tun_flush_tx_calls=14 dirty_handles=1
🔎 tcp-lifecycle-transition handle=SocketHandle(1) source=dead_slot_reap prev_source=remote_payload prev_observed_secs=10 observed_secs=15 prev_state=Established state=Closed prev_active=true active=false prev_can_send=true can_send=false prev_can_recv=true can_recv=false prev_may_send=true may_send=false prev_may_recv=true may_recv=false prev_send_queue=65536 send_queue=0 prev_recv_queue=0 recv_queue=0 ctx_state=Relaying uplink_tx=true local_fin_sent=false local_fin_pending_since=none local_fin_last_remote_progress=none pending=4096 pending_high=2151649 remote_to_global_rx_bytes=3145728 send_slice_accepted=2621440 flush_attempts=23 terminal_candidate=true
🔎 tcp-terminal-remote-payload handle=SocketHandle(1) bytes=128 total_bytes=128 events=1 pending=4096 tcp_state=Closed active=false can_send=false can_recv=false may_send=false may_recv=false send_capacity=1048576 send_queue=0 recv_queue=0
🔎 tcp-handle-close handle=SocketHandle(1) direction=local reason=dead_slot_reap state=Relaying pending=4096 pending_high=2151649 remote_to_global_rx_bytes=3145728 terminal_late_remote_payload_bytes=128 terminal_late_remote_payload_events=1 flush_attempts=23 no_send_capacity=7 send_slice_calls=16 send_slice_accepted=2621440 send_slice_zero=2 send_slice_errors=0 budget_limited_calls=9 send_slice_max_accepted=262144 tun_flush_tx_calls=14 tun_flush_tx_failures=1 tun_flush_deferred=3 close_pending_class=terminal_closed_no_send close_pending_bytes=4096 terminal_pending_reap_bytes=4096 tcp_state=Closed active=false can_send=false can_recv=false
🔎 tcp-handle-close handle=SocketHandle(2) direction=local_to_remote reason=uplink_channel_closed state=Relaying pending=8192 pending_high=589159 remote_to_global_rx_bytes=692415971 terminal_late_remote_payload_bytes=0 terminal_late_remote_payload_events=0 flush_attempts=42 no_send_capacity=11 send_slice_calls=31 send_slice_accepted=691826812 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=17 send_slice_max_accepted=262144 tun_flush_tx_calls=29 tun_flush_tx_failures=0 tun_flush_deferred=0 close_pending_class=active_no_send close_pending_bytes=8192 terminal_pending_reap_bytes=0 tcp_state=Established active=true can_send=false can_recv=false
🔎 tcp-handle-close handle=SocketHandle(4) direction=local_to_remote reason=uplink_channel_closed state=Relaying pending=0 pending_high=65536 remote_to_global_rx_bytes=57959636 terminal_late_remote_payload_bytes=0 terminal_late_remote_payload_events=0 flush_attempts=7157 no_send_capacity=0 send_slice_calls=7157 send_slice_accepted=57959636 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=0 send_slice_max_accepted=65536 tun_flush_tx_calls=7094 tun_flush_tx_failures=0 tun_flush_deferred=63 close_pending_class=none close_pending_bytes=0 terminal_pending_reap_bytes=0 close_egress_class=active_send_capable close_egress_bytes=524288 close_egress_drain_candidate=true tcp_state=CloseWait active=true can_send=true can_recv=false may_send=true may_recv=false send_capacity=1048576 send_queue=524288 recv_queue=0
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

  if ! declare -F forward_discriminator_report_passes >/dev/null; then
    echo "lowrtt probe self-test failed: forward discriminator report gate missing" >&2
    exit 1
  fi
  if ! declare -F routing_expectation_note >/dev/null; then
    echo "lowrtt probe self-test failed: routing expectation helper missing" >&2
    exit 1
  fi

  local discriminator_report
  discriminator_report="$tmpdir/forward-discriminator.md"
  cat > "$discriminator_report" <<'EOF_DISCRIMINATOR'
[  5]   0.00-20.00  sec   420 MBytes   176 Mbits/sec                  receiver
- tun_drops: if=tun0 tun_rx_dropped_delta=0 tun_tx_dropped_delta=0
- quic: samples=3 worst_conn=1 max_lost_bytes_delta=12582912 aggregate_lost_bytes_delta=12582912 max_congestion_events_delta=200
EOF_DISCRIMINATOR
  forward_discriminator_report_passes "$discriminator_report"
  local aggregate_excess_report
  aggregate_excess_report="$tmpdir/forward-discriminator-aggregate-excess.md"
  cat > "$aggregate_excess_report" <<'EOF_AGGREGATE_EXCESS'
[  5]   0.00-20.00  sec   420 MBytes   176 Mbits/sec                  receiver
- tun_drops: if=tun0 tun_rx_dropped_delta=0 tun_tx_dropped_delta=0
- quic: samples=4 worst_conn=0:11 max_lost_bytes_delta=9437184 aggregate_lost_bytes_delta=18874368 max_congestion_events_delta=2
EOF_AGGREGATE_EXCESS
  if forward_discriminator_report_passes "$aggregate_excess_report"; then
    echo "lowrtt probe self-test failed: forward discriminator accepted aggregate pool QUIC loss above the budget" >&2
    exit 1
  fi
  sed -i.bak 's/tun_tx_dropped_delta=0/tun_tx_dropped_delta=1/' "$discriminator_report"
  if forward_discriminator_report_passes "$discriminator_report"; then
    echo "lowrtt probe self-test failed: forward discriminator accepted a TUN TX drop" >&2
    exit 1
  fi
  sed -i.bak 's/tun_tx_dropped_delta=1/tun_tx_dropped_delta=0/; s/max_lost_bytes_delta=12582912/max_lost_bytes_delta=16777217/; s/aggregate_lost_bytes_delta=12582912/aggregate_lost_bytes_delta=16777217/' "$discriminator_report"
  if forward_discriminator_report_passes "$discriminator_report"; then
    echo "lowrtt probe self-test failed: forward discriminator accepted excess QUIC loss" >&2
    exit 1
  fi
  sed -i.bak '/receiver$/d; s/max_lost_bytes_delta=16777217/max_lost_bytes_delta=0/; s/aggregate_lost_bytes_delta=16777217/aggregate_lost_bytes_delta=0/' "$discriminator_report"
  if forward_discriminator_report_passes "$discriminator_report"; then
    echo "lowrtt probe self-test failed: forward discriminator accepted a missing receiver result" >&2
    exit 1
  fi
  assert_contains "$(routing_expectation_note target-only)" "not applicable"
  assert_not_contains "$(routing_expectation_note target-only)" "must be the exit IP"
  assert_contains "$(routing_expectation_note full-tunnel)" "must be the exit IP"

  local transfer_spec
  transfer_spec="$(IPERF_BYTES=64M DURATION=20 tcp_transfer_spec)"
  assert_contains "$transfer_spec" "-n 64M"
  transfer_spec="$(IPERF_BYTES= DURATION=20 tcp_transfer_spec)"
  assert_contains "$transfer_spec" "-t 20"

  summary="$(summarize_metrics_window 0 "self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_receiver_mbps: 191.000"
  assert_contains "$summary" "tcp_pool: opens=1 conns=3 reconnects=1 reasons=stale_tcp_pool_slot"
  assert_contains "$summary" "tcp_pool_health: selections=1 generations=2 probe_results=alive reconnect_reasons=none max_last_success_age_secs=12"
  assert_contains "$summary" "local_write_pressure: events=1 max_wait_ms=3607.684"
  assert_contains "$summary" "global_rx_pressure: events=0 max_wait_ms=0.000 queue_used_max=0 queue_capacity=0"
  assert_contains "$summary" "global_rx_receive: pause_edges=1 resume_edges=1 max_pending_bytes=8388480 max_total_pending_bytes=8388480 max_tx_queue_bytes=1048576 receive_high=8388480 receive_low=2097120 receive_total_high=16776960 receive_total_low=4194240 local_egress_paused=1 tun_feedback_paused=0"
  assert_contains "$summary" "downlink_backpressure: pause_edges=1 resume_edges=1 max_pending_bytes=2151649 max_total_pending_bytes=2151649 max_tx_queue_bytes=1048576 max_total_tx_queue_bytes=1048576 max_pressure_bytes=2151649 max_total_pressure_bytes=3200225"
  assert_contains "$summary" "downlink_flush: attempts=23 no_send_capacity=7 send_window_samples=23 send_capacity_min=0 send_capacity_max=1048576 send_queue_max=1048576 recv_queue_max=4096 may_send_false=1 may_recv_false=1 no_send_streak_max=7 no_send_pending_max=524233 send_slice_calls=16 accepted_bytes=2621440 zero=2 errors=0 budget_limited=9 headroom_limited=4 headroom_deferred_bytes=4096 drain_credit_granted_bytes=8192 drain_credit_planned_bytes=4096 drain_credit_used_bytes=2048 egress_payload_credit_bytes=12345 drop_credit_debt_bytes=1024 drop_credit_debt_paid_bytes=512 drop_credit_blocked_bytes=1536 pressure_credit_debt_bytes=2048 pressure_credit_debt_paid_bytes=256 pressure_credit_blocked_bytes=768 hard_edge_guard_bytes=24576 hard_edge_guard_limited=2 hard_edge_guard_deferred_bytes=8192 max_accepted_bytes=262144 tun_flush_calls=14 tun_flush_failures=1 tun_flush_deferred=3"
  assert_contains "$summary" "remote_to_global_rx_bytes=3145728 downstream_permit_bytes=3145728 downstream_permit_released_bytes=2621440 downstream_permit_pending_high=524233"
  assert_contains "$summary" "terminal_late_remote_payload_bytes=128 terminal_late_remote_payload_events=1"
  assert_contains "$summary" "tcp_lifecycle: transitions=1 closed_edges=1 terminal_candidates=1 max_pending_bytes=4096 max_remote_to_global_rx_bytes=3145728 sources=dead_slot_reap states=Closed"
  assert_contains "$summary" "terminal_pending_reap: events=1 bytes=4096 max_bytes=4096"
  assert_contains "$summary" "terminal_late_remote_payload: events=1 bytes=128 max_bytes=128 close_max_bytes=128 close_max_events=1"
  assert_contains "$summary" "pending_at_close: events=2 bytes=12288 max_bytes=8192 terminal_events=1 terminal_bytes=4096 active_no_send_events=1 active_no_send_bytes=8192 send_capable_events=0 send_capable_bytes=0 inactive_no_send_events=0 inactive_no_send_bytes=0 unknown_events=0 unknown_bytes=0"
  assert_contains "$summary" "egress_at_close: events=1 bytes=524288 max_bytes=524288 terminal_events=0 terminal_bytes=0 active_no_send_events=0 active_no_send_bytes=0 send_capable_events=1 send_capable_bytes=524288 inactive_no_send_events=0 inactive_no_send_bytes=0 drain_candidate_events=1 drain_candidate_bytes=524288 unknown_events=0 unknown_bytes=0"
  assert_contains "$summary" "relay_remote_timing: first_read_max_ms=0 max_read_gap_ms=0 current_gap_max_ms=0 no_first_read_gap_max_ms=0"
  assert_contains "$summary" "relay_gap_hints: events=1 max_gap_ms=7002 max_budget=240 cadence_events=1 max_cadence_floor=15340"
  assert_contains "$summary" "tuic_tcp_stream: first_rx_events=0 first_rx_max_ms=0 read_gap_events=0 read_gap_max_ms=0 close_events=0 close_first_rx_max_ms=0 close_gap_max_ms=0 rx_bytes_max=0 reads_max=0 zero_rx_closes=0"
  assert_contains "$summary" "tuic_stream_pending: events=0 max_pending_gap_ms=0 pending_polls_max=0 data_streams=0 data_pending_gap_max_ms=0 data_rx_bytes_max=0"
  assert_contains "$summary" "tuic_stream_pending_causes: connection_stream_frames_pending=0 connection_fresh_stream_frames_pending=0 connection_stale_stream_frames_pending=0 connection_rx_no_stream_frames=0 no_connection_rx=0 no_transport_sample=0"
  assert_contains "$summary" "tuic_stream_polling: polls_max=0 max_poll_gap_ms=0 data_streams=0 data_polls_max=0 data_poll_gap_max_ms=0 data_rx_bytes_max=0"
  assert_contains "$summary" "runtime_tun_egress: samples=1 drop_events=1 drop_delta_total=1423 max_delta=1423 unavailable=0 resets=0"
  assert_contains "$summary" "tun_rx_drain: attempts=4 packets=11 timer_active_flow=0 timer_stalled_read=2 tcp=9 dns=1 udp=1 budget_exhausted=1 would_block=3 errors=0"
  assert_contains "$summary" "max_lost_bytes_delta=439956"
  assert_contains "$summary" "aggregate_lost_bytes_delta=439956"
  assert_contains "$summary" "attribution: quic_loss_congestion+local_write_pressure+local_tun_egress_drop+local_drop_credit+local_pressure_credit+local_downlink_backpressure+local_global_rx_receive_window+terminal_late_remote_payload+terminal_pending_reap"
  assert_contains "$summary" "pending_at_close+pending_close_active_no_send"
  assert_contains "$summary" "egress_at_close+egress_close_send_capable+egress_close_drain_candidate"

  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99 stream=8 relay_mode=ordered_join startup_auth_attempts=0 handle=SocketHandle(1) epoch=1
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=90000 lost=10/120 lost_bytes=1000 congestion_events=5 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=3 id=99 rtt=1ms cwnd=70000 lost=15/180 lost_bytes=1500 congestion_events=8 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "existing-conn-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "max_lost_bytes_delta=500"
  assert_contains "$summary" "aggregate_lost_bytes_delta=500"
  assert_contains "$summary" "max_congestion_events_delta=3"

  cat > "$log_sample" <<'EOF_LOG'
📊 TUIC QUIC stats conn=0 id=100 rtt=1ms cwnd=90000 lost=0/100 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=1 id=101 rtt=1ms cwnd=90000 lost=0/100 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=10/1000B udp_rx=2/200B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=0 id=100 rtt=1ms cwnd=80000 lost=10/200 lost_bytes=9437184 congestion_events=1 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=20/2000B udp_rx=4/400B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=1 id=101 rtt=1ms cwnd=80000 lost=10/200 lost_bytes=9437184 congestion_events=1 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=20/2000B udp_rx=4/400B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "aggregate-pool-loss-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "max_lost_bytes_delta=9437184"
  assert_contains "$summary" "aggregate_lost_bytes_delta=18874368"

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
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-1.00   sec  32.0 MBytes   268 Mbits/sec
[  5]   1.00-2.00   sec  20.0 MBytes   168 Mbits/sec
[  5]   2.00-3.00   sec  17.2 MBytes   145 Mbits/sec
[  5]   3.00-4.00   sec  24.8 MBytes   208 Mbits/sec
[  5]   4.00-5.00   sec  17.0 MBytes   143 Mbits/sec
[  5]   5.00-6.00   sec  26.0 MBytes   218 Mbits/sec
[  5]   6.00-7.00   sec  15.2 MBytes   128 Mbits/sec
[  5]   7.00-8.00   sec  25.5 MBytes   214 Mbits/sec
[  5]   8.00-9.00   sec  15.5 MBytes   130 Mbits/sec
[  5]   9.00-10.00  sec  28.2 MBytes   237 Mbits/sec
[  5]  10.00-11.00  sec  22.0 MBytes   185 Mbits/sec
[  5]  11.00-12.00  sec  19.2 MBytes   162 Mbits/sec
[  5]  12.00-13.00  sec  24.4 MBytes   204 Mbits/sec
[  5]  13.00-14.00  sec  20.2 MBytes   170 Mbits/sec
[  5]  14.00-15.00  sec  22.6 MBytes   190 Mbits/sec
[  5]  15.00-16.00  sec  15.0 MBytes   126 Mbits/sec
[  5]  16.00-17.00  sec  23.2 MBytes   195 Mbits/sec
[  5]  17.00-18.00  sec  23.1 MBytes   194 Mbits/sec
[  5]  18.00-19.00  sec  23.6 MBytes   198 Mbits/sec
[  5]  19.00-20.00  sec  22.1 MBytes   186 Mbits/sec
[  5]  20.00-21.00  sec  21.8 MBytes   182 Mbits/sec
[  5]  21.00-22.00  sec  22.1 MBytes   186 Mbits/sec
[  5]  22.00-23.00  sec  21.5 MBytes   180 Mbits/sec
[  5]  23.00-24.00  sec  17.1 MBytes   144 Mbits/sec
[  5]  24.00-25.00  sec  2.00 MBytes  16.8 Mbits/sec
[  5]  25.00-26.00  sec  1.88 MBytes  15.7 Mbits/sec
[  5]  26.00-27.00  sec  2.00 MBytes  16.8 Mbits/sec
[  5]  27.00-28.00  sec  1.88 MBytes  15.7 Mbits/sec
[  5]  28.00-29.00  sec  1.88 MBytes  15.7 Mbits/sec
[  5]  29.00-30.00  sec  2.00 MBytes  16.8 Mbits/sec
[  5]   0.00-30.00  sec   537 MBytes   150 Mbits/sec  264             sender
[  5]   0.00-30.00  sec   531 MBytes   149 Mbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=0 id=97320573301536
📊 TUIC QUIC stats conn=0 id=97320573301536 rtt=0ms cwnd=12000 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=188,max_stream_data=672) rx_window(max_data=0,max_stream_data=0) udp_tx=35/4096B udp_rx=350/497000B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-downlink-backpressure paused=true max_pending=0 total_pending=0 max_tx_queue=589819 total_tx_queue=589819 max_pressure=589819 total_pressure=589819 high=524288 low=131072
🔎 tcp-downlink-backpressure paused=false max_pending=0 total_pending=0 max_tx_queue=120000 total_tx_queue=120000 max_pressure=120000 total_pressure=120000 high=524288 low=131072
🔎 tcp-downlink-flush pending_total=0 pending_max=0 pending_high=65536 remote_to_global_rx_bytes=550507645 terminal_late_remote_payload_bytes=0 terminal_late_remote_payload_events=0 flush_attempts=29264 no_send_capacity=0 send_window_samples=29264 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=524283 recv_queue_max=0 may_send_false=0 may_recv_false=0 no_send_capacity_streak_max=0 no_send_capacity_pending_max=0 send_slice_calls=29264 send_slice_accepted=550507645 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=0 send_slice_max_accepted=65536 tun_flush_tx_calls=28571 tun_flush_tx_failures=0 tun_flush_deferred=693 dirty_handles=1
🔎 tcp-tun-egress if=tun0 status=delta tx_dropped_total=14804 tx_dropped_delta=14804 global_rx_paused=true pending_total=0 pending_max=0 pending_high=65536 remote_to_global_rx_bytes=361787036 tun_flush_tx_calls=16263 dirty_handles=0
🔎 tcp-tun-egress-feedback paused=true reason=drop_delta tx_dropped_delta=14804 max_pressure=589816 total_pressure=589816 high=524288 low=131072 drop_events=5 drop_delta_total=14804 max_delta=5472 pause_edges=5 resume_edges=4
🔎 tcp-tun-egress-feedback paused=false reason=pressure_low tx_dropped_delta=0 max_pressure=0 total_pressure=0 high=524288 low=131072 drop_events=5 drop_delta_total=14804 max_delta=5472 pause_edges=5 resume_edges=5
🔎 tcp-terminal-remote-payload handle=SocketHandle(1) bytes=1474528 total_bytes=1474528 events=835 pending=0 tcp_state=Closed active=false can_send=false can_recv=false may_send=false may_recv=false send_capacity=1048576 send_queue=2816 recv_queue=0
🔎 tcp-handle-close handle=SocketHandle(1) direction=local reason=dead_slot_reap state=Relaying pending=0 pending_high=65536 remote_to_global_rx_bytes=557140382 terminal_late_remote_payload_bytes=1474528 terminal_late_remote_payload_events=835 flush_attempts=33044 no_send_capacity=0 send_window_samples=33044 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=524283 recv_queue_max=0 may_send_false=0 may_recv_false=2 no_send_capacity_streak_max=0 no_send_capacity_pending_max=0 send_slice_calls=33044 send_slice_accepted=557140382 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=0 send_slice_max_accepted=65536 tun_flush_tx_calls=32351 tun_flush_tx_failures=0 tun_flush_deferred=693 close_pending_class=none close_pending_bytes=0 terminal_pending_reap_bytes=0 tcp_state=Closed active=false can_send=false can_recv=false may_send=false may_recv=false send_capacity=1048576 send_queue=2816 recv_queue=0
📊 TUIC QUIC stats conn=0 id=97320573301536 rtt=0ms cwnd=12000 lost=0/82929 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=190,max_stream_data=680) rx_window(max_data=0,max_stream_data=0) udp_tx=82927/9796183B udp_rx=414492/589086494B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "tail-collapse-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_interval_profile: samples=30"
  assert_contains "$summary" "tail_samples=6 tail_avg_mbps=16.250 tail_min_mbps=15.700 tail_collapse=1"
  assert_contains "$summary" "throughput_shape: shape=tail_collapse_local_pressure tail_collapse=1 local_pressure=1"
  assert_contains "$summary" "iperf_tail_collapse"
  assert_contains "$summary" "tail_collapse_local_pressure"
  assert_not_contains "$summary" "shape=stable_high"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-1.00   sec  21.5 MBytes   180 Mbits/sec
[  5]   1.00-2.00   sec  22.6 MBytes   190 Mbits/sec
[  5]   2.00-3.00   sec  21.8 MBytes   183 Mbits/sec
[  5]   3.00-4.00   sec  22.4 MBytes   188 Mbits/sec
[  5]   4.00-5.00   sec  22.0 MBytes   185 Mbits/sec
[  5]   5.00-6.00   sec  21.9 MBytes   184 Mbits/sec
[  5]   6.00-7.00   sec  22.1 MBytes   186 Mbits/sec
[  5]   7.00-8.00   sec  22.0 MBytes   185 Mbits/sec
[  5]   8.00-9.00   sec  21.8 MBytes   183 Mbits/sec
[  5]   9.00-10.00  sec  22.4 MBytes   188 Mbits/sec
[  5]   0.00-10.00  sec   220 MBytes   185 Mbits/sec    0             sender
[  5]   0.00-10.00  sec   219 MBytes   184 Mbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=99
📊 TUIC QUIC stats conn=1 id=99 rtt=1ms cwnd=247092 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
📊 TUIC QUIC stats conn=1 id=99 rtt=1ms cwnd=247092 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "stable-high-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "iperf_interval_profile: samples=10"
  assert_contains "$summary" "throughput_shape: shape=stable_high tail_collapse=0 local_pressure=0 no_data=0 stable_high=1"
  assert_not_contains "$summary" "iperf_tail_collapse"

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
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=3 id=99 stream=8 relay_mode=ordered_join startup_auth_attempts=0 handle=SocketHandle(1) epoch=1
📊 TUIC QUIC stats conn=3 id=99 rtt=8ms cwnd=247211 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=0 remote_reads=0 local_finish_events=0 first_local_finish_after_first_remote_read_ms=0 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=0 max_remote_read_gap_ms=0 max_remote_read_gap_before_local_finish_ms=0 max_remote_read_gap_after_local_finish_ms=0 current_remote_read_gap_ms=12000 global_rx_wait_max_us=0 global_rx_pressure_events=0 global_rx_queue_used_max=0 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tuic-tcp-stream-first-rx target=43.130.32.77:5201 conn=3 id=99 stream=8 first_rx_ms=20500 read_bytes=35244 reads=1
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=75128 remote_reads=2 local_finish_events=1 first_local_finish_after_first_remote_read_ms=1100 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=20500 max_remote_read_gap_ms=15000 max_remote_read_gap_before_local_finish_ms=7000 max_remote_read_gap_after_local_finish_ms=15000 current_remote_read_gap_ms=100 global_rx_wait_max_us=4 global_rx_pressure_events=0 global_rx_queue_used_max=1 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tuic-tcp-stream-read-gap target=43.130.32.77:5201 conn=3 id=99 stream=8 gap_ms=15000 read_bytes=39884 reads=2 rx_bytes=75128
🔎 tuic-tcp-stream-pending target=43.130.32.77:5201 conn=3 id=99 stream=8 pending_gap_ms=1000 pending_polls=10 polls=40 max_poll_gap_ms=500 rx_bytes=75128 reads=2 pending_cause=connection_fresh_stream_frames_pending conn_rx_stream_frames_since_read=12 conn_rx_stream_frames_since_pending=12
🔎 tuic-tcp-stream-pending target=43.130.32.77:5201 conn=3 id=99 stream=8 pending_gap_ms=2000 pending_polls=20 polls=80 max_poll_gap_ms=1000 rx_bytes=75128 reads=2 pending_cause=connection_stale_stream_frames_pending conn_rx_stream_frames_since_read=12 conn_rx_stream_frames_since_pending=0
🔎 tuic-tcp-stream-pending target=43.130.32.77:5201 conn=3 id=99 stream=8 pending_gap_ms=12000 pending_polls=97 polls=112 max_poll_gap_ms=5000 rx_bytes=75128 reads=2 pending_cause=connection_stream_frames_pending conn_rx_stream_frames_since_read=24 conn_rx_stream_frames_since_pending=12
🔎 tcp-stream-service-window handle=SocketHandle(1) epoch=1 remote_poll_ticks=12 remote_poll_len_min=65536 remote_poll_len_max=131072 pending_no_transport_sample=0 pending_no_connection_rx=0 pending_rx_no_stream_frames=0 pending_fresh_stream_frames=1 pending_stale_stream_frames=2 remote_read_chunks=64 remote_read_bytes=109304 global_rx_wait_max_us=21 global_rx_pressure_events=2 global_rx_queue_used_max=3 global_rx_queue_capacity=1024 local_accepted_bytes=98304 local_egress_drain_bytes=4096 local_tun_rx_packets=2 local_flush_tx_calls=3 local_flush_tx_failures=0 local_dirty_passes=2 last_blocked_reason=local_admission_no_work useful_progress=true
🔎 tcp-local-egress-service windows=12 cycles=4 accepted_bytes=98304 egress_drain_bytes=4096 tun_rx_packets=2 flush_tx_calls=3 flush_tx_failures=0 dirty_passes=2 target_reached=1 no_progress=2 cycle_budget=0 hard_pause=0 no_work=7
🔎 tuic-tcp-stream-close target=43.130.32.77:5201 conn=3 id=99 stream=8 first_rx_ms=20500 max_read_gap_ms=15000 rx_bytes=109304 reads=3 pending_polls=97 max_pending_gap_ms=12000 polls=126 max_poll_gap_ms=5000
📊 TUIC QUIC stats conn=3 id=99 rtt=5ms cwnd=247289 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "stream-timing-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "relay_remote_timing: first_read_max_ms=20500 max_read_gap_ms=15000 current_gap_max_ms=12000 no_first_read_gap_max_ms=12000"
  assert_contains "$summary" "max_read_gap_before_finish_ms=7000 max_read_gap_after_finish_ms=15000 data_max_read_gap_before_finish_ms=7000 data_max_read_gap_after_finish_ms=15000 first_local_finish_after_first_remote_read_max_ms=1100"
  assert_contains "$summary" "tuic_tcp_stream: first_rx_events=1 first_rx_max_ms=20500 read_gap_events=1 read_gap_max_ms=15000 close_events=1 close_first_rx_max_ms=20500 close_gap_max_ms=15000 rx_bytes_max=109304 reads_max=3 zero_rx_closes=0"
  assert_contains "$summary" "tuic_stream_pending: events=3 max_pending_gap_ms=12000 pending_polls_max=97 data_streams=1 data_pending_gap_max_ms=12000 data_rx_bytes_max=109304"
  assert_contains "$summary" "tuic_stream_pending_causes: connection_stream_frames_pending=1 connection_fresh_stream_frames_pending=1 connection_stale_stream_frames_pending=1 connection_rx_no_stream_frames=0 no_connection_rx=0 no_transport_sample=0"
  assert_contains "$summary" "tuic_stream_frame_delta: pending_events=3 conn_rx_stream_frames_since_read_max=24 conn_rx_stream_frames_since_pending_max=12 fresh_since_pending_max=12 stale_since_pending_max=0 connection_stream_since_pending_max=12 fresh_nonzero_since_pending=1 stale_zero_since_pending=1"
  assert_contains "$summary" "tuic_stream_polling: polls_max=126 max_poll_gap_ms=5000 data_streams=1 data_polls_max=126 data_poll_gap_max_ms=5000 data_rx_bytes_max=109304"
  assert_contains "$summary" "stream_service_window: windows=1 remote_poll_ticks_max=12 remote_poll_len_min_min=65536 remote_poll_len_max_max=131072 remote_read_chunks_max=64 remote_read_bytes_max=109304 global_rx_pressure_events_max=2 global_rx_queue_used_max=3 global_rx_queue_capacity_max=1024 local_accepted_bytes_max=98304 local_egress_drain_bytes_max=4096 local_flush_tx_calls_max=3 local_flush_tx_failures_max=0 local_dirty_passes_max=2 pending_fresh_stream_frames_max=1 pending_stale_stream_frames_max=2 useful_progress_windows=1 last_blocked_reason=local_admission_no_work"
  assert_contains "$summary" "local_egress_service: windows_max=12 cycles_max=4 accepted_bytes_max=98304 egress_drain_bytes_max=4096 tun_rx_packets_max=2 flush_tx_calls_max=3 flush_tx_failures_max=0 dirty_passes_max=2 target_reached_max=1 no_progress_max=2 cycle_budget_max=0 hard_pause_max=0 no_work_max=7"
  assert_contains "$summary" "d2_flow_timeline: joinable=1 reason=joined_by_bridge tuic_data_streams=1 stream_service_windows=1 data_poll_gap_max_ms=5000 data_read_gap_max_ms=15000 data_pending_gap_max_ms=12000 local_accepted_bytes_max=98304 local_egress_drain_bytes_max=4096 pressure_credit_debt_bytes=0 headroom_limited=0 tun_flush_deferred=0"
  assert_contains "$summary" "attribution: tuic_stream_first_byte_slow+tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_first_byte_slow+relay_remote_read_gap"
  assert_not_contains "$summary" "reverse_sender_backpressured"

  cat > "$iperf_sample" <<'EOF_IPERF'
Reverse mode, remote host 43.130.32.77 is sending
[  5]   0.00-30.04  sec  1.13 MBytes   315 Kbits/sec    3             sender
[  5]   0.00-30.00  sec   414 KBytes   113 Kbits/sec                  receiver
EOF_IPERF
  cat > "$log_sample" <<'EOF_LOG'
🔎 tuic-open-tcp target=43.130.32.77:5201 conn=4 id=99
📊 TUIC QUIC stats conn=4 id=99 rtt=8ms cwnd=247211 lost=0/35 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=33/9175B udp_rx=177/232816B dg_max=Some(1418) dg_space=1048576B
🔎 tcp-reverse-window handle=SocketHandle(1) ctx_state=Relaying payload_bytes=65536 accepted_bytes=65536 pending=0 remote_to_global_rx_bytes=65536 local_fin_sent=false tcp_state=Established active=true can_send=true can_recv=false may_send=true may_recv=true send_capacity=1048576 send_queue=65536 recv_queue=0
🔎 tcp-relay-live handle=SocketHandle(1) epoch=1 writer_done=false read_only_after_local_finish=false uplink_bytes=37 uplink_writes=1 remote_to_global_rx_bytes=723424 remote_reads=27 remote_after_local_finish_bytes=0 remote_after_local_finish_reads=0 first_remote_read_ms=4 max_remote_read_gap_ms=20567 current_remote_read_gap_ms=20567 global_rx_wait_max_us=5 global_rx_pressure_events=0 global_rx_queue_used_max=1 global_rx_queue_capacity=1024 local_write_wait_max_us=0 local_write_pressure_events=0
🔎 tcp-reverse-window handle=SocketHandle(1) ctx_state=Relaying payload_bytes=65536 accepted_bytes=65536 pending=0 remote_to_global_rx_bytes=723424 local_fin_sent=false tcp_state=Established active=true can_send=true can_recv=false may_send=true may_recv=true send_capacity=1048576 send_queue=65536 recv_queue=0
🔎 tuic-tcp-stream-pending target=43.130.32.77:5201 conn=4 id=99 stream=4 pending_gap_ms=20567 pending_polls=88 polls=115 max_poll_gap_ms=5000 rx_bytes=723424 reads=27
🔎 tuic-tcp-stream-close target=43.130.32.77:5201 conn=4 id=99 stream=4 first_rx_ms=4 max_read_gap_ms=20567 rx_bytes=723424 reads=27 pending_polls=88 max_pending_gap_ms=20567 polls=119 max_poll_gap_ms=5000
🔎 tcp-terminal-remote-payload handle=SocketHandle(1) bytes=241304 total_bytes=241304 events=9 pending=0 tcp_state=Closed active=false can_send=false can_recv=false may_send=false may_recv=false send_capacity=1048576 send_queue=0 recv_queue=0
🔎 tcp-handle-close handle=SocketHandle(1) direction=local reason=dead_slot_reap state=Relaying pending=0 pending_high=65536 remote_to_global_rx_bytes=723424 terminal_late_remote_payload_bytes=241304 terminal_late_remote_payload_events=9 flush_attempts=27 no_send_capacity=0 send_window_samples=27 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=65536 recv_queue_max=0 may_send_false=0 may_recv_false=0 no_send_capacity_streak_max=0 no_send_capacity_pending_max=0 send_slice_calls=27 send_slice_accepted=723424 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=0 send_slice_max_accepted=65536 tun_flush_tx_calls=27 tun_flush_tx_failures=0 tun_flush_deferred=0 close_pending_class=none close_pending_bytes=0 terminal_pending_reap_bytes=0 tcp_state=Closed active=false can_send=false can_recv=false may_send=false may_recv=false send_capacity=1048576 send_queue=0 recv_queue=0
📊 TUIC QUIC stats conn=4 id=99 rtt=5ms cwnd=247289 lost=0/105 lost_bytes=0 congestion_events=0 tx_blocked(data=0,stream=0,streams_bidi=0,streams_uni=0) rx_blocked(data=0,stream=0) tx_window(max_data=0,max_stream_data=0) rx_window(max_data=0,max_stream_data=0) udp_tx=103/14703B udp_rx=535/734789B dg_max=Some(1418) dg_space=1048576B
EOF_LOG
  summary="$(summarize_metrics_window 0 "reverse-starvation-self-test" "$iperf_sample" "$log_sample")"
  assert_contains "$summary" "throughput_shape: shape=no_data tail_collapse=0 local_pressure=0 no_data=1 stable_high=0"
  assert_contains "$summary" "tcp_reverse_window: events=2 payload_bytes=131072 accepted_bytes=131072 pending_max=0 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=65536 recv_queue_max=0 may_recv_false=0 active_false=0 can_send_false=0"
  assert_contains "$summary" "tuic_stream_starved"
  assert_contains "$summary" "target_sender_stalled"
  assert_contains "$summary" "terminal_closed_late_payload"
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
  assert_contains "$summary" "throughput_shape: shape=no_data tail_collapse=0 local_pressure=0 no_data=1 stable_high=0"
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
IPERF_BYTES="${IPERF_BYTES:-}"
IPERF_TIMEOUT_SECS="${IPERF_TIMEOUT_SECS:-$((DURATION + 20))}"
LOG="${LOG:-/tmp/mvpn_accept.log}"
RUN_UDP="${RUN_UDP:-0}"
UDP_BW="${UDP_BW:-90M}"
UDP_LEN="${UDP_LEN:-1200}"
IPERF_BUSY_RETRIES="${IPERF_BUSY_RETRIES:-3}"
IPERF_BUSY_WAIT_SECS="${IPERF_BUSY_WAIT_SECS:-5}"
POST_IPERF_METRICS_SETTLE_SECS="${POST_IPERF_METRICS_SETTLE_SECS:-2}"
PROBE_ORDER="${PROBE_ORDER:-forward-first}"
ROUTING_MODE="${ROUTING_MODE:-full-tunnel}"
FORWARD_DISCRIMINATOR="${FORWARD_DISCRIMINATOR:-0}"
IPERF_COMMAND_FAILURES=0
OUT="${OUT:-/tmp/mvpn_knife14b_lowrtt_$(date +%Y%m%d_%H%M%S).md}"
METRIC_RE='📊 数据面|🔬 主循环|TUIC datagram|UDP relay mode|TCP socket buffers|TUIC QUIC stats|tuic-open-tcp|tuic-tcp-stream-first-rx|tuic-tcp-stream-read-gap|tuic-tcp-stream-pending|tuic-tcp-stream-close|tuic-tcp-pool-(probe|selection|reconnect)|tuic-tcp-unordered-staging|tcp-relay-live|tcp-relay-ack-drain-hint|tcp-relay-write-half-closed|tcp-relay-close|tcp-d16-relay-close|tcp-handle-close|tcp-deferred-close-egress|tcp-lifecycle-transition|tcp-reverse-window|tcp-local-egress-service|tcp-local-write-pressure|tcp-global-rx-pressure|tcp-global-rx-backpressure|tcp-downlink-backpressure|tcp-downlink-flush|tcp-tun-rx-drain|tcp-tun-egress'
if [[ "${MINI_VPN_PROBE_INCLUDE_STREAM_SERVICE_WINDOW:-0}" == "1" ]]; then
  METRIC_RE="$METRIC_RE|tcp-stream-service-window"
fi

case "$PROBE_ORDER" in
  forward-first|reverse-first|forward-only|reverse-only) ;;
  *)
    echo "invalid PROBE_ORDER=$PROBE_ORDER (expected forward-first|reverse-first|forward-only|reverse-only)" >&2
    exit 2
    ;;
esac
case "$ROUTING_MODE" in
  full-tunnel|target-only) ;;
  *)
    echo "invalid ROUTING_MODE=$ROUTING_MODE (expected full-tunnel|target-only)" >&2
    exit 2
    ;;
esac
case "$FORWARD_DISCRIMINATOR" in
  0|1) ;;
  *)
    echo "invalid FORWARD_DISCRIMINATOR=$FORWARD_DISCRIMINATOR (expected 0|1)" >&2
    exit 2
    ;;
esac
if [[ "$FORWARD_DISCRIMINATOR" == "1" && "$PROBE_ORDER" != "forward-only" ]]; then
  echo "FORWARD_DISCRIMINATOR=1 requires PROBE_ORDER=forward-only" >&2
  exit 2
fi
if ! is_uint "$POST_IPERF_METRICS_SETTLE_SECS"; then
  echo "invalid POST_IPERF_METRICS_SETTLE_SECS=$POST_IPERF_METRICS_SETTLE_SECS (expected non-negative integer seconds)" >&2
  exit 2
fi
if [[ -n "$IPERF_BYTES" && ! "$IPERF_BYTES" =~ ^[1-9][0-9]*([KMG])?$ ]]; then
  echo "invalid IPERF_BYTES=$IPERF_BYTES (expected positive bytes with optional K/M/G suffix)" >&2
  exit 2
fi
read -r TCP_TRANSFER_FLAG TCP_TRANSFER_VALUE <<< "$(tcp_transfer_spec)"

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
  if ((status != 0)); then
    IPERF_COMMAND_FAILURES=$((IPERF_COMMAND_FAILURES + 1))
  fi
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
  echo "- iperf_bytes: ${IPERF_BYTES:-<timed>}"
  echo "- iperf_timeout: ${IPERF_TIMEOUT_SECS}s"
  echo "- iperf_busy_retries: ${IPERF_BUSY_RETRIES}"
  echo "- iperf_busy_wait_secs: ${IPERF_BUSY_WAIT_SECS}"
  echo "- post_iperf_metrics_settle_secs: ${POST_IPERF_METRICS_SETTLE_SECS}"
  echo "- probe_order: ${PROBE_ORDER}"
  echo "- routing_mode: ${ROUTING_MODE}"
  echo "- forward_discriminator: ${FORWARD_DISCRIMINATOR}"
  echo "- forward_discriminator_max_quic_lost_bytes: ${FORWARD_DISCRIMINATOR_MAX_QUIC_LOST_BYTES}"
  echo "- log: ${LOG}"
  echo
  echo "> $(routing_expectation_note "$ROUTING_MODE")"
} > "$OUT"

append_cleanliness_check

if [[ "$ROUTING_MODE" == "target-only" ]]; then
  append_section "Target-Only Routing Checks"
  echo "$(routing_expectation_note "$ROUTING_MODE")" | tee -a "$OUT"
  append_cmd ip route get "$TARGET"
else
  append_section "Tunnel Gold Checks"
  append_cmd curl -fsS ipinfo.io
  if command -v dig >/dev/null 2>&1; then
    append_cmd dig example.com +short
  else
    echo "dig not found; skipping fake-IP DNS check" | tee -a "$OUT"
  fi
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
      iperf3 -c "$TARGET" -p "$PORT" "$TCP_TRANSFER_FLAG" "$TCP_TRANSFER_VALUE" -P "$p"
  done
}

run_tcp_reverse_sweep() {
  append_section "TCP Reverse Sweep"
  for p in $PARALLEL_SET; do
    append_iperf_cmd "mini_vpn Metrics during TCP Reverse P=$p" \
      iperf3 -c "$TARGET" -p "$PORT" "$TCP_TRANSFER_FLAG" "$TCP_TRANSFER_VALUE" -P "$p" -R
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

PROBE_STATUS=0
if [[ "$FORWARD_DISCRIMINATOR" == "1" ]]; then
  append_section "Forward Discriminator Decision"
  if ((IPERF_COMMAND_FAILURES > 0)); then
    echo "FAIL: iperf command failures=$IPERF_COMMAND_FAILURES" | tee -a "$OUT"
    PROBE_STATUS=1
  elif forward_discriminator_report_passes "$OUT"; then
    echo "PASS: receiver present, all TUN TX drop deltas are zero, and aggregate pool QUIC lost-byte delta is within ${FORWARD_DISCRIMINATOR_MAX_QUIC_LOST_BYTES}B." | tee -a "$OUT"
  else
    echo "FAIL: missing receiver result, non-zero/unknown TUN TX drop delta, missing aggregate QUIC sample, or aggregate pool QUIC lost-byte delta above ${FORWARD_DISCRIMINATOR_MAX_QUIC_LOST_BYTES}B." | tee -a "$OUT"
    PROBE_STATUS=1
  fi
fi

echo
echo "report written: $OUT"
exit "$PROBE_STATUS"
