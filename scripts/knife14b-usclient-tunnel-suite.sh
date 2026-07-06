#!/usr/bin/env bash
# 刀14c US-client tunnel suite.
#
# Run on the Ubuntu Client VPS. It validates the environment, starts mini_vpn
# client-tun, routes only the iperf target into the TUN, runs the low-RTT probe,
# and writes a self-contained report bundle under /tmp/conn.

set -uo pipefail

readonly DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES=""
readonly DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES=""
readonly DEFAULT_DOWNLINK_FLUSH_MAX_BYTES=262144
readonly DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216
readonly DEFAULT_TUN_RX_DRAIN_BUDGET=0
readonly LEGACY_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288
readonly LEGACY_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072
readonly DEFAULT_SERVER_EVIDENCE_SING_BOX_TAIL=220
readonly DEFAULT_SERVER_EVIDENCE_TARGET_JOURNAL_TAIL=260
readonly DEFAULT_KNIFE14_EXIT_HOST=43.153.32.33
readonly DEFAULT_KNIFE14_TARGET_HOST=43.130.32.77
readonly DEFAULT_KNIFE14_VPS_SSH_USER=ubuntu
readonly DEFAULT_KNIFE14_VPS_SSH_KEY=/home/ubuntu/.ssh/vpn

extract_client_tun_pids_from_ps() {
  awk '
    {
      pid = $1
      comm = $2
      args = $0
      sub(/^[[:space:]]*[0-9]+[[:space:]]+[^[:space:]]+[[:space:]]*/, "", args)
      if (comm != "mini_vpn") {
        next
      }
      n = split(args, fields, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        if (fields[i] == "client-tun") {
          print pid
          break
        }
      }
    }
  '
}

client_tun_pids() {
  ps -eo pid=,comm=,args= | extract_client_tun_pids_from_ps
}

auth_config_match_python() {
  cat <<'PY'
import json
import os
import subprocess
import sys

host = os.environ.get("EXIT_SSH_HOST", "")
if not host:
    print("exit_ssh_host_set=0")
    sys.exit(0)

ssh_cmd = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    "-o",
    f"StrictHostKeyChecking={os.environ.get('EXIT_SSH_STRICT_HOST_KEY_CHECKING', 'accept-new')}",
]
known_hosts = os.environ.get("EXIT_SSH_KNOWN_HOSTS_FILE", "")
if known_hosts:
    ssh_cmd += ["-o", f"UserKnownHostsFile={known_hosts}"]
key = os.environ.get("EXIT_SSH_KEY", "")
if key:
    ssh_cmd += ["-i", key]
port = os.environ.get("EXIT_SSH_PORT", "")
if port:
    ssh_cmd += ["-p", port]
ssh_cmd += [host, "sudo", "python3", "-"]

remote_script = r'''
import json
cfg = json.load(open('/etc/sing-box/config.json'))
inbounds = [item for item in cfg.get('inbounds', []) if item.get('type') == 'tuic']
if not inbounds:
    print(json.dumps({"error": "no_tuic_inbound"}))
    raise SystemExit(0)
ib = inbounds[0]
users = ib.get('users') or []
user = users[0] if users else {}
tls = ib.get('tls') or {}
alpn = tls.get('alpn') or []
print(json.dumps({
    "listen": ib.get('listen', ''),
    "listen_port": ib.get('listen_port', ''),
    "users_len": len(users),
    "uuid": user.get('uuid', ''),
    "password": user.get('password', ''),
    "server_name": tls.get('server_name', ''),
    "alpn0": alpn[0] if alpn else '',
    "certificate_path": tls.get('certificate_path', ''),
    "congestion_control": ib.get('congestion_control', ''),
}))
'''

try:
    raw = subprocess.check_output(
        ssh_cmd,
        input=remote_script,
        text=True,
        stderr=subprocess.STDOUT,
        timeout=15,
    )
except Exception as exc:
    print("exit_config_compare_error=1")
    print(f"exit_config_compare_error_type={type(exc).__name__}")
    sys.exit(1)

try:
    remote = json.loads(raw)
except json.JSONDecodeError:
    print("exit_config_parse_error=1")
    sys.exit(1)

if remote.get("error"):
    print(f"exit_config_error={remote['error']}")
    sys.exit(2)

checks = {
    "uuid_match": os.environ.get("MINI_VPN_TUIC_UUID", "") == remote.get("uuid", ""),
    "password_match": os.environ.get("MINI_VPN_TUIC_PASSWORD", "") == remote.get("password", ""),
    "sni_match": os.environ.get("MINI_VPN_TUIC_SNI", "") == remote.get("server_name", ""),
    "alpn_match": os.environ.get("MINI_VPN_TUIC_ALPN", "") == remote.get("alpn0", ""),
}

print(f"tuic_listen={remote.get('listen', '')}:{remote.get('listen_port', '')}")
print(f"tuic_users={remote.get('users_len', 0)}")
print(f"tuic_uuid_len={len(str(remote.get('uuid', '')))}")
print(f"tuic_password_len={len(str(remote.get('password', '')))}")
print(f"tuic_server_name_len={len(str(remote.get('server_name', '')))}")
print(f"tuic_alpn0_len={len(str(remote.get('alpn0', '')))}")
print(f"tuic_certificate_path_set={int(bool(remote.get('certificate_path', '')))}")
print(f"tuic_congestion_control_set={int(bool(remote.get('congestion_control', '')))}")
for key in ("uuid_match", "password_match", "sni_match", "alpn_match"):
    print(f"{key}={int(checks[key])}")

sys.exit(0 if all(checks.values()) else 2)
PY
}

server_evidence_default_ssh_host() {
  local role="$1"
  local host="$2"

  case "$role:$host" in
    exit:$DEFAULT_KNIFE14_EXIT_HOST | target:$DEFAULT_KNIFE14_TARGET_HOST)
      printf '%s@%s' "$DEFAULT_KNIFE14_VPS_SSH_USER" "$host"
      ;;
  esac
}

server_evidence_default_ssh_key() {
  local key_path="${KNIFE14_DEFAULT_VPS_SSH_KEY:-$DEFAULT_KNIFE14_VPS_SSH_KEY}"
  if [[ -f "$key_path" ]]; then
    printf '%s' "$key_path"
  fi
}

apply_server_evidence_ssh_defaults() {
  if [[ "$SERVER_EVIDENCE_CHECK" != "1" ]]; then
    return 0
  fi

  local default_host default_key
  if [[ -z "$EXIT_SSH_HOST" ]]; then
    default_host="$(server_evidence_default_ssh_host exit "$EXIT_HOST")"
    if [[ -n "$default_host" ]]; then
      EXIT_SSH_HOST="$default_host"
    fi
  fi
  if [[ -z "$TARGET_SSH_HOST" ]]; then
    default_host="$(server_evidence_default_ssh_host target "$TARGET")"
    if [[ -n "$default_host" ]]; then
      TARGET_SSH_HOST="$default_host"
    fi
  fi

  default_key="$(server_evidence_default_ssh_key)"
  if [[ -n "$default_key" ]]; then
    if [[ -n "$(server_evidence_default_ssh_host exit "$EXIT_HOST")" && -z "$EXIT_SSH_KEY" ]]; then
      EXIT_SSH_KEY="$default_key"
    fi
    if [[ -n "$(server_evidence_default_ssh_host target "$TARGET")" && -z "$TARGET_SSH_KEY" ]]; then
      TARGET_SSH_KEY="$default_key"
    fi
  fi
}

is_positive_integer() {
  local value="${1:-}"
  [[ "$value" =~ ^[0-9]+$ ]] && ((10#$value > 0))
}

normalize_downlink_backpressure_env() {
  DOWNLINK_BACKPRESSURE_AUTO_REASON=""
  if [[ "${KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE:-0}" == "1" ]]; then
    return 0
  fi

  local tx_bytes="${MINI_VPN_TCP_TX_BUFFER_BYTES:-}"
  local high_bytes="${MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES:-}"
  local low_bytes="${MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES:-}"
  if ! is_positive_integer "$tx_bytes" || ! is_positive_integer "$high_bytes"; then
    return 0
  fi
  if ((10#$tx_bytes <= LEGACY_DOWNLINK_BACKPRESSURE_HIGH_BYTES)); then
    return 0
  fi
  if [[ "$high_bytes" != "$LEGACY_DOWNLINK_BACKPRESSURE_HIGH_BYTES" ]]; then
    return 0
  fi
  if [[ -n "$low_bytes" && "$low_bytes" != "$LEGACY_DOWNLINK_BACKPRESSURE_LOW_BYTES" ]]; then
    return 0
  fi

  MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=""
  MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=""
  export MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES
  export MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES
  DOWNLINK_BACKPRESSURE_AUTO_REASON="legacy_512k_for_scaled_tx_buffer"
}

summarize_final_lifecycle_window() {
  local start_line="${1:-0}"
  local title="${2:-final}"

  awk -v start_line="$start_line" -v title="$title" '
    function numeric_token(token, key, value) {
      value = token
      sub("^" key "=", "", value)
      gsub(/[^0-9]/, "", value)
      if (value == "") {
        return 0
      }
      return value + 0
    }

    function text_token(token, key, value) {
      value = token
      sub("^" key "=", "", value)
      return value
    }

    function update_downlink_maxima() {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^flush_attempts=/) {
          value = numeric_token($i, "flush_attempts")
          if (value > max_flush_attempts) {
            max_flush_attempts = value
          }
        } else if ($i ~ /^send_queue_max=/) {
          value = numeric_token($i, "send_queue_max")
          if (value > max_send_queue_max) {
            max_send_queue_max = value
          }
        } else if ($i ~ /^may_recv_false=/) {
          value = numeric_token($i, "may_recv_false")
          if (value > max_may_recv_false) {
            max_may_recv_false = value
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
        } else if ($i ~ /^send_slice_zero=/) {
          value = numeric_token($i, "send_slice_zero")
          if (value > max_send_slice_zero) {
            max_send_slice_zero = value
          }
        } else if ($i ~ /^send_slice_errors=/) {
          value = numeric_token($i, "send_slice_errors")
          if (value > max_send_slice_errors) {
            max_send_slice_errors = value
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
        }
      }
    }

    NR <= start_line {
      next
    }

    /tcp-downlink-flush/ {
      update_downlink_maxima()
    }

    /tcp-handle-close/ {
      pending = 0
      close_pending_class = ""
      close_egress_class = ""
      close_egress_bytes = 0
      close_egress_drain_candidate = 0
      terminal_pending = 0
      update_downlink_maxima()
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^pending=/) {
          pending = numeric_token($i, "pending")
        } else if ($i ~ /^close_pending_bytes=/) {
          pending = numeric_token($i, "close_pending_bytes")
        } else if ($i ~ /^close_pending_class=/) {
          close_pending_class = text_token($i, "close_pending_class")
        } else if ($i ~ /^close_egress_bytes=/) {
          close_egress_bytes = numeric_token($i, "close_egress_bytes")
        } else if ($i ~ /^close_egress_class=/) {
          close_egress_class = text_token($i, "close_egress_class")
        } else if ($i == "close_egress_drain_candidate=true") {
          close_egress_drain_candidate = 1
        } else if ($i ~ /^terminal_pending_reap_bytes=/) {
          terminal_pending = numeric_token($i, "terminal_pending_reap_bytes")
        }
      }
      if (pending > 0) {
        pending_events++
        pending_bytes += pending
        if (pending > max_pending_bytes) {
          max_pending_bytes = pending
        }
        if (close_pending_class == "active_send_capable" ||
            close_pending_class == "inactive_send_capable") {
          pending_send_capable_events++
          pending_send_capable_bytes += pending
        } else if (close_pending_class == "active_no_send") {
          pending_active_no_send_events++
          pending_active_no_send_bytes += pending
        } else if (close_pending_class == "terminal_closed_no_send") {
          pending_terminal_events++
          pending_terminal_bytes += pending
        }
      }
      if (terminal_pending > 0) {
        terminal_pending_events++
        terminal_pending_bytes += terminal_pending
        if (terminal_pending > max_terminal_pending_bytes) {
          max_terminal_pending_bytes = terminal_pending
        }
      }
      if (close_egress_bytes > 0) {
        egress_events++
        egress_bytes += close_egress_bytes
        if (close_egress_bytes > max_egress_bytes) {
          max_egress_bytes = close_egress_bytes
        }
        if (close_egress_class == "active_send_capable" ||
            close_egress_class == "inactive_send_capable") {
          egress_send_capable_events++
          egress_send_capable_bytes += close_egress_bytes
        } else if (close_egress_class == "active_no_send") {
          egress_active_no_send_events++
          egress_active_no_send_bytes += close_egress_bytes
        } else if (close_egress_class == "terminal_closed_no_send") {
          egress_terminal_events++
          egress_terminal_bytes += close_egress_bytes
        }
        if (close_egress_drain_candidate == 1) {
          egress_drain_candidate_events++
          egress_drain_candidate_bytes += close_egress_bytes
        }
      }
    }

    /tcp-tun-egress[[:space:]]/ {
      runtime_tun_samples++
      runtime_delta = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^tx_dropped_delta=/) {
          runtime_delta = $i
          sub(/^tx_dropped_delta=/, "", runtime_delta)
        }
      }
      if (runtime_delta ~ /^[0-9]+$/) {
        runtime_delta += 0
        runtime_drop_delta_total += runtime_delta
        if (runtime_delta > 0) {
          runtime_drop_events++
        }
        if (runtime_delta > max_runtime_delta) {
          max_runtime_delta = runtime_delta
        }
      }
    }

    /tcp-tun-egress-feedback/ {
      feedback_delta = ""
      for (i = 1; i <= NF; i++) {
        if ($i == "paused=true") {
          feedback_pause_edges++
        } else if ($i == "paused=false") {
          feedback_resume_edges++
        } else if ($i ~ /^tx_dropped_delta=/) {
          feedback_delta = $i
          sub(/^tx_dropped_delta=/, "", feedback_delta)
        } else if ($i ~ /^drop_events=/) {
          value = numeric_token($i, "drop_events")
          if (value > feedback_drop_events_from_log) {
            feedback_drop_events_from_log = value
          }
        } else if ($i ~ /^drop_delta_total=/) {
          value = numeric_token($i, "drop_delta_total")
          if (value > feedback_drop_delta_total_from_log) {
            feedback_drop_delta_total_from_log = value
          }
        } else if ($i ~ /^max_delta=/) {
          value = numeric_token($i, "max_delta")
          if (value > max_feedback_delta) {
            max_feedback_delta = value
          }
        } else if ($i ~ /^max_pressure=/) {
          value = numeric_token($i, "max_pressure")
          if (value > max_feedback_pressure) {
            max_feedback_pressure = value
          }
        }
      }
      if (feedback_delta ~ /^[0-9]+$/) {
        feedback_delta += 0
        feedback_drop_delta_sum += feedback_delta
        if (feedback_delta > 0) {
          feedback_drop_events_from_delta++
        }
        if (feedback_delta > max_feedback_delta) {
          max_feedback_delta = feedback_delta
        }
      }
    }

    END {
      feedback_drop_events = feedback_drop_events_from_log
      if (feedback_drop_events_from_delta > feedback_drop_events) {
        feedback_drop_events = feedback_drop_events_from_delta
      }
      feedback_drop_delta_total = feedback_drop_delta_total_from_log
      if (feedback_drop_delta_sum > feedback_drop_delta_total) {
        feedback_drop_delta_total = feedback_drop_delta_sum
      }

      labels = ""
      if (pending_events > 0) {
        labels = labels == "" ? "final_pending_at_close" : labels "+final_pending_at_close"
      }
      if (pending_send_capable_events > 0) {
        labels = labels == "" ? "final_pending_send_capable" : labels "+final_pending_send_capable"
      }
      if (terminal_pending_events > 0) {
        labels = labels == "" ? "final_terminal_pending_reap" : labels "+final_terminal_pending_reap"
      }
      if (egress_events > 0) {
        labels = labels == "" ? "final_egress_at_close" : labels "+final_egress_at_close"
      }
      if (egress_drain_candidate_events > 0) {
        labels = labels == "" ? "final_egress_drain_candidate" : labels "+final_egress_drain_candidate"
      }
      if (runtime_drop_delta_total > 0) {
        labels = labels == "" ? "final_runtime_tun_egress_drop" : labels "+final_runtime_tun_egress_drop"
      }
      if (feedback_drop_delta_total > 0) {
        labels = labels == "" ? "final_tun_egress_feedback_drop" : labels "+final_tun_egress_feedback_drop"
      }
      if (max_headroom_limited > 0 || max_headroom_deferred_bytes > 0) {
        labels = labels == "" ? "final_headroom_limited" : labels "+final_headroom_limited"
      }
      if (max_drain_credit_granted_bytes > 0 || max_drain_credit_used_bytes > 0) {
        labels = labels == "" ? "final_drain_credit" : labels "+final_drain_credit"
      }
      if (labels == "") {
        labels = "final_no_pressure_signal"
      }

      print "- final_metrics_title: " title
      printf "- final_pending_at_close: events=%d bytes=%d max_bytes=%d send_capable_events=%d send_capable_bytes=%d active_no_send_events=%d active_no_send_bytes=%d terminal_events=%d terminal_bytes=%d\n", pending_events, pending_bytes, max_pending_bytes, pending_send_capable_events, pending_send_capable_bytes, pending_active_no_send_events, pending_active_no_send_bytes, pending_terminal_events, pending_terminal_bytes
      printf "- final_terminal_pending_reap: events=%d bytes=%d max_bytes=%d\n", terminal_pending_events, terminal_pending_bytes, max_terminal_pending_bytes
      printf "- final_egress_at_close: events=%d bytes=%d max_bytes=%d send_capable_events=%d send_capable_bytes=%d active_no_send_events=%d active_no_send_bytes=%d terminal_events=%d terminal_bytes=%d drain_candidate_events=%d drain_candidate_bytes=%d\n", egress_events, egress_bytes, max_egress_bytes, egress_send_capable_events, egress_send_capable_bytes, egress_active_no_send_events, egress_active_no_send_bytes, egress_terminal_events, egress_terminal_bytes, egress_drain_candidate_events, egress_drain_candidate_bytes
      printf "- final_downlink_flush: attempts=%d send_queue_max=%d may_recv_false=%d headroom_limited=%d headroom_deferred_bytes=%d drain_credit_granted_bytes=%d drain_credit_planned_bytes=%d drain_credit_used_bytes=%d send_slice_zero=%d send_slice_errors=%d tun_flush_failures=%d tun_flush_deferred=%d\n", max_flush_attempts, max_send_queue_max, max_may_recv_false, max_headroom_limited, max_headroom_deferred_bytes, max_drain_credit_granted_bytes, max_drain_credit_planned_bytes, max_drain_credit_used_bytes, max_send_slice_zero, max_send_slice_errors, max_tun_flush_failures, max_tun_flush_deferred
      printf "- final_runtime_tun_egress: samples=%d drop_events=%d drop_delta_total=%d max_delta=%d\n", runtime_tun_samples, runtime_drop_events, runtime_drop_delta_total, max_runtime_delta
      printf "- final_tun_egress_feedback: pause_edges=%d resume_edges=%d drop_events=%d drop_delta_total=%d max_delta=%d max_pressure_bytes=%d\n", feedback_pause_edges, feedback_resume_edges, feedback_drop_events, feedback_drop_delta_total, max_feedback_delta, max_feedback_pressure
      print "- final_attribution: " labels
    }
  '
}

suite_self_test() {
  local sample expected actual help_text

  sample="$(cat <<'EOF'
111 bash ssh ubuntu@43.172.75.27 cd /home/ubuntu/mini_vpn && bash scripts/knife14b-usclient-tunnel-suite.sh
222 mini_vpn /home/ubuntu/mini_vpn/target/release/mini_vpn client-tun
333 sudo sudo -E env MINI_VPN_TUN_MTU=1200 /home/ubuntu/mini_vpn/target/release/mini_vpn client-tun
444 mini_vpn mini_vpn client-tun
555 mini_vpn /home/ubuntu/mini_vpn/target/release/mini_vpn reality-probe
666 bash pgrep -af [m]ini_vpn.*client-tun
EOF
)"
  expected="$(printf '222\n444\n')"
  actual="$(printf '%s\n' "$sample" | extract_client_tun_pids_from_ps)"

  if [[ "$actual" != "$expected" ]]; then
    echo "suite self-test failed: client_tun pid matcher" >&2
    echo "expected:" >&2
    printf '%s\n' "$expected" >&2
    echo "actual:" >&2
    printf '%s\n' "$actual" >&2
    return 1
  fi

  if [[ -n "$DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES$DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES" ]]; then
    echo "suite self-test failed: downlink backpressure defaults should defer to binary auto-scaling" >&2
    return 1
  fi
  if [[ "$DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES" != "16777216" ]]; then
    echo "suite self-test failed: downlink egress default must preserve old immediate-flush behavior" >&2
    return 1
  fi

  help_text="$(usage)"
  if ! grep -q "MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=<auto>" <<<"$help_text"; then
    echo "suite self-test failed: high watermark help must advertise auto default" >&2
    return 1
  fi
  if ! grep -q "MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=<auto>" <<<"$help_text"; then
    echo "suite self-test failed: low watermark help must advertise auto default" >&2
    return 1
  fi
  if ! grep -q "MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=$DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES" <<<"$help_text"; then
    echo "suite self-test failed: downlink egress help default drifted" >&2
    return 1
  fi
  if ! grep -q "MINI_VPN_TUN_RX_DRAIN_BUDGET=$DEFAULT_TUN_RX_DRAIN_BUDGET" <<<"$help_text"; then
    echo "suite self-test failed: tun rx drain budget help default drifted" >&2
    return 1
  fi
  if ! grep -q "KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=0" <<<"$help_text"; then
    echo "suite self-test failed: downlink backpressure normalization help missing" >&2
    return 1
  fi
  if ! grep -q "STOP_AFTER_REVERSE_FIRST_P1=0" <<<"$help_text"; then
    echo "suite self-test failed: reverse-only stop help missing" >&2
    return 1
  fi

  if ! (
    MINI_VPN_TCP_TX_BUFFER_BYTES=1048576
    MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288
    MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072
    KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=0
    normalize_downlink_backpressure_env &&
      [[ -z "${MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES:-}" ]] &&
      [[ -z "${MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES:-}" ]] &&
      [[ "${DOWNLINK_BACKPRESSURE_AUTO_REASON:-}" == legacy_512k_for_scaled_tx_buffer ]]
  ); then
    echo "suite self-test failed: legacy downlink backpressure should normalize to auto under scaled tx buffer" >&2
    return 1
  fi

  if ! (
    MINI_VPN_TCP_TX_BUFFER_BYTES=1048576
    MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288
    MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072
    KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=1
    normalize_downlink_backpressure_env &&
      [[ "${MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES:-}" == "524288" ]] &&
      [[ "${MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES:-}" == "131072" ]] &&
      [[ -z "${DOWNLINK_BACKPRESSURE_AUTO_REASON:-}" ]]
  ); then
    echo "suite self-test failed: keep flag should preserve explicit legacy backpressure" >&2
    return 1
  fi

  if ! (
    MINI_VPN_TCP_TX_BUFFER_BYTES=1048576
    MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=786432
    MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=196608
    KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=0
    normalize_downlink_backpressure_env &&
      [[ "${MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES:-}" == "786432" ]] &&
      [[ "${MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES:-}" == "196608" ]] &&
      [[ -z "${DOWNLINK_BACKPRESSURE_AUTO_REASON:-}" ]]
  ); then
    echo "suite self-test failed: non-legacy explicit backpressure should be preserved" >&2
    return 1
  fi
  if ! grep -q "SERVER_EVIDENCE_CHECK=0" <<<"$help_text"; then
    echo "suite self-test failed: server evidence help missing" >&2
    return 1
  fi
  if ! grep -q "TARGET_SSH_HOST=\"\"" <<<"$help_text"; then
    echo "suite self-test failed: target ssh help missing" >&2
    return 1
  fi

  local auth_script
  auth_script="$(auth_config_match_python)"
  for field in uuid_match password_match sni_match alpn_match tuic_password_len; do
    if ! grep -q "$field" <<<"$auth_script"; then
      echo "suite self-test failed: auth diagnostic script missing $field" >&2
      return 1
    fi
  done
  if grep -Eq 'hexdigest|sha256' <<<"$auth_script" ||
    grep -Eq 'print[(]f?"(uuid|password)=' <<<"$auth_script"; then
    echo "suite self-test failed: auth diagnostic script must not print secrets or derived hashes" >&2
    return 1
  fi

  local final_log final_summary
  final_log="$(cat <<'EOF'
🔎 tcp-downlink-flush pending_total=0 pending_max=0 pending_high=585869 remote_to_global_rx_bytes=55599078 flush_attempts=4242 no_send_capacity=0 send_window_samples=4242 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=720896 recv_queue_max=0 may_send_false=0 may_recv_false=0 send_slice_calls=4148 send_slice_accepted=55599078 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=418 headroom_limited_calls=1094 headroom_deferred_bytes=203578857 drain_credit_granted_bytes=1048576 drain_credit_planned_bytes=524288 drain_credit_used_bytes=262144 send_slice_max_accepted=178048 tun_flush_tx_calls=3148 tun_flush_tx_failures=0 tun_flush_deferred=0 dirty_handles=1
🔎 tcp-tun-egress-feedback paused=true reason=drop_delta tx_dropped_delta=1349 max_pressure=720896 total_pressure=1853914 high=524288 low=131072 drop_events=1 drop_delta_total=1349 max_delta=1349 pause_edges=1 resume_edges=0
🔎 tcp-handle-close handle=SocketHandle(1) direction=local_to_remote reason=uplink_channel_closed state=Relaying pending=566509 pending_high=585869 remote_to_global_rx_bytes=56886483 flush_attempts=16139 no_send_capacity=0 send_window_samples=16139 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=720896 recv_queue_max=0 may_send_false=0 may_recv_false=11897 send_slice_calls=4167 send_slice_accepted=56319974 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=12291 headroom_limited_calls=12973 headroom_deferred_bytes=3317103134 drain_credit_granted_bytes=2097152 drain_credit_planned_bytes=1048576 drain_credit_used_bytes=786432 send_slice_max_accepted=178048 tun_flush_tx_calls=3167 tun_flush_tx_failures=0 tun_flush_deferred=0 close_pending_class=active_send_capable close_pending_bytes=566509 terminal_pending_reap_bytes=0 close_egress_class=active_send_capable close_egress_bytes=720896 close_egress_drain_candidate=true tcp_state=CloseWait active=true can_send=true can_recv=false may_send=true may_recv=false send_capacity=1048576 send_queue=720896 recv_queue=0
🔎 tcp-tun-egress if=tun0 status=delta tx_dropped_total=2691 tx_dropped_delta=122 global_rx_paused=true pending_total=0 pending_max=0 pending_high=0 remote_to_global_rx_bytes=0 tun_flush_tx_calls=0 dirty_handles=0
🔎 tcp-tun-egress-feedback paused=true reason=drop_delta tx_dropped_delta=122 max_pressure=720896 total_pressure=1287405 high=524288 low=131072 drop_events=5 drop_delta_total=2691 max_delta=1349 pause_edges=1 resume_edges=0
EOF
)"
  final_summary="$(printf '%s\n' "$final_log" | summarize_final_lifecycle_window 0 "self-test-final")"
  if ! grep -q "final_pending_at_close: events=1 bytes=566509 max_bytes=566509 send_capable_events=1 send_capable_bytes=566509" <<<"$final_summary"; then
    echo "suite self-test failed: final lifecycle summary missed send-capable pending close" >&2
    printf '%s\n' "$final_summary" >&2
    return 1
  fi
  if ! grep -q "final_tun_egress_feedback: pause_edges=2 resume_edges=0 drop_events=5 drop_delta_total=2691 max_delta=1349 max_pressure_bytes=720896" <<<"$final_summary"; then
    echo "suite self-test failed: final lifecycle summary missed post-probe tun feedback drops" >&2
    printf '%s\n' "$final_summary" >&2
    return 1
  fi
  if ! grep -q "final_downlink_flush: attempts=16139 send_queue_max=720896 may_recv_false=11897 headroom_limited=12973 headroom_deferred_bytes=3317103134 drain_credit_granted_bytes=2097152 drain_credit_planned_bytes=1048576 drain_credit_used_bytes=786432" <<<"$final_summary"; then
    echo "suite self-test failed: final lifecycle summary missed close-line downlink maxima" >&2
    printf '%s\n' "$final_summary" >&2
    return 1
  fi

  local tmp_key
  tmp_key="$(mktemp "${TMPDIR:-/tmp}/knife14bp_vps_key.XXXXXX")" || return 1
  if ! (
    SERVER_EVIDENCE_CHECK=1
    EXIT_HOST=43.153.32.33
    TARGET=43.130.32.77
    EXIT_SSH_HOST=""
    TARGET_SSH_HOST=""
    EXIT_SSH_KEY=""
    TARGET_SSH_KEY=""
    KNIFE14_DEFAULT_VPS_SSH_KEY="$tmp_key"
    apply_server_evidence_ssh_defaults
    [[ "$EXIT_SSH_HOST" == "ubuntu@43.153.32.33" ]] &&
      [[ "$TARGET_SSH_HOST" == "ubuntu@43.130.32.77" ]] &&
      [[ "$EXIT_SSH_KEY" == "$tmp_key" ]] &&
      [[ "$TARGET_SSH_KEY" == "$tmp_key" ]]
  ); then
    rm -f "$tmp_key"
    echo "suite self-test failed: server evidence defaults for known VPS topology" >&2
    return 1
  fi

  if ! (
    SERVER_EVIDENCE_CHECK=1
    EXIT_HOST=43.153.32.33
    TARGET=43.130.32.77
    EXIT_SSH_HOST="custom-exit"
    TARGET_SSH_HOST="custom-target"
    EXIT_SSH_KEY="/custom/exit/key"
    TARGET_SSH_KEY="/custom/target/key"
    KNIFE14_DEFAULT_VPS_SSH_KEY="$tmp_key"
    apply_server_evidence_ssh_defaults
    [[ "$EXIT_SSH_HOST" == "custom-exit" ]] &&
      [[ "$TARGET_SSH_HOST" == "custom-target" ]] &&
      [[ "$EXIT_SSH_KEY" == "/custom/exit/key" ]] &&
      [[ "$TARGET_SSH_KEY" == "/custom/target/key" ]]
  ); then
    rm -f "$tmp_key"
    echo "suite self-test failed: server evidence defaults overwrote explicit SSH env" >&2
    return 1
  fi

  if ! (
    SERVER_EVIDENCE_CHECK=0
    EXIT_HOST=43.153.32.33
    TARGET=43.130.32.77
    EXIT_SSH_HOST=""
    TARGET_SSH_HOST=""
    EXIT_SSH_KEY=""
    TARGET_SSH_KEY=""
    KNIFE14_DEFAULT_VPS_SSH_KEY="$tmp_key"
    apply_server_evidence_ssh_defaults
    [[ -z "$EXIT_SSH_HOST" ]] &&
      [[ -z "$TARGET_SSH_HOST" ]] &&
      [[ -z "$EXIT_SSH_KEY" ]] &&
      [[ -z "$TARGET_SSH_KEY" ]]
  ); then
    rm -f "$tmp_key"
    echo "suite self-test failed: server evidence defaults applied while disabled" >&2
    return 1
  fi

  if ! (
    SERVER_EVIDENCE_CHECK=1
    EXIT_HOST=203.0.113.33
    TARGET=203.0.113.77
    EXIT_SSH_HOST=""
    TARGET_SSH_HOST=""
    EXIT_SSH_KEY=""
    TARGET_SSH_KEY=""
    KNIFE14_DEFAULT_VPS_SSH_KEY="$tmp_key"
    apply_server_evidence_ssh_defaults
    [[ -z "$EXIT_SSH_HOST" ]] &&
      [[ -z "$TARGET_SSH_HOST" ]] &&
      [[ -z "$EXIT_SSH_KEY" ]] &&
      [[ -z "$TARGET_SSH_KEY" ]]
  ); then
    rm -f "$tmp_key"
    echo "suite self-test failed: server evidence defaults applied to unknown topology" >&2
    return 1
  fi
  rm -f "$tmp_key"

  echo "suite self-test passed"
}

usage() {
  cat <<'USAGE'
usage: scripts/knife14b-usclient-tunnel-suite.sh
       scripts/knife14b-usclient-tunnel-suite.sh --self-test

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
  CC_SWEEP=""                optional space-separated CC variants, e.g. "cubic bbr"; empty keeps single-run behavior
  SUITE_TAG=knife14c        report/bundle filename tag
  MTU=1200                  TUN MTU passed to mini_vpn before client-tun starts
  TUN_TX_QUEUE_LEN=""       optional Linux TUN txqueuelen after startup; empty keeps OS default
  MINI_VPN_TCP_DIAG=1       emit knife14c per-handle TCP diagnostics
  RUN_BASE_MTU_P1=0         14c keeps one aligned MTU per process; use a separate MTU=1500 run for baseline
  BUILD_RELEASE=1           build target/release/mini_vpn before running; set 0 to reuse existing binary
  CARGO=<path>              cargo binary override, useful when running as root with rustup under /home/ubuntu
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
  DIRECT_IPERF_REVERSE_CHECK=1   direct Target iperf3 -R baseline before routing Target into TUN
  DIRECT_IPERF_REVERSE_REQUIRED=1 fail when the direct reverse check command fails; set 0 to warn
  EXIT_TO_TARGET_IPERF_CHECK=0    optionally SSH to Exit and test Exit<->Target iperf path
  EXIT_TO_TARGET_IPERF_REQUIRED=1 fail when enabled Exit<->Target check fails; set 0 to warn
  EXIT_SSH_HOST=""                SSH destination for Exit, e.g. ubuntu@43.153.32.33
  EXIT_SSH_PORT=22
  EXIT_SSH_KEY=""                 optional private key for Exit SSH; known Knife14 evidence host uses /home/ubuntu/.ssh/vpn if present
  EXIT_SSH_STRICT_HOST_KEY_CHECKING=accept-new  noninteractive host-key policy for Exit SSH
  EXIT_SSH_KNOWN_HOSTS_FILE="$OUT_DIR/exit_ssh_known_hosts"
  SERVER_EVIDENCE_CHECK=0         collect bounded .33/.77 evidence around each probe
  SERVER_EVIDENCE_SING_BOX_TAIL=220
  SERVER_EVIDENCE_TARGET_JOURNAL_TAIL=260
  TARGET_SSH_HOST=""              SSH destination for Target, e.g. ubuntu@43.130.32.77
  TARGET_SSH_PORT=22
  TARGET_SSH_KEY=""               optional private key for Target SSH; known Knife14 evidence host uses /home/ubuntu/.ssh/vpn if present
  TARGET_SSH_STRICT_HOST_KEY_CHECKING=accept-new
  TARGET_SSH_KNOWN_HOSTS_FILE="$OUT_DIR/target_ssh_known_hosts"
  RUN_REVERSE_FIRST_P1=0   run a fresh reverse-only P1 probe before the normal forward-first probe
  STOP_AFTER_REVERSE_FIRST_P1=0  stop after the fresh reverse-only P1 and final snapshots
  WAIT_QUIET_BEFORE_FULL=1  after standalone P1, wait for active relays to drop before full sweep
  QUIET_TIMEOUT_SECS=20
  QUIET_POLL_SECS=1
  IPERF_BUSY_RETRIES=3      retry each iperf sub-run when Target reports "server is busy"
  IPERF_BUSY_WAIT_SECS=5    seconds to wait between iperf busy retries
  MINI_VPN_TUIC_TCP_POOL=1  TUIC TCP connection pool; set >1 to isolate concurrent flow congestion
  MINI_VPN_TCP_RX_BUFFER_BYTES=1048576
  MINI_VPN_TCP_TX_BUFFER_BYTES=1048576
  MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=<auto>  empty/unset lets mini_vpn scale to TCP tx buffer
  MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=<auto>   empty/unset lets mini_vpn scale to TCP tx buffer / 4
  KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=0     set 1 to preserve inherited 524288/131072 legacy A/B values
  MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144
  MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216
  MINI_VPN_TUN_RX_DRAIN_BUDGET=0   default off; set >0 only for explicit TUN ingress drain A/B

Output:
  /tmp/conn/mvpn_knife14c_usclient_suite_<timestamp>.md
  /tmp/conn/mvpn_knife14c_usclient_suite_<timestamp>.tar.gz
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--self-test" ]]; then
  suite_self_test
  exit $?
fi

TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_DIR:-/tmp/conn}"
SUITE_TAG="${SUITE_TAG:-knife14c}"
TARGET="${TARGET:-43.130.32.77}"
IPERF_PORT="${IPERF_PORT:-5201}"
DURATION="${DURATION:-30}"
PARALLEL_SET="${PARALLEL_SET:-1 2 4 8}"
CC_SWEEP="${CC_SWEEP:-}"
CC_SWEEP_ACTIVE="${CC_SWEEP_ACTIVE:-0}"
CC_VARIANT_LABEL="${CC_VARIANT_LABEL:-}"
MTU="${MTU:-1200}"
TUN_TX_QUEUE_LEN="${TUN_TX_QUEUE_LEN:-}"
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
DIRECT_IPERF_REVERSE_CHECK="${DIRECT_IPERF_REVERSE_CHECK:-1}"
DIRECT_IPERF_REVERSE_REQUIRED="${DIRECT_IPERF_REVERSE_REQUIRED:-1}"
EXIT_TO_TARGET_IPERF_CHECK="${EXIT_TO_TARGET_IPERF_CHECK:-0}"
EXIT_TO_TARGET_IPERF_REQUIRED="${EXIT_TO_TARGET_IPERF_REQUIRED:-1}"
EXIT_SSH_HOST="${EXIT_SSH_HOST:-}"
EXIT_SSH_PORT="${EXIT_SSH_PORT:-22}"
EXIT_SSH_KEY="${EXIT_SSH_KEY:-}"
EXIT_SSH_STRICT_HOST_KEY_CHECKING="${EXIT_SSH_STRICT_HOST_KEY_CHECKING:-accept-new}"
EXIT_SSH_KNOWN_HOSTS_FILE="${EXIT_SSH_KNOWN_HOSTS_FILE:-$OUT_DIR/exit_ssh_known_hosts}"
SERVER_EVIDENCE_CHECK="${SERVER_EVIDENCE_CHECK:-0}"
SERVER_EVIDENCE_SING_BOX_TAIL="${SERVER_EVIDENCE_SING_BOX_TAIL:-$DEFAULT_SERVER_EVIDENCE_SING_BOX_TAIL}"
SERVER_EVIDENCE_TARGET_JOURNAL_TAIL="${SERVER_EVIDENCE_TARGET_JOURNAL_TAIL:-$DEFAULT_SERVER_EVIDENCE_TARGET_JOURNAL_TAIL}"
TARGET_SSH_HOST="${TARGET_SSH_HOST:-}"
TARGET_SSH_PORT="${TARGET_SSH_PORT:-22}"
TARGET_SSH_KEY="${TARGET_SSH_KEY:-}"
TARGET_SSH_STRICT_HOST_KEY_CHECKING="${TARGET_SSH_STRICT_HOST_KEY_CHECKING:-accept-new}"
TARGET_SSH_KNOWN_HOSTS_FILE="${TARGET_SSH_KNOWN_HOSTS_FILE:-$OUT_DIR/target_ssh_known_hosts}"
RUN_REVERSE_FIRST_P1="${RUN_REVERSE_FIRST_P1:-0}"
STOP_AFTER_REVERSE_FIRST_P1="${STOP_AFTER_REVERSE_FIRST_P1:-0}"
WAIT_QUIET_BEFORE_FULL="${WAIT_QUIET_BEFORE_FULL:-1}"
QUIET_TIMEOUT_SECS="${QUIET_TIMEOUT_SECS:-20}"
QUIET_POLL_SECS="${QUIET_POLL_SECS:-1}"
IPERF_BUSY_RETRIES="${IPERF_BUSY_RETRIES:-3}"
IPERF_BUSY_WAIT_SECS="${IPERF_BUSY_WAIT_SECS:-5}"

if ! mkdir -p "$OUT_DIR"; then
  echo "ERROR: cannot create OUT_DIR=$OUT_DIR" >&2
  echo "fix: sudo mkdir -p '$OUT_DIR' && sudo chown -R \"\$(id -u):\$(id -g)\" '$OUT_DIR'" >&2
  exit 1
fi
OUT_DIR_WRITE_PROBE="$OUT_DIR/.mvpn_write_probe_$$"
if ! touch "$OUT_DIR_WRITE_PROBE" >/dev/null 2>&1; then
  echo "ERROR: OUT_DIR is not writable by the current user: $OUT_DIR" >&2
  echo "fix: sudo chown -R \"\$(id -u):\$(id -g)\" '$OUT_DIR' && chmod u+rwx '$OUT_DIR'" >&2
  exit 1
fi
rm -f "$OUT_DIR_WRITE_PROBE"
REPORT="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_suite_${TS}.md"
if [[ -n "$CC_VARIANT_LABEL" ]]; then
  CLIENT_LOG="$OUT_DIR/mvpn_accept_${CC_VARIANT_LABEL}_${TS}.log"
else
  CLIENT_LOG="$OUT_DIR/mvpn_accept_${TS}.log"
fi
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

cc_variant_label() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_.-]/_/g'
}

run_cc_sweep_wrapper() {
  local all_status=0
  local cc
  local -a cc_variants
  read -r -a cc_variants <<< "$CC_SWEEP"

  append ""
  append "## CC Sweep"
  append "- cc_sweep: $CC_SWEEP"
  append "- parent_report: $REPORT"
  append "- parent_bundle: $BUNDLE"
  append ""
  append "Each CC variant is executed by a fresh child suite with its own client log,"
  append "probe reports, cleanup, and bundle. The parent bundle includes all child"
  append "artifacts found under $OUT_DIR."

  for cc in "${cc_variants[@]}"; do
    local label child_tag child_keep status f
    label="$(cc_variant_label "$cc")"
    if [[ -z "$label" ]]; then
      label="cc"
    fi
    child_tag="${SUITE_TAG}_${label}"
    child_keep=0

    append ""
    append "## CC Variant: $cc"
    append "- label: $label"
    append "- child_suite_tag: $child_tag"
    append '```bash'
    printf '$ env CC_SWEEP= CC_SWEEP_ACTIVE=1 CC_VARIANT_LABEL=%q MINI_VPN_TUIC_CC=%q SUITE_TAG=%q KEEP_TUNNEL=%q bash %q\n' \
      "$label" "$cc" "$child_tag" "$child_keep" "$0" | tee -a "$REPORT"
    append '```'

    set +e
    env CC_SWEEP= CC_SWEEP_ACTIVE=1 CC_VARIANT_LABEL="$label" \
      MINI_VPN_TUIC_CC="$cc" SUITE_TAG="$child_tag" KEEP_TUNNEL="$child_keep" \
      bash "$0" 2>&1 | tee -a "$REPORT"
    status=${PIPESTATUS[0]}
    set -u

    append ""
    append "- child_exit=$status"
    for f in "$OUT_DIR"/mvpn_"${child_tag}"_* "$OUT_DIR"/mvpn_accept_"${label}"_*.log; do
      [[ -f "$f" ]] && ARTIFACTS+=("$f")
    done

    if ((status != 0)); then
      all_status=$status
      warn "CC variant $cc failed; stopping sweep so the failure can be inspected before more VPS time is spent."
      break
    fi
  done

  if ((all_status == 0)); then
    RESULT_STATUS="COMPLETED"
    append ""
    append "## Send Back"
    append "请把这个 parent bundle 发回来："
    append_block text "$BUNDLE"
    append "里面包含每个 CC 子 suite 的 report、client log、probe report 和 child bundle。"
  else
    RESULT_STATUS="FAILED"
  fi
  exit "$all_status"
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

kill_client_tun_pids() {
  local pids_text="$1"
  local -a pids=()
  local pid

  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done <<< "$pids_text"
  if ((${#pids[@]} == 0)); then
    return 0
  fi

  run_cmd sudo kill "${pids[@]}" || true
  sleep 2

  local remaining=""
  remaining="$(client_tun_pids || true)"
  if [[ -z "$remaining" ]]; then
    return 0
  fi

  pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done <<< "$remaining"
  if ((${#pids[@]} > 0)); then
    run_cmd sudo kill -9 "${pids[@]}" || true
  fi
}

find_cargo() {
  local candidate

  if [[ -n "${CARGO:-}" && -x "$CARGO" ]]; then
    printf '%s\n' "$CARGO"
    return 0
  fi

  if command_status cargo; then
    command -v cargo
    return 0
  fi

  for candidate in \
    "${HOME:-}/.cargo/bin/cargo" \
    /home/ubuntu/.cargo/bin/cargo \
    /root/.cargo/bin/cargo
  do
    if [[ "$candidate" != "/.cargo/bin/cargo" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
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

run_exit_ssh_cmd() {
  local remote_cmd="$1"
  local -a ssh_cmd=(
    ssh
    -o BatchMode=yes
    -o ConnectTimeout=8
    -o "StrictHostKeyChecking=$EXIT_SSH_STRICT_HOST_KEY_CHECKING"
  )
  if [[ -n "$EXIT_SSH_KNOWN_HOSTS_FILE" ]]; then
    ssh_cmd+=(-o "UserKnownHostsFile=$EXIT_SSH_KNOWN_HOSTS_FILE")
  fi
  if [[ -n "$EXIT_SSH_KEY" ]]; then
    ssh_cmd+=(-i "$EXIT_SSH_KEY")
  fi
  if [[ -n "$EXIT_SSH_PORT" ]]; then
    ssh_cmd+=(-p "$EXIT_SSH_PORT")
  fi
  ssh_cmd+=("$EXIT_SSH_HOST" "$remote_cmd")
  run_cmd "${ssh_cmd[@]}"
}

exit_ssh_raw() {
  local remote_cmd="$1"
  local -a ssh_cmd=(
    ssh
    -o BatchMode=yes
    -o ConnectTimeout=8
    -o "StrictHostKeyChecking=$EXIT_SSH_STRICT_HOST_KEY_CHECKING"
  )
  if [[ -n "$EXIT_SSH_KNOWN_HOSTS_FILE" ]]; then
    ssh_cmd+=(-o "UserKnownHostsFile=$EXIT_SSH_KNOWN_HOSTS_FILE")
  fi
  if [[ -n "$EXIT_SSH_KEY" ]]; then
    ssh_cmd+=(-i "$EXIT_SSH_KEY")
  fi
  if [[ -n "$EXIT_SSH_PORT" ]]; then
    ssh_cmd+=(-p "$EXIT_SSH_PORT")
  fi
  ssh_cmd+=("$EXIT_SSH_HOST" "$remote_cmd")
  "${ssh_cmd[@]}"
}

target_ssh_raw() {
  local remote_cmd="$1"
  local -a ssh_cmd=(
    ssh
    -o BatchMode=yes
    -o ConnectTimeout=8
    -o "StrictHostKeyChecking=$TARGET_SSH_STRICT_HOST_KEY_CHECKING"
  )
  if [[ -n "$TARGET_SSH_KNOWN_HOSTS_FILE" ]]; then
    ssh_cmd+=(-o "UserKnownHostsFile=$TARGET_SSH_KNOWN_HOSTS_FILE")
  fi
  if [[ -n "$TARGET_SSH_KEY" ]]; then
    ssh_cmd+=(-i "$TARGET_SSH_KEY")
  fi
  if [[ -n "$TARGET_SSH_PORT" ]]; then
    ssh_cmd+=(-p "$TARGET_SSH_PORT")
  fi
  ssh_cmd+=("$TARGET_SSH_HOST" "$remote_cmd")
  "${ssh_cmd[@]}"
}

positive_int_or_default() {
  local value="$1"
  local fallback="$2"
  if [[ "$value" =~ ^[0-9]+$ && "$value" -gt 0 ]]; then
    printf '%s' "$value"
  else
    printf '%s' "$fallback"
  fi
}

collect_server_side_evidence() {
  local label="$1"
  local start_epoch="$2"
  local end_epoch="$3"

  append ""
  append "### Server Evidence: $label"
  append "- server_evidence_check: $SERVER_EVIDENCE_CHECK"
  append "- exit_ssh_host: ${EXIT_SSH_HOST:-<unset>}"
  append "- target_ssh_host: ${TARGET_SSH_HOST:-<unset>}"

  if [[ "$SERVER_EVIDENCE_CHECK" != "1" ]]; then
    append "skipped because SERVER_EVIDENCE_CHECK=$SERVER_EVIDENCE_CHECK"
    return 0
  fi

  local evidence_out="$OUT_DIR/mvpn_${SUITE_TAG}_server_evidence_${label}_${TS}.md"
  ARTIFACTS+=("$evidence_out")
  append "- out: $evidence_out"

  if ! [[ "$start_epoch" =~ ^[0-9]+$ && "$end_epoch" =~ ^[0-9]+$ ]]; then
    start_epoch="$(date +%s 2>/dev/null || printf '0')"
    end_epoch="$start_epoch"
  fi
  local since_epoch=$((start_epoch - 5))
  if ((since_epoch < 0)); then
    since_epoch=0
  fi
  local until_epoch=$((end_epoch + 10))
  local since_utc until_utc
  since_utc="$(date -u -d "@$since_epoch" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || printf '@%s' "$since_epoch")"
  until_utc="$(date -u -d "@$until_epoch" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || printf '@%s' "$until_epoch")"

  {
    echo "# Knife14 Server Evidence: $label"
    echo
    echo "- collected_at: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "- probe_start_epoch: $start_epoch"
    echo "- probe_end_epoch: $end_epoch"
    echo "- journal_since: $since_utc"
    echo "- journal_until: $until_utc"
    echo "- exit_ssh_host: ${EXIT_SSH_HOST:-<unset>}"
    if [[ -n "$EXIT_SSH_KEY" ]]; then
      echo "- exit_ssh_key: <set>"
    else
      echo "- exit_ssh_key: <unset>"
    fi
    echo "- target_ssh_host: ${TARGET_SSH_HOST:-<unset>}"
    if [[ -n "$TARGET_SSH_KEY" ]]; then
      echo "- target_ssh_key: <set>"
    else
      echo "- target_ssh_key: <unset>"
    fi
  } > "$evidence_out"

  {
    echo
    echo "## Time Context"
    echo
    echo '```text'
    date -u '+client_utc=%Y-%m-%dT%H:%M:%SZ'
    date '+client_epoch=%s'
    echo '```'
  } >> "$evidence_out"

  if [[ -n "$EXIT_SSH_HOST" ]]; then
    {
      echo
      echo "### Exit Time"
      echo
      echo '```text'
    } >> "$evidence_out"
    set +e
    exit_ssh_raw 'date -u "+exit_utc=%Y-%m-%dT%H:%M:%SZ"; date "+exit_epoch=%s"; timedatectl show -p NTPSynchronized -p SystemClockSynchronized --no-pager 2>/dev/null || true' >> "$evidence_out" 2>&1
    local exit_time_status=$?
    set -u
    {
      echo "exit_time_status=$exit_time_status"
      echo '```'
    } >> "$evidence_out"
  fi

  if [[ -n "$TARGET_SSH_HOST" ]]; then
    {
      echo
      echo "### Target Time"
      echo
      echo '```text'
    } >> "$evidence_out"
    set +e
    target_ssh_raw 'date -u "+target_utc=%Y-%m-%dT%H:%M:%SZ"; date "+target_epoch=%s"; timedatectl show -p NTPSynchronized -p SystemClockSynchronized --no-pager 2>/dev/null || true' >> "$evidence_out" 2>&1
    local target_time_status=$?
    set -u
    {
      echo "target_time_status=$target_time_status"
      echo '```'
    } >> "$evidence_out"
  fi

  {
    echo
    echo "## Exit sing-box Log"
    echo
  } >> "$evidence_out"
  if [[ -z "$EXIT_SSH_HOST" ]]; then
    echo "skipped: EXIT_SSH_HOST is unset." >> "$evidence_out"
  else
    local sing_tail target_regex exit_grep_regex redactor exit_log_cmd
    sing_tail="$(positive_int_or_default "$SERVER_EVIDENCE_SING_BOX_TAIL" "$DEFAULT_SERVER_EVIDENCE_SING_BOX_TAIL")"
    target_regex="${TARGET//./[.]}"
    exit_grep_regex="${target_regex}|inbound.*tuic|outbound/direct|connection download closed|stream .*canceled|fail auth|auth|tuic|error|ERROR|warn|WARN"
    redactor='s/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<uuid-redacted>/g; s/([Pp]assword[=: ]+)[^ ]+/\1<redacted>/g'
    printf -v exit_log_cmd 'sudo grep -E %q /var/log/sing-box.log 2>/dev/null | tail -n %q | sed -E %q || true' \
      "$exit_grep_regex" "$sing_tail" "$redactor"
    {
      echo '```text'
      echo "sing_box_tail_limit=$sing_tail"
    } >> "$evidence_out"
    set +e
    exit_ssh_raw "$exit_log_cmd" >> "$evidence_out" 2>&1
    local exit_log_status=$?
    set -u
    {
      echo "exit_log_status=$exit_log_status"
      echo '```'
    } >> "$evidence_out"
  fi

  {
    echo
    echo "## Target iperf3 Journal"
    echo
  } >> "$evidence_out"
  if [[ -z "$TARGET_SSH_HOST" ]]; then
    echo "skipped: TARGET_SSH_HOST is unset." >> "$evidence_out"
    append "- WARN: SERVER_EVIDENCE_CHECK=1 but TARGET_SSH_HOST is unset; .77 iperf3 journal was not collected."
  else
    local journal_tail target_journal_cmd
    journal_tail="$(positive_int_or_default "$SERVER_EVIDENCE_TARGET_JOURNAL_TAIL" "$DEFAULT_SERVER_EVIDENCE_TARGET_JOURNAL_TAIL")"
    printf -v target_journal_cmd 'journalctl -u iperf3 --since %q --until %q --no-pager 2>/dev/null | tail -n %q || true' \
      "$since_utc" "$until_utc" "$journal_tail"
    {
      echo '```text'
      echo "journal_tail_limit=$journal_tail"
    } >> "$evidence_out"
    set +e
    target_ssh_raw "$target_journal_cmd" >> "$evidence_out" 2>&1
    local target_journal_status=$?
    set -u
    {
      echo "target_journal_status=$target_journal_status"
      echo '```'
    } >> "$evidence_out"
  fi

  append ""
  append "#### Server Evidence $label Summary"
  append '```text'
  grep -E 'fail auth|accepted connection|connected|Mbits/sec|Kbits/sec|bits/sec|sender$|receiver$|outbound|inbound|tuic|canceled|closed|error|ERROR|WARN|warning|status=' "$evidence_out" |
    tail -180 | tee -a "$REPORT" || true
  append '```'
}

diagnose_exit_time_delta() {
  append ""
  append "### Client ↔ Exit Time Check"
  append '```text'
  local client_epoch remote_epoch status delta abs_delta
  client_epoch="$(date +%s 2>/dev/null || true)"
  remote_epoch="$(exit_ssh_raw 'date +%s' 2>&1)"
  status=$?
  {
    printf 'client_epoch=%s\n' "${client_epoch:-unknown}"
    printf 'exit_epoch=%s\n' "${remote_epoch:-unknown}"
    printf 'exit_epoch_status=%s\n' "$status"
    if [[ "$status" == "0" && "$client_epoch" =~ ^[0-9]+$ && "$remote_epoch" =~ ^[0-9]+$ ]]; then
      delta=$((client_epoch - remote_epoch))
      abs_delta="${delta#-}"
      printf 'client_minus_exit_seconds=%s\n' "$delta"
      printf 'abs_time_skew_seconds=%s\n' "$abs_delta"
      if ((abs_delta > 5)); then
        printf 'time_skew_warning=1\n'
      else
        printf 'time_skew_warning=0\n'
      fi
    fi
  } | tee -a "$REPORT"
  append '```'
}

run_auth_config_match_check() {
  if ! command_status python3; then
    warn "python3 not found; skipping no-secret TUIC config match check."
    return 0
  fi
  local script="$OUT_DIR/tuic_auth_match_check_${TS}.py"
  auth_config_match_python > "$script"
  append ""
  append "### No-Secret TUIC Config Match"
  run_cmd python3 "$script" || true
}

run_exit_auth_diagnostics() {
  append ""
  append "### Automated Exit TUIC/Auth Diagnostics"
  append "- exit_ssh_host: ${EXIT_SSH_HOST:-<unset>}"
  if [[ -n "$EXIT_SSH_KEY" ]]; then
    append "- exit_ssh_key: <set>"
  else
    append "- exit_ssh_key: <unset>"
  fi
  append "- note: this section prints only service state, time delta, lengths, and match booleans; it must not print TUIC secrets."

  if [[ -z "$EXIT_SSH_HOST" ]]; then
    warn "EXIT_SSH_HOST is empty; automated .33 auth diagnostics skipped."
    return 0
  fi

  diagnose_exit_time_delta
  run_exit_ssh_cmd 'date -u "+%Y-%m-%dT%H:%M:%SZ"; timedatectl show -p NTPSynchronized -p SystemClockSynchronized -p TimeUSec --no-pager 2>/dev/null || true' || true
  run_exit_ssh_cmd 'systemctl is-active sing-box; systemctl show sing-box -p ActiveState -p SubState -p ExecMainPID -p NRestarts --no-pager' || true
  run_exit_ssh_cmd "sudo ss -lunp | grep ':${EXIT_PORT:-8443}' || true" || true
  run_exit_ssh_cmd 'sudo /usr/bin/sing-box -c /etc/sing-box/config.json check' || true
  run_exit_ssh_cmd 'sudo python3 - <<'"'"'PY'"'"'
import json
cfg = json.load(open("/etc/sing-box/config.json"))
for ib in cfg.get("inbounds", []):
    if ib.get("type") == "tuic":
        users = ib.get("users") or []
        tls = ib.get("tls") or {}
        alpn = tls.get("alpn") or []
        print(f"tuic_listen={ib.get('"'"'listen'"'"', '"'"''"'"')}:{ib.get('"'"'listen_port'"'"', '"'"''"'"')}")
        print(f"tuic_users={len(users)}")
        print(f"tuic_uuid_lengths={[len(str(u.get('"'"'uuid'"'"', '"'"''"'"'))) for u in users]}")
        print(f"tuic_password_lengths={[len(str(u.get('"'"'password'"'"', '"'"''"'"'))) for u in users]}")
        print(f"tuic_alpn_lengths={[len(str(x)) for x in alpn]}")
        print(f"tuic_server_name_len={len(str(tls.get('"'"'server_name'"'"', '"'"''"'"')))}")
        print(f"tuic_certificate_path_set={int(bool(tls.get('"'"'certificate_path'"'"', '"'"''"'"')))}")
        print(f"tuic_congestion_control_set={int(bool(ib.get('"'"'congestion_control'"'"', '"'"''"'"')))}")
PY' || true
  run_exit_ssh_cmd 'sudo openssl x509 -in /etc/sing-box/server-cert.pem -noout -subject -issuer -dates -fingerprint -sha256 2>/dev/null || true' || true
  run_exit_ssh_cmd 'sudo grep "fail auth\|auth\|tuic" /var/log/sing-box.log 2>/dev/null | tail -n 80 | sed -E "s/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<uuid-redacted>/g; s/([Pp]assword[=: ]+)[^ ]+/\1<redacted>/g" || true' || true
  run_auth_config_match_check
}

preflight_exit_to_target_path() {
  append ""
  append "### Exit VPS ↔ Target VPS iperf3 Path (${EXIT_HOST} ↔ ${TARGET}:${IPERF_PORT})"
  append "- exit_to_target_iperf_check: $EXIT_TO_TARGET_IPERF_CHECK"
  append "- exit_to_target_iperf_required: $EXIT_TO_TARGET_IPERF_REQUIRED"
  append "- exit_ssh_host: ${EXIT_SSH_HOST:-<unset>}"
  append "- exit_ssh_strict_host_key_checking: $EXIT_SSH_STRICT_HOST_KEY_CHECKING"
  append "- exit_ssh_known_hosts_file: ${EXIT_SSH_KNOWN_HOSTS_FILE:-<unset>}"

  if [[ "$EXIT_TO_TARGET_IPERF_CHECK" != "1" ]]; then
    append "skipped because EXIT_TO_TARGET_IPERF_CHECK=$EXIT_TO_TARGET_IPERF_CHECK"
    append "manual commands to run on Exit VPS (${EXIT_HOST}) if reverse tunnel remains low:"
    append_block bash \
      "timeout $DIRECT_IPERF_TIMEOUT iperf3 -c $TARGET -p $IPERF_PORT -t $DIRECT_IPERF_DURATION -P 1" \
      "timeout $DIRECT_IPERF_TIMEOUT iperf3 -c $TARGET -p $IPERF_PORT -t $DIRECT_IPERF_DURATION -P 1 -R"
    return 0
  fi

  if [[ -z "$EXIT_SSH_HOST" ]]; then
    if [[ "$EXIT_TO_TARGET_IPERF_REQUIRED" == "1" ]]; then
      fail "EXIT_TO_TARGET_IPERF_CHECK=1 需要设置 EXIT_SSH_HOST，例如 EXIT_SSH_HOST=ubuntu@${EXIT_HOST}。"
    fi
    warn "EXIT_TO_TARGET_IPERF_CHECK=1 but EXIT_SSH_HOST is empty; skipping Exit↔Target path check."
    return 0
  fi

  local remote_preflight="command -v iperf3 >/dev/null && command -v timeout >/dev/null && ip route get $TARGET"
  if ! run_exit_ssh_cmd "$remote_preflight"; then
    if [[ "$EXIT_TO_TARGET_IPERF_REQUIRED" == "1" ]]; then
      fail "Exit VPS SSH/path preflight failed. 请确认 EXIT_SSH_HOST/KEY/PORT、.33 上 iperf3/timeout 是否可用，以及 .33 到 .77 是否有路由。"
    fi
    warn "Exit VPS SSH/path preflight failed; continuing without Exit↔Target attribution."
    return 0
  fi

  local remote_forward="timeout $DIRECT_IPERF_TIMEOUT iperf3 -c $TARGET -p $IPERF_PORT -t $DIRECT_IPERF_DURATION -P 1"
  if ! run_exit_ssh_cmd "$remote_forward"; then
    if [[ "$EXIT_TO_TARGET_IPERF_REQUIRED" == "1" ]]; then
      fail "Exit VPS -> Target VPS direct iperf3 failed. 请检查 .33 -> .77 路由、安全组或 .77 iperf3 服务。"
    fi
    warn "Exit VPS -> Target VPS direct iperf3 failed; continuing but forward tunnel attribution is incomplete."
  fi

  local remote_reverse="timeout $DIRECT_IPERF_TIMEOUT iperf3 -c $TARGET -p $IPERF_PORT -t $DIRECT_IPERF_DURATION -P 1 -R"
  if ! run_exit_ssh_cmd "$remote_reverse"; then
    if [[ "$EXIT_TO_TARGET_IPERF_REQUIRED" == "1" ]]; then
      fail "Target VPS -> Exit VPS direct iperf3 -R failed/low. 反向隧道验收无法归因；请检查 .77 -> .33 路由、安全组或 VPS provider path。"
    fi
    warn "Target VPS -> Exit VPS direct iperf3 -R failed; continuing but reverse tunnel attribution is incomplete."
  fi
}

preflight_vps_services() {
  append ""
  append "## VPS Service Preflight"
  append "- exit_vps: ${EXIT_HOST}:${EXIT_PORT}（TUIC/sing-box；UDP 服务由后续 mini_vpn handshake 做强校验）"
  append "- target_vps: ${TARGET}:${IPERF_PORT}（iperf3）"
  append "- direct_iperf_duration_secs: $DIRECT_IPERF_DURATION"
  append "- direct_iperf_reverse_check: $DIRECT_IPERF_REVERSE_CHECK"
  append "- direct_iperf_reverse_required: $DIRECT_IPERF_REVERSE_REQUIRED"
  append "- exit_to_target_iperf_check: $EXIT_TO_TARGET_IPERF_CHECK"
  append "- exit_to_target_iperf_required: $EXIT_TO_TARGET_IPERF_REQUIRED"
  append "- exit_ssh_host: ${EXIT_SSH_HOST:-<unset>}"

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

  if [[ "$DIRECT_IPERF_REVERSE_CHECK" == "1" ]]; then
    append ""
    append "### Target VPS iperf3 Reverse Baseline (${TARGET})"
    if ! run_cmd timeout "$DIRECT_IPERF_TIMEOUT" \
      iperf3 -c "$TARGET" -p "$IPERF_PORT" -t "$DIRECT_IPERF_DURATION" -P 1 -R; then
      if [[ "$DIRECT_IPERF_REVERSE_REQUIRED" == "1" ]]; then
        fail "Target VPS ${TARGET}:${IPERF_PORT} direct iperf3 -R 检查失败。反向隧道验收无法归因；请检查 .77 iperf3 服务、Client<->Target 路由/安全组，或临时设置 DIRECT_IPERF_REVERSE_REQUIRED=0 继续采样。"
      fi
      warn "Target VPS ${TARGET}:${IPERF_PORT} direct iperf3 -R 检查失败；继续运行，但 reverse tunnel 结果只能作为参考。"
    fi
  else
    append ""
    append "### Target VPS iperf3 Reverse Baseline (${TARGET})"
    append "skipped because DIRECT_IPERF_REVERSE_CHECK=$DIRECT_IPERF_REVERSE_CHECK"
  fi

  preflight_exit_to_target_path
}

route_target_into_tun() {
  TUN_IF="$(ip -o -4 addr show | awk '$4 ~ /^10[.]0[.]0[.]1\// {print $2; exit}')"
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

configure_tun_tx_queue_len() {
  if [[ -z "$TUN_TX_QUEUE_LEN" ]]; then
    return 0
  fi
  if ! [[ "$TUN_TX_QUEUE_LEN" =~ ^[0-9]+$ ]] || (( 10#$TUN_TX_QUEUE_LEN <= 0 )); then
    fail "TUN_TX_QUEUE_LEN 必须是正整数，当前值: $TUN_TX_QUEUE_LEN"
  fi

  append ""
  append "## TUN TX Queue Setup"
  append "- requested_tx_queue_len: $TUN_TX_QUEUE_LEN"
  run_cmd sudo ip link set dev "$TUN_IF" txqueuelen "$TUN_TX_QUEUE_LEN" || \
    fail "无法设置 $TUN_IF txqueuelen=$TUN_TX_QUEUE_LEN。请确认 iproute2/sudo 权限。"
  run_cmd ip link show "$TUN_IF" || true
}

tun_tx_queue_len_actual() {
  ip link show "$TUN_IF" 2>/dev/null | sed -n 's/.* qlen \([0-9][0-9]*\).*/\1/p' | head -1
}

probe_has_receiver_result() {
  local file="$1"
  grep -Eq 'receiver$' "$file"
}

run_lowrtt_probe() {
  local label="$1"
  local parallel="$2"
  local duration="$3"
  local probe_order="${4:-forward-first}"
  local probe_out="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_tunnel_${label}_${TS}.md"
  ARTIFACTS+=("$probe_out")

  append ""
  append "## Probe: $label"
  append "- out: $probe_out"
  append "- parallel_set: $parallel"
  append "- duration: ${duration}s"
  append "- probe_order: $probe_order"
  append "- client_log: $CLIENT_LOG"

  local probe_start_epoch probe_end_epoch
  probe_start_epoch="$(date +%s 2>/dev/null || printf '0')"
  run_cmd env \
    LOG="$CLIENT_LOG" \
    OUT="$probe_out" \
    PARALLEL_SET="$parallel" \
    DURATION="$duration" \
    PROBE_ORDER="$probe_order" \
    TUN_IF="$TUN_IF" \
    IPERF_BUSY_RETRIES="$IPERF_BUSY_RETRIES" \
    IPERF_BUSY_WAIT_SECS="$IPERF_BUSY_WAIT_SECS" \
    bash "$LOWRTT_SCRIPT" "$TARGET" "$IPERF_PORT"
  local status=$?
  probe_end_epoch="$(date +%s 2>/dev/null || printf '%s' "$probe_start_epoch")"

  append ""
  append "### Probe $label Summary"
  if [[ -f "$probe_out" ]]; then
    append '```text'
    grep -E 'Attribution Summary|iperf_sender_mbps|iperf_receiver_mbps|iperf_interval_profile:|throughput_shape:|tcp_pool:|local_write_pressure:|global_rx_pressure:|downlink_backpressure:|downlink_flush:|tcp_reverse_window:|terminal_pending_reap:|pending_at_close:|egress_at_close:|relay_late_remote:|tun_drops:|runtime_tun_egress:|tun_egress_feedback:|tun_rx_drain:|quic:|attribution:|local 10[.]0[.]0[.]1|receiver$|sender$|error -|Connection reset|log not found|📊|🔬|TUIC datagram|UDP relay mode|tuic-tcp-pool-reconnect|tcp-(relay-live|relay-write-half-closed|relay-close|handle-close|deferred-close-egress|reverse-window|local-write-pressure|global-rx-pressure|downlink-backpressure|downlink-flush|tun-rx-drain|tun-egress|loop-flush-tx|tun-flush-fail|send-slice-error)|exit=' "$probe_out" | tail -200 | tee -a "$REPORT" || true
    append '```'
  else
    append "probe report missing: $probe_out"
  fi

  collect_server_side_evidence "$label" "$probe_start_epoch" "$probe_end_epoch"

  return "$status"
}

append_final_lifecycle_summary() {
  local start_line="$1"
  local title="$2"

  append ""
  append "### Final Lifecycle Summary: $title"
  if [[ -f "$CLIENT_LOG" ]]; then
    append '```text'
    summarize_final_lifecycle_window "$start_line" "$title" < "$CLIENT_LOG" | tee -a "$REPORT"
    append '```'
  else
    append "client log not found: $CLIENT_LOG"
  fi
}

diagnose_auth_failure() {
  append ""
  append "## TUIC Startup Diagnosis"
  append "mini_vpn 没有成功连上 TUIC exit。Exit VPS ${EXIT_HOST:-unknown}:${EXIT_PORT:-unknown} 的 sing-box/TUIC 服务可能异常，或凭据/网络不匹配。优先检查这些点："
  run_exit_auth_diagnostics
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
      while IFS= read -r pid; do
        [[ -n "$pid" ]] && sudo kill -9 "$pid" >/dev/null 2>&1 || true
      done <<< "$(client_tun_pids || true)"
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
  echo "- repo_commit: $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "- host: $(hostname 2>/dev/null || echo unknown)"
  echo "- target: ${TARGET}:${IPERF_PORT}"
  echo "- out_dir: $OUT_DIR"
  echo "- report: $REPORT"
  echo "- client_log: $CLIENT_LOG"
} > "$REPORT"

if [[ -n "$CC_SWEEP" && "$CC_SWEEP_ACTIVE" != "1" ]]; then
  run_cc_sweep_wrapper
fi

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
if [[ "$SERVER_EVIDENCE_CHECK" == "1" ]] ||
  { [[ "$CHECK_VPS_SERVICES" == "1" ]] && [[ "$EXIT_TO_TARGET_IPERF_CHECK" == "1" ]]; }; then
  require_cmd ssh "sudo apt update && sudo apt install -y openssh-client" || missing=1
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
export MINI_VPN_TUIC_TCP_POOL="${MINI_VPN_TUIC_TCP_POOL:-1}"
export MINI_VPN_TCP_RX_BUFFER_BYTES="${MINI_VPN_TCP_RX_BUFFER_BYTES:-1048576}"
export MINI_VPN_TCP_TX_BUFFER_BYTES="${MINI_VPN_TCP_TX_BUFFER_BYTES:-1048576}"
export MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES="${MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES:-$DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES}"
export MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES="${MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES:-$DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES}"
export KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE="${KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE:-0}"
normalize_downlink_backpressure_env
export MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES="${MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES:-$DEFAULT_DOWNLINK_FLUSH_MAX_BYTES}"
export MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES="${MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES:-$DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES}"
export MINI_VPN_TUN_RX_DRAIN_BUDGET="${MINI_VPN_TUN_RX_DRAIN_BUDGET:-$DEFAULT_TUN_RX_DRAIN_BUDGET}"

case "$MINI_VPN_TUIC_SERVER" in
  *:*)
    EXIT_HOST="${EXIT_HOST:-${MINI_VPN_TUIC_SERVER%:*}}"
    EXIT_PORT="${MINI_VPN_TUIC_SERVER##*:}"
    ;;
  *) fail "MINI_VPN_TUIC_SERVER 必须包含 host:port，例如 43.153.32.33:8443" ;;
esac
apply_server_evidence_ssh_defaults

append "- EXIT_HOST=$EXIT_HOST"
append "- EXIT_PORT=$EXIT_PORT"
append "- MINI_VPN_UPSTREAM=$MINI_VPN_UPSTREAM"
append "- MINI_VPN_TUN_MTU=$MINI_VPN_TUN_MTU"
append "- MINI_VPN_TCP_DIAG=$MINI_VPN_TCP_DIAG"
append "- MINI_VPN_TUIC_CC=$MINI_VPN_TUIC_CC"
append "- CC_SWEEP=${CC_SWEEP:-<single>}"
append "- CC_VARIANT_LABEL=${CC_VARIANT_LABEL:-<none>}"
append "- RUN_REVERSE_FIRST_P1=$RUN_REVERSE_FIRST_P1"
append "- STOP_AFTER_REVERSE_FIRST_P1=$STOP_AFTER_REVERSE_FIRST_P1"
append "- TUN_TX_QUEUE_LEN=${TUN_TX_QUEUE_LEN:-<default>}"
append "- MINI_VPN_TUIC_UDP_MODE=$MINI_VPN_TUIC_UDP_MODE"
append "- MINI_VPN_TUIC_ZERO_RTT=$MINI_VPN_TUIC_ZERO_RTT"
append "- MINI_VPN_TUIC_TCP_POOL=$MINI_VPN_TUIC_TCP_POOL"
append "- MINI_VPN_TCP_RX_BUFFER_BYTES=$MINI_VPN_TCP_RX_BUFFER_BYTES"
append "- MINI_VPN_TCP_TX_BUFFER_BYTES=$MINI_VPN_TCP_TX_BUFFER_BYTES"
append "- KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=$KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE"
if [[ -n "$DOWNLINK_BACKPRESSURE_AUTO_REASON" ]]; then
  append "- downlink_backpressure_auto_reset=$DOWNLINK_BACKPRESSURE_AUTO_REASON"
fi
append "- MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=${MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES:-<auto>}"
append "- MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=${MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES:-<auto>}"
append "- MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=$MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES"
append "- MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=$MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES"
append "- MINI_VPN_TUN_RX_DRAIN_BUDGET=$MINI_VPN_TUN_RX_DRAIN_BUDGET"
append "- SERVER_EVIDENCE_CHECK=$SERVER_EVIDENCE_CHECK"
append "- SERVER_EVIDENCE_SING_BOX_TAIL=$SERVER_EVIDENCE_SING_BOX_TAIL"
append "- SERVER_EVIDENCE_TARGET_JOURNAL_TAIL=$SERVER_EVIDENCE_TARGET_JOURNAL_TAIL"
append "- EXIT_SSH_HOST=${EXIT_SSH_HOST:-<unset>}"
if [[ -n "$EXIT_SSH_KEY" ]]; then
  append "- EXIT_SSH_KEY=<set>"
else
  append "- EXIT_SSH_KEY=<unset>"
fi
append "- TARGET_SSH_HOST=${TARGET_SSH_HOST:-<unset>}"
if [[ -n "$TARGET_SSH_KEY" ]]; then
  append "- TARGET_SSH_KEY=<set>"
else
  append "- TARGET_SSH_KEY=<unset>"
fi

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

append ""
append "## Git / Binary Snapshot"
run_cmd git -C "$REPO_ROOT" rev-parse --short HEAD || true
run_cmd git -C "$REPO_ROOT" status --short || true

if [[ "$BUILD_RELEASE" == "1" ]]; then
  if ! CARGO_BIN="$(find_cargo)"; then
    append "- MISSING: cargo"
    append "  searched: CARGO, PATH, \$HOME/.cargo/bin/cargo, /home/ubuntu/.cargo/bin/cargo, /root/.cargo/bin/cargo"
    append "  fix: export CARGO=/home/ubuntu/.cargo/bin/cargo 或 export PATH=/home/ubuntu/.cargo/bin:\$PATH 后重跑"
    fail "BUILD_RELEASE=1 但 cargo 不可用。"
  fi
  if [[ -n "${CARGO:-}" && "$CARGO_BIN" != "$CARGO" ]]; then
    warn "CARGO is set but not executable: $CARGO; using $CARGO_BIN"
  fi
  append "- OK: cargo ($CARGO_BIN)"
  run_cmd "$CARGO_BIN" --version || true
  run_cmd "$CARGO_BIN" build --release || fail "cargo build --release 失败。请把 report 发回来。"
elif [[ ! -x "$BIN" ]]; then
  fail "binary missing: $BIN。设置 BUILD_RELEASE=1 或先运行 cargo build --release。"
fi
append "- binary: $BIN"
run_cmd ls -lh "$BIN" || true
if command_status sha256sum; then
  run_cmd sha256sum "$BIN" || true
else
  warn "sha256sum not found; skipping binary checksum."
fi
run_cmd "$BIN" --help || true

append ""
append "## Preflight Routes"
run_cmd ip route get "$EXIT_HOST" || true
run_cmd ip route get "$TARGET" || true
run_cmd ip -brief addr || true

append ""
append "## Stop Old Tunnel"
old_pids="$(client_tun_pids || true)"
if [[ -n "$old_pids" ]]; then
  append "old mini_vpn client-tun pids:"
  append_block text "$old_pids"
  if [[ "$KILL_OLD" == "1" ]]; then
    kill_client_tun_pids "$old_pids"
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
append "- command: sudo -E env MINI_VPN_TUN_MTU=$MTU MINI_VPN_TCP_DIAG=$MINI_VPN_TCP_DIAG MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=$METRICS_SECS MINI_VPN_TUIC_CC=$MINI_VPN_TUIC_CC MINI_VPN_TUIC_TCP_POOL=$MINI_VPN_TUIC_TCP_POOL MINI_VPN_TCP_RX_BUFFER_BYTES=$MINI_VPN_TCP_RX_BUFFER_BYTES MINI_VPN_TCP_TX_BUFFER_BYTES=$MINI_VPN_TCP_TX_BUFFER_BYTES MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=$MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=$MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=$MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=$MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES MINI_VPN_TUN_RX_DRAIN_BUDGET=$MINI_VPN_TUN_RX_DRAIN_BUDGET $BIN client-tun"
sudo -E env MINI_VPN_TUN_MTU="$MTU" MINI_VPN_TCP_DIAG="$MINI_VPN_TCP_DIAG" MINI_VPN_PROFILE_LOOP=1 \
  MINI_VPN_METRICS_SECS="$METRICS_SECS" MINI_VPN_TUIC_CC="$MINI_VPN_TUIC_CC" \
  MINI_VPN_TUIC_TCP_POOL="$MINI_VPN_TUIC_TCP_POOL" \
  MINI_VPN_TCP_RX_BUFFER_BYTES="$MINI_VPN_TCP_RX_BUFFER_BYTES" \
  MINI_VPN_TCP_TX_BUFFER_BYTES="$MINI_VPN_TCP_TX_BUFFER_BYTES" \
  MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES="$MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES" \
  MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES="$MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES" \
  MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES="$MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES" \
  MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES="$MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES" \
  MINI_VPN_TUN_RX_DRAIN_BUDGET="$MINI_VPN_TUN_RX_DRAIN_BUDGET" \
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
configure_tun_tx_queue_len

append ""
append "## Wait For First Metrics Tick"
sleep "$((METRICS_SECS + 2))"
append_block text "$(tail -n 200 "$CLIENT_LOG" 2>/dev/null || true)"

BASE_MTU="$(ip link show "$TUN_IF" 2>/dev/null | sed -n 's/.* mtu \([0-9][0-9]*\) .*/\1/p' | head -1)"
ACTUAL_TUN_TX_QUEUE_LEN="$(tun_tx_queue_len_actual)"
append ""
append "## MTU / Probe Plan"
append "- tun_if: $TUN_IF"
append "- base_mtu: ${BASE_MTU:-unknown}"
append "- test_mtu: $MTU"
append "- tun_tx_queue_len_requested: ${TUN_TX_QUEUE_LEN:-<default>}"
append "- tun_tx_queue_len_actual: ${ACTUAL_TUN_TX_QUEUE_LEN:-unknown}"
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

proceed_to_standard_p1=1
if [[ "$RUN_REVERSE_FIRST_P1" == "1" ]]; then
  run_lowrtt_probe "mtu${MTU}_reverse_first_p1" "1" "$DURATION" "reverse-only" || true
  REVERSE_FIRST_END_LINE="$(client_log_line_count)"
  if [[ "$STOP_AFTER_REVERSE_FIRST_P1" == "1" ]]; then
    proceed_to_standard_p1=0
    append ""
    append "## Standard P1 / Full Sweep Skipped"
    append "STOP_AFTER_REVERSE_FIRST_P1=1；只保留 clean reverse-first P1 窗口，随后采集 final snapshots 和 bundle。"
  elif [[ "$WAIT_QUIET_BEFORE_FULL" == "1" ]]; then
    if ! wait_for_quiet_tunnel "standard P1 probe" "$REVERSE_FIRST_END_LINE"; then
      proceed_to_standard_p1=0
      append ""
      append "## Standard P1 / Full Sweep Skipped"
      append "reverse-first P1 后 tunnel 没有在 ${QUIET_TIMEOUT_SECS}s 内确认归零；跳过后续 sweep，避免把旧连接残留误判成吞吐问题。"
    fi
  fi
fi

if [[ "$proceed_to_standard_p1" == "1" ]]; then
  run_lowrtt_probe "mtu${MTU}_p1" "1" "$DURATION" "forward-first" || true
else
  append ""
  append "## Standard P1 Probe Skipped"
  if [[ "$STOP_AFTER_REVERSE_FIRST_P1" == "1" ]]; then
    append "STOP_AFTER_REVERSE_FIRST_P1=1."
  else
    append "see reverse-first quiet wait result above."
  fi
fi
MTU_P1_OUT="$OUT_DIR/mvpn_${SUITE_TAG}_usclient_tunnel_mtu${MTU}_p1_${TS}.md"
P1_END_LINE="$(client_log_line_count)"

if [[ "$proceed_to_standard_p1" == "1" && -f "$MTU_P1_OUT" ]] && probe_has_receiver_result "$MTU_P1_OUT"; then
  if [[ "$WAIT_QUIET_BEFORE_FULL" == "1" ]]; then
    if wait_for_quiet_tunnel "full sweep" "$P1_END_LINE"; then
      run_lowrtt_probe "mtu${MTU}_full" "$PARALLEL_SET" "$DURATION" "forward-first" || true
    else
      append ""
      append "## Full Sweep Skipped"
      append "P1 后 tunnel 没有在 ${QUIET_TIMEOUT_SECS}s 内确认归零；跳过 full sweep，避免把旧连接残留误判成 P2/P4 问题。"
    fi
  else
    run_lowrtt_probe "mtu${MTU}_full" "$PARALLEL_SET" "$DURATION" "forward-first" || true
  fi
else
  append ""
  append "## Full Sweep Skipped"
  if [[ "$proceed_to_standard_p1" == "1" ]]; then
    append "MTU=$MTU P1 没有 receiver 结果，说明 tunnel 基础连通/iperf 控制连接已经失败；跳过 full sweep，避免浪费时间。"
  else
    if [[ "$STOP_AFTER_REVERSE_FIRST_P1" == "1" ]]; then
      append "STOP_AFTER_REVERSE_FIRST_P1=1；full sweep 同步跳过。"
    else
      append "standard P1 已因 reverse-first quiet wait 未通过而跳过；full sweep 同步跳过。"
    fi
  fi
fi

append ""
append "## Final Snapshots"
run_cmd ip route get "$TARGET" || true
run_cmd ip route get "$EXIT_HOST" || true
run_cmd ip -s link show "$TUN_IF" || true
append_final_lifecycle_summary 0 "whole-suite-final"
if [[ -n "${REVERSE_FIRST_END_LINE:-}" ]]; then
  append_final_lifecycle_summary "$REVERSE_FIRST_END_LINE" "post-reverse-first-final"
fi
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
