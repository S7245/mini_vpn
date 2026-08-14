#!/usr/bin/env bash

# Knife15 H10d16 macOS target-only soak runner.
#
# This is a human-in-the-loop runner. It never invokes sudo itself: the user
# runs every sudo command shown by --help. The first stage adds only explicit
# host routes for TARGET and optional DNS_TARGET, and never changes system DNS.

set -uo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
M2_EXIT_OBSERVER_SCRIPT="${M2_EXIT_OBSERVER_SCRIPT:-$SCRIPT_DIR/knife15-exit-target-observer.sh}"
SAFE_PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin"
PATH="$SAFE_PATH"
export PATH

BIN="${BIN:-$REPO/target/release/mini_vpn}"
TARGET="${TARGET:-43.130.32.77}"
DNS_TARGET="${DNS_TARGET:-}"
DNS_NAME="${DNS_NAME:-example.com}"
IPERF_PORT="${IPERF_PORT:-5201}"
DURATION="${DURATION:-20}"
PARALLEL="${PARALLEL:-1}"
METRICS_SECS="${METRICS_SECS:-30}"
SAMPLE_SECS="${SAMPLE_SECS:-30}"
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-40}"
MAX_LOG_BYTES="${MAX_LOG_BYTES:-268435456}"
LOG_KEEP_BYTES="${LOG_KEEP_BYTES:-134217728}"
MIN_FREE_KB="${MIN_FREE_KB:-1048576}"
M0_BASELINE_DIR="${M0_BASELINE_DIR:-}"
M0_DIRECT_DIR="${M0_DIRECT_DIR:-}"
M1_BASELINE_DIR="${M1_BASELINE_DIR:-}"
M1_DIRECT_DIR="${M1_DIRECT_DIR:-}"
M2_BASELINE_DIR="${M2_BASELINE_DIR:-}"
M2_DIRECT_DIR="${M2_DIRECT_DIR:-}"
M2_RESOURCE_PREFLIGHT_DIR="${M2_RESOURCE_PREFLIGHT_DIR:-}"
M2_RESOURCE_PROFILE_HELPER="$SCRIPT_DIR/knife15-m2-resource-profile.py"
M2_RESOURCE_PREFLIGHT_RUNNER="$SCRIPT_DIR/knife15-m2-resource-preflight.sh"
M0_DIRECT_MAX_AGE_SECS=900
M0_TOTAL_SECS="${M0_TOTAL_SECS:-7200}"
M0_TCP_SECS="${M0_TCP_SECS:-300}"
M0_UDP_SECS="${M0_UDP_SECS:-180}"
M0_SHORT_SECS="${M0_SHORT_SECS:-10}"
M0_SHORT_COUNT="${M0_SHORT_COUNT:-6}"
M0_IDLE_SECS="${M0_IDLE_SECS:-300}"
M0_FINAL_DRAIN_SECS="${M0_FINAL_DRAIN_SECS:-120}"
M1_TOTAL_SECS="${M1_TOTAL_SECS:-28800}"
M1_STEADY_A_SECS="${M1_STEADY_A_SECS:-7200}"
M1_IDLE_SECS="${M1_IDLE_SECS:-300}"
M1_QUIET_SECS="${M1_QUIET_SECS:-3600}"
M1_STEADY_B_SECS="${M1_STEADY_B_SECS:-7200}"
M1_CHURN_SECS="${M1_CHURN_SECS:-3600}"
M1_STEADY_C_SECS="${M1_STEADY_C_SECS:-6000}"
M1_FINAL_DRAIN_SECS="${M1_FINAL_DRAIN_SECS:-300}"
M1_TCP_SECS="${M1_TCP_SECS:-300}"
M1_UDP_SECS="${M1_UDP_SECS:-180}"
M1_SHORT_SECS="${M1_SHORT_SECS:-10}"
M1_STEADY_SHORT_COUNT="${M1_STEADY_SHORT_COUNT:-6}"
M1_QUIET_SHORT_COUNT="${M1_QUIET_SHORT_COUNT:-6}"
M1_CHURN_SHORT_COUNT="${M1_CHURN_SHORT_COUNT:-24}"
M1_RATE_CAP_BPS=200000000
M2_TOTAL_SECS="${M2_TOTAL_SECS:-86400}"
M2_STEADY_A_SECS="${M2_STEADY_A_SECS:-14400}"
M2_IDLE_SECS="${M2_IDLE_SECS:-600}"
M2_QUIET_A_SECS="${M2_QUIET_A_SECS:-10800}"
M2_STEADY_B_SECS="${M2_STEADY_B_SECS:-14400}"
M2_CHURN_SECS="${M2_CHURN_SECS:-10800}"
M2_QUIET_B_SECS="${M2_QUIET_B_SECS:-10800}"
M2_STEADY_C_SECS="${M2_STEADY_C_SECS:-21600}"
M2_FINAL_DRAIN_SECS="${M2_FINAL_DRAIN_SECS:-600}"
M2_TCP_SECS="${M2_TCP_SECS:-300}"
M2_UDP_SECS="${M2_UDP_SECS:-180}"
M2_SHORT_SECS="${M2_SHORT_SECS:-10}"
M2_STEADY_SHORT_COUNT="${M2_STEADY_SHORT_COUNT:-6}"
M2_QUIET_SHORT_COUNT="${M2_QUIET_SHORT_COUNT:-6}"
M2_CHURN_SHORT_COUNT="${M2_CHURN_SHORT_COUNT:-24}"
M2_MIN_FREE_KB=4194304
M2_EXPECTED_CYCLES=93
M2_EXPECTED_TCP_RESULTS=934
M2_EXPECTED_UDP_RESULTS=95
M2_EXPECTED_PHASE_RESULTS=1029
M2_EXPECTED_CHECKPOINTS=6
M2_EGRESS_URL=https://api.ipify.org
M2_BROWSER_URL=https://example.com/
M2_EGRESS_TARGET=api.ipify.org:443
M2_BROWSER_TARGET=example.com:443
# iperf only reports completed application buffers. Shenzhen's 0.131 Mbit/s
# reverse path delivered about 16KiB/s with an 8.3KiB cwnd, so a 1KiB observer
# preserves multiple visible blocks per second while forward stays unchanged.
TCP_REVERSE_IPERF_LENGTH_BYTES=1024
M0_IPERF3_BIN=iperf3
M0_DIG_BIN=dig
M0_SLEEP_BIN=sleep
M2_ROUTE_BIN=route
M2_IFCONFIG_BIN=ifconfig
M2_NETWORKSETUP_BIN=networksetup
M2_DSCACHEUTIL_BIN=dscacheutil
M2_CURL_BIN=curl
M2_PUBLIC_LOW_PROBE=1.1.1.1
M2_PUBLIC_HIGH_PROBE=129.1.1.1
M2_FAKE_PROBE=198.18.0.1
M2_IPV6_PROBE=2001:4860:4860::8888
M2_IPV6_ROUTE_STATUS=unknown
M2_IPV6_ROUTE_CLASSIFICATION=unknown
M2_IPV6_ROUTE_INTERFACE=none
M2_IPV6_ROUTE_TEXT=
STATE_DIR="/var/run/mini_vpn_knife15_macos_state"
SERVER_HOST=""
SERVER_PORT=""
TARGET_READY_UTC=""
TARGET_READY_RECEIVER_BPS=""
DIRECT_CONTINUITY_REASON=not_run
SOAK_STAGE=m0
SOAK_LABEL=M0
SOAK_EVIDENCE_DIR=m0
SOAK_STATUS_FILE=m0.status
SOAK_CONTINUE_DATA_QUALITY=0
SOAK_VIOLATIONS_FILE=
SOAK_SUCCESS_STATUS=complete
SOAK_REAL_CLIENT_PROBE=0
M2_REAL_CLIENT_EVIDENCE_DIR=m2-real-client
M2_COMPLETE_CYCLE_INDEX=0
M2_EXIT_OBSERVER_ARMED=0
M2_EXIT_OBSERVER_RUN_DIR=
M2_EXIT_OBSERVER_TARGET=
M2_EXIT_OBSERVER_IPERF_PORT=
M2_EXIT_OBSERVER_TUIC_PORT=

ACTION="${1:---help}"

usage() {
  cat <<'USAGE'
Knife15 H10d16 macOS target-only soak runner.

The user runs every sudo command. The script never invokes sudo by itself.
Initial scope is target-only; it does not change system DNS or install a
default route.

Usage:
  bash scripts/knife15-macos-soak.sh --self-test
  bash scripts/knife15-macos-soak.sh preflight
  bash scripts/knife15-macos-soak.sh m2-ipv6-check
  bash scripts/knife15-macos-soak.sh baseline
  bash scripts/knife15-macos-soak.sh baseline-check
  bash scripts/knife15-macos-soak.sh direct-discriminator
  sudo -E bash scripts/knife15-macos-soak.sh start
  sudo -E bash scripts/knife15-macos-soak.sh status
  sudo -E bash scripts/knife15-macos-soak.sh event "label"
  sudo -E bash scripts/knife15-macos-soak.sh snapshot
  sudo -E bash scripts/knife15-macos-soak.sh smoke
  sudo -E bash scripts/knife15-macos-soak.sh m0
  sudo -E bash scripts/knife15-macos-soak.sh m1
  sudo -E bash scripts/knife15-macos-soak.sh m1-diagnostic
  sudo -E bash scripts/knife15-macos-soak.sh m2-qualification
  sudo -E bash scripts/knife15-macos-soak.sh m2
  sudo -E bash scripts/knife15-macos-soak.sh stop
  sudo -E bash scripts/knife15-macos-soak.sh bundle

Required local environment (export it locally; never paste secrets into chat):
  MINI_VPN_TUIC_SERVER=<IPv4>:8443
  MINI_VPN_TUIC_UUID=<uuid>
  MINI_VPN_TUIC_PASSWORD=<password>
  MINI_VPN_TUIC_SNI=<server-name>
  MINI_VPN_TUIC_CA_PATH=<certificate-path>

Important optional environment:
  TARGET=43.130.32.77       target-only iperf host route
  DNS_TARGET=8.8.8.8        optional resolver host route; system DNS unchanged
  DNS_NAME=example.com      fake-IP DNS smoke name
  BIN=target/release/mini_vpn
  OUT_DIR=/tmp/mini_vpn_knife15_macos_<timestamp>
  DURATION=20 PARALLEL=1 IPERF_PORT=5201
  METRICS_SECS=30 SAMPLE_SECS=30
  M0_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_REPLACE_WITH_TIMESTAMP
  M0_DIRECT_DIR=/tmp/mini_vpn_knife15_macos_direct_REPLACE_WITH_TIMESTAMP
  M1_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_REPLACE_WITH_TIMESTAMP
  M1_DIRECT_DIR=/tmp/mini_vpn_knife15_macos_direct_REPLACE_WITH_TIMESTAMP
  M2_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_REPLACE_WITH_TIMESTAMP
  M2_DIRECT_DIR=/tmp/mini_vpn_knife15_macos_direct_REPLACE_WITH_TIMESTAMP
  M2_RESOURCE_PREFLIGHT_DIR=/tmp/mini_vpn_knife15_resource_REPLACE_WITH_TIMESTAMP
  EXIT_SSH_HOST=ubuntu@43.153.32.33
  EXIT_SSH_KEY=$HOME/.ssh/vpn

Workflow:
  1. cargo build --release
  2. export the five MINI_VPN_TUIC_* values locally
  3. bash scripts/knife15-macos-soak.sh --self-test
  4. bash scripts/knife15-macos-soak.sh preflight
  5. bash scripts/knife15-macos-soak.sh baseline
  6. export M0_BASELINE_DIR='REPLACE_WITH_BASELINE_DIRECTORY_FROM_STEP_5'
  7. bash scripts/knife15-macos-soak.sh direct-discriminator
  8. export M0_DIRECT_DIR='REPLACE_WITH_DIRECT_DIRECTORY_FROM_STEP_7'
  9. sudo -v
 10. sudo -E bash scripts/knife15-macos-soak.sh start
 11. sudo -E bash scripts/knife15-macos-soak.sh smoke
 12. sudo -E bash scripts/knife15-macos-soak.sh m0
 13. sudo -E bash scripts/knife15-macos-soak.sh status
 14. sudo -E bash scripts/knife15-macos-soak.sh stop

M1 workflow (use a fresh terminal and keep every other VPN/TUN disabled):
  1. unset M0_BASELINE_DIR M0_DIRECT_DIR M1_BASELINE_DIR M1_DIRECT_DIR M2_BASELINE_DIR M2_DIRECT_DIR
  2. cargo build --release
  3. export the five MINI_VPN_TUIC_* values and TARGET/DNS/IPERF settings
  4. bash scripts/knife15-macos-soak.sh --self-test
  5. bash scripts/knife15-macos-soak.sh preflight
  6. bash scripts/knife15-macos-soak.sh baseline
  7. export M1_BASELINE_DIR='REPLACE_WITH_BASELINE_DIRECTORY_FROM_STEP_6'
  8. bash scripts/knife15-macos-soak.sh direct-discriminator
  9. export M1_DIRECT_DIR='REPLACE_WITH_DIRECT_DIRECTORY_FROM_STEP_8'
 10. sudo -v
 11. sudo -E bash scripts/knife15-macos-soak.sh start
 12. sudo -E bash scripts/knife15-macos-soak.sh smoke
 13. sudo -E bash scripts/knife15-macos-soak.sh m1
 14. sudo -E bash scripts/knife15-macos-soak.sh status
 15. sudo -E bash scripts/knife15-macos-soak.sh stop

For a longitudinal diagnostic instead of formal acceptance, replace step 13
with:
  sudo -E bash scripts/knife15-macos-soak.sh m1-diagnostic

Never run m0, m1, m1-diagnostic, m2-qualification, and m2 in one TUN run. Both M1 actions have a
28,800-second traffic/drain budget and normally take slightly more than eight
wall hours.
Use m1-diagnostic only when a complete longitudinal artifact is required:
data-quality violations are recorded and continued, safety failures still
stop immediately, and the result can never satisfy formal M1 acceptance.

M2 workflow (use a fresh terminal; M2 temporarily owns IPv4 split-default
routes and the active physical service DNS until stop):
  1. unset M0_BASELINE_DIR M0_DIRECT_DIR M1_BASELINE_DIR M1_DIRECT_DIR
  2. unset M2_BASELINE_DIR M2_DIRECT_DIR M2_RESOURCE_PREFLIGHT_DIR
  3. cargo build --release
  4. export the five MINI_VPN_TUIC_* values and TARGET/DNS/IPERF settings
  5. bash scripts/knife15-macos-soak.sh --self-test
  6. bash scripts/knife15-macos-soak.sh preflight
  7. disable physical-service IPv6 as documented, then run:
     bash scripts/knife15-macos-soak.sh m2-ipv6-check
  8. bash scripts/knife15-macos-soak.sh baseline
  9. export M2_BASELINE_DIR='REPLACE_WITH_BASELINE_DIRECTORY_FROM_STEP_8'
 10. bash scripts/knife15-macos-soak.sh direct-discriminator
 11. export M2_DIRECT_DIR='REPLACE_WITH_DIRECT_DIRECTORY_FROM_STEP_10'
 12. run the reviewed resource preflight and export its exact
     M2_RESOURCE_PREFLIGHT_DIR (do not use repository fixtures)
 13. sudo -v
 14. sudo -E bash scripts/knife15-macos-soak.sh start
 15. sudo -E bash scripts/knife15-macos-soak.sh smoke
 16. export EXIT_SSH_HOST for the admitted candidate
 17. export EXIT_SSH_KEY="$HOME/.ssh/vpn"
 18. OBSERVER_TIMEOUT_SECS=93600 bash scripts/knife15-exit-target-observer.sh start
 19. caffeinate -dimsu sudo -E bash scripts/knife15-macos-soak.sh m2
 20. sudo -E bash scripts/knife15-macos-soak.sh status
 21. sudo -E bash scripts/knife15-macos-soak.sh stop

For the strict resource qualification, replace step 19 with:
  caffeinate -dimsu sudo -E bash scripts/knife15-macos-soak.sh m2-qualification

M2 qualification runs exactly two 300s forward + 300s reverse + 180s
reverse-UDP + 10s short-forward cycles. It normally takes about 30 minutes,
stops on the same data-quality/safety failures, and can never satisfy formal
M2 acceptance. Always continue with status and stop.

The background watchdog samples process/utun state and removes only the host
routes owned by this run if mini_vpn exits. start proves Target readiness
before mutation. The first stop publishes one sanitized immutable evidence
bundle; repeated stop only reports the same path and checksum.
USAGE
}

timestamp() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
}

m0_progress() {
  [[ "${M0_QUIET:-0}" == "1" ]] || echo "$*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_root() {
  if [[ "$(id -u)" != "0" ]]; then
    die "this action mutates or inspects root-owned TUN state; run exactly: sudo -E bash scripts/knife15-macos-soak.sh $ACTION"
  fi
}

validate_positive_integer() {
  local value="${1:-}"
  [[ "$value" =~ ^[0-9]+$ ]] && ((10#$value > 0))
}

decimal_le() {
  local value="${1:-}"
  local limit="${2:-}"
  awk -v value="$value" -v limit="$limit" 'BEGIN {
    if (value !~ /^[0-9]+([.][0-9]+)?$/ ||
        limit !~ /^[0-9]+([.][0-9]+)?$/) exit 1
    exit !((value + 0) <= (limit + 0))
  }'
}

validate_m0_formal_config() {
  [[ "$M0_TOTAL_SECS" == "7200" && "$M0_TCP_SECS" == "300" && \
    "$M0_UDP_SECS" == "180" && "$M0_SHORT_SECS" == "10" && \
    "$M0_SHORT_COUNT" == "6" && "$M0_IDLE_SECS" == "300" && \
    "$M0_FINAL_DRAIN_SECS" == "120" ]]
}

validate_m1_formal_config() {
  [[ "$M1_TOTAL_SECS" == "28800" && "$M1_STEADY_A_SECS" == "7200" && \
    "$M1_IDLE_SECS" == "300" && "$M1_QUIET_SECS" == "3600" && \
    "$M1_STEADY_B_SECS" == "7200" && "$M1_CHURN_SECS" == "3600" && \
    "$M1_STEADY_C_SECS" == "6000" && "$M1_FINAL_DRAIN_SECS" == "300" && \
    "$M1_TCP_SECS" == "300" && "$M1_UDP_SECS" == "180" && \
    "$M1_SHORT_SECS" == "10" && "$M1_STEADY_SHORT_COUNT" == "6" && \
    "$M1_QUIET_SHORT_COUNT" == "6" && "$M1_CHURN_SHORT_COUNT" == "24" && \
    "$METRICS_SECS" == "30" && "$SAMPLE_SECS" == "30" ]]
}

validate_m2_formal_config() {
  [[ "$M2_TOTAL_SECS" == "86400" && \
    "$M2_STEADY_A_SECS" == "14400" && "$M2_IDLE_SECS" == "600" && \
    "$M2_QUIET_A_SECS" == "10800" && "$M2_STEADY_B_SECS" == "14400" && \
    "$M2_CHURN_SECS" == "10800" && "$M2_QUIET_B_SECS" == "10800" && \
    "$M2_STEADY_C_SECS" == "21600" && "$M2_FINAL_DRAIN_SECS" == "600" && \
    "$M2_TCP_SECS" == "300" && "$M2_UDP_SECS" == "180" && \
    "$M2_SHORT_SECS" == "10" && "$M2_STEADY_SHORT_COUNT" == "6" && \
    "$M2_QUIET_SHORT_COUNT" == "6" && "$M2_CHURN_SHORT_COUNT" == "24" && \
    "$M2_EGRESS_URL" == "https://api.ipify.org" && \
    "$M2_BROWSER_URL" == "https://example.com/" && \
    "$M2_EGRESS_TARGET" == "api.ipify.org:443" && \
    "$M2_BROWSER_TARGET" == "example.com:443" && \
    "$METRICS_SECS" == "30" && "$SAMPLE_SECS" == "30" ]]
}

m2_window_count_model() {
  local remaining="$1"
  local short_count="$2"
  local tcp=0 udp=0 phases=0 cycles=0 duration short_index
  while ((10#$remaining > 0)); do
    duration="$M2_TCP_SECS"
    ((10#$duration > 10#$remaining)) && duration="$remaining"
    tcp=$((tcp + 1))
    phases=$((phases + 1))
    remaining=$((10#$remaining - 10#$duration))
    ((remaining > 0)) || break
    duration="$M2_TCP_SECS"
    ((10#$duration > 10#$remaining)) && duration="$remaining"
    tcp=$((tcp + 1))
    phases=$((phases + 1))
    remaining=$((10#$remaining - 10#$duration))
    ((remaining > 0)) || break
    duration="$M2_UDP_SECS"
    ((10#$duration > 10#$remaining)) && duration="$remaining"
    udp=$((udp + 1))
    phases=$((phases + 1))
    remaining=$((10#$remaining - 10#$duration))
    ((remaining > 0)) || break
    for ((short_index = 1; short_index <= 10#$short_count && remaining > 0; short_index++)); do
      duration="$M2_SHORT_SECS"
      ((10#$duration > 10#$remaining)) && duration="$remaining"
      tcp=$((tcp + 1))
      phases=$((phases + 1))
      remaining=$((10#$remaining - 10#$duration))
    done
    ((remaining > 0)) || break
    cycles=$((cycles + 1))
  done
  printf '%s %s %s %s\n' "$cycles" "$tcp" "$udp" "$phases"
}

m2_formal_count_model() {
  local window mode_short counts
  local cycles=0 tcp=0 udp=0 phases=0 one_cycles one_tcp one_udp one_phases
  for window in \
    "$M2_STEADY_A_SECS:$M2_STEADY_SHORT_COUNT" \
    "$M2_QUIET_A_SECS:$M2_QUIET_SHORT_COUNT" \
    "$M2_STEADY_B_SECS:$M2_STEADY_SHORT_COUNT" \
    "$M2_CHURN_SECS:$M2_CHURN_SHORT_COUNT" \
    "$M2_QUIET_B_SECS:$M2_QUIET_SHORT_COUNT" \
    "$M2_STEADY_C_SECS:$M2_STEADY_SHORT_COUNT"; do
    mode_short="${window#*:}"
    window="${window%%:*}"
    counts="$(m2_window_count_model "$window" "$mode_short")" || return 1
    read -r one_cycles one_tcp one_udp one_phases <<<"$counts"
    cycles=$((cycles + one_cycles))
    tcp=$((tcp + one_tcp))
    udp=$((udp + one_udp))
    phases=$((phases + one_phases))
  done
  printf '%s %s %s %s\n' "$cycles" "$tcp" "$udp" "$phases"
}

validate_ipv4() {
  local value="${1:-}"
  local a b c d extra octet
  IFS=. read -r a b c d extra <<<"$value"
  [[ -n "$a" && -n "$b" && -n "$c" && -n "$d" && -z "${extra:-}" ]] || return 1
  for octet in "$a" "$b" "$c" "$d"; do
    [[ "$octet" =~ ^[0-9]+$ ]] || return 1
    ((10#$octet >= 0 && 10#$octet <= 255)) || return 1
  done
}

validate_uuid() {
  local value="${1:-}"
  [[ "$value" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]
}

validate_dns_name() {
  local value="${1:-}"
  [[ "$value" =~ ^[A-Za-z0-9.-]{1,253}$ ]] && [[ "$value" != .* ]] && [[ "$value" != *. ]]
}

validate_run_dir_path() {
  local value="${1:-}"
  [[ "$value" =~ ^/tmp/mini_vpn_knife15_macos_[A-Za-z0-9._-]+$ ]]
}

validate_baseline_dir_path() {
  local value="${1:-}"
  [[ "$value" =~ ^/tmp/mini_vpn_knife15_macos_baseline_[A-Za-z0-9._-]+$ ]]
}

validate_direct_dir_path() {
  local value="${1:-}"
  [[ "$value" =~ ^/tmp/mini_vpn_knife15_macos_direct_[A-Za-z0-9._-]+$ ]]
}

validate_resource_preflight_dir_path() {
  local value="${1:-}"
  [[ "$value" =~ ^/tmp/mini_vpn_knife15_resource_[A-Za-z0-9._-]+$ ]]
}

m2_resource_json_value() {
  local file_path="${1:-}" key="${2:-}"
  [[ -f "$file_path" && ! -L "$file_path" && \
    "$key" =~ ^[a-z][a-z0-9_]*$ ]] || return 1
  /usr/bin/python3 -I - "$file_path" "$key" <<'PY_M2_RESOURCE_JSON'
import json
import sys


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


with open(sys.argv[1], encoding="utf-8") as handle:
    root = json.load(handle, object_pairs_hook=unique_object)
if not isinstance(root, dict) or sys.argv[2] not in root:
    raise SystemExit(1)
value = root[sys.argv[2]]
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, (str, int)):
    print(value)
else:
    raise SystemExit(1)
PY_M2_RESOURCE_JSON
}

selected_direct_baseline_dir() {
  local selected=0
  [[ -n "$M0_BASELINE_DIR" ]] && selected=$((selected + 1))
  [[ -n "$M1_BASELINE_DIR" ]] && selected=$((selected + 1))
  [[ -n "$M2_BASELINE_DIR" ]] && selected=$((selected + 1))
  if ((selected != 1)); then
    return 1
  fi
  if [[ -n "$M2_BASELINE_DIR" ]]; then
    printf '%s\n' "$M2_BASELINE_DIR"
  elif [[ -n "$M1_BASELINE_DIR" ]]; then
    printf '%s\n' "$M1_BASELINE_DIR"
  elif [[ -n "$M0_BASELINE_DIR" ]]; then
    printf '%s\n' "$M0_BASELINE_DIR"
  else
    return 1
  fi
}

selected_direct_epoch_is_frozen() {
  local baseline_dir="$1"
  if [[ -n "$M2_BASELINE_DIR" && "$baseline_dir" == "$M2_BASELINE_DIR" ]]; then
    [[ "$M2_TCP_SECS" == "300" ]]
  elif [[ -n "$M1_BASELINE_DIR" && "$baseline_dir" == "$M1_BASELINE_DIR" ]]; then
    [[ "$M1_TCP_SECS" == "300" ]]
  elif [[ -n "$M0_BASELINE_DIR" && "$baseline_dir" == "$M0_BASELINE_DIR" ]]; then
    [[ "$M0_TCP_SECS" == "300" ]]
  else
    return 1
  fi
}

parse_server() {
  local server="${1:-}"
  [[ "$server" == *:* ]] || return 1
  SERVER_HOST="${server%:*}"
  SERVER_PORT="${server##*:}"
  validate_ipv4 "$SERVER_HOST" || return 1
  validate_positive_integer "$SERVER_PORT" || return 1
  ((10#$SERVER_PORT <= 65535)) || return 1
}

safe_event_label() {
  printf '%s' "${1:-}" | tr '\t\r\n' '   ' | cut -c 1-160
}

list_utuns() {
  ifconfig -l 2>/dev/null | tr ' ' '\n' | awk '/^utun[0-9]+$/ {print}' | sort
}

route_interface_from_text() {
  awk '/interface:/ {print $2; exit}'
}

route_gateway_from_text() {
  awk '/gateway:/ {print $2; exit}'
}

network_service_for_interface_from_text() {
  local interface="$1"
  awk -v interface="$interface" '
    /^\([0-9]+\) / {
      service = $0
      sub(/^\([0-9]+\) /, "", service)
      disabled = 0
      next
    }
    /^\(\*\) / {
      service = ""
      disabled = 1
      next
    }
    $0 ~ /Device: [^)]+\)$/ {
      device = $0
      sub(/^.*Device: /, "", device)
      sub(/\)$/, "", device)
      if (!disabled && service != "" && device == interface) {
        matches++
        selected = service
      }
    }
    END {
      if (matches != 1) exit 1
      print selected
    }
  '
}

normalized_dns_snapshot_from_text() {
  awk '
    /^There aren.t any DNS Servers set on .+[.]$/ {
      if (NR != 1) invalid++
      empty = 1
      next
    }
    {
      if (empty || $0 == "" ||
          ($0 !~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ &&
           $0 !~ /^[0-9A-Fa-f:%]+$/)) {
        invalid++
      } else {
        values[++count] = $0
      }
    }
    END {
      if (invalid || (empty && count != 0) || (!empty && count == 0)) exit 1
      if (empty) {
        print "EMPTY"
      } else {
        for (i = 1; i <= count; i++) print values[i]
      }
    }
  '
}

ipv4_is_fake() {
  local value="${1:-}"
  local first second third fourth extra
  validate_ipv4 "$value" || return 1
  IFS=. read -r first second third fourth extra <<<"$value"
  [[ "$first" == "198" && \
    ( "$second" == "18" || "$second" == "19" ) ]]
}

m2_ipv6_route_classification_from_text() {
  local route_status="${1:-}" route_text interface
  [[ "$route_status" =~ ^[0-9]+$ ]] || return 1
  route_text="$(sed -n '1,$p')"
  interface="$(route_interface_from_text <<<"$route_text")"
  if [[ -z "$interface" ]] && \
    grep -Fqx 'route: writing to routing socket: not in table' \
      <<<"$route_text"; then
    printf '%s\n' 'safe_absent none'
    return 0
  fi
  if ((10#$route_status != 0)); then
    printf '%s\n' 'unknown none'
    return 0
  fi
  if [[ -z "$interface" ]]; then
    printf '%s\n' 'unknown none'
  elif [[ "$interface" == "lo0" || "$interface" == utun* ]]; then
    printf 'safe_tunnel %s\n' "$interface"
  else
    printf 'unsafe_physical %s\n' "$interface"
  fi
}

observe_m2_ipv6_route() {
  local classification
  M2_IPV6_ROUTE_TEXT="$(
    "$M2_ROUTE_BIN" -n get -inet6 "$M2_IPV6_PROBE" 2>&1
  )"
  M2_IPV6_ROUTE_STATUS=$?
  classification="$(
    printf '%s\n' "$M2_IPV6_ROUTE_TEXT" | \
      m2_ipv6_route_classification_from_text "$M2_IPV6_ROUTE_STATUS"
  )" || {
    M2_IPV6_ROUTE_CLASSIFICATION=unknown
    M2_IPV6_ROUTE_INTERFACE=none
    return 1
  }
  read -r M2_IPV6_ROUTE_CLASSIFICATION M2_IPV6_ROUTE_INTERFACE \
    <<<"$classification"
  [[ -n "$M2_IPV6_ROUTE_CLASSIFICATION" && \
    -n "$M2_IPV6_ROUTE_INTERFACE" ]]
}

write_m2_ipv6_route_evidence() {
  local evidence_file="$1"
  [[ -n "$evidence_file" && ! -L "$evidence_file" ]] || return 1
  observe_m2_ipv6_route || return 1
  {
    printf '%s\n' \
      'schema=knife15-macos-m2-ipv6-route-v1' \
      "timestamp=$(timestamp)" \
      "probe=$M2_IPV6_PROBE" \
      "route_status=$M2_IPV6_ROUTE_STATUS" \
      "classification=$M2_IPV6_ROUTE_CLASSIFICATION" \
      "interface=$M2_IPV6_ROUTE_INTERFACE" \
      'route_text_begin'
    printf '%s\n' "$M2_IPV6_ROUTE_TEXT"
    printf '%s\n' 'route_text_end'
  } >"$evidence_file" || return 1
  [[ "$M2_IPV6_ROUTE_CLASSIFICATION" == "safe_absent" || \
    "$M2_IPV6_ROUTE_CLASSIFICATION" == "safe_tunnel" ]]
}

m2_ipv6_route_interface() {
  observe_m2_ipv6_route || return 1
  case "$M2_IPV6_ROUTE_CLASSIFICATION" in
    safe_absent)
      printf '%s\n' none
      ;;
    safe_tunnel)
      printf '%s\n' "$M2_IPV6_ROUTE_INTERFACE"
      ;;
    *)
      return 1
      ;;
  esac
}

m2_route_text() {
  "$M2_ROUTE_BIN" -n get "$1" 2>/dev/null
}

m2_route_interface() {
  m2_route_text "$1" | route_interface_from_text
}

m2_route_gateway() {
  m2_route_text "$1" | route_gateway_from_text
}

m2_ipv6_route_is_safe() {
  m2_ipv6_route_interface >/dev/null
}

run_m2_ipv6_check() {
  [[ "$(uname -s)" == "Darwin" ]] || \
    die "Knife15 M2 IPv6 check requires Darwin"
  require_command route
  observe_m2_ipv6_route || die "cannot classify the M2 IPv6 route lookup"
  printf '%s\n' \
    'schema=knife15-macos-m2-ipv6-route-v1' \
    "probe=$M2_IPV6_PROBE" \
    "route_status=$M2_IPV6_ROUTE_STATUS" \
    "classification=$M2_IPV6_ROUTE_CLASSIFICATION" \
    "interface=$M2_IPV6_ROUTE_INTERFACE" \
    'route_text_begin'
  printf '%s\n' "$M2_IPV6_ROUTE_TEXT"
  printf '%s\n' 'route_text_end'
  case "$M2_IPV6_ROUTE_CLASSIFICATION" in
    safe_absent|safe_tunnel)
      echo "PASS: M2 IPv6 route check"
      ;;
    unsafe_physical)
      die "M2 IPv6 check found physical interface $M2_IPV6_ROUTE_INTERFACE; do not run baseline"
      ;;
    *)
      die "M2 IPv6 route outcome is unknown; do not run baseline"
      ;;
  esac
}

m2_dns_snapshot_for_service() {
  local service="$1"
  "$M2_NETWORKSETUP_BIN" -getdnsservers "$service" 2>/dev/null | \
    normalized_dns_snapshot_from_text
}

m2_saved_dns_snapshot() {
  local snapshot_file="$1"
  local first lines
  [[ -f "$snapshot_file" && ! -L "$snapshot_file" ]] || return 1
  first="$(sed -n '1p' "$snapshot_file")"
  lines="$(awk 'END {print NR + 0}' "$snapshot_file")"
  if [[ "$first" == "EMPTY" ]]; then
    [[ "$lines" == "1" ]] || return 1
    printf '%s\n' EMPTY
  else
    normalized_dns_snapshot_from_text <"$snapshot_file"
  fi
}

m2_dns_snapshot_matches_target() {
  local service="$1"
  local target="$2"
  [[ "$(m2_dns_snapshot_for_service "$service")" == "$target" ]]
}

m2_restore_dns_snapshot() {
  local service="$1"
  local snapshot_file="$2"
  local snapshot value
  local -a dns_servers=()
  snapshot="$(m2_saved_dns_snapshot "$snapshot_file")" || return 1
  if [[ "$snapshot" == "EMPTY" ]]; then
    "$M2_NETWORKSETUP_BIN" -setdnsservers "$service" Empty
  else
    while IFS= read -r value; do
      [[ -n "$value" ]] || return 1
      dns_servers+=("$value")
    done <<<"$snapshot"
    ((${#dns_servers[@]} > 0)) || return 1
    "$M2_NETWORKSETUP_BIN" -setdnsservers "$service" "${dns_servers[@]}"
  fi
}

m2_curl_metadata_values() {
  local metadata_file="$1"
  [[ -f "$metadata_file" && ! -L "$metadata_file" ]] || return 1
  awk -F= '
    $1 == "remote_ip" && NF == 2 { remote = $2; remote_count++ }
    $1 == "http_code" && NF == 2 { code = $2; code_count++ }
    $1 == "size_download" && NF == 2 { bytes = $2; bytes_count++ }
    END {
      if (NR != 3 || remote_count != 1 || code_count != 1 ||
          bytes_count != 1 || remote !~ /^[0-9.]+$/ ||
          code !~ /^[0-9][0-9][0-9]$/ ||
          bytes !~ /^[0-9]+([.][0-9]+)?$/) exit 1
      printf "%s %s %.0f\n", remote, code, bytes
    }
  ' "$metadata_file"
}

m2_real_client_result_is_valid() {
  local result_file="$1"
  local expected_label expected observed egress_remote egress_code egress_bytes
  local browser_remote browser_code browser_bytes
  local utun physical_interface physical_gateway dns_target ipv6_route_interface
  [[ -f "$result_file" && ! -L "$result_file" ]] || return 1
  [[ "$(m0_profile_value "$result_file" schema)" == \
    "knife15-macos-m2-real-client-v1" ]] || return 1
  expected_label="$(basename "$result_file" .txt)"
  [[ "$(m0_profile_value "$result_file" label)" == "$expected_label" ]] || return 1
  expected="$(m0_profile_value "$result_file" expected_egress)"
  observed="$(m0_profile_value "$result_file" observed_egress)"
  egress_remote="$(m0_profile_value "$result_file" egress_remote_ip)"
  egress_code="$(m0_profile_value "$result_file" egress_http_code)"
  egress_bytes="$(m0_profile_value "$result_file" egress_bytes)"
  browser_remote="$(m0_profile_value "$result_file" browser_remote_ip)"
  browser_code="$(m0_profile_value "$result_file" browser_http_code)"
  browser_bytes="$(m0_profile_value "$result_file" browser_bytes)"
  utun="$(m0_profile_value "$result_file" expected_utun)"
  physical_interface="$(m0_profile_value "$result_file" expected_physical_interface)"
  physical_gateway="$(m0_profile_value "$result_file" expected_physical_gateway)"
  dns_target="$(m0_profile_value "$result_file" expected_dns_target)"
  validate_ipv4 "$expected" && [[ "$observed" == "$expected" ]] || return 1
  ipv4_is_fake "$egress_remote" && ipv4_is_fake "$browser_remote" || return 1
  [[ "$egress_code" =~ ^[0-9]{3}$ && "$browser_code" =~ ^[0-9]{3}$ && \
    "$egress_bytes" =~ ^[1-9][0-9]*$ && \
    "$browser_bytes" =~ ^[1-9][0-9]*$ ]] || return 1
  ((10#$egress_code >= 200 && 10#$egress_code < 300 && \
    10#$browser_code >= 200 && 10#$browser_code < 400)) || return 1
  [[ "$utun" == utun[0-9]* && "$physical_interface" != utun* && \
    -n "$physical_interface" && -n "$physical_gateway" && \
    "$(m0_profile_value "$result_file" target_route)" == "$utun" && \
    "$(m0_profile_value "$result_file" dns_target_route)" == "$utun" && \
    "$(m0_profile_value "$result_file" public_low_route)" == "$utun" && \
    "$(m0_profile_value "$result_file" public_high_route)" == "$utun" && \
    "$(m0_profile_value "$result_file" fake_ip_route)" == "$utun" && \
    "$(m0_profile_value "$result_file" exit_route)" == "$physical_interface" && \
    "$(m0_profile_value "$result_file" exit_gateway)" == "$physical_gateway" && \
    "$(m0_profile_value "$result_file" system_dns)" == "$dns_target" ]] || return 1
  ipv6_route_interface="$(m0_profile_value "$result_file" ipv6_route_interface)"
  [[ "$ipv6_route_interface" == "none" || "$ipv6_route_interface" == "lo0" || \
    "$ipv6_route_interface" == utun* ]] && \
    [[ "$(m0_profile_value "$result_file" network_control_evidence)" == "PASS" ]]
}

run_m2_real_client_probe() {
  local run_dir="$1"
  local label="$2"
  local evidence_dir="${M2_REAL_CLIENT_EVIDENCE_DIR:-m2-real-client}"
  local result_file="$run_dir/$evidence_dir/${label}.txt"
  local egress_body="$run_dir/$evidence_dir/.${label}.egress-body.$$"
  local egress_meta="$run_dir/$evidence_dir/.${label}.egress-meta.$$"
  local browser_body="$run_dir/$evidence_dir/.${label}.browser-body.$$"
  local browser_meta="$run_dir/$evidence_dir/.${label}.browser-meta.$$"
  local expected observed egress_remote egress_code egress_bytes
  local browser_remote browser_code browser_bytes
  local utun target dns_target physical_interface physical_gateway ipv6_interface
  m0_assert_run_healthy "$run_dir" || return 1
  m2_full_tunnel_is_active || return 1
  network_control_is_recent || return 1
  expected="$(read_state exit_host 2>/dev/null || true)"
  validate_ipv4 "$expected" || return 1
  rm -f "$egress_body" "$egress_meta" "$browser_body" "$browser_meta"
  if ! run_m0_logged "$egress_meta" 40 \
    /usr/bin/env -u http_proxy -u https_proxy -u all_proxy -u no_proxy \
      -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u NO_PROXY \
      "$M2_CURL_BIN" --silent --show-error --location --max-redirs 3 \
        --proto '=https' --connect-timeout 10 --max-time 30 \
        --output "$egress_body" \
        --write-out $'remote_ip=%{remote_ip}\nhttp_code=%{http_code}\nsize_download=%{size_download}\n' \
        "$M2_EGRESS_URL"; then
    rm -f "$egress_body" "$browser_body"
    return 1
  fi
  read -r egress_remote egress_code egress_bytes \
    <<<"$(m2_curl_metadata_values "$egress_meta")" || {
    rm -f "$egress_body" "$browser_body"
    return 1
  }
  observed="$(tr -d '[:space:]' <"$egress_body" 2>/dev/null)"
  if ! run_m0_logged "$browser_meta" 40 \
    /usr/bin/env -u http_proxy -u https_proxy -u all_proxy -u no_proxy \
      -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u NO_PROXY \
      "$M2_CURL_BIN" --silent --show-error --location --max-redirs 3 \
        --proto '=https' --connect-timeout 10 --max-time 30 \
        --output "$browser_body" \
        --write-out $'remote_ip=%{remote_ip}\nhttp_code=%{http_code}\nsize_download=%{size_download}\n' \
        "$M2_BROWSER_URL"; then
    rm -f "$egress_body" "$browser_body"
    return 1
  fi
  read -r browser_remote browser_code browser_bytes \
    <<<"$(m2_curl_metadata_values "$browser_meta")" || {
    rm -f "$egress_body" "$browser_body"
    return 1
  }
  rm -f "$egress_body" "$browser_body" "$egress_meta" "$browser_meta" \
    "$egress_meta.command" "$browser_meta.command"
  m2_full_tunnel_is_active && network_control_is_recent || return 1
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  physical_interface="$(read_state m2.physical_interface 2>/dev/null || true)"
  physical_gateway="$(read_state m2.physical_gateway 2>/dev/null || true)"
  ipv6_interface="$(m2_ipv6_route_interface)" || return 1
  cat >"$result_file" <<EOF_M2_REAL_CLIENT
schema=knife15-macos-m2-real-client-v1
timestamp=$(timestamp)
label=$label
expected_egress=$expected
observed_egress=$observed
egress_remote_ip=$egress_remote
egress_http_code=$egress_code
egress_bytes=$egress_bytes
browser_remote_ip=$browser_remote
browser_http_code=$browser_code
browser_bytes=$browser_bytes
expected_utun=$utun
expected_physical_interface=$physical_interface
expected_physical_gateway=$physical_gateway
expected_dns_target=$dns_target
target_route=$(m2_route_interface "$target")
dns_target_route=$(m2_route_interface "$dns_target")
public_low_route=$(m2_route_interface "$M2_PUBLIC_LOW_PROBE")
public_high_route=$(m2_route_interface "$M2_PUBLIC_HIGH_PROBE")
fake_ip_route=$(m2_route_interface "$M2_FAKE_PROBE")
exit_route=$(m2_route_interface "$expected")
exit_gateway=$(m2_route_gateway "$expected")
system_dns=$(m2_dns_snapshot_for_service "$(read_state m2.dns_service)")
ipv6_route_interface=$ipv6_interface
network_control_evidence=PASS
EOF_M2_REAL_CLIENT
  if ! m2_real_client_result_is_valid "$result_file"; then
    return 1
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE real client complete label=$label remote=$browser_remote"
}

m2_full_tunnel_is_active() {
  local utun target dns_target exit_host physical_interface physical_gateway
  local dns_service
  [[ "$(read_state m2.full_tunnel 2>/dev/null || true)" == "active" ]] || return 1
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  physical_interface="$(read_state m2.physical_interface 2>/dev/null || true)"
  physical_gateway="$(read_state m2.physical_gateway 2>/dev/null || true)"
  dns_service="$(read_state m2.dns_service 2>/dev/null || true)"
  [[ -n "$utun" && -n "$target" && -n "$dns_target" && -n "$exit_host" && \
    -n "$physical_interface" && -n "$physical_gateway" && -n "$dns_service" ]] || \
    return 1
  [[ "$(m2_route_interface "$target")" == "$utun" && \
    "$(m2_route_interface "$dns_target")" == "$utun" && \
    "$(m2_route_interface "$M2_PUBLIC_LOW_PROBE")" == "$utun" && \
    "$(m2_route_interface "$M2_PUBLIC_HIGH_PROBE")" == "$utun" && \
    "$(m2_route_interface "$M2_FAKE_PROBE")" == "$utun" && \
    "$(m2_route_interface "$exit_host")" == "$physical_interface" && \
    "$(m2_route_gateway "$exit_host")" == "$physical_gateway" ]] || return 1
  m2_dns_snapshot_matches_target "$dns_service" "$dns_target" || return 1
  m2_ipv6_route_is_safe
}

m2_full_tunnel_is_restored() {
  local utun exit_host dns_service snapshot_file expected_dns
  utun="$(read_state utun 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  dns_service="$(read_state m2.dns_service 2>/dev/null || true)"
  snapshot_file="$(read_state m2.dns_before_file 2>/dev/null || true)"
  [[ -n "$utun" && -n "$exit_host" && -n "$dns_service" && \
    -n "$snapshot_file" ]] || return 1
  [[ "$(m2_route_interface "$M2_PUBLIC_LOW_PROBE")" != "$utun" && \
    "$(m2_route_interface "$M2_PUBLIC_HIGH_PROBE")" != "$utun" && \
    "$(m2_route_interface "$M2_FAKE_PROBE")" != "$utun" && \
    "$(m2_route_interface "$exit_host")" != "$utun" ]] || return 1
  expected_dns="$(m2_saved_dns_snapshot "$snapshot_file")" || return 1
  [[ "$(m2_dns_snapshot_for_service "$dns_service")" == "$expected_dns" ]] || \
    return 1
}

m2_owned_route_cleanup_classification() {
  local current_interface="${1:-}" current_gateway="${2:-}"
  local utun="${3:-}" physical_interface="${4:-}"
  local physical_gateway="${5:-}" tun_available="${6:-}"
  [[ -n "$current_interface" && -n "$utun" && \
    -n "$physical_interface" && -n "$physical_gateway" && \
    ( "$tun_available" == "0" || "$tun_available" == "1" ) ]] || return 1
  if [[ "$current_interface" == "$utun" ]]; then
    printf '%s\n' delete_owned
  elif [[ "$tun_available" == "0" && \
    "$current_interface" == "$physical_interface" && \
    "$current_gateway" == "$physical_gateway" ]]; then
    printf '%s\n' release_kernel_reaped
  else
    printf '%s\n' mismatch
  fi
}

m2_cleanup_owned_interface_route() {
  local run_dir="$1" state_name="$2" label="$3" probe="$4"
  local destination="$5" utun="$6" physical_interface="$7"
  local physical_gateway="$8" tun_available="$9"
  local current_interface current_gateway classification
  [[ "$(read_state "$state_name" 2>/dev/null || true)" == "1" ]] || return 0
  current_interface="$(m2_route_interface "$probe")"
  current_gateway="$(m2_route_gateway "$probe")"
  classification="$(m2_owned_route_cleanup_classification \
    "$current_interface" "$current_gateway" "$utun" "$physical_interface" \
    "$physical_gateway" "$tun_available")" || return 1
  case "$classification" in
    delete_owned)
      "$M2_ROUTE_BIN" -n delete -net "$destination" \
        >>"$run_dir/cleanup.log" 2>&1 || return 1
      ;;
    release_kernel_reaped)
      printf 'release kernel-reaped %s route: interface=%s gateway=%s utun=%s\n' \
        "$label" "$current_interface" "$current_gateway" "$utun" \
        >>"$run_dir/cleanup.log" || return 1
      append_event_to "$run_dir" \
        "m2 kernel-reaped route released: label=$label interface=$current_interface gateway=$current_gateway" || \
        return 1
      ;;
    *)
      printf 'refuse mismatched %s route: interface=%s gateway=%s utun=%s physical=%s/%s tun_available=%s\n' \
        "$label" "${current_interface:-missing}" "${current_gateway:-missing}" \
        "$utun" "$physical_interface" "$physical_gateway" "$tun_available" \
        >>"$run_dir/cleanup.log" || true
      return 1
      ;;
  esac
  write_state "$state_name" 0
}

deactivate_m2_full_tunnel() {
  local run_dir="$1"
  local status utun exit_host dns_target physical_interface physical_gateway
  local dns_service snapshot_file current_interface current_gateway failed=0
  local tun_available=1
  status="$(read_state m2.full_tunnel 2>/dev/null || true)"
  [[ -n "$status" ]] || return 0
  if [[ "$status" == "inactive" ]]; then
    m2_full_tunnel_is_restored
    return
  fi
  utun="$(read_state utun 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  physical_interface="$(read_state m2.physical_interface 2>/dev/null || true)"
  physical_gateway="$(read_state m2.physical_gateway 2>/dev/null || true)"
  dns_service="$(read_state m2.dns_service 2>/dev/null || true)"
  snapshot_file="$(read_state m2.dns_before_file 2>/dev/null || true)"
  [[ -n "$utun" && -n "$exit_host" && -n "$dns_target" && \
    -n "$physical_interface" && -n "$physical_gateway" && \
    -n "$dns_service" && -n "$snapshot_file" ]] || return 1

  "$M2_IFCONFIG_BIN" "$utun" >/dev/null 2>&1 || tun_available=0

  m2_cleanup_owned_interface_route "$run_dir" m2.fake_route_owned fake \
    "$M2_FAKE_PROBE" 198.18.0.0/15 "$utun" "$physical_interface" \
    "$physical_gateway" "$tun_available" || failed=1
  m2_cleanup_owned_interface_route "$run_dir" m2.low_route_owned low \
    "$M2_PUBLIC_LOW_PROBE" 0.0.0.0/1 "$utun" "$physical_interface" \
    "$physical_gateway" "$tun_available" || failed=1
  m2_cleanup_owned_interface_route "$run_dir" m2.high_route_owned high \
    "$M2_PUBLIC_HIGH_PROBE" 128.0.0.0/1 "$utun" "$physical_interface" \
    "$physical_gateway" "$tun_available" || failed=1
  if [[ "$(read_state m2.dns_owned 2>/dev/null || true)" == "1" ]]; then
    if m2_dns_snapshot_matches_target "$dns_service" "$dns_target" && \
      m2_restore_dns_snapshot "$dns_service" "$snapshot_file" \
        >>"$run_dir/cleanup.log" 2>&1 && \
      "$M2_DSCACHEUTIL_BIN" -flushcache >>"$run_dir/cleanup.log" 2>&1 && \
      [[ "$(m2_dns_snapshot_for_service "$dns_service")" == \
        "$(m2_saved_dns_snapshot "$snapshot_file")" ]]; then
      write_state m2.dns_owned 0
    else
      failed=1
    fi
  fi
  if [[ "$(read_state m2.exit_route_owned 2>/dev/null || true)" == "1" ]]; then
    current_interface="$(m2_route_interface "$exit_host")"
    current_gateway="$(m2_route_gateway "$exit_host")"
    if [[ "$current_interface" == "$physical_interface" && \
      "$current_gateway" == "$physical_gateway" ]] && \
      "$M2_ROUTE_BIN" -n delete -host "$exit_host" "$physical_gateway" \
        >>"$run_dir/cleanup.log" 2>&1; then
      write_state m2.exit_route_owned 0
    else
      failed=1
    fi
  fi
  ((failed == 0)) || return 1
  write_state m2.full_tunnel inactive
  m2_full_tunnel_is_restored || return 1
  append_event_to "$run_dir" "m2 full tunnel cleanup complete"
}

activate_m2_full_tunnel() {
  local run_dir="$1"
  local utun target dns_target exit_host exit_route_text physical_interface
  local physical_gateway dns_service dns_snapshot snapshot_file
  local exit_route_owned=0
  [[ -z "$(read_state m2.full_tunnel 2>/dev/null || true)" || \
    "$(read_state m2.full_tunnel 2>/dev/null || true)" == "inactive" ]] || return 1
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  [[ -n "$utun" && -n "$target" && -n "$dns_target" && -n "$exit_host" ]] || \
    return 1
  [[ "$(m2_route_interface "$target")" == "$utun" && \
    "$(m2_route_interface "$dns_target")" == "$utun" && \
    "$(m2_route_interface "$M2_PUBLIC_LOW_PROBE")" != "$utun" && \
    "$(m2_route_interface "$M2_PUBLIC_HIGH_PROBE")" != "$utun" ]] || return 1
  m2_ipv6_route_is_safe || return 1
  exit_route_text="$(m2_route_text "$exit_host")" || return 1
  physical_interface="$(route_interface_from_text <<<"$exit_route_text")"
  physical_gateway="$(route_gateway_from_text <<<"$exit_route_text")"
  [[ -n "$physical_interface" && "$physical_interface" != utun* && \
    -n "$physical_gateway" ]] || return 1
  dns_service="$("$M2_NETWORKSETUP_BIN" -listnetworkserviceorder 2>/dev/null | \
    network_service_for_interface_from_text "$physical_interface")" || return 1
  dns_snapshot="$(m2_dns_snapshot_for_service "$dns_service")" || return 1
  snapshot_file="$run_dir/m2-dns.before"
  printf '%s\n' "$dns_snapshot" >"$snapshot_file" || return 1

  write_state m2.full_tunnel preparing
  write_state m2.physical_interface "$physical_interface"
  write_state m2.physical_gateway "$physical_gateway"
  write_state m2.dns_service "$dns_service"
  write_state m2.dns_before_file "$snapshot_file"
  write_state m2.exit_route_owned 0
  write_state m2.low_route_owned 0
  write_state m2.high_route_owned 0
  write_state m2.fake_route_owned 0
  write_state m2.dns_owned 0

  if "$M2_ROUTE_BIN" -n add -host "$exit_host" "$physical_gateway" \
    >>"$run_dir/route.log" 2>&1; then
    exit_route_owned=1
    write_state m2.exit_route_owned 1
  fi
  if ! "$M2_ROUTE_BIN" -n add -net 0.0.0.0/1 -interface "$utun" \
    >>"$run_dir/route.log" 2>&1; then
    deactivate_m2_full_tunnel "$run_dir" || true
    return 1
  fi
  write_state m2.low_route_owned 1
  if ! "$M2_ROUTE_BIN" -n add -net 128.0.0.0/1 -interface "$utun" \
    >>"$run_dir/route.log" 2>&1; then
    deactivate_m2_full_tunnel "$run_dir" || true
    return 1
  fi
  write_state m2.high_route_owned 1
  if ! "$M2_ROUTE_BIN" -n add -net 198.18.0.0/15 -interface "$utun" \
    >>"$run_dir/route.log" 2>&1; then
    deactivate_m2_full_tunnel "$run_dir" || true
    return 1
  fi
  write_state m2.fake_route_owned 1
  if ! "$M2_NETWORKSETUP_BIN" -setdnsservers "$dns_service" "$dns_target" \
    >>"$run_dir/route.log" 2>&1; then
    deactivate_m2_full_tunnel "$run_dir" || true
    return 1
  fi
  write_state m2.dns_owned 1
  if ! "$M2_DSCACHEUTIL_BIN" -flushcache >>"$run_dir/route.log" 2>&1; then
    deactivate_m2_full_tunnel "$run_dir" || true
    return 1
  fi
  write_state m2.full_tunnel active
  if ! m2_full_tunnel_is_active; then
    write_state m2.full_tunnel preparing
    deactivate_m2_full_tunnel "$run_dir" || true
    return 1
  fi
  append_event_to "$run_dir" \
    "m2 full tunnel active interface=$utun physical=$physical_interface dns_service=$dns_service exit_route_owned=$exit_route_owned"
}

route_interface() {
  route -n get "$1" 2>/dev/null | route_interface_from_text
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

sha256_file_or_unknown() {
  local file_path="$1"
  if [[ -f "$file_path" && ! -L "$file_path" ]]; then
    sha256_file "$file_path" 2>/dev/null || echo unknown
  else
    echo unknown
  fi
}

state_file() {
  printf '%s/%s' "$STATE_DIR" "$1"
}

read_state() {
  local name="$1"
  [[ -f "$(state_file "$name")" ]] || return 1
  sed -n '1p' "$(state_file "$name")"
}

write_state() {
  local name="$1"
  local value="$2"
  printf '%s\n' "$value" >"$(state_file "$name")" || \
    die "cannot write runner state: $name"
}

write_start_network_state() {
  local target="${1:-}"
  local dns_target="${2:-}"
  local exit_host="${3:-}"
  local server_port="${4:-}"
  local iperf_port="${5:-}"
  validate_ipv4 "$target" || die "cannot persist invalid TARGET state"
  [[ -z "$dns_target" ]] || validate_ipv4 "$dns_target" || \
    die "cannot persist invalid DNS_TARGET state"
  validate_ipv4 "$exit_host" || die "cannot persist invalid Exit state"
  validate_positive_integer "$server_port" && ((10#$server_port <= 65535)) || \
    die "cannot persist invalid TUIC server-port state"
  validate_positive_integer "$iperf_port" && ((10#$iperf_port <= 65535)) || \
    die "cannot persist invalid iperf-port state"
  write_state target "$target"
  write_state dns_target "$dns_target"
  write_state exit_host "$exit_host"
  write_state server_port "$server_port"
  write_state iperf_port "$iperf_port"
}

pid_command_matches() {
  local expected_bin="$1"
  local command_text="$2"
  [[ "$command_text" == "$expected_bin client-tun" ]]
}

watchdog_command_matches() {
  local command_text="$1"
  [[ "$command_text" == *"$SCRIPT_PATH __watchdog" ]]
}

workload_command_matches() {
  local command_text="$1"
  [[ "$command_text" == *"knife15-macos-soak.sh m0" || \
    "$command_text" == *"knife15-macos-soak.sh m1" || \
    "$command_text" == *"knife15-macos-soak.sh m1-diagnostic" || \
    "$command_text" == *"knife15-macos-soak.sh m2-qualification" || \
    "$command_text" == *"knife15-macos-soak.sh m2" ]]
}

tcp_pool_latest_active_leases() {
  local log_file="$1"
  [[ -f "$log_file" ]] || return 1
  awk '
    /tuic-tcp-pool-activity active_leases=/ {
      value = $0
      sub(/^.*tuic-tcp-pool-activity active_leases=/, "", value)
      latest = value
      found = 1
    }
    END {
      if (!found) exit 1
      print latest
    }
  ' "$log_file"
}

tcp_pool_activity_is_idle() {
  local active_leases
  active_leases="$(tcp_pool_latest_active_leases "$1")" || return 1
  [[ "$active_leases" =~ ^[0-9]+$ ]] || return 1
  ((10#$active_leases == 0))
}

wait_for_tcp_pool_idle() {
  local log_file="$1"
  local timeout_secs="$2"
  local deadline
  validate_positive_integer "$timeout_secs" || return 1
  deadline=$((SECONDS + 10#$timeout_secs))
  while ((SECONDS < deadline)); do
    tcp_pool_activity_is_idle "$log_file" && return 0
    /bin/sleep 1
  done
  tcp_pool_activity_is_idle "$log_file"
}

watchdog_matches_run() {
  local watchdog_pid watchdog_expected watchdog_current
  watchdog_pid="$(read_state watchdog.pid 2>/dev/null || true)"
  watchdog_expected="$(read_state watchdog.command 2>/dev/null || true)"
  [[ "$watchdog_pid" =~ ^[0-9]+$ ]] || return 1
  [[ -n "$watchdog_expected" ]] || return 1
  kill -0 "$watchdog_pid" 2>/dev/null || return 1
  watchdog_current="$(ps -p "$watchdog_pid" -o command= 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  [[ "$watchdog_current" == "$watchdog_expected" ]] && watchdog_command_matches "$watchdog_current"
}

workload_matches_run() {
  local workload_pid workload_expected workload_current
  workload_pid="$(read_state workload.pid 2>/dev/null || true)"
  workload_expected="$(read_state workload.command 2>/dev/null || true)"
  [[ "$workload_pid" =~ ^[0-9]+$ ]] || return 1
  [[ -n "$workload_expected" ]] || return 1
  kill -0 "$workload_pid" 2>/dev/null || return 1
  workload_current="$(ps -p "$workload_pid" -o command= 2>/dev/null | \
    sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  [[ "$workload_current" == "$workload_expected" ]] && \
    workload_command_matches "$workload_current"
}

pid_matches_run() {
  local pid
  local expected_bin
  local command_text
  pid="$(read_state vpn.pid 2>/dev/null || true)"
  expected_bin="$(read_state bin 2>/dev/null || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ -n "$expected_bin" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  command_text="$(ps -p "$pid" -o command= 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  pid_command_matches "$expected_bin" "$command_text"
}

active_pid() {
  pid_matches_run
}

run_dir_from_state() {
  local run_dir
  run_dir="$(read_state run_dir 2>/dev/null)" || return 1
  validate_run_dir_path "$run_dir" || return 1
  [[ -d "$run_dir" && ! -L "$run_dir" ]] || return 1
  printf '%s\n' "$run_dir"
}

append_event_to() {
  local run_dir="$1"
  local label="$2"
  printf '%s\t%s\n' "$(timestamp)" "$(safe_event_label "$label")" >>"$run_dir/events.tsv"
}

append_event() {
  local run_dir
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  bundle_is_finalized "$run_dir" && \
    die "evidence is finalized; refuse to append an event"
  append_event_to "$run_dir" "$1"
}

release_binary_is_fresh() {
  local binary="${1:-$BIN}"
  local input
  [[ -x "$binary" && ! -L "$binary" ]] || return 1
  git -C "$REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 1
  while IFS= read -r -d '' input; do
    case "$input" in
      Cargo.toml|Cargo.lock|rust-toolchain|rust-toolchain.toml|build.rs|\
      .cargo/*.toml|src/*.rs|third_party/*/Cargo.toml|\
      third_party/*/build.rs|third_party/*/src/*.rs)
        [[ ! "$REPO/$input" -nt "$binary" ]] || return 1
        ;;
    esac
  done < <(git -C "$REPO" ls-files -z)
}

common_preflight() {
  local server target_if exit_if

  [[ "$(uname -s)" == "Darwin" ]] || die "Knife15 macOS runner requires Darwin"
  for command_name in bash route ifconfig netstat ping ps lsof shasum tar awk sed grep sort comm; do
    require_command "$command_name"
  done
  [[ -x "$BIN" ]] || die "release binary not found: $BIN (run cargo build --release first)"
  release_binary_is_fresh "$BIN" || \
    die "release binary is older than tracked Rust/Cargo inputs (run cargo build --release)"
  m2_source_is_accepted || \
    die "Knife15 runner requires reviewed source at cce3bf8 or a descendant"
  m2_worktree_is_clean || \
    die "Knife15 runner requires a clean worktree, including no untracked files, for exact-source evidence"

  : "${MINI_VPN_TUIC_SERVER:?export MINI_VPN_TUIC_SERVER locally}"
  : "${MINI_VPN_TUIC_UUID:?export MINI_VPN_TUIC_UUID locally}"
  : "${MINI_VPN_TUIC_PASSWORD:?export MINI_VPN_TUIC_PASSWORD locally}"
  : "${MINI_VPN_TUIC_SNI:?export MINI_VPN_TUIC_SNI locally}"
  : "${MINI_VPN_TUIC_CA_PATH:?export MINI_VPN_TUIC_CA_PATH locally}"
  [[ -r "$MINI_VPN_TUIC_CA_PATH" ]] || die "TUIC CA path is not readable"
  validate_uuid "$MINI_VPN_TUIC_UUID" || die "MINI_VPN_TUIC_UUID must be a canonical UUID"
  if grep -Eq -- 'BEGIN [A-Z ]*PRIVATE KEY' "$MINI_VPN_TUIC_CA_PATH"; then
    die "TUIC CA path contains private-key material; use a certificate-only file"
  fi

  server="$MINI_VPN_TUIC_SERVER"
  parse_server "$server" || die "MINI_VPN_TUIC_SERVER must be IPv4:port for the first target-only gate"
  validate_ipv4 "$TARGET" || die "TARGET must be an IPv4 address"
  [[ "$TARGET" != "$SERVER_HOST" ]] || die "TARGET and TUIC Exit must be different hosts"
  if [[ -n "$DNS_TARGET" ]]; then
    validate_ipv4 "$DNS_TARGET" || die "DNS_TARGET must be an IPv4 address"
    [[ "$DNS_TARGET" != "$SERVER_HOST" ]] || die "DNS_TARGET must not equal the TUIC Exit"
    [[ "$DNS_TARGET" != "$TARGET" ]] || die "DNS_TARGET must not equal TARGET"
    validate_dns_name "$DNS_NAME" || die "DNS_NAME must be a simple DNS name"
  fi

  for value in "$DURATION" "$PARALLEL" "$METRICS_SECS" "$SAMPLE_SECS" "$STARTUP_TIMEOUT" \
    "$MAX_LOG_BYTES" "$LOG_KEEP_BYTES" "$MIN_FREE_KB"; do
    validate_positive_integer "$value" || die "numeric runner values must be positive integers"
  done
  validate_positive_integer "$IPERF_PORT" || die "IPERF_PORT must be a positive integer"
  ((10#$IPERF_PORT <= 65535)) || die "IPERF_PORT must be at most 65535"
  ((10#$LOG_KEEP_BYTES < 10#$MAX_LOG_BYTES)) || die "LOG_KEEP_BYTES must be below MAX_LOG_BYTES"

  exit_if="$(route_interface "$SERVER_HOST")"
  target_if="$(route_interface "$TARGET")"
  [[ -n "$exit_if" ]] || die "no route to TUIC Exit $SERVER_HOST"
  [[ -n "$target_if" ]] || die "no route to TARGET $TARGET"
  [[ "$exit_if" != utun* ]] || die "TUIC Exit already routes through $exit_if; refuse recursive VPN setup"
  [[ "$target_if" != utun* ]] || die "TARGET already routes through $target_if; clean the prior VPN first"

  if [[ -d "$STATE_DIR" ]] && \
    { active_pid || watchdog_matches_run || workload_matches_run; }; then
    die "an active Knife15 process/watchdog/workload already exists; use status or stop"
  fi
}

print_preflight() {
  local source_commit binary_sha runner_sha
  source_commit="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
  binary_sha="$(sha256_file "$BIN")"
  runner_sha="$(sha256_file "$SCRIPT_PATH")"
  echo "PASS: macOS target-only preflight"
  echo "source_commit=$source_commit"
  echo "binary_sha256=$binary_sha"
  echo "runner_sha256=$runner_sha"
  echo "target=$TARGET route_if=$(route_interface "$TARGET")"
  echo "exit=$SERVER_HOST:$SERVER_PORT route_if=$(route_interface "$SERVER_HOST")"
  echo "dns_target=${DNS_TARGET:-disabled}"
  echo "No route or DNS state was changed."
}

verify_startup_profile() {
  local log_file="$1"
  grep -Fq "TUN runtime started with pool_size=2, tun_mtu=1200, tun_tx_queue_len_estimate=500" "$log_file" &&
    grep -Fq "TUN ingress service: enabled capacity=500" "$log_file" &&
    grep -Fq "TCP socket buffers: rx=1048576B tx=1048576B receive_window_limit=368640" "$log_file" &&
    grep -Fq "H10d16 byte-owned egress: enabled per_flow_cap=524288 global_cap=67108864 quantum=131072" "$log_file" &&
    grep -Fq "TUIC 拥塞控制器=Cubic | UDP relay mode=Native | QUIC MTU policy=default | QUIC GSO policy=enabled | QUIC UDP send service=quinn | QUIC pacing policy=endpoint-window-v1" "$log_file"
}

conservation_check_file() {
  local log_file="$1"
  local triples available live outstanding total samples=0
  while IFS= read -r triples; do
    [[ -n "$triples" ]] || continue
    read -r available live outstanding <<<"$triples"
    total=$((10#$available + 10#$live + 10#$outstanding))
    ((total <= 61440)) || return 1
    samples=$((samples + 1))
  done < <(
    sed -nE 's/.*conservation\(available=([0-9]+)B,live=([0-9]+)B,outstanding=([0-9]+)B.*/\1 \2 \3/p' "$log_file"
  )
  ((samples > 0))
}

interface_csv_from_text() {
  local now="$1"
  local interface="$2"
  local fields
  fields="$(interface_control_fields_from_text "$interface")" || return 1
  printf '%s,%s,%s\n' "$now" "$interface" "$fields"
}

interface_control_fields_from_text() {
  local interface="$1"
  awk -v interface="$interface" '
    $1 == interface && $3 ~ /^<Link#/ {
      first_counter = 4
      if ($4 !~ /^[0-9]+$/) first_counter = 5
      valid = ($2 ~ /^[0-9]+$/)
      for (field = first_counter; field <= first_counter + 6; field++) {
        if ($(field) !~ /^[0-9]+$/) valid = 0
      }
      if (!valid) next
      printf "%s,%s,%s,%s,%s,%s,%s,%s\n", $2,
        $(first_counter), $(first_counter + 1), $(first_counter + 2),
        $(first_counter + 3), $(first_counter + 4), $(first_counter + 5),
        $(first_counter + 6)
      found = 1
      exit
    }
    END { if (!found) exit 1 }
  '
}

ping_control_fields_from_text() {
  awk '
    BEGIN {
      transmitted = "unknown"
      received = "unknown"
      loss = "unknown"
      rtt_min = "unknown"
      rtt_avg = "unknown"
      rtt_max = "unknown"
    }
    / packets transmitted, [0-9]+ packets received, [0-9.]+% packet loss/ {
      if ($1 ~ /^[0-9]+$/) transmitted = $1
      if ($4 ~ /^[0-9]+$/) received = $4
      value = $7
      sub(/%$/, "", value)
      if (value ~ /^[0-9]+([.][0-9]+)?$/) loss = sprintf("%.6f", value + 0)
    }
    /min\/avg\/max\/(stddev|mdev) =/ {
      value = $0
      sub(/^.* = /, "", value)
      sub(/ ms.*$/, "", value)
      if (split(value, rtt, "/") >= 3 &&
          rtt[1] ~ /^[0-9]+([.][0-9]+)?$/ &&
          rtt[2] ~ /^[0-9]+([.][0-9]+)?$/ &&
          rtt[3] ~ /^[0-9]+([.][0-9]+)?$/) {
        rtt_min = rtt[1]
        rtt_avg = rtt[2]
        rtt_max = rtt[3]
      }
    }
    END {
      printf "%s,%s,%s,%s,%s,%s\n", transmitted, received, loss,
        rtt_min, rtt_avg, rtt_max
    }
  '
}

network_control_envelope() {
  local network_file="$1"
  awk -F, '
    function numeric(value) {
      return value ~ /^[0-9]+([.][0-9]+)?$/
    }
    NR > 1 {
      rows++
      schema_valid = (NF == 27)
      physical_valid = (schema_valid &&
        $18 ~ /^[0-9]+$/ && $19 ~ /^[0-9]+$/ &&
        $20 ~ /^[0-9]+$/ && $21 ~ /^[0-9]+$/ &&
        $22 ~ /^[0-9]+$/ && $23 ~ /^[0-9]+$/ &&
        $24 ~ /^[0-9]+$/ && $25 ~ /^[0-9]+$/)
      if (schema_valid && $6 ~ /^[0-9]+$/ && $7 ~ /^[0-9]+$/ && numeric($8)) {
        exit_samples++
        if (!have_exit_loss || $8 + 0 > exit_loss_max) exit_loss_max = $8 + 0
        have_exit_loss = 1
      }
      if (schema_valid && numeric($10)) {
        exit_rtt_samples++
        if (!have_exit_rtt || $10 + 0 > exit_rtt_max) exit_rtt_max = $10 + 0
        have_exit_rtt = 1
      }
      if (schema_valid && numeric($14)) {
        gateway_samples++
        if (!have_gateway_loss || $14 + 0 > gateway_loss_max) gateway_loss_max = $14 + 0
        have_gateway_loss = 1
      }
      if (physical_valid) {
        if (!physical_samples) {
          # Interface counters are lifetime-cumulative. The first valid row is
          # the run baseline; only movement or a reset is a run-time signal.
          ibytes_first = $21 + 0
          obytes_first = $24 + 0
        } else if ($20 + 0 != physical_ierrs_last ||
            $23 + 0 != physical_oerrs_last) {
          physical_error_samples++
        }
        ibytes_last = $21 + 0
        obytes_last = $24 + 0
        physical_ierrs_last = $20 + 0
        physical_oerrs_last = $23 + 0
        physical_samples++
      }
      if (physical_valid && $26 ~ /^[0-9]+$/ &&
          (!have_rx_bps || $26 + 0 > rx_bps_max)) {
        rx_bps_max = $26 + 0
        have_rx_bps = 1
      }
      if (physical_valid && $27 ~ /^[0-9]+$/ &&
          (!have_tx_bps || $27 + 0 > tx_bps_max)) {
        tx_bps_max = $27 + 0
        have_tx_bps = 1
      }
    }
    END {
      printf "%d %d %d %d %d %d %d ", rows + 0, exit_samples + 0,
        rows - exit_samples, exit_rtt_samples + 0, gateway_samples + 0,
        rows - gateway_samples, physical_samples + 0
      printf "%s %s %s %d ",
        have_exit_loss ? sprintf("%.6f", exit_loss_max) : "unknown",
        have_exit_rtt ? sprintf("%.3f", exit_rtt_max) : "unknown",
        have_gateway_loss ? sprintf("%.6f", gateway_loss_max) : "unknown",
        physical_error_samples + 0
      if (physical_samples) {
        printf "%.0f %.0f %.0f %.0f %.0f %.0f ", ibytes_first, ibytes_last,
          ibytes_last - ibytes_first, obytes_first, obytes_last,
          obytes_last - obytes_first
      } else {
        printf "unknown unknown unknown unknown unknown unknown "
      }
      printf "%s %s\n",
        have_rx_bps ? sprintf("%.0f", rx_bps_max) : "unknown",
        have_tx_bps ? sprintf("%.0f", tx_bps_max) : "unknown"
    }
  ' "$network_file" 2>/dev/null
}

network_control_is_sufficient() {
  local run_dir="$1"
  local attempts="${2:-1}"
  local attempt
  local process_samples network_samples exit_samples exit_missing exit_rtt_samples
  local gateway_samples gateway_missing physical_samples rest
  [[ "$attempts" =~ ^[0-9]+$ ]] && ((10#$attempts > 0)) || return 1
  for ((attempt = 0; attempt < 10#$attempts; attempt++)); do
    process_samples="$(awk 'END {print (NR > 0 ? NR - 1 : 0)}' \
      "$run_dir/process.csv" 2>/dev/null)"
    read -r network_samples exit_samples exit_missing exit_rtt_samples \
      gateway_samples gateway_missing physical_samples rest \
      <<<"$(network_control_envelope "$run_dir/network.csv")"
    if [[ "$process_samples" =~ ^[0-9]+$ && "$network_samples" =~ ^[0-9]+$ && \
      "$exit_samples" =~ ^[0-9]+$ && "$physical_samples" =~ ^[0-9]+$ ]] && \
      ((10#$network_samples >= 2 && \
        10#$network_samples == 10#$process_samples && \
        10#$exit_samples == 10#$network_samples && \
        10#$physical_samples == 10#$network_samples)); then
      return 0
    fi
    ((attempt + 1 < 10#$attempts)) && sleep 1
  done
  return 1
}

network_control_is_recent() {
  local valid_epoch now sample_secs max_age
  valid_epoch="$(read_state network.valid.epoch 2>/dev/null || true)"
  now="$(date +%s)"
  sample_secs="$(read_state sample_secs 2>/dev/null || echo "$SAMPLE_SECS")"
  [[ "$valid_epoch" =~ ^[0-9]+$ && "$now" =~ ^[0-9]+$ && \
    "$sample_secs" =~ ^[0-9]+$ ]] || return 1
  max_age=$((2 * 10#$sample_secs + 10))
  ((10#$now >= 10#$valid_epoch && 10#$now - 10#$valid_epoch <= max_age))
}

process_resource_envelope() {
  local process_file="$1"
  awk -F, '
    NR > 1 && $3 ~ /^[0-9]+$/ && $7 ~ /^[0-9]+$/ && $8 ~ /^[0-9]+$/ {
      if (samples == 0) {
        rss_first = $3 + 0
        fd_first = $7 + 0
        threads_first = $8 + 0
        rss_max = rss_first
        fd_max = fd_first
        threads_max = threads_first
      }
      rss_last = $3 + 0
      fd_last = $7 + 0
      threads_last = $8 + 0
      if (rss_last > rss_max) rss_max = rss_last
      if (fd_last > fd_max) fd_max = fd_last
      if (threads_last > threads_max) threads_max = threads_last
      samples++
    }
    END {
      if (samples == 0) {
        print "0 unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown"
      } else {
        printf "%d %d %d %d %d %d %d %d %d %d %d %d %d\n", samples,
          rss_first, rss_last, rss_max, rss_last - rss_first,
          fd_first, fd_last, fd_max, fd_last - fd_first,
          threads_first, threads_last, threads_max, threads_last - threads_first
      }
    }
  ' "$process_file" 2>/dev/null
}

interface_resource_envelope() {
  local interface_file="$1"
  awk -F, '
    NR > 1 && $4 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ &&
      $7 ~ /^[0-9]+$/ && $9 ~ /^[0-9]+$/ {
      if (samples == 0) {
        ipkts_first = $4 + 0
        ibytes_first = $6 + 0
        opkts_first = $7 + 0
        obytes_first = $9 + 0
      }
      ipkts_last = $4 + 0
      ibytes_last = $6 + 0
      opkts_last = $7 + 0
      obytes_last = $9 + 0
      samples++
    }
    END {
      if (samples == 0) {
        print "0 unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown"
      } else {
        printf "%d %d %d %d %d %d %d %d %d %d %d %d %d\n", samples,
          ipkts_first, ipkts_last, ipkts_last - ipkts_first,
          ibytes_first, ibytes_last, ibytes_last - ibytes_first,
          opkts_first, opkts_last, opkts_last - opkts_first,
          obytes_first, obytes_last, obytes_last - obytes_first
      }
    }
  ' "$interface_file" 2>/dev/null
}

endpoint_resource_envelope() {
  local log_file="$1"
  sed -nE \
    's/.*conservation\(available=([0-9]+)B,live=([0-9]+)B,outstanding=([0-9]+)B.*/\1 \2 \3/p' \
    "$log_file" 2>/dev/null | awk '
      {
        available = $1 + 0
        live = $2 + 0
        outstanding = $3 + 0
        total = available + live + outstanding
        if (samples == 0 || total > max_total) max_total = total
        if (samples == 0 || live > max_live) max_live = live
        if (samples == 0 || outstanding > max_outstanding) max_outstanding = outstanding
        last_available = available
        last_live = live
        last_outstanding = outstanding
        samples++
      }
      END {
        if (samples == 0) {
          print "0 unknown unknown unknown unknown unknown unknown"
        } else {
          printf "%d %d %d %d %d %d %d\n", samples, max_total,
            last_available, last_live, last_outstanding, max_live, max_outstanding
        }
      }
    '
}

m1_checkpoint_envelope() {
  local checkpoint_file="$1"
  awk -F, '
    BEGIN {
      expected[1] = "idle-1"
      expected[2] = "idle-2"
      expected[3] = "idle-3"
      expected[4] = "final"
    }
    NR > 1 {
      numeric = 1
      for (field = 3; field <= 8; field++) {
        if ($field !~ /^[0-9]+$/) numeric = 0
      }
      if (!numeric) {
        invalid++
        next
      }
      count++
      if ($2 != expected[count]) labels_invalid++
      rss = $3 + 0
      fd = $4 + 0
      threads = $5 + 0
      available = $6 + 0
      live = $7 + 0
      outstanding = $8 + 0
      if (count == 1) {
        first_rss = rss
        first_fd = fd
        first_threads = threads
        max_rss = rss
        max_fd = fd
        max_threads = threads
      }
      final_rss = rss
      final_fd = fd
      final_threads = threads
      if (rss > max_rss) max_rss = rss
      if (fd > max_fd) max_fd = fd
      if (threads > max_threads) max_threads = threads
      if (live != 0 || outstanding != 0 ||
          available + live + outstanding > 61440) ownership_failures++
    }
    END {
      labels_valid = (count == 4 && labels_invalid == 0 && invalid == 0) ? 1 : 0
      if (count == 0) {
        print "0 0 unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown 0"
      } else {
        printf "%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d\n",
          count, labels_valid, first_rss, final_rss, max_rss,
          final_rss - first_rss, first_fd, final_fd, max_fd,
          final_fd - first_fd, first_threads, final_threads, max_threads,
          final_threads - first_threads, ownership_failures + 0
      }
    }
  ' "$checkpoint_file" 2>/dev/null
}

m1_checkpoint_slo() {
  local checkpoint_file="$1"
  local count labels_valid first_rss final_rss max_rss rss_delta
  local first_fd final_fd max_fd fd_delta first_threads final_threads
  local max_threads threads_delta ownership_failures
  read -r count labels_valid first_rss final_rss max_rss rss_delta \
    first_fd final_fd max_fd fd_delta first_threads final_threads \
    max_threads threads_delta ownership_failures \
    <<<"$(m1_checkpoint_envelope "$checkpoint_file")"
  [[ "$count" == "4" && "$labels_valid" == "1" && \
    "$ownership_failures" == "0" ]] || return 1
  for value in "$first_rss" "$final_rss" "$max_rss" "$first_fd" \
    "$final_fd" "$max_fd" "$first_threads" "$final_threads" "$max_threads"; do
    [[ "$value" =~ ^[0-9]+$ ]] || return 1
  done
  ((max_rss <= 131072 && rss_delta <= 32768 && \
    max_fd - first_fd <= 2 && final_fd - first_fd <= 1 && \
    max_threads - first_threads <= 2 && final_threads - first_threads <= 1))
}

capture_m1_checkpoint() {
  local run_dir="$1"
  local label="$2"
  local previous_endpoint_samples="${3:-0}"
  local process_values endpoint_values
  local rss fd threads samples max_total available live outstanding rest
  process_values="$(awk -F, '
    NR > 1 && $3 ~ /^[0-9]+$/ && $7 ~ /^[0-9]+$/ && $8 ~ /^[0-9]+$/ {
      value = $3 " " $7 " " $8
    }
    END { print value }
  ' "$run_dir/process.csv" 2>/dev/null)"
  read -r rss fd threads <<<"$process_values"
  [[ "$rss" =~ ^[0-9]+$ && "$fd" =~ ^[0-9]+$ && "$threads" =~ ^[0-9]+$ ]] || \
    return 1
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r samples max_total available live outstanding rest <<<"$endpoint_values"
  [[ "$previous_endpoint_samples" =~ ^[0-9]+$ && \
    "$samples" =~ ^[0-9]+$ && \
    10#$samples -gt 10#$previous_endpoint_samples && \
    "$available" =~ ^[0-9]+$ && "$live" =~ ^[0-9]+$ && \
    "$outstanding" =~ ^[0-9]+$ ]] || return 1
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$(timestamp)" "$label" \
    "$rss" "$fd" "$threads" "$available" "$live" "$outstanding" \
    >>"$run_dir/m1-checkpoints.csv" || return 1
  ((10#$live == 0 && 10#$outstanding == 0 && \
    10#$available + 10#$live + 10#$outstanding <= 61440))
}

m2_data_plane_envelope() {
  local log_file="$1"
  sed -nE \
    's/.*DNS forge=([0-9]+)\/drop=([0-9]+) \| TCP relay 活跃=([0-9]+)\/累计=([0-9]+) \| fake-IP 活跃=([0-9]+)\/在册=([0-9]+).*/\1 \2 \3 \4 \5 \6/p' \
    "$log_file" 2>/dev/null | awk '
      {
        forged = $1
        dropped = $2
        active_relays = $3
        total_relays = $4
        fake_active = $5
        fake_total = $6
        samples++
      }
      END {
        if (samples == 0) {
          print "0 unknown unknown unknown unknown unknown unknown"
        } else {
          printf "%d %d %d %d %d %d %d\n", samples, forged, dropped,
            active_relays, total_relays, fake_active, fake_total
        }
      }
    '
}

m2_controlled_tcp_replay_envelope() {
  local log_file="$1"
  local controlled_one="$2"
  local controlled_two="$3"
  local controlled_three="$4"
  awk -v controlled_one="$controlled_one" \
    -v controlled_two="$controlled_two" \
    -v controlled_three="$controlled_three" '
    /tuic-open-tcp target=/ {
      target = $0
      sub(/^.*tuic-open-tcp target=/, "", target)
      sub(/ .*/, "", target)
      handle = $0
      sub(/^.* handle=SocketHandle\(/, "", handle)
      sub(/\).*/, "", handle)
      epoch = $0
      sub(/^.* epoch=/, "", epoch)
      sub(/ .*/, "", epoch)
      if (target == "" || handle !~ /^[0-9]+$/ || epoch !~ /^[0-9]+$/) {
        invalid++
        next
      }
      key = handle SUBSEP epoch
      if (key in pending_target ||
          ((handle in active_target) && active_epoch[handle] == epoch)) {
        invalid++
        next
      }
      pending_target[key] = target
      next
    }
    /tcp-relay-engine handle=SocketHandle\(/ {
      handle = $0
      sub(/^.*tcp-relay-engine handle=SocketHandle\(/, "", handle)
      sub(/\).*/, "", handle)
      epoch = $0
      sub(/^.* epoch=/, "", epoch)
      sub(/ .*/, "", epoch)
      key = handle SUBSEP epoch
      if (handle !~ /^[0-9]+$/ || epoch !~ /^[0-9]+$/ ||
          !(key in pending_target) || handle in active_target) {
        invalid++
        next
      }
      active_target[handle] = pending_target[key]
      active_epoch[handle] = epoch
      if (active_target[handle] == controlled_one ||
          active_target[handle] == controlled_two ||
          active_target[handle] == controlled_three) controlled[handle] = 1
      delete pending_target[key]
      next
    }
    /迟到 open 结果\(epoch [0-9]+≠[0-9]+\) 丢弃/ {
      handle = $0
      sub(/^.*handle SocketHandle\(/, "", handle)
      sub(/\).*/, "", handle)
      epoch = $0
      sub(/^.*迟到 open 结果\(epoch /, "", epoch)
      sub(/≠.*/, "", epoch)
      key = handle SUBSEP epoch
      if (handle !~ /^[0-9]+$/ || epoch !~ /^[0-9]+$/ ||
          !(key in pending_target)) {
        invalid++
        next
      }
      delete pending_target[key]
      next
    }
    /tcp-lifecycle-transition handle=SocketHandle\(/ && /ctx_state=Closing( |$)/ {
      handle = $0
      sub(/^.*tcp-lifecycle-transition handle=SocketHandle\(/, "", handle)
      sub(/\).*/, "", handle)
      if (handle !~ /^[0-9]+$/) {
        invalid++
        next
      }
      if (handle in active_target) {
        delete active_target[handle]
        delete active_epoch[handle]
        delete controlled[handle]
        retired[handle] = 1
      }
      next
    }
    /tcp-handle-close handle=SocketHandle\(/ {
      handle = $0
      sub(/^.*tcp-handle-close handle=SocketHandle\(/, "", handle)
      sub(/\).*/, "", handle)
      if (handle !~ /^[0-9]+$/) {
        invalid++
        next
      }
      if (handle in active_target) {
        delete active_target[handle]
        delete active_epoch[handle]
        delete controlled[handle]
      } else if (handle in retired) {
        delete retired[handle]
      } else {
        pending = 0
        for (key in pending_target) {
          split(key, parts, SUBSEP)
          if (parts[1] == handle) {
            delete pending_target[key]
            pending = 1
          }
        }
        if (!pending &&
            $0 !~ /direction=remote_open reason=handshake_failed state=HandshakePending( |$)/ &&
            $0 !~ /direction=policy reason=encrypted_dns_block state=Listening( |$)/) {
          invalid++
        }
      }
      next
    }
    /📊 数据面:/ {
      samples++
      replayed = 0
      controlled_active = 0
      for (handle in active_target) replayed++
      for (handle in controlled) controlled_active++
      for (key in pending_target) {
        if (pending_target[key] == controlled_one ||
            pending_target[key] == controlled_two ||
            pending_target[key] == controlled_three) controlled_active++
      }
      latest_replayed = replayed
      latest_controlled = controlled_active
    }
    END {
      printf "%d %d %d %d\n", samples + 0, latest_replayed + 0,
        latest_controlled + 0, invalid + 0
    }
  ' "$log_file" 2>/dev/null
}

m2_controlled_tcp_replay_for_profile() {
  local log_file="$1"
  local profile_file="$2"
  local target iperf_port
  [[ -f "$profile_file" && ! -L "$profile_file" ]] || return 1
  target="$(m0_profile_value "$profile_file" target 2>/dev/null || true)"
  iperf_port="$(m0_profile_value "$profile_file" iperf_port 2>/dev/null || true)"
  validate_ipv4 "$target" && validate_positive_integer "$iperf_port" && \
    ((10#$iperf_port <= 65535)) || return 1
  m2_controlled_tcp_replay_envelope "$log_file" "$target:$iperf_port" \
    "$M2_EGRESS_TARGET" "$M2_BROWSER_TARGET"
}

m2_full_tunnel_quiescence_snapshot() {
  local log_file="$1"
  local profile_file="$2"
  local active_leases data_plane_values endpoint_values data_samples dns_forged
  local dns_dropped active_relays total_relays fake_active fake_total
  local endpoint_samples max_total available live outstanding rest
  local replay_values replay_samples replayed_active controlled_active
  local replay_invalid
  active_leases="$(tcp_pool_latest_active_leases "$log_file" 2>/dev/null || true)"
  data_plane_values="$(m2_data_plane_envelope "$log_file")"
  read -r data_samples dns_forged dns_dropped active_relays total_relays \
    fake_active fake_total <<<"$data_plane_values"
  endpoint_values="$(endpoint_resource_envelope "$log_file")"
  read -r endpoint_samples max_total available live outstanding rest \
    <<<"$endpoint_values"
  replay_values="$(m2_controlled_tcp_replay_for_profile \
    "$log_file" "$profile_file" 2>/dev/null || true)"
  read -r replay_samples replayed_active controlled_active replay_invalid \
    <<<"$replay_values"
  printf '%s %s %s %s %s %s %s %s %s %s %s %s %s %s\n' \
    "${data_samples:-unknown}" "${endpoint_samples:-unknown}" \
    "${active_leases:-unknown}" "${active_relays:-unknown}" \
    "${fake_active:-unknown}" "${fake_total:-unknown}" \
    "${dns_dropped:-unknown}" "${available:-unknown}" \
    "${live:-unknown}" "${outstanding:-unknown}" \
    "${replay_samples:-unknown}" "${replayed_active:-unknown}" \
    "${controlled_active:-unknown}" "${replay_invalid:-unknown}"
}

m2_full_tunnel_quiescence_is_clean() {
  local log_file="$1"
  local profile_file="$2"
  local previous_data_plane_samples="$3"
  local previous_endpoint_samples="$4"
  local snapshot data_samples endpoint_samples active_leases active_relays
  local fake_active fake_total dns_dropped available live outstanding value
  local replay_samples replayed_active controlled_active replay_invalid
  snapshot="$(m2_full_tunnel_quiescence_snapshot "$log_file" "$profile_file")"
  read -r data_samples endpoint_samples active_leases active_relays fake_active \
    fake_total dns_dropped available live outstanding replay_samples \
    replayed_active controlled_active replay_invalid \
    <<<"$snapshot"
  [[ "$previous_data_plane_samples" =~ ^[0-9]+$ && \
    "$previous_endpoint_samples" =~ ^[0-9]+$ && \
    "$active_leases" =~ ^[0-9]+$ && "$data_samples" =~ ^[0-9]+$ && \
    "$endpoint_samples" =~ ^[0-9]+$ && \
    10#$data_samples -gt 10#$previous_data_plane_samples && \
    10#$endpoint_samples -gt 10#$previous_endpoint_samples ]] || return 1
  for value in "$dns_dropped" "$active_relays" "$fake_active" "$fake_total" \
    "$available" "$live" "$outstanding" "$replay_samples" \
    "$replayed_active" "$controlled_active" "$replay_invalid"; do
    [[ "$value" =~ ^[0-9]+$ ]] || return 1
  done
  ((10#$replay_samples == 10#$data_samples && \
    10#$controlled_active == 0 && 10#$replay_invalid == 0 && \
    10#$dns_dropped == 0 && \
    10#$live == 0 && 10#$outstanding == 0 && \
    10#$available + 10#$live + 10#$outstanding <= 61440))
}

wait_for_m2_full_tunnel_quiescence() {
  local log_file="$1"
  local profile_file="$2"
  local previous_data_plane_samples="$3"
  local previous_endpoint_samples="$4"
  local timeout_secs="$5"
  local deadline
  validate_positive_integer "$timeout_secs" || return 1
  deadline=$((SECONDS + 10#$timeout_secs))
  while ((SECONDS < deadline)); do
    m2_full_tunnel_quiescence_is_clean \
      "$log_file" "$profile_file" "$previous_data_plane_samples" \
      "$previous_endpoint_samples" && return 0
    /bin/sleep 1
  done
  m2_full_tunnel_quiescence_is_clean \
    "$log_file" "$profile_file" "$previous_data_plane_samples" \
    "$previous_endpoint_samples"
}

record_m2_full_tunnel_quiescence() {
  local run_dir="$1"
  local profile_file="$2"
  local previous_data_plane_samples="$3"
  local previous_endpoint_samples="$4"
  local timeout_secs="$5"
  local result snapshot data_samples endpoint_samples active_leases active_relays
  local fake_active fake_total dns_dropped available live outstanding
  local replay_samples replayed_active controlled_active replay_invalid
  result=FAIL
  wait_for_m2_full_tunnel_quiescence "$run_dir/mini_vpn.log" \
    "$profile_file" "$previous_data_plane_samples" \
    "$previous_endpoint_samples" "$timeout_secs" && result=PASS
  snapshot="$(m2_full_tunnel_quiescence_snapshot \
    "$run_dir/mini_vpn.log" "$profile_file")"
  read -r data_samples endpoint_samples active_leases active_relays fake_active \
    fake_total dns_dropped available live outstanding replay_samples \
    replayed_active controlled_active replay_invalid \
    <<<"$snapshot"
  printf '%s\n' \
    'schema=knife15-macos-m2-controlled-drain-v4' \
    "recorded_utc=$(timestamp)" \
    "timeout_secs=$timeout_secs" \
    "previous_data_plane_samples=$previous_data_plane_samples" \
    "data_plane_samples=$data_samples" \
    "previous_endpoint_samples=$previous_endpoint_samples" \
    "endpoint_samples=$endpoint_samples" \
    "endpoint_available=$available endpoint_live=$live endpoint_outstanding=$outstanding" \
    "active_leases=$active_leases active_relays=$active_relays fake_ip_active=$fake_active fake_ip_registered=$fake_total dns_dropped=$dns_dropped" \
    "replay_samples=$replay_samples replayed_relaying_handles=$replayed_active controlled_active_relays=$controlled_active replay_invalid=$replay_invalid" \
    "result=$result" \
    >"$run_dir/m2-full-tunnel-quiescence.txt" || return 2
  [[ "$result" == PASS ]]
}

m2_checkpoint_envelope() {
  local checkpoint_file="$1"
  awk -F, '
    BEGIN {
      expected[1] = "idle-1"
      expected[2] = "idle-2"
      expected[3] = "idle-3"
      expected[4] = "idle-4"
      expected[5] = "idle-5"
      expected[6] = "final"
    }
    NR > 1 {
      numeric = 1
      for (field = 3; field <= 18; field++) {
        if ($field !~ /^[0-9]+$/) numeric = 0
      }
      if (!numeric) {
        invalid++
        next
      }
      count++
      if ($2 != expected[count]) labels_invalid++
      rss = $3 + 0
      fd = $4 + 0
      threads = $5 + 0
      available = $6 + 0
      live = $7 + 0
      outstanding = $8 + 0
      active_relays = $9 + 0
      dns_dropped = $14 + 0
      active_leases = $15 + 0
      controlled_active = $17 + 0
      replay_invalid = $18 + 0
      if (count == 1) {
        first_rss = rss
        first_fd = fd
        first_threads = threads
        max_rss = rss
        max_fd = fd
        max_threads = threads
      }
      final_rss = rss
      final_fd = fd
      final_threads = threads
      if (rss > max_rss) max_rss = rss
      if (fd > max_fd) max_fd = fd
      if (threads > max_threads) max_threads = threads
      if (live != 0 || outstanding != 0 ||
          available + live + outstanding > 61440 ||
          controlled_active != 0 || replay_invalid != 0 ||
          dns_dropped != 0) ownership_failures++
    }
    END {
      labels_valid = (count == 6 && labels_invalid == 0 && invalid == 0) ? 1 : 0
      if (count == 0) {
        print "0 0 unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown unknown 0"
      } else {
        printf "%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d\n",
          count, labels_valid, first_rss, final_rss, max_rss,
          final_rss - first_rss, first_fd, final_fd, max_fd,
          final_fd - first_fd, first_threads, final_threads, max_threads,
          final_threads - first_threads, ownership_failures + 0
      }
    }
  ' "$checkpoint_file" 2>/dev/null
}

m2_checkpoint_slo() {
  local checkpoint_file="$1"
  local count labels_valid first_rss final_rss max_rss rss_delta
  local first_fd final_fd max_fd fd_delta first_threads final_threads
  local max_threads threads_delta ownership_failures value
  read -r count labels_valid first_rss final_rss max_rss rss_delta \
    first_fd final_fd max_fd fd_delta first_threads final_threads \
    max_threads threads_delta ownership_failures \
    <<<"$(m2_checkpoint_envelope "$checkpoint_file")"
  [[ "$count" == "6" && "$labels_valid" == "1" && \
    "$ownership_failures" == "0" ]] || return 1
  for value in "$first_rss" "$final_rss" "$max_rss" "$first_fd" \
    "$final_fd" "$max_fd" "$first_threads" "$final_threads" "$max_threads"; do
    [[ "$value" =~ ^[0-9]+$ ]] || return 1
  done
  ((max_rss <= 131072 && rss_delta <= 32768 && \
    max_fd - first_fd <= 2 && final_fd - first_fd <= 1 && \
    max_threads - first_threads <= 2 && final_threads - first_threads <= 1))
}

capture_m2_checkpoint() {
  local run_dir="$1"
  local label="$2"
  local previous_endpoint_samples="$3"
  local previous_data_plane_samples="$4"
  local process_values endpoint_values data_plane_values
  local rss fd threads endpoint_samples max_total available live outstanding rest
  local data_samples dns_forged dns_dropped active_relays total_relays
  local fake_active fake_total active_leases profile_file replay_values
  local replay_samples replayed_active controlled_active replay_invalid
  process_values="$(awk -F, '
    NR > 1 && $3 ~ /^[0-9]+$/ && $7 ~ /^[0-9]+$/ && $8 ~ /^[0-9]+$/ {
      value = $3 " " $7 " " $8
    }
    END { print value }
  ' "$run_dir/process.csv" 2>/dev/null)"
  read -r rss fd threads <<<"$process_values"
  [[ "$rss" =~ ^[0-9]+$ && "$fd" =~ ^[0-9]+$ && "$threads" =~ ^[0-9]+$ ]] || \
    return 1
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r endpoint_samples max_total available live outstanding rest \
    <<<"$endpoint_values"
  data_plane_values="$(m2_data_plane_envelope "$run_dir/mini_vpn.log")"
  read -r data_samples dns_forged dns_dropped active_relays total_relays \
    fake_active fake_total <<<"$data_plane_values"
  active_leases="$(tcp_pool_latest_active_leases \
    "$run_dir/mini_vpn.log" 2>/dev/null || true)"
  profile_file="$run_dir/m2-workload.txt"
  replay_values="$(m2_controlled_tcp_replay_for_profile \
    "$run_dir/mini_vpn.log" "$profile_file" 2>/dev/null || true)"
  read -r replay_samples replayed_active controlled_active replay_invalid \
    <<<"$replay_values"
  [[ "$previous_endpoint_samples" =~ ^[0-9]+$ && \
    "$previous_data_plane_samples" =~ ^[0-9]+$ && \
    "$endpoint_samples" =~ ^[0-9]+$ && "$data_samples" =~ ^[0-9]+$ && \
    10#$endpoint_samples -gt 10#$previous_endpoint_samples && \
    10#$data_samples -gt 10#$previous_data_plane_samples ]] || return 1
  for value in "$available" "$live" "$outstanding" "$dns_forged" \
    "$dns_dropped" "$active_relays" "$total_relays" "$fake_active" \
    "$fake_total" "$active_leases" "$replay_samples" "$replayed_active" \
    "$controlled_active" "$replay_invalid"; do
    [[ "$value" =~ ^[0-9]+$ ]] || return 1
  done
  ((10#$replay_samples == 10#$data_samples)) || return 1
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(timestamp)" "$label" "$rss" "$fd" "$threads" "$available" "$live" \
    "$outstanding" "$active_relays" "$total_relays" "$fake_active" \
    "$fake_total" "$dns_forged" "$dns_dropped" "$active_leases" \
    "$replayed_active" "$controlled_active" "$replay_invalid" \
    >>"$run_dir/m2-checkpoints.csv" || return 1
  ((10#$live == 0 && 10#$outstanding == 0 && \
    10#$available + 10#$live + 10#$outstanding <= 61440 && \
    10#$controlled_active == 0 && 10#$replay_invalid == 0 && \
    10#$dns_dropped == 0))
}

capture_m2_checkpoint_after_drain() {
  local run_dir="$1"
  local label="$2"
  local previous_endpoint_samples="$3"
  local previous_data_plane_samples="$4"
  local duration timeout_secs profile_file snapshot
  duration="$(read_state duration 2>/dev/null || true)"
  validate_positive_integer "$duration" || return 1
  timeout_secs=$((10#$duration + 30))
  profile_file="$run_dir/m2-workload.txt"
  if ! wait_for_m2_full_tunnel_quiescence "$run_dir/mini_vpn.log" \
    "$profile_file" "$previous_data_plane_samples" \
    "$previous_endpoint_samples" "$timeout_secs"; then
    snapshot="$(m2_full_tunnel_quiescence_snapshot \
      "$run_dir/mini_vpn.log" "$profile_file")"
    append_event_to "$run_dir" \
      "m2 controlled drain failed label=$label snapshot=$snapshot"
    return 1
  fi
  capture_m2_checkpoint "$run_dir" "$label" "$previous_endpoint_samples" \
    "$previous_data_plane_samples"
}

m0_final_ownership_is_clean() {
  local log_file="$1"
  local samples max_total last_available last_live last_outstanding max_live max_outstanding
  conservation_check_file "$log_file" || return 1
  read -r samples max_total last_available last_live last_outstanding max_live max_outstanding \
    <<<"$(endpoint_resource_envelope "$log_file")"
  [[ "$samples" =~ ^[0-9]+$ && "$samples" != "0" && \
    "$last_live" == "0" && "$last_outstanding" == "0" ]]
}

m0_log_history_is_complete() {
  local run_dir="$1"
  [[ -f "$run_dir/events.tsv" ]] && \
    ! grep -Fq $'\twatchdog compacted mini_vpn.log' "$run_dir/events.tsv"
}

remote_write_close_ownership_is_clean() {
  local log_file="$1"
  awk '
    /tcp-d16-relay-close/ &&
      /terminal_reason=(remote_write_failed|stalled_write_timeout)( |$)/ {
      count++
      if ($0 ~ /terminal_reason=stalled_write_timeout( |$)/ ||
          $0 !~ /queue_queued=0( |$)/ ||
          $0 !~ /queue_leased=0( |$)/ ||
          $0 !~ /queue_reserved=0( |$)/ ||
          $0 !~ /queue_closed=true( |$)/) invalid++
    }
    END { exit !(count > 0 && invalid == 0) }
  ' "$log_file" 2>/dev/null
}

d16_terminal_ownership_is_clean() {
  local log_file="$1"
  # A clean relay task may finish after handing its final lease to smoltcp.
  # Accept that transient state only when the same handle proves an equal
  # local EOF queue and then either drains it or proves that the local TCP peer
  # is terminally closed after D16/pending ownership reached zero. Bytes left
  # in the latter smoltcp send queue cannot be delivered and are not D16-owned.
  awk '
    function field(prefix, position) {
      for (position = 1; position <= NF; position++) {
        if (substr($position, 1, length(prefix)) == prefix) {
          return substr($position, length(prefix) + 1)
        }
      }
      return ""
    }
    /tcp-d16-relay-close/ {
      handle = field("handle=")
      reason = field("terminal_reason=")
      queued = field("queue_queued=")
      leased = field("queue_leased=")
      reserved = field("queue_reserved=")
      closed = field("queue_closed=")
      if (queued !~ /^[0-9]+$/ || leased !~ /^[0-9]+$/ ||
          reserved !~ /^[0-9]+$/ || closed !~ /^(true|false)$/) {
        invalid++
        next
      }
      if (queued + 0 > 0 || reserved + 0 > 0 || closed != "true") {
        invalid++
      }
      if (leased + 0 > 0) {
        if (reason != "clean_queue_lifecycle" ||
            handle == "" || drain_phase[handle] != 0) {
          invalid++
        } else {
          drain_phase[handle] = 1
          drain_bytes[handle] = leased + 0
        }
      }
      next
    }
    /tcp-relay-engine/ {
      handle = field("handle=")
      if (handle != "" && drain_phase[handle] != 0) invalid++
      next
    }
    /tcp-local-eof-close/ {
      handle = field("handle=")
      if (handle == "" || drain_phase[handle] == 0) next
      direction = field("direction=")
      reason = field("reason=")
      send_queue = field("send_queue=")
      if (drain_phase[handle] != 1 ||
          direction != "remote_to_local" || reason != "remote_eof" ||
          send_queue !~ /^[0-9]+$/ ||
          send_queue + 0 != drain_bytes[handle]) {
        invalid++
      } else {
        drain_phase[handle] = 2
      }
      next
    }
    /tcp-handle-close/ {
      handle = field("handle=")
      if (handle == "" || drain_phase[handle] == 0) next
      direction = field("direction=")
      reason = field("reason=")
      pending = field("pending=")
      terminal_reap = field("terminal_pending_reap_bytes=")
      permit_drop_bytes = field("permit_terminal_drop_bytes=")
      permit_drop_events = field("permit_terminal_drop_events=")
      send_queue = field("send_queue=")
      if (drain_phase[handle] != 2 ||
          direction != "remote_to_local" || reason != "remote_eof" ||
          pending != "0" || terminal_reap != "0" ||
          permit_drop_bytes != "0" || permit_drop_events != "0" ||
          send_queue !~ /^[0-9]+$/) {
        invalid++
      } else if (send_queue != "0") {
        close_pending_class = field("close_pending_class=")
        close_pending_bytes = field("close_pending_bytes=")
        close_egress_class = field("close_egress_class=")
        close_egress_bytes = field("close_egress_bytes=")
        drain_candidate = field("close_egress_drain_candidate=")
        tcp_state = field("tcp_state=")
        active = field("active=")
        can_send = field("can_send=")
        may_send = field("may_send=")
        if (send_queue + 0 != drain_bytes[handle] ||
            close_pending_class != "none" || close_pending_bytes != "0" ||
            close_egress_class != "terminal_closed_no_send" ||
            close_egress_bytes !~ /^[0-9]+$/ ||
            close_egress_bytes + 0 != drain_bytes[handle] ||
            drain_candidate != "false" || tcp_state != "Closed" ||
            active != "false" || can_send != "false" || may_send != "false") {
          invalid++
        }
      }
      delete drain_phase[handle]
      delete drain_bytes[handle]
      next
    }
    END {
      for (handle in drain_phase) {
        if (drain_phase[handle] != 0) invalid++
      }
      exit !(invalid == 0)
    }
  ' "$log_file" 2>/dev/null
}

m0_result_envelope() {
  local m0_dir="$1"
  local result_file
  if [[ ! -d "$m0_dir" ]]; then
    echo "0 0 unknown unknown 0 0 0"
    return 0
  fi
  {
    for result_file in "$m0_dir"/*.json; do
      [[ -f "$result_file" ]] || continue
      if ! jq -er '
        def interval_entry_is_proven_partial($duration; $count):
          .key as $index
          | .value.sum.start as $start | .value.sum.end as $end
          | (($duration | type) == "number"
            and $index == ($count - 1)
            and ($start | type) == "number" and ($end | type) == "number"
            and $start >= ($duration - 0.5) and $end >= $duration
            and $end <= ($duration + 0.5) and $end >= $start
            and ($end - $start) < 0.5);
        . as $root
        | $root.end.sum_sent.bytes as $sent
        | $root.end.sum_received.bytes as $received
        | ($sent - $received | if . < 0 then -. else . end) as $gap
        | $root.start.test_start.reverse as $reverse
        | (if $reverse == 0 then $root else $root.server_output_json end) as $sender
        | (if $reverse == 0 then $root.server_output_json else $root end) as $receiver
        | if (($sender | type) != "object"
            or ($receiver | type) != "object"
            or ($sender.intervals | type) != "array"
            or ($receiver.intervals | type) != "array"
            or (all($sender.intervals[];
              (.sum.bits_per_second | type) == "number"
              and .sum.bits_per_second >= 0) | not)
            or (all($receiver.intervals[];
              (.sum.bits_per_second | type) == "number"
              and .sum.bits_per_second >= 0) | not))
          then error("missing directional interval evidence") else . end
        | $sender.intervals as $sender_intervals
        | [$sender_intervals | to_entries[]
            | select((.value.sum.bits_per_second | type) == "number"
              and .value.sum.bits_per_second <= 0
              and (interval_entry_is_proven_partial(
                $sender.start.test_start.duration;
                ($sender_intervals | length)) | not))]
          | length as $sender_zero
        | $receiver.intervals as $receiver_intervals
        | [$receiver_intervals | to_entries[]
            | select((.value.sum.bits_per_second | type) == "number"
              and .value.sum.bits_per_second <= 0
              and (interval_entry_is_proven_partial(
                $receiver.start.test_start.duration;
                ($receiver_intervals | length)) | not))]
          | length as $receiver_zero
        | [
            $root.start.test_start.protocol,
            $gap,
            ($root.end.sum.lost_percent // $root.end.sum_received.lost_percent // -1),
            $sender_zero,
            $receiver_zero
          ]
        | @tsv
      ' "$result_file" 2>/dev/null; then
        echo $'INVALID\t0\t-1\t0\t0'
      fi
    done
  } | awk -F '\t' '
    $1 == "TCP" {
      tcp++
      gap = $2 + 0
      if (tcp == 1 || gap > max_tcp_gap) max_tcp_gap = gap
    }
    $1 == "UDP" {
      udp++
      loss = $3 + 0
      if (loss >= 0 && (!have_udp_loss || loss > max_udp_loss)) {
        max_udp_loss = loss
        have_udp_loss = 1
      }
    }
    $1 == "TCP" || $1 == "UDP" {
      sender_zero += $4 + 0
      receiver_zero += $5 + 0
    }
    $1 == "INVALID" { invalid++ }
    END {
      tcp_gap = tcp > 0 ? max_tcp_gap : "unknown"
      udp_loss = have_udp_loss ? sprintf("%.6f", max_udp_loss) : "unknown"
      printf "%d %d %s %s %d %d %d\n", tcp + 0, udp + 0, tcp_gap,
        udp_loss, invalid + 0, sender_zero + 0, receiver_zero + 0
    }
  '
}

m1_diagnostic_violation_envelope() {
  local violation_file="$1"
  if [[ ! -f "$violation_file" ]]; then
    echo "0 1"
    return 0
  fi
  awk -F '\t' '
    NR == 1 {
      if ($0 != "timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence") invalid++
      next
    }
    {
      count++
      key = $4 SUBSEP $7
      seen[key]++
      if (NF != 7 ||
          $1 !~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$/ ||
          $2 !~ /^[0-9]+$/ ||
          $3 !~ /^[A-Za-z0-9._-]+$/ ||
          $4 !~ /^(receiver_zero_interval|udp_loss_percent|tcp_sender_receiver_gap_bytes)$/ ||
          $5 !~ /^[0-9]+([.][0-9]+)?$/ ||
          $6 == "" || $7 == "" || seen[key] > 1) invalid++
    }
    END { print count + 0, invalid + 0 }
  ' "$violation_file"
}

m0_dns_result_envelope() {
  local m0_dir="$1"
  local result_file count=0 invalid=0
  if [[ ! -d "$m0_dir" ]]; then
    echo "0 0"
    return 0
  fi
  for result_file in "$m0_dir"/*_dns.txt; do
    [[ -f "$result_file" ]] || continue
    count=$((count + 1))
    validate_m0_dns_result "$result_file" || invalid=$((invalid + 1))
  done
  echo "$count $invalid"
}

baseline_receiver_bps() {
  local json_file="$1"
  jq -er '
    (if .start.test_start.reverse == 0 then .server_output_json else . end)
    | .end.sum_received.bits_per_second
    | if type == "number" and . > 0 then floor else error("invalid receiver rate") end
  ' "$json_file" 2>/dev/null
}

baseline_receiver_summary() {
  local baseline_dir="$1"
  local forward_file="$baseline_dir/direct-forward.json"
  local reverse_file="$baseline_dir/direct-reverse.json"
  local forward_bps reverse_bps forward_mbit reverse_mbit
  local forward_zero reverse_zero
  forward_bps="$(baseline_receiver_bps "$forward_file")" || return 1
  reverse_bps="$(baseline_receiver_bps "$reverse_file")" || return 1
  forward_zero="$(jq -er \
    'def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    .server_output_json.start.test_start.duration as $duration
    | .server_output_json.intervals as $intervals
    | [$intervals | to_entries[] | select(
      (.value.sum.bits_per_second | type) != "number" or
      (.value.sum.bits_per_second <= 0 and
        (interval_entry_is_proven_partial(
          $duration; ($intervals | length)) | not)))] | length' \
    "$forward_file" 2>/dev/null)" || return 1
  reverse_zero="$(jq -er \
    'def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    .start.test_start.duration as $duration
    | .intervals as $intervals
    | [$intervals | to_entries[] | select(
      (.value.sum.bits_per_second | type) != "number" or
      (.value.sum.bits_per_second <= 0 and
        (interval_entry_is_proven_partial(
          $duration; ($intervals | length)) | not)))] | length' \
    "$reverse_file" 2>/dev/null)" || return 1
  forward_mbit="$(awk -v bps="$forward_bps" 'BEGIN {printf "%.3f", bps / 1000000}')"
  reverse_mbit="$(awk -v bps="$reverse_bps" 'BEGIN {printf "%.3f", bps / 1000000}')"
  printf 'BASELINE receiver: forward_mbit=%s reverse_mbit=%s forward_zero_intervals=%s reverse_zero_intervals=%s\n' \
    "$forward_mbit" "$reverse_mbit" "$forward_zero" "$reverse_zero"
}

run_m0_baseline_file_validator() {
  local json_file="$1"
  local target="$2"
  local reverse="$3"
  [[ -f "$json_file" && ! -L "$json_file" ]] || return 1
  jq -e --arg target "$target" --argjson reverse "$reverse" '
    def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    ((.error? // "") == "")
    and (.start.connecting_to.host == $target)
    and (.start.test_start.protocol == "TCP")
    and (.start.test_start.reverse == $reverse)
    and ((.intervals | type) == "array" and (.intervals | length) > 0)
    and ((.server_output_json | type) == "object")
    and (.server_output_json.start.test_start.protocol == "TCP")
    and (.server_output_json.start.test_start.reverse == $reverse)
    and ((if $reverse == 0 then .server_output_json else . end) as $receiver
      | (($receiver.intervals | type) == "array"
        and ($receiver.intervals | length) > 0)
      and ($receiver.intervals as $intervals
        | all($intervals | to_entries[];
          .value.sum.bits_per_second as $bps
          | (($bps | type) == "number"
            and ($bps > 0 or ($bps == 0 and
              interval_entry_is_proven_partial(
                $receiver.start.test_start.duration;
                ($intervals | length)))))))
      and ($receiver.end.sum_received.bits_per_second as $bps
        | (($bps | type) == "number" and $bps > 0)))
  ' "$json_file"
}

baseline_file_validation_reason() {
  local json_file="$1"
  local target="$2"
  local reverse="$3"
  local rc
  if [[ ! -f "$json_file" || -L "$json_file" ]]; then
    echo missing_or_symlink
    return 0
  fi
  if run_m0_baseline_file_validator "$json_file" "$target" "$reverse" \
    >/dev/null; then
    echo ok
    return 0
  else
    rc=$?
  fi
  if ((rc == 1)); then
    echo invalid_evidence
  else
    echo "validator_error_rc_$rc"
  fi
}

baseline_pair_validation_reasons() {
  local baseline_dir="$1"
  local target="$2"
  local forward_reason reverse_reason
  forward_reason="$(baseline_file_validation_reason \
    "$baseline_dir/direct-forward.json" "$target" 0)"
  reverse_reason="$(baseline_file_validation_reason \
    "$baseline_dir/direct-reverse.json" "$target" 1)"
  printf '%s %s\n' "$forward_reason" "$reverse_reason"
}

baseline_check_report() {
  local baseline_dir="$1"
  local target="$2"
  local forward_reason reverse_reason summary status reason
  read -r forward_reason reverse_reason \
    <<<"$(baseline_pair_validation_reasons "$baseline_dir" "$target")"
  summary="$(baseline_receiver_summary "$baseline_dir" 2>/dev/null || \
    echo unavailable)"
  status=fail
  reason="forward_${forward_reason}__reverse_${reverse_reason}"
  if [[ "$forward_reason" == "ok" && "$reverse_reason" == "ok" ]]; then
    status=pass
    reason=ok
  fi
  printf '%s\n' \
    'schema=knife15-macos-baseline-check-v1' \
    "status=$status" \
    "reason=$reason" \
    "source_commit=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)" \
    "runner_sha256=$(sha256_file_or_unknown "$SCRIPT_PATH")" \
    "target=$target" \
    "baseline_dir=$baseline_dir" \
    "forward_sha256=$(sha256_file_or_unknown "$baseline_dir/direct-forward.json")" \
    "reverse_sha256=$(sha256_file_or_unknown "$baseline_dir/direct-reverse.json")" \
    "forward_validation=$forward_reason" \
    "reverse_validation=$reverse_reason" \
    "receiver_summary=$summary"
  [[ "$status" == "pass" ]]
}

write_baseline_manifest() {
  local baseline_dir="$1"
  local status="$2"
  local reason="$3"
  local forward_reason="$4"
  local reverse_reason="$5"
  local summary
  summary="$(baseline_receiver_summary "$baseline_dir" 2>/dev/null || \
    echo unavailable)"
  cat >"$baseline_dir/manifest.txt" <<EOF_BASELINE_MANIFEST
schema=knife15-macos-baseline-v1
status=$status
reason=$reason
completed_utc=$(timestamp)
completed_epoch=$(date +%s)
source_commit=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)
runner_sha256=$(sha256_file_or_unknown "$SCRIPT_PATH")
target=$TARGET
target_route=$(route_interface "$TARGET")
duration_secs=$DURATION
parallel=$PARALLEL
forward_sha256=$(sha256_file_or_unknown "$baseline_dir/direct-forward.json")
reverse_sha256=$(sha256_file_or_unknown "$baseline_dir/direct-reverse.json")
forward_validation=$forward_reason
reverse_validation=$reverse_reason
receiver_summary=$summary
EOF_BASELINE_MANIFEST
}

run_baseline_check() {
  local baseline_dir report
  require_command jq
  require_command shasum
  validate_ipv4 "$TARGET" || die "baseline-check requires an IPv4 TARGET"
  baseline_dir="$(selected_direct_baseline_dir)" || \
    die "set exactly one of M0_BASELINE_DIR, M1_BASELINE_DIR, or M2_BASELINE_DIR for baseline-check"
  validate_baseline_dir_path "$baseline_dir" || \
    die "baseline-check requires a simple /tmp baseline directory"
  [[ -d "$baseline_dir" && ! -L "$baseline_dir" ]] || \
    die "baseline-check requires an existing non-symlink baseline directory"
  report="$(baseline_check_report "$baseline_dir" "$TARGET")"
  local rc=$?
  printf '%s\n' "$report"
  ((rc == 0)) || die "baseline-check rejected the selected evidence"
  echo "PASS: direct baseline evidence replay completed"
}

validate_m0_baseline_file() {
  [[ "$(baseline_file_validation_reason "$1" "$2" "$3" 2>/dev/null)" == \
    "ok" ]]
}

validate_m0_baseline_pair() {
  local baseline_dir="$1"
  local target="$2"
  [[ "$(baseline_pair_validation_reasons \
    "$baseline_dir" "$target" 2>/dev/null)" == "ok ok" ]]
}

validate_direct_continuity_result() {
  local result="$1"
  local target="$2"
  jq -e --arg target "$target" '
    (.start.connecting_to.host == $target)
    and (.start.test_start.duration == 300)
    and (.server_output_json.end.sum_received.seconds as $seconds
      | (($seconds | type) == "number" and $seconds >= 299 and $seconds <= 310))
    and ([.server_output_json.intervals[]
      | select((.sum.end - .sum.start) >= 0.5)] | length) >= 300
  ' "$result" >/dev/null 2>&1 && validate_m0_iperf_result "$result" TCP 0
}

validate_direct_continuity_dir() {
  local direct_dir="$1"
  local baseline_dir="$2"
  local target="$3"
  local now_epoch="${4:-$(date +%s)}"
  local manifest="$direct_dir/manifest.txt"
  local result="$direct_dir/direct-forward-300s.json"
  local completed_epoch target_route exit_route forward_bps expected_rate current_source

  [[ -d "$direct_dir" && ! -L "$direct_dir" ]] || return 1
  [[ -f "$manifest" && ! -L "$manifest" && -f "$result" && ! -L "$result" ]] || \
    return 1
  [[ "$(m0_profile_value "$manifest" schema)" == \
    "knife15-macos-direct-continuity-v1" ]] || return 1
  [[ "$(m0_profile_value "$manifest" status)" == "pass" ]] || return 1
  [[ "$(m0_profile_value "$manifest" target)" == "$target" ]] || return 1
  [[ "$(m0_profile_value "$manifest" baseline_dir)" == "$baseline_dir" ]] || return 1
  [[ "$(m0_profile_value "$manifest" baseline_forward_sha256)" == \
    "$(sha256_file "$baseline_dir/direct-forward.json")" ]] || return 1
  [[ "$(m0_profile_value "$manifest" baseline_reverse_sha256)" == \
    "$(sha256_file "$baseline_dir/direct-reverse.json")" ]] || return 1
  [[ "$(m0_profile_value "$manifest" result_sha256)" == \
    "$(sha256_file "$result")" ]] || return 1
  [[ "$(m0_profile_value "$manifest" duration_secs)" == "300" ]] || return 1
  forward_bps="$(baseline_receiver_bps \
    "$baseline_dir/direct-forward.json")" || return 1
  [[ "$forward_bps" =~ ^[0-9]+$ ]] || return 1
  expected_rate=$((10#$forward_bps / 2))
  [[ "$(m0_profile_value "$manifest" rate_bps)" == "$expected_rate" ]] || return 1
  current_source="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
  [[ "$(m0_profile_value "$manifest" source_commit)" == "$current_source" ]] || return 1
  [[ "$(m0_profile_value "$manifest" runner_sha256)" == \
    "$(sha256_file "$SCRIPT_PATH")" ]] || return 1
  [[ "$(m0_profile_value "$manifest" binary_sha256)" == \
    "$(sha256_file "$BIN")" ]] || return 1
  completed_epoch="$(m0_profile_value "$manifest" completed_epoch)"
  [[ "$completed_epoch" =~ ^[0-9]+$ && "$now_epoch" =~ ^[0-9]+$ ]] || return 1
  ((10#$completed_epoch <= 10#$now_epoch && \
    10#$now_epoch - 10#$completed_epoch <= M0_DIRECT_MAX_AGE_SECS)) || return 1
  target_route="$(m0_profile_value "$manifest" target_route)"
  exit_route="$(m0_profile_value "$manifest" exit_route)"
  [[ -n "$target_route" && "$target_route" != utun* ]] || return 1
  [[ -n "$exit_route" && "$exit_route" != utun* ]] || return 1
  validate_direct_continuity_result "$result" "$target"
}

validate_m0_iperf_result() {
  local json_file="$1"
  local protocol="$2"
  local reverse="$3"
  local allow_receiver_zero="${4:-0}"
  jq -e --arg protocol "$protocol" --argjson reverse "$reverse" \
    --argjson allow_receiver_zero "$allow_receiver_zero" '
    def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    ((.error? // "") == "")
    and (.start.test_start.protocol == $protocol)
    and (.start.test_start.reverse == $reverse)
    and ((.intervals | type) == "array" and (.intervals | length) > 0)
    and all(.intervals[];
      .sum.bits_per_second as $bps
      | (($bps | type) == "number" and $bps >= 0))
    and ((.server_output_json | type) == "object")
    and (.server_output_json.start.test_start.protocol == $protocol)
    and (.server_output_json.start.test_start.reverse == $reverse)
    and ((.server_output_json.intervals | type) == "array"
      and (.server_output_json.intervals | length) > 0)
    and all(.server_output_json.intervals[];
      .sum.bits_per_second as $bps
      | (($bps | type) == "number" and $bps >= 0))
    and (.end.sum_received.bits_per_second as $bps
      | (($bps | type) == "number" and $bps > 0))
    and (.end.sum_sent.bytes as $bytes
      | (($bytes | type) == "number" and $bytes > 0))
    and (.end.sum_received.bytes as $bytes
      | (($bytes | type) == "number" and $bytes > 0))
    and (if $protocol == "UDP" then
      ((.end.sum.lost_percent // .end.sum_received.lost_percent) as $loss
        | (($loss | type) == "number" and $loss >= 0 and $loss <= 100))
    else true end)
    and ((if $reverse == 0 then .server_output_json else . end) as $receiver
      | (($receiver | type) == "object")
      and ($receiver.start.test_start.protocol == $protocol)
      and ($receiver.start.test_start.reverse == $reverse)
      and (($receiver.intervals | type) == "array"
        and ($receiver.intervals | length) > 0)
      and ($receiver.intervals as $intervals
        | all($intervals | to_entries[];
          .value.sum.bits_per_second as $bps
          | (($bps | type) == "number"
            and ($bps > 0 or ($bps == 0 and
              ($allow_receiver_zero == 1 or
                interval_entry_is_proven_partial(
                  $receiver.start.test_start.duration;
                  ($intervals | length))))))))
      and ($receiver.end.sum_received.bits_per_second as $bps
        | (($bps | type) == "number" and $bps > 0))
      and ($receiver.end.sum_received.bytes as $bytes
        | (($bytes | type) == "number" and $bytes > 0)))
  ' "$json_file" >/dev/null 2>&1
}

m0_iperf_result_failure_reason() {
  local json_file="$1"
  local protocol="$2"
  local reverse="$3"
  if validate_m0_iperf_result "$json_file" "$protocol" "$reverse"; then
    echo ok
    return 0
  fi
  if ! jq -e '(.server_output_json | type) == "object"' \
    "$json_file" >/dev/null 2>&1; then
    if [[ "$reverse" == "0" ]]; then
      echo missing_receiver_evidence
    else
      echo missing_sender_evidence
    fi
    return 0
  fi
  if jq -e --argjson reverse "$reverse" '
    def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    (if $reverse == 0 then .server_output_json else . end) as $receiver
    | (($receiver.intervals | type) == "array")
      and ($receiver.intervals as $intervals
        | any($intervals | to_entries[];
          (.value.sum.bits_per_second | type) == "number"
          and .value.sum.bits_per_second <= 0
          and (interval_entry_is_proven_partial(
            $receiver.start.test_start.duration;
            ($intervals | length)) | not)))
  ' "$json_file" >/dev/null 2>&1; then
    echo receiver_zero_interval
    return 0
  fi
  echo invalid_iperf_result
}

receiver_zero_interval_evidence() {
  local json_file="$1"
  local reverse="$2"
  jq -er --argjson reverse "$reverse" '
    def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    (if $reverse == 0 then .server_output_json else . end) as $receiver
    | $receiver.intervals as $intervals
    | [$intervals | to_entries[]
        | select((.value.sum.bits_per_second | type) == "number"
          and .value.sum.bits_per_second <= 0
          and (interval_entry_is_proven_partial(
            $receiver.start.test_start.duration;
            ($intervals | length)) | not))
        | "\(.value.sum.start)-\(.value.sum.end)"] as $ranges
    | select(($ranges | length) > 0)
    | "\($ranges | length)\t\($ranges | join(","))"
  ' "$json_file" 2>/dev/null
}

udp_loss_percent() {
  local json_file="$1"
  jq -er '
    (.end.sum.lost_percent // .end.sum_received.lost_percent) as $loss
    | select(($loss | type) == "number" and $loss >= 0 and $loss <= 100)
    | $loss
  ' "$json_file" 2>/dev/null
}

record_soak_data_quality_violation() {
  local run_dir="$1"
  local cycle="$2"
  local phase="$3"
  local kind="$4"
  local value="$5"
  local detail="$6"
  local evidence_file="$7"
  local relative_evidence
  [[ "${SOAK_CONTINUE_DATA_QUALITY:-0}" == "1" ]] || return 1
  [[ -n "${SOAK_VIOLATIONS_FILE:-}" && -f "$SOAK_VIOLATIONS_FILE" ]] || return 1
  [[ "$detail" != *$'\t'* && "$detail" != *$'\n'* ]] || return 1
  relative_evidence="${evidence_file#"$run_dir"/}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(timestamp)" "$cycle" "$phase" "$kind" "$value" "$detail" \
    "$relative_evidence" >>"$SOAK_VIOLATIONS_FILE" || return 1
  append_event_to "$run_dir" \
    "$SOAK_STAGE phase violation cycle=$cycle phase=$phase kind=$kind value=$value detail=$detail"
}

validate_target_ready_result() {
  local json_file="$1"
  local target="$2"
  jq -e --arg target "$target" '
    def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    ((.error? // "") == "")
    and (.start.connecting_to.host == $target)
    and (.start.test_start.protocol == "TCP")
    and (.start.test_start.reverse == 0)
    and ((.intervals | type) == "array" and (.intervals | length) > 0)
    and all(.intervals[];
      .sum.bits_per_second as $bps
      | (($bps | type) == "number" and $bps > 0))
    and (.end.sum_sent.bytes as $bytes
      | (($bytes | type) == "number" and $bytes > 0))
    and (.end.sum_received.bytes as $bytes
      | (($bytes | type) == "number" and $bytes > 0))
    and (.end.sum_received.bits_per_second as $bps
      | (($bps | type) == "number" and $bps > 0))
    and ((.server_output_json // null) as $receiver
      | (($receiver | type) == "object")
      and ($receiver.start.test_start.protocol == "TCP")
      and ($receiver.start.test_start.reverse == 0)
      and (($receiver.intervals | type) == "array"
        and ($receiver.intervals | length) > 0)
      and ($receiver.intervals as $intervals
        | all($intervals | to_entries[];
          .value.sum.bits_per_second as $bps
          | (($bps | type) == "number"
            and ($bps > 0 or ($bps == 0 and
              interval_entry_is_proven_partial(
                $receiver.start.test_start.duration;
                ($intervals | length)))))))
      and ($receiver.end.sum_received.bytes as $bytes
        | (($bytes | type) == "number" and $bytes > 0))
      and ($receiver.end.sum_received.bits_per_second as $bps
        | (($bps | type) == "number" and $bps > 0)))
  ' "$json_file" >/dev/null 2>&1
}

target_ready_probe() {
  local result_file
  require_command iperf3
  require_command jq
  result_file="$(mktemp "${TMPDIR:-/tmp}/mini_vpn_knife15_target_ready.XXXXXX")" || \
    die "cannot create Target readiness result file"
  echo "Checking that Target can complete a fresh direct iperf3 transaction..."
  if ! run_logged "$result_file" \
    iperf3 -c "$TARGET" -p "$IPERF_PORT" -t 1 -P 1 --connect-timeout 5000 \
      --json --get-server-output; then
    rm -f "$result_file"
    die "Target readiness command failed; wait for $TARGET:$IPERF_PORT to recover before start"
  fi
  if ! validate_target_ready_result "$result_file" "$TARGET"; then
    rm -f "$result_file"
    die "Target is busy or could not complete a positive direct transaction; do not rearm yet"
  fi
  TARGET_READY_UTC="$(timestamp)"
  TARGET_READY_RECEIVER_BPS="$(jq -er \
    '.server_output_json.end.sum_received.bits_per_second | floor' "$result_file")" || {
    rm -f "$result_file"
    die "cannot record Target readiness receiver rate"
  }
  rm -f "$result_file"
  echo "PASS: Target readiness transaction completed receiver_bps=$TARGET_READY_RECEIVER_BPS"
}

validate_m0_dns_result() {
  local output_file="$1"
  grep -Eq '(^|[[:space:]])198\.(18|19)\.[0-9]{1,3}\.[0-9]{1,3}([[:space:]]|$)' \
    "$output_file"
}

write_m0_profile() {
  local baseline_dir="$1"
  local output_file="$2"
  local target="${3:-$TARGET}"
  local iperf_port="${4:-$IPERF_PORT}"
  local dns_target="${5:-$DNS_TARGET}"
  local dns_name="${6:-$DNS_NAME}"
  local forward_bps reverse_bps
  forward_bps="$(baseline_receiver_bps "$baseline_dir/direct-forward.json")" || return 1
  reverse_bps="$(baseline_receiver_bps "$baseline_dir/direct-reverse.json")" || return 1
  cat >"$output_file" <<EOF_M0_PROFILE
schema=knife15-macos-m0-v1
created_utc=$(timestamp)
baseline_dir=$baseline_dir
baseline_forward_sha256=$(sha256_file "$baseline_dir/direct-forward.json")
baseline_reverse_sha256=$(sha256_file "$baseline_dir/direct-reverse.json")
target=$target
iperf_port=$iperf_port
dns_target=${dns_target:-disabled}
dns_name=$dns_name
baseline_forward_bps=$forward_bps
baseline_reverse_bps=$reverse_bps
tcp_forward_bps=$((10#$forward_bps / 2))
tcp_reverse_bps=$((10#$reverse_bps / 2))
udp_reverse_bps=$((10#$reverse_bps / 2))
short_forward_bps=$((10#$forward_bps * 4 / 5))
short_reverse_bps=$((10#$reverse_bps * 4 / 5))
tcp_reverse_iperf_length_bytes=$TCP_REVERSE_IPERF_LENGTH_BYTES
udp_payload_bytes=1160
total_secs=$M0_TOTAL_SECS
tcp_epoch_secs=$M0_TCP_SECS
udp_epoch_secs=$M0_UDP_SECS
short_epoch_secs=$M0_SHORT_SECS
short_connections_per_cycle=$M0_SHORT_COUNT
idle_secs=$M0_IDLE_SECS
final_drain_secs=$M0_FINAL_DRAIN_SECS
EOF_M0_PROFILE
}

min_rate_bps() {
  local rate_bps="$1"
  if ((10#$rate_bps < M1_RATE_CAP_BPS)); then
    printf '%s\n' "$rate_bps"
  else
    printf '%s\n' "$M1_RATE_CAP_BPS"
  fi
}

write_m1_profile() {
  local baseline_dir="$1"
  local output_file="$2"
  local target="${3:-$TARGET}"
  local iperf_port="${4:-$IPERF_PORT}"
  local dns_target="${5:-$DNS_TARGET}"
  local dns_name="${6:-$DNS_NAME}"
  local forward_bps reverse_bps
  forward_bps="$(baseline_receiver_bps "$baseline_dir/direct-forward.json")" || return 1
  reverse_bps="$(baseline_receiver_bps "$baseline_dir/direct-reverse.json")" || return 1
  cat >"$output_file" <<EOF_M1_PROFILE
schema=knife15-macos-m1-v1
created_utc=$(timestamp)
baseline_dir=$baseline_dir
baseline_forward_sha256=$(sha256_file "$baseline_dir/direct-forward.json")
baseline_reverse_sha256=$(sha256_file "$baseline_dir/direct-reverse.json")
target=$target
iperf_port=$iperf_port
dns_target=${dns_target:-disabled}
dns_name=$dns_name
baseline_forward_bps=$forward_bps
baseline_reverse_bps=$reverse_bps
rate_cap_bps=$M1_RATE_CAP_BPS
steady_tcp_forward_bps=$(min_rate_bps $((10#$forward_bps / 2)))
steady_tcp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
steady_udp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
steady_short_forward_bps=$(min_rate_bps $((10#$forward_bps * 4 / 5)))
steady_short_reverse_bps=$(min_rate_bps $((10#$reverse_bps * 4 / 5)))
steady_short_connections_per_cycle=$M1_STEADY_SHORT_COUNT
quiet_tcp_forward_bps=$(min_rate_bps $((10#$forward_bps / 4)))
quiet_tcp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
quiet_udp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
quiet_short_forward_bps=$(min_rate_bps $((10#$forward_bps / 2)))
quiet_short_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
quiet_short_connections_per_cycle=$M1_QUIET_SHORT_COUNT
churn_tcp_forward_bps=$(min_rate_bps $((10#$forward_bps / 4)))
churn_tcp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
churn_udp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
churn_short_forward_bps=$(min_rate_bps $((10#$forward_bps / 2)))
churn_short_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
churn_short_connections_per_cycle=$M1_CHURN_SHORT_COUNT
tcp_reverse_iperf_length_bytes=$TCP_REVERSE_IPERF_LENGTH_BYTES
udp_payload_bytes=1160
total_secs=$M1_TOTAL_SECS
steady_a_secs=$M1_STEADY_A_SECS
idle_secs=$M1_IDLE_SECS
quiet_secs=$M1_QUIET_SECS
steady_b_secs=$M1_STEADY_B_SECS
churn_secs=$M1_CHURN_SECS
steady_c_secs=$M1_STEADY_C_SECS
final_drain_secs=$M1_FINAL_DRAIN_SECS
tcp_epoch_secs=$M1_TCP_SECS
udp_epoch_secs=$M1_UDP_SECS
short_epoch_secs=$M1_SHORT_SECS
EOF_M1_PROFILE
}

write_m2_profile() {
  local baseline_dir="$1"
  local output_file="$2"
  local target="${3:-$TARGET}"
  local iperf_port="${4:-$IPERF_PORT}"
  local dns_target="${5:-$DNS_TARGET}"
  local dns_name="${6:-$DNS_NAME}"
  local forward_bps reverse_bps
  forward_bps="$(baseline_receiver_bps "$baseline_dir/direct-forward.json")" || return 1
  reverse_bps="$(baseline_receiver_bps "$baseline_dir/direct-reverse.json")" || return 1
  cat >"$output_file" <<EOF_M2_PROFILE
schema=knife15-macos-m2-v1
created_utc=$(timestamp)
baseline_dir=$baseline_dir
baseline_forward_sha256=$(sha256_file "$baseline_dir/direct-forward.json")
baseline_reverse_sha256=$(sha256_file "$baseline_dir/direct-reverse.json")
target=$target
iperf_port=$iperf_port
dns_target=${dns_target:-disabled}
dns_name=$dns_name
baseline_forward_bps=$forward_bps
baseline_reverse_bps=$reverse_bps
rate_cap_bps=$M1_RATE_CAP_BPS
steady_tcp_forward_bps=$(min_rate_bps $((10#$forward_bps / 2)))
steady_tcp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
steady_udp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
steady_short_forward_bps=$(min_rate_bps $((10#$forward_bps * 4 / 5)))
steady_short_reverse_bps=$(min_rate_bps $((10#$reverse_bps * 4 / 5)))
steady_short_connections_per_cycle=$M2_STEADY_SHORT_COUNT
quiet_tcp_forward_bps=$(min_rate_bps $((10#$forward_bps / 4)))
quiet_tcp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
quiet_udp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
quiet_short_forward_bps=$(min_rate_bps $((10#$forward_bps / 2)))
quiet_short_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
quiet_short_connections_per_cycle=$M2_QUIET_SHORT_COUNT
churn_tcp_forward_bps=$(min_rate_bps $((10#$forward_bps / 4)))
churn_tcp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
churn_udp_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 4)))
churn_short_forward_bps=$(min_rate_bps $((10#$forward_bps / 2)))
churn_short_reverse_bps=$(min_rate_bps $((10#$reverse_bps / 2)))
churn_short_connections_per_cycle=$M2_CHURN_SHORT_COUNT
tcp_reverse_iperf_length_bytes=$TCP_REVERSE_IPERF_LENGTH_BYTES
udp_payload_bytes=1160
total_secs=$M2_TOTAL_SECS
steady_a_secs=$M2_STEADY_A_SECS
idle_secs=$M2_IDLE_SECS
quiet_a_secs=$M2_QUIET_A_SECS
steady_b_secs=$M2_STEADY_B_SECS
churn_secs=$M2_CHURN_SECS
quiet_b_secs=$M2_QUIET_B_SECS
steady_c_secs=$M2_STEADY_C_SECS
final_drain_secs=$M2_FINAL_DRAIN_SECS
tcp_epoch_secs=$M2_TCP_SECS
udp_epoch_secs=$M2_UDP_SECS
short_epoch_secs=$M2_SHORT_SECS
expected_cycles=$M2_EXPECTED_CYCLES
expected_tcp_results=$M2_EXPECTED_TCP_RESULTS
expected_udp_results=$M2_EXPECTED_UDP_RESULTS
expected_phase_results=$M2_EXPECTED_PHASE_RESULTS
expected_checkpoints=$M2_EXPECTED_CHECKPOINTS
egress_url=$M2_EGRESS_URL
browser_url=$M2_BROWSER_URL
EOF_M2_PROFILE
}

runner_self_test() {
  local tmp good_log bad_log route_fixture interface_fixture ping_fixture network_fixture service_fixture dns_fixture m2_route_bin m2_ifconfig_bin m2_networksetup_bin m2_dscacheutil_bin m2_curl_bin m2_route_state m2_run m2_result m2_schedule_run m2_test_profile m2_checkpoint_file m2_capture_run m2_ipv6_evidence original_m2_route_bin original_m2_ifconfig_bin original_m2_networksetup_bin original_m2_dscacheutil_bin original_m2_curl_bin collector_dir collector_bin original_path original_state_dir clean_scan secret_scan_dir secret_value summary_dir baseline_dir baseline_summary_text m0_profile m1_profile m2_profile m1_test_profile m0_run m1_stage_run m1_run m1_diagnostic_run m1_diagnostic_fail_run m1_diagnostic_formal_run m1_checkpoint_file m1_capture_run m1_formal_run m1_tcp_fixture m1_udp_fixture m0_fail_run direct_dir fake_iperf fake_dig fake_sleep usage_text dns_result unrelated_pid target_ready_json finalized_run finalized_bundle finalized_hash bounded_status cycle_index result_index result_label sample_index violations_before violation_count invalid_violations ipv6_class ipv6_interface cleanup_class
  local m2_record_fail_run quiescence_record_status m2_replay_log
  local m2_qualification_run m2_qualification_fail_run
  local observer_status observer_run observer_fake observer_calls
  local observer_exit_run observer_exit_status original_m2_exit_observer_script
  local resource_evidence resource_direct resource_source resource_binary_sha
  local resource_observer_sha resource_provider_sha resource_route_sha
  local resource_candidate resource_file resource_source_dir resource_copy_dir
  local resource_archive resource_state_dir resource_original_state_dir
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/knife15-macos-self-test.XXXXXX")" || return 1
  good_log="$tmp/good.log"
  bad_log="$tmp/bad.log"
  route_fixture="$tmp/route.txt"
  interface_fixture="$tmp/interface.txt"
  service_fixture="$tmp/network-service-order.txt"
  dns_fixture="$tmp/dns-servers.txt"

  validate_ipv4 43.130.32.77 || die "self-test: valid IPv4 rejected"
  ! validate_ipv4 300.1.1.1 || die "self-test: invalid IPv4 accepted"
  validate_uuid 123e4567-e89b-12d3-a456-426614174000 || die "self-test: valid UUID rejected"
  ! validate_uuid not-a-uuid || die "self-test: invalid UUID accepted"
  validate_dns_name example.com || die "self-test: valid DNS name rejected"
  ! validate_dns_name $'bad\nname' || die "self-test: invalid DNS name accepted"
  validate_run_dir_path /tmp/mini_vpn_knife15_macos_20260714_010203 || \
    die "self-test: valid run directory rejected"
  validate_resource_preflight_dir_path \
    /tmp/mini_vpn_knife15_resource_candidate_a || \
    die "self-test: valid resource preflight directory rejected"
  ! validate_resource_preflight_dir_path \
    /tmp/mini_vpn_knife15_resource_../victim || \
    die "self-test: traversal resource preflight directory accepted"
  [[ "$(m2_resource_json_value \
    "$SCRIPT_DIR/fixtures/knife15-m2-resource/distinct-provider.json" \
    candidate_id)" == candidate-distinct-provider ]] || \
    die "self-test: resource JSON value lookup failed"
  resource_evidence="$tmp/resource-evidence"
  resource_direct="$tmp/resource-direct-manifest.txt"
  resource_candidate="$resource_evidence/candidate-profile.json"
  resource_source="$(printf '4%.0s' {1..40})"
  resource_binary_sha="$(printf '5%.0s' {1..64})"
  resource_observer_sha="$(sha256_file "$M2_EXIT_OBSERVER_SCRIPT")"
  mkdir "$resource_evidence"
  printf '%s\n' 'schema=knife15-direct-fixture-v1' >"$resource_direct"
  cp "$SCRIPT_DIR/fixtures/knife15-m2-resource/distinct-provider-identity.txt" \
    "$resource_evidence/provider-identity.txt"
  cp "$SCRIPT_DIR/fixtures/knife15-m2-resource/distinct-route-identity.txt" \
    "$resource_evidence/route-identity.txt"
  resource_provider_sha="$(sha256_file \
    "$resource_evidence/provider-identity.txt")"
  resource_route_sha="$(sha256_file "$resource_evidence/route-identity.txt")"
  cp "$SCRIPT_DIR/knife15-m2-reference-33.json" \
    "$resource_evidence/reference-profile.json"
  jq --arg source "$resource_source" \
    --arg binary "$resource_binary_sha" \
    --arg direct "$(sha256_file "$resource_direct")" \
    --arg observer "$resource_observer_sha" \
    --arg provider_evidence "$resource_provider_sha" \
    --arg route_evidence "$resource_route_sha" \
    '.source_commit = $source |
      .client_binary_sha256 = $binary |
      .workload_profile_sha256 = $direct |
      .observer_sha256 = $observer |
      .provider_identity_evidence_sha256 = $provider_evidence |
      .route_identity_evidence_sha256 = $route_evidence' \
    "$SCRIPT_DIR/fixtures/knife15-m2-resource/distinct-provider.json" \
    >"$resource_candidate"
  /usr/bin/python3 -I "$M2_RESOURCE_PROFILE_HELPER" compare \
    --reference "$resource_evidence/reference-profile.json" \
    --candidate "$resource_candidate" \
    >"$resource_evidence/eligibility.json"
  /usr/bin/python3 -I "$M2_RESOURCE_PROFILE_HELPER" validate-evidence \
    --candidate "$resource_candidate" \
    --provider-evidence "$resource_evidence/provider-identity.txt" \
    --route-evidence "$resource_evidence/route-identity.txt" \
    >"$resource_evidence/evidence-binding.json"
  cp "$resource_direct" "$resource_evidence/direct-manifest.txt"
  printf '%s\n' \
    'route to: 1.1.1.1' 'interface: en0' \
    >"$resource_evidence/exit.route.txt"
  printf '%s\n' \
    'route to: 43.130.32.77' 'interface: en0' \
    >"$resource_evidence/target.route.txt"
  printf '%s\n' 'traceroute fixture exit' \
    >"$resource_evidence/exit.traceroute.txt"
  printf '%s\n' 'traceroute fixture target' \
    >"$resource_evidence/target.traceroute.txt"
  : >"$resource_evidence/remote.stderr"
  printf '%s\n' \
    'schema=knife15-m2-resource-remote-v1' \
    'service_active=active' \
    'server_binary_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
    'server_config_sha256=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
    'cpu_count=4' 'loadavg=0.1,0.2,0.3,1/100,1' \
    'memtotal=8388608kB' 'memavailable=6291456kB' \
    'tuic_udp_listener=1' 'udp_summary_begin' 'UDP: 1' \
    'udp_summary_end' 'interface_counters_begin' 'eth0 fixture' \
    'interface_counters_end' >"$resource_evidence/remote.txt"
  printf '%s\n' 'PASS: no credential-like assignment found' \
    >"$resource_evidence/secret-scan.txt"
  printf '%s\n' \
    'schema=knife15-m2-resource-preflight-v1' \
    'status=pass' \
    'candidate_id=candidate-distinct-provider' \
    'candidate_ipv4=1.1.1.1' \
    'candidate_tuic_port=8443' \
    'target=43.130.32.77' \
    'target_iperf_port=5201' \
    'physical_interface=en0' \
    "source_commit=$resource_source" \
    "binary_sha256=$resource_binary_sha" \
    "direct_manifest_sha256=$(sha256_file "$resource_direct")" \
    "observer_sha256=$resource_observer_sha" \
    "profile_helper_sha256=$(sha256_file "$M2_RESOURCE_PROFILE_HELPER")" \
    "preflight_runner_sha256=$(sha256_file "$M2_RESOURCE_PREFLIGHT_RUNNER")" \
    "eligibility_sha256=$(sha256_file "$resource_evidence/eligibility.json")" \
    "evidence_binding_sha256=$(sha256_file \
      "$resource_evidence/evidence-binding.json")" \
    "provider_identity_sha256=$resource_provider_sha" \
    "route_identity_sha256=$resource_route_sha" \
    "remote_sha256=$(sha256_file "$resource_evidence/remote.txt")" \
    "exit_route_sha256=$(sha256_file "$resource_evidence/exit.route.txt")" \
    "target_route_sha256=$(sha256_file "$resource_evidence/target.route.txt")" \
    "exit_traceroute_sha256=$(sha256_file "$resource_evidence/exit.traceroute.txt")" \
    "target_traceroute_sha256=$(sha256_file "$resource_evidence/target.traceroute.txt")" \
    >"$resource_evidence/result.txt"
  : >"$resource_evidence/SHA256SUMS"
  for resource_file in \
    reference-profile.json candidate-profile.json eligibility.json \
    evidence-binding.json \
    direct-manifest.txt provider-identity.txt route-identity.txt \
    exit.route.txt target.route.txt exit.traceroute.txt target.traceroute.txt \
    remote.txt remote.stderr secret-scan.txt result.txt; do
    printf '%s  %s\n' "$(sha256_file "$resource_evidence/$resource_file")" \
      "$resource_file" >>"$resource_evidence/SHA256SUMS"
  done
  m2_resource_evidence_is_valid "$resource_evidence" \
    "$resource_source" "$resource_binary_sha" "$resource_direct" \
    "$resource_observer_sha" 1.1.1.1 8443 43.130.32.77 5201 en0 || \
    die "self-test: valid resource evidence rejected"
  resource_source_dir="/tmp/mini_vpn_knife15_resource_selftest_$$"
  resource_copy_dir="$tmp/resource-evidence-copy"
  resource_archive="${resource_source_dir}.tar.gz"
  rm -rf "$resource_source_dir" "$resource_archive" \
    "${resource_archive}.sha256"
  cp -R "$resource_evidence" "$resource_source_dir"
  COPYFILE_DISABLE=1 tar -C /tmp -czf "$resource_archive" \
    "$(basename "$resource_source_dir")"
  printf '%s  %s\n' "$(sha256_file "$resource_archive")" \
    "$resource_archive" >"${resource_archive}.sha256"
  m2_copy_resource_preflight_evidence \
    "$resource_source_dir" "$resource_copy_dir" || \
    die "self-test: valid resource evidence copy rejected"
  m2_resource_evidence_is_valid "$resource_copy_dir" \
    "$resource_source" "$resource_binary_sha" "$resource_direct" \
    "$resource_observer_sha" 1.1.1.1 8443 43.130.32.77 5201 en0 || \
    die "self-test: copied resource evidence rejected"
  resource_original_state_dir="$STATE_DIR"
  resource_state_dir="$tmp/resource-state"
  mkdir "$resource_state_dir"
  STATE_DIR="$resource_state_dir"
  write_state m2.resource_stage m2-qualification
  write_state m2.resource_candidate candidate-distinct-provider
  write_state m2.resource_profile_sha256 \
    "$(m2_resource_json_value "$resource_copy_dir/eligibility.json" \
      candidate_profile_sha256)"
  write_state m2.resource_result_sha256 \
    "$(sha256_file "$resource_copy_dir/result.txt")"
  m2_resource_state_binding_is_valid m2-qualification \
    candidate-distinct-provider \
    "$(m2_resource_json_value "$resource_copy_dir/eligibility.json" \
      candidate_profile_sha256)" \
    "$(sha256_file "$resource_copy_dir/result.txt")" || \
    die "self-test: exact resource state binding rejected"
  write_state m2.resource_candidate mutated-candidate
  ! m2_resource_state_binding_is_valid m2-qualification \
    candidate-distinct-provider \
    "$(m2_resource_json_value "$resource_copy_dir/eligibility.json" \
      candidate_profile_sha256)" \
    "$(sha256_file "$resource_copy_dir/result.txt")" || \
    die "self-test: changed resource state binding accepted"
  STATE_DIR="$resource_original_state_dir"
  printf '%s\n' mutation >>"$resource_copy_dir/direct-manifest.txt"
  ! m2_resource_archive_matches_directory \
    "$resource_copy_dir" "${resource_copy_dir}.tar.gz" || \
    die "self-test: resource directory/archive mismatch accepted"
  rm -rf "$resource_source_dir" "$resource_archive" \
    "${resource_archive}.sha256"
  printf '%s\n' mutation >>"$resource_evidence/direct-manifest.txt"
  ! m2_resource_evidence_is_valid "$resource_evidence" \
    "$resource_source" "$resource_binary_sha" "$resource_direct" \
    "$resource_observer_sha" 1.1.1.1 8443 43.130.32.77 5201 en0 || \
    die "self-test: mutated resource evidence accepted"
  ! validate_run_dir_path /tmp/other || die "self-test: unrelated run directory accepted"
  ! validate_run_dir_path /tmp/mini_vpn_knife15_macos_../victim || \
    die "self-test: traversal run directory accepted"
  cat >"$service_fixture" <<'EOF_NETWORK_SERVICES'
An asterisk (*) denotes that a network service is disabled.
(1) USB 10/100/1000 LAN
(Hardware Port: USB 10/100/1000 LAN, Device: en5)
(2) Wi-Fi
(Hardware Port: Wi-Fi, Device: en0)
EOF_NETWORK_SERVICES
  [[ "$(network_service_for_interface_from_text en0 <"$service_fixture")" == \
    "Wi-Fi" ]] || die "self-test: physical network service parser mismatch"
  ! network_service_for_interface_from_text en9 <"$service_fixture" >/dev/null || \
    die "self-test: missing physical network service was accepted"
  printf '%s\n' '8.8.8.8' '1.1.1.1' >"$dns_fixture"
  [[ "$(normalized_dns_snapshot_from_text <"$dns_fixture")" == \
    $'8.8.8.8\n1.1.1.1' ]] || die "self-test: DNS snapshot parser mismatch"
  printf '%s\n' "There aren't any DNS Servers set on Wi-Fi." >"$dns_fixture"
  [[ "$(normalized_dns_snapshot_from_text <"$dns_fixture")" == "EMPTY" ]] || \
    die "self-test: empty DNS snapshot parser mismatch"
  printf '%s\n' '8.8.8.8; injected' >"$dns_fixture"
  ! normalized_dns_snapshot_from_text <"$dns_fixture" >/dev/null || \
    die "self-test: unsafe DNS snapshot was accepted"
  ipv4_is_fake 198.18.0.2 || die "self-test: valid fake IPv4 rejected"
  ipv4_is_fake 198.19.255.254 || die "self-test: upper fake IPv4 rejected"
  ! ipv4_is_fake 198.20.0.1 || die "self-test: non-fake IPv4 accepted"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'interface: lo0' | \
      m2_ipv6_route_classification_from_text 0
  )"
  [[ "$ipv6_class" == "safe_tunnel" && "$ipv6_interface" == "lo0" ]] || \
    die "self-test: loopback IPv6 route classification mismatch"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'interface: utun42' | \
      m2_ipv6_route_classification_from_text 0
  )"
  [[ "$ipv6_class" == "safe_tunnel" && "$ipv6_interface" == "utun42" ]] || \
    die "self-test: tunnel IPv6 route classification mismatch"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'interface: en0' | \
      m2_ipv6_route_classification_from_text 0
  )"
  [[ "$ipv6_class" == "unsafe_physical" && "$ipv6_interface" == "en0" ]] || \
    die "self-test: physical IPv6 route classification mismatch"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'route: writing to routing socket: not in table' | \
      m2_ipv6_route_classification_from_text 1
  )"
  [[ "$ipv6_class" == "safe_absent" && "$ipv6_interface" == "none" ]] || \
    die "self-test: absent IPv6 route classification mismatch"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'route: writing to routing socket: not in table' | \
      m2_ipv6_route_classification_from_text 0
  )"
  [[ "$ipv6_class" == "safe_absent" && "$ipv6_interface" == "none" ]] || \
    die "self-test: status-zero absent IPv6 route classification mismatch"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' \
      'route: writing to routing socket: not in table' \
      'interface: en0' | \
      m2_ipv6_route_classification_from_text 0
  )"
  [[ "$ipv6_class" == "unsafe_physical" && "$ipv6_interface" == "en0" ]] || \
    die "self-test: ambiguous absent/physical IPv6 output was accepted"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'route: Network is unreachable' | \
      m2_ipv6_route_classification_from_text 1
  )"
  [[ "$ipv6_class" == "unknown" && "$ipv6_interface" == "none" ]] || \
    die "self-test: unknown IPv6 error was accepted"
  read -r ipv6_class ipv6_interface <<<"$(
    printf '%s\n' 'gateway: fe80::1' | \
      m2_ipv6_route_classification_from_text 0
  )"
  [[ "$ipv6_class" == "unknown" && "$ipv6_interface" == "none" ]] || \
    die "self-test: successful IPv6 lookup without interface was accepted"
  cleanup_class="$(m2_owned_route_cleanup_classification \
    utun42 '' utun42 en0 192.168.50.1 1)"
  [[ "$cleanup_class" == "delete_owned" ]] || \
    die "self-test: live owned M2 route was not selected for deletion"
  cleanup_class="$(m2_owned_route_cleanup_classification \
    en0 192.168.50.1 utun42 en0 192.168.50.1 0)"
  [[ "$cleanup_class" == "release_kernel_reaped" ]] || \
    die "self-test: exact kernel-reaped M2 route was not releasable"
  cleanup_class="$(m2_owned_route_cleanup_classification \
    en0 192.168.50.1 utun42 en0 192.168.50.1 1)"
  [[ "$cleanup_class" == "mismatch" ]] || \
    die "self-test: live-utun external M2 route mutation was accepted"
  cleanup_class="$(m2_owned_route_cleanup_classification \
    utun99 '' utun42 en0 192.168.50.1 0)"
  [[ "$cleanup_class" == "mismatch" ]] || \
    die "self-test: foreign tunnel M2 route mutation was accepted"
  cleanup_class="$(m2_owned_route_cleanup_classification \
    en0 192.168.60.1 utun42 en0 192.168.50.1 0)"
  [[ "$cleanup_class" == "mismatch" ]] || \
    die "self-test: wrong-gateway M2 route mutation was accepted"

  m2_route_state="$tmp/m2-route-state"
  m2_run="$tmp/m2-route-run"
  mkdir "$m2_route_state" "$m2_run" "$tmp/m2-state" \
    "$m2_run/m2-real-client"
  printf 'timestamp\tevent\n' >"$m2_run/events.tsv"
  : >"$m2_run/route.log"
  : >"$m2_run/cleanup.log"
  printf '%s\n' 9.9.9.9 >"$m2_route_state/dns-current"
  : >"$m2_route_state/commands.log"
  m2_route_bin="$tmp/m2-route"
  m2_ifconfig_bin="$tmp/m2-ifconfig"
  m2_networksetup_bin="$tmp/m2-networksetup"
  m2_dscacheutil_bin="$tmp/m2-dscacheutil"
  m2_curl_bin="$tmp/m2-curl"
  cat >"$m2_route_bin" <<'EOF_M2_FAKE_ROUTE'
#!/usr/bin/env bash
set -u
printf 'route' >>"$M2_TEST_DIR/commands.log"
printf ' %s' "$@" >>"$M2_TEST_DIR/commands.log"
printf '\n' >>"$M2_TEST_DIR/commands.log"
action="${2:-}"
if [[ "$action" == "get" ]]; then
  if [[ "${3:-}" == "-inet6" ]]; then
    if [[ -n "${M2_TEST_IPV6_ERROR:-}" ]]; then
      printf '%s\n' "$M2_TEST_IPV6_ERROR" >&2
      exit 2
    elif [[ -n "${M2_TEST_IPV6_IF:-}" ]]; then
      printf 'interface: %s\n' "$M2_TEST_IPV6_IF"
      exit 0
    fi
    printf '%s\n' 'route: writing to routing socket: not in table' >&2
    exit 1
  fi
  target="${3:-}"
  case "$target" in
    43.153.32.33)
      printf '%s\n' 'gateway: 192.168.50.1' 'interface: en0'
      ;;
    43.130.32.77|8.8.8.8)
      printf 'interface: utun42\n'
      ;;
    1.1.1.1)
      [[ -f "$M2_TEST_DIR/route_low" ]] && printf 'interface: utun42\n' || \
        printf '%s\n' 'gateway: 192.168.50.1' 'interface: en0'
      ;;
    129.1.1.1)
      [[ -f "$M2_TEST_DIR/route_high" ]] && printf 'interface: utun42\n' || \
        printf '%s\n' 'gateway: 192.168.50.1' 'interface: en0'
      ;;
    198.18.0.1)
      [[ -f "$M2_TEST_DIR/route_fake" ]] && printf 'interface: utun42\n' || \
        printf '%s\n' 'gateway: 192.168.50.1' 'interface: en0'
      ;;
    *)
      printf '%s\n' 'gateway: 192.168.50.1' 'interface: en0'
      ;;
  esac
  exit 0
fi
kind="${3:-}"
destination="${4:-}"
if [[ "$action" == "add" && "$destination" == "${M2_TEST_FAIL_ROUTE:-}" ]]; then
  exit 1
fi
case "$kind:$destination" in
  -host:43.153.32.33) marker=route_exit ;;
  -net:0.0.0.0/1) marker=route_low ;;
  -net:128.0.0.0/1) marker=route_high ;;
  -net:198.18.0.0/15) marker=route_fake ;;
  *) exit 2 ;;
esac
if [[ "$action" == "add" ]]; then
  : >"$M2_TEST_DIR/$marker"
elif [[ "$action" == "delete" ]]; then
  rm -f "$M2_TEST_DIR/$marker"
else
  exit 2
fi
EOF_M2_FAKE_ROUTE
  cat >"$m2_ifconfig_bin" <<'EOF_M2_FAKE_IFCONFIG'
#!/usr/bin/env bash
set -u
[[ "${1:-}" == "utun42" && -f "$M2_TEST_DIR/utun_available" ]]
EOF_M2_FAKE_IFCONFIG
  cat >"$m2_networksetup_bin" <<'EOF_M2_FAKE_NETWORKSETUP'
#!/usr/bin/env bash
set -u
printf 'networksetup' >>"$M2_TEST_DIR/commands.log"
printf ' %s' "$@" >>"$M2_TEST_DIR/commands.log"
printf '\n' >>"$M2_TEST_DIR/commands.log"
case "${1:-}" in
  -listnetworkserviceorder)
    printf '%s\n' \
      'An asterisk (*) denotes that a network service is disabled.' \
      '(1) Wi-Fi' \
      '(Hardware Port: Wi-Fi, Device: en0)'
    ;;
  -getdnsservers)
    if [[ "$(sed -n '1p' "$M2_TEST_DIR/dns-current")" == "EMPTY" ]]; then
      printf "There aren't any DNS Servers set on %s.\n" "${2:-}"
    else
      sed -n '1,$p' "$M2_TEST_DIR/dns-current"
    fi
    ;;
  -setdnsservers)
    shift 2
    if [[ "${1:-}" == "Empty" ]]; then
      printf '%s\n' EMPTY >"$M2_TEST_DIR/dns-current"
    else
      : >"$M2_TEST_DIR/dns-current"
      printf '%s\n' "$@" >>"$M2_TEST_DIR/dns-current"
    fi
    ;;
  *)
    exit 2
    ;;
esac
EOF_M2_FAKE_NETWORKSETUP
  cat >"$m2_dscacheutil_bin" <<'EOF_M2_FAKE_DSCACHEUTIL'
#!/usr/bin/env bash
printf 'dscacheutil' >>"$M2_TEST_DIR/commands.log"
printf ' %s' "$@" >>"$M2_TEST_DIR/commands.log"
printf '\n' >>"$M2_TEST_DIR/commands.log"
EOF_M2_FAKE_DSCACHEUTIL
  cat >"$m2_curl_bin" <<'EOF_M2_FAKE_CURL'
#!/usr/bin/env bash
set -u
[[ -z "${http_proxy:-}${https_proxy:-}${all_proxy:-}${no_proxy:-}" ]] || exit 80
[[ -z "${HTTP_PROXY:-}${HTTPS_PROXY:-}${ALL_PROXY:-}${NO_PROXY:-}" ]] || exit 81
printf 'curl' >>"$M2_TEST_DIR/commands.log"
printf ' %s' "$@" >>"$M2_TEST_DIR/commands.log"
printf '\n' >>"$M2_TEST_DIR/commands.log"
output=
url=
while (($# > 0)); do
  case "$1" in
    --resolve|--proxy|-x)
      exit 82
      ;;
    --output)
      output="$2"
      shift 2
      ;;
    --write-out|--proto|--connect-timeout|--max-time|--max-redirs)
      shift 2
      ;;
    --*)
      shift
      ;;
    *)
      url="$1"
      shift
      ;;
  esac
done
[[ -n "$output" && -n "$url" ]] || exit 83
case "$url" in
  https://api.ipify.org)
    printf '%s\n' "${M2_TEST_EGRESS_VALUE:-43.153.32.33}" >"$output"
    ;;
  https://example.com/)
    printf '%s\n' '<html>ok</html>' >"$output"
    ;;
  *)
    exit 84
    ;;
esac
printf '%s\n' \
  "remote_ip=${M2_TEST_REMOTE_IP:-198.18.0.2}" \
  'http_code=200' \
  'size_download=16'
EOF_M2_FAKE_CURL
  chmod +x "$m2_route_bin" "$m2_ifconfig_bin" "$m2_networksetup_bin" \
    "$m2_dscacheutil_bin" "$m2_curl_bin"
  original_state_dir="$STATE_DIR"
  original_m2_route_bin="$M2_ROUTE_BIN"
  original_m2_ifconfig_bin="$M2_IFCONFIG_BIN"
  original_m2_networksetup_bin="$M2_NETWORKSETUP_BIN"
  original_m2_dscacheutil_bin="$M2_DSCACHEUTIL_BIN"
  original_m2_curl_bin="$M2_CURL_BIN"
  STATE_DIR="$tmp/m2-state"
  M2_ROUTE_BIN="$m2_route_bin"
  M2_IFCONFIG_BIN="$m2_ifconfig_bin"
  M2_NETWORKSETUP_BIN="$m2_networksetup_bin"
  M2_DSCACHEUTIL_BIN="$m2_dscacheutil_bin"
  M2_CURL_BIN="$m2_curl_bin"
  export M2_TEST_DIR="$m2_route_state"
  write_start_network_state \
    43.130.32.77 8.8.8.8 43.153.32.33 8443 5201
  [[ "$(read_state server_port)" == "8443" ]] || \
    die "self-test: start state did not preserve the TUIC server port"
  write_state utun utun42
  M2_TEST_IPV6_ERROR='route: invalid option'
  export M2_TEST_IPV6_ERROR
  ! m2_ipv6_route_is_safe || \
    die "self-test: unknown IPv6 route failure was accepted as no route"
  unset M2_TEST_IPV6_ERROR
  m2_ipv6_evidence="$m2_run/m2-ipv6-preflight.txt"
  M2_TEST_IPV6_IF=en0
  export M2_TEST_IPV6_IF
  ! write_m2_ipv6_route_evidence "$m2_ipv6_evidence" || \
    die "self-test: physical IPv6 evidence was accepted"
  grep -Fqx 'classification=unsafe_physical' "$m2_ipv6_evidence" || \
    die "self-test: physical IPv6 evidence missed its classification"
  grep -Fqx 'interface=en0' "$m2_ipv6_evidence" || \
    die "self-test: physical IPv6 evidence missed its interface"
  grep -Fqx 'route_status=0' "$m2_ipv6_evidence" || \
    die "self-test: physical IPv6 evidence missed route status"
  grep -Fq 'interface: en0' "$m2_ipv6_evidence" || \
    die "self-test: physical IPv6 evidence missed raw route text"
  unset M2_TEST_IPV6_IF
  write_m2_ipv6_route_evidence "$m2_ipv6_evidence" || \
    die "self-test: absent IPv6 evidence was rejected"
  grep -Fqx 'classification=safe_absent' "$m2_ipv6_evidence" || \
    die "self-test: absent IPv6 evidence missed its classification"
  activate_m2_full_tunnel "$m2_run" || \
    die "self-test: valid M2 full-tunnel activation failed"
  m2_full_tunnel_is_active || \
    die "self-test: active M2 full-tunnel ownership was rejected"
  [[ "$(sed -n '1p' "$m2_route_state/dns-current")" == "8.8.8.8" ]] || \
    die "self-test: M2 did not own the active DNS service"
  for marker in route_exit route_low route_high route_fake; do
    [[ -f "$m2_route_state/$marker" ]] || \
      die "self-test: M2 route activation missed $marker"
  done
  write_state sample_secs 30
  write_state network.valid.epoch "$(date +%s)"
  HTTP_PROXY=http://poison.invalid
  HTTPS_PROXY=http://poison.invalid
  ALL_PROXY=http://poison.invalid
  export HTTP_PROXY HTTPS_PROXY ALL_PROXY
  SOAK_STAGE=m2
  run_m2_real_client_probe "$m2_run" preflight || \
    die "self-test: valid M2 real-client probe failed"
  m2_result="$m2_run/m2-real-client/preflight.txt"
  m2_real_client_result_is_valid "$m2_result" || \
    die "self-test: valid M2 real-client evidence rejected"
  cp "$m2_result" "$m2_result.valid"
  sed -i '' 's/^public_low_route=utun42$/public_low_route=en0/' "$m2_result"
  ! m2_real_client_result_is_valid "$m2_result" || \
    die "self-test: M2 real-client result with a physical public route was accepted"
  mv "$m2_result.valid" "$m2_result"
  cp "$m2_result" "$m2_result.valid"
  sed -i '' 's/^label=preflight$/label=cycle_001/' "$m2_result"
  ! m2_real_client_result_is_valid "$m2_result" || \
    die "self-test: mislabeled M2 real-client evidence was accepted"
  mv "$m2_result.valid" "$m2_result"
  grep -Fq 'curl --silent --show-error' "$m2_route_state/commands.log" || \
    die "self-test: M2 real-client curl seam was not exercised"
  ! grep -Fq -- '--resolve' "$m2_route_state/commands.log" || \
    die "self-test: M2 real-client probe bypassed the system resolver"
  M2_TEST_EGRESS_VALUE=43.153.32.34
  export M2_TEST_EGRESS_VALUE
  if run_m2_real_client_probe "$m2_run" wrong-egress; then
    die "self-test: wrong M2 public egress identity was accepted"
  fi
  unset M2_TEST_EGRESS_VALUE HTTP_PROXY HTTPS_PROXY ALL_PROXY
  deactivate_m2_full_tunnel "$m2_run" || \
    die "self-test: valid M2 full-tunnel cleanup failed"
  [[ "$(sed -n '1p' "$m2_route_state/dns-current")" == "9.9.9.9" ]] || \
    die "self-test: M2 did not restore the prior DNS service"
  for marker in route_exit route_low route_high route_fake; do
    [[ ! -e "$m2_route_state/$marker" ]] || \
      die "self-test: M2 route cleanup retained $marker"
  done
  deactivate_m2_full_tunnel "$m2_run" || \
    die "self-test: repeated M2 full-tunnel cleanup was not idempotent"

  : >"$m2_route_state/commands.log"
  activate_m2_full_tunnel "$m2_run" || \
    die "self-test: M2 kernel-reap cleanup fixture activation failed"
  rm -f "$m2_route_state/route_low" "$m2_route_state/route_high" \
    "$m2_route_state/route_fake"
  deactivate_m2_full_tunnel "$m2_run" || \
    die "self-test: kernel-reaped M2 interface routes were not released"
  [[ "$(read_state m2.full_tunnel)" == "inactive" ]] || \
    die "self-test: kernel-reaped M2 cleanup did not become inactive"
  for marker in low high fake; do
    [[ "$(read_state "m2.${marker}_route_owned")" == "0" ]] || \
      die "self-test: kernel-reaped M2 cleanup retained $marker ownership"
    grep -Fq "release kernel-reaped $marker route:" "$m2_run/cleanup.log" || \
      die "self-test: kernel-reaped M2 cleanup missed $marker evidence"
  done
  ! grep -Fq 'route -n delete -net' "$m2_route_state/commands.log" || \
    die "self-test: kernel-reaped M2 cleanup deleted a non-owned route"

  rm -f "$m2_route_state"/route_*
  printf '%s\n' 9.9.9.9 >"$m2_route_state/dns-current"
  : >"$m2_route_state/commands.log"
  export M2_TEST_FAIL_ROUTE=128.0.0.0/1
  if activate_m2_full_tunnel "$m2_run"; then
    die "self-test: partial M2 route activation unexpectedly passed"
  fi
  unset M2_TEST_FAIL_ROUTE
  for marker in route_exit route_low route_high route_fake; do
    [[ ! -e "$m2_route_state/$marker" ]] || \
      die "self-test: partial M2 activation stranded $marker"
  done
  [[ "$(sed -n '1p' "$m2_route_state/dns-current")" == "9.9.9.9" ]] || \
    die "self-test: partial M2 activation changed DNS"
  STATE_DIR="$original_state_dir"
  M2_ROUTE_BIN="$original_m2_route_bin"
  M2_IFCONFIG_BIN="$original_m2_ifconfig_bin"
  M2_NETWORKSETUP_BIN="$original_m2_networksetup_bin"
  M2_DSCACHEUTIL_BIN="$original_m2_dscacheutil_bin"
  M2_CURL_BIN="$original_m2_curl_bin"
  SOAK_STAGE=m0
  SOAK_LABEL=M0
  unset M2_TEST_DIR
  validate_baseline_dir_path /tmp/mini_vpn_knife15_macos_baseline_20260714_010203 || \
    die "self-test: valid baseline directory rejected"
  ! validate_baseline_dir_path /tmp/mini_vpn_knife15_macos_baseline_../victim || \
    die "self-test: traversal baseline directory accepted"
  M0_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m0
  M1_BASELINE_DIR=
  [[ "$(selected_direct_baseline_dir)" == "$M0_BASELINE_DIR" ]] || \
    die "self-test: direct discriminator did not select M0 baseline"
  M0_BASELINE_DIR=
  M1_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m1
  [[ "$(selected_direct_baseline_dir)" == "$M1_BASELINE_DIR" ]] || \
    die "self-test: direct discriminator did not select M1 baseline"
  M1_BASELINE_DIR=
  M2_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m2
  [[ "$(selected_direct_baseline_dir)" == "$M2_BASELINE_DIR" ]] || \
    die "self-test: direct discriminator did not select M2 baseline"
  selected_direct_epoch_is_frozen "$M2_BASELINE_DIR" || \
    die "self-test: frozen M2 direct epoch was rejected"
  M2_TCP_SECS=299
  ! selected_direct_epoch_is_frozen "$M2_BASELINE_DIR" || \
    die "self-test: shortened M2 direct epoch was accepted"
  M2_TCP_SECS=300
  M1_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m1
  ! selected_direct_baseline_dir >/dev/null || \
    die "self-test: ambiguous M1/M2 direct baseline selection was accepted"
  M1_BASELINE_DIR=
  M2_BASELINE_DIR=
  M0_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m0
  M1_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m1
  ! selected_direct_baseline_dir >/dev/null || \
    die "self-test: ambiguous M0/M1 direct baseline selection was accepted"
  M0_BASELINE_DIR=
  M1_BASELINE_DIR=
  M2_BASELINE_DIR=
  ! selected_direct_baseline_dir >/dev/null || \
    die "self-test: missing direct baseline selection was accepted"
  parse_server 43.173.101.111:8443 || die "self-test: valid server rejected"
  [[ "$SERVER_HOST" == "43.173.101.111" && "$SERVER_PORT" == "8443" ]] || \
    die "self-test: server parsing mismatch"
  ! parse_server example.com:8443 || die "self-test: first-gate hostname accepted"
  [[ "$(safe_event_label $'wake\tafter\nidle')" == "wake after idle" ]] || \
    die "self-test: event sanitization mismatch"
  pid_command_matches /tmp/mini_vpn '/tmp/mini_vpn client-tun' || \
    die "self-test: exact mini_vpn command rejected"
  ! pid_command_matches /tmp/mini_vpn '/usr/bin/sleep 30' || \
    die "self-test: unrelated PID command accepted"
  ! pid_command_matches /tmp/mini_vpn '/tmp/mini_vpn client-tun --other' || \
    die "self-test: non-exact mini_vpn command accepted"
  if run_logged_with_timeout "$tmp/bounded-timeout.log" 1 /bin/sleep 30; then
    die "self-test: bounded runner accepted a command that exceeded its deadline"
  else
    bounded_status=$?
  fi
  [[ "$bounded_status" == "124" ]] || \
    die "self-test: bounded runner returned $bounded_status instead of timeout status 124"
  original_state_dir="$STATE_DIR"
  STATE_DIR="$tmp/bounded-child-state"
  mkdir "$STATE_DIR"
  M0_TRACK_CHILD=1
  M0_ENFORCE_RUN_HEALTH=0
  M0_ACTIVE_RUN_DIR="$tmp"
  if run_m0_logged "$tmp/bounded-child.log" 1 /bin/sleep 30 2>/dev/null; then
    die "self-test: soak child runner accepted a command beyond its deadline"
  else
    bounded_status=$?
  fi
  [[ "$bounded_status" == "124" ]] || \
    die "self-test: soak child timeout returned $bounded_status instead of 124"
  [[ ! -e "$(state_file workload.child.pid)" ]] || \
    die "self-test: timed-out soak child identity was not cleared"
  M0_TRACK_CHILD=0
  STATE_DIR="$original_state_dir"
  watchdog_command_matches "bash $SCRIPT_PATH __watchdog" || \
    die "self-test: watchdog command rejected"
  ! watchdog_command_matches '/usr/bin/sleep 30' || \
    die "self-test: unrelated watchdog command accepted"
  workload_command_matches 'bash scripts/knife15-macos-soak.sh m0' || \
    die "self-test: M0 workload command rejected"
  workload_command_matches 'bash scripts/knife15-macos-soak.sh m1' || \
    die "self-test: M1 workload command rejected"
  workload_command_matches 'bash scripts/knife15-macos-soak.sh m1-diagnostic' || \
    die "self-test: M1 diagnostic workload command rejected"
  workload_command_matches 'bash scripts/knife15-macos-soak.sh m2-qualification' || \
    die "self-test: M2 qualification workload command rejected"
  workload_command_matches 'bash scripts/knife15-macos-soak.sh m2' || \
    die "self-test: M2 workload command rejected"
  ! workload_command_matches 'bash scripts/knife15-macos-soak.sh smoke' || \
    die "self-test: non-M0 workload command accepted"

  printf '%s\n' 'ordinary log line' >"$tmp/tcp-pool-activity.log"
  ! tcp_pool_activity_is_idle "$tmp/tcp-pool-activity.log" || \
    die "self-test: missing TCP-pool activity evidence was accepted as idle"
  printf '%s\n' \
    '🔎 tuic-tcp-pool-activity active_leases=4' \
    '🔎 tuic-tcp-pool-activity active_leases=2' \
    >>"$tmp/tcp-pool-activity.log"
  ! tcp_pool_activity_is_idle "$tmp/tcp-pool-activity.log" || \
    die "self-test: live TCP-pool ownership was accepted as idle"
  printf '%s\n' '🔎 tuic-tcp-pool-activity active_leases=0' \
    >>"$tmp/tcp-pool-activity.log"
  tcp_pool_activity_is_idle "$tmp/tcp-pool-activity.log" || \
    die "self-test: exact zero TCP-pool ownership was rejected"
  printf '%s\n' '🔎 tuic-tcp-pool-activity active_leases=unknown' \
    >"$tmp/tcp-pool-activity.log"
  ! tcp_pool_activity_is_idle "$tmp/tcp-pool-activity.log" || \
    die "self-test: malformed TCP-pool activity evidence was accepted as idle"

  baseline_dir="$tmp/baseline"
  mkdir "$baseline_dir"
  printf '%s\n' '{"start":{"connecting_to":{"host":"43.130.32.77"},"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bits_per_second":9044757.9}},"server_output_json":{"start":{"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bytes":100,"bits_per_second":9044757.9}}}}' \
    >"$baseline_dir/direct-forward.json"
  [[ "$(baseline_receiver_bps "$baseline_dir/direct-forward.json")" == "9044757" ]] || \
    die "self-test: baseline receiver rate parser mismatch"
  printf '%s\n' '{"end":{"sum_received":{"bits_per_second":0}}}' \
    >"$baseline_dir/invalid.json"
  ! baseline_receiver_bps "$baseline_dir/invalid.json" >/dev/null 2>&1 || \
    die "self-test: zero baseline receiver rate accepted"
  printf '%s\n' '{"start":{"connecting_to":{"host":"43.130.32.77"},"test_start":{"protocol":"TCP","reverse":1}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bytes":100,"bits_per_second":26790141.6}},"server_output_json":{"start":{"test_start":{"protocol":"TCP","reverse":1}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bytes":100,"bits_per_second":26790141.6}}}}' \
    >"$baseline_dir/direct-reverse.json"
  validate_m0_baseline_pair "$baseline_dir" 43.130.32.77 || \
    die "self-test: valid M0 baseline pair rejected"
  [[ "$(baseline_pair_validation_reasons \
    "$baseline_dir" 43.130.32.77)" == "ok ok" ]] || \
    die "self-test: valid baseline pair reasons were not ok/ok"
  printf '%s\n' '{' >"$baseline_dir/malformed.json"
  [[ "$(baseline_file_validation_reason \
    "$baseline_dir/malformed.json" 43.130.32.77 0 2>/dev/null)" == \
    "validator_error_rc_5" ]] || \
    die "self-test: malformed baseline JSON was not a validator error"
  [[ "$(baseline_file_validation_reason \
    "$baseline_dir/missing.json" 43.130.32.77 0)" == \
    "missing_or_symlink" ]] || \
    die "self-test: missing baseline evidence was not classified separately"
  ln -s "$baseline_dir/direct-forward.json" "$baseline_dir/symlink.json"
  [[ "$(baseline_file_validation_reason \
    "$baseline_dir/symlink.json" 43.130.32.77 0)" == \
    "missing_or_symlink" ]] || \
    die "self-test: symlink baseline evidence was not rejected separately"
  baseline_check_text="$(baseline_check_report \
    "$baseline_dir" 43.130.32.77)" || \
    die "self-test: valid baseline replay report failed"
  grep -Fq 'schema=knife15-macos-baseline-check-v1' \
    <<<"$baseline_check_text" || \
    die "self-test: baseline replay schema missing"
  grep -Fq 'status=pass' <<<"$baseline_check_text" || \
    die "self-test: valid baseline replay did not pass"
  grep -Fq 'forward_validation=ok' <<<"$baseline_check_text" || \
    die "self-test: forward baseline replay reason missing"
  grep -Fq 'reverse_validation=ok' <<<"$baseline_check_text" || \
    die "self-test: reverse baseline replay reason missing"
  write_baseline_manifest "$baseline_dir" pass ok ok ok
  [[ "$(m0_profile_value "$baseline_dir/manifest.txt" schema)" == \
    "knife15-macos-baseline-v1" ]] || \
    die "self-test: baseline manifest schema mismatch"
  [[ "$(m0_profile_value "$baseline_dir/manifest.txt" status)" == "pass" && \
    "$(m0_profile_value "$baseline_dir/manifest.txt" reason)" == "ok" ]] || \
    die "self-test: baseline manifest terminal result mismatch"
  [[ "$(m0_profile_value "$baseline_dir/manifest.txt" forward_validation)" == \
    "ok" && \
    "$(m0_profile_value "$baseline_dir/manifest.txt" reverse_validation)" == \
    "ok" ]] || die "self-test: baseline manifest validation reasons missing"
  baseline_summary_text="$(baseline_receiver_summary "$baseline_dir")" || \
    die "self-test: valid M0 baseline pair could not be summarized"
  [[ "$baseline_summary_text" == \
    'BASELINE receiver: forward_mbit=9.045 reverse_mbit=26.790 forward_zero_intervals=0 reverse_zero_intervals=0' ]] || \
    die "self-test: baseline receiver speed/continuity summary mismatch"
  jq '.start.test_start.duration = 20
    | .server_output_json.start.test_start.duration = 20
    | .server_output_json.intervals[0].sum.start = 0
    | .server_output_json.intervals[0].sum.end = 20
    | .server_output_json.intervals += [
        {"sum":{"start":20.001,"end":20.165,"bits_per_second":0}}]' \
    "$baseline_dir/direct-forward.json" \
    >"$baseline_dir/direct-forward.partial-tail.json"
  mv "$baseline_dir/direct-forward.json" \
    "$baseline_dir/direct-forward.without-tail.json"
  mv "$baseline_dir/direct-forward.partial-tail.json" \
    "$baseline_dir/direct-forward.json"
  validate_m0_baseline_pair "$baseline_dir" 43.130.32.77 || \
    die "self-test: baseline with a zero short receiver command tail was rejected"
  [[ "$(baseline_file_validation_reason \
    "$baseline_dir/direct-forward.json" 43.130.32.77 0)" == "ok" ]] || \
    die "self-test: proven partial tail did not classify ok"
  baseline_summary_text="$(baseline_receiver_summary "$baseline_dir")" || \
    die "self-test: partial-tail baseline summary failed"
  [[ "$baseline_summary_text" == \
    'BASELINE receiver: forward_mbit=9.045 reverse_mbit=26.790 forward_zero_intervals=0 reverse_zero_intervals=0' ]] || \
    die "self-test: partial-tail baseline summary counted a complete receiver stall"
  mv "$baseline_dir/direct-forward.json" \
    "$baseline_dir/direct-forward.partial-tail.json"
  mv "$baseline_dir/direct-forward.without-tail.json" \
    "$baseline_dir/direct-forward.json"
  jq '.server_output_json.intervals[0].sum.bits_per_second = 0' \
    "$baseline_dir/direct-forward.json" >"$baseline_dir/direct-forward.stalled.json"
  mv "$baseline_dir/direct-forward.json" "$baseline_dir/direct-forward.valid.json"
  mv "$baseline_dir/direct-forward.stalled.json" "$baseline_dir/direct-forward.json"
  ! validate_m0_baseline_pair "$baseline_dir" 43.130.32.77 || \
    die "self-test: forward baseline with a Target receiver zero interval was accepted"
  [[ "$(baseline_file_validation_reason \
    "$baseline_dir/direct-forward.json" 43.130.32.77 0)" == \
    "invalid_evidence" ]] || \
    die "self-test: complete receiver zero was not invalid evidence"
  mv "$baseline_dir/direct-forward.valid.json" "$baseline_dir/direct-forward.json"
  ! validate_m0_baseline_pair "$baseline_dir" 43.130.32.78 || \
    die "self-test: M0 baseline for another target accepted"
  target_ready_json="$tmp/target-ready.json"
  printf '%s\n' '{"start":{"connecting_to":{"host":"43.130.32.77"},"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_sent":{"bytes":100},"sum_received":{"bytes":100,"bits_per_second":1}},"server_output_json":{"start":{"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bytes":100,"bits_per_second":1}}}}' \
    >"$target_ready_json"
  validate_target_ready_result "$target_ready_json" 43.130.32.77 || \
    die "self-test: healthy Target readiness transaction rejected"
  jq 'del(.server_output_json)' "$target_ready_json" >"$target_ready_json.without-server"
  ! validate_target_ready_result "$target_ready_json.without-server" 43.130.32.77 || \
    die "self-test: Target without JSON receiver evidence was accepted"
  printf '%s\n' '{"error":"the server is busy running a test. try again later"}' \
    >"$target_ready_json"
  ! validate_target_ready_result "$target_ready_json" 43.130.32.77 || \
    die "self-test: busy Target readiness transaction accepted"
  dns_result="$tmp/dns-result.txt"
  printf '%s\n' 'example.com. 60 IN A 198.18.0.2' >"$dns_result"
  validate_m0_dns_result "$dns_result" || die "self-test: fake-IP DNS result rejected"
  printf '%s\n' 'example.com. 60 IN A 93.184.216.34' >"$dns_result"
  ! validate_m0_dns_result "$dns_result" || \
    die "self-test: non-fake-IP DNS result accepted"
  sed 's/"reverse":1/"reverse":0/' "$baseline_dir/direct-reverse.json" \
    >"$baseline_dir/wrong-reverse.json"
  mv "$baseline_dir/direct-reverse.json" "$baseline_dir/direct-reverse.valid.json"
  mv "$baseline_dir/wrong-reverse.json" "$baseline_dir/direct-reverse.json"
  ! validate_m0_baseline_pair "$baseline_dir" 43.130.32.77 || \
    die "self-test: M0 baseline with wrong reverse direction accepted"
  mv "$baseline_dir/direct-reverse.valid.json" "$baseline_dir/direct-reverse.json"
  M0_TOTAL_SECS=19
  M0_TCP_SECS=2
  M0_UDP_SECS=1
  M0_SHORT_SECS=1
  M0_SHORT_COUNT=2
  M0_IDLE_SECS=2
  M0_FINAL_DRAIN_SECS=1
  m0_profile="$tmp/m0-workload.txt"
  write_m0_profile "$baseline_dir" "$m0_profile" 43.130.32.77 5201 8.8.8.8 example.com
  grep -Fq 'tcp_forward_bps=4522378' "$m0_profile" || \
    die "self-test: M0 sustained forward rate must be 50% of baseline"
  grep -Fq 'tcp_reverse_bps=13395070' "$m0_profile" || \
    die "self-test: M0 sustained reverse rate must be 50% of baseline"
  grep -Fq 'short_forward_bps=7235805' "$m0_profile" || \
    die "self-test: M0 burst forward rate must be 80% of baseline"
  grep -Fq 'tcp_reverse_iperf_length_bytes=1024' "$m0_profile" || \
    die "self-test: M0 reverse TCP observer granularity missing from profile"
  grep -Fq 'udp_payload_bytes=1160' "$m0_profile" || \
    die "self-test: M0 UDP payload drifted from the MTU1200-safe shape"

  m1_profile="$tmp/m1-workload.txt"
  write_m1_profile "$baseline_dir" "$m1_profile" \
    43.130.32.77 5201 8.8.8.8 example.com || \
    die "self-test: cannot derive M1 workload profile"
  grep -Fq 'schema=knife15-macos-m1-v1' "$m1_profile" || \
    die "self-test: M1 profile schema mismatch"
  grep -Fq 'total_secs=28800' "$m1_profile" || \
    die "self-test: M1 total duration mismatch"
  grep -Fq 'steady_a_secs=7200' "$m1_profile" || \
    die "self-test: M1 first steady window mismatch"
  grep -Fq 'quiet_secs=3600' "$m1_profile" || \
    die "self-test: M1 quiet window mismatch"
  grep -Fq 'steady_c_secs=6000' "$m1_profile" || \
    die "self-test: M1 final steady window mismatch"
  grep -Fq 'steady_tcp_forward_bps=4522378' "$m1_profile" || \
    die "self-test: M1 steady rate must be 50% of baseline"
  grep -Fq 'quiet_tcp_forward_bps=2261189' "$m1_profile" || \
    die "self-test: M1 quiet rate must be 25% of baseline"
  grep -Fq 'steady_short_forward_bps=7235805' "$m1_profile" || \
    die "self-test: M1 short rate must be 80% of baseline"
  grep -Fq 'churn_short_connections_per_cycle=24' "$m1_profile" || \
    die "self-test: M1 churn count mismatch"
  [[ "$(min_rate_bps 240000000)" == "200000000" ]] || \
    die "self-test: M1 workload capacity cap mismatch"
  m2_profile="$tmp/m2-workload.txt"
  write_m2_profile "$baseline_dir" "$m2_profile" \
    43.130.32.77 5201 8.8.8.8 example.com || \
    die "self-test: cannot derive M2 workload profile"
  grep -Fq 'schema=knife15-macos-m2-v1' "$m2_profile" || \
    die "self-test: M2 profile schema mismatch"
  grep -Fq 'total_secs=86400' "$m2_profile" || \
    die "self-test: M2 total duration mismatch"
  grep -Fq 'expected_cycles=93' "$m2_profile" || \
    die "self-test: M2 complete-cycle arithmetic mismatch"
  grep -Fq 'expected_tcp_results=934' "$m2_profile" || \
    die "self-test: M2 TCP result arithmetic mismatch"
  grep -Fq 'expected_udp_results=95' "$m2_profile" || \
    die "self-test: M2 UDP result arithmetic mismatch"
  grep -Fq 'expected_phase_results=1029' "$m2_profile" || \
    die "self-test: M2 phase result arithmetic mismatch"
  grep -Fq 'expected_checkpoints=6' "$m2_profile" || \
    die "self-test: M2 checkpoint arithmetic mismatch"
  grep -Fq 'quiet_a_secs=10800' "$m2_profile" || \
    die "self-test: M2 first quiet window mismatch"
  grep -Fq 'quiet_b_secs=10800' "$m2_profile" || \
    die "self-test: M2 second quiet window mismatch"
  grep -Fq 'steady_c_secs=21600' "$m2_profile" || \
    die "self-test: M2 final steady window mismatch"

  m0_run="$tmp/m0-run"
  fake_iperf="$tmp/fake-iperf3"
  fake_dig="$tmp/fake-dig"
  fake_sleep="$tmp/fake-sleep"
  mkdir -p "$m0_run/m0"
  printf 'timestamp\tevent\n' >"$m0_run/events.tsv"
  cat >"$fake_iperf" <<'EOF_FAKE_IPERF'
#!/usr/bin/env bash
printf 'iperf3' >>"$M0_TEST_COMMAND_LOG"
printf ' %s' "$@" >>"$M0_TEST_COMMAND_LOG"
printf '\n' >>"$M0_TEST_COMMAND_LOG"
if [[ "${M0_TEST_IPERF_FAIL:-0}" == "1" ]]; then
  exit 86
fi
protocol=TCP
reverse=0
duration=1
target=43.130.32.77
while (($# > 0)); do
  case "$1" in
    -c)
      target="$2"
      shift 2
      ;;
    -t)
      duration="$2"
      shift 2
      ;;
    -u)
      protocol=UDP
      shift
      ;;
    -R)
      reverse=1
      shift
      ;;
    *)
      shift
      ;;
  esac
done
  jq -nc --arg target "$target" --arg protocol "$protocol" \
  --argjson reverse "$reverse" --argjson duration "$duration" \
  --argjson receiver_zero "${M0_TEST_IPERF_RECEIVER_ZERO:-0}" \
  --argjson missing_receiver "${M0_TEST_IPERF_MISSING_RECEIVER:-0}" \
  --argjson invalid_sent "${M0_TEST_IPERF_INVALID_SENT:-0}" \
  --argjson udp_loss "${M0_TEST_IPERF_UDP_LOSS:-1.25}" '
  def samples: [range(0; $duration) as $second
    | {sum: {start: $second, end: ($second + 1), bits_per_second: 1}}];
  def receiver_samples: [range(0; $duration) as $second
    | {sum: {start: $second, end: ($second + 1),
        bits_per_second:
          (if $receiver_zero == 1 and $second == 0 then 0 else 1 end)}}];
  {start: {connecting_to: {host: $target},
      test_start: {protocol: $protocol, reverse: $reverse, duration: $duration}},
    intervals: samples,
    end: {sum_sent: {bytes: 100},
      sum_received: {bytes: 90, bits_per_second: 1},
      sum: {lost_percent: $udp_loss}},
    server_output_json: {
      start: {test_start: {protocol: $protocol, reverse: $reverse,
        duration: $duration}},
      intervals: receiver_samples,
      end: {sum_received: {bytes: 90, bits_per_second: 1,
        seconds: $duration}}}}
  | if $missing_receiver == 1 then del(.server_output_json) else . end
  | if $invalid_sent == 1 then del(.end.sum_sent.bytes) else . end
  '
EOF_FAKE_IPERF
  cat >"$fake_dig" <<'EOF_FAKE_DIG'
#!/usr/bin/env bash
printf 'dig' >>"$M0_TEST_COMMAND_LOG"
printf ' %s' "$@" >>"$M0_TEST_COMMAND_LOG"
printf '\n' >>"$M0_TEST_COMMAND_LOG"
printf '%s\n' '198.18.0.2'
EOF_FAKE_DIG
  cat >"$fake_sleep" <<'EOF_FAKE_SLEEP'
#!/usr/bin/env bash
printf 'sleep %s\n' "$1" >>"$M0_TEST_COMMAND_LOG"
EOF_FAKE_SLEEP
  chmod +x "$fake_iperf" "$fake_dig" "$fake_sleep"
  export M0_TEST_COMMAND_LOG="$tmp/m0-commands.log"
  M0_IPERF3_BIN="$fake_iperf"
  M0_DIG_BIN="$fake_dig"
  M0_SLEEP_BIN="$fake_sleep"
  : >"$M0_TEST_COMMAND_LOG"
  run_direct_baseline_probe "$tmp/baseline-forward.json" 43.130.32.77 5201 \
    20 1 0 "$fake_iperf" >/dev/null || \
    die "self-test: direct forward baseline probe failed"
  run_direct_baseline_probe "$tmp/baseline-reverse.json" 43.130.32.77 5201 \
    20 1 1 "$fake_iperf" >/dev/null || \
    die "self-test: direct reverse baseline probe failed"
  grep -Fxq \
    'iperf3 -c 43.130.32.77 -p 5201 -t 20 -P 1 --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: direct forward baseline shape changed"
  grep -Fxq \
    'iperf3 -c 43.130.32.77 -p 5201 -t 20 -P 1 -l 1024 -R --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: direct reverse baseline lacks low-rate receiver granularity"
  : >"$M0_TEST_COMMAND_LOG"
  run_direct_continuity_probe "$tmp/direct-probe.json" 43.130.32.77 5201 \
    300 4522378 "$fake_iperf" || \
    die "self-test: valid direct continuity probe rejected"
  grep -Fxq \
    'iperf3 -c 43.130.32.77 -p 5201 -t 300 -P 1 -b 4522378 --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: direct continuity probe command drifted from M0 forward shape"
  [[ "$DIRECT_CONTINUITY_REASON" == "ok" ]] || \
    die "self-test: direct continuity probe did not report ok"
  : >"$M0_TEST_COMMAND_LOG"
  M0_QUIET=1
  run_m0_schedule "$m0_run" "$m0_profile"
  [[ -f "$m0_run/m0/idle.sleep.log" ]] || \
    die "self-test: M0 idle child was not tracked through the logged runner"
  [[ -f "$m0_run/m0/final-drain.sleep.log" ]] || \
    die "self-test: M0 final-drain child was not tracked through the logged runner"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 2 -P 1 -b 4522378 --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: M0 forward TCP did not request receiver evidence"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 2 -P 1 -b 13395070 -l 1024 -R --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: M0 sustained reverse TCP lacks low-rate receiver granularity"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 1 -P 1 -b 13395070 -u -l 1160 -R --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || die "self-test: M0 reverse UDP command mismatch"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 1 -P 1 -b 7235805 --json --get-server-output' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: M0 short forward burst command mismatch"
  grep -Fq 'dig @8.8.8.8 example.com A +time=5 +tries=1' "$M0_TEST_COMMAND_LOG" || \
    die "self-test: M0 periodic DNS command mismatch"
  for marker in 'm0 idle start' 'm0 idle complete' 'm0 resume complete' \
    'm0 final drain complete' 'm0 complete'; do
    grep -Fq "$marker" "$m0_run/events.tsv" || \
      die "self-test: M0 event timeline missing $marker"
  done
  grep -Fxq complete "$m0_run/m0.status" || \
    die "self-test: successful M0 schedule status mismatch"
  m1_stage_run="$tmp/m1-stage-run"
  mkdir -p "$m1_stage_run/m1"
  printf 'timestamp\tevent\n' >"$m1_stage_run/events.tsv"
  SOAK_STAGE=m1
  SOAK_LABEL=M1
  SOAK_EVIDENCE_DIR=m1
  SOAK_STATUS_FILE=m1.status
  run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 1 \
    tcp-forward 1 4522378 0 0 || \
    die "self-test: stage-aware M1 phase failed"
  [[ -f "$m1_stage_run/m1/cycle_001_tcp-forward.json" ]] || \
    die "self-test: M1 phase evidence was written outside its stage directory"
  grep -Fq $'\tm1 phase complete cycle=1 phase=tcp-forward' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: M1 phase event used the wrong stage"
  SOAK_STAGE=m1-diagnostic
  SOAK_LABEL=M1-DIAGNOSTIC
  SOAK_CONTINUE_DATA_QUALITY=1
  SOAK_VIOLATIONS_FILE="$m1_stage_run/m1-diagnostic-violations.tsv"
  printf '%s\n' $'timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence' \
    >"$SOAK_VIOLATIONS_FILE"
  M0_TEST_IPERF_RECEIVER_ZERO=1
  export M0_TEST_IPERF_RECEIVER_ZERO
  run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 2 \
    tcp-forward 1 4522378 0 0 || \
    die "self-test: diagnostic receiver-zero phase did not continue"
  grep -Fq $'\tm1-diagnostic phase violation cycle=2 phase=tcp-forward kind=receiver_zero_interval' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: diagnostic receiver-zero violation event missing"
  grep -Fq $'\t2\ttcp-forward\treceiver_zero_interval\t1\t' \
    "$SOAK_VIOLATIONS_FILE" || \
    die "self-test: diagnostic receiver-zero violation row missing"
  grep -Fq $'\tm1-diagnostic phase complete cycle=2 phase=tcp-forward' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: diagnostic receiver-zero phase did not complete"
  unset M0_TEST_IPERF_RECEIVER_ZERO
  M0_TEST_IPERF_MISSING_RECEIVER=1
  export M0_TEST_IPERF_MISSING_RECEIVER
  if run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 3 \
    tcp-forward 1 4522378 0 0; then
    die "self-test: diagnostic policy continued missing receiver evidence"
  fi
  grep -Fq $'\tm1-diagnostic phase failed cycle=3 phase=tcp-forward reason=missing_receiver_evidence' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: diagnostic missing-receiver failure reason missing"
  unset M0_TEST_IPERF_MISSING_RECEIVER
  M0_TEST_IPERF_RECEIVER_ZERO=1
  M0_TEST_IPERF_INVALID_SENT=1
  export M0_TEST_IPERF_RECEIVER_ZERO M0_TEST_IPERF_INVALID_SENT
  if run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 4 \
    tcp-forward 1 4522378 0 0; then
    die "self-test: diagnostic receiver-zero policy hid malformed sender evidence"
  fi
  unset M0_TEST_IPERF_RECEIVER_ZERO M0_TEST_IPERF_INVALID_SENT
  violations_before="$(awk 'END { print NR - 1 }' "$SOAK_VIOLATIONS_FILE")"
  M0_TEST_IPERF_UDP_LOSS=3.000001
  export M0_TEST_IPERF_UDP_LOSS
  run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 5 \
    udp-reverse 1 4522378 1 1 || \
    die "self-test: diagnostic high-loss UDP phase did not continue"
  grep -Fq $'\tm1-diagnostic phase violation cycle=5 phase=udp-reverse kind=udp_loss_percent value=3.000001' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: diagnostic UDP loss violation event missing"
  grep -Fq $'\t5\tudp-reverse\tudp_loss_percent\t3.000001\t' \
    "$SOAK_VIOLATIONS_FILE" || \
    die "self-test: diagnostic UDP loss violation row missing"
  M0_TEST_IPERF_UDP_LOSS=3.0
  run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 6 \
    udp-reverse 1 4522378 1 1 || \
    die "self-test: diagnostic boundary-loss UDP phase failed"
  [[ "$(awk 'END { print NR - 1 }' "$SOAK_VIOLATIONS_FILE")" == \
    "$((10#$violations_before + 1))" ]] || \
    die "self-test: UDP loss at the exact 3.0 percent SLO recorded a violation"
  unset M0_TEST_IPERF_UDP_LOSS
  SOAK_CONTINUE_DATA_QUALITY=0
  SOAK_VIOLATIONS_FILE=
  SOAK_STAGE=m1
  SOAK_LABEL=M1
  M0_TEST_IPERF_UDP_LOSS=3.000001
  export M0_TEST_IPERF_UDP_LOSS
  if run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 7 \
    udp-reverse 1 4522378 1 1; then
    die "self-test: formal high-loss UDP phase continued"
  fi
  grep -Fq $'\tm1 phase failed cycle=7 phase=udp-reverse reason=udp_loss_percent value=3.000001 limit=3.0' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: formal UDP loss failure evidence missing"
  ! grep -Fq $'\tm1 phase complete cycle=7 phase=udp-reverse' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: formal high-loss UDP phase was marked complete"
  M0_TEST_IPERF_UDP_LOSS=3.0
  run_m0_iperf_phase "$m1_stage_run" 43.130.32.77 5201 8 \
    udp-reverse 1 4522378 1 1 || \
    die "self-test: formal boundary-loss UDP phase failed"
  grep -Fq $'\tm1 phase complete cycle=8 phase=udp-reverse' \
    "$m1_stage_run/events.tsv" || \
    die "self-test: formal boundary-loss UDP phase did not complete"
  unset M0_TEST_IPERF_UDP_LOSS
  m1_run="$tmp/m1-run"
  m1_test_profile="$tmp/m1-test-workload.txt"
  mkdir -p "$m1_run/m1"
  printf 'timestamp\tevent\n' >"$m1_run/events.tsv"
  cp "$m1_profile" "$m1_test_profile"
  sed -i '' \
    -e 's/^total_secs=.*/total_secs=74/' \
    -e 's/^steady_a_secs=.*/steady_a_secs=10/' \
    -e 's/^idle_secs=.*/idle_secs=1/' \
    -e 's/^quiet_secs=.*/quiet_secs=10/' \
    -e 's/^steady_b_secs=.*/steady_b_secs=10/' \
    -e 's/^churn_secs=.*/churn_secs=30/' \
    -e 's/^steady_c_secs=.*/steady_c_secs=10/' \
    -e 's/^final_drain_secs=.*/final_drain_secs=1/' \
    -e 's/^tcp_epoch_secs=.*/tcp_epoch_secs=1/' \
    -e 's/^udp_epoch_secs=.*/udp_epoch_secs=1/' \
    -e 's/^short_epoch_secs=.*/short_epoch_secs=1/' \
    "$m1_test_profile"
  : >"$M0_TEST_COMMAND_LOG"
  run_m1_schedule "$m1_run" "$m1_test_profile" || \
    die "self-test: compressed M1 schedule failed"
  [[ "$(grep -Fc $'\tm1 active ' "$m1_run/events.tsv")" == "10" ]] || \
    die "self-test: M1 active-window start/complete count mismatch"
  [[ "$(grep -Fc $'\tm1 idle complete ' "$m1_run/events.tsv")" == "3" ]] || \
    die "self-test: M1 idle count mismatch"
  [[ "$(grep -Fc $'\tm1 resume complete ' "$m1_run/events.tsv")" == "3" ]] || \
    die "self-test: M1 resume count mismatch"
  [[ "$(grep -Fc $'\tm1 final drain complete ' "$m1_run/events.tsv")" == "1" ]] || \
    die "self-test: M1 final-drain count mismatch"
  grep -Fq $'\tm1 phase complete cycle=7 phase=short-reverse-24' \
    "$m1_run/events.tsv" || \
    die "self-test: M1 churn window did not complete 24 short connections"
  [[ "$(grep -Ec $'\tm1 phase complete cycle=[0-9]+ phase=short-' \
    "$m1_run/events.tsv")" == "48" ]] || \
    die "self-test: compressed M1 short-connection count mismatch"
  [[ "$(grep -Fc $'\tm1 DNS complete ' "$m1_run/events.tsv")" == "5" ]] || \
    die "self-test: compressed M1 DNS cycle count mismatch"
  grep -Fxq complete "$m1_run/m1.status" || \
    die "self-test: successful M1 schedule status mismatch"
  m1_diagnostic_run="$tmp/m1-diagnostic-run"
  mkdir -p "$m1_diagnostic_run/m1"
  printf 'timestamp\tevent\n' >"$m1_diagnostic_run/events.tsv"
  printf '%s\n' diagnostic >"$m1_diagnostic_run/m1-mode"
  printf '%s\n' $'timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence' \
    >"$m1_diagnostic_run/m1-diagnostic-violations.tsv"
  SOAK_STAGE=m1-diagnostic
  SOAK_LABEL=M1-DIAGNOSTIC
  SOAK_STATUS_FILE=m1.status
  SOAK_SUCCESS_STATUS=diagnostic_timeline_complete
  SOAK_CONTINUE_DATA_QUALITY=1
  SOAK_VIOLATIONS_FILE="$m1_diagnostic_run/m1-diagnostic-violations.tsv"
  M0_TEST_IPERF_RECEIVER_ZERO=1
  M0_TEST_IPERF_UDP_LOSS=3.000001
  export M0_TEST_IPERF_RECEIVER_ZERO M0_TEST_IPERF_UDP_LOSS
  run_m1_schedule "$m1_diagnostic_run" "$m1_test_profile" || \
    die "self-test: compressed diagnostic M1 schedule did not continue"
  grep -Fxq diagnostic_timeline_complete "$m1_diagnostic_run/m1.status" || \
    die "self-test: diagnostic M1 timeline status mismatch"
  grep -Fq $'\tm1-diagnostic final drain complete ' \
    "$m1_diagnostic_run/events.tsv" || \
    die "self-test: diagnostic M1 did not reach final drain"
  [[ "$(grep -Fc $'\tm1-diagnostic phase failed ' \
    "$m1_diagnostic_run/events.tsv")" == "0" ]] || \
    die "self-test: continuable diagnostic observations emitted phase failure"
  read -r violation_count invalid_violations \
    <<<"$(m1_diagnostic_violation_envelope \
      "$m1_diagnostic_run/m1-diagnostic-violations.tsv")"
  ((10#$violation_count > 1 && 10#$invalid_violations == 0)) || \
    die "self-test: diagnostic M1 did not preserve typed violations"
  unset M0_TEST_IPERF_RECEIVER_ZERO M0_TEST_IPERF_UDP_LOSS

  m1_diagnostic_fail_run="$tmp/m1-diagnostic-fail-run"
  mkdir -p "$m1_diagnostic_fail_run/m1"
  printf 'timestamp\tevent\n' >"$m1_diagnostic_fail_run/events.tsv"
  printf '%s\n' $'timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence' \
    >"$m1_diagnostic_fail_run/m1-diagnostic-violations.tsv"
  SOAK_VIOLATIONS_FILE="$m1_diagnostic_fail_run/m1-diagnostic-violations.tsv"
  M0_TEST_IPERF_FAIL=1
  export M0_TEST_IPERF_FAIL
  if run_m1_schedule "$m1_diagnostic_fail_run" "$m1_test_profile"; then
    die "self-test: diagnostic M1 continued an iperf command failure"
  fi
  grep -Fxq failed "$m1_diagnostic_fail_run/m1.status" || \
    die "self-test: diagnostic command failure status mismatch"
  grep -Fq $'\tm1-diagnostic phase failed cycle=1 phase=tcp-forward' \
    "$m1_diagnostic_fail_run/events.tsv" || \
    die "self-test: diagnostic command failure event missing"
  ! grep -Fq $'\tm1-diagnostic final drain complete ' \
    "$m1_diagnostic_fail_run/events.tsv" || \
    die "self-test: diagnostic command failure reached final drain"
  unset M0_TEST_IPERF_FAIL
  SOAK_STAGE=m1
  SOAK_LABEL=M1
  SOAK_SUCCESS_STATUS=complete
  SOAK_CONTINUE_DATA_QUALITY=0
  SOAK_VIOLATIONS_FILE=
  SOAK_STAGE=m2
  SOAK_LABEL=M2
  SOAK_EVIDENCE_DIR=m2
  SOAK_STATUS_FILE=m2.status
  SOAK_SUCCESS_STATUS=complete
  SOAK_REAL_CLIENT_PROBE=0
  m2_schedule_run="$tmp/m2-schedule-run"
  m2_test_profile="$tmp/m2-test-workload.txt"
  mkdir -p "$m2_schedule_run/m2"
  printf 'timestamp\tevent\n' >"$m2_schedule_run/events.tsv"
  cp "$m2_profile" "$m2_test_profile"
  sed -i '' \
    -e 's/^total_secs=.*/total_secs=84/' \
    -e 's/^steady_a_secs=.*/steady_a_secs=10/' \
    -e 's/^idle_secs=.*/idle_secs=1/' \
    -e 's/^quiet_a_secs=.*/quiet_a_secs=10/' \
    -e 's/^steady_b_secs=.*/steady_b_secs=10/' \
    -e 's/^churn_secs=.*/churn_secs=28/' \
    -e 's/^quiet_b_secs=.*/quiet_b_secs=10/' \
    -e 's/^steady_c_secs=.*/steady_c_secs=10/' \
    -e 's/^final_drain_secs=.*/final_drain_secs=1/' \
    -e 's/^tcp_epoch_secs=.*/tcp_epoch_secs=1/' \
    -e 's/^udp_epoch_secs=.*/udp_epoch_secs=1/' \
    -e 's/^short_epoch_secs=.*/short_epoch_secs=1/' \
    "$m2_test_profile"
  : >"$M0_TEST_COMMAND_LOG"
  run_m2_schedule "$m2_schedule_run" "$m2_test_profile" || \
    die "self-test: compressed M2 schedule failed"
  [[ "$(grep -Fc $'\tm2 active ' "$m2_schedule_run/events.tsv")" == "12" ]] || \
    die "self-test: M2 active-window start/complete count mismatch"
  [[ "$(grep -Fc $'\tm2 idle complete ' "$m2_schedule_run/events.tsv")" == "5" ]] || \
    die "self-test: M2 idle count mismatch"
  [[ "$(grep -Fc $'\tm2 resume complete ' "$m2_schedule_run/events.tsv")" == "5" ]] || \
    die "self-test: M2 resume count mismatch"
  [[ "$(grep -Fc $'\tm2 cycle complete ' "$m2_schedule_run/events.tsv")" == "6" ]] || \
    die "self-test: compressed M2 complete-cycle count mismatch"
  [[ "$M2_COMPLETE_CYCLE_INDEX" == "6" && "$M0_CYCLE_INDEX" == "12" ]] || \
    die "self-test: M2 complete-cycle identity followed partial attempt numbering"
  [[ "$(grep -Fc $'\tm2 DNS complete ' "$m2_schedule_run/events.tsv")" == "6" ]] || \
    die "self-test: compressed M2 DNS count mismatch"
  [[ "$(grep -Fc $'\tm2 phase complete ' "$m2_schedule_run/events.tsv")" == "78" ]] || \
    die "self-test: compressed M2 phase count mismatch"
  [[ "$(grep -Ec $'\tm2 phase complete cycle=[0-9]+ phase=udp-reverse' \
    "$m2_schedule_run/events.tsv")" == "6" ]] || \
    die "self-test: compressed M2 UDP count mismatch"
  grep -Fxq complete "$m2_schedule_run/m2.status" || \
    die "self-test: successful M2 schedule status mismatch"
  SOAK_STAGE=m2-qualification
  SOAK_LABEL=M2-QUALIFICATION
  SOAK_EVIDENCE_DIR=m2-qualification
  SOAK_STATUS_FILE=m2-qualification.status
  SOAK_REAL_CLIENT_PROBE=0
  SOAK_CONTINUE_DATA_QUALITY=0
  M0_ENFORCE_RUN_HEALTH=0
  m2_qualification_run="$tmp/m2-qualification-run"
  mkdir -p "$m2_qualification_run/m2-qualification"
  printf 'timestamp\tevent\n' >"$m2_qualification_run/events.tsv"
  : >"$M0_TEST_COMMAND_LOG"
  run_m2_qualification_schedule \
    "$m2_qualification_run" "$m2_test_profile" || \
    die "self-test: compressed M2 qualification failed"
  [[ "$(grep -Fc $'\tm2-qualification phase complete ' \
    "$m2_qualification_run/events.tsv")" == "8" ]] || \
    die "self-test: M2 qualification phase count mismatch"
  for cycle_index in 1 2; do
    for result_label in tcp-forward tcp-reverse udp-reverse short-forward-1; do
      [[ "$(grep -Fc $'\tm2-qualification phase complete cycle='"$cycle_index"' phase='"$result_label" \
        "$m2_qualification_run/events.tsv")" == "1" ]] || \
        die "self-test: M2 qualification exact phase missing: cycle=$cycle_index $result_label"
    done
  done
  ! grep -Eq $'\tm2-qualification (active|idle|resume|final drain) ' \
    "$m2_qualification_run/events.tsv" || \
    die "self-test: M2 qualification entered a formal timeline window"
  [[ "$(grep -Fc $'\tm2-qualification DNS complete cycle=' \
    "$m2_qualification_run/events.tsv")" == "2" ]] || \
    die "self-test: M2 qualification DNS count mismatch"
  [[ "$(grep -Fc $'\tm2-qualification cycle complete cycle=' \
    "$m2_qualification_run/events.tsv")" == "2" ]] || \
    die "self-test: M2 qualification cycle identity mismatch"
  [[ "$M2_COMPLETE_CYCLE_INDEX" == "2" && "$M0_CYCLE_INDEX" == "2" ]] || \
    die "self-test: M2 qualification complete-cycle state mismatch"
  grep -Fxq PASS_NON_ACCEPTANCE \
    "$m2_qualification_run/m2-qualification.status" || \
    die "self-test: M2 qualification success status mismatch"
  [[ ! -e "$m2_qualification_run/m2.status" && \
    ! -e "$m2_qualification_run/m2-pre-stop-verdict.txt" ]] || \
    die "self-test: M2 qualification created formal acceptance evidence"
  mkdir -p "$m2_qualification_run/m2-qualification-real-client"
  m2_qualification_result_slo "$m2_qualification_run" 0 || \
    die "self-test: M2 qualification exact result envelope failed"
  m2_qualification_terminal_wait_run="$tmp/m2-qualification-terminal-wait-run"
  mkdir "$m2_qualification_terminal_wait_run"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=60031B,live=1409B,outstanding=0B,records=2)' \
    >"$m2_qualification_terminal_wait_run/mini_vpn.log"
  (
    /bin/sleep 0.1
    printf '%s\n' \
      '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
      >>"$m2_qualification_terminal_wait_run/mini_vpn.log"
  ) &
  m2_qualification_terminal_wait_pid=$!
  wait_for_m2_qualification_terminal_safety \
    "$m2_qualification_terminal_wait_run" 2 || \
    die "self-test: M2 qualification did not wait for bounded terminal drain"
  wait "$m2_qualification_terminal_wait_pid" || \
    die "self-test: M2 qualification terminal-drain fixture failed"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=60031B,live=1409B,outstanding=0B,records=2)' \
    >>"$m2_qualification_terminal_wait_run/mini_vpn.log"
  ! wait_for_m2_qualification_terminal_safety \
    "$m2_qualification_terminal_wait_run" 1 || \
    die "self-test: M2 qualification terminal-drain wait was unbounded"
  : >"$m2_qualification_run/mini_vpn.log"
  endpoint_rebind_lifecycle_is_clean "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: empty M2 qualification rebind lifecycle rejected"
  recovery_evidence_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: empty M2 qualification recovery evidence rejected"
  m2_recovery_contract_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: empty formal M2 recovery contract rejected"
  printf '%s\n' \
    'tuic-endpoint-rebind generation=1 trigger=tcp_write_stall' \
    >>"$m2_qualification_run/mini_vpn.log"
  ! endpoint_rebind_lifecycle_is_clean "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: unrecovered M2 qualification rebind accepted"
  printf '%s\n' \
    'tuic-endpoint-rebind-recovered generation=1 first_rx_ms=250 socket_generation=1' \
    >>"$m2_qualification_run/mini_vpn.log"
  endpoint_rebind_lifecycle_is_clean "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: recovered M2 qualification rebind rejected"
  printf '%s\n' \
    'tuic-recovery-evidence kind=tcp_write_pressure_start action=none conn=11 writer=3 stream=7 episode=9 acknowledged=1000B pending_ms=250 ack_stalled_ms=250' \
    'tuic-recovery-evidence kind=tcp_write_pressure_end action=none conn=11 writer=3 stream=7 episode=9 observations=2 observed_ms=500 initial_acknowledged=1000B final_acknowledged=65000B acknowledged_delta=64000B ack_progress_observations=1 max_pending_ms=500 max_ack_stalled_ms=250' \
    'tuic-recovery-evidence kind=tcp_ordered_gap_observed action=none conn=12 reader=4 stream=21 episode=2 read_offset=1000 next_received_offset=1100 initial_highest_received_offset=2000 current_highest_received_offset=3000 initial_buffered=900B current_buffered=1900B gap=100B observations=2 observed_ms=250 tail_advanced=true' \
    >>"$m2_qualification_run/mini_vpn.log"
  recovery_evidence_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: valid recovery evidence rejected"
  printf '%s\n' \
    'tuic-recovery-evidence kind=tcp_write_pressure_start action=none conn=11' \
    >>"$m2_qualification_run/mini_vpn.log"
  ! recovery_evidence_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: malformed recovery evidence accepted"
  ! m2_recovery_contract_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: malformed recovery evidence met the formal M2 contract"
  : >"$m2_qualification_run/mini_vpn.log"
  printf '%s\n' \
    'tuic-endpoint-rebind generation=2 trigger=tcp_ordered_read_gap' \
    'tuic-endpoint-rebind-recovered generation=2 first_rx_ms=250 socket_generation=2' \
    >>"$m2_qualification_run/mini_vpn.log"
  endpoint_rebind_lifecycle_is_clean "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: active ordered-gap lifecycle fixture invalid"
  ! recovery_evidence_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: active ordered-gap rebind was accepted"
  ! m2_recovery_contract_is_safe "$m2_qualification_run/mini_vpn.log" || \
    die "self-test: active ordered-gap rebind met the formal M2 contract"
  printf '%s\n' \
    'qualification_slo_evidence=PASS' \
    'formal_m2_acceptance=NOT_RUN' \
    >"$m2_qualification_run/m2-qualification-verdict.txt"
  : >"$m2_qualification_run/mini_vpn.log"
  write_summary "$m2_qualification_run"
  grep -Fq -- '- m2_qualification_status: PASS_NON_ACCEPTANCE' \
    "$m2_qualification_run/summary.md" || \
    die "self-test: M2 qualification summary status mismatch"
  grep -Fq -- '- m2_qualification_verdict: PASS_NON_ACCEPTANCE' \
    "$m2_qualification_run/summary.md" || \
    die "self-test: M2 qualification summary verdict mismatch"
  grep -Fq -- '- m2_qualification_formal_m2_acceptance: NOT_RUN' \
    "$m2_qualification_run/summary.md" || \
    die "self-test: M2 qualification was promoted to formal acceptance"

  m2_qualification_fail_run="$tmp/m2-qualification-fail-run"
  mkdir -p "$m2_qualification_fail_run/m2-qualification"
  printf 'timestamp\tevent\n' >"$m2_qualification_fail_run/events.tsv"
  M0_TEST_IPERF_RECEIVER_ZERO=1
  export M0_TEST_IPERF_RECEIVER_ZERO
  if run_m2_qualification_schedule \
    "$m2_qualification_fail_run" "$m2_test_profile"; then
    die "self-test: M2 qualification continued a receiver-zero failure"
  fi
  grep -Fxq failed \
    "$m2_qualification_fail_run/m2-qualification.status" || \
    die "self-test: M2 qualification failure status mismatch"
  grep -Fq $'\tm2-qualification phase failed cycle=1 phase=tcp-forward reason=receiver_zero_interval' \
    "$m2_qualification_fail_run/events.tsv" || \
    die "self-test: M2 qualification failure reason missing"
  ! grep -Fq $'\tm2-qualification phase complete cycle=1 phase=tcp-reverse' \
    "$m2_qualification_fail_run/events.tsv" || \
    die "self-test: M2 qualification continued after failure"
  unset M0_TEST_IPERF_RECEIVER_ZERO
  SOAK_STAGE=m1
  SOAK_LABEL=M1
  SOAK_EVIDENCE_DIR=m1
  SOAK_STATUS_FILE=m1.status
  m1_checkpoint_file="$m1_run/m1-checkpoints.csv"
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes' \
    '2026-07-17T00:00:00Z,idle-1,100,15,11,61440,0,0' \
    '2026-07-17T01:00:00Z,idle-2,120,16,12,61440,0,0' \
    '2026-07-17T02:00:00Z,idle-3,130,17,13,61440,0,0' \
    '2026-07-17T03:00:00Z,final,125,16,12,61440,0,0' \
    >"$m1_checkpoint_file"
  [[ "$(m1_checkpoint_envelope "$m1_checkpoint_file")" == \
    '4 1 100 125 130 25 15 16 17 1 11 12 13 1 0' ]] || \
    die "self-test: M1 checkpoint envelope mismatch"
  m1_checkpoint_slo "$m1_checkpoint_file" || \
    die "self-test: valid M1 checkpoint plateau rejected"
  cp "$m1_checkpoint_file" "$m1_checkpoint_file.valid"
  sed -i '' 's/,final,125,16,12,61440,0,0$/,final,33000,16,12,61440,0,0/' \
    "$m1_checkpoint_file"
  ! m1_checkpoint_slo "$m1_checkpoint_file" || \
    die "self-test: M1 RSS growth beyond 32MiB accepted"
  cp "$m1_checkpoint_file.valid" "$m1_checkpoint_file"
  sed -i '' 's/,final,125,16,12,61440,0,0$/,final,125,16,12,61439,0,1/' \
    "$m1_checkpoint_file"
  ! m1_checkpoint_slo "$m1_checkpoint_file" || \
    die "self-test: M1 nonzero checkpoint ownership accepted"
  cp "$m1_checkpoint_file.valid" "$m1_checkpoint_file"
  sed -i '' 's/,idle-3,130,17,13,61440,0,0$/,idle-3,130,18,13,61440,0,0/' \
    "$m1_checkpoint_file"
  ! m1_checkpoint_slo "$m1_checkpoint_file" || \
    die "self-test: M1 checkpoint FD growth beyond two accepted"
  cp "$m1_checkpoint_file.valid" "$m1_checkpoint_file"
  sed -i '' 's/,idle-3,130,17,13,61440,0,0$/,idle-3,130,17,14,61440,0,0/' \
    "$m1_checkpoint_file"
  ! m1_checkpoint_slo "$m1_checkpoint_file" || \
    die "self-test: M1 checkpoint thread growth beyond two accepted"
  cp "$m1_checkpoint_file.valid" "$m1_checkpoint_file"
  sed -i '' '$d' "$m1_checkpoint_file"
  ! m1_checkpoint_slo "$m1_checkpoint_file" || \
    die "self-test: incomplete M1 checkpoint set accepted"
  mv "$m1_checkpoint_file.valid" "$m1_checkpoint_file"
  m1_capture_run="$tmp/m1-capture-run"
  mkdir "$m1_capture_run"
  printf '%s\n' \
    'timestamp,pid,rss_kib,cpu_percent,state,elapsed,fd_count,thread_rows' \
    '2026-07-17T00:00:00Z,1,100,1.0,S,00:01,15,11' \
    >"$m1_capture_run/process.csv"
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes' \
    >"$m1_capture_run/m1-checkpoints.csv"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
    >"$m1_capture_run/mini_vpn.log"
  ! capture_m1_checkpoint "$m1_capture_run" idle-1 1 || \
    die "self-test: stale endpoint sample was accepted for an M1 checkpoint"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
    >>"$m1_capture_run/mini_vpn.log"
  capture_m1_checkpoint "$m1_capture_run" idle-1 1 || \
    die "self-test: fresh endpoint sample was rejected for an M1 checkpoint"
  [[ "$(awk 'END {print NR}' "$m1_capture_run/m1-checkpoints.csv")" == "2" ]] || \
    die "self-test: fresh M1 checkpoint row was not recorded exactly once"

  m2_replay_log="$tmp/m2-controlled-replay.log"
  printf '%s\n' \
    '🔎 tuic-open-tcp target=28-courier.push.apple.com:5223 conn=0 id=1 stream=1 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(3) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(3) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tuic-open-tcp target=api.ipify.org:443 conn=1 id=2 stream=1 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(5) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(5) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tcp-handle-close handle=SocketHandle(5) direction=remote_to_local reason=remote_eof state=Closing' \
    '🔎 tuic-open-tcp target=example.com:443 conn=1 id=2 stream=2 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(5) epoch=3' \
    '🔎 tcp-relay-engine handle=SocketHandle(5) epoch=3 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tcp-lifecycle-transition handle=SocketHandle(5) source=dirty_relay prev_source=dirty_relay ctx_state=Closing uplink_tx=false' \
    '🔎 tcp-handle-close handle=SocketHandle(5) direction=remote_to_local reason=remote_eof state=Closing' \
    '🔎 tcp-handle-close handle=SocketHandle(7) direction=remote_open reason=handshake_failed state=HandshakePending' \
    '🔎 tcp-handle-close handle=SocketHandle(9) direction=policy reason=encrypted_dns_block state=Listening' \
    '🔎 tuic-open-tcp target=api.ipify.org:443 conn=1 id=2 stream=7 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(13) epoch=1' \
    '🗑️ handle SocketHandle(13) 迟到 open 结果(epoch 1≠3) 丢弃，不装到新代 socket' \
    '📊 数据面: DNS forge=18/drop=0 | TCP relay 活跃=1/累计=3 | fake-IP 活跃=1/在册=9 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >"$m2_replay_log"
  [[ "$(m2_controlled_tcp_replay_envelope "$m2_replay_log" \
    43.130.32.77:5201 api.ipify.org:443 example.com:443)" == \
    '1 1 0 0' ]] || \
    die "self-test: ambient relay with drained controlled handles was rejected"
  cp "$m2_replay_log" "$tmp/m2-controlled-replay-pending.log"
  printf '%s\n' \
    '🔎 tuic-open-tcp target=api.ipify.org:443 conn=1 id=2 stream=8 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(15) epoch=1' \
    '📊 数据面: DNS forge=18/drop=0 | TCP relay 活跃=1/累计=3 | fake-IP 活跃=1/在册=9 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$tmp/m2-controlled-replay-pending.log"
  [[ "$(m2_controlled_tcp_replay_envelope \
    "$tmp/m2-controlled-replay-pending.log" 43.130.32.77:5201 \
    api.ipify.org:443 example.com:443)" == '2 1 1 0' ]] || \
    die "self-test: pending controlled install was accepted as drained"
  printf '%s\n' \
    '🔎 tuic-open-tcp target=43.130.32.77:5201 conn=1 id=2 stream=3 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(5) epoch=5' \
    '🔎 tcp-relay-engine handle=SocketHandle(5) epoch=5 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '📊 数据面: DNS forge=18/drop=0 | TCP relay 活跃=2/累计=4 | fake-IP 活跃=1/在册=9 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$m2_replay_log"
  [[ "$(m2_controlled_tcp_replay_envelope "$m2_replay_log" \
    43.130.32.77:5201 api.ipify.org:443 example.com:443)" == \
    '2 2 1 0' ]] || \
    die "self-test: undrained controlled relay was not reconstructed"
  cp "$m2_replay_log" "$tmp/m2-controlled-replay-mismatch.log"
  sed -i '' '$s/TCP relay 活跃=2/TCP relay 活跃=1/' \
    "$tmp/m2-controlled-replay-mismatch.log"
  [[ "$(m2_controlled_tcp_replay_envelope \
    "$tmp/m2-controlled-replay-mismatch.log" 43.130.32.77:5201 \
    api.ipify.org:443 example.com:443)" == '2 2 1 0' ]] || \
    die "self-test: different replay/gauge lifecycle boundaries were coupled"
  cp "$m2_replay_log" "$tmp/m2-controlled-replay-duplicate.log"
  printf '%s\n' \
    '🔎 tuic-open-tcp target=other.example:443 conn=0 id=1 stream=4 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(3) epoch=3' \
    '🔎 tcp-relay-engine handle=SocketHandle(3) epoch=3 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '📊 数据面: DNS forge=18/drop=0 | TCP relay 活跃=2/累计=5 | fake-IP 活跃=2/在册=10 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$tmp/m2-controlled-replay-duplicate.log"
  [[ "$(m2_controlled_tcp_replay_envelope \
    "$tmp/m2-controlled-replay-duplicate.log" 43.130.32.77:5201 \
    api.ipify.org:443 example.com:443)" == '3 2 1 1' ]] || \
    die "self-test: duplicate live handle did not fail closed"

  m2_checkpoint_file="$m2_schedule_run/m2-checkpoints.csv"
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes,active_relays,total_relays,fake_ip_active,fake_ip_registered,dns_forged,dns_dropped,active_leases,replayed_relaying_handles,controlled_active_relays,replay_invalid' \
    '2026-07-30T00:00:00Z,idle-1,100,15,11,61440,0,0,1,10,1,9,10,0,2,1,0,0' \
    '2026-07-30T01:00:00Z,idle-2,110,16,12,61440,0,0,2,20,2,10,20,0,4,1,0,0' \
    '2026-07-30T02:00:00Z,idle-3,120,17,13,61440,0,0,1,30,1,11,30,0,2,4,0,0' \
    '2026-07-30T03:00:00Z,idle-4,125,17,13,61440,0,0,3,40,3,12,40,0,6,2,0,0' \
    '2026-07-30T04:00:00Z,idle-5,130,16,12,61440,0,0,1,50,1,13,50,0,2,3,0,0' \
    '2026-07-30T05:00:00Z,final,125,16,12,61440,0,0,2,60,2,14,60,0,4,1,0,0' \
    >"$m2_checkpoint_file"
  [[ "$(m2_checkpoint_envelope "$m2_checkpoint_file")" == \
    '6 1 100 125 130 25 15 16 17 1 11 12 13 1 0' ]] || \
    die "self-test: M2 checkpoint envelope mismatch"
  m2_checkpoint_slo "$m2_checkpoint_file" || \
    die "self-test: valid M2 checkpoint plateau rejected"
  cp "$m2_checkpoint_file" "$m2_checkpoint_file.valid"
  sed -i '' 's/,final,125,16,12,61440,0,0,2,60,2,14,60,0,4,1,0,0$/,final,125,16,12,61440,0,0,2,60,2,14,60,0,4,1,1,0/' \
    "$m2_checkpoint_file"
  ! m2_checkpoint_slo "$m2_checkpoint_file" || \
    die "self-test: M2 controlled relay at final checkpoint accepted"
  cp "$m2_checkpoint_file.valid" "$m2_checkpoint_file"
  sed -i '' 's/,idle-4,125,17,13,61440,0,0,3,40,3,12,40,0,6,2,0,0$/,idle-4,125,17,13,61440,0,0,3,40,3,19,40,0,6,2,0,0/' \
    "$m2_checkpoint_file"
  m2_checkpoint_slo "$m2_checkpoint_file" || \
    die "self-test: numeric ambient fake-IP growth was rejected"
  cp "$m2_checkpoint_file.valid" "$m2_checkpoint_file"
  sed -i '' 's/,idle-5,130,16,12,61440,0,0,1,50,1,13,50,0,2,3,0,0$/,idle-5,130,16,12,61440,0,0,1,50,1,13,50,1,2,3,0,0/' \
    "$m2_checkpoint_file"
  ! m2_checkpoint_slo "$m2_checkpoint_file" || \
    die "self-test: M2 DNS drop at checkpoint accepted"
  cp "$m2_checkpoint_file.valid" "$m2_checkpoint_file"
  sed -i '' 's/,idle-2,110,16,12,61440,0,0,2,20,2,10,20,0,4,1,0,0$/,idle-2,110,16,12,61440,0,0,2,20,2,10,20,0,4,1,0,1/' \
    "$m2_checkpoint_file"
  ! m2_checkpoint_slo "$m2_checkpoint_file" || \
    die "self-test: M2 invalid lifecycle replay at checkpoint accepted"
  mv "$m2_checkpoint_file.valid" "$m2_checkpoint_file"

  m2_capture_run="$tmp/m2-capture-run"
  mkdir "$m2_capture_run"
  printf '%s\n' \
    'timestamp,pid,rss_kib,cpu_percent,state,elapsed,fd_count,thread_rows' \
    '2026-07-30T00:00:00Z,1,100,1.0,S,00:01,15,11' \
    >"$m2_capture_run/process.csv"
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes,active_relays,total_relays,fake_ip_active,fake_ip_registered,dns_forged,dns_dropped,active_leases,replayed_relaying_handles,controlled_active_relays,replay_invalid' \
    >"$m2_capture_run/m2-checkpoints.csv"
  printf '%s\n' \
    'target=43.130.32.77' \
    'iperf_port=5201' \
    >"$m2_capture_run/m2-workload.txt"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
    '🔎 tuic-tcp-pool-activity active_leases=0' \
    '📊 数据面: DNS forge=10/drop=0 | TCP relay 活跃=0/累计=10 | fake-IP 活跃=0/在册=2 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >"$m2_capture_run/mini_vpn.log"
  ! capture_m2_checkpoint "$m2_capture_run" idle-1 1 1 || \
    die "self-test: stale M2 endpoint/data-plane samples were accepted"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
    '📊 数据面: DNS forge=11/drop=0 | TCP relay 活跃=0/累计=11 | fake-IP 活跃=0/在册=2 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$m2_capture_run/mini_vpn.log"
  capture_m2_checkpoint "$m2_capture_run" idle-1 1 1 || \
    die "self-test: fresh clean M2 checkpoint samples were rejected"
  m2_full_tunnel_quiescence_is_clean \
    "$m2_capture_run/mini_vpn.log" "$m2_capture_run/m2-workload.txt" 1 1 || \
    die "self-test: clean full-tunnel quiescence evidence was rejected"
  wait_for_m2_full_tunnel_quiescence \
    "$m2_capture_run/mini_vpn.log" "$m2_capture_run/m2-workload.txt" 1 1 1 || \
    die "self-test: clean full-tunnel quiescence did not pass immediately"
  cp "$m2_capture_run/mini_vpn.log" "$tmp/m2-quiescence-endpoint.log"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=60031B,live=1409B,outstanding=0B,records=2)' \
    '📊 数据面: DNS forge=12/drop=0 | TCP relay 活跃=0/累计=12 | fake-IP 活跃=0/在册=2 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$tmp/m2-quiescence-endpoint.log"
  ! m2_full_tunnel_quiescence_is_clean \
    "$tmp/m2-quiescence-endpoint.log" "$m2_capture_run/m2-workload.txt" 2 2 || \
    die "self-test: live Endpoint ownership passed full-tunnel quiescence"
  printf '%s\n' \
    '🔎 tuic-open-tcp target=push-a.example:5223 conn=0 id=1 stream=1 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(3) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(3) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tuic-open-tcp target=push-b.example:5223 conn=1 id=2 stream=1 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(5) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(5) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tuic-open-tcp target=sync-a.example:443 conn=0 id=1 stream=2 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(7) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(7) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tuic-open-tcp target=sync-b.example:443 conn=1 id=2 stream=2 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(9) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(9) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tuic-tcp-pool-activity active_leases=8' \
    '📊 数据面: DNS forge=12/drop=0 | TCP relay 活跃=4/累计=15 | fake-IP 活跃=7/在册=19 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$m2_capture_run/mini_vpn.log"
  m2_full_tunnel_quiescence_is_clean \
    "$m2_capture_run/mini_vpn.log" "$m2_capture_run/m2-workload.txt" 2 1 || \
    die "self-test: replay-consistent ambient traffic was rejected"
  printf '%s\n' \
    '🔎 tuic-open-tcp target=43.130.32.77:5201 conn=0 id=1 stream=3 relay_mode=d16_direct_ordered startup_auth_attempts=1 handle=SocketHandle(11) epoch=1' \
    '🔎 tcp-relay-engine handle=SocketHandle(11) epoch=1 engine=d16_byte_owned per_flow_cap=524288 global_cap=16777216 quantum=32768' \
    '🔎 tuic-tcp-pool-activity active_leases=10' \
    '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
    '📊 数据面: DNS forge=13/drop=0 | TCP relay 活跃=5/累计=16 | fake-IP 活跃=8/在册=20 | UDP↓丢=0 背压=0 | UDP↑丢=0 stream兜底=0 | leg=tuic' \
    >>"$m2_capture_run/mini_vpn.log"
  ! m2_full_tunnel_quiescence_is_clean \
    "$m2_capture_run/mini_vpn.log" "$m2_capture_run/m2-workload.txt" 3 2 || \
    die "self-test: undrained controlled traffic passed full-tunnel quiescence"
  [[ "$(m2_full_tunnel_quiescence_snapshot \
    "$m2_capture_run/mini_vpn.log" "$m2_capture_run/m2-workload.txt")" == \
    '4 3 10 5 8 20 0 61440 0 0 4 5 1 0' ]] || \
    die "self-test: full-tunnel quiescence snapshot mismatch"
  ! wait_for_m2_full_tunnel_quiescence \
    "$m2_capture_run/mini_vpn.log" "$m2_capture_run/m2-workload.txt" 3 2 1 || \
    die "self-test: controlled traffic passed the bounded quiescence wait"
  ! record_m2_full_tunnel_quiescence \
    "$m2_capture_run" "$m2_capture_run/m2-workload.txt" 3 2 1 || \
    die "self-test: controlled traffic evidence was recorded as PASS"
  grep -Fq 'result=FAIL' \
    "$m2_capture_run/m2-full-tunnel-quiescence.txt" || \
    die "self-test: dirty full-tunnel quiescence verdict missing"
  grep -Fq 'active_leases=10 active_relays=5 fake_ip_active=8 fake_ip_registered=20 dns_dropped=0' \
    "$m2_capture_run/m2-full-tunnel-quiescence.txt" || \
    die "self-test: ambient full-tunnel observations missing"
  grep -Fq 'replay_samples=4 replayed_relaying_handles=5 controlled_active_relays=1 replay_invalid=0' \
    "$m2_capture_run/m2-full-tunnel-quiescence.txt" || \
    die "self-test: controlled full-tunnel replay evidence missing"
  grep -Fq 'endpoint_available=61440 endpoint_live=0 endpoint_outstanding=0' \
    "$m2_capture_run/m2-full-tunnel-quiescence.txt" || \
    die "self-test: full-tunnel quiescence Endpoint values missing"
  m2_record_fail_run="$tmp/m2-record-fail"
  mkdir "$m2_record_fail_run" \
    "$m2_record_fail_run/m2-full-tunnel-quiescence.txt"
  cp "$m2_capture_run/mini_vpn.log" "$m2_record_fail_run/mini_vpn.log"
  record_m2_full_tunnel_quiescence \
    "$m2_record_fail_run" "$m2_capture_run/m2-workload.txt" 3 2 1 2>/dev/null
  quiescence_record_status=$?
  [[ "$quiescence_record_status" == "2" ]] || \
    die "self-test: full-tunnel quiescence evidence write failure was not distinct"
  : >"$m1_run/mini_vpn.log"
  write_summary "$m1_run"
  grep -Fq -- '- m1_status: complete' "$m1_run/summary.md" || \
    die "self-test: completed M1 summary status mismatch"
  grep -Fq -- '- m1_active_windows_completed: 5' "$m1_run/summary.md" || \
    die "self-test: completed M1 active-window count mismatch"
  grep -Fq -- '- m1_cycles_completed: 5' "$m1_run/summary.md" || \
    die "self-test: completed compressed M1 cycle count mismatch"
  grep -Fq -- '- m1_idle_complete: 3' "$m1_run/summary.md" || \
    die "self-test: completed M1 idle count mismatch"
  grep -Fq -- '- m1_resume_complete: 3' "$m1_run/summary.md" || \
    die "self-test: completed M1 resume count mismatch"
  grep -Fq -- '- m1_final_drain_complete: 1' "$m1_run/summary.md" || \
    die "self-test: completed M1 final-drain count mismatch"
  grep -Fq -- '- m1_timeline_evidence: PASS' "$m1_run/summary.md" || \
    die "self-test: complete M1 timeline evidence rejected"
  grep -Fq -- '- m1_phase_results_completed: 70' "$m1_run/summary.md" || \
    die "self-test: compressed M1 phase-result count mismatch"
  grep -Fq -- '- m1_result_evidence: PASS' "$m1_run/summary.md" || \
    die "self-test: complete M1 result evidence rejected"
  grep -Fq -- '- m1_dns_evidence: PASS' "$m1_run/summary.md" || \
    die "self-test: complete M1 DNS evidence rejected"
  grep -Fq -- '- m1_checkpoint_evidence: PASS' "$m1_run/summary.md" || \
    die "self-test: complete M1 checkpoint evidence rejected"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_run/summary.md" || \
    die "self-test: shortened M1 fixture incorrectly met formal SLO"
  SOAK_STAGE=m0
  SOAK_LABEL=M0
  SOAK_EVIDENCE_DIR=m0
  SOAK_STATUS_FILE=m0.status
  direct_dir="$tmp/direct"
  mkdir "$direct_dir"
  jq '.start.test_start.duration = 300
    | .server_output_json.start.test_start.duration = 300
    | .server_output_json.end.sum_received.seconds = 300
    | .server_output_json.intervals = [range(0; 300) as $second
      | {"sum": {"start": $second, "end": ($second + 1),
          "bits_per_second": 1}}]' \
    "$m0_run/m0/cycle_001_tcp-forward.json" \
    >"$tmp/direct-forward-valid.json"
  jq '.server_output_json.intervals += [
      {"sum":{"start":300.001,"end":300.165,"bits_per_second":0}}]' \
    "$tmp/direct-forward-valid.json" \
    >"$tmp/direct-forward-zero-partial-tail.json"
  validate_direct_continuity_result \
    "$tmp/direct-forward-zero-partial-tail.json" 43.130.32.77 || \
    die "self-test: direct continuity rejected a zero short receiver command tail"
  cp "$tmp/direct-forward-valid.json" "$direct_dir/direct-forward-300s.json"
  cat >"$direct_dir/manifest.txt" <<EOF_DIRECT_FIXTURE
schema=knife15-macos-direct-continuity-v1
status=pass
completed_epoch=1050
source_commit=$(git -C "$REPO" rev-parse HEAD)
runner_sha256=$(sha256_file "$SCRIPT_PATH")
binary_sha256=$(sha256_file "$BIN")
target=43.130.32.77
target_route=en0
exit_route=en0
baseline_dir=$baseline_dir
baseline_forward_sha256=$(sha256_file "$baseline_dir/direct-forward.json")
baseline_reverse_sha256=$(sha256_file "$baseline_dir/direct-reverse.json")
duration_secs=300
rate_bps=4522378
result_sha256=$(sha256_file "$direct_dir/direct-forward-300s.json")
EOF_DIRECT_FIXTURE
  validate_direct_continuity_dir "$direct_dir" "$baseline_dir" \
    43.130.32.77 1100 || die "self-test: valid direct continuity evidence rejected"
  jq '.start.test_start.duration = 30
    | .server_output_json.end.sum_received.seconds = 30' \
    "$direct_dir/direct-forward-300s.json" \
    >"$direct_dir/direct-forward-300s.json.tmp"
  mv "$direct_dir/direct-forward-300s.json.tmp" \
    "$direct_dir/direct-forward-300s.json"
  sed -i '' "s/^result_sha256=.*/result_sha256=$(sha256_file "$direct_dir/direct-forward-300s.json")/" \
    "$direct_dir/manifest.txt"
  ! validate_direct_continuity_dir "$direct_dir" "$baseline_dir" \
    43.130.32.77 1100 || \
    die "self-test: short direct continuity result was accepted as 300s evidence"
  cp "$tmp/direct-forward-valid.json" "$direct_dir/direct-forward-300s.json"
  sed -i '' "s/^result_sha256=.*/result_sha256=$(sha256_file "$direct_dir/direct-forward-300s.json")/" \
    "$direct_dir/manifest.txt"
  jq '.server_output_json.intervals[0].sum.bits_per_second = 0' \
    "$direct_dir/direct-forward-300s.json" \
    >"$direct_dir/direct-forward-300s.json.tmp"
  mv "$direct_dir/direct-forward-300s.json.tmp" \
    "$direct_dir/direct-forward-300s.json"
  sed -i '' "s/^result_sha256=.*/result_sha256=$(sha256_file "$direct_dir/direct-forward-300s.json")/" \
    "$direct_dir/manifest.txt"
  ! validate_direct_continuity_dir "$direct_dir" "$baseline_dir" \
    43.130.32.77 1100 || \
    die "self-test: direct Target receiver zero interval was accepted"
  cp "$tmp/direct-forward-valid.json" "$direct_dir/direct-forward-300s.json"
  sed -i '' "s/^result_sha256=.*/result_sha256=$(sha256_file "$direct_dir/direct-forward-300s.json")/" \
    "$direct_dir/manifest.txt"
  ! validate_direct_continuity_dir "$direct_dir" "$baseline_dir" \
    43.130.32.77 2000 || \
    die "self-test: stale direct continuity evidence was accepted"
  sed -i '' 's/^baseline_forward_sha256=.*/baseline_forward_sha256=wrong/' \
    "$direct_dir/manifest.txt"
  ! validate_direct_continuity_dir "$direct_dir" "$baseline_dir" \
    43.130.32.77 1100 || \
    die "self-test: direct continuity evidence for another baseline was accepted"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=61406B,live=0B,outstanding=0B,records=2)' \
    >"$m0_run/mini_vpn.log"
  printf '%s\n' 'timestamp,pid,rss_kib,cpu_percent,state,elapsed,fd_count,thread_rows' \
    '2026-07-14T00:00:00Z,1,100,1.0,S,00:01,15,11' \
    '2026-07-14T00:00:30Z,1,130,2.0,S,00:31,17,12' \
    '2026-07-14T00:01:00Z,1,120,1.0,S,01:01,16,11' >"$m0_run/process.csv"
  printf '%s\n' 'timestamp,interface,mtu,ipkts,ierrs,ibytes,opkts,oerrs,obytes,collisions' \
    >"$m0_run/interface.csv"
  printf '%s\n' \
    '2026-07-14T00:00:00Z,utun42,1200,10,0,1000,20,0,2000,0' \
    '2026-07-14T00:00:30Z,utun42,1200,20,0,2000,35,0,3500,0' \
    '2026-07-14T00:01:00Z,utun42,1200,30,0,3000,50,0,5000,0' \
    >>"$m0_run/interface.csv"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    '2026-07-14T00:00:00Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,100,0,64000,90,0,60000,0,unknown,unknown' \
    '2026-07-14T00:00:30Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.100,171.100,172.100,3,3,0.000000,1.000,1.500,2.000,1500,200,0,128000,180,0,120000,0,17066,16000' \
    '2026-07-14T00:01:00Z,utun42,en0,en0,192.168.50.1,3,2,33.300000,170.123,180.456,190.789,3,3,0.000000,1.000,1.500,2.000,1500,300,0,192000,270,0,180000,0,17066,16000' \
    >"$m0_run/network.csv"
  write_summary "$m0_run"
  grep -Fq -- '- m0_status: complete' "$m0_run/summary.md" || \
    die "self-test: completed M0 summary status mismatch"
  grep -Fq -- '- m0_cycles_completed: 2' "$m0_run/summary.md" || \
    die "self-test: completed M0 cycle count mismatch"
  grep -Fq -- '- m0_idle_complete: 1' "$m0_run/summary.md" || \
    die "self-test: M0 idle summary marker mismatch"
  grep -Fq -- '- m0_resume_complete: 1' "$m0_run/summary.md" || \
    die "self-test: M0 resume summary marker mismatch"
  grep -Fq -- '- m0_phase_failures: 0' "$m0_run/summary.md" || \
    die "self-test: successful M0 summary reported a phase failure"
  grep -Fq -- '- rss_kib_first_last_max_delta: 100/120/130/20' "$m0_run/summary.md" || \
    die "self-test: M0 RSS envelope summary mismatch"
  grep -Fq -- '- fd_first_last_max_delta: 15/16/17/1' "$m0_run/summary.md" || \
    die "self-test: M0 FD envelope summary mismatch"
  grep -Fq -- '- threads_first_last_max_delta: 11/11/12/0' "$m0_run/summary.md" || \
    die "self-test: M0 thread envelope summary mismatch"
  grep -Fq -- '- utun_ipkts_first_last_delta: 10/30/20' "$m0_run/summary.md" || \
    die "self-test: M0 TUN input packet delta mismatch"
  grep -Fq -- '- utun_opkts_first_last_delta: 20/50/30' "$m0_run/summary.md" || \
    die "self-test: M0 TUN output packet delta mismatch"
  grep -Fq -- '- endpoint_conservation_max_bytes: 61406' "$m0_run/summary.md" || \
    die "self-test: M0 endpoint conservation maximum mismatch"
  grep -Fq -- '- endpoint_last_available_live_outstanding: 61406/0/0' \
    "$m0_run/summary.md" || die "self-test: M0 endpoint final ownership mismatch"
  grep -Fq -- '- m0_tcp_udp_results: 10/2' "$m0_run/summary.md" || \
    die "self-test: M0 TCP/UDP result counts mismatch"
  grep -Fq -- '- m0_tcp_max_sender_receiver_gap_bytes: 10' "$m0_run/summary.md" || \
    die "self-test: M0 TCP byte-gap envelope mismatch"
  grep -Fq -- '- m0_udp_max_loss_percent: 1.250000' "$m0_run/summary.md" || \
    die "self-test: M0 UDP loss envelope mismatch"
  grep -Fq -- '- m0_phase_results_completed: 12' "$m0_run/summary.md" || \
    die "self-test: M0 completed phase count mismatch"
  grep -Fq -- '- m0_result_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: complete M0 result evidence was not accepted"
  grep -Fq -- '- network_control_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: complete M0 network controls were not accepted"
  grep -Fq -- '- network_control_samples: 3' "$m0_run/summary.md" || \
    die "self-test: network control sample count mismatch"
  grep -Fq -- '- exit_ping_max_loss_percent: 33.300000' "$m0_run/summary.md" || \
    die "self-test: Exit control loss envelope mismatch"
  grep -Fq -- '- physical_ibytes_first_last_delta: 64000/192000/128000' \
    "$m0_run/summary.md" || die "self-test: physical input byte envelope mismatch"
  cp "$m0_run/mini_vpn.log" "$m0_run/mini_vpn.no-rebind.log"
  printf '%s\n' \
    '🔄 tuic-endpoint-rebind generation=1 old_local=0.0.0.0:60000 new_local=0.0.0.0:60001 active_tcp=2 udp_active=false live_connections=2 stalled_ms=2000 bound_ms=2000 tx_since_rx=65536B max_rtt_ms=164' \
    '✅ tuic-endpoint-rebind-recovered generation=1 first_rx_ms=250 socket_generation=1' \
    >>"$m0_run/mini_vpn.log"
  write_summary "$m0_run"
  grep -Fq -- '- endpoint_rebind_attempts_recoveries_failures: 1/1/0' \
    "$m0_run/summary.md" || die "self-test: recovered endpoint rebind was not counted"
  grep -Fq -- '- endpoint_rebind_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: recovered endpoint rebind evidence was not accepted"
  grep -Fq -- '- endpoint_rebind_max_first_rx_ms: 250' "$m0_run/summary.md" || \
    die "self-test: endpoint rebind recovery latency mismatch"
  printf '%s\n' \
    '⚠️ tuic-endpoint-rebind-failed generation=2 active_tcp=1 udp_active=false live_connections=1 stalled_ms=2000 bound_ms=2000 tx_since_rx=1024B max_rtt_ms=100 error=bind-failed' \
    >>"$m0_run/mini_vpn.log"
  write_summary "$m0_run"
  grep -Fq -- '- endpoint_rebind_attempts_recoveries_failures: 2/1/1' \
    "$m0_run/summary.md" || die "self-test: failed endpoint rebind was not counted"
  grep -Fq -- '- endpoint_rebind_evidence: MISMATCH' "$m0_run/summary.md" || \
    die "self-test: incomplete endpoint rebind was accepted"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m0_run/summary.md" || \
    die "self-test: incomplete endpoint rebind did not require review"
  mv "$m0_run/mini_vpn.no-rebind.log" "$m0_run/mini_vpn.log"
  write_summary "$m0_run"
  cp "$m0_run/network.csv" "$m0_run/network.complete.csv"
  sed -i '' '$d' "$m0_run/network.csv"
  write_summary "$m0_run"
  grep -Fq -- '- network_control_evidence: PARTIAL' "$m0_run/summary.md" || \
    die "self-test: missing same-window network control was not detected"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m0_run/summary.md" || \
    die "self-test: incomplete M0 network control did not require review"
  mv "$m0_run/network.complete.csv" "$m0_run/network.csv"
  write_summary "$m0_run"
  jq 'if .start.test_start.reverse == 0 then
      .intervals[0].sum.bits_per_second = 0
    else
      .server_output_json.intervals[0].sum.bits_per_second = 0
    end' \
    "$m0_run/m0/cycle_001_tcp-forward.json" \
    >"$m0_run/m0/cycle_001_tcp-forward.json.tmp"
  mv "$m0_run/m0/cycle_001_tcp-forward.json.tmp" \
    "$m0_run/m0/cycle_001_tcp-forward.json"
  write_summary "$m0_run"
  grep -Fq -- '- m0_sender_zero_intervals: 1' "$m0_run/summary.md" || \
    die "self-test: bounded sender stall was not preserved as diagnostic evidence"
  grep -Fq -- '- m0_receiver_zero_intervals: 0' "$m0_run/summary.md" || \
    die "self-test: continuous receiver was misclassified as stalled"
  grep -Fq -- '- m0_result_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: sender-only stall invalidated continuous receiver evidence"
  jq '.server_output_json.intervals[0].sum.bits_per_second = 0' \
    "$m0_run/m0/cycle_001_tcp-forward.json" \
    >"$m0_run/m0/cycle_001_tcp-forward.json.tmp"
  mv "$m0_run/m0/cycle_001_tcp-forward.json.tmp" \
    "$m0_run/m0/cycle_001_tcp-forward.json"
  write_summary "$m0_run"
  grep -Fq -- '- m0_receiver_zero_intervals: 1' "$m0_run/summary.md" || \
    die "self-test: receiver stall was not preserved as acceptance evidence"
  grep -Fq -- '- m0_result_evidence: MISMATCH' "$m0_run/summary.md" || \
    die "self-test: receiver stall did not fail closed in final summary"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m0_run/summary.md" || \
    die "self-test: receiver stall did not require final review"
  jq '.server_output_json.intervals[0].sum.bits_per_second = 1' \
    "$m0_run/m0/cycle_001_tcp-forward.json" \
    >"$m0_run/m0/cycle_001_tcp-forward.json.tmp"
  mv "$m0_run/m0/cycle_001_tcp-forward.json.tmp" \
    "$m0_run/m0/cycle_001_tcp-forward.json"
  write_summary "$m0_run"
  grep -Fq -- '- m0_result_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: restored receiver evidence did not return summary to PASS"
  grep -Fq -- '- m0_dns_result_files: 2' "$m0_run/summary.md" || \
    die "self-test: M0 DNS result count mismatch"
  grep -Fq -- '- m0_dns_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: complete M0 DNS evidence was not accepted"
  grep -Fq -- '- m0_timeline_evidence: PASS' "$m0_run/summary.md" || \
    die "self-test: complete M0 timeline evidence was not accepted"
  m0_log_history_is_complete "$m0_run" || \
    die "self-test: intact M0 log history was rejected"
  cp "$m0_run/events.tsv" "$m0_run/events.pre-compaction.tsv"
  append_event_to "$m0_run" "watchdog compacted mini_vpn.log"
  ! m0_log_history_is_complete "$m0_run" || \
    die "self-test: lossy M0 log compaction was accepted as complete history"
  write_summary "$m0_run"
  grep -Fq -- '- log_compactions: 1' "$m0_run/summary.md" || \
    die "self-test: M0 log compaction was not counted"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m0_run/summary.md" || \
    die "self-test: lossy M0 log compaction did not require review"
  mv "$m0_run/events.pre-compaction.tsv" "$m0_run/events.tsv"
  cp "$m0_run/events.tsv" "$m0_run/events.complete.tsv"
  awk '!/m0 final drain complete/' "$m0_run/events.complete.tsv" >"$m0_run/events.tsv"
  write_summary "$m0_run"
  grep -Fq -- '- m0_timeline_evidence: MISMATCH' "$m0_run/summary.md" || \
    die "self-test: incomplete M0 timeline was not detected"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m0_run/summary.md" || \
    die "self-test: incomplete M0 timeline did not require review"
  mv "$m0_run/events.complete.tsv" "$m0_run/events.tsv"
  rm "$m0_run/m0/cycle_001_tcp-forward.json"
  rm "$m0_run/m0/cycle_001_dns.txt"
  write_summary "$m0_run"
  grep -Fq -- '- m0_result_evidence: MISMATCH' "$m0_run/summary.md" || \
    die "self-test: missing M0 result file was not detected"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m0_run/summary.md" || \
    die "self-test: missing M0 result file did not require review"
  grep -Fq -- '- m0_dns_evidence: MISMATCH' "$m0_run/summary.md" || \
    die "self-test: missing M0 DNS result file was not detected"
  m0_final_ownership_is_clean "$m0_run/mini_vpn.log" || \
    die "self-test: clean M0 final endpoint ownership rejected"
  printf '%s\n' \
    '📊 TUIC endpoint pacing global conservation(available=61439B,live=1B,outstanding=0B,records=2)' \
    >>"$m0_run/mini_vpn.log"
  ! m0_final_ownership_is_clean "$m0_run/mini_vpn.log" || \
    die "self-test: nonzero M0 final endpoint ownership accepted"

  m1_formal_run="$tmp/m1-formal-run"
  mkdir -p "$m1_formal_run/m1"
  printf '%s\n' complete >"$m1_formal_run/m1.status"
  printf 'timestamp\tevent\n' >"$m1_formal_run/events.tsv"
  for result_index in 1 2 3 4 5; do
    printf '2026-07-17T00:00:00Z\tm1 active window-%s complete planned_secs=1\n' \
      "$result_index" >>"$m1_formal_run/events.tsv"
  done
  for result_index in 1 2 3; do
    printf '%s\n' \
      "2026-07-17T00:00:00Z	m1 idle complete label=idle-$result_index planned_secs=300" \
      "2026-07-17T00:00:00Z	m1 resume complete cycle=$result_index" \
      >>"$m1_formal_run/events.tsv"
  done
  printf '%s\n' \
    '2026-07-17T00:00:00Z	m1 final drain complete planned_secs=300' \
    >>"$m1_formal_run/events.tsv"
  for ((result_index = 1; result_index <= 30; result_index++)); do
    printf '%s\n' \
      "2026-07-17T00:00:00Z	m1 cycle complete cycle=$result_index" \
      "2026-07-17T00:00:00Z	m1 DNS complete cycle=$result_index" \
      >>"$m1_formal_run/events.tsv"
    printf '%s\n' '198.18.0.2' \
      >"$m1_formal_run/m1/formal_${result_index}_dns.txt"
  done
  m1_tcp_fixture="$(find "$m0_run/m0" -type f -name '*tcp*.json' | head -n 1)"
  m1_udp_fixture="$(find "$m0_run/m0" -type f -name '*udp*.json' | head -n 1)"
  [[ -f "$m1_tcp_fixture" && -f "$m1_udp_fixture" ]] || \
    die "self-test: M1 formal result fixtures unavailable"
  for ((result_index = 1; result_index <= 302; result_index++)); do
    printf -v result_label '%03d' "$result_index"
    cp "$m1_tcp_fixture" "$m1_formal_run/m1/formal_tcp_${result_label}.json"
    printf '%s\n' \
      "2026-07-17T00:00:00Z	m1 phase complete cycle=1 phase=tcp-$result_index" \
      >>"$m1_formal_run/events.tsv"
  done
  for ((result_index = 1; result_index <= 30; result_index++)); do
    printf -v result_label '%02d' "$result_index"
    cp "$m1_udp_fixture" "$m1_formal_run/m1/formal_udp_${result_label}.json"
    printf '%s\n' \
      "2026-07-17T00:00:00Z	m1 phase complete cycle=1 phase=udp-$result_index" \
      >>"$m1_formal_run/events.tsv"
  done
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes' \
    '2026-07-17T00:00:00Z,idle-1,40000,15,11,61440,0,0' \
    '2026-07-17T02:00:00Z,idle-2,41000,16,12,61440,0,0' \
    '2026-07-17T04:00:00Z,idle-3,42000,17,13,61440,0,0' \
    '2026-07-17T08:00:00Z,final,41000,16,12,61440,0,0' \
    >"$m1_formal_run/m1-checkpoints.csv"
  printf '%s\n' \
    'timestamp,pid,rss_kib,cpu_percent,state,elapsed,fd_count,thread_rows' \
    >"$m1_formal_run/process.csv"
  printf '%s\n' \
    'timestamp,interface,mtu,ipkts,ierrs,ibytes,opkts,oerrs,obytes,collisions' \
    >"$m1_formal_run/interface.csv"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    >"$m1_formal_run/network.csv"
  : >"$m1_formal_run/mini_vpn.log"
  for ((sample_index = 1; sample_index <= 900; sample_index++)); do
    printf '%s\n' \
      "2026-07-17T00:00:00Z,1,41000,1.0,S,01:00,$((15 + sample_index % 2)),$((11 + sample_index % 2))" \
      >>"$m1_formal_run/process.csv"
    printf '%s\n' \
      "2026-07-17T00:00:00Z,utun42,1200,$sample_index,0,$((sample_index * 64)),$sample_index,0,$((sample_index * 64)),0" \
      >>"$m1_formal_run/interface.csv"
    printf '%s\n' \
      "2026-07-17T00:00:00Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,$sample_index,0,$((sample_index * 64)),$sample_index,0,$((sample_index * 64)),0,512,512" \
      >>"$m1_formal_run/network.csv"
    printf '%s\n' \
      '📊 TUIC endpoint pacing global conservation(available=61440B,live=0B,outstanding=0B,records=2)' \
      >>"$m1_formal_run/mini_vpn.log"
  done
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: complete formal M1 evidence did not meet its SLO"
  grep -Fq -- '- formal_m1_acceptance: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: complete formal M1 evidence lost its acceptance verdict"
  grep -Fq -- '- internal_failure_scan: NO_KNOWN_INTERNAL_FAILURE_SIGNAL' \
    "$m1_formal_run/summary.md" || \
    die "self-test: complete formal M1 evidence unexpectedly required review"

  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.clean.log"
  printf '%s\n' \
    '🔎 tcp-d16-relay-close handle=SocketHandle(16) epoch=109 terminal_direction=none terminal_reason=clean_queue_lifecycle writer_progress_events=1 writer_progress_bytes=37 writer_wait_max_us=178 half_closed_idle_blocked_events=0 half_closed_idle_blocked_max_payload_bytes=0 queue_queued=0 queue_leased=19456 queue_reserved=0 queue_closed=true' \
    '🔎 tcp-local-eof-close handle=SocketHandle(16) direction=remote_to_local reason=remote_eof send_queue=19456 tcp_state=Established active=true can_send=true can_recv=false may_send=true may_recv=true' \
    '🔎 tcp-handle-close handle=SocketHandle(16) direction=remote_to_local reason=remote_eof state=Closing pending=0 permit_terminal_drop_bytes=0 permit_terminal_drop_events=0 terminal_pending_reap_bytes=0 tcp_state=Closed active=false send_queue=0 recv_queue=0' \
    >>"$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: proven post-relay egress drain invalidated the M1 SLO"
  printf '%s\n' \
    '🔎 tcp-d16-relay-close handle=SocketHandle(17) epoch=113 terminal_direction=none terminal_reason=clean_queue_lifecycle writer_progress_events=4 writer_progress_bytes=2036 writer_wait_max_us=391 half_closed_idle_blocked_events=0 half_closed_idle_blocked_max_payload_bytes=0 queue_queued=0 queue_leased=24 queue_reserved=0 queue_closed=true' \
    '🔎 tcp-local-eof-close handle=SocketHandle(17) direction=remote_to_local reason=remote_eof send_queue=24 tcp_state=CloseWait active=true can_send=true can_recv=false may_send=true may_recv=false' \
    '🔎 tcp-handle-close handle=SocketHandle(17) direction=remote_to_local reason=remote_eof state=Closing pending=0 pending_high=3735 remote_to_global_rx_bytes=0 terminal_late_remote_payload_bytes=0 terminal_late_remote_payload_events=0 flush_attempts=4 no_send_capacity=0 send_window_samples=4 send_capacity_min=1048576 send_capacity_max=1048576 send_queue_max=1374 recv_queue_max=1160 may_send_false=0 may_recv_false=1 no_send_capacity_streak_max=0 no_send_capacity_pending_max=0 send_slice_calls=4 send_slice_accepted=7660 actor_admitted_bytes=7660 actor_bypass_admitted_bytes=0 permit_terminal_drop_bytes=0 permit_terminal_drop_events=0 send_slice_zero=0 send_slice_errors=0 budget_limited_calls=0 headroom_limited_calls=0 headroom_deferred_bytes=0 drain_credit_granted_bytes=7636 drain_credit_planned_bytes=0 drain_credit_used_bytes=0 drop_credit_debt_bytes=0 drop_credit_debt_paid_bytes=0 drop_credit_blocked_bytes=0 pressure_credit_debt_bytes=0 pressure_credit_debt_paid_bytes=0 pressure_credit_blocked_bytes=0 hard_edge_guard_bytes=15340 hard_edge_guard_limited_calls=0 hard_edge_guard_deferred_bytes=0 send_slice_max_accepted=3735 tun_flush_tx_calls=0 tun_flush_tx_failures=0 tun_flush_deferred=0 close_pending_class=none close_pending_bytes=0 terminal_pending_reap_bytes=0 close_egress_class=terminal_closed_no_send close_egress_bytes=24 close_egress_drain_candidate=false tcp_state=Closed active=false can_send=false can_recv=false may_send=false may_recv=false send_capacity=1048576 send_queue=24 recv_queue=0' \
    >>"$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: proven terminal local-close ownership transfer invalidated the M1 SLO"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.terminal.log"
  sed -i '' \
    '/tcp-handle-close handle=SocketHandle[(]17[)]/ s/permit_terminal_drop_bytes=0/permit_terminal_drop_bytes=1/' \
    "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: terminal local-close permit drop met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.terminal.log" "$m1_formal_run/mini_vpn.log"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.terminal.log"
  sed -i '' \
    '/tcp-handle-close handle=SocketHandle[(]17[)]/ s/close_egress_bytes=24/close_egress_bytes=23/' \
    "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: mismatched terminal local-close egress met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.terminal.log" "$m1_formal_run/mini_vpn.log"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.terminal.log"
  sed -i '' \
    '/tcp-handle-close handle=SocketHandle[(]17[)]/ s/can_send=false/can_send=true/' \
    "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: send-capable terminal local-close egress met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.terminal.log" "$m1_formal_run/mini_vpn.log"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.drained.log"
  sed -i '' '$d' "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: incomplete post-relay egress drain met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.drained.log" "$m1_formal_run/mini_vpn.log"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.drained.log"
  sed -i '' \
    's/reason=remote_eof send_queue=19456/reason=remote_eof send_queue=19455/' \
    "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: mismatched post-relay egress ownership met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.drained.log" "$m1_formal_run/mini_vpn.log"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.drained.log"
  awk '
    /tcp-handle-close handle=SocketHandle[(]16[)]/ {
      print "🔎 tcp-relay-engine handle=SocketHandle(16) epoch=111 engine=d16_byte_owned"
    }
    { print }
  ' "$m1_formal_run/mini_vpn.log" >"$m1_formal_run/mini_vpn.log.reused"
  mv "$m1_formal_run/mini_vpn.log.reused" "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: cross-epoch post-relay drain evidence met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.drained.log" "$m1_formal_run/mini_vpn.log"
  mv "$m1_formal_run/mini_vpn.clean.log" "$m1_formal_run/mini_vpn.log"
  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.clean.log"
  printf '%s\n' \
    '🔎 tcp-d16-relay-close handle=SocketHandle(16) epoch=109 terminal_direction=none terminal_reason=clean_queue_lifecycle queue_queued=0 queue_leased=0 queue_reserved=0 queue_closed=false' \
    >>"$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: open terminal D16 queue met the M1 SLO"
  mv "$m1_formal_run/mini_vpn.clean.log" "$m1_formal_run/mini_vpn.log"

  m1_diagnostic_formal_run="$tmp/m1-diagnostic-formal-run"
  cp -R "$m1_formal_run" "$m1_diagnostic_formal_run"
  printf '%s\n' diagnostic >"$m1_diagnostic_formal_run/m1-mode"
  printf '%s\n' diagnostic_complete_with_violations \
    >"$m1_diagnostic_formal_run/m1.status"
  sed -i '' 's/	m1 /	m1-diagnostic /' \
    "$m1_diagnostic_formal_run/events.tsv"
  printf '%s\n' \
    $'timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence' \
    $'2026-07-17T00:00:00Z\t1\tudp-reverse\tudp_loss_percent\t3.000001\tlimit=3.0\tm1/formal_udp_01.json' \
    >"$m1_diagnostic_formal_run/m1-diagnostic-violations.tsv"
  jq '.end.sum.lost_percent = 3.000001' \
    "$m1_diagnostic_formal_run/m1/formal_udp_01.json" \
    >"$m1_diagnostic_formal_run/m1/formal_udp_01.json.updated"
  mv "$m1_diagnostic_formal_run/m1/formal_udp_01.json.updated" \
    "$m1_diagnostic_formal_run/m1/formal_udp_01.json"
  jq '.end.sum_sent.bytes = 20000000 | .end.sum_received.bytes = 1' \
    "$m1_diagnostic_formal_run/m1/formal_tcp_001.json" \
    >"$m1_diagnostic_formal_run/m1/formal_tcp_001.json.updated"
  mv "$m1_diagnostic_formal_run/m1/formal_tcp_001.json.updated" \
    "$m1_diagnostic_formal_run/m1/formal_tcp_001.json"
  SOAK_STAGE=m1-diagnostic
  SOAK_CONTINUE_DATA_QUALITY=1
  SOAK_VIOLATIONS_FILE="$m1_diagnostic_formal_run/m1-diagnostic-violations.tsv"
  record_m1_diagnostic_aggregate_violations "$m1_diagnostic_formal_run" || \
    die "self-test: diagnostic aggregate TCP gap was not recorded"
  SOAK_STAGE=m0
  SOAK_LABEL=M0
  SOAK_SUCCESS_STATUS=complete
  SOAK_CONTINUE_DATA_QUALITY=0
  SOAK_VIOLATIONS_FILE=
  write_summary "$m1_diagnostic_formal_run"
  grep -Fq -- '- m1_mode: diagnostic' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: complete diagnostic M1 mode missing from summary"
  grep -Fq -- '- formal_m1_acceptance: NOT_APPLICABLE' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: diagnostic M1 was treated as formal acceptance"
  grep -Fq -- '- m1_result_integrity_evidence: PASS' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: valid diagnostic result evidence lost integrity"
  grep -Fq -- '- m1_result_evidence: NOT_APPLICABLE' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: diagnostic result was evaluated as formal M1"
  grep -Fq -- '- m1_slo_evidence: NOT_APPLICABLE' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: diagnostic M1 produced a formal SLO verdict"
  grep -Fq -- '- m1_diagnostic_violation_count: 2' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: diagnostic violation count mismatch"
  grep -Fq -- '- m1_diagnostic_safety_evidence: PASS' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: safe complete diagnostic M1 evidence rejected"
  cp "$m1_diagnostic_formal_run/m1-diagnostic-violations.tsv" \
    "$m1_diagnostic_formal_run/m1-diagnostic-violations.valid.tsv"
  awk '$4 != "udp_loss_percent"' FS=$'\t' OFS=$'\t' \
    "$m1_diagnostic_formal_run/m1-diagnostic-violations.valid.tsv" \
    >"$m1_diagnostic_formal_run/m1-diagnostic-violations.tsv"
  write_summary "$m1_diagnostic_formal_run"
  grep -Fq -- '- m1_diagnostic_safety_evidence: MISMATCH' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: diagnostic violation missing its source coverage was accepted"
  mv "$m1_diagnostic_formal_run/m1-diagnostic-violations.valid.tsv" \
    "$m1_diagnostic_formal_run/m1-diagnostic-violations.tsv"
  printf '%s\n' \
    '📊 数据面 pump_full_waits=1 pump_read_errors=0 tun_flush_tx_failures=0' \
    >>"$m1_diagnostic_formal_run/mini_vpn.log"
  write_summary "$m1_diagnostic_formal_run"
  grep -Fq -- '- m1_diagnostic_safety_evidence: MISMATCH' \
    "$m1_diagnostic_formal_run/summary.md" || \
    die "self-test: diagnostic safety failure was continued"

  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.clean.log"
  printf '%s\n' \
    '写入上游流失败 direction=local_to_remote err=Stopped(0)' \
    'tcp-handle-close conn=1 epoch=1 reason=remote_write_failed terminal_pending_reap_bytes=0 send_slice_errors=0 tun_flush_tx_failures=0' \
    'tcp-d16-relay-close conn=1 epoch=1 terminal_reason=remote_write_failed queue_queued=0 queue_leased=0 queue_reserved=0 queue_closed=true' \
    >>"$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_remote_write_evidence: CLASSIFIED_REVIEW' \
    "$m1_formal_run/summary.md" || \
    die "self-test: clean M1 Stopped(0) boundary was not classified"
  grep -Fq -- '- m1_slo_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: clean M1 Stopped(0) boundary invalidated the SLO"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m1_formal_run/summary.md" || \
    die "self-test: classified M1 Stopped(0) boundary was hidden from review"
  sed -i '' 's/queue_queued=0/queue_queued=1/' "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_remote_write_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: M1 remote close with queued ownership was classified clean"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_formal_run/summary.md" || \
    die "self-test: M1 remote close with queued ownership met the SLO"
  mv "$m1_formal_run/mini_vpn.clean.log" "$m1_formal_run/mini_vpn.log"

  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.clean.log"
  printf '%s\n' \
    '🔄 tuic-endpoint-rebind generation=1 old_local=0.0.0.0:60000 new_local=0.0.0.0:60001 active_tcp=2 udp_active=false live_connections=2 stalled_ms=2000 bound_ms=2000 tx_since_rx=65536B max_rtt_ms=164' \
    '✅ tuic-endpoint-rebind-recovered generation=1 first_rx_ms=7000 socket_generation=1' \
    >>"$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- endpoint_rebind_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: complete M1 endpoint rebind was not accepted"
  grep -Fq -- '- m1_slo_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: M1 endpoint recovery at 7000ms invalidated the SLO"
  sed -i '' 's/first_rx_ms=7000/first_rx_ms=7001/' "$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_formal_run/summary.md" || \
    die "self-test: M1 endpoint recovery above 7000ms met the SLO"
  mv "$m1_formal_run/mini_vpn.clean.log" "$m1_formal_run/mini_vpn.log"

  cp "$m1_formal_run/m1/formal_tcp_001.json" \
    "$m1_formal_run/formal_tcp_001.valid.json"
  jq '.end.sum_sent.bytes = 16777217 | .end.sum_received.bytes = 0' \
    "$m1_formal_run/formal_tcp_001.valid.json" \
    >"$m1_formal_run/m1/formal_tcp_001.json"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_formal_run/summary.md" || \
    die "self-test: M1 TCP gap above 16MiB was accepted"
  mv "$m1_formal_run/formal_tcp_001.valid.json" \
    "$m1_formal_run/m1/formal_tcp_001.json"

  cp "$m1_formal_run/m1/formal_udp_01.json" \
    "$m1_formal_run/formal_udp_01.valid.json"
  jq '.end.sum.lost_percent = 3.000001' \
    "$m1_formal_run/formal_udp_01.valid.json" \
    >"$m1_formal_run/m1/formal_udp_01.json"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_formal_run/summary.md" || \
    die "self-test: M1 UDP loss above three percent was accepted"
  mv "$m1_formal_run/formal_udp_01.valid.json" \
    "$m1_formal_run/m1/formal_udp_01.json"

  cp "$m1_formal_run/m1/formal_tcp_001.json" \
    "$m1_formal_run/formal_tcp_001.valid.json"
  jq 'if .start.test_start.reverse == 0 then
      .intervals[0].sum.bits_per_second = 0
    else
      .server_output_json.intervals[0].sum.bits_per_second = 0
    end' \
    "$m1_formal_run/formal_tcp_001.valid.json" \
    >"$m1_formal_run/m1/formal_tcp_001.json"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_sender_zero_intervals: 1' "$m1_formal_run/summary.md" || \
    die "self-test: M1 sender-only zero interval was not preserved"
  grep -Fq -- '- m1_result_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: M1 sender-only zero invalidated receiver evidence"
  grep -Fq -- '- m1_slo_evidence: PASS' "$m1_formal_run/summary.md" || \
    die "self-test: M1 sender-only zero invalidated the formal SLO"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$m1_formal_run/summary.md" || \
    die "self-test: M1 sender-only zero was hidden from review"
  mv "$m1_formal_run/formal_tcp_001.valid.json" \
    "$m1_formal_run/m1/formal_tcp_001.json"

  cp "$m1_formal_run/mini_vpn.log" "$m1_formal_run/mini_vpn.valid.log"
  printf '%s\n' \
    '⚠️ tuic-endpoint-rebind-failed generation=1 active_tcp=1 udp_active=false live_connections=1 stalled_ms=2000 bound_ms=2000 tx_since_rx=1024B max_rtt_ms=100 error=bind-failed' \
    >>"$m1_formal_run/mini_vpn.log"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_formal_run/summary.md" || \
    die "self-test: incomplete M1 endpoint rebind was accepted"
  mv "$m1_formal_run/mini_vpn.valid.log" "$m1_formal_run/mini_vpn.log"

  cp "$m1_formal_run/process.csv" "$m1_formal_run/process.valid.csv"
  sed -i '' '$d' "$m1_formal_run/process.csv"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_sample_coverage_evidence: MISMATCH' \
    "$m1_formal_run/summary.md" || \
    die "self-test: M1 sample coverage below 900 was accepted"
  mv "$m1_formal_run/process.valid.csv" "$m1_formal_run/process.csv"

  printf '%s\n' '2026-07-17T00:00:00Z	watchdog compacted mini_vpn.log' \
    >>"$m1_formal_run/events.tsv"
  write_summary "$m1_formal_run"
  grep -Fq -- '- m1_slo_evidence: MISMATCH' "$m1_formal_run/summary.md" || \
    die "self-test: lossy M1 log compaction was accepted"
  sed -i '' '$d' "$m1_formal_run/events.tsv"

  printf '%s\n' \
    '{"start":{"test_start":{"protocol":"TCP"}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bits_per_second":1}}}' \
    >"$tmp/m0-positive-rate-missing-bytes.json"
  ! validate_m0_iperf_result "$tmp/m0-positive-rate-missing-bytes.json" TCP 0 || \
    die "self-test: M0 result missing sender/receiver byte evidence was accepted"

  printf '%s\n' \
    '{"start":{"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":0}}],"end":{"sum_sent":{"bytes":100},"sum_received":{"bytes":100,"bits_per_second":1}},"server_output_json":{"start":{"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bytes":100,"bits_per_second":1}}}}' \
    >"$tmp/m0-forward-receiver-continuous.json"
  validate_m0_iperf_result "$tmp/m0-forward-receiver-continuous.json" TCP 0 || \
    die "self-test: continuous forward receiver was rejected because sender stalled"
  jq '.server_output_json.intervals[0].sum.bits_per_second = 0' \
    "$tmp/m0-forward-receiver-continuous.json" \
    >"$tmp/m0-forward-receiver-stalled.json"
  [[ "$(m0_iperf_result_failure_reason \
    "$tmp/m0-forward-receiver-stalled.json" TCP 0)" == "receiver_zero_interval" ]] || \
    die "self-test: zero forward receiver interval was not classified"
  printf '%s\n' \
    '{"start":{"test_start":{"protocol":"TCP","reverse":0,"duration":1}},"intervals":[{"sum":{"start":0,"end":1,"bits_per_second":1}}],"end":{"sum_sent":{"bytes":100},"sum_received":{"bytes":100,"bits_per_second":1}},"server_output_json":{"start":{"test_start":{"protocol":"TCP","reverse":0,"duration":1}},"intervals":[{"sum":{"start":0,"end":1,"bits_per_second":1}},{"sum":{"start":1.001,"end":1.165,"bits_per_second":0}}],"end":{"sum_received":{"bytes":100,"bits_per_second":1,"seconds":1.165}}}}' \
    >"$tmp/m0-forward-zero-partial-tail.json"
  [[ "$(m0_iperf_result_failure_reason \
    "$tmp/m0-forward-zero-partial-tail.json" TCP 0)" == "ok" ]] || \
    die "self-test: zero short receiver command tail was treated as a complete interval"
  mkdir "$tmp/m0-partial-tail-envelope"
  cp "$tmp/m0-forward-zero-partial-tail.json" \
    "$tmp/m0-partial-tail-envelope/tcp-forward.json"
  read -r _ _ _ _ partial_tail_invalid _ partial_tail_receiver_zero \
    <<<"$(m0_result_envelope "$tmp/m0-partial-tail-envelope")"
  [[ "$partial_tail_invalid" == "0" && \
    "$partial_tail_receiver_zero" == "0" ]] || \
    die "self-test: zero short receiver command tail polluted the result envelope"
  jq '.server_output_json.intervals[1].sum.end = 2.001' \
    "$tmp/m0-forward-zero-partial-tail.json" \
    >"$tmp/m0-forward-zero-complete-window.json"
  [[ "$(m0_iperf_result_failure_reason \
    "$tmp/m0-forward-zero-complete-window.json" TCP 0)" == \
    "receiver_zero_interval" ]] || \
    die "self-test: zero complete receiver interval was hidden as a command tail"
  jq 'del(.server_output_json.intervals[1].sum.start,
      .server_output_json.intervals[1].sum.end)' \
    "$tmp/m0-forward-zero-partial-tail.json" \
    >"$tmp/m0-forward-zero-missing-timing.json"
  [[ "$(m0_iperf_result_failure_reason \
    "$tmp/m0-forward-zero-missing-timing.json" TCP 0)" == \
    "receiver_zero_interval" ]] || \
    die "self-test: zero receiver interval without timing proof did not fail closed"
  jq '.server_output_json.intervals += [
      {"sum":{"start":1.165,"end":2.165,"bits_per_second":1}}]' \
    "$tmp/m0-forward-zero-partial-tail.json" \
    >"$tmp/m0-forward-zero-nonterminal-partial.json"
  [[ "$(m0_iperf_result_failure_reason \
    "$tmp/m0-forward-zero-nonterminal-partial.json" TCP 0)" == \
    "receiver_zero_interval" ]] || \
    die "self-test: nonterminal zero receiver interval was hidden as a command tail"
  jq 'del(.server_output_json)' "$tmp/m0-forward-receiver-continuous.json" \
    >"$tmp/m0-forward-missing-receiver.json"
  [[ "$(m0_iperf_result_failure_reason \
    "$tmp/m0-forward-missing-receiver.json" TCP 0)" == "missing_receiver_evidence" ]] || \
    die "self-test: missing forward receiver evidence was not classified"

  m0_fail_run="$tmp/m0-fail-run"
  mkdir -p "$m0_fail_run/m0"
  printf 'timestamp\tevent\n' >"$m0_fail_run/events.tsv"
  cat >"$fake_iperf" <<'EOF_FAIL_IPERF'
#!/usr/bin/env bash
printf '%s\n' '{"start":{"test_start":{"protocol":"TCP"}},"intervals":[{"sum":{"bits_per_second":0}}],"end":{"sum_received":{"bits_per_second":0}}}'
exit 0
EOF_FAIL_IPERF
  chmod +x "$fake_iperf"
  ! run_m0_schedule "$m0_fail_run" "$m0_profile" || \
    die "self-test: M0 workload failure was swallowed"
  grep -Fxq failed "$m0_fail_run/m0.status" || \
    die "self-test: failed M0 schedule status mismatch"
  grep -Fq 'm0 failed' "$m0_fail_run/events.tsv" || \
    die "self-test: failed M0 schedule event missing"
  grep -Fq 'm0 phase failed cycle=1 phase=tcp-forward' "$m0_fail_run/events.tsv" || \
    die "self-test: zero-traffic iperf result was not classified at its phase"
  ! grep -Fq 'm0 idle start' "$m0_fail_run/events.tsv" || \
    die "self-test: failed M0 schedule continued into idle"

  M0_TOTAL_SECS=7200
  M0_TCP_SECS=300
  M0_UDP_SECS=180
  M0_SHORT_SECS=10
  M0_SHORT_COUNT=6
  M0_IDLE_SECS=300
  M0_FINAL_DRAIN_SECS=120
  validate_m0_formal_config || die "self-test: formal M0 schedule rejected"
  M0_TOTAL_SECS=7199
  ! validate_m0_formal_config || die "self-test: shortened formal M0 schedule accepted"
  M0_TOTAL_SECS=7200
  M1_TOTAL_SECS=28800
  M2_TOTAL_SECS=86400
  M2_STEADY_A_SECS=14400
  M2_IDLE_SECS=600
  M2_QUIET_A_SECS=10800
  M2_STEADY_B_SECS=14400
  M2_CHURN_SECS=10800
  M2_QUIET_B_SECS=10800
  M2_STEADY_C_SECS=21600
  M2_FINAL_DRAIN_SECS=600
  M2_TCP_SECS=300
  M2_UDP_SECS=180
  M2_SHORT_SECS=10
  M2_STEADY_SHORT_COUNT=6
  M2_QUIET_SHORT_COUNT=6
  M2_CHURN_SHORT_COUNT=24
  validate_m2_formal_config || die "self-test: formal M2 schedule rejected"
  m2_execution_requires_paired_observer qualification || \
    die "self-test: M2 qualification did not require paired Exit evidence"
  m2_execution_requires_paired_observer formal || \
    die "self-test: formal M2 did not require paired Exit evidence"
  ! m2_execution_requires_paired_observer unknown || \
    die "self-test: unknown M2 execution mode acquired observer authority"
  m2_exit_observer_ssh_host_matches_exit \
    ubuntu@43.153.32.33 43.153.32.33 || \
    die "self-test: matching formal M2 Exit SSH host rejected"
  ! m2_exit_observer_ssh_host_matches_exit \
    ubuntu@43.130.32.77 43.153.32.33 || \
    die "self-test: wrong formal M2 Exit SSH host accepted"
  observer_status=$'schema=knife15-exit-target-observer-v2\nstatus=active\nobserver_healthy=1\ntarget=43.130.32.77\niperf_port=5201\ntuic_port=8443\ntimeout_secs=93600\nelapsed_secs=0'
  m2_exit_observer_status_is_valid \
    "$observer_status" 43.130.32.77 5201 8443 || \
    die "self-test: matching formal M2 Exit observer rejected"
  ! m2_exit_observer_status_is_valid \
    "${observer_status/observer_healthy=1/observer_healthy=0}" \
    43.130.32.77 5201 8443 || \
    die "self-test: unhealthy formal M2 Exit observer accepted"
  ! m2_exit_observer_status_is_valid \
    "${observer_status/timeout_secs=93600/timeout_secs=7200}" \
    43.130.32.77 5201 8443 || \
    die "self-test: short-lived formal M2 Exit observer accepted"
  ! m2_exit_observer_status_is_valid \
    "${observer_status/elapsed_secs=0/elapsed_secs=901}" \
    43.130.32.77 5201 8443 || \
    die "self-test: stale formal M2 Exit observer accepted"
  observer_run="$tmp/m2-exit-observer-run"
  observer_fake="$tmp/fake-exit-observer"
  observer_calls="$tmp/exit-observer.calls"
  mkdir "$observer_run"
  cat >"$observer_fake" <<'EOF_FAKE_EXIT_OBSERVER'
#!/usr/bin/env bash
printf '%s\n' "$1" >>"$M2_EXIT_OBSERVER_TEST_CALLS"
case "$1" in
  status)
    printf '%s\n' \
      'schema=knife15-exit-target-observer-v2' \
      'status=active' \
      'observer_healthy=1' \
      'target=43.130.32.77' \
      'iperf_port=5201' \
      'tuic_port=8443' \
      'timeout_secs=93600' \
      'elapsed_secs=0'
    ;;
  freeze)
    printf '%s\n' 'PASS: Exit observer frozen' \
      'run_dir=/tmp/mini_vpn_knife15_exit_target_observer_20260811_120000'
    ;;
  bundle)
    printf '%s\n' 'PASS: Exit observer bundle finalized' \
      'bundle=/tmp/mini_vpn_knife15_exit_target_observer_20260811_120000.tar.gz' \
      'sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ;;
  *) exit 1 ;;
esac
EOF_FAKE_EXIT_OBSERVER
  chmod +x "$observer_fake"
  export M2_EXIT_OBSERVER_TEST_CALLS="$observer_calls"
  original_m2_exit_observer_script="$M2_EXIT_OBSERVER_SCRIPT"
  M2_EXIT_OBSERVER_SCRIPT="$observer_fake"
  m2_exit_observer_require_active \
    "$observer_run" 43.130.32.77 5201 8443 || \
    die "self-test: formal M2 active Exit observer preflight failed"
  [[ "$(tr '\n' ' ' <"$observer_calls")" == "status " ]] || \
    die "self-test: formal M2 observer preflight did not use status"
  ! m2_exit_observer_require_active \
    "$observer_run" 43.130.32.77 5201 '' || \
    die "self-test: missing formal M2 TUIC port was accepted"
  grep -Fxq 'ERROR: invalid formal M2 Exit observer TUIC port: <missing>' \
    "$observer_run/m2-exit-observer-status.txt" || \
    die "self-test: invalid observer input did not leave readable evidence"
  : >"$observer_calls"
  m2_exit_observer_freeze_and_bundle \
    "$observer_run" workload-failed 43.130.32.77 5201 8443 || \
    die "self-test: formal M2 Exit observer finalization failed"
  [[ "$(tr '\n' ' ' <"$observer_calls")" == "freeze bundle " ]] || \
    die "self-test: formal M2 did not freeze before observer bundling"
  grep -Fxq 'reason=workload-failed' \
    "$observer_run/m2-exit-observer-finalization.txt" || \
    die "self-test: formal M2 observer finalization reason missing"
  grep -Fxq \
    'sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
    "$observer_run/m2-exit-observer-finalization.txt" || \
    die "self-test: formal M2 observer bundle checksum missing"
  observer_exit_run="$tmp/m2-exit-observer-exit-run"
  mkdir "$observer_exit_run"
  printf 'timestamp\tevent\n' >"$observer_exit_run/events.tsv"
  : >"$observer_calls"
  (
    m2_exit_observer_arm \
      "$observer_exit_run" 43.130.32.77 5201 8443
    trap 'm2_exit_observer_exit_trap "$?"' EXIT
    exit 7
  )
  observer_exit_status=$?
  [[ "$observer_exit_status" == "7" ]] || \
    die "self-test: formal M2 observer EXIT trap changed primary status"
  [[ "$(tr '\n' ' ' <"$observer_calls")" == "freeze bundle " ]] || \
    die "self-test: formal M2 unexpected exit did not freeze and bundle"
  grep -Fxq 'reason=unexpected-exit' \
    "$observer_exit_run/m2-exit-observer-finalization.txt" || \
    die "self-test: formal M2 unexpected-exit reason missing"
  M2_EXIT_OBSERVER_SCRIPT="$original_m2_exit_observer_script"
  ! m2_source_is_accepted 9791c32 || \
    die "self-test: source without bounded resource evidence was accepted"
  m2_source_is_accepted 60a4e94 || \
    die "self-test: reviewed bounded resource evidence was rejected"
  stale_release_binary="$tmp/stale-release-binary"
  cp "$BIN" "$stale_release_binary"
  touch -t 200001010000 "$stale_release_binary"
  ! release_binary_is_fresh "$stale_release_binary" || \
    die "self-test: stale release binary was accepted"
  touch "$stale_release_binary"
  release_binary_is_fresh "$stale_release_binary" || \
    die "self-test: fresh release binary was rejected"
  exact_source_repo="$tmp/exact-source-repo"
  mkdir "$exact_source_repo"
  git -C "$exact_source_repo" init -q
  printf '%s\n' clean >"$exact_source_repo/tracked.txt"
  git -C "$exact_source_repo" add tracked.txt
  git -C "$exact_source_repo" -c user.name=knife15 \
    -c user.email=knife15@example.invalid commit -qm fixture
  m2_worktree_is_clean "$exact_source_repo" || \
    die "self-test: clean exact-source worktree was rejected"
  printf '%s\n' 'fn main() {}' >"$exact_source_repo/build.rs"
  ! m2_worktree_is_clean "$exact_source_repo" || \
    die "self-test: untracked build input was accepted as exact source"
  rm "$exact_source_repo/build.rs"
  printf '%s\n' dirty >>"$exact_source_repo/tracked.txt"
  ! m2_worktree_is_clean "$exact_source_repo" || \
    die "self-test: dirty exact-source worktree was accepted"
  M2_EGRESS_TARGET=api.ipify.org:444
  ! validate_m2_formal_config || \
    die "self-test: M2 controlled target drift was accepted"
  M2_EGRESS_TARGET=api.ipify.org:443
  [[ "$(m2_formal_count_model)" == "93 934 95 1029" ]] || \
    die "self-test: M2 formal schedule count model mismatch"
  M2_TOTAL_SECS=86399
  ! validate_m2_formal_config || die "self-test: shortened formal M2 schedule accepted"
  M2_TOTAL_SECS=86400
  [[ "$(m2_formal_acceptance_from_values \
    complete PASS active 0 NOT_APPLICABLE)" == "PENDING_CLEANUP" ]] || \
    die "self-test: live M2 was not held pending cleanup"
  [[ "$(m2_formal_acceptance_from_values \
    complete PASS inactive 1 PASS)" == "PASS" ]] || \
    die "self-test: cleaned M2 acceptance was rejected"
  [[ "$(m2_formal_acceptance_from_values \
    complete PASS inactive 1 MISMATCH)" == "FAIL" ]] || \
    die "self-test: M2 cleanup mismatch was accepted"
  M1_STEADY_A_SECS=7200
  M1_IDLE_SECS=300
  M1_QUIET_SECS=3600
  M1_STEADY_B_SECS=7200
  M1_CHURN_SECS=3600
  M1_STEADY_C_SECS=6000
  M1_FINAL_DRAIN_SECS=300
  M1_TCP_SECS=300
  M1_UDP_SECS=180
  M1_SHORT_SECS=10
  M1_STEADY_SHORT_COUNT=6
  M1_QUIET_SHORT_COUNT=6
  M1_CHURN_SHORT_COUNT=24
  METRICS_SECS=30
  SAMPLE_SECS=30
  validate_m1_formal_config || die "self-test: formal M1 schedule rejected"
  M1_TOTAL_SECS=28799
  ! validate_m1_formal_config || die "self-test: shortened formal M1 schedule accepted"
  M1_TOTAL_SECS=28800
  usage_text="$(usage)"
  grep -Fq 'scripts/knife15-macos-soak.sh direct-discriminator' <<<"$usage_text" || \
    die "self-test: public direct continuity action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh m2-qualification' <<<"$usage_text" || \
    die "self-test: public M2 qualification action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh baseline-check' <<<"$usage_text" || \
    die "self-test: public baseline evidence replay action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh m0' <<<"$usage_text" || \
    die "self-test: public M0 action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh m1' <<<"$usage_text" || \
    die "self-test: public M1 action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh m2' <<<"$usage_text" || \
    die "self-test: public M2 action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh m2-ipv6-check' <<<"$usage_text" || \
    die "self-test: public M2 IPv6 check missing from help"
  grep -Fq 'M0_BASELINE_DIR=' <<<"$usage_text" || \
    die "self-test: M0 baseline requirement missing from help"
  grep -Fq 'M0_DIRECT_DIR=' <<<"$usage_text" || \
    die "self-test: M0 direct continuity requirement missing from help"
  grep -Fq 'M1_BASELINE_DIR=' <<<"$usage_text" || \
    die "self-test: M1 baseline requirement missing from help"
  grep -Fq 'M1_DIRECT_DIR=' <<<"$usage_text" || \
    die "self-test: M1 direct continuity requirement missing from help"
  grep -Fq 'M2_BASELINE_DIR=' <<<"$usage_text" || \
    die "self-test: M2 baseline requirement missing from help"
  grep -Fq 'M2_DIRECT_DIR=' <<<"$usage_text" || \
    die "self-test: M2 direct continuity requirement missing from help"

  printf '%s\n' '   gateway: 192.168.50.1' ' interface: en0' >"$route_fixture"
  [[ "$(route_interface_from_text <"$route_fixture")" == "en0" ]] || \
    die "self-test: route interface parser mismatch"
  [[ "$(route_gateway_from_text <"$route_fixture")" == "192.168.50.1" ]] || \
    die "self-test: route gateway parser mismatch"
  cat >"$interface_fixture" <<'EOF_INTERFACE'
Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll
utun42     1200  <Link#23>                         100     0      64000       90     0      60000     0
EOF_INTERFACE
  [[ "$(interface_csv_from_text 2026-07-14T00:00:00Z utun42 <"$interface_fixture")" == \
    "2026-07-14T00:00:00Z,utun42,1200,100,0,64000,90,0,60000,0" ]] || \
    die "self-test: interface counter parser mismatch"
  [[ "$(interface_control_fields_from_text utun42 <"$interface_fixture")" == \
    "1200,100,0,64000,90,0,60000,0" ]] || \
    die "self-test: physical interface control parser mismatch"
  cat >"$interface_fixture" <<'EOF_PHYSICAL_INTERFACE'
Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll
en1        1500  <Link#15>     0a:0b:f8:b0:f8:93  89891762     0  89665814003 41674842     0 16466942968     0
EOF_PHYSICAL_INTERFACE
  [[ "$(interface_control_fields_from_text en1 <"$interface_fixture")" == \
    "1500,89891762,0,89665814003,41674842,0,16466942968,0" ]] || \
    die "self-test: physical interface MAC address shifted counters"

  ping_fixture="$tmp/ping.txt"
  printf '%s\n' \
    '3 packets transmitted, 2 packets received, 33.3% packet loss' \
    'round-trip min/avg/max/stddev = 170.123/180.456/190.789/8.123 ms' \
    >"$ping_fixture"
  [[ "$(ping_control_fields_from_text <"$ping_fixture")" == \
    "3,2,33.300000,170.123,180.456,190.789" ]] || \
    die "self-test: ping control parser mismatch"
  printf '%s\n' '3 packets transmitted, 0 packets received, 100.0% packet loss' \
    >"$ping_fixture"
  [[ "$(ping_control_fields_from_text <"$ping_fixture")" == \
    "3,0,100.000000,unknown,unknown,unknown" ]] || \
    die "self-test: total-loss ping control parser mismatch"

  network_fixture="$tmp/network.csv"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    '2026-07-14T00:00:00Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,100,0,64000,90,0,60000,0,unknown,unknown' \
    '2026-07-14T00:00:30Z,utun42,en0,en0,192.168.50.1,3,2,33.300000,170.123,180.456,190.789,3,3,0.000000,1.000,1.500,2.000,1500,200,1,128000,180,0,120000,0,17066,16000' \
    >"$network_fixture"
  [[ "$(network_control_envelope "$network_fixture")" == \
    "2 2 0 2 2 0 2 33.300000 180.456 0.000000 1 64000 128000 64000 60000 120000 60000 17066 16000" ]] || \
    die "self-test: network control envelope mismatch"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    '2026-07-14T00:00:00Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,100,16,64000,90,0,60000,0,unknown,unknown' \
    '2026-07-14T00:00:30Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,200,16,128000,180,0,120000,0,17066,16000' \
    >"$network_fixture.cumulative-errors"
  [[ "$(network_control_envelope "$network_fixture.cumulative-errors" | \
    awk '{print $11}')" == "0" ]] || \
    die "self-test: unchanged preexisting physical errors failed the run"
  sed 's/,200,16,128000,180,0,120000,0,17066,16000$/,200,15,128000,180,0,120000,0,17066,16000/' \
    "$network_fixture.cumulative-errors" \
    >"$network_fixture.error-counter-reset"
  [[ "$(network_control_envelope "$network_fixture.error-counter-reset" | \
    awk '{print $11}')" == "1" ]] || \
    die "self-test: physical error counter reset was accepted as continuity"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    '2026-07-14T00:00:00Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,0a:0b:f8:b0:f8:93,100,0,64000,90,0,60000,0,0' \
    '2026-07-14T00:00:30Z,utun42,en0,en0,192.168.50.1,3,3,0.000000,170.000,171.000,172.000,3,3,0.000000,1.000,1.500,2.000,1500,0a:0b:f8:b0:f8:93,200,0,128000,180,0,120000,0,0' \
    >"$network_fixture.malformed"
  [[ "$(network_control_envelope "$network_fixture.malformed")" == \
    "2 2 0 2 2 0 0 0.000000 171.000 0.000000 0 unknown unknown unknown unknown unknown unknown unknown unknown" ]] || \
    die "self-test: shifted physical counters were accepted as network control"

  collector_dir="$tmp/network-collector"
  collector_bin="$tmp/network-collector-bin"
  mkdir -p "$collector_dir" "$collector_bin" "$tmp/network-collector-state"
  cat >"$collector_bin/route" <<'EOF_FAKE_ROUTE'
#!/usr/bin/env bash
if [[ "${3:-}" == "43.153.32.33" ]]; then
  printf '%s\n' 'gateway: 192.168.50.1' 'interface: en0'
else
  printf '%s\n' 'interface: utun42'
fi
EOF_FAKE_ROUTE
  cat >"$collector_bin/ping" <<'EOF_FAKE_PING'
#!/usr/bin/env bash
host=""
for host in "$@"; do :; done
printf '%s\n' \
  "3 packets transmitted, 3 packets received, 0.0% packet loss" \
  "$([[ "$host" == "43.153.32.33" ]] && echo 'round-trip min/avg/max/stddev = 170.000/171.000/172.000/1.000 ms' || echo 'round-trip min/avg/max/stddev = 1.000/1.500/2.000/0.500 ms')"
EOF_FAKE_PING
  cat >"$collector_bin/netstat" <<'EOF_FAKE_NETSTAT'
#!/usr/bin/env bash
printf '%s\n' \
  'Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll' \
  'en0        1500  <Link#15>     0a:0b:f8:b0:f8:93  100     0      64000       90     0      60000     0'
EOF_FAKE_NETSTAT
  chmod +x "$collector_bin/route" "$collector_bin/ping" "$collector_bin/netstat"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    >"$collector_dir/network.csv"
  : >"$collector_dir/network.log"
  printf 'timestamp\tevent\n' >"$collector_dir/events.tsv"
  original_path="$PATH"
  original_state_dir="$STATE_DIR"
  PATH="$collector_bin:$PATH"
  STATE_DIR="$tmp/network-collector-state"
  write_state target 43.130.32.77
  write_state exit_host 43.153.32.33
  sample_network_control_for "$collector_dir" 2026-07-14T00:00:00Z
  network_control_is_recent || die "self-test: fresh network control was rejected"
  write_state network.valid.epoch 1
  ! network_control_is_recent || die "self-test: stale network control was accepted"
  cat >"$collector_bin/netstat" <<'EOF_MALFORMED_NETSTAT'
#!/usr/bin/env bash
printf '%s\n' \
  'Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll' \
  'en0        1500  <Link#15>     0a:0b:f8:b0:f8:93  200     0     128000      180     0     120000  unknown'
EOF_MALFORMED_NETSTAT
  chmod +x "$collector_bin/netstat"
  rm -f "$(state_file network.valid.epoch)"
  sample_network_control_for "$collector_dir" 2026-07-14T00:00:30Z
  [[ ! -e "$(state_file network.valid.epoch)" ]] || \
    die "self-test: malformed physical control refreshed freshness"
  PATH="$original_path"
  STATE_DIR="$original_state_dir"
  [[ "$(awk -F, 'NR == 2 {print NF ":" $2 ":" $3 ":" $4 ":" $5 ":" $8 ":" $10 ":" $14 ":" $20 ":" $23}' \
    "$collector_dir/network.csv")" == \
    "27:utun42:en0:en0:192.168.50.1:0.000000:171.000:0.000000:0:0" ]] || \
    die "self-test: composed network control sample mismatch"
  grep -Fq 'control=exit host=43.153.32.33 route=en0' \
    "$collector_dir/network.log" || die "self-test: raw Exit control evidence missing"

  cat >"$good_log" <<'EOF_GOOD'
🚀 TUN runtime started with pool_size=2, tun_mtu=1200, tun_tx_queue_len_estimate=500
🧺 TUN ingress service: enabled capacity=500 packets source=tun_tx_queue_len
🧱 TCP socket buffers: rx=1048576B tx=1048576B receive_window_limit=368640（fixed）
H10d16 byte-owned egress: enabled per_flow_cap=524288 global_cap=67108864 quantum=131072
🧭 TUIC 拥塞控制器=Cubic | UDP relay mode=Native | QUIC MTU policy=default | QUIC GSO policy=enabled | QUIC UDP send service=quinn | QUIC pacing policy=endpoint-window-v1
📊 TUIC endpoint pacing global conservation(available=61406B,live=0B,outstanding=0B,records=2)
EOF_GOOD
  verify_startup_profile "$good_log" || die "self-test: valid startup fingerprint rejected"
  conservation_check_file "$good_log" || die "self-test: valid conservation sample rejected"

  cp "$good_log" "$bad_log"
  printf '%s\n' '📊 TUIC endpoint pacing global conservation(available=61440B,live=1B,outstanding=0B,records=2)' >>"$bad_log"
  ! conservation_check_file "$bad_log" || die "self-test: conservation violation accepted"
  sed '/QUIC pacing policy=endpoint-window-v1/d' "$good_log" >"$bad_log"
  ! verify_startup_profile "$bad_log" || die "self-test: incomplete startup fingerprint accepted"
  run_logged "$tmp/command-pass.log" bash -c 'printf pass; exit 0' || \
    die "self-test: successful logged command rejected"
  ! run_logged "$tmp/command-fail.log" bash -c 'printf fail; exit 7' || \
    die "self-test: failed logged command accepted"
  ! run_logged "$tmp/missing/command.log" bash -c 'printf pass; exit 0' 2>/dev/null || \
    die "self-test: failed evidence write accepted"

  finalized_run="$tmp/finalized-run"
  finalized_bundle="${finalized_run}.tar.gz"
  mkdir "$finalized_run"
  printf '%s\n' immutable >"$finalized_run/evidence.txt"
  create_bundle_once "$finalized_run" "$finalized_bundle" || \
    die "self-test: initial immutable bundle publication failed"
  bundle_is_finalized "$finalized_run" || \
    die "self-test: valid finalized bundle was not recognized"
  finalized_hash="$(sha256_file "$finalized_bundle")"
  printf '%s\n' later-mutation >"$finalized_run/evidence.txt"
  create_bundle_once "$finalized_run" "$finalized_bundle" || \
    die "self-test: idempotent finalized bundle check failed"
  [[ "$(sha256_file "$finalized_bundle")" == "$finalized_hash" ]] || \
    die "self-test: finalized evidence bundle was overwritten"
  rm "${finalized_bundle}.sha256"
  create_bundle_once "$finalized_run" "$finalized_bundle" || \
    die "self-test: interrupted checksum publication did not recover"
  [[ "$(sha256_file "$finalized_bundle")" == "$finalized_hash" ]] || \
    die "self-test: checksum recovery rewrote the evidence archive"
  printf '%s\n' 'not-a-checksum' >"${finalized_bundle}.sha256"
  ! bundle_is_finalized "$finalized_run" || \
    die "self-test: invalid bundle checksum was accepted as finalized"
  ! create_bundle_once "$finalized_run" "$finalized_bundle" || \
    die "self-test: existing partial/invalid bundle was overwritten"

  clean_scan="$tmp/clean-scan"
  mkdir "$clean_scan"
  printf '%s\n' 'redacted diagnostic' >"$clean_scan/log.txt"
  secret_scan "$clean_scan" || die "self-test: clean evidence rejected"
  secret_scan_dir="$tmp/secret-scan"
  secret_value=123e4567-e89b-12d3-a456-426614174000
  mkdir "$secret_scan_dir"
  printf 'peer=%s\n' "$secret_value" >"$secret_scan_dir/log.txt"
  ! secret_scan "$secret_scan_dir" || die "self-test: UUID-shaped secret accepted"
  ! grep -Fq "$secret_value" "$secret_scan_dir/secret-scan.txt" || \
    die "self-test: secret scan copied the matched value"

  summary_dir="$tmp/summary"
  mkdir "$summary_dir"
  cp "$good_log" "$summary_dir/mini_vpn.log"
  printf '%s\n%s\n%s\n' header sample-1 sample-2 >"$summary_dir/process.csv"
  printf '%s\n%s\n' header event-1 >"$summary_dir/events.tsv"
  printf '%s\n' 'timestamp,interface,mtu,ipkts,ierrs,ibytes,opkts,oerrs,obytes,collisions' \
    >"$summary_dir/interface.csv"
  printf '%s\n' '2026-07-14T00:00:00Z,utun42,1200,1,0,64,1,0,64,0' \
    >>"$summary_dir/interface.csv"
  write_summary "$summary_dir"
  grep -Fq -- '- process_samples: 2' "$summary_dir/summary.md" || \
    die "self-test: summary process count is not macOS-awk compatible"
  grep -Fq -- '- events: 1' "$summary_dir/summary.md" || \
    die "self-test: summary event count is not macOS-awk compatible"
  grep -Fq -- '- interface_error_samples: 0' "$summary_dir/summary.md" || \
    die "self-test: zero-error interface sample was misclassified"
  printf '%s\n' '2026-07-14T00:00:30Z,utun42,1200,2,1,128,2,0,128,0' \
    >>"$summary_dir/interface.csv"
  write_summary "$summary_dir"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$summary_dir/summary.md" || \
    die "self-test: interface error was not marked for review"
  grep -Fq -- '- interface_error_samples: 1' "$summary_dir/summary.md" || \
    die "self-test: interface error count mismatch"
  sed -i '' '$d' "$summary_dir/interface.csv"
  printf '%s\n' \
    '写入上游流失败 direction=local_to_remote err=Stopped(0)' \
    'tcp-handle-close conn=1 epoch=1 reason=remote_write_failed' \
    'tcp-d16-relay-close conn=1 epoch=1 terminal_reason=remote_write_failed queue_queued_bytes=0 queue_leased_bytes=0 queue_reserved_bytes=0' \
    >>"$summary_dir/mini_vpn.log"
  write_summary "$summary_dir"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$summary_dir/summary.md" || \
    die "self-test: remote write failure was not marked for review"
  grep -Fq -- '- remote_write_failures: 1' "$summary_dir/summary.md" || \
    die "self-test: one remote write terminal relay was counted more than once"
  grep -Fq -- '- remote_write_failure_log_matches: 3' "$summary_dir/summary.md" || \
    die "self-test: remote write diagnostic log-match count mismatch"

  STATE_DIR="$tmp/workload-state"
  mkdir "$STATE_DIR"
  /bin/sleep 30 &
  unrelated_pid=$!
  write_state workload.pid "$unrelated_pid"
  write_state workload.command 'bash scripts/knife15-macos-soak.sh m0'
  if terminate_recorded_workload "$summary_dir" 2>/dev/null; then
    kill -TERM "$unrelated_pid" 2>/dev/null || true
    wait "$unrelated_pid" 2>/dev/null || true
    die "self-test: live workload identity mismatch did not block cleanup"
  fi
  kill -0 "$unrelated_pid" 2>/dev/null || {
    wait "$unrelated_pid" 2>/dev/null || true
    die "self-test: workload identity mismatch signaled an unrelated PID"
  }
  kill -TERM "$unrelated_pid" 2>/dev/null || true
  wait "$unrelated_pid" 2>/dev/null || true

  rm -rf "$tmp"
  echo "knife15 macOS runner self-test passed"
}

write_manifest() {
  local run_dir="$1"
  local source_commit owner_uid owner_gid
  source_commit="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
  owner_uid="${SUDO_UID:-$(id -u)}"
  owner_gid="${SUDO_GID:-$(id -g)}"
  cat >"$run_dir/manifest.txt" <<EOF_MANIFEST || die "cannot write run manifest"
stage=Knife15-macOS-M0
started_utc=$(timestamp)
source_commit=$source_commit
binary_sha256=$(sha256_file "$BIN")
runner_sha256=$(sha256_file "$SCRIPT_PATH")
host_os=$(sw_vers -productVersion 2>/dev/null || uname -r)
hardware=$(uname -m)
target=$TARGET
target_route_before=$(route_interface "$TARGET")
target_ready_utc=$TARGET_READY_UTC
target_ready_receiver_bps=$TARGET_READY_RECEIVER_BPS
exit_host=$SERVER_HOST
exit_port=$SERVER_PORT
exit_route_before=$(route_interface "$SERVER_HOST")
binary_path=$BIN
runner_path=$SCRIPT_PATH
dns_target=${DNS_TARGET:-disabled}
dns_name=$DNS_NAME
iperf_port=$IPERF_PORT
smoke_duration_secs=$DURATION
smoke_command_timeout_secs=$((10#$DURATION + 30))
smoke_parallel=$PARALLEL
tun_mtu=1200
tun_ingress_capacity=500
tcp_pool=2
tcp_rx_bytes=1048576
tcp_tx_bytes=1048576
receive_window_limit=368640
cc=cubic
gso_policy=enabled
udp_send_service=quinn
pacing_policy=endpoint-window-v1
h10d16=enabled
network_control_schema=knife15-macos-network-v2
network_control_host=$SERVER_HOST
network_control_ping=3x-icmp-200ms-spacing-1000ms-wait
metrics_secs=$METRICS_SECS
sample_secs=$SAMPLE_SECS
max_log_bytes=$MAX_LOG_BYTES
log_keep_bytes=$LOG_KEEP_BYTES
min_free_kb=$MIN_FREE_KB
m0_total_secs=$M0_TOTAL_SECS
m0_tcp_epoch_secs=$M0_TCP_SECS
m0_udp_epoch_secs=$M0_UDP_SECS
m0_short_epoch_secs=$M0_SHORT_SECS
m0_short_connections_per_cycle=$M0_SHORT_COUNT
m0_idle_secs=$M0_IDLE_SECS
m0_final_drain_secs=$M0_FINAL_DRAIN_SECS
m1_total_secs=$M1_TOTAL_SECS
m1_steady_a_secs=$M1_STEADY_A_SECS
m1_idle_secs=$M1_IDLE_SECS
m1_quiet_secs=$M1_QUIET_SECS
m1_steady_b_secs=$M1_STEADY_B_SECS
m1_churn_secs=$M1_CHURN_SECS
m1_steady_c_secs=$M1_STEADY_C_SECS
m1_final_drain_secs=$M1_FINAL_DRAIN_SECS
m1_tcp_epoch_secs=$M1_TCP_SECS
m1_udp_epoch_secs=$M1_UDP_SECS
m1_short_epoch_secs=$M1_SHORT_SECS
m1_steady_short_connections_per_cycle=$M1_STEADY_SHORT_COUNT
m1_quiet_short_connections_per_cycle=$M1_QUIET_SHORT_COUNT
m1_churn_short_connections_per_cycle=$M1_CHURN_SHORT_COUNT
m1_rate_cap_bps=$M1_RATE_CAP_BPS
m2_total_secs=$M2_TOTAL_SECS
m2_steady_a_secs=$M2_STEADY_A_SECS
m2_idle_secs=$M2_IDLE_SECS
m2_quiet_a_secs=$M2_QUIET_A_SECS
m2_steady_b_secs=$M2_STEADY_B_SECS
m2_churn_secs=$M2_CHURN_SECS
m2_quiet_b_secs=$M2_QUIET_B_SECS
m2_steady_c_secs=$M2_STEADY_C_SECS
m2_final_drain_secs=$M2_FINAL_DRAIN_SECS
m2_tcp_epoch_secs=$M2_TCP_SECS
m2_udp_epoch_secs=$M2_UDP_SECS
m2_short_epoch_secs=$M2_SHORT_SECS
m2_expected_cycles=$M2_EXPECTED_CYCLES
m2_expected_tcp_results=$M2_EXPECTED_TCP_RESULTS
m2_expected_udp_results=$M2_EXPECTED_UDP_RESULTS
m2_expected_phase_results=$M2_EXPECTED_PHASE_RESULTS
m2_expected_checkpoints=$M2_EXPECTED_CHECKPOINTS
owner_uid=$owner_uid
owner_gid=$owner_gid
EOF_MANIFEST
}

cleanup_owned_routes() {
  local run_dir="$1"
  local utun target dns_target current
  if [[ -n "$(read_state m2.full_tunnel 2>/dev/null || true)" ]]; then
    deactivate_m2_full_tunnel "$run_dir" || {
      append_event_to "$run_dir" \
        "m2 full tunnel cleanup failed: owned route or DNS state changed"
      return 1
    }
  fi
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  [[ -n "$utun" ]] || return 0

  if [[ -n "$target" ]]; then
    current="$(route_interface "$target")"
    if [[ "$current" == "$utun" ]]; then
      route -n delete -host "$target" >>"$run_dir/cleanup.log" 2>&1 || true
    fi
  fi
  if [[ -n "$dns_target" ]]; then
    current="$(route_interface "$dns_target")"
    if [[ "$current" == "$utun" ]]; then
      route -n delete -host "$dns_target" >>"$run_dir/cleanup.log" 2>&1 || true
    fi
  fi
}

terminate_recorded_pid() {
  local run_dir="$1"
  local pid i
  pid="$(read_state vpn.pid 2>/dev/null || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  if ! pid_matches_run; then
    if kill -0 "$pid" 2>/dev/null; then
      warn "recorded PID $pid now belongs to another command; refusing to signal it"
      append_event_to "$run_dir" "PID identity mismatch; signal refused"
    fi
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  for ((i = 0; i < 20; i++)); do
    pid_matches_run || return 0
    sleep 1
  done
  if pid_matches_run; then
    warn "mini_vpn did not exit after 20s; sending KILL to the identity-verified PID"
    kill -KILL "$pid" 2>/dev/null || true
  fi
}

clear_workload_state() {
  rm -f "$(state_file workload.pid)" "$(state_file workload.command)" \
    "$(state_file workload.child.pid)"
}

terminate_recorded_workload() {
  local run_dir="$1"
  local workload_pid i
  workload_pid="$(read_state workload.pid 2>/dev/null || true)"
  [[ "$workload_pid" =~ ^[0-9]+$ ]] || return 0
  if ! workload_matches_run; then
    if kill -0 "$workload_pid" 2>/dev/null; then
      warn "recorded workload PID $workload_pid changed identity; refusing to signal it"
      append_event_to "$run_dir" "workload PID identity mismatch; signal refused"
      return 1
    fi
    clear_workload_state
    return 0
  fi
  kill -TERM "$workload_pid" 2>/dev/null || true
  for ((i = 0; i < 10; i++)); do
    workload_matches_run || {
      clear_workload_state
      return 0
    }
    sleep 1
  done
  if workload_matches_run; then
    warn "soak workload did not exit after 10s; sending KILL to the identity-verified PID"
    kill -KILL "$workload_pid" 2>/dev/null || true
    for ((i = 0; i < 2; i++)); do
      workload_matches_run || break
      sleep 1
    done
    if workload_matches_run; then
      append_event_to "$run_dir" "identity-verified workload did not terminate"
      return 1
    fi
  fi
  clear_workload_state
}

terminate_recorded_watchdog() {
  local run_dir="$1"
  local watchdog_pid watchdog_expected watchdog_current i
  watchdog_pid="$(read_state watchdog.pid 2>/dev/null || true)"
  [[ "$watchdog_pid" =~ ^[0-9]+$ ]] || return 0
  kill -0 "$watchdog_pid" 2>/dev/null || return 0
  watchdog_expected="$(read_state watchdog.command 2>/dev/null || true)"
  watchdog_current="$(ps -p "$watchdog_pid" -o command= 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [[ -n "$watchdog_expected" && "$watchdog_current" == "$watchdog_expected" ]] && \
    watchdog_command_matches "$watchdog_current"; then
    kill -TERM "$watchdog_pid" 2>/dev/null || true
    for ((i = 0; i < 5; i++)); do
      watchdog_matches_run || return 0
      sleep 1
    done
    if watchdog_matches_run; then
      warn "watchdog did not exit after 5s; sending KILL to the identity-verified PID"
      kill -KILL "$watchdog_pid" 2>/dev/null || true
      for ((i = 0; i < 2; i++)); do
        watchdog_matches_run || return 0
        sleep 1
      done
      watchdog_matches_run && die "identity-verified watchdog did not terminate"
    fi
  else
    warn "watchdog PID identity mismatch; refusing to signal PID $watchdog_pid"
    append_event_to "$run_dir" "watchdog PID identity mismatch; signal refused"
  fi
}

abort_start() {
  local run_dir="$1"
  local reason="$2"
  append_event_to "$run_dir" "start aborted: $reason"
  terminate_recorded_pid "$run_dir"
  cleanup_owned_routes "$run_dir"
  terminate_recorded_watchdog "$run_dir"
  write_state inactive "$(timestamp)"
  die "$reason; preserved evidence in $run_dir"
}

interrupt_start() {
  local run_dir="$1"
  local signal_name="$2"
  trap - INT TERM HUP
  append_event_to "$run_dir" "start interrupted by $signal_name"
  terminate_recorded_pid "$run_dir"
  cleanup_owned_routes "$run_dir"
  terminate_recorded_watchdog "$run_dir"
  write_state inactive "$(timestamp)"
  echo "ERROR: start interrupted by $signal_name; cleaned owned process/routes; evidence: $run_dir" >&2
  exit 130
}

register_m0_workload() {
  local command_text
  write_state workload.pid "$$"
  command_text="$(ps -p "$$" -o command= 2>/dev/null | \
    sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  workload_command_matches "$command_text" || return 1
  write_state workload.command "$command_text"
  workload_matches_run
}

terminate_current_m0_child() {
  local child_pid child_parent i
  child_pid="$(read_state workload.child.pid 2>/dev/null || true)"
  [[ "$child_pid" =~ ^[0-9]+$ ]] || return 0
  child_parent="$(ps -p "$child_pid" -o ppid= 2>/dev/null | tr -d ' ')"
  [[ "$child_parent" == "$$" ]] || return 0
  kill -TERM "$child_pid" 2>/dev/null || true
  for ((i = 0; i < 5; i++)); do
    kill -0 "$child_pid" 2>/dev/null || {
      rm -f "$(state_file workload.child.pid)"
      return 0
    }
    /bin/sleep 1
  done
  child_parent="$(ps -p "$child_pid" -o ppid= 2>/dev/null | tr -d ' ')"
  if [[ "$child_parent" == "$$" ]]; then
    kill -KILL "$child_pid" 2>/dev/null || true
  fi
  rm -f "$(state_file workload.child.pid)"
}

interrupt_m0() {
  local run_dir="$1"
  local signal_name="$2"
  trap - INT TERM HUP
  terminate_current_m0_child
  printf '%s\n' interrupted >"$run_dir/$SOAK_STATUS_FILE"
  append_event_to "$run_dir" \
    "$SOAK_STAGE interrupted by $signal_name; TUN left running for evidence"
  clear_workload_state
  echo "ERROR: $SOAK_LABEL interrupted by $signal_name; TUN remains running; use status/snapshot/stop" >&2
  exit 130
}

sample_network_control_for() {
  local run_dir="$1"
  local now="$2"
  local lock_dir exit_host target target_route exit_route_text exit_route physical_interface
  local physical_gateway exit_ping_file gateway_ping_file exit_ping_pid gateway_ping_pid
  local exit_ping_fields gateway_ping_fields physical_fields
  local exit_ping_transmitted exit_ping_received exit_ping_loss ignored_ping_fields
  local physical_mtu physical_ipkts physical_ierrs physical_ibytes
  local physical_opkts physical_oerrs physical_obytes physical_collisions
  local epoch previous_file previous_epoch previous_interface previous_ibytes previous_obytes elapsed
  local lock_attempt lock_acquired=0 physical_rx_bps=unknown physical_tx_bps=unknown

  lock_dir="$(state_file network-sample.lock)"
  for ((lock_attempt = 0; lock_attempt < 5; lock_attempt++)); do
    if mkdir "$lock_dir" 2>/dev/null; then
      lock_acquired=1
      break
    fi
    sleep 1
  done
  if ((lock_acquired != 1)); then
    append_event_to "$run_dir" "network control sample skipped: collector busy"
    return 0
  fi

  exit_host="$(read_state exit_host 2>/dev/null || echo "$SERVER_HOST")"
  target="$(read_state target 2>/dev/null || echo "$TARGET")"
  target_route="$(route_interface "$target")"
  exit_route_text="$(route -n get "$exit_host" 2>/dev/null || true)"
  exit_route="$(route_interface_from_text <<<"$exit_route_text")"
  physical_interface="$exit_route"
  physical_gateway="$(route_gateway_from_text <<<"$exit_route_text")"
  exit_ping_file="$run_dir/.network-exit-ping.$$"
  gateway_ping_file="$run_dir/.network-gateway-ping.$$"

  ping -n -q -c 3 -i 0.2 -W 1000 "$exit_host" >"$exit_ping_file" 2>&1 &
  exit_ping_pid=$!
  gateway_ping_pid=""
  if [[ -n "$physical_gateway" ]]; then
    ping -n -q -c 3 -i 0.2 -W 1000 "$physical_gateway" >"$gateway_ping_file" 2>&1 &
    gateway_ping_pid=$!
  fi
  wait "$exit_ping_pid" 2>/dev/null || true
  if [[ -n "$gateway_ping_pid" ]]; then
    wait "$gateway_ping_pid" 2>/dev/null || true
  fi

  exit_ping_fields="$(ping_control_fields_from_text <"$exit_ping_file")"
  gateway_ping_fields="unknown,unknown,unknown,unknown,unknown,unknown"
  if [[ -f "$gateway_ping_file" ]]; then
    gateway_ping_fields="$(ping_control_fields_from_text <"$gateway_ping_file")"
  fi
  {
    printf 'timestamp=%s control=exit host=%s route=%s\n' \
      "$now" "$exit_host" "${exit_route:-unknown}"
    sed 's/^/  /' "$exit_ping_file"
    printf 'timestamp=%s control=gateway host=%s route=%s\n' \
      "$now" "${physical_gateway:-unknown}" "${physical_interface:-unknown}"
    if [[ -f "$gateway_ping_file" ]]; then
      sed 's/^/  /' "$gateway_ping_file"
    else
      printf '  unavailable\n'
    fi
  } >>"$run_dir/network.log"
  rm -f "$exit_ping_file" "$gateway_ping_file"

  physical_fields="unknown,unknown,unknown,unknown,unknown,unknown,unknown,unknown"
  if [[ -n "$physical_interface" ]]; then
    physical_fields="$(netstat -ibn -I "$physical_interface" 2>/dev/null | \
      interface_control_fields_from_text "$physical_interface" || \
      echo unknown,unknown,unknown,unknown,unknown,unknown,unknown,unknown)"
  fi
  IFS=, read -r physical_mtu physical_ipkts physical_ierrs physical_ibytes \
    physical_opkts physical_oerrs physical_obytes physical_collisions <<<"$physical_fields"

  epoch="$(date +%s)"
  previous_file="$(state_file network.previous)"
  if [[ -f "$previous_file" ]]; then
    read -r previous_epoch previous_interface previous_ibytes previous_obytes \
      <"$previous_file" || true
    if [[ "$epoch" =~ ^[0-9]+$ && "$previous_epoch" =~ ^[0-9]+$ && \
      "$physical_interface" == "$previous_interface" && \
      "$physical_ibytes" =~ ^[0-9]+$ && "$previous_ibytes" =~ ^[0-9]+$ && \
      "$physical_obytes" =~ ^[0-9]+$ && "$previous_obytes" =~ ^[0-9]+$ ]] && \
      ((10#$physical_ibytes >= 10#$previous_ibytes && \
        10#$physical_obytes >= 10#$previous_obytes)); then
      elapsed=$((10#$epoch - 10#$previous_epoch))
      if ((elapsed > 0)); then
        physical_rx_bps=$(((10#$physical_ibytes - 10#$previous_ibytes) * 8 / elapsed))
        physical_tx_bps=$(((10#$physical_obytes - 10#$previous_obytes) * 8 / elapsed))
      fi
    fi
  fi
  if [[ "$epoch" =~ ^[0-9]+$ && "$physical_ibytes" =~ ^[0-9]+$ && \
    "$physical_obytes" =~ ^[0-9]+$ ]]; then
    printf '%s %s %s %s\n' "$epoch" "$physical_interface" \
      "$physical_ibytes" "$physical_obytes" >"$previous_file"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$now" "${target_route:-unknown}" "${exit_route:-unknown}" \
    "${physical_interface:-unknown}" "${physical_gateway:-unknown}" \
    "$exit_ping_fields" "$gateway_ping_fields" "$physical_fields" \
    "$physical_rx_bps" "$physical_tx_bps" >>"$run_dir/network.csv"
  IFS=, read -r exit_ping_transmitted exit_ping_received exit_ping_loss \
    ignored_ping_fields <<<"$exit_ping_fields"
  if [[ "$epoch" =~ ^[0-9]+$ && "$exit_ping_transmitted" =~ ^[0-9]+$ && \
    "$exit_ping_received" =~ ^[0-9]+$ && \
    "$exit_ping_loss" =~ ^[0-9]+([.][0-9]+)?$ && \
    "$physical_ibytes" =~ ^[0-9]+$ && "$physical_obytes" =~ ^[0-9]+$ ]]; then
    write_state network.valid.epoch "$epoch"
  fi
  rmdir "$lock_dir" 2>/dev/null || true
}

sample_once_for() {
  local run_dir="$1"
  local pid utun now process_line rss cpu state etime fd_count thread_count log_file log_bytes avail_kb
  pid="$(read_state vpn.pid 2>/dev/null || true)"
  utun="$(read_state utun 2>/dev/null || true)"
  now="$(timestamp)"

  if pid_matches_run; then
    process_line="$(ps -p "$pid" -o rss= -o %cpu= -o state= -o etime= 2>/dev/null | awk '{$1=$1; print}')"
    rss="$(awk '{print $1}' <<<"$process_line")"
    cpu="$(awk '{print $2}' <<<"$process_line")"
    state="$(awk '{print $3}' <<<"$process_line")"
    etime="$(awk '{print $4}' <<<"$process_line")"
    fd_count="$(lsof -p "$pid" 2>/dev/null | awk 'NR>1 {n++} END {print n+0}')"
    thread_count="$(ps -M "$pid" 2>/dev/null | awk 'NR>1 {n++} END {print n+0}')"
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$now" "$pid" "${rss:-unknown}" \
      "${cpu:-unknown}" "${state:-unknown}" "${etime:-unknown}" "$fd_count" "$thread_count" \
      >>"$run_dir/process.csv"
  else
    printf '%s,%s,dead,dead,dead,dead,0,0\n' "$now" "${pid:-unknown}" >>"$run_dir/process.csv"
  fi

  {
    echo "timestamp=$now interface=$utun"
    ifconfig "$utun" 2>&1 || true
    netstat -ibn -I "$utun" 2>&1 || true
  } >>"$run_dir/interface.log"
  if ! netstat -ibn -I "$utun" 2>/dev/null | interface_csv_from_text "$now" "$utun" \
    >>"$run_dir/interface.csv"; then
    printf '%s,%s,unknown,unknown,unknown,unknown,unknown,unknown,unknown,unknown\n' \
      "$now" "$utun" >>"$run_dir/interface.csv"
  fi
  sample_network_control_for "$run_dir" "$now"

  log_file="$run_dir/mini_vpn.log"
  if [[ -f "$log_file" ]]; then
    log_bytes="$(wc -c <"$log_file" | tr -d ' ')"
    if [[ "$log_bytes" =~ ^[0-9]+$ ]] && ((10#$log_bytes > 10#$MAX_LOG_BYTES)); then
      if ! tail -c "$LOG_KEEP_BYTES" "$log_file" >"$run_dir/.mini_vpn.log.tail"; then
        append_event_to "$run_dir" "watchdog log compaction copy failed; terminating mini_vpn"
        pid_matches_run && kill -TERM "$pid" 2>/dev/null || true
        return 1
      fi
      if ! : >"$log_file" || ! cat "$run_dir/.mini_vpn.log.tail" >>"$log_file"; then
        append_event_to "$run_dir" "watchdog log compaction rewrite failed; terminating mini_vpn"
        pid_matches_run && kill -TERM "$pid" 2>/dev/null || true
        return 1
      fi
      rm -f "$run_dir/.mini_vpn.log.tail"
      append_event_to "$run_dir" "watchdog compacted mini_vpn.log"
    fi
  fi

  avail_kb="$(df -Pk "$run_dir" 2>/dev/null | awk 'NR==2 {print $4}')"
  if [[ "$avail_kb" =~ ^[0-9]+$ ]] && ((10#$avail_kb < 10#$MIN_FREE_KB)); then
    append_event_to "$run_dir" "watchdog low disk; terminating mini_vpn"
    pid_matches_run && kill -TERM "$pid" 2>/dev/null || true
    return 1
  fi
}

watchdog_loop() {
  local run_dir pid
  run_dir="$(run_dir_from_state)" || exit 1
  pid="$(read_state vpn.pid)" || exit 1
  append_event_to "$run_dir" "watchdog started"
  while pid_matches_run; do
    sample_once_for "$run_dir"
    sleep "$SAMPLE_SECS"
  done
  append_event_to "$run_dir" "mini_vpn exited; watchdog cleanup"
  terminate_recorded_workload "$run_dir"
  if ! cleanup_owned_routes "$run_dir"; then
    append_event_to "$run_dir" \
      "watchdog cleanup failed: owned route or DNS state requires review"
    sample_once_for "$run_dir"
    return 1
  fi
  sample_once_for "$run_dir"
  write_state inactive "$(timestamp)"
}

start_runner() {
  local ts run_dir before_file after_file log_file vpn_pid ready=0 utun owner_uid owner_gid i
  require_root
  common_preflight
  target_ready_probe

  [[ "$STATE_DIR" == "/var/run/mini_vpn_knife15_macos_state" ]] || \
    die "internal state-directory guard failed"
  [[ ! -L "$STATE_DIR" ]] || die "state directory must not be a symlink: $STATE_DIR"
  if [[ -e "$STATE_DIR" && ! -d "$STATE_DIR" ]]; then
    die "state path exists but is not a directory: $STATE_DIR"
  fi
  if [[ -d "$STATE_DIR" ]]; then
    rm -rf "$STATE_DIR" || die "cannot remove stale state directory"
  fi
  mkdir -p "$STATE_DIR" || die "cannot create state directory"
  chmod 700 "$STATE_DIR" || die "cannot protect state directory"
  ts="$(date -u '+%Y%m%d_%H%M%S')"
  run_dir="${OUT_DIR:-/tmp/mini_vpn_knife15_macos_$ts}"
  validate_run_dir_path "$run_dir" || \
    die "OUT_DIR must be a simple /tmp/mini_vpn_knife15_macos_<label> path"
  [[ ! -e "$run_dir" ]] || die "OUT_DIR already exists; choose a fresh directory: $run_dir"
  [[ ! -L "$run_dir" ]] || die "OUT_DIR must not be a symlink: $run_dir"
  mkdir "$run_dir" || die "cannot create OUT_DIR=$run_dir"
  chmod 700 "$run_dir" || die "cannot protect OUT_DIR=$run_dir"
  before_file="$run_dir/utun.before"
  after_file="$run_dir/utun.after"
  log_file="$run_dir/mini_vpn.log"
  list_utuns >"$before_file" || die "cannot snapshot pre-start utun interfaces"
  : >"$log_file"
  printf 'timestamp,pid,rss_kib,cpu_percent,state,elapsed,fd_count,thread_rows\n' >"$run_dir/process.csv"
  printf 'timestamp,interface,mtu,ipkts,ierrs,ibytes,opkts,oerrs,obytes,collisions\n' \
    >"$run_dir/interface.csv"
  printf '%s\n' \
    'timestamp,target_route,exit_route,physical_interface,physical_gateway,exit_ping_transmitted,exit_ping_received,exit_ping_loss_percent,exit_ping_rtt_min_ms,exit_ping_rtt_avg_ms,exit_ping_rtt_max_ms,gateway_ping_transmitted,gateway_ping_received,gateway_ping_loss_percent,gateway_ping_rtt_min_ms,gateway_ping_rtt_avg_ms,gateway_ping_rtt_max_ms,physical_mtu,physical_ipkts,physical_ierrs,physical_ibytes,physical_opkts,physical_oerrs,physical_obytes,physical_collisions,physical_rx_bps,physical_tx_bps' \
    >"$run_dir/network.csv"
  : >"$run_dir/network.log"
  printf 'timestamp\tevent\n' >"$run_dir/events.tsv"

  write_state run_dir "$run_dir"
  write_start_network_state \
    "$TARGET" "$DNS_TARGET" "$SERVER_HOST" "$SERVER_PORT" "$IPERF_PORT"
  write_state bin "$BIN"
  write_state duration "$DURATION"
  write_state parallel "$PARALLEL"
  write_state sample_secs "$SAMPLE_SECS"
  write_state dns_name "$DNS_NAME"
  owner_uid="${SUDO_UID:-0}"
  owner_gid="${SUDO_GID:-0}"
  [[ "$owner_uid" =~ ^[0-9]+$ ]] || owner_uid=0
  [[ "$owner_gid" =~ ^[0-9]+$ ]] || owner_gid=0
  write_state owner_uid "$owner_uid"
  write_state owner_gid "$owner_gid"
  write_manifest "$run_dir"
  route -n get "$TARGET" >"$run_dir/target.route.before" 2>&1 || true
  route -n get "$SERVER_HOST" >"$run_dir/exit.route.before" 2>&1 || true
  [[ -z "$DNS_TARGET" ]] || route -n get "$DNS_TARGET" >"$run_dir/dns.route.before" 2>&1 || true
  append_event_to "$run_dir" "start requested"
  trap 'interrupt_start "$run_dir" INT' INT
  trap 'interrupt_start "$run_dir" TERM' TERM
  trap 'interrupt_start "$run_dir" HUP' HUP

  cd "$REPO" || abort_start "$run_dir" "cannot cd to repo"
  /usr/bin/nohup /usr/bin/env -i \
    PATH="$SAFE_PATH" \
    HOME="${HOME:-/var/root}" \
    MINI_VPN_TUIC_SERVER="$MINI_VPN_TUIC_SERVER" \
    MINI_VPN_TUIC_UUID="$MINI_VPN_TUIC_UUID" \
    MINI_VPN_TUIC_PASSWORD="$MINI_VPN_TUIC_PASSWORD" \
    MINI_VPN_TUIC_SNI="$MINI_VPN_TUIC_SNI" \
    MINI_VPN_TUIC_CA_PATH="$MINI_VPN_TUIC_CA_PATH" \
    MINI_VPN_UPSTREAM=tuic \
    MINI_VPN_TUN_POOL_SIZE=2 \
    MINI_VPN_TUN_MTU=1200 \
    MINI_VPN_TUN_TX_QUEUE_LEN=500 \
    MINI_VPN_TCP_DIAG=1 \
    MINI_VPN_PROFILE_LOOP=1 \
    MINI_VPN_METRICS_SECS="$METRICS_SECS" \
    MINI_VPN_TUIC_QUIC_STATS_SECS="$METRICS_SECS" \
    MINI_VPN_TUIC_CC=cubic \
    MINI_VPN_TUIC_UDP_MODE=native \
    MINI_VPN_TUIC_MTU_POLICY=default \
    MINI_VPN_TUIC_ZERO_RTT=false \
    MINI_VPN_TUIC_GSO_POLICY=enabled \
    MINI_VPN_TUIC_UDP_SEND_SERVICE=quinn \
    MINI_VPN_TUIC_PACING_POLICY=endpoint-window-v1 \
    MINI_VPN_TUIC_TCP_POOL=2 \
    MINI_VPN_TUIC_TCP_ORDERED_CHUNK=0 \
    MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=0 \
    MINI_VPN_TUIC_TCP_NATIVE_CHUNK_PUMP=0 \
    MINI_VPN_TUIC_TCP_NATIVE_ORDERED_PUMP=0 \
    MINI_VPN_TCP_RX_BUFFER_BYTES=1048576 \
    MINI_VPN_TCP_TX_BUFFER_BYTES=1048576 \
    MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES= \
    MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES= \
    MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144 \
    MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES=16777216 \
    MINI_VPN_TUN_RX_DRAIN_BUDGET=0 \
    MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS=0 \
    MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=0 \
    MINI_VPN_BUFFERED_DOWNLINK=0 \
    MINI_VPN_THIN_TCP_RELAY=0 \
    MINI_VPN_CONTINUOUS_TCP_RELAY=0 \
    MINI_VPN_TRACE=0 \
    MINI_VPN_D2_PERMIT_TCP_RELAY=0 \
    MINI_VPN_D3_EGRESS_ACTOR=0 \
    MINI_VPN_D3_EGRESS_ACTOR_ADAPTIVE_CREDIT=0 \
    MINI_VPN_D3_EGRESS_ACTOR_LEGACY_CREDIT=0 \
    MINI_VPN_D3_EGRESS_ACTOR_SELF_WAKE=0 \
    MINI_VPN_D4_STALLED_READ_SERVICE=0 \
    MINI_VPN_D5_CAPACITY_BACKPRESSURE=0 \
    MINI_VPN_D6_NATIVE_EGRESS_PERMIT=0 \
    MINI_VPN_D11_ORDERED_EGRESS_PERMIT=0 \
    MINI_VPN_H10D16_BYTE_OWNED_EGRESS=1 \
    "$BIN" client-tun >>"$log_file" 2>&1 &
  vpn_pid=$!
  write_state vpn.pid "$vpn_pid"

  for ((i = 0; i < STARTUP_TIMEOUT; i++)); do
    if ! pid_matches_run; then
      tail -n 100 "$log_file" >&2
      abort_start "$run_dir" "mini_vpn exited during startup"
    fi
    if grep -Fq "✅ 已连接 TUIC 出口" "$log_file" && \
      grep -Fq "🌊 UDP relay 数据面就绪" "$log_file"; then
      ready=1
      break
    fi
    sleep 1
  done
  if ((ready != 1)); then
    tail -n 100 "$log_file" >&2
    abort_start "$run_dir" "mini_vpn did not become ready within ${STARTUP_TIMEOUT}s"
  fi
  if ! verify_startup_profile "$log_file"; then
    tail -n 140 "$log_file" >&2
    abort_start "$run_dir" "H10d16 startup fingerprint mismatch"
  fi

  list_utuns >"$after_file" || abort_start "$run_dir" "cannot snapshot post-start utun interfaces"
  utun="$(comm -13 "$before_file" "$after_file" | sed -n '1p')"
  [[ -n "$utun" ]] || {
    abort_start "$run_dir" "no new utun detected"
  }
  [[ "$(comm -13 "$before_file" "$after_file" | wc -l | tr -d ' ')" == "1" ]] || {
    abort_start "$run_dir" "more than one new utun detected; refuse ambiguous routing"
  }
  write_state utun "$utun"

  if ! route -n add -host "$TARGET" -interface "$utun" >>"$run_dir/route.log" 2>&1; then
    abort_start "$run_dir" "failed to add target-only route"
  fi
  if [[ -n "$DNS_TARGET" ]]; then
    if ! route -n add -host "$DNS_TARGET" -interface "$utun" >>"$run_dir/route.log" 2>&1; then
      abort_start "$run_dir" "failed to add optional DNS target route"
    fi
  fi
  [[ "$(route_interface "$TARGET")" == "$utun" ]] || \
    abort_start "$run_dir" "target route did not enter $utun"
  [[ "$(route_interface "$SERVER_HOST")" != "$utun" ]] || {
    abort_start "$run_dir" "Exit route recursed into $utun"
  }
  if [[ -n "$DNS_TARGET" ]]; then
    [[ "$(route_interface "$DNS_TARGET")" == "$utun" ]] || \
      abort_start "$run_dir" "DNS target route did not enter $utun"
  fi

  append_event_to "$run_dir" "ready utun=$utun"
  sample_once_for "$run_dir"
  /usr/bin/nohup /usr/bin/env -i PATH="$SAFE_PATH" HOME="${HOME:-/var/root}" SAMPLE_SECS="$SAMPLE_SECS" \
    MAX_LOG_BYTES="$MAX_LOG_BYTES" LOG_KEEP_BYTES="$LOG_KEEP_BYTES" MIN_FREE_KB="$MIN_FREE_KB" \
    /bin/bash "$SCRIPT_PATH" __watchdog >>"$run_dir/watchdog.log" 2>&1 &
  write_state watchdog.pid "$!"
  sleep 1
  write_state watchdog.command "$(ps -p "$(read_state watchdog.pid)" -o command= 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if ! watchdog_matches_run; then
    abort_start "$run_dir" "watchdog failed its startup identity check"
  fi
  trap - INT TERM HUP
  echo "PASS: Knife15 target-only TUN is ready"
  echo "run_dir=$run_dir"
  echo "vpn_pid=$vpn_pid utun=$utun"
  echo "target=$TARGET -> $utun"
  echo "exit=$SERVER_HOST -> $(route_interface "$SERVER_HOST")"
  echo "dns_target=${DNS_TARGET:-disabled}"
  echo "Next: sudo -E bash scripts/knife15-macos-soak.sh smoke"
}

run_direct_baseline_probe() {
  local output_file="$1"
  local target="$2"
  local iperf_port="$3"
  local duration="$4"
  local parallel="$5"
  local reverse="$6"
  local iperf_bin="$7"
  if [[ "$reverse" == "1" ]]; then
    run_logged "$output_file" "$iperf_bin" -c "$target" -p "$iperf_port" \
      -t "$duration" -P "$parallel" -l "$TCP_REVERSE_IPERF_LENGTH_BYTES" -R \
      --json --get-server-output
  elif [[ "$reverse" == "0" ]]; then
    run_logged "$output_file" "$iperf_bin" -c "$target" -p "$iperf_port" \
      -t "$duration" -P "$parallel" --json --get-server-output
  else
    return 2
  fi
}

run_baseline() {
  local out_dir forward_reason reverse_reason
  common_preflight
  require_command iperf3
  [[ "$(route_interface "$TARGET")" != utun* ]] || die "baseline requires TARGET outside utun"
  out_dir="${BASELINE_OUT_DIR:-/tmp/mini_vpn_knife15_macos_baseline_$(date -u '+%Y%m%d_%H%M%S')}"
  mkdir -p "$out_dir" || die "cannot create baseline output directory"
  echo "Running direct forward baseline..."
  if ! run_direct_baseline_probe "$out_dir/direct-forward.json" "$TARGET" \
    "$IPERF_PORT" "$DURATION" "$PARALLEL" 0 iperf3; then
    write_baseline_manifest "$out_dir" fail forward_command_failed \
      command_failed not_run
    die "direct forward baseline failed; evidence: $out_dir/direct-forward.json"
  fi
  echo "Running direct reverse baseline..."
  if ! run_direct_baseline_probe "$out_dir/direct-reverse.json" "$TARGET" \
    "$IPERF_PORT" "$DURATION" "$PARALLEL" 1 iperf3; then
    forward_reason="$(baseline_file_validation_reason \
      "$out_dir/direct-forward.json" "$TARGET" 0)"
    write_baseline_manifest "$out_dir" fail reverse_command_failed \
      "$forward_reason" command_failed
    die "direct reverse baseline failed; evidence: $out_dir/direct-reverse.json"
  fi
  baseline_receiver_summary "$out_dir" || \
    warn "direct baseline completed but its receiver speed summary is unavailable"
  read -r forward_reason reverse_reason \
    <<<"$(baseline_pair_validation_reasons "$out_dir" "$TARGET")"
  if [[ "$forward_reason" != "ok" || "$reverse_reason" != "ok" ]]; then
    write_baseline_manifest "$out_dir" fail \
      "forward_${forward_reason}__reverse_${reverse_reason}" \
      "$forward_reason" "$reverse_reason"
    die "direct baseline validation failed forward=$forward_reason reverse=$reverse_reason; evidence: $out_dir"
  fi
  write_baseline_manifest "$out_dir" pass ok "$forward_reason" "$reverse_reason"
  echo "PASS: direct baseline complete: $out_dir"
}

run_direct_continuity_probe() {
  local output_file="$1"
  local target="$2"
  local iperf_port="$3"
  local duration="$4"
  local rate_bps="$5"
  local iperf_bin="$6"

  DIRECT_CONTINUITY_REASON=command_failed
  if ! "$iperf_bin" -c "$target" -p "$iperf_port" -t "$duration" -P 1 \
    -b "$rate_bps" --json --get-server-output >"$output_file" 2>&1; then
    return 1
  fi
  DIRECT_CONTINUITY_REASON="$(m0_iperf_result_failure_reason \
    "$output_file" TCP 0)"
  [[ "$DIRECT_CONTINUITY_REASON" == "ok" ]] || return 1
  if ! validate_direct_continuity_result "$output_file" "$target"; then
    DIRECT_CONTINUITY_REASON=incomplete_300s_receiver_evidence
    return 1
  fi
  return 0
}

run_direct_discriminator() {
  local out_dir result_file status reason completed_utc completed_epoch
  local forward_bps rate_bps target_route exit_route receiver_bytes receiver_bps
  local receiver_zero_intervals sender_zero_intervals current_target_route current_exit_route
  local baseline_dir

  common_preflight
  require_command jq
  require_command iperf3
  baseline_dir="$(selected_direct_baseline_dir)" || \
    die "set exactly one of M0_BASELINE_DIR, M1_BASELINE_DIR, or M2_BASELINE_DIR to the fresh baseline directory"
  selected_direct_epoch_is_frozen "$baseline_dir" || \
    die "direct discriminator requires the selected stage's frozen 300s TCP epoch; unset its stage override"
  validate_baseline_dir_path "$baseline_dir" || \
    die "the selected soak baseline must be the fresh directory printed by baseline"
  [[ -d "$baseline_dir" && ! -L "$baseline_dir" ]] || \
    die "the selected soak baseline must be an existing non-symlink directory"
  validate_m0_baseline_pair "$baseline_dir" "$TARGET" || \
    die "direct discriminator requires fresh direction-aware baseline evidence for $TARGET"

  out_dir="${DIRECT_OUT_DIR:-/tmp/mini_vpn_knife15_macos_direct_$(date -u '+%Y%m%d_%H%M%S')}"
  validate_direct_dir_path "$out_dir" || \
    die "DIRECT_OUT_DIR must be a simple /tmp/mini_vpn_knife15_macos_direct_<label> path"
  [[ ! -e "$out_dir" && ! -L "$out_dir" ]] || \
    die "direct discriminator output already exists: $out_dir"
  mkdir "$out_dir" || die "cannot create direct discriminator output: $out_dir"
  chmod 700 "$out_dir" || die "cannot protect direct discriminator output: $out_dir"

  forward_bps="$(baseline_receiver_bps \
    "$baseline_dir/direct-forward.json")" || \
    die "cannot derive direct discriminator rate from baseline"
  rate_bps=$((10#$forward_bps / 2))
  target_route="$(route_interface "$TARGET")"
  exit_route="$(route_interface "$SERVER_HOST")"
  result_file="$out_dir/direct-forward-300s.json"
  echo "Running 300s direct forward continuity discriminator at ${rate_bps} bit/s..."
  status=fail
  reason=command_failed
  if run_direct_continuity_probe "$result_file" "$TARGET" "$IPERF_PORT" \
    300 "$rate_bps" "$(command -v iperf3)"; then
    status=pass
    reason=ok
  else
    reason="$DIRECT_CONTINUITY_REASON"
  fi

  current_target_route="$(route_interface "$TARGET")"
  current_exit_route="$(route_interface "$SERVER_HOST")"
  if [[ "$current_target_route" != "$target_route" || \
    "$current_exit_route" != "$exit_route" || "$target_route" == utun* || \
    "$exit_route" == utun* ]]; then
    status=fail
    reason=physical_route_changed
  fi
  completed_utc="$(timestamp)"
  completed_epoch="$(date +%s)"
  receiver_bytes="$(jq -er \
    '.server_output_json.end.sum_received.bytes | floor' "$result_file" 2>/dev/null || \
    echo unknown)"
  receiver_bps="$(jq -er \
    '.server_output_json.end.sum_received.bits_per_second | floor' \
    "$result_file" 2>/dev/null || echo unknown)"
  receiver_zero_intervals="$(jq -er \
    'def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    .server_output_json.start.test_start.duration as $duration
    | .server_output_json.intervals as $intervals
    | [$intervals | to_entries[]
      | select(.value.sum.bits_per_second <= 0 and
        (interval_entry_is_proven_partial(
          $duration; ($intervals | length)) | not))] | length' \
    "$result_file" 2>/dev/null || echo unknown)"
  sender_zero_intervals="$(jq -er \
    'def interval_entry_is_proven_partial($duration; $count):
      .key as $index
      | .value.sum.start as $start | .value.sum.end as $end
      | (($duration | type) == "number"
        and $index == ($count - 1)
        and ($start | type) == "number" and ($end | type) == "number"
        and $start >= ($duration - 0.5) and $end >= $duration
        and $end <= ($duration + 0.5) and $end >= $start
        and ($end - $start) < 0.5);
    .start.test_start.duration as $duration
    | .intervals as $intervals
    | [$intervals | to_entries[]
      | select(.value.sum.bits_per_second <= 0 and
        (interval_entry_is_proven_partial(
          $duration; ($intervals | length)) | not))] | length' \
    "$result_file" 2>/dev/null || echo unknown)"
  cat >"$out_dir/manifest.txt" <<EOF_DIRECT_MANIFEST
schema=knife15-macos-direct-continuity-v1
status=$status
reason=$reason
completed_utc=$completed_utc
completed_epoch=$completed_epoch
source_commit=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)
runner_sha256=$(sha256_file "$SCRIPT_PATH")
binary_sha256=$(sha256_file "$BIN")
target=$TARGET
target_route=$target_route
exit_host=$SERVER_HOST
exit_route=$exit_route
baseline_dir=$baseline_dir
baseline_forward_sha256=$(sha256_file "$baseline_dir/direct-forward.json")
baseline_reverse_sha256=$(sha256_file "$baseline_dir/direct-reverse.json")
duration_secs=300
rate_bps=$rate_bps
receiver_bytes=$receiver_bytes
receiver_bps=$receiver_bps
receiver_zero_intervals=$receiver_zero_intervals
sender_zero_intervals=$sender_zero_intervals
result_sha256=$(sha256_file "$result_file")
EOF_DIRECT_MANIFEST
  echo "direct_dir=$out_dir"
  echo "result_sha256=$(sha256_file "$result_file")"
  if [[ "$status" != "pass" ]] || \
    ! validate_direct_continuity_dir "$out_dir" "$baseline_dir" \
      "$TARGET" "$completed_epoch"; then
    die "direct continuity discriminator failed reason=$reason; no TUN was started"
  fi
  echo "PASS: 300s direct Target receiver continuity discriminator completed"
}

run_logged() {
  local output_file="$1"
  local statuses
  shift
  "$@" | tee "$output_file"
  statuses=("${PIPESTATUS[@]}")
  ((statuses[0] == 0 && statuses[1] == 0))
}

run_logged_with_timeout() {
  local output_file="$1"
  local timeout_secs="$2"
  local command_pid watchdog_pid parent_pid status timeout_marker
  shift 2
  [[ "$timeout_secs" =~ ^[1-9][0-9]*$ ]] || return 2
  timeout_marker="${output_file}.timeout.$$"
  rm -f "$timeout_marker"
  parent_pid="$$"
  (
    exec "$@" >"$output_file" 2>&1
  ) &
  command_pid=$!
  (
    sleep "$timeout_secs"
    kill -0 "$parent_pid" 2>/dev/null || exit 0
    kill -0 "$command_pid" 2>/dev/null || exit 0
    printf '%s\n' timeout >"$timeout_marker"
    kill -TERM "$command_pid" 2>/dev/null || exit 0
    sleep 2
    kill -KILL "$command_pid" 2>/dev/null || true
  ) &
  watchdog_pid=$!
  trap 'kill -TERM "$command_pid" "$watchdog_pid" 2>/dev/null || true' INT TERM HUP
  wait "$command_pid"
  status=$?
  kill -TERM "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  trap - INT TERM HUP
  if [[ -f "$timeout_marker" ]]; then
    printf 'ERROR: command exceeded hard timeout of %ss\n' "$timeout_secs" \
      >>"$output_file"
    status=124
  fi
  rm -f "$timeout_marker"
  sed -n '1,$p' "$output_file"
  return "$status"
}

m0_profile_value() {
  local profile_file="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key {print substr($0, index($0, "=") + 1); exit}' \
    "$profile_file"
}

m0_min_duration() {
  local requested="$1"
  local remaining="$2"
  if ((10#$requested < 10#$remaining)); then
    printf '%s\n' "$requested"
  else
    printf '%s\n' "$remaining"
  fi
}

run_m0_logged() {
  local output_file="$1"
  local hard_timeout_secs="$2"
  local child_pid result started_epoch current_epoch i
  shift 2
  validate_positive_integer "$hard_timeout_secs" || return 2
  if [[ "${M0_TRACK_CHILD:-0}" != "1" ]]; then
    "$@" >"$output_file" 2>&1
    return
  fi
  "$@" >"$output_file" 2>&1 &
  child_pid=$!
  started_epoch="$(date +%s)"
  write_state workload.child.pid "$child_pid"
  while kill -0 "$child_pid" 2>/dev/null; do
    /bin/sleep 2
    current_epoch="$(date +%s)"
    if [[ "$started_epoch" =~ ^[0-9]+$ && "$current_epoch" =~ ^[0-9]+$ ]] && \
      ((10#$current_epoch - 10#$started_epoch >= 10#$hard_timeout_secs)); then
      append_event_to "$M0_ACTIVE_RUN_DIR" \
        "$SOAK_STAGE child hard timeout after ${hard_timeout_secs}s"
      kill -TERM "$child_pid" 2>/dev/null || true
      for ((i = 0; i < 5; i++)); do
        kill -0 "$child_pid" 2>/dev/null || break
        /bin/sleep 1
      done
      kill -0 "$child_pid" 2>/dev/null && kill -KILL "$child_pid" 2>/dev/null || true
      wait "$child_pid" 2>/dev/null || true
      rm -f "$(state_file workload.child.pid)"
      return 124
    fi
    if ! m0_assert_run_healthy "$M0_ACTIVE_RUN_DIR"; then
      kill -TERM "$child_pid" 2>/dev/null || true
      wait "$child_pid" 2>/dev/null || true
      rm -f "$(state_file workload.child.pid)"
      return 125
    fi
  done
  wait "$child_pid"
  result=$?
  if [[ "$(read_state workload.child.pid 2>/dev/null || true)" == "$child_pid" ]]; then
    rm -f "$(state_file workload.child.pid)"
  fi
  return "$result"
}

m0_assert_run_healthy() {
  local run_dir="$1"
  local utun target exit_host target_if exit_if
  [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]] || return 0
  if ! m0_log_history_is_complete "$run_dir"; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE health failed: mini_vpn log history was compacted"
    return 1
  fi
  if ! active_pid; then
    append_event_to "$run_dir" "$SOAK_STAGE health failed: mini_vpn not running"
    return 1
  fi
  if ! watchdog_matches_run; then
    append_event_to "$run_dir" "$SOAK_STAGE health failed: watchdog not running"
    return 1
  fi
  if ! network_control_is_recent; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE health failed: network control sample stale or invalid"
    return 1
  fi
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  target_if="$(route_interface "$target")"
  exit_if="$(route_interface "$exit_host")"
  if [[ -z "$utun" || "$target_if" != "$utun" ]]; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE health failed: target route expected=$utun actual=${target_if:-missing}"
    return 1
  fi
  if [[ -z "$exit_if" || "$exit_if" == "$utun" ]]; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE health failed: Exit route actual=${exit_if:-missing} forbidden=$utun"
    return 1
  fi
  if [[ ( "$SOAK_STAGE" == "m2" || \
    "$SOAK_STAGE" == "m2-qualification" ) ]] && \
    ! m2_full_tunnel_is_active; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE health failed: full-tunnel route, DNS, or IPv6 invariant"
    return 1
  fi
}

run_m0_iperf_phase() {
  local run_dir="$1"
  local target="$2"
  local iperf_port="$3"
  local cycle="$4"
  local phase="$5"
  local duration="$6"
  local rate_bps="$7"
  local reverse="$8"
  local udp="$9"
  local output_file validation_reason violation_value violation_detail udp_loss
  output_file="$run_dir/$SOAK_EVIDENCE_DIR/cycle_$(printf '%03d' "$cycle")_${phase}.json"
  m0_assert_run_healthy "$run_dir" || return 1
  append_event_to "$run_dir" \
    "$SOAK_STAGE phase start cycle=$cycle phase=$phase duration=$duration rate_bps=$rate_bps"
  m0_progress "$SOAK_LABEL cycle=$cycle phase=$phase duration=${duration}s rate_bps=$rate_bps start"
  if [[ "$udp" == "1" && "$reverse" == "1" ]]; then
    run_m0_logged "$output_file" "$((10#$duration + 30))" \
      "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" -u -l 1160 -R --json --get-server-output
  elif [[ "$udp" == "1" ]]; then
    run_m0_logged "$output_file" "$((10#$duration + 30))" \
      "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" -u -l 1160 --json --get-server-output
  elif [[ "$reverse" == "1" ]]; then
    run_m0_logged "$output_file" "$((10#$duration + 30))" \
      "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" -l "$TCP_REVERSE_IPERF_LENGTH_BYTES" -R \
      --json --get-server-output
  else
    run_m0_logged "$output_file" "$((10#$duration + 30))" \
      "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" --json --get-server-output
  fi || {
    append_event_to "$run_dir" \
      "$SOAK_STAGE phase failed cycle=$cycle phase=$phase"
    return 1
  }
  m0_assert_run_healthy "$run_dir" || return 1
  validation_reason="$(m0_iperf_result_failure_reason \
    "$output_file" "$([[ "$udp" == "1" ]] && echo UDP || echo TCP)" "$reverse")"
  if [[ "$validation_reason" != "ok" ]]; then
    if [[ "$validation_reason" == "receiver_zero_interval" && \
      "${SOAK_CONTINUE_DATA_QUALITY:-0}" == "1" ]] && \
      validate_m0_iperf_result "$output_file" \
        "$([[ "$udp" == "1" ]] && echo UDP || echo TCP)" "$reverse" 1; then
      read -r violation_value violation_detail \
        <<<"$(receiver_zero_interval_evidence "$output_file" "$reverse")"
      [[ "$violation_value" =~ ^[1-9][0-9]*$ && -n "$violation_detail" ]] || return 1
      record_soak_data_quality_violation "$run_dir" "$cycle" "$phase" \
        receiver_zero_interval "$violation_value" "$violation_detail" \
        "$output_file" || return 1
    else
      append_event_to "$run_dir" \
        "$SOAK_STAGE phase failed cycle=$cycle phase=$phase reason=$validation_reason"
      return 1
    fi
  fi
  if [[ "$udp" == "1" ]]; then
    udp_loss="$(udp_loss_percent "$output_file")" || return 1
    if ! decimal_le "$udp_loss" 3.0; then
      if [[ "${SOAK_CONTINUE_DATA_QUALITY:-0}" == "1" ]]; then
        record_soak_data_quality_violation "$run_dir" "$cycle" "$phase" \
          udp_loss_percent "$udp_loss" limit=3.0 "$output_file" || return 1
      else
        append_event_to "$run_dir" \
          "$SOAK_STAGE phase failed cycle=$cycle phase=$phase reason=udp_loss_percent value=$udp_loss limit=3.0"
        return 1
      fi
    fi
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE phase complete cycle=$cycle phase=$phase"
  m0_progress "$SOAK_LABEL cycle=$cycle phase=$phase complete"
  if [[ "${M0_RESUME_PENDING:-0}" == "1" ]]; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE resume complete cycle=$cycle phase=$phase"
    M0_RESUME_PENDING=0
  fi
}

run_m0_dns_phase() {
  local run_dir="$1"
  local dns_target="$2"
  local dns_name="$3"
  local cycle="$4"
  local output_file="$run_dir/$SOAK_EVIDENCE_DIR/cycle_$(printf '%03d' "$cycle")_dns.txt"
  [[ -n "$dns_target" ]] || return 0
  m0_assert_run_healthy "$run_dir" || return 1
  append_event_to "$run_dir" "$SOAK_STAGE DNS start cycle=$cycle"
  if ! run_m0_logged "$output_file" 15 "$M0_DIG_BIN" "@$dns_target" "$dns_name" A \
    +time=5 +tries=1; then
    append_event_to "$run_dir" "$SOAK_STAGE DNS failed cycle=$cycle"
    return 1
  fi
  m0_assert_run_healthy "$run_dir" || return 1
  if ! validate_m0_dns_result "$output_file"; then
    append_event_to "$run_dir" \
      "$SOAK_STAGE DNS failed cycle=$cycle reason=no_fake_ip_answer"
    return 1
  fi
  append_event_to "$run_dir" "$SOAK_STAGE DNS complete cycle=$cycle"
}

run_m0_active_window() {
  local run_dir="$1"
  local label="$2"
  local budget="$3"
  local profile_file="$4"
  local mode="${5:-}"
  local target iperf_port dns_target dns_name tcp_secs udp_secs short_secs short_count
  local tcp_forward_bps tcp_reverse_bps udp_reverse_bps short_forward_bps short_reverse_bps
  local remaining="$budget"
  local duration short_index reverse phase short_rate
  target="$(m0_profile_value "$profile_file" target)"
  iperf_port="$(m0_profile_value "$profile_file" iperf_port)"
  dns_target="$(m0_profile_value "$profile_file" dns_target)"
  [[ "$dns_target" == "disabled" ]] && dns_target=""
  dns_name="$(m0_profile_value "$profile_file" dns_name)"
  tcp_secs="$(m0_profile_value "$profile_file" tcp_epoch_secs)"
  udp_secs="$(m0_profile_value "$profile_file" udp_epoch_secs)"
  short_secs="$(m0_profile_value "$profile_file" short_epoch_secs)"
  if [[ -n "$mode" ]]; then
    short_count="$(m0_profile_value \
      "$profile_file" "${mode}_short_connections_per_cycle")"
    tcp_forward_bps="$(m0_profile_value "$profile_file" "${mode}_tcp_forward_bps")"
    tcp_reverse_bps="$(m0_profile_value "$profile_file" "${mode}_tcp_reverse_bps")"
    udp_reverse_bps="$(m0_profile_value "$profile_file" "${mode}_udp_reverse_bps")"
    short_forward_bps="$(m0_profile_value "$profile_file" "${mode}_short_forward_bps")"
    short_reverse_bps="$(m0_profile_value "$profile_file" "${mode}_short_reverse_bps")"
  else
    short_count="$(m0_profile_value "$profile_file" short_connections_per_cycle)"
    tcp_forward_bps="$(m0_profile_value "$profile_file" tcp_forward_bps)"
    tcp_reverse_bps="$(m0_profile_value "$profile_file" tcp_reverse_bps)"
    udp_reverse_bps="$(m0_profile_value "$profile_file" udp_reverse_bps)"
    short_forward_bps="$(m0_profile_value "$profile_file" short_forward_bps)"
    short_reverse_bps="$(m0_profile_value "$profile_file" short_reverse_bps)"
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE active $label start planned_secs=$budget"
  while ((10#$remaining > 0)); do
    M0_CYCLE_INDEX=$((M0_CYCLE_INDEX + 1))

    duration="$(m0_min_duration "$tcp_secs" "$remaining")"
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$M0_CYCLE_INDEX" \
      tcp-forward "$duration" "$tcp_forward_bps" 0 0 || return 1
    remaining=$((10#$remaining - 10#$duration))
    ((remaining > 0)) || break

    duration="$(m0_min_duration "$tcp_secs" "$remaining")"
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$M0_CYCLE_INDEX" \
      tcp-reverse "$duration" "$tcp_reverse_bps" 1 0 || return 1
    remaining=$((10#$remaining - 10#$duration))
    ((remaining > 0)) || break

    duration="$(m0_min_duration "$udp_secs" "$remaining")"
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$M0_CYCLE_INDEX" \
      udp-reverse "$duration" "$udp_reverse_bps" 1 1 || return 1
    remaining=$((10#$remaining - 10#$duration))
    ((remaining > 0)) || break

    for ((short_index = 1; short_index <= 10#$short_count && remaining > 0; short_index++)); do
      if ((short_index % 2 == 0)); then
        reverse=1
        phase="short-reverse-$short_index"
        short_rate="$short_reverse_bps"
      else
        reverse=0
        phase="short-forward-$short_index"
        short_rate="$short_forward_bps"
      fi
      duration="$(m0_min_duration "$short_secs" "$remaining")"
      run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$M0_CYCLE_INDEX" \
        "$phase" "$duration" "$short_rate" "$reverse" 0 || return 1
      remaining=$((10#$remaining - 10#$duration))
    done
    ((remaining > 0)) || break
    run_m0_dns_phase "$run_dir" "$dns_target" "$dns_name" "$M0_CYCLE_INDEX" || return 1
    if [[ "$SOAK_STAGE" == "m2" ]]; then
      M2_COMPLETE_CYCLE_INDEX=$((M2_COMPLETE_CYCLE_INDEX + 1))
    fi
    if [[ "${SOAK_REAL_CLIENT_PROBE:-0}" == "1" ]]; then
      ((M2_COMPLETE_CYCLE_INDEX > 0)) || return 1
      run_m2_real_client_probe "$run_dir" \
        "cycle_$(printf '%03d' "$M2_COMPLETE_CYCLE_INDEX")" || {
        append_event_to "$run_dir" \
          "$SOAK_STAGE real client failed cycle=$M0_CYCLE_INDEX"
        return 1
      }
    fi
    append_event_to "$run_dir" \
      "$SOAK_STAGE cycle complete cycle=$M0_CYCLE_INDEX"
  done
  append_event_to "$run_dir" \
    "$SOAK_STAGE active $label complete planned_secs=$budget"
}

run_m0_schedule_body() {
  local run_dir="$1"
  local profile_file="$2"
  local total_secs idle_secs final_drain_secs active_secs pre_idle_secs post_idle_secs
  total_secs="$(m0_profile_value "$profile_file" total_secs)"
  idle_secs="$(m0_profile_value "$profile_file" idle_secs)"
  final_drain_secs="$(m0_profile_value "$profile_file" final_drain_secs)"
  active_secs=$((10#$total_secs - 10#$idle_secs - 10#$final_drain_secs))
  ((active_secs > 0)) || return 1
  pre_idle_secs=$((active_secs / 2))
  post_idle_secs=$((active_secs - pre_idle_secs))
  M0_CYCLE_INDEX=0
  M0_RESUME_PENDING=0

  append_event_to "$run_dir" "m0 start planned_secs=$total_secs"
  run_m0_active_window "$run_dir" pre-idle "$pre_idle_secs" "$profile_file" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
  fi
  append_event_to "$run_dir" "m0 idle start planned_secs=$idle_secs"
  m0_progress "M0 idle drain start duration=${idle_secs}s"
  run_m0_logged "$run_dir/m0/idle.sleep.log" "$((10#$idle_secs + 30))" \
    "$M0_SLEEP_BIN" "$idle_secs" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
  fi
  append_event_to "$run_dir" "m0 idle complete planned_secs=$idle_secs"
  append_event_to "$run_dir" "m0 resume start"
  m0_progress "M0 idle drain complete; resumed traffic starts"
  M0_RESUME_PENDING=1
  run_m0_active_window "$run_dir" post-idle "$post_idle_secs" "$profile_file" || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  append_event_to "$run_dir" "m0 final drain start planned_secs=$final_drain_secs"
  m0_progress "M0 final drain start duration=${final_drain_secs}s"
  run_m0_logged "$run_dir/m0/final-drain.sleep.log" \
    "$((10#$final_drain_secs + 30))" \
    "$M0_SLEEP_BIN" "$final_drain_secs" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
  fi
  append_event_to "$run_dir" "m0 final drain complete planned_secs=$final_drain_secs"
  append_event_to "$run_dir" "m0 complete cycles=$M0_CYCLE_INDEX planned_secs=$total_secs"
  m0_progress "M0 workload timeline complete"
}

run_m0_schedule() {
  local run_dir="$1"
  local status_file="$run_dir/m0.status"
  local result
  printf '%s\n' running >"$status_file" || return 1
  if run_m0_schedule_body "$@"; then
    printf '%s\n' complete >"$status_file" || return 1
    return 0
  else
    result=$?
    printf '%s\n' failed >"$status_file" || return 1
    append_event_to "$run_dir" "m0 failed"
    return "$result"
  fi
}

run_m1_idle_window() {
  local run_dir="$1"
  local label="$2"
  local idle_secs="$3"
  local endpoint_samples_before endpoint_values
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r endpoint_samples_before _ <<<"$endpoint_values"
  [[ "$endpoint_samples_before" =~ ^[0-9]+$ ]] || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE idle start label=$label planned_secs=$idle_secs"
  m0_progress "$SOAK_LABEL idle drain $label start duration=${idle_secs}s"
  run_m0_logged "$run_dir/$SOAK_EVIDENCE_DIR/${label}.sleep.log" \
    "$((10#$idle_secs + 30))" \
    "$M0_SLEEP_BIN" "$idle_secs" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
    capture_m1_checkpoint "$run_dir" "$label" "$endpoint_samples_before" || return 1
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE idle complete label=$label planned_secs=$idle_secs"
  append_event_to "$run_dir" "$SOAK_STAGE resume start label=$label"
  M0_RESUME_PENDING=1
}

run_m1_schedule_body() {
  local run_dir="$1"
  local profile_file="$2"
  local total_secs steady_a_secs idle_secs quiet_secs steady_b_secs
  local churn_secs steady_c_secs final_drain_secs planned_sum
  local endpoint_samples_before endpoint_values
  total_secs="$(m0_profile_value "$profile_file" total_secs)"
  steady_a_secs="$(m0_profile_value "$profile_file" steady_a_secs)"
  idle_secs="$(m0_profile_value "$profile_file" idle_secs)"
  quiet_secs="$(m0_profile_value "$profile_file" quiet_secs)"
  steady_b_secs="$(m0_profile_value "$profile_file" steady_b_secs)"
  churn_secs="$(m0_profile_value "$profile_file" churn_secs)"
  steady_c_secs="$(m0_profile_value "$profile_file" steady_c_secs)"
  final_drain_secs="$(m0_profile_value "$profile_file" final_drain_secs)"
  planned_sum=$((10#$steady_a_secs + 3 * 10#$idle_secs + 10#$quiet_secs + \
    10#$steady_b_secs + 10#$churn_secs + 10#$steady_c_secs + \
    10#$final_drain_secs))
  ((planned_sum == 10#$total_secs)) || return 1
  M0_CYCLE_INDEX=0
  M0_RESUME_PENDING=0

  append_event_to "$run_dir" "$SOAK_STAGE start planned_secs=$total_secs"
  run_m0_active_window "$run_dir" steady-a "$steady_a_secs" \
    "$profile_file" steady || return 1
  run_m1_idle_window "$run_dir" idle-1 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" quiet "$quiet_secs" \
    "$profile_file" quiet || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m1_idle_window "$run_dir" idle-2 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" steady-b "$steady_b_secs" \
    "$profile_file" steady || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m1_idle_window "$run_dir" idle-3 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" churn "$churn_secs" \
    "$profile_file" churn || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m0_active_window "$run_dir" steady-c "$steady_c_secs" \
    "$profile_file" steady || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r endpoint_samples_before _ <<<"$endpoint_values"
  [[ "$endpoint_samples_before" =~ ^[0-9]+$ ]] || return 1
  append_event_to "$run_dir" \
    "$SOAK_STAGE final drain start planned_secs=$final_drain_secs"
  m0_progress "$SOAK_LABEL final drain start duration=${final_drain_secs}s"
  run_m0_logged "$run_dir/$SOAK_EVIDENCE_DIR/final-drain.sleep.log" \
    "$((10#$final_drain_secs + 30))" \
    "$M0_SLEEP_BIN" "$final_drain_secs" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
    capture_m1_checkpoint "$run_dir" final "$endpoint_samples_before" || return 1
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE final drain complete planned_secs=$final_drain_secs"
  append_event_to "$run_dir" \
    "$SOAK_STAGE complete cycles=$M0_CYCLE_INDEX planned_secs=$total_secs"
  m0_progress "$SOAK_LABEL workload timeline complete"
}

run_m1_schedule() {
  local run_dir="$1"
  local status_file="$run_dir/$SOAK_STATUS_FILE"
  local result
  printf '%s\n' running >"$status_file" || return 1
  if run_m1_schedule_body "$@"; then
    printf '%s\n' "${SOAK_SUCCESS_STATUS:-complete}" >"$status_file" || return 1
    return 0
  else
    result=$?
    printf '%s\n' failed >"$status_file" || return 1
    append_event_to "$run_dir" "$SOAK_STAGE failed"
    return "$result"
  fi
}

run_m2_idle_window() {
  local run_dir="$1"
  local label="$2"
  local idle_secs="$3"
  local endpoint_values data_plane_values endpoint_samples_before
  local data_plane_samples_before
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r endpoint_samples_before _ <<<"$endpoint_values"
  data_plane_values="$(m2_data_plane_envelope "$run_dir/mini_vpn.log")"
  read -r data_plane_samples_before _ <<<"$data_plane_values"
  [[ "$endpoint_samples_before" =~ ^[0-9]+$ && \
    "$data_plane_samples_before" =~ ^[0-9]+$ ]] || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  [[ "${M0_ENFORCE_RUN_HEALTH:-0}" != "1" ]] || \
    sample_once_for "$run_dir" || return 1
  append_event_to "$run_dir" \
    "$SOAK_STAGE idle start label=$label planned_secs=$idle_secs"
  m0_progress "$SOAK_LABEL idle drain $label start duration=${idle_secs}s"
  run_m0_logged "$run_dir/$SOAK_EVIDENCE_DIR/${label}.sleep.log" \
    "$((10#$idle_secs + 30))" "$M0_SLEEP_BIN" "$idle_secs" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
    capture_m2_checkpoint_after_drain \
      "$run_dir" "$label" "$endpoint_samples_before" \
      "$data_plane_samples_before" || return 1
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE idle complete label=$label planned_secs=$idle_secs"
  append_event_to "$run_dir" "$SOAK_STAGE resume start label=$label"
  M0_RESUME_PENDING=1
}

run_m2_schedule_body() {
  local run_dir="$1"
  local profile_file="$2"
  local total_secs steady_a_secs idle_secs quiet_a_secs steady_b_secs
  local churn_secs quiet_b_secs steady_c_secs final_drain_secs planned_sum
  local endpoint_values data_plane_values endpoint_samples_before
  local data_plane_samples_before
  total_secs="$(m0_profile_value "$profile_file" total_secs)"
  steady_a_secs="$(m0_profile_value "$profile_file" steady_a_secs)"
  idle_secs="$(m0_profile_value "$profile_file" idle_secs)"
  quiet_a_secs="$(m0_profile_value "$profile_file" quiet_a_secs)"
  steady_b_secs="$(m0_profile_value "$profile_file" steady_b_secs)"
  churn_secs="$(m0_profile_value "$profile_file" churn_secs)"
  quiet_b_secs="$(m0_profile_value "$profile_file" quiet_b_secs)"
  steady_c_secs="$(m0_profile_value "$profile_file" steady_c_secs)"
  final_drain_secs="$(m0_profile_value "$profile_file" final_drain_secs)"
  planned_sum=$((10#$steady_a_secs + 5 * 10#$idle_secs + \
    10#$quiet_a_secs + 10#$steady_b_secs + 10#$churn_secs + \
    10#$quiet_b_secs + 10#$steady_c_secs + 10#$final_drain_secs))
  ((planned_sum == 10#$total_secs)) || return 1
  M0_CYCLE_INDEX=0
  M0_RESUME_PENDING=0
  M2_COMPLETE_CYCLE_INDEX=0

  append_event_to "$run_dir" "$SOAK_STAGE start planned_secs=$total_secs"
  run_m0_active_window "$run_dir" steady-a "$steady_a_secs" \
    "$profile_file" steady || return 1
  run_m2_idle_window "$run_dir" idle-1 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" quiet-a "$quiet_a_secs" \
    "$profile_file" quiet || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m2_idle_window "$run_dir" idle-2 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" steady-b "$steady_b_secs" \
    "$profile_file" steady || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m2_idle_window "$run_dir" idle-3 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" churn "$churn_secs" \
    "$profile_file" churn || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m2_idle_window "$run_dir" idle-4 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" quiet-b "$quiet_b_secs" \
    "$profile_file" quiet || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1
  run_m2_idle_window "$run_dir" idle-5 "$idle_secs" || return 1
  run_m0_active_window "$run_dir" steady-c "$steady_c_secs" \
    "$profile_file" steady || return 1
  [[ "$M0_RESUME_PENDING" == "0" ]] || return 1

  m0_assert_run_healthy "$run_dir" || return 1
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r endpoint_samples_before _ <<<"$endpoint_values"
  data_plane_values="$(m2_data_plane_envelope "$run_dir/mini_vpn.log")"
  read -r data_plane_samples_before _ <<<"$data_plane_values"
  [[ "$endpoint_samples_before" =~ ^[0-9]+$ && \
    "$data_plane_samples_before" =~ ^[0-9]+$ ]] || return 1
  append_event_to "$run_dir" \
    "$SOAK_STAGE final drain start planned_secs=$final_drain_secs"
  m0_progress "$SOAK_LABEL final drain start duration=${final_drain_secs}s"
  run_m0_logged "$run_dir/$SOAK_EVIDENCE_DIR/final-drain.sleep.log" \
    "$((10#$final_drain_secs + 30))" \
    "$M0_SLEEP_BIN" "$final_drain_secs" || return 1
  m0_assert_run_healthy "$run_dir" || return 1
  if [[ "${M0_ENFORCE_RUN_HEALTH:-0}" == "1" ]]; then
    sample_once_for "$run_dir" || return 1
    capture_m2_checkpoint_after_drain \
      "$run_dir" final "$endpoint_samples_before" \
      "$data_plane_samples_before" || return 1
  fi
  append_event_to "$run_dir" \
    "$SOAK_STAGE final drain complete planned_secs=$final_drain_secs"
  append_event_to "$run_dir" \
    "$SOAK_STAGE complete cycles=$M0_CYCLE_INDEX planned_secs=$total_secs"
  m0_progress "$SOAK_LABEL workload timeline complete"
}

run_m2_schedule() {
  local run_dir="$1"
  local status_file="$run_dir/m2.status"
  local result
  printf '%s\n' running >"$status_file" || return 1
  if run_m2_schedule_body "$@"; then
    printf '%s\n' complete >"$status_file" || return 1
    return 0
  else
    result=$?
    printf '%s\n' failed >"$status_file" || return 1
    append_event_to "$run_dir" "m2 failed"
    return "$result"
  fi
}

run_m2_qualification_body() {
  local run_dir="$1"
  local profile_file="$2"
  local target iperf_port dns_target dns_name tcp_secs udp_secs short_secs
  local tcp_forward_bps tcp_reverse_bps udp_reverse_bps short_forward_bps
  local cycle_index real_client_label
  target="$(m0_profile_value "$profile_file" target)"
  iperf_port="$(m0_profile_value "$profile_file" iperf_port)"
  dns_target="$(m0_profile_value "$profile_file" dns_target)"
  [[ "$dns_target" == "disabled" ]] && dns_target=""
  dns_name="$(m0_profile_value "$profile_file" dns_name)"
  tcp_secs="$(m0_profile_value "$profile_file" tcp_epoch_secs)"
  udp_secs="$(m0_profile_value "$profile_file" udp_epoch_secs)"
  short_secs="$(m0_profile_value "$profile_file" short_epoch_secs)"
  tcp_forward_bps="$(m0_profile_value "$profile_file" steady_tcp_forward_bps)"
  tcp_reverse_bps="$(m0_profile_value "$profile_file" steady_tcp_reverse_bps)"
  udp_reverse_bps="$(m0_profile_value "$profile_file" steady_udp_reverse_bps)"
  short_forward_bps="$(m0_profile_value "$profile_file" steady_short_forward_bps)"

  M0_CYCLE_INDEX=0
  M0_RESUME_PENDING=0
  M2_COMPLETE_CYCLE_INDEX=0
  append_event_to "$run_dir" \
    "$SOAK_STAGE start exact_cycles=2 planned_secs=$((2 * (10#$tcp_secs * 2 + 10#$udp_secs + 10#$short_secs)))"
  for cycle_index in 1 2; do
    M0_CYCLE_INDEX="$cycle_index"
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$cycle_index" \
      tcp-forward "$tcp_secs" "$tcp_forward_bps" 0 0 || return 1
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$cycle_index" \
      tcp-reverse "$tcp_secs" "$tcp_reverse_bps" 1 0 || return 1
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$cycle_index" \
      udp-reverse "$udp_secs" "$udp_reverse_bps" 1 1 || return 1
    run_m0_iperf_phase "$run_dir" "$target" "$iperf_port" "$cycle_index" \
      short-forward-1 "$short_secs" "$short_forward_bps" 0 0 || return 1
    run_m0_dns_phase "$run_dir" "$dns_target" "$dns_name" "$cycle_index" || return 1
    M2_COMPLETE_CYCLE_INDEX="$cycle_index"
    if [[ "${SOAK_REAL_CLIENT_PROBE:-0}" == "1" ]]; then
      printf -v real_client_label 'cycle_%03d' "$cycle_index"
      run_m2_real_client_probe "$run_dir" "$real_client_label" || {
        append_event_to "$run_dir" \
          "$SOAK_STAGE real client failed cycle=$cycle_index"
        return 1
      }
    fi
    append_event_to "$run_dir" \
      "$SOAK_STAGE cycle complete cycle=$cycle_index"
    m0_progress \
      "$SOAK_LABEL exact mixed cycle complete cycle=$cycle_index/2"
  done
  append_event_to "$run_dir" "$SOAK_STAGE complete exact_cycles=2"
}

run_m2_qualification_schedule() {
  local run_dir="$1"
  local profile_file="$2"
  local status_file="$run_dir/m2-qualification.status"
  local result
  printf '%s\n' running >"$status_file" || return 1
  if run_m2_qualification_body "$run_dir" "$profile_file"; then
    printf '%s\n' PASS_NON_ACCEPTANCE >"$status_file" || return 1
    return 0
  else
    result=$?
    printf '%s\n' failed >"$status_file" || return 1
    append_event_to "$run_dir" "$SOAK_STAGE failed"
    return "$result"
  fi
}

run_smoke() {
  local run_dir utun target exit_host smoke_dir iperf_port duration parallel dns_name dns_target smoke_timeout_secs
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  active_pid || die "mini_vpn is not running"
  utun="$(read_state utun)"
  target="$(read_state target)"
  exit_host="$(read_state exit_host)"
  iperf_port="$(read_state iperf_port)"
  duration="$(read_state duration)"
  parallel="$(read_state parallel)"
  dns_name="$(read_state dns_name)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  smoke_timeout_secs=$((10#$duration + 30))
  [[ "$(route_interface "$target")" == "$utun" ]] || die "TARGET no longer routes through $utun"
  [[ "$(route_interface "$exit_host")" != "$utun" ]] || die "Exit route recursed into $utun"
  require_command iperf3
  smoke_dir="$run_dir/smoke_$(date -u '+%Y%m%d_%H%M%S')"
  mkdir -p "$smoke_dir"
  append_event_to "$run_dir" "smoke start"
  echo "Running tunnel forward smoke: duration=${duration}s hard_timeout=${smoke_timeout_secs}s"
  if ! run_logged_with_timeout "$smoke_dir/tunnel-forward.json" "$smoke_timeout_secs" \
    iperf3 -c "$target" -p "$iperf_port" -t "$duration" -P "$parallel" --json; then
    append_event_to "$run_dir" "smoke forward failed"
    die "tunnel forward smoke failed; leave TUN running for status/stop"
  fi
  echo "Running tunnel reverse smoke: duration=${duration}s hard_timeout=${smoke_timeout_secs}s"
  if ! run_logged_with_timeout "$smoke_dir/tunnel-reverse.json" "$smoke_timeout_secs" \
    iperf3 -c "$target" -p "$iperf_port" -t "$duration" -P "$parallel" -R --json; then
    append_event_to "$run_dir" "smoke reverse failed"
    die "tunnel reverse smoke failed; leave TUN running for status/stop"
  fi
  if [[ -n "$dns_target" ]]; then
    require_command dig
    if ! run_logged "$smoke_dir/dns.txt" \
      dig @"$dns_target" "$dns_name" A +time=5 +tries=1; then
      append_event_to "$run_dir" "smoke DNS failed"
      die "DNS smoke failed; leave TUN running for status/stop"
    fi
  fi
  echo "Waiting for smoke TCP-pool ownership to drain: hard_timeout=${smoke_timeout_secs}s"
  if ! wait_for_tcp_pool_idle "$run_dir/mini_vpn.log" "$smoke_timeout_secs"; then
    append_event_to "$run_dir" "smoke TCP pool drain failed"
    die "smoke TCP-pool ownership did not drain; leave TUN running for status/snapshot/stop"
  fi
  append_event_to "$run_dir" "smoke TCP pool idle"
  sample_once_for "$run_dir"
  append_event_to "$run_dir" "smoke complete"
  echo "PASS: target-only smoke completed: $smoke_dir"
}

run_m0_action() {
  local run_dir utun target exit_host iperf_port dns_target dns_name profile_file
  require_root
  validate_m0_formal_config || \
    die "formal M0 requires the frozen 7200s workload schedule; unset M0_* duration overrides"
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  active_pid || die "mini_vpn is not running"
  workload_matches_run && die "an M0 workload is already running"
  [[ ! -e "$run_dir/m0.status" && ! -e "$run_dir/m0-workload.txt" && \
    ! -e "$run_dir/m0" && ! -e "$run_dir/m1.status" && \
    ! -e "$run_dir/m1-workload.txt" && ! -e "$run_dir/m1" && \
    ! -e "$run_dir/m2.status" && ! -e "$run_dir/m2-workload.txt" && \
    ! -e "$run_dir/m2" && ! -e "$run_dir/m2-qualification.status" && \
    ! -e "$run_dir/m2-qualification" ]] || \
    die "this TUN run already has soak evidence; stop and start a fresh run"
  validate_baseline_dir_path "$M0_BASELINE_DIR" || \
    die "M0_BASELINE_DIR must be the simple /tmp baseline directory printed by baseline"
  [[ -d "$M0_BASELINE_DIR" && ! -L "$M0_BASELINE_DIR" ]] || \
    die "M0_BASELINE_DIR must be an existing non-symlink directory"
  validate_direct_dir_path "$M0_DIRECT_DIR" || \
    die "M0_DIRECT_DIR must be the fresh directory printed by direct-discriminator"
  [[ -d "$M0_DIRECT_DIR" && ! -L "$M0_DIRECT_DIR" ]] || \
    die "M0_DIRECT_DIR must be an existing non-symlink directory"

  utun="$(read_state utun)"
  target="$(read_state target)"
  exit_host="$(read_state exit_host)"
  iperf_port="$(read_state iperf_port)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  dns_name="$(read_state dns_name)"
  [[ -n "$dns_target" ]] || die "formal M0 requires DNS_TARGET for periodic DNS evidence"
  [[ "$(route_interface "$target")" == "$utun" ]] || \
    die "TARGET no longer routes through $utun"
  [[ "$(route_interface "$exit_host")" != "$utun" ]] || \
    die "Exit route recursed into $utun"
  network_control_is_sufficient "$run_dir" 5 && network_control_is_recent || \
    die "formal M0 requires complete, recent Exit and physical-interface controls from start/smoke"
  tcp_pool_activity_is_idle "$run_dir/mini_vpn.log" || \
    die "formal M0 requires fully drained TCP-pool ownership after smoke"
  require_command jq
  require_command iperf3
  require_command dig
  validate_m0_baseline_pair "$M0_BASELINE_DIR" "$target" || \
    die "M0 baseline must contain valid nonzero TCP forward/reverse results for $target"
  validate_direct_continuity_dir "$M0_DIRECT_DIR" "$M0_BASELINE_DIR" "$target" || \
    die "formal M0 requires a matching 300s direct continuity PASS completed within 15 minutes"

  mkdir "$run_dir/m0" || die "cannot create M0 evidence directory"
  mkdir "$run_dir/m0-direct" || die "cannot create M0 direct evidence directory"
  cp "$M0_DIRECT_DIR/manifest.txt" "$run_dir/m0-direct/manifest.txt" || \
    die "cannot preserve direct continuity manifest"
  cp "$M0_DIRECT_DIR/direct-forward-300s.json" \
    "$run_dir/m0-direct/direct-forward-300s.json" || \
    die "cannot preserve direct continuity result"
  profile_file="$run_dir/m0-workload.txt"
  printf '%s\n' preparing >"$run_dir/m0.status"
  if ! write_m0_profile "$M0_BASELINE_DIR" "$profile_file" "$target" "$iperf_port" \
    "$dns_target" "$dns_name"; then
    printf '%s\n' failed >"$run_dir/m0.status"
    append_event_to "$run_dir" "m0 failed: workload profile derivation"
    die "cannot derive the M0 workload profile from baseline"
  fi
  printf '%s\n' \
    "direct_dir=$M0_DIRECT_DIR" \
    "direct_manifest_sha256=$(sha256_file "$M0_DIRECT_DIR/manifest.txt")" \
    "direct_result_sha256=$(sha256_file "$M0_DIRECT_DIR/direct-forward-300s.json")" \
    >>"$profile_file" || die "cannot bind direct continuity evidence to M0 profile"
  printf '%s\n' prepared >"$run_dir/m0.status"
  append_event_to "$run_dir" \
    "m0 prepared baseline=$(basename "$M0_BASELINE_DIR") profile=$(sha256_file "$profile_file")"
  echo "Starting formal Knife15 M0 workload; planned traffic time is 7200 seconds."
  grep -E '^(baseline_(forward|reverse)_bps|tcp_(forward|reverse)_bps|udp_reverse_bps|short_(forward|reverse)_bps|udp_payload_bytes|idle_secs|final_drain_secs)=' \
    "$profile_file"

  M0_IPERF3_BIN="$(command -v iperf3)"
  M0_DIG_BIN="$(command -v dig)"
  M0_SLEEP_BIN=/bin/sleep
  M0_TRACK_CHILD=1
  M0_ENFORCE_RUN_HEALTH=1
  M0_ACTIVE_RUN_DIR="$run_dir"
  if ! register_m0_workload; then
    clear_workload_state
    printf '%s\n' failed >"$run_dir/m0.status"
    append_event_to "$run_dir" "m0 failed: workload identity registration"
    die "cannot establish identity-verified M0 workload state"
  fi
  trap "interrupt_m0 '$run_dir' INT" INT
  trap "interrupt_m0 '$run_dir' TERM" TERM
  trap "interrupt_m0 '$run_dir' HUP" HUP

  if run_m0_schedule "$run_dir" "$profile_file"; then
    trap - INT TERM HUP
    clear_workload_state
    if ! sample_once_for "$run_dir" || ! m0_assert_run_healthy "$run_dir" || \
      ! m0_final_ownership_is_clean "$run_dir/mini_vpn.log" || \
      ! network_control_is_sufficient "$run_dir" 5; then
      printf '%s\n' failed >"$run_dir/m0.status"
      append_event_to "$run_dir" "m0 failed: final evidence, health, ownership, or network controls"
      die "M0 traffic completed but final evidence/health/ownership/network controls failed; TUN remains for stop"
    fi
    echo "PASS: formal 2-hour M0 workload completed; TUN remains running"
    echo "run_dir=$run_dir"
    echo "Next: sudo -E bash scripts/knife15-macos-soak.sh status"
    echo "Then: sudo -E bash scripts/knife15-macos-soak.sh stop"
    return 0
  fi
  trap - INT TERM HUP
  clear_workload_state
  sample_once_for "$run_dir" || true
  die "M0 workload failed; TUN remains running for status/snapshot/stop evidence"
}

record_m1_diagnostic_aggregate_violations() {
  local run_dir="$1"
  local tcp_results udp_results tcp_max_gap rest
  read -r tcp_results udp_results tcp_max_gap rest \
    <<<"$(m0_result_envelope "$run_dir/m1")"
  [[ "$tcp_results" =~ ^[0-9]+$ && "$udp_results" =~ ^[0-9]+$ && \
    "$tcp_max_gap" =~ ^[0-9]+$ ]] || return 1
  if ((10#$tcp_max_gap > 16777216)); then
    record_soak_data_quality_violation "$run_dir" 0 aggregate \
      tcp_sender_receiver_gap_bytes "$tcp_max_gap" limit=16777216 \
      "$run_dir/m1" || return 1
  fi
}

m1_diagnostic_violation_coverage_is_complete() {
  local run_dir="$1"
  local violation_file="$run_dir/m1-diagnostic-violations.tsv"
  local violation_count invalid_violations
  local tcp_results udp_results tcp_max_gap udp_max_loss invalid_results
  local sender_zero receiver_zero
  local row_timestamp cycle phase kind value detail evidence evidence_file
  local actual_value actual_detail reverse expected_udp=0
  local recorded_receiver=0 recorded_receiver_rows=0 recorded_udp=0 recorded_gap=0
  local result_file
  read -r violation_count invalid_violations \
    <<<"$(m1_diagnostic_violation_envelope "$violation_file")"
  [[ "$invalid_violations" == "0" ]] || return 1
  read -r tcp_results udp_results tcp_max_gap udp_max_loss invalid_results \
    sender_zero receiver_zero \
    <<<"$(m0_result_envelope "$run_dir/m1")"
  [[ "$tcp_results" =~ ^[0-9]+$ && "$udp_results" =~ ^[0-9]+$ && \
    "$tcp_max_gap" =~ ^[0-9]+$ && "$invalid_results" == "0" && \
    "$receiver_zero" =~ ^[0-9]+$ ]] || return 1

  while IFS=$'\t' read -r row_timestamp cycle phase kind value detail evidence; do
    [[ -n "$row_timestamp" ]] || continue
    case "$kind" in
      receiver_zero_interval)
        [[ "$evidence" =~ ^m1/[^/]+[.]json$ ]] || return 1
        evidence_file="$run_dir/$evidence"
        [[ -f "$evidence_file" && ! -L "$evidence_file" ]] || return 1
        reverse="$(jq -er '.start.test_start.reverse' "$evidence_file" 2>/dev/null)" || \
          return 1
        read -r actual_value actual_detail \
          <<<"$(receiver_zero_interval_evidence "$evidence_file" "$reverse")"
        [[ "$value" == "$actual_value" && "$detail" == "$actual_detail" ]] || \
          return 1
        recorded_receiver=$((recorded_receiver + 10#$value))
        recorded_receiver_rows=$((recorded_receiver_rows + 1))
        ;;
      udp_loss_percent)
        [[ "$evidence" =~ ^m1/[^/]+[.]json$ ]] || return 1
        evidence_file="$run_dir/$evidence"
        [[ -f "$evidence_file" && ! -L "$evidence_file" ]] || return 1
        [[ "$(jq -er '.start.test_start.protocol' "$evidence_file" 2>/dev/null)" == \
          "UDP" ]] || return 1
        actual_value="$(udp_loss_percent "$evidence_file")" || return 1
        awk -v left="$value" -v right="$actual_value" \
          'BEGIN { exit !((left + 0) == (right + 0)) }' || return 1
        ! decimal_le "$value" 3.0 || return 1
        [[ "$detail" == "limit=3.0" ]] || return 1
        recorded_udp=$((recorded_udp + 1))
        ;;
      tcp_sender_receiver_gap_bytes)
        [[ "$cycle" == "0" && "$phase" == "aggregate" && \
          "$evidence" == "m1" && "$value" == "$tcp_max_gap" && \
          "$detail" == "limit=16777216" ]] || return 1
        ((10#$value > 16777216)) || return 1
        recorded_gap=$((recorded_gap + 1))
        ;;
      *)
        return 1
        ;;
    esac
  done < <(tail -n +2 "$violation_file")

  for result_file in "$run_dir/m1"/*.json; do
    [[ -f "$result_file" ]] || continue
    if jq -e '
      .start.test_start.protocol == "UDP"
      and ((.end.sum.lost_percent // .end.sum_received.lost_percent) > 3.0)
    ' "$result_file" >/dev/null 2>&1; then
      expected_udp=$((expected_udp + 1))
    fi
  done
  ((recorded_receiver == 10#$receiver_zero && recorded_udp == expected_udp)) || \
    return 1
  if ((10#$tcp_max_gap > 16777216)); then
    ((recorded_gap == 1)) || return 1
  else
    ((recorded_gap == 0)) || return 1
  fi
  ((10#$violation_count == recorded_receiver_rows + recorded_udp + recorded_gap))
}

run_m1_action() {
  local execution_mode="${1:-formal}"
  local run_dir utun target exit_host iperf_port dns_target dns_name profile_file
  local action_description=m1 stage=m1 label=M1 success_status=complete
  local diagnostic=0 violation_count invalid_violations
  case "$execution_mode" in
    formal)
      action_description="formal M1"
      ;;
    diagnostic)
      action_description="M1 diagnostic"
      stage=m1-diagnostic
      label=M1-DIAGNOSTIC
      success_status=diagnostic_timeline_complete
      diagnostic=1
      ;;
    *)
      die "unknown M1 execution mode: $execution_mode"
      ;;
  esac
  require_root
  validate_m1_formal_config || \
    die "$action_description requires the frozen 28800s schedule and 30s metrics/sampling; unset M1_* duration overrides"
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  active_pid || die "mini_vpn is not running"
  workload_matches_run && die "a soak workload is already running"
  [[ ! -e "$run_dir/m0.status" && ! -e "$run_dir/m0-workload.txt" && \
    ! -e "$run_dir/m0" && ! -e "$run_dir/m1.status" && \
    ! -e "$run_dir/m1-workload.txt" && ! -e "$run_dir/m1" && \
    ! -e "$run_dir/m2.status" && ! -e "$run_dir/m2-workload.txt" && \
    ! -e "$run_dir/m2" && ! -e "$run_dir/m2-qualification.status" && \
    ! -e "$run_dir/m2-qualification" ]] || \
    die "this TUN run already has soak evidence; stop and start a fresh run"
  [[ -z "$M0_BASELINE_DIR" && -z "$M0_DIRECT_DIR" ]] || \
    die "$action_description requires M0_BASELINE_DIR and M0_DIRECT_DIR to be unset"
  validate_baseline_dir_path "$M1_BASELINE_DIR" || \
    die "M1_BASELINE_DIR must be the simple /tmp baseline directory printed by baseline"
  [[ -d "$M1_BASELINE_DIR" && ! -L "$M1_BASELINE_DIR" ]] || \
    die "M1_BASELINE_DIR must be an existing non-symlink directory"
  validate_direct_dir_path "$M1_DIRECT_DIR" || \
    die "M1_DIRECT_DIR must be the fresh directory printed by direct-discriminator"
  [[ -d "$M1_DIRECT_DIR" && ! -L "$M1_DIRECT_DIR" ]] || \
    die "M1_DIRECT_DIR must be an existing non-symlink directory"

  utun="$(read_state utun)"
  target="$(read_state target)"
  exit_host="$(read_state exit_host)"
  iperf_port="$(read_state iperf_port)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  dns_name="$(read_state dns_name)"
  [[ -n "$dns_target" ]] || \
    die "$action_description requires DNS_TARGET for periodic DNS evidence"
  [[ "$(route_interface "$target")" == "$utun" ]] || \
    die "TARGET no longer routes through $utun"
  [[ "$(route_interface "$exit_host")" != "$utun" ]] || \
    die "Exit route recursed into $utun"
  network_control_is_sufficient "$run_dir" 5 && network_control_is_recent || \
    die "$action_description requires complete, recent Exit and physical-interface controls from start/smoke"
  tcp_pool_activity_is_idle "$run_dir/mini_vpn.log" || \
    die "$action_description requires fully drained TCP-pool ownership after smoke"
  require_command jq
  require_command iperf3
  require_command dig
  validate_m0_baseline_pair "$M1_BASELINE_DIR" "$target" || \
    die "M1 baseline must contain valid nonzero TCP forward/reverse results for $target"
  validate_direct_continuity_dir "$M1_DIRECT_DIR" "$M1_BASELINE_DIR" "$target" || \
    die "$action_description requires a matching 300s direct continuity PASS completed within 15 minutes"

  mkdir "$run_dir/m1" || die "cannot create M1 evidence directory"
  mkdir "$run_dir/m1-baseline" || die "cannot create M1 baseline evidence directory"
  mkdir "$run_dir/m1-direct" || die "cannot create M1 direct evidence directory"
  cp "$M1_BASELINE_DIR/direct-forward.json" \
    "$run_dir/m1-baseline/direct-forward.json" || \
    die "cannot preserve M1 forward baseline"
  cp "$M1_BASELINE_DIR/direct-reverse.json" \
    "$run_dir/m1-baseline/direct-reverse.json" || \
    die "cannot preserve M1 reverse baseline"
  cp "$M1_DIRECT_DIR/manifest.txt" "$run_dir/m1-direct/manifest.txt" || \
    die "cannot preserve direct continuity manifest"
  cp "$M1_DIRECT_DIR/direct-forward-300s.json" \
    "$run_dir/m1-direct/direct-forward-300s.json" || \
    die "cannot preserve direct continuity result"
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes' \
    >"$run_dir/m1-checkpoints.csv" || die "cannot create M1 checkpoint evidence"
  printf '%s\n' "$execution_mode" >"$run_dir/m1-mode" || \
    die "cannot create M1 execution-mode evidence"
  if ((diagnostic == 1)); then
    printf '%s\n' $'timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence' \
      >"$run_dir/m1-diagnostic-violations.tsv" || \
      die "cannot create M1 diagnostic violation evidence"
  fi
  profile_file="$run_dir/m1-workload.txt"
  printf '%s\n' preparing >"$run_dir/m1.status"
  if ! write_m1_profile "$M1_BASELINE_DIR" "$profile_file" "$target" \
    "$iperf_port" "$dns_target" "$dns_name"; then
    printf '%s\n' failed >"$run_dir/m1.status"
    append_event_to "$run_dir" "$stage failed: workload profile derivation"
    die "cannot derive the M1 workload profile from baseline"
  fi
  printf '%s\n' \
    "execution_mode=$execution_mode" \
    "direct_dir=$M1_DIRECT_DIR" \
    "direct_manifest_sha256=$(sha256_file "$M1_DIRECT_DIR/manifest.txt")" \
    "direct_result_sha256=$(sha256_file "$M1_DIRECT_DIR/direct-forward-300s.json")" \
    >>"$profile_file" || die "cannot bind direct continuity evidence to M1 profile"
  printf '%s\n' prepared >"$run_dir/m1.status"
  append_event_to "$run_dir" \
    "$stage prepared baseline=$(basename "$M1_BASELINE_DIR") profile=$(sha256_file "$profile_file")"
  if ((diagnostic == 1)); then
    echo "Starting Knife15 M1 diagnostic workload; planned traffic/drain time is 28800 seconds."
    echo "Data-quality violations will be recorded and continued; safety failures still stop immediately."
    echo "This run can never satisfy formal M1 acceptance."
  else
    echo "Starting formal Knife15 M1 workload; planned traffic/drain time is 28800 seconds."
  fi
  grep -E '^(baseline_(forward|reverse)_bps|rate_cap_bps|steady_(tcp|udp|short)|quiet_(tcp|udp|short)|churn_(tcp|udp|short)|total_secs|steady_a_secs|idle_secs|quiet_secs|steady_b_secs|churn_secs|steady_c_secs|final_drain_secs)=' \
    "$profile_file"

  SOAK_STAGE="$stage"
  SOAK_LABEL="$label"
  SOAK_EVIDENCE_DIR=m1
  SOAK_STATUS_FILE=m1.status
  SOAK_SUCCESS_STATUS="$success_status"
  SOAK_CONTINUE_DATA_QUALITY="$diagnostic"
  SOAK_VIOLATIONS_FILE=
  ((diagnostic == 1)) && \
    SOAK_VIOLATIONS_FILE="$run_dir/m1-diagnostic-violations.tsv"
  M0_IPERF3_BIN="$(command -v iperf3)"
  M0_DIG_BIN="$(command -v dig)"
  M0_SLEEP_BIN=/bin/sleep
  M0_TRACK_CHILD=1
  M0_ENFORCE_RUN_HEALTH=1
  M0_ACTIVE_RUN_DIR="$run_dir"
  if ! register_m0_workload; then
    clear_workload_state
    printf '%s\n' failed >"$run_dir/m1.status"
    append_event_to "$run_dir" "$stage failed: workload identity registration"
    die "cannot establish identity-verified M1 workload state"
  fi
  trap "interrupt_m0 '$run_dir' INT" INT
  trap "interrupt_m0 '$run_dir' TERM" TERM
  trap "interrupt_m0 '$run_dir' HUP" HUP

  if run_m1_schedule "$run_dir" "$profile_file"; then
    trap - INT TERM HUP
    clear_workload_state
    if ! sample_once_for "$run_dir" || ! m0_assert_run_healthy "$run_dir" || \
      ! m0_final_ownership_is_clean "$run_dir/mini_vpn.log" || \
      ! network_control_is_sufficient "$run_dir" 5 || \
      ! m1_checkpoint_slo "$run_dir/m1-checkpoints.csv"; then
      printf '%s\n' failed >"$run_dir/m1.status"
      append_event_to "$run_dir" \
        "$stage failed: final evidence, health, ownership, network controls, or checkpoints"
      die "M1 traffic completed but final evidence/health/ownership/network/checkpoints failed; TUN remains for stop"
    fi
    if ((diagnostic == 1)); then
      if ! record_m1_diagnostic_aggregate_violations "$run_dir"; then
        printf '%s\n' failed >"$run_dir/m1.status"
        append_event_to "$run_dir" \
          "$stage failed: aggregate result evidence"
        die "M1 diagnostic timeline completed but aggregate result evidence failed; TUN remains for stop"
      fi
      read -r violation_count invalid_violations \
        <<<"$(m1_diagnostic_violation_envelope \
          "$run_dir/m1-diagnostic-violations.tsv")"
      if [[ "$invalid_violations" != "0" ]]; then
        printf '%s\n' failed >"$run_dir/m1.status"
        append_event_to "$run_dir" \
          "$stage failed: invalid violation evidence"
        die "M1 diagnostic violation evidence is invalid; TUN remains for stop"
      fi
      if ((10#$violation_count > 0)); then
        printf '%s\n' diagnostic_complete_with_violations >"$run_dir/m1.status"
      else
        printf '%s\n' diagnostic_complete_clean >"$run_dir/m1.status"
      fi
      write_summary "$run_dir"
      if ! grep -Fq -- '- m1_diagnostic_safety_evidence: PASS' \
        "$run_dir/summary.md"; then
        printf '%s\n' failed >"$run_dir/m1.status"
        append_event_to "$run_dir" "$stage failed: final safety mismatch"
        write_summary "$run_dir"
        die "M1 diagnostic timeline completed but final safety evidence failed; TUN remains for status/snapshot/stop"
      fi
      echo "COMPLETE: 8-hour M1 diagnostic timeline collected with $violation_count data-quality violation(s)."
      echo "This is diagnostic evidence, not formal M1 acceptance."
      echo "run_dir=$run_dir"
      echo "Next: sudo -E bash scripts/knife15-macos-soak.sh status"
      echo "Then: sudo -E bash scripts/knife15-macos-soak.sh stop"
      return 0
    fi
    write_summary "$run_dir"
    if ! grep -Fq -- '- m1_slo_evidence: PASS' "$run_dir/summary.md"; then
      printf '%s\n' failed >"$run_dir/m1.status"
      append_event_to "$run_dir" "$stage failed: acceptance SLO mismatch"
      die "M1 traffic completed but its acceptance SLO failed; TUN remains for status/snapshot/stop"
    fi
    echo "PASS: formal 8-hour M1 workload completed; TUN remains running"
    echo "run_dir=$run_dir"
    echo "Next: sudo -E bash scripts/knife15-macos-soak.sh status"
    echo "Then: sudo -E bash scripts/knife15-macos-soak.sh stop"
    return 0
  fi
  trap - INT TERM HUP
  clear_workload_state
  sample_once_for "$run_dir" || true
  die "$action_description workload failed; TUN remains running for status/snapshot/stop evidence"
}

m2_real_client_envelope() {
  local results_dir="$1"
  local expected_index=1 count=0 invalid=0 result_file expected_label
  [[ -d "$results_dir" && ! -L "$results_dir" ]] || {
    printf '0 1\n'
    return
  }
  for result_file in "$results_dir"/cycle_*.txt; do
    [[ -f "$result_file" && ! -L "$result_file" ]] || continue
    expected_label="cycle_$(printf '%03d' "$expected_index")"
    if [[ "$(basename "$result_file" .txt)" != "$expected_label" ]] || \
      ! m2_real_client_result_is_valid "$result_file"; then
      invalid=$((invalid + 1))
    fi
    count=$((count + 1))
    expected_index=$((expected_index + 1))
  done
  printf '%s %s\n' "$count" "$invalid"
}

m2_source_is_accepted() {
  local revision="${1:-HEAD}"
  git -C "$REPO" merge-base --is-ancestor 60a4e94 "$revision" >/dev/null 2>&1
}

m2_resource_result_value() {
  local file_path="${1:-}" key="${2:-}"
  [[ -f "$file_path" && ! -L "$file_path" ]] || return 1
  m2_exit_observer_value_from_text "$key" <"$file_path"
}

m2_resource_evidence_name_is_allowed() {
  case "${1:-}" in
    reference-profile.json|candidate-profile.json|eligibility.json|\
    evidence-binding.json|\
    direct-manifest.txt|provider-identity.txt|route-identity.txt|\
    prior-saturation.txt|replacement-capacity.txt|exit.route.txt|\
    target.route.txt|exit.traceroute.txt|target.traceroute.txt|remote.txt|\
    remote.stderr|secret-scan.txt|result.txt|SHA256SUMS)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

m2_resource_archive_matches_directory() {
  local evidence_dir="${1:-}" archive="${2:-}"
  local archive_listing expected_listing archive_root evidence_file name
  [[ -d "$evidence_dir" && ! -L "$evidence_dir" && \
    -f "$archive" && ! -L "$archive" ]] || return 1
  archive_listing="$(tar -tzf "$archive" 2>/dev/null | LC_ALL=C sort)" || \
    return 1
  archive_root="$(printf '%s\n' "$archive_listing" | \
    awk '{
      value = $0
      slash_count = gsub(/\//, "/", value)
      if (slash_count == 1 && substr($0, length($0), 1) == "/") {
        count++
        root = $0
      }
    }
    END {if (count != 1) exit 1; print root}')" || \
    return 1
  [[ "$archive_root" =~ \
    ^mini_vpn_knife15_resource_[A-Za-z0-9._-]+/$ ]] || return 1
  expected_listing="$archive_root"
  for evidence_file in "$evidence_dir"/*; do
    [[ -f "$evidence_file" && ! -L "$evidence_file" ]] || return 1
    name="$(basename "$evidence_file")"
    m2_resource_evidence_name_is_allowed "$name" || return 1
    expected_listing+=$'\n'"$archive_root$name"
  done
  expected_listing="$(printf '%s\n' "$expected_listing" | LC_ALL=C sort)"
  [[ "$archive_listing" == "$expected_listing" ]] || return 1
  for evidence_file in "$evidence_dir"/*; do
    name="$(basename "$evidence_file")"
    cmp -s "$evidence_file" \
      <(tar -xOzf "$archive" "$archive_root$name" 2>/dev/null) || return 1
  done
}

m2_resource_state_binding_is_valid() {
  local stage="${1:-}" candidate_id="${2:-}"
  local profile_sha="${3:-}" result_sha="${4:-}"
  [[ "$stage" == m2 || "$stage" == m2-qualification ]] || return 1
  [[ -n "$candidate_id" && "$profile_sha" =~ ^[0-9a-f]{64}$ && \
    "$result_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$(read_state m2.resource_stage)" == "$stage" && \
    "$(read_state m2.resource_candidate)" == "$candidate_id" && \
    "$(read_state m2.resource_profile_sha256)" == "$profile_sha" && \
    "$(read_state m2.resource_result_sha256)" == "$result_sha" ]]
}

m2_resource_evidence_is_valid() {
  local evidence_dir="${1:-}" expected_source="${2:-}"
  local expected_binary_sha="${3:-}" expected_direct_manifest="${4:-}"
  local expected_observer_sha="${5:-}" expected_exit="${6:-}"
  local expected_tuic_port="${7:-}" expected_target="${8:-}"
  local expected_iperf_port="${9:-}" expected_interface="${10:-}"
  local required_file evidence_file evidence_name expected_eligibility
  local observed_eligibility candidate_validation reference_validation
  local observed_evidence_binding expected_evidence_binding
  local candidate_profile_sha reference_profile_sha eligibility_reason
  local result_file result_text remote_file candidate_file reference_file
  local actual_sha expected_sha result_key remote_cpu remote_memory
  local bounded_evidence bounded_size

  [[ -d "$evidence_dir" && ! -L "$evidence_dir" ]] || return 1
  for required_file in \
    reference-profile.json candidate-profile.json eligibility.json \
    evidence-binding.json \
    direct-manifest.txt provider-identity.txt route-identity.txt \
    exit.route.txt target.route.txt exit.traceroute.txt target.traceroute.txt \
    remote.txt remote.stderr secret-scan.txt result.txt SHA256SUMS; do
    [[ -f "$evidence_dir/$required_file" && \
      ! -L "$evidence_dir/$required_file" ]] || return 1
  done
  for evidence_file in "$evidence_dir"/*; do
    [[ -f "$evidence_file" && ! -L "$evidence_file" ]] || return 1
    evidence_name="$(basename "$evidence_file")"
    m2_resource_evidence_name_is_allowed "$evidence_name" || return 1
  done
  for bounded_evidence in provider-identity.txt route-identity.txt; do
    bounded_size="$(wc -c <"$evidence_dir/$bounded_evidence" | \
      tr -d '[:space:]')"
    [[ "$bounded_size" =~ ^[0-9]+$ && 10#$bounded_size -gt 0 && \
      10#$bounded_size -le 65536 ]] || return 1
  done
  (cd "$evidence_dir" && \
    /usr/bin/shasum -a 256 -c SHA256SUMS >/dev/null 2>&1) || return 1

  candidate_file="$evidence_dir/candidate-profile.json"
  reference_file="$evidence_dir/reference-profile.json"
  observed_eligibility="$(tr -d '\n' <"$evidence_dir/eligibility.json")"
  expected_eligibility="$(/usr/bin/python3 -I \
    "$M2_RESOURCE_PROFILE_HELPER" compare --reference "$reference_file" \
    --candidate "$candidate_file")" || return 1
  [[ "$observed_eligibility" == "$expected_eligibility" && \
    "$(m2_resource_json_value "$evidence_dir/eligibility.json" eligible)" == \
      true ]] || return 1
  observed_evidence_binding="$(tr -d '\n' \
    <"$evidence_dir/evidence-binding.json")"
  expected_evidence_binding="$(/usr/bin/python3 -I \
    "$M2_RESOURCE_PROFILE_HELPER" validate-evidence \
    --candidate "$candidate_file" \
    --provider-evidence "$evidence_dir/provider-identity.txt" \
    --route-evidence "$evidence_dir/route-identity.txt")" || return 1
  [[ "$observed_evidence_binding" == "$expected_evidence_binding" ]] || return 1
  candidate_validation="$(/usr/bin/python3 -I \
    "$M2_RESOURCE_PROFILE_HELPER" validate "$candidate_file")" || return 1
  reference_validation="$(/usr/bin/python3 -I \
    "$M2_RESOURCE_PROFILE_HELPER" validate "$reference_file")" || return 1
  candidate_profile_sha="$(printf '%s\n' "$candidate_validation" | \
    sed -nE 's/^.*"profile_sha256":"([0-9a-f]{64})".*$/\1/p')"
  reference_profile_sha="$(printf '%s\n' "$reference_validation" | \
    sed -nE 's/^.*"profile_sha256":"([0-9a-f]{64})".*$/\1/p')"
  [[ -n "$candidate_profile_sha" && -n "$reference_profile_sha" && \
    "$candidate_profile_sha" == "$(m2_resource_json_value \
      "$evidence_dir/eligibility.json" candidate_profile_sha256)" && \
    "$reference_profile_sha" == "$(m2_resource_json_value \
      "$evidence_dir/eligibility.json" reference_profile_sha256)" ]] || return 1

  eligibility_reason="$(m2_resource_json_value \
    "$evidence_dir/eligibility.json" reason)" || return 1
  if [[ "$eligibility_reason" == proved_saturation_replacement ]]; then
    [[ -f "$evidence_dir/prior-saturation.txt" && \
      ! -L "$evidence_dir/prior-saturation.txt" && \
      -f "$evidence_dir/replacement-capacity.txt" && \
      ! -L "$evidence_dir/replacement-capacity.txt" ]] || return 1
    for bounded_evidence in prior-saturation.txt replacement-capacity.txt; do
      bounded_size="$(wc -c <"$evidence_dir/$bounded_evidence" | \
        tr -d '[:space:]')"
      [[ "$bounded_size" =~ ^[0-9]+$ && 10#$bounded_size -gt 0 && \
        10#$bounded_size -le 65536 ]] || return 1
    done
  else
    [[ ! -e "$evidence_dir/prior-saturation.txt" && \
      ! -e "$evidence_dir/replacement-capacity.txt" ]] || return 1
  fi

  [[ "$(m2_resource_json_value "$candidate_file" source_commit)" == \
    "$expected_source" && \
    "$(m2_resource_json_value "$candidate_file" client_binary_sha256)" == \
      "$expected_binary_sha" && \
    "$(m2_resource_json_value "$candidate_file" workload_profile_sha256)" == \
      "$(sha256_file "$expected_direct_manifest")" && \
    "$(m2_resource_json_value "$candidate_file" observer_sha256)" == \
      "$expected_observer_sha" && \
    "$(m2_resource_json_value "$candidate_file" public_ipv4)" == \
      "$expected_exit" && \
    "$(m2_resource_json_value "$candidate_file" tuic_port)" == \
      "$expected_tuic_port" && \
    "$(m2_resource_json_value "$candidate_file" target_ipv4)" == \
      "$expected_target" && \
    "$(m2_resource_json_value "$candidate_file" target_iperf_port)" == \
      "$expected_iperf_port" && \
    "$(m2_resource_json_value "$candidate_file" mac_interface)" == \
      "$expected_interface" ]] || return 1
  cmp -s "$evidence_dir/direct-manifest.txt" "$expected_direct_manifest" || \
    return 1

  result_file="$evidence_dir/result.txt"
  result_text="$(cat "$result_file")"
  [[ "$(m2_resource_result_value "$result_file" schema)" == \
      knife15-m2-resource-preflight-v1 && \
    "$(m2_resource_result_value "$result_file" status)" == pass && \
    "$(m2_resource_result_value "$result_file" candidate_id)" == \
      "$(m2_resource_json_value "$candidate_file" candidate_id)" && \
    "$(m2_resource_result_value "$result_file" candidate_ipv4)" == \
      "$expected_exit" && \
    "$(m2_resource_result_value "$result_file" candidate_tuic_port)" == \
      "$expected_tuic_port" && \
    "$(m2_resource_result_value "$result_file" target)" == \
      "$expected_target" && \
    "$(m2_resource_result_value "$result_file" target_iperf_port)" == \
      "$expected_iperf_port" && \
    "$(m2_resource_result_value "$result_file" physical_interface)" == \
      "$expected_interface" && \
    "$(m2_resource_result_value "$result_file" source_commit)" == \
      "$expected_source" && \
    "$(m2_resource_result_value "$result_file" binary_sha256)" == \
      "$expected_binary_sha" && \
    "$(m2_resource_result_value "$result_file" direct_manifest_sha256)" == \
      "$(sha256_file "$expected_direct_manifest")" && \
    "$(m2_resource_result_value "$result_file" observer_sha256)" == \
      "$expected_observer_sha" && \
    "$(m2_resource_result_value "$result_file" profile_helper_sha256)" == \
      "$(sha256_file "$M2_RESOURCE_PROFILE_HELPER")" && \
    "$(m2_resource_result_value "$result_file" preflight_runner_sha256)" == \
      "$(sha256_file "$M2_RESOURCE_PREFLIGHT_RUNNER")" && \
    "$(m2_resource_result_value "$result_file" evidence_binding_sha256)" == \
      "$(sha256_file "$evidence_dir/evidence-binding.json")" ]] || return 1
  [[ "$result_text" != *$'\n\n'* ]] || return 1

  for evidence_name in \
    eligibility.json evidence-binding.json provider-identity.txt \
    route-identity.txt remote.txt \
    exit.route.txt target.route.txt exit.traceroute.txt target.traceroute.txt; do
    case "$evidence_name" in
      eligibility.json) result_key=eligibility_sha256 ;;
      evidence-binding.json) result_key=evidence_binding_sha256 ;;
      provider-identity.txt) result_key=provider_identity_sha256 ;;
      route-identity.txt) result_key=route_identity_sha256 ;;
      remote.txt) result_key=remote_sha256 ;;
      exit.route.txt) result_key=exit_route_sha256 ;;
      target.route.txt) result_key=target_route_sha256 ;;
      exit.traceroute.txt) result_key=exit_traceroute_sha256 ;;
      target.traceroute.txt) result_key=target_traceroute_sha256 ;;
    esac
    actual_sha="$(sha256_file "$evidence_dir/$evidence_name")"
    expected_sha="$(m2_resource_result_value "$result_file" "$result_key")" || \
      return 1
    [[ "$actual_sha" == "$expected_sha" ]] || return 1
  done
  [[ "$(sed -n '1p' "$evidence_dir/secret-scan.txt")" == \
    'PASS: no credential-like assignment found' ]] || return 1

  remote_file="$evidence_dir/remote.txt"
  remote_cpu="$(m2_resource_result_value "$remote_file" cpu_count)" || return 1
  remote_memory="$(m2_resource_result_value "$remote_file" memavailable)" || \
    return 1
  [[ "$(m2_resource_result_value "$remote_file" schema)" == \
      knife15-m2-resource-remote-v1 && \
    "$(m2_resource_result_value "$remote_file" service_active)" == active && \
    "$(m2_resource_result_value "$remote_file" tuic_udp_listener)" == 1 && \
    "$(m2_resource_result_value "$remote_file" server_binary_sha256)" == \
      "$(m2_resource_json_value "$candidate_file" server_binary_sha256)" && \
    "$(m2_resource_result_value "$remote_file" server_config_sha256)" == \
      "$(m2_resource_json_value "$candidate_file" server_config_sha256)" && \
    "$remote_cpu" =~ ^[1-9][0-9]*$ && \
    "$remote_memory" =~ ^[1-9][0-9]*kB$ && \
    -n "$(m2_resource_result_value "$remote_file" loadavg)" && \
    ! -s "$evidence_dir/remote.stderr" && \
    "$(grep -Fxc udp_summary_begin "$remote_file")" == 1 && \
    "$(grep -Fxc udp_summary_end "$remote_file")" == 1 && \
    "$(grep -Fxc interface_counters_begin "$remote_file")" == 1 && \
    "$(grep -Fxc interface_counters_end "$remote_file")" == 1 ]] || \
    return 1
}

m2_copy_resource_preflight_evidence() {
  local source_dir="${1:-}" destination_dir="${2:-}"
  local archive expected_archive_sha actual_archive_sha evidence_file name
  validate_resource_preflight_dir_path "$source_dir" || return 1
  [[ -d "$source_dir" && ! -L "$source_dir" && \
    ! -e "$destination_dir" ]] || return 1
  archive="${source_dir}.tar.gz"
  [[ -f "$archive" && ! -L "$archive" && \
    -f "${archive}.sha256" && ! -L "${archive}.sha256" && \
    "$(awk 'END {print NR + 0}' "${archive}.sha256")" == 1 ]] || return 1
  expected_archive_sha="$(awk '{print $1}' "${archive}.sha256")"
  [[ "$expected_archive_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
  actual_archive_sha="$(sha256_file "$archive")"
  [[ "$actual_archive_sha" == "$expected_archive_sha" ]] || return 1
  m2_resource_archive_matches_directory "$source_dir" "$archive" || return 1

  mkdir "$destination_dir" || return 1
  for evidence_file in "$source_dir"/*; do
    name="$(basename "$evidence_file")"
    cp "$evidence_file" "$destination_dir/$name" || return 1
  done
  cp "$archive" "${destination_dir}.tar.gz" || return 1
  printf '%s  %s\n' "$actual_archive_sha" \
    "$(basename "${destination_dir}.tar.gz")" \
    >"${destination_dir}.tar.gz.sha256" || return 1
}

m2_resource_binding_is_valid() {
  local run_dir="${1:-}" evidence_name="${2:-}"
  local resource_dir status_file profile_file direct_manifest candidate_file
  local candidate_id candidate_profile_sha result_sha archive archive_sha
  local source binary_sha observer_sha exit_host server_port target iperf_port
  local physical_interface
  [[ "$evidence_name" == m2 || "$evidence_name" == m2-qualification ]] || \
    return 1
  resource_dir="$run_dir/${evidence_name}-resource-preflight"
  status_file="$run_dir/${evidence_name}-resource-admission.status"
  profile_file="$run_dir/${evidence_name}-workload.txt"
  direct_manifest="$run_dir/${evidence_name}-direct/manifest.txt"
  candidate_file="$resource_dir/candidate-profile.json"
  [[ -d "$run_dir" && ! -L "$run_dir" && \
    -f "$status_file" && ! -L "$status_file" && \
    "$(sed -n '1p' "$status_file")" == pass && \
    "$(awk 'END {print NR + 0}' "$status_file")" == 1 && \
    -f "$profile_file" && ! -L "$profile_file" && \
    -f "$direct_manifest" && ! -L "$direct_manifest" ]] || return 1

  source="$(git -C "$REPO" rev-parse HEAD)" || return 1
  binary_sha="$(sha256_file "$BIN")"
  observer_sha="$(sha256_file "$M2_EXIT_OBSERVER_SCRIPT")"
  exit_host="$(read_state exit_host)" || return 1
  server_port="$(read_state server_port)" || return 1
  target="$(read_state target)" || return 1
  iperf_port="$(read_state iperf_port)" || return 1
  physical_interface="$(m2_resource_json_value \
    "$candidate_file" mac_interface)" || return 1
  m2_resource_evidence_is_valid "$resource_dir" "$source" "$binary_sha" \
    "$direct_manifest" "$observer_sha" "$exit_host" "$server_port" \
    "$target" "$iperf_port" "$physical_interface" || return 1

  candidate_id="$(m2_resource_json_value "$candidate_file" candidate_id)" || \
    return 1
  candidate_profile_sha="$(m2_resource_json_value \
    "$resource_dir/eligibility.json" candidate_profile_sha256)" || return 1
  result_sha="$(sha256_file "$resource_dir/result.txt")"
  [[ "$(m2_resource_result_value "$profile_file" resource_candidate_id)" == \
      "$candidate_id" && \
    "$(m2_resource_result_value "$profile_file" resource_profile_sha256)" == \
      "$candidate_profile_sha" && \
    "$(m2_resource_result_value \
      "$profile_file" resource_preflight_result_sha256)" == "$result_sha" ]] || \
    return 1
  m2_resource_state_binding_is_valid "$evidence_name" "$candidate_id" \
    "$candidate_profile_sha" "$result_sha" || return 1

  archive="${resource_dir}.tar.gz"
  archive_sha="$(awk '{print $1}' "${archive}.sha256" 2>/dev/null)" || return 1
  [[ -f "$archive" && ! -L "$archive" && \
    -f "${archive}.sha256" && ! -L "${archive}.sha256" && \
    "$archive_sha" =~ ^[0-9a-f]{64}$ && \
    "$(sha256_file "$archive")" == "$archive_sha" ]] || return 1
  m2_resource_archive_matches_directory "$resource_dir" "$archive"
}

m2_execution_requires_paired_observer() {
  [[ "${1:-}" == "formal" || "${1:-}" == "qualification" ]]
}

m2_exit_observer_value_from_text() {
  local key="$1"
  awk -F= -v key="$key" '
    $1 == key {
      count++
      value = substr($0, length(key) + 2)
    }
    END {
      if (count != 1 || value == "") exit 1
      print value
    }
  '
}

m2_exit_observer_ssh_host_matches_exit() {
  local ssh_host="$1"
  local exit_host="$2"
  [[ "$ssh_host" =~ ^[a-z_][a-zA-Z0-9_-]*@([0-9]+[.]){3}[0-9]+$ ]] && \
    validate_ipv4 "$exit_host" && [[ "${ssh_host#*@}" == "$exit_host" ]]
}

m2_exit_observer_status_is_valid() {
  local status_text="$1"
  local target="$2"
  local iperf_port="$3"
  local tuic_port="$4"
  local schema status healthy observed_target observed_iperf_port
  local observed_tuic_port timeout_secs elapsed_secs
  schema="$(m2_exit_observer_value_from_text schema <<<"$status_text")" || return 1
  status="$(m2_exit_observer_value_from_text status <<<"$status_text")" || return 1
  healthy="$(m2_exit_observer_value_from_text observer_healthy \
    <<<"$status_text")" || return 1
  observed_target="$(m2_exit_observer_value_from_text target \
    <<<"$status_text")" || return 1
  observed_iperf_port="$(m2_exit_observer_value_from_text iperf_port \
    <<<"$status_text")" || return 1
  observed_tuic_port="$(m2_exit_observer_value_from_text tuic_port \
    <<<"$status_text")" || return 1
  timeout_secs="$(m2_exit_observer_value_from_text timeout_secs \
    <<<"$status_text")" || return 1
  elapsed_secs="$(m2_exit_observer_value_from_text elapsed_secs \
    <<<"$status_text")" || return 1
  [[ "$schema" == "knife15-exit-target-observer-v2" && \
    "$status" == "active" && "$healthy" == "1" && \
    "$observed_target" == "$target" && \
    "$observed_iperf_port" == "$iperf_port" && \
    "$observed_tuic_port" == "$tuic_port" && \
    "$timeout_secs" == "93600" && "$elapsed_secs" =~ ^[0-9]+$ ]] && \
    ((10#$elapsed_secs <= 900))
}

m2_exit_observer_call() {
  local action="$1"
  local target="$2"
  local iperf_port="$3"
  local tuic_port="$4"
  [[ -f "$M2_EXIT_OBSERVER_SCRIPT" && ! -L "$M2_EXIT_OBSERVER_SCRIPT" ]] || {
    printf '%s\n' \
      'ERROR: formal M2 Exit observer script is missing or symlinked' >&2
    return 1
  }
  validate_ipv4 "$target" || {
    printf '%s\n' 'ERROR: invalid formal M2 Exit observer Target IPv4' >&2
    return 1
  }
  validate_positive_integer "$iperf_port" && ((10#$iperf_port <= 65535)) || {
    if [[ -z "$iperf_port" ]]; then
      printf '%s\n' \
        'ERROR: invalid formal M2 Exit observer iperf port: <missing>' >&2
    else
      printf '%s\n' \
        'ERROR: invalid formal M2 Exit observer iperf port: <invalid>' >&2
    fi
    return 1
  }
  validate_positive_integer "$tuic_port" && ((10#$tuic_port <= 65535)) || {
    if [[ -z "$tuic_port" ]]; then
      printf '%s\n' \
        'ERROR: invalid formal M2 Exit observer TUIC port: <missing>' >&2
    else
      printf '%s\n' \
        'ERROR: invalid formal M2 Exit observer TUIC port: <invalid>' >&2
    fi
    return 1
  }
  TARGET="$target" IPERF_PORT="$iperf_port" TUIC_PORT="$tuic_port" \
    OBSERVER_TIMEOUT_SECS=93600 \
    /bin/bash "$M2_EXIT_OBSERVER_SCRIPT" "$action"
}

m2_exit_observer_finalization_is_valid() {
  local evidence_file="$1"
  [[ -f "$evidence_file" && ! -L "$evidence_file" ]] || return 1
  [[ "$(grep -Ec '^PASS: Exit observer frozen$' "$evidence_file" \
    2>/dev/null || true)" == "1" ]] && \
    [[ "$(grep -Ec '^PASS: Exit observer bundle finalized$' "$evidence_file" \
      2>/dev/null || true)" == "1" ]] && \
    [[ "$(grep -Ec '^bundle=/tmp/mini_vpn_knife15_exit_target_observer_[0-9]{8}_[0-9]{6}[.]tar[.]gz$' \
      "$evidence_file" 2>/dev/null || true)" == "1" ]] && \
    [[ "$(grep -Ec '^sha256=[0-9a-f]{64}$' "$evidence_file" \
      2>/dev/null || true)" == "1" ]]
}

m2_exit_observer_require_active() {
  local run_dir="$1"
  local target="$2"
  local iperf_port="$3"
  local tuic_port="$4"
  local evidence_file="$run_dir/m2-exit-observer-status.txt"
  [[ -d "$run_dir" && ! -L "$run_dir" ]] || return 1
  m2_exit_observer_call status "$target" "$iperf_port" "$tuic_port" \
    >"$evidence_file" 2>&1 || return 1
  m2_exit_observer_status_is_valid \
    "$(sed -n '1,80p' "$evidence_file")" \
    "$target" "$iperf_port" "$tuic_port"
}

m2_exit_observer_freeze_and_bundle() {
  local run_dir="$1"
  local reason="$2"
  local target="$3"
  local iperf_port="$4"
  local tuic_port="$5"
  local evidence_file="$run_dir/m2-exit-observer-finalization.txt"
  [[ -d "$run_dir" && ! -L "$run_dir" ]] || return 1
  [[ "$reason" =~ ^[a-z0-9-]+$ ]] || return 1
  printf '%s\n' \
    'schema=knife15-m2-exit-observer-finalization-v1' \
    "started_at=$(timestamp)" \
    "reason=$reason" \
    "observer_script_sha256=$(sha256_file "$M2_EXIT_OBSERVER_SCRIPT")" \
    >"$evidence_file" || return 1
  if ! m2_exit_observer_call freeze "$target" "$iperf_port" "$tuic_port" \
    >>"$evidence_file" 2>&1; then
    printf '%s\n' 'finalization_status=freeze_failed' >>"$evidence_file"
    return 1
  fi
  if ! m2_exit_observer_call bundle "$target" "$iperf_port" "$tuic_port" \
    >>"$evidence_file" 2>&1; then
    printf '%s\n' 'finalization_status=bundle_failed' >>"$evidence_file"
    return 1
  fi
  printf '%s\n' 'finalization_status=complete' >>"$evidence_file"
  m2_exit_observer_finalization_is_valid "$evidence_file"
}

m2_exit_observer_arm() {
  M2_EXIT_OBSERVER_RUN_DIR="$1"
  M2_EXIT_OBSERVER_TARGET="$2"
  M2_EXIT_OBSERVER_IPERF_PORT="$3"
  M2_EXIT_OBSERVER_TUIC_PORT="$4"
  M2_EXIT_OBSERVER_ARMED=1
}

m2_exit_observer_finalize_armed() {
  local reason="$1"
  local result=0
  [[ "$M2_EXIT_OBSERVER_ARMED" == "1" ]] || return 0
  if ! m2_exit_observer_freeze_and_bundle \
    "$M2_EXIT_OBSERVER_RUN_DIR" "$reason" \
    "$M2_EXIT_OBSERVER_TARGET" "$M2_EXIT_OBSERVER_IPERF_PORT" \
    "$M2_EXIT_OBSERVER_TUIC_PORT"; then
    result=1
  fi
  M2_EXIT_OBSERVER_ARMED=0
  append_event_to "$M2_EXIT_OBSERVER_RUN_DIR" \
    "m2 Exit observer finalization reason=$reason status=$(
      ((result == 0)) && printf complete || printf failed
    )"
  return "$result"
}

m2_exit_observer_exit_trap() {
  local exit_status="$1"
  trap - EXIT
  m2_exit_observer_finalize_armed unexpected-exit || true
  exit "$exit_status"
}

m2_worktree_is_clean() {
  local repo="${1:-$REPO}"
  local status
  status="$(git -C "$repo" status --porcelain=v1 --untracked-files=all 2>/dev/null)" || \
    return 1
  [[ -z "$status" ]]
}

endpoint_rebind_lifecycle_is_clean() {
  local log_file="$1"
  local successes recoveries failures max_first_rx
  [[ -f "$log_file" && ! -L "$log_file" ]] || return 1
  successes="$(grep -Ec 'tuic-endpoint-rebind generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  recoveries="$(grep -Ec \
    'tuic-endpoint-rebind-recovered generation=[0-9]+ first_rx_ms=[0-9]+ socket_generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  failures="$(grep -Ec 'tuic-endpoint-rebind-failed generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  max_first_rx="$(awk '
    match($0, /tuic-endpoint-rebind-recovered generation=[0-9]+ first_rx_ms=[0-9]+/) {
      value = substr($0, RSTART, RLENGTH)
      sub(/^.*first_rx_ms=/, "", value)
      if (value + 0 > maximum) maximum = value + 0
    }
    END { print maximum + 0 }
  ' "$log_file" 2>/dev/null)"
  [[ "$successes" =~ ^[0-9]+$ && "$recoveries" =~ ^[0-9]+$ && \
    "$failures" == "0" && "$max_first_rx" =~ ^[0-9]+$ ]] || return 1
  ((10#$successes == 10#$recoveries && 10#$max_first_rx <= 7000))
}

recovery_evidence_is_safe() {
  local log_file="$1"
  local total ordered_valid writer_start_valid writer_end_valid valid
  [[ -f "$log_file" && ! -L "$log_file" ]] || return 1
  ! grep -Eq \
    'tuic-endpoint-rebind(-failed)? .*trigger=tcp_ordered_read_gap' \
    "$log_file" || return 1
  total="$(grep -Ec 'tuic-recovery-evidence kind=' \
    "$log_file" 2>/dev/null || true)"
  ordered_valid="$(grep -Ec \
    'tuic-recovery-evidence kind=tcp_ordered_gap_observed action=none conn=[0-9]+ reader=[0-9]+ stream=[0-9]+ episode=[0-9]+ read_offset=[0-9]+ next_received_offset=[0-9]+ initial_highest_received_offset=[0-9]+ current_highest_received_offset=[0-9]+ initial_buffered=[0-9]+B current_buffered=[0-9]+B gap=[0-9]+B observations=[0-9]+ observed_ms=[0-9]+ tail_advanced=(true|false)$' \
    "$log_file" 2>/dev/null || true)"
  writer_start_valid="$(grep -Ec \
    'tuic-recovery-evidence kind=tcp_write_pressure_start action=none conn=[0-9]+ writer=[0-9]+ stream=[0-9]+ episode=[0-9]+ acknowledged=[0-9]+B pending_ms=[0-9]+ ack_stalled_ms=[0-9]+$' \
    "$log_file" 2>/dev/null || true)"
  writer_end_valid="$(grep -Ec \
    'tuic-recovery-evidence kind=tcp_write_pressure_end action=none conn=[0-9]+ writer=[0-9]+ stream=[0-9]+ episode=[0-9]+ observations=[0-9]+ observed_ms=[0-9]+ initial_acknowledged=[0-9]+B final_acknowledged=[0-9]+B acknowledged_delta=[0-9]+B ack_progress_observations=[0-9]+ max_pending_ms=[0-9]+ max_ack_stalled_ms=[0-9]+$' \
    "$log_file" 2>/dev/null || true)"
  for valid in "$total" "$ordered_valid" "$writer_start_valid" \
    "$writer_end_valid"; do
    [[ "$valid" =~ ^[0-9]+$ ]] || return 1
  done
  valid=$((10#$ordered_valid + 10#$writer_start_valid + 10#$writer_end_valid))
  [[ "$total" =~ ^[0-9]+$ && "$valid" =~ ^[0-9]+$ && \
    "$total" == "$valid" ]]
}

m2_recovery_contract_is_safe() {
  local log_file="$1"
  endpoint_rebind_lifecycle_is_clean "$log_file" && \
    recovery_evidence_is_safe "$log_file"
}

m2_qualification_result_slo() {
  local run_dir="$1"
  local expected_real_results="$2"
  local status cycles dns phase_results phase_failures health_failures real_failures
  local tcp_results udp_results tcp_gap udp_loss invalid_results sender_zero receiver_zero
  local dns_files invalid_dns real_results invalid_real
  [[ "$expected_real_results" =~ ^[0-9]+$ ]] || return 1
  status="$(sed -n '1p' "$run_dir/m2-qualification.status" 2>/dev/null || true)"
  cycles="$(grep -Fc $'\tm2-qualification cycle complete cycle=' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  dns="$(grep -Fc $'\tm2-qualification DNS complete cycle=' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_results="$(grep -Fc $'\tm2-qualification phase complete ' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_failures="$(grep -Fc $'\tm2-qualification phase failed ' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  health_failures="$(grep -Fc $'\tm2-qualification health failed:' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  real_failures="$(grep -Fc $'\tm2-qualification real client failed ' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  read -r tcp_results udp_results tcp_gap udp_loss invalid_results \
    sender_zero receiver_zero \
    <<<"$(m0_result_envelope "$run_dir/m2-qualification")"
  read -r dns_files invalid_dns \
    <<<"$(m0_dns_result_envelope "$run_dir/m2-qualification")"
  read -r real_results invalid_real \
    <<<"$(m2_real_client_envelope "$run_dir/m2-qualification-real-client")"
  [[ "$status" == "PASS_NON_ACCEPTANCE" && "$cycles" == "2" && \
    "$dns" == "2" && "$phase_results" == "8" && \
    "$phase_failures" == "0" && "$health_failures" == "0" && \
    "$real_failures" == "0" && "$tcp_results" == "6" && \
    "$udp_results" == "2" && "$invalid_results" == "0" && \
    "$sender_zero" =~ ^[0-9]+$ && "$receiver_zero" == "0" && \
    "$dns_files" == "2" && "$invalid_dns" == "0" && \
    "$real_results" == "$expected_real_results" && "$invalid_real" == "0" && \
    "$tcp_gap" =~ ^[0-9]+$ ]] || return 1
  ((10#$tcp_gap <= 16777216)) && decimal_le "$udp_loss" 3.0
}

m2_qualification_terminal_safety() {
  local run_dir="$1"
  local log_file="$run_dir/mini_vpn.log"
  local endpoint_samples endpoint_max endpoint_available endpoint_live
  local endpoint_outstanding endpoint_max_live endpoint_max_outstanding
  local remote_failures remote_log_matches
  local value
  read -r endpoint_samples endpoint_max endpoint_available endpoint_live \
    endpoint_outstanding endpoint_max_live endpoint_max_outstanding \
    <<<"$(endpoint_resource_envelope "$log_file")"
  for value in "$endpoint_samples" "$endpoint_max" "$endpoint_live" \
    "$endpoint_outstanding"; do
    [[ "$value" =~ ^[0-9]+$ ]] || return 1
  done
  ((10#$endpoint_samples > 0 && 10#$endpoint_max <= 61440 && \
    10#$endpoint_live == 0 && 10#$endpoint_outstanding == 0)) || return 1
  remote_log_matches="$(grep -Ec \
    '写入上游流失败|reason=remote_write_failed|reason=stalled_write_timeout' \
    "$log_file" 2>/dev/null || true)"
  remote_failures="$(grep -Ec \
    'tcp-d16-relay-close .*terminal_reason=(remote_write_failed|stalled_write_timeout)( |$)' \
    "$log_file" 2>/dev/null || true)"
  if [[ "$remote_failures" != "0" || "$remote_log_matches" != "0" ]]; then
    [[ "$remote_failures" =~ ^[1-9][0-9]*$ ]] && \
      remote_write_close_ownership_is_clean "$log_file" || return 1
  fi
  m2_recovery_contract_is_safe "$log_file" && \
    conservation_check_file "$log_file" && \
    d16_terminal_ownership_is_clean "$log_file" && \
    ! grep -Eq \
      'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|send_slice_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|reason=stalled_write_timeout|reason=idle_timeout' \
      "$log_file"
}

wait_for_m2_qualification_terminal_safety() {
  local run_dir="$1"
  local timeout_secs="$2"
  local deadline
  validate_positive_integer "$timeout_secs" || return 1
  deadline=$((SECONDS + 10#$timeout_secs))
  while ((SECONDS < deadline)); do
    m2_qualification_terminal_safety "$run_dir" && return 0
    /bin/sleep 1
  done
  m2_qualification_terminal_safety "$run_dir"
}

free_kb_for_path() {
  df -Pk "$1" 2>/dev/null | awk 'NR == 2 && $4 ~ /^[0-9]+$/ {print $4; exit}'
}

m2_workload_slo() {
  local run_dir="$1"
  local log_file="$run_dir/mini_vpn.log"
  local status active_windows cycles dns idle resume final_drain
  local phase_failures health_failures real_failures phase_results
  local tcp_results udp_results tcp_gap udp_loss invalid_results sender_zero receiver_zero
  local dns_files invalid_dns real_results invalid_real
  local process_samples rss_first rss_last rss_max rss_delta
  local fd_first fd_last fd_max fd_delta thread_first thread_last thread_max thread_delta
  local interface_samples rest endpoint_samples endpoint_max endpoint_available
  local endpoint_live endpoint_outstanding endpoint_max_live endpoint_max_outstanding
  local network_samples exit_samples exit_missing exit_rtt gateway_samples gateway_missing
  local physical_samples exit_loss exit_rtt gateway_loss physical_errors
  local remote_failures remote_log_matches rebind_successes rebind_recoveries
  local rebind_failures rebind_max_first_rx log_compactions interface_errors
  local value
  status="$(sed -n '1p' "$run_dir/m2.status" 2>/dev/null || true)"
  active_windows="$(grep -Ec $'\tm2 active .* complete planned_secs=' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  cycles="$(grep -Fc $'\tm2 cycle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  dns="$(grep -Fc $'\tm2 DNS complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  idle="$(grep -Fc $'\tm2 idle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  resume="$(grep -Fc $'\tm2 resume complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  final_drain="$(grep -Fc $'\tm2 final drain complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_failures="$(grep -Fc $'\tm2 phase failed ' "$run_dir/events.tsv" 2>/dev/null || true)"
  health_failures="$(grep -Fc $'\tm2 health failed:' "$run_dir/events.tsv" 2>/dev/null || true)"
  real_failures="$(grep -Fc $'\tm2 real client failed ' "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_results="$(grep -Fc $'\tm2 phase complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  read -r tcp_results udp_results tcp_gap udp_loss invalid_results \
    sender_zero receiver_zero <<<"$(m0_result_envelope "$run_dir/m2")"
  read -r dns_files invalid_dns <<<"$(m0_dns_result_envelope "$run_dir/m2")"
  read -r real_results invalid_real \
    <<<"$(m2_real_client_envelope "$run_dir/m2-real-client")"
  read -r process_samples rss_first rss_last rss_max rss_delta fd_first \
    fd_last fd_max fd_delta thread_first thread_last thread_max thread_delta \
    <<<"$(process_resource_envelope "$run_dir/process.csv")"
  read -r interface_samples rest \
    <<<"$(interface_resource_envelope "$run_dir/interface.csv")"
  read -r endpoint_samples endpoint_max endpoint_available endpoint_live \
    endpoint_outstanding endpoint_max_live endpoint_max_outstanding \
    <<<"$(endpoint_resource_envelope "$log_file")"
  read -r network_samples exit_samples exit_missing exit_rtt gateway_samples \
    gateway_missing physical_samples exit_loss exit_rtt gateway_loss physical_errors rest \
    <<<"$(network_control_envelope "$run_dir/network.csv")"
  interface_errors="$(awk -F, '
    NR > 1 && (($5 ~ /^[0-9]+$/ && $5 + 0 > 0) ||
      ($8 ~ /^[0-9]+$/ && $8 + 0 > 0)) { count++ }
    END { print count + 0 }
  ' "$run_dir/interface.csv" 2>/dev/null)"
  log_compactions="$(grep -Fc $'\twatchdog compacted mini_vpn.log' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  remote_log_matches="$(grep -Ec \
    '写入上游流失败|reason=remote_write_failed|reason=stalled_write_timeout' \
    "$log_file" 2>/dev/null || true)"
  remote_failures="$(grep -Ec \
    'tcp-d16-relay-close .*terminal_reason=(remote_write_failed|stalled_write_timeout)( |$)' \
    "$log_file" 2>/dev/null || true)"
  rebind_successes="$(grep -Ec 'tuic-endpoint-rebind generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  rebind_recoveries="$(grep -Ec \
    'tuic-endpoint-rebind-recovered generation=[0-9]+ first_rx_ms=[0-9]+ socket_generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  rebind_failures="$(grep -Ec 'tuic-endpoint-rebind-failed generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  rebind_max_first_rx="$(awk '
    match($0, /tuic-endpoint-rebind-recovered generation=[0-9]+ first_rx_ms=[0-9]+/) {
      value = substr($0, RSTART, RLENGTH)
      sub(/^.*first_rx_ms=/, "", value)
      if (value + 0 > maximum) maximum = value + 0
    }
    END { print maximum + 0 }
  ' "$log_file" 2>/dev/null)"

  [[ "$status" == "complete" && \
    "$active_windows" == "6" && "$cycles" == "$M2_EXPECTED_CYCLES" && \
    "$dns" == "$M2_EXPECTED_CYCLES" && "$idle" == "5" && \
    "$resume" == "5" && "$final_drain" == "1" && \
    "$phase_failures" == "0" && "$health_failures" == "0" && \
    "$real_failures" == "0" && "$phase_results" == "$M2_EXPECTED_PHASE_RESULTS" && \
    "$tcp_results" == "$M2_EXPECTED_TCP_RESULTS" && \
    "$udp_results" == "$M2_EXPECTED_UDP_RESULTS" && \
    "$invalid_results" == "0" && "$sender_zero" =~ ^[0-9]+$ && \
    "$receiver_zero" == "0" && "$dns_files" == "$M2_EXPECTED_CYCLES" && \
    "$invalid_dns" == "0" && "$real_results" == "$M2_EXPECTED_CYCLES" && \
    "$invalid_real" == "0" ]] || return 1
  for value in "$tcp_gap" "$process_samples" "$interface_samples" \
    "$endpoint_samples" "$endpoint_max" "$endpoint_live" "$endpoint_outstanding" \
    "$network_samples" "$exit_samples" "$physical_samples" "$physical_errors" \
    "$interface_errors" "$log_compactions" "$rebind_successes" \
    "$rebind_recoveries" "$rebind_failures" "$rebind_max_first_rx"; do
    [[ "$value" =~ ^[0-9]+$ ]] || return 1
  done
  ((10#$tcp_gap <= 16777216 && 10#$process_samples >= 2700 && \
    10#$interface_samples >= 2700 && 10#$endpoint_samples >= 2700 && \
    10#$network_samples >= 2700 && 10#$exit_samples == 10#$network_samples && \
    10#$physical_samples == 10#$network_samples && 10#$endpoint_max <= 61440 && \
    10#$endpoint_live == 0 && 10#$endpoint_outstanding == 0 && \
    10#$physical_errors == 0 && 10#$interface_errors == 0 && \
    10#$log_compactions == 0 && 10#$rebind_failures == 0 && \
    10#$rebind_successes == 10#$rebind_recoveries && \
    10#$rebind_max_first_rx <= 7000)) || return 1
  decimal_le "$udp_loss" 3.0 || return 1
  if [[ "$remote_failures" != "0" || "$remote_log_matches" != "0" ]]; then
    [[ "$remote_failures" =~ ^[1-9][0-9]*$ ]] && \
      remote_write_close_ownership_is_clean "$log_file" || return 1
  fi
  conservation_check_file "$log_file" && \
    m0_final_ownership_is_clean "$log_file" && \
    d16_terminal_ownership_is_clean "$log_file" && \
    m2_recovery_contract_is_safe "$log_file" && \
    m2_checkpoint_slo "$run_dir/m2-checkpoints.csv" && \
    m2_real_client_result_is_valid "$run_dir/m2-real-client/preflight.txt" && \
    m2_full_tunnel_is_active && network_control_is_sufficient "$run_dir" && \
    ! grep -Eq \
      'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|send_slice_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|reason=stalled_write_timeout|reason=idle_timeout' \
      "$log_file"
}

run_m2_action() {
  local execution_mode="${1:-formal}"
  local run_dir utun target exit_host server_port iperf_port dns_target dns_name
  local profile_file free_kb command_name ipv6_evidence_file duration
  local data_plane_values endpoint_values data_plane_samples_before
  local endpoint_samples_before quiescence_timeout_secs quiescence_snapshot
  local quiescence_status
  local action_description stage label evidence_dir status_file
  local baseline_evidence_dir direct_evidence_dir
  local resource_evidence_dir resource_status_file resource_candidate_id
  local resource_profile_sha current_source binary_sha observer_sha
  local physical_interface
  local observer_finalization_status=0
  case "$execution_mode" in
    formal)
      action_description="formal M2"
      stage=m2
      label=M2
      evidence_dir=m2
      status_file=m2.status
      M2_REAL_CLIENT_EVIDENCE_DIR=m2-real-client
      ;;
    qualification)
      action_description="M2 qualification"
      stage=m2-qualification
      label=M2-QUALIFICATION
      evidence_dir=m2-qualification
      status_file=m2-qualification.status
      M2_REAL_CLIENT_EVIDENCE_DIR=m2-qualification-real-client
      ;;
    *)
      die "unknown M2 execution mode: $execution_mode"
      ;;
  esac
  require_root
  validate_m2_formal_config || \
    die "$action_description requires frozen M2 rates/durations and 30s sampling; unset M2_* overrides"
  [[ "$(m2_formal_count_model)" == \
    "$M2_EXPECTED_CYCLES $M2_EXPECTED_TCP_RESULTS $M2_EXPECTED_UDP_RESULTS $M2_EXPECTED_PHASE_RESULTS" ]] || \
    die "$action_description requires the immutable formal M2 count model"
  m2_source_is_accepted || \
    die "$action_description requires bounded resource evidence 60a4e94 or a descendant"
  m2_worktree_is_clean || \
    die "$action_description requires a clean worktree, including no untracked files, for exact-source evidence"
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  baseline_evidence_dir="$run_dir/${evidence_dir}-baseline"
  direct_evidence_dir="$run_dir/${evidence_dir}-direct"
  active_pid || die "mini_vpn is not running"
  workload_matches_run && die "a soak workload is already running"
  [[ ! -e "$run_dir/m0.status" && ! -e "$run_dir/m0" && \
    ! -e "$run_dir/m1.status" && ! -e "$run_dir/m1" && \
    ! -e "$run_dir/m2.status" && ! -e "$run_dir/m2" && \
    ! -e "$run_dir/m2-qualification.status" && \
    ! -e "$run_dir/m2-qualification" ]] || \
    die "this TUN run already has soak evidence; stop and start a fresh run"
  [[ -z "$M0_BASELINE_DIR" && -z "$M0_DIRECT_DIR" && \
    -z "$M1_BASELINE_DIR" && -z "$M1_DIRECT_DIR" ]] || \
    die "$action_description requires every M0/M1 baseline and direct variable to be unset"
  validate_baseline_dir_path "$M2_BASELINE_DIR" || \
    die "M2_BASELINE_DIR must be the simple /tmp baseline directory printed by baseline"
  [[ -d "$M2_BASELINE_DIR" && ! -L "$M2_BASELINE_DIR" ]] || \
    die "M2_BASELINE_DIR must be an existing non-symlink directory"
  validate_direct_dir_path "$M2_DIRECT_DIR" || \
    die "M2_DIRECT_DIR must be the fresh directory printed by direct-discriminator"
  [[ -d "$M2_DIRECT_DIR" && ! -L "$M2_DIRECT_DIR" ]] || \
    die "M2_DIRECT_DIR must be an existing non-symlink directory"
  free_kb="$(free_kb_for_path "$run_dir")"
  [[ "$free_kb" =~ ^[0-9]+$ ]] && ((10#$free_kb >= M2_MIN_FREE_KB)) || \
    die "$action_description requires at least 4GiB free before evidence creation"

  utun="$(read_state utun)"
  target="$(read_state target)"
  exit_host="$(read_state exit_host)"
  server_port="$(read_state server_port)"
  iperf_port="$(read_state iperf_port)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  dns_name="$(read_state dns_name)"
  duration="$(read_state duration)"
  validate_positive_integer "$duration" || die "recorded smoke duration is invalid"
  quiescence_timeout_secs=$((10#$duration + 30))
  [[ -n "$dns_target" ]] || die "$action_description requires DNS_TARGET"
  [[ "$(route_interface "$target")" == "$utun" ]] || \
    die "TARGET no longer routes through $utun"
  [[ "$(route_interface "$dns_target")" == "$utun" ]] || \
    die "DNS_TARGET no longer routes through $utun"
  [[ "$(route_interface "$exit_host")" != "$utun" ]] || \
    die "Exit route recursed into $utun"
  ipv6_evidence_file="$run_dir/m2-ipv6-preflight.txt"
  if ! write_m2_ipv6_route_evidence "$ipv6_evidence_file"; then
    append_event_to "$run_dir" \
      "m2 preflight failed: IPv6 classification=$M2_IPV6_ROUTE_CLASSIFICATION interface=$M2_IPV6_ROUTE_INTERFACE status=$M2_IPV6_ROUTE_STATUS"
    die "$action_description IPv6 preflight failed classification=$M2_IPV6_ROUTE_CLASSIFICATION interface=$M2_IPV6_ROUTE_INTERFACE; evidence: $ipv6_evidence_file"
  fi
  network_control_is_sufficient "$run_dir" 5 && network_control_is_recent || \
    die "$action_description requires complete, recent Exit and physical-interface controls"
  tcp_pool_activity_is_idle "$run_dir/mini_vpn.log" || \
    die "$action_description requires fully drained TCP-pool ownership after smoke"
  for command_name in jq iperf3 dig curl route networksetup dscacheutil git; do
    require_command "$command_name"
  done
  validate_m0_baseline_pair "$M2_BASELINE_DIR" "$target" || \
    die "M2 baseline must contain valid nonzero TCP forward/reverse results"
  validate_direct_continuity_dir "$M2_DIRECT_DIR" "$M2_BASELINE_DIR" "$target" || \
    die "$action_description requires a matching 300s direct continuity PASS completed within 15 minutes"

  [[ "$M2_RESOURCE_PROFILE_HELPER" == \
      "$SCRIPT_DIR/knife15-m2-resource-profile.py" && \
    -f "$M2_RESOURCE_PROFILE_HELPER" && ! -L "$M2_RESOURCE_PROFILE_HELPER" && \
    "$M2_RESOURCE_PREFLIGHT_RUNNER" == \
      "$SCRIPT_DIR/knife15-m2-resource-preflight.sh" && \
    -f "$M2_RESOURCE_PREFLIGHT_RUNNER" && \
    ! -L "$M2_RESOURCE_PREFLIGHT_RUNNER" ]] || \
    die "$action_description requires exact tracked resource admission tools"
  validate_resource_preflight_dir_path "$M2_RESOURCE_PREFLIGHT_DIR" || \
    die "$action_description requires the exact /tmp resource preflight directory"
  resource_evidence_dir="$run_dir/${evidence_dir}-resource-preflight"
  resource_status_file="$run_dir/${evidence_dir}-resource-admission.status"
  [[ ! -e "$resource_evidence_dir" && ! -e "$resource_status_file" ]] || \
    die "$action_description resource admission already ran in this TUN"
  printf '%s\n' preparing >"$resource_status_file" || \
    die "cannot create resource admission status"
  current_source="$(git -C "$REPO" rev-parse HEAD)"
  binary_sha="$(sha256_file "$BIN")"
  observer_sha="$(sha256_file "$M2_EXIT_OBSERVER_SCRIPT")"
  physical_interface="$(route_interface "$exit_host")"
  [[ -n "$physical_interface" && "$physical_interface" != utun* ]] || {
    printf '%s\n' failed >"$resource_status_file"
    die "$action_description Exit has no physical resource-admission route"
  }
  if ! m2_copy_resource_preflight_evidence \
    "$M2_RESOURCE_PREFLIGHT_DIR" "$resource_evidence_dir" || \
    ! m2_resource_evidence_is_valid "$resource_evidence_dir" \
      "$current_source" "$binary_sha" "$M2_DIRECT_DIR/manifest.txt" \
      "$observer_sha" "$exit_host" "$server_port" "$target" "$iperf_port" \
      "$physical_interface"; then
    printf '%s\n' failed >"$resource_status_file"
    append_event_to "$run_dir" \
      "$stage resource admission failed preflight=$(basename "$M2_RESOURCE_PREFLIGHT_DIR")"
    die "$action_description resource profile/preflight evidence is invalid; no workload ran"
  fi
  resource_candidate_id="$(m2_resource_json_value \
    "$resource_evidence_dir/candidate-profile.json" candidate_id)" || {
    printf '%s\n' failed >"$resource_status_file"
    die "$action_description admitted resource candidate ID is invalid"
  }
  resource_profile_sha="$(m2_resource_json_value \
    "$resource_evidence_dir/eligibility.json" candidate_profile_sha256)" || {
    printf '%s\n' failed >"$resource_status_file"
    die "$action_description admitted resource profile hash is invalid"
  }
  write_state m2.resource_stage "$evidence_dir"
  write_state m2.resource_candidate "$resource_candidate_id"
  write_state m2.resource_profile_sha256 "$resource_profile_sha"
  write_state m2.resource_result_sha256 \
    "$(sha256_file "$resource_evidence_dir/result.txt")"
  printf '%s\n' pass >"$resource_status_file" || \
    die "cannot finalize resource admission status"
  append_event_to "$run_dir" \
    "$stage resource admission complete candidate=$resource_candidate_id profile=$resource_profile_sha"

  if m2_execution_requires_paired_observer "$execution_mode"; then
    [[ "$M2_EXIT_OBSERVER_SCRIPT" == \
      "$SCRIPT_DIR/knife15-exit-target-observer.sh" ]] || \
      die "$action_description requires the exact tracked Exit observer script"
    [[ -n "${EXIT_SSH_HOST:-}" && -n "${EXIT_SSH_KEY:-}" ]] || \
      die "$action_description requires explicit EXIT_SSH_HOST and EXIT_SSH_KEY for paired Exit evidence"
    m2_exit_observer_ssh_host_matches_exit "$EXIT_SSH_HOST" "$exit_host" || \
      die "$action_description Exit observer SSH host must match the recorded Exit $exit_host"
    if ! m2_exit_observer_require_active \
      "$run_dir" "$target" "$iperf_port" "$server_port"; then
      die "$action_description requires a healthy matching 26-hour Exit observer; start it after smoke and inspect $run_dir/m2-exit-observer-status.txt"
    fi
    append_event_to "$run_dir" \
      "$stage Exit observer admission complete target=$target iperf_port=$iperf_port tuic_port=$server_port"
  fi

  if [[ "$execution_mode" == "formal" ]]; then
    m2_exit_observer_arm "$run_dir" "$target" "$iperf_port" "$server_port"
    trap 'm2_exit_observer_exit_trap "$?"' EXIT
    append_event_to "$run_dir" \
      "m2 Exit observer ownership armed target=$target iperf_port=$iperf_port tuic_port=$server_port"
  fi

  mkdir "$run_dir/$evidence_dir" "$baseline_evidence_dir" "$direct_evidence_dir" \
    "$run_dir/$M2_REAL_CLIENT_EVIDENCE_DIR" || \
    die "cannot create M2 evidence directories"
  cp "$M2_BASELINE_DIR/direct-forward.json" \
    "$baseline_evidence_dir/direct-forward.json" || die "cannot preserve M2 forward baseline"
  cp "$M2_BASELINE_DIR/direct-reverse.json" \
    "$baseline_evidence_dir/direct-reverse.json" || die "cannot preserve M2 reverse baseline"
  cp "$M2_DIRECT_DIR/manifest.txt" "$direct_evidence_dir/manifest.txt" || \
    die "cannot preserve M2 direct manifest"
  cp "$M2_DIRECT_DIR/direct-forward-300s.json" \
    "$direct_evidence_dir/direct-forward-300s.json" || \
    die "cannot preserve M2 direct result"
  printf '%s\n' \
    'timestamp,label,rss_kib,fd_count,thread_rows,endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes,active_relays,total_relays,fake_ip_active,fake_ip_registered,dns_forged,dns_dropped,active_leases,replayed_relaying_handles,controlled_active_relays,replay_invalid' \
    >"$run_dir/m2-checkpoints.csv" || die "cannot create M2 checkpoint evidence"
  profile_file="$run_dir/${evidence_dir}-workload.txt"
  printf '%s\n' preparing >"$run_dir/$status_file"
  if ! write_m2_profile "$M2_BASELINE_DIR" "$profile_file" "$target" \
    "$iperf_port" "$dns_target" "$dns_name"; then
    printf '%s\n' failed >"$run_dir/$status_file"
    append_event_to "$run_dir" "$stage failed: workload profile derivation"
    die "cannot derive M2 workload profile"
  fi
  printf '%s\n' \
    "start_free_kb=$free_kb" \
    "direct_dir=$M2_DIRECT_DIR" \
    "direct_manifest_sha256=$(sha256_file "$M2_DIRECT_DIR/manifest.txt")" \
    "direct_result_sha256=$(sha256_file "$M2_DIRECT_DIR/direct-forward-300s.json")" \
    "resource_candidate_id=$resource_candidate_id" \
    "resource_profile_sha256=$resource_profile_sha" \
    "resource_preflight_dir=$M2_RESOURCE_PREFLIGHT_DIR" \
    "resource_preflight_result_sha256=$(sha256_file "$resource_evidence_dir/result.txt")" \
    >>"$profile_file" || die "cannot bind direct evidence to M2 profile"
  append_event_to "$run_dir" \
    "$stage prepared baseline=$(basename "$M2_BASELINE_DIR") profile=$(sha256_file "$profile_file")"
  if ! m2_resource_binding_is_valid "$run_dir" "$evidence_dir"; then
    printf '%s\n' failed >"$run_dir/$status_file"
    printf '%s\n' failed >"$resource_status_file"
    append_event_to "$run_dir" "$stage failed: final resource binding"
    die "$action_description resource binding changed before full-tunnel activation"
  fi
  activate_m2_full_tunnel "$run_dir" || {
    printf '%s\n' failed >"$run_dir/$status_file"
    die "M2 full-tunnel activation failed or rolled back; use status/snapshot/stop"
  }
  M2_CURL_BIN="$(command -v curl)"
  M0_TRACK_CHILD=1
  M0_ENFORCE_RUN_HEALTH=1
  M0_ACTIVE_RUN_DIR="$run_dir"
  SOAK_STAGE="$stage"
  SOAK_LABEL="$label"
  SOAK_EVIDENCE_DIR="$evidence_dir"
  SOAK_STATUS_FILE="$status_file"
  SOAK_SUCCESS_STATUS=complete
  SOAK_CONTINUE_DATA_QUALITY=0
  SOAK_VIOLATIONS_FILE=
  SOAK_REAL_CLIENT_PROBE=1
  sample_once_for "$run_dir" || true
  data_plane_values="$(m2_data_plane_envelope "$run_dir/mini_vpn.log")"
  read -r data_plane_samples_before _ <<<"$data_plane_values"
  endpoint_values="$(endpoint_resource_envelope "$run_dir/mini_vpn.log")"
  read -r endpoint_samples_before _ <<<"$endpoint_values"
  [[ "$data_plane_samples_before" =~ ^[0-9]+$ && \
    "$endpoint_samples_before" =~ ^[0-9]+$ ]] || {
    printf '%s\n' failed >"$run_dir/$status_file"
    append_event_to "$run_dir" "$stage failed: controlled-drain baseline"
    die "M2 cannot establish the controlled-drain baseline; use status/snapshot/stop"
  }
  if ! run_m2_real_client_probe "$run_dir" preflight; then
    printf '%s\n' failed >"$run_dir/$status_file"
    append_event_to "$run_dir" "$stage failed: real-client preflight"
    die "$action_description real-client preflight failed; no workload ran; use status/snapshot/stop"
  fi
  echo "Waiting for $action_description controlled-workload drain: hard_timeout=${quiescence_timeout_secs}s"
  record_m2_full_tunnel_quiescence "$run_dir" \
    "$profile_file" "$data_plane_samples_before" "$endpoint_samples_before" \
    "$quiescence_timeout_secs"
  quiescence_status=$?
  if [[ "$quiescence_status" != "0" ]]; then
    quiescence_snapshot="$(m2_full_tunnel_quiescence_snapshot \
      "$run_dir/mini_vpn.log" "$profile_file")"
    printf '%s\n' failed >"$run_dir/$status_file"
    if [[ "$quiescence_status" == "2" ]]; then
      append_event_to "$run_dir" \
        "$stage failed: cannot record controlled-drain snapshot=$quiescence_snapshot"
      die "M2 cannot record controlled-drain evidence; use status/snapshot/stop"
    fi
    if ! m0_assert_run_healthy "$run_dir"; then
      append_event_to "$run_dir" \
        "$stage failed: controlled-drain run health snapshot=$quiescence_snapshot"
      die "M2 became unhealthy during controlled drain; use status/snapshot/stop"
    fi
    append_event_to "$run_dir" \
      "$stage failed: controlled-drain ownership/evidence snapshot=$quiescence_snapshot"
    die "$action_description controlled workload did not drain or replay evidence was inconsistent before its schedule; use status/snapshot/stop"
  fi
  quiescence_snapshot="$(m2_full_tunnel_quiescence_snapshot \
    "$run_dir/mini_vpn.log" "$profile_file")"
  append_event_to "$run_dir" \
    "$stage controlled drain complete snapshot=$quiescence_snapshot"

  M0_IPERF3_BIN="$(command -v iperf3)"
  M0_DIG_BIN="$(command -v dig)"
  M0_SLEEP_BIN=/bin/sleep
  if ! register_m0_workload; then
    clear_workload_state
    printf '%s\n' failed >"$run_dir/$status_file"
    append_event_to "$run_dir" "$stage failed: workload identity registration"
    die "cannot establish identity-verified M2 workload state"
  fi
  trap "interrupt_m0 '$run_dir' INT" INT
  trap "interrupt_m0 '$run_dir' TERM" TERM
  trap "interrupt_m0 '$run_dir' HUP" HUP
  if [[ "$execution_mode" == "qualification" ]]; then
    echo "Starting Knife15 M2 qualification; two exact mixed cycles take about 30 minutes."
    echo "This action can never satisfy formal M2 acceptance."
    if run_m2_qualification_schedule "$run_dir" "$profile_file"; then
      trap - INT TERM HUP
      clear_workload_state
      sample_once_for "$run_dir" || true
      if ! m2_qualification_result_slo "$run_dir" 2 || \
        ! m2_resource_binding_is_valid "$run_dir" m2-qualification || \
        ! wait_for_m2_qualification_terminal_safety \
          "$run_dir" "$quiescence_timeout_secs" || \
        ! m0_assert_run_healthy "$run_dir" || \
        ! network_control_is_sufficient "$run_dir" 5; then
        printf '%s\n' failed >"$run_dir/$status_file"
        append_event_to "$run_dir" \
          "$stage failed: final health, network, or Endpoint evidence"
        write_summary "$run_dir"
        die "M2 qualification traffic passed but final safety evidence failed; use status/snapshot/stop"
      fi
      printf '%s\n' \
        'qualification_slo_evidence=PASS' \
        'formal_m2_acceptance=NOT_RUN' \
        >"$run_dir/m2-qualification-verdict.txt" || \
        die "cannot record M2 qualification verdict"
      write_summary "$run_dir"
      echo "PASS: M2 two-cycle qualification completed; this is not formal M2 acceptance."
      echo "run_dir=$run_dir"
      echo "Next: sudo -E bash scripts/knife15-macos-soak.sh status"
      echo "Then: sudo -E bash scripts/knife15-macos-soak.sh stop"
      return 0
    fi
    trap - INT TERM HUP
    clear_workload_state
    sample_once_for "$run_dir" || true
    die "M2 qualification workload failed; full tunnel and TUN remain for status/snapshot/stop evidence"
  fi
  echo "Starting formal Knife15 M2 workload; planned traffic/drain time is 86400 seconds."
  echo "Reserve about 25 wall-clock hours; full-tunnel DNS/routes remain owned until stop."
  if run_m2_schedule "$run_dir" "$profile_file"; then
    trap - INT TERM HUP
    if ! m2_exit_observer_finalize_armed workload-complete; then
      printf '%s\n' failed >"$run_dir/m2.status"
      append_event_to "$run_dir" "m2 failed: Exit observer finalization"
      write_summary "$run_dir"
      die "M2 timeline completed but Exit observer freeze/bundle failed; use status/snapshot/stop"
    fi
    clear_workload_state
    sample_once_for "$run_dir" || true
    if ! m2_workload_slo "$run_dir" || \
      ! m2_resource_binding_is_valid "$run_dir" m2; then
      printf '%s\n' failed >"$run_dir/m2.status"
      append_event_to "$run_dir" "m2 failed: acceptance SLO mismatch"
      write_summary "$run_dir"
      die "M2 timeline completed but acceptance SLO failed; use status/snapshot/stop"
    fi
    printf '%s\n' \
      'm2_slo_evidence=PASS' \
      'formal_m2_acceptance=PENDING_CLEANUP' \
      >"$run_dir/m2-pre-stop-verdict.txt" || \
      die "cannot record M2 pre-stop verdict"
    write_summary "$run_dir"
    echo "PASS: formal 24-hour M2 workload completed; cleanup acceptance is pending."
    echo "run_dir=$run_dir"
    echo "Next: sudo -E bash scripts/knife15-macos-soak.sh status"
    echo "Then: sudo -E bash scripts/knife15-macos-soak.sh stop"
    return 0
  fi
  trap - INT TERM HUP
  m2_exit_observer_finalize_armed workload-failed || \
    observer_finalization_status=$?
  clear_workload_state
  sample_once_for "$run_dir" || true
  if [[ "$observer_finalization_status" != "0" ]]; then
    warn "formal M2 failed and Exit observer finalization also failed; inspect m2-exit-observer-finalization.txt before remote recovery"
  fi
  die "M2 workload failed; full tunnel and TUN remain for status/snapshot/stop evidence"
}

show_status() {
  local run_dir pid utun target exit_host
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  pid="$(read_state vpn.pid 2>/dev/null || echo unknown)"
  utun="$(read_state utun 2>/dev/null || echo unknown)"
  target="$(read_state target 2>/dev/null || echo unknown)"
  exit_host="$(read_state exit_host 2>/dev/null || echo unknown)"
  if active_pid; then
    echo "state=running pid=$pid utun=$utun"
  else
    echo "state=stopped pid=$pid utun=$utun"
  fi
  echo "run_dir=$run_dir"
  echo "m0_status=$(sed -n '1p' "$run_dir/m0.status" 2>/dev/null || echo not_run)"
  echo "m1_status=$(sed -n '1p' "$run_dir/m1.status" 2>/dev/null || echo not_run)"
  echo "m1_mode=$(sed -n '1p' "$run_dir/m1-mode" 2>/dev/null || echo not_run)"
  echo "m2_status=$(sed -n '1p' "$run_dir/m2.status" 2>/dev/null || echo not_run)"
  echo "m2_qualification_status=$(sed -n '1p' \
    "$run_dir/m2-qualification.status" 2>/dev/null || echo not_run)"
  echo "m2_resource_admission=$(sed -n '1p' \
    "$run_dir/m2-resource-admission.status" 2>/dev/null || echo not_run)"
  echo "m2_qualification_resource_admission=$(sed -n '1p' \
    "$run_dir/m2-qualification-resource-admission.status" \
    2>/dev/null || echo not_run)"
  echo "m2_resource_stage=$(read_state m2.resource_stage 2>/dev/null || echo not_run)"
  echo "m2_resource_candidate=$(read_state m2.resource_candidate 2>/dev/null || echo not_run)"
  echo "m2_resource_profile_sha256=$(read_state \
    m2.resource_profile_sha256 2>/dev/null || echo not_run)"
  echo "m2_full_tunnel=$(read_state m2.full_tunnel 2>/dev/null || echo not_run)"
  if [[ -f "$run_dir/m2-ipv6-preflight.txt" ]]; then
    echo "m2_ipv6_preflight_classification=$(m0_profile_value \
      "$run_dir/m2-ipv6-preflight.txt" classification)"
    echo "m2_ipv6_preflight_interface=$(m0_profile_value \
      "$run_dir/m2-ipv6-preflight.txt" interface)"
    echo "m2_ipv6_preflight_route_status=$(m0_profile_value \
      "$run_dir/m2-ipv6-preflight.txt" route_status)"
  fi
  if [[ -f "$run_dir/m1-diagnostic-violations.tsv" ]]; then
    echo "m1_diagnostic_violations=$(awk 'END { print (NR > 0 ? NR - 1 : 0) }' \
      "$run_dir/m1-diagnostic-violations.tsv")"
  fi
  if workload_matches_run; then
    echo "soak_workload=running pid=$(read_state workload.pid) command=$(read_state workload.command)"
  else
    echo "soak_workload=inactive"
  fi
  echo "target_if=$(route_interface "$target") expected=$utun"
  echo "exit_if=$(route_interface "$exit_host") must_not_equal=$utun"
  tail -n 3 "$run_dir/process.csv" 2>/dev/null || true
  grep -E '📊 数据面|🔬 主循环|📊 TUIC QUIC stats|📊 TUIC endpoint pacing global|tuic-endpoint-rebind|tcp-tun-rx-drain|tcp-handle-close|tcp-d16-relay-close' \
    "$run_dir/mini_vpn.log" 2>/dev/null | tail -n 20 || true
}

snapshot_action() {
  local run_dir
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  bundle_is_finalized "$run_dir" && \
    die "evidence is finalized; snapshot cannot mutate it"
  active_pid || die "snapshot requires the run to be active"
  sample_once_for "$run_dir"
  append_event_to "$run_dir" "manual snapshot"
  echo "PASS: snapshot appended to $run_dir"
}

secret_scan() {
  local run_dir="$1"
  local result="$run_dir/secret-scan.txt"
  local scan_status
  grep -Erq --exclude='secret-scan.txt' -- \
    'MINI_VPN_TUIC_(UUID|PASSWORD)[=:]|BEGIN [A-Z ]*PRIVATE KEY|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' \
    "$run_dir" 2>/dev/null
  scan_status=$?
  case "$scan_status" in
    0)
      echo "FAIL: secret-shaped material found; bundle not created" >"$result"
      return 1
      ;;
    1)
      echo "PASS: no credential names, private-key markers, or UUID-shaped values found" >"$result"
      ;;
    *)
      echo "FAIL: evidence secret scan could not read every input" >"$result"
      return 1
      ;;
  esac
}

m2_pre_stop_verdict_is_valid() {
  local verdict_file="$1"
  [[ -f "$verdict_file" && ! -L "$verdict_file" && \
    "$(awk 'END {print NR + 0}' "$verdict_file" 2>/dev/null)" == "2" && \
    "$(sed -n '1p' "$verdict_file")" == "m2_slo_evidence=PASS" && \
    "$(sed -n '2p' "$verdict_file")" == \
      "formal_m2_acceptance=PENDING_CLEANUP" ]]
}

m2_formal_acceptance_from_values() {
  local status="$1"
  local pre_stop="$2"
  local full_tunnel_state="$3"
  local cleanup_complete="$4"
  local cleanup_restored="$5"
  if [[ "$status" != "complete" || "$pre_stop" != "PASS" ]]; then
    [[ "$status" == "not_run" ]] && printf '%s\n' NOT_RUN || printf '%s\n' FAIL
  elif [[ "$full_tunnel_state" == "active" && "$cleanup_complete" == "0" ]]; then
    printf '%s\n' PENDING_CLEANUP
  elif [[ "$full_tunnel_state" == "inactive" && "$cleanup_complete" =~ ^[1-9][0-9]*$ && \
    "$cleanup_restored" == "PASS" ]]; then
    printf '%s\n' PASS
  else
    printf '%s\n' FAIL
  fi
}

append_m2_summary() {
  local run_dir="$1"
  local status active_windows cycles dns idle resume final_drain
  local phase_failures health_failures real_failures phase_results
  local tcp_results udp_results tcp_gap udp_loss invalid_results
  local sender_zero receiver_zero dns_files invalid_dns real_results invalid_real
  local checkpoint_count checkpoint_labels checkpoint_first_rss
  local checkpoint_final_rss checkpoint_max_rss checkpoint_rss_delta
  local checkpoint_first_fd checkpoint_final_fd checkpoint_max_fd checkpoint_fd_delta
  local checkpoint_first_threads checkpoint_final_threads checkpoint_max_threads
  local checkpoint_threads_delta checkpoint_ownership_failures
  local pre_stop=NOT_APPLICABLE full_tunnel_state cleanup_complete=0
  local cleanup_restored=NOT_APPLICABLE formal_m2_acceptance m2_slo_evidence
  local ipv6_preflight_classification=not_run ipv6_preflight_interface=unknown
  local ipv6_preflight_route_status=unknown
  local resource_admission resource_candidate=not_run resource_profile=not_run
  status="$(sed -n '1p' "$run_dir/m2.status" 2>/dev/null || true)"
  status="${status:-not_run}"
  active_windows="$(grep -Ec $'\tm2 active .* complete planned_secs=' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  cycles="$(grep -Fc $'\tm2 cycle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  dns="$(grep -Fc $'\tm2 DNS complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  idle="$(grep -Fc $'\tm2 idle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  resume="$(grep -Fc $'\tm2 resume complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  final_drain="$(grep -Fc $'\tm2 final drain complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_failures="$(grep -Fc $'\tm2 phase failed ' "$run_dir/events.tsv" 2>/dev/null || true)"
  health_failures="$(grep -Fc $'\tm2 health failed:' "$run_dir/events.tsv" 2>/dev/null || true)"
  real_failures="$(grep -Fc $'\tm2 real client failed ' "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_results="$(grep -Fc $'\tm2 phase complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  read -r tcp_results udp_results tcp_gap udp_loss invalid_results \
    sender_zero receiver_zero <<<"$(m0_result_envelope "$run_dir/m2")"
  read -r dns_files invalid_dns <<<"$(m0_dns_result_envelope "$run_dir/m2")"
  read -r real_results invalid_real \
    <<<"$(m2_real_client_envelope "$run_dir/m2-real-client")"
  read -r checkpoint_count checkpoint_labels checkpoint_first_rss \
    checkpoint_final_rss checkpoint_max_rss checkpoint_rss_delta \
    checkpoint_first_fd checkpoint_final_fd checkpoint_max_fd checkpoint_fd_delta \
    checkpoint_first_threads checkpoint_final_threads checkpoint_max_threads \
    checkpoint_threads_delta checkpoint_ownership_failures \
    <<<"$(m2_checkpoint_envelope "$run_dir/m2-checkpoints.csv")"
  resource_admission="$(sed -n '1p' \
    "$run_dir/m2-resource-admission.status" 2>/dev/null || true)"
  resource_admission="${resource_admission:-not_run}"
  if [[ -f "$run_dir/m2-resource-preflight/candidate-profile.json" ]]; then
    resource_candidate="$(m2_resource_json_value \
      "$run_dir/m2-resource-preflight/candidate-profile.json" \
      candidate_id 2>/dev/null || echo invalid)"
    resource_profile="$(m2_resource_json_value \
      "$run_dir/m2-resource-preflight/eligibility.json" \
      candidate_profile_sha256 2>/dev/null || echo invalid)"
  fi
  if m2_pre_stop_verdict_is_valid "$run_dir/m2-pre-stop-verdict.txt"; then
    pre_stop=PASS
  elif [[ "$status" != "not_run" ]]; then
    pre_stop=MISMATCH
  fi
  m2_slo_evidence="$pre_stop"
  full_tunnel_state="$(read_state m2.full_tunnel 2>/dev/null || true)"
  full_tunnel_state="${full_tunnel_state:-not_run}"
  cleanup_complete="$(grep -Fc $'\tstop cleanup complete' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  if [[ "$full_tunnel_state" == "inactive" ]]; then
    if m2_full_tunnel_is_restored; then
      cleanup_restored=PASS
    else
      cleanup_restored=MISMATCH
    fi
  fi
  formal_m2_acceptance="$(m2_formal_acceptance_from_values "$status" \
    "$pre_stop" "$full_tunnel_state" "$cleanup_complete" "$cleanup_restored")"
  if [[ -f "$run_dir/m2-ipv6-preflight.txt" ]]; then
    ipv6_preflight_classification="$(m0_profile_value \
      "$run_dir/m2-ipv6-preflight.txt" classification 2>/dev/null || true)"
    ipv6_preflight_interface="$(m0_profile_value \
      "$run_dir/m2-ipv6-preflight.txt" interface 2>/dev/null || true)"
    ipv6_preflight_route_status="$(m0_profile_value \
      "$run_dir/m2-ipv6-preflight.txt" route_status 2>/dev/null || true)"
    ipv6_preflight_classification="${ipv6_preflight_classification:-unknown}"
    ipv6_preflight_interface="${ipv6_preflight_interface:-unknown}"
    ipv6_preflight_route_status="${ipv6_preflight_route_status:-unknown}"
  fi
  cat >>"$run_dir/summary.md" <<EOF_M2_SUMMARY

## Knife15 M2

- m2_status: $status
- m2_active_windows_completed: ${active_windows:-0}
- m2_cycles_completed: ${cycles:-0}
- m2_dns_completed: ${dns:-0}
- m2_real_client_results: ${real_results:-0}
- m2_invalid_real_client_results: ${invalid_real:-0}
- m2_idle_complete: ${idle:-0}
- m2_resume_complete: ${resume:-0}
- m2_final_drain_complete: ${final_drain:-0}
- m2_phase_failures: ${phase_failures:-0}
- m2_health_failures: ${health_failures:-0}
- m2_real_client_failures: ${real_failures:-0}
- m2_phase_results_completed: ${phase_results:-0}
- m2_tcp_results: ${tcp_results:-0}
- m2_udp_results: ${udp_results:-0}
- m2_tcp_max_sender_receiver_gap_bytes: ${tcp_gap:-unknown}
- m2_udp_max_loss_percent: ${udp_loss:-unknown}
- m2_invalid_result_files: ${invalid_results:-0}
- m2_sender_zero_intervals: ${sender_zero:-0}
- m2_receiver_zero_intervals: ${receiver_zero:-0}
- m2_dns_result_files: ${dns_files:-0}
- m2_invalid_dns_results: ${invalid_dns:-0}
- m2_checkpoint_count_labels_valid: ${checkpoint_count:-0}/${checkpoint_labels:-0}
- m2_checkpoint_rss_first_final_max_delta: ${checkpoint_first_rss:-unknown}/${checkpoint_final_rss:-unknown}/${checkpoint_max_rss:-unknown}/${checkpoint_rss_delta:-unknown}
- m2_checkpoint_fd_first_final_max_delta: ${checkpoint_first_fd:-unknown}/${checkpoint_final_fd:-unknown}/${checkpoint_max_fd:-unknown}/${checkpoint_fd_delta:-unknown}
- m2_checkpoint_threads_first_final_max_delta: ${checkpoint_first_threads:-unknown}/${checkpoint_final_threads:-unknown}/${checkpoint_max_threads:-unknown}/${checkpoint_threads_delta:-unknown}
- m2_checkpoint_ownership_failures: ${checkpoint_ownership_failures:-0}
- m2_ipv6_preflight_classification: $ipv6_preflight_classification
- m2_ipv6_preflight_interface: $ipv6_preflight_interface
- m2_ipv6_preflight_route_status: $ipv6_preflight_route_status
- m2_resource_admission: $resource_admission
- m2_resource_candidate: $resource_candidate
- m2_resource_profile_sha256: $resource_profile
- m2_slo_evidence: $m2_slo_evidence
- m2_full_tunnel_state: $full_tunnel_state
- m2_cleanup_restored: $cleanup_restored
- formal_m2_acceptance: $formal_m2_acceptance
EOF_M2_SUMMARY
}

append_m2_qualification_summary() {
  local run_dir="$1"
  local status phase_results phase_failures cycles dns
  local tcp_results udp_results tcp_gap udp_loss invalid_results
  local sender_zero receiver_zero real_results invalid_real verdict=NOT_RUN
  local resource_admission resource_candidate=not_run resource_profile=not_run
  status="$(sed -n '1p' \
    "$run_dir/m2-qualification.status" 2>/dev/null || true)"
  status="${status:-not_run}"
  phase_results="$(grep -Fc $'\tm2-qualification phase complete ' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  phase_failures="$(grep -Fc $'\tm2-qualification phase failed ' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  cycles="$(grep -Fc $'\tm2-qualification cycle complete cycle=' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  dns="$(grep -Fc $'\tm2-qualification DNS complete cycle=' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  read -r tcp_results udp_results tcp_gap udp_loss invalid_results \
    sender_zero receiver_zero \
    <<<"$(m0_result_envelope "$run_dir/m2-qualification")"
  read -r real_results invalid_real \
    <<<"$(m2_real_client_envelope \
      "$run_dir/m2-qualification-real-client")"
  resource_admission="$(sed -n '1p' \
    "$run_dir/m2-qualification-resource-admission.status" \
    2>/dev/null || true)"
  resource_admission="${resource_admission:-not_run}"
  if [[ -f "$run_dir/m2-qualification-resource-preflight/candidate-profile.json" ]]; then
    resource_candidate="$(m2_resource_json_value \
      "$run_dir/m2-qualification-resource-preflight/candidate-profile.json" \
      candidate_id 2>/dev/null || echo invalid)"
    resource_profile="$(m2_resource_json_value \
      "$run_dir/m2-qualification-resource-preflight/eligibility.json" \
      candidate_profile_sha256 2>/dev/null || echo invalid)"
  fi
  if [[ -f "$run_dir/m2-qualification-verdict.txt" && \
    ! -L "$run_dir/m2-qualification-verdict.txt" && \
    "$(sed -n '1p' "$run_dir/m2-qualification-verdict.txt")" == \
      "qualification_slo_evidence=PASS" && \
    "$(sed -n '2p' "$run_dir/m2-qualification-verdict.txt")" == \
      "formal_m2_acceptance=NOT_RUN" && \
    "$(awk 'END { print NR + 0 }' \
      "$run_dir/m2-qualification-verdict.txt")" == "2" ]]; then
    verdict=PASS_NON_ACCEPTANCE
  elif [[ "$status" != "not_run" ]]; then
    verdict=MISMATCH
  fi
  cat >>"$run_dir/summary.md" <<EOF_M2_QUALIFICATION_SUMMARY

## Knife15 M2 Qualification

- m2_qualification_status: $status
- m2_qualification_phase_results: ${phase_results:-0}
- m2_qualification_phase_failures: ${phase_failures:-0}
- m2_qualification_cycles: ${cycles:-0}
- m2_qualification_dns_results: ${dns:-0}
- m2_qualification_real_client_results: ${real_results:-0}
- m2_qualification_invalid_real_client_results: ${invalid_real:-0}
- m2_qualification_tcp_results: ${tcp_results:-0}
- m2_qualification_udp_results: ${udp_results:-0}
- m2_qualification_tcp_max_sender_receiver_gap_bytes: ${tcp_gap:-unknown}
- m2_qualification_udp_max_loss_percent: ${udp_loss:-unknown}
- m2_qualification_invalid_result_files: ${invalid_results:-0}
- m2_qualification_sender_zero_intervals: ${sender_zero:-0}
- m2_qualification_receiver_zero_intervals: ${receiver_zero:-0}
- m2_qualification_resource_admission: $resource_admission
- m2_qualification_resource_candidate: $resource_candidate
- m2_qualification_resource_profile_sha256: $resource_profile
- m2_qualification_verdict: $verdict
- m2_qualification_formal_m2_acceptance: NOT_RUN
EOF_M2_QUALIFICATION_SUMMARY
}

write_summary() {
  local run_dir="$1"
  local log_file="$run_dir/mini_vpn.log"
  local conservation verdict remote_write_failures remote_write_failure_log_matches
  local interface_error_samples log_bytes log_compactions
  local m0_status m0_cycles m0_dns m0_idle m0_resume m0_final_drain m0_phase_failures m0_health_failures
  local process_numeric_samples rss_first rss_last rss_max rss_delta
  local fd_first fd_last fd_max fd_delta threads_first threads_last threads_max threads_delta
  local interface_numeric_samples ipkts_first ipkts_last ipkts_delta ibytes_first ibytes_last ibytes_delta
  local opkts_first opkts_last opkts_delta obytes_first obytes_last obytes_delta
  local endpoint_samples_count endpoint_conservation_max endpoint_last_available endpoint_last_live
  local endpoint_last_outstanding endpoint_max_live endpoint_max_outstanding
  local endpoint_rebind_successes endpoint_rebind_recoveries endpoint_rebind_failures
  local endpoint_rebind_attempts endpoint_rebind_evidence endpoint_rebind_max_first_rx_ms
  local recovery_evidence_ordered_gaps recovery_evidence_writer_starts
  local recovery_evidence_writer_ends recovery_evidence_safety
  local m0_tcp_results m0_udp_results m0_tcp_max_gap m0_udp_max_loss m0_invalid_results
  local m0_sender_zero_intervals m0_receiver_zero_intervals
  local m0_phase_results m0_result_evidence
  local m0_dns_result_files m0_invalid_dns_results m0_dns_evidence m0_timeline_evidence
  local m1_status m1_mode m1_event_stage m1_completed
  local m1_active_windows m1_cycles m1_dns m1_idle m1_resume
  local m1_final_drain m1_phase_failures m1_health_failures m1_phase_results
  local m1_tcp_results m1_udp_results m1_tcp_max_gap m1_udp_max_loss
  local m1_invalid_results m1_sender_zero_intervals m1_receiver_zero_intervals
  local m1_dns_result_files m1_invalid_dns_results m1_result_evidence
  local m1_dns_evidence m1_timeline_evidence m1_checkpoint_evidence
  local m1_result_integrity_evidence m1_diagnostic_safety_evidence
  local m1_diagnostic_violation_count m1_diagnostic_invalid_violations
  local formal_m1_acceptance
  local m1_checkpoint_count m1_checkpoint_labels m1_checkpoint_first_rss
  local m1_checkpoint_final_rss m1_checkpoint_max_rss m1_checkpoint_rss_delta
  local m1_checkpoint_first_fd m1_checkpoint_final_fd m1_checkpoint_max_fd
  local m1_checkpoint_fd_delta m1_checkpoint_first_threads
  local m1_checkpoint_final_threads m1_checkpoint_max_threads
  local m1_checkpoint_threads_delta m1_checkpoint_ownership_failures
  local m1_sample_coverage_evidence m1_remote_write_evidence m1_slo_evidence
  local process_samples_total network_control_evidence network_control_samples
  local exit_ping_samples exit_ping_missing_samples exit_ping_rtt_samples
  local gateway_ping_samples gateway_ping_missing_samples physical_interface_samples
  local exit_ping_max_loss exit_ping_max_avg_rtt gateway_ping_max_loss physical_interface_error_samples
  local physical_ibytes_first physical_ibytes_last physical_ibytes_delta
  local physical_obytes_first physical_obytes_last physical_obytes_delta
  local physical_rx_bps_max physical_tx_bps_max
  local cleanup_complete process_last_state interface_last_mtu cleanup_evidence
  if conservation_check_file "$log_file"; then
    conservation=PASS
  else
    conservation=FAIL_OR_MISSING
  fi
  remote_write_failure_log_matches="$(grep -Ec '写入上游流失败|reason=remote_write_failed|reason=stalled_write_timeout' "$log_file" 2>/dev/null || true)"
  # One D16 terminal event is also emitted as a raw error and handle-close
  # diagnostic. Count the canonical relay-close record once while preserving
  # the broader log-match count for forensic review.
  remote_write_failures="$(grep -Ec 'tcp-d16-relay-close .*terminal_reason=(remote_write_failed|stalled_write_timeout)( |$)' "$log_file" 2>/dev/null || true)"
  endpoint_rebind_successes="$(grep -Ec 'tuic-endpoint-rebind generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  endpoint_rebind_recoveries="$(grep -Ec 'tuic-endpoint-rebind-recovered generation=[0-9]+ first_rx_ms=[0-9]+ socket_generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  endpoint_rebind_failures="$(grep -Ec 'tuic-endpoint-rebind-failed generation=[0-9]+' \
    "$log_file" 2>/dev/null || true)"
  endpoint_rebind_attempts=$((10#${endpoint_rebind_successes:-0} + 10#${endpoint_rebind_failures:-0}))
  endpoint_rebind_max_first_rx_ms="$(awk '
    match($0, /tuic-endpoint-rebind-recovered generation=[0-9]+ first_rx_ms=[0-9]+/) {
      value = substr($0, RSTART, RLENGTH)
      sub(/^.*first_rx_ms=/, "", value)
      if (value + 0 > maximum) maximum = value + 0
    }
    END { print maximum + 0 }
  ' "$log_file" 2>/dev/null)"
  endpoint_rebind_evidence=NOT_OBSERVED
  if ((10#$endpoint_rebind_attempts > 0)); then
    if ((10#${endpoint_rebind_failures:-0} == 0 && \
      10#${endpoint_rebind_successes:-0} == 10#${endpoint_rebind_recoveries:-0})); then
      endpoint_rebind_evidence=PASS
    else
      endpoint_rebind_evidence=MISMATCH
    fi
  fi
  recovery_evidence_ordered_gaps="$(grep -Ec \
    'tuic-recovery-evidence kind=tcp_ordered_gap_observed action=none ' \
    "$log_file" 2>/dev/null || true)"
  recovery_evidence_writer_starts="$(grep -Ec \
    'tuic-recovery-evidence kind=tcp_write_pressure_start action=none ' \
    "$log_file" 2>/dev/null || true)"
  recovery_evidence_writer_ends="$(grep -Ec \
    'tuic-recovery-evidence kind=tcp_write_pressure_end action=none ' \
    "$log_file" 2>/dev/null || true)"
  if recovery_evidence_is_safe "$log_file"; then
    recovery_evidence_safety=PASS
  else
    recovery_evidence_safety=MISMATCH
  fi
  interface_error_samples="$(awk -F, '
    NR > 1 && (($5 ~ /^[0-9]+$/ && $5 + 0 > 0) || ($8 ~ /^[0-9]+$/ && $8 + 0 > 0)) { count++ }
    END { print count + 0 }
  ' "$run_dir/interface.csv" 2>/dev/null)"
  log_bytes="$(wc -c <"$log_file" 2>/dev/null | tr -d ' ')"
  log_compactions="$(grep -Fc $'\twatchdog compacted mini_vpn.log' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_status="$(sed -n '1p' "$run_dir/m0.status" 2>/dev/null || true)"
  m0_status="${m0_status:-not_run}"
  m0_cycles="$(grep -Fc $'\tm0 cycle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_dns="$(grep -Fc $'\tm0 DNS complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_idle="$(grep -Fc $'\tm0 idle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_resume="$(grep -Fc $'\tm0 resume complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_final_drain="$(grep -Fc $'\tm0 final drain complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_phase_failures="$(grep -Fc $'\tm0 phase failed ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_health_failures="$(grep -Fc $'\tm0 health failed:' "$run_dir/events.tsv" 2>/dev/null || true)"
  m0_phase_results="$(grep -Fc $'\tm0 phase complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_status="$(sed -n '1p' "$run_dir/m1.status" 2>/dev/null || true)"
  m1_status="${m1_status:-not_run}"
  m1_mode="$(sed -n '1p' "$run_dir/m1-mode" 2>/dev/null || true)"
  if [[ -z "$m1_mode" ]]; then
    [[ "$m1_status" == "not_run" ]] && m1_mode=not_run || m1_mode=formal
  fi
  m1_event_stage=m1
  [[ "$m1_mode" == "diagnostic" ]] && m1_event_stage=m1-diagnostic
  m1_completed=0
  if [[ "$m1_status" == "complete" || \
    "$m1_status" == "diagnostic_complete_clean" || \
    "$m1_status" == "diagnostic_complete_with_violations" ]]; then
    m1_completed=1
  fi
  m1_active_windows="$(grep -Ec $'\t'"$m1_event_stage"' active .* complete ' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_cycles="$(grep -Fc $'\t'"$m1_event_stage"' cycle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_dns="$(grep -Fc $'\t'"$m1_event_stage"' DNS complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_idle="$(grep -Fc $'\t'"$m1_event_stage"' idle complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_resume="$(grep -Fc $'\t'"$m1_event_stage"' resume complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_final_drain="$(grep -Fc $'\t'"$m1_event_stage"' final drain complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_phase_failures="$(grep -Fc $'\t'"$m1_event_stage"' phase failed ' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_health_failures="$(grep -Fc $'\t'"$m1_event_stage"' health failed:' "$run_dir/events.tsv" 2>/dev/null || true)"
  m1_phase_results="$(grep -Fc $'\t'"$m1_event_stage"' phase complete ' "$run_dir/events.tsv" 2>/dev/null || true)"
  read -r process_numeric_samples rss_first rss_last rss_max rss_delta \
    fd_first fd_last fd_max fd_delta threads_first threads_last threads_max threads_delta \
    <<<"$(process_resource_envelope "$run_dir/process.csv")"
  read -r interface_numeric_samples ipkts_first ipkts_last ipkts_delta \
    ibytes_first ibytes_last ibytes_delta opkts_first opkts_last opkts_delta \
    obytes_first obytes_last obytes_delta \
    <<<"$(interface_resource_envelope "$run_dir/interface.csv")"
  read -r endpoint_samples_count endpoint_conservation_max endpoint_last_available \
    endpoint_last_live endpoint_last_outstanding endpoint_max_live endpoint_max_outstanding \
    <<<"$(endpoint_resource_envelope "$log_file")"
  read -r m0_tcp_results m0_udp_results m0_tcp_max_gap m0_udp_max_loss m0_invalid_results \
    m0_sender_zero_intervals m0_receiver_zero_intervals \
    <<<"$(m0_result_envelope "$run_dir/m0")"
  read -r m0_dns_result_files m0_invalid_dns_results \
    <<<"$(m0_dns_result_envelope "$run_dir/m0")"
  read -r m1_tcp_results m1_udp_results m1_tcp_max_gap m1_udp_max_loss \
    m1_invalid_results m1_sender_zero_intervals m1_receiver_zero_intervals \
    <<<"$(m0_result_envelope "$run_dir/m1")"
  read -r m1_dns_result_files m1_invalid_dns_results \
    <<<"$(m0_dns_result_envelope "$run_dir/m1")"
  m1_diagnostic_violation_count=0
  m1_diagnostic_invalid_violations=0
  if [[ "$m1_mode" == "diagnostic" ]]; then
    read -r m1_diagnostic_violation_count m1_diagnostic_invalid_violations \
      <<<"$(m1_diagnostic_violation_envelope \
        "$run_dir/m1-diagnostic-violations.tsv")"
  fi
  read -r m1_checkpoint_count m1_checkpoint_labels m1_checkpoint_first_rss \
    m1_checkpoint_final_rss m1_checkpoint_max_rss m1_checkpoint_rss_delta \
    m1_checkpoint_first_fd m1_checkpoint_final_fd m1_checkpoint_max_fd \
    m1_checkpoint_fd_delta m1_checkpoint_first_threads \
    m1_checkpoint_final_threads m1_checkpoint_max_threads \
    m1_checkpoint_threads_delta m1_checkpoint_ownership_failures \
    <<<"$(m1_checkpoint_envelope "$run_dir/m1-checkpoints.csv")"
  process_samples_total="$(awk 'END {print (NR > 0 ? NR - 1 : 0)}' \
    "$run_dir/process.csv" 2>/dev/null)"
  process_last_state="$(awk -F, 'NR > 1 {value=$3} END {print value}' \
    "$run_dir/process.csv" 2>/dev/null)"
  interface_last_mtu="$(awk -F, 'NR > 1 {value=$3} END {print value}' \
    "$run_dir/interface.csv" 2>/dev/null)"
  cleanup_complete="$(grep -Fc $'\tstop cleanup complete' \
    "$run_dir/events.tsv" 2>/dev/null || true)"
  cleanup_evidence=NOT_APPLICABLE
  if [[ "$cleanup_complete" =~ ^[1-9][0-9]*$ ]]; then
    if [[ "$process_last_state" == "dead" && "$interface_last_mtu" == "unknown" ]] && \
      owned_routes_are_restored; then
      cleanup_evidence=PASS
    else
      cleanup_evidence=MISMATCH
    fi
  fi
  read -r network_control_samples exit_ping_samples exit_ping_missing_samples \
    exit_ping_rtt_samples gateway_ping_samples gateway_ping_missing_samples \
    physical_interface_samples exit_ping_max_loss exit_ping_max_avg_rtt \
    gateway_ping_max_loss physical_interface_error_samples physical_ibytes_first \
    physical_ibytes_last physical_ibytes_delta physical_obytes_first \
    physical_obytes_last physical_obytes_delta physical_rx_bps_max physical_tx_bps_max \
    <<<"$(network_control_envelope "$run_dir/network.csv")"
  network_control_evidence=MISSING
  if network_control_is_sufficient "$run_dir"; then
    network_control_evidence=PASS
  elif [[ "$network_control_samples" =~ ^[0-9]+$ ]] && \
    ((10#$network_control_samples > 0)); then
    network_control_evidence=PARTIAL
  fi
  m0_result_evidence=NOT_APPLICABLE
  m0_dns_evidence=NOT_APPLICABLE
  m0_timeline_evidence=NOT_APPLICABLE
  if [[ "$m0_status" == "complete" ]]; then
    if ((10#$m0_phase_results > 0 && \
      10#$m0_phase_results == 10#$m0_tcp_results + 10#$m0_udp_results && \
      10#$m0_invalid_results == 0 && \
      10#$m0_receiver_zero_intervals == 0)); then
      m0_result_evidence=PASS
    else
      m0_result_evidence=MISMATCH
    fi
    if ((10#$m0_dns > 0 && 10#$m0_dns == 10#$m0_dns_result_files && \
      10#$m0_invalid_dns_results == 0)); then
      m0_dns_evidence=PASS
    else
      m0_dns_evidence=MISMATCH
    fi
    if ((10#$m0_cycles > 0 && 10#$m0_dns == 10#$m0_cycles && \
      10#$m0_idle == 1 && 10#$m0_resume == 1 && 10#$m0_final_drain == 1 && \
      10#$m0_phase_failures == 0 && 10#$m0_health_failures == 0)); then
      m0_timeline_evidence=PASS
    else
      m0_timeline_evidence=MISMATCH
    fi
  fi
  m1_result_evidence=NOT_APPLICABLE
  m1_dns_evidence=NOT_APPLICABLE
  m1_timeline_evidence=NOT_APPLICABLE
  m1_checkpoint_evidence=NOT_APPLICABLE
  m1_sample_coverage_evidence=NOT_APPLICABLE
  m1_remote_write_evidence=NOT_APPLICABLE
  m1_slo_evidence=NOT_APPLICABLE
  m1_result_integrity_evidence=NOT_APPLICABLE
  m1_diagnostic_safety_evidence=NOT_APPLICABLE
  formal_m1_acceptance=NOT_RUN
  if ((m1_completed == 1)); then
    if ((10#$m1_phase_results > 0 && \
      10#$m1_phase_results == 10#$m1_tcp_results + 10#$m1_udp_results && \
      10#$m1_invalid_results == 0)); then
      m1_result_integrity_evidence=PASS
    else
      m1_result_integrity_evidence=MISMATCH
    fi
    if [[ "$m1_mode" == "formal" ]]; then
      if [[ "$m1_result_integrity_evidence" == "PASS" ]] && \
        ((10#$m1_receiver_zero_intervals == 0)); then
        m1_result_evidence=PASS
      else
        m1_result_evidence=MISMATCH
      fi
    fi
    if ((10#$m1_dns > 0 && 10#$m1_dns == 10#$m1_dns_result_files && \
      10#$m1_invalid_dns_results == 0)); then
      m1_dns_evidence=PASS
    else
      m1_dns_evidence=MISMATCH
    fi
    if ((10#$m1_active_windows == 5 && 10#$m1_cycles > 0 && \
      10#$m1_dns == 10#$m1_cycles && 10#$m1_idle == 3 && \
      10#$m1_resume == 3 && 10#$m1_final_drain == 1 && \
      10#$m1_phase_failures == 0 && 10#$m1_health_failures == 0)); then
      m1_timeline_evidence=PASS
    else
      m1_timeline_evidence=MISMATCH
    fi
    if m1_checkpoint_slo "$run_dir/m1-checkpoints.csv"; then
      m1_checkpoint_evidence=PASS
    else
      m1_checkpoint_evidence=MISMATCH
    fi
    if [[ "$process_numeric_samples" =~ ^[0-9]+$ && \
      "$interface_numeric_samples" =~ ^[0-9]+$ && \
      "$endpoint_samples_count" =~ ^[0-9]+$ && \
      "$network_control_samples" =~ ^[0-9]+$ ]] && \
      ((10#$process_numeric_samples >= 900 && \
        10#$interface_numeric_samples >= 900 && \
        10#$endpoint_samples_count >= 900 && \
        10#$network_control_samples >= 900)) && \
      [[ "$network_control_evidence" == "PASS" ]]; then
      m1_sample_coverage_evidence=PASS
    else
      m1_sample_coverage_evidence=MISMATCH
    fi
    if [[ "$remote_write_failures" == "0" && \
      "$remote_write_failure_log_matches" == "0" ]]; then
      m1_remote_write_evidence=PASS
    elif [[ "$remote_write_failures" =~ ^[1-9][0-9]*$ ]] && \
      remote_write_close_ownership_is_clean "$log_file" && \
      [[ "$m1_result_integrity_evidence" == "PASS" ]]; then
      m1_remote_write_evidence=CLASSIFIED_REVIEW
    else
      m1_remote_write_evidence=MISMATCH
    fi
  fi
  if [[ "$m1_mode" == "formal" && "$m1_status" == "complete" ]]; then
    m1_slo_evidence=PASS
    [[ "$m1_timeline_evidence" == "PASS" && \
      "$m1_result_evidence" == "PASS" && "$m1_dns_evidence" == "PASS" && \
      "$m1_checkpoint_evidence" == "PASS" && \
      "$m1_sample_coverage_evidence" == "PASS" && \
      "$m1_remote_write_evidence" != "MISMATCH" && \
      "$conservation" == "PASS" && "$endpoint_last_live" == "0" && \
      "$endpoint_last_outstanding" == "0" && \
      "$endpoint_conservation_max" =~ ^[0-9]+$ && \
      "$m1_tcp_max_gap" =~ ^[0-9]+$ && \
      "$log_compactions" =~ ^[0-9]+$ && \
      "$interface_error_samples" =~ ^[0-9]+$ && \
      "$physical_interface_error_samples" =~ ^[0-9]+$ ]] || m1_slo_evidence=MISMATCH
    if [[ "$m1_slo_evidence" == "PASS" ]]; then
      ((10#$m1_cycles == 30 && 10#$m1_dns == 30 && \
        10#$m1_phase_results == 332 && 10#$m1_tcp_results == 302 && \
        10#$m1_udp_results == 30 && 10#$endpoint_conservation_max <= 61440 && \
        10#$m1_tcp_max_gap <= 16777216 && 10#$log_compactions == 0 && \
        10#$interface_error_samples == 0 && \
        10#$physical_interface_error_samples == 0)) || m1_slo_evidence=MISMATCH
    fi
    if [[ "$m1_slo_evidence" == "PASS" ]]; then
      decimal_le "$m1_udp_max_loss" 3.0 || m1_slo_evidence=MISMATCH
    fi
    if [[ "$m1_slo_evidence" == "PASS" && \
      "$endpoint_rebind_evidence" != "NOT_OBSERVED" ]]; then
      [[ "$endpoint_rebind_evidence" == "PASS" ]] && \
        ((10#$endpoint_rebind_max_first_rx_ms <= 7000)) || \
        m1_slo_evidence=MISMATCH
    fi
    if [[ "$m1_slo_evidence" == "PASS" ]] && \
      ! d16_terminal_ownership_is_clean "$log_file"; then
      m1_slo_evidence=MISMATCH
    fi
    if [[ "$m1_slo_evidence" == "PASS" ]] && \
      grep -Eq 'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|send_slice_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|reason=stalled_write_timeout|reason=idle_timeout' "$log_file"; then
      m1_slo_evidence=MISMATCH
    fi
    [[ "$m1_slo_evidence" == "PASS" ]] && \
      formal_m1_acceptance=PASS || formal_m1_acceptance=FAIL
  elif [[ "$m1_mode" == "diagnostic" && "$m1_completed" == "1" ]]; then
    m1_diagnostic_safety_evidence=PASS
    [[ "$m1_timeline_evidence" == "PASS" && \
      "$m1_result_integrity_evidence" == "PASS" && \
      "$m1_dns_evidence" == "PASS" && \
      "$m1_checkpoint_evidence" == "PASS" && \
      "$m1_sample_coverage_evidence" == "PASS" && \
      "$m1_remote_write_evidence" != "MISMATCH" && \
      "$conservation" == "PASS" && "$endpoint_last_live" == "0" && \
      "$endpoint_last_outstanding" == "0" && \
      "$endpoint_conservation_max" =~ ^[0-9]+$ && \
      "$log_compactions" =~ ^[0-9]+$ && \
      "$interface_error_samples" =~ ^[0-9]+$ && \
      "$physical_interface_error_samples" =~ ^[0-9]+$ && \
      "$m1_diagnostic_invalid_violations" == "0" ]] || \
      m1_diagnostic_safety_evidence=MISMATCH
    if [[ "$m1_diagnostic_safety_evidence" == "PASS" ]]; then
      ((10#$m1_cycles == 30 && 10#$m1_dns == 30 && \
        10#$m1_phase_results == 332 && 10#$m1_tcp_results == 302 && \
        10#$m1_udp_results == 30 && 10#$endpoint_conservation_max <= 61440 && \
        10#$log_compactions == 0 && 10#$interface_error_samples == 0 && \
        10#$physical_interface_error_samples == 0)) || \
        m1_diagnostic_safety_evidence=MISMATCH
    fi
    if [[ "$m1_diagnostic_safety_evidence" == "PASS" && \
      "$endpoint_rebind_evidence" != "NOT_OBSERVED" ]]; then
      [[ "$endpoint_rebind_evidence" == "PASS" ]] && \
        ((10#$endpoint_rebind_max_first_rx_ms <= 7000)) || \
        m1_diagnostic_safety_evidence=MISMATCH
    fi
    if [[ "$m1_diagnostic_safety_evidence" == "PASS" ]] && \
      ! d16_terminal_ownership_is_clean "$log_file"; then
      m1_diagnostic_safety_evidence=MISMATCH
    fi
    if [[ "$m1_diagnostic_safety_evidence" == "PASS" ]] && \
      grep -Eq 'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|send_slice_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|reason=stalled_write_timeout|reason=idle_timeout' "$log_file"; then
      m1_diagnostic_safety_evidence=MISMATCH
    fi
    if [[ "$m1_diagnostic_safety_evidence" == "PASS" ]]; then
      m1_diagnostic_violation_coverage_is_complete "$run_dir" || \
        m1_diagnostic_safety_evidence=MISMATCH
    fi
    if [[ "$m1_diagnostic_safety_evidence" == "PASS" ]]; then
      if [[ "$m1_status" == "diagnostic_complete_clean" ]]; then
        ((10#$m1_diagnostic_violation_count == 0)) || \
          m1_diagnostic_safety_evidence=MISMATCH
      elif [[ "$m1_status" == "diagnostic_complete_with_violations" ]]; then
        ((10#$m1_diagnostic_violation_count > 0)) || \
          m1_diagnostic_safety_evidence=MISMATCH
      else
        m1_diagnostic_safety_evidence=MISMATCH
      fi
    fi
    formal_m1_acceptance=NOT_APPLICABLE
  fi
  if [[ "$conservation" != "PASS" ]] || \
    [[ "$m0_status" != "complete" && "$m0_status" != "not_run" ]] || \
    { [[ "$m0_status" == "complete" ]] && \
      [[ "$endpoint_last_live" != "0" || "$endpoint_last_outstanding" != "0" ]]; } || \
    [[ "$m0_result_evidence" == "MISMATCH" ]] || \
    [[ "$m0_dns_evidence" == "MISMATCH" ]] || \
    [[ "$m0_timeline_evidence" == "MISMATCH" ]] || \
    [[ "$m1_status" != "complete" && "$m1_status" != "not_run" ]] || \
    [[ "$m1_result_evidence" == "MISMATCH" ]] || \
    [[ "$m1_dns_evidence" == "MISMATCH" ]] || \
    [[ "$m1_timeline_evidence" == "MISMATCH" ]] || \
    [[ "$m1_checkpoint_evidence" == "MISMATCH" ]] || \
    [[ "$m1_sample_coverage_evidence" == "MISMATCH" ]] || \
    [[ "$m1_remote_write_evidence" == "MISMATCH" ]] || \
    [[ "$m1_slo_evidence" == "MISMATCH" ]] || \
    [[ "$cleanup_evidence" == "MISMATCH" ]] || \
    { [[ "$m0_status" == "complete" ]] && \
      [[ "$network_control_evidence" != "PASS" ]]; } || \
    { [[ "$log_compactions" =~ ^[0-9]+$ ]] && ((10#$log_compactions > 0)); } || \
    { [[ "$m0_invalid_results" =~ ^[0-9]+$ ]] && ((10#$m0_invalid_results > 0)); } || \
    { [[ "$m0_sender_zero_intervals" =~ ^[0-9]+$ ]] && \
    ((10#$m0_sender_zero_intervals > 0)); } || \
    { [[ "$m0_receiver_zero_intervals" =~ ^[0-9]+$ ]] && \
    ((10#$m0_receiver_zero_intervals > 0)); } || \
    { [[ "$m1_sender_zero_intervals" =~ ^[0-9]+$ ]] && \
    ((10#$m1_sender_zero_intervals > 0)); } || \
    { [[ "$m1_receiver_zero_intervals" =~ ^[0-9]+$ ]] && \
    ((10#$m1_receiver_zero_intervals > 0)); } || \
    ((10#${endpoint_rebind_attempts:-0} > 0)) || \
    [[ "$recovery_evidence_safety" != "PASS" ]] || \
    { [[ "$interface_error_samples" =~ ^[0-9]+$ ]] && \
    ((10#$interface_error_samples > 0)); } || \
    { [[ "$physical_interface_error_samples" =~ ^[0-9]+$ ]] && \
    ((10#$physical_interface_error_samples > 0)); } || \
    ! d16_terminal_ownership_is_clean "$log_file" || \
    grep -Eq 'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|写入上游流失败|reason=remote_write_failed|reason=stalled_write_timeout|reason=idle_timeout' "$log_file"; then
    verdict=REVIEW
  else
    verdict=NO_KNOWN_INTERNAL_FAILURE_SIGNAL
  fi
  cat >"$run_dir/summary.md" <<EOF_SUMMARY || die "cannot write run summary"
# Knife15 macOS Run Summary

- generated_utc: $(timestamp)
- endpoint_conservation: $conservation
- internal_failure_scan: $verdict
- remote_write_failures: ${remote_write_failures:-unknown}
- remote_write_failure_log_matches: ${remote_write_failure_log_matches:-unknown}
- endpoint_rebind_attempts_recoveries_failures: ${endpoint_rebind_attempts:-0}/${endpoint_rebind_recoveries:-0}/${endpoint_rebind_failures:-0}
- endpoint_rebind_evidence: $endpoint_rebind_evidence
- endpoint_rebind_max_first_rx_ms: ${endpoint_rebind_max_first_rx_ms:-0}
- recovery_evidence_ordered_gaps: ${recovery_evidence_ordered_gaps:-0}
- recovery_evidence_writer_starts_ends: ${recovery_evidence_writer_starts:-0}/${recovery_evidence_writer_ends:-0}
- recovery_evidence_safety: $recovery_evidence_safety
- interface_error_samples: ${interface_error_samples:-unknown}
- network_control_evidence: $network_control_evidence
- network_control_samples: ${network_control_samples:-0}
- exit_ping_samples_missing_rtt_samples: ${exit_ping_samples:-0}/${exit_ping_missing_samples:-0}/${exit_ping_rtt_samples:-0}
- exit_ping_max_loss_percent: ${exit_ping_max_loss:-unknown}
- exit_ping_max_avg_rtt_ms: ${exit_ping_max_avg_rtt:-unknown}
- gateway_ping_samples_missing: ${gateway_ping_samples:-0}/${gateway_ping_missing_samples:-0}
- gateway_ping_max_loss_percent: ${gateway_ping_max_loss:-unknown}
- physical_interface_samples: ${physical_interface_samples:-0}
- physical_interface_error_samples: ${physical_interface_error_samples:-0}
- physical_ibytes_first_last_delta: ${physical_ibytes_first:-unknown}/${physical_ibytes_last:-unknown}/${physical_ibytes_delta:-unknown}
- physical_obytes_first_last_delta: ${physical_obytes_first:-unknown}/${physical_obytes_last:-unknown}/${physical_obytes_delta:-unknown}
- physical_rx_tx_bps_max: ${physical_rx_bps_max:-unknown}/${physical_tx_bps_max:-unknown}
- mini_vpn_log_bytes: ${log_bytes:-unknown}
- log_compactions: ${log_compactions:-unknown}
- m0_status: $m0_status
- m0_cycles_completed: ${m0_cycles:-0}
- m0_dns_completed: ${m0_dns:-0}
- m0_idle_complete: ${m0_idle:-0}
- m0_resume_complete: ${m0_resume:-0}
- m0_final_drain_complete: ${m0_final_drain:-0}
- m0_phase_failures: ${m0_phase_failures:-0}
- m0_health_failures: ${m0_health_failures:-0}
- m0_timeline_evidence: $m0_timeline_evidence
- m0_phase_results_completed: ${m0_phase_results:-0}
- m0_result_evidence: $m0_result_evidence
- m0_dns_result_files: ${m0_dns_result_files:-0}
- m0_dns_evidence: $m0_dns_evidence
- m0_tcp_udp_results: ${m0_tcp_results:-0}/${m0_udp_results:-0}
- m0_tcp_max_sender_receiver_gap_bytes: ${m0_tcp_max_gap:-unknown}
- m0_udp_max_loss_percent: ${m0_udp_max_loss:-unknown}
- m0_invalid_result_files: ${m0_invalid_results:-0}
- m0_sender_zero_intervals: ${m0_sender_zero_intervals:-0}
- m0_receiver_zero_intervals: ${m0_receiver_zero_intervals:-0}
- m1_status: $m1_status
- m1_mode: $m1_mode
- formal_m1_acceptance: $formal_m1_acceptance
- m1_active_windows_completed: ${m1_active_windows:-0}
- m1_cycles_completed: ${m1_cycles:-0}
- m1_dns_completed: ${m1_dns:-0}
- m1_idle_complete: ${m1_idle:-0}
- m1_resume_complete: ${m1_resume:-0}
- m1_final_drain_complete: ${m1_final_drain:-0}
- m1_phase_failures: ${m1_phase_failures:-0}
- m1_health_failures: ${m1_health_failures:-0}
- m1_timeline_evidence: $m1_timeline_evidence
- m1_phase_results_completed: ${m1_phase_results:-0}
- m1_result_integrity_evidence: $m1_result_integrity_evidence
- m1_result_evidence: $m1_result_evidence
- m1_dns_result_files: ${m1_dns_result_files:-0}
- m1_dns_evidence: $m1_dns_evidence
- m1_tcp_udp_results: ${m1_tcp_results:-0}/${m1_udp_results:-0}
- m1_tcp_max_sender_receiver_gap_bytes: ${m1_tcp_max_gap:-unknown}
- m1_udp_max_loss_percent: ${m1_udp_max_loss:-unknown}
- m1_invalid_result_files: ${m1_invalid_results:-0}
- m1_sender_zero_intervals: ${m1_sender_zero_intervals:-0}
- m1_receiver_zero_intervals: ${m1_receiver_zero_intervals:-0}
- m1_checkpoint_count_labels_valid: ${m1_checkpoint_count:-0}/${m1_checkpoint_labels:-0}
- m1_checkpoint_rss_first_final_max_delta: ${m1_checkpoint_first_rss:-unknown}/${m1_checkpoint_final_rss:-unknown}/${m1_checkpoint_max_rss:-unknown}/${m1_checkpoint_rss_delta:-unknown}
- m1_checkpoint_fd_first_final_max_delta: ${m1_checkpoint_first_fd:-unknown}/${m1_checkpoint_final_fd:-unknown}/${m1_checkpoint_max_fd:-unknown}/${m1_checkpoint_fd_delta:-unknown}
- m1_checkpoint_threads_first_final_max_delta: ${m1_checkpoint_first_threads:-unknown}/${m1_checkpoint_final_threads:-unknown}/${m1_checkpoint_max_threads:-unknown}/${m1_checkpoint_threads_delta:-unknown}
- m1_checkpoint_ownership_failures: ${m1_checkpoint_ownership_failures:-0}
- m1_checkpoint_evidence: $m1_checkpoint_evidence
- m1_sample_coverage_evidence: $m1_sample_coverage_evidence
- m1_remote_write_evidence: $m1_remote_write_evidence
- m1_slo_evidence: $m1_slo_evidence
- m1_diagnostic_violation_count: ${m1_diagnostic_violation_count:-0}
- m1_diagnostic_invalid_violations: ${m1_diagnostic_invalid_violations:-0}
- m1_diagnostic_safety_evidence: $m1_diagnostic_safety_evidence
- process_numeric_samples: ${process_numeric_samples:-0}
- rss_kib_first_last_max_delta: ${rss_first:-unknown}/${rss_last:-unknown}/${rss_max:-unknown}/${rss_delta:-unknown}
- fd_first_last_max_delta: ${fd_first:-unknown}/${fd_last:-unknown}/${fd_max:-unknown}/${fd_delta:-unknown}
- threads_first_last_max_delta: ${threads_first:-unknown}/${threads_last:-unknown}/${threads_max:-unknown}/${threads_delta:-unknown}
- interface_numeric_samples: ${interface_numeric_samples:-0}
- utun_ipkts_first_last_delta: ${ipkts_first:-unknown}/${ipkts_last:-unknown}/${ipkts_delta:-unknown}
- utun_ibytes_first_last_delta: ${ibytes_first:-unknown}/${ibytes_last:-unknown}/${ibytes_delta:-unknown}
- utun_opkts_first_last_delta: ${opkts_first:-unknown}/${opkts_last:-unknown}/${opkts_delta:-unknown}
- utun_obytes_first_last_delta: ${obytes_first:-unknown}/${obytes_last:-unknown}/${obytes_delta:-unknown}
- endpoint_conservation_max_bytes: ${endpoint_conservation_max:-unknown}
- endpoint_last_available_live_outstanding: ${endpoint_last_available:-unknown}/${endpoint_last_live:-unknown}/${endpoint_last_outstanding:-unknown}
- endpoint_max_live_outstanding: ${endpoint_max_live:-unknown}/${endpoint_max_outstanding:-unknown}
- process_samples: ${process_samples_total:-0}
- endpoint_samples: ${endpoint_samples_count:-0}
- stop_cleanup_complete: ${cleanup_complete:-0}
- final_process_sample_state: ${process_last_state:-unknown}
- final_interface_sample_mtu: ${interface_last_mtu:-unknown}
- cleanup_evidence: $cleanup_evidence
- data_plane_samples: $(grep -c '📊 数据面' "$log_file" 2>/dev/null || true)
- events: $(awk 'END {print (NR > 0 ? NR - 1 : 0)}' "$run_dir/events.tsv" 2>/dev/null)

This summary is a discriminator, not a release verdict. Review direct/control
baselines, resource slopes, fault-window event markers, QUIC deltas, TUN
counters, close-tail ownership, and cleanup together.
EOF_SUMMARY
  append_m2_summary "$run_dir"
  append_m2_qualification_summary "$run_dir"
}

bundle_is_finalized() {
  local run_dir="$1"
  local bundle="${run_dir}.tar.gz"
  local checksum_file="${bundle}.sha256"
  local expected actual lines
  [[ -f "$bundle" && ! -L "$bundle" && -f "$checksum_file" && ! -L "$checksum_file" ]] || \
    return 1
  lines="$(awk 'END {print NR + 0}' "$checksum_file" 2>/dev/null)"
  [[ "$lines" == "1" ]] || return 1
  expected="$(awk 'NR == 1 {print $1}' "$checksum_file" 2>/dev/null)"
  [[ "$expected" =~ ^[0-9a-fA-F]{64}$ ]] || return 1
  actual="$(sha256_file "$bundle" 2>/dev/null)" || return 1
  [[ "$actual" == "$expected" ]]
}

owned_routes_are_restored() {
  local utun target exit_host dns_target
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  dns_target="$(read_state dns_target 2>/dev/null || true)"
  [[ -n "$utun" && -n "$target" && -n "$exit_host" ]] || return 1
  [[ "$(route_interface "$target")" != "$utun" ]] || return 1
  [[ "$(route_interface "$exit_host")" != "$utun" ]] || return 1
  [[ -z "$dns_target" || "$(route_interface "$dns_target")" != "$utun" ]] || \
    return 1
  if [[ -n "$(read_state m2.full_tunnel 2>/dev/null || true)" ]]; then
    [[ "$(read_state m2.full_tunnel 2>/dev/null || true)" == "inactive" ]] && \
      m2_full_tunnel_is_restored
  fi
}

owned_tun_is_unavailable() {
  local utun attempt
  utun="$(read_state utun 2>/dev/null || true)"
  [[ -n "$utun" ]] || return 1
  for ((attempt = 0; attempt < 5; attempt++)); do
    if ! ifconfig "$utun" >/dev/null 2>&1; then
      return 0
    fi
    ((attempt + 1 < 5)) && sleep 1
  done
  return 1
}

create_bundle_once() {
  local run_dir="$1"
  local bundle="$2"
  local checksum_file="${bundle}.sha256"
  local tmp_bundle="${bundle}.tmp.$$"
  local tmp_checksum="${checksum_file}.tmp.$$"
  local checksum
  [[ "$bundle" == "${run_dir}.tar.gz" ]] || return 1
  if bundle_is_finalized "$run_dir"; then
    return 0
  fi
  if [[ -f "$bundle" && ! -L "$bundle" && ! -e "$checksum_file" ]]; then
    [[ ! -e "$tmp_checksum" ]] || return 1
    checksum="$(sha256_file "$bundle")" || return 1
    if ! printf '%s  %s\n' "$checksum" "$bundle" >"$tmp_checksum"; then
      rm -f "$tmp_checksum"
      return 1
    fi
    if ! mv "$tmp_checksum" "$checksum_file"; then
      rm -f "$tmp_checksum"
      return 1
    fi
    bundle_is_finalized "$run_dir"
    return
  fi
  [[ ! -e "$bundle" && ! -e "$checksum_file" && \
    ! -e "$tmp_bundle" && ! -e "$tmp_checksum" ]] || return 1
  if ! tar -C "$(dirname "$run_dir")" -czf "$tmp_bundle" "$(basename "$run_dir")"; then
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  fi
  checksum="$(sha256_file "$tmp_bundle")" || {
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  }
  if ! printf '%s  %s\n' "$checksum" "$bundle" >"$tmp_checksum"; then
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  fi
  if ! mv "$tmp_bundle" "$bundle"; then
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  fi
  if ! mv "$tmp_checksum" "$checksum_file"; then
    rm -f "$tmp_checksum"
    return 1
  fi
  bundle_is_finalized "$run_dir"
}

bundle_action() {
  local run_dir owner_uid owner_gid bundle recovering=0
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  bundle="${run_dir}.tar.gz"
  if bundle_is_finalized "$run_dir"; then
    echo "PASS: evidence bundle was already finalized; no files changed"
    echo "bundle=$bundle"
    cat "${bundle}.sha256" || die "cannot read finalized bundle checksum"
    return 0
  fi
  if [[ -f "$bundle" && ! -L "$bundle" && ! -e "${bundle}.sha256" ]]; then
    recovering=1
  else
    [[ ! -e "$bundle" && ! -e "${bundle}.sha256" ]] || \
      die "refuse to overwrite an existing invalid or partial evidence bundle"
  fi
  active_pid && die "refuse a mutable bundle while mini_vpn is running; use snapshot or stop"
  watchdog_matches_run && die "refuse a mutable bundle while the watchdog is running; use stop"
  workload_matches_run && die "refuse a mutable bundle while a soak workload is running; use stop"
  owned_routes_are_restored || \
    die "refuse to finalize evidence while an owned target/Exit/DNS route still points to the run utun"
  if ((recovering == 0)); then
    write_summary "$run_dir"
    secret_scan "$run_dir" || die "secret scan failed; inspect $run_dir/secret-scan.txt locally"
  fi
  create_bundle_once "$run_dir" "$bundle" || die "one-shot evidence archive publication failed"
  owner_uid="$(read_state owner_uid 2>/dev/null || echo "${SUDO_UID:-0}")"
  owner_gid="$(read_state owner_gid 2>/dev/null || echo "${SUDO_GID:-0}")"
  if [[ "$owner_uid" =~ ^[0-9]+$ && "$owner_gid" =~ ^[0-9]+$ ]]; then
    chown -R "$owner_uid:$owner_gid" "$run_dir" "$bundle" "${bundle}.sha256" 2>/dev/null || \
      warn "could not return evidence ownership to uid=$owner_uid gid=$owner_gid"
  else
    warn "invalid evidence owner state; leaving root ownership unchanged"
  fi
  if ((recovering == 1)); then
    echo "PASS: finalized checksum for the existing immutable evidence archive"
  else
    echo "PASS: evidence bundle created"
  fi
  echo "bundle=$bundle"
  cat "${bundle}.sha256" || die "cannot read bundle checksum"
}

stop_runner() {
  local run_dir bundle
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  bundle="${run_dir}.tar.gz"
  if bundle_is_finalized "$run_dir"; then
    echo "PASS: Knife15 run was already stopped and evidence is immutable"
    echo "bundle=$bundle"
    cat "${bundle}.sha256" || die "cannot read finalized bundle checksum"
    return 0
  fi
  if [[ -f "$bundle" && ! -L "$bundle" && ! -e "${bundle}.sha256" ]]; then
    bundle_action
    return 0
  fi
  [[ ! -e "$bundle" && ! -e "${bundle}.sha256" ]] || \
    die "existing evidence bundle is invalid or partial; refuse repeated stop mutation"
  append_event_to "$run_dir" "stop requested"
  terminate_recorded_workload "$run_dir" || \
    die "soak workload identity/termination check failed; refusing process and route cleanup"
  terminate_recorded_pid "$run_dir"
  terminate_recorded_watchdog "$run_dir"
  cleanup_owned_routes "$run_dir" || \
    die "owned route or DNS cleanup failed; inspect status/snapshot before retrying stop"
  active_pid && die "mini_vpn still matches the recorded process after cleanup"
  owned_tun_is_unavailable || \
    die "the owned utun is still available after the mini_vpn process stopped"
  owned_routes_are_restored || \
    die "an owned target/Exit/DNS route still points to the run utun after cleanup"
  sample_once_for "$run_dir" || die "cannot record the final cleanup sample"
  append_event_to "$run_dir" "stop cleanup complete"
  write_state inactive "$(timestamp)"
  bundle_action
}

case "$ACTION" in
  --help|-h|help)
    usage
    ;;
  --self-test)
    runner_self_test
    ;;
  preflight)
    common_preflight
    print_preflight
    ;;
  baseline)
    run_baseline
    ;;
  baseline-check)
    run_baseline_check
    ;;
  direct-discriminator)
    run_direct_discriminator
    ;;
  m2-ipv6-check)
    run_m2_ipv6_check
    ;;
  start)
    start_runner
    ;;
  status)
    show_status
    ;;
  event)
    require_root
    [[ -n "${2:-}" ]] || die "event requires a label"
    append_event "$2"
    echo "PASS: event recorded"
    ;;
  snapshot)
    snapshot_action
    ;;
  smoke)
    run_smoke
    ;;
  m0)
    run_m0_action
    ;;
  m1)
    run_m1_action formal
    ;;
  m1-diagnostic)
    run_m1_action diagnostic
    ;;
  m2-qualification)
    run_m2_action qualification
    ;;
  m2)
    run_m2_action formal
    ;;
  stop)
    stop_runner
    ;;
  bundle)
    bundle_action
    ;;
  __watchdog)
    require_root
    watchdog_loop
    ;;
  *)
    usage >&2
    exit 64
    ;;
esac
