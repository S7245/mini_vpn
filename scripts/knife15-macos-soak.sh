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
# iperf only reports completed application buffers. Shenzhen's 0.131 Mbit/s
# reverse path delivered about 16KiB/s with an 8.3KiB cwnd, so a 1KiB observer
# preserves multiple visible blocks per second while forward stays unchanged.
TCP_REVERSE_IPERF_LENGTH_BYTES=1024
M0_IPERF3_BIN=iperf3
M0_DIG_BIN=dig
M0_SLEEP_BIN=sleep
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
  bash scripts/knife15-macos-soak.sh baseline
  bash scripts/knife15-macos-soak.sh direct-discriminator
  sudo -E bash scripts/knife15-macos-soak.sh start
  sudo -E bash scripts/knife15-macos-soak.sh status
  sudo -E bash scripts/knife15-macos-soak.sh event "label"
  sudo -E bash scripts/knife15-macos-soak.sh snapshot
  sudo -E bash scripts/knife15-macos-soak.sh smoke
  sudo -E bash scripts/knife15-macos-soak.sh m0
  sudo -E bash scripts/knife15-macos-soak.sh m1
  sudo -E bash scripts/knife15-macos-soak.sh m1-diagnostic
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
  1. unset M0_BASELINE_DIR M0_DIRECT_DIR M1_BASELINE_DIR M1_DIRECT_DIR
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

Never run m0, m1, and m1-diagnostic in one TUN run. Both M1 actions have a
28,800-second traffic/drain budget and normally take slightly more than eight
wall hours.
Use m1-diagnostic only when a complete longitudinal artifact is required:
data-quality violations are recorded and continued, safety failures still
stop immediately, and the result can never satisfy formal M1 acceptance.

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

selected_direct_baseline_dir() {
  if [[ -n "$M0_BASELINE_DIR" && -n "$M1_BASELINE_DIR" ]]; then
    return 1
  fi
  if [[ -n "$M1_BASELINE_DIR" ]]; then
    printf '%s\n' "$M1_BASELINE_DIR"
  elif [[ -n "$M0_BASELINE_DIR" ]]; then
    printf '%s\n' "$M0_BASELINE_DIR"
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

route_interface() {
  route -n get "$1" 2>/dev/null | route_interface_from_text
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
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
    "$command_text" == *"knife15-macos-soak.sh m1-diagnostic" ]]
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

common_preflight() {
  local server target_if exit_if

  [[ "$(uname -s)" == "Darwin" ]] || die "Knife15 macOS runner requires Darwin"
  for command_name in bash route ifconfig netstat ping ps lsof shasum tar awk sed grep sort comm; do
    require_command "$command_name"
  done
  [[ -x "$BIN" ]] || die "release binary not found: $BIN (run cargo build --release first)"

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
      if (physical_valid && ($20 + 0 > 0 || $23 + 0 > 0)) {
        physical_error_samples++
      }
      if (physical_valid) {
        if (!physical_samples) {
          ibytes_first = $21 + 0
          obytes_first = $24 + 0
        }
        ibytes_last = $21 + 0
        obytes_last = $24 + 0
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

validate_m0_baseline_file() {
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
  ' "$json_file" >/dev/null 2>&1
}

validate_m0_baseline_pair() {
  local baseline_dir="$1"
  local target="$2"
  validate_m0_baseline_file "$baseline_dir/direct-forward.json" "$target" 0 &&
    validate_m0_baseline_file "$baseline_dir/direct-reverse.json" "$target" 1
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

runner_self_test() {
  local tmp good_log bad_log route_fixture interface_fixture ping_fixture network_fixture collector_dir collector_bin original_path original_state_dir clean_scan secret_scan_dir secret_value summary_dir baseline_dir baseline_summary_text m0_profile m1_profile m1_test_profile m0_run m1_stage_run m1_run m1_diagnostic_run m1_diagnostic_fail_run m1_diagnostic_formal_run m1_checkpoint_file m1_capture_run m1_formal_run m1_tcp_fixture m1_udp_fixture m0_fail_run direct_dir fake_iperf fake_dig fake_sleep usage_text dns_result unrelated_pid target_ready_json finalized_run finalized_bundle finalized_hash bounded_status result_index result_label sample_index violations_before violation_count invalid_violations
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/knife15-macos-self-test.XXXXXX")" || return 1
  good_log="$tmp/good.log"
  bad_log="$tmp/bad.log"
  route_fixture="$tmp/route.txt"
  interface_fixture="$tmp/interface.txt"

  validate_ipv4 43.130.32.77 || die "self-test: valid IPv4 rejected"
  ! validate_ipv4 300.1.1.1 || die "self-test: invalid IPv4 accepted"
  validate_uuid 123e4567-e89b-12d3-a456-426614174000 || die "self-test: valid UUID rejected"
  ! validate_uuid not-a-uuid || die "self-test: invalid UUID accepted"
  validate_dns_name example.com || die "self-test: valid DNS name rejected"
  ! validate_dns_name $'bad\nname' || die "self-test: invalid DNS name accepted"
  validate_run_dir_path /tmp/mini_vpn_knife15_macos_20260714_010203 || \
    die "self-test: valid run directory rejected"
  ! validate_run_dir_path /tmp/other || die "self-test: unrelated run directory accepted"
  ! validate_run_dir_path /tmp/mini_vpn_knife15_macos_../victim || \
    die "self-test: traversal run directory accepted"
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
  M0_BASELINE_DIR=/tmp/mini_vpn_knife15_macos_baseline_m0
  ! selected_direct_baseline_dir >/dev/null || \
    die "self-test: ambiguous M0/M1 direct baseline selection was accepted"
  M0_BASELINE_DIR=
  M1_BASELINE_DIR=
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
  grep -Fq 'scripts/knife15-macos-soak.sh m0' <<<"$usage_text" || \
    die "self-test: public M0 action missing from help"
  grep -Fq 'scripts/knife15-macos-soak.sh m1' <<<"$usage_text" || \
    die "self-test: public M1 action missing from help"
  grep -Fq 'M0_BASELINE_DIR=' <<<"$usage_text" || \
    die "self-test: M0 baseline requirement missing from help"
  grep -Fq 'M0_DIRECT_DIR=' <<<"$usage_text" || \
    die "self-test: M0 direct continuity requirement missing from help"
  grep -Fq 'M1_BASELINE_DIR=' <<<"$usage_text" || \
    die "self-test: M1 baseline requirement missing from help"
  grep -Fq 'M1_DIRECT_DIR=' <<<"$usage_text" || \
    die "self-test: M1 direct continuity requirement missing from help"

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
owner_uid=$owner_uid
owner_gid=$owner_gid
EOF_MANIFEST
}

cleanup_owned_routes() {
  local run_dir="$1"
  local utun target dns_target current
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
  cleanup_owned_routes "$run_dir"
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
  write_state target "$TARGET"
  write_state dns_target "$DNS_TARGET"
  write_state exit_host "$SERVER_HOST"
  write_state bin "$BIN"
  write_state iperf_port "$IPERF_PORT"
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
  local out_dir
  common_preflight
  require_command iperf3
  [[ "$(route_interface "$TARGET")" != utun* ]] || die "baseline requires TARGET outside utun"
  out_dir="${BASELINE_OUT_DIR:-/tmp/mini_vpn_knife15_macos_baseline_$(date -u '+%Y%m%d_%H%M%S')}"
  mkdir -p "$out_dir" || die "cannot create baseline output directory"
  echo "Running direct forward baseline..."
  if ! run_direct_baseline_probe "$out_dir/direct-forward.json" "$TARGET" \
    "$IPERF_PORT" "$DURATION" "$PARALLEL" 0 iperf3; then
    die "direct forward baseline failed; evidence: $out_dir/direct-forward.json"
  fi
  echo "Running direct reverse baseline..."
  if ! run_direct_baseline_probe "$out_dir/direct-reverse.json" "$TARGET" \
    "$IPERF_PORT" "$DURATION" "$PARALLEL" 1 iperf3; then
    die "direct reverse baseline failed; evidence: $out_dir/direct-reverse.json"
  fi
  baseline_receiver_summary "$out_dir" || \
    warn "direct baseline completed but its receiver speed summary is unavailable"
  validate_m0_baseline_pair "$out_dir" "$TARGET" || \
    die "direct baseline receiver continuity/evidence failed; evidence: $out_dir"
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
    die "set exactly one of M0_BASELINE_DIR or M1_BASELINE_DIR to the fresh baseline directory"
  if [[ "$baseline_dir" == "$M1_BASELINE_DIR" ]]; then
    [[ "$M1_TCP_SECS" == "300" ]] || \
      die "direct discriminator requires the frozen 300s M1 TCP epoch; unset M1_TCP_SECS"
  else
    [[ "$M0_TCP_SECS" == "300" ]] || \
      die "direct discriminator requires the frozen 300s M0 TCP epoch; unset M0_TCP_SECS"
  fi
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
  if [[ "$udp" == "1" && "${SOAK_CONTINUE_DATA_QUALITY:-0}" == "1" ]]; then
    udp_loss="$(udp_loss_percent "$output_file")" || return 1
    if ! decimal_le "$udp_loss" 3.0; then
      record_soak_data_quality_violation "$run_dir" "$cycle" "$phase" \
        udp_loss_percent "$udp_loss" limit=3.0 "$output_file" || return 1
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
    ! -e "$run_dir/m1-workload.txt" && ! -e "$run_dir/m1" ]] || \
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
    ! -e "$run_dir/m1-workload.txt" && ! -e "$run_dir/m1" ]] || \
    die "this TUN run already has M0 or M1 evidence; stop and start a fresh run"
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
      grep -Eq 'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|send_slice_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|queue_(queued|leased|reserved)=[1-9][0-9]*|reason=stalled_write_timeout|reason=idle_timeout' "$log_file"; then
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
      grep -Eq 'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|send_slice_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*|queue_(queued|leased|reserved)=[1-9][0-9]*|reason=stalled_write_timeout|reason=idle_timeout' "$log_file"; then
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
    { [[ "$interface_error_samples" =~ ^[0-9]+$ ]] && \
    ((10#$interface_error_samples > 0)); } || \
    { [[ "$physical_interface_error_samples" =~ ^[0-9]+$ ]] && \
    ((10#$physical_interface_error_samples > 0)); } || \
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
  [[ -z "$dns_target" || "$(route_interface "$dns_target")" != "$utun" ]]
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
  cleanup_owned_routes "$run_dir"
  terminate_recorded_watchdog "$run_dir"
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
  direct-discriminator)
    run_direct_discriminator
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
