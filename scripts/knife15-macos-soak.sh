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
MAX_LOG_BYTES="${MAX_LOG_BYTES:-134217728}"
LOG_KEEP_BYTES="${LOG_KEEP_BYTES:-67108864}"
MIN_FREE_KB="${MIN_FREE_KB:-1048576}"
STATE_DIR="/var/run/mini_vpn_knife15_macos_state"
SERVER_HOST=""
SERVER_PORT=""

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
  sudo -E bash scripts/knife15-macos-soak.sh start
  sudo -E bash scripts/knife15-macos-soak.sh status
  sudo -E bash scripts/knife15-macos-soak.sh event "label"
  sudo -E bash scripts/knife15-macos-soak.sh snapshot
  sudo -E bash scripts/knife15-macos-soak.sh smoke
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

Workflow:
  1. cargo build --release
  2. export the five MINI_VPN_TUIC_* values locally
  3. bash scripts/knife15-macos-soak.sh --self-test
  4. bash scripts/knife15-macos-soak.sh preflight
  5. bash scripts/knife15-macos-soak.sh baseline
  6. sudo -v
  7. sudo -E bash scripts/knife15-macos-soak.sh start
  8. sudo -E bash scripts/knife15-macos-soak.sh smoke
  9. sudo -E bash scripts/knife15-macos-soak.sh status
 10. sudo -E bash scripts/knife15-macos-soak.sh stop

The background watchdog samples process/utun state and removes only the host
routes owned by this run if mini_vpn exits. stop is idempotent and creates a
sanitized evidence bundle.
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
  append_event_to "$run_dir" "$1"
}

common_preflight() {
  local server target_if exit_if

  [[ "$(uname -s)" == "Darwin" ]] || die "Knife15 macOS runner requires Darwin"
  for command_name in bash route ifconfig netstat ps lsof shasum tar awk sed grep sort comm; do
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

  if [[ -d "$STATE_DIR" ]] && { active_pid || watchdog_matches_run; }; then
    die "an active Knife15 process/watchdog already exists; use status or stop"
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
  awk -v now="$now" -v interface="$interface" '
    $1 == interface && $3 ~ /^<Link#/ {
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", now, $1, $2, $4, $5, $6, $7, $8, $9, $10
      found = 1
      exit
    }
    END { if (!found) exit 1 }
  '
}

runner_self_test() {
  local tmp good_log bad_log route_fixture interface_fixture clean_scan secret_scan_dir secret_value
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
  watchdog_command_matches "bash $SCRIPT_PATH __watchdog" || \
    die "self-test: watchdog command rejected"
  ! watchdog_command_matches '/usr/bin/sleep 30' || \
    die "self-test: unrelated watchdog command accepted"

  printf '   interface: en0\n' >"$route_fixture"
  [[ "$(route_interface_from_text <"$route_fixture")" == "en0" ]] || \
    die "self-test: route interface parser mismatch"
  cat >"$interface_fixture" <<'EOF_INTERFACE'
Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll
utun42     1200  <Link#23>                         100     0      64000       90     0      60000     0
EOF_INTERFACE
  [[ "$(interface_csv_from_text 2026-07-14T00:00:00Z utun42 <"$interface_fixture")" == \
    "2026-07-14T00:00:00Z,utun42,1200,100,0,64000,90,0,60000,0" ]] || \
    die "self-test: interface counter parser mismatch"

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
exit_host=$SERVER_HOST
exit_port=$SERVER_PORT
exit_route_before=$(route_interface "$SERVER_HOST")
binary_path=$BIN
runner_path=$SCRIPT_PATH
dns_target=${DNS_TARGET:-disabled}
dns_name=$DNS_NAME
iperf_port=$IPERF_PORT
smoke_duration_secs=$DURATION
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
metrics_secs=$METRICS_SECS
sample_secs=$SAMPLE_SECS
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
  printf '%s,target_if=%s,exit_if=%s\n' "$now" \
    "$(route_interface "$(read_state target 2>/dev/null || echo "$TARGET")")" \
    "$(route_interface "$(read_state exit_host 2>/dev/null || echo "$SERVER_HOST")")" \
    >>"$run_dir/network.csv"

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
  cleanup_owned_routes "$run_dir"
  sample_once_for "$run_dir"
  write_state inactive "$(timestamp)"
}

start_runner() {
  local ts run_dir before_file after_file log_file vpn_pid ready=0 utun owner_uid owner_gid i
  require_root
  common_preflight

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
  printf 'timestamp,target_route,exit_route\n' >"$run_dir/network.csv"
  printf 'timestamp\tevent\n' >"$run_dir/events.tsv"

  write_state run_dir "$run_dir"
  write_state target "$TARGET"
  write_state dns_target "$DNS_TARGET"
  write_state exit_host "$SERVER_HOST"
  write_state bin "$BIN"
  write_state iperf_port "$IPERF_PORT"
  write_state duration "$DURATION"
  write_state parallel "$PARALLEL"
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

run_baseline() {
  local out_dir
  common_preflight
  require_command iperf3
  [[ "$(route_interface "$TARGET")" != utun* ]] || die "baseline requires TARGET outside utun"
  out_dir="${BASELINE_OUT_DIR:-/tmp/mini_vpn_knife15_macos_baseline_$(date -u '+%Y%m%d_%H%M%S')}"
  mkdir -p "$out_dir" || die "cannot create baseline output directory"
  echo "Running direct forward baseline..."
  if ! run_logged "$out_dir/direct-forward.json" \
    iperf3 -c "$TARGET" -p "$IPERF_PORT" -t "$DURATION" -P "$PARALLEL" --json; then
    die "direct forward baseline failed; evidence: $out_dir/direct-forward.json"
  fi
  echo "Running direct reverse baseline..."
  if ! run_logged "$out_dir/direct-reverse.json" \
    iperf3 -c "$TARGET" -p "$IPERF_PORT" -t "$DURATION" -P "$PARALLEL" -R --json; then
    die "direct reverse baseline failed; evidence: $out_dir/direct-reverse.json"
  fi
  echo "PASS: direct baseline complete: $out_dir"
}

run_logged() {
  local output_file="$1"
  local statuses
  shift
  "$@" | tee "$output_file"
  statuses=("${PIPESTATUS[@]}")
  ((statuses[0] == 0 && statuses[1] == 0))
}

run_smoke() {
  local run_dir utun target exit_host smoke_dir iperf_port duration parallel dns_name dns_target
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
  [[ "$(route_interface "$target")" == "$utun" ]] || die "TARGET no longer routes through $utun"
  [[ "$(route_interface "$exit_host")" != "$utun" ]] || die "Exit route recursed into $utun"
  require_command iperf3
  smoke_dir="$run_dir/smoke_$(date -u '+%Y%m%d_%H%M%S')"
  mkdir -p "$smoke_dir"
  append_event_to "$run_dir" "smoke start"
  if ! run_logged "$smoke_dir/tunnel-forward.json" \
    iperf3 -c "$target" -p "$iperf_port" -t "$duration" -P "$parallel" --json; then
    append_event_to "$run_dir" "smoke forward failed"
    die "tunnel forward smoke failed; leave TUN running for status/stop"
  fi
  if ! run_logged "$smoke_dir/tunnel-reverse.json" \
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
  sample_once_for "$run_dir"
  append_event_to "$run_dir" "smoke complete"
  echo "PASS: target-only smoke completed: $smoke_dir"
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
  echo "target_if=$(route_interface "$target") expected=$utun"
  echo "exit_if=$(route_interface "$exit_host") must_not_equal=$utun"
  tail -n 3 "$run_dir/process.csv" 2>/dev/null || true
  grep -E '📊 数据面|🔬 主循环|📊 TUIC QUIC stats|📊 TUIC endpoint pacing global|tcp-tun-rx-drain|tcp-handle-close|tcp-d16-relay-close' \
    "$run_dir/mini_vpn.log" 2>/dev/null | tail -n 20 || true
}

snapshot_action() {
  local run_dir
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
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
  local conservation verdict
  if conservation_check_file "$log_file"; then
    conservation=PASS
  else
    conservation=FAIL_OR_MISSING
  fi
  if grep -Eq 'pump_full_waits=[1-9][0-9]*|pump_read_errors=[1-9][0-9]*|tun_flush_tx_failures=[1-9][0-9]*|terminal_pending_reap_bytes=[1-9][0-9]*' "$log_file"; then
    verdict=REVIEW
  else
    verdict=NO_KNOWN_INTERNAL_FAILURE_SIGNAL
  fi
  cat >"$run_dir/summary.md" <<EOF_SUMMARY || die "cannot write run summary"
# Knife15 macOS Run Summary

- generated_utc: $(timestamp)
- endpoint_conservation: $conservation
- internal_failure_scan: $verdict
- process_samples: $(awk 'END {print NR>0?NR-1:0}' "$run_dir/process.csv" 2>/dev/null)
- endpoint_samples: $(grep -c '📊 TUIC endpoint pacing global' "$log_file" 2>/dev/null || true)
- data_plane_samples: $(grep -c '📊 数据面' "$log_file" 2>/dev/null || true)
- events: $(awk 'END {print NR>0?NR-1:0}' "$run_dir/events.tsv" 2>/dev/null)

This summary is a discriminator, not a release verdict. Review direct/control
baselines, resource slopes, fault-window event markers, QUIC deltas, TUN
counters, close-tail ownership, and cleanup together.
EOF_SUMMARY
}

bundle_action() {
  local run_dir owner_uid owner_gid bundle
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  active_pid && die "refuse a mutable bundle while mini_vpn is running; use snapshot or stop"
  watchdog_matches_run && die "refuse a mutable bundle while the watchdog is running; use stop"
  write_summary "$run_dir"
  secret_scan "$run_dir" || die "secret scan failed; inspect $run_dir/secret-scan.txt locally"
  bundle="${run_dir}.tar.gz"
  tar -C "$(dirname "$run_dir")" -czf "$bundle" "$(basename "$run_dir")" || \
    die "evidence archive creation failed"
  shasum -a 256 "$bundle" >"${bundle}.sha256" || die "bundle checksum failed"
  owner_uid="$(read_state owner_uid 2>/dev/null || echo "${SUDO_UID:-0}")"
  owner_gid="$(read_state owner_gid 2>/dev/null || echo "${SUDO_GID:-0}")"
  if [[ "$owner_uid" =~ ^[0-9]+$ && "$owner_gid" =~ ^[0-9]+$ ]]; then
    chown -R "$owner_uid:$owner_gid" "$run_dir" "$bundle" "${bundle}.sha256" 2>/dev/null || \
      warn "could not return evidence ownership to uid=$owner_uid gid=$owner_gid"
  else
    warn "invalid evidence owner state; leaving root ownership unchanged"
  fi
  echo "PASS: evidence bundle created"
  echo "bundle=$bundle"
  cat "${bundle}.sha256" || die "cannot read bundle checksum"
}

stop_runner() {
  local run_dir
  require_root
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  append_event_to "$run_dir" "stop requested"
  terminate_recorded_pid "$run_dir"
  cleanup_owned_routes "$run_dir"
  terminate_recorded_watchdog "$run_dir"
  sample_once_for "$run_dir"
  append_event_to "$run_dir" "stop cleanup complete"
  if [[ "$(route_interface "$(read_state target)")" == "$(read_state utun)" ]]; then
    die "target route still points to the run utun after cleanup"
  fi
  if [[ "$(route_interface "$(read_state exit_host)")" == "$(read_state utun)" ]]; then
    die "Exit route points to the run utun after cleanup"
  fi
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
