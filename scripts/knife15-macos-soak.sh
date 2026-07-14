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
M0_TOTAL_SECS="${M0_TOTAL_SECS:-7200}"
M0_TCP_SECS="${M0_TCP_SECS:-300}"
M0_UDP_SECS="${M0_UDP_SECS:-180}"
M0_SHORT_SECS="${M0_SHORT_SECS:-10}"
M0_SHORT_COUNT="${M0_SHORT_COUNT:-6}"
M0_IDLE_SECS="${M0_IDLE_SECS:-300}"
M0_FINAL_DRAIN_SECS="${M0_FINAL_DRAIN_SECS:-120}"
M0_IPERF3_BIN=iperf3
M0_DIG_BIN=dig
M0_SLEEP_BIN=sleep
STATE_DIR="/var/run/mini_vpn_knife15_macos_state"
SERVER_HOST=""
SERVER_PORT=""
TARGET_READY_UTC=""
TARGET_READY_RECEIVER_BPS=""

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
  sudo -E bash scripts/knife15-macos-soak.sh m0
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

Workflow:
  1. cargo build --release
  2. export the five MINI_VPN_TUIC_* values locally
  3. bash scripts/knife15-macos-soak.sh --self-test
  4. bash scripts/knife15-macos-soak.sh preflight
  5. bash scripts/knife15-macos-soak.sh baseline
  6. sudo -v
  7. sudo -E bash scripts/knife15-macos-soak.sh start
  8. sudo -E bash scripts/knife15-macos-soak.sh smoke
  9. export M0_BASELINE_DIR='REPLACE_WITH_BASELINE_DIRECTORY_FROM_STEP_5'
 10. sudo -E bash scripts/knife15-macos-soak.sh m0
 11. sudo -E bash scripts/knife15-macos-soak.sh status
 12. sudo -E bash scripts/knife15-macos-soak.sh stop

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

validate_m0_formal_config() {
  [[ "$M0_TOTAL_SECS" == "7200" && "$M0_TCP_SECS" == "300" && \
    "$M0_UDP_SECS" == "180" && "$M0_SHORT_SECS" == "10" && \
    "$M0_SHORT_COUNT" == "6" && "$M0_IDLE_SECS" == "300" && \
    "$M0_FINAL_DRAIN_SECS" == "120" ]]
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

workload_command_matches() {
  local command_text="$1"
  [[ "$command_text" == *"knife15-macos-soak.sh m0" ]]
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
  awk -v now="$now" -v interface="$interface" '
    $1 == interface && $3 ~ /^<Link#/ {
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", now, $1, $2, $4, $5, $6, $7, $8, $9, $10
      found = 1
      exit
    }
    END { if (!found) exit 1 }
  '
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

m0_result_envelope() {
  local m0_dir="$1"
  local result_file
  if [[ ! -d "$m0_dir" ]]; then
    echo "0 0 unknown unknown 0"
    return 0
  fi
  {
    for result_file in "$m0_dir"/*.json; do
      [[ -f "$result_file" ]] || continue
      if ! jq -er '
        .end.sum_sent.bytes as $sent
        | .end.sum_received.bytes as $received
        | ($sent - $received | if . < 0 then -. else . end) as $gap
        | [
            .start.test_start.protocol,
            $gap,
            (.end.sum.lost_percent // .end.sum_received.lost_percent // -1)
          ]
        | @tsv
      ' "$result_file" 2>/dev/null; then
        echo $'INVALID\t0\t-1'
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
    $1 == "INVALID" { invalid++ }
    END {
      tcp_gap = tcp > 0 ? max_tcp_gap : "unknown"
      udp_loss = have_udp_loss ? sprintf("%.6f", max_udp_loss) : "unknown"
      printf "%d %d %s %s %d\n", tcp + 0, udp + 0, tcp_gap, udp_loss, invalid + 0
    }
  '
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
    .end.sum_received.bits_per_second
    | if type == "number" and . > 0 then floor else error("invalid receiver rate") end
  ' "$json_file" 2>/dev/null
}

validate_m0_baseline_file() {
  local json_file="$1"
  local target="$2"
  local reverse="$3"
  [[ -f "$json_file" && ! -L "$json_file" ]] || return 1
  jq -e --arg target "$target" --argjson reverse "$reverse" '
    ((.error? // "") == "")
    and (.start.connecting_to.host == $target)
    and (.start.test_start.protocol == "TCP")
    and (.start.test_start.reverse == $reverse)
    and ((.intervals | type) == "array" and (.intervals | length) > 0)
    and all(.intervals[];
      .sum.bits_per_second as $bps
      | (($bps | type) == "number" and $bps > 0))
    and (.end.sum_received.bits_per_second as $bps
      | (($bps | type) == "number" and $bps > 0))
  ' "$json_file" >/dev/null 2>&1
}

validate_m0_baseline_pair() {
  local baseline_dir="$1"
  local target="$2"
  validate_m0_baseline_file "$baseline_dir/direct-forward.json" "$target" 0 &&
    validate_m0_baseline_file "$baseline_dir/direct-reverse.json" "$target" 1
}

validate_m0_iperf_result() {
  local json_file="$1"
  local protocol="$2"
  jq -e --arg protocol "$protocol" '
    ((.error? // "") == "")
    and (.start.test_start.protocol == $protocol)
    and ((.intervals | type) == "array" and (.intervals | length) > 0)
    and all(.intervals[];
      .sum.bits_per_second as $bps
      | (($bps | type) == "number" and $bps > 0))
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
  ' "$json_file" >/dev/null 2>&1
}

validate_target_ready_result() {
  local json_file="$1"
  local target="$2"
  jq -e --arg target "$target" '
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
  ' "$json_file" >/dev/null 2>&1
}

target_ready_probe() {
  local result_file
  require_command iperf3
  require_command jq
  result_file="$(mktemp "${TMPDIR:-/tmp}/mini_vpn_knife15_target_ready.XXXXXX.json")" || \
    die "cannot create Target readiness result file"
  echo "Checking that Target can complete a fresh direct iperf3 transaction..."
  if ! run_logged "$result_file" \
    iperf3 -c "$TARGET" -p "$IPERF_PORT" -t 1 -P 1 --connect-timeout 5000 --json; then
    rm -f "$result_file"
    die "Target readiness command failed; wait for $TARGET:$IPERF_PORT to recover before start"
  fi
  if ! validate_target_ready_result "$result_file" "$TARGET"; then
    rm -f "$result_file"
    die "Target is busy or could not complete a positive direct transaction; do not rearm yet"
  fi
  TARGET_READY_UTC="$(timestamp)"
  TARGET_READY_RECEIVER_BPS="$(jq -er '.end.sum_received.bits_per_second | floor' "$result_file")" || {
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

runner_self_test() {
  local tmp good_log bad_log route_fixture interface_fixture clean_scan secret_scan_dir secret_value summary_dir baseline_dir m0_profile m0_run m0_fail_run fake_iperf fake_dig fake_sleep usage_text dns_result unrelated_pid target_ready_json finalized_run finalized_bundle finalized_hash
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
  workload_command_matches 'bash scripts/knife15-macos-soak.sh m0' || \
    die "self-test: M0 workload command rejected"
  ! workload_command_matches 'bash scripts/knife15-macos-soak.sh smoke' || \
    die "self-test: non-M0 workload command accepted"

  baseline_dir="$tmp/baseline"
  mkdir "$baseline_dir"
  printf '%s\n' '{"start":{"connecting_to":{"host":"43.130.32.77"},"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bits_per_second":9044757.9}}}' \
    >"$baseline_dir/direct-forward.json"
  [[ "$(baseline_receiver_bps "$baseline_dir/direct-forward.json")" == "9044757" ]] || \
    die "self-test: baseline receiver rate parser mismatch"
  printf '%s\n' '{"end":{"sum_received":{"bits_per_second":0}}}' \
    >"$baseline_dir/invalid.json"
  ! baseline_receiver_bps "$baseline_dir/invalid.json" >/dev/null 2>&1 || \
    die "self-test: zero baseline receiver rate accepted"
  printf '%s\n' '{"start":{"connecting_to":{"host":"43.130.32.77"},"test_start":{"protocol":"TCP","reverse":1}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bits_per_second":26790141.6}}}' \
    >"$baseline_dir/direct-reverse.json"
  validate_m0_baseline_pair "$baseline_dir" 43.130.32.77 || \
    die "self-test: valid M0 baseline pair rejected"
  ! validate_m0_baseline_pair "$baseline_dir" 43.130.32.78 || \
    die "self-test: M0 baseline for another target accepted"
  target_ready_json="$tmp/target-ready.json"
  printf '%s\n' '{"start":{"connecting_to":{"host":"43.130.32.77"},"test_start":{"protocol":"TCP","reverse":0}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_sent":{"bytes":100},"sum_received":{"bytes":100,"bits_per_second":1}}}' \
    >"$target_ready_json"
  validate_target_ready_result "$target_ready_json" 43.130.32.77 || \
    die "self-test: healthy Target readiness transaction rejected"
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
  grep -Fq 'udp_payload_bytes=1160' "$m0_profile" || \
    die "self-test: M0 UDP payload drifted from the MTU1200-safe shape"

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
protocol=TCP
for arg in "$@"; do
  [[ "$arg" == "-u" ]] && protocol=UDP
done
printf '%s\n' "{\"start\":{\"test_start\":{\"protocol\":\"$protocol\"}},\"intervals\":[{\"sum\":{\"bits_per_second\":1}}],\"end\":{\"sum_sent\":{\"bytes\":100},\"sum_received\":{\"bytes\":90,\"bits_per_second\":1},\"sum\":{\"lost_percent\":1.25}}}"
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
  M0_QUIET=1
  run_m0_schedule "$m0_run" "$m0_profile"
  [[ -f "$m0_run/m0/idle.sleep.log" ]] || \
    die "self-test: M0 idle child was not tracked through the logged runner"
  [[ -f "$m0_run/m0/final-drain.sleep.log" ]] || \
    die "self-test: M0 final-drain child was not tracked through the logged runner"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 2 -P 1 -b 4522378 --json' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: M0 sustained forward TCP command mismatch"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 2 -P 1 -b 13395070 -R --json' \
    "$M0_TEST_COMMAND_LOG" || \
    die "self-test: M0 sustained reverse TCP command mismatch"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 1 -P 1 -b 13395070 -u -l 1160 -R --json' \
    "$M0_TEST_COMMAND_LOG" || die "self-test: M0 reverse UDP command mismatch"
  grep -Fq 'iperf3 -c 43.130.32.77 -p 5201 -t 1 -P 1 -b 7235805 --json' \
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

  printf '%s\n' \
    '{"start":{"test_start":{"protocol":"TCP"}},"intervals":[{"sum":{"bits_per_second":1}}],"end":{"sum_received":{"bits_per_second":1}}}' \
    >"$tmp/m0-positive-rate-missing-bytes.json"
  ! validate_m0_iperf_result "$tmp/m0-positive-rate-missing-bytes.json" TCP || \
    die "self-test: M0 result missing sender/receiver byte evidence was accepted"

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
  usage_text="$(usage)"
  grep -Fq 'scripts/knife15-macos-soak.sh m0' <<<"$usage_text" || \
    die "self-test: public M0 action missing from help"
  grep -Fq 'M0_BASELINE_DIR=' <<<"$usage_text" || \
    die "self-test: M0 baseline requirement missing from help"

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
  printf '%s\n' '写入上游流失败 direction=local_to_remote err=Stopped(0)' \
    >>"$summary_dir/mini_vpn.log"
  write_summary "$summary_dir"
  grep -Fq -- '- internal_failure_scan: REVIEW' "$summary_dir/summary.md" || \
    die "self-test: remote write failure was not marked for review"
  grep -Fq -- '- remote_write_failures: 1' "$summary_dir/summary.md" || \
    die "self-test: remote write failure count mismatch"

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
    warn "M0 workload did not exit after 10s; sending KILL to the identity-verified PID"
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
  printf '%s\n' interrupted >"$run_dir/m0.status"
  append_event_to "$run_dir" "m0 interrupted by $signal_name; TUN left running for evidence"
  clear_workload_state
  echo "ERROR: M0 interrupted by $signal_name; TUN remains running; use status/snapshot/stop" >&2
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
  local child_pid result
  shift
  if [[ "${M0_TRACK_CHILD:-0}" != "1" ]]; then
    "$@" >"$output_file" 2>&1
    return
  fi
  "$@" >"$output_file" 2>&1 &
  child_pid=$!
  write_state workload.child.pid "$child_pid"
  while kill -0 "$child_pid" 2>/dev/null; do
    /bin/sleep 2
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
    append_event_to "$run_dir" "m0 health failed: mini_vpn log history was compacted"
    return 1
  fi
  if ! active_pid; then
    append_event_to "$run_dir" "m0 health failed: mini_vpn not running"
    return 1
  fi
  utun="$(read_state utun 2>/dev/null || true)"
  target="$(read_state target 2>/dev/null || true)"
  exit_host="$(read_state exit_host 2>/dev/null || true)"
  target_if="$(route_interface "$target")"
  exit_if="$(route_interface "$exit_host")"
  if [[ -z "$utun" || "$target_if" != "$utun" ]]; then
    append_event_to "$run_dir" \
      "m0 health failed: target route expected=$utun actual=${target_if:-missing}"
    return 1
  fi
  if [[ -z "$exit_if" || "$exit_if" == "$utun" ]]; then
    append_event_to "$run_dir" \
      "m0 health failed: Exit route actual=${exit_if:-missing} forbidden=$utun"
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
  local output_file
  output_file="$run_dir/m0/cycle_$(printf '%03d' "$cycle")_${phase}.json"
  m0_assert_run_healthy "$run_dir" || return 1
  append_event_to "$run_dir" \
    "m0 phase start cycle=$cycle phase=$phase duration=$duration rate_bps=$rate_bps"
  m0_progress "M0 cycle=$cycle phase=$phase duration=${duration}s rate_bps=$rate_bps start"
  if [[ "$udp" == "1" && "$reverse" == "1" ]]; then
    run_m0_logged "$output_file" "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" -u -l 1160 -R --json
  elif [[ "$udp" == "1" ]]; then
    run_m0_logged "$output_file" "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" -u -l 1160 --json
  elif [[ "$reverse" == "1" ]]; then
    run_m0_logged "$output_file" "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" -R --json
  else
    run_m0_logged "$output_file" "$M0_IPERF3_BIN" -c "$target" -p "$iperf_port" \
      -t "$duration" -P 1 -b "$rate_bps" --json
  fi || {
    append_event_to "$run_dir" "m0 phase failed cycle=$cycle phase=$phase"
    return 1
  }
  m0_assert_run_healthy "$run_dir" || return 1
  if ! validate_m0_iperf_result "$output_file" "$([[ "$udp" == "1" ]] && echo UDP || echo TCP)"; then
    append_event_to "$run_dir" \
      "m0 phase failed cycle=$cycle phase=$phase reason=invalid_iperf_result"
    return 1
  fi
  append_event_to "$run_dir" "m0 phase complete cycle=$cycle phase=$phase"
  m0_progress "M0 cycle=$cycle phase=$phase complete"
  if [[ "${M0_RESUME_PENDING:-0}" == "1" ]]; then
    append_event_to "$run_dir" "m0 resume complete cycle=$cycle phase=$phase"
    M0_RESUME_PENDING=0
  fi
}

run_m0_dns_phase() {
  local run_dir="$1"
  local dns_target="$2"
  local dns_name="$3"
  local cycle="$4"
  local output_file="$run_dir/m0/cycle_$(printf '%03d' "$cycle")_dns.txt"
  [[ -n "$dns_target" ]] || return 0
  m0_assert_run_healthy "$run_dir" || return 1
  append_event_to "$run_dir" "m0 DNS start cycle=$cycle"
  if ! run_m0_logged "$output_file" "$M0_DIG_BIN" "@$dns_target" "$dns_name" A \
    +time=5 +tries=1; then
    append_event_to "$run_dir" "m0 DNS failed cycle=$cycle"
    return 1
  fi
  m0_assert_run_healthy "$run_dir" || return 1
  if ! validate_m0_dns_result "$output_file"; then
    append_event_to "$run_dir" "m0 DNS failed cycle=$cycle reason=no_fake_ip_answer"
    return 1
  fi
  append_event_to "$run_dir" "m0 DNS complete cycle=$cycle"
}

run_m0_active_window() {
  local run_dir="$1"
  local label="$2"
  local budget="$3"
  local profile_file="$4"
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
  short_count="$(m0_profile_value "$profile_file" short_connections_per_cycle)"
  tcp_forward_bps="$(m0_profile_value "$profile_file" tcp_forward_bps)"
  tcp_reverse_bps="$(m0_profile_value "$profile_file" tcp_reverse_bps)"
  udp_reverse_bps="$(m0_profile_value "$profile_file" udp_reverse_bps)"
  short_forward_bps="$(m0_profile_value "$profile_file" short_forward_bps)"
  short_reverse_bps="$(m0_profile_value "$profile_file" short_reverse_bps)"
  append_event_to "$run_dir" "m0 active $label start planned_secs=$budget"
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
    append_event_to "$run_dir" "m0 cycle complete cycle=$M0_CYCLE_INDEX"
  done
  append_event_to "$run_dir" "m0 active $label complete planned_secs=$budget"
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
  run_m0_logged "$run_dir/m0/idle.sleep.log" "$M0_SLEEP_BIN" "$idle_secs" || return 1
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

run_m0_action() {
  local run_dir utun target exit_host iperf_port dns_target dns_name profile_file
  require_root
  validate_m0_formal_config || \
    die "formal M0 requires the frozen 7200s workload schedule; unset M0_* duration overrides"
  run_dir="$(run_dir_from_state)" || die "no Knife15 run state"
  active_pid || die "mini_vpn is not running"
  workload_matches_run && die "an M0 workload is already running"
  [[ ! -e "$run_dir/m0.status" && ! -e "$run_dir/m0-workload.txt" && \
    ! -e "$run_dir/m0" ]] || \
    die "this TUN run already has M0 evidence; stop and start a fresh run"
  validate_baseline_dir_path "$M0_BASELINE_DIR" || \
    die "M0_BASELINE_DIR must be the simple /tmp baseline directory printed by baseline"
  [[ -d "$M0_BASELINE_DIR" && ! -L "$M0_BASELINE_DIR" ]] || \
    die "M0_BASELINE_DIR must be an existing non-symlink directory"

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
  require_command jq
  require_command iperf3
  require_command dig
  validate_m0_baseline_pair "$M0_BASELINE_DIR" "$target" || \
    die "M0 baseline must contain valid nonzero TCP forward/reverse results for $target"

  mkdir "$run_dir/m0" || die "cannot create M0 evidence directory"
  profile_file="$run_dir/m0-workload.txt"
  printf '%s\n' preparing >"$run_dir/m0.status"
  if ! write_m0_profile "$M0_BASELINE_DIR" "$profile_file" "$target" "$iperf_port" \
    "$dns_target" "$dns_name"; then
    printf '%s\n' failed >"$run_dir/m0.status"
    append_event_to "$run_dir" "m0 failed: workload profile derivation"
    die "cannot derive the M0 workload profile from baseline"
  fi
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
      ! m0_final_ownership_is_clean "$run_dir/mini_vpn.log"; then
      printf '%s\n' failed >"$run_dir/m0.status"
      append_event_to "$run_dir" "m0 failed: final evidence sample or health check"
      die "M0 traffic completed but final evidence/health/ownership failed; TUN remains for stop"
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
  if workload_matches_run; then
    echo "m0_workload=running pid=$(read_state workload.pid)"
  else
    echo "m0_workload=inactive"
  fi
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
  local conservation verdict remote_write_failures interface_error_samples log_bytes log_compactions
  local m0_status m0_cycles m0_dns m0_idle m0_resume m0_final_drain m0_phase_failures m0_health_failures
  local process_numeric_samples rss_first rss_last rss_max rss_delta
  local fd_first fd_last fd_max fd_delta threads_first threads_last threads_max threads_delta
  local interface_numeric_samples ipkts_first ipkts_last ipkts_delta ibytes_first ibytes_last ibytes_delta
  local opkts_first opkts_last opkts_delta obytes_first obytes_last obytes_delta
  local endpoint_samples_count endpoint_conservation_max endpoint_last_available endpoint_last_live
  local endpoint_last_outstanding endpoint_max_live endpoint_max_outstanding
  local m0_tcp_results m0_udp_results m0_tcp_max_gap m0_udp_max_loss m0_invalid_results
  local m0_phase_results m0_result_evidence
  local m0_dns_result_files m0_invalid_dns_results m0_dns_evidence m0_timeline_evidence
  if conservation_check_file "$log_file"; then
    conservation=PASS
  else
    conservation=FAIL_OR_MISSING
  fi
  remote_write_failures="$(grep -Ec '写入上游流失败|reason=remote_write_failed|reason=stalled_write_timeout' "$log_file" 2>/dev/null || true)"
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
    <<<"$(m0_result_envelope "$run_dir/m0")"
  read -r m0_dns_result_files m0_invalid_dns_results \
    <<<"$(m0_dns_result_envelope "$run_dir/m0")"
  m0_result_evidence=NOT_APPLICABLE
  m0_dns_evidence=NOT_APPLICABLE
  m0_timeline_evidence=NOT_APPLICABLE
  if [[ "$m0_status" == "complete" ]]; then
    if ((10#$m0_phase_results > 0 && \
      10#$m0_phase_results == 10#$m0_tcp_results + 10#$m0_udp_results && \
      10#$m0_invalid_results == 0)); then
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
  if [[ "$conservation" != "PASS" ]] || \
    [[ "$m0_status" != "complete" && "$m0_status" != "not_run" ]] || \
    { [[ "$m0_status" == "complete" ]] && \
      [[ "$endpoint_last_live" != "0" || "$endpoint_last_outstanding" != "0" ]]; } || \
    [[ "$m0_result_evidence" == "MISMATCH" ]] || \
    [[ "$m0_dns_evidence" == "MISMATCH" ]] || \
    [[ "$m0_timeline_evidence" == "MISMATCH" ]] || \
    { [[ "$log_compactions" =~ ^[0-9]+$ ]] && ((10#$log_compactions > 0)); } || \
    { [[ "$m0_invalid_results" =~ ^[0-9]+$ ]] && ((10#$m0_invalid_results > 0)); } || \
    { [[ "$interface_error_samples" =~ ^[0-9]+$ ]] && \
    ((10#$interface_error_samples > 0)); } || \
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
- interface_error_samples: ${interface_error_samples:-unknown}
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
- process_samples: $(awk 'END {print (NR > 0 ? NR - 1 : 0)}' "$run_dir/process.csv" 2>/dev/null)
- endpoint_samples: ${endpoint_samples_count:-0}
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
  workload_matches_run && die "refuse a mutable bundle while the M0 workload is running; use stop"
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
    die "M0 workload identity/termination check failed; refusing process and route cleanup"
  terminate_recorded_pid "$run_dir"
  cleanup_owned_routes "$run_dir"
  terminate_recorded_watchdog "$run_dir"
  sample_once_for "$run_dir"
  append_event_to "$run_dir" "stop cleanup complete"
  owned_routes_are_restored || \
    die "an owned target/Exit/DNS route still points to the run utun after cleanup"
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
  m0)
    run_m0_action
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
