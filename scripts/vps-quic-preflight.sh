#!/usr/bin/env bash
# mini_vpn VPS QUIC readiness helper.
#
# Run locally on the Linux VPS role being checked. The script intentionally
# avoids reading .env files or dumping sing-box config, certificates, keys, or
# TUIC credentials.

set -euo pipefail

readonly REQUIRED_RMEM_MAX=16777216
readonly REQUIRED_WMEM_MAX=16777216
readonly REQUIRED_RMEM_DEFAULT=1048576
readonly REQUIRED_WMEM_DEFAULT=1048576

CONF_PATH="${MINI_VPN_QUIC_SYSCTL_CONF:-/etc/sysctl.d/99-mini-vpn-quic.conf}"
SING_BOX_SERVICE="${SING_BOX_SERVICE:-sing-box}"
SING_BOX_BIN="${SING_BOX_BIN:-sing-box}"
SING_BOX_CONFIG="${SING_BOX_CONFIG:-/etc/sing-box/config.json}"
SING_BOX_LOG="${SING_BOX_LOG:-/var/log/sing-box.log}"
TUIC_PORT="${TUIC_PORT:-8443}"
IPERF3_SERVICE="${IPERF3_SERVICE:-iperf3}"
IPERF3_PORT="${IPERF3_PORT:-5201}"
LOG_TAIL_LINES="${LOG_TAIL_LINES:-80}"
RESTART_SING_BOX="${RESTART_SING_BOX:-1}"

FAILURES=0
WARNINGS=0

usage() {
  cat <<'USAGE'
usage: scripts/vps-quic-preflight.sh <command>

commands:
  check-exit       Check Linux QUIC socket buffers, sing-box, and TUIC UDP listen socket.
  install-exit     Install /etc/sysctl.d/99-mini-vpn-quic.conf, apply sysctl, and restart sing-box.
  diagnose-exit    Run check-exit plus bounded, redacted operator diagnostics.
  check-client     Check Linux client/test VPS socket-buffer readiness for mini_vpn.
  check-target     Check target iperf3 service and TCP listen socket.
  checklist        Print the no-secret acceptance preflight checklist.
  --self-test      Run parser/redaction/unit checks without touching the host.

env:
  MINI_VPN_QUIC_SYSCTL_CONF=/etc/sysctl.d/99-mini-vpn-quic.conf
  SING_BOX_SERVICE=sing-box
  SING_BOX_BIN=sing-box
  SING_BOX_CONFIG=/etc/sing-box/config.json
  SING_BOX_LOG=/var/log/sing-box.log
  TUIC_PORT=8443
  IPERF3_SERVICE=iperf3
  IPERF3_PORT=5201
  LOG_TAIL_LINES=80
  RESTART_SING_BOX=1        set 0 during install-exit to skip service restart

This helper never reads .env files and never prints TUIC UUIDs/passwords from
configuration. Run install-exit only during a maintenance window; restarting
sing-box recreates the TUIC UDP socket under the larger kernel buffers.
USAGE
}

info() {
  printf 'INFO %s\n' "$*"
}

ok() {
  printf 'OK   %s\n' "$*"
}

warn() {
  WARNINGS=$((WARNINGS + 1))
  printf 'WARN %s\n' "$*" >&2
}

fail_check() {
  FAILURES=$((FAILURES + 1))
  printf 'FAIL %s\n' "$*" >&2
}

die() {
  printf 'ERROR %s\n' "$*" >&2
  exit 2
}

finish_checks() {
  local label="$1"
  if ((FAILURES > 0)); then
    printf '%s: FAIL failures=%d warnings=%d\n' "$label" "$FAILURES" "$WARNINGS" >&2
    exit 1
  fi
  printf '%s: PASS failures=0 warnings=%d\n' "$label" "$WARNINGS"
}

require_linux() {
  local kernel
  kernel="$(uname -s 2>/dev/null || true)"
  if [[ "$kernel" != "Linux" ]]; then
    die "this helper is for Linux VPS hosts; kernel=${kernel:-unknown}"
  fi
}

as_root() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    if [[ -t 0 && -t 2 ]]; then
      sudo "$@"
    else
      sudo -n "$@"
    fi
  else
    die "root privileges required for: $*"
  fi
}

is_uint() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

uint_ge() {
  local value="$1"
  local minimum="$2"
  is_uint "$value" && is_uint "$minimum" && ((10#$value >= 10#$minimum))
}

sysctl_value() {
  local key="$1"
  sysctl -n "$key" 2>/dev/null | awk 'NR == 1 { print $1 }'
}

conf_value_from_file() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '
    {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left != key) {
        next
      }
      right = $2
      sub(/#.*/, "", right)
      gsub(/[[:space:]]/, "", right)
      value = right
    }
    END {
      if (value != "") {
        print value
      }
    }
  ' "$file"
}

required_conf() {
  cat <<EOF
net.core.rmem_max = $REQUIRED_RMEM_MAX
net.core.wmem_max = $REQUIRED_WMEM_MAX
net.core.rmem_default = $REQUIRED_RMEM_DEFAULT
net.core.wmem_default = $REQUIRED_WMEM_DEFAULT
EOF
}

check_one_sysctl() {
  local key="$1"
  local minimum="$2"
  local value
  value="$(sysctl_value "$key")"
  if uint_ge "$value" "$minimum"; then
    ok "$key=$value >= $minimum"
  else
    fail_check "$key=${value:-missing} < required $minimum"
  fi
}

check_sysctl_floor() {
  check_one_sysctl net.core.rmem_max "$REQUIRED_RMEM_MAX"
  check_one_sysctl net.core.wmem_max "$REQUIRED_WMEM_MAX"
  check_one_sysctl net.core.rmem_default "$REQUIRED_RMEM_DEFAULT"
  check_one_sysctl net.core.wmem_default "$REQUIRED_WMEM_DEFAULT"
}

check_persistent_conf() {
  local required="$1"

  if [[ ! -f "$CONF_PATH" ]]; then
    if [[ "$required" == "1" ]]; then
      fail_check "$CONF_PATH is missing; runtime sysctl may not survive reboot"
    else
      warn "$CONF_PATH is missing; client/test VPS runtime values may not survive reboot"
    fi
    return 0
  fi

  check_one_conf_value net.core.rmem_max "$REQUIRED_RMEM_MAX"
  check_one_conf_value net.core.wmem_max "$REQUIRED_WMEM_MAX"
  check_one_conf_value net.core.rmem_default "$REQUIRED_RMEM_DEFAULT"
  check_one_conf_value net.core.wmem_default "$REQUIRED_WMEM_DEFAULT"
}

check_one_conf_value() {
  local key="$1"
  local minimum="$2"
  local value
  value="$(conf_value_from_file "$CONF_PATH" "$key")"
  if uint_ge "$value" "$minimum"; then
    ok "$CONF_PATH $key=$value >= $minimum"
  else
    fail_check "$CONF_PATH $key=${value:-missing} < required $minimum"
  fi
}

check_systemd_active() {
  local service="$1"
  if ! command -v systemctl >/dev/null 2>&1; then
    fail_check "systemctl not found; cannot verify service $service"
    return 0
  fi
  if systemctl is-active --quiet "$service"; then
    ok "$service service active"
  else
    fail_check "$service service is not active"
  fi
}

check_sing_box_binary() {
  if command -v "$SING_BOX_BIN" >/dev/null 2>&1; then
    ok "sing-box binary found: $(command -v "$SING_BOX_BIN")"
    "$SING_BOX_BIN" version 2>/dev/null | sed -n '1,3p' || true
  else
    fail_check "sing-box binary not found: $SING_BOX_BIN"
  fi
}

check_sing_box_config() {
  if ! command -v "$SING_BOX_BIN" >/dev/null 2>&1; then
    return 0
  fi
  if [[ ! -f "$SING_BOX_CONFIG" ]]; then
    fail_check "sing-box config not found: $SING_BOX_CONFIG"
    return 0
  fi

  local output
  local status
  set +e
  output="$(as_root "$SING_BOX_BIN" check -c "$SING_BOX_CONFIG" 2>&1)"
  status=$?
  if ((status != 0)); then
    output="$(as_root "$SING_BOX_BIN" -c "$SING_BOX_CONFIG" check 2>&1)"
    status=$?
  fi
  set -e

  if ((status == 0)); then
    ok "sing-box config check passed for $SING_BOX_CONFIG"
  else
    fail_check "sing-box config check failed for $SING_BOX_CONFIG"
    redact_secrets <<<"$output" >&2
  fi
}

check_udp_listen() {
  if ! command -v ss >/dev/null 2>&1; then
    fail_check "ss not found; cannot verify UDP listen socket"
    return 0
  fi

  local output
  set +e
  output="$(as_root ss -lunp 2>&1)"
  local status=$?
  set -e
  if ((status != 0)); then
    fail_check "ss -lunp failed"
    redact_secrets <<<"$output" >&2
    return 0
  fi

  if awk -v port="$TUIC_PORT" '$0 ~ ":" port "([^0-9]|$)" { found = 1 } END { exit(found ? 0 : 1) }' <<<"$output"; then
    ok "UDP listen socket found on :$TUIC_PORT"
    awk -v port="$TUIC_PORT" '$0 ~ ":" port "([^0-9]|$)" { print }' <<<"$output" | redact_secrets
  else
    fail_check "no UDP listen socket found on :$TUIC_PORT"
  fi
}

check_time_sync() {
  if ! command -v timedatectl >/dev/null 2>&1; then
    warn "timedatectl not found; skipping clock sync check"
    return 0
  fi

  local output
  output="$(timedatectl show -p NTPSynchronized -p SystemClockSynchronized 2>/dev/null || true)"
  printf '%s\n' "$output"
  if grep -q '=no' <<<"$output"; then
    warn "clock sync is not fully synchronized"
  else
    ok "clock sync check did not report unsynchronized state"
  fi
}

check_tcp_listen() {
  if ! command -v ss >/dev/null 2>&1; then
    fail_check "ss not found; cannot verify TCP listen socket"
    return 0
  fi

  local output
  set +e
  output="$(ss -ltnp 2>&1)"
  local status=$?
  set -e
  if ((status != 0)); then
    fail_check "ss -ltnp failed"
    redact_secrets <<<"$output" >&2
    return 0
  fi

  if awk -v port="$IPERF3_PORT" '$0 ~ ":" port "([^0-9]|$)" { found = 1 } END { exit(found ? 0 : 1) }' <<<"$output"; then
    ok "TCP listen socket found on :$IPERF3_PORT"
    awk -v port="$IPERF3_PORT" '$0 ~ ":" port "([^0-9]|$)" { print }' <<<"$output" | redact_secrets
  else
    fail_check "no TCP listen socket found on :$IPERF3_PORT"
  fi
}

redact_secrets() {
  sed -E \
    -e 's/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<uuid-redacted>/g' \
    -e 's/([Pp]assword[=: ]+)[^ ,;"]+/\1<redacted>/g' \
    -e 's/([Tt][Uu][Ii][Cc]_?[Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd][=: ]+)[^ ,;"]+/\1<redacted>/g'
}

install_exit() {
  require_linux
  local tmp
  tmp="$(mktemp)"
  required_conf >"$tmp"
  as_root install -d -m 0755 "$(dirname "$CONF_PATH")"
  as_root install -m 0644 "$tmp" "$CONF_PATH"
  rm -f "$tmp"
  ok "installed $CONF_PATH"

  as_root sysctl --system
  ok "applied sysctl --system"

  if [[ "$RESTART_SING_BOX" == "1" ]]; then
    as_root systemctl restart "$SING_BOX_SERVICE"
    ok "restarted $SING_BOX_SERVICE"
  else
    warn "RESTART_SING_BOX=0; restart $SING_BOX_SERVICE before throughput acceptance"
  fi

  check_exit
}

check_exit() {
  require_linux
  info "checking exit VPS QUIC readiness"
  check_sysctl_floor
  check_persistent_conf 1
  check_systemd_active "$SING_BOX_SERVICE"
  check_sing_box_binary
  check_sing_box_config
  check_udp_listen
  check_time_sync
  finish_checks "check-exit"
}

diagnose_exit() {
  require_linux
  info "operator diagnostics for exit VPS"
  uname -a || true
  date -u '+%Y-%m-%dT%H:%M:%SZ' || true
  check_sysctl_floor
  check_persistent_conf 1
  check_systemd_active "$SING_BOX_SERVICE"
  check_sing_box_binary
  check_sing_box_config
  check_udp_listen
  check_time_sync

  if command -v systemctl >/dev/null 2>&1; then
    info "systemd state for $SING_BOX_SERVICE"
    systemctl show "$SING_BOX_SERVICE" -p ActiveState -p SubState -p ExecMainPID -p NRestarts -p ActiveEnterTimestamp --no-pager || true
  fi

  if [[ -f "$SING_BOX_LOG" ]]; then
    info "redacted tail of $SING_BOX_LOG"
    as_root tail -n "$LOG_TAIL_LINES" "$SING_BOX_LOG" | redact_secrets || true
  else
    warn "$SING_BOX_LOG not found; skipping sing-box log tail"
  fi

  finish_checks "diagnose-exit"
}

check_client() {
  require_linux
  info "checking Linux client/test VPS QUIC readiness"
  check_sysctl_floor
  check_persistent_conf 0
  if command -v sysctl >/dev/null 2>&1; then
    ok "mini_vpn should be able to request large QUIC UDP socket buffers"
  fi
  finish_checks "check-client"
}

check_target() {
  require_linux
  info "checking target iperf3 readiness"
  check_systemd_active "$IPERF3_SERVICE"
  check_tcp_listen
  finish_checks "check-target"
}

print_checklist() {
  cat <<EOF
# mini_vpn no-secret VPS acceptance preflight

Exit VPS:
  sudo bash scripts/vps-quic-preflight.sh check-exit
  # If values are low and a maintenance restart is acceptable:
  sudo bash scripts/vps-quic-preflight.sh install-exit

Client/test VPS:
  bash scripts/vps-quic-preflight.sh check-client
  # mini_vpn startup should later log:
  # QUIC UDP socket buffers: requested=8388608B recv=16777216B send=16777216B

Target VPS:
  bash scripts/vps-quic-preflight.sh check-target

Direct exit-to-target baseline before blaming mini_vpn:
  iperf3 -c <TARGET_IP> -p $IPERF3_PORT -t 30 -P 1
  iperf3 -c <TARGET_IP> -p $IPERF3_PORT -t 30 -P 1 -R

No secrets: keep TUIC UUID/passwords, private keys, .env files, and sudo
passwords out of commands, reports, docs, and git.
EOF
}

self_test_assert_eq() {
  local name="$1"
  local got="$2"
  local want="$3"
  if [[ "$got" != "$want" ]]; then
    printf 'self-test failed: %s got=%q want=%q\n' "$name" "$got" "$want" >&2
    exit 1
  fi
}

self_test_assert_success() {
  local name="$1"
  shift
  if ! "$@"; then
    printf 'self-test failed: %s\n' "$name" >&2
    exit 1
  fi
}

self_test_assert_failure() {
  local name="$1"
  shift
  if "$@"; then
    printf 'self-test failed: %s unexpectedly succeeded\n' "$name" >&2
    exit 1
  fi
}

run_self_test() {
  local tmp
  tmp="$(mktemp)"
  {
    echo '# comment'
    echo 'net.core.rmem_max = 16777216'
    echo 'net.core.wmem_max=33554432'
    echo 'net.core.rmem_default = 1048576 # inline comment'
    echo 'net.core.wmem_default = 1048576'
  } >"$tmp"

  self_test_assert_eq conf_rmem_max "$(conf_value_from_file "$tmp" net.core.rmem_max)" "16777216"
  self_test_assert_eq conf_wmem_max "$(conf_value_from_file "$tmp" net.core.wmem_max)" "33554432"
  self_test_assert_eq conf_rmem_default "$(conf_value_from_file "$tmp" net.core.rmem_default)" "1048576"
  self_test_assert_success uint_ge_equal uint_ge 16777216 16777216
  self_test_assert_success uint_ge_higher uint_ge 33554432 16777216
  self_test_assert_failure uint_ge_lower uint_ge 212992 16777216

  local redacted
  redacted="$(printf 'uuid=11111111-2222-3333-4444-555555555555 password=secret TUIC_PASSWORD=secret2\n' | redact_secrets)"
  self_test_assert_eq redact "$redacted" "uuid=<uuid-redacted> password=<redacted> TUIC_PASSWORD=<redacted>"

  rm -f "$tmp"
  echo "self-test: PASS"
}

main() {
  local command="${1:-}"
  case "$command" in
    -h | --help | help)
      usage
      ;;
    check-exit)
      check_exit
      ;;
    install-exit)
      install_exit
      ;;
    diagnose-exit)
      diagnose_exit
      ;;
    check-client)
      check_client
      ;;
    check-target)
      check_target
      ;;
    checklist)
      print_checklist
      ;;
    --self-test)
      run_self_test
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
