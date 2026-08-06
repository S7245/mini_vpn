#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
ACTION="${1:---help}"

EXIT_SSH_HOST="${EXIT_SSH_HOST:-ubuntu@43.153.32.33}"
EXIT_SSH_KEY="${EXIT_SSH_KEY:-$HOME/.ssh/vpn}"
TARGET="${TARGET:-43.130.32.77}"
IPERF_PORT="${IPERF_PORT:-5201}"
OBSERVER_TIMEOUT_SECS="${OBSERVER_TIMEOUT_SECS:-7200}"
OBSERVER_STATE_DIR="${OBSERVER_STATE_DIR:-/var/run/mini_vpn_knife15_exit_target_observer}"
OBSERVER_RUN_ROOT="${OBSERVER_RUN_ROOT:-/tmp}"
TCPDUMP_BIN="${TCPDUMP_BIN:-/usr/bin/tcpdump}"
SS_BIN="${SS_BIN:-/usr/bin/ss}"
TIMEOUT_BIN="${TIMEOUT_BIN:-/usr/bin/timeout}"
SETSID_BIN="${SETSID_BIN:-/usr/bin/setsid}"
PS_BIN="${PS_BIN:-/usr/bin/ps}"
OBSERVER_STOP_WAIT_SECS="${OBSERVER_STOP_WAIT_SECS:-15}"

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/knife15-exit-target-observer.sh --self-test
  bash scripts/knife15-exit-target-observer.sh start
  bash scripts/knife15-exit-target-observer.sh status
  bash scripts/knife15-exit-target-observer.sh stop
  bash scripts/knife15-exit-target-observer.sh bundle

Defaults:
  EXIT_SSH_HOST=ubuntu@43.153.32.33
  EXIT_SSH_KEY=$HOME/.ssh/vpn
  TARGET=43.130.32.77
  IPERF_PORT=5201
  OBSERVER_TIMEOUT_SECS=7200

The remote observer captures only 96-byte snapshots for TARGET:IPERF_PORT,
rotates 17 x 20,000,000-byte files, samples matching TCP_INFO every 250ms,
and is hard-stopped after two hours. It does not capture TUIC UDP or
credentials.
USAGE
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

validate_ipv4() {
  local value="$1" part
  local -a parts
  [[ "$value" =~ ^[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+$ ]] || return 1
  IFS=. read -r -a parts <<<"$value"
  [[ "${#parts[@]}" == "4" ]] || return 1
  for part in "${parts[@]}"; do
    [[ "$part" =~ ^[0-9]+$ && 10#$part -le 255 ]] || return 1
  done
}

validate_port() {
  [[ "$1" =~ ^[0-9]+$ && 10#$1 -ge 1 && 10#$1 -le 65535 ]]
}

validate_timeout() {
  [[ "$1" =~ ^[0-9]+$ && 10#$1 -ge 900 && 10#$1 -le 7200 ]]
}

validate_ssh_host() {
  local value="$1"
  [[ "$value" =~ ^[a-z_][a-zA-Z0-9_-]*@([0-9]+[.]){3}[0-9]+$ ]] || return 1
  validate_ipv4 "${value#*@}"
}

capture_filter() {
  printf 'host %s and tcp port %s\n' "$1" "$2"
}

state_value() {
  local name="$1"
  [[ -f "$OBSERVER_STATE_DIR/$name" && \
    ! -L "$OBSERVER_STATE_DIR/$name" ]] || return 1
  sed -n '1p' "$OBSERVER_STATE_DIR/$name"
}

process_command() {
  local pid="$1"
  "$PS_BIN" -p "$pid" -o args= 2>/dev/null | \
    sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

process_matches() {
  local pid="$1" needle="$2" command_text
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  command_text="$(process_command "$pid")"
  [[ -n "$command_text" && "$command_text" == *"$needle"* ]]
}

observer_live() {
  local run_dir tcpdump_pid sampler_pid
  run_dir="$(state_value run_dir 2>/dev/null || true)"
  tcpdump_pid="$(state_value tcpdump.pid 2>/dev/null || true)"
  sampler_pid="$(state_value sampler.pid 2>/dev/null || true)"
  [[ -n "$run_dir" ]] || return 1
  process_matches "$tcpdump_pid" "$run_dir/capture.pcap" || \
    process_matches "$sampler_pid" "$run_dir"
}

require_remote_command() {
  [[ -x "$1" ]] || die "required remote command is unavailable: $1"
}

remote_start() {
  local target="$1" port="$2" timeout_secs="$3"
  local run_dir tcpdump_pid sampler_pid filter sampler_code started_epoch
  local tcpdump_version ss_version
  validate_ipv4 "$target" || die "invalid Target IPv4: $target"
  validate_port "$port" || die "invalid iperf port: $port"
  validate_timeout "$timeout_secs" || die "observer timeout must be 900..7200 seconds"
  require_remote_command "$TCPDUMP_BIN"
  require_remote_command "$SS_BIN"
  require_remote_command "$TIMEOUT_BIN"
  require_remote_command "$SETSID_BIN"
  [[ ! -e "$OBSERVER_STATE_DIR" ]] || \
    die "observer state already exists; use status, stop, then bundle"

  tcpdump_version="$("$TCPDUMP_BIN" --version 2>&1 | sed -n '1p')" || \
    die "cannot read tcpdump version"
  ss_version="$("$SS_BIN" -V 2>&1 | sed -n '1p')" || \
    die "cannot read ss version"
  [[ -n "$tcpdump_version" && -n "$ss_version" ]] || \
    die "observer command version evidence is empty"
  run_dir="$OBSERVER_RUN_ROOT/mini_vpn_knife15_exit_target_observer_$(date -u '+%Y%m%d_%H%M%S')"
  started_epoch="$(date -u '+%s')"
  mkdir -m 0755 "$OBSERVER_STATE_DIR" "$run_dir" || \
    die "cannot create observer state/evidence directories"
  filter="$(capture_filter "$target" "$port")"
  printf '%s\n' \
    'schema=knife15-exit-target-observer-v1' \
    "started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "started_epoch=$started_epoch" \
    "target=$target" \
    "iperf_port=$port" \
    "timeout_secs=$timeout_secs" \
    "tcpdump_bin=$TCPDUMP_BIN" \
    "tcpdump_version=$tcpdump_version" \
    "ss_bin=$SS_BIN" \
    "ss_version=$ss_version" \
    'snaplen_bytes=96' \
    'ring_files=17' \
    'ring_file_limit_bytes=20000000' \
    'socket_sample_interval_ms=250' \
    "capture_filter=$filter" \
    >"$run_dir/metadata.txt"

  nohup "$SETSID_BIN" "$TIMEOUT_BIN" --signal=INT --kill-after=15s \
    "$timeout_secs" "$TCPDUMP_BIN" -i any -n -s 96 -B 4096 \
    -C 20 -W 17 -Z root -w "$run_dir/capture.pcap" "$filter" \
    >"$run_dir/tcpdump.stdout" 2>"$run_dir/tcpdump.stderr" </dev/null &
  tcpdump_pid=$!

  sampler_code='trap "exit 0" TERM INT; target="$1"; port="$2"; while :; do date -u "+timestamp=%Y-%m-%dT%H:%M:%S.%NZ"; "$3" -Htin dst "$target:$port" 2>&1 || true; sleep 0.25; done'
  nohup "$SETSID_BIN" "$TIMEOUT_BIN" --signal=TERM --kill-after=5s \
    "$timeout_secs" /bin/bash -c "$sampler_code" observer \
    "$target" "$port" "$SS_BIN" "$run_dir" \
    >"$run_dir/tcp-info.log" 2>"$run_dir/tcp-info.stderr" </dev/null &
  sampler_pid=$!

  printf '%s\n' "$run_dir" >"$OBSERVER_STATE_DIR/run_dir"
  printf '%s\n' "$tcpdump_pid" >"$OBSERVER_STATE_DIR/tcpdump.pid"
  printf '%s\n' "$sampler_pid" >"$OBSERVER_STATE_DIR/sampler.pid"
  printf '%s\n' active >"$OBSERVER_STATE_DIR/status"
  printf '%s\n' "$target" >"$OBSERVER_STATE_DIR/target"
  printf '%s\n' "$port" >"$OBSERVER_STATE_DIR/port"
  printf '%s\n' "$started_epoch" >"$OBSERVER_STATE_DIR/started_epoch"
  /bin/sleep 1
  if ! process_matches "$tcpdump_pid" "$run_dir/capture.pcap" || \
    ! process_matches "$sampler_pid" "$run_dir"; then
    remote_stop >/dev/null 2>&1 || true
    die "observer process identity/startup check failed; evidence: $run_dir"
  fi
  printf 'PASS: Exit observer started\nrun_dir=%s\ntcpdump_pid=%s\nsampler_pid=%s\n' \
    "$run_dir" "$tcpdump_pid" "$sampler_pid"
}

remote_status() {
  local run_dir status tcpdump_pid sampler_pid started_epoch elapsed_secs
  local capture_files capture_bytes capture_drops=unknown
  local tcpdump_live=0 sampler_live=0
  [[ -d "$OBSERVER_STATE_DIR" && ! -L "$OBSERVER_STATE_DIR" ]] || \
    die "no observer state"
  run_dir="$(state_value run_dir)"
  status="$(state_value status 2>/dev/null || echo unknown)"
  tcpdump_pid="$(state_value tcpdump.pid 2>/dev/null || true)"
  sampler_pid="$(state_value sampler.pid 2>/dev/null || true)"
  started_epoch="$(state_value started_epoch 2>/dev/null || echo 0)"
  if [[ "$started_epoch" =~ ^[0-9]+$ && "$started_epoch" != "0" ]]; then
    elapsed_secs=$(( $(date -u '+%s') - 10#$started_epoch ))
  else
    elapsed_secs=unknown
  fi
  process_matches "$tcpdump_pid" "$run_dir/capture.pcap" && tcpdump_live=1
  process_matches "$sampler_pid" "$run_dir" && sampler_live=1
  capture_files="$(find "$run_dir" -maxdepth 1 -type f \
    -name 'capture.pcap*' 2>/dev/null | awk 'END { print NR + 0 }')"
  capture_bytes="$(find "$run_dir" -maxdepth 1 -type f \
    -name 'capture.pcap*' -exec wc -c {} \; 2>/dev/null | \
    awk '{ total += $1 } END { print total + 0 }')"
  if [[ -f "$run_dir/tcpdump.stderr" && ! -L "$run_dir/tcpdump.stderr" ]]; then
    capture_drops="$(awk '
      /packets dropped by kernel$/ && $1 ~ /^[0-9]+$/ { value=$1 }
      END { if (value == "") print "unknown"; else print value }
    ' "$run_dir/tcpdump.stderr")"
  fi
  printf 'status=%s\nrun_dir=%s\nelapsed_secs=%s\ntcpdump_pid=%s\ntcpdump_live=%s\nsampler_pid=%s\nsampler_live=%s\ncapture_files=%s\ncapture_bytes=%s\ncapture_kernel_drops=%s\n' \
    "$status" "$run_dir" "$elapsed_secs" \
    "$tcpdump_pid" "$tcpdump_live" \
    "$sampler_pid" "$sampler_live" "$capture_files" "$capture_bytes" \
    "$capture_drops"
  du -sk "$run_dir" 2>/dev/null | awk '{ print "evidence_kib=" $1 }'
  tail -n 8 "$run_dir/tcpdump.stderr" 2>/dev/null || true
}

stop_process_group() {
  local pid="$1" needle="$2" signal="$3" i
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  process_matches "$pid" "$needle" || \
    die "refusing ambiguous observer PID $pid; expected marker: $needle"
  kill -"$signal" -- "-$pid" 2>/dev/null || kill -"$signal" "$pid" 2>/dev/null || true
  for ((i = 0; i < 10#$OBSERVER_STOP_WAIT_SECS; i++)); do
    kill -0 "$pid" 2>/dev/null || {
      wait "$pid" 2>/dev/null || true
      return 0
    }
    /bin/sleep 1
  done
  process_matches "$pid" "$needle" || \
    die "observer PID identity changed while stopping: $pid"
  kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

remote_stop() {
  local run_dir tcpdump_pid sampler_pid
  [[ -d "$OBSERVER_STATE_DIR" && ! -L "$OBSERVER_STATE_DIR" ]] || \
    die "no observer state"
  run_dir="$(state_value run_dir)"
  tcpdump_pid="$(state_value tcpdump.pid 2>/dev/null || true)"
  sampler_pid="$(state_value sampler.pid 2>/dev/null || true)"
  stop_process_group "$tcpdump_pid" "$run_dir/capture.pcap" INT
  stop_process_group "$sampler_pid" "$run_dir" TERM
  printf '%s\n' inactive >"$OBSERVER_STATE_DIR/status"
  printf 'stopped_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    >>"$run_dir/metadata.txt"
  "$SS_BIN" -Htin dst "$(state_value target):$(state_value port)" \
    >"$run_dir/tcp-info-final.txt" 2>&1 || true
  printf 'PASS: Exit observer stopped\nrun_dir=%s\n' "$run_dir"
}

text_secret_scan() {
  local run_dir="$1" matched=1
  while IFS= read -r -d '' file_path; do
    if grep -Eaq \
      'MINI_VPN_TUIC_(UUID|PASSWORD)|BEGIN [A-Z ]*PRIVATE KEY|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' \
      "$file_path"; then
      matched=0
      break
    fi
  done < <(find "$run_dir" -type f ! -name '*.pcap*' -print0)
  [[ "$matched" == "1" ]]
}

remote_bundle() {
  local run_dir parent base bundle checksum
  [[ -d "$OBSERVER_STATE_DIR" && ! -L "$OBSERVER_STATE_DIR" ]] || \
    die "no observer state"
  observer_live && die "refuse to bundle while observer processes are live"
  run_dir="$(state_value run_dir)"
  [[ -d "$run_dir" && ! -L "$run_dir" ]] || die "observer run directory is invalid"
  text_secret_scan "$run_dir" || die "secret-shaped material found in observer text evidence"
  printf '%s\n' \
    'PASS: no credential names, private-key markers, or UUID-shaped values in text evidence' \
    >"$run_dir/secret-scan.txt"
  (
    cd "$run_dir"
    find . -type f ! -name SHA256SUMS -print0 | sort -z | \
      xargs -0 sha256sum >SHA256SUMS
  )
  parent="$(dirname "$run_dir")"
  base="$(basename "$run_dir")"
  bundle="${run_dir}.tar.gz"
  tar -C "$parent" -czf "$bundle" "$base"
  checksum="$(sha256sum "$bundle" | awk '{ print $1 }')"
  printf '%s  %s\n' "$checksum" "$bundle" >"$bundle.sha256"
  chmod 0644 "$bundle" "$bundle.sha256"
  rm -rf "$OBSERVER_STATE_DIR"
  printf 'PASS: Exit observer bundle finalized\nbundle=%s\nsha256=%s\n' \
    "$bundle" "$checksum"
}

remote_dispatch() {
  local action="$1" target="${2:-}" port="${3:-}" timeout_secs="${4:-}"
  case "$action" in
    start) remote_start "$target" "$port" "$timeout_secs" ;;
    status) remote_status ;;
    stop) remote_stop ;;
    bundle) remote_bundle ;;
    *) die "unknown remote observer action: $action" ;;
  esac
}

remote_call() {
  local action="$1"
  validate_ssh_host "$EXIT_SSH_HOST" || die "unsafe EXIT_SSH_HOST: $EXIT_SSH_HOST"
  [[ -f "$EXIT_SSH_KEY" && ! -L "$EXIT_SSH_KEY" ]] || \
    die "EXIT_SSH_KEY must be a readable non-symlink file"
  validate_ipv4 "$TARGET" || die "invalid TARGET: $TARGET"
  validate_port "$IPERF_PORT" || die "invalid IPERF_PORT: $IPERF_PORT"
  validate_timeout "$OBSERVER_TIMEOUT_SECS" || \
    die "OBSERVER_TIMEOUT_SECS must be 900..7200"
  ssh -i "$EXIT_SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new "$EXIT_SSH_HOST" \
    sudo -n bash -s -- __remote "$action" "$TARGET" "$IPERF_PORT" \
    "$OBSERVER_TIMEOUT_SECS" <"$SCRIPT_PATH"
}

self_test() {
  local tmp fake run_dir bundle checksum status_output tcpdump_pid
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/knife15-exit-observer-test.XXXXXX")"
  fake="$tmp/fake"
  mkdir -p "$fake" "$tmp/runs"
cat >"$fake/tcpdump" <<'EOF_FAKE_TCPDUMP'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then
  echo 'tcpdump fake-v1'
  exit 0
fi
printf '%s\n' "$*" >"$OBSERVER_TEST_ARGUMENTS"
printf '%s\n' "0 packets dropped by kernel" >&2
args=("$@")
for ((i = 0; i + 1 < ${#args[@]}; i++)); do
  if [[ "${args[$i]}" == "-w" ]]; then
    : >"${args[$((i + 1))]}00"
    break
  fi
done
if [[ "${OBSERVER_TEST_FORCE_KILL:-0}" == "1" ]]; then
  trap '' INT TERM
else
  trap 'printf "%s\n" "10 packets captured" "10 packets received by filter" "0 packets dropped by kernel" >&2; exit 0' INT TERM
fi
while :; do sleep 1; done
EOF_FAKE_TCPDUMP
cat >"$fake/ss" <<'EOF_FAKE_SS'
#!/usr/bin/env bash
if [[ "${1:-}" == "-V" ]]; then
  echo 'ss fake-v1'
  exit 0
fi
printf '%s\n' 'ESTAB 0 0 172.26.0.2:40000 43.130.32.77:5201' ' cubic wscale:7,7 rto:200 rtt:1/1 cwnd:10'
EOF_FAKE_SS
cat >"$fake/timeout" <<'EOF_FAKE_TIMEOUT'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$OBSERVER_TEST_TIMEOUT_ARGUMENTS"
while [[ "${1:-}" == --* ]]; do shift; done
shift
exec "$@"
EOF_FAKE_TIMEOUT
  cat >"$fake/setsid" <<'EOF_FAKE_SETSID'
#!/usr/bin/env bash
exec "$@"
EOF_FAKE_SETSID
  chmod +x "$fake/tcpdump" "$fake/ss" "$fake/timeout" "$fake/setsid"
  OBSERVER_STATE_DIR="$tmp/state"
  OBSERVER_RUN_ROOT="$tmp/runs"
  TCPDUMP_BIN="$fake/tcpdump"
  SS_BIN="$fake/ss"
  TIMEOUT_BIN="$fake/timeout"
  SETSID_BIN="$fake/setsid"
  PS_BIN="$(command -v ps)"
  OBSERVER_TEST_ARGUMENTS="$tmp/tcpdump.args"
  OBSERVER_TEST_TIMEOUT_ARGUMENTS="$tmp/timeout.args"
  OBSERVER_STOP_WAIT_SECS=2
  export OBSERVER_TEST_ARGUMENTS OBSERVER_TEST_TIMEOUT_ARGUMENTS

  validate_ipv4 43.130.32.77 || die "self-test: valid IPv4 rejected"
  ! validate_ipv4 43.130.32.999 || die "self-test: invalid IPv4 accepted"
  validate_port 5201 || die "self-test: valid port rejected"
  ! validate_port 0 || die "self-test: invalid port accepted"
  validate_timeout 900 || die "self-test: lower timeout rejected"
  ! validate_timeout 899 || die "self-test: short timeout accepted"
  [[ "$(capture_filter 43.130.32.77 5201)" == \
    'host 43.130.32.77 and tcp port 5201' ]] || \
    die "self-test: capture filter mismatch"

  remote_start 43.130.32.77 5201 900 >/dev/null
  if (remote_start 43.130.32.77 5201 900) >/dev/null 2>&1; then
    die "self-test: stacked observer start was accepted"
  fi
  run_dir="$(state_value run_dir)"
  grep -Fxq 'tcpdump_version=tcpdump fake-v1' "$run_dir/metadata.txt" || \
    die "self-test: tcpdump version provenance missing"
  grep -Fxq 'ss_version=ss fake-v1' "$run_dir/metadata.txt" || \
    die "self-test: ss version provenance missing"
  grep -Fxq 'ring_files=17' "$run_dir/metadata.txt" || \
    die "self-test: capture ring file count mismatch"
  for _ in 1 2 3 4 5; do
    [[ -s "$OBSERVER_TEST_ARGUMENTS" ]] && break
    /bin/sleep 1
  done
  grep -Fq -- '-s 96' "$OBSERVER_TEST_ARGUMENTS" || \
    die "self-test: header snap length missing"
  grep -Fq -- '-C 20 -W 17' "$OBSERVER_TEST_ARGUMENTS" || \
    die "self-test: bounded capture ring missing"
  grep -Fq -- '-Z root' "$OBSERVER_TEST_ARGUMENTS" || \
    die "self-test: rotating capture lost write ownership"
  grep -Fq -- 'host 43.130.32.77 and tcp port 5201' \
    "$OBSERVER_TEST_ARGUMENTS" || die "self-test: exact capture filter missing"
  grep -Fq -- '--signal=INT --kill-after=15s 900' \
    "$OBSERVER_TEST_TIMEOUT_ARGUMENTS" || \
    die "self-test: capture hard-timeout ownership missing"
  status_output="$(remote_status)"
  grep -Fq 'tcpdump_live=1' <<<"$status_output" || \
    die "self-test: live observer status mismatch"
  grep -Fq 'capture_files=1' <<<"$status_output" || \
    die "self-test: capture file status missing"
  grep -Eq '^elapsed_secs=[0-9]+$' <<<"$status_output" || \
    die "self-test: observer elapsed time missing"
  if (remote_bundle) >/dev/null 2>&1; then
    die "self-test: live observer bundle was accepted"
  fi
  remote_stop >/dev/null
  status_output="$(remote_status)"
  grep -Fq 'status=inactive' <<<"$status_output" || \
    die "self-test: stopped observer status mismatch"
  grep -Fq 'capture_kernel_drops=0' <<<"$status_output" || \
    die "self-test: capture drop evidence missing"
  printf '%s\n' 'MINI_VPN_TUIC_PASSWORD=forbidden' >"$run_dir/forbidden.txt"
  if (remote_bundle) >/dev/null 2>&1; then
    die "self-test: secret-shaped observer evidence was bundled"
  fi
  rm -f "$run_dir/forbidden.txt"
  remote_bundle >/dev/null
  bundle="${run_dir}.tar.gz"
  checksum="$(awk 'NR == 1 { print $1 }' "$bundle.sha256")"
  [[ "$checksum" == "$(sha256sum "$bundle" | awk '{ print $1 }')" ]] || \
    die "self-test: observer bundle checksum mismatch"
  [[ ! -e "$OBSERVER_STATE_DIR" ]] || \
    die "self-test: finalized observer state was retained"

  OBSERVER_TEST_FORCE_KILL=1
  export OBSERVER_TEST_FORCE_KILL
  remote_start 43.130.32.77 5201 900 >/dev/null
  run_dir="$(state_value run_dir)"
  tcpdump_pid="$(state_value tcpdump.pid)"
  printf '%s\n' "$$" >"$OBSERVER_STATE_DIR/tcpdump.pid"
  if (remote_stop) >/dev/null 2>&1; then
    die "self-test: ambiguous observer PID was killed"
  fi
  printf '%s\n' "$tcpdump_pid" >"$OBSERVER_STATE_DIR/tcpdump.pid"
  remote_stop >/dev/null
  ! process_matches "$tcpdump_pid" "$run_dir/capture.pcap" || \
    die "self-test: forced observer cleanup left tcpdump live"
  remote_bundle >/dev/null
  unset OBSERVER_TEST_FORCE_KILL
  rm -rf "$tmp"
  echo "knife15 Exit-to-Target observer self-test passed"
}

case "$ACTION" in
  --help|-h|help) usage ;;
  --self-test) self_test ;;
  start|status|stop|bundle) remote_call "$ACTION" ;;
  __remote) shift; remote_dispatch "$@" ;;
  *) usage >&2; exit 64 ;;
esac
