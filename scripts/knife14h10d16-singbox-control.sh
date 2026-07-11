#!/usr/bin/env bash
# Versioned Knife14 H10d16 mature-client capability precondition.
# Credentials are consumed from the environment and streamed to sing-box over
# a mode-0600 FIFO. They are never written to an artifact or command line.

set -euo pipefail

usage() {
  cat <<'USAGE'
usage: scripts/knife14h10d16-singbox-control.sh
       scripts/knife14h10d16-singbox-control.sh --self-test

Run on the Client VPS as root (normally through sudo -E) after sourcing .env.
The fixed capability shape is one 20s reverse P1 through a target-only MTU1500
sing-box TUN. Receiver throughput must exceed 150 Mbit/s and UDP socket drops
must remain zero before mini_vpn Gate A may run.
USAGE
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

render_config() {
  python3 - <<'PY'
import json
import os

cfg = {
    "log": {"level": "info", "timestamp": True},
    "inbounds": [{
        "type": "tun",
        "tag": "control-tun-in",
        "interface_name": os.environ["CONTROL_IF"],
        "address": ["172.30.16.1/30"],
        "mtu": int(os.environ["CONTROL_MTU"]),
        "auto_route": True,
        "strict_route": True,
        "route_address": [os.environ["TARGET"] + "/32"],
        "route_exclude_address": [os.environ["TUIC_SERVER_HOST"] + "/32"],
        "stack": "system",
    }],
    "outbounds": [{
        "type": "tuic",
        "tag": "tuic-control-out",
        "server": os.environ["TUIC_SERVER_HOST"],
        "server_port": int(os.environ["TUIC_SERVER_PORT"]),
        "uuid": os.environ["TUIC_UUID"],
        "password": os.environ["TUIC_PASSWORD"],
        "congestion_control": "bbr",
        "udp_relay_mode": "native",
        "zero_rtt_handshake": False,
        "tls": {
            "enabled": True,
            "server_name": os.environ["TUIC_SNI"],
            "certificate_path": os.environ["TUIC_CA_PATH"],
            "alpn": ["h3"],
        },
    }],
    "route": {
        "auto_detect_interface": True,
        "final": "tuic-control-out",
    },
}
json.dump(cfg, fp=os.sys.stdout, separators=(",", ":"))
PY
}

parse_iperf_summary() {
  python3 -c '
import json, sys
data = json.load(sys.stdin)
end = data.get("end", {})
sent = end.get("sum_sent", {}).get("bits_per_second", 0.0) / 1_000_000
received = end.get("sum_received", {}).get("bits_per_second", 0.0) / 1_000_000
print(f"sender_mbps={sent:.3f}")
print(f"receiver_mbps={received:.3f}")
'
}

socket_evidence_from_ss() {
  local port="$1"
  PORT="$port" python3 -c '
import os, re, sys
lines = sys.stdin.read().splitlines()
port = os.environ["PORT"]
best = None
for index, line in enumerate(lines):
    if not re.search(r":" + re.escape(port) + r"(?:\s|$)", line):
        continue
    window = " ".join(lines[index:index + 3])
    match = re.search(r"skmem:\([^)]*rb(\d+)[^)]*tb(\d+)[^)]*d(\d+)\)", window)
    if match:
        candidate = tuple(map(int, match.groups()))
        if best is None or candidate[:2] > best[:2]:
            best = candidate
if best is None:
    raise SystemExit(1)
print(f"rb={best[0]} tb={best[1]} drop={best[2]}")
'
}

field_value() {
  local fields="$1"
  local name="$2"
  awk -v key="$name" '{for (i=1; i<=NF; i++) if ($i ~ ("^" key "=")) {sub("^" key "=", "", $i); print $i; exit}}' <<<"$fields"
}

self_test() {
  local rendered summary socket

  rendered="$({
    TUIC_SERVER_HOST=192.0.2.10 TUIC_SERVER_PORT=8443 \
      TUIC_UUID=dummy-uuid TUIC_PASSWORD=dummy-password \
      TUIC_SNI=control.invalid TUIC_CA_PATH=/tmp/dummy-ca.pem \
      CONTROL_MTU=1500 CONTROL_IF=sb-d16-control \
      TARGET=198.51.100.77 render_config
  })"
  RENDERED="$rendered" python3 - <<'PY'
import json
import os

cfg = json.loads(os.environ["RENDERED"])
assert cfg["inbounds"][0]["mtu"] == 1500
assert cfg["inbounds"][0]["route_address"] == ["198.51.100.77/32"]
assert cfg["inbounds"][0]["route_exclude_address"] == ["192.0.2.10/32"]
assert cfg["outbounds"][0]["uuid"] == "dummy-uuid"
assert cfg["outbounds"][0]["password"] == "dummy-password"
PY
  unset rendered RENDERED

  summary="$(parse_iperf_summary <<'JSON'
{"end":{"sum_sent":{"bits_per_second":181000000},"sum_received":{"bits_per_second":179500000}}}
JSON
)"
  [[ "$summary" == $'sender_mbps=181.000\nreceiver_mbps=179.500' ]] || {
    echo "control self-test failed: iperf summary parser" >&2
    return 1
  }

  socket="$(socket_evidence_from_ss 8443 <<'SS'
ESTAB 0 0 10.0.0.1:40000 192.0.2.10:8443 users:(("sing-box",pid=9,fd=7))
 skmem:(r0,rb16777216,t0,tb16777216,f0,w0,o0,bl0,d0)
SS
)"
  [[ "$socket" == "rb=16777216 tb=16777216 drop=0" ]] || {
    echo "control self-test failed: UDP socket evidence parser" >&2
    return 1
  }

  echo "sing-box control self-test passed"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit $?
fi

readonly CONTROL_MTU=1500
readonly CONTROL_IF="${CONTROL_IF:-sb-d16-control}"
readonly TARGET="${TARGET:-43.130.32.77}"
readonly DURATION=20
readonly PARALLEL=1
readonly CONTROL_FLOOR_MBPS="${CONTROL_FLOOR_MBPS:-150}"
readonly SOCKET_TARGET_BYTES="${SOCKET_TARGET_BYTES:-16777216}"
readonly DIRECT_DURATION="${DIRECT_DURATION:-5}"
readonly IPERF_PORT="${IPERF_PORT:-5201}"
readonly EXIT_SSH_HOST="${EXIT_SSH_HOST:-ubuntu@43.153.32.33}"
readonly EXIT_SSH_KEY="${EXIT_SSH_KEY:-/home/ubuntu/.ssh/vpn}"

if ((EUID != 0)); then
  echo "ERROR: run as root through sudo -E so TUN routes and temporary sysctls can be restored" >&2
  exit 1
fi

for command_name in python3 ip iperf3 ss sysctl ssh mkfifo git; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "ERROR: required command missing: $command_name" >&2
    exit 1
  }
done

: "${MINI_VPN_TUIC_SERVER:?MINI_VPN_TUIC_SERVER is required}"
: "${MINI_VPN_TUIC_UUID:?MINI_VPN_TUIC_UUID is required}"
: "${MINI_VPN_TUIC_PASSWORD:?MINI_VPN_TUIC_PASSWORD is required}"
: "${MINI_VPN_TUIC_SNI:?MINI_VPN_TUIC_SNI is required}"
: "${MINI_VPN_TUIC_CA_PATH:?MINI_VPN_TUIC_CA_PATH is required}"

case "$MINI_VPN_TUIC_SERVER" in
  *:*)
    TUIC_SERVER_HOST="${MINI_VPN_TUIC_SERVER%:*}"
    TUIC_SERVER_PORT="${MINI_VPN_TUIC_SERVER##*:}"
    ;;
  *)
    echo "ERROR: MINI_VPN_TUIC_SERVER must be host:port" >&2
    exit 1
    ;;
esac
[[ "$TUIC_SERVER_PORT" =~ ^[0-9]+$ ]] || {
  echo "ERROR: TUIC server port must be numeric" >&2
  exit 1
}

TUIC_UUID="$MINI_VPN_TUIC_UUID"
TUIC_PASSWORD="$MINI_VPN_TUIC_PASSWORD"
TUIC_SNI="$MINI_VPN_TUIC_SNI"
TUIC_CA_PATH="$MINI_VPN_TUIC_CA_PATH"
if [[ "$TUIC_CA_PATH" != /* ]]; then
  TUIC_CA_PATH="$(pwd)/$TUIC_CA_PATH"
fi
[[ -r "$TUIC_CA_PATH" ]] || {
  echo "ERROR: configured TUIC CA path is not readable" >&2
  exit 1
}

SING_BOX_BIN="${SING_BOX_BIN:-}"
if [[ -z "$SING_BOX_BIN" ]]; then
  SING_BOX_BIN="$(command -v sing-box 2>/dev/null || true)"
fi
if [[ -z "$SING_BOX_BIN" ]]; then
  SING_BOX_BIN="$(find /tmp/mini_vpn -type f -name sing-box -perm -111 2>/dev/null | sort | tail -1 || true)"
fi
[[ -n "$SING_BOX_BIN" && -x "$SING_BOX_BIN" ]] || {
  echo "ERROR: sing-box binary not found; set SING_BOX_BIN to the mature client binary" >&2
  exit 1
}

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
REPO_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/.." && pwd)"
TS="$(date +%Y%m%d_%H%M%S)"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/mini_vpn_h10d16_singbox_control_$TS}"
RUNTIME_DIR="$(mktemp -d /tmp/mini_vpn-h10d16-control-runtime.XXXXXX)"
CONFIG_FIFO="$RUNTIME_DIR/config.fifo"
REPORT="$ARTIFACT_DIR/report.txt"
CLIENT_LOG="$ARTIFACT_DIR/sing-box-client.log"
IPERF_JSON="$ARTIFACT_DIR/iperf3-reverse-20s.json"
DIRECT_JSON="$ARTIFACT_DIR/direct-reverse.json"
CLIENT_SS="$ARTIFACT_DIR/client-udp-ss.txt"
SERVER_SS="$ARTIFACT_DIR/server-udp-ss.txt"
mkdir -p "$ARTIFACT_DIR"
chmod 700 "$ARTIFACT_DIR" "$RUNTIME_DIR"
mkfifo -m 600 "$CONFIG_FIFO"

SING_BOX_PID=""
CONFIG_WRITER_PID=""
IPERF_PID=""
RESULT="failed"
OLD_RMEM_MAX="$(sysctl -n net.core.rmem_max)"
OLD_WMEM_MAX="$(sysctl -n net.core.wmem_max)"
OLD_RMEM_DEFAULT="$(sysctl -n net.core.rmem_default)"
OLD_WMEM_DEFAULT="$(sysctl -n net.core.wmem_default)"

append() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

cleanup() {
  local status=$?
  set +e
  [[ -n "$IPERF_PID" ]] && kill "$IPERF_PID" >/dev/null 2>&1
  [[ -n "$SING_BOX_PID" ]] && kill "$SING_BOX_PID" >/dev/null 2>&1
  [[ -n "$CONFIG_WRITER_PID" ]] && kill "$CONFIG_WRITER_PID" >/dev/null 2>&1
  [[ -n "$IPERF_PID" ]] && wait "$IPERF_PID" >/dev/null 2>&1
  [[ -n "$SING_BOX_PID" ]] && wait "$SING_BOX_PID" >/dev/null 2>&1
  [[ -n "$CONFIG_WRITER_PID" ]] && wait "$CONFIG_WRITER_PID" >/dev/null 2>&1
  ip link delete "$CONTROL_IF" >/dev/null 2>&1
  sysctl -q -w net.core.rmem_default="$OLD_RMEM_DEFAULT" >/dev/null
  sysctl -q -w net.core.wmem_default="$OLD_WMEM_DEFAULT" >/dev/null
  sysctl -q -w net.core.rmem_max="$OLD_RMEM_MAX" >/dev/null
  sysctl -q -w net.core.wmem_max="$OLD_WMEM_MAX" >/dev/null
  rm -rf "$RUNTIME_DIR"
  if [[ -f "$REPORT" ]]; then
    printf 'cleanup=complete sysctls=restored tun=removed fifo=removed\n' >> "$REPORT"
  fi
  local secret artifact
  for secret in "$TUIC_UUID" "$TUIC_PASSWORD"; do
    [[ -n "$secret" ]] || continue
    for artifact in "$ARTIFACT_DIR"/*; do
      [[ -f "$artifact" ]] || continue
      if grep -Fq -- "$secret" "$artifact" 2>/dev/null; then
        rm -rf "$ARTIFACT_DIR"
        echo "ERROR: credential material reached an artifact; the artifact directory was deleted" >&2
        status=1
        break 2
      fi
    done
  done
  if [[ "$RESULT" == "passed" && $status -eq 0 ]]; then
    echo "control_result=PASS artifact_dir=$ARTIFACT_DIR"
  elif [[ "$RESULT" == "incapable" && $status -eq 2 ]]; then
    echo "control_result=INCAPABLE artifact_dir=$ARTIFACT_DIR"
  else
    echo "control_result=FAILED artifact_dir=$ARTIFACT_DIR" >&2
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

append "Knife14 H10d16 sing-box capability control"
append "source_commit=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=no 2>/dev/null || true)" ]]; then
  append "source_dirty=1"
else
  append "source_dirty=0"
fi
append "runner_sha256=$(sha256_file "$SCRIPT_PATH")"
append "sing_box_sha256=$(sha256_file "$SING_BOX_BIN")"
append "profile=historical-mtu1500-reverse-p1 duration_secs=$DURATION parallel=$PARALLEL"
append "target=$TARGET exit=$TUIC_SERVER_HOST mtu=$CONTROL_MTU floor_mbps=$CONTROL_FLOOR_MBPS"
append "credentials=piped-via-mode-0600-fifo config_persisted=0"
"$SING_BOX_BIN" version | head -3 | tee -a "$REPORT"

append "direct_reverse_preflight=running"
iperf3 -c "$TARGET" -p "$IPERF_PORT" -t "$DIRECT_DURATION" -P 1 -R --json > "$DIRECT_JSON"
DIRECT_SUMMARY="$(parse_iperf_summary < "$DIRECT_JSON")"
append "direct_${DIRECT_SUMMARY//$'\n'/ }"

sysctl -q -w net.core.rmem_max="$SOCKET_TARGET_BYTES" >/dev/null
sysctl -q -w net.core.wmem_max="$SOCKET_TARGET_BYTES" >/dev/null
sysctl -q -w net.core.rmem_default="$SOCKET_TARGET_BYTES" >/dev/null
sysctl -q -w net.core.wmem_default="$SOCKET_TARGET_BYTES" >/dev/null
append "temporary_socket_defaults=$SOCKET_TARGET_BYTES"

export CONTROL_MTU CONTROL_IF TARGET TUIC_SERVER_HOST TUIC_SERVER_PORT
export TUIC_UUID TUIC_PASSWORD TUIC_SNI TUIC_CA_PATH
render_config > "$CONFIG_FIFO" &
CONFIG_WRITER_PID=$!
"$SING_BOX_BIN" run -c "$CONFIG_FIFO" > "$CLIENT_LOG" 2>&1 &
SING_BOX_PID=$!
wait "$CONFIG_WRITER_PID"
CONFIG_WRITER_PID=""
rm -f "$CONFIG_FIFO"

ready=0
for _ in $(seq 1 30); do
  kill -0 "$SING_BOX_PID" >/dev/null 2>&1 || break
  if ip link show "$CONTROL_IF" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" == "1" ]] || {
  append "startup=failed"
  exit 1
}

ACTUAL_MTU="$(ip -o link show "$CONTROL_IF" | sed -n 's/.* mtu \([0-9][0-9]*\).*/\1/p')"
TARGET_ROUTE="$(ip route get "$TARGET")"
EXIT_ROUTE="$(ip route get "$TUIC_SERVER_HOST")"
append "tun_if=$CONTROL_IF actual_mtu=${ACTUAL_MTU:-unknown}"
append "target_route=$TARGET_ROUTE"
append "exit_route=$EXIT_ROUTE"
[[ "$ACTUAL_MTU" == "$CONTROL_MTU" ]] || exit 1
grep -qw "$CONTROL_IF" <<<"$TARGET_ROUTE" || exit 1
if grep -qw "$CONTROL_IF" <<<"$EXIT_ROUTE"; then
  append "route_assertion=failed-exit-loop"
  exit 1
fi
append "route_assertion=passed-target-only"

iperf3 -c "$TARGET" -p "$IPERF_PORT" -t "$DURATION" -P "$PARALLEL" -R --json > "$IPERF_JSON" &
IPERF_PID=$!
for _ in $(seq 1 10); do
  ss -u -a -m -n -p 2>/dev/null | grep -A2 ":$TUIC_SERVER_PORT" > "$CLIENT_SS" || true
  [[ -s "$CLIENT_SS" ]] && break
  sleep 1
done
timeout 15s ssh -i "$EXIT_SSH_KEY" -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new \
  "$EXIT_SSH_HOST" "sudo -n ss -u -a -m -n -p 2>/dev/null | grep -A2 ':$TUIC_SERVER_PORT' || true" \
  > "$SERVER_SS"
wait "$IPERF_PID"
IPERF_PID=""

CLIENT_SOCKET="$(socket_evidence_from_ss "$TUIC_SERVER_PORT" < "$CLIENT_SS")"
SERVER_SOCKET="$(socket_evidence_from_ss "$TUIC_SERVER_PORT" < "$SERVER_SS")"
append "client_udp_socket=$CLIENT_SOCKET"
append "server_udp_socket=$SERVER_SOCKET"

for evidence in "$CLIENT_SOCKET" "$SERVER_SOCKET"; do
  rb="$(field_value "$evidence" rb)"
  tb="$(field_value "$evidence" tb)"
  drop="$(field_value "$evidence" drop)"
  ((rb >= SOCKET_TARGET_BYTES && tb >= SOCKET_TARGET_BYTES && drop == 0)) || {
    append "socket_gate=failed"
    exit 1
  }
done
append "socket_gate=passed"

SUMMARY="$(parse_iperf_summary < "$IPERF_JSON")"
append "$SUMMARY"
RECEIVER_MBPS="$(field_value "${SUMMARY//$'\n'/ }" receiver_mbps)"
if python3 - "$RECEIVER_MBPS" "$CONTROL_FLOOR_MBPS" <<'PY'
import sys
raise SystemExit(0 if float(sys.argv[1]) > float(sys.argv[2]) else 1)
PY
then
  RESULT="passed"
  append "capability_gate=passed"
else
  RESULT="incapable"
  append "capability_gate=incapable"
  exit 2
fi

exit 0
