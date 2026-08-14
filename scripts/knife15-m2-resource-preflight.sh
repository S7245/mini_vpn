#!/usr/bin/env bash
set -euo pipefail
umask 077
PATH=/usr/bin:/bin:/usr/sbin:/sbin
export PATH
COPYFILE_DISABLE=1
export COPYFILE_DISABLE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE_HELPER="$SCRIPT_DIR/knife15-m2-resource-profile.py"
PREFLIGHT_RUNNER="$SCRIPT_DIR/knife15-m2-resource-preflight.sh"
REFERENCE_PROFILE="${M2_RESOURCE_REFERENCE_PROFILE:-}"
CANDIDATE_PROFILE="${M2_RESOURCE_CANDIDATE_PROFILE:-}"
DIRECT_DIR="${M2_DIRECT_DIR:-}"
BIN="${BIN:-$REPO_DIR/target/release/mini_vpn}"
EXIT_SSH_HOST="${EXIT_SSH_HOST:-}"
EXIT_SSH_KEY="${EXIT_SSH_KEY:-$HOME/.ssh/vpn}"
SERVER_CONFIG_PATH="${M2_EXIT_SERVER_CONFIG_PATH:-/etc/sing-box/config.json}"
PROVIDER_IDENTITY_EVIDENCE="${M2_PROVIDER_IDENTITY_EVIDENCE:-}"
ROUTE_IDENTITY_EVIDENCE="${M2_ROUTE_IDENTITY_EVIDENCE:-}"
PRIOR_SATURATION_EVIDENCE="${M2_PRIOR_SATURATION_EVIDENCE:-}"
REPLACEMENT_CAPACITY_EVIDENCE="${M2_REPLACEMENT_CAPACITY_EVIDENCE:-}"
OUT_DIR="${OUT_DIR:-/tmp/mini_vpn_knife15_resource_$(date -u '+%Y%m%d_%H%M%S')}"
ROUTE_BIN="${M2_RESOURCE_ROUTE_BIN:-/sbin/route}"
TRACEROUTE_BIN="${M2_RESOURCE_TRACEROUTE_BIN:-/usr/sbin/traceroute}"
SSH_BIN="${M2_RESOURCE_SSH_BIN:-/usr/bin/ssh}"
PREFLIGHT_EVIDENCE_ACTIVE=0

die() {
  if [[ "$PREFLIGHT_EVIDENCE_ACTIVE" == 1 && -d "$OUT_DIR" && \
    ! -e "$OUT_DIR/result.txt" ]]; then
    {
      echo 'schema=knife15-m2-resource-preflight-v1'
      echo 'status=fail'
      printf 'reason=%s\n' "$*"
    } >"$OUT_DIR/result.txt" 2>/dev/null || true
  fi
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

profile_value() {
  local profile="$1" key="$2"
  python3 -I - "$profile" "$key" <<'PY_PROFILE_VALUE'
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
    value = json.load(handle, object_pairs_hook=unique_object)
result = value.get(sys.argv[2])
if isinstance(result, bool):
    print("true" if result else "false")
elif isinstance(result, (str, int)):
    print(result)
else:
    raise SystemExit(1)
PY_PROFILE_VALUE
}

ssh_host_matches_ipv4() {
  local destination="${1:-}" expected="${2:-}" host user
  [[ -n "$destination" && -n "$expected" ]] || return 1
  if [[ "$destination" == *@* ]]; then
    user="${destination%@*}"
    host="${destination##*@}"
    [[ "$user" =~ ^[A-Za-z_][A-Za-z0-9._-]{0,63}$ ]] || return 1
  else
    host="$destination"
  fi
  [[ "$host" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || return 1
  [[ "$host" == "$expected" ]]
}

server_config_path_is_safe() {
  [[ "${1:-}" =~ ^/[A-Za-z0-9._/-]+$ ]]
}

tuic_server_parts() {
  local value="${1:-}"
  [[ "$value" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5}$ ]] || return 1
  printf '%s %s\n' "${value%:*}" "${value##*:}"
}

runner_self_test() {
  local reference="$SCRIPT_DIR/knife15-m2-reference-33.json"
  local candidate="$SCRIPT_DIR/fixtures/knife15-m2-resource/distinct-provider.json"
  local equivalent="$SCRIPT_DIR/fixtures/knife15-m2-resource/equivalent-resize.json"
  local result tmp evidence_sha
  python3 -I "$PROFILE_HELPER" --self-test >/dev/null || \
    die "profile helper self-test failed"
  result="$(python3 -I "$PROFILE_HELPER" compare \
    --reference "$reference" --candidate "$candidate")" || \
    die "eligible resource fixture was rejected"
  [[ "$result" == *'"eligible":true'* ]] || \
    die "eligible resource fixture did not produce eligibility"
  result="$(python3 -I "$PROFILE_HELPER" compare \
    --reference "$reference" --candidate "$equivalent")" || \
    die "equivalent resource fixture was not classifiable"
  [[ "$result" == *'"eligible":false'* ]] || \
    die "equivalent resource fixture was accepted"
  ssh_host_matches_ipv4 ubuntu@43.153.32.33 43.153.32.33 || \
    die "matching SSH identity was rejected"
  ! ssh_host_matches_ipv4 ubuntu@43.130.32.77 43.153.32.33 || \
    die "mismatched SSH identity was accepted"
  ! ssh_host_matches_ipv4 '-oProxyCommand=bad@43.153.32.33' 43.153.32.33 || \
    die "option-shaped SSH identity was accepted"
  server_config_path_is_safe /etc/sing-box/config.json || \
    die "safe server configuration path was rejected"
  ! server_config_path_is_safe '/etc/sing-box/$(id)' || \
    die "unsafe server configuration path was accepted"
  [[ "$(tuic_server_parts 43.153.32.33:8443)" == \
    "43.153.32.33 8443" ]] || die "TUIC endpoint parsing failed"
  ! tuic_server_parts 'example.com:8443' >/dev/null || \
    die "non-IPv4 TUIC endpoint was accepted"
  [[ "$(profile_value "$candidate" candidate_id)" == \
    candidate-distinct-provider ]] || die "profile value lookup failed"
  tmp="$(mktemp -d)"
  mkdir "$tmp/out"
  printf '%s\n' 'provider=fixture' >"$tmp/provider.txt"
  evidence_sha="$(sha256_file "$tmp/provider.txt")"
  (
    local OUT_DIR="$tmp/out"
    copy_hash_bound_evidence "$tmp/provider.txt" "$evidence_sha" \
      "$OUT_DIR/provider-copy.txt" "provider identity"
    cmp -s "$tmp/provider.txt" "$OUT_DIR/provider-copy.txt"
  ) || die "hash-bound evidence copy failed"
  if (
    local OUT_DIR="$tmp/out"
    copy_hash_bound_evidence "$tmp/provider.txt" "$(printf '0%.0s' {1..64})" \
      "$OUT_DIR/provider-invalid.txt" "provider identity"
  ) >/dev/null 2>&1; then
    rm -rf "$tmp"
    die "mismatched hash-bound evidence was accepted"
  fi
  rm -rf "$tmp"
  echo "knife15 M2 resource preflight self-test passed"
}

write_remote_probe() {
  local output="$1" tuic_port="$2"
  "$SSH_BIN" -i "$EXIT_SSH_KEY" -o BatchMode=yes \
    -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
    "$EXIT_SSH_HOST" "bash -s -- '$tuic_port' '$SERVER_CONFIG_PATH'" \
    >"$output" 2>"${output%.txt}.stderr" <<'REMOTE_RESOURCE_PROBE'
set -euo pipefail
tuic_port="$1"
server_config="$2"
server_bin="$(command -v sing-box)"
printf 'schema=knife15-m2-resource-remote-v1\n'
printf 'service_active=%s\n' "$(systemctl is-active sing-box)"
printf 'server_binary_path=%s\n' "$server_bin"
printf 'server_binary_sha256=%s\n' "$(sha256sum "$server_bin" | awk '{print $1}')"
printf 'server_config_sha256=%s\n' "$(sudo -n sha256sum "$server_config" | awk '{print $1}')"
printf 'cpu_count=%s\n' "$(getconf _NPROCESSORS_ONLN)"
printf 'loadavg=%s\n' "$(tr ' ' ',' </proc/loadavg)"
awk '/MemTotal:|MemAvailable:/ {
  key = tolower($1)
  sub(/:$/, "", key)
  printf "%s=%s%s\n", key, $2, $3
}' \
  /proc/meminfo
if ss -H -lun "sport = :$tuic_port" | grep -q .; then
  printf 'tuic_udp_listener=1\n'
else
  printf 'tuic_udp_listener=0\n'
fi
printf '%s\n' 'udp_summary_begin'
ss -s
printf '%s\n' 'udp_summary_end' 'interface_counters_begin'
ip -s link
printf '%s\n' 'interface_counters_end'
REMOTE_RESOURCE_PROBE
}

remote_value() {
  local file="$1" key="$2"
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$file"
}

copy_hash_bound_evidence() {
  local source_file="$1" expected_sha="$2" output_file="$3" label="$4"
  [[ -n "$source_file" && -f "$source_file" && ! -L "$source_file" ]] || \
    die "set exact regular $label evidence; evidence: $OUT_DIR"
  [[ "$(sha256_file "$source_file")" == "$expected_sha" ]] || \
    die "$label evidence hash does not match the profile; evidence: $OUT_DIR"
  cp "$source_file" "$output_file"
}

run_preflight() {
  local candidate_id candidate_ip candidate_port candidate_target
  local candidate_iperf_port
  local candidate_interface candidate_source candidate_binary_sha
  local candidate_direct_sha candidate_observer_sha classification
  local current_source current_binary_sha current_direct_sha current_observer_sha
  local current_interface target_interface remote_binary_sha remote_config_sha
  local expected_server_binary_sha expected_server_config_sha result archive
  local endpoint_ip endpoint_port evidence_file reference_copy candidate_copy
  local eligibility_reason expected_provider_evidence_sha
  local expected_route_evidence_sha expected_prior_saturation_sha
  local expected_replacement_capacity_sha

  ((EUID != 0)) || die "resource preflight is read-only and must not run with sudo"
  [[ "$ROUTE_BIN" == /sbin/route && \
    "$TRACEROUTE_BIN" == /usr/sbin/traceroute && \
    "$SSH_BIN" == /usr/bin/ssh ]] || \
    die "resource preflight run requires exact macOS system network tools"
  require_command python3
  require_command git
  require_command shasum
  [[ -x "$ROUTE_BIN" ]] || die "route command is unavailable: $ROUTE_BIN"
  [[ -x "$TRACEROUTE_BIN" ]] || \
    die "traceroute command is unavailable: $TRACEROUTE_BIN"
  [[ -x "$SSH_BIN" ]] || die "ssh command is unavailable: $SSH_BIN"
  [[ -f "$PROFILE_HELPER" && ! -L "$PROFILE_HELPER" ]] || \
    die "resource profile helper is missing or symlinked"
  [[ -n "$REFERENCE_PROFILE" && -f "$REFERENCE_PROFILE" && \
    ! -L "$REFERENCE_PROFILE" ]] || die "set an exact regular M2_RESOURCE_REFERENCE_PROFILE"
  [[ -n "$CANDIDATE_PROFILE" && -f "$CANDIDATE_PROFILE" && \
    ! -L "$CANDIDATE_PROFILE" ]] || die "set an exact regular M2_RESOURCE_CANDIDATE_PROFILE"
  [[ -n "$DIRECT_DIR" && -f "$DIRECT_DIR/manifest.txt" && \
    ! -L "$DIRECT_DIR/manifest.txt" ]] || die "set M2_DIRECT_DIR to exact direct evidence"
  [[ -f "$BIN" && ! -L "$BIN" ]] || die "release binary is missing or symlinked"
  [[ -n "$EXIT_SSH_HOST" && -f "$EXIT_SSH_KEY" && ! -L "$EXIT_SSH_KEY" ]] || \
    die "set exact EXIT_SSH_HOST and regular EXIT_SSH_KEY"
  server_config_path_is_safe "$SERVER_CONFIG_PATH" || \
    die "M2_EXIT_SERVER_CONFIG_PATH is unsafe"
  [[ ! -e "$OUT_DIR" ]] || die "resource evidence directory already exists: $OUT_DIR"
  [[ ! -e "${OUT_DIR}.tar.gz" && ! -e "${OUT_DIR}.tar.gz.sha256" ]] || \
    die "resource evidence archive already exists: ${OUT_DIR}.tar.gz"
  [[ -z "$(git -C "$REPO_DIR" status --short --untracked-files=no)" ]] || \
    die "resource preflight requires a clean tracked worktree"

  mkdir -m 700 "$OUT_DIR"
  PREFLIGHT_EVIDENCE_ACTIVE=1
  reference_copy="$OUT_DIR/reference-profile.json"
  candidate_copy="$OUT_DIR/candidate-profile.json"
  cp "$REFERENCE_PROFILE" "$reference_copy"
  cp "$CANDIDATE_PROFILE" "$candidate_copy"
  cp "$DIRECT_DIR/manifest.txt" "$OUT_DIR/direct-manifest.txt"
  classification="$(python3 -I "$PROFILE_HELPER" compare \
    --reference "$reference_copy" --candidate "$candidate_copy")" || \
    die "resource profile comparison failed; evidence: $OUT_DIR"
  printf '%s\n' "$classification" >"$OUT_DIR/eligibility.json"
  [[ "$classification" == *'"eligible":true'* ]] || \
    die "resource candidate is not materially distinct; evidence: $OUT_DIR"
  eligibility_reason="$(profile_value "$OUT_DIR/eligibility.json" reason)" || \
    die "resource eligibility reason is missing; evidence: $OUT_DIR"

  candidate_id="$(profile_value "$candidate_copy" candidate_id)" || \
    die "candidate ID is missing"
  candidate_ip="$(profile_value "$candidate_copy" public_ipv4)" || \
    die "candidate IPv4 is missing"
  candidate_port="$(profile_value "$candidate_copy" tuic_port)" || \
    die "candidate TUIC port is missing"
  candidate_target="$(profile_value "$candidate_copy" target_ipv4)" || \
    die "candidate Target is missing"
  candidate_iperf_port="$(profile_value "$candidate_copy" target_iperf_port)" || \
    die "candidate Target iperf port is missing"
  candidate_interface="$(profile_value "$candidate_copy" mac_interface)" || \
    die "candidate physical interface is missing"
  candidate_source="$(profile_value "$candidate_copy" source_commit)" || \
    die "candidate source commit is missing"
  candidate_binary_sha="$(profile_value "$candidate_copy" client_binary_sha256)" || \
    die "candidate binary hash is missing"
  candidate_direct_sha="$(profile_value "$candidate_copy" workload_profile_sha256)" || \
    die "candidate workload hash is missing"
  candidate_observer_sha="$(profile_value "$candidate_copy" observer_sha256)" || \
    die "candidate observer hash is missing"
  expected_server_binary_sha="$(profile_value \
    "$candidate_copy" server_binary_sha256)" || die "server binary hash is missing"
  expected_server_config_sha="$(profile_value \
    "$candidate_copy" server_config_sha256)" || die "server config hash is missing"
  expected_provider_evidence_sha="$(profile_value \
    "$candidate_copy" provider_identity_evidence_sha256)" || \
    die "provider identity evidence hash is missing"
  expected_route_evidence_sha="$(profile_value \
    "$candidate_copy" route_identity_evidence_sha256)" || \
    die "route identity evidence hash is missing"

  copy_hash_bound_evidence "$PROVIDER_IDENTITY_EVIDENCE" \
    "$expected_provider_evidence_sha" "$OUT_DIR/provider-identity.txt" \
    "provider identity"
  copy_hash_bound_evidence "$ROUTE_IDENTITY_EVIDENCE" \
    "$expected_route_evidence_sha" "$OUT_DIR/route-identity.txt" \
    "route identity"
  if [[ "$eligibility_reason" == proved_saturation_replacement ]]; then
    expected_prior_saturation_sha="$(profile_value \
      "$candidate_copy" prior_saturation_evidence_sha256)" || \
      die "prior saturation evidence hash is missing"
    expected_replacement_capacity_sha="$(profile_value \
      "$candidate_copy" replacement_capacity_evidence_sha256)" || \
      die "replacement capacity evidence hash is missing"
    copy_hash_bound_evidence "$PRIOR_SATURATION_EVIDENCE" \
      "$expected_prior_saturation_sha" "$OUT_DIR/prior-saturation.txt" \
      "prior saturation"
    copy_hash_bound_evidence "$REPLACEMENT_CAPACITY_EVIDENCE" \
      "$expected_replacement_capacity_sha" \
      "$OUT_DIR/replacement-capacity.txt" "replacement capacity"
  fi

  read -r endpoint_ip endpoint_port \
    <<<"$(tuic_server_parts "${MINI_VPN_TUIC_SERVER:-}")" || \
    die "MINI_VPN_TUIC_SERVER must be exact IPv4:port"
  [[ "$endpoint_ip" == "$candidate_ip" && "$endpoint_port" == "$candidate_port" ]] || \
    die "TUIC endpoint does not match the candidate profile; evidence: $OUT_DIR"
  [[ "${TARGET:-$candidate_target}" == "$candidate_target" ]] || \
    die "TARGET does not match the candidate profile; evidence: $OUT_DIR"
  ssh_host_matches_ipv4 "$EXIT_SSH_HOST" "$candidate_ip" || \
    die "EXIT_SSH_HOST does not match the candidate profile; evidence: $OUT_DIR"

  current_source="$(git -C "$REPO_DIR" rev-parse HEAD)"
  current_binary_sha="$(sha256_file "$BIN")"
  current_direct_sha="$(sha256_file "$DIRECT_DIR/manifest.txt")"
  current_observer_sha="$(sha256_file "$SCRIPT_DIR/knife15-exit-target-observer.sh")"
  [[ "$current_source" == "$candidate_source" ]] || \
    die "source commit does not match the candidate profile; evidence: $OUT_DIR"
  [[ "$current_binary_sha" == "$candidate_binary_sha" ]] || \
    die "binary hash does not match the candidate profile; evidence: $OUT_DIR"
  [[ "$current_direct_sha" == "$candidate_direct_sha" ]] || \
    die "direct profile hash does not match the candidate profile; evidence: $OUT_DIR"
  [[ "$current_observer_sha" == "$candidate_observer_sha" ]] || \
    die "observer hash does not match the candidate profile; evidence: $OUT_DIR"

  "$ROUTE_BIN" -n get "$candidate_ip" >"$OUT_DIR/exit.route.txt" 2>&1 || \
    die "candidate route lookup failed; evidence: $OUT_DIR"
  "$ROUTE_BIN" -n get "$candidate_target" >"$OUT_DIR/target.route.txt" 2>&1 || \
    die "Target route lookup failed; evidence: $OUT_DIR"
  current_interface="$(awk '/interface:/ {print $2; exit}' \
    "$OUT_DIR/exit.route.txt")"
  target_interface="$(awk '/interface:/ {print $2; exit}' \
    "$OUT_DIR/target.route.txt")"
  [[ "$current_interface" == "$candidate_interface" && \
    "$target_interface" == "$candidate_interface" && \
    "$candidate_interface" != utun* ]] || \
    die "candidate/Target route is not on the profiled physical interface; evidence: $OUT_DIR"
  "$TRACEROUTE_BIN" -n -m 12 -q 1 -w 1 "$candidate_ip" \
    >"$OUT_DIR/exit.traceroute.txt" 2>&1 || true
  "$TRACEROUTE_BIN" -n -m 12 -q 1 -w 1 "$candidate_target" \
    >"$OUT_DIR/target.traceroute.txt" 2>&1 || true

  write_remote_probe "$OUT_DIR/remote.txt" "$candidate_port" || \
    die "candidate remote probe failed; evidence: $OUT_DIR"
  [[ "$(remote_value "$OUT_DIR/remote.txt" service_active)" == active && \
    "$(remote_value "$OUT_DIR/remote.txt" tuic_udp_listener)" == 1 ]] || \
    die "candidate TUIC service is not healthy; evidence: $OUT_DIR"
  remote_binary_sha="$(remote_value "$OUT_DIR/remote.txt" server_binary_sha256)"
  remote_config_sha="$(remote_value "$OUT_DIR/remote.txt" server_config_sha256)"
  [[ "$remote_binary_sha" == "$expected_server_binary_sha" && \
    "$remote_config_sha" == "$expected_server_config_sha" ]] || \
    die "candidate server hash does not match the profile; evidence: $OUT_DIR"

  if grep -E -i \
    '(password|secret|token|private[ _-]?key|access[ _-]?key)[[:space:]]*[:=]' \
    "$OUT_DIR"/*.json "$OUT_DIR"/*.txt \
    >"$OUT_DIR/secret-scan.txt" 2>/dev/null; then
    die "resource evidence contains a credential-like assignment; evidence: $OUT_DIR"
  fi
  printf '%s\n' 'PASS: no credential-like assignment found' \
    >"$OUT_DIR/secret-scan.txt"

  result="$OUT_DIR/result.txt"
  {
    echo 'schema=knife15-m2-resource-preflight-v1'
    echo 'status=pass'
    echo "candidate_id=$candidate_id"
    echo "candidate_ipv4=$candidate_ip"
    echo "candidate_tuic_port=$candidate_port"
    echo "target=$candidate_target"
    echo "target_iperf_port=$candidate_iperf_port"
    echo "physical_interface=$candidate_interface"
    echo "source_commit=$current_source"
    echo "binary_sha256=$current_binary_sha"
    echo "direct_manifest_sha256=$current_direct_sha"
    echo "observer_sha256=$current_observer_sha"
    echo "profile_helper_sha256=$(sha256_file "$PROFILE_HELPER")"
    echo "preflight_runner_sha256=$(sha256_file "$PREFLIGHT_RUNNER")"
    echo "eligibility_sha256=$(sha256_file "$OUT_DIR/eligibility.json")"
    echo "provider_identity_sha256=$(sha256_file "$OUT_DIR/provider-identity.txt")"
    echo "route_identity_sha256=$(sha256_file "$OUT_DIR/route-identity.txt")"
    echo "remote_sha256=$(sha256_file "$OUT_DIR/remote.txt")"
    echo "exit_route_sha256=$(sha256_file "$OUT_DIR/exit.route.txt")"
    echo "target_route_sha256=$(sha256_file "$OUT_DIR/target.route.txt")"
    echo "exit_traceroute_sha256=$(sha256_file "$OUT_DIR/exit.traceroute.txt")"
    echo "target_traceroute_sha256=$(sha256_file "$OUT_DIR/target.traceroute.txt")"
  } >"$result"
  : >"$OUT_DIR/SHA256SUMS"
  for evidence_file in \
    reference-profile.json candidate-profile.json eligibility.json \
    provider-identity.txt route-identity.txt direct-manifest.txt \
    exit.route.txt target.route.txt exit.traceroute.txt target.traceroute.txt \
    remote.txt remote.stderr secret-scan.txt result.txt; do
    printf '%s  %s\n' "$(sha256_file "$OUT_DIR/$evidence_file")" \
      "$evidence_file" >>"$OUT_DIR/SHA256SUMS"
  done
  if [[ "$eligibility_reason" == proved_saturation_replacement ]]; then
    for evidence_file in prior-saturation.txt replacement-capacity.txt; do
      printf '%s  %s\n' "$(sha256_file "$OUT_DIR/$evidence_file")" \
        "$evidence_file" >>"$OUT_DIR/SHA256SUMS"
    done
  fi
  chmod -R a-w "$OUT_DIR"
  archive="${OUT_DIR}.tar.gz"
  tar -C "$(dirname "$OUT_DIR")" -czf "$archive" "$(basename "$OUT_DIR")"
  shasum -a 256 "$archive" >"${archive}.sha256"
  PREFLIGHT_EVIDENCE_ACTIVE=0
  echo "PASS: distinct Knife15 M2 resource preflight completed"
  echo "resource_dir=$OUT_DIR"
  cat "${archive}.sha256"
}

case "${1:-}" in
  --self-test)
    runner_self_test
    ;;
  run)
    run_preflight
    ;;
  *)
    echo "Usage:" >&2
    echo "  bash scripts/knife15-m2-resource-preflight.sh --self-test" >&2
    echo "  bash scripts/knife15-m2-resource-preflight.sh run" >&2
    exit 64
    ;;
esac
