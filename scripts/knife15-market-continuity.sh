#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
ACTION="${1:---help}"

TARGET="${TARGET:-43.130.32.77}"
IPERF_PORT="${IPERF_PORT:-5201}"
MARKET_PROFILE_ROOT="${MARKET_PROFILE_ROOT:-${TMPDIR:-/tmp}}"
MARKET_CYCLES=6
MARKET_TCP_SECS=300
MARKET_UDP_SECS=180
MARKET_SHORT_SECS=10
MARKET_SHORT_COUNT=6

die() {
  echo "ERROR: $*" >&2
  exit 1
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
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

regular_file() {
  [[ -f "$1" && ! -L "$1" ]]
}

absolute_file_path() {
  local path="$1" parent base
  [[ "$path" == /* ]] || return 1
  parent="$(dirname "$path")"
  base="$(basename "$path")"
  [[ -d "$parent" && ! -L "$parent" && "$base" != . && "$base" != .. ]] || \
    return 1
  printf '%s/%s\n' "$(cd "$parent" && pwd -P)" "$base"
}

baseline_receiver_bps() {
  local json_file="$1"
  jq -er '
    (if .start.test_start.reverse == 0 then .server_output_json else . end)
    | .end.sum_received.bits_per_second
    | if type == "number" and . > 0 then floor
      else error("invalid receiver rate") end
  ' "$json_file" 2>/dev/null
}

baseline_continuity_is_valid() {
  local json_file="$1" reverse="$2"
  regular_file "$SCRIPT_DIR/knife15-market-iperf-summary.py" || return 1
  python3 "$SCRIPT_DIR/knife15-market-iperf-summary.py" \
    tcp --reverse "$reverse" "$json_file" 2>/dev/null | \
    jq -e '
      .schema == "knife15-market-iperf-summary-v1"
      and .protocol == "TCP"
      and .receiver_zero_intervals == 0
      and .complete_receiver_intervals > 0
      and .receiver_bits_per_second > 0
    ' >/dev/null 2>&1
}

profile_value() {
  local profile="$1" key="$2"
  awk -F= -v key="$key" '
    $1 == key {
      count++
      value = substr($0, length(key) + 2)
    }
    END {
      if (count != 1 || value == "") exit 1
      print value
    }
  ' "$profile"
}

create_profile() {
  local baseline_dir="$1" requested_output="$2"
  local output_file checksum_file output_parent tmp_profile tmp_checksum
  local forward_file reverse_file forward_bps reverse_bps source_commit
  validate_ipv4 "$TARGET" || return 1
  validate_port "$IPERF_PORT" || return 1
  [[ -d "$baseline_dir" && ! -L "$baseline_dir" ]] || return 1
  baseline_dir="$(cd "$baseline_dir" && pwd -P)" || return 1
  forward_file="$baseline_dir/direct-forward.json"
  reverse_file="$baseline_dir/direct-reverse.json"
  regular_file "$forward_file" && regular_file "$reverse_file" || return 1
  baseline_continuity_is_valid "$forward_file" 0 && \
    baseline_continuity_is_valid "$reverse_file" 1 || return 1
  output_file="$(absolute_file_path "$requested_output")" || return 1
  checksum_file="$output_file.sha256"
  [[ ! -e "$output_file" && ! -L "$output_file" && \
    ! -e "$checksum_file" && ! -L "$checksum_file" ]] || return 1
  forward_bps="$(baseline_receiver_bps "$forward_file")" || return 1
  reverse_bps="$(baseline_receiver_bps "$reverse_file")" || return 1
  [[ "$forward_bps" =~ ^[1-9][0-9]*$ && "$reverse_bps" =~ ^[1-9][0-9]*$ ]] || \
    return 1
  source_commit="$(git -C "$REPO" rev-parse HEAD 2>/dev/null)" || return 1
  output_parent="$(dirname "$output_file")"
  tmp_profile="$(mktemp "$output_parent/.knife15-market-profile.XXXXXX")" || return 1
  tmp_checksum="$(mktemp "$output_parent/.knife15-market-profile-sha.XXXXXX")" || {
    rm -f "$tmp_profile"
    return 1
  }
  chmod 0600 "$tmp_profile" "$tmp_checksum" || {
    rm -f "$tmp_profile" "$tmp_checksum"
    return 1
  }
  if ! printf '%s\n' \
    'schema=knife15-market-profile-v1' \
    "created_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "source_commit=$source_commit" \
    "runner_sha256=$(sha256_file "$SCRIPT_PATH")" \
    "summary_sha256=$(sha256_file "$SCRIPT_DIR/knife15-market-iperf-summary.py")" \
    "baseline_dir=$baseline_dir" \
    "baseline_forward_sha256=$(sha256_file "$forward_file")" \
    "baseline_reverse_sha256=$(sha256_file "$reverse_file")" \
    "target=$TARGET" \
    "iperf_port=$IPERF_PORT" \
    "baseline_forward_bps=$forward_bps" \
    "baseline_reverse_bps=$reverse_bps" \
    "forward_rate_bps=$((10#$forward_bps / 2))" \
    "reverse_rate_bps=$((10#$reverse_bps / 2))" \
    "udp_reverse_rate_bps=$((10#$reverse_bps / 2))" \
    "short_forward_rate_bps=$((10#$forward_bps * 4 / 5))" \
    "short_reverse_rate_bps=$((10#$reverse_bps * 4 / 5))" \
    "cycles=$MARKET_CYCLES" \
    "tcp_secs=$MARKET_TCP_SECS" \
    "udp_secs=$MARKET_UDP_SECS" \
    "short_secs=$MARKET_SHORT_SECS" \
    "short_count=$MARKET_SHORT_COUNT" \
    >"$tmp_profile"; then
    rm -f "$tmp_profile" "$tmp_checksum"
    return 1
  fi
  printf '%s  %s\n' "$(sha256_file "$tmp_profile")" "$(basename "$output_file")" \
    >"$tmp_checksum" || {
      rm -f "$tmp_profile" "$tmp_checksum"
      return 1
    }
  if ! mv "$tmp_profile" "$output_file"; then
    rm -f "$tmp_profile" "$tmp_checksum"
    return 1
  fi
  if ! mv "$tmp_checksum" "$checksum_file"; then
    rm -f "$output_file" "$tmp_checksum"
    return 1
  fi
}

verify_profile() {
  local requested_profile="$1" profile checksum_file baseline_dir
  local forward_file reverse_file forward_bps reverse_bps checksum_fields
  local expected_sha expected_name
  regular_file "$requested_profile" || return 1
  profile="$(absolute_file_path "$requested_profile")" || return 1
  checksum_file="$profile.sha256"
  regular_file "$checksum_file" || return 1
  checksum_fields="$(awk 'NF == 2 {count++; hash=$1; name=$2} END {
    if (count != 1 || hash !~ /^[0-9a-f]{64}$/) exit 1
    print hash "\t" name
  }' "$checksum_file")" || return 1
  IFS=$'\t' read -r expected_sha expected_name <<<"$checksum_fields"
  [[ "$expected_name" == "$(basename "$profile")" ]] || return 1
  [[ "$expected_sha" == "$(sha256_file "$profile")" ]] || return 1
  [[ "$(profile_value "$profile" schema)" == "knife15-market-profile-v1" ]] || \
    return 1
  [[ "$(profile_value "$profile" source_commit)" == \
    "$(git -C "$REPO" rev-parse HEAD 2>/dev/null)" ]] || return 1
  [[ "$(profile_value "$profile" runner_sha256)" == \
    "$(sha256_file "$SCRIPT_PATH")" ]] || return 1
  [[ "$(profile_value "$profile" summary_sha256)" == \
    "$(sha256_file "$SCRIPT_DIR/knife15-market-iperf-summary.py")" ]] || return 1
  [[ "$(profile_value "$profile" target)" == "$TARGET" ]] || return 1
  [[ "$(profile_value "$profile" iperf_port)" == "$IPERF_PORT" ]] || return 1
  baseline_dir="$(profile_value "$profile" baseline_dir)" || return 1
  [[ "$baseline_dir" == /* && -d "$baseline_dir" && ! -L "$baseline_dir" ]] || \
    return 1
  forward_file="$baseline_dir/direct-forward.json"
  reverse_file="$baseline_dir/direct-reverse.json"
  regular_file "$forward_file" && regular_file "$reverse_file" || return 1
  baseline_continuity_is_valid "$forward_file" 0 && \
    baseline_continuity_is_valid "$reverse_file" 1 || return 1
  [[ "$(profile_value "$profile" baseline_forward_sha256)" == \
    "$(sha256_file "$forward_file")" ]] || return 1
  [[ "$(profile_value "$profile" baseline_reverse_sha256)" == \
    "$(sha256_file "$reverse_file")" ]] || return 1
  forward_bps="$(baseline_receiver_bps "$forward_file")" || return 1
  reverse_bps="$(baseline_receiver_bps "$reverse_file")" || return 1
  [[ "$(profile_value "$profile" baseline_forward_bps)" == "$forward_bps" ]] || \
    return 1
  [[ "$(profile_value "$profile" baseline_reverse_bps)" == "$reverse_bps" ]] || \
    return 1
  [[ "$(profile_value "$profile" forward_rate_bps)" == \
    "$((10#$forward_bps / 2))" ]] || return 1
  [[ "$(profile_value "$profile" reverse_rate_bps)" == \
    "$((10#$reverse_bps / 2))" ]] || return 1
  [[ "$(profile_value "$profile" udp_reverse_rate_bps)" == \
    "$((10#$reverse_bps / 2))" ]] || return 1
  [[ "$(profile_value "$profile" short_forward_rate_bps)" == \
    "$((10#$forward_bps * 4 / 5))" ]] || return 1
  [[ "$(profile_value "$profile" short_reverse_rate_bps)" == \
    "$((10#$reverse_bps * 4 / 5))" ]] || return 1
  [[ "$(profile_value "$profile" cycles)" == "$MARKET_CYCLES" ]] || return 1
  [[ "$(profile_value "$profile" tcp_secs)" == "$MARKET_TCP_SECS" ]] || return 1
  [[ "$(profile_value "$profile" udp_secs)" == "$MARKET_UDP_SECS" ]] || return 1
  [[ "$(profile_value "$profile" short_secs)" == "$MARKET_SHORT_SECS" ]] || \
    return 1
  [[ "$(profile_value "$profile" short_count)" == "$MARKET_SHORT_COUNT" ]]
}

profile_action() {
  local baseline_dir="${1:-}" profile_root profile_dir profile_file
  [[ -n "$baseline_dir" ]] || die "profile requires BASELINE_DIR"
  [[ -d "$MARKET_PROFILE_ROOT" && ! -L "$MARKET_PROFILE_ROOT" ]] || \
    die "MARKET_PROFILE_ROOT must be an existing real directory"
  profile_root="$(cd "$MARKET_PROFILE_ROOT" && pwd -P)" || \
    die "cannot resolve MARKET_PROFILE_ROOT"
  profile_dir="$profile_root/mini_vpn_knife15_market_profile_$(date -u '+%Y%m%d_%H%M%S')"
  mkdir -m 0700 "$profile_dir" || die "cannot create market profile directory"
  profile_file="$profile_dir/profile.txt"
  if ! create_profile "$baseline_dir" "$profile_file"; then
    rmdir "$profile_dir" 2>/dev/null || true
    die "cannot create immutable market profile"
  fi
  printf 'profile=%s\nsha256=%s\n' \
    "$profile_file" "$(sha256_file "$profile_file")"
}

verify_profile_action() {
  local profile_file="${1:-}"
  [[ -n "$profile_file" ]] || die "verify-profile requires PROFILE_FILE"
  verify_profile "$profile_file" || die "market profile verification failed"
  printf 'PASS: market profile verified\nprofile=%s\nsha256=%s\n' \
    "$(absolute_file_path "$profile_file")" "$(sha256_file "$profile_file")"
}

self_test() {
  local tmp baseline_dir profile_file public_output public_profile bad_profile
  local zero_baseline
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/knife15-market-self-test.XXXXXX")"
  trap 'rm -rf "$tmp"' RETURN
  baseline_dir="$tmp/baseline"
  mkdir "$baseline_dir"
  jq '.server_output_json.end.sum_received.bits_per_second = 32000000
      | (.server_output_json.intervals[].sum.bits_per_second) = 32000000
      | (.server_output_json.intervals[].sum.bytes) = 4000000' \
    "$SCRIPT_DIR/fixtures/knife15-market/tcp-two-zero-runs.json" \
    >"$baseline_dir/direct-forward.json"
  jq '.end.sum_received.bits_per_second = 56000000' \
    "$SCRIPT_DIR/fixtures/knife15-market/tcp-partial-tail.json" \
    >"$baseline_dir/direct-reverse.json"
  profile_file="$tmp/market-profile.txt"
  create_profile "$baseline_dir" "$profile_file"
  grep -Fxq 'schema=knife15-market-profile-v1' "$profile_file"
  grep -Fxq 'target=43.130.32.77' "$profile_file"
  grep -Fxq 'iperf_port=5201' "$profile_file"
  grep -Fxq 'forward_rate_bps=16000000' "$profile_file"
  grep -Fxq 'reverse_rate_bps=28000000' "$profile_file"
  grep -Fxq 'short_forward_rate_bps=25600000' "$profile_file"
  grep -Fxq 'short_reverse_rate_bps=44800000' "$profile_file"
  grep -Fxq 'cycles=6' "$profile_file"
  verify_profile "$profile_file"
  if (TARGET=43.130.32.78; verify_profile "$profile_file"); then
    die "self-test: changed Target was accepted"
  fi
  if (IPERF_PORT=5202; verify_profile "$profile_file"); then
    die "self-test: changed iperf port was accepted"
  fi
  if create_profile "$baseline_dir" "$profile_file"; then
    die "self-test: existing profile was overwritten"
  fi
  zero_baseline="$tmp/zero-baseline"
  mkdir "$zero_baseline"
  cp "$SCRIPT_DIR/fixtures/knife15-market/tcp-two-zero-runs.json" \
    "$zero_baseline/direct-forward.json"
  cp "$baseline_dir/direct-reverse.json" "$zero_baseline/direct-reverse.json"
  if create_profile "$zero_baseline" "$tmp/zero-profile.txt"; then
    die "self-test: receiver-zero baseline was accepted"
  fi
  ln -s "$profile_file" "$tmp/profile-link"
  if verify_profile "$tmp/profile-link"; then
    die "self-test: symlinked profile was accepted"
  fi
  bad_profile="$tmp/copied-profile.txt"
  cp "$profile_file" "$bad_profile"
  printf '%s  %s\n' "$(sha256_file "$bad_profile")" 'wrong-profile.txt' \
    >"$bad_profile.sha256"
  if verify_profile "$bad_profile"; then
    die "self-test: checksum filename mismatch was accepted"
  fi
  mkdir "$tmp/public"
  public_output="$(MARKET_PROFILE_ROOT="$tmp/public" \
    /bin/bash "$SCRIPT_PATH" profile "$baseline_dir")"
  public_profile="$(awk -F= '$1 == "profile" {print substr($0, 9)}' \
    <<<"$public_output")"
  regular_file "$public_profile"
  MARKET_PROFILE_ROOT="$tmp/public" \
    /bin/bash "$SCRIPT_PATH" verify-profile "$public_profile" >/dev/null
  echo "knife15 market continuity self-test passed"
}

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/knife15-market-continuity.sh --self-test
  bash scripts/knife15-market-continuity.sh profile BASELINE_DIR
  bash scripts/knife15-market-continuity.sh verify-profile PROFILE_FILE
USAGE
}

case "$ACTION" in
  --self-test) self_test ;;
  profile) profile_action "${2:-}" ;;
  verify-profile) verify_profile_action "${2:-}" ;;
  --help|-h|help) usage ;;
  *) usage >&2; exit 64 ;;
esac
