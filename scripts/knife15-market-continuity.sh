#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
ACTION="${1:---help}"

TARGET="${TARGET:-43.130.32.77}"
IPERF_PORT="${IPERF_PORT:-5201}"
MARKET_PROFILE_ROOT="${MARKET_PROFILE_ROOT:-${TMPDIR:-/tmp}}"
MARKET_RUN_ROOT="${MARKET_RUN_ROOT:-${TMPDIR:-/tmp}}"
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

route_interface_from_text() {
  awk '
    $1 == "interface:" {count++; interface=$2}
    END {
      if (count != 1 || interface == "") exit 1
      print interface
    }
  '
}

route_contract_is_valid() {
  local target_text="$1" exit_text="$2" default_text="$3" dns_text="$4"
  local vpn_if="$5" physical_if="$6"
  local target_if exit_if default_if dns_if
  [[ "$vpn_if" =~ ^utun[0-9]+$ ]] || return 1
  [[ "$physical_if" =~ ^[a-z][a-z0-9]*$ && \
    "$physical_if" != utun* && "$physical_if" != lo0 ]] || return 1
  target_if="$(route_interface_from_text <<<"$target_text")" || return 1
  exit_if="$(route_interface_from_text <<<"$exit_text")" || return 1
  default_if="$(route_interface_from_text <<<"$default_text")" || return 1
  dns_if="$(route_interface_from_text <<<"$dns_text")" || return 1
  [[ "$target_if" == "$vpn_if" && "$dns_if" == "$vpn_if" ]] || return 1
  [[ "$exit_if" == "$physical_if" ]] || return 1
  [[ "$default_if" == "$vpn_if" || "$default_if" == "$physical_if" ]]
}

validate_client_identity() {
  local label="$1" version="$2" vpn_if="$3" physical_if="$4" exit_ipv4="$5"
  [[ "$label" =~ ^[A-Za-z0-9._-]+$ && ${#label} -le 64 ]] || return 1
  [[ -n "$version" && ${#version} -le 160 && \
    "$version" != *$'\n'* && "$version" != *$'\r'* && "$version" != *$'\t'* ]] || \
    return 1
  [[ "$vpn_if" =~ ^utun[0-9]+$ ]] || return 1
  [[ "$physical_if" =~ ^[a-z][a-z0-9]*$ && \
    "$physical_if" != utun* && "$physical_if" != lo0 ]] || return 1
  validate_ipv4 "$exit_ipv4"
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

preflight_fail() {
  local out_dir="$1" reason="$2"
  printf 'status=INVALID\nreason=%s\n' "$reason" >"$out_dir/failure.txt" 2>/dev/null || true
  return 1
}

perform_preflight() {
  local out_dir="$1" profile_file="$2" client_label="$3" client_version="$4"
  local vpn_if="$5" physical_if="$6" tuic_exit_ipv4="$7"
  local expected_egress_ipv4="$8" dns_target="$9" client_binary="${10}"
  local route_bin="${11}" ifconfig_bin="${12}" scutil_bin="${13}"
  local networksetup_bin="${14}" curl_bin="${15}" iperf_bin="${16}"
  local git_bin="${17}"
  local target_route_text exit_route_text default_route_text dns_route_text
  local target_if exit_if default_if dns_if egress source_commit client_binary_sha
  local network_service
  local command_path
  [[ -d "$out_dir" && ! -L "$out_dir" ]] || return 1
  if [[ -n "$(find "$out_dir" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    preflight_fail "$out_dir" output_directory_not_empty
    return 1
  fi
  validate_client_identity "$client_label" "$client_version" "$vpn_if" \
    "$physical_if" "$tuic_exit_ipv4" || {
    preflight_fail "$out_dir" invalid_client_identity
    return 1
  }
  validate_ipv4 "$expected_egress_ipv4" && validate_ipv4 "$dns_target" || {
    preflight_fail "$out_dir" invalid_network_identity
    return 1
  }
  [[ "$TARGET" != "$tuic_exit_ipv4" && "$TARGET" != "$dns_target" && \
    "$tuic_exit_ipv4" != "$dns_target" ]] || {
    preflight_fail "$out_dir" overlapping_network_identity
    return 1
  }
  for command_path in "$route_bin" "$ifconfig_bin" "$scutil_bin" \
    "$networksetup_bin" "$curl_bin" "$iperf_bin" "$git_bin"; do
    [[ -x "$command_path" ]] || {
      preflight_fail "$out_dir" required_command_unavailable
      return 1
    }
  done
  verify_profile "$profile_file" || {
    preflight_fail "$out_dir" profile_verification_failed
    return 1
  }
  "$git_bin" -C "$REPO" status --porcelain --untracked-files=all \
    >"$out_dir/git-status.txt" 2>"$out_dir/git-status.stderr" || {
    preflight_fail "$out_dir" git_status_failed
    return 1
  }
  [[ ! -s "$out_dir/git-status.txt" ]] || {
    preflight_fail "$out_dir" dirty_effective_source
    return 1
  }
  source_commit="$("$git_bin" -C "$REPO" rev-parse HEAD 2>/dev/null)" || {
    preflight_fail "$out_dir" source_identity_failed
    return 1
  }
  [[ "$source_commit" == "$(profile_value "$profile_file" source_commit)" ]] || {
    preflight_fail "$out_dir" source_profile_mismatch
    return 1
  }
  "$route_bin" -n get "$TARGET" >"$out_dir/route-target.txt" 2>&1 || {
    preflight_fail "$out_dir" target_route_unavailable
    return 1
  }
  "$route_bin" -n get "$tuic_exit_ipv4" >"$out_dir/route-exit.txt" 2>&1 || {
    preflight_fail "$out_dir" exit_route_unavailable
    return 1
  }
  "$route_bin" -n get default >"$out_dir/route-default.txt" 2>&1 || {
    preflight_fail "$out_dir" default_route_unavailable
    return 1
  }
  "$route_bin" -n get "$dns_target" >"$out_dir/route-dns.txt" 2>&1 || {
    preflight_fail "$out_dir" dns_route_unavailable
    return 1
  }
  target_route_text="$(sed -n '1,80p' "$out_dir/route-target.txt")"
  exit_route_text="$(sed -n '1,80p' "$out_dir/route-exit.txt")"
  default_route_text="$(sed -n '1,80p' "$out_dir/route-default.txt")"
  dns_route_text="$(sed -n '1,80p' "$out_dir/route-dns.txt")"
  route_contract_is_valid "$target_route_text" "$exit_route_text" \
    "$default_route_text" "$dns_route_text" "$vpn_if" "$physical_if" || {
    preflight_fail "$out_dir" route_contract_failed
    return 1
  }
  target_if="$(route_interface_from_text <<<"$target_route_text")"
  exit_if="$(route_interface_from_text <<<"$exit_route_text")"
  default_if="$(route_interface_from_text <<<"$default_route_text")"
  dns_if="$(route_interface_from_text <<<"$dns_route_text")"
  "$ifconfig_bin" "$vpn_if" >"$out_dir/ifconfig-vpn.txt" 2>&1 || {
    preflight_fail "$out_dir" vpn_interface_unavailable
    return 1
  }
  "$ifconfig_bin" "$physical_if" >"$out_dir/ifconfig-physical.txt" 2>&1 || {
    preflight_fail "$out_dir" physical_interface_unavailable
    return 1
  }
  "$scutil_bin" --proxy >"$out_dir/scutil-proxy.txt" 2>&1 || {
    preflight_fail "$out_dir" proxy_snapshot_failed
    return 1
  }
  "$networksetup_bin" -listnetworkserviceorder \
    >"$out_dir/network-service-order.txt" 2>&1 || {
    preflight_fail "$out_dir" network_service_snapshot_failed
    return 1
  }
  network_service="$(network_service_for_interface_from_text "$physical_if" \
    <"$out_dir/network-service-order.txt")" || {
    preflight_fail "$out_dir" physical_network_service_ambiguous
    return 1
  }
  "$curl_bin" -4 --fail --silent --show-error --max-time 15 \
    https://api.ipify.org >"$out_dir/public-egress.txt" 2>"$out_dir/public-egress.stderr" || {
    preflight_fail "$out_dir" public_egress_probe_failed
    return 1
  }
  egress="$(tr -d '\r\n' <"$out_dir/public-egress.txt")"
  [[ "$egress" == "$expected_egress_ipv4" ]] || {
    preflight_fail "$out_dir" public_egress_mismatch
    return 1
  }
  "$iperf_bin" -c "$TARGET" -p "$IPERF_PORT" -t 1 -P 1 \
    --connect-timeout 5000 --json --get-server-output \
    >"$out_dir/target-ready.json" 2>"$out_dir/target-ready.stderr" || {
    preflight_fail "$out_dir" target_readiness_command_failed
    return 1
  }
  jq -e --arg target "$TARGET" --argjson port "$IPERF_PORT" '
    (.error? // "") == ""
    and .start.connecting_to.host == $target
    and .start.connecting_to.port == $port
    and .start.test_start.protocol == "TCP"
    and .start.test_start.reverse == 0
    and .start.test_start.duration == 1
    and .server_output_json.start.test_start.protocol == "TCP"
    and .server_output_json.start.test_start.reverse == 0
    and .server_output_json.start.test_start.duration == 1
  ' "$out_dir/target-ready.json" >/dev/null 2>&1 || {
    preflight_fail "$out_dir" target_readiness_evidence_invalid
    return 1
  }
  python3 "$SCRIPT_DIR/knife15-market-iperf-summary.py" tcp --reverse 0 \
    "$out_dir/target-ready.json" >"$out_dir/target-ready-summary.json" 2>&1 || {
    preflight_fail "$out_dir" target_readiness_evidence_invalid
    return 1
  }
  jq -e '.receiver_zero_intervals == 0
    and .complete_receiver_intervals > 0
    and .receiver_bits_per_second > 0' \
    "$out_dir/target-ready-summary.json" >/dev/null 2>&1 || {
    preflight_fail "$out_dir" target_readiness_continuity_failed
    return 1
  }
  if [[ -n "$client_binary" ]]; then
    regular_file "$client_binary" || {
      preflight_fail "$out_dir" client_binary_invalid
      return 1
    }
    client_binary_sha="$(sha256_file "$client_binary")" || {
      preflight_fail "$out_dir" client_binary_hash_failed
      return 1
    }
  else
    client_binary_sha=unavailable
  fi
  cp "$profile_file" "$out_dir/profile.txt" && \
    printf '%s  profile.txt\n' "$(sha256_file "$out_dir/profile.txt")" \
      >"$out_dir/profile.txt.sha256" || {
    preflight_fail "$out_dir" profile_copy_failed
    return 1
  }
  printf '%s\n' \
    'schema=knife15-market-preflight-v1' \
    'status=PASS' \
    "created_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "source_commit=$source_commit" \
    "runner_sha256=$(sha256_file "$SCRIPT_PATH")" \
    "summary_sha256=$(sha256_file "$SCRIPT_DIR/knife15-market-iperf-summary.py")" \
    "profile_sha256=$(sha256_file "$profile_file")" \
    "client_label=$client_label" \
    "client_version=$client_version" \
    "client_binary_sha256=$client_binary_sha" \
    "target=$TARGET" \
    "iperf_port=$IPERF_PORT" \
    "tuic_exit_ipv4=$tuic_exit_ipv4" \
    "expected_egress_ipv4=$expected_egress_ipv4" \
    "public_egress_ipv4=$egress" \
    "dns_target=$dns_target" \
    "vpn_interface=$vpn_if" \
    "physical_interface=$physical_if" \
    "physical_network_service=$network_service" \
    "target_route=$target_if" \
    "exit_route=$exit_if" \
    "default_route=$default_if" \
    "dns_route=$dns_if" \
    >"$out_dir/manifest.txt" || {
    preflight_fail "$out_dir" manifest_write_failed
    return 1
  }
}

preferred_command() {
  local preferred="$1" name="$2"
  if [[ -x "$preferred" ]]; then
    printf '%s\n' "$preferred"
  else
    command -v "$name"
  fi
}

preflight_action() {
  local profile_file="${PROFILE_FILE:-}" client_label="${CLIENT_LABEL:-}"
  local client_version="${CLIENT_VERSION:-}" vpn_if="${EXPECTED_VPN_IF:-}"
  local physical_if="${PHYSICAL_IF:-}" tuic_exit_ipv4="${TUIC_EXIT_IPV4:-}"
  local expected_egress_ipv4="${EXPECTED_EXIT_IPV4:-}"
  local dns_target="${DNS_TARGET:-8.8.8.8}" client_binary="${CLIENT_BINARY:-}"
  local run_root out_dir route_bin ifconfig_bin scutil_bin networksetup_bin
  local curl_bin iperf_bin git_bin
  [[ -n "$profile_file" && -n "$client_label" && -n "$client_version" && \
    -n "$vpn_if" && -n "$physical_if" && -n "$tuic_exit_ipv4" && \
    -n "$expected_egress_ipv4" ]] || \
    die "preflight requires PROFILE_FILE, CLIENT_LABEL, CLIENT_VERSION, EXPECTED_VPN_IF, PHYSICAL_IF, TUIC_EXIT_IPV4, and EXPECTED_EXIT_IPV4"
  validate_client_identity "$client_label" "$client_version" "$vpn_if" \
    "$physical_if" "$tuic_exit_ipv4" || die "invalid external-client identity"
  validate_ipv4 "$expected_egress_ipv4" && validate_ipv4 "$dns_target" || \
    die "invalid preflight network identity"
  [[ -d "$MARKET_RUN_ROOT" && ! -L "$MARKET_RUN_ROOT" ]] || \
    die "MARKET_RUN_ROOT must be an existing real directory"
  run_root="$(cd "$MARKET_RUN_ROOT" && pwd -P)" || die "cannot resolve MARKET_RUN_ROOT"
  out_dir="$run_root/mini_vpn_knife15_market_preflight_${client_label}_$(date -u '+%Y%m%d_%H%M%S')"
  mkdir -m 0700 "$out_dir" || die "cannot create market preflight directory"
  route_bin="$(preferred_command /sbin/route route)" || \
    die "route command unavailable; evidence: $out_dir"
  ifconfig_bin="$(preferred_command /sbin/ifconfig ifconfig)" || \
    die "ifconfig command unavailable; evidence: $out_dir"
  scutil_bin="$(preferred_command /usr/sbin/scutil scutil)" || \
    die "scutil command unavailable; evidence: $out_dir"
  networksetup_bin="$(preferred_command /usr/sbin/networksetup networksetup)" || \
    die "networksetup command unavailable; evidence: $out_dir"
  curl_bin="$(preferred_command /usr/bin/curl curl)" || \
    die "curl command unavailable; evidence: $out_dir"
  iperf_bin="$(command -v iperf3)" || die "iperf3 unavailable; evidence: $out_dir"
  git_bin="$(command -v git)" || die "git unavailable; evidence: $out_dir"
  if ! perform_preflight "$out_dir" "$profile_file" "$client_label" \
    "$client_version" "$vpn_if" "$physical_if" "$tuic_exit_ipv4" \
    "$expected_egress_ipv4" "$dns_target" "$client_binary" "$route_bin" \
    "$ifconfig_bin" "$scutil_bin" "$networksetup_bin" "$curl_bin" \
    "$iperf_bin" "$git_bin"; then
    die "market external-client preflight failed; evidence: $out_dir"
  fi
  printf 'PASS: market external-client preflight\npreflight_dir=%s\n' "$out_dir"
}

self_test() {
  local tmp baseline_dir profile_file public_output public_profile bad_profile
  local zero_baseline target_route exit_route default_route dns_route
  local fake_bin fake_command preflight_dir invalid_preflight_dir current_commit
  local target_ready_json wrong_ready_json
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
  target_route=$'route to: 43.130.32.77\ninterface: utun9'
  exit_route=$'route to: 43.153.32.33\ninterface: en0'
  default_route=$'route to: default\ninterface: en0'
  dns_route=$'route to: 8.8.8.8\ninterface: utun9'
  route_contract_is_valid "$target_route" "$exit_route" "$default_route" \
    "$dns_route" utun9 en0 || die "self-test: valid route contract rejected"
  if route_contract_is_valid "$target_route" \
    $'route to: 43.153.32.33\ninterface: utun9' "$default_route" \
    "$dns_route" utun9 en0; then
    die "self-test: recursive Exit route accepted"
  fi
  if route_contract_is_valid \
    $'route to: 43.130.32.77\ninterface: en0' "$exit_route" \
    "$default_route" "$dns_route" utun9 en0; then
    die "self-test: Target bypass route accepted"
  fi
  if route_contract_is_valid "$target_route" "$exit_route" \
    $'route to: default\ninterface: utun99' "$dns_route" utun9 en0; then
    die "self-test: second default-route utun accepted"
  fi
  if route_contract_is_valid "$target_route" "$exit_route" \
    "$default_route" $'route to: 8.8.8.8\ninterface: en0' utun9 en0; then
    die "self-test: DNS bypass route accepted"
  fi
  validate_client_identity mihomo-tuic 'Mihomo 1.19.0' utun9 en0 \
    43.153.32.33 || die "self-test: valid client identity rejected"
  if validate_client_identity $'mihomo\nforged' '1.19.0' utun9 en0 \
    43.153.32.33; then
    die "self-test: unsafe client label accepted"
  fi
  if validate_client_identity mihomo-tuic '1.19.0' utun9 utun10 \
    43.153.32.33; then
    die "self-test: utun physical interface accepted"
  fi
  [[ "$(network_service_for_interface_from_text en0 <<'EOF_NETWORK'
(1) Wi-Fi
(Hardware Port: Wi-Fi, Device: en0)
(2) Thunderbolt Bridge
(Hardware Port: Thunderbolt Bridge, Device: bridge0)
EOF_NETWORK
)" == "Wi-Fi" ]] || die "self-test: physical network service parser mismatch"
  fake_bin="$tmp/fake-bin"
  fake_command="$fake_bin/fake-command"
  mkdir "$fake_bin"
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'case "$(basename "$0")" in' \
    '  route)' \
    '    destination="${3:-}"' \
    '    case "$destination" in' \
    '      43.130.32.77|8.8.8.8) interface=utun9 ;;' \
    '      43.153.32.33|default) interface=en0 ;;' \
    '      *) exit 1 ;;' \
    '    esac' \
    '    if [[ "${MARKET_TEST_ROUTE_MODE:-}" == target-bypass && "$destination" == 43.130.32.77 ]]; then interface=en0; fi' \
    '    printf "route to: %s\ninterface: %s\n" "$destination" "$interface"' \
    '    ;;' \
    '  ifconfig)' \
    '    [[ "${1:-}" == utun9 || "${1:-}" == en0 ]] || exit 1' \
    '    printf "%s: flags=8051<UP,RUNNING> mtu 1500\n" "$1"' \
    '    ;;' \
    '  scutil) printf "<dictionary> { HTTPEnable : 0 }\n" ;;' \
    '  networksetup)' \
    '    printf "(1) Wi-Fi\n(Hardware Port: Wi-Fi, Device: en0)\n"' \
    '    ;;' \
    '  curl) printf "%s\n" "${MARKET_TEST_EGRESS:-43.153.32.33}" ;;' \
    '  iperf3) /bin/cat "$MARKET_TEST_READY_JSON" ;;' \
    '  git)' \
    '    if [[ "$*" == *"status --porcelain --untracked-files=all"* ]]; then exit 0; fi' \
    '    if [[ "$*" == *"rev-parse HEAD"* ]]; then printf "%s\n" "$MARKET_TEST_COMMIT"; exit 0; fi' \
    '    exit 1' \
    '    ;;' \
    '  *) exit 1 ;;' \
    'esac' >"$fake_command"
  chmod 0700 "$fake_command"
  for fake_name in route ifconfig scutil networksetup curl iperf3 git; do
    ln -s "$fake_command" "$fake_bin/$fake_name"
  done
  current_commit="$(git -C "$REPO" rev-parse HEAD)"
  target_ready_json="$tmp/target-ready.json"
  jq '.start.connecting_to.host = "43.130.32.77"
      | .start.connecting_to.port = 5201
      | .start.test_start.duration = 1
      | .server_output_json.start.test_start.duration = 1
      | .intervals = [.intervals[0]]
      | .server_output_json.intervals = [.server_output_json.intervals[0]]' \
    "$baseline_dir/direct-forward.json" >"$target_ready_json"
  export MARKET_TEST_READY_JSON="$target_ready_json"
  export MARKET_TEST_COMMIT="$current_commit"
  preflight_dir="$tmp/preflight"
  mkdir "$preflight_dir"
  perform_preflight "$preflight_dir" "$profile_file" mihomo-tuic \
    'Mihomo 1.19.0' utun9 en0 43.153.32.33 43.153.32.33 8.8.8.8 '' \
    "$fake_bin/route" "$fake_bin/ifconfig" "$fake_bin/scutil" \
    "$fake_bin/networksetup" "$fake_bin/curl" "$fake_bin/iperf3" \
    "$fake_bin/git" || \
    die "self-test: valid external-client preflight rejected"
  grep -Fxq 'status=PASS' "$preflight_dir/manifest.txt"
  grep -Fxq 'client_label=mihomo-tuic' "$preflight_dir/manifest.txt"
  grep -Fxq 'target_route=utun9' "$preflight_dir/manifest.txt"
  grep -Fxq 'exit_route=en0' "$preflight_dir/manifest.txt"
  verify_profile "$preflight_dir/profile.txt" || \
    die "self-test: copied profile lost exact verification"
  invalid_preflight_dir="$tmp/preflight-target-bypass"
  mkdir "$invalid_preflight_dir"
  if (export MARKET_TEST_ROUTE_MODE=target-bypass; \
    perform_preflight "$invalid_preflight_dir" "$profile_file" mihomo-tuic \
      'Mihomo 1.19.0' utun9 en0 43.153.32.33 43.153.32.33 8.8.8.8 '' \
      "$fake_bin/route" "$fake_bin/ifconfig" "$fake_bin/scutil" \
      "$fake_bin/networksetup" "$fake_bin/curl" "$fake_bin/iperf3" \
      "$fake_bin/git"); then
    die "self-test: live Target bypass preflight accepted"
  fi
  grep -Fxq 'reason=route_contract_failed' "$invalid_preflight_dir/failure.txt"
  invalid_preflight_dir="$tmp/preflight-egress-mismatch"
  mkdir "$invalid_preflight_dir"
  if (export MARKET_TEST_EGRESS=43.153.32.34; \
    perform_preflight "$invalid_preflight_dir" "$profile_file" mihomo-tuic \
      'Mihomo 1.19.0' utun9 en0 43.153.32.33 43.153.32.33 8.8.8.8 '' \
      "$fake_bin/route" "$fake_bin/ifconfig" "$fake_bin/scutil" \
      "$fake_bin/networksetup" "$fake_bin/curl" "$fake_bin/iperf3" \
      "$fake_bin/git"); then
    die "self-test: wrong public egress preflight accepted"
  fi
  grep -Fxq 'reason=public_egress_mismatch' "$invalid_preflight_dir/failure.txt"
  wrong_ready_json="$tmp/wrong-target-ready.json"
  jq '.start.connecting_to.host = "43.130.32.78"' "$target_ready_json" \
    >"$wrong_ready_json"
  invalid_preflight_dir="$tmp/preflight-wrong-target-ready"
  mkdir "$invalid_preflight_dir"
  if (export MARKET_TEST_READY_JSON="$wrong_ready_json"; \
    perform_preflight "$invalid_preflight_dir" "$profile_file" mihomo-tuic \
      'Mihomo 1.19.0' utun9 en0 43.153.32.33 43.153.32.33 8.8.8.8 '' \
      "$fake_bin/route" "$fake_bin/ifconfig" "$fake_bin/scutil" \
      "$fake_bin/networksetup" "$fake_bin/curl" "$fake_bin/iperf3" \
      "$fake_bin/git"); then
    die "self-test: wrong Target readiness evidence accepted"
  fi
  grep -Fxq 'reason=target_readiness_evidence_invalid' \
    "$invalid_preflight_dir/failure.txt"
  echo "knife15 market continuity self-test passed"
}

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/knife15-market-continuity.sh --self-test
  bash scripts/knife15-market-continuity.sh profile BASELINE_DIR
  bash scripts/knife15-market-continuity.sh verify-profile PROFILE_FILE
  bash scripts/knife15-market-continuity.sh preflight
USAGE
}

case "$ACTION" in
  --self-test) self_test ;;
  profile) profile_action "${2:-}" ;;
  verify-profile) verify_profile_action "${2:-}" ;;
  preflight) preflight_action ;;
  --help|-h|help) usage ;;
  *) usage >&2; exit 64 ;;
esac
