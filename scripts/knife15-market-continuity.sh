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
MARKET_ACTIVE_COMMAND_PID=''
MARKET_ACTIVE_WATCHDOG_PID=''
MARKET_OBSERVER_ARMED=0
MARKET_OBSERVER_FINALIZE_ATTEMPTED=0
MARKET_OBSERVER_FINALIZATION_STATUS=not_started
MARKET_OBSERVER_REMOTE_RUN_DIR=''
MARKET_OBSERVER_BUNDLE=''
MARKET_OBSERVER_SHA256=''
MARKET_INTERRUPT_SIGNAL=''
MARKET_RUN_DIR=''
MARKET_OBSERVER_SCRIPT=''
MARKET_OBSERVER_TARGET=''
MARKET_OBSERVER_IPERF_PORT=''
MARKET_OBSERVER_TUIC_PORT=''
MARKET_MAC_BUNDLE=''
MARKET_MAC_SHA256=''

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
  [[ ! "$version" =~ [0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12} && \
    "$version" != *MINI_VPN_TUIC_UUID* && \
    "$version" != *MINI_VPN_TUIC_PASSWORD* ]] || return 1
  [[ "$vpn_if" =~ ^utun[0-9]+$ ]] || return 1
  [[ "$physical_if" =~ ^[a-z][a-z0-9]*$ && \
    "$physical_if" != utun* && "$physical_if" != lo0 ]] || return 1
  validate_ipv4 "$exit_ipv4"
}

validate_client_kind() {
  [[ "$1" == mature || "$1" == mini_vpn || "$1" == commercial ]]
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

monotonic_millis() {
  python3 -c 'import time; print(time.monotonic_ns() // 1_000_000)'
}

run_logged_with_timeout() {
  local output_file="$1" timeout_secs="$2" command_pid watchdog_pid status
  local timeout_marker
  shift 2
  [[ "$timeout_secs" =~ ^[1-9][0-9]*$ ]] || return 2
  timeout_marker="$output_file.timeout.$$"
  rm -f "$timeout_marker"
  (exec "$@" >"$output_file" 2>&1) &
  command_pid=$!
  MARKET_ACTIVE_COMMAND_PID="$command_pid"
  (
    sleep "$timeout_secs"
    kill -0 "$command_pid" 2>/dev/null || exit 0
    printf 'timeout\n' >"$timeout_marker"
    kill -TERM "$command_pid" 2>/dev/null || exit 0
    sleep 2
    kill -KILL "$command_pid" 2>/dev/null || true
  ) &
  watchdog_pid=$!
  MARKET_ACTIVE_WATCHDOG_PID="$watchdog_pid"
  if wait "$command_pid" 2>/dev/null; then
    status=0
  else
    status=$?
  fi
  kill -TERM "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  MARKET_ACTIVE_COMMAND_PID=''
  MARKET_ACTIVE_WATCHDOG_PID=''
  if [[ -f "$timeout_marker" ]]; then
    printf 'ERROR: command exceeded hard timeout of %ss\n' "$timeout_secs" \
      >>"$output_file"
    status=124
  fi
  rm -f "$timeout_marker"
  return "$status"
}

validate_phase_identity() {
  local json_file="$1" protocol="$2" reverse="$3" duration="$4"
  jq -e --arg target "$TARGET" --arg protocol "$protocol" \
    --argjson port "$IPERF_PORT" --argjson reverse "$reverse" \
    --argjson duration "$duration" '
    (.error? // "") == ""
    and .start.connecting_to.host == $target
    and .start.connecting_to.port == $port
    and .start.test_start.protocol == $protocol
    and .start.test_start.reverse == $reverse
    and .start.test_start.duration == $duration
    and .server_output_json.start.test_start.protocol == $protocol
    and .server_output_json.start.test_start.reverse == $reverse
    and .server_output_json.start.test_start.duration == $duration
  ' "$json_file" >/dev/null 2>&1
}

append_quality_event() {
  local run_dir="$1" cycle="$2" phase="$3" kind="$4" value="$5"
  local detail="$6" evidence="$7"
  [[ "$detail" != *$'\t'* && "$detail" != *$'\n'* ]] || return 1
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$phase" "$kind" \
    "$value" "$detail" "$evidence" >>"$run_dir/events.tsv"
}

run_market_phase() {
  local run_dir="$1" cycle="$2" phase="$3" protocol="$4" reverse="$5"
  local duration="$6" rate_bps="$7" iperf_bin="$8" timeout_grace="$9"
  local result_file summary_file timeout_secs start_utc end_utc start_ms end_ms
  local receiver_zero max_zero_run sender_zero gap_bytes loss_percent
  local protocol_cli
  local -a command
  [[ -z "$MARKET_INTERRUPT_SIGNAL" ]] || return 1
  result_file="$run_dir/cycle_$(printf '%03d' "$cycle")_${phase}.json"
  summary_file="$run_dir/cycle_$(printf '%03d' "$cycle")_${phase}.summary.json"
  timeout_secs=$((10#$duration + 10#$timeout_grace))
  command=("$iperf_bin" -c "$TARGET" -p "$IPERF_PORT" -t "$duration" -P 1
    --connect-timeout 5000 -b "$rate_bps" --json --get-server-output)
  [[ "$reverse" == "0" ]] || command+=(-R)
  if [[ "$protocol" == UDP ]]; then
    command+=(-u -l 1160)
  fi
  start_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  start_ms="$(monotonic_millis)" || return 1
  if ! run_logged_with_timeout "$result_file" "$timeout_secs" "${command[@]}"; then
    printf '%s\t%s\t%s\tcommand_failed\t%s\n' \
      "$start_utc" "$cycle" "$phase" "$(basename "$result_file")" \
      >>"$run_dir/invalid.tsv"
    return 1
  fi
  end_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  end_ms="$(monotonic_millis)" || return 1
  validate_phase_identity "$result_file" "$protocol" "$reverse" "$duration" || {
    printf '%s\t%s\t%s\tidentity_mismatch\t%s\n' \
      "$end_utc" "$cycle" "$phase" "$(basename "$result_file")" \
      >>"$run_dir/invalid.tsv"
    return 1
  }
  if [[ "$protocol" == TCP ]]; then protocol_cli=tcp; else protocol_cli=udp; fi
  python3 "$SCRIPT_DIR/knife15-market-iperf-summary.py" \
    "$protocol_cli" --reverse "$reverse" "$result_file" >"$summary_file" 2>&1 || {
    printf '%s\t%s\t%s\tresult_invalid\t%s\n' \
      "$end_utc" "$cycle" "$phase" "$(basename "$result_file")" \
      >>"$run_dir/invalid.tsv"
    return 1
  }
  jq -e --argjson duration "$duration" '
    .complete_receiver_intervals >= ($duration - 1)
    and .complete_receiver_intervals <= ($duration + 1)
    and .complete_sender_intervals >= ($duration - 1)
    and .complete_sender_intervals <= ($duration + 1)
  ' "$summary_file" >/dev/null 2>&1 || {
    printf '%s\t%s\t%s\tincomplete_intervals\t%s\n' \
      "$end_utc" "$cycle" "$phase" "$(basename "$summary_file")" \
      >>"$run_dir/invalid.tsv"
    return 1
  }
  receiver_zero="$(jq -er '.receiver_zero_intervals' "$summary_file")" || return 1
  max_zero_run="$(jq -er '.max_consecutive_receiver_zero_intervals' \
    "$summary_file")" || return 1
  sender_zero="$(jq -er '.sender_zero_intervals' "$summary_file")" || return 1
  if ((10#$receiver_zero > 0)); then
    append_quality_event "$run_dir" "$cycle" "$phase" receiver_zero_interval \
      "$receiver_zero" "max_consecutive=$max_zero_run" \
      "$(basename "$summary_file")" || return 1
  fi
  if ((10#$sender_zero > 0)); then
    append_quality_event "$run_dir" "$cycle" "$phase" sender_zero_interval \
      "$sender_zero" observed "$(basename "$summary_file")" || return 1
  fi
  if [[ "$protocol" == TCP ]]; then
    gap_bytes="$(jq -er '.sender_receiver_gap_bytes' "$summary_file")" || return 1
    if ((10#$gap_bytes > 16777216)); then
      append_quality_event "$run_dir" "$cycle" "$phase" \
        tcp_sender_receiver_gap_bytes "$gap_bytes" above_16MiB \
        "$(basename "$summary_file")" || return 1
    fi
  else
    loss_percent="$(jq -er '.lost_percent' "$summary_file")" || return 1
    if awk -v loss="$loss_percent" 'BEGIN {exit !(loss > 3.0)}'; then
      append_quality_event "$run_dir" "$cycle" "$phase" udp_loss_percent \
        "$loss_percent" above_3_percent "$(basename "$summary_file")" || return 1
    fi
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$start_utc" "$end_utc" "$start_ms" "$end_ms" "$cycle" "$phase" \
    "$protocol" "$reverse" "$duration" "$rate_bps" \
    "$(basename "$result_file")" "$(basename "$summary_file")" valid \
    >>"$run_dir/phases.tsv"
}

validate_workload_value() {
  local value="$1" maximum="$2"
  [[ "$value" =~ ^[1-9][0-9]*$ && 10#$value -le 10#$maximum ]]
}

validate_dns_name() {
  local name="$1"
  [[ -n "$name" && ${#name} -le 253 && "$name" =~ ^[A-Za-z0-9.-]+$ && \
    "$name" != .* && "$name" != *. && "$name" != *..* ]]
}

run_cycle_probes() {
  local run_dir="$1" cycle="$2" dig_bin="$3" curl_bin="$4"
  local dns_target="$5" dns_name="$6" expected_egress="$7"
  local route_bin="$8" vpn_if="$9" physical_if="${10}" tuic_exit_ipv4="${11}"
  local dns_file egress_file dns_ipv4='' line egress timestamp prefix
  local target_route_text exit_route_text default_route_text dns_route_text
  [[ -z "$MARKET_INTERRUPT_SIGNAL" ]] || return 1
  prefix="$run_dir/cycle_$(printf '%03d' "$cycle")"
  dns_file="$run_dir/cycle_$(printf '%03d' "$cycle")_dns.txt"
  egress_file="$run_dir/cycle_$(printf '%03d' "$cycle")_egress.txt"
  "$route_bin" -n get "$TARGET" >"${prefix}_route-target.txt" 2>&1 && \
    "$route_bin" -n get "$tuic_exit_ipv4" >"${prefix}_route-exit.txt" 2>&1 && \
    "$route_bin" -n get default >"${prefix}_route-default.txt" 2>&1 && \
    "$route_bin" -n get "$dns_target" >"${prefix}_route-dns.txt" 2>&1 || {
    printf '%s\t%s\tcycle-probes\troute_snapshot_failed\t%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" \
      "$(basename "$prefix")" >>"$run_dir/invalid.tsv"
    return 1
  }
  target_route_text="$(sed -n '1,80p' "${prefix}_route-target.txt")"
  exit_route_text="$(sed -n '1,80p' "${prefix}_route-exit.txt")"
  default_route_text="$(sed -n '1,80p' "${prefix}_route-default.txt")"
  dns_route_text="$(sed -n '1,80p' "${prefix}_route-dns.txt")"
  route_contract_is_valid "$target_route_text" "$exit_route_text" \
    "$default_route_text" "$dns_route_text" "$vpn_if" "$physical_if" || {
    printf '%s\t%s\tcycle-probes\troute_contract_failed\t%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" \
      "$(basename "$prefix")" >>"$run_dir/invalid.tsv"
    return 1
  }
  if ! run_logged_with_timeout "$dns_file" 10 "$dig_bin" \
    +time=5 +tries=1 +short A "$dns_name" "@$dns_target"; then
    printf '%s\t%s\tcycle-probes\tdns_command_failed\t%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$(basename "$dns_file")" \
      >>"$run_dir/invalid.tsv"
    return 1
  fi
  while IFS= read -r line; do
    if validate_ipv4 "$line"; then
      dns_ipv4="$line"
      break
    fi
  done <"$dns_file"
  [[ -n "$dns_ipv4" ]] || {
    printf '%s\t%s\tcycle-probes\tdns_evidence_invalid\t%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$(basename "$dns_file")" \
      >>"$run_dir/invalid.tsv"
    return 1
  }
  if ! run_logged_with_timeout "$egress_file" 20 "$curl_bin" -4 --fail \
    --silent --show-error --max-time 15 https://api.ipify.org; then
    printf '%s\t%s\tcycle-probes\tegress_command_failed\t%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" \
      "$(basename "$egress_file")" >>"$run_dir/invalid.tsv"
    return 1
  fi
  egress="$(tr -d '\r\n' <"$egress_file")"
  [[ "$egress" == "$expected_egress" ]] || {
    printf '%s\t%s\tcycle-probes\tegress_identity_mismatch\t%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$cycle" \
      "$(basename "$egress_file")" >>"$run_dir/invalid.tsv"
    return 1
  }
  timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf '%s\t%s\t%s\t%s\t%s\n' "$timestamp" "$cycle" "$dns_name" \
    "$dns_ipv4" "$egress" >>"$run_dir/probes.tsv"
}

run_market_workload() {
  local run_dir="$1" profile_file="$2" iperf_bin="$3" timeout_grace="$4"
  local dig_bin="$5" curl_bin="$6" dns_target="$7" dns_name="$8"
  local expected_egress="$9"
  local route_bin="${10}" vpn_if="${11}" physical_if="${12}"
  local tuic_exit_ipv4="${13}"
  local cycles tcp_secs udp_secs short_secs short_count forward_rate reverse_rate
  local udp_rate short_forward_rate short_reverse_rate cycle short_index
  local phase reverse rate event_count
  [[ -d "$run_dir" && ! -L "$run_dir" && -x "$iperf_bin" && \
    -x "$dig_bin" && -x "$curl_bin" && -x "$route_bin" ]] || return 1
  validate_ipv4 "$dns_target" && validate_dns_name "$dns_name" && \
    validate_ipv4 "$expected_egress" && validate_client_identity market-health \
      internal "$vpn_if" "$physical_if" "$tuic_exit_ipv4" || return 1
  [[ "$timeout_grace" =~ ^[0-9]+$ && 10#$timeout_grace -le 30 ]] || return 1
  cycles="$(profile_value "$profile_file" cycles)" || return 1
  tcp_secs="$(profile_value "$profile_file" tcp_secs)" || return 1
  udp_secs="$(profile_value "$profile_file" udp_secs)" || return 1
  short_secs="$(profile_value "$profile_file" short_secs)" || return 1
  short_count="$(profile_value "$profile_file" short_count)" || return 1
  forward_rate="$(profile_value "$profile_file" forward_rate_bps)" || return 1
  reverse_rate="$(profile_value "$profile_file" reverse_rate_bps)" || return 1
  udp_rate="$(profile_value "$profile_file" udp_reverse_rate_bps)" || return 1
  short_forward_rate="$(profile_value "$profile_file" short_forward_rate_bps)" || \
    return 1
  short_reverse_rate="$(profile_value "$profile_file" short_reverse_rate_bps)" || \
    return 1
  validate_workload_value "$cycles" 6 && validate_workload_value "$tcp_secs" 300 && \
    validate_workload_value "$udp_secs" 180 && \
    validate_workload_value "$short_secs" 10 && \
    validate_workload_value "$short_count" 6 || return 1
  for rate in "$forward_rate" "$reverse_rate" "$udp_rate" \
    "$short_forward_rate" "$short_reverse_rate"; do
    validate_workload_value "$rate" 1000000000000 || return 1
  done
  printf '%s\n' RUNNING >"$run_dir/status"
  printf 'timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence\n' \
    >"$run_dir/events.tsv"
  printf 'timestamp\tcycle\tphase\treason\tevidence\n' >"$run_dir/invalid.tsv"
  printf 'start_utc\tend_utc\tstart_monotonic_ms\tend_monotonic_ms\tcycle\tphase\tprotocol\treverse\tduration_secs\trate_bps\tresult\tsummary\tstatus\n' \
    >"$run_dir/phases.tsv"
  printf 'timestamp\tcycle\tdns_name\tdns_ipv4\tpublic_egress_ipv4\n' \
    >"$run_dir/probes.tsv"
  for ((cycle = 1; cycle <= 10#$cycles; cycle++)); do
    run_market_phase "$run_dir" "$cycle" tcp-forward TCP 0 "$tcp_secs" \
      "$forward_rate" "$iperf_bin" "$timeout_grace" || {
      printf '%s\n' INVALID >"$run_dir/status"
      return 1
    }
    run_market_phase "$run_dir" "$cycle" tcp-reverse TCP 1 "$tcp_secs" \
      "$reverse_rate" "$iperf_bin" "$timeout_grace" || {
      printf '%s\n' INVALID >"$run_dir/status"
      return 1
    }
    run_market_phase "$run_dir" "$cycle" udp-reverse UDP 1 "$udp_secs" \
      "$udp_rate" "$iperf_bin" "$timeout_grace" || {
      printf '%s\n' INVALID >"$run_dir/status"
      return 1
    }
    for ((short_index = 1; short_index <= 10#$short_count; short_index++)); do
      if ((short_index % 2 == 1)); then
        phase="short-forward-$short_index"
        reverse=0
        rate="$short_forward_rate"
      else
        phase="short-reverse-$short_index"
        reverse=1
        rate="$short_reverse_rate"
      fi
      run_market_phase "$run_dir" "$cycle" "$phase" TCP "$reverse" \
        "$short_secs" "$rate" "$iperf_bin" "$timeout_grace" || {
        printf '%s\n' INVALID >"$run_dir/status"
        return 1
      }
    done
    run_cycle_probes "$run_dir" "$cycle" "$dig_bin" "$curl_bin" \
      "$dns_target" "$dns_name" "$expected_egress" "$route_bin" "$vpn_if" \
      "$physical_if" "$tuic_exit_ipv4" || {
      printf '%s\n' INVALID >"$run_dir/status"
      return 1
    }
  done
  event_count="$(awk 'END {print NR-1}' "$run_dir/events.tsv")"
  if ((10#$event_count > 0)); then
    printf '%s\n' PASS_WITH_EVENTS >"$run_dir/status"
  else
    printf '%s\n' PASS_NO_EVENTS >"$run_dir/status"
  fi
}

value_from_text() {
  local text="$1" key="$2"
  awk -F= -v key="$key" '
    $1 == key {
      count++
      value = substr($0, length(key) + 2)
    }
    END {
      if (count != 1 || value == "") exit 1
      print value
    }
  ' <<<"$text"
}

observer_status_identity_matches() {
  local status_text="$1" target="$2" iperf_port="$3" tuic_port="$4"
  local expected_run_dir="$5"
  [[ "$(value_from_text "$status_text" schema)" == \
    knife15-exit-target-observer-v2 ]] || return 1
  [[ "$(value_from_text "$status_text" run_dir)" == "$expected_run_dir" ]] || \
    return 1
  [[ "$(value_from_text "$status_text" target)" == "$target" ]] || return 1
  [[ "$(value_from_text "$status_text" iperf_port)" == "$iperf_port" ]] || \
    return 1
  [[ "$(value_from_text "$status_text" tuic_port)" == "$tuic_port" ]] || \
    return 1
  [[ "$(value_from_text "$status_text" timeout_secs)" == 93600 ]]
}

observer_status_is_valid() {
  local status_text="$1" target="$2" iperf_port="$3" tuic_port="$4"
  local expected_run_dir="$5" elapsed drops
  observer_status_identity_matches "$status_text" "$target" "$iperf_port" \
    "$tuic_port" "$expected_run_dir" || return 1
  [[ "$(value_from_text "$status_text" status)" == active ]] || return 1
  [[ "$(value_from_text "$status_text" observer_healthy)" == 1 ]] || return 1
  [[ "$(value_from_text "$status_text" tcpdump_live)" == 1 ]] || return 1
  [[ "$(value_from_text "$status_text" sampler_live)" == 1 ]] || return 1
  [[ "$(value_from_text "$status_text" counter_sampler_live)" == 1 ]] || return 1
  elapsed="$(value_from_text "$status_text" elapsed_secs)" || return 1
  [[ "$elapsed" =~ ^[0-9]+$ && 10#$elapsed -le 900 ]] || return 1
  drops="$(value_from_text "$status_text" capture_kernel_drops)" || return 1
  [[ "$drops" == unknown || "$drops" == 0 ]]
}

observer_call() {
  local observer_script="$1" action="$2" target="$3" iperf_port="$4"
  local tuic_port="$5"
  regular_file "$observer_script" || return 1
  TARGET="$target" IPERF_PORT="$iperf_port" TUIC_PORT="$tuic_port" \
    OBSERVER_TIMEOUT_SECS=93600 /bin/bash "$observer_script" "$action"
}

observer_call_to_file() {
  local output_file="$1" timeout_secs="$2" observer_script="$3" action="$4"
  local target="$5" iperf_port="$6" tuic_port="$7"
  regular_file "$observer_script" || return 1
  run_logged_with_timeout "$output_file" "$timeout_secs" /usr/bin/env \
    "TARGET=$target" "IPERF_PORT=$iperf_port" "TUIC_PORT=$tuic_port" \
    OBSERVER_TIMEOUT_SECS=93600 /bin/bash "$observer_script" "$action"
}

start_observer_and_bind() {
  local evidence_dir="$1" observer_script="$2" target="$3" iperf_port="$4"
  local tuic_port="$5" start_text status_text remote_run_dir
  [[ -d "$evidence_dir" && ! -L "$evidence_dir" ]] || return 1
  MARKET_OBSERVER_ARMED=0
  MARKET_OBSERVER_FINALIZE_ATTEMPTED=0
  MARKET_OBSERVER_FINALIZATION_STATUS=not_started
  MARKET_OBSERVER_REMOTE_RUN_DIR=''
  MARKET_OBSERVER_BUNDLE=''
  MARKET_OBSERVER_SHA256=''
  if observer_call_to_file "$evidence_dir/observer-pre-start-status.txt" 30 \
    "$observer_script" status "$target" "$iperf_port" "$tuic_port"; then
    printf 'reason=preexisting_observer_state\n' \
      >"$evidence_dir/observer-start-failure.txt"
    return 1
  fi
  if ! observer_call_to_file "$evidence_dir/observer-start.txt" 60 \
    "$observer_script" start "$target" "$iperf_port" "$tuic_port"; then
    printf 'reason=observer_start_failed\n' \
      >"$evidence_dir/observer-start-failure.txt"
    return 1
  fi
  start_text="$(sed -n '1,40p' "$evidence_dir/observer-start.txt")"
  remote_run_dir="$(value_from_text "$start_text" run_dir)" || return 1
  [[ "$remote_run_dir" =~ ^/tmp/mini_vpn_knife15_exit_target_observer_[0-9]{8}_[0-9]{6}$ ]] || \
    return 1
  MARKET_OBSERVER_REMOTE_RUN_DIR="$remote_run_dir"
  MARKET_OBSERVER_ARMED=1
  grep -Fxq 'PASS: Exit observer started' \
    "$evidence_dir/observer-start.txt" || return 1
  printf '%s\n' \
    'schema=knife15-market-observer-binding-v1' \
    "bound_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "observer_script_sha256=$(sha256_file "$observer_script")" \
    "remote_run_dir=$remote_run_dir" \
    "target=$target" \
    "iperf_port=$iperf_port" \
    "tuic_port=$tuic_port" \
    >"$evidence_dir/observer-binding.txt" || return 1
  if ! observer_call_to_file "$evidence_dir/observer-start-status.txt" 30 \
    "$observer_script" status "$target" "$iperf_port" "$tuic_port"; then
    return 1
  fi
  status_text="$(sed -n '1,80p' "$evidence_dir/observer-start-status.txt")"
  observer_status_is_valid "$status_text" "$target" "$iperf_port" \
    "$tuic_port" "$remote_run_dir"
}

finalize_bound_observer() {
  local evidence_dir="$1" observer_script="$2" target="$3" iperf_port="$4"
  local tuic_port="$5" reason="$6" final_file status_text freeze_text bundle_text
  local bundle sha256 frozen_status_text capture_drops finalization_valid=1
  if [[ "$MARKET_OBSERVER_ARMED" != 1 ]]; then
    [[ "$MARKET_OBSERVER_FINALIZATION_STATUS" == complete ]]
    return
  fi
  if [[ "$MARKET_OBSERVER_FINALIZE_ATTEMPTED" == 1 ]]; then
    [[ "$MARKET_OBSERVER_FINALIZATION_STATUS" == complete ]]
    return
  fi
  MARKET_OBSERVER_FINALIZE_ATTEMPTED=1
  MARKET_OBSERVER_FINALIZATION_STATUS=failed
  final_file="$evidence_dir/observer-finalization.txt"
  printf '%s\n' \
    'schema=knife15-market-observer-finalization-v1' \
    "started_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "reason=$reason" >"$final_file" || return 1
  if ! observer_call_to_file "$evidence_dir/observer-final-status.txt" 30 \
    "$observer_script" status "$target" "$iperf_port" "$tuic_port"; then
    printf 'finalization_status=status_failed\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  fi
  status_text="$(sed -n '1,80p' "$evidence_dir/observer-final-status.txt")"
  if ! observer_status_identity_matches "$status_text" "$target" "$iperf_port" \
    "$tuic_port" "$MARKET_OBSERVER_REMOTE_RUN_DIR"; then
    printf 'finalization_status=identity_mismatch\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  fi
  if ! observer_call_to_file "$evidence_dir/observer-freeze.txt" 60 \
    "$observer_script" freeze "$target" "$iperf_port" "$tuic_port"; then
    printf 'finalization_status=freeze_failed\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  fi
  freeze_text="$(sed -n '1,40p' "$evidence_dir/observer-freeze.txt")"
  if [[ "$(value_from_text "$freeze_text" run_dir)" != \
    "$MARKET_OBSERVER_REMOTE_RUN_DIR" ]] || \
    ! grep -Fxq 'PASS: Exit observer frozen' "$evidence_dir/observer-freeze.txt"; then
    printf 'finalization_status=freeze_identity_mismatch\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  fi
  if ! observer_call_to_file "$evidence_dir/observer-frozen-status.txt" 30 \
    "$observer_script" status "$target" "$iperf_port" "$tuic_port"; then
    finalization_valid=0
  else
    frozen_status_text="$(sed -n '1,80p' \
      "$evidence_dir/observer-frozen-status.txt")"
    if ! observer_status_identity_matches "$frozen_status_text" "$target" \
      "$iperf_port" "$tuic_port" "$MARKET_OBSERVER_REMOTE_RUN_DIR"; then
      finalization_valid=0
    else
      capture_drops="$(value_from_text "$frozen_status_text" \
        capture_kernel_drops 2>/dev/null || echo unknown)"
      [[ "$capture_drops" == 0 ]] || finalization_valid=0
    fi
  fi
  if ! observer_call_to_file "$evidence_dir/observer-bundle.txt" 120 \
    "$observer_script" bundle "$target" "$iperf_port" "$tuic_port"; then
    printf 'finalization_status=bundle_failed\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  fi
  bundle_text="$(sed -n '1,40p' "$evidence_dir/observer-bundle.txt")"
  grep -Fxq 'PASS: Exit observer bundle finalized' \
    "$evidence_dir/observer-bundle.txt" || {
    printf 'finalization_status=bundle_evidence_invalid\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  }
  bundle="$(value_from_text "$bundle_text" bundle)" || {
    printf 'finalization_status=bundle_path_missing\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  }
  sha256="$(value_from_text "$bundle_text" sha256)" || {
    printf 'finalization_status=bundle_sha_missing\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  }
  [[ "$bundle" =~ ^/tmp/mini_vpn_knife15_exit_target_observer_[0-9]{8}_[0-9]{6}[.]tar[.]gz$ && \
    "$sha256" =~ ^[0-9a-f]{64}$ ]] || {
    printf 'finalization_status=bundle_identity_invalid\n' >>"$final_file"
    MARKET_OBSERVER_ARMED=0
    return 1
  }
  MARKET_OBSERVER_BUNDLE="$bundle"
  MARKET_OBSERVER_SHA256="$sha256"
  MARKET_OBSERVER_ARMED=0
  if ((finalization_valid == 1)); then
    MARKET_OBSERVER_FINALIZATION_STATUS=complete
  else
    MARKET_OBSERVER_FINALIZATION_STATUS=evidence_invalid
  fi
  printf '%s\n' \
    "observer_bundle=$bundle" \
    "observer_sha256=$sha256" \
    "finalization_status=$MARKET_OBSERVER_FINALIZATION_STATUS" >>"$final_file"
  ((finalization_valid == 1))
}

market_secret_scan() {
  local run_dir="$1" result_file="$run_dir/secret-scan.txt" scan_status
  grep -Erq --exclude='secret-scan.txt' --exclude='SHA256SUMS' -- \
    'MINI_VPN_TUIC_(UUID|PASSWORD)[=:]|BEGIN [A-Z ]*PRIVATE KEY|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' \
    "$run_dir" 2>/dev/null
  scan_status=$?
  case "$scan_status" in
    0)
      printf 'FAIL: secret-shaped material found; bundle not created\n' \
        >"$result_file"
      return 1
      ;;
    1)
      printf 'PASS: no credential names, private-key markers, or UUID-shaped values found\n' \
        >"$result_file"
      ;;
    *)
      printf 'FAIL: secret scan could not read every input\n' >"$result_file"
      return 1
      ;;
  esac
}

market_bundle_is_valid() {
  local run_dir="$1" bundle checksum_file
  local fields expected name
  bundle="$run_dir.tar.gz"
  checksum_file="$run_dir.tar.gz.sha256"
  regular_file "$bundle" && regular_file "$checksum_file" || return 1
  fields="$(awk 'NF == 2 {count++; print $1 "\t" $2} END {
    if (count != 1) exit 1
  }' "$checksum_file")" || return 1
  IFS=$'\t' read -r expected name <<<"$fields"
  [[ "$expected" =~ ^[0-9a-f]{64}$ && "$name" == "$bundle" ]] || return 1
  [[ "$expected" == "$(sha256_file "$bundle")" ]]
}

publish_market_bundle() {
  local run_dir="$1" bundle checksum_file
  local parent base tmp_bundle tmp_checksum sums_tmp checksum relative hash
  bundle="$run_dir.tar.gz"
  checksum_file="$run_dir.tar.gz.sha256"
  [[ -d "$run_dir" && ! -L "$run_dir" ]] || return 1
  if market_bundle_is_valid "$run_dir"; then
    MARKET_MAC_BUNDLE="$bundle"
    MARKET_MAC_SHA256="$(sha256_file "$bundle")"
    return 0
  fi
  [[ ! -e "$bundle" && ! -L "$bundle" && ! -e "$checksum_file" && \
    ! -L "$checksum_file" ]] || return 1
  market_secret_scan "$run_dir" || return 1
  parent="$(dirname "$run_dir")"
  base="$(basename "$run_dir")"
  sums_tmp="$(mktemp "$parent/.knife15-market-sums.XXXXXX")" || return 1
  if ! (
    cd "$run_dir"
    while IFS= read -r relative; do
      hash="$(sha256_file "$relative")" || exit 1
      printf '%s  %s\n' "$hash" "$relative"
    done < <(find . -type f ! -name SHA256SUMS | LC_ALL=C sort)
  ) >"$sums_tmp"; then
    rm -f "$sums_tmp"
    return 1
  fi
  mv "$sums_tmp" "$run_dir/SHA256SUMS" || {
    rm -f "$sums_tmp"
    return 1
  }
  tmp_bundle="$bundle.tmp.$$"
  tmp_checksum="$checksum_file.tmp.$$"
  [[ ! -e "$tmp_bundle" && ! -e "$tmp_checksum" ]] || return 1
  tar -C "$parent" -czf "$tmp_bundle" "$base" || {
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  }
  checksum="$(sha256_file "$tmp_bundle")" || {
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  }
  printf '%s  %s\n' "$checksum" "$bundle" >"$tmp_checksum" || {
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  }
  mv "$tmp_bundle" "$bundle" || {
    rm -f "$tmp_bundle" "$tmp_checksum"
    return 1
  }
  mv "$tmp_checksum" "$checksum_file" || {
    rm -f "$tmp_checksum"
    return 1
  }
  chmod 0644 "$bundle" "$checksum_file" || return 1
  market_bundle_is_valid "$run_dir" || return 1
  MARKET_MAC_BUNDLE="$bundle"
  MARKET_MAC_SHA256="$checksum"
}

write_market_run_manifest() {
  local run_dir="$1" binding_profile="$2" workload_profile="$3"
  local client_label="$4" client_version="$5" client_kind="$6"
  local final_status="$7" reason="$8"
  local phase_count=0 probe_count=0 event_count=0 invalid_count=0
  [[ ! -f "$run_dir/workload/phases.tsv" ]] || \
    phase_count="$(awk 'END {print (NR > 0 ? NR-1 : 0)}' \
      "$run_dir/workload/phases.tsv")"
  [[ ! -f "$run_dir/workload/probes.tsv" ]] || \
    probe_count="$(awk 'END {print (NR > 0 ? NR-1 : 0)}' \
      "$run_dir/workload/probes.tsv")"
  [[ ! -f "$run_dir/workload/events.tsv" ]] || \
    event_count="$(awk 'END {print (NR > 0 ? NR-1 : 0)}' \
      "$run_dir/workload/events.tsv")"
  [[ ! -f "$run_dir/workload/invalid.tsv" ]] || \
    invalid_count="$(awk 'END {print (NR > 0 ? NR-1 : 0)}' \
      "$run_dir/workload/invalid.tsv")"
  printf '%s\n' \
    'schema=knife15-market-run-v1' \
    "status=$final_status" \
    "reason=$reason" \
    "completed_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    "source_commit=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)" \
    "runner_sha256=$(sha256_file "$SCRIPT_PATH")" \
    "summary_sha256=$(sha256_file "$SCRIPT_DIR/knife15-market-iperf-summary.py")" \
    "binding_profile_sha256=$(sha256_file "$binding_profile")" \
    "workload_profile_sha256=$(sha256_file "$workload_profile")" \
    "client_label=$client_label" \
    "client_version=$client_version" \
    "client_kind=$client_kind" \
    "phase_count=$phase_count" \
    "probe_count=$probe_count" \
    "quality_event_count=$event_count" \
    "invalid_evidence_count=$invalid_count" \
    "interrupt_signal=${MARKET_INTERRUPT_SIGNAL:-none}" \
    "observer_remote_run_dir=${MARKET_OBSERVER_REMOTE_RUN_DIR:-unavailable}" \
    "observer_bundle=${MARKET_OBSERVER_BUNDLE:-unavailable}" \
    "observer_sha256=${MARKET_OBSERVER_SHA256:-unavailable}" \
    "observer_finalization_status=$MARKET_OBSERVER_FINALIZATION_STATUS" \
    >"$run_dir/manifest.txt"
}

preserve_market_baseline() {
  local run_dir="$1" profile_file="$2" baseline_dir
  baseline_dir="$(profile_value "$profile_file" baseline_dir)" || return 1
  [[ -d "$baseline_dir" && ! -L "$baseline_dir" ]] || return 1
  mkdir "$run_dir/baseline" || return 1
  cp "$baseline_dir/direct-forward.json" "$run_dir/baseline/direct-forward.json" && \
    cp "$baseline_dir/direct-reverse.json" "$run_dir/baseline/direct-reverse.json"
}

execute_market_run() {
  local run_dir="$1" binding_profile="$2" workload_profile="$3"
  local client_label="$4" client_version="$5" client_kind="$6"
  local vpn_if="$7" physical_if="$8" tuic_exit_ipv4="$9"
  local expected_egress="${10}" dns_target="${11}" dns_name="${12}"
  local client_binary="${13}" timeout_grace="${14}" observer_script="${15}"
  local tuic_port="${16}" route_bin="${17}" ifconfig_bin="${18}"
  local scutil_bin="${19}" networksetup_bin="${20}" curl_bin="${21}"
  local dig_bin="${22}" iperf_bin="${23}" git_bin="${24}"
  local workload_ok=0 workload_status=INVALID final_status reason
  [[ -d "$run_dir" && ! -L "$run_dir" ]] || return 1
  validate_client_kind "$client_kind" || return 1
  mkdir "$run_dir/preflight" "$run_dir/observer" "$run_dir/workload" || return 1
  if ! perform_preflight "$run_dir/preflight" "$binding_profile" "$client_label" \
    "$client_version" "$vpn_if" "$physical_if" "$tuic_exit_ipv4" \
    "$expected_egress" "$dns_target" "$client_binary" "$route_bin" \
    "$ifconfig_bin" "$scutil_bin" "$networksetup_bin" "$curl_bin" \
    "$iperf_bin" "$git_bin"; then
    printf 'INVALID\n' >"$run_dir/status"
    write_market_run_manifest "$run_dir" "$binding_profile" "$workload_profile" \
      "$client_label" "$client_version" "$client_kind" INVALID preflight_failed || \
      return 1
    publish_market_bundle "$run_dir" || return 1
    return 1
  fi
  if ! preserve_market_baseline "$run_dir" "$binding_profile"; then
    printf 'INVALID\n' >"$run_dir/status"
    write_market_run_manifest "$run_dir" "$binding_profile" "$workload_profile" \
      "$client_label" "$client_version" "$client_kind" INVALID \
      baseline_preservation_failed || \
      return 1
    publish_market_bundle "$run_dir" || return 1
    return 1
  fi
  if ! start_observer_and_bind "$run_dir/observer" "$observer_script" \
    "$TARGET" "$IPERF_PORT" "$tuic_port"; then
    if [[ "$MARKET_OBSERVER_ARMED" == 1 ]]; then
      finalize_bound_observer "$run_dir/observer" "$observer_script" \
        "$TARGET" "$IPERF_PORT" "$tuic_port" start-validation-failure || true
    fi
    printf 'INVALID\n' >"$run_dir/status"
    write_market_run_manifest "$run_dir" "$binding_profile" "$workload_profile" \
      "$client_label" "$client_version" "$client_kind" INVALID \
      observer_start_failed || return 1
    publish_market_bundle "$run_dir" || return 1
    return 1
  fi
  if run_market_workload "$run_dir/workload" "$workload_profile" "$iperf_bin" \
    "$timeout_grace" "$dig_bin" "$curl_bin" "$dns_target" "$dns_name" \
    "$expected_egress" "$route_bin" "$vpn_if" "$physical_if" \
    "$tuic_exit_ipv4"; then
    workload_ok=1
  fi
  workload_status="$(sed -n '1p' "$run_dir/workload/status" 2>/dev/null || \
    echo INVALID)"
  if ((workload_ok == 1)); then
    reason=workload_complete
  elif [[ -n "$MARKET_INTERRUPT_SIGNAL" ]]; then
    reason=interrupted
  else
    reason=workload_invalid
  fi
  if ! finalize_bound_observer "$run_dir/observer" "$observer_script" \
    "$TARGET" "$IPERF_PORT" "$tuic_port" "$reason"; then
    workload_ok=0
    reason=observer_finalization_failed
  fi
  if ((workload_ok == 1)) && \
    [[ "$workload_status" == PASS_NO_EVENTS || \
      "$workload_status" == PASS_WITH_EVENTS ]]; then
    final_status="$workload_status"
  else
    final_status=INVALID
  fi
  printf '%s\n' "$final_status" >"$run_dir/status"
  write_market_run_manifest "$run_dir" "$binding_profile" "$workload_profile" \
    "$client_label" "$client_version" "$client_kind" "$final_status" \
    "$reason" || return 1
  publish_market_bundle "$run_dir" || return 1
  [[ "$final_status" == PASS_NO_EVENTS || "$final_status" == PASS_WITH_EVENTS ]]
}

market_signal_handler() {
  local signal="$1"
  [[ -z "$MARKET_INTERRUPT_SIGNAL" ]] || return 0
  MARKET_INTERRUPT_SIGNAL="$signal"
  if [[ "$MARKET_ACTIVE_COMMAND_PID" =~ ^[1-9][0-9]*$ ]]; then
    kill -TERM "$MARKET_ACTIVE_COMMAND_PID" 2>/dev/null || true
  fi
  if [[ "$MARKET_ACTIVE_WATCHDOG_PID" =~ ^[1-9][0-9]*$ ]]; then
    kill -TERM "$MARKET_ACTIVE_WATCHDOG_PID" 2>/dev/null || true
  fi
}

market_emergency_exit_trap() {
  local exit_status="$1"
  trap - EXIT INT TERM HUP
  market_signal_handler unexpected-exit || true
  if [[ "$MARKET_OBSERVER_ARMED" == 1 && -n "$MARKET_RUN_DIR" && \
    -n "$MARKET_OBSERVER_SCRIPT" ]]; then
    finalize_bound_observer "$MARKET_RUN_DIR/observer" "$MARKET_OBSERVER_SCRIPT" \
      "$MARKET_OBSERVER_TARGET" "$MARKET_OBSERVER_IPERF_PORT" \
      "$MARKET_OBSERVER_TUIC_PORT" unexpected-exit || true
  fi
  if [[ -n "$MARKET_RUN_DIR" && -d "$MARKET_RUN_DIR" ]] && \
    ! market_bundle_is_valid "$MARKET_RUN_DIR"; then
    printf 'INVALID\n' >"$MARKET_RUN_DIR/status" 2>/dev/null || true
    printf 'exit_status=%s\n' "$exit_status" \
      >"$MARKET_RUN_DIR/unexpected-exit.txt" 2>/dev/null || true
    publish_market_bundle "$MARKET_RUN_DIR" >/dev/null 2>&1 || true
  fi
}

validate_exit_ssh_host() {
  local ssh_host="$1" expected_ipv4="$2"
  [[ "$ssh_host" =~ ^[a-z_][A-Za-z0-9_-]*@([0-9]+[.]){3}[0-9]+$ ]] || \
    return 1
  [[ "${ssh_host#*@}" == "$expected_ipv4" ]]
}

run_action() {
  local profile_file="${PROFILE_FILE:-}" client_label="${CLIENT_LABEL:-}"
  local client_version="${CLIENT_VERSION:-}" client_kind="${CLIENT_KIND:-}"
  local vpn_if="${EXPECTED_VPN_IF:-}"
  local physical_if="${PHYSICAL_IF:-}" tuic_exit_ipv4="${TUIC_EXIT_IPV4:-}"
  local expected_egress="${EXPECTED_EXIT_IPV4:-}" dns_target="${DNS_TARGET:-}"
  local dns_name="${DNS_NAME:-}" client_binary="${CLIENT_BINARY:-}"
  local tuic_port="${TUIC_PORT:-8443}" observer_script
  local run_root run_dir route_bin ifconfig_bin scutil_bin networksetup_bin
  local curl_bin dig_bin iperf_bin git_bin result=0 final_status
  [[ -n "$profile_file" && -n "$client_label" && -n "$client_version" && \
    -n "$client_kind" && \
    -n "$vpn_if" && -n "$physical_if" && -n "$tuic_exit_ipv4" && \
    -n "$expected_egress" && -n "$dns_target" && -n "$dns_name" ]] || \
    die "run requires PROFILE_FILE, CLIENT_LABEL, CLIENT_VERSION, CLIENT_KIND, EXPECTED_VPN_IF, PHYSICAL_IF, TUIC_EXIT_IPV4, EXPECTED_EXIT_IPV4, DNS_TARGET, and DNS_NAME"
  validate_client_identity "$client_label" "$client_version" "$vpn_if" \
    "$physical_if" "$tuic_exit_ipv4" || die "invalid market client identity"
  validate_client_kind "$client_kind" || die "CLIENT_KIND must be mature, mini_vpn, or commercial"
  validate_ipv4 "$expected_egress" && validate_ipv4 "$dns_target" && \
    validate_dns_name "$dns_name" && validate_port "$tuic_port" || \
    die "invalid market network identity"
  verify_profile "$profile_file" || die "market profile verification failed"
  observer_script="$SCRIPT_DIR/knife15-exit-target-observer.sh"
  regular_file "$observer_script" || die "exact tracked Exit observer is unavailable"
  [[ -n "${EXIT_SSH_HOST:-}" && -n "${EXIT_SSH_KEY:-}" ]] || \
    die "run requires EXIT_SSH_HOST and EXIT_SSH_KEY for paired evidence"
  validate_exit_ssh_host "$EXIT_SSH_HOST" "$tuic_exit_ipv4" || \
    die "EXIT_SSH_HOST must be user@$tuic_exit_ipv4"
  regular_file "$EXIT_SSH_KEY" || die "EXIT_SSH_KEY must be a real readable file"
  [[ -d "$MARKET_RUN_ROOT" && ! -L "$MARKET_RUN_ROOT" ]] || \
    die "MARKET_RUN_ROOT must be an existing real directory"
  run_root="$(cd "$MARKET_RUN_ROOT" && pwd -P)" || die "cannot resolve MARKET_RUN_ROOT"
  run_dir="$run_root/mini_vpn_knife15_market_${client_label}_$(date -u '+%Y%m%d_%H%M%S')"
  mkdir -m 0700 "$run_dir" || die "cannot create market run directory"
  route_bin="$(preferred_command /sbin/route route)" || \
    die "route unavailable; evidence: $run_dir"
  ifconfig_bin="$(preferred_command /sbin/ifconfig ifconfig)" || \
    die "ifconfig unavailable; evidence: $run_dir"
  scutil_bin="$(preferred_command /usr/sbin/scutil scutil)" || \
    die "scutil unavailable; evidence: $run_dir"
  networksetup_bin="$(preferred_command /usr/sbin/networksetup networksetup)" || \
    die "networksetup unavailable; evidence: $run_dir"
  curl_bin="$(preferred_command /usr/bin/curl curl)" || \
    die "curl unavailable; evidence: $run_dir"
  dig_bin="$(preferred_command /usr/bin/dig dig)" || \
    die "dig unavailable; evidence: $run_dir"
  iperf_bin="$(command -v iperf3)" || die "iperf3 unavailable; evidence: $run_dir"
  git_bin="$(command -v git)" || die "git unavailable; evidence: $run_dir"
  MARKET_RUN_DIR="$run_dir"
  MARKET_OBSERVER_SCRIPT="$observer_script"
  MARKET_OBSERVER_TARGET="$TARGET"
  MARKET_OBSERVER_IPERF_PORT="$IPERF_PORT"
  MARKET_OBSERVER_TUIC_PORT="$tuic_port"
  MARKET_INTERRUPT_SIGNAL=''
  trap 'market_emergency_exit_trap "$?"' EXIT
  trap 'market_signal_handler INT' INT
  trap 'market_signal_handler TERM' TERM
  trap 'market_signal_handler HUP' HUP
  if ! execute_market_run "$run_dir" "$profile_file" "$profile_file" \
    "$client_label" "$client_version" "$client_kind" "$vpn_if" "$physical_if" \
    "$tuic_exit_ipv4" "$expected_egress" "$dns_target" "$dns_name" \
    "$client_binary" 30 "$observer_script" "$tuic_port" "$route_bin" \
    "$ifconfig_bin" "$scutil_bin" "$networksetup_bin" "$curl_bin" \
    "$dig_bin" "$iperf_bin" "$git_bin"; then
    result=1
  fi
  trap - EXIT INT TERM HUP
  final_status="$(sed -n '1p' "$run_dir/status" 2>/dev/null || echo INVALID)"
  printf '%s\n' \
    "market_status=$final_status" \
    "run_dir=$run_dir" \
    "bundle=${MARKET_MAC_BUNDLE:-unavailable}" \
    "sha256=${MARKET_MAC_SHA256:-unavailable}" \
    "observer_bundle=${MARKET_OBSERVER_BUNDLE:-unavailable}" \
    "observer_sha256=${MARKET_OBSERVER_SHA256:-unavailable}"
  if ((result == 0)); then
    printf 'PASS: market calibration trial completed; this is not formal M2 acceptance\n'
  else
    printf 'ERROR: market calibration evidence is INVALID; external VPN client remains operator-owned\n' >&2
  fi
  return "$result"
}

classify_market_counts() {
  local mature_trials="$1" mature_receiver_event_trials="$2"
  local mini_trials="$3" mini_receiver_event_trials="$4"
  if ((10#$mature_receiver_event_trials > 0)); then
    printf 'CALIBRATE_PRODUCT_SLI\n'
  elif ((10#$mature_trials == 0)); then
    printf 'NO_DECISION\n'
  elif ((10#$mature_trials >= 2 && 10#$mini_trials >= 2 && \
    10#$mini_receiver_event_trials == 10#$mini_trials)); then
    printf 'MINI_VPN_DIFFERENTIAL\n'
  elif ((10#$mini_trials > 0 && 10#$mini_receiver_event_trials == 0)); then
    printf 'EXTEND_MATCHED_DURATION\n'
  else
    printf 'RUN_MATCHED_C1\n'
  fi
}

market_bundle_manifest() {
  local bundle="$1" run_dir base
  [[ "$bundle" == /*.tar.gz ]] || return 1
  run_dir="${bundle%.tar.gz}"
  market_bundle_is_valid "$run_dir" || return 1
  base="$(basename "$run_dir")"
  tar -xOf "$bundle" "$base/manifest.txt" 2>/dev/null
}

market_bundle_receiver_event_count() {
  local bundle="$1" run_dir base
  run_dir="${bundle%.tar.gz}"
  base="$(basename "$run_dir")"
  tar -xOf "$bundle" "$base/workload/events.tsv" 2>/dev/null | \
    awk -F '\t' '
      NR == 1 {
        if ($0 != "timestamp\tcycle\tphase\tkind\tvalue\tdetail\tevidence") {
          invalid = 1
        }
        next
      }
      NF != 7 { invalid = 1; next }
      $4 == "receiver_zero_interval" {
        if ($5 !~ /^[1-9][0-9]*$/) {
          invalid = 1
          next
        }
        count += $5
      }
      END {
        if (NR < 1 || invalid) exit 1
        print count + 0
      }
    '
}

summarize_market_trials() {
  local token role bundle manifest status receiver_events recorded_kind
  local mature_trials=0 mature_event_trials=0 mini_trials=0 mini_event_trials=0
  local commercial_trials=0 invalid_trials=0 decision
  (($# > 0)) || return 1
  for token in "$@"; do
    [[ "$token" == *=* ]] || return 1
    role="${token%%=*}"
    bundle="${token#*=}"
    [[ "$role" == mature || "$role" == mini_vpn || "$role" == commercial ]] || \
      return 1
    if ! manifest="$(market_bundle_manifest "$bundle")"; then
      invalid_trials=$((invalid_trials + 1))
      continue
    fi
    status="$(value_from_text "$manifest" status 2>/dev/null || echo INVALID)"
    recorded_kind="$(value_from_text "$manifest" client_kind 2>/dev/null || \
      echo missing)"
    if [[ "$recorded_kind" != "$role" ]]; then
      invalid_trials=$((invalid_trials + 1))
      continue
    fi
    if [[ "$status" != PASS_NO_EVENTS && "$status" != PASS_WITH_EVENTS ]]; then
      invalid_trials=$((invalid_trials + 1))
      continue
    fi
    receiver_events="$(market_bundle_receiver_event_count "$bundle")" || {
      invalid_trials=$((invalid_trials + 1))
      continue
    }
    [[ "$receiver_events" =~ ^[0-9]+$ ]] || {
      invalid_trials=$((invalid_trials + 1))
      continue
    }
    case "$role" in
      mature)
        mature_trials=$((mature_trials + 1))
        ((10#$receiver_events == 0)) || \
          mature_event_trials=$((mature_event_trials + 1))
        ;;
      mini_vpn)
        mini_trials=$((mini_trials + 1))
        ((10#$receiver_events == 0)) || mini_event_trials=$((mini_event_trials + 1))
        ;;
      commercial) commercial_trials=$((commercial_trials + 1)) ;;
    esac
  done
  if ((invalid_trials > 0)); then
    decision=NO_DECISION
  else
    decision="$(classify_market_counts "$mature_trials" "$mature_event_trials" \
      "$mini_trials" "$mini_event_trials")" || return 1
  fi
  printf '%s\n' \
    'schema=knife15-market-decision-v1' \
    "mature_trials=$mature_trials" \
    "mature_receiver_event_trials=$mature_event_trials" \
    "mini_vpn_trials=$mini_trials" \
    "mini_vpn_receiver_event_trials=$mini_event_trials" \
    "commercial_qoe_trials=$commercial_trials" \
    "invalid_trials=$invalid_trials" \
    "decision=$decision"
}

summarize_action() {
  summarize_market_trials "$@" || \
    die "summarize requires role=/absolute/immutable-bundle.tar.gz inputs"
}

self_test() {
  local tmp baseline_dir profile_file public_output public_profile bad_profile
  local zero_baseline target_route exit_route default_route dns_route
  local fake_bin fake_command preflight_dir invalid_preflight_dir current_commit
  local target_ready_json wrong_ready_json
  local workload_profile workload_dir invalid_workload_dir workload_count
  local observer_status_text
  local observer_mock observer_evidence drop_observer_evidence observer_actions action_count
  local bundle_run_dir secret_run_dir full_run_dir
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
    '  dig) printf "93.184.216.34\n" ;;' \
    '  curl) printf "%s\n" "${MARKET_TEST_EGRESS:-43.153.32.33}" ;;' \
    '  iperf3)' \
    '    if [[ -z "${MARKET_TEST_IPERF_COUNT_FILE:-}" ]]; then /bin/cat "$MARKET_TEST_READY_JSON"; exit 0; fi' \
    '    if [[ -n "${MARKET_TEST_IPERF_SKIP_ONCE_FILE:-}" && ! -e "$MARKET_TEST_IPERF_SKIP_ONCE_FILE" ]]; then printf "skip\n" >"$MARKET_TEST_IPERF_SKIP_ONCE_FILE"; /bin/cat "$MARKET_TEST_READY_JSON"; exit 0; fi' \
    '    count=0' \
    '    [[ ! -f "$MARKET_TEST_IPERF_COUNT_FILE" ]] || count="$(sed -n "1p" "$MARKET_TEST_IPERF_COUNT_FILE")"' \
    '    count=$((count + 1))' \
    '    printf "%s\n" "$count" >"$MARKET_TEST_IPERF_COUNT_FILE"' \
    '    [[ "${MARKET_TEST_IPERF_FAIL_AT:-}" != "$count" ]] || exit 2' \
    '    if [[ "${MARKET_TEST_IPERF_HANG_AT:-}" == "$count" ]]; then sleep 5; fi' \
    '    if [[ "${MARKET_TEST_IPERF_MALFORMED_AT:-}" == "$count" ]]; then printf "{}\n"; exit 0; fi' \
    '    protocol=TCP; reverse=0; duration=0' \
    '    while (($#)); do' \
    '      case "$1" in' \
    '        -u) protocol=UDP ;;' \
    '        -R) reverse=1 ;;' \
    '        -t) shift; duration="$1" ;;' \
    '      esac' \
    '      shift' \
    '    done' \
    '    if [[ "$protocol" == UDP ]]; then' \
    '      jq --argjson reverse "$reverse" --argjson duration "$duration" --argjson quality "$([[ "$count" == 3 ]] && echo 1 || echo 0)" '\''
        .start.connecting_to.host = "43.130.32.77"
        | .start.connecting_to.port = 5201
        | .start.test_start.protocol = "UDP"
        | .start.test_start.reverse = $reverse
        | .start.test_start.duration = $duration
        | .server_output_json.start.test_start.protocol = "UDP"
        | .server_output_json.start.test_start.reverse = $reverse
        | .server_output_json.start.test_start.duration = $duration
        | .intervals = [.intervals[0]]
        | .server_output_json.intervals = [.server_output_json.intervals[0]]
        | .end.sum.lost_percent = (if $quality == 1 then 4.5 else 1.0 end)
        | .end.sum.lost_packets = (if $quality == 1 then 4 else 1 end)
        | .end.sum.packets = 100
      '\'' "$MARKET_TEST_UDP_JSON"' \
    '    else' \
    '      jq --argjson reverse "$reverse" --argjson duration "$duration" --argjson quality "$([[ "$count" == 2 ]] && echo 1 || echo 0)" '\''
        .start.connecting_to.host = "43.130.32.77"
        | .start.connecting_to.port = 5201
        | .start.test_start.protocol = "TCP"
        | .start.test_start.reverse = $reverse
        | .start.test_start.duration = $duration
        | .server_output_json.start.test_start.protocol = "TCP"
        | .server_output_json.start.test_start.reverse = $reverse
        | .server_output_json.start.test_start.duration = $duration
        | .intervals = [.intervals[0]]
        | .server_output_json.intervals = [.server_output_json.intervals[0]]
        | if $reverse == 1 and $quality == 1
          then .intervals[0].sum.bits_per_second = 0
          else . end
      '\'' "$MARKET_TEST_TCP_JSON"' \
    '    fi' \
    '    ;;' \
    '  git)' \
    '    if [[ "$*" == *"status --porcelain --untracked-files=all"* ]]; then exit 0; fi' \
    '    if [[ "$*" == *"rev-parse HEAD"* ]]; then printf "%s\n" "$MARKET_TEST_COMMIT"; exit 0; fi' \
    '    exit 1' \
    '    ;;' \
    '  *) exit 1 ;;' \
    'esac' >"$fake_command"
  chmod 0700 "$fake_command"
  for fake_name in route ifconfig scutil networksetup curl dig iperf3 git; do
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
  export MARKET_TEST_TCP_JSON="$target_ready_json"
  export MARKET_TEST_UDP_JSON="$SCRIPT_DIR/fixtures/knife15-market/udp-complete.json"
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
  workload_profile="$tmp/test-workload-profile.txt"
  awk '
    /^cycles=/ {print "cycles=2"; next}
    /^tcp_secs=/ {print "tcp_secs=1"; next}
    /^udp_secs=/ {print "udp_secs=1"; next}
    /^short_secs=/ {print "short_secs=1"; next}
    /^short_count=/ {print "short_count=2"; next}
    {print}
  ' "$profile_file" >"$workload_profile"
  workload_dir="$tmp/workload"
  mkdir "$workload_dir"
  export MARKET_TEST_IPERF_COUNT_FILE="$tmp/iperf-count"
  if ! run_market_workload "$workload_dir" "$workload_profile" \
    "$fake_bin/iperf3" 0 "$fake_bin/dig" "$fake_bin/curl" 8.8.8.8 \
    example.com 43.153.32.33 "$fake_bin/route" utun9 en0 43.153.32.33; then
    die "self-test: quality-event workload did not complete"
  fi
  workload_count="$(sed -n '1p' "$MARKET_TEST_IPERF_COUNT_FILE")"
  [[ "$workload_count" == "10" ]] || \
    die "self-test: quality events stopped the schedule at $workload_count/10"
  [[ "$(sed -n '1p' "$workload_dir/status")" == PASS_WITH_EVENTS ]] || \
    die "self-test: quality-event workload status mismatch"
  [[ "$(awk 'END {print NR-1}' "$workload_dir/phases.tsv")" == "10" ]] || \
    die "self-test: workload phase evidence count mismatch"
  [[ "$(awk 'END {print NR-1}' "$workload_dir/probes.tsv")" == "2" ]] || \
    die "self-test: cycle probe evidence count mismatch"
  [[ "$(awk 'END {print NR-1}' "$workload_dir/events.tsv")" == "2" ]] || \
    die "self-test: workload quality event count mismatch"
  grep -Fq $'\treceiver_zero_interval\t1\t' "$workload_dir/events.tsv"
  grep -Fq $'\tudp_loss_percent\t4.5\t' "$workload_dir/events.tsv"
  invalid_workload_dir="$tmp/workload-command-failure"
  mkdir "$invalid_workload_dir"
  if (export MARKET_TEST_IPERF_COUNT_FILE="$tmp/iperf-fail-count"; \
    export MARKET_TEST_IPERF_FAIL_AT=4; \
    run_market_workload "$invalid_workload_dir" "$workload_profile" \
      "$fake_bin/iperf3" 0 "$fake_bin/dig" "$fake_bin/curl" 8.8.8.8 \
      example.com 43.153.32.33 "$fake_bin/route" utun9 en0 \
      43.153.32.33); then
    die "self-test: command-failed workload was accepted"
  fi
  [[ "$(sed -n '1p' "$invalid_workload_dir/status")" == INVALID ]] || \
    die "self-test: command failure did not invalidate workload"
  [[ "$(sed -n '1p' "$tmp/iperf-fail-count")" == 4 ]] || \
    die "self-test: command failure did not stop at exact phase"
  invalid_workload_dir="$tmp/workload-malformed"
  mkdir "$invalid_workload_dir"
  if (export MARKET_TEST_IPERF_COUNT_FILE="$tmp/iperf-malformed-count"; \
    export MARKET_TEST_IPERF_MALFORMED_AT=2; \
    run_market_workload "$invalid_workload_dir" "$workload_profile" \
      "$fake_bin/iperf3" 0 "$fake_bin/dig" "$fake_bin/curl" 8.8.8.8 \
      example.com 43.153.32.33 "$fake_bin/route" utun9 en0 \
      43.153.32.33); then
    die "self-test: malformed workload evidence was accepted"
  fi
  [[ "$(sed -n '1p' "$tmp/iperf-malformed-count")" == 2 ]] || \
    die "self-test: malformed evidence did not stop at exact phase"
  invalid_workload_dir="$tmp/workload-timeout"
  mkdir "$invalid_workload_dir"
  if (export MARKET_TEST_IPERF_COUNT_FILE="$tmp/iperf-timeout-count"; \
    export MARKET_TEST_IPERF_HANG_AT=2; \
    run_market_workload "$invalid_workload_dir" "$workload_profile" \
      "$fake_bin/iperf3" 0 "$fake_bin/dig" "$fake_bin/curl" 8.8.8.8 \
      example.com 43.153.32.33 "$fake_bin/route" utun9 en0 \
      43.153.32.33); then
    die "self-test: timed-out workload was accepted"
  fi
  [[ "$(sed -n '1p' "$tmp/iperf-timeout-count")" == 2 ]] || \
    die "self-test: timeout did not stop at exact phase"
  observer_status_text=$'schema=knife15-exit-target-observer-v2\nstatus=active\nobserver_healthy=1\nrun_dir=/tmp/mini_vpn_knife15_exit_target_observer_20260814_120000\ntarget=43.130.32.77\niperf_port=5201\ntuic_port=8443\ntimeout_secs=93600\nelapsed_secs=2\ntcpdump_live=1\nsampler_live=1\ncounter_sampler_live=1\ncapture_kernel_drops=unknown'
  observer_status_is_valid "$observer_status_text" 43.130.32.77 5201 8443 \
    /tmp/mini_vpn_knife15_exit_target_observer_20260814_120000 || \
    die "self-test: valid observer identity rejected"
  if observer_status_is_valid "${observer_status_text/tuic_port=8443/tuic_port=8444}" \
    43.130.32.77 5201 8443 \
    /tmp/mini_vpn_knife15_exit_target_observer_20260814_120000; then
    die "self-test: mismatched observer identity accepted"
  fi
  observer_mock="$tmp/fake-observer.sh"
  observer_actions="$tmp/observer-actions.log"
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'action="${1:-}"' \
    'printf "%s\n" "$action" >>"$MARKET_TEST_OBSERVER_ACTIONS"' \
    'case "$action" in' \
    '  start)' \
    '    [[ ! -e "$MARKET_TEST_OBSERVER_STATE" ]] || exit 1' \
    '    printf "active\n" >"$MARKET_TEST_OBSERVER_STATE"' \
    '    printf "PASS: Exit observer started\nrun_dir=%s\ntcpdump_pid=11\nsampler_pid=12\ncounter_sampler_pid=13\n" "$MARKET_TEST_OBSERVER_RUN_DIR"' \
    '    ;;' \
    '  status)' \
    '    [[ -f "$MARKET_TEST_OBSERVER_STATE" ]] || exit 1' \
    '    printf "schema=knife15-exit-target-observer-v2\nstatus=active\nobserver_healthy=1\nrun_dir=%s\ntarget=%s\niperf_port=%s\ntuic_port=%s\ntimeout_secs=93600\nelapsed_secs=2\ntcpdump_live=1\nsampler_live=1\ncounter_sampler_live=1\ncapture_kernel_drops=%s\n" "$MARKET_TEST_OBSERVER_RUN_DIR" "$TARGET" "$IPERF_PORT" "$TUIC_PORT" "${MARKET_TEST_OBSERVER_DROPS:-0}"' \
    '    ;;' \
    '  freeze)' \
    '    [[ -f "$MARKET_TEST_OBSERVER_STATE" ]] || exit 1' \
    '    printf "PASS: Exit observer frozen\nrun_dir=%s\n" "$MARKET_TEST_OBSERVER_RUN_DIR"' \
    '    ;;' \
    '  bundle)' \
    '    [[ -f "$MARKET_TEST_OBSERVER_STATE" ]] || exit 1' \
    '    rm -f "$MARKET_TEST_OBSERVER_STATE"' \
    '    printf "PASS: Exit observer bundle finalized\nbundle=/tmp/mini_vpn_knife15_exit_target_observer_20260814_120000.tar.gz\nsha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"' \
    '    ;;' \
    '  *) exit 1 ;;' \
    'esac' >"$observer_mock"
  chmod 0700 "$observer_mock"
  export MARKET_TEST_OBSERVER_STATE="$tmp/observer-state"
  export MARKET_TEST_OBSERVER_ACTIONS="$observer_actions"
  export MARKET_TEST_OBSERVER_RUN_DIR=/tmp/mini_vpn_knife15_exit_target_observer_20260814_120000
  printf 'preexisting\n' >"$MARKET_TEST_OBSERVER_STATE"
  observer_evidence="$tmp/observer-preexisting"
  mkdir "$observer_evidence"
  if start_observer_and_bind "$observer_evidence" "$observer_mock" \
    43.130.32.77 5201 8443; then
    die "self-test: pre-existing observer ownership was claimed"
  fi
  [[ "$(awk 'END {print NR}' "$observer_actions")" == 1 ]] || \
    die "self-test: pre-existing observer was mutated"
  rm -f "$MARKET_TEST_OBSERVER_STATE" "$observer_actions"
  observer_evidence="$tmp/observer-owned"
  mkdir "$observer_evidence"
  start_observer_and_bind "$observer_evidence" "$observer_mock" \
    43.130.32.77 5201 8443 || die "self-test: fresh observer bind failed"
  finalize_bound_observer "$observer_evidence" "$observer_mock" \
    43.130.32.77 5201 8443 normal-completion || \
    die "self-test: owned observer finalization failed"
  finalize_bound_observer "$observer_evidence" "$observer_mock" \
    43.130.32.77 5201 8443 repeated-finalization || \
    die "self-test: observer finalization was not idempotent"
  action_count="$(awk 'END {print NR}' "$observer_actions")"
  [[ "$action_count" == 7 ]] || \
    die "self-test: observer lifecycle call count mismatch: $action_count"
  grep -Fxq 'finalization_status=complete' \
    "$observer_evidence/observer-finalization.txt"
  rm -f "$observer_actions" "$MARKET_TEST_OBSERVER_STATE"
  unset MARKET_TEST_OBSERVER_DROPS
  drop_observer_evidence="$tmp/observer-drops"
  mkdir "$drop_observer_evidence"
  start_observer_and_bind "$drop_observer_evidence" "$observer_mock" \
    43.130.32.77 5201 8443 || die "self-test: drop-case observer bind failed"
  export MARKET_TEST_OBSERVER_DROPS=5
  if finalize_bound_observer "$drop_observer_evidence" "$observer_mock" \
    43.130.32.77 5201 8443 capture-drop-test; then
    die "self-test: observer kernel drops were accepted"
  fi
  [[ "$MARKET_OBSERVER_FINALIZATION_STATUS" == evidence_invalid && \
    "$MARKET_OBSERVER_BUNDLE" == \
      /tmp/mini_vpn_knife15_exit_target_observer_20260814_120000.tar.gz && \
    ! -e "$MARKET_TEST_OBSERVER_STATE" ]] || \
    die "self-test: drop-case observer evidence was not preserved"
  grep -Fxq 'finalization_status=evidence_invalid' \
    "$drop_observer_evidence/observer-finalization.txt"
  unset MARKET_TEST_OBSERVER_DROPS
  rm -f "$observer_actions" "$MARKET_TEST_OBSERVER_STATE"
  export MARKET_TEST_IPERF_COUNT_FILE="$tmp/iperf-full-run-count"
  export MARKET_TEST_IPERF_SKIP_ONCE_FILE="$tmp/iperf-full-run-skip"
  full_run_dir="$tmp/mini_vpn_knife15_market_mihomo_20260814_130000"
  mkdir "$full_run_dir"
  execute_market_run "$full_run_dir" "$profile_file" "$workload_profile" \
    mihomo-tuic 'Mihomo 1.19.0' mature utun9 en0 43.153.32.33 43.153.32.33 \
    8.8.8.8 example.com '' 0 "$observer_mock" 8443 \
    "$fake_bin/route" "$fake_bin/ifconfig" "$fake_bin/scutil" \
    "$fake_bin/networksetup" "$fake_bin/curl" "$fake_bin/dig" \
    "$fake_bin/iperf3" "$fake_bin/git" || \
    die "self-test: complete market run wiring failed"
  [[ "$(sed -n '1p' "$full_run_dir/status")" == PASS_WITH_EVENTS ]] || \
    die "self-test: complete market run status mismatch: $(sed -n '1p' "$full_run_dir/status" 2>/dev/null || echo missing) / $(sed -n '1,6p' "$full_run_dir/manifest.txt" 2>/dev/null | tr '\n' ' ')"
  grep -Fxq 'observer_finalization_status=complete' "$full_run_dir/manifest.txt"
  market_bundle_is_valid "$full_run_dir" || \
    die "self-test: complete market run bundle invalid"
  [[ "$(summarize_market_trials \
    "mature=$full_run_dir.tar.gz" | \
    awk -F= '$1 == "decision" {print $2}')" == CALIBRATE_PRODUCT_SLI ]] || \
    die "self-test: mature receiver-zero decision mismatch"
  [[ "$(summarize_market_trials \
    "mini_vpn=$full_run_dir.tar.gz" | \
    awk -F= '$1 == "decision" {print $2}')" == NO_DECISION ]] || \
    die "self-test: mismatched immutable client role was accepted"
  [[ "$(classify_market_counts 2 0 2 2)" == MINI_VPN_DIFFERENTIAL ]] || \
    die "self-test: matched differential decision mismatch"
  [[ "$(classify_market_counts 2 0 2 0)" == EXTEND_MATCHED_DURATION ]] || \
    die "self-test: all-clean matched decision mismatch"
  [[ "$(classify_market_counts 1 0 0 0)" == RUN_MATCHED_C1 ]] || \
    die "self-test: clean C0 decision mismatch"
  bundle_run_dir="$tmp/mini_vpn_knife15_market_mihomo_20260814_120000"
  mkdir "$bundle_run_dir"
  printf 'PASS_WITH_EVENTS\n' >"$bundle_run_dir/status"
  printf 'safe evidence\n' >"$bundle_run_dir/evidence.txt"
  publish_market_bundle "$bundle_run_dir" || \
    die "self-test: safe market bundle publication failed"
  market_bundle_is_valid "$bundle_run_dir" || \
    die "self-test: published market bundle is invalid"
  publish_market_bundle "$bundle_run_dir" || \
    die "self-test: market bundle publication was not idempotent"
  secret_run_dir="$tmp/mini_vpn_knife15_market_secret_20260814_120000"
  mkdir "$secret_run_dir"
  printf 'peer=123e4567-e89b-12d3-a456-426614174000\n' \
    >"$secret_run_dir/evidence.txt"
  if publish_market_bundle "$secret_run_dir"; then
    die "self-test: secret-shaped market evidence was bundled"
  fi
  [[ ! -e "$secret_run_dir.tar.gz" ]] || \
    die "self-test: rejected secret evidence left a bundle"
  echo "knife15 market continuity self-test passed"
}

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/knife15-market-continuity.sh --self-test
  bash scripts/knife15-market-continuity.sh profile BASELINE_DIR
  bash scripts/knife15-market-continuity.sh verify-profile PROFILE_FILE
  bash scripts/knife15-market-continuity.sh preflight
  bash scripts/knife15-market-continuity.sh run
  bash scripts/knife15-market-continuity.sh summarize \
    mature=/absolute/market-bundle.tar.gz [mini_vpn=/absolute/market-bundle.tar.gz ...]
USAGE
}

case "$ACTION" in
  --self-test) self_test ;;
  profile) profile_action "${2:-}" ;;
  verify-profile) verify_profile_action "${2:-}" ;;
  preflight) preflight_action ;;
  run) run_action ;;
  summarize) shift; summarize_action "$@" ;;
  --help|-h|help) usage ;;
  *) usage >&2; exit 64 ;;
esac
