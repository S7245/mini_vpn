#!/usr/bin/env bash

# Detached owner for one Knife15 Tier-B m2-frequency run. Baseline, direct,
# resource preflight, start, smoke, and Exit observer admission must already be
# complete in the same exported environment before this controller starts.

set -uo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
RUNNER="${KNIFE15_FREQUENCY_RUNNER:-$SCRIPT_DIR/knife15-macos-soak.sh}"
OBSERVER="${KNIFE15_FREQUENCY_OBSERVER:-$SCRIPT_DIR/knife15-exit-target-observer.sh}"
SUDO_BIN="${KNIFE15_FREQUENCY_SUDO_BIN:-/usr/bin/sudo}"
MSMTP_BIN="${KNIFE15_FREQUENCY_MSMTP_BIN:-$(command -v msmtp 2>/dev/null || true)}"
NOTICE_RECIPIENT=870941563@qq.com

notified=0
cleanup_owned=0
cleanup_done=0
cleanup_rc=0
observer_cleanup_done=0
observer_cleanup_rc=0
workload_pid=
keepalive_pid=
keepalive_failure_file=

notify_completion() {
  local notice_pid= attempt= timeout_secs=15
  [[ "$notified" == "0" ]] || return 0
  notified=1
  if [[ -n "$MSMTP_BIN" && -x "$MSMTP_BIN" ]]; then
    if [[ "${KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY:-0}" == "1" && \
      "${KNIFE15_TEST_NOTICE_TIMEOUT_SECS:-}" =~ ^[1-5]$ ]]; then
      timeout_secs="$KNIFE15_TEST_NOTICE_TIMEOUT_SECS"
    fi
    printf "Subject: 执行结束~" | "$MSMTP_BIN" "$NOTICE_RECIPIENT" &
    notice_pid=$!
    for ((attempt = 0; attempt < timeout_secs; attempt++)); do
      if ! kill -0 "$notice_pid" 2>/dev/null; then
        wait "$notice_pid" 2>/dev/null || true
        return 0
      fi
      /bin/sleep 1
    done
    kill "$notice_pid" 2>/dev/null || true
    /bin/sleep 1
    kill -0 "$notice_pid" 2>/dev/null && \
      kill -KILL "$notice_pid" 2>/dev/null || true
    wait "$notice_pid" 2>/dev/null || true
    echo "WARNING: msmtp completion notice exceeded ${timeout_secs}s and was terminated" >&2
  else
    echo "WARNING: msmtp unavailable; completion notice was not sent" >&2
  fi
  return 0
}

run_cleanup() {
  local result=0
  [[ "$cleanup_done" == "0" ]] || return "$cleanup_rc"
  cleanup_done=1
  "$SUDO_BIN" -n -E bash "$RUNNER" status || result=1
  "$SUDO_BIN" -n -E bash "$RUNNER" snapshot || result=1
  "$SUDO_BIN" -n -E bash "$RUNNER" stop || result=1
  cleanup_rc="$result"
  return "$result"
}

run_observer_cleanup() {
  local output result=0
  [[ "$observer_cleanup_done" == "0" ]] || return "$observer_cleanup_rc"
  observer_cleanup_done=1
  if output="$("$OBSERVER" status 2>&1)"; then
    printf '%s\n' "$output"
    if grep -Fxq 'status=active' <<<"$output"; then
      "$OBSERVER" freeze || result=1
      ((result != 0)) || "$OBSERVER" bundle || result=1
    elif grep -Fxq 'status=inactive' <<<"$output"; then
      "$OBSERVER" bundle || result=1
    else
      echo "ERROR: Exit observer returned an unclassified successful status" >&2
      result=1
    fi
  elif grep -Fxq 'ERROR: no observer state' <<<"$output"; then
    # A successful runner-owned finalization removes the remote state after it
    # publishes the immutable observer bundle.
    echo "PASS: Exit observer has no remaining remote state"
  else
    printf '%s\n' "$output" >&2
    echo "ERROR: cannot prove Exit observer cleanup" >&2
    result=1
  fi
  observer_cleanup_rc="$result"
  return "$result"
}

run_sudo_keepalive() {
  local owner_pid="$1" failure_file="$2" sleep_pid=
  trap '[[ -n "$sleep_pid" ]] && kill "$sleep_pid" 2>/dev/null || true; exit 0' \
    INT TERM HUP
  while kill -0 "$owner_pid" 2>/dev/null; do
    "$SUDO_BIN" -n -v || {
      : >"$failure_file"
      return 1
    }
    /bin/sleep 45 &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null || true
    sleep_pid=
  done
}

controller_exit() {
  local original_rc="$?" final_rc
  trap - EXIT
  final_rc="$original_rc"
  if [[ "$keepalive_pid" =~ ^[1-9][0-9]*$ ]]; then
    kill "$keepalive_pid" 2>/dev/null || true
    wait "$keepalive_pid" 2>/dev/null || true
  fi
  if [[ "$cleanup_owned" == "1" && "$cleanup_done" == "0" ]]; then
    run_cleanup || final_rc=1
  fi
  if [[ "$cleanup_owned" == "1" && "$observer_cleanup_done" == "0" ]]; then
    run_observer_cleanup || final_rc=1
  fi
  notify_completion
  [[ -n "$keepalive_failure_file" ]] && rm -f "$keepalive_failure_file"
  exit "$final_rc"
}

controller_main() {
  local workload_rc final_rc
  # This command is entered only after start/smoke/observer preparation. Own
  # cleanup before validating any executable so a local source/configuration
  # failure cannot silently abandon the already-created run.
  cleanup_owned=1
  [[ -f "$RUNNER" && ! -L "$RUNNER" ]] || {
    echo "ERROR: exact Knife15 runner is missing or symlinked" >&2
    return 1
  }
  [[ -f "$OBSERVER" && ! -L "$OBSERVER" ]] || {
    echo "ERROR: exact Knife15 Exit observer is missing or symlinked" >&2
    return 1
  }
  [[ -x "$SUDO_BIN" ]] || {
    echo "ERROR: sudo executable is unavailable" >&2
    return 1
  }
  if [[ "${KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY:-0}" != "1" ]]; then
    [[ "$SUDO_BIN" == /usr/bin/sudo && \
      "$RUNNER" == "$SCRIPT_DIR/knife15-macos-soak.sh" && \
      "$OBSERVER" == "$SCRIPT_DIR/knife15-exit-target-observer.sh" ]] || {
      echo "ERROR: production controller refuses runner/observer/sudo overrides" >&2
      return 1
    }
  fi
  "$SUDO_BIN" -n -v || {
    echo "ERROR: detached controller requires a live noninteractive sudo ticket" >&2
    return 1
  }

  keepalive_failure_file="$(mktemp \
    "${TMPDIR:-/tmp}/knife15-frequency-sudo-keepalive.XXXXXX")" || return 1
  rm -f "$keepalive_failure_file"
  "$SUDO_BIN" -n -E bash "$RUNNER" m2-frequency &
  workload_pid=$!
  run_sudo_keepalive "$workload_pid" "$keepalive_failure_file" &
  keepalive_pid=$!
  wait "$workload_pid"
  workload_rc=$?
  kill "$keepalive_pid" 2>/dev/null || true
  wait "$keepalive_pid" 2>/dev/null || true

  # User progress notice is deliberately after m2-frequency returns and before
  # cleanup. Failure to send cannot rewrite workload or cleanup evidence.
  notify_completion
  run_cleanup || true
  run_observer_cleanup || true

  final_rc="$workload_rc"
  [[ ! -e "$keepalive_failure_file" ]] || final_rc=1
  [[ "$cleanup_rc" == "0" ]] || final_rc=1
  [[ "$observer_cleanup_rc" == "0" ]] || final_rc=1
  rm -f "$keepalive_failure_file"
  keepalive_failure_file=
  return "$final_rc"
}

controller_self_test() {
  local tmp fake_sudo fake_msmtp fake_runner fake_observer events output status
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/knife15-frequency-controller.XXXXXX")" || \
    return 1
  trap 'rm -rf "$tmp"' RETURN
  fake_sudo="$tmp/sudo"
  fake_msmtp="$tmp/msmtp"
  fake_runner="$tmp/runner.sh"
  fake_observer="$tmp/observer.sh"
  events="$tmp/events"
  : >"$events"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$fake_runner"
  chmod 700 "$fake_runner"
  cat >"$fake_sudo" <<'EOF_FAKE_SUDO'
#!/usr/bin/env bash
set -u
if [[ "${1:-}" == "-n" && "${2:-}" == "-v" ]]; then
  exit "${KNIFE15_TEST_SUDO_TICKET_RC:-0}"
fi
action="${@: -1}"
printf 'sudo:%s\n' "$action" >>"$KNIFE15_TEST_EVENTS"
if [[ "$action" == m2-frequency ]]; then
  exit "${KNIFE15_TEST_WORKLOAD_RC:-0}"
fi
if [[ "$action" == "${KNIFE15_TEST_CLEANUP_FAIL_ACTION:-none}" ]]; then
  exit 1
fi
exit 0
EOF_FAKE_SUDO
  cat >"$fake_msmtp" <<'EOF_FAKE_MSMTP'
#!/usr/bin/env bash
set -u
body="$(cat)"
printf 'notify:%s:%s\n' "${1:-}" "$body" >>"$KNIFE15_TEST_EVENTS"
if [[ "${KNIFE15_TEST_NOTICE_HANG:-0}" == "1" ]]; then
  trap 'exit 0' INT TERM HUP
  while :; do /bin/sleep 0.1; done
fi
exit "${KNIFE15_TEST_NOTICE_RC:-0}"
EOF_FAKE_MSMTP
  cat >"$fake_observer" <<'EOF_FAKE_OBSERVER'
#!/usr/bin/env bash
set -u
action="${1:-}"
printf 'observer:%s\n' "$action" >>"$KNIFE15_TEST_EVENTS"
if [[ "$action" == status ]]; then
  case "${KNIFE15_TEST_OBSERVER_STATE:-absent}" in
    active) printf '%s\n' 'status=active' ;;
    inactive) printf '%s\n' 'status=inactive' ;;
    absent) printf '%s\n' 'ERROR: no observer state'; exit 1 ;;
    error) printf '%s\n' 'ERROR: observer transport failed'; exit 9 ;;
    *) exit 64 ;;
  esac
fi
exit 0
EOF_FAKE_OBSERVER
  chmod 700 "$fake_sudo" "$fake_msmtp" "$fake_observer"

  output="$tmp/output"
  if KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY=1 \
    KNIFE15_FREQUENCY_RUNNER="$fake_runner" \
    KNIFE15_FREQUENCY_OBSERVER="$fake_observer" \
    KNIFE15_FREQUENCY_SUDO_BIN="$fake_sudo" \
    KNIFE15_FREQUENCY_MSMTP_BIN="$fake_msmtp" \
    KNIFE15_TEST_EVENTS="$events" KNIFE15_TEST_WORKLOAD_RC=7 \
    KNIFE15_TEST_OBSERVER_STATE=active \
    KNIFE15_TEST_NOTICE_RC=1 bash "$SCRIPT_PATH" __test-run >"$output" 2>&1; then
    echo "ERROR: self-test workload failure became controller success" >&2
    return 1
  else
    status=$?
  fi
  [[ "$status" == "7" ]] || {
    echo "ERROR: self-test lost the workload failure status" >&2
    return 1
  }
  diff -u - "$events" <<'EOF_EXPECTED_FAILURE'
sudo:m2-frequency
notify:870941563@qq.com:Subject: 执行结束~
sudo:status
sudo:snapshot
sudo:stop
observer:status
observer:freeze
observer:bundle
EOF_EXPECTED_FAILURE

  : >"$events"
  KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY=1 \
    KNIFE15_FREQUENCY_RUNNER="$fake_runner" \
    KNIFE15_FREQUENCY_OBSERVER="$fake_observer" \
    KNIFE15_FREQUENCY_SUDO_BIN="$fake_sudo" \
    KNIFE15_FREQUENCY_MSMTP_BIN="$fake_msmtp" \
    KNIFE15_TEST_EVENTS="$events" KNIFE15_TEST_WORKLOAD_RC=0 \
    KNIFE15_TEST_NOTICE_RC=0 bash "$SCRIPT_PATH" __test-run || return 1
  [[ "$(grep -Fc 'notify:' "$events")" == "1" ]] || {
    echo "ERROR: self-test completion notice was not exactly once" >&2
    return 1
  }
  [[ "$(tail -n 1 "$events")" == "observer:status" ]] || {
    echo "ERROR: self-test controller did not finish cleanup" >&2
    return 1
  }
  : >"$events"
  if KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY=1 \
    KNIFE15_FREQUENCY_RUNNER="$fake_runner" \
    KNIFE15_FREQUENCY_OBSERVER="$fake_observer" \
    KNIFE15_FREQUENCY_SUDO_BIN="$fake_sudo" \
    KNIFE15_FREQUENCY_MSMTP_BIN="$fake_msmtp" \
    KNIFE15_TEST_EVENTS="$events" KNIFE15_TEST_SUDO_TICKET_RC=1 \
    KNIFE15_TEST_OBSERVER_STATE=active \
    bash "$SCRIPT_PATH" __test-run >"$output" 2>&1; then
    echo "ERROR: self-test accepted an expired sudo ticket" >&2
    return 1
  fi
  diff -u - "$events" <<'EOF_EXPECTED_EARLY_FAILURE'
sudo:status
sudo:snapshot
sudo:stop
observer:status
observer:freeze
observer:bundle
notify:870941563@qq.com:Subject: 执行结束~
EOF_EXPECTED_EARLY_FAILURE

  : >"$events"
  if KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY=1 \
    KNIFE15_FREQUENCY_RUNNER="$fake_runner" \
    KNIFE15_FREQUENCY_OBSERVER="$fake_observer" \
    KNIFE15_FREQUENCY_SUDO_BIN="$fake_sudo" \
    KNIFE15_FREQUENCY_MSMTP_BIN="$fake_msmtp" \
    KNIFE15_TEST_EVENTS="$events" KNIFE15_TEST_WORKLOAD_RC=0 \
    KNIFE15_TEST_OBSERVER_STATE=error \
    bash "$SCRIPT_PATH" __test-run >"$output" 2>&1; then
    echo "ERROR: self-test hid an unproved Exit observer cleanup" >&2
    return 1
  fi
  grep -Fxq 'observer:status' "$events" || {
    echo "ERROR: self-test did not inspect uncertain Exit observer state" >&2
    return 1
  }

  : >"$events"
  KNIFE15_FREQUENCY_CONTROLLER_TEST_ONLY=1 \
    KNIFE15_FREQUENCY_RUNNER="$fake_runner" \
    KNIFE15_FREQUENCY_OBSERVER="$fake_observer" \
    KNIFE15_FREQUENCY_SUDO_BIN="$fake_sudo" \
    KNIFE15_FREQUENCY_MSMTP_BIN="$fake_msmtp" \
    KNIFE15_TEST_EVENTS="$events" KNIFE15_TEST_WORKLOAD_RC=0 \
    KNIFE15_TEST_NOTICE_HANG=1 KNIFE15_TEST_NOTICE_TIMEOUT_SECS=1 \
    bash "$SCRIPT_PATH" __test-run >"$output" 2>&1 || {
      echo "ERROR: self-test notification timeout changed the controller verdict" >&2
      return 1
    }
  [[ "$(grep -Fc 'notify:' "$events")" == "1" && \
    "$(grep -Fc 'sudo:stop' "$events")" == "1" && \
    "$(grep -Fc 'observer:status' "$events")" == "1" ]] || {
    echo "ERROR: self-test notification timeout skipped once-only cleanup" >&2
    return 1
  }
  echo "knife15 M2 frequency controller self-test passed"
}

trap controller_exit EXIT
case "${1:-}" in
  --self-test)
    trap - EXIT
    controller_self_test
    ;;
  __test-run)
    controller_main
    ;;
  "")
    controller_main
    ;;
  *)
    trap - EXIT
    echo "usage: bash scripts/knife15-m2-frequency-controller.sh [--self-test]" >&2
    exit 64
    ;;
esac
