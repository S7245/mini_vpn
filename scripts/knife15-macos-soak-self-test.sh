#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/knife15-macos-soak.sh"

fail() {
  echo "knife15 macOS soak self-test failed: $*" >&2
  exit 1
}

[[ -x "$RUNNER" ]] || fail "runner is missing or not executable: $RUNNER"

help_text="$("$RUNNER" --help)" || fail "--help returned nonzero"
for mode in preflight baseline start status event snapshot smoke stop bundle; do
  grep -Fq "$mode" <<<"$help_text" || fail "--help omits mode: $mode"
done
grep -Fq "user runs every sudo command" <<<"$help_text" || \
  fail "--help omits the HITL execution boundary"
grep -Fq "target-only" <<<"$help_text" || fail "--help omits target-only scope"

self_test_output="$("$RUNNER" --self-test)" || fail "runner --self-test returned nonzero"
grep -Fq "knife15 macOS runner self-test passed" <<<"$self_test_output" || \
  fail "runner self-test did not report PASS"

echo "knife15 macOS soak self-test passed"
