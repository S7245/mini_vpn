# Knife15 M2 Market Continuity Calibration Local Results

Date: 2026-08-14

Status: **LOCAL IMPLEMENTATION PASS; HITL ROUTE CANCELED AND SUPERSEDED**

The implementation and review results below remain accurate. The user later
canceled the mature-client comparison before C0. Do not run its HITL
workflow. The reviewed complete-interval reducer may be reused under the
tiered resource strategy, but external-client comparison is no longer a gate.

## Outcome

The isolated market-calibration harness is implemented and reviewed through
`3474dff`. It does not change Rust production code, the frozen formal M2
runner contract, Endpoint pacing, D16, MTU, pools, QUIC windows, Cubic, GSO,
self-wake, or any formal workload/SLI value.

The harness now provides:

- strict deterministic TCP/UDP iperf JSON reduction;
- one immutable profile bound to exact source, runner/reducer hashes, direct
  baseline hashes, Target/port, and derived rates;
- read-only admission for an operator-owned mature client, with exact Target,
  DNS, Exit, default-route, physical-interface, public-egress, source, and
  target-readiness evidence;
- six bounded cycles (`5,040s` active traffic) that continue through quality
  events but stop on invalid evidence;
- one fresh exact Exit observer per trial, automatically started, bound,
  frozen, bundled, and checksum-recorded;
- secret-scanned immutable Mac bundles; and
- decision summaries whose CLI role must match the immutable manifest's
  `client_kind`.

The next action is one roughly 90-minute mature-TUIC C0 on the HK Mac, using
only
`docs/tech/2026-08-14-knife15-m2-market-continuity-calibration-runbook.md`.
Formal M2 and M3 remain blocked.

## Deterministic Evidence

The reducer self-test covers separated and consecutive TCP receiver-zero
windows, partial-tail exclusion, forward/reverse role selection, UDP loss,
missing out-of-order normalization, and malformed numeric evidence.

Replay of the immutable `cdbfe36` failure's cycle-5 forward result produced:

```text
receiver_zero_intervals=5
max_consecutive_receiver_zero_intervals=2
receiver_zero_windows=57.001044..58.001048,
                      58.001048..59.001030,
                      60.000204..61.001031,
                      61.001031..62.001035,
                      111.000997..112.001050
sender_zero_intervals=83
sender_receiver_gap_bytes=9175040
receiver_bits_per_second=14982010.460751154
```

This preserves the accepted formal-failure evidence while changing only the
next product decision process.

The shell self-test covers profile mutation/source/hash mismatch, zero-event
baseline rejection, route recursion/bypass/second-utun/DNS mismatch, target
identity, quality-event continuation, command failure, malformed result,
timeout, observer pre-existence, observer capture drops, immutable bundling,
secret rejection, end-to-end scaled execution, role mismatch, and every exact
decision outcome.

## Review Repair

Concentrated code review found one P1 before HITL. A remote observer `start`
could succeed while malformed stdout prevented local `run_dir` ownership from
being recorded. The original branch could leave that observer active until its
26-hour bound and block the next run.

The focused RED simulated a successful start without `run_dir` and proved that
ownership was lost. Reviewed `3474dff` now queries status only after the
successful start, requires a healthy exact Target/iperf/TUIC/run identity,
marks the trial invalid, and uses the normal bounded finalizer. A failed start
is still never claimed, so another operator's observer cannot be stopped.

No unresolved P0/P1 remains in invalid-evidence handling, external-client
non-ownership, route/observer exactness, JSON parsing, timeout/signal behavior,
secret handling, evidence bounds, role binding, or formal-M2 isolation.

## Local Gates

PASS:

```text
bash -n scripts/knife15-market-continuity.sh
bash -n scripts/knife15-exit-target-observer.sh
bash -n scripts/knife15-macos-soak.sh
python3 -m py_compile scripts/knife15-market-iperf-summary.py
python3 scripts/knife15-market-iperf-summary.py --self-test
  knife15 market iperf summary self-test passed
bash scripts/knife15-market-continuity.sh --self-test
  knife15 market continuity self-test passed
bash scripts/knife15-exit-target-observer.sh --self-test
  knife15 Exit-to-Target observer self-test passed
bash scripts/knife15-macos-soak.sh --self-test
  ERROR: command exceeded hard timeout of 1s
  passfailpassknife15 macOS runner self-test passed
cargo fmt --all -- --check
git diff --check
```

The formal-runner timeout line is its intentional bounded-timeout self-test;
the command exited zero after the final pass line.

Reviewed implementation identities:

```text
3474dffd479b676e29fa9dbf2714922041828b8c
market runner sha256=3d187b0bb74c11f207b9117d4f261b0c145b695cb95005dbf20a0100d2d2192e
iperf reducer sha256=5ea5131f377a6b176985600f236949e19f47a9ceebd740c8fc3f8410d0271cc6
Exit observer sha256=ed550b968978df549d9b37b86e6f4f423ddbd142fc3abd458c92eab6e4edc1da
```

## Stop Rules

- Mature C0 has any complete receiver-zero interval:
  `CALIBRATE_PRODUCT_SLI`; do not run C1 or select a custom protocol.
- Mature C0 has zero receiver-zero intervals: `RUN_MATCHED_C1`; review the
  bundle before preparing matched alternating trials.
- Identity, integrity, source/profile, route, observer, result, or cleanup
  evidence is invalid: `NO_DECISION`; repair and repeat only that invalid C0.
- Do not run formal M2/M3 or tune production/frozen values during calibration.
