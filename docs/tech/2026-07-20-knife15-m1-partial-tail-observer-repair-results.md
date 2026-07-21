# Knife15 M1 Partial-Tail Observer Repair Results

Date: 2026-07-20

Status: **LOCAL PASS — observer repair and review complete; the partial M1 is
not an eight-hour acceptance**

## Outcome

The user-operated bundle
`/tmp/mini_vpn_knife15_macos_20260720_090636.tar.gz`, SHA-256
`5fd183b10ce7c9a0333795d86127005322bac752c1f303f43f7b160d62806578`,
matches the supplied checksum and exact source `dc8cfb1`. The operation was
correct. It was not stopped by the user, a delayed sudo password, Clash-TUN,
or route contamination.

M1 completed five full mixed cycles. Cycle 6 forward TCP then completed its
entire 300-second command and produced `292,945,920B` at
`7.807598 Mbit/s`, but the runner returned `receiver_zero_interval`. The
authoritative Target receiver contains `300` complete positive intervals and
one final `0B` row from `300.001041s` through `300.164959s`. That last
`0.163918s` row is an iperf command-tail observation, not a missing one-second
delivery window.

The root cause was an evidence-classifier mismatch: the direct discriminator
already distinguished complete intervals from partial tails, while baseline,
phase, failure-reason, and summary paths required every emitted interval row
to be positive. This is a runner observer defect, not a mini_vpn data-plane
failure.

## Rejected Failure Branches

- Operator interruption is rejected because the failing iperf ran through its
  300-second end and status/snapshot/stop occurred afterward.
- Clash or recursive routing is rejected because Target remained on the owned
  `utun4` and Exit remained on physical `en0` through the run.
- TUN/Endpoint ownership failure is rejected by conservation
  `61,440B` maximum and final `61,403/0/0B` available/live/outstanding.
- Resource or lifecycle collapse is rejected by zero health failures, zero
  interface errors, zero log compactions, no endpoint rebind, bounded process
  resources, and complete process/TUN/route cleanup.
- A complete Target receiver stall is rejected because all `300` formal
  receiver windows are positive. The local sender did retain one complete zero
  interval, which remains diagnostic `REVIEW` and does not replace receiver
  continuity.

## Repair Contract

A zero-rate row is excluded from continuity only when all of the following are
proven:

- command duration, start, and end are numeric;
- the row is the final interval entry;
- it is shorter than `0.5s`;
- it starts no earlier than `duration - 0.5s`;
- it ends between `duration` and `duration + 0.5s`.

The same rule now applies to baseline validation/summary, formal phase
validation and failure classification, result envelopes, target readiness,
and direct-discriminator sender/receiver counts. A complete zero interval, a
nonterminal short zero interval, missing timing, malformed evidence, or a
negative rate continues to fail closed.

No H10d16, EndpointWindowV1, MTU, UDP payload, pool, QUIC window, chunk, Cubic,
GSO, D16 capacity, self-wake, schedule, rate, resource SLO, or recovery value
changed.

## TDD, Replay, And Review

The first RED reproduced the real `0.164s` zero receiver tail and was rejected
by the old validator. The minimal classifier turned it GREEN. Focused guards
then proved that a complete `1.0s` zero and a zero row without timing still
return `receiver_zero_interval`.

The direct-discriminator fixture initially failed unexpectedly because its
client duration had been changed to `300s` while its embedded server duration
remained the compressed `2s` test value. Work stopped at that unexpected
failure; after the repair plan was confirmed, the fixture was corrected to
keep both endpoints consistent.

Code review then found a P1 in the first classifier: timing alone could hide a
short zero row followed by later evidence. A RED nonterminal-tail fixture
reproduced it, and every classifier now additionally requires the row to be
the final array entry. Review has no unresolved P0/P1. Repeated inline jq
definitions remain a P2 maintainability cost, intentionally left outside this
bounded repair.

Final replay of `cycle_006_tcp-forward.json` reports:

```text
classification=ok
complete_receiver_zero=0
partial_receiver_zero=1
receiver_intervals=301
complete_positive_receiver_intervals=300
```

The partial archive contains `46` workload JSON artifacts (`41` TCP and `5`
UDP): `45` phases from five complete cycles plus the cycle 6 forward result.
It remains partial evidence and cannot satisfy the exact M1 timeline/result/
checkpoint contract.

## Local Gates

All final gates passed:

```text
bash scripts/knife15-macos-soak.sh --self-test                PASS
bash scripts/knife15-macos-soak-self-test.sh                  PASS
bash scripts/knife14b-lowrtt-probe.sh --self-test             PASS
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test    PASS
bash scripts/knife14h10d16-singbox-control.sh --self-test     PASS
bash -n scripts/knife15-macos-soak.sh                         PASS
cargo test --all-targets                                      PASS
cargo build --release                                         PASS
cargo clippy --all-targets                                    PASS
cargo fmt --all -- --check                                    PASS
git diff --check                                               PASS
changed-content secret scan                                    PASS
```

The runner self-test intentionally prints
`ERROR: command exceeded hard timeout of 1s` for its negative timeout fixture;
its final PASS line and exit status `0` are authoritative. Compilation and
Clippy retain only pre-existing vendored/project warnings.

## Accepted Stop Position

This closes the local observer repair only. The failed bundle is not an M1
PASS because it stopped before the first active window completed and before
all four drain checkpoints. M2 and M3 remain blocked.

The next formal action is one fresh user-operated HK macOS M1 from the pushed
repair source. Because the runner changed, build a new release and take fresh
M1 baseline/direct artifacts. Completely disable Clash-TUN and every other
VPN/TUN before baseline and keep them disabled through Knife15 `stop`. Run
`start -> smoke -> m1 -> status -> stop`; on any failure preserve
`status -> snapshot -> stop`. Do not reuse the old baseline/direct artifacts
or tune frozen constants.
