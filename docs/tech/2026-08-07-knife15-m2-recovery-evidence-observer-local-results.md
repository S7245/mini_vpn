# Knife15 M2 Recovery Evidence Observer Local Results

Date: 2026-08-07

Status: **LOCAL GATES PASS; ONE MAC QUALIFICATION REQUIRED; FORMAL M2 REMAINS
BLOCKED**

Implementation: `847a5d7` (`fix(knife15): make recovery evidence
observation-only`)

Failure source:
`docs/tech/2026-08-07-knife15-m2-ordered-gap-qualification-failure-results.md`.

Architecture:
`docs/tech/2026-08-07-knife15-m2-recovery-evidence-observer-architecture-spec.md`.

## Outcome

The ordered-gap Endpoint migration authority is removed. Exact Quinn receive
progress remains available only as bounded diagnostic evidence, and a new
pure `RecoveryEvidenceObserver` records exact writer-Pending episode starts
and terminal ACK-service aggregates. Neither evidence branch can rebind,
reset, replay, retry, close, or replace a connection or stream.

The existing writer ACK-stall Endpoint rebind, writer plus PLPMTUD
connection-local path reset, and UDP-demand generic rebind remain unchanged.
No D16, MTU, pool, QUIC window, chunk, Cubic, GSO, Endpoint pacing, self-wake,
or recovery-bound value changed.

## Safety And Locality

- Ordered-gap anchors are exact to stable connection identity, reader,
  stream, read offset, and next received offset.
- One persistent gap produces at most one observation record; prefix progress,
  changed gap, close, inactivity, or identity replacement clears ownership.
- Writer anchors are exact to stable connection identity, writer, stream, and
  Pending episode. Start and end records preserve first/final sampled ACK
  bytes, ACK-progress sample count, maximum Pending, and maximum ACK stall.
- The observer and D16 reader registration exist only under the established
  `MINI_VPN_TCP_DIAG=1` switch, so the production default adds no observer
  aggregation or read-progress registry ownership.
- The runner rejects any legacy successful or failed
  `trigger=tcp_ordered_read_gap` action and any malformed recovery-evidence
  line. Summary output retains exact ordered-gap and writer start/end counts.

## TDD And Review

Focused TDD passed:

```text
recovery evidence policy:              3 passed
existing Endpoint recovery policy:    16 passed
ordered-read registry lifecycle:       1 passed
rebind concurrency contract:          30 repeated passes
```

The concurrency test initially asserted that every live connection must still
report generation zero immediately after rebind. Quinn sends a PING during
rebind, so loopback can authenticate generation one before the test task is
rescheduled. The repaired contract accepts only old/current generation at
that asynchronous boundary and still requires both established connections
to reach exactly generation one after explicit round trips. This is a
test-contract repair, not a product or frozen-value change.

Final code review checked false positives, identity and episode reuse,
boundedness, action precedence, lock placement, log volume, default-off
overhead, runner schema validation, and TCP/UDP/TUN/D16 regressions. Two
findings were repaired before the final gates: default-off D16 read registry
ownership was removed, and a fresh ordered-gap episode was explicitly tested
after prefix progress. No unresolved P0/P1 remains.

## Capacity And Full Gates

The exact 32 MiB Endpoint gate executed one test and produced:

```text
sender:                 240.370 Mbit/s
available/live/outstanding: 61,440/0/0B
socket would-block:     0
exact bytes and EOF:    PASS
```

The failed Mac short phase offered only `19.221146 Mbit/s`, so the unchanged
Endpoint capacity remains more than sufficient. The conservation invariant
remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= 61,440B
```

Complete gates:

```text
root library:                     684 passed, 3 ignored
main binary:                        2 passed
concurrency integration:           10 passed, 4 ignored
release build:                      PASS
established all-target Clippy:      PASS (existing warnings only)
Knife15 internal/wrapper shell:     PASS
Knife14/Exit observer shell:        PASS
vendored Quinn:                     40 passed, 3 ignored; doc 1 passed
vendored Quinn integration:         1 expected ignored
vendored quinn-proto:              311 passed; docs 3 passed
root doc compile:                    PASS
fmt/diff/vendor-patch/secret:        PASS
```

The first strict Clippy command incorrectly promoted nineteen established
repository lints plus one new expression to errors; the new expression was
fixed and the documented warning-tolerant lane passed. The first standalone
Quinn command also bypassed the root local-proto patch and selected registry
`quinn-proto`; it was rejected, then rerun after `cargo tree` proved the
absolute local `third_party/quinn-proto-0.11.16` dependency. Neither invalid
command is counted as a gate.

## Qualification Boundary

Run exactly one fresh two-cycle Mac qualification with the Exit observer
active:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop
```

It normally schedules about thirty minutes and can only produce
`PASS_NON_ACCEPTANCE`. Preserve `status/snapshot/stop` after any post-start
failure. Do not run formal M2 or repeat/tune unchanged.

Interpret paired evidence as follows:

- receiver zero plus exact writer ACK progress and declining Exit application
  supply selects client-to-Exit per-stream scheduling/service;
- receiver zero plus continuous Exit supply and broken Target writes selects
  the mature-server forwarding seam;
- no receiver zero is a healthy differential comparator only.

Formal M2 and M3 remain blocked until the next architecture discriminator is
classified.
