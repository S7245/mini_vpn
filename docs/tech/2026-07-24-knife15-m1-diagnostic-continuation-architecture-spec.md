# Knife15 M1 Diagnostic Continuation Architecture Specification

Date: 2026-07-24

Status: **Approved for local TDD implementation**

## Stage Goal

Add a separate `m1-diagnostic` runner action that preserves the frozen M1
eight-hour workload but does not discard the rest of the run after a
path-quality observation. A complete diagnostic run must show how receiver
continuity and reverse-UDP quality evolve across all five active windows,
three drains, and the post-churn period.

This action is an evidence collector. It can never satisfy formal M1
acceptance and does not unblock M2 or M3.

## Motivation

The exact-source `297dee9` HK run completed about three hours and twenty
minutes before one TCP phase reported three complete Target receiver-zero
seconds. Two earlier reverse-UDP phases also exceeded the frozen `3%` SLO.
The failures correlated with direct Exit degradation and bulk-connection QUIC
loss while local ownership, TUN service, Endpoint conservation, resources,
and cleanup remained healthy.

Repeating the unchanged fail-fast M1 can spend another multi-hour window only
to rediscover the first path-quality event. A diagnostic continuation lane
turns that same time into a complete longitudinal artifact without weakening
formal acceptance.

## Frozen Boundary

`m1-diagnostic` reuses the formal M1:

- exact `28,800s` schedule and all segment durations;
- baseline-derived quiet, steady, and churn offered rates;
- `302` TCP, `30` reverse-UDP, `30` DNS, and `332` total result targets;
- target-only routes, TCP pool idle precondition, fresh baseline/direct
  provenance, and `30s` process/interface/network sampling;
- every Knife14 H10d16, Endpoint pacing, QUIC, MTU, pool, GSO, and recovery
  value;
- formal result parsers and the final resource, ownership, recovery, and
  conservation bounds.

The existing public `m1` action and its fail-fast behavior remain unchanged.

## Failure Classification

Only valid, complete data-quality observations are continuable:

1. a TCP or UDP result contains one or more complete direction-aware Target
   receiver-zero intervals;
2. a valid reverse-UDP result reports loss greater than `3.0%`;
3. after the timeline, the maximum valid TCP sender/receiver byte gap exceeds
   `16,777,216B`.

The first two are recorded immediately after each completed iperf command.
The third is a final aggregate observation and cannot shorten the timeline.

Everything else remains fail-fast:

- iperf or sleep command failure and hard timeout;
- missing, malformed, wrong-direction, or otherwise invalid iperf evidence;
- DNS command or fake-IP answer failure;
- mini_vpn, watchdog, TUN route, Exit route, or network-control health failure;
- log compaction or insufficient sample coverage;
- Endpoint conservation, final ownership, checkpoint, resource, rebind, D16,
  pump, send, or TUN-flush safety mismatch;
- signal interruption or workload identity failure.

This classification prevents a broad “keep going” switch from hiding a
product or evidence failure.

## Evidence Contract

The diagnostic action uses the existing `m1/`, `m1-workload.txt`,
`m1-checkpoints.csv`, and `m1.status` artifact family so the immutable bundle
and existing parsers keep one M1-shaped data set. It additionally writes:

```text
m1-mode
m1-diagnostic-violations.tsv
```

`m1-mode` is exactly `diagnostic`. The TSV schema is:

```text
timestamp	cycle	phase	kind	value	detail	evidence
```

Kinds are:

- `receiver_zero_interval`, with the number of complete zero intervals and
  their relative iperf ranges in `detail`;
- `udp_loss_percent`, with the numeric percentage;
- `tcp_sender_receiver_gap_bytes`, with the aggregate maximum.

All evidence paths are run-directory-relative. Every continuable phase emits
both a `m1-diagnostic phase violation` event and the normal phase-complete
event. It must not emit a phase-failed event.

Final status is one of:

- `diagnostic_complete_clean`;
- `diagnostic_complete_with_violations`;
- `failed`;
- `interrupted`.

The summary must publish diagnostic mode, violation count, timeline/result
integrity, safety evidence, and `formal_m1_acceptance: NOT_APPLICABLE`.

## Invariants

1. `SOAK_CONTINUE_DATA_QUALITY` defaults to `0` and is set to `1` only by the
   `m1-diagnostic` action or a focused self-test fixture.
2. The continuation predicate accepts only
   `receiver_zero_interval`; malformed or missing evidence cannot be
   converted into a violation.
3. UDP loss is inspected only after the ordinary UDP validator returns `ok`.
4. A diagnostic completion requires the same exact result/DNS/timeline
   cardinality and the same non-data-quality safety envelope as formal M1.
5. A diagnostic status or violation can never produce
   `m1_slo_evidence: PASS`.
6. Workload PID identity, signal handling, stop, snapshot, and bundling own
   `m1-diagnostic` exactly as they own `m1`.

## Failure Discriminators

The completed artifact supports comparisons across:

- receiver-zero interval positions and the nearest `network.csv` Exit/gateway
  samples;
- reverse-UDP loss windows and same-window physical controls;
- conn0/conn1 QUIC loss, congestion, cwnd, PLPMTUD, ACK progress, and rebind
  events in `mini_vpn.log`;
- idle/resume checkpoints and post-churn resource/ownership plateaus.

A later healthy-control receiver gap selects connection isolation/failover
architecture work. Repeated loss aligned with Exit degradation selects path
qualification or multi-path product planning. Neither result authorizes
frozen-value tuning.

## Stop Rules

- Expected RED fixtures may enter the corresponding minimal GREEN change.
- Unexpected local regression failures are repaired before any user TUN run.
- A diagnostic safety failure stops immediately and preserves
  `status/snapshot/stop` evidence.
- Data-quality violations continue only to the end of the unchanged schedule.
- Formal M1 must still pass in a separate fresh run before M2/M3.
