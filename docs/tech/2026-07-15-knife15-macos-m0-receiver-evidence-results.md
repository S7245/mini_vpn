# Knife15 macOS M0 Receiver-Evidence Results

Date: 2026-07-15

Status: **EVIDENCE-SEMANTICS ROOT CAUSE REPAIRED LOCALLY; FRESH M0 PENDING**

## Evidence And Provenance

- bundle:
  `/tmp/mini_vpn_knife15_macos_20260715_013158.tar.gz`
- verified SHA-256:
  `5c04031e5a6bd07640aae2a2cacd4ebe33acb85d1681c196a8201c5f9f2e4327`
- source: `b6f399aebe2c31a02ea82f3703ae44c374008796`
- binary SHA-256:
  `3fda9f9d16635388e6869520b0270ea3e4f7f9d70a742a27f54aad43aad70306`

The source, binary, runner, baseline, route, and secret-scan evidence are
internally consistent. The user's `start -> smoke -> m0 -> status/snapshot ->
stop` operation was correct.

## Exact Failure

The first formal phase actually completed its full `300s` forward TCP
transaction at the requested `14,441,361 bit/s`:

- client sent `541,589,504B` at `14.442 Mbit/s`;
- Target received `541,065,216B` at `14.420 Mbit/s`;
- sender/receiver gap was `524,288B`;
- all 300 client JSON intervals were present, but the client sender interval
  at `213.003-214.003s` reported `0 bit/s`;
- the Target receiver journal had `301` interval/tail rows and zero zero-rate
  rows; at `213-214s` it still received `640 KiB` at `5.24 Mbit/s`.

The old validator applied its no-zero rule to the client root intervals. For
forward TCP those are sender application-write intervals, not the receiving
user's delivery intervals. It therefore stopped M0 with
`reason=invalid_iperf_result` even though the receiver continued to move
buffered data.

## Data-Plane Decision

This run validates the prior relay-lifecycle repair rather than regressing it:

- the phase crossed the old 90-second boundary and both D16 relays ended with
  `clean_queue_lifecycle`;
- there was no `idle_timeout`, `stalled_write_timeout`, reader/writer task
  failure, Broken pipe, or M0 relay reset;
- endpoint conservation remained at or below `61,440B` and ended with
  `live=0B`, `outstanding=0B`;
- macOS utun errors, pump full waits, pump read errors, and TUN flush failures
  were zero;
- the data relay's longest upstream write wait was about `1.548s`.

The sender pause coincided with a real WAN congestion episode. Across the M0
phase the data QUIC connection added about `570` lost packets, `798,197B` of
formal loss, and `173` congestion events while RTT remained about
`166-180ms`. This explains sender backpressure, but the Target receiver stayed
continuous. HK path variability is not permission to tune H10d16 or the M0
load constants.

The summary's three `remote_write_failures` matches came from one earlier
smoke close-tail `Stopped(0)` event represented in three log forms. The M0
epoch itself closed cleanly, so that counter is not the phase failure cause.

## Repair

- Every M0 iperf command now requests `--get-server-output`.
- The Target iperf3 service emits JSON, allowing the client artifact to carry
  `server_output_json`.
- Forward phases validate server receiver intervals; reverse phases validate
  client receiver intervals.
- Both sender and receiver structures remain mandatory. Receiver intervals
  must all be positive; sender intervals may be zero but must remain numeric
  and are counted separately in the summary.
- A sender-only zero interval makes the final summary `REVIEW` without
  aborting the two-hour evidence collection. A receiver zero interval remains
  a phase failure.
- Failure events now distinguish `missing_receiver_evidence`,
  `missing_sender_evidence`, `receiver_zero_interval`, and other invalid
  results.
- The pre-mutation Target readiness transaction requires JSON receiver
  evidence, so an incorrectly configured service fails before TUN or route
  changes.
- The BSD `mktemp` template now ends in `XXXXXX`; it no longer uses one fixed
  literal `.XXXXXX.json` path.

The `.77` service received a reversible systemd drop-in for
`iperf3 -s --json --forceflush`. It passed an independent `.27 -> .77` one-
second capability transaction with positive structured receiver evidence.

## Local Gate And Next Acceptance

The runner internal/external self-tests and shell syntax pass after focused
RED/GREEN replays for sender-only zero, receiver zero, missing receiver JSON,
direction selection, command shape, and final-summary fail-closed behavior.
Commit `4f836b9` (`fix(macos): validate M0 receiver continuity`) contains the
runner and operational-contract repair. Final gates passed: `cargo fmt`, root
workspace tests (`635` passed, `3` ignored) plus main tests (`2` passed), both
macOS runner self-tests, shell syntax, diff checks, release build, and real
iperf3 forward/reverse TCP plus reverse UDP schema qualification. Focused code
review found no unresolved P0/P1 issue.

The failed bundle remains a valid diagnosis artifact but is not M0
acceptance. The next run requires a physical/non-`utun` Target/Exit route, a
fresh direct baseline, the rebuilt runner, one complete M0, and a clean
start/smoke/stop rearm. Current inspection found the HK Target route on
`utun1024`; exit that other VPN/proxy before the next preflight.

Release-It score: **9/10**. Deep readiness, fail-fast behavior, immutable
evidence, structured receiver SLIs, and a tested service rollback are present.
The remaining point is to move the Target systemd drop-in from documented
host configuration into fully versioned infrastructure provisioning.
