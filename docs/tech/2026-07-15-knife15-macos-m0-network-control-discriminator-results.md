# Knife15 macOS M0 Network-Control Discriminator Results

Date: 2026-07-15

Status: **REAL RECEIVER INTERRUPTION CONFIRMED; INTERNAL REGRESSION REJECTED;
SAME-WINDOW PATH CONTROL REPAIRED LOCALLY; FRESH SHENZHEN M0 PENDING**

> Follow-up (2026-07-15): the next real bundle proved that the physical
> `netstat` parser in source `b0fcb76` shifted counters when a Link Address was
> present, while still emitting 27 columns and a false network-control PASS.
> Exit/gateway ping collection and the diagnosis below remain valid, but the
> physical-counter acceptance claim is superseded by repair commit `524139b`
> and
> `docs/tech/2026-07-15-knife15-macos-m0-physical-counter-observer-repair-results.md`.

## Goal And Evidence

Review the next user-executed macOS M0 without blaming operator procedure or
changing any frozen Knife14/Knife15 data-plane setting. The reviewed artifacts
are:

- direct baseline:
  `/tmp/mini_vpn_knife15_macos_baseline_20260715_032306`;
- M0 bundle:
  `/tmp/mini_vpn_knife15_macos_20260715_032723.tar.gz`, SHA-256
  `865aa44038d8b5de5b6b18b72c45239283b3bf9b9ea87f9fafb5b4a8b6b21c20`;
- stop/re-create/smoke/stop bundle:
  `/tmp/mini_vpn_knife15_macos_20260715_051746.tar.gz`, SHA-256
  `823926e6f8e9443fd12d13a857b2285eaa0488f778ac44e7a2e2cf48ba2019d8`.

Both archives passed path and secret scans. Their manifest provenance matches
exact source `b5c39634a13563ad0cea23c6e3ab7875bd775115`, binary SHA-256
`3fda9f9d16635388e6869520b0270ea3e4f7f9d70a742a27f54aad43aad70306`,
and runner SHA-256
`48e1bd0de8d1a3e45feebc3d33563450650a51ad5b939a337c20d797a338b187`.
The pre-run Target and Exit routes used physical `en1`; the operation sequence
was correct.

## M0 Result

The direct receiver baseline was `12.263 Mbit/s` forward and
`42.355 Mbit/s` reverse. M0 therefore offered the frozen relative rates,
including `6,131,504 bit/s` for sustained forward TCP. Two full mixed cycles
completed: forward/reverse TCP, reverse UDP, twelve short TCP connections, and
two fake-IP DNS checks.

Cycle 3 forward ran its full `300s` command and transferred exactly
`230,031,360B` at both sender and Target receiver, averaging
`6.131 Mbit/s`. It nevertheless contained four real Target receiver
interruptions:

- `24.001024-25.001022s`;
- `50.001033-51.001029s`;
- `83.001018-84.001018s`;
- `85.000805-86.001019s`.

Each interval carried zero bytes. The direction-aware no-zero receiver SLI
therefore correctly stopped M0 with `reason=receiver_zero_interval`. This is
not the earlier sender-versus-receiver evidence defect.

## Failure Tree

### Rejected internal causes

- Endpoint conservation passed with a maximum exactly `61,440B`; final
  available/live/outstanding was `61,414/0/0B`, with only `1,409B` maximum
  live reservation and `1,442B` maximum socket-blocked outstanding data.
- utun input/output error counters were zero. The TUN ingress pump had no full
  wait, read error, closure, or flush failure.
- No `idle_timeout`, `stalled_write_timeout`, terminal pending reap, stranded
  D16 queue, or ownership leak occurred. Cycle 3 closed via
  `clean_queue_lifecycle` after writing `230,031,397B`; all queued, leased,
  and reserved bytes were zero.
- The process remained bounded for about `1h50m` until evidence collection and
  stop: RSS `10,096 -> 9,088 KiB` with `37,456 KiB` maximum, FDs `15 -> 15`,
  and threads `11 -> 11`. No log compaction occurred.

The summary's `15` remote-write matches are five earlier short forward
close-tail events represented in three log forms each. They do not overlap the
cycle 3 failure window. Reverse terminal-close tails also ended with zero
global ownership.

### Positive QUIC discriminator

At cycle 3 entry the data connection was near `rtt=180ms`, `cwnd=142,929B`,
`lost=1,387`, `lost_bytes=1,049,909`, and `congestion_events=193`. Around the
receiver interruptions, cwnd contracted through `43,994B`, `38,735B`, and
`25,174B`, while formal loss and congestion events rose. By the cycle-end
snapshot, the connection had added about `550` lost packets, `772,134B` lost
bytes, and `237` congestion events before recovering to multi-megabyte cwnd.

This supports the causal chain QUIC/path congestion -> sender backpressure ->
four Target receiver zero seconds -> recovery and backlog drain. It rejects
endpoint accounting, TUN egress, relay lifecycle, and local resource leakage
as the selected repair branch.

### Attribution limit

The old `network.csv` contained only Target and Exit route-interface strings.
It did not implement the release-readiness plan's required direct/control RTT,
loss, and throughput samples. Consequently the bundle cannot prove whether the
same window also degraded on the physical path or only on the tunnel's
UDP/QUIC path. The correct decision is **highly likely WAN/QUIC congestion,
but exact external-path versus tunnel-path attribution remains unproven**.

## Rearm Result

The independent rearm run passed the requested lifecycle check. A fresh
`utun4` was created, forward/reverse TCP and fake-IP DNS smoke completed, and
stop restored owned routes and removed the TUN. Conservation was at most
`61,281B`, final live/outstanding ownership was zero, and TUN/pump errors were
zero. One expected forward smoke `Stopped(0)` close-tail made the generic
summary `REVIEW`, but it ended with zero queue and ownership and did not block
rearm.

## Local Evidence-Seam Repair

The macOS runner now records a versioned 27-column network sample every
periodic snapshot:

- Target and Exit route interfaces;
- direct ICMP transmitted/received/loss and min/avg/max RTT to the TUIC Exit,
  which remains outside the utun route;
- the same local-link control to the current physical gateway;
- current physical interface MTU, packets, bytes, and input/output errors;
- physical receive/transmit bit rates derived from consecutive counter
  samples.

Raw ping output remains in `network.log`; parsed controls remain in
`network.csv`. Total ICMP loss is valid evidence with unknown RTT, while an
unparseable or absent control remains explicit `unknown`. Formal M0 now:

- proves complete and recent controls after start/smoke, before spending two
  hours on the workload;
- continuously checks that the identity-verified watchdog and a recent valid
  network sample remain alive;
- serializes colliding periodic/manual network collectors;
- requires one network row and valid Exit/physical counters for every process
  sample before final PASS;
- emits `PASS`, `PARTIAL`, or `MISSING` control evidence and makes incomplete
  formal M0 evidence `REVIEW`.

The repair changes no mini_vpn Rust data plane and no frozen rate, duration,
MTU, pacing, queue, pool, QUIC window, chunk, Cubic, GSO, or wake setting.

## Local Gates And Decision

- focused RED/GREEN parser, all-loss, route/gateway, physical-interface,
  composed collector, freshness, summary-envelope, and fail-closed missing-row
  tests: PASS;
- Knife15 internal/external shell self-tests and shell syntax: PASS;
- root all-target tests: library `635 passed`, `3 ignored`; main `2 passed`;
- release build: PASS with only established vendored smoltcp warnings;
- Knife14 low-RTT, US-client-suite, and sing-box-control self-tests: PASS;
- formatting/diff checks and focused review: PASS, no unresolved P0/P1.

M0 remains incomplete and blocks M1. The next formal M0 should run on the
dedicated Shenzhen Mac, as required by the accepted task order, using a fresh
physical-route baseline and the rebuilt runner. HK remains useful for short
qualification, but another HK M0 is not a substitute for the planned Shenzhen
gate. If the receiver interruption repeats, correlate its exact seconds with
Exit/gateway RTT/loss, physical rates/errors, and QUIC deltas. Do not relax the
receiver SLI or tune frozen constants.
