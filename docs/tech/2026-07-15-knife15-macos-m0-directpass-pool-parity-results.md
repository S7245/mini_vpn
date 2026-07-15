# Knife15 macOS M0 Direct-Pass Pool-Parity Results

Date: 2026-07-15

Status: **OPERATOR CORRECT; DIRECT GATE PASS; GLOBAL TCP POOL PARITY FAILURE
CONFIRMED; LOCAL REPAIR PASS; FRESH USER-RUN M0 PENDING**

## Evidence

- baseline:
  `/tmp/mini_vpn_knife15_macos_baseline_20260715_102641`;
- physical direct result:
  `/tmp/mini_vpn_knife15_macos_direct_20260715_102948`;
- direct result SHA-256:
  `63606a7677526ed930c239479ea04b53cb85bd12fdecdc4f8caa0749da8099cb`;
- M0 bundle:
  `/tmp/mini_vpn_knife15_macos_20260715_104417.tar.gz`;
- M0 bundle SHA-256:
  `5743b352d76b418d45caa0f71b3592a70c9323bc3a6908833a59a90f7b6a0a50`;
- exact source:
  `5c127ebec85e311040f24881adbc45a579485bb3`.

The archive checksum is exact, contains regular files only, and binds the
expected source, release binary, runner, baseline, and direct result hashes.
The direct discriminator completed at `10:34:50Z`; M0 prepared at `10:45:05Z`,
so the evidence was 615 seconds old and valid under the 15-minute gate.

## Operator Verdict

The user followed the procedure correctly. Start became ready at `10:44:19Z`,
smoke completed at `10:45:05Z`, and M0 failed at `10:58:25Z`. The later
snapshot/stop ran at `10:59:55Z` and cleaned up the owned TUN/routes. Password
delay, premature cleanup, stale direct evidence, and target/route/provenance
mismatch are rejected.

## Direct Gate

The 300-second physical direct forward run transferred 248,643,584 bytes at
both sender and Target receiver, averaged 6.626752 Mbit/s at the receiver, and
contained 300 complete positive receiver intervals. Target and Exit remained
on `en1`. The physical path therefore met the exact formal continuity gate
immediately before M0.

## M0 Phase Results

- sustained forward TCP: PASS, 248,512,512 Target receiver bytes,
  6.622875 Mbit/s, zero receiver interruptions;
- sustained reverse TCP: PASS, 812,908,544 local receiver bytes,
  21.677250 Mbit/s, zero receiver interruptions;
- reverse UDP: PASS, 480,772,440 receiver bytes, 21.367346 Mbit/s,
  1.427655% loss, zero receiver interruptions;
- short forward TCP 1: FAIL, Target receiver interruption.

The failed short flow offered 10,606,476 bit/s. The client reported
10,616,832 sender bytes but the Target received only 3,670,016 bytes at
2.882108 Mbit/s. The first two complete Target receiver seconds were zero; the
client also recorded two later sender-zero seconds. The sender/receiver gap was
6,946,816 bytes.

## Internal And External Discriminators

Internal data-plane invariants remained clean:

- endpoint conservation PASS, maximum 61,440 bytes, final
  `61,414/0/0` available/live/outstanding;
- maximum live reservation 1,409 bytes and outstanding 38 bytes;
- ingress pump high-water 76/500, zero full waits/read errors;
- zero physical-interface/TUN errors, stable FD/thread counts 15/11, and no
  positive RSS slope;
- no endpoint pacing delay/socket-blocked event, ownership leak, log
  compaction, reconnect, or health failure.

Exit and gateway controls covering the failed short window both had zero ICMP
loss; Exit RTT was about 169-170 ms and gateway RTT about 4 ms. Earlier samples
did show intermittent Exit ICMP loss, so the broader path was not perfect, but
there was no same-window whole-host outage. Exit sing-box was active with zero
restart count and logged both TUIC Connects plus both Target direct outbound
connections at 0 ms. Server dial delay is rejected.

## Positive Pool-Parity Evidence

The connection-opening order was deterministic:

| Phase | Control | Data |
|---|---:|---:|
| sustained forward | conn0 stream2 | conn1 stream2 |
| sustained reverse | conn0 stream3 | conn1 stream3 |
| reverse UDP | conn0 stream4 | UDP on primary |
| short forward 1 | conn1 stream4 | conn0 stream5 |

The reverse UDP phase opens only one TCP control flow, making the global
round-robin cursor odd. It therefore inverted the next control/data pair. The
short data stream on connection 0 began with cwnd 12,000 bytes and accumulated
loss/congestion while expanding. Its D16 writer waited up to 3.864382 seconds
and the Target payload arrived too late. Connection 1 handled only the small
control flow.

The data stream's lack of reverse payload is normal for a forward-only test;
`tuic-tcp-stream-pending` is not a liveness failure signal here. Connection 0
was alive and exchanging ACK/datagram traffic. Stale-timeout or liveness-probe
tuning would therefore not repair this placement defect.

## Decision

The accepted repair is lease-aware least-active placement with atomic
reservation and stable primary-first ties. It preserves pool=2 and
primary-owned UDP/health while making every idle control/data pair select
`conn0`, then `conn1`, independent of historical opening parity.

Do not retry global round-robin, collapse TCP to one slot, tune pool/pacing/
QUIC constants, or relax the receiver SLI. If a future M0 shows corrected
placement but the short flow still fails, stop and reopen the flow-specific
QUIC/path architecture branch with a usable same-path mature-client control.

## Local Repair Result

Commit `c945a41` removes the global cursor and makes `live_tcp_conn` reserve
the least-active slot with stable primary-first ties. Each reservation owns:

- the existing active relay RAII lease;
- a per-slot RAII preparation gate held across the slot-mutex wait, optional
  probe/reconnect, and connection clone;
- one wake notification on preparation release.

The preparation gate was added after code review found that an active-count
reservation alone could still allow a later same-slot opener to overtake an
idle-exclusive reconnect/clone. Focused tests now lock down phase-history
independence, simultaneous first reservations, no preparation overtaking,
busy-wait wakeup, empty/saturated fail-closed behavior, and diagnostic fields.

Local acceptance passed: focused pool tests `15/15`; root all-target library
`640 passed + 3 ignored` and main `2/2`; release build, Clippy, Knife15 and
Knife14 shell gates, formatting, and diff checks all passed. Final review has
no unresolved P0/P1. No frozen constant, pool size, workload rate, or SLI was
changed.

The next evidence must come from a freshly rebuilt exact-source binary, new
baseline, and new 300-second direct discriminator before user-run
start/smoke/M0. The previous direct result is valid diagnosis evidence but
cannot authorize a new-source M0. M1 remains blocked until corrected M0 and an
independent rearm both pass.
