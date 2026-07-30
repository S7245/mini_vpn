# Knife15 HK Formal M1 Acceptance Results

Date: 2026-07-30

## Decision

The exact-source HK formal M1 bundle is accepted:

```text
bundle=/tmp/mini_vpn_knife15_macos_20260729_105127.tar.gz
sha256=c263507c52d0146a39a74516b90a51b36fe43d082ea1b897d8f6c9c7dffc1b91
source_commit=ee1a42383097090e36146c33dc33fececa135b44
```

The user operated the run correctly. The immutable archive hash matches, the
fresh physical baseline and 300-second direct discriminator passed, every
planned M1 traffic/idle/drain window completed, and cleanup restored the
process, TUN, and owned routes.

The reported `acceptance SLO mismatch` was a two-part observer false negative,
not a data-plane, path-quality, operator, or frozen-value failure:

1. `en0` already had 16 lifetime input errors before the run. The value stayed
   exactly 16 through the formal M1 window and until stop, but the old summary
   counted every nonzero cumulative row as a new error.
2. Two clean D16 reader tasks ended after handing their final
   `19,456B`/`18,944B` leases to smoltcp. The same handles then proved an equal
   local-EOF send queue, drained it to zero, and closed before reuse. The old
   global grep rejected the transient relay-task snapshot without following
   the owned egress lifecycle to its terminal record.

Focused RED/GREEN repairs make physical errors delta/reset-aware and accept a
nonzero clean relay lease only when the same handle proves:

```text
clean closed queue with queued=0 and reserved=0
-> equal remote_eof local send queue
-> terminal_pending_reap_bytes=0 and send_queue=0
-> no handle reuse between those records
```

Missing/malformed evidence, counter movement or reset, an open terminal queue,
queued/reserved bytes, unequal ownership, missing final drain, terminal
ownership, or cross-epoch reuse remains fail-closed.

No Rust data-plane code, D16 value, MTU, pool, QUIC window, chunk, Cubic, GSO,
Endpoint pacing value, workload rate, duration, or M1 SLI changed.

## Provenance And Preconditions

The bundle manifest records:

```text
runner_sha256=480338657cccbe6590626c574e0ac5923e175996f0f68ff9bcfab662777af6a2
binary_sha256=2f358f07569baa0de641c87ab6029a8a9c23099ecde1acd5ef4029949262a5fa
target=43.130.32.77
target_route_before=en0
exit_host=43.153.32.33
exit_route_before=en0
dns_target=8.8.8.8
```

The bound baseline was fresh and direction-aware:

```text
forward_receiver=22.303867 Mbit/s
reverse_receiver=51.226517 Mbit/s
```

The direct discriminator completed at `10:50:42Z`, 93 seconds before M1
preparation, and passed 300 seconds at:

```text
receiver_bytes=418250752
receiver_bps=11147205
receiver_zero_intervals=0
sender_zero_intervals=0
```

Smoke then completed forward/reverse TCP plus fake-IP DNS and reached exact
TCP-pool idle before formal M1 registration.

## Complete Formal Timeline

The formal controller prepared at `10:52:15Z`. The exact `28,800s` schedule
completed at `19:07:49Z`; the old observer wrote its mismatch at `19:07:56Z`.

Raw events and artifacts prove:

```text
active_windows=5/5
cycles=30/30
DNS=30/30
idle=3/3
resume=3/3
final_drain=1/1
phase_results=332/332
TCP_results=302/302
UDP_results=30/30
phase_failures=0
health_failures=0
invalid_results=0
```

The run transferred approximately:

```text
sum_sent_bytes=62092767520
sum_received_bytes=61187617616
```

All 332 result files retain valid structured sender and receiver evidence.

## Formal M1 SLO Replay

The raw immutable evidence was copied to a temporary review directory. Replay
restored only `m1.status=complete`, the state already written by
`run_m1_schedule` immediately before the old summary changed it to `failed`.
No workload JSON, event, network sample, process sample, checkpoint, or
mini_vpn log record was changed.

The repaired public summary path reports:

```text
m1_timeline_evidence=PASS
m1_result_integrity_evidence=PASS
m1_result_evidence=PASS
m1_dns_evidence=PASS
m1_checkpoint_evidence=PASS
m1_sample_coverage_evidence=PASS
m1_remote_write_evidence=CLASSIFIED_REVIEW
m1_slo_evidence=PASS
formal_m1_acceptance=PASS
```

The formal quality bounds are inside the frozen limits:

```text
Target receiver zero intervals=0
TCP maximum sender/receiver gap=10485760B       limit=16777216B
UDP maximum result loss=2.446087%               limit=3.0%
Endpoint conservation maximum=61440B            limit=61440B
Endpoint final available/live/outstanding=61403/0/0B
log compactions=0
TUN interface error samples=0
physical interface error movements/resets=0
```

The 22 sender-zero intervals remain visible REVIEW evidence. They occur only
in forward client-sender views across six results; every corresponding Target
receiver interval is positive. Sender cadence is not substituted for the
direction-aware receiver SLI.

## D16 And Boundary Classification

The bundle contains 638 D16 relay-close records:

```text
400 clean_queue_lifecycle, zero queued/leased/reserved
2   clean_queue_lifecycle, transient leased egress proved drained
148 local_socket_terminal, zero queued/leased/reserved
88  remote_write_failed, zero queued/leased/reserved
```

All 88 raw upstream-write errors are:

```text
Custom { kind: ConnectionReset, error: Stopped(0) }
```

There is no `stalled_write_timeout`. Every canonical remote-write terminal
record has `queue_closed=true` and zero queued, leased, and reserved bytes.
Keeping these expected iperf command-boundary resets as
`CLASSIFIED_REVIEW` preserves visibility without treating them as receiver or
ownership failure.

The two transient leases are not waived by reason text. For each, the replay
requires the same handle to show:

```text
relay lease 19456B -> local EOF send_queue 19456B -> handle close send_queue 0
relay lease 18944B -> local EOF send_queue 18944B -> handle close send_queue 0
```

Both terminal handle records have zero pending reap, and neither handle is
reused before its drain proof completes.

## Resources And Controls

All four post-drain checkpoints are valid:

```text
labels=idle-1,idle-2,idle-3,final
RSS first/final/max=9392/9216/9392 KiB
FD first/final/max=15/15/15
threads first/final/max=11/11/11
checkpoint ownership failures=0
```

The process-wide RSS maximum was a bounded transient `45,696 KiB`; it returned
to `9,168 KiB` by the final packaged sample. FD and thread counts remained
15/11.

During formal M1, 975 same-window physical controls were recorded. The local
gateway had zero loss in every sample. Exit ICMP had six nonzero three-probe
samples, including one 100% sample, but recovered without a receiver gap or an
incomplete command. Physical `en0` errors stayed:

```text
ierrs=16 -> 16
oerrs=0 -> 0
```

The pump high-water was `182/500`; full waits, read errors, TUN flush
failures, send-slice errors, terminal late payload, permit terminal drops,
log compactions, and Endpoint rebind failures were zero.

The user stopped at `23:54:24Z`, after preserving several additional hours of
idle evidence. Cleanup completed one second later and restored the process,
TUN, target route, Exit route, and DNS route. The delayed stop did not cause
the earlier summary mismatch.

## Local Gates And Review

```text
Knife15 runner self-test                         PASS
Knife15 external wrapper self-test               PASS
Knife14 low-RTT self-test                        PASS
Knife14 US-client suite self-test                PASS
Knife14 sing-box control self-test               PASS
shell syntax                                     PASS
root all-targets + harness                       667 passed / 3 ignored
main                                               2 passed
integration harness                              10 passed / 4 ignored
cargo build --release                            PASS
cargo clippy --all-targets --features harness    PASS (existing warnings)
cargo fmt --all -- --check                       PASS
git diff --check                                 PASS
changed-content secret scan                      PASS
real immutable-bundle replay                     formal M1 PASS
code review                                      no unresolved P0/P1
```

Code review found and repaired one P1 in the first observer implementation:
zero-byte terminal snapshots now also require `queue_closed=true`.

## Accepted Stop Position

Knife15 M1 is complete. Do not repeat baseline, direct, smoke, diagnostic, or
formal M1 for this gate.

M2 planning is now unblocked. The next coherent stage is to write the M2
24-hour real-client soak architecture spec and implementation plan, including
bounded logs, product-like TCP/UDP/video/DNS activity, idle periods, resource
plateau evidence, route/DNS leak checks, and Shenzhen/HK path attribution.
M3 recovery events remain sequenced after the M2 runner and acceptance
contract are defined; each recovery event must remain single-variable.
