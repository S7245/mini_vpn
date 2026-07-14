# Knife14 H10d16 GSO-Disabled Forward Discriminator Results

Date: 2026-07-13
Base source: `a54fb171ad57dc48902a79f9d31bd08d2ac41802`
Verdict: **FAIL; Task 12 step 4 remains stopped**

## Scope

This was the one authorized reversible follow-up to the prior forward-first
failure. It added a default-enabled Quinn GSO policy seam, proved the disabled
path locally, and then spent exactly one same-window Gate-aligned sing-box
control plus one fresh mini_vpn forward-only P1 with GSO disabled. D16, TUN
MTU, QUIC MTU policy, pool, QUIC windows, chunk behavior, congestion control,
and self-wake were unchanged. No macOS TUN test ran.

The clean Linux build used only the reviewed D16 lifecycle repair, GSO policy,
probe, and runner changes on top of `a54fb17`:

```text
binary_sha256=f10190cf6ccda0d7dea94c65ef2fcd72a48a5b43c4d0379c61894cacf20cd28e
suite_sha256=4010ec563b357403fcbe0ddf74cb0bbf32063946d655d3ef96bb9bdfa8b40c5e
control_runner_sha256=b6bbee22ce20f421901e6dea60d5179cc86ff250362f43cd562b7e20cc5302e7
```

## Local Gate

The policy parser, production default, `TransportConfig` reachability, and a
real 32 MiB disabled-GSO loopback upload were developed red/green. The upload
delivered exact patterned bytes, observed clean EOF, and exceeded
`170 Mbit/s`. The complete local gate passed:

```text
library: 605 passed, 2 ignored
normal harness: 10 passed, 4 ignored
concurrency: 64/64, 256/256, 1024/1024
UDP sweep: 500/500 at 1000/1400/4000/8000B, zero loss
cargo check --features harness: PASS
bash -n / rustfmt / diff check: PASS
```

A post-failure review reran the focused QUIC set: `12 passed` with no failure.
The startup fingerprint and live log both reported
`QUIC GSO policy=disabled`, so this is not an environment fallback or stale
binary result.

## Same-Window Control

The official sing-box `1.13.14` control and mini_vpn used the same Shoes
`v0.2.7` PID on `.111:8443`, the same `.77` target, target-only routing, and
the same temporary socket-buffer settings. The first successful control lacked
the required bilateral captures and was retained only as incomplete evidence;
the corrected captured control below is the sole comparator:

```text
direct forward sender/receiver: 220.352 / 211.100 Mbit/s
control sender/receiver:        177.303 / 175.102 Mbit/s
client socket rb/tb/drop:       16777216 / 16777216 / 0
Exit socket rb/tb/drop:         17250000 / 17250000 / 0
route assertion:                target-only PASS
client/Exit tcpdump drops:      0 / 0
```

The control therefore qualified the window and the endpoint sockets did not
drop traffic.

## GSO-Disabled mini_vpn — FAIL

The fresh forward-only P1 completed, but throughput did not satisfy the
product discriminator because transport loss and TUN drops remained severe:

```text
sender/receiver:                  206 / 193 Mbit/s
intervals:                        20/20 nonzero
iperf retransmits:                30
TUN RX/TX drop delta:             0 / 30
formal QUIC lost-bytes delta:      67,826,613B
final QUIC lost bytes:             73,572,300B
formal congestion-events delta:   40,887
final congestion events:          44,198
final lost packets / sent packets: 57,487 / 446,415
QUIC flow-control blocked deltas:  0
pending / terminal reap at close:  0 / 0
```

This is the same failed edge as the GSO-enabled sample: receiver throughput
alone stayed high while local TUN drops and excessive client-to-Exit QUIC loss
remained. The disabled sample did not improve the prior `30` TUN drops or
`57,632,755B` formal lost-byte delta; its formal loss and congestion counts
were higher.

## Bilateral Capture

Both tcpdump processes reported zero kernel capture drops. Because the Exit
capture used GRO and coalesced multiple wire datagrams, byte totals and timing
buckets are authoritative; raw packet-count equality is not expected.

| Window | Client-out UDP payload | Exit-in UDP payload | Gap | Gap ratio |
| --- | ---: | ---: | ---: | ---: |
| sing-box control | 453,361,295B | 442,185,588B | 11,175,707B | 2.47% |
| mini_vpn GSO disabled | 565,156,612B | 492,300,734B | 72,855,878B | 12.89% |

The mini_vpn bilateral gap is within about one percent of the final Quinn
`73,572,300B` lost-byte count and matches the bytes successfully admitted by
the remote writer. This locates the failure between client UDP egress and Exit
UDP ingress rather than in D16, smoltcp delivery, or tcpdump itself.

GSO-disabled changed packetization but did not bound service bursts:

| Window | Max per 1 ms | Max per 10 ms |
| --- | ---: | ---: |
| sing-box control | 108 packets / 154,346B | 845 / 1,206,107B |
| mini_vpn GSO disabled | 163 packets / 208,640B | 928 / 1,184,383B |

Mini_vpn client egress was predominantly `1280B` UDP payloads. Exit GRO
reported multiples such as `2560`, `3840`, and `5120B`; these are receive-side
coalescing, not oversized packets on the wire. Disabling GSO reduced the prior
mini_vpn 1 ms peak from `261` to `163` packets but did not remove the loss
edge. It therefore falsifies GSO aggregation as a sufficient root while
retaining send-service burst/packet cadence as the active branch.

## Post-Failure Code Review

The GSO policy implementation is reachable and preserves the production
default. No hot-path correctness, ownership, lifecycle, or frozen-profile
regression was found in `src/quic.rs` or `src/tuic.rs`.

The mechanism is nevertheless necessary-only:

1. Quinn-proto `0.11.16` maps disabled segmentation offload to
   `max_datagrams=1` inside one `Connection::poll_transmit` call.
2. Quinn `0.11.11` then loops `poll_transmit`/`try_send` until it has produced
   `MAX_TRANSMIT_DATAGRAMS=20` in one connection-driver poll.
3. When that cap is reached, the driver calls `wake_by_ref` and can immediately
   run again.
4. Quinn-proto's userspace pacer independently clamps its token capacity at
   `MAX_BURST_SIZE=256` packets with a nominal `2ms` burst interval.

Thus GSO-disabled changes the number of datagrams per UDP syscall; it does not
provide a bounded inter-poll send service. The pcap result is consistent with
that code reachability.

The review also found two runner defects:

- `scripts/knife14b-usclient-tunnel-suite.sh` discards the standard-P1 return
  status with `|| true` and has no formal loss/TUN-drop discriminator, so this
  failed sample still produced a successful suite exit and bundle.
- `scripts/knife14b-lowrtt-probe.sh` says curl and DNS must traverse the VPN
  even when the suite intentionally installs only a target `/32` route. Those
  outputs are expected to remain direct in target-only mode and must not be
  presented as failed full-tunnel gold checks.

Neither defect changes the evidence verdict, but both can misclassify future
automation and must be corrected before another paid VPS sample.

## Proposed Next Modification Plan — Awaiting Confirmation

No further implementation or VPS run is authorized by this result.

1. Red/green the runner first: propagate probe failure, add an explicit
   forward discriminator requiring receiver output plus zero TUN drops and a
   bounded QUIC-loss decision, and label target-only curl/DNS checks as not
   applicable. Preserve the existing broader sweep behavior outside the
   explicit discriminator mode.
2. Keep GSO production-default `enabled`; retain the default-off diagnostic
   seam only until final cleanup. Do not spend another GSO-disabled sample.
3. Add one shared, bounded QUIC UDP egress service at Quinn's public
   `AsyncUdpSocket` abstraction instead of forking Quinn or stacking another
   transport parameter. The wrapper must account wire-datagram equivalents
   across all pool connections, return `WouldBlock` when its service quantum
   is exhausted, and rearm through a timer-backed `UdpPoller` without busy
   self-wake.
4. Capacity math for the first fixed tracer is at least
   `170 Mbit/s = 21.25 MB/s`. At a `1280B` UDP payload that is about
   `16,602` datagrams/s. A fixed ceiling of `48` datagram equivalents per
   `2ms` permits `24,000` datagrams/s and `30.72 MB/s` raw UDP capacity while
   sharply bounding the observed `163 packets/ms` edge. This is a code
   invariant, not an environment sweep; adjust it only if deterministic
   capacity math disproves sufficiency before VPS use.
5. TDD with a mock abstract socket and paused time must prove the bound is
   shared across at least two connection pollers, no busy wake occurs, inner
   `WouldBlock` remains correct, and accepted bytes/datagrams are conserved.
   Then the real 32 MiB upload must retain exact delivery, clean EOF, and
   `>170 Mbit/s`; full library, harness, 64/256/1024 concurrency, and UDP
   sweeps remain mandatory.
6. If those gates and a fresh code review pass, spend at most one new
   same-window control plus one fresh forward-only P1 with bilateral capture.
   Require zero TUN drops and removal of the excess client/Exit loss edge. A
   second failure stops again for review; P8, UDP/live-streaming, fake-IP DNS,
   and rearm remain unspent until the forward discriminator passes.

This seam is intended to be sufficient only for the next forward-loss
discriminator. It is not by itself sufficient for final Task 12 product
acceptance. Quinn's existing pacer remains active beneath it; D16, TUN/QUIC
MTU, pool, QUIC windows, chunk, congestion control, and self-wake stay frozen.

## Artifacts And Cleanup

The sanitized report bundle is retained locally at:

```text
/tmp/mini_vpn_h10d16_gso_disabled_discriminator_a54fb17/
mini_vpn_gso_a54fb17_allowed_artifacts.tar.gz
sha256=d6ba8d1922bb5e925e74fd5f4f1d0c4ea3a8722905e785989d913b77a73b5834
```

The bilateral pcaps were analyzed in place, recorded by exact hashes and
zero-drop tcpdump summaries, and removed from both VPSes after producing the
tables above. The `.27` process, TUN, route, clean clone, output directory, and
capture units are gone. Shoes, watchdogs, FIFOs, runtime material, and capture
units are gone from `.111`; UDP `8443` is free and all four temporary socket
sysctls are restored to `212992`. `.77` iperf3 remains active.
