# Knife15 M2 Post-QUIC Exit-to-Target Forwarding Results

Date: 2026-08-06

## Result

Exact-source commit `727f00b1023935ec2be6faa1e30b2d055191867d`
did not pass formal M2. The failure is a real Target-receiver continuity
violation in cycle 1 `short-forward-1`, after all cheap and long preconditions
passed. It is not an operator error, baseline/direct failure, IPv6 leak,
cleanup failure, local TUN/D16 failure, Endpoint pacing failure, or a frozen
parameter branch.

The selected unresolved seam is after the Exit has acknowledged TUIC/QUIC
stream data and before the Target has received the corresponding TCP bytes.
The reviewed new-stream startup-turn implementation was reached and consumed,
but it was not sufficient. Do not tune its priority delta, turn count, pool,
MTU, QUIC windows, chunk size, Cubic, GSO, Endpoint constants, or self-wake.

## Immutable Client Evidence

- Artifact:
  `/tmp/mini_vpn_knife15_macos_20260806_035626.tar.gz`
- SHA-256:
  `4013a05b0288e28cdbb9777cf8d063618a1260635cde1eeaf73daf7adaffd25a`
- Source: `727f00b1023935ec2be6faa1e30b2d055191867d`
- Binary SHA-256:
  `9aed13838e30708cdd495367403e49636fd0a4202c19a50564d96e943eb96a1c`
- Runner SHA-256:
  `2a57206ef02cc56b168f3d9622260d69c17f3dd27366b28afcd8203191ee3918`
- Baseline: `38.747/47.278 Mbit/s`, zero receiver gaps.
- Direct discriminator: `19.361574 Mbit/s` for 300 seconds, zero sender or
  receiver gaps.
- Smoke, IPv6 `safe_absent`, full-tunnel, real-client, controlled drain, and
  cleanup gates: PASS.

M2 cycle 1 passed:

- 300-second forward: `726,532,096B`, about `19.36 Mbit/s`;
- 300-second reverse: about `23.64 Mbit/s`;
- 180-second reverse UDP: `1.327477%` loss, below the frozen `3%` limit.

The first 10-second short forward then failed:

- Mac sender: `10,092,544B`;
- Target receiver: `4,325,376B`;
- sender/receiver gap: `5,767,168B`;
- complete Target receiver-zero intervals: `0-1.001037s` and
  `2.001039-3.001045s`;
- complete local sender-zero intervals: `2-3s`, `3-4s`, and `5-6s`.

This is not a partial command tail. The Target received sparse multiples of
`131,072B`, including `0B`, `131,072B`, `0B`, `262,144B`, and `131,072B` in
the first five complete intervals.

## Exact Data-Plane Correlation

The iperf control stream used qualified installed conn1 generation 2 and saw
its first response in `162ms`. The bulk data stream used qualified conn0
generation 1. Its startup service was definitely consumed:

```text
first_payload_bytes=37 priority_before=1 priority_after=0 state=consumed
```

The data writer accepted `6,298,300B` and recorded a maximum write wait of
`4,236,944us`. While the Target delivery stalled:

- the Mac QUIC connection continued receiving ACK frames;
- the client iperf sender admitted about `3.9 MiB` in its first second;
- the TUIC data stream received no response bytes, as expected for the bulk
  direction after the small control exchange;
- the local smoltcp receive queue remained bounded at about `367,720B`;
- Endpoint conservation never exceeded `61,440B` and ended at
  `61,414/0/0B`;
- D16 queued, leased, and reserved ownership was zero at close;
- same-window Exit and gateway controls were 3/3 with zero loss;
- physical and TUN interface error counters did not move.

The Exit sing-box log recorded immediate TUIC inbound and direct-outbound
creation for both connections, with no matching error. Standard sing-box info
logging does not record first outbound TCP write, bytes, blocking, retransmits,
or Target ACK timing, so it cannot close the remaining seam.

## Exit-to-Target Discriminators

The first discriminator reproduced the exact M2 ordering directly on the Exit,
without macOS, TUN, or TUIC multiplexing:

1. forward TCP, 300 seconds at `19,373,578 bit/s`;
2. reverse TCP, 300 seconds at `23,638,822 bit/s` and 1024-byte writes;
3. reverse UDP, 180 seconds at `23,638,822 bit/s` and 1160-byte payloads;
4. forward TCP, 10 seconds at `30,997,725 bit/s`.

Artifact:
`/tmp/mini_vpn_knife15_exit_target_20260806_054000.tar.gz`, SHA-256
`23d1b8447ce74fc7ef880d940aae1f7359468cb1ebf3f8081da141f45e871291`.

Results:

- forward Target receiver: 300/300 nonzero intervals, `726,532,096B`, minimum
  interval `18.856 Mbit/s`, zero retransmits;
- reverse Exit receiver: 300/300 nonzero intervals, `886,459,392B`, minimum
  interval `23.454 Mbit/s`, zero Target retransmits;
- reverse UDP: 180/180 nonzero intervals, `0/458,515` packets lost;
- final short Target receiver: 10/10 nonzero intervals, first interval
  `31.448 Mbit/s`, minimum interval `30.409 Mbit/s`, zero retransmits.

A second discriminator opened sixty consecutive fresh 10-second direct TCP
connections from Exit to Target at the exact short-forward rate.

Artifact:
`/tmp/mini_vpn_knife15_exit_target_short60_20260806_055300.tar.gz`, SHA-256
`2a1eef22a7ab56129e311a6543247ac5e38ee235e49a6f85666689e187c5d3dc`.

Results:

- 60/60 commands completed;
- 600/600 complete Target receiver intervals were nonzero;
- total Target receiver bytes: `2,327,838,720B`;
- minimum complete receiver interval: `30.378 Mbit/s`;
- total sender retransmits across all sixty connections: `1`.

These controls do not prove that an instantaneous remote path transient was
impossible in the historical M2 window. They do reject a persistent or
readily recurring bare Exit-to-Target startup/path limitation and select the
TUIC Exit receive-to-direct-TCP copy boundary for the next falsifiable probe.

## Architecture Decision

The one-turn startup implementation is retained only as bounded lifecycle-safe
code; it is rejected as sufficient initial-stream service. Blind replay on a
second TUIC stream is not allowed: TUIC v5 has no Connect response, and a QUIC
ACK proves peer transport receipt rather than Target application delivery.
Replaying arbitrary TCP bytes could duplicate non-idempotent operations.

The next stage is observability and qualification, not another client constant
tweak:

- capture bounded, 96-byte header-oriented Exit-to-Target packet snapshots
  around one exact mixed
  cycle;
- sample the matching Exit kernel TCP socket state;
- stop after the first exact `300 + 300 + 180 + 10` second cycle;
- keep formal M2 and M3 blocked until the seam is classified and the chosen
  architecture passes the short qualification plus all local/review gates.
