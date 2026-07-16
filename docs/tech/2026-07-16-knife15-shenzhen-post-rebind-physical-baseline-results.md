# Knife15 Shenzhen Post-Rebind Physical Baseline Results

Date: 2026-07-16

Status: **FORMAL M0 BLOCKED BEFORE TUN; PHYSICAL CONTINUITY FAILED; NOT A
MINI_VPN REGRESSION**

Candidate source: `fe3ec83d42068687f8c031cf3e93f198ab337f3d`.

## Scope

These were pre-TUN physical baseline attempts on the Shenzhen Mac after the
endpoint socket-rebind implementation was pushed. Baseline does not execute
the release binary, create a TUN, or route Target traffic through mini_vpn.
It therefore measures only the Shenzhen-to-Target physical path and the
iperf3 infrastructure.

## Attempt 1 — Data Phase Did Not Start

Evidence:

- directory:
  `/tmp/mini_vpn_knife15_macos_baseline_20260716_134708`;
- forward SHA-256:
  `f401897a8c542d34f3093e195d5f5d6c44b1ea69ae46051a4030e98737b48940`.

The client connected from `192.168.110.7` to `43.130.32.77:5201` and requested
one 20-second forward TCP stream. It then produced zero intervals and reported
`control socket has closed unexpectedly`.

The Target independently accepted both the control and data connections from
the Shenzhen public address, received no data payload, and reported
`idle timeout for receiving data`. The iperf3 service remained active with
`NRestarts=0` and continued listening on `5201`. This rejects operator error,
closed port, service restart, and script validation as causes. The physical
data phase stalled before producing evidence.

## Attempt 2 — Complete But Discontinuous

Evidence:

- directory:
  `/tmp/mini_vpn_knife15_macos_baseline_20260716_135653`;
- forward SHA-256:
  `7902521ed9bd92dc27ad34c0a8b1ef4f03081718652a7275245c9180adffac74`;
- reverse SHA-256:
  `af723ae95eadd886f06b3a510fdff01412d63c4a75fe9c2231136403848d280d`.

Both directions completed without an iperf JSON error:

```text
forward Target receiver = 6,553,600B / 2.598 Mbit/s
forward zero intervals  = 2 (one complete second plus the short tail)
forward retransmits     = 24

reverse local receiver  = 245,760B / 0.098 Mbit/s
reverse zero intervals  = 4 (three consecutive plus one later second)
reverse retransmits     = 50
reverse RTT             ~= 166-190ms
reverse minimum cwnd    = 1,388B
```

The reverse observer used the accepted `1KiB` application block. At about
`12KiB/s` average receiver delivery, it normally exposes multiple blocks per
second, yet three consecutive complete receiver seconds were zero. The Target
sender also spent many intervals at zero with repeated retransmission and a
one-to-two-segment cwnd. This rejects application-block quantization as the
cause of the four receiver zeros.

The Target service remained active with zero restarts throughout. Low average
speed is not a failure condition, but complete receiver-zero seconds are a
physical continuity failure under the unchanged SLI.

## Decision

Neither directory is valid `M0_BASELINE_DIR`. Do not run the 300-second direct
discriminator, start the TUN, or run M0 from this pair. Do not tune mini_vpn,
lower workload rates, increase observer blocks, or relax receiver continuity.

The next formal attempt must use a different physical-network window or a Mac
whose direct Target path can first pass the unchanged direction-aware baseline.
There is no minimum Mbps requirement. A passing baseline must merely complete
both directions with positive receiver evidence in every required interval.

A degraded-path observational lane, if later desired, must remain explicitly
separate from formal socket-rebind acceptance and cannot promote an M0 result.
