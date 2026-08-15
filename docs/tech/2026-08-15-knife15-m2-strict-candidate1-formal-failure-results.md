# Knife15 M2 Strict Candidate 1 Formal Failure Results

Date: 2026-08-15

Status: **CANDIDATE 1 REJECTED BY STRICT LEDGER; DO NOT REPEAT OR TUNE;
CANDIDATE 2 NEXT; TIER A PENDING; M3 BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260814_135029.tar.gz`

Mac SHA-256:
`0791b4e5f6eb1e65f6a19e834022d76f4913a68162d473786090b636ba4f7456`

Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260814_135212.tar.gz`

Exit SHA-256:
`8778fe15474ed35769f30309c17ddc1a758216f6a35b9984a40ae17c42e618f9`

Exact source: `b4244a7c3fb58efe9fbfc832cda402d6ed4e96d7`.

## Verdict

Alibaba candidate 1 passed its strict qualification but failed its first
formal run after `13h04m21s`. The failure is valid under the frozen Tier-A
application-observed SLI: cycle 53 `short-forward-1` contains one complete
Target receiver interval with zero reported bytes. Exact resource binding,
observer coverage, result integrity, safety, and cleanup all pass, so this is
not an invalid environment, operator, power, VPS-outage, or runner-finalization
attempt.

The immutable strict ledger seals this as sequence 2,
`quality_failure/receiver_zero`, and changes candidate 1 from
`awaiting_formal` to `rejected`. Its result remains `TIER_A_PENDING` because
candidate 2 has not run. Candidate 1 must not be repeated, resized, tuned, or
used to seek a favorable sample.

This failure is narrower than the earlier `.33` established-stream outage.
The paired packet evidence shows no complete one-second wire blackout and no
mini_vpn ownership failure. The failed result is the first application
interval of a new short connection. During its first approximately one-second
Target window, the Exit had already placed `128,505B` of raw TCP payload on
the wire: a 37-byte iperf header plus `128,468B` of test data, only `2,604B`
short of iperf3's `131,072B` application block. The Target report
therefore emitted zero for that first complete interval and reported the bytes
in later intervals. The strict contract intentionally treats that
application-observed zero as a failure; this result does not authorize changing
the block, interval, workload, or SLI.

## Completed Envelope

- direct baseline: `23.752/51.250 Mbit/s` forward/reverse;
- bounded direct forward: `11.870 Mbit/s`, zero sender/receiver-zero
  intervals;
- formal start: `2026-08-14T13:53:30Z`;
- formal failure: `2026-08-15T02:57:51Z`;
- 49 complete cycles, three complete idle/resume boundaries, and 503 complete
  phases before the failed phase;
- 453 TCP and 51 UDP result files, 49 DNS checks, and 49 real-client checks;
- maximum completed UDP loss: `2.700519%`, below `3%`;
- maximum TCP sender/receiver gap: `10,878,976B`, below `16MiB`;
- sender-zero intervals: 6; Target receiver-zero intervals: 1;
- Endpoint terminal conservation: `61,403/0/0B`
  available/live/outstanding;
- process, route, DNS, TUN, observer, and Exit cleanup: PASS.

The failed ten-second client sent `13,107,200B` with zero local TCP
retransmits. Target reported `4,325,376B`; only its first complete interval
reported zero. This was the only Target receiver-zero result among 228 formal
forward result files.

## Exact Failure Window

The failing data socket was Exit local port 35218 to
`43.130.32.77:5201`. Packet and socket evidence show:

```text
Mac-to-Exit QUIC ingress maximum packet gap: 144.568ms
Exit-to-Target maximum TCP payload gap:      165.236ms
Exit-to-Target TCP retransmitted:              1,371B
maximum sampled Exit TCP send queue:            5,524B
Target TCP RTT:                                  1..5ms
first-window raw Target TCP payload:           128,505B
first-window iperf test data:                   128,468B
iperf3 application block:                      131,072B
shortfall at the reporting boundary:             2,604B
```

The client opened the control stream on TUIC pool connection 1 and received
its first response in `169ms`. The data stream then remained forward-only, as
expected. Connection 1 stayed at about `163..165ms` QUIC RTT and `195,216B`
cwnd with only `1,509B` cumulative loss and three congestion events; no path
reset, Endpoint rebind, generation replacement, D16 ownership loss, TUN pump
error, or socket would-block event occurred in the failure window.

The Exit observer captured `235,650,063` packets with zero kernel drops. All
four nftables directions are nonzero. Its state and table were removed after
automatic freeze/bundle; sing-box remains active with zero restarts.

## Qualification And Ledger Chain

The earlier exact-source qualification pair remains a valid pass:

- Mac SHA-256:
  `64bcd30bbbf6fd828908c75e4b6fd3b76346bec6ab70261821bdc7d8a7fd31f7`;
- Exit SHA-256:
  `2a737c4a61afb15244bdcb7904052a20f81341430ccf34b655ca4b40ec1c8535`;
- two cycles/eight phases, zero Target receiver-zero intervals, maximum TCP
  gap `6,946,816B`, maximum UDP loss `2.545332%`, and cleanup PASS.

The immutable evidence root is:

```text
/Users/liushan/knife15-evidence/candidate1-alibaba-usw1/
  b4244a7c3fb58efe9fbfc832cda402d6ed4e96d7
```

`attempt-002.json` was produced by the reviewed sealer from both formal
archives. `knife15-tier-a-ledger-002.json` plus `evaluation-002.json` emits:

```text
status=TIER_A_PENDING
candidate1-alibaba-usw1.state=rejected
candidate1-alibaba-usw1.formal_passes=0
```

The existing qualification attempt and evaluation files remain immutable.

## Decision

Freeze source, binary, workload, server settings, and every data-plane value.
Do not repair this outcome through iperf block size, first-interval omission,
rate, pool, Cubic, MTU/PLPMTUD, QUIC windows, D16, Endpoint pacing, retry,
timeout, GSO, or self-wake changes.

Provision exactly one eligible candidate 2 on the separately reviewed AWS
resource contract. It must pass the same identity, direct, resource,
qualification, observer, strict formal, safety, and cleanup gates. A first
genuine strict failure rejects candidate 2 and exhausts Tier A; two consecutive
formal passes accept Tier A. M3 remains blocked.
