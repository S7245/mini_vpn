# Knife15 M2 Cold Pool Placement Qualification Failure Results

Date: 2026-08-07

Status: **QUALIFICATION FAILED; COLD POOL PLACEMENT SELECTED; FORMAL M2 AND M3 REMAIN BLOCKED**

Client artifact:
`/tmp/mini_vpn_knife15_macos_20260807_054320.tar.gz` (SHA-256
`916db92ba0d5d0c6d8f0c01765da8a98c6804331a27cfc9ef854e022ca4717a2`).

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260807_043324.tar.gz`
(SHA-256
`936e8a162aac13c1fe949fe7973956fc68075567eb23783454026e34680233e6`).

Exact source: `12e845f` (`docs(knife15): close recovery observer local plan`).

## Outcome

The exact bounded qualification failed its first 300-second forward phase
because both the Mac sender and Target receiver reported two complete zero
intervals during the first six seconds. The transfer did not stall later: it
completed exact sender/receiver totals of `488,636,416B / 486,670,336B` at
`13.030 / 12.971 Mbit/s`.

The paired artifacts select cold TUIC TCP pool placement, not a physical
path, Exit kernel, Target, TUN, D16, Endpoint, ordered-gap action, or cleanup
failure. The smoke workload had advanced conn1's path from its authenticated
`12,000B` cwnd to `871,763B`; conn0 remained at `12,000B`. Qualification
control selected warm conn1. Its reservation then made conn0 strictly less
loaded, so the data stream selected cold conn0 because the current admission
rule considers path service only for equal nonzero lease load.

## Provenance And Prerequisites

- Source, binary, runner, baseline hashes, and direct manifest all bind to
  `12e845f`.
- Baseline forward/reverse receivers passed at `26.054 / 60.199 Mbit/s`.
- The 300-second direct discriminator passed at `13.020 Mbit/s`, exact
  `488,505,344B`, with zero sender and receiver intervals.
- Tunnel smoke passed at `28.645 / 49.073 Mbit/s` forward/reverse.
- IPv6, full-tunnel, controlled-drain, DNS/real-client, route, process, TUN,
  and cleanup gates passed.

The artifact is therefore valid architecture evidence, not an operator or
environment failure.

## Exact Placement Evidence

Immediately before the qualification pair:

```text
control candidates:
  conn0 active=2 cwnd=12,000B  rtt=164.215ms
  conn1 active=2 cwnd=871,763B rtt=163.889ms
control selected conn1 by equal-load path-service tie-break

data candidates:
  conn0 active=2 cwnd=12,000B  rtt=164.215ms
  conn1 active=4 cwnd=871,763B rtt=163.853ms
data selected conn0 by lower lease load
```

The selected data writer was conn0/stream3. Its first observed Pending episode
lasted `3,500ms`, reached `3,456ms` maximum Pending, and advanced exact QUIC
acknowledgement by `414,072B` over thirteen progress samples. Its maximum
exact ACK stall was `0ms` in that episode and `9ms` over the whole run. D16
reported a `3,573,839us` maximum writer wait and clean final ownership
`queued/leased/reserved=0/0/0B`.

This is slow but live service on the selected cold connection. It cannot
satisfy the existing ACK-stall Endpoint rebind or writer-plus-black-hole
connection-local reset, and those policies correctly did not fire.

## Paired Exit Discriminator

The exact Exit data socket was `172.26.0.2:54524 -> 43.130.32.77:5201`.
Across the capture it supplied `486,791,898B` of TCP payload in `239,195`
packets. During startup the per-second supplied bytes increased from roughly
`24KB`, `72KB`, `98KB`, `124KB`, `153KB`, and `181KB`; the maximum positive
payload packet gap was only `223.987ms`.

The Target ACKed the supplied data with about `1ms` TCP RTT. tcpdump captured
`515,691/515,691` packets with zero kernel drops. Therefore:

- the Exit application never lost a complete second of Target supply;
- the Target path did not stop ACKing or enter a retransmission black hole;
- the two Target iperf zero intervals reflect the cold QUIC supply ramp and
  128 KiB application read accounting, not an Exit-to-Target outage.

## Other Invariants

- Recovery evidence remained observation-only: seven ordered-gap records,
  `103/103` writer start/end records, and zero Endpoint rebinds.
- Endpoint conservation passed at maximum `61,440B` and final
  `61,414/0/0B` available/live/outstanding.
- No TUN/interface errors, route loss, gateway/Exit control loss, process
  death, or cleanup debt occurred.
- One smoke-era peer `Stopped(0)` write ended with D16 ownership zero. It
  explains `internal_failure_scan=REVIEW` but is not the qualification
  failure; the formal data relay closed cleanly.

## Rejected Branches

- Do not ignore initial intervals or relax the unchanged zero-interval SLI.
- Do not tune timeout, priority duration, cwnd, Cubic, MTU, pool, QUIC
  windows, chunk, GSO, pacing, self-wake, or workload rate.
- Do not reopen ordered-gap recovery, Endpoint rebind, connection reset,
  threshold/weight selector scoring, or payload replay. Exact load divided by
  current service remains available only after categorical forward
  qualification; it cannot re-admit a degraded generation.
- Do not repeat the unchanged qualification.

The next architecture must preserve forward qualification and bounded
generation replacement while preventing a much lower-capacity path from
outranking a qualified higher-capacity path solely because the control open
changed raw lease ownership first.
