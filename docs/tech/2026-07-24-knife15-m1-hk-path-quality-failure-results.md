# Knife15 M1 HK cross-region path-quality failure results

Date: 2026-07-24

Status: **M1 NOT ACCEPTED — failure attributed to the external HK-to-Exit
path; no local product or parameter repair selected**

## Provenance And Operation

The user-provided archive is:

```text
/tmp/mini_vpn_knife15_macos_20260724_034941.tar.gz
SHA-256 182cc89619c4da4e39622ec2d0b405b404ffe14b45ed110e205d694f5f8a9985
```

The archive path list is safe, its checksum matches, and its manifest records
the exact pushed source `297dee9d5c84cb74addaa2e1d62d3550415e0edb`.
The release binary and runner hashes are present, and the secret scan passes.

The fresh physical baseline was `28.692274 Mbit/s` forward and
`48.218002 Mbit/s` reverse. The 300-second direct Target continuity
discriminator passed at `14.339179 Mbit/s` with zero sender and Target
receiver gaps.

The repaired smoke boundary also worked:

- smoke completed successfully;
- `smoke TCP pool idle` was recorded before M1;
- the first formal workload began with no pool lease owner;
- both later idle checkpoints ended at Endpoint state `61,403/0/0B`;
- after `idle-2`, cycle 15 control/data selected conn0/conn1 from an idle
  pool rather than co-locating.

This rejects source mismatch, user operation, stale smoke ownership, and a
regression in the `297dee9` idle barrier.

## Exact Failure

M1 ran from `03:50:29Z` to `07:10:52Z`, about 3 hours 20 minutes. It completed
`steady-a`, `idle-1`, `quiet`, `idle-2`, and the first resume. The first
`steady-b` phase then failed:

```text
cycle=15 phase=tcp-forward reason=receiver_zero_interval
```

The client offered and sent the full 300-second workload:

```text
sender bytes       538,050,560
sender rate         14.347869 Mbit/s
Target bytes       536,215,552
Target rate         14.291175 Mbit/s
```

The Target had three complete zero-byte receiver seconds at:

```text
24.001037s -> 25.001026s
39.001039s -> 40.001032s
41.001036s -> 42.001030s
```

The local sender then had fifteen complete zero-byte seconds between roughly
`26s` and `48s`. The data relay eventually delivered every admitted byte and
closed cleanly, but its maximum upstream writer wait reached
`9,256,932us`. This is a real interruption, not the accepted sub-`0.5s`
command-tail class.

## Path Correlation

The data stream was correctly isolated on conn1. In the first approximately
30-second sample after traffic started, conn1 changed from:

```text
lost packets       7,272 -> 7,537       (+265)
lost bytes     5,035,077 -> 5,384,140   (+349,063B)
congestion         3,956 -> 4,027       (+71)
PLPMTUD black holes   16 -> 20           (+4)
cwnd             151,599 -> 96,134B
```

Across the complete phase it added `783` lost packets, `898,964` lost bytes,
`366` congestion events, and `18` PLPMTUD black-hole detections. The control
connection did not add QUIC loss.

The physical Exit control independently recorded one of three ICMP replies
missing at `07:06:22Z`, inside the receiver/sender interruption window. Its
two replies had about `168ms` RTT. The physical gateway remained `3/3` with
zero loss, and the physical interface had zero errors. Exact-stream ACKs kept
advancing, so the accepted ACK-qualified recovery correctly did not repeat
the destructive false-rebind loop from `e479013`.

The aligned direct Exit loss, bulk-connection QUIC loss/congestion, cwnd
contraction, and clean local ownership select the external cross-region path
quality boundary. The 30-second ICMP probe is coarse, but it is independent
path evidence at the exact failure window; there is no contradictory local
counter.

## Independent UDP SLO Results

Two completed reverse-UDP windows independently exceeded the frozen `3%`
loss SLO:

```text
cycle 8    15,709 / 468,058 packets    3.356208%
cycle 12    7,977 / 234,037 packets    3.408435%
```

Cycle 8 coincided with the direct Exit RTT rising to
`174.885ms` average / `183.547ms` maximum. Cycle 12 coincided with another
one-of-three direct Exit ICMP loss sample. The gateway remained lossless,
while mini_vpn reported zero internal UDP downlink drops/backpressure and zero
TUN interface errors.

These windows remain formal M1 failures. They are not waived, but their
evidence does not authorize changes to TCP recovery, Endpoint pacing, UDP
payload size, or workload rates.

## Healthy Invariants

- Endpoint conservation never exceeded `61,440B` and ended at
  `61,403/0/0B`.
- Both recorded idle checkpoints had zero live/outstanding ownership.
- The failing D16 relay closed with `queue_queued=0`, `queue_leased=0`, and
  `queue_reserved=0`.
- TUN interface errors, pump-full waits, pump read errors, flush failures, and
  Endpoint rebind failures were zero.
- RSS, FD, and thread counts remained bounded.
- `stop` cleaned the process, TUN, and owned routes.

## Decision And Stop Position

This bundle does not pass M1, so M2 and M3 remain blocked. It also does not
select a local code repair: changing a threshold, retry timer, pool setting,
MTU, pacing constant, or strict SLI would be parameter tuning against external
path evidence.

Review found no unresolved P0/P1 in this evidence classification. No Rust,
runner, workload, SLO, or frozen data-plane value changed; documentation,
diff, and changed-content secret checks pass.

Do not immediately repeat in the same network window. One fresh user-operated
HK M1 in a later qualified window remains the next acceptance action under the
existing spec. Keep every other VPN/TUN disabled through `stop`, rebuild the
exact reviewed release, and take fresh baseline/direct evidence.

If a later run repeats a TCP receiver gap while the direct Exit/gateway
controls remain healthy, stop the single-connection retry branch and open a
connection-isolation/failover architecture stage. If reverse UDP again exceeds
`3%` with healthy same-window controls, open a UDP/TUIC quality architecture
stage. Neither branch permits tuning the frozen Knife14/Knife15 values.
