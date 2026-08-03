# Knife15 HK M2 Direct Continuity Preflight Failure Results

Date: 2026-08-03

Status: **EXTERNAL PREFLIGHT FAILURE — TUN not started; M2 not run; M3 blocked**

## 1. Evidence And Provenance

The synchronized directories are:

```text
/tmp/mini_vpn_knife15_macos_baseline_20260803_035615
/tmp/mini_vpn_knife15_macos_direct_20260803_035733
```

The direct manifest binds the correct reviewed source and artifacts:

```text
source_commit=6231048204f38da237910eca9eb1cac59ec4c1da
runner_sha256=befb14c9c982f668e722516cd350b1702bbac2a9e7c7dce89f9c731cd4993b5f
binary_sha256=32774b821f31d19442c18d81593fdad3e143178331702f030dcf5bd0ff1bb0bd
baseline_forward_sha256=180f0a538b4c493789339c7408217dbcd8d1691d01684bb3a5d6fcc129e2a5cc
baseline_reverse_sha256=716fc0a259ad693216147be32b1861cd276c85d16cbc65814557750cc2a2856d
result_sha256=c2bca5e947983139b22ef13f45b03c8b476b5853bc408bdb41bfa558ce2e1b8b
```

Both the Target and Exit resolved through physical `en1`. No Knife15 `start`
action or TUN/full-tunnel ownership occurred.

## 2. Baseline

The 20-second physical baseline was complete and receiver-positive:

```text
forward receiver: 12.930 Mbit/s, zero intervals=0
reverse receiver: 49.357 Mbit/s, zero intervals=0
```

Slow bandwidth is not a failure. The runner correctly derived the formal
direct offered rate from the lower forward baseline. The baseline did,
however, already show a noisy path: forward recorded `2,206` retransmits and
reverse recorded `1,406` retransmits.

## 3. Direct Discriminator Failure

The exact 300-second direct discriminator ran at only `6.462 Mbit/s`, roughly
half the passing forward baseline, but still failed:

```text
status=fail
reason=receiver_zero_interval
receiver_zero_intervals=7
sender_zero_intervals=8
receiver_bytes=242,483,200
retransmits=533
```

The Target receiver had two nonterminal complete outages:

```text
62.001043s -> 65.001056s    three consecutive zero-byte seconds
152.001032s -> 156.000466s  four consecutive zero-byte seconds
```

The client sender stopped across the same episodes. Its congestion window
collapsed as low as `1,344B`, with per-second retransmission bursts up to 83.
These are not iperf command-tail rows and cannot be waived as slow throughput.

## 4. Classification

This is a valid fail-closed physical Target-path continuity discriminator,
not evidence against mini_vpn, the new path-service-aware pool selector, D16,
Endpoint pacing, TUN, route/DNS ownership, or M2 workload behavior. The TUN
was never started, so none of those product paths was reachable.

The evidence cannot distinguish local physical-link contention, the WAN path,
or the Target receiver host. It does prove that a later receiver-zero interval
inside M2 could not be attributed to mini_vpn in this network window.
Therefore entering `start` would invalidate the experiment rather than add
useful evidence.

Physical interface name `en1` is not itself a failure: the runbook derives the
current Exit route and does not require a hard-coded `en0`.

## 5. Next Action

Because failure occurred before `start`, restore the exact recorded physical
network service from IPv6 Off to its original Automatic mode immediately.
Preserve both directories; no `status`, `snapshot`, or `stop` is needed
because Knife15 owns no TUN, route, DNS, or process state.

Do not repeat unchanged in the same network window. After a materially later
or repaired physical-network window, take a wholly fresh transaction from
`6231048` or a descendant:

```text
m2-ipv6-check -> baseline -> direct-discriminator
```

Only a direct manifest with `status=pass`, zero complete receiver intervals,
correct source/hash provenance, and fresh evidence may proceed immediately to:

```text
start -> smoke -> m2 -> status -> stop
```

No constant, workload, SLI, pool, MTU, QUIC window, pacing, D16, or source
change is selected. M3 remains blocked pending complete formal M2 acceptance.
