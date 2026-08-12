# Knife15 M2 Replacement Current-Service Handoff Formal Failure Results

Date: 2026-08-12

Status: **FORMAL M2 FAIL; PREVIOUS SUCCESSOR FORWARD-SERVICE INHERITANCE IS
REJECTED AS SUFFICIENT; M3 REMAINS BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260812_034701.tar.gz`

Mac SHA-256:
`9d33b1afc2886a838f19cd567ec20332770923c642371e1f4f1b3e83fa678828`

Exact source: `85d8772`

Paired Exit observer artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260812_035039.tar.gz`

Exit SHA-256:
`643a019a9e1122f26672b527c19464ea4c6a9384169b87145fd8396f0a65f36c`

## Result

The repaired formal transaction passed baseline `17.776/64.868 Mbit/s`,
direct discrimination, smoke, observer admission, every preflight, fifteen
complete M2 cycles, and cleanup. Cycle 16 `short-forward-5` then lost its
first complete Target receiver interval. Formal M2 remains failed.

This was not the prior observer-state bug. The matching `.33` v2 observer was
healthy and ten seconds old at admission, formal traffic ran for nearly four
hours, the observer froze and bundled on failure, and routes, DNS, full-tunnel
ownership, Endpoint conservation, process state, and stop cleanup all passed.

The failed client result sent `11,927,552B` in `10.010s`; the Target received
`3,932,160B`. One client sender interval and one Target receiver interval were
zero. The failure was the single frozen receiver-zero SLI; UDP loss remained
below the frozen `3%` limit and there were no health or evidence failures.

## Exact Path Attribution

The failed Target data socket was
`172.26.0.2:35650 -> 43.130.32.77:5201`. Paired Exit packet evidence shows:

```text
first payload:                 1786520599.694215
payload through +1.001031s:   113,261B
minimum useful iperf interval: 131,072B
maximum Exit supply gap:       180.701ms
total payload:                 3,982,260B
```

The Exit sent and the Target ACKed continuously. TCP_INFO sampled a normally
app-limited socket, negligible send queue, about `1..5ms` RTT, and immediate
ACK progress. The first application interval was zero because the Exit had
received less than one `128KiB` iperf block, not because the Exit-to-Target
socket stopped.

Mac evidence selects the upstream cause. Immediately before the failed open,
auxiliary generation 2 was replaced after its PLPMTUD black-hole count changed
`2 -> 3`:

```text
predecessor current cwnd:            361,778B
stored successor-service floor:       26,338B
successor initial/final proof cwnd: 12,000/26,338B
proof bytes / elapsed:              12,972B / 356ms
successor RTT:                      about 178ms
```

The replacement installed successfully and immediately owned the control and
business streams. D16 accepted `5,774,517B`; Quinn ACK progress was continuous
but initially capacity-limited. There was no ACK stall, ordered receive gap,
D16 ownership leak, TUN error, Endpoint conservation error, or Target-side
backpressure. The low fresh-generation proof was working as implemented.

## Decision

The previous mechanism only raised the replacement floor when the recovery
monitor explicitly applied `path_changed()`. A generation replaced directly
for degraded qualification could therefore surrender a much larger current
cwnd while retaining only its old installation certificate. In this artifact
the handoff regressed from `361,778B` to `26,338B`.

This satisfies the previous architecture's rejection discriminator: a
receiver-zero recurred after a logged nonzero inherited floor. Do not repeat
or tune `85d8772`. The next stage must make replacement preparation atomically
snapshot and publish the exact current generation's positive cwnd before the
successor proof. Static cwnd/rate thresholds, extra pool lanes, keep-warm
timers, workload changes, and SLI relaxation remain rejected.
