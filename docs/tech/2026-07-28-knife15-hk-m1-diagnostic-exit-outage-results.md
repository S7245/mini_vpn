# Knife15 HK M1 Diagnostic Exit-Outage Results

Date: 2026-07-28 UTC

Status: **7h34m diagnostic wall time preserved — Exit became unreachable near
the end of steady-c; no product/runner change selected**

## Outcome

The user-supplied archive is:

```text
/tmp/mini_vpn_knife15_macos_20260727_085340.tar.gz
SHA-256 0fdfbfd4ba095beb675eaa47e44b99106785bb014f0cacdf481bd055510fee3d
```

It is the first long real-Mac run after the `44ff086` half-close progress
repair. Fresh baseline/direct, `start`, smoke, four active windows, all three
idle/resume boundaries, and most of the final `steady-c` window completed.
The diagnostic then stopped in cycle 32 reverse TCP because iperf produced an
incomplete result with:

```text
error: control socket has closed unexpectedly
```

The failure aligns with the TUIC Exit becoming directly unreachable from the
physical interface. The Exit control changed from three successful probes at
`16:28:33Z` to three lost probes at `16:29:04Z`, while the local gateway
remained lossless. The Exit then stayed at 100% loss for 861 consecutive
samples through `00:04:07Z`. TUIC socket rebind could not recover and repeated
reconnect attempts timed out. This is an Exit VPS or Exit upstream-path
outage, not an operator, local-network, pacing, D16, or SLI-threshold failure.

The completed evidence before that outage is useful and clean: 305 valid
TCP/UDP phase results, 27 successful DNS results, three zero-ownership idle
checkpoints, no diagnostic data-quality ledger entry, UDP loss below 3%, TCP
gap below 16 MiB, and exact Endpoint conservation. The run is still not a
complete diagnostic and cannot accept formal M1 or unblock M2/M3.

## Provenance And Archive Safety

The supplied checksum matches. Gzip integrity passes, and all 370 archive
entries are regular files/directories with no absolute path, parent traversal,
link, or special-device entry.

The manifest identities exactly match the local reviewed and pushed source:

```text
source  0b43141de75cef48f4b673948cc58f75daa1ebb5
binary  2f358f07569baa0de641c87ab6029a8a9c23099ecde1acd5ef4029949262a5fa
runner  480338657cccbe6590626c574e0ac5923e175996f0f68ff9bcfab662777af6a2
```

The local branch, upstream branch, release binary, and runner hashes all
matched the archive during review. The archive secret scan passed.

The legacy top-level manifest label remains `Knife15-macOS-M0`, but the
run-owned mode, status, workload profile, result directory, events, and
violation ledger unambiguously identify a real `m1-diagnostic` execution.

## Fresh Preconditions And Operator Sequence

The paired evidence directories still exist and their hashes match
`m1-workload.txt`:

```text
baseline /tmp/mini_vpn_knife15_macos_baseline_20260727_083326
direct   /tmp/mini_vpn_knife15_macos_direct_20260727_084801
```

Direction-aware preconditions passed:

```text
baseline forward receiver   37.259988 Mbit/s
baseline reverse receiver   51.313104 Mbit/s
direct forward receiver     18.619146 Mbit/s over 300s
baseline/direct receiver-zero intervals  0
```

Direct completed at `08:53:03Z`, `start` began 37 seconds later, and
`m1-diagnostic` began 84 seconds after direct completion. This is well inside
the frozen 900-second freshness bound. `DNS_TARGET=8.8.8.8` was captured before
start. The complete initial sequence was:

```text
08:53:40Z start requested
08:53:41Z ready utun=utun4
08:53:43Z smoke start
08:54:26Z smoke TCP pool idle
08:54:27Z smoke complete
08:54:27Z m1-diagnostic start
```

Target and Exit both used physical `en0`. No other VPN/TUN route appeared
during the active diagnostic or at its failure. `utun1024` appeared only at
`01:27:39Z`, about nine hours after the diagnostic had already failed and one
minute before stop. It contaminates only the late post-failure monitoring
tail and cannot have caused the failure.

The test was operated correctly.

## Completed Longitudinal Evidence

Before the failing phase, the run completed:

- `steady-a`, `quiet`, `steady-b`, and `churn`;
- all three 300-second idle windows and all three resume boundaries;
- idle checkpoints `idle-1`, `idle-2`, and `idle-3`;
- steady-c cycles 28 through 31 and cycle 32 forward TCP;
- 305 valid completed phase results: 278 TCP and 27 reverse UDP;
- 27 valid fake-IP DNS results.

The wall-clock interval from diagnostic start to failure was 7h34m43s. The
remaining frozen schedule was about 39 minutes: the tail of the current
reverse command, approximately 34 more minutes of steady-c budget, and the
five-minute final drain.

The diagnostic violation ledger contains only its header. Replay of every
completed result gives:

```text
complete receiver-zero violations     0
maximum reverse-UDP loss               2.127049%  (< 3%)
maximum TCP sender/receiver gap       12,451,840B (< 16 MiB)
invalid completed result files         0
successful DNS observations           27
```

The three idle checkpoints were:

```text
idle-1  RSS 34,048 KiB  fd 15  threads 11  Endpoint 61,403/0/0B
idle-2  RSS 25,616 KiB  fd 15  threads 11  Endpoint 61,403/0/0B
idle-3  RSS 49,328 KiB  fd 15  threads 11  Endpoint 61,403/0/0B
```

Endpoint conservation never exceeded `61,440B`; the stopped snapshot ended
`59,040/0/0B`. TUN/interface errors, pump errors/full waits, pacing abandoned
bytes, and log compactions were zero. The repaired smoke pool-idle barrier
reached exact `active_leases=0`, and no half-closed-idle block occurred.

All 99 upstream-write error messages were the already reviewed
`ConnectionReset: Stopped(0)` command-boundary class. The available matching
close records had zero D16 queue/permit/close ownership, and no
`stalled_write_timeout` occurred. They remain REVIEW evidence, not the cause
of the final failure.

This real run does not reproduce the exact static-owned timeout branch fixed
by `44ff086`; deterministic paused-time TDD remains the proof of that branch.
It does provide more than seven hours of real-Mac non-regression evidence for
the repaired source.

## Failure Boundary

Cycle 32 reverse TCP started at `16:24:26Z`. It transferred normally through
interval 267. Intervals 268 through 281 then contained 14 complete zero-byte
rows, followed by only 395 bytes, and iperf terminated after roughly 284
seconds with an empty `end` object and:

```text
control socket has closed unexpectedly
```

The simultaneous physical controls were:

```text
16:28:33Z Exit 3/3, 0% loss, 161.838ms avg; gateway 3/3, 0% loss
16:29:04Z Exit 0/3, 100% loss;             gateway 3/3, 0% loss
16:29:10Z Exit 0/3, 100% loss;             gateway 3/3, 0% loss
```

The first Endpoint `no_rx` rebind changed the local UDP source port after the
unchanged RTT-derived 2-second bound, but no current-generation packet could
arrive. TUIC then reported repeated five-second reconnect timeouts. The Exit
control stayed at 100% loss for 861 consecutive samples until `00:04:07Z`,
recovering in the `00:04:38Z` sample. The physical route remained `en0` and
the gateway remained lossless throughout.

That correlation rejects:

- a user interrupt or delayed password entry;
- Clash or another TUN taking the route during the workload;
- local Wi-Fi/default-gateway loss;
- an iperf observer-only partial-tail error;
- a pacing, D16, MTU, pool, QUIC-window, or threshold tuning branch;
- a recoverable five-tuple/source-port black hole.

Client evidence cannot distinguish an Exit host power event, service-host
network failure, provider routing failure, or upstream filtering. It proves
the Exit host and TUIC endpoint were unreachable from the client while the
local network remained available.

## Decision And Next Action

Do not change Rust, the runner, workload shape, an SLI, or any frozen value.
The diagnostic correctly failed closed on an incomplete command result; a
data-quality continuation must not turn missing command/evidence integrity
into a valid observation.

Do not spend another full run repeating `m1-diagnostic`. Its longitudinal
purpose produced 7h34m of wall-time evidence with no recorded data-quality
violation before an independently proven infrastructure outage. The next
necessary gate is one fresh formal `m1`, after confirming the Exit VPS is
intended to remain continuously powered and its sing-box TUIC service is
stable.

At review time, both Exit and Target had recovered to 3/3 ICMP replies with
about 164 ms RTT. That proves current host reachability, not eight-hour
service stability. Before formal M1, take the normal fresh physical baseline
and 300-second direct discriminator, then run:

```text
start -> smoke -> m1 -> status -> stop
```

Keep every other VPN/TUN disabled through stop. Preserve
`status/snapshot/stop` evidence on any failure. Formal M1 remains the only gate
that can unblock M2/M3.
