# Knife15 macOS HITL Short Qualification Results

Date: 2026-07-14
Status: **PASS — target-only HITL runner qualified; 2-hour M0 not yet run**

## Provenance And Topology

- Source: `2a85fd4ee770f1441ceb99cf36f77894989ce1ce`
- Host: HK arm64 Mac, macOS `26.5.1`
- Physical interface during the window: `en1`
- TUIC Exit: the persistent `.33:8443` sing-box service
- Target: `.77:5201`
- TUN: newly discovered `utun4`, MTU1200
- Bundle:
  `/tmp/mini_vpn_knife15_macos_20260714_084323.tar.gz`
- Bundle SHA-256:
  `01ff8491b396749948ccd4ae5a47c620da43084a6b72cab871a1ac8f648b7f34`
- Secret scan: PASS

The Exit and target both used `en1` before start. During the run, only target
and resolver host routes entered `utun4`; the Exit remained on `en1`.

## Same-Window TCP Evidence

| Window | Receiver | Sender | Retransmits | Nonzero intervals |
|---|---:|---:|---:|---:|
| Direct forward | 9.045 Mbit/s | 9.225 Mbit/s | 446 | 20/20 |
| Direct reverse | 26.790 Mbit/s | 27.969 Mbit/s | 1,342 | 20/20 |
| TUN forward | 31.444 Mbit/s | 35.748 Mbit/s | 0 | 20/20 |
| TUN reverse | 48.490 Mbit/s | 53.016 Mbit/s | 134 | 20/20 |

The direct TCP path was variable and lossy; the TUN result exceeded the direct
receiver result in both directions. This is useful functionality evidence, not
an absolute H10d16 capacity measurement. The capable Linux/VPS lane remains
the `170/200 Mbit/s` architecture gate.

The forward TUN tail recorded one remote `Stopped(0)` write failure after the
iperf window. The D16 writer reported `queue_queued=0`, `queue_leased=0`,
`queue_reserved=0`, no terminal permit drop/reap, and the same socket slot
successfully rearmed from epoch 1 to epoch 3 for the reverse test. Classify the
event as `REVIEW`/iperf close-tail evidence, not a leak or sustained data-plane
failure. M0 must retain an idle drain window before stop and continue tracking
sender/receiver byte gaps.

## Endpoint, QUIC, TUN, And Resource Evidence

- Endpoint conservation samples were `61,365+0+75=61,440`,
  `60,240+0+0=60,240`, and `61,338+0+0=61,338`; all obeyed the accepted
  `61,440B` bound.
- Latest endpoint totals were `98,228,884B` granted,
  `10,116,947B` refunded, and `88,111,937B` sent, with zero abandoned and zero
  outstanding bytes. The equality `granted - refunded = sent` held exactly.
- TUN ingress pump reached `317/500` with zero full waits, read errors, or
  closure; backlog pause/resume was `11/11`.
- At the final live interface sample, `utun4` carried `307,502` input packets
  / `256,383,192B` and `96,742` output packets / `93,257,426B`, with
  `ierrs=0` and `oerrs=0`.
- D16 actor bypass, permit terminal drop, terminal pending/reap, send errors,
  and TUN flush failures were zero in the available aggregate/close evidence.
- `.33` data connection QUIC at the 30-second snapshot had RTT `180ms`,
  `5,313,648B` lost, `1,042` congestion events, and PLPMTUD black-hole signals.
  Direct TCP also showed high retransmission/variance, so this is path/peer
  evidence to correlate during M0, not permission to tune frozen constants.
- RSS sampled from `10,096 KiB` to a short-run maximum `35,040 KiB`, then
  `30,064 KiB`; file descriptors stayed at `15`, threads at `11`. The window
  is too short to establish a resource slope.
- DNS smoke returned fake IP `198.18.0.2` for `example.com` through the
  explicit `8.8.8.8` host route.

## Cleanup

At stop, the mini_vpn PID was gone, `utun4` was absent, and target, resolver,
and Exit routes had returned to `en1`. A later `utun1024` route belongs to the
user's pre-existing VPN/proxy after the test and is not a Knife15 leak.

## Runner Findings And Repairs

The first real bundle found three report/operability issues without
invalidating the raw evidence:

1. macOS `awk` rejected the unspaced summary ternary, leaving process/event
   counts blank. A shell TDD fixture now reproduces and fixes that parser.
2. The summary did not mark `remote_write_failed` for review. A second fixture
   now requires `REVIEW` and an explicit failure count.
3. `8,909/9,185` log lines, about 89% of log bytes, were per-flush permit
   details already represented by the 30-second aggregate. They are moved
   behind the explicit full-trace gate, and the runner's bounded log envelope
   is raised to `256 MiB`/`128 MiB` so long-run evidence keeps the useful
   lifecycle and aggregate signal.

These repairs change reporting/log density only. H10d16, EndpointWindowV1,
MTU, pool, windows, chunk, Cubic, GSO, queue/FIFO/batch bounds, driver bound,
and self-wake remain frozen.

## Decision And Next Gate

The short target-only HITL qualification passes. It authorizes a user-run
2-hour M0 on HK or Shenzhen after rebuilding the repaired source. It does not
complete M0 and does not authorize M1/M2 until a mixed-workload controller,
idle-drain epoch, complete summary, and 2-hour resource evidence pass.
