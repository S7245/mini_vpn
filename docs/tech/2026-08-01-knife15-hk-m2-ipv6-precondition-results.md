# Knife15 HK M2 IPv6 Precondition Results

Date: 2026-08-01

Status: **RUNNER FALSE NEGATIVE SELECTED — smoke passed; local repair complete**

## Evidence

The decisive fresh bundle is:

```text
bundle=/tmp/mini_vpn_knife15_macos_20260801_041142.tar.gz
sha256=93d384a508fb0c822e1875f120dd38f9f78e1fd293f4db7393dc77f3d5e9ef22
source_commit=753691aacbb1b7dc2461eff6f3c18c55f36ecee0
```

The terminal reported:

```text
Waiting for smoke TCP-pool ownership to drain: hard_timeout=50s
PASS: target-only smoke completed: /tmp/mini_vpn_knife15_macos_20260801_041142/smoke_20260801_041145
ERROR: formal M2 blocks a routable physical IPv6 path; disable IPv6 for this dedicated test service
```

The earlier exact-source bundle
`/tmp/mini_vpn_knife15_macos_20260731_111004.tar.gz` (SHA-256
`2c5ce9f6ce2cde34034cc95d13e93896e5e049f6c40a8047ba51dd79ad14090b`)
also contained no M2 evidence, but its terminal error was not
preserved. It kept only a target-only TUN alive for about 16h25m. The fresh
attempt selected the same reported IPv6 prerequisite branch for the current
physical-network configuration; neither artifact preserved enough raw route
evidence to distinguish a physical route from an observer error.

A second attempt after the documented IPv6 service change produced:

```text
bundle=/tmp/mini_vpn_knife15_macos_20260801_044032.tar.gz
sha256=651f977bd4756f0c4d25a086d583f6f58d5d1ba240e1f87ff59be6ae43ebf417
source_commit=19b5ceb1c04f4b8cb5b268f98df3fb5794998947
```

It again passed smoke and cleanup but kept M2 `not_run`. A local replay of the
exact formal probe selected a runner false negative: macOS printed the known
`not in table` absence while returning status zero, and the old classifier
recognized that text only for a nonzero status. The first runbook revision
also used a different IPv6 probe from formal M2. The focused repair and gates
are recorded in
`docs/tech/2026-08-01-knife15-m2-ipv6-route-status-observability-local-results.md`.

## Timeline And Boundary

```text
04:11:42Z  start requested
04:11:43Z  target-only utun4 ready; Exit remained on en0
04:11:45Z  smoke started
04:12:28Z  TCP-pool ownership reached idle
04:12:29Z  smoke completed
04:13:19Z  snapshot and stop requested
04:13:20Z  cleanup completed
```

This is not a smoke failure. The `m2` action rejected the run after smoke and
before creating M2 evidence, adding IPv4 split-default routes, or changing the
physical service DNS:

```text
m2_status=not_run
m2_full_tunnel_state=not_run
formal_m2_acceptance=NOT_RUN
m2 cycles/results/checkpoints=0
```

## Passed Evidence

- Smoke forward receiver: `63.427802 Mbit/s`, 20 complete intervals.
- Smoke reverse receiver: `49.695125 Mbit/s`, 20 complete intervals.
- Target and DNS Target used `utun4`; the TUIC Exit remained on physical
  `en0` through gateway `192.168.133.1`.
- Exit and gateway controls were complete and lossless in the short run.
- Endpoint conservation passed with maximum `61,414B` and final
  `61,414/0/0B` available/live/outstanding.
- The one smoke `Stopped(0)` close had zero queued, leased, and reserved D16
  ownership. It is a clean application-first boundary, not the M2 rejection.
- Process, TUN, route, secret, and cleanup evidence passed. No M2-owned route
  or DNS mutation existed to restore.

## Classification

The M2 architecture is intentionally IPv4-only. A routable physical IPv6
path would bypass the owned IPv4 split-default routes, so the runner must not
claim a leak-free full tunnel while that path exists. The repeated rejection
after the documented service change was an observer false negative and is not
evidence of an operator, smoke, pacing, D16, QUIC, pool, MTU, or throughput
defect.

The initial operational defect was in the HITL runbook: it warned that M2
blocks physical IPv6 but did not place an explicit inspect/disable/restore
procedure before the expiring baseline/direct transaction. The second attempt
then selected the status/text classifier defect. The repaired runbook and
runner now:

1. derives the active physical interface and its exact macOS network service;
2. records the original IPv6 mode;
3. permits the documented temporary change only from `Automatic` to `Off`;
4. proves the global IPv6 discriminator no longer has a physical route before
   baseline;
5. runs the exact shared `m2-ipv6-check` before baseline;
6. restores immediately if a pre-start gate fails, or only after Knife15
   `stop` once `start` has been invoked.

The runner continues to check the IPv6 invariant before mutation and
throughout M2. No Rust data plane, workload, SLO, route scope, frozen value, or
IPv6 non-goal changed.

## Next Action

On the dedicated HK test Mac, use the updated M2 HITL runbook. With every
other VPN/TUN disabled, identify the service owning the Exit route, verify its
current IPv6 mode is `Automatic`, temporarily set that service to IPv6 `Off`,
and run only the exact public `m2-ipv6-check`. Take one fresh uninterrupted
transaction only after that action reports PASS:

```text
baseline -> direct-discriminator -> start -> smoke -> m2 -> status -> stop
```

Restore that same service to IPv6 `Automatic` immediately on a pre-start
failure; once `start` has been invoked, restore only after `stop`. Do not reuse
the rejected baseline/direct evidence, tune a constant, or bypass the runner's
IPv6 gate. M3 remains blocked pending a real formal M2 bundle.
