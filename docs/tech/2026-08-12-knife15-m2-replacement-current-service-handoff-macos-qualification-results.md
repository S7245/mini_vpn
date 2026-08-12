# Knife15 M2 Replacement Current-Service Handoff macOS Qualification Results

Date: 2026-08-12

Status: **PASS_NON_ACCEPTANCE; FORMAL M2 REOPENED ON REVIEWED DESCENDANT OF
`579fff4`; M3 REMAINS BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260812_092030.tar.gz`

Mac SHA-256:
`c6d9c33947273c3fdc13d70dda2bc48519819fb28a04e5696d80b6aba8f44399`

Exact source: `579fff4272a7a3b3d313579d4e3de18cc939174f`

Paired Exit observer artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260812_092139.tar.gz`

Exit SHA-256:
`95afe2403c38312220becdc8d36148cf2e28d527847d825be3dea611545be8a4`

## Verdict

The reviewed current-service-handoff build passed the entire two-cycle
qualification and cleanup. The immutable artifact records:

```text
m2_qualification_status=PASS_NON_ACCEPTANCE
qualification_slo_evidence=PASS
formal_m2_acceptance=NOT_RUN
```

This is the intended qualification verdict. It reopens one formal M2 run but
does not itself satisfy formal acceptance.

## Traffic And Lifecycle Evidence

- direct baseline: `38.274/61.255 Mbit/s` forward/reverse receiver rate;
- bounded 300-second direct discriminator: `19.125 Mbit/s` receiver rate;
- two exact qualification cycles, eight completed phases, two DNS checks, and
  two real-client checks;
- six TCP results and two UDP results, with zero invalid result files;
- Target receiver-zero intervals: `0`;
- sender-zero intervals: `2`;
- maximum sender/receiver TCP gap: `7,733,248B`, below the frozen `16MiB`
  bound;
- maximum UDP loss: `1.954378%`, below the frozen `3%` bound;
- process, watchdog, physical interface, route, DNS, full tunnel, and cleanup:
  PASS.

The four `Stopped(0)` remote-write records are timed-transfer close tails.
Each terminal D16 record had queued/leased/reserved ownership `0/0/0B`; there
was no stalled-write timeout or idle timeout. Endpoint conservation stayed
within `61,440B`, terminal live/outstanding ownership was `0/0B`, socket
would-block was zero, and the final process state was dead after controlled
cleanup.

The run observed eleven ordered-gap evidence events and 544 matched writer
start/end pairs. Recovery-evidence safety passed. `gap_ack_reinforcements`
reached `24` and remained bounded; no endpoint rebind was attempted.

## Replacement-Branch Reachability

No generation replacement or path reset occurred in this thirty-minute run.
There is therefore no `observed_current_cwnd`, successor proof, or replacement
install record. The qualification proves that exact source `579fff4` is
regression-clean across the frozen mixed TCP/UDP/TUN/D16/Endpoint workload; it
does not independently prove the rare replacement current-service handoff.

This absence does not authorize repetition or tuning. The accepted stage
contract says a clean qualification reopens formal M2, whose long mixed
schedule is the decisive branch/effectiveness gate. If formal M2 reaches a
replacement, the diagnostic must show the exact current cwnd, inherited floor,
multi-round proof, and install outcome. A Target receiver-zero after proof
reaches the handoff floor rejects the mechanism.

## Paired Exit Observer

The fresh v2 observer covered `09:21:39Z..09:55:00Z`, including the full
qualification. It recorded:

```text
packets captured:          12,235,760
packets received by filter:12,235,760
kernel drops:              0
Target ingress:            3,512,253 packets / 3,832,749,576B
Target egress:             1,890,868 packets / 1,543,011,385B
TUIC ingress:              2,516,015 packets / 1,614,657,080B
TUIC egress:               3,045,009 packets / 3,996,276,511B
```

After qualification, the observer was explicitly frozen and bundled. Its
owned nftables table is absent, all observer state is removed, and sing-box
remains active. The complete local copy matches the authoritative Exit hash,
its gzip tar is structurally valid, and all 28 internal `SHA256SUMS` entries
pass.

## Formal Source Gate

Qualification established `579fff4` as the reviewed production baseline for
the next formal run. The runner source gate now rejects `85d8772` and requires
`579fff4` or a descendant. Focused RED failed because the old gate still
accepted `85d8772`; after the minimal ancestor update, the full runner
self-test passed. No data-plane code, workload, SLI, or frozen value changed.

Shell syntax, the macOS runner self-test, the Exit observer self-test,
`git diff --check`, documentation/source-floor consistency, and secret scans
pass. Code review found no unresolved P0/P1 in source ancestry, formal versus
qualification behavior, observer lifecycle, evidence integrity, or operator
runbook scope.

## Next Action

Do not repeat qualification. Pull and rebuild the pushed reviewed descendant,
then take exactly one fresh formal transaction while the HK Mac, `.33` Exit,
and `.77` Target can remain uninterrupted for about 25 hours:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> fresh .33 observer start -> m2 -> status -> stop
```

The formal runner automatically freezes and bundles the observer on every
terminal path. Sync both final bundles. M3 remains blocked until formal M2 and
cleanup pass.
