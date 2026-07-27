# Knife15 HK Post-Repair Smoke-Only Results

Date: 2026-07-27

Status: **Post-repair smoke PASS — M1 diagnostic was not run; one fresh
complete sequence remains pending**

## Outcome

The user-supplied archive is:

```text
/tmp/mini_vpn_knife15_macos_20260727_074752.tar.gz
SHA-256 27f105d846686cd8ed7a863ab8fd1a19ab08990d196d611c9b1fe05e1df355a4
```

It contains an exact-source `start -> smoke -> stop` run lasting about
49 seconds. Both TCP smoke directions completed, the repaired pool-idle
barrier reached exact zero, Endpoint/D16 invariants and network controls
passed, and cleanup completed. This is useful post-repair non-regression
evidence for commit `44ff086`.

It is not an M1 diagnostic result. No M1 action, status, controller, result
directory, checkpoint, violation ledger, or workload event exists. The run
also had two independent M1 prerequisite failures: `DNS_TARGET` was disabled
when `start` captured immutable run state, and the preceding direct
discriminator was already older than the frozen 900-second bound at `start`.
No code or frozen-value change is selected.

## Provenance And Archive Safety

The supplied checksum matches. Gzip integrity passes, and all 20 archive
entries are regular files/directories with no absolute path, parent traversal,
link, or special-device entry.

The manifest identities exactly match the local reviewed and pushed
artifacts:

```text
source  5c3cd95c106099991120aa9787f9fabf3d34b990
binary  2f358f07569baa0de641c87ab6029a8a9c23099ecde1acd5ef4029949262a5fa
runner  480338657cccbe6590626c574e0ac5923e175996f0f68ff9bcfab662777af6a2
```

Target and Exit were both on physical `en0` before mutation. The run created
fresh `utun4`, routed only Target through it, kept the TUIC Exit on `en0`, and
restored the route and TUN on stop. The secret scan passed.

## Actual Timeline

```text
07:47:52Z start requested
07:47:53Z ready utun=utun4
07:47:55Z smoke start
07:48:39Z smoke TCP pool idle
07:48:40Z smoke complete
07:48:40Z stop requested
07:48:41Z stop cleanup complete
```

The summary is unambiguous:

```text
m1_status: not_run
m1_mode: not_run
formal_m1_acceptance: NOT_RUN
m1_phase_results_completed: 0
m1_diagnostic_safety_evidence: NOT_APPLICABLE
```

The absence of M1 files is consistent with either stopping after smoke or an
`m1-diagnostic` invocation being rejected before evidence creation. The
archive cannot prove which shell command the user entered, but it proves no
M1 workload started.

## Baseline And Direct Evidence

The paired local baseline and direct directories still exist and match the
direct manifest:

```text
baseline /tmp/mini_vpn_knife15_macos_baseline_20260727_070826
direct   /tmp/mini_vpn_knife15_macos_direct_20260727_071135
```

Their direction-aware evidence is healthy:

```text
baseline forward receiver   30.884767 Mbit/s  zero 0/21
baseline reverse receiver   52.529850 Mbit/s  zero 0/20
direct forward receiver     15.412469 Mbit/s  zero 0/301
direct receiver bytes       578,289,664
```

The direct discriminator completed at `07:16:37Z`. At the `07:47:52Z`
`start`, it was 1,875 seconds old, exceeding the frozen 900-second freshness
limit. `run_m1_action` would reject it even if every other prerequisite
passed.

The manifest also records `dns_target=disabled`. M1 reads DNS configuration
from state written by `start`, not from a later shell export. It checks
`DNS_TARGET` before creating M1 directories/events, so a rejected invocation
can leave exactly this no-M1 archive. Exporting DNS after `start` cannot repair
the current run.

## Smoke And Repair Evidence

```text
direction  sent bytes   received bytes   sent rate       received rate
forward     74,842,112       67,502,080   29.934772 Mb/s  26.762505 Mb/s
reverse    127,926,272      120,717,312   50.708667 Mb/s  48.274755 Mb/s
```

The forward sender has four top-level cold-start zero intervals, the already
classified smoke REVIEW pattern. Its aggregate gap is `7,340,032B`, below the
M1 `16MiB` bound. Reverse has zero receiver gaps and a `7,208,960B` aggregate
gap.

The previous failure boundary did not recur:

- no `tcp-d16-half-closed-idle-blocked` event;
- pool ownership transitioned to `active_leases=0`;
- every relay ended with zero queued/leased/reserved D16 ownership;
- Endpoint conservation peaked at `61,365B` and ended `61,276/0/0B`;
- abandoned Endpoint bytes, pacing leaks, pump-full waits, pump errors, TUN
  flush failures, interface errors, and rebind failures were zero.

The forward command-boundary `Stopped(0)` had zero D16 ownership. Reverse
application close used bounded dead-slot reap and reached exact pool zero. It
is the established smoke close-tail REVIEW class, not a new P0/P1.

This run does not reproduce the exact static-ownership timer branch repaired
by `44ff086`; deterministic paused-time TDD remains the proof for that branch.
It does prove the repaired binary passes a fresh real-Mac smoke without
pool-idle regression.

All five Exit and gateway controls were lossless. Exit RTT stayed around
`162–163ms`; physical interface errors were zero. RSS rose from `10,064` to a
short-run maximum of `41,968 KiB`; file descriptors and threads stayed
constant at `15` and `11`. Process, route, and TUN cleanup passed.

## Decision And Next Action

Accept this bundle only as post-repair smoke non-regression evidence. Do not
claim M1 diagnostic completion, M1 acceptance, or an M2/M3 unblock. Do not
modify Rust, the runner, an SLI, or a frozen value.

Take a new complete HK sequence in one uninterrupted freshness window:

```text
export DNS_TARGET=8.8.8.8 before start
fresh baseline
fresh direct-discriminator
export M1_BASELINE_DIR and M1_DIRECT_DIR
start -> smoke -> m1-diagnostic -> status -> stop
```

Check that Target and Exit use `en0` before baseline. Run `start`, `smoke`, and
`m1-diagnostic` immediately after direct PASS so the direct evidence remains
under 900 seconds. Do not stop after smoke. `m1-diagnostic` normally runs
slightly more than eight hours; only after it returns should `status` and
`stop` run. On a safety failure, preserve `status/snapshot/stop`.

The diagnostic remains longitudinal evidence only. Formal M1 still requires
a separate clean `m1` run.
