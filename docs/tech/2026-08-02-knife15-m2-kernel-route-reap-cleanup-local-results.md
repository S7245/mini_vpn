# Knife15 M2 Kernel Route-Reap Cleanup Local Results

Date: 2026-08-02

Status: **LOCAL PASS — existing real-Mac evidence recovery pending**

## Selected Failure

The first formal M2 run from repaired source `6f1df4a` created:

```text
run_dir=/tmp/mini_vpn_knife15_macos_20260801_053127
baseline=/tmp/mini_vpn_knife15_macos_baseline_20260801_052223
direct=/tmp/mini_vpn_knife15_macos_direct_20260801_052524
```

The direct discriminator passed at `27.216 Mbit/s` with 300 complete positive
receiver intervals. M2 passed start, smoke, the exact IPv6 preflight, full
tunnel activation, real-client preflight, and one complete mixed cycle. Cycle
2 completed its forward TCP, reverse TCP, and reverse UDP phases, then failed
`short-forward-1` with `receiver_zero_interval`. That workload failure remains
an independent bundle-review item; this stage does not classify or waive it.

The later `status -> snapshot -> stop` sequence could not finalize evidence:

```text
ERROR: owned route or DNS cleanup failed; inspect status/snapshot before retrying stop
ERROR: refuse to finalize evidence while an owned target/Exit/DNS route still points to the run utun
```

The synchronized diagnostic is:

```text
path=/tmp/mini_vpn_knife15_cleanup_diag_20260801_053127.txt
sha256=146eb0e1e6710887da5d49ec7305747fbdceba068986e19309ba879dc0a0d643
lines=151
bytes=21520
```

## Decisive State

The diagnostic proved:

```text
runner state=stopped
owned utun=utun4
ifconfig utun4=does not exist
m2.full_tunnel=active
m2.low_route_owned=1
m2.high_route_owned=1
m2.fake_route_owned=1
m2.exit_route_owned=0
m2.dns_owned=0

Target/DNS/Exit/public-low/public-high/fake probes:
interface=en0
gateway=192.168.133.1

current Ethernet DNS=EMPTY
saved pre-M2 DNS=EMPTY
```

The first cleanup pass had already restored DNS and deleted the exact owned
Exit host route. When mini_vpn terminated, macOS removed `utun4` and its three
interface routes. Their ownership markers remained one. The old cleanup code
accepted only a route that still pointed to the owned utun, so it treated the
kernel's exact restoration to the recorded physical interface/gateway as an
external mutation. Repeated stop could not converge.

There is no Clash/foreign-interface, DNS mismatch, wrong gateway, or failed
route-delete evidence in this discriminator. This selects a runner lifecycle
defect, not an operator or data-plane defect.

## Repair Contract

For each M2 interface route with an ownership marker:

```text
current interface == owned utun
  -> delete the exact owned route, then clear ownership

owned utun absent
and current interface == recorded physical interface
and current gateway == recorded physical gateway
  -> perform no route mutation; record kernel-reap evidence; clear ownership

every other observation
  -> retain ownership and fail closed
```

The second branch is observationally the exact pre-M2 route state and cannot
delete foreign state. A still-live utun, another utun/interface, a changed
gateway, missing route evidence, DNS mismatch, or IPv6 mismatch remains
rejected. Final cleanup still requires the utun to be absent, all Target/DNS/
Exit/public routes not to use it, and DNS to equal the protected snapshot.

## Focused TDD

The real pattern first failed the full cleanup fixture:

```text
ERROR: self-test: kernel-reaped M2 interface routes were not released
```

The GREEN fixture activates full-tunnel ownership, removes only the three fake
utun route markers to model macOS interface teardown, keeps ownership state at
one, and then runs the real `deactivate_m2_full_tunnel` seam. It requires:

- all three ownership markers become zero;
- `m2.full_tunnel` becomes inactive;
- every release is recorded in cleanup evidence;
- no `route delete -net` command runs for an already reaped route;
- live-utun physical changes, foreign tunnels, and wrong gateways remain
  `mismatch`.

Internal and external Knife15 shell self-tests, Bash 3.2 syntax, diff/link/
secret hygiene, and code review pass with no unresolved P0/P1. No Rust data
plane, route scope, workload, SLO, frozen value, or IPv6 non-goal changed.

## Recovery And Next Analysis

The existing run remains mutable and recoverable. After pulling the repair,
one repeated `stop` must release only the stale markers, prove final cleanup,
write the summary/secret scan, and create the immutable bundle. `bundle` may
then confirm the same finalized artifact.

Do not rerun baseline or M2. Once the bundle is available, replay cycle 2
`short-forward-1` independently against Target receiver intervals, sender
continuity, physical/Exit controls, Endpoint/D16 ownership, pool placement,
QUIC loss/rebind, and cleanup. M2 remains failed and M3 remains blocked until
that artifact review is complete.
