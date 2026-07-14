# Knife14h10d16 ACK-Barrier Recovery Architecture Spec

Date: 2026-07-13
Status: **GO for local TDD and one fresh frozen reverse P8 after local review**
Baseline: `55792b33be3839a8505f9548696bcf748ef15e70`

## Stage Goal

Repair the D16 liveness failure proved by the reverse P8 without weakening its
drop/backlog safety contract. A flow that had nonzero smoltcp `send_queue` when
a TUN backlog edge forced DrainOnly must retain that historical obligation
until a clean snapshot proves its queue has reached zero. That proof must be
consumed exactly once as positive recovery evidence.

This is intended to be sufficient for a new reverse P8. It is not a throughput
parameter change and does not claim to close later UDP, DNS, or stop/rearm
gates.

## Frozen Inputs And Non-Goals

Preserve the complete profile listed in the reverse P8 spec: H10d16,
EndpointWindowV1 `30,720,000B/s` / `61,440B` / `10,240B` / `20,480B`, TUN MTU
`1200`, queue/pump `500`, `48/240` ingress service, physical smoltcp RX/TX
`1 MiB`, local receive credit `368,640B`, pool `2`, QUIC windows, `64 KiB`
chunk, Cubic, GSO enabled, Quinn sender, driver bound `20`, and disabled
D3/self-wake.

Do not alter D16's `512 KiB` Running reservoir, `128 KiB` actor/Recovery
quantum, global byte budget, four clean Recovery cycles, actor exclusivity, or
permit release. Do not reopen cap64, bounded sender, GSO-only, generic buffered
downlink, or parameter tuning. Do not run macOS TUN.

## Failure Model

The rejected interleaving is:

```text
Running, send_queue > 0
-> device backlog: arm ACK barrier, force DrainOnly
-> ACKs drain send_queue to 0 while device hard pressure still dominates
-> old code clears barrier, transition remains DrainOnly
-> device pressure clears
-> send_queue already 0, cycle-local drain is 0
-> no remaining recovery evidence; DrainOnly forever
```

The boolean barrier already proves the left side of the transition: it was set
only when `send_queue > 0`. Observing `send_queue == 0` later therefore proves
strictly positive ACK/egress completion even when a cycle-local before/after
sample missed the decrease. The defect is evidence lifetime, not missing data
capacity.

## State Contract

For each D16 flow:

1. backlog forcing arms `d16_tun_rx_ack_barrier` iff a nonzero send queue has
   been observed; repeated edges preserve it;
2. hard pressure, active drop debt, or terminal no-send dominates and keeps the
   flow DrainOnly;
3. while any dominating condition exists, a zero queue snapshot does not
   consume the barrier;
4. at the first snapshot with no dominating condition and `send_queue == 0`,
   the flow atomically:
   - records ACK-completion progress,
   - clears the barrier,
   - evaluates `DrainOnly -> Recovery` with positive drain evidence;
5. the existing low-watermark, Recovery quantum, and four-clean-cycle rules
   remain unchanged;
6. a flow without a barrier receives no synthetic progress.

Terminal sockets remain DrainOnly/reap candidates; zero must not resurrect a
terminal no-send flow.

## Capacity And Reachability

The repair changes only a state transition and adds no buffer, timer, loop, or
packet work. Target capacity remains aggregate `>170 Mbit/s = 21.25 MB/s`.
Once Recovery reopens, each of eight flows can request at most `128 KiB` until
it earns Running again; Running retains the per-flow `512 KiB` owned
opportunity and the global `64 MiB` budget. The P8 failure already proved that
all relays, Quinn readers, byte queues, actor admission, smoltcp, TUN, and pool
connections are reachable. It stopped specifically at the phase permission
gate.

The repaired hot path is unchanged except for the clean transition:

```text
TUN backlog edge -> per-flow ACK barrier -> DrainOnly local drain
-> clean zero queue snapshot -> consume completion evidence -> Recovery
-> publish nonzero D16 read credit -> existing Quinn reader wakes/reads
-> owned queue -> actor -> smoltcp -> TUN
```

## TDD Contract

The focused RED must reproduce the exact lost-evidence interleaving:

- arm a barrier from a nonzero queue;
- observe zero under hard pressure and require the barrier to remain;
- remove hard pressure without adding cycle-local drain and require Recovery;
- require the barrier to clear exactly then;
- repeat across eight independent flows and require every eligible flow to
  recover without a device-global all-zero transaction;
- prove active hard pressure, drop debt, and terminal no-send still dominate;
- prove no-barrier zero snapshots do not invent progress.

Then run focused phase/guard/reader tests, the D16 harness, root regression,
concurrency/UDP gates, fmt/clippy/diff checks, and code review before VPS.

## Acceptance And Stop Rules

Local acceptance requires all new tests GREEN, all existing D16 backlog/drop
and EOF/permit tests unchanged, no actor bypass, and no unresolved P0/P1.

The fresh reverse P8 uses the exact prior profile and requires strictly
`>170 Mbit/s`, `60/60` nonzero intervals, zero TUN drops, pump high below
`500`, no full waits/errors, no flow-control stall, exact endpoint/D16
conservation, bounded close tail, and cleanup. It must also show all eight data
flows progressing beyond one `128 KiB` quantum and no eligible flow stranded
in DrainOnly after a clean guard edge.

An expected RED may enter this minimal implementation. An unrelated repair or
regression failure is analyzed before another edit. A second frozen P8 failure
is an architecture failure and must not be answered by tuning constants.
