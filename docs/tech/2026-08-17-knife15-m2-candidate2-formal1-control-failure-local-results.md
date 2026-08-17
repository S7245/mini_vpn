# Knife15 M2 Candidate-2 Formal-1 Control Failure And Local Repair Results

Date: 2026-08-17

Status: **FORMAL 1 INVALID; LOCAL EVIDENCE REPAIR PASS; FRESH FORMAL 1 NEXT;
TIER A PENDING; M3 BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260815_113344.tar.gz`

Mac SHA-256:
`5d26e42dc8ea9a2b348da14abcc3b8d3b47107ac26570ac0d5d5854090e37cc5`

Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260815_113432.tar.gz`

Exit SHA-256:
`d6e1dad8bcf0506a91e354728b7f68697c7d81fedc71ba51ab34c74c83317fb0`

## Verdict

Candidate-2 Formal 1 is invalid evidence, not a candidate quality failure. It
completed 17 cycles and the active portion of cycle 18, then ran the first
600-second idle window to completion. At the checkpoint boundary the runner
returned a generic workload failure before writing the first checkpoint row.
No TCP/UDP phase, DNS, real-client, mini_vpn health, route, observer, safety,
or cleanup failure occurred.

The paired Exit captured `64,792,664` packets with zero kernel drops. All
completed Target receiver-zero counts were zero and maximum UDP loss was
`0.561407%`. Endpoint/D16, process, interface, routes, DNS, TUN, observer, and
Exit cleanup passed. The strict ledger remains unchanged at sequence 3:
candidate 2 is `awaiting_formal`, formal passes are zero, and the evaluation is
`TIER_A_PENDING`.

## Exact Failure Boundary

- `idle-1` started at `2026-08-15T15:44:11Z` for 600 seconds.
- A fresh process/network/interface sample exists at `15:54:11Z`; mini_vpn,
  `utun4`, the Target route, physical Exit route, and zero-loss probes were
  healthy.
- The failure event was written at `15:54:15Z`.
- `m2-checkpoints.csv` contains only its header.
- There is no `health failed`, phase failure, child timeout, or interrupt
  event.
- Replaying the completed archive produces matching data-plane/replay sample
  counts, zero replay invalidity, zero controlled active relays, and clean
  Endpoint ownership.

This rejects the earlier hypothesis that `/bin/sleep 600`, SSH detachment, or
the candidate VPS failed. The failure lies between the healthy post-idle
sample and checkpoint row emission.

## Root Cause

The runner parsed a live append-only `mini_vpn.log` with inconsistent
complete-record rules. `m2_data_plane_envelope` ignored an incomplete metrics
tail, while `m2_controlled_tcp_replay_envelope` counted any line beginning
with `📊 数据面:`. Replay lifecycle readers also immediately classified a
syntactically incomplete EOF event as invalid, and the active-lease reader
allowed an incomplete EOF record to hide the last numeric value.

At the concurrent append boundary these readers can transiently disagree;
after the writer completes the record, archive replay is clean. The old runner
collapsed that transient into the generic M2 workload error and preserved no
checkpoint row, exactly matching the artifact.

## Repair

Reviewed runner SHA-256:
`8b0d0c9ed220075481f4459dbfbde7d8ef88738d9ca6f2afc802b65d96f25a86`.

The repair:

- counts a data-plane sample only after the same required numeric prefix is
  complete in both readers;
- defers syntactic replay invalidity at EOF until a later record proves the
  malformed line was complete;
- ignores only an incomplete empty active-lease EOF fragment, while a later
  record makes a completed empty/malformed ownership line fail closed; and
- adds deterministic incomplete-tail and completed-malformed fixtures.

No Rust source, release behavior, M2 duration/rate/phase, Endpoint/D16/TUN,
server, observer, resource identity, or frozen constant changed.

The strict ledger maps only the qualified runner SHA-256
`c55dc940d974539f98e2f449387159965972249dcda476fad451c44d3466e192`
and repaired runner SHA-256 above into one compatibility class, and only when
the Mac release SHA-256 remains exactly
`5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032`.
Arbitrary source/runner drift and a different binary remain rejected.

## Local And Environment Gates

- runner incomplete-tail RED/GREEN: PASS;
- runner full self-test: PASS (including intentional 1-second timeout text);
- resource profile/preflight/ledger/observer self-tests: PASS;
- existing strict ledger content/source replay: `TIER_A_PENDING`, PASS;
- shell syntax, Python compile, diff and focused secret scan: PASS;
- local release build: PASS with existing vendored warnings; no Rust diff;
- concentrated correctness/lifecycle/evidence review: PASS, no unresolved
  P0/P1.

Candidate Exit precheck also passes exact instance/EIP/zone/host key,
sing-box binary/config hashes, UDP 8443 listener, zero observer ownership,
clock, disk/memory, Target reachability, and a bounded Target iperf probe with
zero retransmits. The latest shutdown was an explicit ACPI power action; the
current boot is healthy. Automatic update/firmware/snap maintenance remains to
be disabled for the bounded formal window immediately before execution.

## Next

Commit and push the reviewed evidence-only repair, sync/build on the HK Mac,
prove the exact contracted Mac release hash, isolate bounded VPS maintenance,
take fresh direct/resource evidence, and run a fresh Formal 1. Do not tune or
change any data-plane or workload value. A genuine fresh quality failure is
decision-bearing; this invalid control attempt is not.

Spec and plan:

- `docs/tech/2026-08-17-knife15-m2-checkpoint-log-tail-consistency-spec.md`;
- `docs/tech/2026-08-17-knife15-m2-checkpoint-log-tail-consistency-plan.md`.
