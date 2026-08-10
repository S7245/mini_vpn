# Knife15 M2 Reverse Gap ACK Reinforcement Local Results

Date: 2026-08-10

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; ONE PAIRED MAC QUALIFICATION REQUIRED; FORMAL M2 AND M3 REMAIN BLOCKED**

Architecture:
`docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-architecture-spec.md`.

Failure evidence:
`docs/tech/2026-08-10-knife15-m2-reverse-gap-ack-reinforcement-qualification-failure-results.md`.

## Implemented Contract

The existing `STREAM_DATA_BLOCKED` receive handler requests one bounded ACK
reinforcement only when the exact open receive stream has an ordered assembler
gap, buffered tail, and a highest received offset covered by the peer's
blocked offset. `PendingAcks` additionally requires split Data packet-number
ranges. A packet-number gap alone is ineligible.

The next normal ACK owns one deadline at the currently negotiated
`max_ack_delay`. New ACK progress replaces the deadline. At expiry the same
received ranges may be acknowledged once more; that ACK is non-ack-eliciting,
consumes the opportunity, and cannot re-arm itself. Confirmed range collapse,
packet-space discard, or connection teardown removes the state naturally.

The public frame statistics and periodic TUIC line expose only ACKs actually
sent through this branch as `gap_ack_reinforcements`. D16, MTU/PLPMTUD, pool,
QUIC windows, chunk, Cubic, GSO, Endpoint, self-wake, retry, workload, and SLI
values are unchanged.

## TDD

The deterministic Pair replay uses `82ms` one-way latency, a scaled `64KiB`
receive/stream window, test-only send/cwnd capacity above that window,
deterministic packet numbers, and disabled test MTUD. It writes beyond the
window and proves the sender is flow-control blocked, drops the first STREAM
datagram, buffers the remaining same-stream tail, applies the production
receive-side pressure semantics before ACK transmit, and drops that final gap
ACK.

Current Quinn was RED because no ACK was emitted before sender loss recovery.
The implementation emits exactly one reinforcement at negotiated
`max_ack_delay`, delivers it before the recorded sender LossDetection/PTO
deadline, retransmits the prefix, and reduces ordered gap bytes to zero.

Additional tests prove:

- skipped or historical packet-number gaps alone emit no reinforcement;
- a newer below-threshold packet cannot overwrite the older deadline;
- newer ACK progress replaces rather than accumulates work;
- range collapse disarms stale work;
- a reinforcement cannot self-rearm;
- ordinary single-packet delayed ACK behavior remains unchanged.

## Capacity And Gates

The exact release 32MiB Endpoint gate passed:

```text
sender capacity:                         239.815 Mbit/s
exact bytes/pattern and clean EOF:       PASS
socket would-block:                      0
available/live reservation/outstanding: 61,440/0/0B
```

Complete gates:

```text
root library:                     700 passed, 3 ignored
main binary:                        2 passed
concurrency integration:           10 passed, 4 ignored
release build:                      PASS
established all-target Clippy:      PASS (existing warnings only)
Knife15/Knife14 shell:              PASS
vendored Quinn:                     40 passed, 3 ignored; doc 1 passed
vendored Quinn integration:          1 expected ignored
vendored quinn-proto:              330 passed; docs 3 passed
vendored quinn-proto Clippy:        PASS (default feature lane)
root doc compile:                    PASS
root fmt and narrow diff:            PASS
```

The first standalone Quinn command omitted the absolute local proto patch and
failed on missing fork APIs. The corrected command ran the exact local proto
and passed. A later extra standalone Quinn Clippy invocation resolved registry
proto despite the transient command config; it is not an established gate.
The established root and quinn-proto Clippy lanes passed, and both generated
ignored lockfiles were removed. A whole-vendor rustfmt invocation also created
mechanical upstream noise; it was fully removed before the narrow diff gate.

## Review

Review checked frame ordering and validation, exact stream/offset authority,
normal/reinforcement deadline coexistence, ACK_FREQUENCY changes, ACK-of-ACK
range retirement, packet-space lifecycle, no-self-rearm, repeated peer
pressure amplification, hot-path allocation, Endpoint Control accounting,
observability, and TCP/UDP/TUN regressions. No unresolved P0/P1 remains.

## Qualification Boundary

Pull and rebuild the pushed reviewed descendant. Start one fresh bounded `.33`
observer only when the Mac is ready, then take exactly one:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> m2-qualification -> status -> stop
```

Preserve `status/snapshot/stop` after failure. Do not run formal M2,
repeat unchanged, or tune constants. A repeated receiver-zero interval with
nonzero `gap_ack_reinforcements` rejects this mechanism as sufficient; zero
reinforcements keeps the exact reachability branch open for paired analysis.
