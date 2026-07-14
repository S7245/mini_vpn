# Knife14h10d16 ACK-Barrier Recovery Local Gate Results

Date: 2026-07-13
Source: `5f9da90f734b1754fd8c41bcb70fa4c8b6ae9f74`
Verdict: **LOCAL PASS; one fresh frozen reverse P8 is eligible**

## Scope

This stage repaired only the D16 phase transition that lost per-flow
ACK-completion evidence when a zero send-queue snapshot occurred while hard
pressure still dominated. It changed no byte capacity, packet budget, timer,
queue, pacing constant, QUIC window, MTU, pool, chunk, Cubic/GSO policy,
driver bound, or wake behavior. No macOS TUN or repair-build VPS run occurred.

## Focused RED And GREEN

The focused RED reproduced the formal P8 ordering:

```text
nonzero send_queue -> arm ACK barrier -> DrainOnly
zero send_queue while hard pressure remains
clean zero snapshot with no new cycle-local drain
```

The old implementation failed exactly because the hard-pressure zero snapshot
cleared the barrier. The minimal GREEN now retains the barrier while external
hard pressure, drop debt, or terminal no-send dominates. The first eligible
clean zero snapshot proves a nonzero-to-zero ACK completion, clears the barrier
once, and supplies positive progress to the existing
`DrainOnly -> Recovery` transition.

An eight-flow test proves all eight independent barriers survive the hard edge
and recover without a global all-zero transaction. Negative controls prove a
zero snapshot without a prior barrier creates no progress, while drop debt and
terminal no-send still retain DrainOnly.

## Local Regression Evidence

```text
focused ACK-completion tests:       2/2
all D16 tests:                      60/60
root library:                       632 passed, 3 ignored
normal concurrency harness:         10 passed, 4 ignored
explicit TCP concurrency:           64/64, 256/256, 1024/1024
UDP payload sweep:                  500/500 at 1000/1400/4000/8000B
cargo check all targets + harness:  PASS
suite/probe/control shell tests:     PASS / PASS / PASS
fmt and diff checks:                PASS
```

The D16 set includes real Quinn ordered-read progress, queue reservation,
global byte budget, actor exclusivity, pending-read cancellation/refund,
backlog control, drop recovery, EOF ordering, terminal close, permit release,
and local drain. The root real-loopback gates retained exact EndpointWindowV1,
cap64-characterization, reverse-stream, GSO-disabled, and D16 delivery.

Rust `1.95.0` strict all-target `-D warnings` Clippy exposed `19` established
repository-wide lints in unchanged code (`too_many_arguments`,
`enum_variant_names`, `manual_clamp`, `collapsible_if`, and
`bool_assert_comparison`). The controlled all-target/all-feature rerun allowed
only those five baseline classes and passed with no new lint from this change.
The vendored smoltcp warnings are also pre-existing. This stage did not expand
into an unrelated lint migration.

## Review

No unresolved P0/P1 was found:

- hard pressure, debt, and terminal state remain authoritative;
- the barrier can only exist after a nonzero send queue was observed;
- consuming it on a later clean zero is therefore strict positive progress,
  not a zero-byte Recovery exception;
- consumption is atomic and cannot repeat;
- terminal rearm still clears per-flow state through the existing lifecycle;
- no async task, reader, waker, queue, or permit ownership changed;
- all frozen Running/Recovery read and admission quanta remain exact.

## Decision

The repair passes the code-level reachability and local regression gate. One
fresh target-only, reverse-only, eight-flow, 60-second VPS gate may run from
this exact source after secret-free export/hash verification and profile-only
rehearsal. It must retain every frozen input and require `>170 Mbit/s`,
`60/60` nonzero intervals, zero TUN drops, pump high below `500`, zero full
waits/errors, useful progress beyond one `128 KiB` quantum on all data flows,
no eligible DrainOnly strand, exact endpoint/D16 ownership, bounded close
tail, and complete cleanup.

If the fresh P8 fails, use its new discriminator as an architecture failure;
do not tune constants or revive rejected sender/pacer/wake branches.
