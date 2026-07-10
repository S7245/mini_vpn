# Knife14 H10d16 ACK-Capacity Gate A Results

Date: 2026-07-10

## Scope

This stage completed the five approved local ACK-capacity repairs and then ran
the single authorized replacement Gate A. The remote run was one `20s`,
reverse-first, P1 mini_vpn probe at MTU 1200. Gate B was not started.

The code under test was the exact pushed `f7847dd` tree, built in a separate
clean remote worktree. The existing dirty `.27` repository and all unrelated
local dirty files were preserved. No VPS, iperf3, MTU/PLPMTUD, QUIC-window,
pool, chunk-size, or self-wake parameter was changed.

## Local Closure Before VPS

The local repair closed five distinct gaps:

1. a ready local TCP/TUN packet re-enters the existing bounded D16 actor
   without waiting for the 5ms timer;
2. partial bytes already in the smoltcp send queue consume a complete modeled
   packet slot;
3. the local TUN RX service allowance covers the modeled 48-packet ACK/window
   feedback budget before declaring exceptional backlog;
4. the 24-payload-packet sliding admission window remains unchanged and
   actor-exclusive;
5. the integrated capacity and sequential-repeat gates assert both clean
   ownership/lifecycle and the `>=170 Mbit/s` service target.

The 64 MiB production seam improved from about `9.5s` / `56 Mbit/s` to about
`2.40s` / `224 Mbit/s`. Fifty consecutive capacity-qualified repeats passed
with complete delivery, zero modeled drop, zero actor bypass, zero EOF/tail
bytes, and no more than 24 payload packets per flush. Full normal, harness,
integration, check, fmt, clippy, script-self-test, and diff gates passed.

Code review found no remaining P0/P1 defect in the repaired local egress path.

## Preflight And Artifact

- Exit sing-box: active.
- Target iperf3: active.
- Exit socket-buffer maxima: 16 MiB; defaults: 1 MiB.
- Direct reverse baseline: `277 Mbit/s` receiver.
- Remote clean build commit: `f7847dd`.
- Remote bundle:
  `/tmp/mini_vpn_knife14h10d16_ack_capacity_gatea/mvpn_knife14h10d16_ack_capacity_gatea_usclient_suite_20260710_233419.tar.gz`
- Local bundle:
  `/tmp/mini_vpn_knife14h10d16_ack_capacity_gatea_local/mvpn_knife14h10d16_ack_capacity_gatea_usclient_suite_20260710_233419.tar.gz`
- SHA-256:
  `d68a65b69cdef44afb7fe1504db5958dadc01f9e657339d002013f2aeeda7566`

The suite completed with exit code 0, restored the test route, stopped the
tunnel, and created the evidence bundle normally.

## Gate A Decision

The probe moved `45.8 MiB` at the sender and `42.8 MiB` at the receiver:

```text
sender   19.2 Mbit/s
receiver 17.9 Mbit/s
```

The one-second receiver intervals were burst/idle rather than stable. They
ranged from zero to `126 Mbit/s`, with multi-second zero intervals between
bursts.

| Gate | Observation | Decision |
|---|---:|---|
| receiver `>150 Mbit/s` | `17.9 Mbit/s` | fail |
| `tx_dropped_delta=0` | `0` | pass |
| actor bypass | `0` bytes | pass |
| local pressure/backlog | no pressure; pause/resume `0/0` | pass |
| active remote read gap `<1s` | max data read gap `3548ms` | fail |
| QUIC loss/congestion/blocking non-root | all probe deltas `0` | pass |
| close/tail zero | observed counters `0`, but data flow still active | not established |

Gate A therefore failed and Gate B remains frozen.

The final lifecycle parser reported `pending_at_close=0`,
`terminal_pending_reap=0`, and `egress_at_close=0`. These are useful clean
signals, but they are not a complete data-flow EOF proof in this run: the data
handle remained active/dirty at the final snapshot and emitted no natural D16
relay-close event before suite shutdown. The zero counters must not be
overstated as a passed close gate.

## What The Run Proved

The repaired local egress path was not the active throughput limiter:

- D16 actor admitted all `44,964,270` bytes reported by downlink accounting;
- actor-bypass admission, send-slice zero/errors, and TUN flush failures were
  all zero;
- smoltcp send capacity stayed at `1,048,576B`, with only `25,520B` maximum
  send queue;
- local pending stayed at zero with `27,840B` high-water;
- TUN RX recorded `16,126` TCP packets, no errors, no budget exhaustion, and
  no backlog pause/resume edge;
- the actor executed `4,555` bounded cycles and drained `81,737,245B` of local
  egress work;
- the main loop remained mostly parked rather than CPU-saturated;
- QUIC loss, congestion, and tx/rx flow-control blocking deltas remained zero.

The active data stream instead showed repeated ordered-read starvation:

- first data arrived in `3ms`;
- application polling stayed active, normally with `7-19ms` maximum poll gaps
  and one later `402ms` gap;
- successful ordered reads were separated by `1701ms`, `3516ms`, `3548ms`,
  `1697ms`, `3394ms`, and `1646ms` gaps;
- pending episodes reached `3008ms` and thousands of polls;
- six pending samples observed fresh connection-level STREAM-frame progress,
  while six later samples saw no new connection-level STREAM frames.

This matches the architecture discriminator “actor drains, but remote read
gaps return.” It places the next investigation at the
`QuinnDirectOrderedNativeChunkRecv -> TuicNativeOrderedReader -> D16 reader`
service boundary, upstream of the now-clean local actor.

Connection-level STREAM frame counters are not proof that the missing bytes
were already deliverable on this exact stream. The current evidence therefore
does not yet distinguish:

- a same-stream ordered offset/reassembly gap;
- a RecvStream wake/read-operation composition defect under sustained load;
- TUIC/server-side stream service arriving in bursts despite a healthy QUIC
  connection.

It is not evidence for reopening the local 24/48 packet budgets, byte-owned
queue, DrainOnly semantics, VPS tuning, MTU/PLPMTUD, broad QUIC windows, pool,
chunk size, or periodic self-wake.

## Proposed Next Stage

Per the failed-test stop rule, no new production fix was made after Gate A.
The next stage should remain diagnose-first and red-first:

1. Build a sustained real-Quinn, same-stream production-seam test through the
   exact direct ordered adapter, reservation owner, readiness queue, and D16
   actor. The existing single delayed-write test proves one wake only; it does
   not prove continuous high-volume ordered service.
2. Add per-stream evidence that distinguishes poll-Pending with no contiguous
   bytes from poll-Pending despite contiguous deliverable bytes. Do not infer
   this from connection-global frame counts.
3. Run an in-process A/B between direct `RecvStream` consumption and the TUIC
   ordered-reader composition with identical sustained writes. The first seam
   that reproduces burst/idle gaps owns the repair.
4. Make only the repair identified by that RED, preserving pre-read RAII
   ownership, one RecvStream owner, bounded ordered reads, readiness-only
   queueing, actor-exclusive admission, and EOF ordering.
5. Rerun the full local architecture/capacity gates and code review. Request
   authorization for a new Gate A only after that seam is green; do not enter
   Gate B first.

## Architecture Conclusion

The byte-owned egress architecture should be retained. This stage removed a
real local `56 Mbit/s` ceiling and kept the VPS drop/pressure path clean. Gate
A still fails because the actor is starved by intermittent upstream ordered
delivery, not because byte ownership, actor exclusivity, DrainOnly, or the
24-packet admission contract regressed. The remaining work is a focused stream
service diagnosis, not another egress architecture rewrite.
