# Knife14 H10d16 Bidirectional Follow-Up Gate A Results

Date: 2026-07-10

## Outcome

The explicitly authorized replacement Gate A failed. The `20s` reverse-first
P1 completed normally but reported `0.000 Mbit/s` receiver throughput. Gate B
was not started, and no VPS or transport parameter was changed.

The prior one-byte control stall was improved: two target TCP flows opened,
the control flow completed with a clean D16 queue ledger, and the reverse data
flow was established. The remaining blocker is earlier in the data-flow read
path than local actor capacity: the Quinn connection received stream frames
throughout the test, while the D16 ordered reader was not polled to completion
until the `20s` test deadline.

## Run Shape And Preflight

- Client VPS: `43.172.75.27`
- Exit VPS: `43.153.32.33`
- Target VPS: `43.130.32.77:5201`
- Profile: `MINI_VPN_H10D16_BYTE_OWNED_EGRESS=1`
- Shape: `MTU=1200`, reverse-first, `P=1`, `20s`, stop after first P1
- Direct reverse baseline: `286 Mbit/s` receiver
- Exit sing-box: active
- Target iperf3: active
- Exit socket buffers: `16 MiB` max and `1 MiB` defaults
- Remote `cargo check` and release build: pass

## Artifact

Local bundle:

```text
/tmp/mini_vpn_knife14h10d16_bidirectional_followup_gatea_local/
  mvpn_knife14h10d16_bidirectional_followup_gatea_usclient_suite_20260710_140124.tar.gz
```

SHA-256:

```text
01a753e177f528de05e965495ac30310051038689974b70e9714f68768f4c6b7
```

The bundle contains the suite report, client log, reverse-first probe report,
and bounded server-evidence report. No credential material is included.

## Gate A Decision

- `receiver > 150 Mbit/s`: **fail**, `0.000 Mbit/s`
- `tx_dropped_delta = 0`: pass
- `close_egress_bytes = 0`: **fail**, `557386B`
- `terminal_pending_reap = 0`: **fail**, `106710B`
- `actor_bypass_admitted_bytes = 0`: pass
- active remote-read gap `< 1s`: **fail**, `20001ms`
- QUIC loss/congestion as root cause: rejected; both remained zero

The target sender reported `10.5 MiB` sent in its first second, then no further
progress because the receive side stopped advancing. The client-side Quinn
connection concurrently reported about `18.3 MiB` of UDP receive traffic and
`7225` stream frames at the first metrics sample, rising to `7692` frames,
while the D16 data reader still had not returned its first chunk.

## Exact Flow Evidence

The control flow (`SocketHandle(0)`) improved over the first Gate A:

- first remote byte in `3ms`;
- `323B` actor-admitted with no bypass;
- writer made `6` progress events and wrote `491B`;
- final queue state was `queued=0`, `leased=0`, `reserved=0`, `closed=true`;
- close accounting was clean.

The data flow (`SocketHandle(1)`) exposed the remaining failure:

- the TUIC stream opened immediately and its writer sent the `37B` data-flow
  setup payload;
- Quinn connection metrics showed thousands of received stream frames during
  the test;
- the direct ordered reader's first successful read was delayed `20004ms`;
- after that wake it read `799504B` in `596` reads with a data pending gap of
  only `3ms`;
- the actor admitted `692794B`, but this happened after the iperf receive
  window had already completed;
- terminal close left `106710B` pending and `557386B` in the smoltcp send
  queue, with an explicit `524288B` terminal permit drop.

The final D16 queue snapshot was nevertheless exact and empty. This indicates
that terminal ownership refund worked, but the useful read was serviced too
late for the TCP lifecycle to drain cleanly.

## Diagnosis

This matches the architecture spec's discriminator:

> Local gates pass, actor drains, but remote read gaps return: Quinn read wake
> or reservation rearm is the limiter.

The existing unit test named
`d16_direct_ordered_reader_keeps_one_pending_read_future` did not exercise a
real Quinn `RecvStream`, so it could not initially distinguish an adapter wake
defect from a feedback cancellation defect. The local follow-up added that
missing real-Quinn discriminator; the adapter woke normally after delayed
ordered payload. The failure was instead in D16 feedback composition.

The failure is not evidence for changing VPS settings, MTU/PLPMTUD, pool size,
QUIC windows, read chunk size, or self-wake. The transport had already accepted
the remote data; the D16 owner task did not surface it to the byte-owned queue
until the test deadline.

## Proposed Red-First Repair

1. Add a deterministic test using a real local Quinn connection and the exact
   `QuinnDirectOrderedNativeChunkRecv -> TuicNativeOrderedReader` adapter. Poll
   once to Pending, send ordered bytes later, and require the same task to wake
   and return the bytes without a timer or self-wake.
2. Add a second production-seam test around `run_d16_native_reader`: hold a
   live read reservation with unchanged Running credit, make remote bytes
   readable, and require one bounded `DataReady` plus exact reservation/queue
   accounting. These two tests distinguish adapter waker loss from supervisor
   feedback/rearm loss.
3. Only after the red test identifies the boundary, keep one real Quinn read
   operation alive for the reservation lifetime. Preserve one-task
   `RecvStream` ownership, ordered bounded reads, feedback cancellation, RAII
   refund, readiness-only queueing, and actor-exclusive admission. Do not add a
   nested payload task/channel or periodic self-wake.
4. Replace the mock-only pending-future assertion with a test that observes the
   real adapter operation, then rerun all D16 focused and full local gates.
5. Repair server-evidence routing before any later VPS request. Target SSH from
   `.27` currently follows the target `/32` TUN route, creates extra D16 port-22
   flows, and can hang the parent suite. Use a bounded management path such as
   an Exit-host ProxyJump, or collect target evidence only after route cleanup.

No further VPS run is authorized or justified until these local red/green
gates pass and the resulting code is reviewed against the architecture spec.

## Local Red/Green Repair

The real Quinn delayed-readability test passed: after the exact production
adapter returned Pending, a payload written `20ms` later woke the same task and
was returned within the `1s` bound. This rejected the short-lived Quinn adapter
as the active root.

The next production-seam test failed immediately with the exact deadlock:

```text
expected: RelayReadCredit { paused: false, max_batch_bytes: 524288 }
actual:   RelayReadCredit { paused: true,  max_batch_bytes: 0 }
```

The causal sequence was:

1. D16 starts with `512 KiB` Running credit, equal to the per-flow capacity.
2. The reader correctly reserves all `512 KiB` before polling Quinn.
3. `d16_relay_read_credit` subtracts `reserved_bytes` from available capacity,
   sees zero, and publishes pause.
4. The pending Quinn read selects the feedback update, cancels, and refunds its
   reservation by RAII.
5. No byte was committed, so no `DataReady` exists to make the main loop
   publish a resumed credit. The flow remains paused until an unrelated event.

The minimal fix treats the current flow's reservation as the already-armed read
opportunity when publishing feedback:

```text
read_opportunity = available_bytes + own_reserved_bytes
```

This does not add capacity or release ownership. The queue still enforces both
the `512 KiB` per-flow ledger and the `64 MiB` process ledger before Quinn is
polled. `Recovery` still caps the opportunity to `128 KiB`, and `DrainOnly`
still publishes `paused=true, max_batch_bytes=0` and refunds a pending read.

Post-fix gates:

- D16 focused: `39/39`;
- normal library: `567/567`;
- harness library: `573/573`;
- integration targets: `2/2` and `10 passed`, with `4` existing non-D16
  ignored tests;
- `cargo fmt --check`, `cargo check -q`, shell syntax, suite self-test, and
  `git diff --check`: pass.

The server-evidence path was also repaired locally. For the known topology,
Target evidence now uses a bounded Exit-host proxy command carrying the Exit
identity, port, and host-key policy, so the Target `/32` TUN route cannot turn
management SSH into another flow under test. Every evidence SSH command also
has a `20s` outer timeout. No remote validation has been run after these fixes.
