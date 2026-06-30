# Knife14d spec — async TCP open to protect downlink/reap progress

> Date: 2026-06-30 | Branch: `codex/knife14d-downlink-reap-open`
> Companion plan: `docs/tech/2026-06-30-knife14d-downlink-reap-open-plan.md`.
> Inputs: knife14c code on `main`, `docs/tech/2026-06-30-knife14c-downlink-mtu-spec.md`, and the
> 2026-06-30 14b US-client result. No post-14c US-client bundle was present locally during grounding.

## TL;DR

Knife14c aligned TUN MTU and added TCP downlink diagnostics, but the current control flow still has one
loop-stall hazard: in pure TUIC mode, the first local payload opens the remote TUIC `Connect` stream inline
inside the single `run_event_loop` task. `TuicUpstream::open_tcp` has a 5s timeout because `open_bi` or the
Connect write can hang on a blackholed or send-window-stalled connection. While that await is inline, the
main loop cannot progress TCP downlink, timer retransmits, DNS hijack, UDP, or `reap_dead_slots`.

Knife14d changes the first-open path so every TCP open runs through the already-existing
`HandshakePending` / `HandshakeDone` asynchronous open machinery. This keeps the main loop responsive while
a remote stream open is in flight. It is not connection pooling and it does not rewrite downlink buffering.

## Grounding

Known code facts:

- `run_event_loop` is a single task multiplexing `global_rx`, `device.wait_for_rx`, `tuic_downlink_rx`,
  sweeps, metrics, and the 5ms smoltcp timer.
- Established TCP uplink is already non-blocking after knife13: the loop reserves channel capacity before
  reading from smoltcp, and leaves bytes in the socket on `Full`.
- TCP downlink correctness relies on `downlink_pending` being flushed on later dirty/timer turns.
- `TuicUpstream::open_tcp` is currently considered cheap by default trait behavior, so pure TUIC opens are
  inline. That function still wraps `open_bi + Connect write` in `TUIC_OPEN_TIMEOUT` because those awaits can
  stall for up to 5s under blackhole/send-window conditions.
- Failover mode already returns `open_is_cheap=false` for all opens to avoid a TOCTOU and loop-stall class.
  Reality also returns `false`.
- The existing async-open path has the needed safety pieces: `HandshakePending`, `conn_epoch`,
  `uplink_buffer`, `HandshakeDone`, failure rearm, and stale-result discard.
- `reap_dead_slots` currently treats `OpeningRemote && uplink_tx.is_none()` as a dead inline-open slot; it
  deliberately does not reap `HandshakePending` unless the local socket is inactive.
- Baseline on this branch is green: `cargo test` passed 213 tests; selected harness tests
  `single_tcp_connection_round_trips`, `metrics_relays_spawned_tracks_load`, and
  `stalled_tcp_uplink_does_not_block_other_flows` passed.

## Grill Design Tree

### Q1. Is 14d allowed to infer from code without a post-14c bundle?

Recommended answer: yes, but only for the structural loop-stall fix. Throughput claims still require the
next US-client rerun. The local machine has the 14b bundle, not a 14c rerun bundle, so the spec must not
claim reverse P1 is fixed.

### Q2. Is the next lever connection pool?

Recommended answer: no. 14b already showed P1 reverse and P2 control/result failure before pool work can be
meaningfully isolated. 14d keeps the pool deferred.

### Q3. Is the downlink bug in `send_slice` pending-tail handling?

Recommended answer: not from current evidence. `flush_downlink` preserves partial tails in
`downlink_pending`, and knife14c added diagnostics around that path. The remaining obvious stall is before
the loop gets to keep flushing: inline first-open awaits.

### Q4. Should only failover/Reality opens be async?

Recommended answer: no. That is already true. The pure TUIC path is the remaining inline path, and
`TuicUpstream::open_tcp` itself documents a 5s hang mode. If a supposedly cheap open can wait for remote
ACK/window progress, it must not run in the single loop task.

### Q5. Should `ProxyUpstream::open_is_cheap` be deleted?

Recommended answer: not in this knife. Keep the trait for now but make `TuicUpstream` override it to
`false`, so all production upstreams use the same async-open path. The default can remain `true` for simple
unit-test mocks until a later cleanup proves it is dead weight.

### Q6. How do we protect bytes arriving while open is in flight?

Recommended answer: reuse `HandshakePending` and `uplink_buffer`. First payload is buffered before spawning
open; later payloads append to the same bounded buffer; successful open flushes the buffer into the relay in
order; failure re-arms the socket so the application TCP stack retries.

### Q7. How should reap interact with in-flight opens?

Recommended answer: `HandshakePending` remains a normal in-flight state. `reap_dead_slots` may reclaim it
only if the local smoltcp socket is no longer active or reaches `CloseWait`. It must not treat it as a stuck
inline open merely because `uplink_tx` is not installed yet.

### Q8. What is the user-facing acceptance?

Recommended answer: the one-command US-client suite remains the real gate. Success is either reverse P1 no
longer collapsing and P2 no longer resetting, or diagnostics showing a different next mechanism. Local tests
only prove the loop no longer stalls behind slow TCP open.

## Scope

### In

- Add a harness scenario where one TCP open is slow but eventually succeeds, while another normal flow must
  complete before the slow open resolves.
- Make pure TUIC open use the async-open path by overriding `open_is_cheap` to `false`.
- Rename or clarify comments/logs so `HandshakePending` is described as a generic async TCP-open state, not
  only REALITY handshaking.
- Add focused tests for reap/open invariants if the behavior is not already directly covered.
- Extend docs/TODO/HANDOFF only as needed to describe knife14d and the next acceptance rerun.

### Out

- No connection pool.
- No downlink buffer rewrite unless a new failing test proves a bug there.
- No MTU/MSS policy change.
- No MetricsSnapshot contract change.
- No claim that US-client reverse throughput is fixed without a fresh bundle.

## Design

### D1. Treat remote TCP open as asynchronous work

The main loop should never await an operation whose worst case is network timeout or QUIC flow-control wait.
Remote TCP open becomes a background task for TUIC, Reality, and Failover. The existing state machine already
supports this: `HandshakePending` means "remote open in flight"; `HandshakeDone` means "open completed";
`conn_epoch` guards stale results.

### D2. Preserve first bytes through `uplink_buffer`

The first local payload that triggers open is buffered in `SocketCtx::uplink_buffer` before the background
task starts. While the open is in flight, additional local payloads are buffered up to `MAX_UPLINK_BUFFER`.
This matches the current Reality path and avoids a second implementation.

### D3. Reap only proven-dead in-flight opens

The periodic reap remains low-frequency and must not reap a live `HandshakePending` open just because no
uplink channel exists yet. It can reap inactive sockets and `CloseWait` sockets. Inline `OpeningRemote`
should become rare or test-only; if still present, the old stuck-open guard may remain for compatibility.

### D4. Harness proves responsiveness, not throughput

The failing test uses a mock upstream whose `open_tcp` for port A waits on a release signal, while port B
opens normally. Under the old inline TUIC-style path, B cannot complete while A is stuck opening. After the
fix, B completes, then A completes after release, with exactly two opens and byte-exact echoes.

## Acceptance

Local gates:

- The new slow-open harness test fails before implementation and passes after.
- Existing knife13 HoL harness still passes.
- `cargo test`
- `cargo test --features harness --test concurrency_harness -- --nocapture`
- `cargo clippy --all-targets --features harness`
- `cargo build --release`
- `git diff --check`

US-client gates after the user reruns the suite:

- The client log contains async-open/relay diagnostics sufficient to distinguish open stalls from downlink
  pending stalls.
- Forward P1 at MTU 1200 does not regress.
- Reverse P1 no longer collapses to about 2M, or the bundle identifies a different next mechanism.
- P2 no longer breaks iperf result/control, or reset logs identify direction and counters.
