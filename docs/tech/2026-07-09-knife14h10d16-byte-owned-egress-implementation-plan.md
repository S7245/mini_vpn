# Knife14h10d16 Byte-Owned Egress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. If those skills are unavailable, execute inline with the repository-required `diagnose`, `tdd`, `code-review`, and `self-improving-agent` gates.

**Goal:** Build a byte-owned TUIC TCP read reservoir and actor-exclusive TCP/TUN egress path that preserves sing-box-class throughput near `170 Mbit/s` without TUN drops or close-tail loss.

**Architecture:** One task owns the Quinn receive stream and must reserve byte capacity before every ordered read. Committed bytes live in one per-flow leased byte queue; `global_rx` carries coalesced readiness rather than payload. The single smoltcp/TUN actor owns all D16 downlink admission and uses a `Running -> DrainOnly -> Recovery` state machine that stops reads/admission under pressure while continuing local drain.

**Tech Stack:** Rust, Tokio, Quinn 0.10, smoltcp, bytes, project `TunIo`/harness, shell VPS acceptance scripts.

---

## File Map

- `src/tcp_downlink_pump.rs`: byte reservation, leased per-flow queue, coalesced readiness, exact ownership snapshots.
- `src/tuic.rs`: direct ordered Quinn reader with no nested payload pump/channel and a real `max_len` contract.
- `src/upstream.rs`: native reader contract and D16 relay mode boundary.
- `src/tcp_egress.rs`: pure egress phase/permission state machine and ownership model.
- `src/client_tun.rs`: feature-gate composition, per-flow queue attachment, readiness handling, actor-exclusive admission, permit release, lifecycle.
- `src/harness.rs`: deterministic integrated pressure, drain, EOF, and fairness scenarios.
- `src/lib.rs`: module exports only if a new public test seam is required.
- `scripts/knife14b-usclient-tunnel-suite.sh`: D16 option, self-test, redacted/reproducible environment report, acceptance summary fields.
- `docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-architecture-spec.md`: approved source of truth.
- `.learnings/LEARNINGS.md` and `.learnings/ERRORS.md`: stage results and reusable failures; never include credentials.

Do not modify unrelated REALITY, DNS, UDP, failover, or product-control-plane files. Stage only the files listed by each task because the worktree contains unrelated existing changes.

The D16 files themselves already contain uncommitted H4-H10 experimental work.
Before the first commit, inspect `git diff -- <each staged file>` and show the
user the exact baseline changes that would be included. Do not stage a whole
overlapping file merely because a task lists it. If a clean partial stage is
not possible, continue implementation and verification without committing
until the user approves a baseline/commit strategy.

### Task 1: Freeze Baseline And Add Architecture Reachability Tests

**Files:**
- Modify: `src/client_tun.rs` test module near the existing D3/D6 tests
- Modify: `src/tuic.rs` test module near native ordered reader tests
- Test: `src/client_tun.rs`, `src/tuic.rs`

- [ ] **Step 1: Record the pre-change local baseline**

Run:

```bash
cargo test -q --lib
cargo check -q
cargo fmt --check
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
```

Expected: `518` lib tests pass at the current baseline; check, fmt, and suite self-test exit `0`. If counts differ because the user changed the worktree, record the new count without reverting their changes.

- [ ] **Step 2: Add red tests for the four required contracts**

Add these exact test names, initially calling seams introduced by later tasks:

```rust
#[test]
fn d16_read_reservation_counts_before_remote_read() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    assert_eq!(queue.blocking_snapshot_for_test().reserved_bytes, 0);
}

#[test]
fn d16_actor_mode_rejects_non_actor_downlink_admission() {
    assert!(!DownlinkAdmissionOrigin::ControlOnly.allows_d16_admission());
    assert!(DownlinkAdmissionOrigin::EgressActor.allows_d16_admission());
}

#[test]
fn d16_drain_only_stops_read_and_admit_but_keeps_drain() {
    let permissions = EgressPermissions::for_phase(EgressPhase::DrainOnly);
    assert!(!permissions.allow_read);
    assert!(!permissions.allow_admission);
    assert!(permissions.allow_drain);
}

#[test]
fn d16_remote_eof_requires_closed_and_empty_queue() {
    assert!(!remote_eof_ready_for_local_close(false, true, 0, 0));
    assert!(!remote_eof_ready_for_local_close(true, false, 1, 0));
    assert!(remote_eof_ready_for_local_close(true, true, 0, 0));
}
```

- [ ] **Step 3: Run the focused tests and verify red**

Run:

```bash
cargo test -q --lib d16_ -- --nocapture
```

Expected: compilation fails only because the planned D16 types and functions do not exist. Do not make unrelated tests red.

- [ ] **Step 4: Commit only the red tests if the session workflow uses red-test commits**

```bash
git add src/client_tun.rs src/tuic.rs
git commit -m "test(tcp): specify d16 byte-owned egress contracts"
```

If the repository workflow prefers red and green in one commit, leave the tests unstaged and continue to Task 2.

### Task 2: Add Pre-Read RAII Reservation To The Leased Byte Queue

**Files:**
- Modify: `src/tcp_downlink_pump.rs:340-658`
- Test: `src/tcp_downlink_pump.rs` test module

- [ ] **Step 1: Extend the queue snapshot and state**

Add `reserved_bytes` to both snapshot and state, and include it in available-capacity math:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeasedByteQueueSnapshot {
    pub queued_bytes: usize,
    pub leased_bytes: usize,
    pub reserved_bytes: usize,
    pub capacity_bytes: usize,
    pub high_water_bytes: usize,
    pub chunks: usize,
    pub closed: bool,
}

impl LeasedByteQueueSnapshot {
    pub fn owned_bytes(self) -> usize {
        self.queued_bytes
            .saturating_add(self.leased_bytes)
            .saturating_add(self.reserved_bytes)
    }

    pub fn available_bytes(self) -> usize {
        self.capacity_bytes.saturating_sub(self.owned_bytes())
    }
}
```

`LeasedByteFlowQueueState::available_bytes` and `note_high_water` must use the same three-state sum.

- [ ] **Step 2: Add the reservation type and API**

Implement this public contract:

```rust
pub struct ByteQueueReadReservation {
    inner: Arc<AsyncLeasedByteFlowQueueInner>,
    reserved_bytes: usize,
}

impl ByteQueueReadReservation {
    pub fn max_len(&self) -> usize {
        self.reserved_bytes
    }

    pub fn commit(mut self, bytes: Bytes) -> Result<bool, ByteQueuePushError> {
        let len = bytes.len();
        if len == 0 {
            return Err(ByteQueuePushError::EmptyChunk);
        }
        if len > self.reserved_bytes {
            return Err(ByteQueuePushError::ChunkExceedsCapacity {
                len,
                capacity: self.reserved_bytes,
            });
        }
        let mut state = self.inner.lock_state();
        if state.closed {
            return Err(ByteQueuePushError::Closed);
        }
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        self.reserved_bytes = 0;
        let should_wake = state.queued_bytes == 0 && !state.wake_pending;
        state.queued_bytes = state.queued_bytes.saturating_add(len);
        state.chunks.push_back(bytes);
        state.wake_pending = true;
        state.note_high_water();
        drop(state);
        self.inner.not_empty.notify_one();
        Ok(should_wake)
    }
}

impl Drop for ByteQueueReadReservation {
    fn drop(&mut self) {
        if self.reserved_bytes == 0 {
            return;
        }
        let mut state = self.inner.lock_state();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        self.reserved_bytes = 0;
        drop(state);
        self.inner.not_full.notify_waiters();
    }
}
```

Add `wake_pending: bool` and `reserved_bytes: usize` to `LeasedByteFlowQueueState`.

Add:

```rust
pub async fn reserve_read_up_to(
    &self,
    requested: usize,
) -> Result<ByteQueueReadReservation, AsyncByteQueueWait>
```

The method waits until capacity is available, moves `min(requested, available)` into `reserved_bytes`, and returns a reservation. `requested == 0` returns a zero reservation without mutating state; production callers must not poll Quinn with it.

- [ ] **Step 3: Add reservation tests**

Cover all four cases:

```rust
#[tokio::test]
async fn read_reservation_is_counted_before_commit_and_refunded_on_drop() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert_eq!(reservation.max_len(), 64 * 1024);
    assert_eq!(queue.snapshot().await.reserved_bytes, 64 * 1024);
    drop(reservation);
    assert_eq!(queue.snapshot().await.reserved_bytes, 0);
}

#[tokio::test]
async fn read_reservation_commit_refunds_unused_capacity() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(reservation.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.queued_bytes, 16 * 1024);
    assert_eq!(snapshot.available_bytes(), 112 * 1024);
}
```

Also test over-commit rejection and `reserved + queued + leased <= capacity` across reserve, commit, receive, partial release, and drop.

- [ ] **Step 4: Run focused and module tests**

```bash
cargo test -q --lib read_reservation -- --nocapture
cargo test -q --lib tcp_downlink_pump -- --nocapture
```

Expected: all queue tests pass; no capacity wait spins and no poisoned-lock panic is introduced.

- [ ] **Step 5: Commit**

```bash
git add src/tcp_downlink_pump.rs
git commit -m "feat(tcp): reserve downstream bytes before remote reads"
```

### Task 3: Replace The Hidden TUIC Ordered Pump With A Direct Bounded Reader

**Files:**
- Modify: `src/tuic.rs:784-1121`
- Modify: `src/upstream.rs:22-47`
- Test: `src/tuic.rs` test module

- [ ] **Step 1: Tighten the native reader contract**

Document on `NativeTcpReader::poll_read_chunk` that a successful chunk must be ordered for the selected D16 reader and `bytes.len() <= max_len`. Add no client-tun types to `upstream.rs`.

- [ ] **Step 2: Introduce a direct ordered Quinn adapter**

Replace the nested `TuicNativeOrderedPumpReader` task/channel in the D16 path with a reader that owns `quinn::RecvStream` directly:

```rust
struct TuicNativeOrderedReader {
    recv: quinn::RecvStream,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Connection,
    next_offset: u64,
    pending_self_wake_deadline: Option<Instant>,
    _lease: TcpPoolSlotLease,
}
```

Its poll path must use the caller's limit:

```rust
let read_len = max_len.max(1).min(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES * 2);
let poll = {
    let fut = self.recv.read_chunk(read_len, true);
    tokio::pin!(fut);
    fut.poll(cx)
};
```

On data, reject an offset different from `next_offset`, advance `next_offset`, and return at most `read_len`. Remove the D16 dependency on `TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS`, `TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS`, and `read_chunks`.

- [ ] **Step 3: Add a max-length and pending-future test seam**

Reuse or extend the existing `OrderedChunkRecv` abstraction so tests can assert the requested `max_len`. Add tests named:

```text
d16_direct_ordered_reader_forwards_exact_max_len
d16_direct_ordered_reader_never_returns_more_than_max_len
d16_direct_ordered_reader_keeps_one_pending_read_future
d16_direct_ordered_reader_rejects_offset_discontinuity
```

The first test must assert that a `128 * 1024` caller request reaches the adapter unchanged except for the explicit D16 maximum.

- [ ] **Step 4: Run TUIC focused tests**

```bash
cargo test -q --lib d16_direct_ordered_reader -- --nocapture
cargo test -q --lib native_ordered -- --nocapture
```

Expected: the new direct reader tests pass; legacy ordered join, H4 diagnostic ordered chunk, and generic TUIC tests remain green.

- [ ] **Step 5: Commit**

```bash
git add src/tuic.rs src/upstream.rs
git commit -m "refactor(tuic): make ordered TCP reads directly byte bounded"
```

### Task 4: Make The Per-Flow Queue The Only Remote Payload Reservoir

**Files:**
- Modify: `src/tcp_downlink_pump.rs`
- Modify: `src/client_tun.rs:1600-2200, 9953-12306`
- Test: `src/client_tun.rs`, `src/tcp_downlink_pump.rs`

- [ ] **Step 1: Add nonblocking queue drain and closed-empty state**

Add:

```rust
pub enum LeasedQueuePoll {
    Data(LeasedBytes),
    Empty,
    Closed,
}

pub fn try_recv_up_to(&self, limit: usize) -> LeasedQueuePoll
```

`Data` moves bytes from queued to leased. `Empty` clears `wake_pending` only when the queue is open and empty. `Closed` is returned only when `closed && queued_bytes == 0 && reserved_bytes == 0`.

Change queue close to return whether a wake is needed:

```rust
pub async fn close_and_mark_ready(&self) -> bool
```

- [ ] **Step 2: Replace payload relay events with readiness**

Add the D16 event without deleting legacy `RelayEvent::Data`:

```rust
DataReady {
    epoch: u64,
},
```

Add the relay spawn result:

```rust
struct SpawnedRemoteRelay {
    read_credit_tx: watch::Sender<RelayReadCredit>,
    d16_downlink_queue: Option<AsyncLeasedByteFlowQueue>,
}
```

Store the optional queue in `SocketCtx` and clear it during rearm.

- [ ] **Step 3: Change the D16 read task to reserve, read, commit, and wake**

The core loop must have this ordering:

```rust
let reservation = queue.reserve_read_up_to(read_request).await?;
let max_len = reservation.max_len();
let chunk = tokio::select! {
    _ = &mut stop_rx => {
        drop(reservation);
        return;
    }
    update = read_credit_rx.changed() => {
        drop(reservation);
        update?;
        continue;
    }
    chunk = poll_native_tcp_chunk(&mut remote_reader, max_len) => chunk?,
};
match chunk {
    Some(chunk) => {
        let should_wake = reservation.commit(chunk.bytes)?;
        if should_wake {
            back_tx.try_send((handle, RelayEvent::DataReady { epoch }))?;
        }
    }
    None => {
        let should_wake = queue.close_and_mark_ready().await;
        if should_wake {
            back_tx.try_send((handle, RelayEvent::DataReady { epoch }))?;
        }
        return;
    }
}
```

Use explicit match-based error handling in production rather than `?` where the task must emit a diagnostic close reason.

- [ ] **Step 4: Remove the D16 dispatcher payload copy**

The D16 path must not spawn `run_permit_relay_dispatcher` and must not call `bytes.to_vec()` for remote payload delivery. Keep the dispatcher only for legacy diagnostics until cleanup.

- [ ] **Step 5: Add tests**

Add deterministic tests proving:

- several remote chunks coalesce to one pending `DataReady` wake;
- the actor can drain the queue even if the wake channel contains only one event;
- queue payload bytes plus leased bytes never exceed `512 KiB`;
- a read-credit pause while the read future is pending cancels the read and refunds the reservation;
- no `RelayEvent::Data` is emitted by the D16 path.

- [ ] **Step 6: Run focused tests and commit**

```bash
cargo test -q --lib d16_ -- --nocapture
cargo test -q --lib native_permit -- --nocapture
git add src/client_tun.rs src/tcp_downlink_pump.rs
git commit -m "feat(tcp): use one byte-owned remote downlink reservoir"
```

### Task 5: Make D16 Actor The Exclusive Downlink Admission Owner

**Files:**
- Modify: `src/client_tun.rs:3096-3243, 7760-8293, 9120-9775`
- Modify: `src/tcp_egress.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Introduce explicit admission origin**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownlinkAdmissionOrigin {
    Legacy,
    EgressActor,
    ControlOnly,
}

impl DownlinkAdmissionOrigin {
    fn allows_d16_admission(self) -> bool {
        matches!(self, Self::EgressActor)
    }
}
```

Pass this value to `process_listener_activity`. Move the current top-level `flush_downlink` block into `admit_downlink_for_handle`, which is called only when the origin permits admission for that flow.

- [ ] **Step 2: Route all D16 call sites correctly**

- Remote payload/readiness, timer, TUN RX, ACK hint, and maintenance call sites outside `service_local_egress_until` pass `ControlOnly`.
- `service_local_egress_until` passes `EgressActor` after its drain/poll/flush phase.
- Legacy default mode passes `Legacy` and retains existing behavior.

Do not add a second `send_slice` call inside the actor. The existing
`flush_downlink` remains the single admission primitive.

- [ ] **Step 3: Add bypass accounting**

Add `actor_bypass_admitted_bytes` to the local egress diagnostics. If a D16 flow is called with a non-actor origin, it must admit zero bytes and increment no accepted-byte counter. A debug log may record the attempted origin without logging payload.

- [ ] **Step 4: Add red/green tests**

Tests must prove:

```text
d16_timer_control_pass_does_not_call_send_slice
d16_tun_rx_control_pass_does_not_call_send_slice
d16_actor_pass_admits_pending_bytes
d16_actor_admitted_equals_total_d16_send_slice_accepted
```

- [ ] **Step 5: Run and commit**

```bash
cargo test -q --lib d16_actor -- --nocapture
cargo test -q --lib d3_egress_actor -- --nocapture
git add src/client_tun.rs src/tcp_egress.rs
git commit -m "refactor(tcp): make the d16 actor the sole downlink writer"
```

### Task 6: Add Running, DrainOnly, And Recovery Feedback

**Files:**
- Modify: `src/tcp_egress.rs`
- Modify: `src/client_tun.rs:1629-1900, 4432-4619, 6767-6893, 8103-8293`
- Test: `src/tcp_egress.rs`, `src/client_tun.rs`

- [ ] **Step 1: Add the pure state machine**

```rust
pub const D16_RECOVERY_CLEAN_CYCLES: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressPhase {
    Running,
    DrainOnly,
    Recovery { clean_cycles: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressPermissions {
    pub allow_read: bool,
    pub allow_admission: bool,
    pub allow_drain: bool,
    pub max_quantum_bytes: usize,
}

impl EgressPermissions {
    pub fn for_phase(phase: EgressPhase) -> Self {
        match phase {
            EgressPhase::Running => Self {
                allow_read: true,
                allow_admission: true,
                allow_drain: true,
                max_quantum_bytes: 512 * 1024,
            },
            EgressPhase::DrainOnly => Self {
                allow_read: false,
                allow_admission: false,
                allow_drain: true,
                max_quantum_bytes: 0,
            },
            EgressPhase::Recovery { .. } => Self {
                allow_read: true,
                allow_admission: true,
                allow_drain: true,
                max_quantum_bytes: 128 * 1024,
            },
        }
    }
}
```

Add a pure transition function taking `hard_pressure`, `drop_debt`,
`below_low_watermark`, and `drain_progress`. Implement the transitions exactly
as defined in the spec.

- [ ] **Step 2: Separate drain permission from admission permission**

Delete the D16 early return that currently occurs before TUN RX/poll/flush.
`service_local_egress_until` must always execute drain work when
`allow_drain=true`; it skips only queue-to-pending and pending-to-smoltcp
admission when `allow_admission=false`.

- [ ] **Step 3: Couple relay read credit to phase**

- `DrainOnly` publishes `paused=true, max_batch_bytes=0`.
- `Recovery` caps read requests at `128 KiB`.
- `Running` permits up to the available reservation, with a service request of
  at least `128 KiB` when nonzero progress is allowed.

The pending read `tokio::select!` must include `read_credit_rx.changed()` so a
phase transition cancels the pending read and drops its reservation.

- [ ] **Step 4: Add state and integration tests**

Required tests:

```text
d16_running_enters_drain_only_on_drop
d16_drain_only_keeps_poll_and_flush_progress
d16_drain_only_does_not_read_or_admit
d16_drain_only_enters_recovery_below_low_after_progress
d16_recovery_requires_four_clean_cycles
d16_recovery_falls_back_on_new_pressure
d16_pending_read_pause_refunds_reservation_before_delivery
```

- [ ] **Step 5: Run and commit**

```bash
cargo test -q --lib d16_drain_only -- --nocapture
cargo test -q --lib d16_recovery -- --nocapture
git add src/tcp_egress.rs src/client_tun.rs
git commit -m "feat(tcp): separate egress drain from read admission pause"
```

### Task 7: Order EOF Behind The Owned Queue And Close Cleanly

**Files:**
- Modify: `src/tcp_downlink_pump.rs`
- Modify: `src/client_tun.rs:1600-1630, 8400-9065, 11189-12306`
- Test: `src/client_tun.rs`, `src/tcp_downlink_pump.rs`

- [ ] **Step 1: Represent clean EOF in queue state**

Queue close means no further pushes. `try_recv_up_to` reports `Closed` only
after reserved and queued bytes reach zero. Leased bytes remain owned by the
actor until local egress commit or explicit terminal drop.

- [ ] **Step 2: Add a single lifecycle predicate**

```rust
fn remote_eof_ready_for_local_close(
    queue_closed: bool,
    queue_empty: bool,
    pending_bytes: usize,
    inflight_permit_bytes: usize,
) -> bool {
    queue_closed
        && queue_empty
        && pending_bytes == 0
        && inflight_permit_bytes == 0
}
```

- [ ] **Step 3: Prevent cross-producer data/EOF races**

The D16 reader closes the queue and emits `DataReady`. It does not send a clean
remote `Closed` event directly. The actor observes `LeasedQueuePoll::Closed`
after draining data and then installs deferred remote EOF in the existing local
TCP lifecycle.

Remote read errors remain explicit error events and may drop owned bytes only
through the terminal accounting path.

- [ ] **Step 4: Add lifecycle tests**

Required tests:

```text
d16_data_ready_payload_is_drained_before_remote_eof
d16_remote_eof_waits_for_inflight_tun_permit_release
d16_local_fin_keeps_useful_reverse_queue_alive
d16_terminal_local_close_releases_exact_owned_bytes_once
d16_rearm_detaches_old_epoch_queue_and_ignores_late_wake
```

Each test must assert queue, pending, inflight, terminal-drop, and released-byte
totals, not only socket state.

- [ ] **Step 5: Run and commit**

```bash
cargo test -q --lib d16_remote_eof -- --nocapture
cargo test -q --lib d16_terminal -- --nocapture
git add src/client_tun.rs src/tcp_downlink_pump.rs
git commit -m "fix(tcp): drain byte-owned payload before remote EOF"
```

### Task 8: Add Process-Wide Budget And Deterministic Integrated Harness

**Files:**
- Modify: `src/tcp_downlink_pump.rs`
- Modify: `src/client_tun.rs`
- Modify: `src/harness.rs`
- Test: `src/harness.rs`, module tests

- [ ] **Step 1: Add the shared global budget**

Create one process-wide budget with default `64 MiB`. Each D16 read reservation
must acquire both per-flow and global capacity; drop or commit refunds unused
capacity to both. Do not allocate `512 KiB` eagerly for idle flows.

Expose a snapshot containing `reserved`, `owned`, `capacity`, and high water.

- [ ] **Step 2: Add an integrated heavy-flow harness scenario**

The scenario must run the production queue, readiness, actor, state machine,
mock smoltcp/TUN drain seam, and lifecycle transitions with deterministic
steps equivalent to `128 KiB` every `5ms` for 30 seconds.

Assertions:

```rust
assert!(result.receiver_capacity_mbps >= 200.0);
assert_eq!(result.actor_bypass_admitted_bytes, 0);
assert_eq!(result.tun_drop_bytes, 0);
assert_eq!(result.close_egress_bytes, 0);
assert!(result.per_flow_high_water_bytes <= 512 * 1024);
assert!(result.global_high_water_bytes <= 64 * 1024 * 1024);
```

- [ ] **Step 3: Add pressure and fairness scenarios**

- Pause one flow while another continues; the second flow must make progress.
- Saturate enough flows to reach the global budget; total ownership must remain
  bounded and no flow may busy-spin.
- Inject drop debt; remote read/admission stop while drain continues.
- Release pressure; recovery proceeds in four clean cycles.

- [ ] **Step 4: Run harness and regression tests**

```bash
cargo test -q --features harness d16_ -- --nocapture
cargo test -q --lib tcp_egress -- --nocapture
cargo test -q --lib tcp_downlink_pump -- --nocapture
```

- [ ] **Step 5: Commit**

```bash
git add src/tcp_downlink_pump.rs src/client_tun.rs src/harness.rs
git commit -m "test(tcp): prove d16 capacity pressure and ownership"
```

### Task 9: Add One Reproducible D16 Runtime Gate

**Files:**
- Modify: `src/client_tun.rs:6077-6492`
- Modify: `src/tuic.rs` configuration/mode selection
- Modify: `scripts/knife14b-usclient-tunnel-suite.sh`
- Test: module parser tests and suite self-test

- [ ] **Step 1: Add one composition gate**

Add `MINI_VPN_H10D16_BYTE_OWNED_EGRESS=1` to `TunRuntimeConfig`. When enabled,
it selects the direct ordered native reader, byte-owned reservoir, D16 actor,
and D16 feedback state machine. It must not require an undocumented combination
of D3/D4/D5/D6/D15 environment variables.

Keep the old flags available only for diagnostics until Task 12 cleanup.

- [ ] **Step 2: Add startup diagnostics**

Print one non-secret line containing:

```text
H10d16 byte-owned egress: enabled per_flow_cap=524288 global_cap=67108864 quantum=131072
```

- [ ] **Step 3: Make the suite export and report the gate**

Add a default of `0`, help text, self-test assertion, environment summary, and
the variable in the actual `sudo -E env` launch command. Never print `.env`,
credentials, private keys, or passwords.

- [ ] **Step 4: Run configuration gates**

```bash
cargo test -q --lib h10d16 -- --nocapture
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
```

Expected: parser is opt-in, startup format test passes, and the suite report
contains the D16 flag exactly once in summary and launch command.

- [ ] **Step 5: Commit**

```bash
git add src/client_tun.rs src/tuic.rs scripts/knife14b-usclient-tunnel-suite.sh
git commit -m "feat(tcp): expose the h10d16 byte-owned egress profile"
```

### Task 10: Run Full Local Gates And Stage Code Review

**Files:**
- Review: all files changed by Tasks 2-9
- Modify if findings require: only files in the D16 scope
- Update: `.learnings/LEARNINGS.md`
- Update on failed commands that change procedure: `.learnings/ERRORS.md`

- [ ] **Step 1: Run formatting and static gates**

```bash
cargo fmt
cargo fmt --check
cargo check -q
git diff --check
```

- [ ] **Step 2: Run full tests**

```bash
cargo test -q --lib
cargo test -q --features harness
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
```

Expected: all tests pass. Do not request VPS acceptance with ignored, flaky, or
failing D16 ownership/lifecycle tests.

- [ ] **Step 3: Run code review with required lenses**

Review specifically for:

- bytes read without reservation;
- reservation leak or double release;
- payload-bearing message-count queues;
- D16 `send_slice` outside actor origin;
- DrainOnly stopping local drain;
- EOF racing payload or epoch reuse;
- one flow starving another;
- hot-path blocking mutex held across `.await`;
- secrets in suite output.

- [ ] **Step 4: Fix all P0/P1 findings and rerun the affected gates**

Do not run VPS with an unresolved P1. Record a concise stage learning with the
new commit IDs and local test counts.

- [ ] **Step 5: Commit review fixes and local result**

```bash
git add src/tcp_downlink_pump.rs src/tcp_egress.rs src/upstream.rs src/tuic.rs src/client_tun.rs src/harness.rs src/lib.rs scripts/knife14b-usclient-tunnel-suite.sh .learnings/LEARNINGS.md .learnings/ERRORS.md
git commit -m "fix(tcp): close d16 review findings before vps gate"
```

Before committing, inspect `git diff --cached --name-only` and unstage any
unrelated pre-existing file.

### Task 11: Run One Scoped VPS Gate A

**Files:**
- No source edits before the run
- Add after analysis: a dated D16 result document in `docs/tech/`
- Update: `.learnings/LEARNINGS.md`; update `.learnings/ERRORS.md` only for reusable failures

- [x] **Step 1: Sync only reviewed D16 files to `.27`**

Use `rsync -R` from the repository root so `src/` and `scripts/` paths are
preserved. Verify remote `git status --short` for expected paths before build.

- [x] **Step 2: Run no-secret service preflight**

```bash
ssh -i ~/.ssh/vpn ubuntu@43.153.32.33 'systemctl is-active sing-box'
ssh -i ~/.ssh/vpn ubuntu@43.130.32.77 'systemctl is-active iperf3'
```

Check the persisted `.33` socket-buffer values, but do not tune them.

- [x] **Step 3: Build and run focused Gate A with a true writable TTY**

Start the command with tool-level `tty=true` and remote `ssh -tt`. Source the
existing remote `.env` without printing it:

```bash
ssh -tt -i ~/.ssh/vpn ubuntu@43.172.75.27 'cd /home/ubuntu/mini_vpn && . "$HOME/.cargo/env" && . ./.env && SUITE_TAG=knife14h10d16_byte_owned_gatea DURATION=20 RUN_REVERSE_FIRST_P1=1 STOP_AFTER_REVERSE_FIRST_P1=1 SERVER_EVIDENCE_CHECK=1 MINI_VPN_H10D16_BYTE_OWNED_EGRESS=1 bash scripts/knife14b-usclient-tunnel-suite.sh'
```

Enter sudo credentials only at the interactive prompt. Never place them in a
command, file, documentation, log, or summary.

- [x] **Step 4: Apply the Gate A decision table**

Pass only if all are true:

```text
receiver > 150 Mbit/s
tx_dropped_delta = 0
close_egress_bytes = 0
terminal_pending_reap = 0
actor_bypass_admitted_bytes = 0
max active remote-read gap < 1s
max active local-egress gap < 1s
QUIC loss/congestion/blocking remains non-root
```

Failure interpretation:

- ownership/actor invariant nonzero: implementation bug, return to the owning
  local task;
- queue full plus low actor drain: actor/TUN bottleneck;
- actor drain healthy plus remote gap: Quinn reservation/wake bottleneck;
- high throughput plus drops: release/pressure semantics insufficient;
- zero drops plus close bytes: lifecycle ordering insufficient.

Per project rules, analyze a failed run and present the next modification plan
before editing again.

- [x] **Step 5: Save evidence and stage learning**

Copy the generated bundle to a local `/tmp/mini_vpn_knife14h10d16_*`
directory, write a dated result document with no secrets, and record the exact
pass/fail discriminator.

Gate result (2026-07-10): the clean `f7847dd` build completed the authorized
20-second reverse-first P1 at `19.2/17.9 Mbit/s`, so Gate A failed and Gate B
remains frozen. Local egress discriminators stayed clean: TUN drops, actor
bypass, pressure/backlog edges, send/flush failures, and QUIC loss/congestion/
blocking were zero. The actor admitted all bytes delivered to it, while the
ordered data stream had repeated read gaps up to `3548ms` despite active
polling. Observed close-tail counters were zero, but the data flow remained
active at the final snapshot, so natural EOF/close was not established.

To preserve the existing dirty `.27` repository, Step 1 used a separate remote
clean worktree at exact commit `f7847dd` rather than overwriting the dirty
tree. The suite script remained in its existing repository and its SHA-256 was
verified equal to the local script before launch; the release binary came only
from the clean worktree.

The next red-first task is a sustained same-stream discriminator through the
exact Quinn/TUIC/D16 ordered-reader composition. It must separate contiguous
same-stream availability from connection-global frame progress before any
production fix. Result:
`docs/tech/2026-07-10-knife14h10d16-ack-capacity-gate-a-results.md`.

### Task 11A: Close Global Drop Recovery And TUN RX Starvation Locally

This task is mandatory after the failed credit-rearm Gate A and before any
replacement Gate A. It adopts the evidence-backed parts of the 2026-07-10
egress architecture review while preserving the approved D16 ownership,
single-actor, EOF, and strict acceptance contracts.

**Files:**
- Modify: `src/client_tun.rs`
- Modify only if the production seam requires it: `src/harness.rs`,
  `src/device.rs`, `src/tcp_egress.rs`
- Modify: this plan and the D16 architecture spec
- Update after green/review: `.learnings/LEARNINGS.md`,
  `.learnings/ERRORS.md`

- [x] **Step 1: Lock the Linux TUN direction into the design**

Treat `tun0 tx_dropped` as kernel-to-userspace TUN RX ring loss: local
ACK/control/uplink packets were not consumed by mini_vpn in time. Do not use it
as proof that `VirtualTunDevice::flush_tx` dropped a userspace-to-kernel write.

Keep Gate A strict. Diagnostic `capacity`, `clean`, and `gap` sub-results may be
reported, but all approved Gate A conditions remain an AND gate.

- [x] **Step 2: RED — reproduce global drop debt missing cross-sample drain**

Add one deterministic production-composition test with this sequence:

```text
two active D16 flows in Running
-> global drop edge at nonzero aggregate pressure
-> both flows enter DrainOnly and global drop debt is installed
-> aggregate pressure falls below low before another per-flow send-limit call
-> pressure-low edge carries positive observed drain
```

The assertion is behavioral: the episode must pay no more than the measured
global drain, must pay each byte once, must not mint admission credit, and must
make every non-terminal eligible D16 flow enter Recovery. The current code
must fail this test by leaving the flows in DrainOnly.

- [x] **Step 3: GREEN — close the global episode at one scope**

Introduce one small drop-episode policy object or equivalent pure seam owned by
the event loop. It records the pressure baseline and debt generation at the
drop edge, consumes the matching aggregate drain once at pressure-low, and
returns explicit recovery evidence. Per-flow code consumes that evidence but
does not independently pay global debt.

Correct ordinary transition evidence from `completed_drain_bytes.is_some()` to
strictly positive completed bytes. Keep zero-byte DrainOnly service cycles in
diagnostics without advancing Recovery.

- [x] **Step 4: RED — reproduce bounded TUN RX starvation at the production seam**

Build a deterministic `TunIo`/event-loop harness that models the Linux
kernel-to-userspace TUN ring as bounded. Sustained D16 downlink must cause the
local TCP side to enqueue ACK/control packets into that ring. Drive the same
actor, TUN read, `iface.poll`, writer, and close paths as production.

Required red observation:

```text
actor_bypass_admitted_bytes = 0
TUN RX ring reaches capacity or records a modeled drop
TUN RX drain reports repeated budget exhaustion/backlog
useful reverse progress or close cleanliness fails
```

Do not use an inflight-permit literal as the pressure source and do not add a
timer/self-wake to manufacture progress.

- [x] **Step 5: GREEN — add only the proven device-level self-resetting guard**

Only if Step 4 reproduces the production failure, expose a device-level TUN RX
backlog observation from the adapter seam. While backlog remains proven, stop
D16 Quinn reads and actor admission for all affected flows but continue TUN RX,
`iface.poll`, `flush_tx`, permit release, and close drain. A first clean
nonblocking `WouldBlock` only arms recovery; require one admission-free
`ControlOnly` poll/flush epoch and a later independent clean probe before
clearing the device guard.

At the pause edge, arm a per-flow ACK-completion barrier only when that flow's
smoltcp `send_queue` is nonzero. After device recovery, keep only that flow in
DrainOnly until its own `send_queue` reaches zero. Do not replace this with a
global all-flows-zero barrier. Bound Running and Recovery admission by an
MTU-derived 24-payload-packet sliding window after deducting existing
`send_queue`; this leaves room for up to two ACK/window-update packets per
payload packet plus the ordinary 16-packet TUN RX drain in the modeled
64-packet ring. Do not create another debt/credit clock and do not infer device
pressure from `flush_tx` writability.

- [x] **Step 6: Run local gates and architecture review**

Run the focused D16/drop/TUN-RX tests first, then:

```bash
cargo test -q --lib
cargo test -q --features harness
cargo check -q
cargo fmt --check
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
git diff --check
```

Review byte conservation, actor exclusivity, device-vs-flow ownership,
zero-byte progress, EOF/close ordering, TCP/UDP fairness, bounded queues, and
hot-path wake behavior. No VPS run is permitted from this task.

Local result (2026-07-10): the calibrated 64 MiB/64-packet production-seam
scenario first reproduced modeled drops and premature/unstable recovery. A
single clean edge and a fixed per-flush byte cap were necessary but not
sufficient: later admission could reopen before its own ACK feedback drained,
and prior `send_queue` bytes accumulated outside the nominal burst limit. The
GREEN path now combines an authoritative two-epoch device guard, per-flow
ACK-completion barriers, and the cumulative 24-packet sliding admission window.
The slow-flow isolation test proves one nonzero `send_queue` remains DrainOnly
while a zero-queue peer independently enters Recovery.

The final production-seam scenario proves complete 64 MiB delivery, real
pause/resume edges, at most 24 TCP payload packets per flush, zero actor bypass,
zero modeled drop, remote EOF after owned/pending/inflight bytes reach zero,
and zero terminal/close-tail bytes. It passed 50 consecutive full repeats.
Post-review gates passed: normal library `582/582`; harness library `589/589`;
integration `2/2`; harness targets `10 passed/4 ignored`; default and harness
checks; fmt; diff-check; US-client suite self-test; and low-RTT probe self-test.

Capacity follow-up (2026-07-10): review found that the prior 50/50 result was a
cleanliness proof, not a `170 Mbit/s` reachability proof. The release 64 MiB
production seam took `9.51s` (`56.4 Mbit/s`). Frozen-time RED showed one
coalesced readiness stopped after exactly one 24-packet admission, and the
full seam showed the 16-packet local drain misclassified every normal feedback
batch as backlog (`1915` pause/resume episodes), making the two-epoch guard a
5ms primary pacer.

Commit `f7847dd` adds ACK-driven bounded actor re-entry, rounds partial
unacknowledged segments to packet slots, and aligns local TUN RX service with
the 48-packet feedback allowance. The 64 MiB seam improved to about `2.40s` /
`224 Mbit/s`, with zero drop/bypass/tail and at most 24 payload packets per
flush. Fifty consecutive capacity-qualified repeats passed, followed by lib
`583/583`, harness `591/591`, integration `2/2`, harness targets `10 passed/4
ignored`, checks, fmt, clippy, diff-check, and both script self-tests.

- [x] **Step 7: Preserve commit and acceptance gates**

Before the first commit, list every pre-D16 diff that would be included and ask
the user to confirm the commit strategy. The user confirmed the cumulative D16
code-baseline commit followed by a separate docs/learnings commit; both staged
trees must be validated from a clean detached worktree. After local
green/review, request separate authorization for one strict replacement Gate A.
Gate B remains frozen until that Gate A passes.

Commit result (2026-07-10): the independently validated cumulative D16 code
baseline was committed as `8496b8f` and pushed to
`codex/knife14d-downlink-reap-open`. Unrelated Reality, DNS, failover, main,
metrics, script, and historical-result diffs remained unstaged. The next remote
step is still exactly one strict Gate A; no Gate B run is authorized by this
commit.

### Task 11B: Preserve Terminal Cause And Make Gate A Evidence Feasible

This task follows the alternate-Exit capacity run. It does not change the D16
reservoir, actor quantum, readiness model, DrainOnly recovery, or EOF ordering.
It corrects one lifecycle-observability defect and replaces an impossible
same-socket acceptance assertion with two traffic-shaped subproofs under one
AND gate.

**Files:**
- Modify: `src/tcp_downlink_pump.rs`, `src/client_tun.rs`
- Modify: `scripts/knife14b-lowrtt-probe.sh`,
  `scripts/knife14b-usclient-tunnel-suite.sh`
- Modify: this plan, the D16 architecture spec, `TODO.md`, `HANDOFF.md`
- Update: `.learnings/LEARNINGS.md`, `.learnings/ERRORS.md`

- [x] **Step 1: RED — preserve a local terminal cause across teardown**

Add focused tests where a local socket becomes terminal while D16 owns bytes.
Require the first terminal direction/reason to survive queue cleanup, reader
shutdown, writer-channel closure, and relay `Closed` publication. Require
one-shot ownership release and no later cause overwrite.

- [x] **Step 2: GREEN — model closure explicitly**

Replace the ambiguous leased-queue boolean with:

```text
Open | RemoteEof | Terminal(direction, reason)
```

Keep remote EOF drain semantics unchanged. Classify an inactive smoltcp
`Closed`/`!can_send` rearm as
`local_to_remote/local_socket_terminal`. Do not publish a payload readiness
event solely because a terminal close woke internal queue waiters.

- [x] **Step 3: Remove dead lifecycle plumbing**

Remove unused reaper backpressure/close-guard parameters and the duplicate
low-level terminal-drop call. Keep terminal ownership cleanup at one explicit
rearm boundary.

- [x] **Step 4: RED/GREEN — version terminal classification and fixed-byte TCP**

Extend the versioned lifecycle summary to count clean D16 closes,
`local_socket_terminal`, and every other terminal cause separately. Add an
optional `IPERF_BYTES` mode to the low-RTT runner and a
`RUN_D16_EOF_CLOSE_PROBE=1`, `D16_EOF_CLOSE_BYTES=64M` suite mode. The suite
runs the fixed-byte reverse probe only after the timed capacity flow becomes
quiet and emits an isolated final lifecycle window.

- [x] **Step 5: Local regression and review**

Focused RED/GREEN tests, default library `588/588`, harness library `597/597`,
integration `2/2`, harness targets `10 passed/4 ignored`, default/harness
checks, focused rustfmt, diff-check, and both runner self-tests passed. Strict
`clippy -D warnings` remains red on 13 pre-existing lint sites under the current
toolchain; no new warning points at the terminal-state change.

Review conclusion: D16 has not drifted from byte ownership or actor
exclusivity. Queue-cap, chunk, MTU, QUIC-window, self-wake, and actor cadence
changes are rejected. Commits `879e904` and `7a7ca04` remove dead plumbing and
preserve terminal cause respectively.

- [x] **Step 6: Run one composite Gate A on `.27`**

Use one clean committed build, exact safe1200 D16 profile, one capable Exit
window, and one tunnel process:

1. A-capacity: `20s`, reverse-first P1, receiver `>150 Mbit/s`, zero TUN
   drop/bypass/error, sub-second service gaps; only an exact bounded
   `local_socket_terminal` is allowed at the timed boundary.
2. Wait for the capacity flow to become quiet.
3. A-clean: fixed `64 MiB`, reverse P1 using `iperf3 -n 64M -P 1 -R`; require
   `clean_queue_lifecycle` and zero queue/pending/inflight/terminal-drop/
   close-egress bytes.

Gate A passes only if both subproofs pass. Any other terminal reason, mixed
lifecycle window, TUN drop, or incomplete fixed-byte flow stops before Gate B.

Result (2026-07-11): failed before Gate B. The mature control qualified the
window at `157.650 Mbit/s` receiver, but pool-2 mini_vpn reached only
`0.0265 Mbit/s` in A-capacity and A-clean received about `3.68 MiB` before
timeout. D16 ownership, actor, pressure, TUN drop, and QUIC loss/blocking
surfaces stayed clean. A pool-1 discriminator reached `115 Mbit/s`, selecting
the TCP pool lifecycle seam rather than D16 egress. See Task 11C.

### Task 11C: Replace Destructive Idle Reconnect With Evidence-Based Pool Health

This is a transport-pool lifecycle correction, not a D16 egress redesign.
Pool 1 is diagnostic only and must not become the product default to hide the
auxiliary-slot failure.

**Files:**
- Modify: `src/tuic.rs`
- Modify diagnostics/parsers only as required to prove connection generation
- Add focused tests in the existing TUIC test module
- Update current result docs and learning memory

- [x] **Step 1: RED — healthy auxiliary idle is not stale**

Add a pure policy test that distinguishes explicit closed/unhealthy state from
elapsed idle age. A healthy authenticated auxiliary slot with no close reason
must remain reusable after `10s`; active-stream exclusion remains mandatory.

- [x] **Step 2: GREEN — reconnect only from observable health evidence**

Remove time alone as the reconnect cause. Reconnect a slot when Quinn reports
it closed or a bounded open/transport failure proves it unusable. Preserve the
per-slot mutex, lease accounting, startup degradation, and primary UDP/health
semantics.

- [x] **Step 3: Add bounded connection-generation evidence**

Report slot index, stable connection id/generation, reconnect reason, last-use
age, and active lease count at open/reconnect. Do not log payload or auth data.

- [x] **Step 4: Local TDD and review**

Run focused pool tests, all TUIC/D16 libraries and harnesses, checks, focused
formatting, runner self-tests, and code review. Verify no TCP pool change can
route UDP away from primary or weaken actor/ownership/EOF invariants.

- [x] **Step 5: One scoped pool-2 auxiliary A/B**

After a mature control exceeds `150 Mbit/s`, run one clean pool-2 reverse P1
whose data stream is proven to use an auxiliary slot without destructive idle
reconnect. This is a discriminator, not Gate B. Require receiver `>150 Mbit/s`
and zero D16/TUN/QUIC error surfaces before authorizing a new composite Gate A.

Result (2026-07-11): correctness implementation and local gates passed at
`6209910`. A capable temporary Exit carried the mature control at
`195.033 Mbit/s` receiver. The clean pool-2 mini_vpn flow used auxiliary
`conn=1`, generation `1`, without probe or reconnect, but reached only
`108 Mbit/s`. D16/TUN/QUIC error surfaces stayed zero and the middle service
window sustained about `188-190 Mbit/s`; multi-second starvation and tail
collapse remained upstream of the actor. The A/B failed its throughput
criterion and does not authorize Gate A. Next isolate auxiliary TUIC stream
service/frontier progress; do not retune D16. Result:
`2026-07-11-knife14h10d16-pool-health-probe-results.md`.

### Task 12: Prove 170M Parity, Regress Product Paths, And Clean Experiments

**Files:**
- Add: dated Gate B and regression result documents under `docs/tech/`
- Modify: old feature-gate code only after Gate B evidence
- Update: `TODO.md`, `HANDOFF.md`, `.learnings/LEARNINGS.md`, `.learnings/ERRORS.md` as applicable

- [ ] **Step 1: Run one same-window sing-box control**

Use the already established mature-client procedure. Do not change VPS, MTU,
iperf3, or broad QUIC settings between control and mini_vpn runs.

- [ ] **Step 2: Run three focused mini_vpn parity repeats**

Use the same D16 Gate A profile and `20s` reverse-first P1 shape. Stop if a run
produces TUN drops, an unclassified terminal cause, or non-exact terminal
accounting. A timed `local_socket_terminal` at the generator boundary is
reported separately from the fixed-byte clean-close proof.

- [ ] **Step 3: Apply Gate B**

Pass when every mini_vpn run exceeds `150 Mbit/s`, median is at least
`170 Mbit/s`, all terminal causes are classified/exact, and one fixed-byte
clean-close repeat on the same build has a zero tail. If the same-window control is below
`170 Mbit/s`, accept parity only when mini_vpn median is at least `90%` of the
control and every run still exceeds `150 Mbit/s`.

- [ ] **Step 4: Run product regressions**

Run:

- a sustained `60s` reverse TCP flow;
- TCP multi-flow/concurrency harness and available VPS concurrency gate;
- UDP/live-streaming suite;
- fake-IP DNS tests;
- TUN create/start/stop/rearm lifecycle tests.

Require bounded per-flow/global ownership, no event-loop stall, zero unexpected
relay reap, and no TCP change that routes UDP through the TCP actor.

- [ ] **Step 5: Decide old path cleanup**

- Revert H4 if it has no remaining diagnostic value, or keep it explicitly
  diagnostic and default-off.
- Remove D15 as a standalone product candidate; its useful service-quantum
  behavior belongs inside D16 reservations.
- Retain D3/D6 diagnostics only if they distinguish a future failure; otherwise
  remove them after the D16 default decision.
- Keep one rollback feature gate until sustained and concurrency acceptance is
  complete.

- [ ] **Step 6: Final code review, docs, and coherent commits**

Run all local gates again, update current status in `TODO.md` and `HANDOFF.md`,
write final learnings, and commit cleanup separately from result documentation.
Do not claim stable `170 Mbit/s` until Gate B and the product regression gate
both pass.

## Plan Self-Review

- Spec coverage: byte reservation, hidden queue removal, actor exclusivity,
  DrainOnly, recovery, EOF ordering, observability, global memory bound, VPS
  parity, product regression, and cleanup all map to explicit tasks.
- Type consistency: `ByteQueueReadReservation`, `LeasedQueuePoll`,
  `SpawnedRemoteRelay`, `DownlinkAdmissionOrigin`, `EgressPhase`, and
  `EgressPermissions` retain the same names throughout the plan.
- Scope: only the TCP reverse data path, shared TUN actor seam, harness, suite,
  and current project memory are in scope. UDP and control-plane redesign are
  excluded.
- No VPS run occurs before the deterministic ownership and lifecycle gates.
