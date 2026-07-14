# Knife14h10 TCP/TUN Egress Actor Spec

Date: 2026-07-09

## Scope

Knife14h10 replaces the local TCP/TUN downlink egress contract, not the TUIC
transport. D2.4 proved the same `.27 -> .33 -> .77` mini_vpn TUIC TCP stream can
read `477101276` bytes in `30s` (`127.227 Mbit/s`) when bytes are drained
directly to a sink. The remaining throughput bottleneck is therefore in the
full VPN local path:

```text
TUIC read task
  -> RelayEvent::Data / global_rx
  -> SocketCtx.downlink_pending
  -> smoltcp TcpSocket::send_slice
  -> iface.poll
  -> TunIo::flush_tx
  -> kernel TUN
```

The h10 goal is to make local egress progress a first-class actor contract with
single ownership, bounded byte capacity, explicit lease release, and deterministic
progress tests.

## Non-goals

- Do not change VPS sysctl, MTU/PLPMTUD, sing-box, iperf3, stale pool handling,
  or broad QUIC windows.
- Do not promote H4 ordered chunk or D2.3 permit pump as accepted fixes.
- Do not let multiple async tasks mutate `smoltcp::Interface`, `SocketSet`, or
  the TUN device. Rust ownership and smoltcp make these single-owner resources.
- Do not remove lifecycle/close-tail guards until their invariants are covered
  by the new actor tests.

## Capacity Math

- `100 Mbit/s = 12.5 MB/s`.
- A `10ms` egress service window must be able to admit/drain about `125 KB`.
- A `5ms` window must be able to admit/drain about `62.5 KB`.
- D2.4 raw sink sustained about `15.9 MB/s`, so the local actor must preserve at
  least that cadence through smoltcp/TUN.
- Existing `TCP_SOCKET_BUFFER_SIZE` is about `64 KB`; therefore the actor must
  respond to ACK/TUN drain repeatedly within a service window. One successful
  `send_slice` is not enough for `100+ Mbit/s`.

## Old-path Inventory

The current path spreads one logical operation across several call sites:

- `handle_remote_payload`: appends bytes to `SocketCtx.downlink_pending`, calls
  `flush_downlink`, maybe calls `iface.poll` and `flush_tx`.
- `flush_downlink`: mixes byte admission, headroom/debt policy, close-drain
  expansion, permit release, and diagnostic counters.
- `process_listener_activity`: calls `flush_downlink` again during dirty relay
  processing, then handles uplink and lifecycle.
- `service_local_egress_until`: drains TUN RX, polls smoltcp, flushes TUN TX,
  then calls `process_dirty_relay`.
- `drain_ready_tun_rx`: can indirectly process TCP ACK/TUN RX and cause egress
  progress before the old local-egress snapshot begins.

This is correct enough for functionality, but it makes throughput depend on
which event happens to wake the loop. That is the active architecture gap.

## New Actor Contract

### Ownership

The production actor remains inside the current main loop task because
`Interface`, `SocketSet`, and `TunIo` are not safe to mutate from multiple
independent async tasks. Remote TUIC reads may stay in separate tasks, but local
egress is single-owner.

### Byte states

Each downlink byte is in exactly one of these states:

```text
remote queue lease
  -> local pending
  -> smoltcp send queue
  -> TUN TX / kernel
  -> released
```

Lease capacity is released only when bytes are accepted by local egress or
explicitly dropped during a terminal/lifecycle path. Dispatcher pop alone must
not release downstream capacity.

### Service cycle

One egress service window is:

```text
while cycles < max_cycles and admitted < target_bytes:
    drain TUN RX / ACK readiness
    iface.poll
    flush TUN TX
    admit pending downlink bytes into smoltcp
    if progress: continue
    else: arm external readiness and stop
```

If backlog remains and progress was made, the actor requests an immediate wake.
If backlog remains but no progress was possible, the actor waits for external
readiness such as TUN RX/ACK/socket capacity. This avoids both timer-only idle
and busy spinning.

## Local Test Seam

`src/tcp_egress.rs` introduces a pure `TcpEgressFlowModel` to lock the actor
contract before touching `client_tun.rs`.

Covered invariants:

- backlog is admitted repeatedly in one service window until the byte target is
  reached;
- socket/TUN drain opens capacity and admission happens in the same service
  window;
- no-progress with backlog preserves pending/lease bytes and waits for external
  readiness;
- explicit drop releases remaining pending lease;
- invalid zero budgets are rejected before service starts.

This is not the production actor yet. It is the executable specification for the
next branch-by-abstraction step.

## Implementation Stages

### H10a: Spec and pure contract

- Add this spec.
- Add `tcp_egress.rs` pure model and unit tests.
- No production hot-path behavior change.

Acceptance:

```text
cargo test -q tcp_egress --lib
cargo fmt --check
cargo check -q
```

### H10b: Extract production-facing types

- Add production DTOs around `SocketCtx.downlink_pending`,
  downstream permits, socket send-window snapshots, and egress outcome.
- Keep old `flush_downlink` behavior, but call through a small facade whose
  contract matches `TcpEgressFlowModel`.

Acceptance:

- existing `client_tun` tests pass;
- new tests prove lease release on admit/drop and no release on no-progress;
- default runtime behavior unchanged.

H10b local result:

- added `TcpEgressLease`, `TcpEgressPendingQueueView`,
  `TcpEgressPendingSnapshot`, `TcpEgressPendingAppend`, and
  `TcpEgressPendingConsume` in `src/tcp_egress.rs`;
- implemented `TcpEgressLease` for `DownstreamBytePermit`;
- routed `SocketCtx` pending append, prefix consume, and clear through the
  facade while keeping callers and default hot-path control flow unchanged;
- added facade tests for retain-on-append, release-on-admit, no-release-on-
  no-progress, and release-on-clear/drop.

H10b is still not the actor switch. It is a branch-by-abstraction extraction
that makes the old pending/permit representation obey the new byte ownership
contract through one facade.

### H10c: Feature-gated actor

- Add `MINI_VPN_D3_EGRESS_ACTOR=1`.
- In actor mode, remote payload install only appends bytes and schedules egress;
  local egress actor owns repeated drain/poll/flush/admit cycles.
- Old path remains default.

Acceptance:

- local harness proves a backlog triggers immediate service without waiting for
  a timer or another remote read;
- close-tail tests remain green;
- metrics include `egress_actor_cycles`, `egress_actor_admitted_bytes`,
  `egress_actor_no_progress`, `egress_actor_immediate_wake`, and
  `egress_actor_external_wait`.

H10c local result:

- added opt-in runtime gate `MINI_VPN_D3_EGRESS_ACTOR=1`; default remains
  disabled;
- in actor mode, `handle_remote_payload` appends payload bytes and any
  downstream permit into `SocketCtx.downlink_pending`, records remote progress,
  and returns without inline `flush_downlink`;
- the production call site still marks pending flows dirty and immediately
  enters `service_local_egress_until` when `downlink_pending` is non-empty, so
  the smoltcp/TUN owner remains the single local egress writer;
- existing `tcp-local-egress-service` diagnostics now include actor-subset
  fields: `egress_actor_windows`, `egress_actor_cycles`,
  `egress_actor_admitted_bytes`, `egress_actor_drain_bytes`,
  `egress_actor_no_progress`, `egress_actor_immediate_wake`, and
  `egress_actor_external_wait`;
- local tests prove the gate is opt-in, actor payload install does not perform
  inline flush/send/TUN flush calls, pending bytes still mark local egress work
  despite `accepted_bytes=0`, and actor diagnostic counters are emitted.

H10c is a local architecture gate, not throughput acceptance. It proves the
old remote-payload inline writer can be bypassed behind a feature flag while
leaving default behavior unchanged and keeping egress ownership in the main
smoltcp/TUN loop.

### H10d: VPS first gate

Run one focused reverse-first P1 with actor mode.

Acceptance:

- receiver clears `>30 Mbit/s`;
- TUN drops stay `0`;
- close-tail pending stays `0`;
- active data gaps and egress no-progress gaps do not show multi-second idle;
- if this fails, compare actor metrics to D2.4 raw sink before editing.

H10d VPS result:

- remote bundle:
  `/tmp/conn/mvpn_knife14h10c_d3_actor_p1_usclient_suite_20260709_215654.tar.gz`
- local copy:
  `/tmp/mini_vpn_knife14h10c_d3_actor/mvpn_knife14h10c_d3_actor_p1_usclient_suite_20260709_215654.tar.gz`
- runtime gate was active:
  `MINI_VPN_D3_EGRESS_ACTOR=1` and startup logged
  `D3 TCP/TUN egress actor: enabled`;
- reverse-first P1 produced high-throughput evidence:
  `iperf_receiver_mbps=102.000`, interval profile
  `overall_avg_mbps=170.172`, `prefix_avg_mbps=189.130`;
- actor path was active and carried the data:
  `egress_actor_admitted_bytes=620309763`,
  `egress_actor_drain_bytes=139651239`,
  `egress_actor_immediate_wake=31938`;
- QUIC/path stayed non-root:
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`, and no
  tx/rx blocked deltas;
- H10d is not final accepted yet: the probe exited by iperf timeout
  (`exit=124`), `tun_tx_dropped_delta=97`, `pending_at_close=421577`, and
  `terminal_pending_reap=421577`;
- remaining discriminators: `data_read_gap_max_ms=3402` and
  `data_pending_gap_max_ms=3402` persist, now in a high-throughput rather than
  `15-25M` regime.

Interpretation: H10c/D3 has enough architectural capacity to exceed `100M` on
the `.27 -> .33 -> .77` reverse path, but the accepted fix still needs a
close-tail / TUN egress drop cleanup pass before H10e repeat acceptance.

### H10d2: actor clean-headroom local pacing

H10d showed that the D3 actor architecture has `100M+` capacity, but it also
showed a local TUN egress drop at the same pressure region where the legacy
downlink flush path can spend observed drain credit up to the credit edge.
That old credit behavior was useful when remote payload handling performed
inline flushes, but in actor mode it undermines the "single smoltcp/TUN owner"
contract by allowing a service cycle to refill beyond clean headroom after a
drain observation.

Design:

- keep legacy flush-credit behavior unchanged for the default path;
- mark D3 actor flows as `clean_headroom_only` when remote payload bytes are
  appended into `downlink_pending`;
- in actor mode, `flush_downlink` may only admit bytes up to
  `tx_queue_flush_threshold`, even if `DownlinkEgressClock` has drain credit;
- keep hard drop-debt behavior unchanged;
- do not use close-drain terminal expansion in actor clean-headroom mode;
- clear the actor pacing flag on `rearm_socket`, so the policy cannot leak into
  the next flow.

Local acceptance:

- the legacy policy still spends clean drain credit up to
  `tx_queue_credit_spend_threshold`;
- the actor clean-headroom policy leaves bytes pending instead of spending
  drain credit past `tx_queue_flush_threshold`;
- D3 actor remote payload install still avoids inline `send_slice`;
- rearm resets the actor pacing flag;
- `cargo test -q d3_actor_clean_headroom_policy_does_not_spend_drain_credit_to_credit_edge --lib`;
- `cargo test -q d3_egress_actor --lib`;
- `cargo test -q tcp_egress --lib`;
- `cargo test -q --lib`;
- `cargo check -q`;
- `cargo fmt --check`;
- `git diff --check`.

VPS acceptance prediction:

- `MINI_VPN_D3_EGRESS_ACTOR=1` remains required;
- reverse-first P1 should retain the H10d `100M+` capacity signal;
- `tun_tx_dropped_delta` should fall to `0`;
- `pending_at_close` and `terminal_pending_reap` should fall to `0`;
- if throughput stays high but drops persist, the clean-headroom hypothesis is
  insufficient and the next suspect is TUN drain cadence / qdisc feedback;
- if drops disappear but throughput regresses below `100M`, the actor needs a
  larger owner-cycle drain cadence, not a return to remote-payload inline
  writes.

H10d2 VPS result:

- remote bundle:
  `/tmp/conn/mvpn_knife14h10d2_actor_clean_headroom_usclient_suite_20260709_221940.tar.gz`
- local copy:
  `/tmp/mini_vpn_knife14h10d2_actor_clean_headroom/mvpn_knife14h10d2_actor_clean_headroom_usclient_suite_20260709_221940.tar.gz`
- runtime gate was active:
  `MINI_VPN_D3_EGRESS_ACTOR=1` and startup logged
  `D3 TCP/TUN egress actor: enabled`;
- reverse-first P1 regressed to
  `sender=11.6 Mbit/s`, `receiver=9.72 Mbit/s`;
- the clean-tail half succeeded:
  `tun_tx_dropped_delta=0`, `pending_at_close=0`, and
  `terminal_pending_reap=0`;
- the throughput half failed:
  `drain_credit_planned_bytes=0`, `send_queue_max=449999`, and
  `headroom_limited_calls=13881`, while H10d had reached
  `send_queue_max=557386` and `drain_credit_used_bytes=1683528`;
- actor throughput evidence collapsed from H10d
  `egress_actor_admitted_bytes=620309763` and
  `egress_actor_drain_bytes=139651239` to H10d2
  `egress_actor_admitted_bytes=34826203` and
  `egress_actor_drain_bytes=9352989`;
- remote-read cadence stayed bursty:
  `max_read_gap_ms=3707`, `max_pending_gap_ms=3706`;
- QUIC/path remained non-root:
  no loss/congestion/blocking deltas were implicated.

Interpretation: pure clean-headroom actor pacing is not sufficient and should
not be accepted as the H10 fix. H10d2 falsifies the "never spend drain credit
in actor mode" hypothesis. The next design must keep the H10d high-throughput
elasticity while making credit drop-aware, probably by spending a bounded
per-cycle credit budget above clean headroom only when recent TUN drain was
observed and immediately backing off on any qdisc drop or hard pause.

### H10d3: actor hybrid target-edge credit

H10d2 split the problem cleanly: pure clean-headroom actor pacing removed
qdisc drops and terminal close-tail, but it also capped the local write path so
hard that reverse throughput fell below the old `15-25M` band. H10d3 therefore
keeps the D3 single local egress owner, but restores bounded elastic credit
above clean headroom.

Design:

- legacy/default path remains unchanged and can still spend credit up to
  `tx_queue_credit_spend_threshold`;
- D3 actor flows use `ActorHybridCredit` instead of legacy credit or pure
  clean-headroom;
- actor hybrid spends recent `DownlinkEgressClock` drain credit only up to
  `tx_queue_egress_target_threshold`;
- bytes above the target edge remain pending for a later owner cycle;
- active TUN drop debt blocks actor drain-credit planning and forces the actor
  back to clean-headroom admission;
- close-drain terminal expansion remains legacy-only, so actor mode cannot use
  the old close-tail expansion path.

Local acceptance:

- `cargo test -q d3_actor_hybrid_policy --lib` proves actor hybrid spends
  recent drain credit to the target edge and blocks it under active drop debt;
- `cargo test -q d3_egress_actor --lib` proves actor mode still avoids inline
  remote-payload writes and marks actor-owned egress;
- `cargo test -q --lib` passed with `474` tests;
- `cargo check -q`, `cargo fmt --check`, and `git diff --check` passed.

VPS falsifier:

- if reverse-first P1 remains below `30M`, target-edge credit is still too
  conservative or owner-cycle drain cadence is insufficient;
- if reverse-first P1 clears `100M` but `tun_tx_dropped_delta`,
  `pending_at_close`, or `terminal_pending_reap` reappears, target-edge credit
  is still too aggressive or the drop-debt feedback is too late;
- if it clears `100M` with drop/tail `0`, run one repeat before considering
  H10e final acceptance.

H10d3 VPS result:

- remote bundle:
  `/tmp/conn/mvpn_knife14h10d3_actor_hybrid_target_usclient_suite_20260709_224720.tar.gz`
- local copy:
  `/tmp/mini_vpn_knife14h10d3_actor_hybrid_target/mvpn_knife14h10d3_actor_hybrid_target_usclient_suite_20260709_224720.tar.gz`
- runtime gate was active:
  `MINI_VPN_D3_EGRESS_ACTOR=1`;
- reverse-first P1 improved over H10d2 but remained below the `100M` gate:
  `sender=32.2 Mbit/s`, `receiver=29.8 Mbit/s`;
- the clean-tail half stayed fixed:
  `tun_tx_dropped_delta=0`, `pending_at_close=0`, and
  `terminal_pending_reap=0`;
- the capacity half remained insufficient:
  `send_queue_max=503692`, `hard_edge_guard_limited=18275`,
  `headroom_limited=18275`, `pending_total_max=454619`,
  `drain_credit_planned_bytes=391013`, and
  `pressure_credit_debt_bytes=122727`;
- actor carried more data than H10d2 but far less than H10d:
  `egress_actor_admitted_bytes=106029558` and
  `egress_actor_drain_bytes=29635934`;
- QUIC/path remained non-root:
  no loss/congestion/blocking deltas were implicated.

Interpretation: target-edge actor hybrid credit is safe but still too
conservative. H10d3 falsifies "target edge is enough" while preserving the
value of drop-aware actor credit. The next design should keep D3 actor
ownership, keep active drop debt as a clean-headroom backoff, keep close-drain
expansion disabled in actor mode, but allow a higher adaptive elastic edge
between `tx_queue_egress_target_threshold` and
`tx_queue_credit_spend_threshold`.

### H10d4: actor adaptive credit edge

H10d4 tests the narrowest remaining actor-credit hypothesis after H10d3:
perhaps the missing capacity is the elastic window between
`tx_queue_egress_target_threshold` and `tx_queue_credit_spend_threshold`.

Design:

- default/non-actor path remains `LegacyCredit`;
- D3 actor flows use `ActorAdaptiveCredit`;
- with no active drop debt, actor drain credit may refill up to
  `tx_queue_credit_spend_threshold`;
- active drop debt still clears/blocks actor drain credit and forces admission
  back to clean headroom;
- actor mode still cannot use legacy close-drain expansion.

Local acceptance:

- `cargo test -q d3_actor_adaptive_policy --lib` proves actor adaptive credit
  reaches the credit edge, blocks stale credit under active drop debt, and
  keeps close-drain expansion disabled;
- `cargo test -q --lib` passed with `477` tests;
- `cargo check -q`, `cargo fmt --check`, and `git diff --check` passed.

H10d4 VPS result:

- remote bundle:
  `/tmp/conn/mvpn_knife14h10d4_actor_adaptive_credit_usclient_suite_20260709_231822.tar.gz`
- local copy:
  `/tmp/mini_vpn_knife14h10d4_actor_adaptive_credit/mvpn_knife14h10d4_actor_adaptive_credit_usclient_suite_20260709_231822.tar.gz`
- runtime gate was active:
  `MINI_VPN_D3_EGRESS_ACTOR=1`;
- reverse-first P1 failed Gate A:
  `sender=22.5 Mbit/s`, `receiver=20.6 Mbit/s`;
- clean-tail/drop safety remained good:
  `tun_tx_dropped_delta=0`, `pending_at_close=0`, and
  `terminal_pending_reap=0`;
- local pressure was not the active limiter in this run:
  `send_queue_max=390896`, below `tx_queue_flush_high=449999`;
  `headroom_limited=0`, `pressure_credit_debt_bytes=0`, and
  `hard_edge_guard_limited=0`;
- actor path was active but carried too little data:
  `egress_actor_admitted_bytes=73959831` and
  `egress_actor_drain_bytes=17743007`;
- TUIC read/pending/poll gaps remained multi-second:
  `data_read_gap_max_ms=3849`, `data_pending_gap_max_ms=3848`, and
  `data_poll_gap_max_ms=3394`;
- QUIC/path remained non-root:
  no loss/congestion/blocking deltas were implicated.

Interpretation: H10d4 falsifies "restoring the target-to-credit elastic edge
is sufficient." The run did not even reach the clean flush edge, so the active
bottleneck was not the actor credit limit. The next design should stop
adjusting actor credit thresholds and instead discriminate why the full VPN
path still has multi-second TUIC data read/pending/poll gaps while local
pressure, TUN drops, close-tail, global_rx pressure, and QUIC path counters are
clean.

### H10e: VPS 100M gate

Only after H10d passes.

Acceptance:

- reverse-first receiver `>100 Mbit/s` on at least two focused runs;
- actor backlog remains bounded;
- local egress gaps stay sub-second during active transfer;
- QUIC loss/congestion/blocking counters remain non-root.

## Failure Discriminators

- Raw TUIC read below `100M`: contradicts D2.4; rerun sink probe before blaming
  local egress.
- Actor admitted bytes low while pending high: smoltcp admission/dirty scheduling
  bottleneck.
- Actor drain bytes low while socket queue high: ACK/TUN RX or `iface.poll`/TUN
  flush bottleneck.
- Actor no-progress high with no pending: lifecycle/close accounting issue.
- Actor queue/lease high with local pressure clean: byte ownership or wake
  scheduling issue.

## Design Score

Current production egress architecture: `6/10`.

- Strengths: lifecycle guards, bounded queues, diagnostics, single smoltcp owner.
- Weaknesses: egress progress is distributed across event side effects, byte
  ownership is not represented as one contract, and acceptance depends on VPS
  timing too early.

Target architecture after H10e: `9/10`.

- Remaining point is reserved for long-duration, high-concurrency, UDP/TCP mixed
  release validation after `100+ Mbit/s` is stable.
