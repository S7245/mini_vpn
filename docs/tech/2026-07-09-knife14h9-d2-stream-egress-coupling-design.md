# Knife14h9 / D2 Stream Readiness and Local Egress Coupling Design

Date: 2026-07-09

## Stage

This is a design-only stage after H8. It does not change product code.

The next implementation target is not another chunk-size, self-wake, VPS,
MTU/PLPMTUD, stale-pool, iperf3, broad QUIC-window, or relay-read-credit tweak.
H8 falsified the narrow hypothesis that normal `RelayReadCredit` clocking is
the main remaining root.

D2 targets the remaining code-level coupling:

```text
tuic.rs ordered stream readiness / pending / poll cadence
  x
client_tun.rs local downlink backpressure / flush_downlink / pressure_credit_debt
```

Current design score: `8/10`.

To reach `10/10`, D2 must first add a deterministic local timeline/harness and
decide the H4 ordered-chunk state before any canonical VPS acceptance.

## Evidence Base

Already excluded for this branch:

- VPS capacity: mature sing-box client reached `173/173 Mbit/s` in the current
  `.27/.33/.77` environment.
- Exit socket buffers: `.33` high socket-buffer settings are persisted.
- iperf3 and direct path: direct baselines stayed healthy in recent runs.
- MTU/PLPMTUD and broad QUIC windows: recent failures had clean QUIC
  loss/congestion/blocking counters.
- stale TUIC pool slots: the stale-slot branch is closed.
- local TUN drops and close-tail in the failing H4/H8 windows: drops and
  terminal pending were clean.
- normal `RelayReadCredit` as remote-read clock: H8 enabled the continuous pump,
  showed `continuous_queue_wait_events=0`, and still had multi-second active
  stream gaps.

Key remaining facts:

- H4 `ordered_chunk` was active and real, but only reached `17.1/15.7 Mbit/s`.
- H8 `continuous_pump` was active and only reached `30.7/29.6 Mbit/s`.
- H8 still reported:
  - `data_read_gap_max_ms=3823`
  - `data_pending_gap_max_ms=3408`
  - `data_poll_gap_max_ms=3406`
  - `continuous_queue_wait_events=0`
  - `pressure_credit_debt_bytes=122727`
  - `headroom_limited=6`
  - `tun_flush_deferred=3`
- Therefore the byte queue between reader and dispatcher was not the observed
  pressure edge, but local egress feedback still changed state while ordered
  stream polling/readiness went idle for seconds.

## Capacity Math

The product target remains `100+ Mbit/s`.

```text
100 Mbit/s = 12.5 MB/s
30 Mbit/s  = 3.75 MB/s

64 KiB reads needed for 100 Mbit/s:
  12,500,000 / 65,536 = ~191 reads/s = one read every ~5.2 ms

64 KiB reads needed for 30 Mbit/s:
  3,750,000 / 65,536 = ~58 reads/s = one read every ~17.3 ms
```

Any active-flow `poll/read/pending` gap above `500ms` is already far too large
for a stable `100+ Mbit/s` design. A `3.4s` to `3.8s` gap loses the opportunity
to move tens of megabytes at the target rate.

D2 local gates should therefore require sub-`50ms` cadence under synthetic
pressure. VPS first gate should require sub-`500ms` active-window gaps before
claiming progress toward throughput.

## End-to-End Hot Path Inventory

Current reverse/downlink path under H8:

```text
TuicUpstream::open_tcp
  -> TrackedRelayStream<TuicOrderedRelayStream>::poll_read
  -> run_relay_continuous_reader
  -> AsyncByteBoundedFlowQueue
  -> run_continuous_relay_dispatcher
  -> send_remote_payload_batch_to_main
  -> global_rx.recv in client_tun main loop
  -> handle_remote_payload
  -> SocketCtx.downlink_pending
  -> flush_downlink
  -> TcpSocket::send_slice
  -> iface.poll
  -> device.flush_tx
  -> TUN egress
```

Continuous progress is currently provided by several independent clocks:

- QUIC stream wakeups through `TrackedRelayStream::poll_read`.
- relay reader task polling and H8 queue capacity waits.
- dispatcher `global_rx` reservation.
- main-loop `global_rx.recv`.
- `handle_remote_payload` and `flush_downlink`.
- dirty-handle passes through `process_listener_activity`.
- local egress service and deferred ACK/TUN RX drain timers.
- periodic timer paths.

D2 must prove whether these clocks form one service contract or whether local
egress pressure can leave the remote stream read future effectively idle.

## Hypotheses

### H1: Local egress feedback indirectly starves ordered stream polling

If `downlink_backpressure`, `flush_downlink`, `pressure_credit_debt`,
headroom-limited flushes, or deferred TUN flushes are causing the bad cadence,
then large `poll/read/pending` gaps should align in time with those transitions.

Predictions:

- a local timeline will show backpressure/debt/headroom/flush-deferred events
  immediately before or during active data stream `poll/read` gaps;
- a deterministic harness with synthetic local pressure will reproduce delayed
  remote polling in the current task model;
- fixing the coupling should reduce active data gaps before it has to prove
  `100+ Mbit/s`.

### H2: Ordered QUIC stream readiness is independent of local egress pressure

If Quinn ordered-stream readiness or the current adapter/waker usage is the
root, then `poll/read/pending` gaps will persist without matching local
pressure transitions.

Predictions:

- raw `tuic-tcp-stream-pending` lines will show fresh/stale stream-frame
  pending or no-stream-frame receive progress while local pressure remains
  clean;
- a local pressure harness will not reproduce the gap;
- D2 should move to a dedicated ordered-stream readiness seam, not deeper
  `flush_downlink` tuning.

### H3: H4 ordered-chunk state contaminates the baseline

H4 is a real seam but not an accepted fix. If it remains always-on, future
results cannot distinguish default ordered join behavior from the experimental
`read_chunk(max, true)` adapter.

Prediction:

- canonical VPS results will be ambiguous unless D2 first reverts H4 or gates
  it behind an explicit diagnostic flag and records `relay_mode`.

### H4: H8 queue capacity ends too early

H8 releases byte capacity when the dispatcher pops bytes into `RelayEvent::Data`,
not when smoltcp accepts them or TUN egress drains them. If that is the missing
backpressure boundary, then `continuous_queue_wait_events=0` can coexist with
downstream pressure/debt.

Prediction:

- D2 timeline can show reader/dispatcher queue capacity is available while
  `downlink_pending`, send queue, or pressure debt still constrain local
  admission;
- a later architecture may need permit lifetime through `downlink_pending` and
  `flush_downlink`, but only after the timeline proves this edge is causal.

## Old-Path Audit

The following old mechanisms remain active and prevent H8 from being called a
full data-plane architecture replacement:

- `handle_remote_payload` still extends `SocketCtx.downlink_pending` and calls
  `flush_downlink` immediately.
- `flush_downlink` still computes a bounded flush limit from send window,
  egress clock, drop debt, and local backpressure thresholds.
- `publish_relay_read_credit_for_handle` still publishes local pressure credit
  for legacy/thin paths, and the continuous reader still receives credit
  updates for diagnostics/lifecycle even though it ignores pauses for reads.
- `downlink_egress_drop_debt` and pressure-credit debt still affect local
  admission and hard-pause decisions.
- `DownlinkEgressPacer` can defer immediate TUN flushes after accepted bytes.
- H8 queue permits are not tied to smoltcp/TUN acceptance.
- H4 ordered-chunk adapter is still experimental and must not be treated as an
  accepted default without an explicit D2 decision.

## Difference Table

| Difference | Status | Can explain `15-30M` vs `173M`? | D2 action |
| --- | --- | --- | --- |
| VPS socket buffers | Excluded | No | Do not revisit unless evidence changes |
| iperf3/direct path | Excluded | No | Do not revisit |
| MTU/PLPMTUD/broad QUIC windows | Excluded for this branch | No current evidence | Do not tune |
| Stale TUIC pool slots | Excluded | No | Do not revisit |
| sing-box mature client has a continuous copy contract | Relevant | Yes, as architectural contrast | Preserve as target shape |
| mini_vpn H8 reader is continuous only until dispatcher queue pop | Relevant | Possibly | Test permit lifetime vs local admission |
| mini_vpn local egress credit/debt/headroom path remains coupled to admission | Relevant | Yes | Build timeline and harness |
| H4 ordered chunk adapter | Real but insufficient | Not alone | Revert or feature-gate before canonical VPS |
| Self-wake timer cadence | Necessary diagnostic only | Not alone | Do not tune standalone |
| Formatting/logging style differences | Style | No | Ignore |

## Executable Stage List

### D2.0 - Baseline and H4 Decision [critical]

Goal: make future D2 results interpretable.

Required decision before canonical VPS:

- either revert H4 to the accepted ordered default;
- or gate H4 behind an explicit diagnostic flag such as an ordered-chunk mode
  switch, default off, with `relay_mode` proving the selected path.

Acceptance:

- local test proves the default path and diagnostic path selection;
- result reports identify whether the run used default ordered join or
  ordered chunk;
- no throughput claim is made from a mixed/dirty H4 state.

Failure interpretation:

- if H4 remains always-on, D2 can still run local analysis, but VPS output is
  diagnostic only and not a canonical mini_vpn baseline.

### D2.1 - Unified Per-Flow Timeline [critical]

Goal: correlate stream readiness with local egress pressure in the same flow.

Required event families:

- TUIC stream `poll_read` Pending/Ready, read bytes, pending cause, and
  connection transport deltas;
- relay reader read start/end and H8 queue wait/capacity state;
- dispatcher `RelayEvent::Data` enqueue and `global_rx` wait;
- main-loop receive of `RelayEvent::Data`;
- `handle_remote_payload` start/end and accepted bytes;
- `flush_downlink` limit, accepted bytes, headroom-limited flag, and pressure
  debt/debt-paid result;
- `downlink_backpressure` pause/resume edges;
- `tun_flush_deferred`, `flush_tx`, and local egress service outcome.

Acceptance:

- parser/self-test can join one flow by handle/epoch/stream id and produce an
  ordered timeline without relying on secrets or environment values;
- raw freshness fields are used or the suite parser is updated and self-tested
  for `connection_fresh_stream_frames_pending` and
  `connection_stale_stream_frames_pending`;
- local tests prove the timeline records a pressure edge before a later
  stream event when both happen in a fixture.

Failure interpretation:

- if events cannot be joined reliably, the next step is seam extraction for a
  per-flow service timeline, not performance code.

### D2.2 - Local Coupling Harness [critical]

Goal: determine whether the current Rust task model can starve remote stream
polling during local pressure transitions.

Harness shape:

```text
scripted AsyncRead / RelayStream
  -> continuous reader or candidate reader
  -> bounded byte queue / dispatcher
  -> synthetic local admission and flush-pressure model
```

Required behaviors:

- stream-ready data remains polled while queue capacity exists;
- synthetic `flush_downlink` headroom/debt events do not create a remote
  poll/read gap above `50ms`;
- if the current model does create such a gap, the harness reports the exact
  transition that caused it.

Acceptance:

- at least one red/green tracer-bullet test for the causal behavior;
- no test asserts private implementation trivia unless no correct seam exists;
- if no correct seam exists, document that architecture gap and stop before
  adding throughput logic.

Failure interpretation:

- failure with reproduced gap supports H1;
- no reproduced gap with clean timeline supports H2 and redirects to the
  ordered stream readiness seam.

### D2.3 - Service Contract Selection

Goal: choose the smallest architecture that can plausibly clear the next Mbps
gate.

Candidate contracts:

1. Preserve H8 reader, but extend byte-permit lifetime until
   `SocketCtx.downlink_pending` is accepted by smoltcp or explicitly rejected
   by lifecycle.
2. Bind remote read service and local admission into one measured service
   window: every active window must show remote bytes and local accepted/drain
   progress, or an explicit bounded pressure reason.
3. Extract a deeper ordered-stream readiness adapter if D2.1/D2.2 show the
   gap is independent of local egress pressure.

Acceptance:

- code-level reachability gate includes capacity math, hot-path inventory, old
  path audit, and failure discriminators;
- design states whether it is necessary-only or intended to be sufficient for
  the `>30 Mbit/s` first gate;
- no `100+ Mbit/s` claim before repeatable `>30 Mbit/s` with clean cadence.

### D2.4 - VPS First Gate

Goal: run one scoped VPS acceptance only after D2.0-D2.3 pass locally.

Required command shape:

- focused safe1200 reverse-first P1 only;
- check `.33` sing-box and `.77` iperf3 liveness;
- use a real TTY if `sudo -v` is required;
- do not write secrets or sudo passwords into commands, scripts, docs, logs, or
  summaries.

Acceptance:

- receiver `>30 Mbit/s`;
- `data_poll_gap_max_ms < 500`;
- `data_read_gap_max_ms < 500`;
- `data_pending_gap_max_ms < 500`;
- `continuous_queue_wait_events` either `0` or explicitly explains a queue-full
  edge;
- TUN drops `0`;
- QUIC loss/congestion/blocking deltas `0`;
- `pending_at_close=0` and `terminal_pending_reap=0`;
- pressure/debt/headroom changes either disappear or align with a bounded,
  intentional pause.

Failure interpretation:

- gaps improve below `500ms` but throughput remains low: stream-read cadence is
  no longer the primary root; move to smoltcp/TUN egress capacity and local
  accepted/drain progress.
- gaps remain multi-second and align with local pressure/debt: H1 remains
  active; fix the service contract.
- gaps remain multi-second without local pressure alignment: H1 is weakened;
  focus on ordered QUIC readiness/waker behavior.
- queue wait becomes nonzero: H8 queue capacity finally became the real
  pressure edge; extend permit lifetime rather than tuning chunks.

### D2.5 - Repeatable `100+ Mbit/s` Gate

Goal: only after D2.4 passes, test whether the design can become a stable
`100+ Mbit/s` candidate.

Acceptance:

- at least two focused reverse-first P1 runs exceed `100 Mbit/s` receiver;
- both runs have `tx_dropped_delta=0`, `terminal_pending_reap_bytes=0`, no data
  relay `terminal_closed_no_send`, clean QUIC loss/blocking, and no unexplained
  multi-second data gaps;
- only then move to longer duration, P2/P4, concurrency, and UDP regression
  work.

## Stop Rules

- Do not edit throughput logic if D2.1 cannot produce a reliable timeline.
- Do not ask VPS to decide a hypothesis that a local harness can decide.
- Do not call H4 an accepted fix unless it passes the D2 gates as part of a
  broader service contract.
- Do not claim a plausible path to `100+ Mbit/s` from a necessary-only fix.
- After any failed VPS run, compare the failed metrics to the predictions above
  before proposing the next code change.

## Next Immediate Work

The next executable step is D2.0 plus D2.1 design implementation planning:

1. decide H4 revert vs feature-gated diagnostic path;
2. specify the per-flow timeline fields and parser self-test;
3. add the first local coupling harness test before touching the throughput
   path.

## D2.0 Result

D2.0 is complete locally.

Code changes:

- `src/tuic.rs` now separates the TUIC TCP relay modes into:
  - `ordered_join` as the default canonical ordered path;
  - `ordered_chunk` as an explicit H4 diagnostic path;
  - `unordered_reassembly_diag` as the existing unordered diagnostic path.
- `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=1` selects the H4 ordered chunk adapter.
- If both ordered-chunk and unordered-reassembly diagnostics are enabled,
  unordered reassembly wins, preserving the existing explicit diagnostic
  precedence.
- The US-client VPS suite now reports and forwards
  `MINI_VPN_TUIC_TCP_ORDERED_CHUNK` through `sudo -E env`.

Local gates passed:

```text
cargo test -q tuic_tcp_relay_mode_defaults_to_ordered_join_and_gates_diagnostics --lib
cargo test -q format_tuic_tcp_open_line_includes_target_pool_and_id --lib
cargo test -q ordered_relay_stream_ --lib
cargo test -q tuic_tcp_relay_mode --lib
bash -n scripts/knife14b-usclient-tunnel-suite.sh
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
cargo fmt --check
cargo check -q
cargo test -q tuic::tests:: --lib
cargo test -q --lib
git diff --check
```

`cargo test -q --lib` passed with `450` tests.

D2.0 acceptance:

- Canonical future VPS runs should show `relay_mode=ordered_join`.
- H4 diagnostic repeats must explicitly set `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=1`
  and should show `relay_mode=ordered_chunk`.
- This stage does not claim throughput improvement; it makes future D2
  evidence interpretable.

Next step:

- D2.1: add the per-flow timeline/parser seam that can join TUIC
  `poll/read/pending`, relay dispatch, `RelayEvent::Data`, `flush_downlink`,
  pressure debt, headroom, and TUN flush transitions without relying on mixed
  H4/default behavior.

## D2.1a Result

D2.1a is complete locally as an observability/parser stage, but it uncovered a
join-key gap that prevents a strong per-flow causal timeline today.

Code changes:

- `scripts/knife14b-lowrtt-probe.sh` now parses
  `tcp-stream-service-window` lines and summarizes local stream-service
  progress next to the existing TUIC stream read/pending/poll metrics.
- The summary now includes:
  - `stream_service_window`: remote poll/read maxima, local accepted/drain
    maxima, global-rx pressure maxima, local flush counters, pending freshness,
    useful-progress windows, and the last blocked reason.
  - `d2_flow_timeline`: a joinability discriminator plus the key TUIC data
    gaps and local egress pressure fields.
- `scripts/knife14b-usclient-tunnel-suite.sh` now preserves
  `stream_service_window`, `d2_flow_timeline`, and raw
  `tcp-stream-service-window` lines in the parent report summary.

TDD result:

- A red self-test fixture first required a single synthetic reverse flow with
  both `tuic-tcp-stream-*` lines and a `tcp-stream-service-window` line to
  produce the new summary.
- The self-test is now green and explicitly reports:
  `d2_flow_timeline: joinable=0 reason=missing_tuic_handle_epoch_bridge`.

Local gates passed:

```text
bash scripts/knife14b-lowrtt-probe.sh --self-test
bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
cargo test -q stream_service_window_diag_line_includes_remote_and_local_progress --lib
cargo fmt --check
cargo check -q
cargo test -q --lib
git diff --check
```

`cargo test -q --lib` passed with `450` tests.

Finding:

- TUIC stream timing logs are keyed by `conn/id/stream`.
- Local stream-service windows are keyed by `handle/epoch`.
- No current log line bridges those two key spaces. Therefore current VPS
  evidence can show temporal correlation between TUIC pending/read gaps and
  local egress windows, but it cannot prove the same flow caused both events.

D2.1b is now required before a canonical VPS run can act as a reliable
architecture discriminator. The next change should be observability-only:
add a minimal bridge log/field that ties the local `handle/epoch` to the TUIC
`conn/id/stream` for the same TCP relay, then extend the parser self-test to
expect `joinable=1`.

## D2.1b Result

D2.1b is complete locally as an observability-only bridge. No throughput logic,
read cadence, queue sizing, backpressure policy, or close/drain behavior was
changed.

Code changes:

- `src/upstream.rs` adds `TcpRelayOpenDiag` plus a task-local helper,
  `with_tcp_relay_open_diag`, so the caller that owns the local TCP
  `handle/epoch` can scope one `open_tcp` call with diagnostic context.
- `src/client_tun.rs` wraps both inline and spawned remote opens with that
  diagnostic context.
- `src/tuic.rs` reads the task-local context when formatting
  `tuic-open-tcp`; when present, the line now includes `handle=... epoch=...`
  in addition to TUIC `conn/id/stream`.
- `scripts/knife14b-lowrtt-probe.sh` recognizes that bridged `tuic-open-tcp`
  line and can now mark the D2 flow timeline as joined.

TDD result:

- Rust task-local test proves the diagnostic context is scoped to the current
  async task and disappears after the scoped open future completes.
- TUIC format tests prove the default open line has no local bridge fields,
  while a scoped open line includes `handle/epoch`.
- The low RTT probe self-test now expects and passes:
  `d2_flow_timeline: joinable=1 reason=joined_by_bridge`.

Local gates passed:

```text
cargo test -q tcp_relay_open_diag_context_is_task_scoped --lib
cargo test -q format_tuic_tcp_open_line --lib
bash scripts/knife14b-lowrtt-probe.sh --self-test
bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
cargo fmt --check
cargo check -q
cargo test -q --lib
git diff --check
```

`cargo test -q --lib` passed with `452` tests.

Acceptance implication:

- The next canonical VPS run is now allowed as a discriminator, but only to
  collect evidence. It should first prove that real logs contain
  `tuic-open-tcp ... conn=... id=... stream=... handle=... epoch=...` and that
  the probe report emits `d2_flow_timeline: joinable=1`.
- The run is not expected to fix throughput by itself. If throughput remains
  low, the joined timeline must decide whether multi-second TUIC data gaps
  align with local service-window stalled/no-progress windows, local pressure
  credit/headroom/TUN flush, or neither.

## D2.1c VPS Gate Result

One focused canonical VPS gate was run from the `.27` US client against `.33`
TUIC/sing-box and `.77` iperf3:

- Remote report:
  `/tmp/conn/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`
- Local bundle copy:
  `/tmp/mini_vpn_knife14h9_d2_bridge/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`
- Probe:
  `mtu1200_reverse_first_p1`, `PARALLEL_SET=1`, `DURATION=30`,
  `STOP_AFTER_REVERSE_FIRST_P1=1`.
- Canonical flags:
  `relay_mode=ordered_join`, `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=0`,
  `MINI_VPN_CONTINUOUS_TCP_RELAY=0`, `MINI_VPN_THIN_TCP_RELAY=0`.

Gate outcome:

- The D2 bridge succeeded in real VPS logs:
  - `tuic-open-tcp ... conn=... id=... stream=... relay_mode=ordered_join ... handle=SocketHandle(...) epoch=1`
  - `d2_flow_timeline: joinable=1 reason=joined_by_bridge`
- Throughput failed badly:
  - `iperf_sender_mbps=1.500`
  - `iperf_receiver_mbps=0.615`
  - `throughput_shape=no_data`

Key metrics:

- `data_poll_gap_max_ms=4`: the data stream was being polled continuously.
- `data_read_gap_max_ms=6837` and `data_pending_gap_max_ms=6010`: despite
  continuous polling, data reads still had multi-second gaps.
- TUIC pending causes were all connection stream-frame related:
  `connection_fresh_stream_frames_pending=10`,
  `connection_stale_stream_frames_pending=16`, with no
  `no_connection_rx`, `connection_rx_no_stream_frames`, QUIC loss,
  congestion, or QUIC blocking signal.
- Local pressure was clean:
  `global_rx_pressure_events_max=0`, `pressure_credit_debt_bytes=0`,
  `headroom_limited=0`, `tun_flush_deferred=0`,
  `tun_tx_dropped_delta=0`, `tun_flush_failures=0`.
- Local stream-service windows showed actual local admission, not queue
  starvation:
  `stream_service_windows=1018`, `remote_poll_ticks_max=1010`,
  `remote_read_chunks_max=1016`, `local_accepted_bytes_max=131072`.
- `local_egress_drain_bytes_max=0` and
  `last_blocked_reason=local_admission_no_progress` appeared in the joined
  service windows. The aggregate local egress service also recorded many
  no-progress/no-work windows.
- Close-tail stayed clean for the observed summary:
  `terminal_pending_reap=0`, `pending_at_close=0`, `egress_at_close=0`.

Interpretation:

- D2.1c falsifies "the relay task is not polling the TUIC stream" for this run:
  poll cadence was healthy at `4ms` max for the data stream.
- It also weakens ordinary local pressure explanations for this specific run:
  global-rx, pressure credit, headroom, TUN drops, TUN flush failures, and
  close-tail accounting were clean.
- The remaining sharp branch is now narrower: under a joined flow, Quinn/TUIC
  `poll_read` repeatedly returned `Pending` for seconds while the connection
  sampled fresh/stale stream frames, and local smoltcp admission accepted bytes
  in small/medium bursts with no local egress drain progress. The next design
  should separate:
  - QUIC connection frames that belong to the joined data stream vs unrelated
    connection-level stream frames;
  - smoltcp/TUN egress drain progress vs merely admitting bytes into the local
    TCP send queue;
  - whether `local_admission_no_progress` is a harmless idle result or evidence
    that ACK/window/TUN egress service is not driving the target sender.

Stop rule:

- Do not tune chunk size, self-wake, read batching, VPS buffers, MTU, stale
  pool, or broad QUIC windows from this result. The next change must be another
  discriminator or a small architecture change that explicitly explains
  continuous poll + connection stream frames + multi-second read gaps.

## D2.2a Parser Discriminator Result

D2.2a did not change data-plane behavior. It extended the existing low RTT
probe/report parser so the next design has a sharper, falsifiable signal:

- `tuic_stream_frame_delta` summarizes `conn_rx_stream_frames_since_read` and
  `conn_rx_stream_frames_since_pending` from `tuic-tcp-stream-pending` lines.
- `local_egress_service` summarizes aggregate `tcp-local-egress-service`
  progress: service windows/cycles, accepted bytes, egress drain bytes, TUN RX
  packets, flush calls/failures, dirty passes, and no-progress/no-work causes.
- `d2_flow_timeline` now includes `local_egress_drain_bytes_max`.
- The parent US-client suite preserves both new summary lines and the raw
  `tcp-local-egress-service` metrics.

Local gates:

- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`
- `cargo fmt --check`
- `cargo check -q`
- `cargo test -q --lib` (`452` tests)
- `git diff --check`

The existing D2.1c artifact could be re-summarized locally without a new VPS
run:

- Re-summary:
  `/tmp/mini_vpn_knife14h9_d2_bridge/d2_2a_resummary.md`
- Source bundle:
  `/tmp/mini_vpn_knife14h9_d2_bridge/mvpn_knife14h9_d2_bridge_p1_usclient_suite_20260709_190629.tar.gz`

Re-summarized D2.1c signals:

- Throughput stayed `1.500/0.615 Mbit/s`, `throughput_shape=no_data`.
- `d2_flow_timeline` stayed joined:
  `joinable=1 reason=joined_by_bridge`.
- Data stream poll cadence stayed healthy:
  `data_poll_gap_max_ms=4`.
- Read/pending gaps stayed bad:
  `data_read_gap_max_ms=6837`,
  `data_pending_gap_max_ms=6010`.
- TUIC connection stream-frame deltas were real:
  `conn_rx_stream_frames_since_read_max=142`,
  `conn_rx_stream_frames_since_pending_max=1201`,
  `fresh_since_pending_max=1201`,
  `fresh_nonzero_since_pending=10`,
  `stale_zero_since_pending=16`.
- Local stream-service admission was real but egress drain stayed absent:
  `local_accepted_bytes_max=131072`,
  `local_egress_drain_bytes_max=0`.
- Aggregate local egress service also showed no drain:
  `windows_max=1356`,
  `cycles_max=394`,
  `egress_drain_bytes_max=0`,
  `tun_rx_packets_max=195`,
  `flush_tx_calls_max=394`,
  `dirty_passes_max=394`,
  `no_progress_max=364`,
  `no_work_max=992`.

Interpretation:

- The "relay task not polling TUIC" branch remains falsified for D2.1c.
- The "no connection-level stream frames are arriving" branch is also falsified:
  fresh pending samples had non-zero connection stream-frame deltas, with a max
  delta of `1201` since the previous pending sample.
- The unresolved architectural branch is now explicit: a joined data stream is
  polled continuously, connection-level stream frames arrive, and bytes are
  admitted into the local side, but no measured local egress drain occurs. The
  next design must explain whether the missing drain is:
  - the local TCP/TUN egress service failing to progress ACK/window output;
  - a coupling artifact where admission/read cadence outruns egress feedback;
  - or a TUIC/Quinn readiness issue where connection-level stream frames are
    not being delivered to the joined data stream.

Next acceptance requirement:

- A design that claims it can clear the next Mbps gate must improve at least
  one of these primary signals in a focused VPS run:
  `local_egress_drain_bytes_max > 0`, sustained `tuic_stream_frame_delta`
  without multi-second `data_pending_gap`, or a clear counter proving the fresh
  connection stream frames do not belong to the joined data stream. If none
  improves, the design is falsified and should not be tuned further.

## D2.2b Snapshot and Unordered Stream Discriminator Result

D2.2b changed observability and suite wiring, not accepted product behavior.
It also ran one focused VPS diagnostic to decide whether the D2.2a
connection-level stream-frame samples can belong to the joined TUIC data stream.

Code changes:

- `src/client_tun.rs` adds `LocalEgressPressureSnapshot`, capturing both
  queued TUN TX bytes and smoltcp send-queue bytes before `drain_ready_tun_rx`
  and after `iface.poll` / `device.flush_tx`.
- `service_local_egress_until` now reports local egress drain across the whole
  ACK/TUN RX plus poll/flush service cycle. This fixes the D2.2a blind spot
  where ACK/TUN RX could reduce pressure before the old snapshot point, making
  `egress_drain_bytes=0` look stronger than it was.
  The metric is a conservative progress discriminator, not an exact byte-level
  accounting of every buffer movement.
- `scripts/knife14b-usclient-tunnel-suite.sh` now forwards and reports
  `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY`.
- `scripts/knife14b-lowrtt-probe.sh` preserves
  `tuic-tcp-unordered-staging` lines so the report can distinguish same-stream
  unordered chunks from unrelated connection-level stream frames.

Focused local gates:

```text
cargo test -q local_egress_pressure_snapshot --lib
cargo test -q local_egress_service --lib
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
bash scripts/knife14b-lowrtt-probe.sh --self-test
bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh
cargo fmt --check
cargo check -q
cargo test -q --lib
git diff --check
```

`cargo test -q --lib` passed with `455` tests after the D2.2b edits.

Focused VPS diagnostic:

- Remote report:
  `/tmp/conn/mvpn_knife14c_usclient_suite_20260709_193528.md`
- Remote bundle:
  `/tmp/conn/mvpn_knife14c_usclient_suite_20260709_193528.tar.gz`
- Local bundle copy:
  `/tmp/mini_vpn_knife14h9_d2_2b_unordered_diag/mvpn_knife14c_usclient_suite_20260709_193528.tar.gz`
- Probe:
  `mtu1200_reverse_first_p1`, `PARALLEL_SET=1`, `DURATION=30`,
  `STOP_AFTER_REVERSE_FIRST_P1=1`.
- Diagnostic flags:
  `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1`,
  `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=0`,
  `MINI_VPN_CONTINUOUS_TCP_RELAY=0`,
  `MINI_VPN_THIN_TCP_RELAY=0`.
- Real logs confirmed `relay_mode=unordered_reassembly_diag`.

VPS outcome:

- Throughput was still low:
  `iperf_sender_mbps=18.400`, `iperf_receiver_mbps=17.500`,
  `throughput_shape=low_average`.
- The unordered diagnostic read substantially more stream data than D2.2a but
  did not fix cadence:
  `remote_read_chunks_max=17049`, `remote_read_bytes_max=65577634`,
  `data_poll_gap_max_ms=4`, `data_read_gap_max_ms=3831`,
  `data_pending_gap_max_ms=3831`.
- Local egress drain is no longer truly absent:
  `stream_service_window.local_egress_drain_bytes_max=9215` and
  `local_egress_service.egress_drain_bytes_max=672066`.
  Therefore D2.2a's zero-drain signal was an observability false negative for
  actual drain progress, although the measured drain is still too small to
  explain a healthy `100+ Mbit/s` path.
- QUIC/path and local pressure stayed clean:
  `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`,
  `max_lost_bytes_delta=0`, `max_congestion_events_delta=0`,
  `max_tx_blocked_data_delta=0`, `max_tx_blocked_stream_delta=0`,
  `pressure_credit_debt_bytes=0`, `headroom_limited=0`,
  `tun_flush_deferred=0`.

The decisive discriminator is the same-stream unordered staging evidence:

```text
first staged chunk:
next_offset=0 chunk_offset=14102 chunk_bytes=18 gap_bytes=14102

mid-run staged chunk:
next_offset=7148574 chunk_offset=7665810 chunk_bytes=4 gap_bytes=517236

run maxima:
max_gap_bytes=1169774
max_buffered=1057134B
unordered_chunks=47070
out_of_order_chunks=30287
cap_hits=0
cap=4194304B
```

Interpretation:

- The D2.2a connection-level fresh stream-frame samples are not merely
  unrelated connection noise. In this diagnostic run, the joined TUIC data
  stream itself delivered higher-offset chunks while the contiguous
  `next_offset` lagged behind.
- H4's ordered read adapter and D2.2b's unordered diagnostic are both real
  seams. Neither is a sufficient product fix by itself.
- The remaining high-value architecture gap is no longer generic chunk sizing
  or self-wake. It is a per-flow service contract that keeps QUIC receive
  service, bounded reassembly, contiguous byte delivery, local admission, and
  local egress drain observable as one bounded pipeline.

## Sing-box Code-Level Contrast

The current sing-box source inspected for this comparison was official
`SagerNet/sing-box` commit `f3b0c77` with
`github.com/sagernet/sing-quic` version
`v0.6.4-0.20260709034545-e23afe1172dc`.

Relevant code shape:

- `protocol/tuic/outbound.go` creates a `tuic.Client` and its TCP
  `DialContext` returns `h.client.DialConn(ctx, destination)`, a normal
  `net.Conn`.
- `route/conn.go` then handles each TCP flow with two independent goroutines:
  one copying inbound to remote, and one copying remote to inbound.
- Each direction calls
  `bufio.CopyWithIncreateBuffer(destination, source,
  bufio.DefaultIncreaseBufferAfter, bufio.DefaultBatchSize)`.
- The copy helper starts with normal buffers and can increase buffering after
  `512 * 1000` bytes; its default batch size is `8`.
- Lifecycle is half-close aware: on clean copy completion sing-box attempts
  `CloseWrite` on duplex destinations; on error it closes both sides.

Architectural difference that can explain `15-30M` vs `173M`:

- sing-box's TCP hot path is a direct two-goroutine `net.Conn` copy contract.
  The remote-read loop is not coupled to a TUN/smoltcp main loop, local
  downlink admission queue, pressure-credit debt, dirty-handle pass, or
  periodic local egress service.
- mini_vpn's reverse path is a multi-clock pipeline:
  TUIC read task -> relay event/global queue -> main loop ->
  `handle_remote_payload` -> `SocketCtx.downlink_pending` ->
  `flush_downlink` -> smoltcp send queue -> `iface.poll` ->
  `device.flush_tx` -> TUN. Capacity can be released before the local side has
  produced durable egress/drain feedback, and progress is spread across several
  event-loop responsibilities.
- D2.2b proves that mini_vpn's QUIC receive side can observe large
  same-stream offset gaps while the local pressure summary remains mostly
  clean. A mature-client-shaped design therefore needs an explicit per-flow
  receive/copy contract rather than more global queue or pacer tuning.

Differences that do not explain the current gap by themselves:

- sing-box uses Go goroutines and mini_vpn uses Rust/Tokio tasks; the language
  difference is not the root. The relevant Rust-specific requirement is to
  encode ownership and backpressure so a per-flow task can hold the read side,
  bounded buffers, and shutdown/half-close state without borrowing the whole
  TUN/smoltcp loop.
- sing-box can use ordinary kernel TCP sockets on both ends, while mini_vpn
  terminates local TCP in smoltcp behind TUN. That matters, but the observed
  failure is not simply "TUN is slow": TUN drops and QUIC blocking stayed zero,
  and local admission did accept bursts up to `131072` bytes.
- Buffer size and batching are capacity enablers, not sufficient fixes. D2.2b
  still had `65536` read lengths and a 4 MiB unordered staging cap with
  `cap_hits=0`, yet throughput remained low.

## D2.3 Selected Design: Bounded Per-Flow Copy Contract

The next suitable architecture is a reversible diagnostic/product seam, not a
single parameter change:

```text
TUIC RecvStream read service
  -> bounded same-stream chunk/reassembly stage
  -> contiguous downlink byte pump
  -> bounded local admission permit
  -> smoltcp/TUN egress service feedback
```

Rust-specific design constraints:

- one owner task owns the QUIC `RecvStream`; no shared mutable stream state
  across the TUN loop;
- bounded memory is explicit: per-flow out-of-order bytes, contiguous bytes,
  and downstream admission permits each have caps and cap-hit counters;
- the assembler is a pure state machine with deterministic tests for
  out-of-order chunks, overlapping chunks, duplicate chunks, FIN/error, cap
  hits, and contiguous drain;
- local backpressure is represented as permits or acknowledgements from
  smoltcp admission/egress progress, not as implicit queue-pop success;
- cancellation and half-close are explicit: remote EOF, local close, reset, and
  terminal pending bytes are separate states;
- the default ordered path remains available until the new contract passes
  local and VPS gates.

Necessary vs sufficient classification:

- The D2.2b local egress snapshot fix is necessary observability only.
- The unordered diagnostic path is necessary evidence only.
- D2.3 is intended to be sufficient only for the first `>30 Mbit/s` gate, not
  for a promised `100+ Mbit/s` result. A later repeatable VPS gate is required
  before calling it a `100+ Mbit/s` candidate.

D2.3 local/TDD acceptance:

- pure assembler tests prove contiguous output under out-of-order input without
  unbounded memory growth;
- pump tests prove an active stream continues reading while reassembly and
  contiguous queues have capacity;
- downstream permit tests prove capacity is not returned merely because a
  dispatcher popped bytes; it is returned on smoltcp admission, explicit
  rejection, close, or measured egress/drain according to the chosen contract;
- lifecycle tests prove EOF/CloseWrite/reset do not strand buffered bytes or
  spin forever;
- parser self-tests expose:
  `max_gap_bytes`, `max_buffered`, `cap_hits`, contiguous output bytes,
  downstream permit used/released, local accepted bytes, and local egress drain.

D2.3 VPS first gate:

- run one focused reverse-first P1 only after local gates pass;
- receiver must exceed `30 Mbit/s`;
- `data_poll_gap_max_ms < 500`,
  `data_read_gap_max_ms < 500`, and
  `data_pending_gap_max_ms < 500`;
- unordered/reassembly `cap_hits=0`;
- `max_gap_bytes` either shrinks materially or is paired with continuous
  contiguous output progress;
- `global_rx_pressure_events=0` or a bounded intentional pressure reason;
- `tun_rx_dropped_delta=0`, `tun_tx_dropped_delta=0`;
- QUIC loss/congestion/blocking deltas remain `0`;
- close-tail pending remains `0`.

D2.3 falsification rules:

- If the pump has capacity but `data_read_gap_max_ms` remains multi-second,
  the read-pump/reassembly hypothesis is weakened and Quinn/TUIC transport
  readiness must be isolated below the mini_vpn relay abstraction.
- If read gaps improve but receiver remains below `30 Mbit/s`, the next root is
  local smoltcp/TUN egress service, not TUIC stream readiness.
- If cap hits occur, the design is bounded correctly but undersized or missing
  downstream release; do not raise caps until the permit/release counters prove
  where bytes are stuck.
- If local pressure/debt/headroom counters become active again, the old local
  admission path is still coupled and the design is not a complete data-plane
  replacement.

## D2.3 Permit Pump VPS Result

Date: 2026-07-09

Implementation summary:

- added a feature-gated `permit_pump` relay engine selected by
  `MINI_VPN_D2_PERMIT_TCP_RELAY=1`;
- added `AsyncLeasedByteFlowQueue`, where reader capacity remains leased until
  downstream local admission or explicit drop/close releases it;
- threaded `DownstreamBytePermit` through `RelayEvent::Data`,
  `handle_remote_payload`, `SocketCtx.downlink_pending`, and `flush_downlink`;
- added parser/suite fields for `downstream_permit_bytes`,
  `downstream_permit_released_bytes`, and
  `downstream_permit_pending_high`;
- removed the permit reader's active wait on read-credit updates so permit mode
  is gated by stop or leased queue capacity, not local read-credit chatter.

Local gates passed:

```text
cargo test -q permit --lib
cargo test -q --lib
cargo fmt --check
cargo check -q
bash scripts/knife14b-lowrtt-probe.sh --self-test
bash scripts/knife14b-usclient-tunnel-suite.sh --self-test
git diff --check
```

VPS run:

- remote bundle:
  `/tmp/conn/mvpn_knife14h9_d23_permit_p1_retry1_usclient_suite_20260709_202637.tar.gz`
- local copy:
  `/tmp/mini_vpn_knife14h9_d23_permit_p1/mvpn_knife14h9_d23_permit_p1_retry1_usclient_suite_20260709_202637.tar.gz`
- settings: `MINI_VPN_D2_PERMIT_TCP_RELAY=1`,
  `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1`,
  `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=0`,
  `STOP_AFTER_REVERSE_FIRST_P1=1`, `PARALLEL_SET=1`, `DURATION=30`.

Result:

- reverse-first P1: sender `0.699 Mbit/s`, receiver `0.489 Mbit/s`;
- `relay_mode=unordered_reassembly_diag` and `engine=permit_pump` were active;
- `downstream_permit_bytes=1867790`,
  `downstream_permit_released_bytes=1867790`,
  `downstream_permit_pending_high=120177`;
- `global_rx_pressure.events=0`, `downlink_backpressure.pause_edges=0`,
  `pressure_credit_debt_bytes=0`, `headroom_limited=0`,
  `tun_flush_deferred=0`;
- TUN drops were `0/0`, QUIC loss/congestion/blocking deltas were `0`;
- close-tail pending stayed `0`;
- unordered staging stayed bounded (`cap_hits=0`) but still had same-stream
  gaps (`max_gap_bytes=198244`, `max_buffered=194020B`);
- active data stream cadence failed the gate:
  `data_read_gap_max_ms=6836`, `data_pending_gap_max_ms=6006`,
  while `data_poll_gap_max_ms=3`.

Interpretation:

- D2.3 falsified "dispatcher queue capacity is returned too early" as the
  sufficient bottleneck. Permit capacity was fully released and local pressure
  counters stayed clean, yet throughput collapsed below even H4/H8.
- The remaining active discriminator is below the client-tun downlink permit
  boundary: TUIC/Quinn stream readiness still reports multi-second Pending/Read
  gaps while the stream is polled frequently and connection stream frames are
  present.
- Do not continue by raising queue sizes, tuning permit release, or reintroducing
  read-credit coupling. The next design must isolate the TUIC `RecvStream`
  readiness/waker/reassembly owner below `client_tun.rs`, or replace the
  diagnostic unordered adapter with a true independent per-flow copy/reassembly
  task that can prove continuous same-stream contiguous output before local
  admission.

## D2.4 TUIC TCP Raw Sink Capacity Gate

Date: 2026-07-09

Goal:

- decide whether mini_vpn's TUIC/Quinn TCP stream read path can exceed the
  `100 Mbit/s` class without the TUN/smoltcp/local-TCP downlink machinery;
- if raw TUIC read still failed, keep the next design below `client_tun.rs`;
- if raw TUIC read passed, move the bottleneck back into the local TCP/TUN
  data-plane architecture.

Implementation:

- added a diagnostic-only binary mode:
  `mini_vpn tuic-tcp-sink-probe <host:port> [duration_secs]`;
- the probe builds `TuicUpstream` from existing TUIC environment config, opens a
  TUIC Connect stream to the target, reads into a 256 KiB scratch buffer, and
  discards bytes locally;
- output fields are `bytes`, `read_mbps`, `reads`, `first_rx_ms`,
  `max_read_gap_ms`, and `eof`;
- default `client-tun` behavior is unchanged.

Local gates passed:

```text
cargo fmt --check
cargo test -q --bin mini_vpn
cargo test -q --lib
cargo check -q
```

VPS diagnostic:

- `.33` sing-box was active and socket buffers were still persisted at
  `rmem_max/wmem_max=16777216`, `rmem_default/wmem_default=1048576`;
- an initial high-port target (`.77:5297`) was rejected by the path:
  sing-box logged `dial tcp ... i/o timeout`, so that run was discarded as an
  invalid capacity signal;
- `.77` iperf3 was temporarily stopped, a one-shot Python byte source was bound
  to the already-open `.77:5201`, and iperf3 was restarted afterward; final
  check showed `iperf3` active and `*:5201` listening.

Run settings:

- `MINI_VPN_TUIC_TCP_POOL=1`;
- `MINI_VPN_TCP_DIAG=1`;
- `MINI_VPN_TUIC_TCP_UNORDERED_REASSEMBLY=1`;
- `MINI_VPN_TUIC_TCP_ORDERED_CHUNK=0`;
- target `.77:5201`;
- duration `30s`.

Result:

```text
tuic_tcp_sink_probe target=43.130.32.77:5201 requested_duration_secs=30 elapsed_ms=30000 bytes=477101276 read_mbps=127.227 reads=266952 first_rx_ms=3 max_read_gap_ms=196 eof=false
```

Corroborating target-side source:

```text
byte_source_done sent_bytes=481820672 elapsed=44.999 timeouts=14 errors=1
```

Interpretation:

- `100 Mbit/s` is `12.5 MB/s`; this raw TUIC sink read delivered about
  `15.9 MB/s`, so mini_vpn's Rust/TUIC/Quinn receive path has demonstrated
  `100+ Mbit/s` capacity on the same VPS route.
- The low reverse-first suite results (`~15-25 Mbit/s`, and D2.3's
  `0.489 Mbit/s` failure) are therefore not explained by VPS socket buffers,
  MTU/PLPMTUD, stale pool, broad QUIC flow windows, sing-box, iperf3, or a
  fundamental mini_vpn TUIC raw-read ceiling.
- The remaining sufficient bottleneck is the local TCP/TUN egress architecture:
  remote TUIC bytes can arrive fast when they are drained directly, but the
  `client_tun.rs` path still spreads progress across relay task dispatch,
  global queue, local admission, `downlink_pending`, smoltcp socket send
  buffer, dirty-handle scheduling, `iface.poll`, `flush_tx`, and TUN write.
- The next accepted design must preserve the D2.4 raw-stream cadence while
  coupling downstream capacity only to real local admission/egress progress.
  More chunk-size, self-wake, permit-size, or VPS-side tuning is now explicitly
  lower priority.

Conclusion:

- mini_vpn has now proven a code-level `100+ Mbit/s` transport capability.
- The current VPN data-plane architecture has not proven `100+ Mbit/s`; it
  needs a local downlink/egress architecture replacement or equivalent bounded
  copy contract before another full reverse-first acceptance run is meaningful.
