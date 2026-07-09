# 2026-07-09 Knife14gs G6 Local Egress VPS Results

## Goal

Run the focused VPS acceptance after the Knife14gs G2-G5 local egress service
lane.

Gate shape:

- safe1200 reverse-first P1
- `DURATION=30`
- `PARALLEL_SET=1`
- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `IPERF_TIMEOUT_SECS=120`
- `MINI_VPN_BUFFERED_DOWNLINK=1`

Acceptance for this slice was deliberately narrow: first exceed `30 Mbit/s`
receiver before claiming any path to `100+ Mbit/s`.

## Code State

- Commit: `4a44a11` (`fix(knife14gs): add local egress service lane`)
- Remote workdir on `.27`: `/tmp/mini_vpn_knife14gs_4a44a11`
- The dirty `/home/ubuntu/mini_vpn` worktree on `.27` was not modified.

Remote focused gates passed before the VPS run:

- `cargo test local_egress_service -- --nocapture`
- `cargo build --release`

## Artifacts

Remote bundle:

- `/tmp/knife14gs_g6_4a44a11/mvpn_knife14gs_g6_local_egress_safe1200_p1_usclient_suite_20260709_075624.tar.gz`

Local pulled bundle:

- `/tmp/mini_vpn_knife14gs_g6/mvpn_knife14gs_g6_local_egress_safe1200_p1_usclient_suite_20260709_075624.tar.gz`

Pulled files:

- `/tmp/mini_vpn_knife14gs_g6/mvpn_knife14gs_g6_local_egress_safe1200_p1_usclient_suite_20260709_075624.md`
- `/tmp/mini_vpn_knife14gs_g6/mvpn_accept_20260709_075624.log`
- `/tmp/mini_vpn_knife14gs_g6/mvpn_knife14gs_g6_local_egress_safe1200_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_075624.md`

## Preflight

Direct path was healthy:

| Path | Receiver |
| --- | ---: |
| `.27 -> .77` | `279 Mbit/s` |
| `.77 -> .27` | `280 Mbit/s` |

Startup confirmed:

- buffered downlink enabled
- safe1200 TUIC MTU policy
- QUIC UDP socket buffers at `16777216B`
- TUN MTU `1200`, qlen `500`

## VPS Result

Reverse-first P1:

| Metric | Value |
| --- | ---: |
| iperf sender | `28.6 Mbit/s` |
| iperf receiver | `27.6 Mbit/s` |

Stage answer: **G6 did not pass the `>30 Mbit/s` gate.**

Interval shape stayed burst/idle. The first second reached `177 Mbit/s` and
later bursts reached `111`, `97.5`, and `113 Mbit/s`, but many seconds were
`0.00`, leaving the receiver average at `27.6 Mbit/s`.

## Key Signals

Clean surfaces:

- TUN TX drops stayed `0`.
- `send_slice_zero=0`.
- `send_slice_errors=0`.
- `tun_flush_tx_failures=0`.
- QUIC loss, congestion, and blocking stayed clear:
  `lost_bytes=0`, `congestion_events=0`,
  `tx_blocked(data=0,stream=0)`, `rx_blocked(data=0,stream=0)`.
- Close-tail was not the primary failure:
  `terminal_late_remote_payload_bytes=0`,
  `terminal_late_remote_payload_events=0`.

Useful progress:

- Data moved: `remote_to_global_rx_bytes=104039283`.
- Data accepted into smoltcp:
  `send_slice_accepted=104031765`.
- Reader credit did not collapse:
  `remote_read_service_len_min=65536`,
  `remote_read_service_len_max=65536`,
  `remote_batch_limit_bytes_min=524288`,
  `read_credit_pause_updates=0`.

Still failing:

- Local egress service did run, but it did not make useful write-admission
  progress:
  `tcp-local-egress-service windows=6024 cycles=1718 accepted_bytes=0
  tun_rx_packets=660 no_progress=1620 no_work=4400 cycle_budget=4`.
- The main downlink path still did all useful socket admission:
  `send_slice_accepted=104031765`, while the new service lane reported
  `accepted_bytes=0`.
- Ordered TUIC stream delivery still had multi-second gaps:
  `max_remote_read_gap_ms=5029`, repeated
  `tuic-tcp-stream-pending ... pending_cause=connection_stream_frames_pending`,
  and read gaps such as `gap_ms=5031`, `3618`, `3421`, and `3409`.
- The final metrics tick reintroduced local headroom/backpressure edge
  symptoms after the burst:
  `budget_limited_calls=2033`,
  `headroom_limited_calls=2043`,
  `hard_edge_guard_limited_calls=2043`,
  `hard_edge_guard_deferred_bytes=2549768`,
  `may_recv_false=2086`.

## Interpretation

Knife14gs G5 was a real local scheduler change, but G6 proves it was not the
right sufficient architecture slice for the current bottleneck.

The new lane services only work already visible to the main loop through dirty
handles and buffered local state. It does not actively pull new bytes from the
TUIC stream. In this run, the dominant failure remained remote stream read
cadence: the reader stayed in repeated pending/read-gap windows while the local
surfaces were mostly clean and the new local egress lane had no accepted-byte
progress of its own.

Therefore the next fix must not be another local flush constant, TUN qlen,
MTU/PLPMTUD, stale pool, VPS, broad QUIC window, or self-wake-only change. It
must be a TDD-first architecture slice that joins TUIC stream read service and
main-loop local injection/egress into one measured service contract.

## Next Design Requirement

The next slice should prove, before another VPS run, a code-level service loop
with all of these properties:

- remote TUIC stream readiness/read service and local socket admission are
  measured in the same window;
- useful progress counts both newly read remote bytes and bytes accepted into
  smoltcp;
- local egress service is not declared successful when it only flushes/TUN-drains
  but accepts no payload;
- diagnostics expose max gap between useful remote reads and useful local
  admission;
- hard guards remain intact for terminal sockets, per-flow/global pending
  hard watermarks, and TUN drop feedback.

The first VPS gate after that slice remains `>30 Mbit/s` on the same safe1200
reverse-first P1 shape. Only after that passes should the route to clean
`100+ Mbit/s` be re-opened.
