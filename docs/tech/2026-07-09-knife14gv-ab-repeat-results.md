# 2026-07-09 Knife14gv A/B Repeat Results

## Goal

Execute the stop-condition A/B after Knife14gu G8 failed to preserve the G7
`100+ Mbit/s` cadence.

The A/B question was:

- Does `1a3c5cb` repeat as the same low result seen in G8?
- If it stays far below G7, does parent `4caf60a` still reproduce the G7
  `147/144 Mbit/s` high-throughput run under the same suite shape?

No code was changed for this stage.

## Code Points

Candidate:

- Commit: `1a3c5cb`
  (`fix(knife14gu): stop relay ready bursts at rx edge`)
- Remote workdir: `/tmp/mini_vpn_knife14gu_1a3c5cb`
- Binary SHA-256:
  `ce53d63c7ba0ef593ff0b478e4146a33eab9a7896afcdf8b182652285b6f4008`

Parent:

- Commit: `4caf60a`
  (`fix(knife14gt): align relay dispatch with egress window`)
- Remote workdir: `/tmp/mini_vpn_knife14gt_4caf60a`
- Binary SHA-256:
  `1b2f81c54169f6e34fb7e0fcc75b35a5e838fe96345b0273ebe3962f0a2c1b96`

Both remote workdirs were rsync clean copies without `.git`, so the suite's
`git rev-parse` check reported `not a git repository`. The code points above
come from the pushed local commits used to create those workdirs.

## Suite Shape

Both runs used the same focused gate:

- safe1200 reverse-first P1
- `DURATION=30`
- `PARALLEL_SET=1`
- `RUN_REVERSE_FIRST_P1=1`
- `STOP_AFTER_REVERSE_FIRST_P1=1`
- `IPERF_TIMEOUT_SECS=120`
- `MINI_VPN_BUFFERED_DOWNLINK=1`
- `BUILD_RELEASE=0`

No VPS, iperf3, MTU/PLPMTUD, stale-pool, or broad QUIC-window changes were
made.

## Artifacts

Candidate pulled bundle:

- `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gu_rxedge_repeat1_safe1200_p1_usclient_suite_20260709_093320.tar.gz`
- `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gu_rxedge_repeat1_safe1200_p1_usclient_suite_20260709_093320.md`
- `/tmp/mini_vpn_knife14gv_ab/mvpn_accept_20260709_093320.log`
- `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gu_rxedge_repeat1_safe1200_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_093320.md`

Parent pulled bundle:

- `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gt_parent_ab_safe1200_p1_usclient_suite_20260709_093511.tar.gz`
- `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gt_parent_ab_safe1200_p1_usclient_suite_20260709_093511.md`
- `/tmp/mini_vpn_knife14gv_ab/mvpn_accept_20260709_093511.log`
- `/tmp/mini_vpn_knife14gv_ab/mvpn_knife14gt_parent_ab_safe1200_p1_usclient_tunnel_mtu1200_reverse_first_p1_20260709_093511.md`

## Preflight

Read-only preflight before the A/B:

- `.33` sing-box: `active`
- `.33` socket buffers:
  - `net.core.rmem_max = 16777216`
  - `net.core.wmem_max = 16777216`
  - `net.core.rmem_default = 1048576`
  - `net.core.wmem_default = 1048576`
- `.77` iperf3: `active`

Suite direct baselines were healthy:

| Run | `.27 -> .77` receiver | `.77 -> .27` receiver |
| --- | ---: | ---: |
| `1a3c5cb` repeat | `275 Mbit/s` | `278 Mbit/s` |
| `4caf60a` parent A/B | `279 Mbit/s` | `297 Mbit/s` |

## Results

| Code point | Sender | Receiver | Result |
| --- | ---: | ---: | --- |
| `1a3c5cb` repeat | `41.4 Mbit/s` | `39.9 Mbit/s` | Data-moving, above `30M`, below `100M` |
| `4caf60a` parent A/B | `0.349 Mbit/s` | `0.151 Mbit/s` | No-data/starved |

This A/B did **not** reproduce G7's `147/144 Mbit/s` parent result.

## Candidate Signals: `1a3c5cb`

Useful progress:

- Data moved: `remote_to_global_rx_bytes=150915248`.
- Data accepted into smoltcp:
  `send_slice_accepted=150181213`.
- Receiver exceeded the old `>30 Mbit/s` discriminator at `39.9 Mbit/s`.
- TUN drops stayed clean: `tx_dropped_delta=0`.
- The new RX-edge limiter still did not activate:
  `remote_batch_limited=0`.
- Data relay `global_rx_queue_used_max=161/1024`, far from the critical edge.

Still failing:

- Receiver remained far below `100 Mbit/s`.
- Ordered stream read gaps remained multi-second:
  `max_remote_read_gap_ms=3415`.
- The tunnel report attributed the run to
  `local_downlink_backpressure`.
- Close-tail/backpressure returned:
  `pending_total=734035`, `pending_high=734035`,
  `may_recv_false=6519`, `headroom_limited_calls=6507`, and
  `hard_edge_guard_limited_calls=6507`.
- `tcp-local-egress-service accepted_bytes=0`, so the local service lane still
  did not become the useful data-admission path.

Interpretation:

`1a3c5cb` is not a clean `100M` solution, but this repeat does not support a
simple claim that the RX-edge code directly caused the G8 `19.2 Mbit/s`
collapse. The limiter did not fire, and the run moved substantially more data
than G8.

## Parent Signals: `4caf60a`

Useful controls:

- Direct baselines were healthy.
- TUN drops stayed clean: `tx_dropped_delta=0`.
- Local pending stayed clean: `pending_total=0`.
- No local pressure/headroom blocker was visible:
  `may_recv_false=0`, `headroom_limited_calls=0`,
  `hard_edge_guard_limited_calls=0`.
- The parent preserved useful read-service floors:
  `remote_read_service_len_min=65536`,
  `remote_batch_limit_bytes_min=524288`.

Failure:

- Almost no data moved: `remote_to_global_rx_bytes=667766`,
  `send_slice_accepted=667766`.
- The tunnel report attributed the run to
  `tuic_stream_read_gap+tuic_stream_read_pending+relay_remote_read_gap+target_sender_stalled+tuic_stream_starved`.
- Ordered stream delivery had long starvation:
  `max_remote_read_gap_ms=10457`.
- Repeated pending windows remained despite active self-wake:
  `self_wake_armed` and `self_wake_fired` both increased, while
  `pending_cause=connection_stream_frames_pending` repeated.
- `global_rx_queue_used_max=1/1024`, so relay-to-main queue pressure was not
  involved.

Interpretation:

The parent high-throughput G7 result is not currently reproducible as a stable
baseline. In this A/B window, parent `4caf60a` regressed to a no-data/starved
shape even with healthy direct path and clean local/TUN/QUIC surfaces.

## Stage Conclusion

The A/B changes the working model:

1. G7 remains evidence that mini_vpn can reach `100+ Mbit/s` on this topology,
   but it is not yet a reproducible acceptance baseline.
2. G8 was not explained by the RX-edge limiter firing, because both G8 and the
   repeat had `remote_batch_limited=0`.
3. `1a3c5cb` is better than parent `4caf60a` in this exact A/B window
   (`39.9 Mbit/s` vs `0.151 Mbit/s`), but still not a clean solution.
4. The active root is not global RX queue edge pressure. The active root is
   unstable TUIC ordered stream service: the stream can enter long pending/read
   gaps while direct path, TUN drops, QUIC loss/blocking, and local queue
   pressure remain clean.

## Stop Condition

No further code change should start from this A/B alone.

The next implementation plan must be designed around a stricter code-level and
VPS gate for TUIC ordered stream service stability:

- detect and classify fresh versus stale `connection_stream_frames_pending`;
- prove why a stream can remain pending for seconds while connection-level UDP
  and stream-frame counters move;
- keep useful remote reads and local admission in the same measured service
  contract;
- require multiple repeat passes before treating any `100+ Mbit/s` run as a
  stable baseline.

Do not continue queue-edge guards, VPS tuning, iperf3 tuning, MTU/PLPMTUD,
stale-pool, broad QUIC-window, or unordered-reassembly work from this result.
