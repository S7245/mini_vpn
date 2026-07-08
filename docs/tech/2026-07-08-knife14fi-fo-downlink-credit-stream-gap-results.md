# 2026-07-08 Knife14fi-fo Downlink Credit And Stream Gap Results

## Goal

Finish the last Knife14 reverse-first throughput gap without restarting closed
branches. The scoped goal was to make local downlink credit/backpressure driven
by actual egress progress, then continue through alternative schemes if that did
not produce the target `100+ Mbit/s` receiver result.

Acceptance target:

- reverse-first P1 receiver `100+ Mbit/s`
- `may_recv_false` and `headroom_deferred` significantly reduced
- `pending_at_close=0`
- `terminal_pending_reap=0`
- `tun_tx_dropped_delta=0`
- QUIC loss/blocking/congestion `0`

## Kept Changes

The current kept code changes are the parts that either improved local pressure
or added useful diagnostics without causing regressions:

- UDP socket receive/send buffer configuration for QUIC client endpoints.
- Progress-sensitive downlink credit accounting.
- Clean empty-staging read-credit expansion so a pressure-free flow can publish
  a full relay batch before pressure feedback exists.
- TUIC stream pending cause diagnostics and bounded pending self-wake.
- Safe1200 still disables PLPMTUD and keeps the normal QUIC receive windows.

Local gates passed after the kept code:

- `cargo test --lib --quiet`
- `cargo test --features harness --quiet`
- `cargo build --release --quiet`
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
- `rustfmt --edition 2024 --check src/client_tun.rs src/quic.rs src/tuic.rs`
- `git diff --check`

Remote focused gates on `.27` also passed for the downlink credit, safe1200
MTU policy, QUIC UDP socket buffer, release build, and harness clippy checks.

## Result Matrix

| Stage | Scheme | Result | Decision |
| --- | --- | --- | --- |
| FI | Progress-sensitive downlink credit | `15.8/15.1 Mbit/s` | Kept as useful local pressure cleanup, not sufficient |
| FJ | Relay remote read service tick | `17.1/15.1 Mbit/s` | Rejected and reverted |
| FK | TUIC unordered chunk reads | `245 Kbit/s / 30.2 Kbit/s` | Rejected and reverted |
| FL | Safe1200 receive windows `1MB/4MB` | `23.8/22.6 Mbit/s` | Rejected and reverted |
| FM | Clean empty-staging full-batch read credit | `25.9/24.7 Mbit/s` | Kept as a safe pressure-free improvement |
| FN | Pool size `1` discriminator | `17.6/16.5 Mbit/s` | Rejected as default direction |
| FO | Default MTU/PLPMTUD discriminator | `25.3/24.4 Mbit/s` | Rejected as final fix |

Direct exit-to-target iperf baselines were healthy and sequential:

- `.33 -> .77`: receiver about `209 Mbit/s`
- `.77 -> .33`: receiver about `207 Mbit/s`

## Key Evidence

### FI: local pressure cleaned, stream gaps remain

Bundle:
`/tmp/mini_vpn/knife14fi_progress_credit_p1_30/mvpn_knife14fi_progress_credit_p1_30_usclient_suite_20260708_033821.tar.gz`

The local downlink controller was no longer the obvious bottleneck:

- `pending_max=0`
- `headroom_deferred=0`
- `may_recv_false=0`
- `read_credit_limit_bytes_min=65536`
- QUIC loss/blocking/congestion `0`

But the data stream still had multi-second gaps:

- `tuic_stream_pending data_pending_gap_max_ms=4310`
- `relay_remote_timing data_max_read_gap_ms=4312`
- `data_poll_gap_max_ms=1689`

### FJ: service ticks do not fix the gap

Bundle:
`/tmp/mini_vpn/knife14fj_read_service_p1_30/mvpn_knife14fj_read_service_p1_30_usclient_suite_20260708_034404.tar.gz`

The added service tick made local polling frequent, but remote reads still had
long gaps and local pressure got worse:

- service poll gap about `3ms`
- read gap about `5277ms`
- pending high about `334356`
- headroom deferral and `may_recv_false` returned
- terminal pending reaping returned

This rejected the "local relay waker is asleep" hypothesis.

### FK: unordered chunks are not a safe TUIC stream workaround

Bundle:
`/tmp/mini_vpn/knife14fk_unordered_chunks_p1_30/mvpn_knife14fk_unordered_chunks_p1_30_usclient_suite_20260708_035205.tar.gz`

The attempt collapsed throughput and delivered only a tiny amount of data before
the first useful data read. This suggests missing contiguous stream offsets or
true ordered-stream HOL. The branch was fully reverted.

### FL: smaller safe1200 receive windows add flow-control stalls

Bundle:
`/tmp/mini_vpn/knife14fl_safe1200_flow_window_p1_30_retry/mvpn_knife14fl_safe1200_flow_window_p1_30_retry_usclient_suite_20260708_040202.tar.gz`

The run used:

- stream receive window `1048576B`
- connection receive window `4194304B`

It did not improve throughput and introduced flow-control blocking:

- `rx_blocked(stream=29)`
- `pending_max=94253`
- `may_recv_false=5193`
- `headroom_deferred_bytes=7443987`
- QUIC loss/congestion/PLPMTUD black holes `0`

The receive window shrink was reverted. Safe1200 should keep default receive
windows unless new evidence appears.

### FM: clean full-batch credit helps only slightly

Bundle:
`/tmp/mini_vpn/knife14fm_clean_batch_credit_p1_30/mvpn_knife14fm_clean_batch_credit_p1_30_usclient_suite_20260708_040812.tar.gz`

This was the best result in this stage, but still far below acceptance:

- sender `25.9 Mbit/s`
- receiver `24.7 Mbit/s`
- `may_recv_false=14`
- `pending_max=0`
- `headroom_deferred=1747693`
- `tun_tx_dropped_delta=0`
- QUIC loss/blocking/congestion/rx_blocked `0`

The data stream still had multi-second gaps:

- data max remote read gap about `3443ms`
- data pending gap about `3481ms`

### FN/FO: pool and MTU are not the final root

FN pool size `1` made throughput worse and still had read gaps up to about
`5665ms`, with all data on the primary connection and no QUIC loss/blocking.

FO default MTU/PLPMTUD performed similarly to safe1200:

- sender `25.3 Mbit/s`
- receiver `24.4 Mbit/s`
- PLPMTUD sent `4`, lost `0`, black holes `0`
- `dg_max=Some(1418)`
- QUIC loss/blocking/rx_blocked `0`
- data stream read gap about `3482ms`

## Conclusion

The accepted local controller work fixed or greatly reduced the original local
pressure symptoms, but it did not unlock the target throughput. The remaining
shape is now more specific:

- local pending can stay at `0`
- headroom and `may_recv_false` can stay near clean
- direct `.33 <-> .77` throughput is healthy
- QUIC client-side loss/congestion/blocking stays `0`
- the client still sees repeated multi-second waits on the active TUIC data
  stream before bytes become available

That evidence points away from more local credit tuning and toward TUIC/QUIC
ordered stream delivery behavior on the `.33 -> .27` leg, sing-box server-side
stream sending behavior, or an interoperability limit between mini_vpn's TUIC
client and the current sing-box exit.

## Mature Client A/B Follow-up

After the FI-FO runs, a mature sing-box client A/B was run on `.27` using the
official sing-box `v1.13.14` Linux amd64 release. The config used a TUN inbound
that routed only `.77/32` through a TUIC outbound to the same `.33` service; the
route to `.33` stayed on `eth0`, so the test avoided a proxy loop.

References used for the config shape:

- `https://sing-box.sagernet.org/configuration/outbound/tuic/`
- `https://sing-box.sagernet.org/configuration/inbound/tun/`
- `https://sing-box.sagernet.org/configuration/shared/tls/`

Local pulled artifacts:

- `/tmp/mini_vpn/knife14ab_singbox_client_ab/iperf3-reverse-30s.json`
- `/tmp/mini_vpn/knife14ab_singbox_client_ab/iperf3-reverse-30s-mtu1500.json`
- `/tmp/mini_vpn/knife14ab_singbox_client_ab/sing-box-client.log`
- `/tmp/mini_vpn/knife14ab_singbox_client_ab/sing-box-client-mtu1500.log`

The temporary runtime config files contained TUIC credentials and were removed
from `.27` after the run. They were not archived into the repository or local
result directory.

| Test | Client | Path | Receiver | Notes |
| --- | --- | --- | ---: | --- |
| B1 | sing-box `v1.13.14`, TUN MTU `1200` | `.27 -> TUIC .33 -> .77`, reverse | `17.196 Mbit/s` | interval max `162.529 Mbit/s`, repeated zero/near-zero seconds |
| B2 | sing-box `v1.13.14`, TUN MTU `1500` | `.27 -> TUIC .33 -> .77`, reverse | `27.751 Mbit/s` | interval max `125.913 Mbit/s`, repeated zero/near-zero seconds |
| Direct control | direct iperf on `.33 -> .77`, reverse | no TUIC | `205.814 Mbit/s` | same target service remained healthy |

Additional no-secret server-side observation: `.33` had a TUIC inbound with
`congestion_control=bbr` and `zero_rtt_handshake=true`, so the reverse-direction
QUIC sender was not using a conservative cubic-only server default during this
A/B.

This A/B rejects "mini_vpn client-local downlink credit/backpressure is the
remaining 100M blocker" as the main explanation. A mature sing-box client on the
same client VPS, same exit, same target, and same TUIC server also stayed in the
same low-throughput band as mini_vpn.

Follow-up Knife14fp found the missing server-side factor: `.33` had Linux socket
buffer caps/defaults of only `212992B`. Raising the exit VPS socket buffers to
`rmem_max/wmem_max=16777216` and `rmem_default/wmem_default=1048576`, then
restarting sing-box, moved the mature sing-box client to `185.242 Mbit/s` and
mini_vpn safe1200 reverse-first P1 to a `114.000 Mbit/s` reported receiver with
`stable_high` intervals averaging `189.483 Mbit/s`. See
`docs/tech/2026-07-08-knife14fp-server-socket-buffer-results.md`.

## Next Branch

Do not keep iterating these branches without new evidence:

- stale pool slots
- iperf3 or `.33 <-> .77` path
- sing-box service liveness
- egress pacer constants
- TUN queue length
- MTU/PLPMTUD
- local relay polling/waker cadence
- local read-credit caps
- smaller receive windows
- single-connection pool size

Recommended next work after Knife14fp:

1. Keep the `100+ Mbit/s` target. Do not lower it to `30 Mbit/s`.
2. Treat exit-side Linux socket buffers as a required preflight for
   high-throughput TUIC acceptance.
3. Fix the remaining high-rate close-tail dirty surfaces:
   terminal pending at close, timeout-driven reaping, small TUN drop delta, and
   the single `rx_blocked_stream` delta.
4. Compare with another mature TUIC implementation only if new evidence suggests
   sing-box-specific behavior after the socket-buffer preflight is satisfied.
