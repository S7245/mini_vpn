# 2026-07-08 Knife14fz sing-box Client Comparison And Pressure-Credit Plan

## Goal

Study the mature sing-box client path that reached `173/173 Mbit/s` in the
same `.27 -> .33 -> .77` window, compare it with the current mini_vpn branch
and with `f8765c1`, and extract a narrow mini_vpn plan for reaching the same
bandwidth class.

This stage is read-only. It does not change VPS settings, iperf3, MTU,
PLPMTUD, stale pool behavior, or broad QUIC window settings.

## Evidence

Current mini_vpn evidence:

- [docs/tech/2026-07-08-knife14fu-fw-fx-reverse-discriminator-results.md](/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/2026-07-08-knife14fu-fw-fx-reverse-discriminator-results.md)
- [docs/tech/2026-07-08-knife14fy-ordered-read-service-restore-results.md](/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/2026-07-08-knife14fy-ordered-read-service-restore-results.md)

sing-box source inspected for the same client family used in the `.27` A/B:

- local clone: `/tmp/mini_vpn/sing-box-src-v1.13.14`
- source commit: `25a600d`
- module versions:
  - `github.com/sagernet/sing-quic v0.6.1`
  - `github.com/sagernet/sing v0.8.11`
  - `github.com/sagernet/sing-tun v0.8.11`

Key files inspected:

- `/tmp/mini_vpn/sing-box-src-v1.13.14/protocol/tuic/outbound.go`
- `/tmp/mini_vpn/sing-box-src-v1.13.14/route/conn.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing-quic@v0.6.1/tuic/client.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing@v0.8.11/common/bufio/copy.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing@v0.8.11/common/bufio/splice_linux.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing-tun@v0.8.11/stack_gvisor.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing-tun@v0.8.11/stack_gvisor_tcp.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing-tun@v0.8.11/stack_gvisor_lazy.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing-tun@v0.8.11/tun_linux_gvisor.go`
- `/Users/liushan/go/pkg/mod/github.com/sagernet/sing-tun@v0.8.11/tun_offload.go`

## sing-box Findings

### 1. TUIC TCP open path is thin

The sing-box TUIC outbound does not add a custom staged TCP relay layer above
the QUIC stream. The client path is:

1. `Outbound.DialContext(...)`
2. `tuic.Client.DialConn(...)`
3. `quicConn.OpenStream()`
4. first `Write(...)` prepends the TUIC connect request and then payload

That means TCP downlink progress is primarily controlled by the QUIC stream,
the `net.Conn` copy loop, and the local TCP/TUN stack rather than by a custom
"remote read service -> staging queue -> headroom gate -> self-wake" chain.

### 2. TCP relay is a plain full-duplex copy loop

sing-box routes TCP with two goroutines and lets each direction run a steady
copy loop:

- `go m.connectionCopy(ctx, conn, remoteConn, false, ...)`
- `go m.connectionCopy(ctx, remoteConn, conn, true, ...)`

The copy path uses `bufio.CopyWithIncreateBuffer(...)` with:

- `DefaultIncreaseBufferAfter = 512 * 1000`
- `DefaultBatchSize = 8`

It relies on pooled buffers and, where possible, direct copy/splice-style
paths. There is no equivalent of mini_vpn's "install pressure debt at target
edge and then stop granting useful read credit."

### 3. The local TCP stack is mature and owns most backpressure decisions

sing-tun creates a gVisor TCP stack and exposes accepted TCP flows as
`net.Conn` through `gLazyConn`. The gVisor stack enables:

- TCP SACK
- moderate receive buffer growth
- explicit TCP send/receive buffer ranges

The result is that throughput control mostly happens inside a mature TCP stack
that can absorb pressure, grow buffers, and expose normal `Read`/`Write`
backpressure semantics to the copy loop.

### 4. Linux TUN output is aggressively batched

The Linux gVisor TUN path uses:

- `rawfile.NonBlockingWriteIovec(...)`
- GSO/GRO support when available
- `idealBatchSize = 128`

This is a real performance advantage, but it is a second-order explanation for
the current mini_vpn gap. The present mini_vpn failure occurs earlier: reverse
throughput is being suppressed before the current branch reaches TUN drops or a
hard send-queue pause edge.

## mini_vpn Contrast

The current mini_vpn branch restored data movement, but it still throttles
reverse downlink too early.

Current branch retry on `653d62bf`:

- reverse-first P1: `21.4/20.0 Mbit/s`
- `remote_to_global_rx_bytes=75502079`
- `may_recv_false=7655`
- `headroom_limited=7330`
- `headroom_deferred_bytes=10188203`
- `pressure_credit_blocked_bytes=960157`
- `send_queue_max=503160`
- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocking deltas `0`

Known data-moving comparison on `f8765c1`:

- reverse-first P1: `19.9/18.7 Mbit/s`
- `remote_to_global_rx_bytes=70141010`
- `may_recv_false=0`
- `headroom_limited=5`
- `headroom_deferred_bytes=425984`
- `pressure_credit_blocked_bytes=236701`
- `send_queue_max=458752`
- `tun_tx_dropped_delta=0`
- QUIC loss/congestion/blocking deltas `0`

Mature sing-box client in the same VPS window:

- reverse-first P1: `173/173 Mbit/s`

Direct code diff note against `f8765c1`:

- the inspected pressure-credit controller skeleton is already present there:
  target-edge debt installation, drain-credit accounting, and target-edge flush
  clamping are not new to the current branch;
- the most visible current-vs-`f8765c1` differences in the inspected areas are
  TUIC diagnostics and observability around ordered relay mode, startup auth
  attempts, self-wake counters, and relay read-service range reporting.

The current mini_vpn limiter is visible in the code:

- `should_install_downlink_pressure_credit_debt(...)` installs pressure debt as
  soon as raw pressure reaches `tx_queue_egress_target_threshold(cfg)`.
- `bounded_downlink_flush_limit_for_window_with_clock_and_drop(...)` then
  clamps available bytes with `guarded_hard_headroom =
  target_edge - send_queue`.

That means the branch begins treating the derived target edge as a hard stop
even when:

- `send_capacity` is still large;
- `tun_tx_dropped_delta=0`;
- QUIC is clean; and
- the real hard pause edge has not been reached.

So the actionable regression is not "the current branch introduced a totally
new controller." Instead, the current restored ordered path is driving the same
controller into a much harsher runtime shape than `f8765c1`, and sing-box shows
that this early local suppression is not required for `100+ Mbit/s`.

## Borrowed Lessons

mini_vpn does not need to copy sing-box architecture wholesale to recover the
next `100+ Mbit/s` step.

The immediate lesson to borrow is behavioral:

1. Treat the target edge as a soft warning, not as an automatic debt trigger.
2. Spend clean/drain credit until a real hard safety signal appears.
3. Escalate to hard read suppression only on stronger evidence:
   - actual TUN drops;
   - flush failures;
   - hitting the true pause edge; or
   - sustained target-edge pressure across multiple observations.

The medium-term lesson is architectural:

1. Favor natural `Read`/`Write` backpressure over custom receive throttles.
2. Keep the hot path close to "read QUIC stream -> write local TCP/TUN" rather
   than inserting more local debt accounting in front of the stream reader.
3. Revisit TUN batching/GSO later if mini_vpn still lags after the local
   pressure-credit fix.

## Proposed TDD Slice

The next code stage should stay narrow and start with tests.

Tests to change or add in `src/client_tun.rs`:

1. Update the current target-edge debt tests so a single target-edge sample is
   no longer sufficient to install pressure debt by default.
2. Add a flush-limit test proving that, without drop debt and without hard
   pause, drain credit may be spent above the target edge and up to the hard
   credit spend threshold.
3. Add a controller/read-credit test proving that a no-drop, send-capacity
   available window does not flip into `may_recv=false` merely because the
   target edge was touched once.
4. Preserve the existing hard invariants:
   - bounded pending bytes;
   - close-drain remains bounded;
   - drop debt still blocks extra credit;
   - pause-edge protection still holds.

## Proposed Implementation Shape

The smallest implementation experiment is:

1. Stop installing pressure credit debt on a single target-edge observation.
2. Keep target-edge accounting as a diagnostic watermark.
3. Only install pressure debt when one of these becomes true:
   - `max_tx_queue >= tx_queue_pause_threshold(cfg)`;
   - pending is high and tx queue is already at or above the clean flush edge
     for a sustained window; or
   - explicit TUN/egress failure feedback requests debt.
4. In `bounded_downlink_flush_limit_for_window_with_clock_and_drop(...)`,
   replace the current target-edge hard clamp with a harder bound closer to
   `tx_queue_credit_spend_threshold(cfg)` when there is no active debt and no
   drop signal.

This keeps the safety model bounded while making mini_vpn behave more like the
sing-box client path: allow useful reverse downlink to keep moving until a real
pressure signal appears.

## Acceptance Bias After The Change

Do not ask the next stage to prove `173 Mbit/s` immediately.

The first acceptance goal after the code change should be:

- clearly above the current `20 Mbit/s` band;
- materially reduced `may_recv_false`;
- materially reduced `headroom_deferred_bytes`;
- materially reduced `pressure_credit_blocked_bytes`;
- still clean `pending_at_close`, `terminal_pending_reap`, TUN drop accounting,
  and QUIC loss/blocking surfaces.

If that focused fix does not move the branch into a much higher throughput
band, then the next architecture step should revisit TUN batching/GSO and the
broader copy/backpressure model instead of iterating constants blindly.
