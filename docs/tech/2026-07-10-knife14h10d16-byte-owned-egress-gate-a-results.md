# Knife14 H10d16 Byte-Owned Egress Gate A Results

Date: 2026-07-10

## Outcome

Gate A failed. The single approved `20s` reverse-first P1 produced no iperf
interval samples and the probe exited by its `40s` outer timeout. Gate B was not
started.

The failure is not a throughput-capacity result. The iperf control exchange
stalled after the first remote byte, before a reverse data stream existed.

## Run Shape

- Client VPS: `43.172.75.27`
- Exit VPS: `43.153.32.33`
- Target VPS: `43.130.32.77:5201`
- Profile: `MINI_VPN_H10D16_BYTE_OWNED_EGRESS=1`
- Shape: reverse-first, `P=1`, requested duration `20s`, stop after first P1
- Direct reverse baseline: `277 Mbit/s` receiver
- Exit service: active
- Target service: active
- Exit socket buffers: accepted `16 MiB` max and `1 MiB` defaults

No VPS, MTU, iperf3, QUIC-window, pool, or socket-buffer setting was changed.

## Evidence

Local bundle:

```text
/tmp/mini_vpn_knife14h10d16_byte_owned_gatea/
  mvpn_knife14h10d16_byte_owned_gatea_usclient_suite_20260710_133358.tar.gz
  mvpn_knife14h10d16_byte_owned_gatea_usclient_suite_20260710_133358.md
  mvpn_knife14h10d16_byte_owned_gatea_usclient_tunnel_mtu1200_reverse_first_p1_20260710_133358.md
  mvpn_knife14h10d16_byte_owned_gatea_server_evidence_mtu1200_reverse_first_p1_20260710_133358.md
  mvpn_accept_20260710_133358.log
```

Key client signals:

- D16 production selection was reached:
  `relay_mode=d16_direct_ordered`, `engine=d16_byte_owned`.
- The first remote byte arrived in `3ms`.
- The actor accepted and flushed that byte:
  `actor_admitted_bytes=1`, `actor_bypass_admitted_bytes=0`, followed by
  `flushed_tcp_payload=1 released_inflight_permits=1`.
- No local pressure or transport failure explained the stall:
  `tx_dropped_delta=0`, no send-slice error, no flush failure, no QUIC loss,
  congestion, or flow-control blocking.
- The TUIC stream then had a `39995ms` read gap. It closed with only `2` total
  received bytes and no data-stream classification.
- The local TCP socket closed with the first response byte still unacknowledged:
  `send_queue=1`, `close_egress_class=terminal_closed_no_send`,
  `close_egress_bytes=1`.
- The iperf result had no interval samples and unknown sender/receiver Mbps.

Exit/target evidence from the same time window:

- sing-box accepted the TUIC connection and opened a direct connection to
  `43.130.32.77:5201`; it recorded no path/auth/connect error for this flow.
- iperf3 accepted the connection from the exit VPS and later reported that the
  client terminated.

The suite's server-evidence collection lost its SSH session while reading the
target clock. The cleanup trap still stopped client-tun and restored the target
route, and the client bundle was complete. Exit and target evidence above was
collected read-only afterward for the exact run window; no traffic was rerun.

## Gate A Decision Table

- `receiver > 150 Mbit/s`: **fail**; no data interval existed.
- `tx_dropped_delta = 0`: pass.
- `close_egress_bytes = 0`: **fail**; value was `1`.
- `terminal_pending_reap = 0`: pass.
- `actor_bypass_admitted_bytes = 0`: pass.
- active remote-read gap `< 1s`: **fail**; `39995ms`.
- active local-egress gap `< 1s`: not a capacity stream; the only available
  bytes were admitted and flushed promptly.
- QUIC loss/congestion/blocking non-root: pass.

## Diagnosis

The current deterministic harness proves the one-way owned-byte path, but it
does not exercise the production bidirectional control loop:

```text
remote byte -> D16 queue -> actor -> smoltcp/TUN
            -> local ACK/control payload -> smoltcp -> relay writer
```

The run proves the left half delivered the first byte and that the target then
waited for the next client control message. The first byte remained in the
smoltcp send queue until close, so the next discriminator is local TUN RX / ACK
service and writer progress after a D16-triggered flush. It is not justified to
tune Quinn windows, MTU, chunk size, pool size, stale-slot logic, or self-wake.

## Proposed Modification Plan

1. Add a deterministic bidirectional production-seam test: deliver a one-byte
   remote control response through the owned queue, observe its TUN flush,
   inject the local ACK plus next control payload, and assert the existing
   relay writer receives that payload without a timer-only dependency.
2. Add bounded counters for D16 TUN-flushed payload, intercepted local TCP
   packets/payload, and relay-writer progress. Record lengths/counts only; never
   log payload or credentials.
3. Use the red test to determine whether D16 readiness must arm the existing
   bounded active-flow TUN RX service, or whether a lower-level lost-readiness
   seam must be fixed. Do not enable the legacy D3 self-wake flag or change its
   timing as a guess.
4. Rerun focused D16 tests and all Task 10 local gates. Do not run Gate B.
5. The implementation plan allowed only one Gate A, and that run is now spent.
   Any replacement scoped VPS validation requires explicit user authorization.

## Local Follow-Up

After user confirmation, the proposed bidirectional seam was implemented with
two real smoltcp stacks and a D16 native byte-owned relay. The harness suppresses
the async TUN wait edge immediately after the first TCP payload flush while
leaving nonblocking `try_recv_rx` available. This recreates the production
failure shape without a VPS:

- before the fix: first response observed, TCP opens `1`, second control echo
  absent after `2s`;
- after the fix: the one-byte first response and the following `1532B` control
  message both round-trip intact in about `20ms`, with TCP opens still `1`.

Root cause: the D16 `DataReady` branch invoked the actor but did not arm the
existing bounded `10ms` active-flow TUN RX follow-up that the legacy payload
branch arms after useful downlink progress. Once the actor flushed its first
response and removed the flow from `dirty`, a suppressed/cancelled wait edge
left the local ACK/control packet unserviced.

The fix reuses the existing active-flow duration and drain budget only after
measured D16 actor progress. It does not add or retune a timer, enable D3
self-wake, or change MTU, QUIC windows, chunk size, or queue capacity.

Post-fix local gates:

- D16 focused feature tests: `40/40`;
- normal library: `564/564`;
- feature library: `570/570`;
- integration targets: `2/2` and `10 passed` with `4` existing non-D16 ignored;
- formatting, check, diff, and suite self-test: pass.

No replacement Gate A or Gate B has been run. Replacement VPS validation still
requires explicit user authorization.
