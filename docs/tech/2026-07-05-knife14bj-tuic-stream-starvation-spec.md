# Knife14bj TUIC Stream Starvation Spec

Date: 2026-07-05

## Stage Goal

Knife14bj must explain the clean reverse-first shape where the TUIC TCP data
stream starts quickly but then delivers only tens of KiB while local
downlink/TUN/close accounting remains quiet.

The stage also upgrades TUIC auth-failure diagnostics so a future `.33`
`fail auth` or client-side `tuic auth finish` failure is classified before any
data-plane code is blamed.

## Grounding

Knife14bi restart rerun:

- reverse P1 receiver: `0.017 Mbit/s`;
- `TUN RX drain budget: 0`;
- `tun_rx_drain attempts=0`;
- no downlink backpressure, no TUN drops, no terminal pending, no send-slice
  errors, no QUIC loss/congestion/blocking;
- data stream first RX: `3ms`;
- data stream delivered only about `86KB`;
- data read/pending gaps were about `17s`.

Knife14bg showed a similar starvation shape before the drain A/B:

- receiver: `0.025 Mbit/s`;
- data stream delivered about `92KB`;
- local pending/backpressure/TUN/QUIC counters were quiet.

Knife14bh drain0 remains the known-good counterexample:

- receiver: `24.5 Mbit/s`;
- data stream delivered about `99MB`;
- data pending gap was under `2s`.

Therefore the next branch is not stale pool slots, TUN RX drain, close-drain,
terminal pending, local downlink backpressure, TUN drops, or clean-window QUIC
loss. The active branch is TUIC TCP stream starvation / remote-read scheduling.

## Failure Tree

Knife14bj must distinguish these branches:

1. `.33` auth/config/time failure:
   - sing-box rejects auth, has stale config, bad cert, ALPN/SNI mismatch, time
     drift, or UDP 8443/service state issue.
2. Server/target send starvation:
   - sing-box accepted the TUIC Connect but is not receiving enough bytes from
     `.77`, despite direct `.33 -> .77` baseline.
3. TUIC stream read scheduling:
   - mini_vpn polls the stream only on slow incidental wakeups, or a read task
     wakeup is missing.
4. QUIC receive/flow-control stall not visible in current summary:
   - current connection-level counters stay clean but stream-level flow-control
     or receive-window progress is blocked.
5. Local ACK/window branch not represented by downlink pending:
   - local smoltcp/TUN path allows initial bytes but then fails to sustain
     receive-window progress without showing app pending.

## Non-Goals

- Do not tune sing-box, iperf3, TUN queue length, egress pacing, TCP socket
  buffers, or connection pool in this stage.
- Do not change default data-plane behavior unless a focused test and local
  review identify a narrow diagnostic-only bug.
- Do not print TUIC UUIDs, passwords, password hashes, private keys, or full
  sing-box config in reports.

## Invariants

- Auth diagnosis must be no-secret: print booleans, lengths, ports, ALPN/SNI,
  cert validity, time delta, and service state only.
- If auth/startup fails, the suite must record enough `.33` evidence to
  separate service/config/time mismatch from mini_vpn interop.
- Stream-starvation diagnostics must be behavior-neutral by default: log only,
  no wakeup/backpressure changes.
- VPS reruns must remain scoped reverse-first P1 with
  `STOP_AFTER_REVERSE_FIRST_P1=1`.

## Acceptance

Local:

- suite self-test covers the new auth-diagnosis helper shape;
- shell syntax passes;
- focused Rust tests cover any new stream diagnostic formatter/accounting;
- parser self-test covers any new summary line;
- `git diff --check` and relevant cargo tests pass.

VPS:

- preflight `.33` service/config/time evidence is present when auth fails;
- default reverse-first run reports stream starvation with enough counters to
  decide whether the stream is idle because mini_vpn is not polling, because
  QUIC has no data ready, or because sing-box/target stopped sending.

## Stop Conditions

- If `.33` reports auth failure with matching config and low time skew, stop
  and inspect TUIC auth/QUIC interop instead of restarting repeatedly.
- If stream diagnostics show mini_vpn is polling an idle TUIC stream while
  `.33 -> .77` direct reverse is healthy, stop and perform TUIC/sing-box
  interoperability review before local lifecycle tuning.
