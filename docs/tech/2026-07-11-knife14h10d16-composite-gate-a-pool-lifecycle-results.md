# Knife14h10d16 Composite Gate A And TCP Pool Lifecycle Results

Date: 2026-07-11

## Outcome

The composite Gate A failed before Gate B, but it did not reopen the D16
byte-owned TCP/TUN egress architecture.

The capable-window precondition passed with the mature sing-box client at
`159.993/157.650 Mbit/s` sender/receiver. The direct reverse baseline was
`212.607 Mbit/s` receiver. The exact clean `f1627bc` pool-2 D16 build then
failed both composite subproofs:

- A-capacity: `0.262/0.0265 Mbit/s` sender/receiver;
- A-clean fixed `64 MiB`: only about `3.68 MiB` arrived before timeout,
  reported receiver `0.772 Mbit/s`.

Both flows kept TUN drops, actor bypass, local pressure/backlog, send/flush
errors, and QUIC loss/congestion/blocking at zero. The target-side iperf log
showed that the sender itself produced only a small burst through the TUIC
stream; mini_vpn admitted all payload it received. The selected failure is
therefore upstream of the D16 actor: the sing-box-to-Quinn stream stopped
making sustained progress.

Gate A is not passed. Gate B remains frozen.

## Controlled Discriminators

The temporary Exit used the same sing-box version and was removed after the
runs. The original `.77` iperf3 service remained active.

1. Full server configuration with Cubic passed the mature-client precondition
   at `157.650 Mbit/s` receiver.
2. Switching only the temporary server to BBR left mini_vpn low at about
   `14.6 Mbit/s` receiver. Congestion-control choice is not a sufficient fix.
3. Minimal TUIC-only server configurations did not preserve the required
   `>150 Mbit/s` mature-client control. They cannot qualify a Gate A result.
4. Keeping the capable full Cubic server and changing only
   `MINI_VPN_TUIC_TCP_POOL=1` improved mini_vpn to `116/115 Mbit/s`. It
   transferred about `274 MiB`, with zero TUN drops and zero local pressure,
   but had a `3.807s` maximum data read gap and a tail-collapse shape.

Pool 1 is a discriminator, not an accepted product configuration: it remains
below the `>150 Mbit/s` Gate A floor and removes connection-level concurrency.

## Code-Level Reachability Review

The D16 hot path still satisfies its approved contracts:

- Quinn read is preceded by RAII byte reservation;
- payload is held by the bounded leased byte queue;
- global events carry readiness, not payload;
- only the D16 actor calls downlink `send_slice`;
- DrainOnly continues ACK/TUN RX, poll, flush, and permit release;
- remote EOF is hidden until owned bytes drain.

The connection-pool path contains a separate lifecycle policy:

- `live_tcp_conn` selects slots round-robin;
- the primary connection (`conn=0`) is exempt from idle stale reconnect;
- an idle auxiliary slot is forcibly closed and re-authenticated after only
  `10s` before reuse;
- the failed pool-2 data stream ran on the freshly reconnected `conn=1`;
- the pool-1 discriminator ran on persistent `conn=0` and made much more
  progress.

This is evidence against changing the D16 queue cap, actor quantum, MTU,
receive windows, chunk size, or self-wake. It selects the destructive
auxiliary stale-reconnect policy and main/aux lifecycle asymmetry as the next
testable seam. It does not yet prove that removing the reconnect alone is
sufficient for `>150 Mbit/s`, because the persistent primary result was only
`115 Mbit/s` and a prior pool-2 run reached `183 Mbit/s`.

## Proposed Next Stage

Use diagnose plus TDD before another Gate A:

1. Add a focused RED policy test showing that a healthy authenticated
   auxiliary connection with no close reason must not be destructively
   reconnected solely because ten seconds elapsed.
2. Separate observable transport health from elapsed idle time. Closed or
   explicitly unhealthy slots may reconnect; healthy idle slots remain
   reusable. Preserve per-slot locking and active-stream exclusion.
3. Add connection-selection diagnostics that report slot generation, reconnect
   reason, last-use age, and whether the Gate data flow used primary or
   auxiliary, without payload or credentials.
4. Run local pool lifecycle tests and all existing TUIC/D16 regressions.
5. Run one scoped VPS A/B on the same capable Exit: pool 2 with the healthy-slot
   policy, proving the data flow uses an auxiliary slot. Require the mature
   control `>150 Mbit/s` first. Only a clean `>150 Mbit/s` result authorizes a
   new composite Gate A.

Do not promote pool 1 as the fix and do not proceed to Gate B from the current
evidence.

## Cleanup

- `.77` temporary TUIC Exit stopped and its runtime files were removed.
- `.77` socket buffer values were restored to their original `212992` values.
- `.77` iperf3 remained active on port `5201`.
- `.33` sing-box remained active with the persistent high-buffer settings.
- `.27` had no mini_vpn/TUN residue and target routing returned to `eth0`.
- The clean `.27` detached worktree and transient helpers were removed.
- Secret-free diagnostic bundles were pulled to local `/tmp`.
