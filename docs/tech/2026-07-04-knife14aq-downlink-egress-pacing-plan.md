# Knife14aq plan - downlink egress pacing

Spec: `docs/tech/2026-07-04-knife14aq-downlink-egress-pacing-spec.md`

## Design Tree

Rejected branches:

1. iperf3 or target host issue: rejected by healthy direct `.27 -> .77`
   reverse baseline in Knife14ap.
2. sing-box or exit host issue: rejected by healthy `.33 -> .77` reverse
   baseline and clean TUIC service state.
3. QUIC loss/congestion as clean-window root cause: rejected by zero clean
   reverse loss/congestion delta and no inherited congestion.
4. `send_slice` failure or zero-progress root cause: rejected by Knife14ap
   `send_slice_zero=0`, `errors=0`, and accepted byte count matching remote
   bytes.
5. TUN flush syscall failure: rejected by `tun_flush_failures=0`.
6. TUN txqueuelen as product fix: useful as evidence only, but too
   OS/config-specific to be the code-level stage fix.

Chosen branch:

- Gate immediate remote-payload `iface.poll + flush_tx` behind an event-loop
  egress budget. Once the per-timer-interval budget is exhausted, keep accepting
  bytes into smoltcp when possible but let the timer poll/flush perform the TUN
  write. This adds local egress feedback without changing TUIC, server config,
  or downlink byte preservation.

## Tasks

1. TDD red: add focused unit tests for:
   - `DownlinkEgressPacer` immediate budget and timer reset;
   - zero immediate budget deferring all remote-payload flushes;
   - env parser defaults/bounds;
   - `TcpDownlinkDiag` aggregate formatting with deferred flush count;
   - low-RTT probe self-test summary parsing the new field.
2. Green: implement:
   - `MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES`;
   - `DownlinkEgressPacer`;
   - deferred immediate-flush diagnostic counter;
   - startup log line and suite env propagation;
   - low-RTT summary parsing.
3. Local verification:
   - focused Rust tests;
   - full `cargo test --lib client_tun`;
   - relevant shell self-tests;
   - `git diff --check`.
4. Stage code review:
   - hot-path behavior, byte preservation, config bounds, observability, and
     acceptance risk.
5. Learning:
   - record the stage and whether local verification supports a VPS run.
6. Commit and push this coherent task.

## VPS Checklist After Commit

Use `.27` writable TTY from the beginning. Keep `.env` sourced in the same shell.

Environment:

- `SUITE_TAG=knife14aq_egress_pacing_default`
- `BUILD_RELEASE=1`
- `RUN_REVERSE_FIRST_P1=1`
- `PARALLEL_SET="1"`
- `DURATION=30`
- `MINI_VPN_TUIC_CC=bbr`
- `MINI_VPN_TUIC_TCP_POOL=1`
- `MINI_VPN_TCP_DIAG=1`
- `CHECK_VPS_SERVICES=1`
- `DIRECT_IPERF_REVERSE_CHECK=1`
- `EXIT_TO_TARGET_IPERF_CHECK=1`
- `KILL_OLD=1`

Primary acceptance signals:

- clean reverse-first tunnel throughput improves materially from Knife14ap;
- `tun_tx_dropped_delta` falls or reaches zero;
- `downlink_flush` shows whether immediate flushes were deferred;
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`;
- no clean-window QUIC loss/congestion regression.
