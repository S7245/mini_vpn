# Knife14s: batch established uplink drain per dirty pass

## Grounding

- Project goal from `AGENTS.md`: mini_vpn is the VPN data-plane core; under
  high concurrency and long-running TCP flows, correctness and stable throughput
  matter more than cosmetic changes.
- Test bundle:
  `/tmp/mvpn_knife14r_usclient_suite_20260702_171341.tar.gz`.
- Preflight was healthy: `.33` ping passed and `.77:5201` direct iperf worked at
  hundreds of Mbit/s.
- `remote_write_timeout` stayed absent, so the knife14q uplink write-timeout fix
  did not regress.
- Forward tunnel throughput stayed around 1-2 Mbit/s and showed long zero-bps
  windows. This matches the current code shape: an established relay drains only
  one local payload per dirty pass, so if no new inbound packet wakes the loop,
  the 5ms timer bounds throughput around one MSS per tick.
- Reverse/downlink still shows pending reaps, but that is a separate lifecycle
  branch. This stage targets the forward uplink throttle first because the code
  gives a deterministic explanation for the measured rate.

## Problem

`process_listener_activity` reads at most one smoltcp payload for an established
uplink and then returns. When the socket already has multiple readable chunks,
the rest wait for a future dirty pass. Under quiet periods that future pass is
the 5ms timer, effectively rate-limiting forward TCP to timer frequency.

## Goal

Drain multiple established-uplink payloads in one dirty pass while preserving
bounded fairness and TCP backpressure.

## Non-Goals

- Do not read from smoltcp when the relay mpsc channel is full.
- Do not introduce an unbounded per-flow loop that can monopolize the main loop.
- Do not change downlink pending/backpressure watermarks in this stage.
- Do not claim reverse pending reaps are fully solved.

## Invariants

- Each dirty pass may forward up to `MAX_ESTABLISHED_UPLINK_BATCH` payloads from
  one established socket.
- If `try_reserve` reports `Full`, the function must not consume local TCP
  bytes; smoltcp then naturally advertises a smaller window.
- If `try_reserve` reports `Closed`, the existing `uplink_channel_closed` rearm
  path stays in effect.
- Local FIN (`CloseWait`) still sends exactly one `RelayCommand::Finish` after
  payload data drains.
- Remaining readable data keeps the handle dirty through the existing
  `socket.can_recv()` check.

## Acceptance

- Unit tests cover multi-payload batch drain, batch cap, channel-full
  backpressure, and local-FIN behavior.
- Local verification passes: `cargo test --lib client_tun`, full `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, and `git diff --check`.
- Next VPS run should improve forward P1/P2/P4/P8 out of the one-MSS-per-timer
  pattern, or show a new bottleneck such as relay mpsc full / QUIC write
  pressure instead of the old 5ms drain limit.
