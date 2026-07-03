# Knife14w spec - coalesce relay writer uplink data

## Grounding

- Project goal from `AGENTS.md`: mini_vpn is the VPN data-plane core; stable
  TCP throughput under realistic pressure matters more than cosmetic cleanup.
- Test bundle:
  `/tmp/mvpn_knife14v_usclient_suite_20260703_103350.tar.gz`.
- Tested commit: `c90471c`.
- VPS preflight and build were healthy, and reverse TCP improved from the prior
  Kbit/s or timeout shape to roughly 153-183 Mbit/s for P1/P2/P4/P8.
- Forward TCP remains weak: standalone P1 receiver was 2.37 Mbit/s; full sweep
  P1/P2/P4/P8 receiver stayed around 1.15-2.33 Mbit/s with long zero-rate
  windows.
- The accept log shows forward P1 wrote about 10.5 MB upstream in 8529 writes,
  i.e. many 1160-byte class writes. There is no `remote_write_timeout`.

## Problem

`run_relay_writer` writes every `RelayCommand::Data` to the upstream stream as
one separate `write_all`. Under the current TUN MTU that usually means one
small MSS-sized payload per await. TUIC/QUIC stream scheduling and flow-control
can amplify that per-write overhead, causing local TCP to backpressure even
though the path can move reverse traffic at hundreds of Mbit/s.

## Goal

Reduce local-to-remote small-write amplification by coalescing already queued
relay `Data` commands into a bounded write batch inside the writer task.

## Non-goals

- Do not change downlink pending lifecycle, local Finish defer, or dead-slot
  reap behavior.
- Do not change TUIC congestion control, TCP pool settings, MTU defaults, or VPS
  service scripts.
- Do not introduce unbounded buffering outside the existing relay mpsc channel.
- Do not hide real write failures; direct `write_all` errors must still close
  the relay with `remote_write_failed`.

## Invariants

- Coalescing consumes only data already accepted into the bounded relay mpsc
  channel, preserving existing TCP backpressure at the channel boundary.
- A single coalesced write is capped by bytes and message count.
- `RelayCommand::Finish` preserves FIFO order: queued data is written first, then
  the upstream write half is shut down.
- Relay close diagnostics expose local write wait time so the next VPS run can
  distinguish writer backpressure from downlink/global_rx pressure.

## Acceptance

- Unit tests prove queued data coalesces into one writer call and queued Finish
  is deferred until after the coalesced payload.
- Existing relay failure, pending-write, local-finish, and established-uplink
  tests still pass.
- Local gates pass: focused `cargo test --lib client_tun`, full `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, and `git diff --check`.
- Next VPS run should show either materially better forward receiver throughput
  or clear `tcp-local-write-pressure` / close-log diagnostics proving the
  remaining bottleneck is true upstream write wait.
