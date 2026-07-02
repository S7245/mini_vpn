# Knife14p plan - downlink backpressure

Date: 2026-07-02

## Tasks

1. Add red unit tests:
   - pending stats report max/total backlog;
   - backpressure enters at high watermark;
   - backpressure remains active above low watermark;
   - backpressure resumes `global_rx` after pending drains below low watermark;
   - env parsing keeps sane defaults for invalid values.
2. Implement small helpers:
   - `DownlinkBackpressureConfig`;
   - pending stats collector;
   - hysteresis predicate.
3. Wire the event loop:
   - track `global_rx_paused`;
   - recompute pressure before `tokio::select!`;
   - guard `global_rx.recv()` with `if !global_rx_paused`;
   - emit one diagnostic line when pause/resume toggles.
4. Verify:
   - focused helper tests;
   - `cargo test --lib client_tun`;
   - `cargo test`;
   - `cargo clippy --all-targets -- -D warnings`;
   - `git diff --check`.
5. Code-review:
   - no payload drops;
   - no unbounded pending growth path remains in normal relay scheduling;
   - dirty flush, TUN ingress, timers, and reap still run while `global_rx` is
     paused;
   - watermarks are configurable for different VPS/RTT deployments.
